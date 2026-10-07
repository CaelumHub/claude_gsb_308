"""并发调度：构建池 + 用例池 + 定时触发循环。

这是平台「测试调度与并发」难点的核心。一次构建要并发执行大量用例，多场
构建又要并行推进，同时定时任务到点还要自动触发新构建。三层并发：

1. **构建级并发**：一个线程池（``build_pool``）承载多场同时进行的构建，
   用 ``max_build_workers`` 限制并发构建数，避免磁盘/CPU 被打满；
2. **用例级并发**：每场构建内部再用一个线程池（``case_pool``）并发跑
   用例，用 ``max_case_workers`` 限制单构建内的并发度；结果通过
   :meth:`storage.buildstore.BuildStore.record_result` 在文件锁保护下
   并发安全地收集与聚合；
3. **定时触发**：每条计划绑定自己的 IANA 时区，cron 按该时区的墙上时间
   解释；计划持久化一个 ``next_fire_at``（UTC 时间戳），后台循环按
   ``tick`` 间隔扫描，到点即触发并推进 ``next_fire_at``，天然不会同一
   分钟重复触发。若系统停机错过了触发点，恢复后按计划的
   ``misfire_policy`` 处理：``catch_up`` 立即补跑一次（合并所有错过），
   ``mark_missed`` 记录一条「已错过」历史——绝不悄悄跳过。夏令时行为见
   :mod:`engine.cron` 的约定（重叠取第一次、不存在则顺延）。

取消：每个构建持有一个 ``threading.Event``，用例执行器在步骤之间检查它，
取消后已在跑或用例尽快中止、未跑的不再启动，最终构建标为 ``cancelled``。
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from .cron import local_timezone_name, next_fire_timestamp, parse_cron
from .models import new_id


class Scheduler:
    """测试并发调度器。"""

    def __init__(self, registry, build_registry, executor, env_manager,
                 report_gen, coverage_analyzer, defect_manager, notify_manager,
                 max_build_workers: int = 4, max_case_workers: int = 8,
                 tick_seconds: float = 20.0, misfire_threshold: float = 120.0):
        self.registry = registry
        self.builds = build_registry
        self.executor = executor
        self.env_manager = env_manager
        self.report_gen = report_gen
        self.coverage = coverage_analyzer
        self.defects = defect_manager
        self.notify = notify_manager

        self.max_build_workers = max_build_workers
        self.max_case_workers = max_case_workers
        self.tick_seconds = tick_seconds
        # 到点判定宽限：触发点距今不超过该秒数视为「正常到点」；超过则
        # 认定为停机/卡顿造成的错过，走 misfire_policy 补偿。必须大于
        # tick_seconds，否则正常 tick 抖动会被误判为错过。
        self.misfire_threshold = misfire_threshold

        self._build_pool = ThreadPoolExecutor(
            max_workers=max_build_workers, thread_name_prefix="build")
        self._running: dict[str, dict] = {}
        self._running_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._tick_thread: Optional[threading.Thread] = None
        self._scan_lock = threading.Lock()

    # ------------------------------------------------------------------ 启动
    def start(self) -> None:
        if self._tick_thread is None:
            self._tick_thread = threading.Thread(
                target=self._tick_loop, name="scheduler-tick", daemon=True)
            self._tick_thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        self._build_pool.shutdown(wait=False, cancel_futures=True)

    # ------------------------------------------------------------------ 触发
    def submit_build(self, project_id: str, suite_id: str,
                     env_id: Optional[str] = None, trigger: str = "manual") -> dict:
        """提交一场构建，立即返回构建元信息（构建在后台线程池运行）。"""
        suites = self.registry.store("suites")
        cases_store = self.registry.store("cases")

        suite = suites.get(suite_id)
        if suite is None:
            return {"error": "测试套件不存在"}

        env_id = env_id or suite.get("env_id")
        if not env_id:
            envs = self.env_manager.list(project_id)
            if not envs:
                return {"error": "项目还没有可用环境，请先创建环境"}
            env_id = envs[0]["id"]
        if self.env_manager.get(env_id) is None:
            return {"error": "环境不存在"}

        case_ids = suite.get("case_ids") or []
        cases = cases_store.get_many(case_ids)
        if not cases:
            return {"error": "套件内没有用例"}

        build_id = new_id("build")
        build = self.builds.for_project(project_id).create(
            build_id,
            suite_id=suite_id,
            env_id=env_id,
            name=suite.get("name", ""),
            trigger=trigger,
        )

        cancel_event = threading.Event()
        with self._running_lock:
            self._running[build_id] = {
                "cancel": cancel_event,
                "project_id": project_id,
                "suite_id": suite_id,
            }

        self._build_pool.submit(
            self._run_build, project_id, build_id, cases, env_id, cancel_event)

        return build

    def cancel_build(self, build_id: str) -> dict:
        handle = self._running.get(build_id)
        if handle is None:
            return {"error": "构建不在运行中或不存在"}
        handle["cancel"].set()
        return {"ok": True, "build_id": build_id}

    def running(self) -> list[dict]:
        out = []
        with self._running_lock:
            for build_id, handle in list(self._running.items()):
                build = self.builds.for_project(handle["project_id"]).get(build_id)
                out.append({
                    "build_id": build_id,
                    "project_id": handle["project_id"],
                    "suite_id": handle["suite_id"],
                    "status": build.get("status") if build else "running",
                    "total": build.get("total", 0) if build else 0,
                    "passed": build.get("passed", 0) if build else 0,
                    "failed": build.get("failed", 0) if build else 0,
                    "started_at": build.get("started_at") if build else None,
                })
        return out

    # ------------------------------------------------------------------ 构建执行
    def _run_build(self, project_id: str, build_id: str, cases: list,
                   env_id: str, cancel_event: threading.Event) -> None:
        store = self.builds.for_project(project_id)
        env_config = self.env_manager.to_executor_config(env_id)
        store.set_total(build_id, len(cases))
        store.append_log(build_id, f"构建 {build_id} 开始，共 {len(cases)} 个用例，"
                                   f"环境 {env_id}")

        case_workers = max(1, min(self.max_case_workers, len(cases)))
        try:
            with ThreadPoolExecutor(max_workers=case_workers,
                                    thread_name_prefix=f"case-{build_id[:6]}") as pool:
                futures = {}
                for i, case in enumerate(cases):
                    if cancel_event.is_set():
                        break
                    futures[pool.submit(
                        self._run_one, case, env_config, env_id, i,
                        cancel_event)] = case

                for future in as_completed(futures):
                    case = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:  # noqa: BLE001
                        result = {
                            "case_id": case.get("id"),
                            "case_name": case.get("name", "未命名用例"),
                            "group": (case.get("tags") or ["默认"])[0],
                            "priority": case.get("priority", "P3"),
                            "status": "error",
                            "duration": 0.0,
                            "steps": [],
                            "assertions": [],
                            "logs": [f"用例执行异常: {exc}"],
                        }
                    self._persist_result(store, build_id, case, result)
                # 未提交的用例（被取消跳过）记为 skipped
                submitted = {case.get("id") for case in futures.values()}
                for case in cases:
                    if case.get("id") not in submitted:
                        skipped = {
                            "case_id": case.get("id"),
                            "case_name": case.get("name", "未命名用例"),
                            "group": (case.get("tags") or ["默认"])[0],
                            "priority": case.get("priority", "P3"),
                            "status": "skipped",
                            "duration": 0.0,
                            "steps": [], "assertions": [],
                            "logs": ["因取消而未执行"],
                        }
                        self._persist_result(store, build_id, case, skipped)
        except Exception as exc:  # noqa: BLE001
            store.append_log(build_id, f"构建执行异常: {exc}")

        # 终态判定
        build = store.get(build_id)
        if cancel_event.is_set():
            status = "cancelled"
        elif (build.get("failed", 0) + build.get("error", 0) + build.get("timeout", 0)) == 0:
            status = "passed"
        else:
            status = "failed"
        store.finish(build_id, status)
        store.append_log(build_id, f"构建结束: {status}（通过 {build.get('passed', 0)}"
                                   f"/{build.get('total', 0)}）")

        # 收尾：报告 + 覆盖率 + 通知 + 自动缺陷
        self._finalize(project_id, build_id)

        with self._running_lock:
            self._running.pop(build_id, None)

    def _run_one(self, case: dict, env_config: dict, env_id: str,
                 index: int, cancel_event: threading.Event) -> dict:
        result = self.executor.execute_case(
            case, env_config, cancel_event=cancel_event,
            timeout=case.get("timeout", 60))
        result["env_id"] = env_id
        result["order"] = index
        return result

    def _persist_result(self, store, build_id: str, case: dict, result: dict) -> None:
        store.record_result(build_id, result)
        case_id = case.get("id")
        if case_id:
            log_text = "\n".join(result.get("logs", []))
            store.write_case_log(build_id, case_id, log_text)

    def _finalize(self, project_id: str, build_id: str) -> None:
        store = self.builds.for_project(project_id)
        build = store.get(build_id)
        if build is None:
            return
        total = build.get("total", 0)
        passed = build.get("passed", 0)
        passed_ratio = (passed / total) if total else 1.0

        try:
            self.report_gen.build_report(project_id, build_id, force=True)
        except Exception:  # noqa: BLE001
            pass
        try:
            self.coverage.generate(project_id, build_id, passed_ratio)
        except Exception:  # noqa: BLE001
            pass

        # 通知
        event = "build.passed" if build["status"] == "passed" else "build.failed"
        payload = {
            "build_id": build_id,
            "project_id": project_id,
            "status": build["status"],
            "passed": passed,
            "total": total,
            "pass_rate": round(passed_ratio * 100, 1),
            "duration": build.get("duration", 0.0),
        }
        self.notify.fire(project_id, "build.finished", payload)
        self.notify.fire(project_id, event, payload)

        # 自动缺陷（项目配置开启时，把失败用例转成缺陷）
        project = self.registry.store("projects").get(project_id)
        if project and project.get("auto_create_defects"):
            failures = store.results(build_id, where=[("status", "in", ["failed", "error", "timeout"])])
            for fr in failures[:20]:
                self.defects.create_from_case(project_id, fr, build_id)

    # ------------------------------------------------------------------ 定时循环
    def _tick_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._scan_schedules()
            except Exception:  # noqa: BLE001
                pass
            self._stop_event.wait(self.tick_seconds)

    def _scan_schedules(self, now_ts: Optional[float] = None) -> None:
        # 串行化扫描，避免多个线程（后台 tick + 手动触发）同时读到
        # 「同一触发点未到」而重复触发同一计划。
        with self._scan_lock:
            self._scan_schedules_locked(now_ts)

    def _scan_schedules_locked(self, now_ts: Optional[float] = None) -> None:
        """扫描所有启用的计划，触发到点的构建并处理错过补偿。

        ``now_ts`` 可注入（测试用），默认取当前时间。判定完全基于计划上
        持久化的 ``next_fire_at``（UTC 时间戳），与服务器本地时区无关。
        """
        now_ts = time.time() if now_ts is None else now_ts
        schedules_store = self.registry.store("schedules")
        for schedule in schedules_store.all():
            if not schedule.get("enabled", True):
                continue
            tz_name = schedule.get("timezone") or local_timezone_name()
            cron = schedule.get("cron", "* * * * *")
            try:
                due_at = schedule.get("next_fire_at")
                if due_at is None:
                    # 新计划（或老数据迁移）：先落一个未来的触发点，不补历史
                    nxt = next_fire_timestamp(cron, tz_name, now_ts)
                    schedules_store.update(schedule["id"], {"next_fire_at": nxt})
                    continue
                if now_ts < due_at:
                    continue  # 未到点
                self._fire_due(schedule, tz_name, due_at, now_ts)
            except ValueError:
                continue  # 表达式 / 时区非法（创建时已校验，防御性跳过）

    def _fire_due(self, schedule: dict, tz_name: str, due_at: float,
                  now_ts: float) -> None:
        """处理一个已到触发点的计划：正常触发或按策略补偿错过。"""
        sid = schedule["id"]
        cron = schedule.get("cron", "* * * * *")
        store = self.registry.store("schedules")

        if now_ts - due_at <= self.misfire_threshold:
            # 正常到点：触发一次，并把下一个触发点推到本次之后（无损推进，
            # 即便 tick 抖动跨过下一个触发点，下个扫描周期也会补上）。
            build = self.submit_build(
                schedule.get("project_id"), schedule.get("suite_id"),
                env_id=schedule.get("env_id"), trigger="schedule")
            self._record_run_from_build(schedule, build, status="submitted",
                                        scheduled_at=due_at)
            store.update(sid, {"next_fire_at": next_fire_timestamp(cron, tz_name, due_at),
                               "last_fired_at": now_ts})
            return

        # ---- 错过（停机 / 严重卡顿）：统计 [due_at, now_ts] 内所有触发点 ----
        missed = [due_at]
        cursor = due_at
        truncated = False
        while len(missed) < 5000:  # 上限保护：超长停机不让扫描循环失控
            nxt = next_fire_timestamp(cron, tz_name, cursor)
            if nxt is None or nxt > now_ts:
                break
            missed.append(nxt)
            cursor = nxt
        else:
            truncated = True

        policy = schedule.get("misfire_policy") or "catch_up"
        patch = {}
        if policy == "catch_up":
            # 补跑一次：合并所有错过的触发点，立即补跑最近的一次
            build = self.submit_build(
                schedule.get("project_id"), schedule.get("suite_id"),
                env_id=schedule.get("env_id"), trigger="schedule_catchup")
            self._record_run_from_build(
                schedule, build, status="caught_up", scheduled_at=missed[-1],
                missed_count=len(missed) - 1, missed_truncated=truncated)
            patch["last_fired_at"] = now_ts
        else:
            # 标记为已错过：不补跑，留一条明确的错过记录
            self._record_schedule_run(
                sid, schedule.get("project_id"), None, status="missed",
                scheduled_at=missed[0], window_end=missed[-1],
                missed_count=len(missed), missed_truncated=truncated)
            patch["last_missed_at"] = now_ts

        # 错过窗口内的触发点已按策略清算完毕，从当前时刻重新排程
        patch["next_fire_at"] = next_fire_timestamp(cron, tz_name, now_ts)
        store.update(sid, patch)

    def _record_run_from_build(self, schedule: dict, build: dict,
                               status: str, **extra) -> None:
        """按构建提交结果记录计划运行历史（提交失败也留痕）。"""
        if "id" in build:
            self._record_schedule_run(schedule["id"], schedule.get("project_id"),
                                      build["id"], status=status, **extra)
        else:
            self._record_schedule_run(schedule["id"], schedule.get("project_id"),
                                      None, status="submit_failed",
                                      error=build.get("error", "提交构建失败"),
                                      **extra)

    def _record_schedule_run(self, schedule_id: str, project_id: Optional[str],
                             build_id: Optional[str], status: str = "submitted",
                             **extra) -> None:
        run = {
            "id": new_id("schrun"),
            "schedule_id": schedule_id,
            "project_id": project_id,
            "build_id": build_id,
            "fired_at": time.time(),
            "status": status,
        }
        run.update(extra)
        self.registry.store("schedule_runs").insert(run)

    def compute_next_fire(self, cron: str, tz_name: str,
                          after_ts: Optional[float] = None) -> Optional[float]:
        """计算某 cron 计划在指定时区下、``after_ts``（默认现在）之后的
        下次触发 UTC 时间戳；地平线内找不到返回 ``None``。"""
        return next_fire_timestamp(cron, tz_name, after_ts or time.time())

    def describe_cron(self, expr: str) -> str:
        """把 cron 表达式转成人话（供前端展示）。"""
        try:
            sched = parse_cron(expr)
        except ValueError:
            return "无效表达式"
        parts = []
        if sched.minute == list(range(0, 60)):
            parts.append("每分钟")
        else:
            parts.append(f"第 {','.join(map(str, sched.minute[:6]))} 分" + ("…" if len(sched.minute) > 6 else ""))
        if sched.hour != list(range(0, 24)):
            parts.append(f"{','.join(map(str, sched.hour[:6]))} 时" + ("…" if len(sched.hour) > 6 else ""))
        if sched.day != list(range(1, 32)):
            parts.append(f"每月 {','.join(map(str, sched.day[:8]))} 日" + ("…" if len(sched.day) > 8 else ""))
        if sched.weekday != list(range(0, 7)):
            parts.append(f"周 {','.join(map(str, sched.weekday))}")
        return " · ".join(parts) if parts else "每分钟"
