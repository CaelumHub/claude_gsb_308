"""并发调度：构建池 + 用例池 + 定时触发循环。

这是平台「测试调度与并发」难点的核心。一次构建要并发执行大量用例，多场
构建又要并行推进，同时定时任务到点还要自动触发新构建。三层并发：

1. **构建级并发**：一个线程池（``build_pool``）承载多场同时进行的构建，
   用 ``max_build_workers`` 限制并发构建数，避免磁盘/CPU 被打满；
2. **用例级并发**：每场构建内部再用一个线程池（``case_pool``）并发跑
   用例，用 ``max_case_workers`` 限制单构建内的并发度；结果通过
   :meth:`storage.buildstore.BuildStore.record_result` 在文件锁保护下
   并发安全地收集与聚合；
3. **时区定时触发**：一个后台循环线程按 ``tick`` 间隔扫描启用的计划。
   每条计划有自己的 IANA 时区，cron 按「该时区墙上时间」解释；触发点用
   UTC 整分标识，夏令时 gap/overlap 的处理规则见 :mod:`engine.tzsched`。

停机补跑（misfire）
-------------------
扫描不看「这一分钟是否命中」，而是比对计划记录的水位线 ``last_fire_epoch``
与当前应到的触发点，因此停机错过的触发点不会被悄悄跳过。恢复后按每条
计划配置的 ``misfire_policy`` 处理：

- ``run_once``：补跑一次（按最近错过的那一点提交构建），其余明确标记
  为 ``missed``；
- ``mark_missed``：不补跑，全部明确标记为 ``missed``。

无论哪种策略，错过都有可查的运行记录，且每个触发点只处理一次。

取消：每个构建持有一个 ``threading.Event``，用例执行器在步骤之间检查它，
取消后已在跑或用例尽快中止、未跑的不再启动，最终构建标为 ``cancelled``。
"""

from __future__ import annotations

import datetime
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from . import tzsched
from .cron import parse_cron
from .models import new_id

# 一次停机最多为单条计划保留多少条 missed 明细；更多则折叠成一条汇总
MISSED_RUN_CAP = 50

_MISFIRE_POLICIES = ("run_once", "mark_missed")


class Scheduler:
    """测试并发调度器。"""

    def __init__(self, registry, build_registry, executor, env_manager,
                 report_gen, coverage_analyzer, defect_manager, notify_manager,
                 max_build_workers: int = 4, max_case_workers: int = 8,
                 tick_seconds: float = 20.0, local_timezone: Optional[str] = None):
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

        # 平台本地时区：旧计划（未配置时区）与新建计划默认值的基准。
        self.local_timezone = local_timezone or tzsched.detect_local_timezone()

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

    def schedule_timezone(self, schedule: dict):
        """取计划时区，未配置的旧计划回落到平台本地时区。"""
        name = schedule.get("timezone") or self.local_timezone
        return tzsched.get_timezone(name)

    # ------------------------------------------------------------------ 触发
    def submit_build(self, project_id: str, suite_id: str,
                     env_id: Optional[str] = None, trigger: str = "manual",
                     schedule_id: Optional[str] = None) -> dict:
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

    def _scan_schedules(self, now_utc: Optional[datetime.datetime] = None) -> None:
        """扫描所有计划并处理到点/错过的触发。

        ``now_utc`` 仅用于测试注入；默认取当前 UTC 时刻。
        """
        # 串行化扫描，避免多个线程（后台 tick + 手动触发）同时越过水位线
        # 而重复触发同一计划。
        with self._scan_lock:
            if now_utc is None:
                now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_minute = int(now_utc.timestamp() // 60)
            schedules_store = self.registry.store("schedules")
            for schedule in schedules_store.all():
                try:
                    self._evaluate_schedule(schedule, now_minute, schedules_store)
                except Exception:  # noqa: BLE001
                    # 单条计划解析失败不影响其它计划
                    continue

    def _evaluate_schedule(self, schedule: dict, now_minute: int,
                           schedules_store) -> None:
        if not schedule.get("enabled", True):
            return
        sid = schedule["id"]
        cron = parse_cron(schedule.get("cron", "* * * * *"))
        tz = self.schedule_timezone(schedule)
        policy = schedule.get("misfire_policy", "run_once")
        if policy not in _MISFIRE_POLICIES:
            policy = "run_once"

        # 首次见到该计划（新建 / 停用后重新启用 / 修改 cron 或时区后重置）：
        # 以当前整分建立水位线，不回补历史。
        baseline = schedule.get("baseline_epoch_minute")
        if baseline is None:
            schedules_store.update(sid, {
                "baseline_epoch_minute": now_minute,
                "last_fire_epoch_minute": now_minute,
            })
            return

        watermark = schedule.get("last_fire_epoch_minute")
        if watermark is None:
            # 兼容只写了 baseline 的中间态
            watermark = baseline
        if now_minute <= watermark:
            return

        # 只逐点处理「最近」MAX_MISSED_PER_SCAN 个错过点；更早的折叠成一条
        # 汇总。海量错过（如停机一年的每分钟计划）也不会长时间持锁、写爆库。
        # 一次性枚举到当前整分：区间内严格落在过去的是错过点，落在当前整分
        # 的是到点正常触发，二者用同一时区/夏令时模型，语义一致。
        recent, total = tzsched.recent_fires_between(
            cron, tz, watermark, now_minute,
            max_recent=tzsched.MAX_MISSED_PER_SCAN + 1)
        if recent and recent[-1][0] == now_minute:
            current = recent[-1]
            missed = recent[:-1]
            total_missed = max(0, total - 1)
        else:
            current = None
            missed = recent
            total_missed = total

        folded_older = total_missed - len(missed)
        if not missed and current is None:
            return

        if missed:
            self._handle_missed(schedule, missed, policy, tz,
                                folded_older=folded_older)
        if current is not None:
            self._fire_occurrence(schedule, current, tz,
                                  kind="on_time", trigger="schedule")

        # 水位线推进到当前整分（正常到点）或最后一个已处理的错过点，
        # 保证每个触发点只处理一次；折叠掉的更早点也一并越过。
        advance_to = now_minute if current is not None else (
            missed[-1][0] if missed else watermark)
        schedules_store.update(sid, {"last_fire_epoch_minute": advance_to})

    def _handle_missed(self, schedule: dict, missed: list, policy: str,
                       tz, folded_older: int = 0) -> None:
        """按策略处理一批错过的触发点（停机恢复后调用）。"""
        total = len(missed) + folded_older
        if folded_older:
            # 更早、未逐点处理的错过：折叠成一条汇总，明确数量与时间范围
            fold_note = (f"停机窗口过长，最早的 {folded_older} 次错过折叠为一条；"
                         f"共错过约 {total} 次")
            self._insert_run(
                schedule, status="missed",
                scheduled_epoch_minute=missed[0][0],
                tz=tz, dst_adjusted=False, build_id=None, note=fold_note)
        if policy == "run_once":
            # 补跑一次：按最近错过的那一点补一场构建，其余标记 missed
            catchup = missed[-1]
            self._fire_occurrence(schedule, catchup, tz,
                                  kind="catchup", trigger="schedule")
            self._mark_missed_batch(schedule, missed[:-1], tz,
                                    policy_note="已按策略补跑最近一次")
        else:
            # mark_missed：不补跑，全部明确标记
            self._mark_missed_batch(schedule, missed, tz,
                                    policy_note="按策略跳过，不补跑")

    def _mark_missed_batch(self, schedule: dict, missed: list, tz,
                           policy_note: str) -> None:
        """把错过的触发点落为 missed 记录；超过上限的折叠成一条汇总。"""
        total = len(missed)
        if total > MISSED_RUN_CAP:
            folded = total - MISSED_RUN_CAP
            first_epoch = missed[0][0]
            self._insert_run(
                schedule, status="missed",
                scheduled_epoch_minute=first_epoch, tz=tz,
                dst_adjusted=missed[0][1], build_id=None,
                note=f"另有 {folded} 次更早的错过记录已折叠；{policy_note}")
            missed = missed[-MISSED_RUN_CAP:]
        for epoch_minute, adjusted in missed:
            self._insert_run(
                schedule, status="missed",
                scheduled_epoch_minute=epoch_minute, tz=tz,
                dst_adjusted=adjusted, build_id=None, note=policy_note)

    def _fire_occurrence(self, schedule: dict, occurrence: tuple, tz,
                         kind: str, trigger: str) -> None:
        """对单个触发点提交构建并落一条计划运行记录。

        kind: ``on_time`` 正常到点 / ``catchup`` 停机恢复补跑。
        """
        epoch_minute, dst_adjusted = occurrence
        note = None
        if kind == "catchup":
            wall = tzsched.wall_text(epoch_minute, tz)
            note = f"停机恢复后补跑（计划时刻 {wall['text']} {wall['offset']}）"
        result = self.submit_build(
            schedule.get("project_id"),
            schedule.get("suite_id"),
            env_id=schedule.get("env_id"),
            trigger=trigger,
        )
        if "id" in result:
            self._insert_run(
                schedule, status=kind,
                scheduled_epoch_minute=epoch_minute, tz=tz,
                dst_adjusted=dst_adjusted, build_id=result["id"], note=note)
        else:
            # 提交失败（套件/环境缺失等）也要留下记录，不能悄悄跳过
            self._insert_run(
                schedule, status="error",
                scheduled_epoch_minute=epoch_minute, tz=tz,
                dst_adjusted=dst_adjusted, build_id=None,
                note=f"触发但提交构建失败: {result.get('error', '未知错误')}")

    def _insert_run(self, schedule: dict, status: str,
                    scheduled_epoch_minute: int, tz, dst_adjusted: bool,
                    build_id: Optional[str], note: Optional[str]) -> None:
        wall = tzsched.wall_text(scheduled_epoch_minute, tz)
        self.registry.store("schedule_runs").insert({
            "id": new_id("schrun"),
            "schedule_id": schedule["id"],
            "project_id": schedule.get("project_id"),
            "build_id": build_id,
            "status": status,
            "kind": status,
            "scheduled_epoch": scheduled_epoch_minute * 60,
            "scheduled_local": wall["text"],
            "scheduled_tz": str(tz),
            "dst_adjusted": dst_adjusted,
            "fired_at": time.time(),
            "note": note,
        })

    # ------------------------------------------------------------------ 展示
    def next_fire_info(self, schedule: dict) -> Optional[dict]:
        """计算某条计划的下次触发点（计划时区 + 浏览器本地两套展示字段）。

        严格晚于「当前 UTC 整分」；表达式在任何时区都无意义时返回 ``None``。
        """
        try:
            cron = parse_cron(schedule.get("cron", ""))
            tz = self.schedule_timezone(schedule)
        except ValueError:
            return None
        now_minute = int(datetime.datetime.now(datetime.timezone.utc).timestamp() // 60)
        nxt = tzsched.next_fire(cron, tz, now_minute)
        if nxt is None:
            return None
        epoch_minute, dst_adjusted = nxt
        wall = tzsched.wall_text(epoch_minute, tz)
        wall["dst_adjusted"] = dst_adjusted
        return wall

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
