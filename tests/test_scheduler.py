"""调度器集成测试。

覆盖：并发调度（构建池 + 用例池）、结果收集与聚合、报告/覆盖率/通知收尾、
取消、定时触发（到点触发、不重复、停机错过的补跑 / 标记策略）。
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import (CoverageAnalyzer, DefectManager, EnvironmentManager,
                    NotificationManager, ReportGenerator, Scheduler, TestExecutor)
from storage import BuildStoreRegistry, StoreRegistry


def _make_scheduler(data_root):
    registry = StoreRegistry(os.path.join(data_root, "store"), shard_size=50)
    builds = BuildStoreRegistry(os.path.join(data_root, "builds"))
    executor = TestExecutor()
    env_mgr = EnvironmentManager(registry, data_root)
    coverage = CoverageAnalyzer(builds)
    report = ReportGenerator(builds)
    defects = DefectManager(registry)
    notify = NotificationManager(registry)
    sched = Scheduler(registry, builds, executor, env_mgr, report, coverage,
                      defects, notify, max_build_workers=2, max_case_workers=4,
                      tick_seconds=0.2)
    return registry, builds, env_mgr, sched


class TestSchedulerEndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry, self.builds, self.env_mgr, self.sched = _make_scheduler(self.tmp.name)

    def tearDown(self):
        self.sched.shutdown()
        self.tmp.cleanup()

    def _setup_project(self, n_cases=12):
        pid = self.registry.store("projects").insert({"name": "P"})
        env = self.env_mgr.create(pid, {"name": "dev", "config": {"latency_ms": 0, "fail_rate": 0.0}})
        cases_store = self.registry.store("cases")
        ids = []
        for i in range(n_cases):
            ids.append(cases_store.insert({
                "id": f"case_{i}", "project_id": pid, "name": f"用例{i}",
                "priority": "P2", "tags": ["g1" if i % 2 else "g2"], "timeout": 30,
                "steps": [
                    {"action": "request", "method": "GET", "url": "/api/health"},
                    {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200},
                ],
            }))
        suite = {
            "id": "suite_1", "project_id": pid, "name": "冒烟",
            "env_id": env["id"], "case_ids": ids,
        }
        self.registry.store("suites").insert(suite)
        return pid, suite

    def test_full_run_collects_results(self):
        pid, suite = self._setup_project(12)
        result = self.sched.submit_build(pid, suite["id"], trigger="manual")
        self.assertIn("id", result)
        build_id = result["id"]

        # 等待构建完成
        deadline = time.time() + 20
        build = None
        while time.time() < deadline:
            build = self.builds.for_project(pid).get(build_id)
            if build and build["status"] in ("passed", "failed", "cancelled", "error"):
                break
            time.sleep(0.05)
        self.assertIsNotNone(build)
        self.assertEqual(build["status"], "passed")
        self.assertEqual(build["passed"], 12)
        self.assertEqual(len(self.builds.for_project(pid).results(build_id)), 12)

        # 报告 / 覆盖率已生成
        self.assertIsNotNone(self.builds.for_project(pid).read_report(build_id))
        self.assertIsNotNone(self.builds.for_project(pid).read_coverage(build_id))

    def test_concurrent_builds(self):
        pid, suite = self._setup_project(20)
        results = [self.sched.submit_build(pid, suite["id"]) for _ in range(3)]
        ids = [r["id"] for r in results]
        deadline = time.time() + 30
        while time.time() < deadline:
            builds = [self.builds.for_project(pid).get(b) for b in ids]
            if all(b and b["status"] in ("passed", "failed", "cancelled", "error") for b in builds):
                break
            time.sleep(0.05)
        for b in self.builds.for_project(pid).list_builds():
            if b["id"] in ids:
                self.assertEqual(b["status"], "passed")
                self.assertEqual(b["passed"], 20)
                self.assertEqual(len(self.builds.for_project(pid).results(b["id"])), 20)

    def test_cancel_build(self):
        pid, suite = self._setup_project(30)
        result = self.sched.submit_build(pid, suite["id"])
        build_id = result["id"]
        self.sched.cancel_build(build_id)
        deadline = time.time() + 15
        while time.time() < deadline:
            b = self.builds.for_project(pid).get(build_id)
            if b and b["status"] in ("cancelled", "passed", "failed"):
                break
            time.sleep(0.05)
        b = self.builds.for_project(pid).get(build_id)
        self.assertIn(b["status"], ("cancelled", "passed", "failed"))

    # ---------------------------------------------------------- 定时触发
    def _insert_schedule(self, pid, suite, **overrides):
        sch = {
            "id": "sch_1", "project_id": pid, "name": "定时", "cron": "* * * * *",
            "timezone": "UTC", "misfire_policy": "catch_up",
            "suite_id": suite["id"], "env_id": suite["env_id"], "enabled": True,
            "next_fire_at": None, "last_fired_at": None,
        }
        sch.update(overrides)
        self.registry.store("schedules").insert(sch)
        return sch

    def _runs_of(self, schedule_id):
        return self.registry.store("schedule_runs").query(
            where=[("schedule_id", "eq", schedule_id)])

    def test_schedule_fires_once_when_due(self):
        """到点触发一次；紧接着再扫不会重复触发。"""
        pid, suite = self._setup_project(3)
        now = time.time()
        sch = self._insert_schedule(pid, suite, next_fire_at=now - 1)
        self.sched._scan_schedules(now_ts=now)
        self.sched._scan_schedules(now_ts=now + 1)
        runs = self._runs_of(sch["id"])
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "submitted")
        self.assertEqual(runs[0]["scheduled_at"], now - 1)
        # 触发点已推进到未来
        updated = self.registry.store("schedules").get(sch["id"])
        self.assertGreater(updated["next_fire_at"], now)
        self.assertIsNotNone(updated["last_fired_at"])

    def test_new_schedule_initializes_next_fire_without_firing(self):
        """新计划（还没有 next_fire_at）首次扫描只落触发点，不补跑历史。"""
        pid, suite = self._setup_project(3)
        sch = self._insert_schedule(pid, suite)
        now = time.time()
        self.sched._scan_schedules(now_ts=now)
        self.assertEqual(len(self._runs_of(sch["id"])), 0)
        updated = self.registry.store("schedules").get(sch["id"])
        self.assertIsNotNone(updated["next_fire_at"])
        self.assertGreater(updated["next_fire_at"], now)

    def test_not_due_schedule_is_untouched(self):
        """未到点的计划不触发、不改触发点。"""
        pid, suite = self._setup_project(3)
        now = time.time()
        future = now + 3600
        sch = self._insert_schedule(pid, suite, next_fire_at=future)
        self.sched._scan_schedules(now_ts=now)
        self.assertEqual(len(self._runs_of(sch["id"])), 0)
        updated = self.registry.store("schedules").get(sch["id"])
        self.assertEqual(updated["next_fire_at"], future)

    def test_misfire_catch_up_fires_once_and_records(self):
        """停机错过：catch_up 策略恢复后立即补跑一次，并记录合并的错过数。"""
        pid, suite = self._setup_project(3)
        now = time.time()
        # 「停机」一小时：每 10 分钟一次的计划错过约 6 次
        sch = self._insert_schedule(pid, suite, cron="*/10 * * * *",
                                    next_fire_at=now - 3600)
        self.sched._scan_schedules(now_ts=now)
        runs = self._runs_of(sch["id"])
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "caught_up")
        self.assertGreaterEqual(runs[0]["missed_count"], 5)
        self.assertIsNotNone(runs[0]["build_id"])
        # 只补跑一次：构建数恰为 1，触发来源标记为 schedule_catchup
        builds = self.builds.for_project(pid).list_builds()
        self.assertEqual(len(builds), 1)
        self.assertEqual(builds[0]["trigger"], "schedule_catchup")
        # 触发点推进到当前之后，再扫不会重复补跑
        updated = self.registry.store("schedules").get(sch["id"])
        self.assertGreater(updated["next_fire_at"], now)
        self.sched._scan_schedules(now_ts=now + 1)
        self.assertEqual(len(self._runs_of(sch["id"])), 1)

    def test_misfire_mark_missed_records_without_firing(self):
        """停机错过：mark_missed 策略不补跑，明确记录「已错过」。"""
        pid, suite = self._setup_project(3)
        now = time.time()
        sch = self._insert_schedule(pid, suite, cron="*/10 * * * *",
                                    misfire_policy="mark_missed",
                                    next_fire_at=now - 3600)
        self.sched._scan_schedules(now_ts=now)
        runs = self._runs_of(sch["id"])
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "missed")
        self.assertGreaterEqual(runs[0]["missed_count"], 5)
        self.assertIsNone(runs[0]["build_id"])
        # 没有提交任何构建
        self.assertEqual(len(self.builds.for_project(pid).list_builds()), 0)
        # 触发点推进，再扫不会重复标记
        updated = self.registry.store("schedules").get(sch["id"])
        self.assertGreater(updated["next_fire_at"], now)
        self.sched._scan_schedules(now_ts=now + 1)
        self.assertEqual(len(self._runs_of(sch["id"])), 1)

    def test_disabled_schedule_never_fires(self):
        pid, suite = self._setup_project(3)
        now = time.time()
        sch = self._insert_schedule(pid, suite, enabled=False, next_fire_at=now - 10)
        self.sched._scan_schedules(now_ts=now)
        self.assertEqual(len(self._runs_of(sch["id"])), 0)

    def test_schedule_timezone_changes_fire_instant(self):
        """同一 cron 在不同时区下算出的触发时刻不同（上海 9 点 = UTC 1 点）。"""
        pid, suite = self._setup_project(3)
        now = time.time()
        sch = self._insert_schedule(pid, suite, cron="0 9 * * *",
                                    timezone="Asia/Shanghai")
        self.sched._scan_schedules(now_ts=now)
        updated = self.registry.store("schedules").get(sch["id"])
        nxt = updated["next_fire_at"]
        self.assertIsNotNone(nxt)
        # 触发时刻换算到 UTC 必须是 01:00（上海 09:00）
        import datetime as _dt
        utc_dt = _dt.datetime.fromtimestamp(nxt, _dt.timezone.utc)
        self.assertEqual((utc_dt.hour, utc_dt.minute), (1, 0))


if __name__ == "__main__":
    unittest.main()
