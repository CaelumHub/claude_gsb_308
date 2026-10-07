"""调度器集成测试。

覆盖：并发调度（构建池 + 用例池）、结果收集与聚合、报告/覆盖率/通知收尾、
取消、以及定时任务的触发去重。
"""

from __future__ import annotations

import datetime
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
        # 先等后台构建线程排空（它们仍在往临时目录写结果），再停调度与清目录
        deadline = time.time() + 15
        while time.time() < deadline and self.sched.running():
            time.sleep(0.02)
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

    def _insert_schedule(self, pid, suite, cron="* * * * *",
                         timezone="UTC", misfire_policy="run_once",
                         baseline=None, enabled=True, sid="sch_1"):
        now_minute = int(time.time()) // 60
        sch = {
            "id": sid, "project_id": pid, "name": "定时", "cron": cron,
            "timezone": timezone, "misfire_policy": misfire_policy,
            "suite_id": suite["id"], "env_id": suite["env_id"],
            "enabled": enabled,
            "baseline_epoch_minute": baseline if baseline is not None else now_minute,
            "last_fire_epoch_minute": baseline if baseline is not None else now_minute,
        }
        self.registry.store("schedules").insert(sch)
        return sch

    def _runs(self, sid):
        return self.registry.store("schedule_runs").query(
            where=[("schedule_id", "eq", sid)])

    def test_schedule_fires_once_per_minute(self):
        """当前整分命中即触发，同一分钟重复扫描不重复触发。"""
        pid, suite = self._setup_project(3)
        now_minute = int(time.time()) // 60
        sch = self._insert_schedule(pid, suite, cron="* * * * *",
                                    baseline=now_minute - 1)
        now_utc = datetime.datetime.fromtimestamp(now_minute * 60,
                                                  tz=datetime.timezone.utc)
        self.sched._scan_schedules(now_utc=now_utc)
        self.sched._scan_schedules(now_utc=now_utc)
        runs = self._runs(sch["id"])
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "on_time")

    def test_new_schedule_does_not_backfill(self):
        """首次见到的计划以当前整分为基线，不回补历史。"""
        pid, suite = self._setup_project(3)
        sch = self._insert_schedule(pid, suite)  # baseline=now
        self.sched._scan_schedules()
        self.assertEqual(len(self._runs(sch["id"])), 0)
        stored = self.registry.store("schedules").get(sch["id"])
        self.assertIsNotNone(stored["baseline_epoch_minute"])

    def test_fires_in_schedule_timezone_not_server_timezone(self):
        """cron 按计划时区解释：上海 17:30 对应 UTC 09:30，UTC 17:30 不触发。"""
        pid, suite = self._setup_project(3)
        # 上海 17:30 = UTC 09:30
        sh_fire = datetime.datetime(2026, 10, 7, 9, 30, tzinfo=datetime.timezone.utc)
        baseline = int(sh_fire.timestamp()) // 60 - 1
        sch = self._insert_schedule(pid, suite, cron="30 17 * * *",
                                    timezone="Asia/Shanghai", baseline=baseline)
        self.sched._scan_schedules(now_utc=sh_fire)
        self.assertEqual(len(self._runs(sch["id"])), 1)

        # UTC 17:30（上海已是次日 01:30）对「上海 17:30」不应触发
        pid2, suite2 = self._setup_project(3)
        other = datetime.datetime(2026, 10, 7, 17, 30, tzinfo=datetime.timezone.utc)
        sch2 = self._insert_schedule(pid2, suite2, cron="30 17 * * *",
                                     timezone="Asia/Shanghai",
                                     baseline=int(other.timestamp()) // 60 - 1,
                                     sid="sch_2")
        self.sched._scan_schedules(now_utc=other)
        self.assertEqual(len(self._runs(sch2["id"])), 0)

    def test_misfire_run_once_catches_up_and_marks_rest(self):
        """停机错过多个点（run_once）：补跑最近一次，其余明确标记 missed。"""
        pid, suite = self._setup_project(3)
        # 每分钟计划，水位线停在 3 分钟前 -> 错过 2 个过去点 + 1 个当前点
        now_utc = datetime.datetime.now(datetime.timezone.utc).replace(
            second=0, microsecond=0)
        now_minute = int(now_utc.timestamp()) // 60
        sch = self._insert_schedule(pid, suite, cron="* * * * *",
                                    misfire_policy="run_once",
                                    baseline=now_minute - 3)
        self.sched._scan_schedules(now_utc=now_utc)
        runs = self._runs(sch["id"])
        statuses = sorted(r["status"] for r in runs)
        self.assertEqual(statuses, ["catchup", "missed", "on_time"])
        catchup = next(r for r in runs if r["status"] == "catchup")
        self.assertTrue(catchup["build_id"])
        self.assertIn("补跑", catchup["note"])
        missed = next(r for r in runs if r["status"] == "missed")
        self.assertIsNone(missed["build_id"])
        # 水位线推进，再次扫描不会重复处理
        self.sched._scan_schedules(now_utc=now_utc)
        self.assertEqual(len(self._runs(sch["id"])), 3)

    def test_misfire_mark_missed_does_not_build(self):
        """停机错过多个点（mark_missed）：不补跑，全部标记 missed。"""
        pid, suite = self._setup_project(3)
        now_utc = datetime.datetime.now(datetime.timezone.utc).replace(
            second=0, microsecond=0)
        now_minute = int(now_utc.timestamp()) // 60
        sch = self._insert_schedule(pid, suite, cron="* * * * *",
                                    misfire_policy="mark_missed",
                                    baseline=now_minute - 3)
        self.sched._scan_schedules(now_utc=now_utc)
        runs = self._runs(sch["id"])
        statuses = sorted(r["status"] for r in runs)
        self.assertEqual(statuses, ["missed", "missed", "on_time"])
        self.assertTrue(all(r["build_id"] is None
                            for r in runs if r["status"] == "missed"))

    def test_dst_gap_shifts_and_dedups(self):
        """春令时 gap：02:30 不存在，落到 03:30 EDT 并标记 dst_adjusted。"""
        from engine.cron import parse_cron
        from engine import tzsched
        ny = tzsched.get_timezone("America/New_York")
        # 2026-03-08 02:30 (不存在) -> 03:30 EDT = 07:30 UTC
        fire = datetime.datetime(2026, 3, 8, 7, 30, tzinfo=datetime.timezone.utc)
        baseline = int(fire.timestamp()) // 60 - 60
        pid, suite = self._setup_project(3)
        sch = self._insert_schedule(pid, suite, cron="30 2 * * *",
                                    timezone="America/New_York",
                                    baseline=baseline)
        self.sched._scan_schedules(now_utc=fire)
        runs = self._runs(sch["id"])
        self.assertEqual(len(runs), 1)
        self.assertTrue(runs[0]["dst_adjusted"])
        self.assertEqual(runs[0]["scheduled_local"], "2026-03-08 03:30")

        # */30 在 gap 内的 02:00/02:30 不重复触发（03:00 只一次）
        pid2, suite2 = self._setup_project(3)
        sch2 = self._insert_schedule(pid2, suite2, cron="*/30 * * * *",
                                     timezone="America/New_York",
                                     baseline=baseline, sid="sch_2")
        at_gap_end = datetime.datetime(2026, 3, 8, 7, 0,
                                       tzinfo=datetime.timezone.utc)
        self.sched._scan_schedules(now_utc=at_gap_end)
        runs2 = self._runs("sch_2")
        gap_runs = [r for r in runs2 if r["scheduled_local"] == "2026-03-08 03:00"]
        self.assertEqual(len(gap_runs), 1)

    def test_dst_overlap_fires_twice(self):
        """秋令时 overlap：01:30 出现两次（EDT 与 EST），各触发一次。"""
        # 2026-11-01 01:30 EDT = 05:30 UTC; 01:30 EST = 06:30 UTC
        first = datetime.datetime(2026, 11, 1, 5, 30, tzinfo=datetime.timezone.utc)
        second = datetime.datetime(2026, 11, 1, 6, 30, tzinfo=datetime.timezone.utc)
        baseline = int(first.timestamp()) // 60 - 60
        pid, suite = self._setup_project(3)
        sch = self._insert_schedule(pid, suite, cron="30 1 * * *",
                                    timezone="America/New_York",
                                    baseline=baseline)
        # 第一次到点
        self.sched._scan_schedules(now_utc=first)
        runs = self._runs(sch["id"])
        self.assertEqual(len(runs), 1)
        self.assertFalse(runs[0]["dst_adjusted"])
        # 走到两次出现之间（06:00 UTC）不应重放第一次
        self.sched._scan_schedules(
            now_utc=datetime.datetime(2026, 11, 1, 6, 0,
                                      tzinfo=datetime.timezone.utc))
        self.assertEqual(len(self._runs(sch["id"])), 1)
        # 第二次到点
        self.sched._scan_schedules(now_utc=second)
        runs = self._runs(sch["id"])
        self.assertEqual(len(runs), 2)
        self.assertEqual(len({r["scheduled_epoch"] for r in runs}), 2)

    def test_disabled_schedule_skipped(self):
        pid, suite = self._setup_project(3)
        now_minute = int(time.time()) // 60
        sch = self._insert_schedule(pid, suite, baseline=now_minute - 5,
                                    enabled=False)
        self.sched._scan_schedules()
        self.assertEqual(len(self._runs(sch["id"])), 0)


if __name__ == "__main__":
    unittest.main()
