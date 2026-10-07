"""时区感知调度与夏令时（DST）行为测试。

覆盖：
- cron 按计划时区的墙上时间解释，同一表达式在不同时区触发时刻不同；
- 下次触发时刻计算（``next_fire_timestamp``）；
- 夏令时明确行为：不存在的墙钟时间顺延、重叠的墙钟时间只触发一次；
- 时区名校验与服务器本地时区探测。
"""

from __future__ import annotations

import datetime
import os
import sys
import unittest
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.cron import (COMMON_TIMEZONES, get_timezone, local_timezone_name,
                         next_fire_time, next_fire_timestamp, parse_cron,
                         resolve_wall_time)

UTC = datetime.timezone.utc
NY = ZoneInfo("America/New_York")
SH = ZoneInfo("Asia/Shanghai")


def utc_ts(y, mo, d, h, mi=0):
    """UTC 时刻的时间戳。"""
    return datetime.datetime(y, mo, d, h, mi, tzinfo=UTC).timestamp()


class TestTimezoneValidation(unittest.TestCase):
    def test_valid_timezone(self):
        self.assertEqual(get_timezone("Asia/Shanghai").key, "Asia/Shanghai")

    def test_invalid_timezone_raises_chinese_message(self):
        with self.assertRaises(ValueError) as ctx:
            get_timezone("Mars/Olympus")
        self.assertIn("未知时区", str(ctx.exception))

    def test_empty_timezone_raises(self):
        with self.assertRaises(ValueError):
            get_timezone("")

    def test_local_timezone_name_is_loadable(self):
        name = local_timezone_name()
        self.assertEqual(get_timezone(name).key, name)

    def test_common_timezones_all_loadable(self):
        for name in COMMON_TIMEZONES:
            get_timezone(name)


class TestResolveWallTime(unittest.TestCase):
    def test_normal_time_resolves_to_one_instant(self):
        naive = datetime.datetime(2026, 1, 15, 10, 0)
        resolved = resolve_wall_time(naive, NY)
        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0].utcoffset(), datetime.timedelta(hours=-5))

    def test_spring_forward_gap_resolves_to_empty(self):
        # 2026-03-08 美东 02:00→03:00，02:30 不存在
        naive = datetime.datetime(2026, 3, 8, 2, 30)
        self.assertEqual(resolve_wall_time(naive, NY), [])

    def test_fall_back_overlap_resolves_to_two_instants(self):
        # 2026-11-01 美东 02:00→01:00，01:30 出现两次
        naive = datetime.datetime(2026, 11, 1, 1, 30)
        resolved = resolve_wall_time(naive, NY)
        self.assertEqual(len(resolved), 2)
        self.assertEqual(resolved[1].timestamp() - resolved[0].timestamp(), 3600)
        # 第一次是 EDT（UTC-4），第二次是 EST（UTC-5）
        self.assertEqual(resolved[0].utcoffset(), datetime.timedelta(hours=-4))
        self.assertEqual(resolved[1].utcoffset(), datetime.timedelta(hours=-5))


class TestNextFireTime(unittest.TestCase):
    def test_basic_daily_in_schedule_timezone(self):
        # 上海 09:00 = UTC 01:00
        nxt = next_fire_timestamp("0 9 * * *", "Asia/Shanghai",
                                  utc_ts(2026, 10, 7, 0, 0))
        self.assertEqual(nxt, utc_ts(2026, 10, 7, 1, 0))

    def test_same_cron_different_timezones(self):
        after = utc_ts(2026, 10, 7, 0, 0)
        sh = next_fire_timestamp("0 9 * * *", "Asia/Shanghai", after)
        ny = next_fire_timestamp("0 9 * * *", "America/New_York", after)
        self.assertEqual(sh, utc_ts(2026, 10, 7, 1, 0))    # 上海 09:00
        self.assertEqual(ny, utc_ts(2026, 10, 7, 13, 0))   # 纽约 09:00（EDT）

    def test_minute_granularity_strictly_after(self):
        # after 落在 10:20:30，下一分钟 10:21:00 才触发
        after = utc_ts(2026, 6, 15, 10, 20) + 30
        nxt = next_fire_timestamp("* * * * *", "UTC", after)
        self.assertEqual(nxt, utc_ts(2026, 6, 15, 10, 21))

    def test_exact_occurrence_requires_next_one(self):
        # after 恰好是 09:00:00，应取「下一个」09:00（严格晚于）
        after = utc_ts(2026, 6, 15, 9, 0)
        nxt = next_fire_timestamp("0 9 * * *", "UTC", after)
        self.assertEqual(nxt, utc_ts(2026, 6, 16, 9, 0))

    def test_weekday_and_month_fields(self):
        # 2026-10-05 是周一；每周一 09:00
        after = utc_ts(2026, 10, 7, 0, 0)  # 周三
        nxt = next_fire_timestamp("0 9 * * 1", "UTC", after)
        self.assertEqual(nxt, utc_ts(2026, 10, 12, 9, 0))  # 下周一

    def test_feb29_schedule_found_within_horizon(self):
        after = utc_ts(2027, 1, 1, 0, 0)
        nxt = next_fire_timestamp("0 0 29 2 *", "UTC", after)
        self.assertEqual(nxt, utc_ts(2028, 2, 29, 0, 0))

    def test_step_expression(self):
        after = utc_ts(2026, 6, 15, 10, 7)
        nxt = next_fire_timestamp("*/15 * * * *", "UTC", after)
        self.assertEqual(nxt, utc_ts(2026, 6, 15, 10, 15))


class TestDstBehavior(unittest.TestCase):
    """夏令时切换时刻的明确行为。"""

    def test_spring_forward_nonexistent_time_shifts_forward(self):
        """02:30 不存在 → 顺延到 03:00（拨快后第一个真实时刻）。"""
        after = utc_ts(2026, 3, 7, 12, 0)
        nxt = next_fire_timestamp("30 2 * * *", "America/New_York", after)
        # 2026-03-08 03:00 EDT = 07:00 UTC
        self.assertEqual(nxt, utc_ts(2026, 3, 8, 7, 0))
        # 再下一次恢复正常：03-09 02:30 EDT = 06:30 UTC
        nxt2 = next_fire_timestamp("30 2 * * *", "America/New_York", nxt)
        self.assertEqual(nxt2, utc_ts(2026, 3, 9, 6, 30))

    def test_fall_back_overlap_fires_once_at_first_occurrence(self):
        """01:30 出现两次 → 只在第一次（EDT）触发，不重复。"""
        after = utc_ts(2026, 10, 31, 12, 0)
        nxt = next_fire_timestamp("30 1 * * *", "America/New_York", after)
        # 2026-11-01 01:30 EDT（第一次）= 05:30 UTC
        self.assertEqual(nxt, utc_ts(2026, 11, 1, 5, 30))
        # 下一次必须是第二天 01:30 EST = 06:30 UTC，而不是当天的第二次 01:30
        nxt2 = next_fire_timestamp("30 1 * * *", "America/New_York", nxt)
        self.assertEqual(nxt2, utc_ts(2026, 11, 2, 6, 30))

    def test_fire_instants_are_monotonic_across_dst(self):
        """连续推进一年的触发时刻：严格递增，间隔不超过 2 小时。

        （秋季拨回日 01:00 只在第一次出现触发，UTC 上会出现一个 2 小时间隔，
        这是「墙钟每小时一次」语义的正确表现。）
        """
        sched = parse_cron("0 * * * *")
        cursor = utc_ts(2026, 1, 1, 0, 0)
        end = utc_ts(2027, 1, 1, 0, 0)
        prev = None
        count = 0
        while True:
            nxt = next_fire_time(sched, NY, cursor)
            self.assertIsNotNone(nxt)
            ts = nxt.timestamp()
            if ts > end:
                break
            if prev is not None:
                self.assertGreater(ts, prev)
                self.assertLessEqual(ts - prev, 7200)
            prev = ts
            cursor = ts
            count += 1
        # 363 个正常日 ×24 + 春季拨快日 23 次 + 秋季拨回日 24 次 = 8759
        self.assertEqual(count, 8759)


if __name__ == "__main__":
    unittest.main()
