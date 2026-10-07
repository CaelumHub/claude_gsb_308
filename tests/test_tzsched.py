"""时区定时计算（engine.tzsched）单元测试。

重点覆盖：跨时区触发点、夏令时 gap（时间不存在）调整与去重、overlap
（时间重复）两面各触发、next_fire 预览、非法时区。
"""

from __future__ import annotations

import datetime
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.cron import parse_cron
from engine import tzsched


def epoch(y, mo, d, h, minute, tz_name):
    tz = tzsched.get_timezone(tz_name)
    dt = datetime.datetime(y, mo, d, h, minute).replace(tzinfo=tz)
    return int(dt.timestamp() // 60)


class TestTimezoneFires(unittest.TestCase):
    def test_invalid_timezone(self):
        with self.assertRaises(ValueError):
            tzsched.get_timezone("Mars/Olympus")
        with self.assertRaises(ValueError):
            tzsched.get_timezone("")

    def test_shanghai_evening_cron(self):
        """上海 17:30 的下一点就是当天上海墙上 17:30（UTC 09:30）。"""
        sh = tzsched.get_timezone("Asia/Shanghai")
        cron = parse_cron("30 17 * * *")
        after = epoch(2026, 10, 7, 0, 0, "Asia/Shanghai")
        nxt = tzsched.next_fire(cron, sh, after)
        wall = tzsched.wall_text(nxt[0], sh)
        self.assertEqual(wall["text"], "2026-10-07 17:30")
        self.assertEqual(wall["offset"], "UTC+08:00")
        self.assertFalse(nxt[1])
        self.assertEqual(nxt[0], epoch(2026, 10, 7, 9, 30, "UTC"))

    def test_weekday_cron_uses_schedule_timezone(self):
        """周字段按计划时区的日历解释。"""
        ny = tzsched.get_timezone("America/New_York")
        cron = parse_cron("0 9 * * 1")  # 每周一 09:00 纽约
        # 2026-10-05 是周一
        nxt = tzsched.next_fire(cron, ny, epoch(2026, 10, 4, 12, 0, "America/New_York"))
        wall = tzsched.wall_text(nxt[0], ny)
        self.assertEqual(wall["text"], "2026-10-05 09:00")
        self.assertEqual(wall["weekday"], "周一")

    def test_spring_forward_gap_shifted(self):
        """2026-03-08 纽约 02:00 不存在 -> 03:00 EDT，标记调整。"""
        ny = tzsched.get_timezone("America/New_York")
        cron = parse_cron("0 2 * * *")
        lo = epoch(2026, 3, 8, 0, 0, "America/New_York")
        hi = epoch(2026, 3, 8, 23, 59, "America/New_York")
        hits = tzsched.fires_between(cron, ny, lo, hi)
        self.assertEqual(len(hits), 1)
        minute, adjusted = hits[0]
        self.assertTrue(adjusted)
        self.assertEqual(tzsched.wall_text(minute, ny)["text"], "2026-03-08 03:00")

    def test_spring_forward_gap_dedup(self):
        """*/30 在 gap 内的两个标称点：03:00 与 03:30 各一次，03:00 不重复。"""
        ny = tzsched.get_timezone("America/New_York")
        cron = parse_cron("*/30 * * * *")
        lo = epoch(2026, 3, 8, 0, 0, "America/New_York")
        hi = epoch(2026, 3, 8, 5, 0, "America/New_York")
        hits = tzsched.fires_between(cron, ny, lo, hi)
        at_three = [m for m, _ in hits
                    if tzsched.wall_text(m, ny)["text"] == "2026-03-08 03:00"]
        self.assertEqual(len(at_three), 1)
        # 不存在的 02:xx 一个都不直接出现
        self.assertFalse(any("02:" in tzsched.wall_text(m, ny)["text"]
                             for m, _ in hits))

    def test_fall_back_overlap_two_fires(self):
        """2026-11-01 纽约 01:30 出现两次（EDT/EST），各触发一次。"""
        ny = tzsched.get_timezone("America/New_York")
        cron = parse_cron("30 1 * * *")
        lo = epoch(2026, 11, 1, 0, 0, "America/New_York")
        hi = epoch(2026, 11, 1, 2, 59, "America/New_York")
        hits = tzsched.fires_between(cron, ny, lo, hi)
        self.assertEqual(len(hits), 2)
        offsets = [tzsched.wall_text(m, ny)["offset"] for m, _ in hits]
        self.assertEqual(offsets, ["UTC-04:00", "UTC-05:00"])
        # 两个不同物理分钟
        self.assertEqual(len({m for m, _ in hits}), 2)

    def test_overlap_first_fold_not_replayed_between(self):
        """两次出现之间扫描：第一面已处理就不重放，第二面到点才再触发。"""
        ny = tzsched.get_timezone("America/New_York")
        cron = parse_cron("30 1 * * *")
        # 第一面 01:30 EDT=05:30Z；中间 06:00Z；第二面 01:30 EST=06:30Z
        first = int(datetime.datetime(2026, 11, 1, 5, 30,
                                      tzinfo=datetime.timezone.utc).timestamp() // 60)
        between = int(datetime.datetime(2026, 11, 1, 6, 0,
                                        tzinfo=datetime.timezone.utc).timestamp() // 60)
        second = int(datetime.datetime(2026, 11, 1, 6, 30,
                                       tzinfo=datetime.timezone.utc).timestamp() // 60)
        self.assertEqual(tzsched.fires_between(cron, ny, first, between), [])
        hits = tzsched.fires_between(cron, ny, first, second)
        self.assertEqual(len(hits), 1)
        self.assertEqual(tzsched.wall_text(hits[0][0], ny)["offset"], "UTC-05:00")

    def test_never_matching_expr_returns_none(self):
        utc = tzsched.get_timezone("UTC")
        cron = parse_cron("0 0 30 2 *")  # 2 月 30 日不存在
        self.assertIsNone(tzsched.next_fire(cron, utc, epoch(2026, 1, 1, 0, 0, "UTC")))

    def test_detect_local_timezone(self):
        name = tzsched.detect_local_timezone()
        self.assertIn(name, tzsched.list_timezones() + ["UTC"])


if __name__ == "__main__":
    unittest.main()
