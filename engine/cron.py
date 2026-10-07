"""5 段 cron 表达式解析、匹配与时区感知的触发时刻计算。

支持标准 cron 五段式 ``分 时 日 月 周``：

- ``*``          任意值
- ``*/n``        每隔 n
- ``a-b``        区间
- ``a,b,c``      枚举
- ``n``          单个值

时区语义
--------
每条定时计划绑定一个 IANA 时区（如 ``Asia/Shanghai``），cron 表达式按该时区的
「墙上时间」解释：上海同事配 ``0 18 * * *`` 就是上海时间每天 18:00 触发，
与服务器所在时区无关。:func:`next_fire_time` 负责把「下一次命中的墙上时间」
换算成真实的 UTC 时刻，调度器据此到点触发。

夏令时（DST）行为约定——明确且一致：

- **不存在的墙钟时间**（春季拨快跳过的时段，如美东 02:30）：顺延到该墙钟
  时间之后第一个真实存在的时刻触发（02:30 → 03:00），不丢、不提前；
- **重复的墙钟时间**（秋季拨回重叠的时段，如美东 01:30 出现两次）：只在
  第一次出现时触发一次，不会重复触发。
"""

from __future__ import annotations

import datetime
import os
from dataclasses import dataclass, field
from typing import List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


_WEEKDAYS = {"sun": 0, "mon": 1, "tue": 2, "wed": 3,
             "thu": 4, "fri": 5, "sat": 6}
_MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
           "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


@dataclass
class CronSchedule:
    """解析后的 cron 计划。"""

    minute: List[int] = field(default_factory=list)
    hour: List[int] = field(default_factory=list)
    day: List[int] = field(default_factory=list)
    month: List[int] = field(default_factory=list)
    weekday: List[int] = field(default_factory=list)
    raw: str = ""

    def matches(self, dt: datetime.datetime) -> bool:
        """判断给定时刻是否命中该计划。"""
        # cron 的星期采用 0=周日、1=周一…6=周六；Python 的 weekday() 是
        # 0=周一…6=周日，需 +1 取模对齐到 cron 约定。
        cron_weekday = (dt.weekday() + 1) % 7
        return (
            dt.minute in self.minute
            and dt.hour in self.hour
            and dt.day in self.day
            and dt.month in self.month
            and cron_weekday in self.weekday
        )


def _parse_field(field: str, lo: int, hi: int,
                 names: dict = None) -> List[int]:
    """解析一个 cron 字段为取值集合。"""
    values = set()
    field = field.strip().lower()
    if names and field in names:
        values.add(names[field])
        return sorted(values)

    for part in field.split(","):
        part = part.strip()
        if not part:
            continue
        if part == "*":
            values.update(range(lo, hi + 1))
        elif part.startswith("*/"):
            step = int(part[2:])
            values.update(range(lo, hi + 1, step))
        elif "-" in part:
            a, b = part.split("-", 1)
            if names and a in names:
                a = names[a]
            if names and b in names:
                b = names[b]
            values.update(range(int(a), int(b) + 1))
        else:
            if names and part in names:
                values.add(names[part])
            else:
                values.add(int(part))
    return sorted(values)


def parse_cron(expr: str) -> CronSchedule:
    """解析五段 cron 表达式。"""
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"cron 表达式需要 5 个字段，收到 {len(parts)}: {expr!r}")
    minute, hour, day, month, weekday = parts
    return CronSchedule(
        minute=_parse_field(minute, 0, 59),
        hour=_parse_field(hour, 0, 23),
        day=_parse_field(day, 1, 31),
        month=_parse_field(month, 1, 12, _MONTHS),
        weekday=_parse_field(weekday, 0, 6, _WEEKDAYS),
        raw=expr,
    )


def cron_matches(expr: str, dt: datetime.datetime = None) -> bool:
    """判断 cron 表达式在给定时刻（默认当前时刻）是否命中。"""
    dt = dt or datetime.datetime.now()
    return parse_cron(expr).matches(dt)


# ---------------------------------------------------------------------------
# 时区
# ---------------------------------------------------------------------------

# 前端下拉框提供的常用 IANA 时区（服务器本地时区会由接口动态并入）
COMMON_TIMEZONES = [
    "Asia/Shanghai", "Asia/Tokyo", "Asia/Singapore", "Asia/Dubai",
    "Europe/London", "Europe/Berlin", "Europe/Moscow",
    "America/New_York", "America/Chicago", "America/Denver",
    "America/Los_Angeles", "America/Sao_Paulo",
    "Australia/Sydney", "Pacific/Auckland", "UTC",
]


def get_timezone(name: str) -> ZoneInfo:
    """按 IANA 名称取时区，非法名称抛出带中文说明的 ``ValueError``。"""
    name = (name or "").strip()
    if not name:
        raise ValueError("时区不能为空（请使用 IANA 时区名，如 Asia/Shanghai）")
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, KeyError, ValueError):
        raise ValueError(f"未知时区: {name!r}（请使用 IANA 时区名，如 Asia/Shanghai）")


def local_timezone_name() -> str:
    """探测服务器本地时区的 IANA 名称，探测失败回退 ``UTC``。"""
    candidates: List[str] = []
    tz_env = os.environ.get("TZ", "").strip().lstrip(":")
    if tz_env and not tz_env.startswith("/"):
        candidates.append(tz_env)
    try:  # /etc/localtime 通常是指向 zoneinfo 的符号链接
        link = os.readlink("/etc/localtime")
        if "zoneinfo/" in link:
            candidates.append(link.split("zoneinfo/", 1)[1])
    except OSError:
        pass
    try:  # Debian 系把时区名写在 /etc/timezone
        with open("/etc/timezone", "r", encoding="utf-8") as fh:
            candidates.append(fh.read().strip())
    except OSError:
        pass
    candidates.append("UTC")
    for name in candidates:
        try:
            ZoneInfo(name)
            return name
        except Exception:  # noqa: BLE001
            continue
    return "UTC"


# ---------------------------------------------------------------------------
# 下次触发时刻计算（时区感知 + 夏令时处理）
# ---------------------------------------------------------------------------

# 搜索地平线：8 年，足够覆盖「2 月 29 日」这类四年一遇的计划（含世纪闰年间隔）
_SEARCH_DAYS = 366 * 8 + 2


def resolve_wall_time(naive: datetime.datetime, tz: ZoneInfo) -> List[datetime.datetime]:
    """把一个墙钟时间解析成真实时刻，返回按先后排序的 aware datetime 列表。

    - 普通时刻 → 恰有 1 个；
    - 夏令时拨回造成的重复时刻 → 2 个（第一次在前）；
    - 夏令时拨快造成的不存在时刻 → 空列表。
    """
    out: List[datetime.datetime] = []
    for fold in (0, 1):
        aware = naive.replace(tzinfo=tz, fold=fold)
        # 经 UTC 往返校验：不存在的墙钟时间往返后会「漂移」到别的墙钟
        back = aware.astimezone(datetime.timezone.utc).astimezone(tz)
        if back.replace(tzinfo=None) == naive and back.fold == fold:
            if not out or aware.timestamp() != out[-1].timestamp():
                out.append(aware)
    return out


def _shift_out_of_gap(naive: datetime.datetime,
                      tz: ZoneInfo) -> Optional[datetime.datetime]:
    """夏令时拨快时不存在的墙钟时间：顺延到之后第一个真实存在的时刻。"""
    probe = naive
    for _ in range(180):  # 现实中间隙不超过约 2 小时，逐分钟探测足够
        probe += datetime.timedelta(minutes=1)
        resolved = resolve_wall_time(probe, tz)
        if resolved:
            return resolved[0]
    return None


def next_fire_time(sched: CronSchedule, tz: ZoneInfo,
                   after_ts: float) -> Optional[datetime.datetime]:
    """计算计划 ``sched`` 在时区 ``tz`` 下、严格晚于 ``after_ts`` 的下一触发时刻。

    返回带时区的 datetime；地平线（8 年）内找不到时返回 ``None``。
    夏令时行为遵循模块 docstring 的约定：重叠取第一次、不存在则顺延。
    """
    after_local = datetime.datetime.fromtimestamp(after_ts, tz)
    # cron 粒度为分钟：从「当前分钟的下一分钟」开始找，保证严格晚于 after
    start = after_local.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
    start_date = start.date()
    start_naive = start.replace(tzinfo=None)

    day = start_date
    for _ in range(_SEARCH_DAYS):
        if (day.month in sched.month and day.day in sched.day
                and (day.weekday() + 1) % 7 in sched.weekday):
            for hour in sched.hour:
                for minute in sched.minute:
                    naive = datetime.datetime(day.year, day.month, day.day,
                                              hour, minute)
                    if day == start_date and naive < start_naive:
                        continue
                    resolved = resolve_wall_time(naive, tz)
                    if resolved:
                        # 重叠时刻只取第一次出现（fold=0），保证只触发一次
                        first = resolved[0]
                    else:
                        # 不存在的墙钟时间：顺延到间隙后第一个真实时刻
                        first = _shift_out_of_gap(naive, tz)
                        if first is None:
                            continue
                    if first.timestamp() > after_ts:
                        return first
                    # 该墙钟的首次时刻已过去（after 落在重叠区第二次里），
                    # 继续向后找；墙钟顺序与真实时刻顺序单调一致，首个
                    # 满足条件的候选即全局最早。
        day += datetime.timedelta(days=1)
    return None


def next_fire_timestamp(expr: str, tz_name: str,
                        after_ts: float) -> Optional[float]:
    """便捷封装：cron 表达式 + 时区名 → 下次触发的 UTC 时间戳。"""
    sched = parse_cron(expr)
    tz = get_timezone(tz_name)
    nxt = next_fire_time(sched, tz, after_ts)
    return nxt.timestamp() if nxt is not None else None
