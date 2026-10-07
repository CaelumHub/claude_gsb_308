"""时区感知的定时计算：IANA 时区、触发点枚举、夏令时语义。

每条定时计划绑定一个 IANA 时区（如 ``Asia/Shanghai``、``America/New_York``），
cron 表达式始终按「该时区墙上时间」解释，与服务器所在时区无关。本模块只做纯
计算（无时钟、无 IO），扫描器在 :mod:`engine.scheduler` 中。

内部一律以 **UTC 整分（epoch minute）** 作为触发点的唯一标识：枚举时先在
「该时区墙上时间」逐候选找命中 cron 的标称分钟，再展开成物理时刻（UTC
整分），最后排序去重。因此春令时跳过、秋令时重复都有确定且一致的行为。

夏令时（DST）规则
-----------------
1. **春令时 gap（墙上时间不存在，如纽约 02:00→03:00）**：落进 gap 的标称触发
   点不丢弃，统一解析到「用跳变后偏移解释该标称时间」的物理时刻（即 02:00
   落到 03:00、02:30 落到 03:30），并标记 ``dst_adjusted=True``。同一跳变若
   有多个标称点解析到同一分钟，靠 UTC 整分去重为一次触发（如 ``*/30`` 在
   gap 内的 02:00/02:30 两个标称点只在 03:00 触发一次）。
2. **秋令时 overlap（墙上时间重复，如 01:00 出现两次）**：两个物理时刻各触发
   一次（``01:00 EDT`` 与 ``01:00 EST`` 是两个不同的 UTC 整分），符合「墙上
   每到一次点就触发一次」的直觉；同一标称时间的第一次出现取 ``fold=0``。
3. 非 DST 时区（如上海、UTC）行为完全平常。
"""

from __future__ import annotations

import datetime
import os
import zoneinfo
from typing import Optional

from .cron import CronSchedule

# 单次扫描最多产出多少个到点，防止极端表达式或长时间停机造成超长循环
MAX_FIRE_LIMIT = 100_000

# 单次扫描为「停机错过」逐点处理（补跑 / 落 missed 明细）的上限。更早的
# 错过折叠成一条汇总记录。5000 点约等于每分钟一跑停 3.5 天；对每小时 /
# 每天这类常见计划则覆盖 200 天 / 13 年，正常停机窗口内不会触发折叠。
MAX_MISSED_PER_SCAN = 5000

# 枚举候选墙上时间的最远年限（超过还没命中，视为表达式无意义）
_MAX_YEARS_AHEAD = 8

# 任何一个时区两次 fold 的物理时刻之差都远小于 24 小时；预览下一触发点时
# 向后多看这么久，保证 overlap 的第二次出现不会被漏排。
_OVERLAP_SAFETY = datetime.timedelta(hours=24)

_WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

# 常用时区，前端下拉置顶分组展示
COMMON_TIMEZONES = [
    "Asia/Shanghai", "Asia/Hong_Kong", "Asia/Taipei", "Asia/Tokyo",
    "Asia/Singapore", "Asia/Seoul", "Asia/Bangkok", "Asia/Dubai",
    "Asia/Kolkata", "Europe/London", "Europe/Paris", "Europe/Berlin",
    "Europe/Moscow", "America/New_York", "America/Chicago",
    "America/Denver", "America/Los_Angeles", "America/Sao_Paulo",
    "Australia/Sydney", "Pacific/Auckland", "UTC",
]

_UTC = datetime.timezone.utc


# ---------------------------------------------------------------------------
# 时区
# ---------------------------------------------------------------------------

def get_timezone(name: Optional[str]) -> zoneinfo.ZoneInfo:
    """按 IANA 名取时区；name 为空/非法时抛出 :class:`ValueError`。"""
    if not name:
        raise ValueError("时区不能为空")
    if name not in zoneinfo.available_timezones():
        raise ValueError(f"无效的 IANA 时区: {name!r}")
    return zoneinfo.ZoneInfo(name)


def list_timezones() -> list[str]:
    """全部可选 IANA 时区，过滤掉只用于向后兼容的 GMT 偏移项，按名字排序。"""
    names = []
    for name in zoneinfo.available_timezones():
        if name == "UTC":
            names.append(name)
        elif "/" in name and not (name.startswith("Etc/") and name[4:5] in "+-"):
            names.append(name)
    return sorted(set(names))


def detect_local_timezone() -> str:
    """探测服务器本地时区名，探测不到时退回 ``UTC``。

    用于旧计划（未配置时区字段）与新计划的默认值，保证升级后行为与升级前
    「按服务器时区触发」保持一致。
    """
    # 1) 显式环境变量 TZ
    tz_name = os.environ.get("TZ")
    if tz_name and tz_name in zoneinfo.available_timezones():
        return tz_name
    # 2) /etc/timezone（Debian 系，内容就是 IANA 名）
    try:
        with open("/etc/timezone", "r", encoding="utf-8") as fh:
            tz_name = fh.read().strip()
            if tz_name and tz_name in zoneinfo.available_timezones():
                return tz_name
    except OSError:
        pass
    # 3) /etc/localtime 符号链接（.../zoneinfo/<IANA 名>）
    try:
        link = os.readlink("/etc/localtime")
        marker = "/zoneinfo/"
        if marker in link:
            tz_name = link.split(marker, 1)[1]
            if tz_name in zoneinfo.available_timezones():
                return tz_name
    except OSError:
        pass
    # 4) 兜底
    return "UTC"


# ---------------------------------------------------------------------------
# 墙上时间 <-> UTC 整分
# ---------------------------------------------------------------------------

def _epoch_minute(utc_dt: datetime.datetime) -> int:
    return int(utc_dt.timestamp() // 60)


def _physical_minutes(wall: datetime.datetime, tz: datetime.tzinfo
                      ) -> list[tuple[int, bool]]:
    """把一个「标称墙上整分」展开成物理触发时刻。

    返回去重后的 ``(utc_epoch_minute, dst_adjusted)`` 列表（按时间升序）：

    - 正常分钟：一个时刻；
    - gap（该墙上时间不存在）：一个时刻——用跳变后偏移解释的落点，标记调整；
    - overlap（该墙上时间出现两次）：两个时刻，先 ``fold=0`` 后 ``fold=1``。
    """
    aware0 = wall.replace(tzinfo=tz, fold=0)
    aware1 = wall.replace(tzinfo=tz, fold=1)
    e0 = _epoch_minute(aware0.astimezone(_UTC))
    e1 = _epoch_minute(aware1.astimezone(_UTC))
    if e0 == e1:
        return [(e0, False)]
    if e0 > e1:
        # gap：fold=0 是「用跳变后偏移解释标称时间」的落点（如 02:00→03:00）
        return [(e0, True)]
    # overlap：两次物理出现都要触发
    return [(e0, False), (e1, False)]


# ---------------------------------------------------------------------------
# cron 墙上候选步进
# ---------------------------------------------------------------------------

def _next_wall_candidate(wall: datetime.datetime, cron: CronSchedule
                         ) -> Optional[datetime.datetime]:
    """从 ``wall``（不含）起，找下一个可能命中 cron 的墙上整分。

    采用「不命中就把不满足的最小字段推到下一个取值、小字段清零」的逐级
    跳跃，而不是逐分钟傻遍历。日/周是 AND 关系，候选不保证最终命中，由
    外层循环再次确认；超过年限仍无候选则返回 ``None``。
    """
    cand = wall.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
    limit_year = wall.year + _MAX_YEARS_AHEAD

    def next_value(values, current):
        for value in values:
            if value > current:
                return value
        return None

    while cand.year <= limit_year:
        if cand.month not in cron.month:
            nxt = next_value(cron.month, cand.month)
            if nxt is None:
                cand = datetime.datetime(cand.year + 1, 1, 1)
            else:
                cand = cand.replace(month=nxt, day=1, hour=0, minute=0)
            continue
        if (cand.day not in cron.day
                or ((cand.weekday() + 1) % 7) not in cron.weekday):
            cand = cand.replace(hour=0, minute=0) + datetime.timedelta(days=1)
            continue
        if cand.hour not in cron.hour:
            nxt = next_value(cron.hour, cand.hour)
            if nxt is None:
                cand = cand.replace(hour=0, minute=0) + datetime.timedelta(days=1)
            else:
                cand = cand.replace(hour=nxt, minute=0)
            continue
        if cand.minute not in cron.minute:
            nxt = next_value(cron.minute, cand.minute)
            if nxt is None:
                cand = cand.replace(minute=0) + datetime.timedelta(hours=1)
            else:
                cand = cand.replace(minute=nxt)
            continue
        return cand
    return None


def _iter_wall_candidates(after_label: datetime.datetime,
                          end_label: Optional[datetime.datetime],
                          cron: CronSchedule):
    """枚举墙上标签在 ``(after_label, end_label]`` 内的 cron 候选。"""
    wall = after_label
    while True:
        cand = _next_wall_candidate(wall, cron)
        if cand is None or (end_label is not None and cand > end_label):
            return
        yield cand
        wall = cand


def _prev_wall_candidate(wall: datetime.datetime, cron: CronSchedule
                         ) -> Optional[datetime.datetime]:
    """从 ``wall``（不含）起，找前一个可能命中 cron 的墙上整分（正向的镜像）。"""
    cand = wall.replace(second=0, microsecond=0) - datetime.timedelta(minutes=1)
    limit_year = wall.year - _MAX_YEARS_AHEAD

    def prev_value(values, current):
        for value in reversed(values):
            if value < current:
                return value
        return None

    while cand.year >= limit_year:
        if cand.month not in cron.month:
            nxt = prev_value(cron.month, cand.month)
            if nxt is None:
                cand = datetime.datetime(cand.year - 1, 12, 31, 23, 59)
            else:
                # 落到目标月最后一天 23:59（用「目标月次月 1 日减 1 分钟」求月末）
                month_after_year = cand.year if nxt < 12 else cand.year - 1
                month_after_month = nxt + 1 if nxt < 12 else 1
                cand = datetime.datetime(month_after_year, month_after_month, 1) \
                    - datetime.timedelta(minutes=1)
            continue
        if (cand.day not in cron.day
                or ((cand.weekday() + 1) % 7) not in cron.weekday):
            cand = cand.replace(hour=23, minute=59) - datetime.timedelta(days=1)
            continue
        if cand.hour not in cron.hour:
            nxt = prev_value(cron.hour, cand.hour)
            if nxt is None:
                cand = cand.replace(hour=23, minute=59) - datetime.timedelta(days=1)
            else:
                cand = cand.replace(hour=nxt, minute=59)
            continue
        if cand.minute not in cron.minute:
            nxt = prev_value(cron.minute, cand.minute)
            if nxt is None:
                cand = cand.replace(minute=0) - datetime.timedelta(minutes=1)
            else:
                cand = cand.replace(minute=nxt)
            continue
        return cand
    return None


# ---------------------------------------------------------------------------
# 触发点枚举（核心对外 API）
# ---------------------------------------------------------------------------

def _to_naive(epoch_minute: int, tz: datetime.tzinfo) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(epoch_minute * 60, tz=_UTC).astimezone(tz) \
        .replace(tzinfo=None)


def fires_between(cron: CronSchedule, tz: datetime.tzinfo,
                  after_epoch_minute: int, upto_epoch_minute: int,
                  limit: int = MAX_FIRE_LIMIT) -> list[tuple[int, bool]]:
    """枚举物理时刻落在 ``(after, upto]`` 的全部触发点（UTC 整分，升序去重）。

    先按墙上标签枚举 cron 候选（标签窗口与物理窗口对齐，overlap 的第二次
    出现不会漏），再把每个标称点展开成物理时刻，最后按 UTC 整分排序去重
    并裁剪到物理区间。停机补跑就用它一次性算出错过的所有触发点。
    """
    # 标签起点与 after 对齐（after 可能正处在 overlap 两次出现之间，第二次
    # 出现的标签更早，所以从同标签开始枚举，靠物理区间过滤）。
    after_label = _to_naive(after_epoch_minute, tz)
    end_label = _to_naive(upto_epoch_minute, tz)
    items: dict[int, bool] = {}
    for cand in _iter_wall_candidates(after_label - datetime.timedelta(minutes=1),
                                      end_label, cron):
        for epoch_minute, adjusted in _physical_minutes(cand, tz):
            if after_epoch_minute < epoch_minute <= upto_epoch_minute:
                # 同一物理分钟只记一次；只要它来自任一 gap 标称点就标调整
                items[epoch_minute] = items.get(epoch_minute, False) or adjusted
    result = sorted(items.items())
    return result[:limit]


def next_fire(cron: CronSchedule, tz: datetime.tzinfo,
              after_epoch_minute: int) -> Optional[tuple[int, bool]]:
    """严格晚于某物理时刻的下一个触发点；找不到（表达式无意义）返回 ``None``。"""
    # gap 会让较早的墙上标签映射到较晚的物理时刻（如 02:30→03:30），因此
    # 标签起点要回退一个 gap 余量（24h 覆盖任意时区），再用物理时刻过滤。
    after_label = _to_naive(after_epoch_minute, tz)
    start_label = after_label - _OVERLAP_SAFETY
    safety_minutes = int(_OVERLAP_SAFETY.total_seconds() // 60)
    best: Optional[tuple[int, bool]] = None
    wall = start_label - datetime.timedelta(minutes=1)
    while True:
        cand = _next_wall_candidate(wall, cron)
        if cand is None:
            return None
        earliest = _physical_minutes(cand, tz)[0][0]
        # 此后的标称点严格更晚，其物理时刻即便遇 gap 前移也不会更早；一旦
        # （最早物理时刻 - 余量）都不早于当前最优解，后续更不可能更优。
        if best is not None and earliest - safety_minutes >= best[0]:
            break
        for epoch_minute, adjusted in _physical_minutes(cand, tz):
            if epoch_minute > after_epoch_minute:
                if best is None or epoch_minute < best[0]:
                    best = (epoch_minute, adjusted)
        wall = cand
    return best


def recent_fires_between(cron: CronSchedule, tz: datetime.tzinfo,
                         after_epoch_minute: int, upto_epoch_minute: int,
                         max_recent: int, count_cap: int = MAX_FIRE_LIMIT
                         ) -> tuple[list[tuple[int, bool]], int]:
    """统计区间内触发点总数，并返回最近的至多 ``max_recent`` 个（升序）。

    用于停机恢复：错过点数可能极大（如停机一年的每分钟计划），不能全部
    落库。这里从 ``upto`` 倒着枚举，凑够最近 ``max_recent`` 个就停止收集，
    但继续数到 ``count_cap`` 以得出「更早还有多少次」的折叠数量。
    返回 ``(recent_ascending, total_count)``，``total_count`` 在超过
    ``count_cap`` 时记为 ``count_cap``（折叠文案会提示数量已封顶）。
    """
    upto_label = _to_naive(upto_epoch_minute, tz)
    stop_label = _to_naive(after_epoch_minute, tz) - datetime.timedelta(minutes=1)

    # adjusted_map 保留窗口内每个物理点的 gap 调整标记；count 只计数不存点，
    # 因此即便错过数十万也不占内存。倒序枚举，最近的点最后插入最稳妥，
    # 这里用 epoch -> adjusted 的 dict，最后统一按 epoch 排序取最近 N 个。
    adjusted_map: dict[int, bool] = {}
    counted_epochs: set[int] = set()
    wall = upto_label + datetime.timedelta(minutes=1)
    while True:
        cand = _prev_wall_candidate(wall, cron)
        if cand is None or cand < stop_label:
            break
        for epoch_minute, adjusted in _physical_minutes(cand, tz):
            if not (after_epoch_minute < epoch_minute <= upto_epoch_minute):
                continue
            # 同一点可能先以真实标称点、后以 gap 调整标称点出现，标记取 OR
            adjusted_map[epoch_minute] = adjusted_map.get(epoch_minute, False) or adjusted
            counted_epochs.add(epoch_minute)
        wall = cand
        # 已倒序走过的物理跨度足够覆盖 max_recent 个点 + overlap 余量后，
        # 较早的点不再可能进入最近窗口，逐步剔除以控制内存。
        if len(adjusted_map) > max_recent + 64:
            kept = sorted(adjusted_map)[-max_recent:]
            adjusted_map = {k: adjusted_map[k] for k in kept}
        if len(counted_epochs) >= count_cap:
            break
    items = sorted(adjusted_map.items())
    recent = items[-max_recent:] if len(items) > max_recent else items
    return recent, len(counted_epochs)


# ---------------------------------------------------------------------------
# 展示
# ---------------------------------------------------------------------------

def wall_text(epoch_minute: int, tz: datetime.tzinfo) -> dict:
    """把 UTC 整分格式化成「计划时区」的墙上时间描述（供页面展示）。"""
    utc_dt = datetime.datetime.fromtimestamp(epoch_minute * 60, tz=_UTC)
    local = utc_dt.astimezone(tz)
    offset = local.utcoffset() or datetime.timedelta()
    total_min = int(offset.total_seconds() // 60)
    sign = "+" if total_min >= 0 else "-"
    total_min = abs(total_min)
    return {
        "epoch": epoch_minute * 60,             # 秒级时间戳，浏览器按本地时区渲染
        "text": local.strftime("%Y-%m-%d %H:%M"),
        "weekday": _WEEKDAY_CN[local.weekday()],
        "offset": f"UTC{sign}{total_min // 60:02d}:{total_min % 60:02d}",
        "tz_abbr": local.tzname() or "",
        "dst": bool(local.dst()),
    }
