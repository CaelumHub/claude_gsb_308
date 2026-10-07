"""测试执行引擎与调度核心。

模块划分
--------
- :mod:`engine.models`      领域模型与通用工具（id 生成、优先级、状态枚举）
- :mod:`engine.executor`    测试用例执行器（步骤 + 断言 + 模拟请求 + 超时）
- :mod:`engine.cron`        5 段 cron 表达式匹配与时区感知的触发时刻计算（定时触发）
- :mod:`engine.environments`环境管理（配置、依赖解析、工作区隔离）
- :mod:`engine.coverage`    代码覆盖率分析（模拟，按构建稳定生成）
- :mod:`engine.report`      测试报告生成（通过率 / 耗时 / 分组 / 趋势）
- :mod:`engine.defects`     缺陷跟踪
- :mod:`engine.notify`      通知与集成
- :mod:`engine.scheduler`   并发调度（构建池 + 用例池 + 定时触发循环）
"""

from .models import (
    PRIORITIES,
    CASE_STATUSES,
    BUILD_STATUSES,
    MISFIRE_POLICIES,
    new_id,
    now,
)
from .cron import (COMMON_TIMEZONES, CronSchedule, cron_matches, get_timezone,
                   local_timezone_name, next_fire_time, next_fire_timestamp,
                   parse_cron, resolve_wall_time)
from .executor import TestExecutor, ExecutionError
from .environments import EnvironmentManager
from .coverage import CoverageAnalyzer
from .report import ReportGenerator
from .defects import DefectManager
from .notify import NotificationManager
from .scheduler import Scheduler

__all__ = [
    "PRIORITIES",
    "CASE_STATUSES",
    "BUILD_STATUSES",
    "MISFIRE_POLICIES",
    "new_id",
    "now",
    "COMMON_TIMEZONES",
    "CronSchedule",
    "cron_matches",
    "get_timezone",
    "local_timezone_name",
    "next_fire_time",
    "next_fire_timestamp",
    "parse_cron",
    "resolve_wall_time",
    "TestExecutor",
    "ExecutionError",
    "EnvironmentManager",
    "CoverageAnalyzer",
    "ReportGenerator",
    "DefectManager",
    "NotificationManager",
    "Scheduler",
]
