"""系统资源统计功能"""

import platform
from typing import Any, Dict

import psutil


def get_system_stats() -> Dict[str, Any]:
    """获取系统资源使用情况"""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()

    return {
        "cpu_percent": cpu_percent,
        "memory_percent": memory.percent,
        "memory_used_gb": round(memory.used / (1024**3), 2),
        "memory_total_gb": round(memory.total / (1024**3), 2),
        "platform": platform.platform(),
        "hostname": platform.node(),
    }
