"""健康检查API"""

from sanic import Blueprint, Request
from sanic.response import json as sanic_json


from tradingflow.depot.config import CONFIG
from tradingflow.station.common.node_task_manager import NodeTaskManager
from tradingflow.station.core.system_stats import get_system_stats

# 获取配置项
WORKER_ID = CONFIG["WORKER_ID"]
node_manager = NodeTaskManager.get_instance()


health_bp = Blueprint("health_api")


@health_bp.get("/health")
async def health_check(request: Request):
    """健康检查接口 - 供server检查worker存活"""
    return sanic_json(
        {
            "status": "healthy",
            "worker_id": WORKER_ID,
            "uptime": (
                request.app.ctx.uptime
                if hasattr(request.app.ctx, "uptime")
                else "unknown"
            ),
            "running_nodes": len(
                [
                    n
                    for n in await node_manager.get_all_tasks()
                    if n.get("status") == "running"
                ]
            ),
            "system_stats": get_system_stats(),
        }
    )
