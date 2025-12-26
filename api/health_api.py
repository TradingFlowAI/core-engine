"""Health check API"""

from sanic import Blueprint, Request
from sanic.response import json as sanic_json


from infra.config import CONFIG
from common.node_task_manager import NodeTaskManager
from core.system_stats import get_system_stats

# Get configuration items
WORKER_ID = CONFIG["WORKER_ID"]
node_manager = NodeTaskManager.get_instance()


health_bp = Blueprint("health_api")


@health_bp.get("/health")
async def health_check(request: Request):
    """Health check endpoint - for server to check worker liveness"""
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
