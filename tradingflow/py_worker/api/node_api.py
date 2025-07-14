"""节点管理API"""

import asyncio
import logging
from datetime import datetime

from sanic import Blueprint, Request
from sanic.response import json as sanic_json

from tradingflow.common.config import CONFIG
from tradingflow.py_worker.common.node_registry import NodeRegistry
from tradingflow.py_worker.common.node_task_manager import NodeTaskManager
from tradingflow.py_worker.core.node_executor import execute_node_task

logger = logging.getLogger(__name__)

# 获取配置
WORKER_ID = CONFIG["WORKER_ID"]

# 获取和message queue相关配置
MESSAGE_QUEUE_CONFIG = {
    "type": CONFIG["MESSAGE_QUEUE_TYPE"],
    "config": {
        "host": CONFIG["RABBITMQ_HOST"],
        "port": CONFIG["RABBITMQ_PORT"],
        "username": CONFIG["RABBITMQ_USERNAME"],
        "password": CONFIG["RABBITMQ_PASSWORD"],
        "virtual_host": CONFIG["RABBITMQ_VHOST"],
        "exchange": CONFIG["RABBITMQ_EXCHANGE"],
        "exchange_type": CONFIG["RABBITMQ_EXCHANGE_TYPE"],
        "url": CONFIG["RABBITMQ_URL"],
    },
}


node_manager = NodeTaskManager.get_instance()

node_bp = Blueprint("node_api")


@node_bp.post("/nodes/execute")
async def execute_node(request: Request):
    """执行节点的接口"""
    try:
        logger.debug("Received request to execute node")
        node_data = request.json

        logger.debug("Node data: %s", node_data)

        # 必要参数检查
        required_fields = [
            "flow_id",
            "component_id",  # 指的是图中的分量序号
            "cycle",
            "node_id",
            "node_type",
            "config",
        ]
        for field in required_fields:
            if field not in node_data:
                return sanic_json(
                    {"error": f"Missing required field: {field}"}, status=400
                )
        flow_id = node_data["flow_id"]
        component_id = node_data["component_id"]
        cycle = node_data["cycle"]
        node_id = node_data["node_id"]
        node_type = node_data["node_type"]

        node_task_id = f"{flow_id}_{cycle}_{node_id}"
        # 检查节点类型是否支持
        supported_types = NodeRegistry.get_instance().get_supported_node_types()
        if node_type not in supported_types:
            logger.warning(
                "Unsupported node type: %s. Supported types: %s",
                node_type,
                supported_types,
            )
            return sanic_json(
                {"error": f"Unsupported node type: {node_type}"}, status=400
            )

        # 检查节点是否已在运行
        existing_node_task = await node_manager.get_task(node_task_id)
        logger.info("Existing node task %s, info: %s", node_task_id, existing_node_task)
        if existing_node_task and existing_node_task.get("status") in [
            "running",
            "initializing",
        ]:
            return sanic_json(
                {
                    "error": "Node is already running",
                    "status": existing_node_task.get("status"),
                },
                status=400,
            )

        # 创建节点执行任务
        task = asyncio.create_task(
            execute_node_task(
                node_task_id,
                flow_id,
                component_id,
                cycle,
                node_id,
                node_type,
                node_data,
            )
        )

        # 注册节点到管理器

        node_info = {
            "flow_id": flow_id,
            "component_id": component_id,
            "cycle": cycle,
            "task_id": str(id(task)),  # 保存任务ID而不是对象
            "node_task_id": node_task_id,
            "start_time": datetime.now().isoformat(),
            "status": "initializing",
            "node_type": node_type,
            "progress": 0,
            "config": node_data.get("config", {}),
        }

        await node_manager.register_task(node_task_id, node_info)

        return sanic_json(
            {
                "node_id": node_id,
                "status": "started",
                "message": "Node execution started",
            }
        )

    except Exception as e:
        logger.error(f"Node execution error: {str(e)}")
        return sanic_json({"error": str(e)}, status=500)


@node_bp.get("/nodes/<node_task_id>/status")
async def get_node_status(request: Request, node_task_id: str):
    """查询节点执行状态"""
    node_info = await node_manager.get_task(node_task_id)
    if not node_info:
        return sanic_json({"error": "Node not found"}, status=404)

    # 构建状态响应，排除task等不可序列化的字段
    return sanic_json({"node_id": node_task_id, **node_info})


@node_bp.get("/nodes")
async def get_all_nodes(request: Request):
    """获取所有节点信息"""
    all_nodes = await node_manager.get_all_tasks()
    return sanic_json({"nodes": all_nodes, "count": len(all_nodes)})


@node_bp.get("/worker/nodes")
async def get_worker_nodes(request: Request):
    """获取当前worker的所有节点信息"""
    worker_nodes = await node_manager.get_worker_tasks()
    return sanic_json({"nodes": worker_nodes, "count": len(worker_nodes)})


@node_bp.post("/nodes/<node_task_id>/stop")
async def stop_node(request: Request, node_task_id: str):
    """停止节点执行"""
    from datetime import datetime

    import httpx
    from tradingflow.common.config import CONFIG

    SERVER_URL = CONFIG["SERVER_URL"]

    node_info = await node_manager.get_task(node_task_id)
    if not node_info:
        return sanic_json({"error": "Node not found"}, status=404)

    # 检查节点是否正在运行
    if node_info.get("status") not in ["running", "initializing", "starting"]:
        return sanic_json(
            {
                "status": node_info.get("status"),
                "message": "Node is not running",
            }
        )

    try:
        # 通过NodeManager设置终止标志
        stop_success = await node_manager.stop_node(node_task_id)
        if not stop_success:
            return sanic_json({"error": "Failed to stop node"}, status=500)

        # 通知server节点被停止
        try:
            stop_data = {
                "node_task_id": node_task_id,
                "worker_id": WORKER_ID,
                "status": "stopping",
                "stop_requested_at": datetime.now().isoformat(),
            }

            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{SERVER_URL}/api/nodes/{node_task_id}/callback", json=stop_data
                )
        except Exception as e:
            logger.error(f"Failed to notify server about node stopping: {str(e)}")

        return sanic_json(
            {
                "node_id": node_task_id,
                "status": "stopping",
                "message": "Node termination requested",
            }
        )

    except Exception as e:
        logger.error(f"Error stopping node: {str(e)}")
        return sanic_json({"error": str(e)}, status=500)
