"""节点执行核心功能"""

import logging
from datetime import datetime
from typing import Any, Dict

from tradingflow.depot.python.config import CONFIG
from tradingflow.depot.python.exceptions.tf_exception import (
    NodeExecutionException,
    NodeResourceException,
    NodeStopExecutionException,
    NodeTimeoutException,
    NodeValidationException,
)
from tradingflow.station.common.edge import Edge
from tradingflow.station.common.node_registry import NodeRegistry
from tradingflow.station.common.node_task_manager import NodeTaskManager
from tradingflow.station.nodes.node_base import NodeStatus
from tradingflow.station.utils.result_extractor import extract_node_result

logger = logging.getLogger(__name__)

# 获取配置
WORKER_ID = CONFIG["WORKER_ID"]

# 获取共享实例
node_manager = NodeTaskManager.get_instance()
node_registry = NodeRegistry.get_instance()

# 导入所有注册的节点类型，确保装饰器被执行
import tradingflow.station.nodes  # noqa: F401, E402


async def execute_node_task(
    node_task_id: str,
    flow_id: str,
    component_id: int,
    cycle: int,
    node_id: str,
    node_type: str,
    node_data: Dict[str, Any],
):
    """执行节点的具体逻辑"""
    node_instance = None

    try:
        # 初始化状态
        await _update_node_status(node_task_id, NodeStatus.PENDING, "Task created")

        # 确保node_manager的状态存储已初始化
        if not node_manager._initialized:
            await node_manager.initialize()

        await _update_node_status(node_task_id, NodeStatus.RUNNING, "Initializing node")

        # 创建节点实例
        node_instance = await _create_node_instance(
            node_type, node_data, flow_id, component_id, cycle, node_id
        )

        await _update_node_status(
            node_task_id,
            NodeStatus.RUNNING,
            "Node created, starting execution",
            {"created_at": datetime.now().isoformat()},
        )

        # 执行节点逻辑
        success = await node_instance.start()

        # 处理执行结果
        if success:
            await _update_node_status(
                node_task_id,
                NodeStatus.COMPLETED,
                "Node execution completed successfully",
                {"progress": 100, "completed_at": datetime.now().isoformat()},
            )
            logger.info(f"Node {node_id} execution completed successfully")
        else:
            await _update_node_status(
                node_task_id,
                NodeStatus.FAILED,
                "Node execution returned False",
                {"failed_at": datetime.now().isoformat()},
            )
            logger.error("Node task %s execution returned False", node_task_id)

    except NodeStopExecutionException as e:
        # 处理停止执行异常
        await _update_node_status(
            node_task_id,
            NodeStatus.TERMINATED,
            f"Node stopped by signal: {e.reason}",
            {
                "stop_reason": e.reason,
                "source_node": e.source_node,
                "terminated_at": datetime.now().isoformat(),
            },
        )
        logger.warning(f"Node {node_id} terminated by stop signal: {e.reason}")

    except NodeTimeoutException as e:
        # 处理超时异常
        await _update_node_status(
            node_task_id,
            NodeStatus.FAILED,
            f"Node execution timeout: {e.message}",
            {
                "timeout_seconds": e.timeout_seconds,
                "failed_at": datetime.now().isoformat(),
            },
        )
        logger.error(f"Node {node_id} execution timeout: {e.message}")

    except NodeValidationException as e:
        # 处理验证异常
        await _update_node_status(
            node_task_id,
            NodeStatus.FAILED,
            f"Node validation failed: {e.message}",
            {
                "invalid_params": e.invalid_params,
                "failed_at": datetime.now().isoformat(),
            },
        )
        logger.error(f"Node {node_id} validation failed: {e.message}")

    except NodeResourceException as e:
        # 处理资源异常
        await _update_node_status(
            node_task_id,
            NodeStatus.FAILED,
            f"Node resource error: {e.message}",
            {"resource_type": e.resource_type, "failed_at": datetime.now().isoformat()},
        )
        logger.error(f"Node {node_id} resource error: {e.message}")

    except NodeExecutionException as e:
        # 处理通用节点执行异常
        status = (
            NodeStatus.FAILED if e.status != "terminated" else NodeStatus.TERMINATED
        )
        await _update_node_status(
            node_task_id,
            status,
            f"Node execution error: {e.message}",
            {"failed_at": datetime.now().isoformat()},
        )
        logger.error(f"Node {node_id} execution error: {e.message}")

    except Exception as e:
        # 处理未知异常
        await _update_node_status(
            node_task_id,
            NodeStatus.FAILED,
            f"Unexpected error: {str(e)}",
            {"failed_at": datetime.now().isoformat()},
        )
        logger.exception(f"Unexpected error executing node {node_id}: {str(e)}")

    finally:
        # 清理资源
        if node_instance:
            try:
                await node_instance.cleanup()
            except Exception as e:
                logger.error(f"Error cleaning up node {node_id}: {str(e)}")

        # 通知server节点执行完成
        await _notify_server_completion(node_task_id, node_instance)


async def _create_node_instance(
    node_type: str,
    node_data: Dict[str, Any],
    flow_id: str,
    component_id: int,
    cycle: int,
    node_id: str,
):
    """创建节点实例"""
    try:
        if node_type == "python":
            node_class_type = node_data.get("config", {}).get("node_class_type")
            if not node_class_type:
                raise NodeValidationException(
                    "Missing node_class_type in config",
                    node_id,
                    {"missing_field": "node_class_type"},
                )
        else:
            node_class_type = node_type

        config = node_data.get("config", {})
        logger.info("Creating node instance with config: %s", config)

        input_edges = [
            Edge.from_dict(edge) for edge in node_data.get("input_edges") or []
        ]
        output_edges = [
            Edge.from_dict(edge) for edge in node_data.get("output_edges") or []
        ]

        node_instance = node_registry.create_node(
            node_class_type=node_class_type,
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            input_edges=input_edges,
            output_edges=output_edges,
            config={
                **config,
                "state_store": node_manager.state_store,
            },
        )

        if not node_instance:
            raise NodeExecutionException(
                f"Failed to create node instance for type: {node_class_type}", node_id
            )

        return node_instance

    except ValueError as e:
        raise NodeValidationException(f"Invalid node configuration: {str(e)}", node_id)


async def _update_node_status(
    node_task_id: str,
    status: NodeStatus,
    message: str = None,
    additional_info: Dict[str, Any] = None,
):
    """更新节点状态"""
    try:
        info = additional_info or {}
        if message:
            info["message"] = message

        await node_manager.update_task_status(node_task_id, status.value, info)
        logger.debug(f"Updated node {node_task_id} status to {status.value}: {message}")
    except Exception as e:
        logger.error(f"Failed to update node status: {str(e)}")


async def _notify_server_completion(node_task_id: str, node_instance):
    """通知服务器节点执行完成"""
    try:
        node_info = await node_manager.get_task(node_task_id)
        if not node_info:
            logger.warning(f"Node task {node_task_id} not found in NodeManager")
            return

        result_data = {
            "node_task_id": node_task_id,
            "worker_id": WORKER_ID,
            "status": node_info.get("status", "unknown"),
            "result": {
                "success": node_info.get("status") == "completed",
                "message": node_info.get(
                    "message", f"Node task execution {node_info.get('status')}"
                ),
                "data": extract_node_result(node_instance) if node_instance else None,
            },
            "completed_at": node_info.get("completed_at", datetime.now().isoformat()),
        }

        if node_info.get("status") in ["failed", "terminated"]:
            result_data["error"] = node_info.get("message", "Unknown error")

        # TODO: 取消注释以启用服务器通知
        # async with httpx.AsyncClient() as client:
        #     await client.post(
        #         f"{SERVER_URL}/api/nodes/{node_task_id}/callback",
        #         json=result_data
        #     )
        # logger.info(f"Notified server about node {node_task_id} completion")

    except Exception as e:
        logger.error(f"Failed to notify server about node completion: {str(e)}")
