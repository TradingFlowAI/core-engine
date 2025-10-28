"""节点执行核心功能"""

import logging
from datetime import datetime
from typing import Any, Dict

from weather_depot.config import CONFIG
from weather_depot.exceptions.tf_exception import (
    InsufficientCreditsException,
    NodeExecutionException,
    NodeResourceException,
    NodeStopExecutionException,
    NodeTimeoutException,
    NodeValidationException,
)
from common.edge import Edge
from common.node_registry import NodeRegistry
from common.node_task_manager import NodeTaskManager
from nodes.node_base import NodeStatus
from utils.result_extractor import extract_node_result

logger = logging.getLogger(__name__)

# 获取配置
WORKER_ID = CONFIG["WORKER_ID"]

# 获取共享实例
node_manager = NodeTaskManager.get_instance()
node_registry = NodeRegistry.get_instance()

# 导入所有注册的节点类型，确保装饰器被执行
import nodes  # noqa: F401, E402


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

    except InsufficientCreditsException as e:
        # 处理余额不足异常 - 标记为 TERMINATED 并停止整个 component
        await _update_node_status(
            node_task_id,
            NodeStatus.TERMINATED,
            f"Insufficient credits: {e.message}",
            {
                "user_id": e.user_id,
                "required_credits": e.required_credits,
                "current_balance": e.current_balance,
                "terminated_at": datetime.now().isoformat(),
            },
        )
        logger.error(
            f"Node {node_id} terminated due to insufficient credits: "
            f"required={e.required_credits}, balance={e.current_balance}"
        )
        
        # 发送停止信号到整个 component（如果 node_instance 可用）
        if node_instance:
            try:
                await node_instance.send_stop_execution_signal(
                    reason="insufficient_credits",
                    metadata={
                        "user_id": e.user_id,
                        "required_credits": e.required_credits,
                        "current_balance": e.current_balance,
                    }
                )
                logger.info(f"Stop signal sent for component due to insufficient credits")
            except Exception as stop_error:
                logger.error(f"Failed to send stop signal: {stop_error}")

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
    """创建节点实例（支持版本管理）"""
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
        
        # 提取版本信息（支持多个位置）
        version_spec = None
        if "version" in node_data:
            version_spec = node_data["version"]
        elif "version" in config:
            version_spec = config["version"]
        else:
            version_spec = "latest"  # 默认使用最新版本
        
        logger.info(
            "Creating node instance: type=%s, version=%s, config=%s", 
            node_class_type, version_spec, config
        )
        
        # 注意：当前实现使用本地 Worker Registry (common.node_registry)
        # 它不支持版本解析，因为装饰器已经注册了节点类到本地 Registry
        # 版本信息被记录但不影响实例化（所有版本使用同一个类）
        #
        # TODO: 完整的版本支持需要以下改进：
        # 1. 在 Flow 调度时使用 core.node_registry.NodeRegistry.resolve_version()
        # 2. 将解析后的具体版本号传递到这里
        # 3. 动态加载对应版本的节点类文件
        # 4. 或者实现多版本文件加载机制（nodes/{node_type}/v{X}_{Y}_{Z}.py）
        #
        # 当前行为：记录版本信息用于日志和调试，实际使用最新注册的类

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
                "_version_spec": version_spec,  # 保存版本信息到配置中
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
