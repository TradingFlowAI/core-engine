"""Node management API"""

import asyncio
import logging
from datetime import datetime

from sanic import Blueprint, Request
from sanic.response import json as sanic_json

from infra.config import CONFIG
from common.node_registry import NodeRegistry
from common.node_task_manager import NodeTaskManager
from core.node_executor import execute_node_task

logger = logging.getLogger(__name__)

# Get configuration
WORKER_ID = CONFIG["WORKER_ID"]

# Get message queue related configuration
MESSAGE_QUEUE_CONFIG = {
    "type": CONFIG["MESSAGE_QUEUE_TYPE"],
    "config": {
        "host": CONFIG["RABBITMQ_HOST"],
        "port": CONFIG["RABBITMQ_PORT"],
        "username": CONFIG["RABBITMQ_USER"],  # Fixed: use RABBITMQ_USER instead of RABBITMQ_USERNAME
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
    """Execute node endpoint"""
    try:
        logger.debug("Received request to execute node")
        node_data = request.json

        logger.debug("Node data: %s", node_data)

        # Required parameters check
        required_fields = [
            "flow_id",
            "component_id",  # Refers to the component index in the graph
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
        # Check if node type is supported
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

        # Check if node is already running
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

        # Create node execution task
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

        # Register node to manager

        node_info = {
            "flow_id": flow_id,
            "component_id": component_id,
            "cycle": cycle,
            "task_id": str(id(task)),  # Save task ID instead of object
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


@node_bp.get("/nodes/types")
async def get_node_types(request: Request):
    """Get all node type information, including base class and instance relationships"""
    try:
        registry = NodeRegistry.get_instance()

        types_info = {}
        for node_type, node_class in registry._node_classes.items():
            # Get default parameters
            default_params = registry.get_default_params(node_type)

            # Prioritize getting metadata from default params (backward compatible)
            node_category = default_params.get('node_category', 'base')
            display_name = default_params.get('display_name', node_type)
            base_node_type = default_params.get('base_node_type', None)
            version = default_params.get('version', '0.0.1')
            description = default_params.get('description', node_class.__doc__ or "")
            author = default_params.get('author', "")
            tags = default_params.get('tags', [])

            # Try to get from class-level metadata (if exists)
            class_metadata = getattr(node_class, '_metadata', None)
            if class_metadata:
                node_category = class_metadata.node_category
                display_name = class_metadata.display_name or display_name
                base_node_type = class_metadata.base_node_type or base_node_type
                version = class_metadata.version
                description = class_metadata.description or description
                author = class_metadata.author or author
                tags = class_metadata.tags or tags

            # Get input handle information
            input_handles = {}
            if hasattr(node_class, '_input_handles'):
                for handle_name, handle in node_class._input_handles.items():
                    input_handles[handle_name] = handle.to_dict()

            # Build node type information
            types_info[node_type] = {
                "class_name": node_class.__name__,
                "category": node_category,
                "display_name": display_name,
                "base_type": base_node_type,
                "version": version,
                "description": description,
                "author": author,
                "tags": tags,
                "default_params": default_params,
                "input_handles": input_handles,
                "docstring": node_class.__doc__ or ""
            }

        return sanic_json({
            "status": "success",
            "data": types_info,
            "count": len(types_info)
        })
    except Exception as e:
        logger.error(f"Error getting node types: {str(e)}")
        return sanic_json({"status": "error", "message": str(e)}, status=500)


@node_bp.get("/nodes/<node_task_id>/status")
async def get_node_status(request: Request, node_task_id: str):
    """Query node execution status"""
    node_info = await node_manager.get_task(node_task_id)
    if not node_info:
        return sanic_json({"error": "Node not found"}, status=404)

    # Build status response, excluding non-serializable fields like task
    return sanic_json({"node_id": node_task_id, **node_info})


@node_bp.get("/nodes")
async def get_all_nodes(request: Request):
    """Get all node information"""
    all_nodes = await node_manager.get_all_tasks()
    return sanic_json({"nodes": all_nodes, "count": len(all_nodes)})


@node_bp.get("/worker/nodes")
async def get_worker_nodes(request: Request):
    """Get all node information for current worker"""
    worker_nodes = await node_manager.get_worker_tasks()
    return sanic_json({"nodes": worker_nodes, "count": len(worker_nodes)})


@node_bp.post("/nodes/<node_task_id>/stop")
async def stop_node(request: Request, node_task_id: str):
    """Stop node execution"""
    from datetime import datetime

    import httpx
    from infra.config import CONFIG

    # SERVER_URL = CONFIG["SERVER_URL"]

    node_info = await node_manager.get_task(node_task_id)
    if not node_info:
        return sanic_json({"error": "Node not found"}, status=404)

    # Check if node is running
    if node_info.get("status") not in ["running", "initializing", "starting"]:
        return sanic_json(
            {
                "status": node_info.get("status"),
                "message": "Node is not running",
            }
        )

    try:
        # Set termination flag through NodeManager
        stop_success = await node_manager.stop_task(node_task_id)
        if not stop_success:
            return sanic_json({"error": "Failed to stop node"}, status=500)

        # Notify server that node is stopped
        # try:
        #     stop_data = {
        #         "node_task_id": node_task_id,
        #         "worker_id": WORKER_ID,
        #         "status": "stopping",
        #         "stop_requested_at": datetime.now().isoformat(),
        #     }

        #     async with httpx.AsyncClient() as client:
        #         await client.post(
        #             f"{SERVER_URL}/api/nodes/{node_task_id}/callback", json=stop_data
        #         )
        # except Exception as e:
        #     logger.error(f"Failed to notify server about node stopping: {str(e)}")

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
