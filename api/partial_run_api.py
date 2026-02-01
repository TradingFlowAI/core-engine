"""
Partial Run API: 局部执行 API 端点

支持四种模式：
- single: 只运行触发节点
- upstream: 从源头运行到触发节点
- downstream: 从触发节点运行到末尾
- component: 运行整个连通分量
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json as sanic_json

from flow.scheduler import get_scheduler_instance, NodeConflictError
from flow.graph_utils import FlowGraph, build_graph_from_flow_structure, get_nodes_to_execute

logger = logging.getLogger(__name__)

partial_run_bp = Blueprint("partial_run_api", url_prefix="/flows")


def generate_partial_execution_id() -> str:
    """生成局部执行 ID"""
    return f"partial_{uuid.uuid4().hex[:12]}"


@partial_run_bp.post("/<flow_id:str>/partial-run")
async def partial_run(request: Request, flow_id: str):
    """
    执行局部 Run

    Request Body:
    {
        "trigger_node_id": "node_123",
        "mode": "single" | "upstream" | "downstream" | "component",
        "signal_config": {
            "use_previous": true,
            "custom_signals": { ... }
        },
        "options": {
            "skip_cache": false,
            "debug_mode": false
        }
    }

    Response:
    {
        "execution_id": "partial_run_xxx",
        "mode": "upstream",
        "nodes_to_execute": ["node_a", "node_b"],
        "status": "started"
    }
    """
    try:
        body = request.json or {}

        # 验证必需字段
        trigger_node_id = body.get("trigger_node_id")
        if not trigger_node_id:
            return sanic_json({
                "error": "Missing required field: trigger_node_id"
            }, status=400)

        mode = body.get("mode", "single")
        if mode not in ["single", "upstream", "downstream", "component"]:
            return sanic_json({
                "error": f"Invalid mode: {mode}. Must be one of: single, upstream, downstream, component"
            }, status=400)

        signal_config = body.get("signal_config", {})
        options = body.get("options", {})
        
        # 获取前端传递的 nodes 和 edges（可选，用于覆盖 Redis 中的数据）
        nodes = body.get("nodes")
        edges = body.get("edges")

        # 获取 scheduler
        scheduler = get_scheduler_instance()
        if not scheduler.redis:
            await scheduler.initialize()

        # 执行局部 Run
        result = await scheduler.execute_partial(
            flow_id=flow_id,
            trigger_node_id=trigger_node_id,
            mode=mode,
            signal_config=signal_config,
            options=options,
            nodes=nodes,
            edges=edges,
        )

        return sanic_json(result)

    except ValueError as e:
        logger.warning(f"Partial run validation error: {e}")
        return sanic_json({"error": str(e)}, status=400)
    except NodeConflictError as e:
        # 🔥 节点冲突：返回 409，让前端显示 Wait/Force 选项
        logger.warning(f"Node conflict in partial run: {e.node_id} - {e.message}")
        return sanic_json(e.to_dict(), status=409)
    except Exception as e:
        logger.exception(f"Error executing partial run for flow {flow_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


@partial_run_bp.get("/<flow_id:str>/partial-run/<execution_id:str>")
async def get_partial_run_status(request: Request, flow_id: str, execution_id: str):
    """
    获取局部 Run 的执行状态

    Response:
    {
        "execution_id": "partial_run_xxx",
        "flow_id": "flow_123",
        "status": "running" | "completed" | "failed",
        "nodes_status": {
            "node_1": "completed",
            "node_2": "running"
        },
        "started_at": "...",
        "completed_at": "..."
    }
    """
    try:
        scheduler = get_scheduler_instance()
        if not scheduler.redis:
            await scheduler.initialize()

        # 从 Redis 获取执行状态
        status_key = f"partial_run:{flow_id}:{execution_id}"
        status_data = await scheduler.redis.get(status_key)

        if not status_data:
            return sanic_json({
                "error": f"Partial run {execution_id} not found"
            }, status=404)

        return sanic_json(json.loads(status_data))

    except Exception as e:
        logger.exception(f"Error getting partial run status: {e}")
        return sanic_json({"error": str(e)}, status=500)


@partial_run_bp.get("/<flow_id:str>/partial-runs")
async def list_partial_runs(request: Request, flow_id: str):
    """
    列出 Flow 的所有局部 Run

    Query params:
    - limit: 返回数量限制（默认 20）
    - status: 过滤状态

    Response:
    {
        "flow_id": "flow_123",
        "partial_runs": [...],
        "total": 10
    }
    """
    try:
        limit = int(request.args.get("limit", 20))
        status_filter = request.args.get("status")

        scheduler = get_scheduler_instance()
        if not scheduler.redis:
            await scheduler.initialize()

        # 获取所有局部 Run 的 keys
        pattern = f"partial_run:{flow_id}:*"
        partial_runs = []

        async for key in scheduler.redis.scan_iter(match=pattern):
            data_str = await scheduler.redis.get(key)
            if data_str:
                data = json.loads(data_str)
                if status_filter and data.get("status") != status_filter:
                    continue
                partial_runs.append(data)

        # 按时间排序
        partial_runs.sort(key=lambda x: x.get("started_at", ""), reverse=True)

        return sanic_json({
            "flow_id": flow_id,
            "partial_runs": partial_runs[:limit],
            "total": len(partial_runs),
        })

    except Exception as e:
        logger.exception(f"Error listing partial runs for flow {flow_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


@partial_run_bp.post("/<flow_id:str>/partial-run/<execution_id:str>/cancel")
async def cancel_partial_run(request: Request, flow_id: str, execution_id: str):
    """
    取消正在进行的局部 Run

    Response:
    {
        "execution_id": "partial_run_xxx",
        "status": "cancelled"
    }
    """
    try:
        scheduler = get_scheduler_instance()
        if not scheduler.redis:
            await scheduler.initialize()

        # 获取执行状态
        status_key = f"partial_run:{flow_id}:{execution_id}"
        status_data = await scheduler.redis.get(status_key)

        if not status_data:
            return sanic_json({
                "error": f"Partial run {execution_id} not found"
            }, status=404)

        data = json.loads(status_data)

        if data.get("status") in ["completed", "failed", "cancelled"]:
            return sanic_json({
                "error": f"Partial run is already {data.get('status')}"
            }, status=400)

        # 更新状态为取消
        data["status"] = "cancelled"
        data["cancelled_at"] = datetime.now(timezone.utc).isoformat()

        await scheduler.redis.set(status_key, json.dumps(data), ex=86400)

        return sanic_json({
            "execution_id": execution_id,
            "status": "cancelled",
        })

    except Exception as e:
        logger.exception(f"Error cancelling partial run {execution_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


@partial_run_bp.get("/<flow_id:str>/graph/nodes-to-execute")
async def preview_nodes_to_execute(request: Request, flow_id: str):
    """
    预览将要执行的节点列表（不实际执行）

    Query params:
    - trigger_node_id: 触发节点 ID
    - mode: 执行模式

    Response:
    {
        "trigger_node_id": "node_123",
        "mode": "upstream",
        "nodes_to_execute": ["node_a", "node_b"],
        "count": 2
    }
    """
    try:
        trigger_node_id = request.args.get("trigger_node_id")
        mode = request.args.get("mode", "single")

        if not trigger_node_id:
            return sanic_json({
                "error": "Missing required query param: trigger_node_id"
            }, status=400)

        if mode not in ["single", "upstream", "downstream", "component"]:
            return sanic_json({
                "error": f"Invalid mode: {mode}"
            }, status=400)

        scheduler = get_scheduler_instance()
        if not scheduler.redis:
            await scheduler.initialize()

        # 获取 Flow 结构
        flow_data = await scheduler.redis.hgetall(f"flow:{flow_id}")
        if not flow_data:
            return sanic_json({
                "error": f"Flow {flow_id} not found"
            }, status=404)

        structure_str = flow_data.get("structure", "{}")
        flow_structure = json.loads(structure_str)

        # 构建图并计算节点
        graph = build_graph_from_flow_structure(flow_structure)
        nodes_to_execute = get_nodes_to_execute(graph, trigger_node_id, mode)

        return sanic_json({
            "trigger_node_id": trigger_node_id,
            "mode": mode,
            "nodes_to_execute": nodes_to_execute,
            "count": len(nodes_to_execute),
        })

    except Exception as e:
        logger.exception(f"Error previewing nodes to execute: {e}")
        return sanic_json({"error": str(e)}, status=500)
