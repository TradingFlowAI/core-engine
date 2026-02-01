"""
Pause/Resume API: 暂停和恢复执行的 API 端点

支持三级粒度：
- Flow 级别: POST /flows/{flow_id}/pause, POST /flows/{flow_id}/resume
- Component 级别: POST /flows/{flow_id}/components/{component_id}/pause/resume
- Node 级别: POST /flows/{flow_id}/nodes/{node_id}/pause/resume
"""

import logging
from typing import Optional

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json as sanic_json

from flow.pause_manager import PauseManager, PauseType, get_pause_manager

logger = logging.getLogger(__name__)

pause_bp = Blueprint("pause_api", url_prefix="/flows")


# ==================== Flow 级别 ====================


@pause_bp.post("/<flow_id:str>/pause")
async def pause_flow(request: Request, flow_id: str):
    """
    暂停整个 Flow

    Request Body (optional):
    {
        "pause_type": "manual" | "error" | "breakpoint",
        "resume_context": { ... }
    }
    """
    try:
        body = request.json or {}
        pause_type_str = body.get("pause_type", "manual")
        resume_context = body.get("resume_context")

        try:
            pause_type = PauseType(pause_type_str)
        except ValueError:
            pause_type = PauseType.MANUAL

        manager = get_pause_manager()
        await manager.initialize()

        pause_data = await manager.pause_flow(
            flow_id=flow_id,
            pause_type=pause_type,
            paused_by="user",
            resume_context=resume_context,
        )

        return sanic_json({
            "status": "paused",
            "flow_id": flow_id,
            "pause_data": pause_data,
        })

    except Exception as e:
        logger.exception(f"Error pausing flow {flow_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.post("/<flow_id:str>/resume")
async def resume_flow(request: Request, flow_id: str):
    """
    恢复 Flow 执行

    Request Body (optional):
    {
        "input_data": { ... },
        "skip_paused": false
    }
    """
    try:
        body = request.json or {}
        input_data = body.get("input_data")
        skip_paused = body.get("skip_paused", False)

        manager = get_pause_manager()
        await manager.initialize()

        resume_context = await manager.resume_flow(
            flow_id=flow_id,
            input_data=input_data,
            skip_paused=skip_paused,
        )

        if resume_context is None:
            return sanic_json({
                "error": f"Flow {flow_id} is not paused",
            }, status=404)

        # TODO: 触发实际的恢复执行逻辑
        # 这里需要调用 scheduler 来恢复执行

        return sanic_json({
            "status": "resumed",
            "flow_id": flow_id,
            "resume_context": resume_context,
        })

    except Exception as e:
        logger.exception(f"Error resuming flow {flow_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.get("/<flow_id:str>/pause-state")
async def get_flow_pause_state(request: Request, flow_id: str):
    """获取 Flow 的暂停状态"""
    try:
        manager = get_pause_manager()
        await manager.initialize()

        pause_state = await manager.get_flow_pause_state(flow_id)

        if pause_state is None:
            return sanic_json({
                "paused": False,
                "flow_id": flow_id,
            })

        return sanic_json({
            "paused": True,
            "flow_id": flow_id,
            "pause_state": pause_state,
        })

    except Exception as e:
        logger.exception(f"Error getting pause state for flow {flow_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


# ==================== Component 级别 ====================


@pause_bp.post("/<flow_id:str>/components/<component_id:str>/pause")
async def pause_component(request: Request, flow_id: str, component_id: str):
    """
    暂停某个连通分量

    Request Body (optional):
    {
        "pause_type": "manual" | "error" | "breakpoint",
        "resume_context": { ... }
    }
    """
    try:
        body = request.json or {}
        pause_type_str = body.get("pause_type", "manual")
        resume_context = body.get("resume_context")

        try:
            pause_type = PauseType(pause_type_str)
        except ValueError:
            pause_type = PauseType.MANUAL

        manager = get_pause_manager()
        await manager.initialize()

        pause_data = await manager.pause_component(
            flow_id=flow_id,
            component_id=component_id,
            pause_type=pause_type,
            paused_by="user",
            resume_context=resume_context,
        )

        return sanic_json({
            "status": "paused",
            "flow_id": flow_id,
            "component_id": component_id,
            "pause_data": pause_data,
        })

    except Exception as e:
        logger.exception(
            f"Error pausing component {component_id} in flow {flow_id}: {e}"
        )
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.post("/<flow_id:str>/components/<component_id:str>/resume")
async def resume_component(request: Request, flow_id: str, component_id: str):
    """
    恢复 Component 执行

    Request Body (optional):
    {
        "input_data": { ... },
        "skip_paused": false
    }
    """
    try:
        body = request.json or {}
        input_data = body.get("input_data")
        skip_paused = body.get("skip_paused", False)

        manager = get_pause_manager()
        await manager.initialize()

        resume_context = await manager.resume_component(
            flow_id=flow_id,
            component_id=component_id,
            input_data=input_data,
            skip_paused=skip_paused,
        )

        if resume_context is None:
            return sanic_json({
                "error": f"Component {component_id} in flow {flow_id} is not paused",
            }, status=404)

        return sanic_json({
            "status": "resumed",
            "flow_id": flow_id,
            "component_id": component_id,
            "resume_context": resume_context,
        })

    except Exception as e:
        logger.exception(
            f"Error resuming component {component_id} in flow {flow_id}: {e}"
        )
        return sanic_json({"error": str(e)}, status=500)


# ==================== Node 级别 ====================


@pause_bp.post("/<flow_id:str>/nodes/<node_id:str>/pause")
async def pause_node(request: Request, flow_id: str, node_id: str):
    """
    暂停单个节点

    Request Body:
    {
        "cycle": 1,
        "pause_type": "awaiting_input" | "manual" | "error" | "breakpoint",
        "input_request": { ... },
        "resume_context": { ... }
    }
    """
    try:
        body = request.json or {}
        cycle = body.get("cycle", 0)
        pause_type_str = body.get("pause_type", "awaiting_input")
        input_request = body.get("input_request")
        resume_context = body.get("resume_context")

        try:
            pause_type = PauseType(pause_type_str)
        except ValueError:
            pause_type = PauseType.AWAITING_INPUT

        manager = get_pause_manager()
        await manager.initialize()

        pause_data = await manager.pause_node(
            flow_id=flow_id,
            node_id=node_id,
            cycle=cycle,
            pause_type=pause_type,
            paused_by="node",
            resume_context=resume_context,
            input_request=input_request,
        )

        return sanic_json({
            "status": "paused",
            "flow_id": flow_id,
            "node_id": node_id,
            "pause_data": pause_data,
        })

    except Exception as e:
        logger.exception(f"Error pausing node {node_id} in flow {flow_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.post("/<flow_id:str>/nodes/<node_id:str>/resume")
async def resume_node(request: Request, flow_id: str, node_id: str):
    """
    恢复 Node 执行

    Request Body:
    {
        "input_data": { ... },  // 用户输入数据（如 YesNo 的 decision）
        "skip_node": false       // 是否跳过该节点
    }
    """
    try:
        body = request.json or {}
        input_data = body.get("input_data")
        skip_node = body.get("skip_node", False)

        manager = get_pause_manager()
        await manager.initialize()

        resume_context = await manager.resume_node(
            flow_id=flow_id,
            node_id=node_id,
            input_data=input_data,
            skip_node=skip_node,
        )

        if resume_context is None:
            return sanic_json({
                "error": f"Node {node_id} in flow {flow_id} is not paused",
            }, status=404)

        # TODO: 触发实际的恢复执行逻辑
        # 需要根据 resume_context 决定如何继续执行

        return sanic_json({
            "status": "resumed",
            "flow_id": flow_id,
            "node_id": node_id,
            "resume_context": resume_context,
        })

    except Exception as e:
        logger.exception(f"Error resuming node {node_id} in flow {flow_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.get("/<flow_id:str>/nodes/<node_id:str>/pause-state")
async def get_node_pause_state(request: Request, flow_id: str, node_id: str):
    """获取 Node 的暂停状态"""
    try:
        manager = get_pause_manager()
        await manager.initialize()

        pause_state = await manager.get_node_pause_state(flow_id, node_id)

        if pause_state is None:
            return sanic_json({
                "paused": False,
                "flow_id": flow_id,
                "node_id": node_id,
            })

        return sanic_json({
            "paused": True,
            "flow_id": flow_id,
            "node_id": node_id,
            "pause_state": pause_state,
        })

    except Exception as e:
        logger.exception(
            f"Error getting pause state for node {node_id} in flow {flow_id}: {e}"
        )
        return sanic_json({"error": str(e)}, status=500)


# ==================== 批量查询 ====================


@pause_bp.get("/<flow_id:str>/paused-nodes")
async def get_paused_nodes(request: Request, flow_id: str):
    """获取 Flow 中所有暂停的节点"""
    try:
        manager = get_pause_manager()
        await manager.initialize()

        paused_nodes = await manager.get_all_paused_nodes(flow_id)

        return sanic_json({
            "flow_id": flow_id,
            "paused_nodes": paused_nodes,
            "count": len(paused_nodes),
        })

    except Exception as e:
        logger.exception(f"Error getting paused nodes for flow {flow_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.delete("/<flow_id:str>/pause-states")
async def clear_pause_states(request: Request, flow_id: str):
    """清除 Flow 的所有暂停状态"""
    try:
        manager = get_pause_manager()
        await manager.initialize()

        cleared = await manager.clear_all_pause_states(flow_id)

        return sanic_json({
            "flow_id": flow_id,
            "cleared_count": cleared,
        })

    except Exception as e:
        logger.exception(f"Error clearing pause states for flow {flow_id}: {e}")
        return sanic_json({"error": str(e)}, status=500)


# ==================== 🔥 清理和统计 API ====================


@pause_bp.get("/pause-stats")
async def get_pause_stats(request: Request):
    """获取全局暂停状态统计"""
    try:
        manager = get_pause_manager()
        await manager.initialize()

        stats = await manager.get_pause_stats()

        return sanic_json(stats)

    except Exception as e:
        logger.exception(f"Error getting pause stats: {e}")
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.post("/cleanup/pause-states")
async def cleanup_pause_states(request: Request):
    """
    清理过期的暂停状态
    
    Request Body (optional):
    {
        "max_age_seconds": 604800  // 最大存活时间（秒），默认 7 天
    }
    """
    try:
        body = request.json or {}
        max_age_seconds = body.get("max_age_seconds", 604800)

        manager = get_pause_manager()
        await manager.initialize()

        # 清理过期引用
        ref_result = await manager.cleanup_expired_pause_states()
        
        # 清理过时状态
        stale_result = await manager.cleanup_stale_pause_states(
            max_age_seconds=max_age_seconds
        )

        return sanic_json({
            "cleaned_refs": ref_result.get("cleaned_count", 0),
            "cleaned_stale": stale_result.get("cleaned_count", 0),
            "total_cleaned": (
                ref_result.get("cleaned_count", 0) + 
                stale_result.get("cleaned_count", 0)
            ),
            "success": True,
        })

    except Exception as e:
        logger.exception(f"Error cleaning up pause states: {e}")
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.post("/cleanup/signals")
async def cleanup_signals(request: Request):
    """
    清理过期的 Signal 记录
    
    Request Body (optional):
    {
        "retention_days": 7  // 保留天数，默认 7 天
    }
    """
    try:
        body = request.json or {}
        retention_days = body.get("retention_days", 7)

        from infra.db.services.flow_execution_signal_service import (
            FlowExecutionSignalService,
        )

        service = FlowExecutionSignalService()
        result = await service.cleanup_expired_signals(retention_days=retention_days)

        return sanic_json(result)

    except Exception as e:
        logger.exception(f"Error cleaning up signals: {e}")
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.get("/signal-stats")
async def get_signal_stats(request: Request):
    """
    获取 Signal 统计信息
    
    Query params:
        flow_id: 可选，指定 Flow ID
    """
    try:
        flow_id = request.args.get("flow_id")

        from infra.db.services.flow_execution_signal_service import (
            FlowExecutionSignalService,
        )

        service = FlowExecutionSignalService()
        stats = await service.get_signal_stats(flow_id=flow_id)

        return sanic_json(stats)

    except Exception as e:
        logger.exception(f"Error getting signal stats: {e}")
        return sanic_json({"error": str(e)}, status=500)


@pause_bp.post("/cleanup/all")
async def cleanup_all(request: Request):
    """
    运行所有清理任务
    
    Request Body (optional):
    {
        "signal_retention_days": 7,
        "pause_max_age_seconds": 604800
    }
    """
    try:
        body = request.json or {}
        signal_retention_days = body.get("signal_retention_days", 7)
        pause_max_age_seconds = body.get("pause_max_age_seconds", 604800)

        from tasks.cleanup_tasks import (
            cleanup_expired_signals,
            cleanup_expired_pause_states,
        )

        results = {}

        # 清理 Signal
        results["signals"] = await cleanup_expired_signals(
            retention_days=signal_retention_days
        )

        # 清理暂停状态
        results["pause_states"] = await cleanup_expired_pause_states(
            max_age_seconds=pause_max_age_seconds
        )

        return sanic_json({
            "results": results,
            "success": all(
                r.get("success", False) for r in results.values()
            ),
        })

    except Exception as e:
        logger.exception(f"Error running cleanup tasks: {e}")
        return sanic_json({"error": str(e)}, status=500)
