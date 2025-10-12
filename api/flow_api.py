"""flow执行和管理API"""

import logging
import traceback

from flow.scheduler import get_scheduler_instance

from sanic import Blueprint
from sanic.response import json as sanic_json

logger = logging.getLogger(__name__)

flow_bp = Blueprint("flow_api")


@flow_bp.post("/flows/execute")
async def execute_flow(request):
    """执行Flow的接口"""
    try:
        flow_data = request.json

        # 必要参数检查
        required_fields = ["flow_id", "flow_json", "cycle_interval"]
        for field in required_fields:
            if field not in flow_data:
                return sanic_json(
                    {"error": f"Missing required field: {field}"}, status=400
                )
        flow_id = flow_data["flow_id"]
        cycle_interval = flow_data["cycle_interval"]
        flow_json = flow_data["flow_json"]
        user_id = flow_data.get("user_id")  # Extract user_id for Quest tracking (optional)

        # 检查Flow JSON格式
        if not isinstance(flow_json, dict):
            return sanic_json({"error": "Invalid flow_json format"}, status=400)

        # 获取调度器实例
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        # 构建Flow配置
        flow_config = {
            "interval": cycle_interval,  # 使用客户端提供的周期间隔
            "nodes": flow_json.get("nodes", []),
            "edges": flow_json.get("edges", []),
        }

        # 注册Flow到调度系统
        try:
            # 先检查Flow是否已存在
            flow_exists = await scheduler.redis.exists(f"flow:{flow_id}")

            if flow_exists:
                # 如果已存在，更新配置
                await scheduler.stop_flow(flow_id)  # 先停止已有的Flow
                flow_data = await scheduler.register_flow(flow_id, flow_config, user_id=user_id)
                message = f"Flow {flow_id} updated and registered"
            else:
                # 如果不存在，新建Flow
                flow_data = await scheduler.register_flow(flow_id, flow_config, user_id=user_id)
                message = f"Flow {flow_id} registered"

        except ValueError as e:
            return sanic_json({"error": str(e)}, status=400)

        # 启动Flow调度
        try:
            result = await scheduler.start_flow(flow_id)
            result["message"] = message

            return sanic_json(result)

        except ValueError as e:
            return sanic_json({"error": str(e)}, status=400)
        except Exception as e:
            logger.error(traceback.format_exc())
            return sanic_json({"error": f"Error starting flow: {str(e)}"}, status=500)

    except Exception as e:
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.get("/flows/<flow_id>/status")
async def get_flow_status(request, flow_id):
    """获取Flow状态的接口"""
    try:
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        status = await scheduler.get_flow_status(flow_id)
        return sanic_json(status)
    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.get("/flows/<flow_id>/comprehensive-status")
async def get_comprehensive_flow_status(request, flow_id):
    """获取Flow综合状态的接口，包含所有节点状态、日志和信号"""
    try:
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        # 获取可选的cycle参数
        cycle = request.args.get("cycle")
        if cycle is not None:
            try:
                cycle = int(cycle)
            except ValueError:
                return sanic_json({"error": "Invalid cycle parameter"}, status=400)

        status = await scheduler.get_comprehensive_flow_status(flow_id, cycle)
        return sanic_json(status)
    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        logger.error("Error getting comprehensive flow status: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)

@flow_bp.post("/flows/<flow_id>/stop")
async def stop_flow(request, flow_id):
    """停止Flow的接口"""
    try:
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        result = await scheduler.stop_flow(flow_id)
        return sanic_json(result)
    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.post("/flows/<flow_id>/cycles")
async def execute_flow_cycle(request, flow_id):
    """手动触发执行一个Flow周期的接口"""
    try:
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        # 检查是否指定了cycle
        cycle = None
        if request.json and "cycle" in request.json:
            cycle = int(request.json["cycle"])

        result = await scheduler.execute_cycle(flow_id, cycle)
        return sanic_json(result)
    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.get("/flows/<flow_id>/cycles/<cycle:int>")
async def get_cycle_status(request, flow_id, cycle):
    """获取特定Flow周期状态的接口"""
    try:
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        status = await scheduler.get_cycle_status(flow_id, cycle)
        return sanic_json(status)
    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.post("/flows/<flow_id>/cycles/<cycle:int>/components/<component_id>/stop")
async def stop_component(request, flow_id, cycle, component_id):
    """停止特定组件执行的接口"""
    try:
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        result = await scheduler.stop_component(flow_id, cycle, component_id)
        return sanic_json(result)
    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.get("/flows/<flow_id>/logs")
async def get_flow_execution_logs(request, flow_id):
    """获取Flow执行日志列表的接口"""
    try:
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        # 获取查询参数
        cycle = request.args.get("cycle")
        if cycle is not None:
            try:
                cycle = int(cycle)
            except ValueError:
                return sanic_json({"error": "Invalid cycle parameter"}, status=400)

        node_id = request.args.get("node_id")
        log_level = request.args.get("log_level")
        log_source = request.args.get("log_source")

        # 分页参数
        try:
            limit = int(request.args.get("limit", 100))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            return sanic_json({"error": "Invalid pagination parameters"}, status=400)

        # 排序参数
        order_by = request.args.get("order_by", "created_at")
        order_direction = request.args.get("order_direction", "desc")

        # 验证排序参数
        valid_order_fields = ["created_at", "log_level", "cycle", "node_id"]
        if order_by not in valid_order_fields:
            return sanic_json(
                {"error": f"Invalid order_by field. Valid options: {valid_order_fields}"},
                status=400,
            )

        if order_direction not in ["asc", "desc"]:
            return sanic_json(
                {"error": "Invalid order_direction. Valid options: asc, desc"},
                status=400,
            )

        # 获取日志
        result = await scheduler.get_flow_execution_logs(
            flow_id=flow_id,
            cycle=cycle,
            node_id=node_id,
            log_level=log_level,
            log_source=log_source,
            limit=limit,
            offset=offset,
            order_by=order_by,
            order_direction=order_direction,
        )

        return sanic_json(result)

    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        logger.error("Error getting flow execution logs: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.get("/flows/logs/<log_id:int>")
async def get_flow_execution_log_detail(request, log_id):
    """获取Flow执行日志详情的接口"""
    try:
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        result = await scheduler.get_flow_execution_log_detail(log_id)
        return sanic_json(result)

    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        logger.error("Error getting flow execution log detail: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.get("/flows/<flow_id>/cycles/<cycle:int>/nodes/<node_id>/logs")
async def get_flow_cycle_node_logs(request, flow_id, cycle, node_id):
    """获取特定Flow、Cycle和Node的日志接口"""
    try:
        scheduler = get_scheduler_instance()

        # 确保调度器已初始化
        if not scheduler.redis:
            await scheduler.initialize()

        # 处理特殊的 node_id 值
        if node_id.lower() == "system":
            node_id = None  # None 表示系统日志

        # 获取查询参数
        log_level = request.args.get("log_level")
        log_source = request.args.get("log_source")

        # 分页参数
        try:
            limit = int(request.args.get("limit", 100))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            return sanic_json({"error": "Invalid pagination parameters"}, status=400)

        # 排序参数
        order_by = request.args.get("order_by", "created_at")
        order_direction = request.args.get("order_direction", "desc")

        # 验证排序参数
        valid_order_fields = ["created_at", "log_level", "log_source"]
        if order_by not in valid_order_fields:
            return sanic_json(
                {"error": f"Invalid order_by field. Valid options: {valid_order_fields}"},
                status=400,
            )

        if order_direction not in ["asc", "desc"]:
            return sanic_json(
                {"error": "Invalid order_direction. Valid options: asc, desc"},
                status=400,
            )

        # 获取日志
        result = await scheduler.get_flow_cycle_node_logs(
            flow_id=flow_id,
            cycle=cycle,
            node_id=node_id,
            log_level=log_level,
            log_source=log_source,
            limit=limit,
            offset=offset,
            order_by=order_by,
            order_direction=order_direction,
        )

        return sanic_json(result)

    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        logger.error("Error getting flow cycle node logs: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)
