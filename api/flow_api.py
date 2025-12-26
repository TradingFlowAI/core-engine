"""Flow execution and management API"""

import logging
import traceback

from flow.scheduler import get_scheduler_instance

from sanic import Blueprint
from sanic.response import json as sanic_json

logger = logging.getLogger(__name__)

flow_bp = Blueprint("flow_api")


@flow_bp.post("/flows/execute")
async def execute_flow(request):
    """Execute Flow endpoint"""
    try:
        flow_data = request.json

        # Required parameters check
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

        # Check Flow JSON format
        if not isinstance(flow_json, dict):
            return sanic_json({"error": "Invalid flow_json format"}, status=400)

        # Get scheduler instance
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
        if not scheduler.redis:
            await scheduler.initialize()

        # Build Flow configuration
        flow_config = {
            "interval": cycle_interval,  # Use client-provided cycle interval
            "nodes": flow_json.get("nodes", []),
            "edges": flow_json.get("edges", []),
        }

        # Register Flow to scheduling system
        try:
            # First check if Flow already exists
            flow_exists = await scheduler.redis.exists(f"flow:{flow_id}")

            if flow_exists:
                # If exists, update configuration
                await scheduler.stop_flow(flow_id)  # Stop existing Flow first
                flow_data = await scheduler.register_flow(flow_id, flow_config, user_id=user_id)
                message = f"Flow {flow_id} updated and registered"
            else:
                # If not exists, create new Flow
                flow_data = await scheduler.register_flow(flow_id, flow_config, user_id=user_id)
                message = f"Flow {flow_id} registered"

        except ValueError as e:
            return sanic_json({"error": str(e)}, status=400)

        # Start Flow scheduling
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
    """Get Flow status endpoint"""
    try:
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
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
    """
    Get comprehensive Flow status endpoint, including all node states, logs and signals
    
    Query params:
        cycle: Optional cycle number
        include_node_logs: Whether to include logs per node (default: true, set to 'false' to reduce payload)
        include_node_signals: Whether to include signals per node (default: true)
    """
    try:
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
        if not scheduler.redis:
            await scheduler.initialize()

        # Get optional cycle parameter
        cycle = request.args.get("cycle")
        if cycle is not None:
            try:
                cycle = int(cycle)
            except ValueError:
                return sanic_json({"error": "Invalid cycle parameter"}, status=400)

        # Get optional include_node_logs parameter (default true)
        include_node_logs = request.args.get("include_node_logs", "true").lower() != "false"
        # Get optional include_node_signals parameter (default true)
        include_node_signals = request.args.get("include_node_signals", "true").lower() != "false"

        status = await scheduler.get_comprehensive_flow_status(
            flow_id,
            cycle,
            include_node_logs=include_node_logs,
            include_node_signals=include_node_signals
        )
        return sanic_json(status)
    except ValueError as e:
        return sanic_json({"error": str(e)}, status=404)
    except Exception as e:
        logger.error("Error getting comprehensive flow status: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)

@flow_bp.post("/flows/<flow_id>/stop")
async def stop_flow(request, flow_id):
    """Stop Flow endpoint"""
    try:
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
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
    """Manually trigger a Flow cycle execution endpoint"""
    try:
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
        if not scheduler.redis:
            await scheduler.initialize()

        # Check if cycle is specified
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
    """Get specific Flow cycle status endpoint"""
    try:
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
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
    """Stop specific component execution endpoint"""
    try:
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
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
    """Get Flow execution logs list endpoint"""
    try:
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
        if not scheduler.redis:
            await scheduler.initialize()

        # Get query parameters
        cycle = request.args.get("cycle")
        if cycle is not None:
            try:
                cycle = int(cycle)
            except ValueError:
                return sanic_json({"error": "Invalid cycle parameter"}, status=400)

        node_id = request.args.get("node_id")
        log_level = request.args.get("log_level")
        log_source = request.args.get("log_source")

        # Pagination parameters
        try:
            limit = int(request.args.get("limit", 100))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            return sanic_json({"error": "Invalid pagination parameters"}, status=400)

        # Sorting parameters
        order_by = request.args.get("order_by", "created_at")
        order_direction = request.args.get("order_direction", "desc")

        # Validate sorting parameters
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

        # Get logs
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
    """Get Flow execution log detail endpoint"""
    try:
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
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
    """Get logs for specific Flow, Cycle and Node endpoint"""
    try:
        scheduler = get_scheduler_instance()

        # Ensure scheduler is initialized
        if not scheduler.redis:
            await scheduler.initialize()

        # Handle special node_id value
        if node_id.lower() == "system":
            node_id = None  # None means system logs

        # Get query parameters
        log_level = request.args.get("log_level")
        log_source = request.args.get("log_source")

        # Pagination parameters
        try:
            limit = int(request.args.get("limit", 100))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            return sanic_json({"error": "Invalid pagination parameters"}, status=400)

        # Sorting parameters
        order_by = request.args.get("order_by", "created_at")
        order_direction = request.args.get("order_direction", "desc")

        # Validate sorting parameters
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

        # Get logs
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
