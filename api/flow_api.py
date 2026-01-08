"""Flow execution and management API"""

import json
import logging
import os
import traceback

import redis.asyncio as aioredis

from flow.scheduler import get_scheduler_instance
from common.interactive_node_manager import get_interactive_manager

from sanic import Blueprint
from sanic.response import json as sanic_json

# 异步 Redis 客户端单例
_async_redis_client = None


async def get_async_redis():
    """获取异步 Redis 客户端"""
    global _async_redis_client
    if _async_redis_client is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _async_redis_client = await aioredis.from_url(redis_url, decode_responses=True)
    return _async_redis_client

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


# ============= Interactive Node API =============

@flow_bp.get("/flows/<flow_id>/interactions")
async def get_pending_interactions(request, flow_id):
    """Get all pending interactions for a flow"""
    try:
        manager = get_interactive_manager()
        await manager.initialize()

        interactions = await manager.get_all_pending_interactions(flow_id)

        return sanic_json({
            "flow_id": flow_id,
            "interactions": interactions,
            "count": len(interactions),
        })
    except Exception as e:
        logger.error("Error getting pending interactions: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.get("/flows/<flow_id>/cycles/<cycle:int>/nodes/<node_id>/interaction")
async def get_node_interaction(request, flow_id, cycle, node_id):
    """Get pending interaction for a specific node"""
    try:
        manager = get_interactive_manager()
        await manager.initialize()

        interaction = await manager.get_pending_interaction(flow_id, cycle, node_id)

        if not interaction:
            return sanic_json({
                "error": "No pending interaction found for this node"
            }, status=404)

        return sanic_json(interaction)
    except Exception as e:
        logger.error("Error getting node interaction: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.post("/flows/<flow_id>/cycles/<cycle:int>/nodes/<node_id>/interaction/respond")
async def submit_interaction_response(request, flow_id, cycle, node_id):
    """Submit user response for an interactive node"""
    try:
        data = request.json or {}

        if "response" not in data:
            return sanic_json({"error": "Missing 'response' field"}, status=400)

        response_data = data["response"]

        # 检查是否是绘图更新 action
        if isinstance(response_data, dict) and response_data.get("action") == "update_drawings":
            # 特殊处理：保存绘图到专用 key
            drawings = response_data.get("drawings", [])
            redis_client = await get_async_redis()
            drawings_key = f"candleline:drawings:{flow_id}:{node_id}"
            await redis_client.set(
                drawings_key,
                json.dumps(drawings),
                ex=86400 * 30,  # 30 天过期
            )
            logger.info(f"Drawings updated for {flow_id}/{node_id}: {len(drawings)} items")
            return sanic_json({
                "success": True,
                "action": "update_drawings",
                "drawings_count": len(drawings),
            })

        # 默认处理：通过 InteractiveManager 处理
        manager = get_interactive_manager()
        await manager.initialize()

        result = await manager.submit_response(
            flow_id=flow_id,
            cycle=cycle,
            node_id=node_id,
            response=response_data,
            user_id=data.get("user_id"),
        )

        if not result.get("success"):
            return sanic_json(result, status=400)

        return sanic_json(result)
    except Exception as e:
        logger.error("Error submitting interaction response: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.post("/flows/<flow_id>/cycles/<cycle:int>/nodes/<node_id>/interaction/cancel")
async def cancel_interaction(request, flow_id, cycle, node_id):
    """Cancel a pending interaction"""
    try:
        data = request.json or {}
        reason = data.get("reason", "cancelled_by_user")

        manager = get_interactive_manager()
        await manager.initialize()

        success = await manager.cancel_interaction(
            flow_id=flow_id,
            cycle=cycle,
            node_id=node_id,
            reason=reason,
        )

        if not success:
            return sanic_json({
                "error": "Interaction not found or already processed"
            }, status=404)

        return sanic_json({
            "success": True,
            "message": "Interaction cancelled",
        })
    except Exception as e:
        logger.error("Error cancelling interaction: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.post("/flows/<flow_id>/cycles/<cycle:int>/nodes/<node_id>/interaction/request")
async def request_interaction(request, flow_id, cycle, node_id):
    """
    Request user interaction (called by node execution)
    
    This endpoint is typically called internally by Interactive Nodes
    when they need user input.
    """
    try:
        data = request.json or {}

        required_fields = ["node_type", "interaction_type", "prompt"]
        for field in required_fields:
            if field not in data:
                return sanic_json({"error": f"Missing required field: {field}"}, status=400)

        manager = get_interactive_manager()
        await manager.initialize()

        result = await manager.request_interaction(
            flow_id=flow_id,
            cycle=cycle,
            node_id=node_id,
            node_type=data["node_type"],
            interaction_type=data["interaction_type"],
            prompt=data["prompt"],
            context_data=data.get("context_data"),
            timeout_seconds=data.get("timeout_seconds", 300),
            timeout_action=data.get("timeout_action", "reject"),
            options=data.get("options"),
        )

        return sanic_json(result, status=201)
    except Exception as e:
        logger.error("Error requesting interaction: %s", str(e))
        return sanic_json({"error": str(e)}, status=500)


# ============= Run Upstream Nodes API =============

@flow_bp.post("/flows/<flow_id>/run-upstream/<target_node_id>")
async def run_upstream_nodes(request, flow_id, target_node_id):
    """
    Run all upstream dependency nodes for a specified target node.
    
    This is used when opening an Interactive Node Modal to fetch data
    from upstream nodes (e.g., PriceNode -> CandlelineNode).
    
    The function:
    1. Builds a dependency graph from the flow edges
    2. Finds all upstream nodes using reverse BFS
    3. Executes nodes in topological order
    4. Returns the execution status
    """
    try:
        data = request.json or {}
        nodes = data.get("nodes", [])
        edges = data.get("edges", [])
        user_id = data.get("user_id")
        
        logger.info("run_upstream_nodes called: flow_id=%s, target_node=%s, nodes=%d, edges=%d",
                    flow_id, target_node_id, len(nodes) if nodes else 0, len(edges) if edges else 0)
        
        if not nodes:
            return sanic_json({
                "error": "Missing nodes in request body"
            }, status=400)
        
        # edges 可以为空（单个节点的情况）
        if edges is None:
            edges = []
        
        # Build dependency graph (reverse: target -> sources)
        # edge format: { source: nodeId, sourceHandle: handleId, target: nodeId, targetHandle: handleId }
        upstream_graph = {}  # nodeId -> list of upstream nodeIds
        for edge in edges:
            target = edge.get("target")
            source = edge.get("source")
            if target and source:
                if target not in upstream_graph:
                    upstream_graph[target] = []
                upstream_graph[target].append(source)
        
        # Find all upstream nodes using BFS
        upstream_nodes = set()
        queue = [target_node_id]
        visited = set()
        
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            
            # Add upstream dependencies
            for upstream in upstream_graph.get(current, []):
                if upstream not in visited:
                    upstream_nodes.add(upstream)
                    queue.append(upstream)
        
        # If no upstream nodes, return immediately
        if not upstream_nodes:
            return sanic_json({
                "success": True,
                "message": "No upstream nodes to run",
                "upstream_nodes": [],
                "execution_id": None
            })
        
        # Filter nodes to only include upstream ones
        upstream_node_list = [n for n in nodes if n.get("id") in upstream_nodes]
        
        # Get the scheduler and run these nodes
        scheduler = get_scheduler_instance()
        
        if not scheduler.redis:
            await scheduler.initialize()
        
        # Build a mini flow config with only upstream nodes
        # We need to filter edges to only include those between upstream nodes
        upstream_edges = [
            e for e in edges 
            if e.get("source") in upstream_nodes and e.get("target") in upstream_nodes
        ]
        # Also include edges that connect upstream nodes to the target node
        target_edges = [
            e for e in edges
            if e.get("source") in upstream_nodes and e.get("target") == target_node_id
        ]
        
        # Include target node in the execution so it can receive upstream data
        target_node = next((n for n in nodes if n.get("id") == target_node_id), None)
        nodes_to_run = upstream_node_list
        if target_node:
            nodes_to_run = nodes_to_run + [target_node]
        
        flow_config = {
            "interval": "once",  # Run only once
            "nodes": nodes_to_run,
            "edges": upstream_edges + target_edges,
        }
        
        logger.info(
            "Running upstream nodes for %s in flow %s: %s (using original flow_id for WebSocket)",
            target_node_id, flow_id, list(upstream_nodes)
        )
        
        # Execute using original flow_id - this ensures WebSocket updates go to the channel
        # that frontend is already subscribed to
        try:
            # Register with original flow_id (will update existing registration)
            await scheduler.register_flow(flow_id, flow_config, user_id=user_id)
            
            # Execute a single cycle
            result = await scheduler.execute_cycle(flow_id)
            
            return sanic_json({
                "success": True,
                "message": f"Started execution of {len(upstream_nodes)} upstream nodes",
                "upstream_nodes": list(upstream_nodes),
                "execution_id": flow_id,
                "flow_id": flow_id,
                "result": result
            })
            
        except Exception as exec_error:
            logger.error("Failed to execute upstream nodes: %s", str(exec_error))
            return sanic_json({
                "success": False,
                "error": str(exec_error)
            }, status=500)
            
    except Exception as e:
        logger.error("Error running upstream nodes: %s\n%s", str(e), traceback.format_exc())
        return sanic_json({"error": str(e)}, status=500)


@flow_bp.post("/flows/<flow_id>/run-connected/<node_id>")
async def run_connected_component(request, flow_id, node_id):
    """
    Run all nodes in the connected component containing the specified node.
    
    This finds the complete connected subgraph (both upstream and downstream)
    and executes all nodes in topological order. Status updates are pushed
    via WebSocket in real-time using the existing execution infrastructure.
    """
    try:
        data = request.json or {}
        nodes = data.get("nodes", [])
        edges = data.get("edges", [])
        user_id = data.get("user_id")
        
        logger.info("run_connected_component called: flow_id=%s, node_id=%s, nodes=%d, edges=%d",
                    flow_id, node_id, len(nodes) if nodes else 0, len(edges) if edges else 0)
        
        if not nodes:
            return sanic_json({
                "error": "Missing nodes in request body"
            }, status=400)
        
        # edges 可以为空（单个节点的情况）
        if edges is None:
            edges = []
        
        # Build adjacency lists for both directions (undirected graph for connectivity)
        adjacency = {}  # nodeId -> set of connected nodeIds
        for node in nodes:
            node_id_key = node.get("id")
            if node_id_key:
                adjacency[node_id_key] = set()
        
        for edge in edges:
            source = edge.get("source")
            target = edge.get("target")
            if source and target:
                if source not in adjacency:
                    adjacency[source] = set()
                if target not in adjacency:
                    adjacency[target] = set()
                adjacency[source].add(target)
                adjacency[target].add(source)
        
        # Find all nodes in the connected component using BFS
        connected_nodes = set()
        queue = [node_id]
        visited = set()
        
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            connected_nodes.add(current)
            
            # Add all connected neighbors
            for neighbor in adjacency.get(current, []):
                if neighbor not in visited:
                    queue.append(neighbor)
        
        # If no connected nodes found (node not in graph), return error
        if not connected_nodes:
            return sanic_json({
                "error": f"Node {node_id} not found in flow graph"
            }, status=404)
        
        # Filter nodes and edges for the connected component
        component_nodes = [n for n in nodes if n.get("id") in connected_nodes]
        component_edges = [
            e for e in edges 
            if e.get("source") in connected_nodes and e.get("target") in connected_nodes
        ]
        
        logger.info(
            "Running connected component for node %s in flow %s: %d nodes",
            node_id, flow_id, len(connected_nodes)
        )
        
        # Get the scheduler
        scheduler = get_scheduler_instance()
        
        if not scheduler.redis:
            await scheduler.initialize()
        
        # Build flow config for the connected component
        # Use original flow_id so status updates go to the correct WebSocket channel
        flow_config = {
            "interval": "once",  # Run only once
            "nodes": component_nodes,
            "edges": component_edges,
        }
        
        # Execute directly using the original flow_id
        # This ensures WebSocket updates go to the channel frontend is subscribed to
        try:
            # Temporarily register this flow configuration
            # If flow already exists, this will update it for this execution
            await scheduler.register_flow(flow_id, flow_config, user_id=user_id)
            
            # Execute a single cycle - this will push status via existing WebSocket
            result = await scheduler.execute_cycle(flow_id)
            
            return sanic_json({
                "success": True,
                "message": f"Started execution of connected component with {len(connected_nodes)} nodes",
                "connected_nodes": list(connected_nodes),
                "flow_id": flow_id,
                "result": result
            })
            
        except Exception as exec_error:
            logger.error("Failed to start connected component execution: %s", str(exec_error))
            return sanic_json({
                "error": f"Failed to start execution: {str(exec_error)}"
            }, status=500)
            
    except Exception as e:
        logger.error("Error running connected component: %s\n%s", str(e), traceback.format_exc())
        return sanic_json({"error": str(e)}, status=500)
