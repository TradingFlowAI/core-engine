import asyncio
import json
import logging
import random
import time
import traceback
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import httpx
import redis.asyncio as aioredis

from weather_depot.config import CONFIG
from common.node_registry import NodeRegistry
from common.node_task_manager import NodeTaskManager
from mq.activity_publisher import publish_activity

logger = logging.getLogger(__name__)


class FlowScheduler:
    """
    Flow Scheduler: Responsible for managing periodic flow execution and status tracking

    Main features:
    1. Periodic scheduling of flow execution
    2. Managing execution state of each node in the flow
    3. Support for multi-instance deployment (Redis state store)
    4. Support for flow execution control (pause/resume/stop)
    """

    def __init__(self):
        # Initialize Redis connection pool
        self.redis = None
        self.running_flows = set()  # Track currently scheduled flow_ids

        # Initialize flow execution log service
        self._log_service = None

        # Cycle monitoring configuration
        self._cycle_monitor_tasks: Dict[str, asyncio.Task] = {}
        self.monitor_poll_interval = self._parse_int_config(
            CONFIG.get("FLOW_MONITOR_POLL_INTERVAL", 3), default=3
        )
        self.monitor_max_wait_seconds = self._parse_int_config(
            CONFIG.get("FLOW_MONITOR_MAX_WAIT_SECONDS", 300), default=300
        )

    async def initialize(self):
        """Initialize scheduler, connect to Redis"""
        # Connect to Redis
        redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
        self.redis = await aioredis.from_url(redis_url, decode_responses=True)
        logger.info("Flow scheduler initialized, Redis connected to %s", redis_url)

        # Initialize log service
        await self._initialize_log_service()

        # Attempt to recover flows in case of ungraceful shutdown
        await self._recover_flows_on_start()

    async def shutdown(self):
        """Shutdown scheduler, release resources"""
        if self.redis:
            await self.redis.close()
        logger.info("Flow scheduler has been shut down")

    async def _initialize_log_service(self):
        """Initialize flow execution log service lazily"""
        if self._log_service is None:
            try:
                from weather_depot.db.services.flow_execution_log_service import (
                    FlowExecutionLogService,
                )
                self._log_service = FlowExecutionLogService()
            except Exception as e:
                logger.warning("Failed to initialize log service: %s", str(e))

    async def persist_log(
        self,
        flow_id: str,
        cycle: int,
        message: str,
        log_level: str = "INFO",
        log_source: str = "scheduler",
        node_id: Optional[str] = None,
        log_metadata: Optional[Dict] = None,
    ):
        """
        Persist log message to database and also log to console

        Args:
            flow_id: Flow ID
            cycle: Current cycle number
            message: Log message content
            log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_source: Log source (scheduler, node, system, user)
            node_id: Optional node ID if log is related to specific node
            log_metadata: Additional structured metadata
        """
        # Log to console first
        log_level_upper = log_level.upper()
        if log_level_upper == "DEBUG":
            logger.debug(message)
        elif log_level_upper == "INFO":
            logger.info(message)
        elif log_level_upper == "WARNING":
            logger.warning(message)
        elif log_level_upper == "ERROR":
            logger.error(message)
        elif log_level_upper == "CRITICAL":
            logger.critical(message)
        else:
            logger.info(message)

        # Persist to database
        try:
            await self._initialize_log_service()
            if self._log_service:
                await self._log_service.create_log(
                    flow_id=flow_id,
                    cycle=cycle,
                    message=message,
                    node_id=node_id,
                    log_level=log_level_upper,
                    log_source=log_source,
                    log_metadata=log_metadata,
                )
        except Exception as e:
            # Don't let logging errors break scheduler execution
            logger.warning("Failed to persist log to database: %s", str(e))

        # Send log to WebSocket (via Redis Pub/Sub)
        # Use the async redis_log_publisher for consistency with NodeBase
        try:
            from core.redis_log_publisher_async import publish_log_async

            log_entry = {
                "node_id": node_id,
                "level": log_level_upper.lower(),
                "message": message,
                "log_source": log_source,
                "metadata": log_metadata,
            }

            # Publish to Redis asynchronously (with automatic retry)
            await publish_log_async(flow_id, cycle, log_entry, max_retries=3)
        except Exception as e:
            # Don't let Redis publish errors break scheduler execution
            logger.warning(
                "Failed to publish log to Redis for flow %s cycle %s: %s",
                flow_id, cycle, str(e)
            )

    async def cleanup_old_logs(self, flow_id: str, current_cycle: int, keep_cycles: int = 5):
        """
        Clean up logs older than specified number of cycles

        Args:
            flow_id: Flow ID
            current_cycle: Current cycle number
            keep_cycles: Number of recent cycles to keep (default: 5)
        """
        try:
            await self._initialize_log_service()
            if self._log_service:
                # Calculate the oldest cycle to keep
                oldest_cycle_to_keep = max(0, current_cycle - keep_cycles + 1)

                # Delete logs older than the oldest cycle to keep
                deleted_count = await self._log_service.delete_logs_before_cycle(
                    flow_id=flow_id,
                    cycle=oldest_cycle_to_keep
                )

                if deleted_count > 0:
                    await self.persist_log(
                        flow_id=flow_id,
                        cycle=current_cycle,
                        message=f"Cleaned up {deleted_count} old log entries (keeping last {keep_cycles} cycles)",
                        log_level="INFO",
                        log_source="scheduler"
                    )
        except Exception as e:
            logger.warning("Failed to cleanup old logs for flow %s: %s", flow_id, str(e))


    async def _recover_flows_on_start(self) -> None:
        """Recover flows that might have been left in running state after restart."""
        if not self.redis:
            return

        try:
            flow_ids = await self._list_registered_flow_ids()
            if not flow_ids:
                return

            node_manager = NodeTaskManager.get_instance()
            await node_manager.initialize()

            for flow_id in flow_ids:
                try:
                    await self._recover_flow(flow_id, node_manager)
                except Exception as exc:
                    logger.warning(
                        "Failed to recover flow %s: %s",
                        flow_id,
                        exc,
                    )
        except Exception as exc:
            logger.warning("Flow recovery skipped due to error: %s", exc)

    async def _list_registered_flow_ids(self) -> List[str]:
        """Enumerate all registered flows (excluding cycle/meta keys)."""
        if not self.redis:
            return []

        flow_ids: Set[str] = set()
        async for key in self.redis.scan_iter(match="flow:*"):
            if not key.startswith("flow:"):
                continue
            suffix = key.split("flow:", 1)[1]
            if ":" in suffix:
                continue
            flow_ids.add(suffix)
        return sorted(flow_ids)

    async def _recover_flow(
        self,
        flow_id: str,
        node_manager: NodeTaskManager,
    ) -> None:
        """Recover a single flow based on its configuration/state."""
        flow_key = f"flow:{flow_id}"
        flow_data = await self.redis.hgetall(flow_key)
        if not flow_data:
            return

        try:
            flow_config = json.loads(flow_data.get("config", "{}"))
        except Exception:
            flow_config = {}

        interval_seconds = self._parse_interval(flow_config.get("interval", "0"))
        flow_status = flow_data.get("status", "registered")

        if interval_seconds == 0:
            await self._handle_stale_run_once_flow(flow_id, flow_data)
            return

        if flow_status in {"running", "monitoring"}:
            await self._resume_flow_scheduler(flow_id)

        last_cycle = int(flow_data.get("last_cycle", -1))
        if last_cycle < 0 or interval_seconds <= 0:
            return

        await self._maybe_trigger_flow_catchup(
            flow_id=flow_id,
            flow_data=flow_data,
            last_cycle=last_cycle,
            interval_seconds=interval_seconds,
            node_manager=node_manager,
        )

    async def _handle_stale_run_once_flow(self, flow_id: str, flow_data: Dict[str, Any]) -> None:
        """Mark orphaned run-once flows as expired."""
        status = flow_data.get("status", "registered")
        if status not in {"running", "monitoring"}:
            return

        await self.redis.hset(
            f"flow:{flow_id}",
            mapping={
                "status": "expired",
                "last_cycle_status": "expired",
                "last_cycle_completed_at": datetime.now().isoformat(),
            },
        )
        await self.persist_log(
            flow_id=flow_id,
            cycle=int(flow_data.get("last_cycle", -1)),
            message="Detected stale run-once flow after restart, marking as expired.",
            log_level="WARNING",
            log_source="scheduler",
        )

    async def _resume_flow_scheduler(self, flow_id: str) -> None:
        """Resume scheduling loop for flows that were running before restart."""
        if flow_id in self.running_flows:
            return

        self.running_flows.add(flow_id)
        asyncio.create_task(self._schedule_flow_execution(flow_id))
        logger.info("Resumed scheduling loop for flow %s", flow_id)

    async def _maybe_trigger_flow_catchup(
        self,
        flow_id: str,
        flow_data: Dict[str, Any],
        last_cycle: int,
        interval_seconds: int,
        node_manager: NodeTaskManager,
    ) -> None:
        """Trigger a catch-up cycle when downtime exceeded allowed threshold."""
        flow_structure_raw = flow_data.get("structure", "{}")
        try:
            flow_structure = json.loads(flow_structure_raw)
        except Exception:
            flow_structure = {}

        node_map = flow_structure.get("node_map") or {}
        expected_node_ids = list(node_map.keys())
        if not expected_node_ids:
            return

        last_activity = await self._determine_last_activity_timestamp(flow_id, last_cycle, flow_data)
        if not last_activity:
            return

        now_utc = datetime.now(timezone.utc)
        elapsed_seconds = (now_utc - last_activity).total_seconds()
        if elapsed_seconds <= interval_seconds * 2:
            return

        statuses = await self._collect_node_statuses(
            node_manager,
            flow_id=flow_id,
            cycle=last_cycle,
            expected_node_ids=expected_node_ids,
        )

        if any(status in {"pending", "running"} for status in statuses.values()):
            await self.persist_log(
                flow_id=flow_id,
                cycle=last_cycle,
                message=(
                    "Flow recovery check detected pending/running nodes, "
                    "skipping automatic catch-up."
                ),
                log_level="INFO",
                log_source="scheduler",
            )
            return

        await self.persist_log(
            flow_id=flow_id,
            cycle=last_cycle,
            message=(
                f"No activity detected for {int(elapsed_seconds)}s "
                f"(>2x interval). Triggering catch-up cycle."
            ),
            log_level="WARNING",
            log_source="scheduler",
        )

        asyncio.create_task(self._trigger_catchup_cycle(flow_id))

    async def _trigger_catchup_cycle(self, flow_id: str) -> None:
        """Fire a new cycle asynchronously for catch-up purposes."""
        try:
            await self.execute_cycle(flow_id)
            await self.persist_log(
                flow_id=flow_id,
                cycle=-1,
                message="Catch-up cycle dispatched successfully after downtime.",
                log_level="INFO",
                log_source="scheduler",
            )
        except Exception as exc:
            await self.persist_log(
                flow_id=flow_id,
                cycle=-1,
                message=f"Failed to dispatch catch-up cycle: {exc}",
                log_level="ERROR",
                log_source="scheduler",
            )

    async def _determine_last_activity_timestamp(
        self,
        flow_id: str,
        last_cycle: int,
        flow_data: Dict[str, Any],
    ) -> Optional[datetime]:
        """Determine the last time the flow executed a cycle."""
        candidates = [flow_data.get("last_cycle_completed_at")]

        if self.redis:
            cycle_key = f"flow:{flow_id}:cycle:{last_cycle}"
            cycle_data = await self.redis.hgetall(cycle_key)
        else:
            cycle_data = {}

        if cycle_data:
            candidates.append(cycle_data.get("end_time"))
            candidates.append(cycle_data.get("start_time"))

        for candidate in candidates:
            dt = self._parse_iso_datetime(candidate)
            if dt:
                return dt
        return None

    @staticmethod
    def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
        """Parse ISO timestamps (with or without timezone) into aware datetime."""
        if not value:
            return None
        try:
            normalized = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    async def register_flow(self, flow_id: str, flow_config: Dict, user_id: Optional[str] = None):
        """
        Register a new Flow to the scheduling system

        Args:
            flow_id: Flow unique identifier
            flow_config: Flow configuration, including interval, nodes, edges, etc.
            user_id: User ID for Quest activity tracking (optional)
        """
        # Basic parameter checks
        if not flow_config.get("interval"):
            raise ValueError("Flow configuration missing required parameter: interval")
        if not flow_config.get("nodes"):
            raise ValueError("Flow configuration missing required parameter: nodes")

        # Ensure edges key exists, initialize as empty list if not present
        if "edges" not in flow_config:
            flow_config["edges"] = []

        # Analyze flow structure, identify DAG components
        flow_structure = self._analyze_flow_structure(flow_config)
        # logger.debug(
        #     "Flow structure analysis completed: %s",
        #     json.dumps(flow_structure, indent=2),
        # )

        # Check if flow already exists to preserve last_cycle
        existing_flow = await self.redis.hgetall(f"flow:{flow_id}")
        existing_last_cycle = int(existing_flow.get("last_cycle", -1)) if existing_flow else -1

        # Store flow information in Redis
        flow_data = {
            "id": flow_id,
            "config": json.dumps(flow_config),
            "structure": json.dumps(flow_structure),
            "status": "registered",
            "last_cycle": existing_last_cycle,  # Preserve existing cycle count
            "next_execution": 0,  # Timestamp, 0 means execute immediately
            "created_at": existing_flow.get("created_at") if existing_flow else datetime.now().isoformat(),
        }

        # Add user_id for Quest tracking if provided
        if user_id:
            flow_data["user_id"] = user_id
            logger.info("Flow %s registered with user_id %s for Quest tracking", flow_id, user_id)

        await self.redis.hset(f"flow:{flow_id}", mapping=flow_data)
        logger.info("Flow %s has been registered to the scheduling system", flow_id)

        return flow_data

    async def start_flow(self, flow_id: str):
        """Start periodic scheduling of the flow"""
        # Check if flow exists
        flow_exists = await self.redis.exists(f"flow:{flow_id}")
        if not flow_exists:
            raise ValueError(f"Flow {flow_id} does not exist")

        # Update flow status
        await self.redis.hset(f"flow:{flow_id}", "status", "running")

        # If the flow is not already being scheduled in this instance, start the scheduling task
        if flow_id not in self.running_flows:
            self.running_flows.add(flow_id)
            asyncio.create_task(self._schedule_flow_execution(flow_id))

        logger.info("Flow %s scheduling has been started", flow_id)
        return {"status": "running", "flow_id": flow_id}

    async def stop_flow(self, flow_id: str):
        """Stop periodic scheduling of the flow"""
        # Check if flow exists
        flow_exists = await self.redis.exists(f"flow:{flow_id}")
        if not flow_exists:
            raise ValueError(f"Flow {flow_id} does not exist")

        # Update flow status
        await self.redis.hset(f"flow:{flow_id}", "status", "stopped")

        # 🛑 Update all pending/running nodes to terminated status
        # Get the latest cycle
        flow_data = await self.redis.hgetall(f"flow:{flow_id}")
        last_cycle = int(flow_data.get("last_cycle", -1))

        if last_cycle >= 0:
            # Get node task manager
            node_manager = NodeTaskManager.get_instance()
            await node_manager.initialize()

            # Get all nodes in the latest cycle
            comprehensive_nodes = await node_manager.get_comprehensive_node_status(
                flow_id=flow_id,
                cycle=last_cycle
            )

            # Update pending and running nodes to terminated
            terminated_count = 0
            for node_id, node_data in comprehensive_nodes.items():
                node_status = node_data.get('status', '').lower()
                if node_status in ['pending', 'running']:
                    node_task_id = f"{flow_id}_{last_cycle}_{node_id}"
                    await node_manager.update_task_status(
                        node_task_id,
                        "terminated",
                        {
                            "message": "Flow stopped by user",
                            "terminated_at": datetime.now().isoformat(),
                        }
                    )
                    terminated_count += 1
                    await self.persist_log(
                        flow_id=flow_id,
                        cycle=last_cycle,
                        message=f"Node {node_id} status updated to terminated (was {node_status})",
                        log_level="INFO",
                        log_source="scheduler",
                        node_id=node_id
                    )

            logger.info(
                "Flow %s stopped, %d nodes updated to terminated status",
                flow_id,
                terminated_count
            )

        # Remove from running list
        if flow_id in self.running_flows:
            self.running_flows.remove(flow_id)

        await self._cancel_cycle_monitor(flow_id)

        logger.info("Flow %s scheduling has been stopped", flow_id)
        return {"status": "stopped", "flow_id": flow_id, "terminated_nodes": terminated_count if last_cycle >= 0 else 0}

    async def get_flow_status(self, flow_id: str) -> Dict:
        """Get flow status information"""
        flow_data = await self.redis.hgetall(f"flow:{flow_id}")
        if not flow_data:
            raise ValueError(f"Flow {flow_id} does not exist")

        # Parse JSON fields
        if "config" in flow_data:
            flow_data["config"] = json.loads(flow_data["config"])
        if "structure" in flow_data:
            flow_data["structure"] = json.loads(flow_data["structure"])

        # Get current cycle status
        current_cycle = int(flow_data.get("last_cycle", -1))
        if current_cycle >= 0:
            cycle_status = await self.get_cycle_status(flow_id, current_cycle)
            flow_data["current_cycle_status"] = cycle_status

        return flow_data

    async def get_cycle_status(self, flow_id: str, cycle: int) -> Dict:
        """Get execution status for a specific cycle"""
        # Get cycle basic information
        cycle_key = f"flow:{flow_id}:cycle:{cycle}"
        cycle_data = await self.redis.hgetall(cycle_key)

        if not cycle_data:
            return {"error": f"Cycle {cycle} does not exist"}

        # Get all node statuses
        nodes_key = f"flow:{flow_id}:cycle:{cycle}:nodes"
        node_ids = await self.redis.smembers(nodes_key)

        # 获取 NodeTaskManager 实例
        task_manager = NodeTaskManager.get_instance()
        if not task_manager._initialized:
            await task_manager.initialize()

        nodes_status = {}
        for node_id in node_ids:
            node_task_id = f"{flow_id}_{cycle}_{node_id}"
            # 使用 NodeTaskManager 获取任务信息
            node_data = await task_manager.get_task(node_task_id)
            if node_data:
                nodes_status[node_id] = node_data

        return {
            "cycle": cycle,
            "status": cycle_data.get("status", "unknown"),
            "start_time": cycle_data.get("start_time"),
            "end_time": cycle_data.get("end_time"),
            "nodes": nodes_status,
            "node_count": len(node_ids),
        }

    async def get_comprehensive_flow_status(self, flow_id: str, cycle: int = None) -> Dict:
        """
        Get comprehensive flow status including all node statuses, logs, and signals

        Args:
            flow_id: Flow identifier
            cycle: Optional cycle number (defaults to latest)

        Returns:
            Dict containing flow info and comprehensive node status data
        """
        try:
            # Get flow data
            flow_data = await self.redis.hgetall(f"flow:{flow_id}")
            if not flow_data:
                raise ValueError(f"Flow {flow_id} does not exist")

            # Use latest cycle if not specified
            if cycle is None:
                cycle = int(flow_data.get("last_cycle", 1))

            # Get cycle data
            cycle_data = await self.redis.hgetall(f"flow:{flow_id}:cycle:{cycle}")

            # Get comprehensive node status from NodeTaskManager
            node_manager = NodeTaskManager.get_instance()
            await node_manager.initialize()

            comprehensive_nodes = await node_manager.get_comprehensive_node_status(
                flow_id=flow_id,
                cycle=cycle
            )

            # Get total nodes in the flow from Redis set
            total_flow_nodes = await self.redis.scard(f"flow:{flow_id}:cycle:{cycle}:nodes")

            # 📊 详细统计所有节点状态（区分 failed 和 terminated）
            total_nodes = len(comprehensive_nodes)
            running_nodes = sum(1 for node in comprehensive_nodes.values() if node['status'] == 'running')
            completed_nodes = sum(1 for node in comprehensive_nodes.values() if node['status'] == 'completed')
            failed_nodes = sum(1 for node in comprehensive_nodes.values() if node['status'] == 'failed')
            terminated_nodes = sum(1 for node in comprehensive_nodes.values() if node['status'] == 'terminated')
            pending_nodes = sum(1 for node in comprehensive_nodes.values() if node['status'] == 'pending')
            # 保留 error 作为向后兼容（可能有些节点使用 'error' 而非 'failed'）
            error_nodes = sum(1 for node in comprehensive_nodes.values() if node['status'] == 'error')

            # 🔄 Flow 状态计算逻辑（优先使用存储的终态状态）
            # 优先使用 Redis 中存储的 flow status（如果是终态：completed/stopped）
            stored_status = flow_data.get("status")

            if stored_status in ["completed", "stopped"]:
                # Run once (interval=0) 完成后或手动停止的 flow，使用存储的状态
                flow_status = stored_status
            else:
                # 否则根据节点状态动态计算
                # 1. 如果有节点在运行或等待 -> flow 状态为 running
                if running_nodes > 0 or pending_nodes > 0:
                    flow_status = "running"
                # 2. 如果所有节点已完成执行（无论成功、失败或终止）-> flow 状态为 completed
                # 这符合 Run once 的语义：Flow 层面已跑完，节点错误单独显示
                elif (completed_nodes + failed_nodes + terminated_nodes) == total_flow_nodes and total_flow_nodes > 0:
                    flow_status = "completed"
                # 3. 默认状态
                else:
                    flow_status = "running"

            return {
                "flow_id": flow_id,
                "cycle": cycle,
                "flow_status": flow_status,
                "start_time": cycle_data.get("start_time"),
                "end_time": cycle_data.get("end_time"),
                "nodes": comprehensive_nodes,
                "statistics": {
                    "total_nodes": total_nodes,
                    "running_nodes": running_nodes,
                    "completed_nodes": completed_nodes,
                    "failed_nodes": failed_nodes,
                    "terminated_nodes": terminated_nodes,
                    "pending_nodes": pending_nodes,
                    "error_nodes": error_nodes  # 向后兼容
                },
                "flow_metadata": {
                    "name": flow_data.get("name"),
                    "description": flow_data.get("description"),
                    "created_at": flow_data.get("created_at"),
                    "updated_at": flow_data.get("updated_at"),
                    "total_cycles": int(flow_data.get("last_cycle", 0)) + 1
                }
            }

        except ValueError as e:
            # ValueError should be re-raised to trigger 404 in API layer
            raise e
        except Exception as e:
            logger.exception(f"Error getting comprehensive flow status: {str(e)}")
            return {
                "flow_id": flow_id,
                "cycle": cycle,
                "flow_status": "error",
                "error": str(e),
                "nodes": {},
                "statistics": {
                    "total_nodes": 0,
                    "running_nodes": 0,
                    "completed_nodes": 0,
                    "error_nodes": 0,
                    "pending_nodes": 0
                }
            }

    async def execute_cycle(self, flow_id: str, cycle: Optional[int] = None) -> Dict:
        """
        Manually trigger execution of a specific flow cycle

        If cycle is None, automatically increment cycle number
        """
        # Check if flow exists
        flow_data = await self.redis.hgetall(f"flow:{flow_id}")
        if not flow_data:
            raise ValueError(f"Flow {flow_id} does not exist")

        # Determine cycle
        if cycle is None:
            last_cycle = int(flow_data.get("last_cycle", -1))
            cycle = last_cycle + 1

            # Clean up old logs when starting a new cycle
            await self.cleanup_old_logs(flow_id, cycle)

            # Log cycle start
            await self.persist_log(
                flow_id=flow_id,
                cycle=cycle,
                message=f"Starting new cycle {cycle} for flow {flow_id}",
                log_level="INFO",
                log_source="scheduler"
            )

        # Execute cycle
        result = await self._execute_flow_cycle(flow_id, cycle)
        return result

    async def stop_component(self, flow_id: str, cycle: int, component_id: str) -> Dict:
        """
        Stop execution of a specific flow component (connected component)

        This will stop all nodes in the component that have not yet been executed
        """
        # Get flow component structure
        flow_data = await self.redis.hgetall(f"flow:{flow_id}")
        if not flow_data:
            raise ValueError(f"Flow {flow_id} does not exist")

        structure = json.loads(flow_data.get("structure", "{}"))
        components = structure.get("components", {})

        if component_id not in components:
            raise ValueError(
                f"Component {component_id} does not exist in Flow {flow_id}"
            )

        # Get all nodes in the component
        component_nodes = components[component_id].get("nodes", [])

        # Add stop flag to Redis
        await self.redis.set(
            f"flow:{flow_id}:cycle:{cycle}:component:{component_id}:stop", "1"
        )

        # Log the action
        logger.info(
            "Marked component %s of flow %s cycle %s as stopped",
            component_id,
            flow_id,
            cycle,
        )

        return {
            "flow_id": flow_id,
            "cycle": cycle,
            "component_id": component_id,
            "status": "stopping",
            "affected_nodes": len(component_nodes),
        }

    async def is_component_stopped(
        self, flow_id: str, cycle: int, component_id: str
    ) -> bool:
        """Check if a specific component has been marked as stopped"""
        stop_flag = await self.redis.get(
            f"flow:{flow_id}:cycle:{cycle}:component:{component_id}:stop"
        )
        return bool(stop_flag)

    async def get_flow_execution_logs(
        self,
        flow_id: str,
        cycle: Optional[int] = None,
        node_id: Optional[str] = None,
        log_level: Optional[str] = None,
        log_source: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        order_direction: str = "desc",
    ) -> Dict:
        """
        Get flow execution logs with filtering and pagination

        Args:
            flow_id: Flow identifier
            cycle: Optional cycle filter
            node_id: Optional node filter
            log_level: Optional log level filter
            log_source: Optional log source filter
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Field to order by
            order_direction: Order direction (asc/desc)

        Returns:
            Dict containing logs list, total count, and pagination info
        """
        try:
            # Check if flow exists
            flow_exists = await self.redis.exists(f"flow:{flow_id}")
            if not flow_exists:
                raise ValueError(f"Flow {flow_id} does not exist")

            # Use database session context manager
            from weather_depot.db import db_session
            from weather_depot.db.services.flow_execution_log_service import (
                FlowExecutionLogService,
            )

            with db_session() as db:
                log_service = FlowExecutionLogService()

                # Get logs with filters
                logs = await log_service.get_logs_by_flow(
                    flow_id=flow_id,
                    cycle=cycle,
                    node_id=node_id,
                    log_level=log_level,
                    log_source=log_source,
                    limit=limit,
                    offset=offset,
                    order_by=order_by,
                    order_direction=order_direction,
                    db=db,  # Pass database session
                )

                # Get total count
                total_count = await log_service.get_logs_count(
                    flow_id=flow_id,
                    cycle=cycle,
                    node_id=node_id,
                    log_level=log_level,
                    log_source=log_source,
                    db=db,  # Pass database session
                )

                # Convert logs to dict format within the session
                logs_data = []
                for log in logs:
                    # Access all needed attributes while session is active
                    log_dict = {
                        'id': log.id,
                        'flow_id': log.flow_id,
                        'cycle': log.cycle,
                        'node_id': log.node_id,
                        'log_level': log.log_level,
                        'log_source': log.log_source,
                        'message': log.message,
                        'metadata': log.log_metadata,
                        'created_at': log.created_at.isoformat() if log.created_at else None,
                    }
                    logs_data.append(log_dict)

                # Get cycle status information for cycles present in logs
                cycle_status_info = await self._get_cycle_status_info(flow_id, logs_data)

                return {
                    "flow_id": flow_id,
                    "logs": logs_data,
                    "cycle_status": cycle_status_info,
                    "pagination": {
                        "total": total_count,
                        "limit": limit,
                        "offset": offset,
                        "has_more": offset + len(logs_data) < total_count,
                    },
                    "filters": {
                        "cycle": cycle,
                        "node_id": node_id,
                        "log_level": log_level,
                        "log_source": log_source,
                    },
                }

        except Exception as e:
            logger.error("Error retrieving flow execution logs: %s", str(e))
            raise

    async def get_flow_execution_log_detail(self, log_id: int) -> Dict:
        """
        Get detailed information for a specific flow execution log

        Args:
            log_id: Log record identifier

        Returns:
            Dict containing log details
        """
        try:
            # Use database session context manager
            from weather_depot.db import db_session
            from weather_depot.db.services.flow_execution_log_service import (
                FlowExecutionLogService,
            )

            with db_session() as db:
                log_service = FlowExecutionLogService()

                # Get log by ID
                log = await log_service.get_log_by_id(log_id, db=db)

                if not log:
                    raise ValueError(f"Log with ID {log_id} not found")

                # Convert to dict format within the session
                log_dict = {
                    'id': log.id,
                    'flow_id': log.flow_id,
                    'cycle': log.cycle,
                    'node_id': log.node_id,
                    'log_level': log.log_level,
                    'log_source': log.log_source,
                    'message': log.message,
                    'metadata': log.log_metadata,
                    'created_at': log.created_at.isoformat() if log.created_at else None,
                }

                # Get additional flow context if available
                flow_exists = await self.redis.exists(f"flow:{log.flow_id}")
                if flow_exists:
                    flow_info = await self.redis.hgetall(f"flow:{log.flow_id}")
                    log_dict["flow_context"] = {
                        "flow_name": flow_info.get("name", "Unknown"),
                        "flow_status": flow_info.get("status", "Unknown"),
                        "current_cycle": int(flow_info.get("current_cycle", 0)),
                    }

                return log_dict

        except Exception as e:
            logger.error("Error retrieving flow execution log detail: %s", str(e))
            raise

    async def _get_cycle_status_info(self, flow_id: str, logs: list = None) -> Dict:
        """
        Get cycle status information for a flow from Redis

        Args:
            flow_id: Flow identifier
            logs: Optional list of logs to determine which cycles to fetch

        Returns:
            Dict containing cycle status information for relevant cycles
        """
        try:
            cycle_status = {}

            # Get unique cycles from logs if provided
            cycles_to_fetch = set()
            if logs:
                cycles_to_fetch = set(log.get('cycle', 0) for log in logs)
            else:
                # If no logs provided, try to get all available cycles
                try:
                    cycle_keys = await self.redis.keys(f"flow:{flow_id}:cycle:*")
                    for key in cycle_keys:
                        parts = key.split(':')
                        if len(parts) >= 4 and parts[4] != 'status':  # Skip non-status keys
                            continue
                        if len(parts) >= 4:
                            try:
                                cycle_num = int(parts[3])
                                cycles_to_fetch.add(cycle_num)
                            except ValueError:
                                continue
                except Exception as e:
                    logger.warning("Could not scan for cycle keys: %s", str(e))

            # Fetch status for each cycle from Redis
            for cycle_num in cycles_to_fetch:
                try:
                    cycle_key = f"flow:{flow_id}:cycle:{cycle_num}"
                    cycle_data = await self.redis.hgetall(cycle_key)

                    if cycle_data:
                        cycle_status[cycle_num] = {
                            'cycle': cycle_num,
                            'status': cycle_data.get('status', 'unknown'),
                            'start_time': cycle_data.get('start_time'),
                            'end_time': cycle_data.get('end_time'),
                            'flow_id': cycle_data.get('flow_id', flow_id)
                        }
                    else:
                        # If no data found in Redis, set as unknown
                        cycle_status[cycle_num] = {
                            'cycle': cycle_num,
                            'status': 'unknown',
                            'start_time': None,
                            'end_time': None,
                            'flow_id': flow_id
                        }

                except Exception as cycle_error:
                    logger.warning("Could not fetch cycle %s status: %s", cycle_num, str(cycle_error))
                    # Set as unknown if we can't fetch the data
                    cycle_status[cycle_num] = {
                        'cycle': cycle_num,
                        'status': 'unknown',
                        'start_time': None,
                        'end_time': None,
                        'flow_id': flow_id
                    }

            return cycle_status

        except Exception as e:
            logger.error("Error getting cycle status info: %s", str(e))
            return {}

    async def get_flow_cycle_node_logs(
        self,
        flow_id: str,
        cycle: int,
        node_id: Optional[str] = None,
        log_level: Optional[str] = None,
        log_source: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        order_direction: str = "desc",
    ) -> Dict:
        """
        Get logs for a specific flow, cycle and node

        Args:
            flow_id: Flow identifier
            cycle: Cycle number
            node_id: Node identifier (None for system/user logs)
            log_level: Optional log level filter
            log_source: Optional log source filter
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Field to order by
            order_direction: Order direction (asc/desc)

        Returns:
            Dict containing logs list and pagination info
        """
        try:
            # Check if flow exists
            flow_exists = await self.redis.exists(f"flow:{flow_id}")
            if not flow_exists:
                raise ValueError(f"Flow {flow_id} does not exist")

            # Initialize log service
            from weather_depot.db.services.flow_execution_log_service import (
                FlowExecutionLogService,
            )
            log_service = FlowExecutionLogService()

            # Get logs for specific flow, cycle and node
            logs = await log_service.get_logs_by_flow_cycle_node(
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

            # Get total count for this specific combination
            total_count = await log_service.get_logs_count(
                flow_id=flow_id,
                cycle=cycle,
                node_id=node_id,
                log_level=log_level,
                log_source=log_source,
            )

            # logs is already in dict format from the service
            logs_data = logs

            return {
                "flow_id": flow_id,
                "cycle": cycle,
                "node_id": node_id,
                "logs": logs_data,
                "pagination": {
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "has_more": offset + len(logs_data) < total_count,
                },
                "filters": {
                    "log_level": log_level,
                    "log_source": log_source,
                },
            }

        except Exception as e:
            logger.error("Error retrieving flow cycle node logs: %s", str(e))
            raise

    # Internal methods

    def _analyze_flow_structure(self, flow_config: Dict) -> Dict:
        """
        Analyze flow structure, identify DAG connected components

        Returns:
            Dict: Dictionary containing connected component information with complete node details
        """
        nodes = flow_config.get("nodes", [])
        edges = flow_config.get("edges", [])

        # Create node lookup map with complete node information
        node_map = {node.get("id"): node for node in nodes}

        # Build directed graph for DAG analysis
        graph = {}
        for node in nodes:
            node_id = node.get("id")
            graph[node_id] = []

        # Build edge relationships
        input_edges_map = {}  # target_node -> list of input edges
        output_edges_map = {}  # source_node -> list of output edges

        for edge in edges:
            source = edge.get("source")
            target = edge.get("target")
            source_handle = edge.get("source_handle", "default")
            target_handle = edge.get("target_handle", "default")

            # Add to directed graph for DAG analysis
            if source in graph:
                graph[source].append(target)

            # Store complete edge information
            edge_info = {
                "source": source,
                "target": target,
                "source_handle": source_handle,
                "target_handle": target_handle,
            }

            # Add to input edges map
            if target not in input_edges_map:
                input_edges_map[target] = []
            input_edges_map[target].append(edge_info)

            # Add to output edges map
            if source not in output_edges_map:
                output_edges_map[source] = []
            output_edges_map[source].append(edge_info)

        # Find all connected components and check if each is a DAG
        components = self._find_components(graph, nodes)

        # Enhance component information with complete node data and edge connections
        enhanced_components = {}
        for comp_id, comp_data in components.items():
            node_ids = comp_data.get("nodes", [])
            enhanced_nodes = []

            for node_id in node_ids:
                # Get complete node information
                node_info = node_map.get(node_id, {"id": node_id})

                # Add edge connection information
                node_with_edges = {
                    "node": node_info,
                    "input_edges": input_edges_map.get(node_id, []),
                    "output_edges": output_edges_map.get(node_id, []),
                }
                enhanced_nodes.append(node_with_edges)

            enhanced_components[comp_id] = {
                "nodes": enhanced_nodes,
                "is_dag": comp_data.get("is_dag", False),
            }

        # Find entry nodes (nodes with no incoming edges)
        entry_nodes = self._find_entry_nodes(graph)

        # Get complete information for entry nodes
        enhanced_entry_nodes = []
        for node_id in entry_nodes:
            entry_node = {
                "id": node_id,
                "node_info": node_map.get(node_id, {}),
                "output_edges": output_edges_map.get(node_id, []),
            }
            enhanced_entry_nodes.append(entry_node)

        return {
            "components": enhanced_components,
            "entry_nodes": entry_nodes,
            "enhanced_entry_nodes": enhanced_entry_nodes,
            "node_map": node_map,
            "input_edges_map": input_edges_map,
            "output_edges_map": output_edges_map,
        }

    def _find_components(self, graph: Dict, nodes: List) -> Dict[str, Dict]:
        """
        Find all connected components in the graph

        Returns:
            Dict: Mapping from component ID to component information
        """
        # Build undirected graph for finding connected components
        undirected_graph = {}
        for node_id in graph:
            if node_id not in undirected_graph:
                undirected_graph[node_id] = set()
            for target in graph[node_id]:
                if target not in undirected_graph:
                    undirected_graph[target] = set()
                undirected_graph[node_id].add(target)
                undirected_graph[target].add(node_id)

        # Use BFS to find connected components
        visited = set()
        components = {}
        component_id = 0

        for node_id in graph:
            if node_id not in visited:
                # Found a new connected component
                component = set()
                queue = [node_id]
                visited.add(node_id)
                component.add(node_id)

                while queue:
                    current = queue.pop(0)
                    for neighbor in undirected_graph.get(current, []):
                        if neighbor not in visited:
                            visited.add(neighbor)
                            component.add(neighbor)
                            queue.append(neighbor)

                # Check if this connected component is a DAG
                is_dag = self._is_dag(graph, component)

                # Save component information
                components[str(component_id)] = {
                    "nodes": list(component),
                    "is_dag": is_dag,
                }
                component_id += 1

        return components

    def _is_dag(self, graph: Dict, component: Set[str]) -> bool:
        """
        Check if a connected component is a DAG (acyclic)
        """
        # Use DFS to detect cycles
        visited = {
            node_id: 0 for node_id in component
        }  # 0: not visited, 1: visiting, 2: completed

        def dfs(node_id):
            if node_id not in visited:
                return True

            visited[node_id] = 1  # Mark as visiting

            for neighbor in graph.get(node_id, []):
                if neighbor in component:  # Only consider nodes in current component
                    if neighbor not in visited:
                        visited[neighbor] = 0

                    if visited[neighbor] == 0:  # Not visited
                        if not dfs(neighbor):
                            return False
                    elif visited[neighbor] == 1:  # Visiting, cycle exists
                        return False

            visited[node_id] = 2  # Mark as completed
            return True

        for node_id in component:
            if visited[node_id] == 0:  # Not visited
                if not dfs(node_id):
                    return False

        return True

    def _find_entry_nodes(self, graph: Dict) -> List[str]:
        """
        Find all entry nodes (nodes with no incoming edges)
        """
        # Calculate in-degree for all nodes
        in_degree = {node: 0 for node in graph}
        for node in graph:
            for target in graph[node]:
                if target in in_degree:
                    in_degree[target] += 1
                else:
                    in_degree[target] = 1

        # Nodes with in-degree 0 are entry nodes
        entry_nodes = [node for node, degree in in_degree.items() if degree == 0]
        return entry_nodes

    async def _schedule_flow_execution(self, flow_id: str):
        """
        Flow scheduling loop
        """
        try:
            tried = 0
            MAX_TRIED = 500
            while flow_id in self.running_flows and tried < MAX_TRIED:
                # Get flow information
                flow_data = await self.redis.hgetall(f"flow:{flow_id}")
                if not flow_data:
                    logger.error("Flow %s does not exist, stopping scheduling", flow_id)
                    self.running_flows.remove(flow_id)
                    break

                # Check flow status
                flow_status = flow_data.get("status")
                if flow_status != "running":
                    logger.info(
                        "Flow %s status is %s, pausing scheduling", flow_id, flow_status
                    )
                    self.running_flows.remove(flow_id)
                    break

                # Check if next execution time has been reached
                next_execution = float(flow_data.get("next_execution", 0))
                current_time = time.time()

                if current_time >= next_execution:
                    # Execute new cycle, if not exists, start from 1
                    last_cycle = int(flow_data.get("last_cycle", 0))
                    new_cycle = last_cycle + 1

                    # Clean up old logs when starting a new cycle
                    await self.cleanup_old_logs(flow_id, new_cycle)

                    # Log cycle start
                    await self.persist_log(
                        flow_id=flow_id,
                        cycle=new_cycle,
                        message=f"Executing scheduled flow {flow_id} cycle {new_cycle} at {datetime.now().isoformat()}",
                        log_level="INFO",
                        log_source="scheduler"
                    )

                    # Execute flow cycle
                    execution_result = None
                    execution_error = None
                    try:
                        execution_result = await self._execute_flow_cycle(flow_id, new_cycle)
                    except Exception as e:
                        logger.error(
                            "Error executing flow %s cycle %s: %s",
                            flow_id,
                            new_cycle,
                            str(e),
                        )
                        execution_error = str(e)

                    # 🔔 发送执行完成事件到 WebSocket (通过 Redis Pub/Sub)
                    try:
                        completion_event = {
                            "flow_id": flow_id,
                            "cycle": new_cycle,
                            "status": (
                                "error"
                                if execution_error
                                else (execution_result or {}).get("status", "unknown")
                            ),
                            "error": execution_error,
                            "result": execution_result,
                            "timestamp": datetime.now().isoformat(),
                        }

                        # 发布到 execution_complete 频道（最终状态由监控任务再次推送）
                        await self.redis.publish(
                            f"execution_complete:flow:{flow_id}",
                            json.dumps(completion_event)
                        )
                        logger.info(
                            f"Published execution_complete event for flow {flow_id} cycle {new_cycle} with status {completion_event['status']}"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to publish execution_complete event: {e}")

                    # Update next execution time
                    flow_config = json.loads(flow_data.get("config", "{}"))
                    interval_seconds = self._parse_interval(
                        flow_config.get("interval", "0")
                    )

                # 如果 interval_seconds 为 0，表示只执行一次
                if interval_seconds == 0:
                    logger.info(
                        "Flow %s has interval=0, executing once and stopping scheduling",
                        flow_id
                    )
                    # 从运行中的流程列表中移除
                    if flow_id in self.running_flows:
                        self.running_flows.remove(flow_id)
                    # 更新状态为监控中，等待异步监控完成后再写最终结果
                    await self.redis.hset(
                        f"flow:{flow_id}",
                        mapping={
                            "status": "monitoring",
                            "last_cycle": str(new_cycle),
                        },
                    )
                    # 退出循环
                    break
                else:
                    # 正常情况，计算下次执行时间
                    next_execution = current_time + interval_seconds
                    await self.redis.hset(
                        f"flow:{flow_id}", "next_execution", str(next_execution)
                    )
                    await self.redis.hset(
                        f"flow:{flow_id}", "last_cycle", str(new_cycle)
                    )

                await asyncio.sleep(interval_seconds)
                tried += 1

        except Exception as e:
            logger.error("Error in flow %s scheduling loop: %s", flow_id, str(e))
            logger.debug(traceback.format_exc())
            if flow_id in self.running_flows:
                self.running_flows.remove(flow_id)

    async def _execute_flow_cycle(self, flow_id: str, cycle: int) -> Dict:
        """
        Execute one cycle of the flow

        This method executes all nodes in the flow, letting edge signal propagation
        control which nodes can run at a given time.
        """
        await self.persist_log(
            flow_id=flow_id,
            cycle=cycle,
            message=f"[_execute_flow_cycle] Executing flow {flow_id} cycle {cycle}",
            log_level="INFO",
            log_source="scheduler"
        )

        # Get flow information
        flow_data = await self.redis.hgetall(f"flow:{flow_id}")
        if not flow_data:
            error_msg = f"Flow {flow_id} does not exist"
            await self.persist_log(
                flow_id=flow_id,
                cycle=cycle,
                message=error_msg,
                log_level="ERROR",
                log_source="scheduler"
            )
            raise ValueError(error_msg)

        # Get user_id for Quest activity tracking
        user_id = flow_data.get("user_id")

        # Publish RUN_FLOW event for Quest tracking
        if user_id:
            try:
                publish_activity(
                    user_id=user_id,
                    event_type='RUN_FLOW',
                    metadata={
                        'flowId': flow_id,
                        'cycle': cycle,
                        'startTime': datetime.now().isoformat()
                    }
                )
                logger.info(f"Published RUN_FLOW event for user {user_id}, flow {flow_id}, cycle {cycle}")
            except Exception as e:
                logger.warning(f"Failed to publish RUN_FLOW activity: {e}")
        else:
            logger.debug(f"Flow {flow_id} has no user_id, skipping Quest activity publication")

        flow_structure = json.loads(flow_data.get("structure", "{}"))

        # Create cycle record
        cycle_key = f"flow:{flow_id}:cycle:{cycle}"
        cycle_data = {
            "flow_id": flow_id,
            "cycle": str(cycle),
            "status": "running",
            "start_time": datetime.now().isoformat(),
            "end_time": "",
        }
        await self.redis.hset(cycle_key, mapping=cycle_data)
        nodes_key = f"flow:{flow_id}:cycle:{cycle}:nodes"

        # Get all nodes from the structure
        node_map = flow_structure.get("node_map", {})
        if not node_map:
            error_msg = f"No nodes found in flow structure for flow {flow_id}"
            await self.persist_log(
                flow_id=flow_id,
                cycle=cycle,
                message=error_msg,
                log_level="ERROR",
                log_source="scheduler"
            )
            return {
                "flow_id": flow_id,
                "cycle": cycle,
                "status": "error",
                "error": "No nodes found in flow structure",
            }

        # Get node component mapping from the enhanced structure
        node_component_map = {}
        for comp_id, comp_data in flow_structure.get("components", {}).items():
            for node_data in comp_data.get("nodes", []):
                if isinstance(node_data, dict) and "node" in node_data:
                    node_id = node_data["node"].get("id")
                    if node_id:
                        node_component_map[node_id] = comp_id

        # Get pre-computed edge mappings
        input_edges_map = flow_structure.get("input_edges_map", {})
        output_edges_map = flow_structure.get("output_edges_map", {})

        # 🚀 Initialize all nodes with running status before dispatching to workers
        # This ensures the frontend immediately sees all nodes as running
        node_manager = NodeTaskManager.get_instance()
        await node_manager.initialize()

        for node_id, node_info in node_map.items():
            component_id = node_component_map.get(node_id)
            if component_id:
                node_task_id = f"{flow_id}_{cycle}_{node_id}"
                initial_task_info = {
                    "node_task_id": node_task_id,
                    "flow_id": flow_id,
                    "cycle": cycle,
                    "node_id": node_id,
                    "component_id": component_id,
                    "node_type": node_info.get("type", "unknown"),
                    "status": "pending",  # 🔥 Set initial status to pending (worker will update to running)
                    "created_at": datetime.now().isoformat(),
                    "message": "Flow execution started, node dispatched",
                }
                await node_manager.register_task(node_task_id, initial_task_info)
                await self.persist_log(
                    flow_id=flow_id,
                    cycle=cycle,
                    message=f"Node {node_id} initialized with pending status",
                    log_level="INFO",
                    log_source="scheduler",
                    node_id=node_id
                )

        # Create node execution tasks for ALL nodes (not just entry nodes)
        tasks = {}

        for node_id, node_info in node_map.items():
            # Get node's component ID
            component_id = node_component_map.get(node_id)
            if not component_id:
                await self.persist_log(
                    flow_id=flow_id,
                    cycle=cycle,
                    message=f"Cannot find component ID for node {node_id}, skipping execution",
                    log_level="ERROR",
                    log_source="scheduler",
                    node_id=node_id
                )
                continue

            # Get node's input and output edges
            input_edges = input_edges_map.get(node_id, [])
            output_edges = output_edges_map.get(node_id, [])

            # Create node execution task
            task = self._execute_node(
                flow_id=flow_id,
                component_id=component_id,
                cycle=cycle,
                node_id=node_id,
                node_type=node_info.get("type", "unknown"),
                input_edges=input_edges,
                output_edges=output_edges,
                node_config=node_info.get("config", {}),
            )

            tasks[node_id] = task
            # Register node in the cycle
            await self.redis.sadd(nodes_key, node_id)

        # Execute all tasks and wait for them to complete
        # Note: This will rely on the worker's implementation to handle data dependencies
        # via input and output edge signals

        # Start all tasks but don't await them
        running_tasks = {
            node_id: asyncio.create_task(task) for node_id, task in tasks.items()
        }

        # Wait for all tasks to complete (dispatch stage)
        completed_results = {}
        for node_id, task in running_tasks.items():
            try:
                result = await task
                completed_results[node_id] = result
                await self.persist_log(
                    flow_id=flow_id,
                    cycle=cycle,
                    message=f"Node {node_id} execution dispatched to worker",
                    log_level="INFO",
                    log_source="scheduler",
                    node_id=node_id
                )
            except Exception as e:
                error_msg = f"Error executing node {node_id}: {str(e)}"
                await self.persist_log(
                    flow_id=flow_id,
                    cycle=cycle,
                    message=error_msg,
                    log_level="ERROR",
                    log_source="scheduler",
                    node_id=node_id
                )
                completed_results[node_id] = {"status": "error", "error": str(e)}

        # Mark cycle as being monitored rather than completed immediately
        await self.redis.hset(
            cycle_key,
            mapping={
                "status": "monitoring",
                "end_time": "",  # Actual end time filled when monitor finishes
            },
        )

        await self.persist_log(
            flow_id=flow_id,
            cycle=cycle,
            message=(
                f"Flow {flow_id} cycle {cycle} dispatched {len(completed_results)} nodes, "
                "entering monitoring phase until nodes finish or timeout"
            ),
            log_level="INFO",
            log_source="scheduler"
        )

        await self._start_cycle_monitor(
            flow_id=flow_id,
            cycle=cycle,
            expected_node_ids=list(node_map.keys()),
            user_id=user_id,
        )

        return {
            "flow_id": flow_id,
            "cycle": cycle,
            "status": "monitoring",
            "nodes_count": len(node_map),
            "nodes_dispatched": len(completed_results),
            "results": completed_results,
            "monitoring": {
                "timeout_seconds": self.monitor_max_wait_seconds,
                "poll_interval_seconds": self.monitor_poll_interval,
            },
        }

    # Update _execute_node method to use input_edges and output_edges instead of downstream_nodes
    async def _execute_node(
        self,
        flow_id: str,
        component_id: str,
        cycle: int,
        node_id: str,
        node_type: str,
        input_edges: List[Dict],
        output_edges: List[Dict],
        node_config: Dict,
    ) -> Dict:
        """
        Execute a single node
        """
        await self.persist_log(
            flow_id=flow_id,
            cycle=cycle,
            message=f"Starting execution of node {node_id} (type: {node_type})",
            log_level="INFO",
            log_source="scheduler",
            node_id=node_id
        )
        # Check if component has been marked for stopping
        if await self.is_component_stopped(flow_id, cycle, component_id):
            await self.persist_log(
                flow_id=flow_id,
                cycle=cycle,
                message=f"Component {component_id} has been marked as stopped, skipping execution of node {node_id}",
                log_level="INFO",
                log_source="scheduler",
                node_id=node_id
            )
            return {"status": "skipped", "reason": "component_stopped"}

        # Use NodeRegistry to find workers supporting this node type
        node_registry = NodeRegistry.get_instance()

        # Ensure NodeRegistry is initialized
        if not node_registry.redis:
            await node_registry.initialize()

        await self.persist_log(
            flow_id=flow_id,
            cycle=cycle,
            message=f"Looking for workers supporting node type {node_type}",
            log_level="DEBUG",
            log_source="scheduler",
            node_id=node_id
        )
        workers = await node_registry.find_workers_for_node_type(node_type)

        if not workers:
            error_msg = f"No available workers found supporting node type {node_type}"
            await self.persist_log(
                flow_id=flow_id,
                cycle=cycle,
                message=error_msg,
                log_level="ERROR",
                log_source="scheduler",
                node_id=node_id
            )

            # Record node error status
            error_data = {
                "flow_id": flow_id,
                "cycle": cycle,
                "component_id": component_id,
                "status": "error",
                "error": error_msg,
                "start_time": datetime.now().isoformat(),
            }
            await self.redis.hset(f"node:{node_id}", mapping=error_data)

            return {"status": "error", "error": error_msg}

        # Simple load balancing - randomly select a worker
        selected_worker = random.choice(workers)
        worker_api_url = selected_worker["api_url"]

        # Get user_id from flow data for Quest tracking
        flow_data = await self.redis.hgetall(f"flow:{flow_id}")
        user_id = flow_data.get("user_id")

        # Prepare node execution request data
        node_data = {
            "flow_id": flow_id,
            "component_id": component_id,
            "cycle": cycle,
            "node_id": node_id,
            "node_type": node_type,
            "user_id": user_id,  # Pass user_id to worker for Quest tracking
            "input_edges": input_edges,
            "output_edges": output_edges,
            "config": node_config,
        }

        try:
            # Call the selected worker's node execution API
            async with httpx.AsyncClient(timeout=30) as client:
                await self.persist_log(
                    flow_id=flow_id,
                    cycle=cycle,
                    message=f"Node {node_id} (type: {node_type}) will be executed by worker {selected_worker['id']}",
                    log_level="INFO",
                    log_source="scheduler",
                    node_id=node_id
                )
                try:
                    await self.persist_log(
                        flow_id=flow_id,
                        cycle=cycle,
                        message=f"Node execution data: {json.dumps(node_data)}",
                        log_level="DEBUG",
                        log_source="scheduler",
                        node_id=node_id
                    )
                    response = await client.post(
                        f"{worker_api_url}/nodes/execute", json=node_data
                    )
                    response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    await self.persist_log(
                        flow_id=flow_id,
                        cycle=cycle,
                        message=f"Node execution error: {e.response.text}",
                        log_level="ERROR",
                        log_source="scheduler",
                        node_id=node_id
                    )
                    await self.persist_log(
                        flow_id=flow_id,
                        cycle=cycle,
                        message=f"Node execution data: {json.dumps(node_data)}",
                        log_level="ERROR",
                        log_source="scheduler",
                        node_id=node_id
                    )
                    raise
                result = response.json()

                # Record node start status
                node_start_data = {
                    "flow_id": flow_id,
                    "cycle": cycle,
                    "component_id": component_id,
                    "worker_id": selected_worker["id"],
                    "status": result.get("status", "unknown"),
                    "start_time": datetime.now().isoformat(),
                }

                await self.redis.hset(f"node:{node_id}", mapping=node_start_data)

                return result

        except Exception as e:
            await self.persist_log(
                flow_id=flow_id,
                cycle=cycle,
                message=f"Error executing node {node_id}: {str(e)}",
                log_level="ERROR",
                log_source="scheduler",
                node_id=node_id
            )

            # Record node error status
            error_data = {
                "flow_id": flow_id,
                "cycle": cycle,
                "component_id": component_id,
                "worker_id": (
                    selected_worker["id"] if "id" in selected_worker else "unknown"
                ),
                "status": "error",
                "error": str(e),
                "start_time": datetime.now().isoformat(),
            }

            await self.redis.hset(f"node:{node_id}", mapping=error_data)

            return {"status": "error", "error": str(e)}

    def _monitor_task_key(self, flow_id: str, cycle: int) -> str:
        """Generate unique key for monitor tasks."""
        return f"{flow_id}:{cycle}"

    async def _start_cycle_monitor(
        self,
        flow_id: str,
        cycle: int,
        expected_node_ids: List[str],
        user_id: Optional[str] = None,
    ) -> None:
        """Spawn or replace the background task that watches node completion."""
        monitor_key = self._monitor_task_key(flow_id, cycle)
        existing = self._cycle_monitor_tasks.pop(monitor_key, None)
        if existing and not existing.done():
            existing.cancel()
            try:
                await existing
            except asyncio.CancelledError:
                pass

        monitor_task = asyncio.create_task(
            self._monitor_cycle_until_terminal(
                flow_id=flow_id,
                cycle=cycle,
                expected_node_ids=expected_node_ids,
                user_id=user_id,
            )
        )

        self._cycle_monitor_tasks[monitor_key] = monitor_task

        def _cleanup(task: asyncio.Task, key: str = monitor_key):
            self._cycle_monitor_tasks.pop(key, None)

        monitor_task.add_done_callback(_cleanup)

    async def _cancel_cycle_monitor(
        self, flow_id: str, cycle: Optional[int] = None
    ) -> None:
        """Cancel monitoring tasks for a flow (optionally scoped to a cycle)."""
        keys = []
        if cycle is None:
            prefix = f"{flow_id}:"
            keys = [key for key in self._cycle_monitor_tasks.keys() if key.startswith(prefix)]
        else:
            keys = [self._monitor_task_key(flow_id, cycle)]

        for key in keys:
            task = self._cycle_monitor_tasks.pop(key, None)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    _, _, key_cycle = key.partition(":")
                    try:
                        cycle_for_log = int(key_cycle)
                    except ValueError:
                        cycle_for_log = -1
                    await self.persist_log(
                        flow_id=flow_id,
                        cycle=cycle_for_log,
                        message=f"Cycle monitor {key} cancelled",
                        log_level="INFO",
                        log_source="scheduler",
                    )

    async def _collect_node_statuses(
        self,
        node_manager: NodeTaskManager,
        flow_id: str,
        cycle: int,
        expected_node_ids: List[str],
    ) -> Dict[str, str]:
        """Fetch current node statuses from NodeTaskManager."""
        statuses: Dict[str, str] = {}
        for node_id in expected_node_ids:
            node_task_id = f"{flow_id}_{cycle}_{node_id}"
            task_info = await node_manager.get_task(node_task_id)
            raw_status = (task_info or {}).get("status") or "pending"
            statuses[node_id] = str(raw_status).lower()
        return statuses

    def _summarize_node_statuses(
        self, statuses: Dict[str, str]
    ) -> Dict[str, Any]:
        """Summarize node status counts and classify success/failure/pending."""
        counter = Counter(statuses.values())
        success_states = {"completed", "skipped"}
        failure_states = {"failed", "error", "terminated"}

        success_nodes = [node_id for node_id, state in statuses.items() if state in success_states]
        failure_nodes = [node_id for node_id, state in statuses.items() if state in failure_states]
        pending_nodes = [
            node_id
            for node_id, state in statuses.items()
            if state not in success_states | failure_states
        ]

        return {
            "counts": dict(counter),
            "success_nodes": success_nodes,
            "failed_nodes": failure_nodes,
            "pending_nodes": pending_nodes,
            "success_count": len(success_nodes),
            "failure_count": len(failure_nodes),
            "pending_count": len(pending_nodes),
        }

    async def _monitor_cycle_until_terminal(
        self,
        flow_id: str,
        cycle: int,
        expected_node_ids: List[str],
        user_id: Optional[str] = None,
    ):
        """Poll node statuses until they reach a terminal state or timeout."""
        if not expected_node_ids:
            summary = {
                "counts": {},
                "success_nodes": [],
                "failed_nodes": [],
                "pending_nodes": [],
                "success_count": 0,
                "failure_count": 0,
                "pending_count": 0,
                "note": "No nodes to monitor",
            }
            await self._finalize_cycle_state(
                flow_id=flow_id,
                cycle=cycle,
                final_status="completed",
                summary=summary,
                user_id=user_id,
            )
            return

        poll_interval = max(1, self.monitor_poll_interval)
        timeout_seconds = max(1, self.monitor_max_wait_seconds)
        deadline = time.time() + timeout_seconds

        node_manager = NodeTaskManager.get_instance()
        await node_manager.initialize()

        iteration = 0
        last_counts = None
        final_status = "timeout"
        summary: Dict[str, Any] = {}
        error_message: Optional[str] = None

        await self.persist_log(
            flow_id=flow_id,
            cycle=cycle,
            message=(
                f"Started monitoring flow {flow_id} cycle {cycle}: "
                f"{len(expected_node_ids)} nodes, timeout={timeout_seconds}s, poll={poll_interval}s"
            ),
            log_level="INFO",
            log_source="scheduler",
        )

        while True:
            iteration += 1
            statuses = await self._collect_node_statuses(
                node_manager, flow_id, cycle, expected_node_ids
            )
            summary = self._summarize_node_statuses(statuses)
            summary["iterations"] = iteration

            counts_snapshot = summary.get("counts", {})
            if counts_snapshot != last_counts:
                await self.persist_log(
                    flow_id=flow_id,
                    cycle=cycle,
                    message=(
                        f"[Monitor] Cycle {cycle} status update: {counts_snapshot}, "
                        f"pending_nodes={summary.get('pending_nodes')}"
                    ),
                    log_level="DEBUG",
                    log_source="scheduler",
                )
                last_counts = counts_snapshot

            if summary["failed_nodes"]:
                final_status = "failed"
                error_message = (
                    f"Detected failed/terminated nodes: {', '.join(summary['failed_nodes'])}"
                )
                break

            node_total = len(expected_node_ids)
            if summary["success_count"] >= node_total and node_total > 0:
                final_status = "completed"
                break

            if time.time() >= deadline:
                final_status = "timeout"
                pending_nodes = summary.get("pending_nodes") or []
                error_message = (
                    f"Monitoring timeout after {timeout_seconds}s; pending nodes: {pending_nodes}"
                )
                break

            await asyncio.sleep(poll_interval)

        summary.update(
            {
                "timeout_seconds": timeout_seconds,
                "poll_interval_seconds": poll_interval,
                "finished_at": datetime.now().isoformat(),
            }
        )

        await self._finalize_cycle_state(
            flow_id=flow_id,
            cycle=cycle,
            final_status=final_status,
            summary=summary,
            error_message=error_message,
            user_id=user_id,
        )

    async def _finalize_cycle_state(
        self,
        flow_id: str,
        cycle: int,
        final_status: str,
        summary: Dict[str, Any],
        error_message: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> None:
        """Persist final cycle status and publish events/logs."""
        cycle_key = f"flow:{flow_id}:cycle:{cycle}"
        await self.redis.hset(
            cycle_key,
            mapping={
                "status": final_status,
                "end_time": datetime.now().isoformat(),
                "result_summary": json.dumps(summary),
            },
        )

        await self.persist_log(
            flow_id=flow_id,
            cycle=cycle,
            message=(
                f"Flow {flow_id} cycle {cycle} finished with status '{final_status}'. "
                f"Summary: {summary}"
            ),
            log_level="INFO" if final_status == "completed" else "WARNING",
            log_source="scheduler",
        )

        await self._publish_cycle_completion_event(
            flow_id=flow_id,
            cycle=cycle,
            final_status=final_status,
            summary=summary,
            error_message=error_message,
        )

        await self._update_flow_completion_status(flow_id, final_status)

        if user_id:
            try:
                total_nodes = sum(summary.get("counts", {}).values())
                publish_activity(
                    user_id=user_id,
                    event_type="COMPLETE_FLOW",
                    metadata={
                        "flowId": flow_id,
                        "cycle": cycle,
                        "success": final_status == "completed",
                        "nodesCount": total_nodes,
                        "endTime": datetime.now().isoformat(),
                    },
                )
            except Exception as exc:
                logger.warning(f"Failed to publish COMPLETE_FLOW activity: {exc}")

    async def _publish_cycle_completion_event(
        self,
        flow_id: str,
        cycle: int,
        final_status: str,
        summary: Dict[str, Any],
        error_message: Optional[str] = None,
    ) -> None:
        """Publish completion event to Redis."""
        if not self.redis:
            return

        event_payload = {
            "flow_id": flow_id,
            "cycle": cycle,
            "status": final_status,
            "error": error_message,
            "summary": summary,
            "timestamp": datetime.now().isoformat(),
        }

        try:
            await self.redis.publish(
                f"execution_complete:flow:{flow_id}", json.dumps(event_payload)
            )
        except Exception as exc:
            logger.warning(f"Failed to publish completion event for flow {flow_id}: {exc}")

    async def _update_flow_completion_status(self, flow_id: str, final_status: str) -> None:
        """Update flow level status fields when a cycle ends."""
        flow_data = await self.redis.hgetall(f"flow:{flow_id}")
        if not flow_data:
            return

        metadata = {
            "last_cycle_status": final_status,
            "last_cycle_completed_at": datetime.now().isoformat(),
        }

        await self.redis.hset(f"flow:{flow_id}", mapping=metadata)

        # Only mark entire flow as completed/failed when interval is 0 (run once flows)
        try:
            flow_config = json.loads(flow_data.get("config", "{}"))
        except Exception:
            flow_config = {}

        interval_seconds = self._parse_interval(flow_config.get("interval", "0"))
        if interval_seconds == 0:
            await self.redis.hset(f"flow:{flow_id}", "status", final_status)

    def _parse_int_config(self, value, default: int) -> int:
        """Safely parse integer configuration values."""
        try:
            if value is None:
                return default
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _parse_interval(self, interval_str: str) -> int:
        """
        Parse interval string, convert to seconds

        Supported formats: 10s, 5m, 1h, 1d
        """
        try:
            if isinstance(interval_str, (int, float)):
                return int(interval_str)

            unit = interval_str[-1]
            value = int(interval_str[:-1])

            if unit == "s":
                return value
            elif unit == "m":
                return value * 60
            elif unit == "h":
                return value * 60 * 60
            elif unit == "d":
                return value * 24 * 60 * 60
            else:
                # Default to treating as seconds
                return int(interval_str)
        except Exception:
            # Default to 1 hour if parsing fails
            return 0


# Singleton pattern to get instance
_scheduler_instance = None


def get_scheduler_instance():
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = FlowScheduler()
    return _scheduler_instance


# Example usage code
async def example_usage():
    import logging

    # Set up logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("example_usage")
    logger.setLevel(logging.DEBUG)

    scheduler = get_scheduler_instance()
    await scheduler.initialize()

    # Register flow
    flow_config = {
        "interval": "1m",
        "nodes": [
            {
                "id": "A",
                "type": "price_node",
                "config": {
                    "node_class_type": "price_node",
                    "source": "coingecko",
                    "data_type": "kline",
                    "symbol": "bitcoin",
                },
            },
        ],
        "edges": [],
    }

    await scheduler.register_flow("example_flow", flow_config)

    # Start flow scheduling
    await scheduler.start_flow("example_flow")

    # ... Application running ...
    # Wait some time to observe scheduler behavior
    # await asyncio.sleep(120)  # Wait 2 minutes
    while 1:
        await asyncio.sleep(5)
        # Add additional logic here
        # Query flow status
        flow_status = await scheduler.get_flow_status("example_flow")
        logger.info("Flow status: %s", flow_status)
        # Query cycle status
        cycle_status = await scheduler.get_cycle_status("example_flow", 0)
        logger.info("Cycle status: %s", cycle_status)

    # Shut down scheduler
    await scheduler.shutdown()


if __name__ == "__main__":
    # Run example
    asyncio.run(example_usage())
