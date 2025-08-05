"""Node registry that manages all available node types and worker instances"""

import asyncio
import json
import logging
import socket
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type

import redis.asyncio as aioredis

from tradingflow.depot.python.config import CONFIG

if TYPE_CHECKING:
    from tradingflow.station.nodes.node_base import NodeBase

logger = logging.getLogger(__name__)


class NodeRegistry:
    """
    Node registry that manages all available node types and worker instances

    Features:
    1. Local node type registration and instantiation (existing function)
    2. Worker instance registration to Redis (new)
    3. Query available worker instances (new)
    """

    _instance = None
    _node_classes: Dict[str, Type["NodeBase"]] = {}
    _node_params: Dict[str, Dict[str, Any]] = {}

    def __init__(self):
        """Initialize node registry"""
        self.redis = None
        self.worker_id = CONFIG.get("WORKER_ID", socket.gethostname())
        self.api_url = CONFIG.get("WORKER_API_URL")
        # Set heartbeat interval to 2 minutes
        self.heartbeat_interval = int(CONFIG.get("REGISTRY_HEARTBEAT_INTERVAL", 120))
        # TTL set to be slightly longer than registration interval to ensure buffer time even if registration fails once
        self.node_ttl = int(CONFIG.get("NODE_TTL", 45))
        self._heartbeat_task = None
        self._is_running = False
        self.supported_node_types = set()  # Node types supported by current worker

    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ========== Local node type registration (existing function) ==========

    def register_node(
        self,
        node_class_type: str,
        node_class: Type["NodeBase"],
        default_params: Dict[str, Any] = None,
    ):
        """
        Register node type to local registry

        Args:
            node_class_type: Node type identifier
            node_class: Node class
            default_params: Default node parameters
        """
        self._node_classes[node_class_type] = node_class
        self._node_params[node_class_type] = default_params or {}

        # Add this node type to the types supported by current worker
        self.supported_node_types.add(node_class_type)

        # If Redis is connected, also sync to Redis (asynchronously)
        if self.redis:
            # Since we can't use await directly in a synchronous method, wrap with asyncio.create_task
            asyncio.create_task(
                self._sync_node_type_to_redis(node_class_type, default_params)
            )

        logger.info(
            "Node type %s has been registered to local registry", node_class_type
        )

    def get_node_class(self, node_class_type: str) -> Type["NodeBase"]:
        """Get node class"""
        if node_class_type not in self._node_classes:
            raise KeyError(f"Node type not found: {node_class_type}")
        return self._node_classes[node_class_type]

    def get_default_params(self, node_class_type: str) -> Dict[str, Any]:
        """Get default node parameters"""
        if node_class_type not in self._node_params:
            raise KeyError(f"Node type not found: {node_class_type}")
        return self._node_params[node_class_type]

    def create_node(
        self,
        node_class_type: str,
        flow_id,
        component_id: str,
        cycle: int,
        node_id: str,
        input_edges: List[str] = None,
        output_edges: List[str] = None,
        config: Dict[str, Any] = None,
    ) -> "NodeBase":
        """
        Create node instance

        Args:
            node_class_type: Node type
            flow_id: Flow ID
            component_id: Component ID
            cycle: Cycle
            node_id: Node ID
            config: Node configuration

        Returns:
            Node instance
        """
        logger.debug("[create_node]self._node_classes: %s", self._node_classes)
        logger.debug("[create_node]self: %s", self)
        if node_class_type not in self._node_classes:
            raise ValueError(f"Unknown node class type: {node_class_type}")

        node_class = self._node_classes[node_class_type]
        default_params = self._node_params[node_class_type].copy()

        # Create basic parameters
        params = {
            "flow_id": flow_id,
            "component_id": component_id,
            "cycle": cycle,
            "node_id": node_id,
            # If name exists in config use it, otherwise use name from default_params,
            # if both don't exist, use type+ID as default name
            "name": config.get("name")
            or default_params.get("name")
            or f"{node_class_type}-{node_id}",
            "input_edges": input_edges,
            "output_edges": output_edges,
        }

        # Update default parameters
        if config:
            for key, value in default_params.items():
                if key in config:
                    # If config has corresponding parameter, use value from config with appropriate type conversion
                    if isinstance(value, (int, float, bool)) and isinstance(
                        config[key], str
                    ):
                        # Try to convert string to corresponding type
                        if isinstance(value, int):
                            params[key] = int(config[key]) if config[key] else value
                        elif isinstance(value, float):
                            params[key] = float(config[key]) if config[key] else value
                        elif isinstance(value, bool):
                            params[key] = (
                                config[key].lower() == "true" if config[key] else value
                            )
                    else:
                        params[key] = config[key]
                else:
                    # Otherwise use default value
                    params[key] = value

        # Add other config items (not in default params)
        if config:
            for key, value in config.items():
                if key not in params and key != "node_class_type" and key != "name":
                    params[key] = value

        # Create node instance
        logger.debug(
            "Creating node instance of type %s with params: %s", node_class_type, params
        )
        return node_class(**params)

    def get_supported_node_types(self) -> List[str]:
        """Get all node types supported by current worker"""
        return list(self._node_classes.keys())

    # ========== Worker registration and discovery (new feature) ==========

    async def initialize(self) -> bool:
        """
        Initialize registry, connect to Redis

        Returns:
            bool: Whether initialization was successful
        """
        try:
            # Connect to Redis
            redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
            self.redis = await aioredis.from_url(redis_url, decode_responses=True)
            logger.info("Node registry connected to Redis: %s", redis_url)
            return True
        except Exception as e:
            logger.error("Node registry initialization failed: %s", str(e))
            return False

    async def shutdown(self):
        """Shut down registry, stop heartbeat and release resources"""
        # Stop heartbeat task
        await self.stop_heartbeat()

        # Close Redis connection
        if self.redis:
            await self.redis.close()
            logger.info("Node registry has been shut down")

    async def register_worker(self, node_types: List[str] = None) -> bool:
        """
        Register current worker instance and its supported node types

        Args:
            node_types: List of node types supported by this worker, use locally registered types if None

        Returns:
            bool: Whether registration was successful
        """
        if not self.redis:
            logger.error("Cannot register worker: Redis not connected")
            return False

        try:
            # Use provided node types or locally registered types
            supported_types = node_types or list(self.supported_node_types)

            # Check if there are supported node types
            if not supported_types:
                logger.warning("Worker %s has no supported node types", self.worker_id)
                return False

            # Prepare worker data
            worker_key = f"worker:{self.worker_id}"
            worker_data = {
                "id": self.worker_id,
                "api_url": self.api_url,
                "supported_node_types": json.dumps(supported_types),
                "last_heartbeat": datetime.now().isoformat(),
                "status": "active",
                "registered_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }

            # Save worker information, set expiration time
            await self.redis.hset(worker_key, mapping=worker_data)
            await self.redis.expire(worker_key, self.node_ttl)

            # For each node type, add mapping between this worker and node type
            for node_type in supported_types:
                await self._register_node_type_mapping(node_type)

            # logger.info(
            #     "Worker %s has been registered, supported node types: %s, api_url: %s",
            #     self.worker_id,
            #     ", ".join(supported_types),
            #     self.api_url,
            # )
            return True
        except Exception as e:
            logger.error("Failed to register Worker %s: %s", self.worker_id, str(e))
            return False

    async def _register_node_type_mapping(self, node_type: str) -> bool:
        """
        Register mapping between node type and worker

        Args:
            node_type: Node type

        Returns:
            bool: Whether registration was successful
        """
        try:
            # Register node type metadata
            node_key = f"node_type:{node_type}"
            node_data = {"type": node_type, "updated_at": datetime.now().isoformat()}

            # Save node type information
            await self.redis.hset(node_key, mapping=node_data)

            # Add to node type set
            await self.redis.sadd("node_types", node_type)

            # Mapping from node type to workers
            type_workers_key = f"node_type:{node_type}:workers"
            await self.redis.sadd(type_workers_key, self.worker_id)

            # Set expiration time for mapping
            await self.redis.expire(type_workers_key, self.node_ttl)

            return True
        except Exception as e:
            logger.error(
                "Failed to register node type mapping %s: %s", node_type, str(e)
            )
            return False

    async def unregister_worker(self) -> bool:
        """
        Unregister current worker instance

        Returns:
            bool: Whether unregistration was successful
        """
        if not self.redis:
            return False

        try:
            # Get node types supported by worker
            worker_key = f"worker:{self.worker_id}"
            worker_data = await self.redis.hgetall(worker_key)

            if worker_data:
                supported_types = json.loads(
                    worker_data.get("supported_node_types", "[]")
                )

                # Remove this worker from each node type's worker set
                for node_type in supported_types:
                    type_workers_key = f"node_type:{node_type}:workers"
                    await self.redis.srem(type_workers_key, self.worker_id)

                # Delete worker information
                await self.redis.delete(worker_key)

                logger.info("Worker %s has been unregistered", self.worker_id)

            return True
        except Exception as e:
            logger.error("Failed to unregister Worker %s: %s", self.worker_id, str(e))
            return False

    async def start_heartbeat(self):
        """Start periodic heartbeat task to update worker status"""
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._is_running = True
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            logger.info("Worker %s heartbeat task started", self.worker_id)

    async def stop_heartbeat(self):
        """Stop heartbeat task"""
        self._is_running = False
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

            logger.info("Worker %s heartbeat task stopped", self.worker_id)

    async def _heartbeat_loop(self):
        """Heartbeat loop, periodically update worker status and node type mappings"""
        try:
            while self._is_running:
                # First sync node type information from Redis
                await self.sync_node_types_from_redis()
                # Re-register worker information and all node type mappings
                # This is more complete than a simple heartbeat, ensuring system state is always up to date
                await self.register_worker()

                # Wait until next heartbeat cycle
                await asyncio.sleep(self.heartbeat_interval)
        except asyncio.CancelledError:
            # Task cancelled, exit normally
            pass
        except Exception as e:
            logger.error("Worker %s heartbeat task error: %s", self.worker_id, str(e))
            # Try to restart heartbeat task
            asyncio.create_task(self._restart_heartbeat())

    async def _restart_heartbeat(self):
        """Restart heartbeat task"""
        await asyncio.sleep(5)  # Wait 5 seconds before retrying
        if self._is_running:
            logger.info("Attempting to restart heartbeat task")
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _send_heartbeat(self) -> bool:
        """
        Send heartbeat signal to update worker status

        Returns:
            bool: Whether heartbeat was successful
        """
        if not self.redis:
            return False

        try:
            worker_key = f"worker:{self.worker_id}"

            # Update last heartbeat time
            heartbeat_data = {
                "last_heartbeat": datetime.now().isoformat(),
                "status": "active",
                "updated_at": datetime.now().isoformat(),
            }

            # Update heartbeat information, reset expiration time
            await self.redis.hset(worker_key, mapping=heartbeat_data)
            await self.redis.expire(worker_key, self.node_ttl)

            return True
        except Exception as e:
            logger.error("Failed to send heartbeat: %s", str(e))
            return False

    # ========== Query methods ==========

    async def get_all_node_types(self) -> List[Dict]:
        """
        Get all registered node types

        Returns:
            List[Dict]: List of node types
        """
        if not self.redis:
            return []

        try:
            # Get all node type IDs
            node_type_ids = await self.redis.smembers("node_types")

            # Get detailed information for each node type
            result = []
            for node_type in node_type_ids:
                type_data = await self.redis.hgetall(f"node_type:{node_type}")
                if type_data:
                    # Get number of workers supporting this type
                    type_workers = await self.redis.smembers(
                        f"node_type:{node_type}:workers"
                    )
                    type_data["worker_count"] = len(type_workers)

                    result.append(type_data)

            return result
        except Exception as e:
            logger.error("Failed to get all node types: %s", str(e))
            return []

    async def get_all_workers(self) -> List[Dict]:
        """
        Get all registered workers

        Returns:
            List[Dict]: List of workers
        """
        if not self.redis:
            return []

        try:
            # Get all worker keys
            keys = await self.redis.keys("worker:*")

            # Get detailed information for each worker
            result = []
            for key in keys:
                worker_data = await self.redis.hgetall(key)
                if worker_data:
                    # Parse JSON fields
                    if "supported_node_types" in worker_data:
                        try:
                            worker_data["supported_node_types"] = json.loads(
                                worker_data["supported_node_types"]
                            )
                        except:
                            worker_data["supported_node_types"] = []

                    # Check if active
                    try:
                        last_heartbeat = datetime.fromisoformat(
                            worker_data.get("last_heartbeat", "")
                        )
                        time_since_heartbeat = datetime.now() - last_heartbeat
                        worker_data["is_alive"] = time_since_heartbeat < timedelta(
                            seconds=self.node_ttl
                        )
                    except:
                        worker_data["is_alive"] = False

                    result.append(worker_data)

            return result
        except Exception as e:
            logger.error("Failed to get all workers: %s", str(e))
            return []

    async def find_workers_for_node_type(
        self, node_type: str, only_active: bool = True
    ) -> List[Dict]:
        """
        Find workers that can handle specified node type

        Args:
            node_type: Node type identifier
            only_active: Whether to return only active workers

        Returns:
            List[Dict]: List of matching workers
        """
        if not self.redis:
            return []

        try:
            # Get list of worker IDs supporting this node type
            type_workers_key = f"node_type:{node_type}:workers"
            worker_ids = await self.redis.smembers(type_workers_key)

            if not worker_ids:
                return []

            # Get detailed information for each worker
            result = []
            for worker_id in worker_ids:
                worker_key = f"worker:{worker_id}"
                worker_data = await self.redis.hgetall(worker_key)

                if not worker_data:
                    continue

                # Parse JSON fields
                if "supported_node_types" in worker_data:
                    try:
                        worker_data["supported_node_types"] = json.loads(
                            worker_data["supported_node_types"]
                        )
                    except:
                        worker_data["supported_node_types"] = []

                # Check if active
                is_alive = True
                if only_active:
                    try:
                        last_heartbeat = datetime.fromisoformat(
                            worker_data.get("last_heartbeat", "")
                        )
                        time_since_heartbeat = datetime.now() - last_heartbeat
                        is_alive = time_since_heartbeat < timedelta(
                            seconds=self.node_ttl
                        )
                    except:
                        is_alive = False

                if not only_active or is_alive:
                    worker_data["is_alive"] = is_alive
                    result.append(worker_data)

            return result
        except Exception as e:
            logger.error(
                "Failed to find workers for node type %s: %s", node_type, str(e)
            )
            return []

    async def get_worker_by_id(self, worker_id: str) -> Optional[Dict]:
        """
        Get worker information by ID

        Args:
            worker_id: Worker identifier

        Returns:
            Dict: Worker information, or None if not found
        """
        if not self.redis:
            return None

        try:
            worker_key = f"worker:{worker_id}"
            worker_data = await self.redis.hgetall(worker_key)

            if not worker_data:
                return None

            # Parse JSON fields
            if "supported_node_types" in worker_data:
                try:
                    worker_data["supported_node_types"] = json.loads(
                        worker_data["supported_node_types"]
                    )
                except:
                    worker_data["supported_node_types"] = []

            # Check if active
            try:
                last_heartbeat = datetime.fromisoformat(
                    worker_data.get("last_heartbeat", "")
                )
                time_since_heartbeat = datetime.now() - last_heartbeat
                worker_data["is_alive"] = time_since_heartbeat < timedelta(
                    seconds=self.node_ttl
                )
            except:
                worker_data["is_alive"] = False

            return worker_data
        except Exception as e:
            logger.error("Failed to get worker %s information: %s", worker_id, str(e))
            return None

    # Add new method
    async def sync_node_types_from_redis(self):
        """Sync node type information from Redis to local"""
        if not self.redis:
            return

        try:
            # Get this worker's persistent node types
            worker_node_types_key = f"worker:{self.worker_id}:node_types"
            registered_types = await self.redis.smembers(worker_node_types_key)

            # Update local set
            for node_type in registered_types:
                self.supported_node_types.add(node_type)

            logger.debug("Synced node types from Redis: %s", self.supported_node_types)
        except Exception as e:
            logger.error("Failed to sync node types from Redis: %s", str(e))

    async def _sync_node_type_to_redis(
        self, node_type: str, default_params: Dict = None
    ):
        """
        Sync node type to Redis

        Args:
            node_type: Node type
            default_params: Default parameters for node type
        """
        if not self.redis:
            return

        try:
            # Store node type in Redis
            node_key = f"node_type:{node_type}"
            await self.redis.hset(node_key, "type", node_type)
            await self.redis.hset(node_key, "updated_at", datetime.now().isoformat())

            # If there are default parameters, store them as well
            if default_params:
                param_json = json.dumps(default_params)
                await self.redis.hset(node_key, "default_params", param_json)

            # Add to node types set
            await self.redis.sadd("node_types", node_type)

            # Add to this worker's node types
            worker_node_types_key = f"worker:{self.worker_id}:node_types"
            await self.redis.sadd(worker_node_types_key, node_type)

            # logger.debug("Synced node type %s to Redis", node_type)
        except Exception as e:
            logger.error("Failed to sync node type %s to Redis: %s", node_type, str(e))
