import abc
import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

# Redis client dependency
import redis.asyncio as aioredis


class StateStore(abc.ABC):
    """State store abstract base class, serves as anti-corruption layer to isolate storage implementation"""

    @abc.abstractmethod
    async def initialize(self) -> bool:
        """Initialize storage connection."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Close storage connection."""

    @abc.abstractmethod
    async def set_node_task_status(
        self, node_task_id: str, status: str, error_message: Optional[str] = None
    ) -> bool:
        """Set node status."""

    @abc.abstractmethod
    async def get_node_task_status(self, node_task_id: str) -> Dict[str, Any]:
        """Get node status."""

    @abc.abstractmethod
    async def set_termination_flag(
        self, node_task_id: str, reason: Optional[str] = None
    ) -> bool:
        """Set node termination flag."""

    @abc.abstractmethod
    async def get_termination_flag(self, node_task_id: str) -> Optional[Dict[str, Any]]:
        """Get node termination flag."""

    @abc.abstractmethod
    async def clear_termination_flag(self, node_task_id: str) -> bool:
        """Clear node termination flag."""

    # Generic key-value store methods for NodeManager
    @abc.abstractmethod
    async def set_value(self, key: str, value: Any) -> bool:
        """Set key-value."""

    @abc.abstractmethod
    async def get_value(self, key: str) -> Any:
        """Get key-value."""

    @abc.abstractmethod
    async def delete_value(self, key: str) -> bool:
        """Delete key-value."""

    @abc.abstractmethod
    async def add_to_set(self, key: str, value: Any) -> bool:
        """Add value to set."""

    @abc.abstractmethod
    async def remove_from_set(self, key: str, value: Any) -> bool:
        """Remove value from set."""

    @abc.abstractmethod
    async def get_set_members(self, key: str) -> List[Any]:
        """Get all members of set."""


class RedisStateStore(StateStore):
    """Redis implementation of state store"""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        key_prefix: str = "trading_flow:",
    ):
        """
        Initialize Redis state store.

        Args:
            redis_url: Redis connection URL
            key_prefix: Redis key prefix
        """
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.redis_client = None
        self.logger = logging.getLogger("RedisStateStore")

    async def initialize(self) -> bool:
        """
        Initialize Redis connection.

        Returns:
            bool: Whether initialization was successful
        """
        try:
            self.redis_client = await aioredis.from_url(self.redis_url)
            self.logger.info("Redis state store initialized")
            return True
        except Exception as e:
            self.logger.error("Failed to initialize Redis state store: %s", str(e))
            return False

    async def close(self) -> None:
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            self.logger.info("Redis state store connection closed")

    async def set_node_task_status(
        self, node_task_id: str, status: str, error_message: Optional[str] = None
    ) -> bool:
        """
        Set node status.

        Args:
            node_task_id: Node ID
            status: Status value
            error_message: Error message (if any)

        Returns:
            bool: Whether operation was successful
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            # Update node status in Redis
            node_key = f"{self.key_prefix}node:{node_task_id}"
            node_data = {
                "status": status,
                "updated_at": asyncio.get_event_loop().time(),
            }

            if error_message:
                node_data["error_message"] = error_message

            # Use hash to store node data
            await self.redis_client.hset(node_key, mapping=node_data)
            # Set appropriate expiration time, e.g., 24 hours
            await self.redis_client.expire(node_key, 86400)
            return True
        except Exception as e:
            self.logger.error("Failed to set node status: %s", str(e))
            return False

    async def get_node_task_status(self, node_task_id: str) -> Dict[str, Any]:
        """
        Get node status.

        Args:
            node_task_id: Node ID

        Returns:
            Dict[str, Any]: Node status data
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return {}

            node_key = f"{self.key_prefix}node:{node_task_id}"
            node_data = await self.redis_client.hgetall(node_key)

            # Convert bytes to string
            result = {}
            for k, v in node_data.items():
                key = k.decode("utf-8") if isinstance(k, bytes) else k
                value = v.decode("utf-8") if isinstance(v, bytes) else v
                result[key] = value

            return result
        except Exception as e:
            self.logger.error("Failed to get node status: %s", str(e))
            return {}

    async def set_termination_flag(
        self, node_task_id: str, reason: Optional[str] = None
    ) -> bool:
        """
        Set node termination flag.

        Args:
            node_task_id: Node ID
            reason: Termination reason

        Returns:
            bool: Whether operation was successful
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            terminate_key = f"{self.key_prefix}node:{node_task_id}:terminate"
            terminate_data = {
                "reason": reason or "No reason provided",
                "timestamp": asyncio.get_event_loop().time(),
            }

            await self.redis_client.set(
                terminate_key, json.dumps(terminate_data), ex=3600  # Set 1 hour expiration
            )
            return True
        except Exception as e:
            self.logger.error("Failed to set termination flag: %s", str(e))
            return False

    async def get_termination_flag(self, node_task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get node termination flag.

        Args:
            node_task_id: Node ID

        Returns:
            Optional[Dict[str, Any]]: Termination flag data, None if not exists
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return None

            terminate_key = f"{self.key_prefix}node:{node_task_id}:terminate"
            terminate_data = await self.redis_client.get(terminate_key)

            if terminate_data:
                return json.loads(terminate_data)
            return None
        except Exception as e:
            self.logger.error("Failed to get termination flag: %s", str(e))
            return None

    async def clear_termination_flag(self, node_task_id: str) -> bool:
        """
        Clear node termination flag.

        Args:
            node_task_id: Node ID

        Returns:
            bool: Whether operation was successful
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            terminate_key = f"{self.key_prefix}node:{node_task_id}:terminate"
            await self.redis_client.delete(terminate_key)
            return True
        except Exception as e:
            self.logger.error("Failed to clear termination flag: %s", str(e))
            return False

    async def set_value(self, key: str, value: Any) -> bool:
        """
        Set key-value.

        Args:
            key: Key name
            value: Value (will be JSON serialized)

        Returns:
            bool: Whether operation was successful
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            # Serialize value to JSON
            value_str = json.dumps(value)
            await self.redis_client.set(key, value_str)
            return True
        except Exception as e:
            self.logger.error(f"Failed to set value for key {key}: {str(e)}")
            return False

    async def get_value(self, key: str) -> Any:
        """
        Get key-value.

        Args:
            key: Key name

        Returns:
            Any: Value (JSON deserialized object)
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return None

            value = await self.redis_client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            self.logger.error(f"Failed to get value for key {key}: {str(e)}")
            return None

    async def delete_value(self, key: str) -> bool:
        """
        Delete key-value.

        Args:
            key: Key name

        Returns:
            bool: Whether operation was successful
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            await self.redis_client.delete(key)
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete key {key}: {str(e)}")
            return False

    async def add_to_set(self, key: str, value: Any) -> bool:
        """
        Add value to set.

        Args:
            key: Set key name
            value: Value to add

        Returns:
            bool: Whether operation was successful
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            # Redis SADD command
            await self.redis_client.sadd(key, str(value))
            return True
        except Exception as e:
            self.logger.error(f"Failed to add value to set {key}: {str(e)}")
            return False

    async def remove_from_set(self, key: str, value: Any) -> bool:
        """
        Remove value from set.

        Args:
            key: Set key name
            value: Value to remove

        Returns:
            bool: Whether operation was successful
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            # Redis SREM command
            await self.redis_client.srem(key, str(value))
            return True
        except Exception as e:
            self.logger.error(f"Failed to remove value from set {key}: {str(e)}")
            return False

    async def get_set_members(self, key: str) -> List[Any]:
        """
        Get all members of set.

        Args:
            key: Set key name

        Returns:
            List[Any]: List of set members
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return []

            # Redis SMEMBERS command
            members = await self.redis_client.smembers(key)
            # Convert bytes to string
            return [
                member.decode("utf-8") if isinstance(member, bytes) else member
                for member in members
            ]
        except Exception as e:
            self.logger.error(f"Failed to get members of set {key}: {str(e)}")
            return []


class InMemoryStateStore(StateStore):
    """In-memory implementation of state store, suitable for testing and single-process environments"""

    def __init__(self):
        """Initialize in-memory state store."""
        self.node_status = {}  # Store node status
        self.termination_flags = {}  # Store termination flags
        self.key_values = {}  # Store key-value pairs
        self.sets = {}  # Store sets
        self.logger = logging.getLogger("InMemoryStateStore")

    async def initialize(self) -> bool:
        """
        Initialize storage connection.

        Returns:
            bool: Always returns True
        """
        self.logger.info("In-memory state store initialized")
        return True

    async def close(self) -> None:
        """Close storage connection."""
        self.logger.info("In-memory state store closed")

    async def set_node_task_status(
        self, node_task_id: str, status: str, error_message: Optional[str] = None
    ) -> bool:
        """
        Set node status.

        Args:
            node_task_id: Node ID
            status: Status value
            error_message: Error message (if any)

        Returns:
            bool: Whether operation was successful
        """
        try:
            self.node_status[node_task_id] = {
                "status": status,
                "updated_at": asyncio.get_event_loop().time(),
            }

            if error_message:
                self.node_status[node_task_id]["error_message"] = error_message

            return True
        except Exception as e:
            self.logger.error("Failed to set node status: %s", str(e))
            return False

    async def get_node_task_status(self, node_task_id: str) -> Dict[str, Any]:
        """
        Get node status.

        Args:
            node_task_id: Node ID

        Returns:
            Dict[str, Any]: Node status data
        """
        return self.node_status.get(node_task_id, {})

    async def set_termination_flag(
        self, node_task_id: str, reason: Optional[str] = None
    ) -> bool:
        """
        Set node termination flag.

        Args:
            node_task_id: Node ID
            reason: Termination reason

        Returns:
            bool: Whether operation was successful
        """
        try:
            self.termination_flags[node_task_id] = {
                "reason": reason or "No reason provided",
                "timestamp": asyncio.get_event_loop().time(),
            }
            return True
        except Exception as e:
            self.logger.error("Failed to set termination flag: %s", str(e))
            return False

    async def get_termination_flag(self, node_task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get node termination flag.

        Args:
            node_task_id: Node ID

        Returns:
            Optional[Dict[str, Any]]: Termination flag data, None if not exists
        """
        return self.termination_flags.get(node_task_id)

    async def clear_termination_flag(self, node_task_id: str) -> bool:
        """
        Clear node termination flag.

        Args:
            node_task_id: Node ID

        Returns:
            bool: Whether operation was successful
        """
        try:
            if node_task_id in self.termination_flags:
                del self.termination_flags[node_task_id]
            return True
        except Exception as e:
            self.logger.error("Failed to clear termination flag: %s", str(e))
            return False

    async def set_value(self, key: str, value: Any) -> bool:
        """Set key-value."""
        try:
            self.key_values[key] = value
            return True
        except Exception as e:
            self.logger.error(f"Failed to set value for key {key}: {str(e)}")
            return False

    async def get_value(self, key: str) -> Any:
        """Get key-value."""
        return self.key_values.get(key)

    async def delete_value(self, key: str) -> bool:
        """Delete key-value."""
        try:
            if key in self.key_values:
                del self.key_values[key]
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete key {key}: {str(e)}")
            return False

    async def add_to_set(self, key: str, value: Any) -> bool:
        """Add value to set."""
        try:
            if key not in self.sets:
                self.sets[key] = set()
            self.sets[key].add(str(value))
            return True
        except Exception as e:
            self.logger.error(f"Failed to add value to set {key}: {str(e)}")
            return False

    async def remove_from_set(self, key: str, value: Any) -> bool:
        """Remove value from set."""
        try:
            if key in self.sets and str(value) in self.sets[key]:
                self.sets[key].remove(str(value))
            return True
        except Exception as e:
            self.logger.error(f"Failed to remove value from set {key}: {str(e)}")
            return False

    async def get_set_members(self, key: str) -> List[Any]:
        """Get all members of set."""
        try:
            return list(self.sets.get(key, set()))
        except Exception as e:
            self.logger.error(f"Failed to get members of set {key}: {str(e)}")
            return []


# Factory class to create appropriate storage implementation
class StateStoreFactory:
    """State store factory class"""

    @staticmethod
    def create(store_type: str = "redis", config: Dict[str, Any] = None) -> StateStore:
        """
        Create state store instance.

        Args:
            store_type: Storage type, supports "redis" and "memory", reads from CONFIG if None
            config: Configuration params, reads from CONFIG if None

        Returns:
            StateStore: State store instance
        """
        # Import CONFIG
        from infra.config import CONFIG

        # If store_type not provided, read from CONFIG
        if store_type is None:
            store_type = CONFIG.get("STATE_STORE_TYPE", "redis")

        # If config not provided, read from CONFIG
        if config is None:
            config = {
                "redis_url": CONFIG.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
                "key_prefix": "trading_flow:",
            }

        if store_type == "redis":
            return RedisStateStore(
                redis_url=config.get("redis_url", CONFIG.get("REDIS_URL", "redis://127.0.0.1:6379/0")),
                key_prefix=config.get("key_prefix", "trading_flow:"),
            )
        elif store_type == "memory":
            return InMemoryStateStore()
        else:
            raise ValueError(f"Unsupported state store type: {store_type}")
