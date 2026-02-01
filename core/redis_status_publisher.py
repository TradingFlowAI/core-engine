"""
Redis Status Publisher

Responsibilities:
- Publish node status changes to Redis Pub/Sub
- For Control service to subscribe and forward to frontend WebSocket
"""

import redis
import json
import os
from typing import Dict, Any, Optional
from datetime import datetime
from infra.config import CONFIG

class RedisStatusPublisher:
    """Redis Status Publisher"""

    def __init__(self):
        """Initialize Redis connection."""
        # Use auto-encoded password URL from CONFIG
        redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
        self.redis_client = redis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )

        # Test connection
        try:
            self.redis_client.ping()
            print("[RedisStatusPublisher] Connected to Redis successfully")
        except redis.ConnectionError as e:
            print(f"[RedisStatusPublisher] Warning: Failed to connect to Redis: {e}")
            print("[RedisStatusPublisher] Status streaming will be disabled")

    def publish_node_status(
        self,
        flow_id: str,
        cycle: int,
        node_id: str,
        status: str,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        execution_type: str = "global",
        execution_id: Optional[str] = None,
    ) -> bool:
        """
        Publish node status change to Redis channel.

        Args:
            flow_id: Flow ID
            cycle: Execution cycle
            node_id: Node ID
            status: Node status (pending, running, completed, failed, skipped, terminated)
            error_message: Error message (if any)
            metadata: Additional metadata
            execution_type: "global" or "partial"
            execution_id: Partial run execution ID (if applicable)

        Returns:
            bool: Whether publish was successful
        """
        try:
            # Build channel name
            channel = f"status:flow:{flow_id}:cycle:{cycle}"

            # Build status update message
            status_update = {
                "timestamp": datetime.now().isoformat(),
                "flow_id": flow_id,
                "cycle": cycle,
                "node_id": node_id,
                "status": status,
                "error_message": error_message,
                "metadata": metadata or {},
                "execution_type": execution_type,
            }

            # Add execution_id for partial runs
            if execution_id:
                status_update["execution_id"] = execution_id

            # Serialize to JSON
            message = json.dumps(status_update)

            # Publish to Redis
            self.redis_client.publish(channel, message)

            # 🔍 调试：如果有 lastOutput，打印日志
            if metadata and metadata.get('lastOutput'):
                print(f"[RedisStatusPublisher] Published status with lastOutput: {node_id} -> {status}")
            else:
                print(f"[RedisStatusPublisher] Published status: {node_id} -> {status}")
            return True

        except redis.RedisError as e:
            print(f"[RedisStatusPublisher] Failed to publish status to Redis: {e}")
            return False
        except Exception as e:
            print(f"[RedisStatusPublisher] Unexpected error publishing status: {e}")
            return False

    def publish_flow_status(
        self,
        flow_id: str,
        cycle: int,
        status: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Publish overall Flow status change.

        Args:
            flow_id: Flow ID
            cycle: Execution cycle
            status: Flow status (running, completed, failed, cancelled)
            metadata: Additional metadata

        Returns:
            bool: Whether publish was successful
        """
        try:
            channel = f"status:flow:{flow_id}:cycle:{cycle}"

            flow_status_update = {
                "timestamp": datetime.now().isoformat(),
                "flow_id": flow_id,
                "cycle": cycle,
                "type": "flow",
                "status": status,
                "metadata": metadata or {}
            }

            message = json.dumps(flow_status_update)
            self.redis_client.publish(channel, message)

            print(f"[RedisStatusPublisher] Published flow status: {flow_id} -> {status}")
            return True

        except redis.RedisError as e:
            print(f"[RedisStatusPublisher] Failed to publish flow status: {e}")
            return False
        except Exception as e:
            print(f"[RedisStatusPublisher] Unexpected error: {e}")
            return False

    def close(self):
        """Close Redis connection."""
        try:
            self.redis_client.close()
            print("[RedisStatusPublisher] Redis connection closed")
        except Exception as e:
            print(f"[RedisStatusPublisher] Error closing Redis connection: {e}")


# Global singleton instance
_status_publisher = None


def get_status_publisher() -> RedisStatusPublisher:
    """
    Get Redis status publisher singleton instance.

    Returns:
        RedisStatusPublisher: Status publisher instance
    """
    global _status_publisher
    if _status_publisher is None:
        _status_publisher = RedisStatusPublisher()
    return _status_publisher


def publish_node_status(
    flow_id: str,
    cycle: int,
    node_id: str,
    status: str,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    execution_type: str = "global",
    execution_id: Optional[str] = None,
) -> bool:
    """
    Convenience function: Publish node status to Redis.

    Args:
        flow_id: Flow ID
        cycle: Execution cycle
        node_id: Node ID
        status: Node status
        error_message: Error message
        metadata: Metadata
        execution_type: "global" or "partial"
        execution_id: Partial run execution ID

    Returns:
        bool: Whether publish was successful
    """
    publisher = get_status_publisher()
    return publisher.publish_node_status(
        flow_id, cycle, node_id, status, error_message, metadata,
        execution_type, execution_id
    )


def publish_flow_status(
    flow_id: str,
    cycle: int,
    status: str,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Convenience function: Publish Flow status to Redis.
    """
    publisher = get_status_publisher()
    return publisher.publish_flow_status(flow_id, cycle, status, metadata)


def close_status_publisher():
    """Close status publisher."""
    global _status_publisher
    if _status_publisher is not None:
        _status_publisher.close()
        _status_publisher = None
