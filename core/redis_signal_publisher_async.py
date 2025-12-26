"""
Async Redis Signal Publisher

Responsibilities:
- Async publish inter-node signals to Redis Pub/Sub
- For Control service to subscribe and forward to frontend WebSocket
- Used for debugging and monitoring data flow between nodes
"""

import asyncio
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

try:
    import redis.asyncio as aioredis
except ImportError:
    import aioredis

from infra.config import CONFIG

logger = logging.getLogger(__name__)


class AsyncRedisSignalPublisher:
    """Async Redis Signal Publisher"""

    def __init__(self):
        """Initialize (lazy connection)."""
        self.redis_client: Optional[aioredis.Redis] = None
        self._connected = False
        self._lock = asyncio.Lock()

        # Statistics
        self._publish_success_count = 0
        self._publish_failure_count = 0

        logger.info("[AsyncRedisSignalPublisher] Initialized (connection will be lazy-loaded)")

    async def connect(self):
        """Connect to Redis."""
        if self._connected:
            return

        async with self._lock:
            if self._connected:
                return

            try:
                redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")

                self.redis_client = await aioredis.from_url(
                    redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    max_connections=10,
                )

                await self.redis_client.ping()
                self._connected = True

                logger.info(
                    "[AsyncRedisSignalPublisher] Connected to Redis: %s",
                    redis_url
                )
            except Exception as e:
                logger.error(
                    "[AsyncRedisSignalPublisher] Failed to connect to Redis: %s",
                    str(e)
                )
                self.redis_client = None
                self._connected = False
                raise

    async def ensure_connected(self):
        """Ensure connected to Redis."""
        if not self._connected:
            await self.connect()

    async def publish_signal(
        self,
        flow_id: str,
        cycle: int,
        signal_data: Dict[str, Any],
        max_retries: int = 2,
        retry_delay: float = 0.1
    ) -> bool:
        """
        Async publish signal to Redis channel.

        Args:
            flow_id: Flow ID
            cycle: Execution cycle
            signal_data: Signal data, should contain:
                - direction: 'input' | 'output'
                - from_node_id: Source node ID
                - to_node_id: Target node ID
                - handle_id: Handle ID
                - payload: Transmitted data
                - data_type: Data type identifier
            max_retries: Maximum retry attempts
            retry_delay: Retry delay (seconds)

        Returns:
            bool: Whether publish was successful
        """
        try:
            await self.ensure_connected()
        except Exception as e:
            logger.warning(
                "[AsyncRedisSignalPublisher] Cannot connect to Redis, skipping signal publish: %s",
                str(e)
            )
            self._publish_failure_count += 1
            return False

        if not self.redis_client:
            self._publish_failure_count += 1
            return False

        # Build channel name
        channel = f"signal:flow:{flow_id}"

        # Build complete signal message
        complete_signal = {
            "type": "signal",
            "timestamp": datetime.now().isoformat(),
            "flow_id": flow_id,
            "cycle": cycle,
            **signal_data
        }

        # Try to publish (with retry)
        for attempt in range(max_retries):
            try:
                message = json.dumps(complete_signal)
                await self.redis_client.publish(channel, message)
                self._publish_success_count += 1

                logger.debug(
                    "[AsyncRedisSignalPublisher] Signal published: %s -> %s",
                    signal_data.get("from_node_id", "?"),
                    signal_data.get("to_node_id", "?")
                )

                return True

            except (aioredis.RedisError, aioredis.ConnectionError) as e:
                self._publish_failure_count += 1

                if attempt < max_retries - 1:
                    logger.warning(
                        "[AsyncRedisSignalPublisher] Failed to publish signal (attempt %d/%d): %s",
                        attempt + 1, max_retries, str(e)
                    )
                    await asyncio.sleep(retry_delay * (attempt + 1))

                    try:
                        self._connected = False
                        await self.connect()
                    except Exception:
                        pass
                else:
                    logger.error(
                        "[AsyncRedisSignalPublisher] Failed to publish signal after %d attempts: %s",
                        max_retries, str(e)
                    )
                    return False

            except Exception as e:
                self._publish_failure_count += 1
                logger.error(
                    "[AsyncRedisSignalPublisher] Unexpected error publishing signal: %s",
                    str(e)
                )
                return False

        return False

    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            try:
                await self.redis_client.close()
                await self.redis_client.connection_pool.disconnect()
                self._connected = False
                logger.info("[AsyncRedisSignalPublisher] Redis connection closed")
            except Exception as e:
                logger.warning(
                    "[AsyncRedisSignalPublisher] Error closing Redis connection: %s",
                    str(e)
                )

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics."""
        total_count = self._publish_success_count + self._publish_failure_count
        success_rate = (
            self._publish_success_count / total_count * 100
            if total_count > 0
            else 0.0
        )

        return {
            "connected": self._connected,
            "success_count": self._publish_success_count,
            "failure_count": self._publish_failure_count,
            "total_count": total_count,
            "success_rate": round(success_rate, 2),
        }


# Global singleton instance
_async_signal_publisher: Optional[AsyncRedisSignalPublisher] = None
# Fix: Lazy create Lock to avoid event loop not initialized issue during module load
_publisher_lock: Optional[asyncio.Lock] = None


def _get_publisher_lock() -> asyncio.Lock:
    """Get or create publisher lock."""
    global _publisher_lock
    if _publisher_lock is None:
        _publisher_lock = asyncio.Lock()
    return _publisher_lock


async def get_async_signal_publisher() -> AsyncRedisSignalPublisher:
    """Get async Redis signal publisher singleton instance."""
    global _async_signal_publisher

    if _async_signal_publisher is None:
        lock = _get_publisher_lock()
        async with lock:
            if _async_signal_publisher is None:
                _async_signal_publisher = AsyncRedisSignalPublisher()
                try:
                    await _async_signal_publisher.connect()
                except Exception as e:
                    logger.warning(
                        "Failed to connect async signal publisher on initialization: %s",
                        str(e)
                    )

    return _async_signal_publisher


async def publish_signal_async(
    flow_id: str,
    cycle: int,
    source_node_id: str,
    source_handle: str,
    target_node_ids: list = None,
    signal_type: str = "ANY",
    payload: Any = None,
    direction: str = "output",  # 'input' | 'output'
    data_type: str = "unknown",
    max_retries: int = 2,
    # Backward compatibility parameters
    from_node_id: str = None,
    to_node_id: str = None,
    handle_id: str = None,
) -> bool:
    """
    Convenience function: Async publish signal to Redis.

    Args:
        flow_id: Flow ID
        cycle: Execution cycle
        source_node_id: Source node ID
        source_handle: Source Handle ID
        target_node_ids: Target node ID list
        signal_type: Signal type (e.g., 'ANY', 'NUMBER', 'STRING')
        payload: Transmitted data
        direction: Direction ('input' or 'output')
        data_type: Data type identifier
        max_retries: Maximum retry attempts
        from_node_id: (compat) Source node ID
        to_node_id: (compat) Target node ID
        handle_id: (compat) Handle ID

    Returns:
        bool: Whether publish was successful
    """
    try:
        publisher = await get_async_signal_publisher()
        
        # Backward compatibility for old parameters
        actual_from_node = source_node_id or from_node_id or ""
        actual_targets = target_node_ids or ([to_node_id] if to_node_id else [])
        
        # Determine handle_id: prefer explicitly passed handle_id, otherwise use source_handle
        # For input signal, handle_id should be target handle (receiver)
        # For output signal, handle_id should be source handle (sender)
        actual_handle = handle_id or source_handle or ""
        
        signal_data = {
            "direction": direction,
            "from_node_id": actual_from_node,
            "to_node_ids": actual_targets,  # Note: changed to array format
            "to_node_id": actual_targets[0] if actual_targets else None,  # Backward compat
            "handle_id": actual_handle,
            "signal_type": signal_type,
            "payload": _serialize_payload(payload),
            "data_type": data_type,
        }

        return await publisher.publish_signal(flow_id, cycle, signal_data, max_retries)
    except Exception as e:
        logger.error("Failed to publish signal via async publisher: %s", str(e))
        return False


def _serialize_payload(payload: Any) -> Any:
    """
    Serialize payload, handle types that cannot be directly JSON serialized.
    """
    if payload is None:
        return None
    
    if isinstance(payload, (str, int, float, bool)):
        return payload
    
    if isinstance(payload, (list, tuple)):
        return [_serialize_payload(item) for item in payload]
    
    if isinstance(payload, dict):
        return {k: _serialize_payload(v) for k, v in payload.items()}
    
    # For complex objects, convert to string representation
    try:
        return str(payload)
    except Exception:
        return "<unserializable>"


async def close_async_signal_publisher():
    """Close async signal publisher."""
    global _async_signal_publisher

    if _async_signal_publisher is not None:
        await _async_signal_publisher.close()
        _async_signal_publisher = None


async def get_signal_publisher_stats() -> Dict[str, Any]:
    """Get publisher statistics."""
    if _async_signal_publisher is None:
        return {
            "connected": False,
            "success_count": 0,
            "failure_count": 0,
            "total_count": 0,
            "success_rate": 0.0,
        }

    return _async_signal_publisher.get_stats()
