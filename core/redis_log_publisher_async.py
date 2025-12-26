"""
Async Redis Log Publisher

Responsibilities:
- Async publish node execution logs to Redis Pub/Sub
- For Control service to subscribe and forward to frontend WebSocket
- Supports connection pooling, error retry, performance monitoring
"""

import asyncio
import json
import logging
import os
from typing import Dict, Any, Optional
from datetime import datetime
from infra.config import CONFIG

try:
    import redis.asyncio as aioredis
except ImportError:
    # Fallback for older redis-py versions
    import aioredis

from core.metrics import (
    record_log_publish_success,
    record_log_publish_failure,
    record_log_publish_retry,
    set_redis_connection_status,
    is_metrics_enabled,
)

logger = logging.getLogger(__name__)


class AsyncRedisLogPublisher:
    """Async Redis Log Publisher"""

    def __init__(self):
        """Initialize (lazy connection)."""
        self.redis_client: Optional[aioredis.Redis] = None
        self._connected = False
        self._lock = asyncio.Lock()

        # Statistics
        self._publish_success_count = 0
        self._publish_failure_count = 0
        self._total_publish_time = 0.0

        logger.info("[AsyncRedisLogPublisher] Initialized (connection will be lazy-loaded)")

    async def connect(self):
        """Connect to Redis."""
        if self._connected:
            return

        async with self._lock:
            if self._connected:
                return

            try:
                # Use auto-encoded password URL from CONFIG
                redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")

                self.redis_client = await aioredis.from_url(
                    redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    max_connections=10,  # Connection pool size
                )

                # Test connection
                await self.redis_client.ping()
                self._connected = True

                # Update metrics
                set_redis_connection_status(True)

                logger.info(
                    "[AsyncRedisLogPublisher] Connected to Redis: %s",
                    redis_url
                )
            except Exception as e:
                logger.error(
                    "[AsyncRedisLogPublisher] Failed to connect to Redis: %s",
                    str(e)
                )
                logger.warning(
                    "[AsyncRedisLogPublisher] Log streaming will be disabled"
                )
                self.redis_client = None
                self._connected = False

                # Update metrics
                set_redis_connection_status(False)

                raise

    async def ensure_connected(self):
        """Ensure connected to Redis."""
        if not self._connected:
            await self.connect()

    async def publish_log(
        self,
        flow_id: str,
        cycle: int,
        log_entry: Dict[str, Any],
        max_retries: int = 3,
        retry_delay: float = 0.1
    ) -> bool:
        """
        Async publish log to Redis channel (with retry support).

        Args:
            flow_id: Flow ID
            cycle: Execution cycle
            log_entry: Log entry data
            max_retries: Maximum retry attempts
            retry_delay: Retry delay (seconds)

        Returns:
            bool: Whether publish was successful
        """
        # Ensure connected
        try:
            await self.ensure_connected()
        except Exception as e:
            logger.warning(
                "[AsyncRedisLogPublisher] Cannot connect to Redis, skipping log publish: %s",
                str(e)
            )
            self._publish_failure_count += 1
            return False

        if not self.redis_client:
            self._publish_failure_count += 1
            return False

        # Build channel name (cycle is in message body, not in channel name)
        channel = f"logs:flow:{flow_id}"

        # Ensure log entry contains required fields
        complete_log_entry = {
            "timestamp": datetime.now().isoformat(),
            "flow_id": flow_id,
            "cycle": cycle,
            **log_entry
        }

        # Try to publish (with retry)
        for attempt in range(max_retries):
            try:
                start_time = asyncio.get_event_loop().time()

                # Serialize to JSON
                message = json.dumps(complete_log_entry)

                # Publish to Redis
                await self.redis_client.publish(channel, message)

                # Statistics
                elapsed = asyncio.get_event_loop().time() - start_time
                self._total_publish_time += elapsed
                self._publish_success_count += 1

                # Record success metrics
                record_log_publish_success(flow_id, cycle, elapsed)

                if attempt > 0:
                    logger.info(
                        "[AsyncRedisLogPublisher] Log published successfully after %d retries",
                        attempt
                    )

                return True

            except (aioredis.RedisError, aioredis.ConnectionError) as e:
                self._publish_failure_count += 1

                # Record retry metrics
                if attempt < max_retries - 1:
                    record_log_publish_retry(flow_id, cycle, attempt + 1)

                    logger.warning(
                        "[AsyncRedisLogPublisher] Failed to publish log (attempt %d/%d): %s. Retrying...",
                        attempt + 1, max_retries, str(e)
                    )
                    await asyncio.sleep(retry_delay * (attempt + 1))  # Exponential backoff

                    # Try to reconnect
                    try:
                        self._connected = False
                        await self.connect()
                    except Exception:
                        pass
                else:
                    # Record failure metrics
                    error_type = type(e).__name__
                    record_log_publish_failure(flow_id, cycle, error_type)

                    logger.error(
                        "[AsyncRedisLogPublisher] Failed to publish log after %d attempts: %s",
                        max_retries, str(e)
                    )
                    return False
            except Exception as e:
                self._publish_failure_count += 1

                # Record failure metrics
                error_type = type(e).__name__
                record_log_publish_failure(flow_id, cycle, error_type)

                logger.error(
                    "[AsyncRedisLogPublisher] Unexpected error publishing log: %s",
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

                # Update metrics
                set_redis_connection_status(False)

                logger.info("[AsyncRedisLogPublisher] Redis connection closed")
            except Exception as e:
                logger.warning(
                    "[AsyncRedisLogPublisher] Error closing Redis connection: %s",
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
        avg_time = (
            self._total_publish_time / self._publish_success_count
            if self._publish_success_count > 0
            else 0.0
        )

        return {
            "connected": self._connected,
            "success_count": self._publish_success_count,
            "failure_count": self._publish_failure_count,
            "total_count": total_count,
            "success_rate": round(success_rate, 2),
            "avg_publish_time_ms": round(avg_time * 1000, 2),
        }


# Global singleton instance
_async_log_publisher: Optional[AsyncRedisLogPublisher] = None
_publisher_lock = asyncio.Lock()


async def get_async_log_publisher() -> AsyncRedisLogPublisher:
    """
    Get async Redis log publisher singleton instance.

    Returns:
        AsyncRedisLogPublisher: Async log publisher instance
    """
    global _async_log_publisher

    if _async_log_publisher is None:
        async with _publisher_lock:
            if _async_log_publisher is None:
                _async_log_publisher = AsyncRedisLogPublisher()
                try:
                    await _async_log_publisher.connect()
                except Exception as e:
                    logger.warning(
                        "Failed to connect async log publisher on initialization: %s",
                        str(e)
                    )

    return _async_log_publisher


async def publish_log_async(
    flow_id: str,
    cycle: int,
    log_entry: Dict[str, Any],
    max_retries: int = 3
) -> bool:
    """
    Convenience function: Async publish log to Redis.

    Args:
        flow_id: Flow ID
        cycle: Execution cycle
        log_entry: Log entry data
        max_retries: Maximum retry attempts

    Returns:
        bool: Whether publish was successful
    """
    try:
        publisher = await get_async_log_publisher()
        return await publisher.publish_log(flow_id, cycle, log_entry, max_retries)
    except Exception as e:
        logger.error("Failed to publish log via async publisher: %s", str(e))
        return False


async def close_async_log_publisher():
    """Close async log publisher."""
    global _async_log_publisher

    if _async_log_publisher is not None:
        await _async_log_publisher.close()
        _async_log_publisher = None


async def get_publisher_stats() -> Dict[str, Any]:
    """Get publisher statistics."""
    if _async_log_publisher is None:
        return {
            "connected": False,
            "success_count": 0,
            "failure_count": 0,
            "total_count": 0,
            "success_rate": 0.0,
            "avg_publish_time_ms": 0.0,
        }

    return _async_log_publisher.get_stats()
