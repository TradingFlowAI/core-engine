"""
Async Redis Signal Publisher
异步 Redis 信号发布器

职责：
- 将节点间信号异步发布到 Redis Pub/Sub
- 供 Control 服务订阅并转发到前端 WebSocket
- 用于调试和监控节点间数据流
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

from weather_depot.config import CONFIG

logger = logging.getLogger(__name__)


class AsyncRedisSignalPublisher:
    """异步 Redis 信号发布器"""

    def __init__(self):
        """初始化（延迟连接）"""
        self.redis_client: Optional[aioredis.Redis] = None
        self._connected = False
        self._lock = asyncio.Lock()

        # 统计信息
        self._publish_success_count = 0
        self._publish_failure_count = 0

        logger.info("[AsyncRedisSignalPublisher] Initialized (connection will be lazy-loaded)")

    async def connect(self):
        """连接到 Redis"""
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
        """确保已连接到 Redis"""
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
        异步发布信号到 Redis 频道

        Args:
            flow_id: Flow ID
            cycle: 执行周期
            signal_data: 信号数据，应包含:
                - direction: 'input' | 'output'
                - from_node_id: 源节点 ID
                - to_node_id: 目标节点 ID
                - handle_id: Handle ID
                - payload: 传输的数据
                - data_type: 数据类型标识
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）

        Returns:
            bool: 是否发布成功
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

        # 构建频道名称
        channel = f"signal:flow:{flow_id}"

        # 构建完整的信号消息
        complete_signal = {
            "type": "signal",
            "timestamp": datetime.now().isoformat(),
            "flow_id": flow_id,
            "cycle": cycle,
            **signal_data
        }

        # 尝试发布（带重试）
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
        """关闭 Redis 连接"""
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
        """获取统计信息"""
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


# 全局单例实例
_async_signal_publisher: Optional[AsyncRedisSignalPublisher] = None
# 🔧 修复：延迟创建 Lock，避免在模块加载时事件循环未初始化的问题
_publisher_lock: Optional[asyncio.Lock] = None


def _get_publisher_lock() -> asyncio.Lock:
    """获取或创建 publisher lock"""
    global _publisher_lock
    if _publisher_lock is None:
        _publisher_lock = asyncio.Lock()
    return _publisher_lock


async def get_async_signal_publisher() -> AsyncRedisSignalPublisher:
    """获取异步 Redis 信号发布器的单例实例"""
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
    # 兼容旧版参数
    from_node_id: str = None,
    to_node_id: str = None,
    handle_id: str = None,
) -> bool:
    """
    便捷函数：异步发布信号到 Redis

    Args:
        flow_id: Flow ID
        cycle: 执行周期
        source_node_id: 源节点 ID
        source_handle: 源 Handle ID
        target_node_ids: 目标节点 ID 列表
        signal_type: 信号类型 (e.g., 'ANY', 'NUMBER', 'STRING')
        payload: 传输的数据
        direction: 方向 ('input' 或 'output')
        data_type: 数据类型标识
        max_retries: 最大重试次数
        from_node_id: (兼容) 源节点 ID
        to_node_id: (兼容) 目标节点 ID
        handle_id: (兼容) Handle ID

    Returns:
        bool: 是否发布成功
    """
    try:
        publisher = await get_async_signal_publisher()
        
        # 兼容旧版参数
        actual_from_node = source_node_id or from_node_id or ""
        actual_targets = target_node_ids or ([to_node_id] if to_node_id else [])
        
        # 确定 handle_id：优先使用显式传入的 handle_id，否则使用 source_handle
        # 对于 input 信号，handle_id 应该是目标 handle（接收方）
        # 对于 output 信号，handle_id 应该是源 handle（发送方）
        actual_handle = handle_id or source_handle or ""
        
        signal_data = {
            "direction": direction,
            "from_node_id": actual_from_node,
            "to_node_ids": actual_targets,  # 注意：改为数组格式
            "to_node_id": actual_targets[0] if actual_targets else None,  # 兼容旧格式
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
    序列化 payload，处理不可直接 JSON 序列化的类型
    """
    if payload is None:
        return None
    
    if isinstance(payload, (str, int, float, bool)):
        return payload
    
    if isinstance(payload, (list, tuple)):
        return [_serialize_payload(item) for item in payload]
    
    if isinstance(payload, dict):
        return {k: _serialize_payload(v) for k, v in payload.items()}
    
    # 对于复杂对象，转换为字符串表示
    try:
        return str(payload)
    except Exception:
        return "<unserializable>"


async def close_async_signal_publisher():
    """关闭异步信号发布器"""
    global _async_signal_publisher

    if _async_signal_publisher is not None:
        await _async_signal_publisher.close()
        _async_signal_publisher = None


async def get_signal_publisher_stats() -> Dict[str, Any]:
    """获取发布器统计信息"""
    if _async_signal_publisher is None:
        return {
            "connected": False,
            "success_count": 0,
            "failure_count": 0,
            "total_count": 0,
            "success_rate": 0.0,
        }

    return _async_signal_publisher.get_stats()
