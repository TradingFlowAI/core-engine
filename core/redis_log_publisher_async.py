"""
Async Redis Log Publisher
异步 Redis 日志发布器

职责：
- 将节点执行日志异步发布到 Redis Pub/Sub
- 供 Control 服务订阅并转发到前端 WebSocket
- 支持连接池、错误重试、性能监控
"""

import asyncio
import json
import logging
import os
from typing import Dict, Any, Optional
from datetime import datetime
from weather_depot.config import CONFIG

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
    """异步 Redis 日志发布器"""
    
    def __init__(self):
        """初始化（延迟连接）"""
        self.redis_client: Optional[aioredis.Redis] = None
        self._connected = False
        self._lock = asyncio.Lock()
        
        # 统计信息
        self._publish_success_count = 0
        self._publish_failure_count = 0
        self._total_publish_time = 0.0
        
        logger.info("[AsyncRedisLogPublisher] Initialized (connection will be lazy-loaded)")
    
    async def connect(self):
        """连接到 Redis"""
        if self._connected:
            return
        
        async with self._lock:
            if self._connected:
                return
            
            try:
                # 使用 CONFIG 中自动编码密码的 URL
                redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
                
                self.redis_client = await aioredis.from_url(
                    redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    max_connections=10,  # 连接池大小
                )
                
                # 测试连接
                await self.redis_client.ping()
                self._connected = True
                
                # 更新 metrics
                set_redis_connection_status(True)
                
                logger.info(
                    "[AsyncRedisLogPublisher] Connected to Redis at %s:%s",
                    redis_host, redis_port
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
                
                # 更新 metrics
                set_redis_connection_status(False)
                
                raise
    
    async def ensure_connected(self):
        """确保已连接到 Redis"""
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
        异步发布日志到 Redis 频道（支持重试）
        
        Args:
            flow_id: Flow ID
            cycle: 执行周期
            log_entry: 日志条目数据
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            
        Returns:
            bool: 是否发布成功
        """
        # 确保已连接
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
        
        # 构建频道名称
        channel = f"logs:flow:{flow_id}:cycle:{cycle}"
        
        # 确保日志条目包含必要字段
        complete_log_entry = {
            "timestamp": datetime.now().isoformat(),
            "flow_id": flow_id,
            "cycle": cycle,
            **log_entry
        }
        
        # 尝试发布（带重试）
        for attempt in range(max_retries):
            try:
                start_time = asyncio.get_event_loop().time()
                
                # 序列化为 JSON
                message = json.dumps(complete_log_entry)
                
                # 发布到 Redis
                await self.redis_client.publish(channel, message)
                
                # 统计
                elapsed = asyncio.get_event_loop().time() - start_time
                self._total_publish_time += elapsed
                self._publish_success_count += 1
                
                # 记录成功 metrics
                record_log_publish_success(flow_id, cycle, elapsed)
                
                if attempt > 0:
                    logger.info(
                        "[AsyncRedisLogPublisher] Log published successfully after %d retries",
                        attempt
                    )
                
                return True
                
            except (aioredis.RedisError, aioredis.ConnectionError) as e:
                self._publish_failure_count += 1
                
                # 记录重试 metrics
                if attempt < max_retries - 1:
                    record_log_publish_retry(flow_id, cycle, attempt + 1)
                    
                    logger.warning(
                        "[AsyncRedisLogPublisher] Failed to publish log (attempt %d/%d): %s. Retrying...",
                        attempt + 1, max_retries, str(e)
                    )
                    await asyncio.sleep(retry_delay * (attempt + 1))  # 指数退避
                    
                    # 尝试重新连接
                    try:
                        self._connected = False
                        await self.connect()
                    except Exception:
                        pass
                else:
                    # 记录失败 metrics
                    error_type = type(e).__name__
                    record_log_publish_failure(flow_id, cycle, error_type)
                    
                    logger.error(
                        "[AsyncRedisLogPublisher] Failed to publish log after %d attempts: %s",
                        max_retries, str(e)
                    )
                    return False
            except Exception as e:
                self._publish_failure_count += 1
                
                # 记录失败 metrics
                error_type = type(e).__name__
                record_log_publish_failure(flow_id, cycle, error_type)
                
                logger.error(
                    "[AsyncRedisLogPublisher] Unexpected error publishing log: %s",
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
                
                # 更新 metrics
                set_redis_connection_status(False)
                
                logger.info("[AsyncRedisLogPublisher] Redis connection closed")
            except Exception as e:
                logger.warning(
                    "[AsyncRedisLogPublisher] Error closing Redis connection: %s",
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


# 全局单例实例
_async_log_publisher: Optional[AsyncRedisLogPublisher] = None
_publisher_lock = asyncio.Lock()


async def get_async_log_publisher() -> AsyncRedisLogPublisher:
    """
    获取异步 Redis 日志发布器的单例实例
    
    Returns:
        AsyncRedisLogPublisher: 异步日志发布器实例
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
    便捷函数：异步发布日志到 Redis
    
    Args:
        flow_id: Flow ID
        cycle: 执行周期
        log_entry: 日志条目数据
        max_retries: 最大重试次数
        
    Returns:
        bool: 是否发布成功
    """
    try:
        publisher = await get_async_log_publisher()
        return await publisher.publish_log(flow_id, cycle, log_entry, max_retries)
    except Exception as e:
        logger.error("Failed to publish log via async publisher: %s", str(e))
        return False


async def close_async_log_publisher():
    """关闭异步日志发布器"""
    global _async_log_publisher
    
    if _async_log_publisher is not None:
        await _async_log_publisher.close()
        _async_log_publisher = None


async def get_publisher_stats() -> Dict[str, Any]:
    """获取发布器统计信息"""
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
