"""
测试异步 Redis 日志发布器

运行方式：
    python -m pytest tests/test_redis_log_publisher_async.py -v
    或
    python tests/test_redis_log_publisher_async.py
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# 假设测试环境中可能没有 redis，提供 mock
try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None


@pytest.mark.asyncio
async def test_async_log_publisher_connect():
    """测试连接到 Redis"""
    from core.redis_log_publisher_async import AsyncRedisLogPublisher
    
    publisher = AsyncRedisLogPublisher()
    
    # Mock Redis client
    with patch('core.redis_log_publisher_async.aioredis.from_url') as mock_from_url:
        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock()
        mock_from_url.return_value = mock_redis
        
        await publisher.connect()
        
        assert publisher._connected is True
        assert publisher.redis_client is not None
        mock_redis.ping.assert_called_once()


@pytest.mark.asyncio
async def test_async_log_publisher_publish_success():
    """测试成功发布日志"""
    from core.redis_log_publisher_async import AsyncRedisLogPublisher
    
    publisher = AsyncRedisLogPublisher()
    
    # Mock Redis client
    mock_redis = AsyncMock()
    mock_redis.publish = AsyncMock()
    publisher.redis_client = mock_redis
    publisher._connected = True
    
    # 发布日志
    result = await publisher.publish_log(
        flow_id="test_flow",
        cycle=1,
        log_entry={"message": "test log"},
        max_retries=3
    )
    
    assert result is True
    mock_redis.publish.assert_called_once()
    
    # 检查调用参数
    call_args = mock_redis.publish.call_args
    channel = call_args[0][0]
    message = call_args[0][1]
    
    assert channel == "logs:flow:test_flow:cycle:1"
    
    parsed_message = json.loads(message)
    assert parsed_message["flow_id"] == "test_flow"
    assert parsed_message["cycle"] == 1
    assert parsed_message["message"] == "test log"


@pytest.mark.asyncio
async def test_async_log_publisher_publish_with_retry():
    """测试发布失败后重试"""
    from core.redis_log_publisher_async import AsyncRedisLogPublisher
    
    publisher = AsyncRedisLogPublisher()
    
    # Mock Redis client - 第一次失败，第二次成功
    mock_redis = AsyncMock()
    mock_redis.publish = AsyncMock(
        side_effect=[Exception("Connection error"), None]
    )
    publisher.redis_client = mock_redis
    publisher._connected = True
    
    # 模拟 ensure_connected 不做任何事
    publisher.ensure_connected = AsyncMock()
    
    # Mock connect 方法
    with patch.object(publisher, 'connect', new_callable=AsyncMock):
        result = await publisher.publish_log(
            flow_id="test_flow",
            cycle=1,
            log_entry={"message": "test log"},
            max_retries=3
        )
        
        # 应该失败（因为 Exception 不是 RedisError）
        assert result is False


@pytest.mark.asyncio
async def test_async_log_publisher_stats():
    """测试统计信息"""
    from core.redis_log_publisher_async import AsyncRedisLogPublisher
    
    publisher = AsyncRedisLogPublisher()
    
    # 初始统计
    stats = publisher.get_stats()
    assert stats["success_count"] == 0
    assert stats["failure_count"] == 0
    assert stats["total_count"] == 0
    assert stats["success_rate"] == 0.0
    
    # 模拟成功发布
    publisher._publish_success_count = 8
    publisher._publish_failure_count = 2
    publisher._total_publish_time = 0.5
    
    stats = publisher.get_stats()
    assert stats["success_count"] == 8
    assert stats["failure_count"] == 2
    assert stats["total_count"] == 10
    assert stats["success_rate"] == 80.0
    assert stats["avg_publish_time_ms"] == 62.5  # 0.5 / 8 * 1000


@pytest.mark.asyncio
async def test_publish_log_async_convenience_function():
    """测试便捷函数"""
    from core.redis_log_publisher_async import publish_log_async
    
    # Mock get_async_log_publisher
    with patch('core.redis_log_publisher_async.get_async_log_publisher') as mock_get:
        mock_publisher = AsyncMock()
        mock_publisher.publish_log = AsyncMock(return_value=True)
        mock_get.return_value = mock_publisher
        
        result = await publish_log_async(
            flow_id="test_flow",
            cycle=1,
            log_entry={"message": "test"},
            max_retries=3
        )
        
        assert result is True
        mock_publisher.publish_log.assert_called_once_with(
            "test_flow", 1, {"message": "test"}, 3
        )


# 如果直接运行此文件
if __name__ == "__main__":
    print("运行异步 Redis 日志发布器测试...")
    print("=" * 60)
    
    # 运行所有测试
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s"]))
