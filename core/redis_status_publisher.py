"""
Redis Status Publisher
Redis 状态发布器

职责：
- 将节点状态变化发布到 Redis Pub/Sub
- 供 Control 服务订阅并转发到前端 WebSocket
"""

import redis
import json
import os
from typing import Dict, Any, Optional
from datetime import datetime
from weather_depot.config import CONFIG

class RedisStatusPublisher:
    """Redis 状态发布器"""

    def __init__(self):
        """初始化 Redis 连接"""
        # 使用 CONFIG 中自动编码密码的 URL
        redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
        self.redis_client = redis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )

        # 测试连接
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
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        发布节点状态变化到 Redis 频道

        Args:
            flow_id: Flow ID
            cycle: 执行周期
            node_id: Node ID
            status: 节点状态 (pending, running, completed, failed, skipped, terminated)
            error_message: 错误信息（如果有）
            metadata: 额外元数据

        Returns:
            bool: 是否发布成功
        """
        try:
            # 构建频道名称
            channel = f"status:flow:{flow_id}:cycle:{cycle}"

            # 构建状态更新消息
            status_update = {
                "timestamp": datetime.now().isoformat(),
                "flow_id": flow_id,
                "cycle": cycle,
                "node_id": node_id,
                "status": status,
                "error_message": error_message,
                "metadata": metadata or {}
            }

            # 序列化为 JSON
            message = json.dumps(status_update)

            # 发布到 Redis
            self.redis_client.publish(channel, message)

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
        发布Flow整体状态变化

        Args:
            flow_id: Flow ID
            cycle: 执行周期
            status: Flow状态 (running, completed, failed, cancelled)
            metadata: 额外元数据

        Returns:
            bool: 是否发布成功
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
        """关闭 Redis 连接"""
        try:
            self.redis_client.close()
            print("[RedisStatusPublisher] Redis connection closed")
        except Exception as e:
            print(f"[RedisStatusPublisher] Error closing Redis connection: {e}")


# 全局单例实例
_status_publisher = None


def get_status_publisher() -> RedisStatusPublisher:
    """
    获取 Redis 状态发布器的单例实例

    Returns:
        RedisStatusPublisher: 状态发布器实例
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
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    便捷函数：发布节点状态到 Redis

    Args:
        flow_id: Flow ID
        cycle: 执行周期
        node_id: Node ID
        status: 节点状态
        error_message: 错误信息
        metadata: 元数据

    Returns:
        bool: 是否发布成功
    """
    publisher = get_status_publisher()
    return publisher.publish_node_status(
        flow_id, cycle, node_id, status, error_message, metadata
    )


def publish_flow_status(
    flow_id: str,
    cycle: int,
    status: str,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    便捷函数：发布Flow状态到 Redis
    """
    publisher = get_status_publisher()
    return publisher.publish_flow_status(flow_id, cycle, status, metadata)


def close_status_publisher():
    """关闭状态发布器"""
    global _status_publisher
    if _status_publisher is not None:
        _status_publisher.close()
        _status_publisher = None
