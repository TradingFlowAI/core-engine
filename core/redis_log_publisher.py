"""
Redis Log Publisher
Redis 日志发布器

职责：
- 将节点执行日志发布到 Redis Pub/Sub
- 供 Control 服务订阅并转发到前端 WebSocket
"""

import redis
import json
import os
from typing import Dict, Any
from datetime import datetime


class RedisLogPublisher:
    """Redis 日志发布器"""
    
    def __init__(self):
        """初始化 Redis 连接"""
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        
        # 测试连接
        try:
            self.redis_client.ping()
            print("[RedisLogPublisher] Connected to Redis successfully")
        except redis.ConnectionError as e:
            print(f"[RedisLogPublisher] Warning: Failed to connect to Redis: {e}")
            print("[RedisLogPublisher] Log streaming will be disabled")
    
    def publish_log(
        self,
        flow_id: str,
        cycle: int,
        log_entry: Dict[str, Any]
    ) -> bool:
        """
        发布日志到 Redis 频道
        
        Args:
            flow_id: Flow ID
            cycle: 执行周期
            log_entry: 日志条目数据
            
        Returns:
            bool: 是否发布成功
        """
        try:
            # 构建频道名称
            channel = f"logs:flow:{flow_id}:cycle:{cycle}"
            
            # 确保日志条目包含必要字段
            complete_log_entry = {
                "timestamp": datetime.now().isoformat(),
                "flow_id": flow_id,
                "cycle": cycle,
                **log_entry
            }
            
            # 序列化为 JSON
            message = json.dumps(complete_log_entry)
            
            # 发布到 Redis
            self.redis_client.publish(channel, message)
            
            return True
            
        except redis.RedisError as e:
            print(f"[RedisLogPublisher] Failed to publish log to Redis: {e}")
            return False
        except Exception as e:
            print(f"[RedisLogPublisher] Unexpected error publishing log: {e}")
            return False
    
    def close(self):
        """关闭 Redis 连接"""
        try:
            self.redis_client.close()
            print("[RedisLogPublisher] Redis connection closed")
        except Exception as e:
            print(f"[RedisLogPublisher] Error closing Redis connection: {e}")


# 全局单例实例
_log_publisher = None


def get_log_publisher() -> RedisLogPublisher:
    """
    获取 Redis 日志发布器的单例实例
    
    Returns:
        RedisLogPublisher: 日志发布器实例
    """
    global _log_publisher
    if _log_publisher is None:
        _log_publisher = RedisLogPublisher()
    return _log_publisher


def publish_log(flow_id: str, cycle: int, log_entry: Dict[str, Any]) -> bool:
    """
    便捷函数：发布日志到 Redis
    
    Args:
        flow_id: Flow ID
        cycle: 执行周期
        log_entry: 日志条目数据
        
    Returns:
        bool: 是否发布成功
    """
    publisher = get_log_publisher()
    return publisher.publish_log(flow_id, cycle, log_entry)


def close_log_publisher():
    """关闭日志发布器"""
    global _log_publisher
    if _log_publisher is not None:
        _log_publisher.close()
        _log_publisher = None
