"""
Redis Log Publisher

Responsibilities:
- Publish node execution logs to Redis Pub/Sub
- For Control service to subscribe and forward to frontend WebSocket
"""

import redis
import json
import os
from typing import Dict, Any
from datetime import datetime
from infra.config import CONFIG


class RedisLogPublisher:
    """Redis Log Publisher"""
    
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
        Publish log to Redis channel.
        
        Args:
            flow_id: Flow ID
            cycle: Execution cycle
            log_entry: Log entry data
            
        Returns:
            bool: Whether publish was successful
        """
        try:
            # Build channel name
            channel = f"logs:flow:{flow_id}:cycle:{cycle}"
            
            # Ensure log entry contains required fields
            complete_log_entry = {
                "timestamp": datetime.now().isoformat(),
                "flow_id": flow_id,
                "cycle": cycle,
                **log_entry
            }
            
            # Serialize to JSON
            message = json.dumps(complete_log_entry)
            
            # Publish to Redis
            self.redis_client.publish(channel, message)
            
            return True
            
        except redis.RedisError as e:
            print(f"[RedisLogPublisher] Failed to publish log to Redis: {e}")
            return False
        except Exception as e:
            print(f"[RedisLogPublisher] Unexpected error publishing log: {e}")
            return False
    
    def close(self):
        """Close Redis connection."""
        try:
            self.redis_client.close()
            print("[RedisLogPublisher] Redis connection closed")
        except Exception as e:
            print(f"[RedisLogPublisher] Error closing Redis connection: {e}")


# Global singleton instance
_log_publisher = None


def get_log_publisher() -> RedisLogPublisher:
    """
    Get Redis log publisher singleton instance.
    
    Returns:
        RedisLogPublisher: Log publisher instance
    """
    global _log_publisher
    if _log_publisher is None:
        _log_publisher = RedisLogPublisher()
    return _log_publisher


def publish_log(flow_id: str, cycle: int, log_entry: Dict[str, Any]) -> bool:
    """
    Convenience function: Publish log to Redis.
    
    Args:
        flow_id: Flow ID
        cycle: Execution cycle
        log_entry: Log entry data
        
    Returns:
        bool: Whether publish was successful
    """
    publisher = get_log_publisher()
    return publisher.publish_log(flow_id, cycle, log_entry)


def close_log_publisher():
    """Close log publisher."""
    global _log_publisher
    if _log_publisher is not None:
        _log_publisher.close()
        _log_publisher = None
