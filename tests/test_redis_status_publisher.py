"""
Unit tests for redis_status_publisher.py

测试 RedisStatusPublisher 的核心功能
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# 导入要测试的模块
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.redis_status_publisher import (
    RedisStatusPublisher,
    get_status_publisher,
    publish_node_status,
    publish_flow_status,
    close_status_publisher
)


class TestRedisStatusPublisher:
    """RedisStatusPublisher 类的单元测试"""
    
    @pytest.fixture
    def mock_redis(self):
        """Mock Redis 客户端"""
        with patch('core.redis_status_publisher.redis.Redis') as MockRedis:
            mock_redis_instance = Mock()
            mock_redis_instance.ping.return_value = True
            mock_redis_instance.publish.return_value = 1
            MockRedis.return_value = mock_redis_instance
            yield mock_redis_instance
    
    @pytest.fixture
    def publisher(self, mock_redis):
        """创建 Publisher 实例"""
        return RedisStatusPublisher()
    
    def test_initialization_success(self, mock_redis):
        """测试初始化成功"""
        publisher = RedisStatusPublisher()
        
        # 验证 Redis 连接被创建
        assert publisher.redis_client is not None
        mock_redis.ping.assert_called_once()
    
    def test_initialization_connection_failure(self):
        """测试 Redis 连接失败的情况"""
        with patch('core.redis_status_publisher.redis.Redis') as MockRedis:
            mock_redis = Mock()
            mock_redis.ping.side_effect = Exception("Connection failed")
            MockRedis.return_value = mock_redis
            
            # 初始化不应该抛出异常
            publisher = RedisStatusPublisher()
            assert publisher.redis_client is not None
    
    def test_publish_node_status_success(self, publisher, mock_redis):
        """测试成功发布节点状态"""
        result = publisher.publish_node_status(
            flow_id="flow-123",
            cycle=0,
            node_id="node-456",
            status="running",
            error_message=None,
            metadata={"node_type": "test_node"}
        )
        
        # 验证返回值
        assert result is True
        
        # 验证 publish 被调用
        assert mock_redis.publish.called
        
        # 验证 channel 格式
        call_args = mock_redis.publish.call_args
        channel = call_args[0][0]
        assert channel == "status:flow:flow-123:cycle:0"
        
        # 验证消息格式
        message = json.loads(call_args[0][1])
        assert message["flow_id"] == "flow-123"
        assert message["cycle"] == 0
        assert message["node_id"] == "node-456"
        assert message["status"] == "running"
        assert message["error_message"] is None
        assert message["metadata"]["node_type"] == "test_node"
        assert "timestamp" in message
    
    def test_publish_node_status_with_error(self, publisher, mock_redis):
        """测试发布带有错误信息的节点状态"""
        result = publisher.publish_node_status(
            flow_id="flow-123",
            cycle=0,
            node_id="node-456",
            status="failed",
            error_message="Test error",
            metadata=None
        )
        
        assert result is True
        
        # 验证错误信息被包含
        call_args = mock_redis.publish.call_args
        message = json.loads(call_args[0][1])
        assert message["status"] == "failed"
        assert message["error_message"] == "Test error"
    
    def test_publish_node_status_redis_failure(self, publisher, mock_redis):
        """测试 Redis 发布失败的情况"""
        mock_redis.publish.side_effect = Exception("Redis error")
        
        result = publisher.publish_node_status(
            flow_id="flow-123",
            cycle=0,
            node_id="node-456",
            status="running"
        )
        
        # 应该返回 False 但不抛异常
        assert result is False
    
    def test_publish_flow_status_success(self, publisher, mock_redis):
        """测试成功发布 Flow 状态"""
        result = publisher.publish_flow_status(
            flow_id="flow-123",
            cycle=0,
            status="completed",
            metadata={"duration_ms": 5000}
        )
        
        assert result is True
        
        # 验证消息格式
        call_args = mock_redis.publish.call_args
        message = json.loads(call_args[0][1])
        assert message["type"] == "flow"
        assert message["status"] == "completed"
        assert message["metadata"]["duration_ms"] == 5000
    
    def test_close(self, publisher, mock_redis):
        """测试关闭连接"""
        publisher.close()
        mock_redis.close.assert_called_once()


class TestGlobalFunctions:
    """测试全局函数"""
    
    @pytest.fixture(autouse=True)
    def reset_global_instance(self):
        """每个测试前重置全局单例"""
        import core.redis_status_publisher
        core.redis_status_publisher._status_publisher = None
        yield
        core.redis_status_publisher._status_publisher = None
    
    def test_get_status_publisher_singleton(self):
        """测试单例模式"""
        with patch('core.redis_status_publisher.redis.Redis'):
            publisher1 = get_status_publisher()
            publisher2 = get_status_publisher()
            
            # 应该返回同一个实例
            assert publisher1 is publisher2
    
    def test_publish_node_status_convenience_function(self):
        """测试便捷函数 publish_node_status"""
        with patch('core.redis_status_publisher.redis.Redis') as MockRedis:
            mock_redis = Mock()
            mock_redis.ping.return_value = True
            mock_redis.publish.return_value = 1
            MockRedis.return_value = mock_redis
            
            result = publish_node_status(
                flow_id="flow-123",
                cycle=0,
                node_id="node-456",
                status="running"
            )
            
            assert result is True
            assert mock_redis.publish.called
    
    def test_publish_flow_status_convenience_function(self):
        """测试便捷函数 publish_flow_status"""
        with patch('core.redis_status_publisher.redis.Redis') as MockRedis:
            mock_redis = Mock()
            mock_redis.ping.return_value = True
            mock_redis.publish.return_value = 1
            MockRedis.return_value = mock_redis
            
            result = publish_flow_status(
                flow_id="flow-123",
                cycle=0,
                status="completed"
            )
            
            assert result is True
    
    def test_close_status_publisher(self):
        """测试关闭全局 publisher"""
        with patch('core.redis_status_publisher.redis.Redis') as MockRedis:
            mock_redis = Mock()
            mock_redis.ping.return_value = True
            MockRedis.return_value = mock_redis
            
            # 获取 publisher
            get_status_publisher()
            
            # 关闭
            close_status_publisher()
            
            # 验证 close 被调用
            mock_redis.close.assert_called_once()


class TestEdgeCases:
    """边界情况测试"""
    
    @pytest.fixture
    def mock_redis(self):
        with patch('core.redis_status_publisher.redis.Redis') as MockRedis:
            mock_redis = Mock()
            mock_redis.ping.return_value = True
            mock_redis.publish.return_value = 1
            MockRedis.return_value = mock_redis
            yield mock_redis
    
    def test_publish_with_none_metadata(self, mock_redis):
        """测试 metadata 为 None 的情况"""
        publisher = RedisStatusPublisher()
        result = publisher.publish_node_status(
            flow_id="flow-123",
            cycle=0,
            node_id="node-456",
            status="running",
            metadata=None
        )
        
        assert result is True
        
        # 验证空字典被使用
        call_args = mock_redis.publish.call_args
        message = json.loads(call_args[0][1])
        assert message["metadata"] == {}
    
    def test_publish_with_special_characters(self, mock_redis):
        """测试特殊字符处理"""
        publisher = RedisStatusPublisher()
        result = publisher.publish_node_status(
            flow_id="flow-测试-123",
            cycle=0,
            node_id="node-特殊字符",
            status="running",
            error_message="Error: 错误信息 🚀"
        )
        
        assert result is True
        
        # 验证 JSON 序列化成功
        call_args = mock_redis.publish.call_args
        message = json.loads(call_args[0][1])
        assert message["flow_id"] == "flow-测试-123"
        assert message["error_message"] == "Error: 错误信息 🚀"
    
    def test_publish_with_large_metadata(self, mock_redis):
        """测试大量 metadata"""
        publisher = RedisStatusPublisher()
        large_metadata = {f"key_{i}": f"value_{i}" for i in range(100)}
        
        result = publisher.publish_node_status(
            flow_id="flow-123",
            cycle=0,
            node_id="node-456",
            status="running",
            metadata=large_metadata
        )
        
        assert result is True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
