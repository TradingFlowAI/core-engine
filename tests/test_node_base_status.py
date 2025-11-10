"""
Integration tests for NodeBase.set_status()

测试 NodeBase 的状态发布集成
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from nodes.node_base import NodeBase, NodeStatus


class TestNodeBase(NodeBase):
    """用于测试的 NodeBase 子类"""
    
    def __init__(self, *args, node_type="test_node", **kwargs):
        super().__init__(*args, **kwargs)
        self.node_type = node_type
    
    async def execute(self):
        """简单的执行方法"""
        return True


class TestNodeBaseStatusIntegration:
    """NodeBase set_status 集成测试"""
    
    @pytest.fixture
    def mock_state_store(self):
        """Mock StateStore"""
        store = AsyncMock()
        store.set_node_task_status = AsyncMock(return_value=None)
        return store
    
    @pytest.fixture
    def test_node(self, mock_state_store):
        """创建测试节点"""
        node = TestNodeBase(
            flow_id="flow-123",
            component_id=1,
            cycle=0,
            node_id="node-456",
            name="Test Node",
            node_type="test_node",
            input_edges=[],
            output_edges=[],
            enable_credits=False
        )
        node.state_store = mock_state_store
        return node
    
    @pytest.mark.asyncio
    async def test_set_status_updates_state_store(self, test_node, mock_state_store):
        """测试 set_status 更新 state store"""
        await test_node.set_status(NodeStatus.RUNNING)
        
        # 验证 state store 被更新
        mock_state_store.set_node_task_status.assert_called_once_with(
            "node-456",
            "running",
            None
        )
        
        # 验证节点状态被更新
        assert test_node.status == NodeStatus.RUNNING
    
    @pytest.mark.asyncio
    async def test_set_status_with_error_message(self, test_node, mock_state_store):
        """测试带错误信息的状态更新"""
        error_msg = "Test error occurred"
        await test_node.set_status(NodeStatus.FAILED, error_msg)
        
        # 验证错误信息被传递
        mock_state_store.set_node_task_status.assert_called_once_with(
            "node-456",
            "failed",
            error_msg
        )
        
        assert test_node.status == NodeStatus.FAILED
        assert test_node.error_message == error_msg
    
    @pytest.mark.asyncio
    async def test_set_status_publishes_to_redis(self, test_node):
        """测试状态发布到 Redis"""
        with patch('core.redis_status_publisher.publish_node_status') as mock_publish:
            mock_publish.return_value = True
            
            await test_node.set_status(NodeStatus.RUNNING)
            
            # 验证 Redis 发布被调用
            mock_publish.assert_called_once()
            
            # 验证参数
            call_args = mock_publish.call_args
            assert call_args[1]['flow_id'] == "flow-123"
            assert call_args[1]['cycle'] == 0
            assert call_args[1]['node_id'] == "node-456"
            assert call_args[1]['status'] == "running"
            assert call_args[1]['error_message'] is None
            assert call_args[1]['metadata']['node_type'] == "test_node"
    
    @pytest.mark.asyncio
    async def test_set_status_redis_failure_does_not_affect_execution(self, test_node, mock_state_store):
        """测试 Redis 发布失败不影响节点执行"""
        with patch('core.redis_status_publisher.publish_node_status') as mock_publish:
            # Redis 发布失败
            mock_publish.side_effect = Exception("Redis connection failed")
            
            # set_status 不应该抛异常
            await test_node.set_status(NodeStatus.RUNNING)
            
            # state store 仍然应该被更新
            mock_state_store.set_node_task_status.assert_called_once()
            
            # 节点状态仍然应该被设置
            assert test_node.status == NodeStatus.RUNNING
    
    @pytest.mark.asyncio
    async def test_set_status_all_status_types(self, test_node):
        """测试所有状态类型"""
        statuses = [
            NodeStatus.PENDING,
            NodeStatus.RUNNING,
            NodeStatus.COMPLETED,
            NodeStatus.FAILED,
            NodeStatus.SKIPPED,
            NodeStatus.TERMINATED
        ]
        
        with patch('core.redis_status_publisher.publish_node_status') as mock_publish:
            for status in statuses:
                await test_node.set_status(status)
                
                # 验证每个状态都被发布
                call_args = mock_publish.call_args
                assert call_args[1]['status'] == status.value
    
    @pytest.mark.asyncio
    async def test_set_status_without_state_store(self, test_node):
        """测试没有 state store 的情况"""
        test_node.state_store = None
        
        with patch('core.redis_status_publisher.publish_node_status') as mock_publish:
            # 不应该抛异常
            await test_node.set_status(NodeStatus.RUNNING)
            
            # Redis 发布仍然应该工作
            mock_publish.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_concurrent_status_updates(self, test_node):
        """测试并发状态更新"""
        with patch('core.redis_status_publisher.publish_node_status') as mock_publish:
            # 并发更新多个状态
            await asyncio.gather(
                test_node.set_status(NodeStatus.RUNNING),
                test_node.set_status(NodeStatus.RUNNING),
                test_node.set_status(NodeStatus.RUNNING)
            )
            
            # 应该有3次调用
            assert mock_publish.call_count == 3


class TestNodeBaseStatusSequence:
    """测试状态转换序列"""
    
    @pytest.fixture
    def test_node(self):
        """创建测试节点"""
        node = TestNodeBase(
            flow_id="test-flow",
            component_id=1,
            cycle=0,
            node_id="test-node",
            name="Test Node",
            node_type="test_node",
            input_edges=[],
            output_edges=[],
            enable_credits=False
        )
        node.state_store = AsyncMock()
        return node
    
    @pytest.mark.asyncio
    async def test_normal_execution_flow(self, test_node):
        """测试正常执行流程的状态转换"""
        with patch('core.redis_status_publisher.publish_node_status') as mock_publish:
            # 模拟正常执行流程
            await test_node.set_status(NodeStatus.PENDING)
            await test_node.set_status(NodeStatus.RUNNING)
            await test_node.set_status(NodeStatus.COMPLETED)
            
            # 验证状态转换顺序
            calls = mock_publish.call_args_list
            assert len(calls) == 3
            assert calls[0][1]['status'] == 'pending'
            assert calls[1][1]['status'] == 'running'
            assert calls[2][1]['status'] == 'completed'
    
    @pytest.mark.asyncio
    async def test_error_execution_flow(self, test_node):
        """测试错误执行流程的状态转换"""
        with patch('core.redis_status_publisher.publish_node_status') as mock_publish:
            # 模拟错误流程
            await test_node.set_status(NodeStatus.PENDING)
            await test_node.set_status(NodeStatus.RUNNING)
            await test_node.set_status(NodeStatus.FAILED, "Execution error")
            
            # 验证最后的失败状态包含错误信息
            final_call = mock_publish.call_args_list[-1]
            assert final_call[1]['status'] == 'failed'
            assert final_call[1]['error_message'] == "Execution error"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
