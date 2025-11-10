"""
Community Node Unit Tests
社区节点单元测试
"""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Add parent directory to path
sys.path.insert(0, '..')

from core.community_node import CommunityNode
from core.community_node_executor import CommunityNodeExecutor


class TestCommunityNode:
    """社区节点测试"""
    
    def setup_method(self):
        """每个测试前的设置"""
        self.node_config = {
            'nodeId': 'test_node_123',
            'name': 'test_node',
            'displayName': 'Test Node',
            'category': 'utility',
            'version': '0.0.1',
            'description': 'A test node',
            'authorId': 'user_123',
            'authorName': 'Test Author',
            'inputs': [
                {
                    'name': 'input1',
                    'type': 'string',
                    'required': True,
                    'description': 'First input'
                },
                {
                    'name': 'input2',
                    'type': 'number',
                    'required': False,
                    'default': 42
                }
            ],
            'outputs': [
                {
                    'name': 'output1',
                    'type': 'string',
                    'description': 'First output'
                }
            ],
            'executionType': 'python',
            'executionConfig': {
                'pythonCode': 'outputs["output1"] = str(inputs.get("input1", "")) + str(inputs.get("input2", 0))',
                'timeout': 10
            }
        }
    
    def test_init(self):
        """测试节点初始化"""
        node = CommunityNode(self.node_config)
        
        assert node.node_id == 'test_node_123'
        assert node.execution_type == 'python'
        assert node.display_name == 'Test Node'
        assert len(node.input_handles) == 2
        assert len(node.output_handles) == 1
    
    def test_build_handles(self):
        """测试构建句柄"""
        node = CommunityNode(self.node_config)
        
        # Check input handles
        input_handles = node.input_handles
        assert input_handles[0]['name'] == 'input1'
        assert input_handles[0]['type'] == 'string'
        assert input_handles[0]['required'] == True
        
        assert input_handles[1]['name'] == 'input2'
        assert input_handles[1]['default'] == 42
    
    def test_validate_inputs_success(self):
        """测试输入验证成功"""
        node = CommunityNode(self.node_config)
        
        data = {'input1': 'test', 'input2': 123}
        is_valid, error = node.validate_inputs(data)
        
        assert is_valid == True
        assert error is None
    
    def test_validate_inputs_missing_required(self):
        """测试缺少必需输入"""
        node = CommunityNode(self.node_config)
        
        data = {'input2': 123}  # Missing required input1
        is_valid, error = node.validate_inputs(data)
        
        assert is_valid == False
        assert 'input1' in error
    
    def test_validate_inputs_use_default(self):
        """测试使用默认值"""
        node = CommunityNode(self.node_config)
        
        data = {'input1': 'test'}  # input2 will use default
        is_valid, error = node.validate_inputs(data)
        
        assert is_valid == True
        assert data['input2'] == 42  # Default value applied
    
    def test_validate_type(self):
        """测试类型验证"""
        node = CommunityNode(self.node_config)
        
        assert node._validate_type('hello', 'string') == True
        assert node._validate_type(123, 'number') == True
        assert node._validate_type(12.5, 'number') == True
        assert node._validate_type(True, 'boolean') == True
        assert node._validate_type({'key': 'value'}, 'json') == True
        assert node._validate_type([1, 2, 3], 'array') == True
        assert node._validate_type({'key': 'value'}, 'object') == True
        
        # Invalid types
        assert node._validate_type(123, 'string') == False
        assert node._validate_type('hello', 'number') == False
    
    @patch('core.community_node_executor.CommunityNodeExecutor.execute')
    def test_execute_success(self, mock_execute):
        """测试成功执行"""
        node = CommunityNode(self.node_config)
        
        mock_execute.return_value = {
            'success': True,
            'outputs': {'output1': 'test42'},
            'logs': []
        }
        
        result = node.execute(input1='test', input2=42)
        
        assert 'output1' in result
        assert result['output1'] == 'test42'
        mock_execute.assert_called_once()
    
    @patch('core.community_node_executor.CommunityNodeExecutor.execute')
    def test_execute_validation_failure(self, mock_execute):
        """测试执行时验证失败"""
        node = CommunityNode(self.node_config)
        
        # Missing required input
        result = node.execute(input2=42)
        
        assert 'success' in result
        assert result['success'] == False
        assert 'error' in result
        mock_execute.assert_not_called()
    
    @patch('core.community_node_executor.CommunityNodeExecutor.execute')
    def test_execute_executor_error(self, mock_execute):
        """测试执行器错误"""
        node = CommunityNode(self.node_config)
        
        mock_execute.return_value = {
            'success': False,
            'error': 'Execution error',
            'error_type': 'RuntimeError'
        }
        
        result = node.execute(input1='test', input2=42)
        
        assert result['success'] == False
        assert 'error' in result
    
    def test_from_database(self):
        """测试从数据库创建"""
        node = CommunityNode.from_database(self.node_config)
        
        assert node.node_id == 'test_node_123'
        assert node.display_name == 'Test Node'
    
    def test_to_dict(self):
        """测试转换为字典"""
        node = CommunityNode(self.node_config)
        
        node_dict = node.to_dict()
        
        assert node_dict['node_id'] == 'test_node_123'
        assert node_dict['execution_type'] == 'python'
        assert 'node_config' in node_dict
    
    def test_metadata(self):
        """测试元数据"""
        node = CommunityNode(self.node_config)
        
        metadata = node.get_metadata()
        
        assert metadata['node_id'] == 'test_node_123'
        assert metadata['author_id'] == 'user_123'
        assert metadata['author_name'] == 'Test Author'
        assert metadata['is_community_node'] == True


class TestCommunityNodeHTTP:
    """测试 HTTP 执行类型的社区节点"""
    
    def setup_method(self):
        """设置 HTTP 节点配置"""
        self.http_config = {
            'nodeId': 'http_node_123',
            'name': 'http_test',
            'displayName': 'HTTP Test Node',
            'category': 'data',
            'version': '0.0.1',
            'description': 'HTTP API test node',
            'authorId': 'user_456',
            'authorName': 'HTTP Author',
            'inputs': [
                {'name': 'query', 'type': 'string', 'required': True}
            ],
            'outputs': [
                {'name': 'response', 'type': 'json'}
            ],
            'executionType': 'http',
            'executionConfig': {
                'url': 'https://api.example.com/search',
                'method': 'GET',
                'headers': {'Content-Type': 'application/json'},
                'timeout': 30
            }
        }
    
    @patch('core.community_node_executor.CommunityNodeExecutor.execute')
    def test_http_execution(self, mock_execute):
        """测试 HTTP 执行"""
        node = CommunityNode(self.http_config)
        
        mock_execute.return_value = {
            'success': True,
            'outputs': {
                'results': ['result1', 'result2']
            },
            'metadata': {
                'status_code': 200,
                'execution_time': 0.5
            }
        }
        
        result = node.execute(query='test search')
        
        assert 'results' in result
        assert len(result['results']) == 2
        mock_execute.assert_called_once()


class TestCommunityNodeEdgeCases:
    """测试边界情况"""
    
    def test_empty_inputs(self):
        """测试空输入列表"""
        config = {
            'nodeId': 'empty_inputs',
            'name': 'test',
            'displayName': 'Test',
            'category': 'utility',
            'description': 'Test',
            'executionType': 'python',
            'executionConfig': {'pythonCode': 'pass'},
            'inputs': [],
            'outputs': []
        }
        
        node = CommunityNode(config)
        assert len(node.input_handles) == 0
        assert len(node.output_handles) == 0
    
    def test_unknown_type(self):
        """测试未知类型"""
        config = {
            'nodeId': 'unknown_type',
            'name': 'test',
            'displayName': 'Test',
            'category': 'utility',
            'description': 'Test',
            'executionType': 'python',
            'executionConfig': {'pythonCode': 'pass'},
            'inputs': [
                {'name': 'weird_input', 'type': 'unknown_type', 'required': False}
            ],
            'outputs': []
        }
        
        node = CommunityNode(config)
        
        # Should not fail on unknown type
        is_valid, error = node.validate_inputs({'weird_input': 'anything'})
        assert is_valid == True
    
    @patch('core.community_node_executor.CommunityNodeExecutor.execute')
    def test_unexpected_exception(self, mock_execute):
        """测试意外异常"""
        config = {
            'nodeId': 'exception_node',
            'name': 'test',
            'displayName': 'Test',
            'category': 'utility',
            'description': 'Test',
            'executionType': 'python',
            'executionConfig': {'pythonCode': 'pass'},
            'inputs': [{'name': 'input1', 'type': 'string', 'required': True}],
            'outputs': []
        }
        
        node = CommunityNode(config)
        mock_execute.side_effect = RuntimeError('Unexpected error')
        
        result = node.execute(input1='test')
        
        assert result['success'] == False
        assert 'error' in result
        assert 'error_type' in result


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
