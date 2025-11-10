"""
Community Node Executor Unit Tests
社区节点执行器单元测试
"""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
import requests

# Add parent directory to path
sys.path.insert(0, '..')

from core.community_node_executor import CommunityNodeExecutor


class TestPythonExecution:
    """测试 Python 代码执行"""
    
    def test_simple_execution(self):
        """测试简单执行"""
        code = '''
outputs["result"] = inputs["value"] * 2
'''
        inputs = {'value': 21}
        
        result = CommunityNodeExecutor.execute_python(code, inputs, timeout=5)
        
        assert result['success'] == True
        assert result['outputs']['result'] == 42
    
    def test_with_allowed_modules(self):
        """测试使用允许的模块"""
        code = '''
import json
import math

data = {"value": math.sqrt(inputs["number"])}
outputs["json_result"] = json.dumps(data)
outputs["sqrt"] = math.sqrt(inputs["number"])
'''
        inputs = {'number': 16}
        
        result = CommunityNodeExecutor.execute_python(code, inputs, timeout=5)
        
        assert result['success'] == True
        assert result['outputs']['sqrt'] == 4.0
    
    def test_print_output(self):
        """测试打印输出"""
        code = '''
print("Processing data...")
outputs["result"] = "done"
'''
        inputs = {}
        
        result = CommunityNodeExecutor.execute_python(code, inputs, timeout=5)
        
        assert result['success'] == True
        assert len(result['logs']) > 0
        assert 'Processing data' in result['logs'][0]['message']
    
    def test_error_handling(self):
        """测试错误处理"""
        code = '''
raise ValueError("Something went wrong")
'''
        inputs = {}
        
        result = CommunityNodeExecutor.execute_python(code, inputs, timeout=5)
        
        assert result['success'] == False
        assert 'error' in result
        assert result['error_type'] == 'ValueError'
    
    def test_restricted_builtins(self):
        """测试受限的内置函数"""
        # Allowed builtins
        code_allowed = '''
outputs["sum"] = sum([1, 2, 3, 4, 5])
outputs["max"] = max([1, 5, 3])
outputs["min"] = min([1, 5, 3])
'''
        result = CommunityNodeExecutor.execute_python(code_allowed, {}, timeout=5)
        assert result['success'] == True
        assert result['outputs']['sum'] == 15
        assert result['outputs']['max'] == 5
        assert result['outputs']['min'] == 1
    
    def test_complex_data_processing(self):
        """测试复杂数据处理"""
        code = '''
import json
from datetime import datetime

data = inputs.get("data", [])
processed = []

for item in data:
    if isinstance(item, dict):
        processed.append({
            "id": item.get("id"),
            "value": item.get("value", 0) * 2,
            "processed": True
        })

outputs["processed_data"] = processed
outputs["count"] = len(processed)
'''
        inputs = {
            'data': [
                {'id': 1, 'value': 10},
                {'id': 2, 'value': 20},
                {'id': 3, 'value': 30}
            ]
        }
        
        result = CommunityNodeExecutor.execute_python(code, inputs, timeout=5)
        
        assert result['success'] == True
        assert result['outputs']['count'] == 3
        assert result['outputs']['processed_data'][0]['value'] == 20


class TestHTTPExecution:
    """测试 HTTP 执行"""
    
    @patch('core.community_node_executor.requests.request')
    def test_get_request(self, mock_request):
        """测试 GET 请求"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'test'}
        mock_response.content = b'{"data": "test"}'
        mock_request.return_value = mock_response
        
        config = {
            'url': 'https://api.example.com/data',
            'method': 'GET',
            'headers': {'Accept': 'application/json'},
            'timeout': 10
        }
        inputs = {'query': 'test'}
        
        result = CommunityNodeExecutor.execute_http(config, inputs)
        
        assert result['success'] == True
        assert result['outputs']['data'] == 'test'
        assert result['metadata']['status_code'] == 200
    
    @patch('core.community_node_executor.requests.request')
    def test_post_request(self, mock_request):
        """测试 POST 请求"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'id': 123, 'created': True}
        mock_response.content = b'{"id": 123}'
        mock_request.return_value = mock_response
        
        config = {
            'url': 'https://api.example.com/create',
            'method': 'POST',
            'headers': {'Content-Type': 'application/json'},
            'timeout': 15
        }
        inputs = {'name': 'Test Item', 'value': 42}
        
        result = CommunityNodeExecutor.execute_http(config, inputs)
        
        assert result['success'] == True
        assert result['outputs']['id'] == 123
        assert result['outputs']['created'] == True
    
    @patch('core.community_node_executor.requests.request')
    def test_timeout_error(self, mock_request):
        """测试超时错误"""
        mock_request.side_effect = requests.Timeout('Request timeout')
        
        config = {
            'url': 'https://api.example.com/slow',
            'method': 'GET',
            'timeout': 5
        }
        inputs = {}
        
        result = CommunityNodeExecutor.execute_http(config, inputs)
        
        assert result['success'] == False
        assert result['error_type'] == 'timeout'
    
    @patch('core.community_node_executor.requests.request')
    def test_http_error(self, mock_request):
        """测试 HTTP 错误"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError('404 Not Found')
        mock_request.return_value = mock_response
        
        config = {
            'url': 'https://api.example.com/notfound',
            'method': 'GET',
            'timeout': 10
        }
        inputs = {}
        
        result = CommunityNodeExecutor.execute_http(config, inputs)
        
        assert result['success'] == False
        assert result['error_type'] == 'http_error'
    
    def test_missing_url(self):
        """测试缺少 URL"""
        config = {
            'method': 'GET',
            'timeout': 10
        }
        inputs = {}
        
        result = CommunityNodeExecutor.execute_http(config, inputs)
        
        assert result['success'] == False
        assert 'URL is required' in result['error']
    
    @patch('core.community_node_executor.requests.request')
    def test_non_json_response(self, mock_request):
        """测试非 JSON 响应"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError('Not JSON')
        mock_response.text = 'Plain text response'
        mock_response.content = b'Plain text response'
        mock_request.return_value = mock_response
        
        config = {
            'url': 'https://api.example.com/text',
            'method': 'GET',
            'timeout': 10
        }
        inputs = {}
        
        result = CommunityNodeExecutor.execute_http(config, inputs)
        
        assert result['success'] == True
        assert result['outputs']['raw'] == 'Plain text response'


class TestUnifiedExecute:
    """测试统一执行接口"""
    
    @patch('core.community_node_executor.CommunityNodeExecutor.execute_python')
    def test_python_type(self, mock_python):
        """测试 Python 类型"""
        mock_python.return_value = {'success': True, 'outputs': {}}
        
        config = {'pythonCode': 'pass', 'timeout': 10}
        inputs = {}
        
        result = CommunityNodeExecutor.execute('python', config, inputs)
        
        assert result['success'] == True
        mock_python.assert_called_once()
    
    @patch('core.community_node_executor.CommunityNodeExecutor.execute_http')
    def test_http_type(self, mock_http):
        """测试 HTTP 类型"""
        mock_http.return_value = {'success': True, 'outputs': {}}
        
        config = {'url': 'https://api.test.com', 'method': 'GET'}
        inputs = {}
        
        result = CommunityNodeExecutor.execute('http', config, inputs)
        
        assert result['success'] == True
        mock_http.assert_called_once()
    
    def test_unsupported_type(self):
        """测试不支持的类型"""
        result = CommunityNodeExecutor.execute('unsupported', {}, {})
        
        assert result['success'] == False
        assert 'Unsupported execution type' in result['error']


class TestPythonCodeValidation:
    """测试 Python 代码验证"""
    
    def test_valid_code(self):
        """测试有效代码"""
        code = '''
x = 10
y = 20
outputs["sum"] = x + y
'''
        result = CommunityNodeExecutor.validate_python_code(code)
        
        assert result['valid'] == True
        assert len(result['issues']) == 0
    
    def test_syntax_error(self):
        """测试语法错误"""
        code = '''
x = 10
y = 
outputs["sum"] = x + y
'''
        result = CommunityNodeExecutor.validate_python_code(code)
        
        assert result['valid'] == False
        assert len(result['issues']) > 0
    
    def test_dangerous_import(self):
        """测试危险导入"""
        code = '''
import os
os.system("rm -rf /")
'''
        result = CommunityNodeExecutor.validate_python_code(code)
        
        assert result['valid'] == False
        assert any('import os' in issue for issue in result['issues'])
    
    def test_dangerous_builtins(self):
        """测试危险内置函数"""
        dangerous_codes = [
            'eval("malicious code")',
            'exec("bad code")',
            'open("/etc/passwd")',
            '__import__("os")',
        ]
        
        for code in dangerous_codes:
            result = CommunityNodeExecutor.validate_python_code(code)
            assert result['valid'] == False


class TestHTTPConfigValidation:
    """测试 HTTP 配置验证"""
    
    def test_valid_config(self):
        """测试有效配置"""
        config = {
            'url': 'https://api.example.com/data',
            'method': 'GET',
            'timeout': 10
        }
        
        result = CommunityNodeExecutor.validate_http_config(config)
        
        assert result['valid'] == True
        assert len(result['issues']) == 0
    
    def test_missing_url(self):
        """测试缺少 URL"""
        config = {
            'method': 'POST',
            'timeout': 10
        }
        
        result = CommunityNodeExecutor.validate_http_config(config)
        
        assert result['valid'] == False
        assert any('URL is required' in issue for issue in result['issues'])
    
    def test_invalid_url_protocol(self):
        """测试无效 URL 协议"""
        config = {
            'url': 'ftp://example.com/data',
            'method': 'GET'
        }
        
        result = CommunityNodeExecutor.validate_http_config(config)
        
        assert result['valid'] == False
        assert any('http://' in issue or 'https://' in issue for issue in result['issues'])
    
    def test_invalid_method(self):
        """测试无效方法"""
        config = {
            'url': 'https://api.example.com',
            'method': 'INVALID'
        }
        
        result = CommunityNodeExecutor.validate_http_config(config)
        
        assert result['valid'] == False
        assert any('Unsupported HTTP method' in issue for issue in result['issues'])
    
    def test_invalid_timeout(self):
        """测试无效超时"""
        configs = [
            {'url': 'https://api.example.com', 'timeout': -5},
            {'url': 'https://api.example.com', 'timeout': 'not a number'},
            {'url': 'https://api.example.com', 'timeout': 200}  # Exceeds max
        ]
        
        for config in configs:
            result = CommunityNodeExecutor.validate_http_config(config)
            assert result['valid'] == False


class TestExecutionEdgeCases:
    """测试执行边界情况"""
    
    def test_empty_code(self):
        """测试空代码"""
        result = CommunityNodeExecutor.execute_python('', {}, timeout=5)
        
        assert result['success'] == True
        assert result['outputs'] == {}
    
    def test_very_long_output(self):
        """测试超长输出"""
        code = '''
outputs["long_string"] = "x" * 10000
'''
        result = CommunityNodeExecutor.execute_python(code, {}, timeout=5)
        
        assert result['success'] == True
        assert len(result['outputs']['long_string']) == 10000
    
    def test_nested_data_structures(self):
        """测试嵌套数据结构"""
        code = '''
outputs["nested"] = {
    "level1": {
        "level2": {
            "level3": {
                "value": [1, 2, 3, [4, 5, [6, 7]]]
            }
        }
    }
}
'''
        result = CommunityNodeExecutor.execute_python(code, {}, timeout=5)
        
        assert result['success'] == True
        assert result['outputs']['nested']['level1']['level2']['level3']['value'][3][2][1] == 7


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
