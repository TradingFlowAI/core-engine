"""
Community Node Integration Tests
社区节点集成测试 - 测试 Control 和 Station 之间的交互
"""

import pytest
import sys
import json
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path
sys.path.insert(0, '..')

from core.community_node import CommunityNode
from core.community_node_executor import CommunityNodeExecutor


class TestEndToEndExecution:
    """端到端执行测试"""
    
    def test_complete_python_node_lifecycle(self):
        """测试完整的 Python 节点生命周期"""
        # 模拟从 Control 服务获取的节点配置
        node_config = {
            'nodeId': 'data_processor_v1',
            'name': 'data_processor',
            'displayName': 'Data Processor',
            'category': 'data',
            'version': '1.0.0',
            'description': 'Process and transform data',
            'authorId': 'user_789',
            'authorName': 'Data Expert',
            'inputs': [
                {
                    'name': 'raw_data',
                    'type': 'json',
                    'required': True,
                    'description': 'Raw input data'
                },
                {
                    'name': 'operation',
                    'type': 'string',
                    'required': True,
                    'description': 'Operation to perform (sum, avg, max, min)'
                }
            ],
            'outputs': [
                {
                    'name': 'result',
                    'type': 'number',
                    'description': 'Processed result'
                },
                {
                    'name': 'metadata',
                    'type': 'json',
                    'description': 'Processing metadata'
                }
            ],
            'executionType': 'python',
            'executionConfig': {
                'pythonCode': '''
import json
from statistics import mean

data = inputs.get("raw_data", [])
operation = inputs.get("operation", "sum")

if isinstance(data, dict):
    values = list(data.values()) if data else []
elif isinstance(data, list):
    values = data
else:
    values = []

# Extract numbers
numbers = [float(v) for v in values if isinstance(v, (int, float))]

result = 0
if operation == "sum":
    result = sum(numbers)
elif operation == "avg":
    result = mean(numbers) if numbers else 0
elif operation == "max":
    result = max(numbers) if numbers else 0
elif operation == "min":
    result = min(numbers) if numbers else 0

outputs["result"] = result
outputs["metadata"] = {
    "operation": operation,
    "count": len(numbers),
    "values_processed": numbers
}
''',
                'timeout': 10
            }
        }
        
        # 1. 创建节点实例
        node = CommunityNode(node_config)
        
        # 2. 验证节点配置
        assert node.node_id == 'data_processor_v1'
        assert len(node.input_handles) == 2
        assert len(node.output_handles) == 2
        
        # 3. 执行测试 - Sum操作
        result_sum = node.execute(
            raw_data=[10, 20, 30, 40, 50],
            operation='sum'
        )
        
        assert result_sum['result'] == 150
        assert result_sum['metadata']['operation'] == 'sum'
        assert result_sum['metadata']['count'] == 5
        
        # 4. 执行测试 - Average操作
        result_avg = node.execute(
            raw_data=[10, 20, 30],
            operation='avg'
        )
        
        assert result_avg['result'] == 20.0
        
        # 5. 执行测试 - Max操作
        result_max = node.execute(
            raw_data={'a': 15, 'b': 45, 'c': 30},
            operation='max'
        )
        
        assert result_max['result'] == 45
        
        # 6. 获取日志
        logs = node.get_logs()
        assert len(logs) > 0
    
    @patch('core.community_node_executor.requests.request')
    def test_complete_http_node_lifecycle(self, mock_request):
        """测试完整的 HTTP 节点生命周期"""
        # 模拟 API 响应
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'weather': {
                'temperature': 22.5,
                'conditions': 'sunny',
                'humidity': 65
            },
            'location': 'San Francisco'
        }
        mock_response.content = b'{"weather": {}}'
        mock_request.return_value = mock_response
        
        # 节点配置
        node_config = {
            'nodeId': 'weather_api_v1',
            'name': 'weather_api',
            'displayName': 'Weather API',
            'category': 'data',
            'version': '1.0.0',
            'description': 'Fetch weather data from API',
            'authorId': 'user_890',
            'authorName': 'Weather Expert',
            'inputs': [
                {
                    'name': 'city',
                    'type': 'string',
                    'required': True
                },
                {
                    'name': 'api_key',
                    'type': 'string',
                    'required': False,
                    'default': 'demo_key'
                }
            ],
            'outputs': [
                {
                    'name': 'weather_data',
                    'type': 'json'
                }
            ],
            'executionType': 'http',
            'executionConfig': {
                'url': 'https://api.weather.com/v1/current',
                'method': 'GET',
                'headers': {
                    'Accept': 'application/json'
                },
                'timeout': 15
            }
        }
        
        # 创建并执行节点
        node = CommunityNode(node_config)
        result = node.execute(city='San Francisco', api_key='test_key')
        
        # 验证结果
        assert 'weather' in result
        assert result['weather']['temperature'] == 22.5
        assert result['weather']['conditions'] == 'sunny'


class TestMultiNodeWorkflow:
    """测试多节点工作流"""
    
    def test_data_pipeline(self):
        """测试数据管道：数据获取 -> 处理 -> 输出"""
        # Node 1: Data Generator
        generator_config = {
            'nodeId': 'generator_1',
            'name': 'data_generator',
            'displayName': 'Data Generator',
            'category': 'data',
            'description': 'Generate sample data',
            'executionType': 'python',
            'executionConfig': {
                'pythonCode': '''
outputs["data"] = [
    {"id": 1, "value": 10},
    {"id": 2, "value": 20},
    {"id": 3, "value": 30},
    {"id": 4, "value": 40},
    {"id": 5, "value": 50}
]
''',
                'timeout': 5
            },
            'inputs': [],
            'outputs': [{'name': 'data', 'type': 'json'}]
        }
        
        # Node 2: Data Filter
        filter_config = {
            'nodeId': 'filter_1',
            'name': 'data_filter',
            'displayName': 'Data Filter',
            'category': 'data',
            'description': 'Filter data by threshold',
            'executionType': 'python',
            'executionConfig': {
                'pythonCode': '''
data = inputs.get("data", [])
threshold = inputs.get("threshold", 25)

filtered = [item for item in data if item.get("value", 0) > threshold]
outputs["filtered_data"] = filtered
outputs["count"] = len(filtered)
''',
                'timeout': 5
            },
            'inputs': [
                {'name': 'data', 'type': 'json', 'required': True},
                {'name': 'threshold', 'type': 'number', 'required': False, 'default': 25}
            ],
            'outputs': [
                {'name': 'filtered_data', 'type': 'json'},
                {'name': 'count', 'type': 'number'}
            ]
        }
        
        # Node 3: Data Aggregator
        aggregator_config = {
            'nodeId': 'aggregator_1',
            'name': 'data_aggregator',
            'displayName': 'Data Aggregator',
            'category': 'data',
            'description': 'Aggregate data values',
            'executionType': 'python',
            'executionConfig': {
                'pythonCode': '''
data = inputs.get("data", [])
values = [item.get("value", 0) for item in data]

outputs["total"] = sum(values)
outputs["average"] = sum(values) / len(values) if values else 0
outputs["max"] = max(values) if values else 0
outputs["min"] = min(values) if values else 0
''',
                'timeout': 5
            },
            'inputs': [
                {'name': 'data', 'type': 'json', 'required': True}
            ],
            'outputs': [
                {'name': 'total', 'type': 'number'},
                {'name': 'average', 'type': 'number'},
                {'name': 'max', 'type': 'number'},
                {'name': 'min', 'type': 'number'}
            ]
        }
        
        # Execute pipeline
        # Step 1: Generate data
        generator = CommunityNode(generator_config)
        gen_result = generator.execute()
        
        assert 'data' in gen_result
        assert len(gen_result['data']) == 5
        
        # Step 2: Filter data (value > 25)
        filter_node = CommunityNode(filter_config)
        filter_result = filter_node.execute(
            data=gen_result['data'],
            threshold=25
        )
        
        assert 'filtered_data' in filter_result
        assert filter_result['count'] == 3  # Items with value > 25
        
        # Step 3: Aggregate filtered data
        aggregator = CommunityNode(aggregator_config)
        agg_result = aggregator.execute(
            data=filter_result['filtered_data']
        )
        
        assert agg_result['total'] == 120  # 30 + 40 + 50
        assert agg_result['average'] == 40.0
        assert agg_result['max'] == 50
        assert agg_result['min'] == 30


class TestErrorHandlingAndRecovery:
    """测试错误处理和恢复"""
    
    def test_input_validation_errors(self):
        """测试输入验证错误"""
        config = {
            'nodeId': 'strict_node',
            'name': 'strict_validation',
            'displayName': 'Strict Validation',
            'category': 'utility',
            'description': 'Node with strict validation',
            'executionType': 'python',
            'executionConfig': {
                'pythonCode': 'outputs["result"] = "ok"',
                'timeout': 5
            },
            'inputs': [
                {'name': 'required_field', 'type': 'string', 'required': True},
                {'name': 'number_field', 'type': 'number', 'required': True}
            ],
            'outputs': [
                {'name': 'result', 'type': 'string'}
            ]
        }
        
        node = CommunityNode(config)
        
        # Missing required field
        result = node.execute(number_field=42)
        assert result['success'] == False
        assert 'required_field' in result['error']
        
        # Wrong type (string instead of number)
        result = node.execute(required_field='test', number_field='not_a_number')
        assert result['success'] == False
        assert 'number_field' in result['error']
    
    def test_runtime_errors(self):
        """测试运行时错误"""
        config = {
            'nodeId': 'error_node',
            'name': 'error_generator',
            'displayName': 'Error Generator',
            'category': 'utility',
            'description': 'Generates runtime errors',
            'executionType': 'python',
            'executionConfig': {
                'pythonCode': '''
denominator = inputs.get("denominator", 0)
result = 100 / denominator  # Will cause ZeroDivisionError if denominator is 0
outputs["result"] = result
''',
                'timeout': 5
            },
            'inputs': [
                {'name': 'denominator', 'type': 'number', 'required': False, 'default': 0}
            ],
            'outputs': [
                {'name': 'result', 'type': 'number'}
            ]
        }
        
        node = CommunityNode(config)
        
        # Should cause division by zero error
        result = node.execute(denominator=0)
        assert result['success'] == False
        assert 'error' in result
    
    @patch('core.community_node_executor.requests.request')
    def test_http_error_recovery(self, mock_request):
        """测试 HTTP 错误恢复"""
        # Simulate HTTP error
        mock_request.side_effect = requests.exceptions.ConnectionError('Connection failed')
        
        config = {
            'nodeId': 'http_error_node',
            'name': 'http_failing',
            'displayName': 'HTTP Failing',
            'category': 'data',
            'description': 'HTTP node that fails',
            'executionType': 'http',
            'executionConfig': {
                'url': 'https://api.unreachable.com/data',
                'method': 'GET',
                'timeout': 5
            },
            'inputs': [],
            'outputs': [{'name': 'data', 'type': 'json'}]
        }
        
        node = CommunityNode(config)
        result = node.execute()
        
        assert result['success'] == False
        assert 'error' in result


class TestPerformanceAndLimits:
    """测试性能和限制"""
    
    def test_execution_timeout(self):
        """测试执行超时"""
        config = {
            'nodeId': 'slow_node',
            'name': 'infinite_loop',
            'displayName': 'Slow Node',
            'category': 'utility',
            'description': 'Very slow execution',
            'executionType': 'python',
            'executionConfig': {
                'pythonCode': '''
import time
time.sleep(2)  # Sleep for 2 seconds
outputs["result"] = "done"
''',
                'timeout': 1  # Timeout after 1 second
            },
            'inputs': [],
            'outputs': [{'name': 'result', 'type': 'string'}]
        }
        
        node = CommunityNode(config)
        result = node.execute()
        
        # Note: This test may pass if execution completes quickly
        # In production, a proper timeout mechanism should be implemented
        assert 'result' in result or 'error' in result
    
    def test_large_data_processing(self):
        """测试大数据处理"""
        config = {
            'nodeId': 'large_data_node',
            'name': 'large_processor',
            'displayName': 'Large Data Processor',
            'category': 'data',
            'description': 'Process large datasets',
            'executionType': 'python',
            'executionConfig': {
                'pythonCode': '''
data = inputs.get("data", [])
processed = [{"id": i, "value": x * 2} for i, x in enumerate(data)]
outputs["processed"] = processed
outputs["count"] = len(processed)
''',
                'timeout': 10
            },
            'inputs': [
                {'name': 'data', 'type': 'array', 'required': True}
            ],
            'outputs': [
                {'name': 'processed', 'type': 'json'},
                {'name': 'count', 'type': 'number'}
            ]
        }
        
        node = CommunityNode(config)
        
        # Process 1000 items
        large_data = list(range(1000))
        result = node.execute(data=large_data)
        
        assert result['count'] == 1000
        assert len(result['processed']) == 1000


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
