"""
Community Node
Generic node class for community-defined nodes
"""

from typing import Dict, Any, Optional, List
from .node_base import NodeBase
from .community_node_executor import CommunityNodeExecutor


class CommunityNode(NodeBase):
    """
    社区节点 - 可以执行用户定义的代码或 HTTP 调用
    
    这个类作为所有社区节点的基类，根据配置动态执行
    """
    
    def __init__(self, node_config: Dict[str, Any], **kwargs):
        """
        初始化社区节点
        
        Args:
            node_config: 节点配置字典（从数据库加载）
                - nodeId: 节点ID
                - name: 节点名称
                - displayName: 显示名称
                - category: 类别
                - version: 版本
                - description: 描述
                - inputs: 输入配置列表
                - outputs: 输出配置列表
                - executionType: 执行类型 ('python' or 'http')
                - executionConfig: 执行配置
                - authorId: 创作者ID
                - authorName: 创作者名称
        """
        self.node_config = node_config
        self.node_id = node_config.get('nodeId')
        self.execution_type = node_config.get('executionType', 'python')
        self.execution_config = node_config.get('executionConfig', {})
        
        # 构建输入输出句柄
        input_handles = self._build_handles(node_config.get('inputs', []))
        output_handles = self._build_handles(node_config.get('outputs', []))
        
        # 初始化基类
        super().__init__(
            version=node_config.get('version', '0.0.1'),
            display_name=node_config.get('displayName', node_config.get('name')),
            node_category=node_config.get('category', 'community'),
            description=node_config.get('description', ''),
            input_handles=input_handles,
            output_handles=output_handles,
            **kwargs
        )
        
        # 设置额外的元数据
        self.update_metadata({
            'node_id': self.node_id,
            'author_id': node_config.get('authorId'),
            'author_name': node_config.get('authorName'),
            'execution_type': self.execution_type,
            'is_community_node': True
        })
    
    def _build_handles(self, handle_configs: List[Dict]) -> List[Dict]:
        """
        从配置构建输入/输出句柄
        
        Args:
            handle_configs: 句柄配置列表
            
        Returns:
            格式化的句柄列表
        """
        handles = []
        for config in handle_configs:
            handle = {
                'name': config.get('name'),
                'type': config.get('type', 'string'),
                'description': config.get('description', '')
            }
            
            # 输入句柄的额外字段
            if 'required' in config:
                handle['required'] = config['required']
            if 'default' in config:
                handle['default'] = config['default']
            
            handles.append(handle)
        
        return handles
    
    def validate_inputs(self, data: Dict[str, Any]) -> tuple:
        """
        验证输入数据
        
        Args:
            data: 输入数据字典
            
        Returns:
            (is_valid, error_message)
        """
        # 获取输入句柄配置
        input_configs = {h['name']: h for h in self.node_config.get('inputs', [])}
        
        # 检查必需字段
        for name, config in input_configs.items():
            if config.get('required', False) and name not in data:
                # 检查是否有默认值
                if 'default' not in config:
                    return False, f"Required input '{name}' is missing"
                # 使用默认值
                data[name] = config['default']
        
        # 类型验证（基础）
        for name, value in data.items():
            if name in input_configs:
                expected_type = input_configs[name].get('type', 'string')
                if not self._validate_type(value, expected_type):
                    return False, f"Input '{name}' has invalid type. Expected: {expected_type}"
        
        return True, None
    
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """
        验证值的类型
        
        Args:
            value: 值
            expected_type: 期望的类型字符串
            
        Returns:
            是否匹配
        """
        type_mapping = {
            'string': str,
            'number': (int, float),
            'boolean': bool,
            'json': (dict, list),
            'array': list,
            'object': dict
        }
        
        expected_python_type = type_mapping.get(expected_type)
        if expected_python_type is None:
            return True  # 未知类型，不验证
        
        return isinstance(value, expected_python_type)
    
    def execute(self, **inputs) -> Dict[str, Any]:
        """
        执行社区节点
        
        Args:
            **inputs: 输入参数
            
        Returns:
            输出字典
        """
        try:
            # 记录开始
            self.log(f"Executing community node: {self.node_id}", level="info")
            
            # 验证输入
            is_valid, error_msg = self.validate_inputs(inputs)
            if not is_valid:
                self.log(f"Input validation failed: {error_msg}", level="error")
                return {
                    'success': False,
                    'error': error_msg
                }
            
            # 执行节点
            result = CommunityNodeExecutor.execute(
                self.execution_type,
                self.execution_config,
                inputs
            )
            
            # 记录执行结果
            if result.get('success'):
                self.log("Execution successful", level="info")
                
                # 记录执行日志
                if 'logs' in result:
                    for log_entry in result['logs']:
                        self.log(log_entry['message'], level=log_entry['level'])
                
                return result.get('outputs', {})
            else:
                error = result.get('error', 'Unknown error')
                self.log(f"Execution failed: {error}", level="error")
                return {
                    'success': False,
                    'error': error,
                    'error_type': result.get('error_type')
                }
                
        except Exception as e:
            self.log(f"Unexpected error: {str(e)}", level="error")
            return {
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__
            }
    
    @classmethod
    def from_database(cls, node_config: Dict[str, Any]):
        """
        从数据库配置创建节点实例
        
        Args:
            node_config: 节点配置字典
            
        Returns:
            CommunityNode 实例
        """
        return cls(node_config)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典（用于序列化）
        
        Returns:
            节点配置字典
        """
        base_dict = super().to_dict()
        base_dict.update({
            'node_id': self.node_id,
            'execution_type': self.execution_type,
            'node_config': self.node_config
        })
        return base_dict
