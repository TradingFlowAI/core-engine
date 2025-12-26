"""
Community Node
Generic node class for community-defined nodes
"""

from typing import Dict, Any, Optional, List
from nodes.node_base import NodeBase
from .community_node_executor import CommunityNodeExecutor


class CommunityNode(NodeBase):
    """
    Community Node - Can execute user-defined code or HTTP calls.
    
    This class serves as base class for all community nodes, executes dynamically based on config.
    """
    
    def __init__(self, node_config: Dict[str, Any], **kwargs):
        """
        Initialize community node.
        
        Args:
            node_config: Node config dictionary (loaded from database)
                - nodeId: Node ID
                - name: Node name
                - displayName: Display name
                - category: Category
                - version: Version
                - description: Description
                - inputs: Input config list
                - outputs: Output config list
                - executionType: Execution type ('python' or 'http')
                - executionConfig: Execution config
                - authorId: Author ID
                - authorName: Author name
        """
        self.node_config = node_config
        self.node_id = node_config.get('nodeId')
        self.execution_type = node_config.get('executionType', 'python')
        self.execution_config = node_config.get('executionConfig', {})
        
        # Build input/output handles
        input_handles = self._build_handles(node_config.get('inputs', []))
        output_handles = self._build_handles(node_config.get('outputs', []))
        
        # Initialize base class
        super().__init__(
            version=node_config.get('version', '0.0.1'),
            display_name=node_config.get('displayName', node_config.get('name')),
            node_category=node_config.get('category', 'community'),
            description=node_config.get('description', ''),
            input_handles=input_handles,
            output_handles=output_handles,
            **kwargs
        )
        
        # Set additional metadata
        self.update_metadata({
            'node_id': self.node_id,
            'author_id': node_config.get('authorId'),
            'author_name': node_config.get('authorName'),
            'execution_type': self.execution_type,
            'is_community_node': True
        })
    
    def _build_handles(self, handle_configs: List[Dict]) -> List[Dict]:
        """
        Build input/output handles from config.
        
        Args:
            handle_configs: Handle config list
            
        Returns:
            Formatted handle list
        """
        handles = []
        for config in handle_configs:
            handle = {
                'name': config.get('name'),
                'type': config.get('type', 'string'),
                'description': config.get('description', '')
            }
            
            # Additional fields for input handles
            if 'required' in config:
                handle['required'] = config['required']
            if 'default' in config:
                handle['default'] = config['default']
            
            handles.append(handle)
        
        return handles
    
    def validate_inputs(self, data: Dict[str, Any]) -> tuple:
        """
        Validate input data.
        
        Args:
            data: Input data dictionary
            
        Returns:
            (is_valid, error_message)
        """
        # Get input handle configs
        input_configs = {h['name']: h for h in self.node_config.get('inputs', [])}
        
        # Check required fields
        for name, config in input_configs.items():
            if config.get('required', False) and name not in data:
                # Check if has default value
                if 'default' not in config:
                    return False, f"Required input '{name}' is missing"
                # Use default value
                data[name] = config['default']
        
        # Type validation (basic)
        for name, value in data.items():
            if name in input_configs:
                expected_type = input_configs[name].get('type', 'string')
                if not self._validate_type(value, expected_type):
                    return False, f"Input '{name}' has invalid type. Expected: {expected_type}"
        
        return True, None
    
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """
        Validate value type.
        
        Args:
            value: Value
            expected_type: Expected type string
            
        Returns:
            Whether matches
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
            return True  # Unknown type, skip validation
        
        return isinstance(value, expected_python_type)
    
    def execute(self, **inputs) -> Dict[str, Any]:
        """
        Execute community node.
        
        Args:
            **inputs: Input parameters
            
        Returns:
            Output dictionary
        """
        try:
            # Log start
            self.log(f"Executing community node: {self.node_id}", level="info")
            
            # Validate inputs
            is_valid, error_msg = self.validate_inputs(inputs)
            if not is_valid:
                self.log(f"Input validation failed: {error_msg}", level="error")
                return {
                    'success': False,
                    'error': error_msg
                }
            
            # Execute node
            result = CommunityNodeExecutor.execute(
                self.execution_type,
                self.execution_config,
                inputs
            )
            
            # Log execution result
            if result.get('success'):
                self.log("Execution successful", level="info")
                
                # Log execution logs
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
        Create node instance from database config.
        
        Args:
            node_config: Node config dictionary
            
        Returns:
            CommunityNode instance
        """
        return cls(node_config)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary (for serialization).
        
        Returns:
            Node config dictionary
        """
        base_dict = super().to_dict()
        base_dict.update({
            'node_id': self.node_id,
            'execution_type': self.execution_type,
            'node_config': self.node_config
        })
        return base_dict
