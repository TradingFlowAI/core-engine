"""
版本验证器 - 用于 Flow Linter

提供版本语法验证和兼容性检查功能
"""

import re
from typing import List, Dict, Optional, Tuple
from .version_manager import VersionManager
from .node_registry import NodeRegistry


class VersionValidationError:
    """版本验证错误"""
    
    def __init__(
        self,
        node_id: str,
        error_type: str,
        message: str,
        severity: str = 'error',  # 'error', 'warning', 'info'
        suggestion: Optional[str] = None
    ):
        self.node_id = node_id
        self.error_type = error_type
        self.message = message
        self.severity = severity
        self.suggestion = suggestion
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'node_id': self.node_id,
            'error_type': self.error_type,
            'message': self.message,
            'severity': self.severity,
            'suggestion': self.suggestion
        }


class VersionValidator:
    """版本验证器"""
    
    # 有效的版本规范模式
    VERSION_PATTERNS = {
        'exact': r'^\d+\.\d+\.\d+(-[a-zA-Z0-9.-]+)?(\+[a-zA-Z0-9.-]+)?$',  # 1.2.3, 1.2.3-beta.1
        'latest': r'^latest(-[a-zA-Z]+)?$',  # latest, latest-beta
        'caret': r'^\^\d+\.\d+\.\d+$',  # ^1.2.0
        'tilde': r'^~\d+\.\d+\.\d+$',  # ~1.2.0
        'comparison': r'^(>=?|<=?|>|<)\d+\.\d+\.\d+$',  # >=1.0.0, <2.0.0
    }
    
    @classmethod
    def validate_version_syntax(cls, version_spec: str) -> Tuple[bool, Optional[str]]:
        """
        验证版本规范语法
        
        Args:
            version_spec: 版本规范字符串
            
        Returns:
            (is_valid, error_message) 元组
        """
        if not version_spec or not isinstance(version_spec, str):
            return False, "Version specification is required"
        
        # 检查是否匹配任何有效模式
        for pattern_type, pattern in cls.VERSION_PATTERNS.items():
            if re.match(pattern, version_spec):
                return True, None
        
        return False, (
            f"Invalid version specification: '{version_spec}'. "
            f"Expected format: '1.2.3', 'latest', '^1.2.0', '~1.2.0', etc."
        )
    
    @classmethod
    def validate_node_version(
        cls,
        node_id: str,
        node_type: str,
        version_spec: str
    ) -> List[VersionValidationError]:
        """
        验证节点版本
        
        Args:
            node_id: 节点ID
            node_type: 节点类型
            version_spec: 版本规范
            
        Returns:
            验证错误列表
        """
        errors = []
        
        # 1. 验证版本语法
        is_valid, error_msg = cls.validate_version_syntax(version_spec)
        if not is_valid:
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='invalid_syntax',
                message=error_msg,
                severity='error',
                suggestion="Use a valid version format like '1.2.3', 'latest', or '^1.2.0'"
            ))
            return errors  # 语法错误时直接返回，不进行后续检查
        
        # 2. 检查节点类型是否存在
        try:
            available_versions = NodeRegistry.get_all_versions(node_type)
        except KeyError:
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='unknown_node_type',
                message=f"Unknown node type: '{node_type}'",
                severity='error',
                suggestion=f"Check if the node type '{node_type}' is registered"
            ))
            return errors
        
        # 3. 检查版本是否存在
        if not available_versions:
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='no_versions',
                message=f"No versions available for node type '{node_type}'",
                severity='error',
                suggestion="Register at least one version for this node type"
            ))
            return errors
        
        # 4. 尝试解析版本规范
        try:
            resolved_version = NodeRegistry.resolve_version(node_type, version_spec)
            if not resolved_version:
                errors.append(VersionValidationError(
                    node_id=node_id,
                    error_type='no_matching_version',
                    message=f"No version matching '{version_spec}' found for '{node_type}'",
                    severity='error',
                    suggestion=f"Available versions: {', '.join(available_versions)}"
                ))
        except ValueError as e:
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='version_resolution_error',
                message=str(e),
                severity='error',
                suggestion=f"Available versions: {', '.join(available_versions)}"
            ))
        
        # 5. 预发布版本警告
        if '-' in version_spec and not version_spec.startswith('latest'):
            errors.append(VersionValidationError(
                node_id=node_id,
                error_type='prerelease_version',
                message=f"Using prerelease version: '{version_spec}'",
                severity='warning',
                suggestion="Consider using a stable version for production"
            ))
        
        return errors
    
    @classmethod
    def validate_flow_versions(cls, flow_data: Dict) -> List[VersionValidationError]:
        """
        验证整个 Flow 的版本
        
        Args:
            flow_data: Flow JSON 数据
            
        Returns:
            验证错误列表
        """
        errors = []
        
        nodes = flow_data.get('nodes', [])
        for node in nodes:
            node_id = node.get('id')
            node_type = node.get('type') or node.get('nodeType')
            
            if not node_type:
                errors.append(VersionValidationError(
                    node_id=node_id,
                    error_type='missing_node_type',
                    message="Node type is missing",
                    severity='error'
                ))
                continue
            
            # 获取版本规范
            version_spec = None
            if 'version' in node:
                version_spec = node['version']
            elif 'data' in node and isinstance(node['data'], dict):
                version_spec = node['data'].get('version')
            
            # 如果没有指定版本，使用默认值
            if not version_spec:
                version_spec = 'latest'
                # 添加信息提示
                errors.append(VersionValidationError(
                    node_id=node_id,
                    error_type='missing_version',
                    message=f"No version specified, using default: 'latest'",
                    severity='info',
                    suggestion="Consider specifying an explicit version for better stability"
                ))
            
            # 验证节点版本
            node_errors = cls.validate_node_version(node_id, node_type, version_spec)
            errors.extend(node_errors)
        
        return errors
    
    @classmethod
    def check_version_compatibility(
        cls,
        source_node: Dict,
        target_node: Dict
    ) -> List[VersionValidationError]:
        """
        检查连接的两个节点之间的版本兼容性
        
        Args:
            source_node: 源节点数据
            target_node: 目标节点数据
            
        Returns:
            兼容性警告列表
        """
        warnings = []
        
        # TODO: 实现具体的兼容性检查逻辑
        # 例如：检查主版本号是否兼容
        # 例如：检查是否一个是预发布版本另一个是稳定版本
        
        return warnings
