"""
Node Registry
Manages node registration and version tracking
"""

from typing import Dict, List, Optional, Type, Any
from .version_manager import VersionManager
from .node_base import NodeBase


class NodeRegistry:
    """
    节点注册表 - 管理所有节点类型和版本
    
    存储结构:
    {
        'node_type': {
            '0.0.1': NodeClass,
            '0.0.2': NodeClass,
            ...
        }
    }
    
    Examples:
        >>> NodeRegistry.register('vault_node', '0.0.1', VaultNode)
        >>> node_class = NodeRegistry.get_node('vault_node', '0.0.1')
        >>> versions = NodeRegistry.get_all_versions('vault_node')
    """
    
    _nodes: Dict[str, Dict[str, Type[NodeBase]]] = {}
    _metadata_cache: Dict[str, Dict[str, Dict]] = {}
    
    @classmethod
    def register(cls, node_type: str, version: str, node_class: Type[NodeBase],
                 metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        注册节点类型和版本
        
        Args:
            node_type: 节点类型标识符 (如 'vault_node')
            version: 版本号 (如 '0.0.1')
            node_class: 节点类
            metadata: 额外的元数据
        
        Raises:
            ValueError: 如果版本格式不正确或已存在
        """
        # 验证版本格式
        if not VersionManager.validate_version(version):
            raise ValueError(f"Invalid version format: {version}")
        
        # 初始化节点类型字典
        if node_type not in cls._nodes:
            cls._nodes[node_type] = {}
            cls._metadata_cache[node_type] = {}
        
        # 检查版本是否已存在
        if version in cls._nodes[node_type]:
            raise ValueError(f"Node {node_type} version {version} already registered")
        
        # 注册节点
        cls._nodes[node_type][version] = node_class
        
        # 缓存元数据
        if metadata:
            cls._metadata_cache[node_type][version] = metadata
        
        print(f"✓ Registered: {node_type} v{version}")
    
    @classmethod
    def get_node(cls, node_type: str, version: Optional[str] = None) -> Type[NodeBase]:
        """
        获取节点类
        
        Args:
            node_type: 节点类型
            version: 版本号，如果为 None 则返回最新版本
            
        Returns:
            节点类
            
        Raises:
            KeyError: 如果节点类型或版本不存在
        """
        if node_type not in cls._nodes:
            raise KeyError(f"Node type '{node_type}' not found")
        
        # 如果未指定版本，获取最新版本
        if version is None:
            version = cls.get_latest_version(node_type)
            if version is None:
                raise KeyError(f"No versions available for node type '{node_type}'")
        
        if version not in cls._nodes[node_type]:
            available = ', '.join(cls._nodes[node_type].keys())
            raise KeyError(f"Version {version} not found for {node_type}. Available: {available}")
        
        return cls._nodes[node_type][version]
    
    @classmethod
    def get_latest_version(cls, node_type: str) -> Optional[str]:
        """
        获取节点类型的最新版本
        
        Args:
            node_type: 节点类型
            
        Returns:
            最新版本号，如果不存在则返回 None
        """
        if node_type not in cls._nodes:
            return None
        
        versions = list(cls._nodes[node_type].keys())
        return VersionManager.get_latest_version(versions)
    
    @classmethod
    def get_all_versions(cls, node_type: str) -> List[str]:
        """
        获取节点类型的所有版本
        
        Args:
            node_type: 节点类型
            
        Returns:
            版本列表，按版本号排序
        """
        if node_type not in cls._nodes:
            return []
        
        versions = list(cls._nodes[node_type].keys())
        from functools import cmp_to_key
        return sorted(versions, key=cmp_to_key(VersionManager.compare_versions), reverse=True)
    
    @classmethod
    def get_all_node_types(cls) -> List[str]:
        """
        获取所有注册的节点类型
        
        Returns:
            节点类型列表
        """
        return list(cls._nodes.keys())
    
    @classmethod
    def get_node_info(cls, node_type: str, version: Optional[str] = None) -> Dict[str, Any]:
        """
        获取节点信息（包含元数据）
        
        Args:
            node_type: 节点类型
            version: 版本号
            
        Returns:
            节点信息字典
        """
        if version is None:
            version = cls.get_latest_version(node_type)
        
        node_class = cls.get_node(node_type, version)
        
        # 获取类级别元数据
        metadata = {}
        if hasattr(node_class, 'get_class_metadata'):
            metadata = node_class.get_class_metadata()
        
        # 合并缓存的元数据
        if node_type in cls._metadata_cache and version in cls._metadata_cache[node_type]:
            metadata.update(cls._metadata_cache[node_type][version])
        
        return {
            'node_type': node_type,
            'version': version,
            'class': node_class.__name__,
            'metadata': metadata,
            'all_versions': cls.get_all_versions(node_type)
        }
    
    @classmethod
    def list_all_nodes(cls, include_versions: bool = False) -> List[Dict[str, Any]]:
        """
        列出所有节点
        
        Args:
            include_versions: 是否包含所有版本信息
            
        Returns:
            节点信息列表
        """
        result = []
        
        for node_type in cls.get_all_node_types():
            if include_versions:
                # 包含所有版本
                for version in cls.get_all_versions(node_type):
                    result.append(cls.get_node_info(node_type, version))
            else:
                # 只包含最新版本
                latest_version = cls.get_latest_version(node_type)
                if latest_version:
                    result.append(cls.get_node_info(node_type, latest_version))
        
        return result
    
    @classmethod
    def is_registered(cls, node_type: str, version: Optional[str] = None) -> bool:
        """
        检查节点是否已注册
        
        Args:
            node_type: 节点类型
            version: 版本号（可选）
            
        Returns:
            是否已注册
        """
        if node_type not in cls._nodes:
            return False
        
        if version is None:
            return True
        
        return version in cls._nodes[node_type]
    
    @classmethod
    def clear(cls) -> None:
        """
        清空注册表（主要用于测试）
        """
        cls._nodes.clear()
        cls._metadata_cache.clear()
    
    @classmethod
    def get_compatible_version(cls, node_type: str, version_range: str) -> Optional[str]:
        """
        获取兼容的版本
        
        Args:
            node_type: 节点类型
            version_range: 版本范围表达式 (如 '^1.0.0', '~1.2.0')
            
        Returns:
            兼容的最新版本，如果没有则返回 None
        """
        if node_type not in cls._nodes:
            return None
        
        versions = cls.get_all_versions(node_type)
        
        # 找到所有匹配的版本
        compatible_versions = [
            v for v in versions 
            if VersionManager.match_version_range(v, version_range)
        ]
        
        if not compatible_versions:
            return None
        
        # 返回最新的兼容版本
        return VersionManager.get_latest_version(compatible_versions)


def register_node(node_type: str, version: str = '0.0.1', **metadata):
    """
    节点注册装饰器
    
    使用方式:
        @register_node('vault_node', version='0.0.1')
        class VaultNode(NodeBase):
            pass
    
    Args:
        node_type: 节点类型
        version: 版本号
        **metadata: 额外的元数据
        
    Returns:
        装饰器函数
    """
    def decorator(node_class: Type[NodeBase]):
        # 设置类级别元数据
        if hasattr(node_class, 'set_class_metadata'):
            node_class.set_class_metadata({
                'version': version,
                **metadata
            })
        
        # 注册到注册表
        NodeRegistry.register(node_type, version, node_class, metadata)
        
        return node_class
    
    return decorator
