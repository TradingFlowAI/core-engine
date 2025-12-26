"""
Node Registry
Manages node registration and version tracking
"""

from __future__ import annotations
from typing import Dict, List, Optional, Type, Any, TYPE_CHECKING
from .version_manager import VersionManager

if TYPE_CHECKING:
    from nodes.node_base import NodeBase


class NodeRegistry:
    """
    Node Registry - Manages all node types and versions
    
    Storage structure:
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
        Register node type and version.
        
        Args:
            node_type: Node type identifier (e.g., 'vault_node')
            version: Version number (e.g., '0.0.1')
            node_class: Node class
            metadata: Additional metadata
        
        Raises:
            ValueError: If version format is invalid or already exists
        """
        # Validate version format
        if not VersionManager.validate_version(version):
            raise ValueError(f"Invalid version format: {version}")
        
        # Initialize node type dict
        if node_type not in cls._nodes:
            cls._nodes[node_type] = {}
            cls._metadata_cache[node_type] = {}
        
        # Check if version already exists
        if version in cls._nodes[node_type]:
            raise ValueError(f"Node {node_type} version {version} already registered")
        
        # Register node
        cls._nodes[node_type][version] = node_class
        
        # Cache metadata
        if metadata:
            cls._metadata_cache[node_type][version] = metadata
        
        print(f"✓ Registered: {node_type} v{version}")
    
    @classmethod
    def get_node(cls, node_type: str, version: Optional[str] = None) -> Type[NodeBase]:
        """
        Get node class.
        
        Args:
            node_type: Node type
            version: Version number, returns latest if None
            
        Returns:
            Node class
            
        Raises:
            KeyError: If node type or version not found
        """
        if node_type not in cls._nodes:
            raise KeyError(f"Node type '{node_type}' not found")
        
        # If version not specified, get latest
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
        Get latest version for node type.
        
        Args:
            node_type: Node type
            
        Returns:
            Latest version number, None if not exists
        """
        if node_type not in cls._nodes:
            return None
        
        versions = list(cls._nodes[node_type].keys())
        return VersionManager.get_latest_version(versions)
    
    @classmethod
    def get_all_versions(cls, node_type: str) -> List[str]:
        """
        Get all versions for node type.
        
        Args:
            node_type: Node type
            
        Returns:
            Version list, sorted by version number
        """
        if node_type not in cls._nodes:
            return []
        
        versions = list(cls._nodes[node_type].keys())
        from functools import cmp_to_key
        return sorted(versions, key=cmp_to_key(VersionManager.compare_versions), reverse=True)
    
    @classmethod
    def get_all_node_types(cls) -> List[str]:
        """
        Get all registered node types.
        
        Returns:
            List of node types
        """
        return list(cls._nodes.keys())
    
    @classmethod
    def get_node_info(cls, node_type: str, version: Optional[str] = None) -> Dict[str, Any]:
        """
        Get node info (including metadata).
        
        Args:
            node_type: Node type
            version: Version number
            
        Returns:
            Node info dictionary
        """
        if version is None:
            version = cls.get_latest_version(node_type)
        
        node_class = cls.get_node(node_type, version)
        
        # Get class-level metadata
        metadata = {}
        if hasattr(node_class, 'get_class_metadata'):
            metadata = node_class.get_class_metadata()
        
        # Merge cached metadata
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
        List all nodes.
        
        Args:
            include_versions: Whether to include all version info
            
        Returns:
            Node info list
        """
        result = []
        
        for node_type in cls.get_all_node_types():
            if include_versions:
                # Include all versions
                for version in cls.get_all_versions(node_type):
                    result.append(cls.get_node_info(node_type, version))
            else:
                # Only include latest version
                latest_version = cls.get_latest_version(node_type)
                if latest_version:
                    result.append(cls.get_node_info(node_type, latest_version))
        
        return result
    
    @classmethod
    def is_registered(cls, node_type: str, version: Optional[str] = None) -> bool:
        """
        Check if node is registered.
        
        Args:
            node_type: Node type
            version: Version number (optional)
            
        Returns:
            Whether registered
        """
        if node_type not in cls._nodes:
            return False
        
        if version is None:
            return True
        
        return version in cls._nodes[node_type]
    
    @classmethod
    def clear(cls) -> None:
        """
        Clear registry (mainly for testing).
        """
        cls._nodes.clear()
        cls._metadata_cache.clear()
    
    @classmethod
    def resolve_version(cls, node_type: str, version_spec: str) -> str:
        """
        Resolve version spec to actual version (supports latest, range expressions, etc.)
        
        Args:
            node_type: Node type
            version_spec: Version specification
                - "latest": Latest stable version
                - "latest-beta": Latest beta version
                - "^1.2.0": Compatible with 1.x.x
                - "~1.2.0": Compatible with 1.2.x
                - "1.2.3": Exact version
                - "1.2.3-beta.1": Exact prerelease version
                
        Returns:
            Resolved specific version number
            
        Raises:
            KeyError: If node type not found
            ValueError: If cannot resolve version or version not found
            
        Examples:
            >>> NodeRegistry.resolve_version("code_node", "latest")
            "1.2.3"
            >>> NodeRegistry.resolve_version("code_node", "^1.0.0")
            "1.2.5"
            >>> NodeRegistry.resolve_version("code_node", "latest-beta")
            "1.3.0-beta.1"
        """
        if node_type not in cls._nodes:
            raise KeyError(f"Node type '{node_type}' not found")
        
        available_versions = cls.get_all_versions(node_type)
        if not available_versions:
            raise ValueError(f"No versions available for node type '{node_type}'")
        
        # Use VersionManager to resolve version spec
        resolved = VersionManager.resolve_version_spec(version_spec, available_versions)
        
        if resolved is None:
            raise ValueError(
                f"Cannot resolve version spec '{version_spec}' for node type '{node_type}'. "
                f"Available versions: {', '.join(available_versions)}"
            )
        
        return resolved
    
    @classmethod
    def get_compatible_version(cls, node_type: str, version_range: str) -> Optional[str]:
        """
        Get compatible version (deprecated, use resolve_version instead).
        
        Args:
            node_type: Node type
            version_range: Version range expression (e.g., '^1.0.0', '~1.2.0')
            
        Returns:
            Latest compatible version, None if not found
        """
        try:
            return cls.resolve_version(node_type, version_range)
        except (KeyError, ValueError):
            return None


def register_node(node_type: str, version: str = '0.0.1', **metadata):
    """
    Node registration decorator.
    
    Usage:
        @register_node('vault_node', version='0.0.1')
        class VaultNode(NodeBase):
            pass
    
    Args:
        node_type: Node type
        version: Version number
        **metadata: Additional metadata
        
    Returns:
        Decorator function
    """
    def decorator(node_class: Type[NodeBase]):
        # Set class-level metadata
        if hasattr(node_class, 'set_class_metadata'):
            node_class.set_class_metadata({
                'version': version,
                **metadata
            })
        
        # Register to registry
        NodeRegistry.register(node_type, version, node_class, metadata)
        
        return node_class
    
    return decorator
