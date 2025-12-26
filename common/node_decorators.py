"""Node decorators for automatic node type registration (with version management)"""

import logging
from functools import wraps
from typing import Any, Dict, Optional

from .node_registry import NodeRegistry as LocalNodeRegistry
from core.node_registry import NodeRegistry as VersionedNodeRegistry

logger = logging.getLogger(__name__)


def register_node_type(
    node_class_type: str, 
    version: Optional[str] = None,
    default_params: Dict[str, Any] = None
):
    """
    Node type registration decorator (with version management).
    
    Args:
        node_class_type: Node type identifier (e.g., 'code_node')
        version: Version number (e.g., '1.0.0', '1.2.3-beta.1')
                If not specified, will get from class metadata, defaults to '0.0.1'
        default_params: Default parameter configuration
    
    Returns:
        Decorator function
        
    Examples:
        # Explicitly specify version
        @register_node_type("code_node", version="1.0.0", default_params={...})
        class CodeNode(NodeBase):
            pass
        
        # Automatically get version from class metadata
        @register_node_type("code_node", default_params={...})
        class CodeNode(NodeBase):
            def __init__(self, **kwargs):
                super().__init__(version="1.0.0", ...)
    """
    local_registry = LocalNodeRegistry.get_instance()

    def decorator(cls):
        # 1. Determine version number
        node_version = version
        if node_version is None:
            # Try to get version from class metadata
            if hasattr(cls, 'get_class_metadata'):
                try:
                    metadata = cls.get_class_metadata()
                    node_version = metadata.get('version', '0.0.1')
                except:
                    node_version = '0.0.1'
            else:
                # Get version from default_params
                node_version = (default_params or {}).get('version', '0.0.1')
        
        logger.info(
            f"Registering node type: {node_class_type} v{node_version}, class: {cls.__name__}"
        )

        # 2. Register to local Worker Registry (for runtime instantiation)
        local_registry.register_node(node_class_type, cls, default_params)

        # 3. Register to versioned Registry (for version management)
        try:
            VersionedNodeRegistry.register(
                node_type=node_class_type,
                version=node_version,
                node_class=cls,
                metadata=default_params
            )
            logger.debug(
                f"✓ Registered to version system: {node_class_type} v{node_version}"
            )
        except Exception as e:
            # If version exists or other error, log warning but don't interrupt registration
            logger.warning(
                f"Version registration warning: {node_class_type} v{node_version} - {str(e)}"
            )

        # 4. Add class attributes for reflection
        cls.NODE_CLASS_TYPE = node_class_type
        cls.NODE_VERSION = node_version

        # 5. Return class directly, don't wrap, to support class inheritance
        return cls

    return decorator
