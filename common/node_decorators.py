"""节点装饰器，用于自动注册节点类型（支持版本管理）"""

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
    节点类型注册装饰器（支持版本管理）
    
    Args:
        node_class_type: 节点类型标识符（如 'code_node'）
        version: 版本号（如 '1.0.0'，'1.2.3-beta.1'）
                如果未指定，将从类元数据中获取，默认为 '0.0.1'
        default_params: 默认参数配置
    
    Returns:
        装饰器函数
        
    Examples:
        # 显式指定版本
        @register_node_type("code_node", version="1.0.0", default_params={...})
        class CodeNode(NodeBase):
            pass
        
        # 从类元数据自动获取版本
        @register_node_type("code_node", default_params={...})
        class CodeNode(NodeBase):
            def __init__(self, **kwargs):
                super().__init__(version="1.0.0", ...)
    """
    local_registry = LocalNodeRegistry.get_instance()

    def decorator(cls):
        # 1. 确定版本号
        node_version = version
        if node_version is None:
            # 尝试从类元数据获取版本
            if hasattr(cls, 'get_class_metadata'):
                try:
                    metadata = cls.get_class_metadata()
                    node_version = metadata.get('version', '0.0.1')
                except:
                    node_version = '0.0.1'
            else:
                # 从 default_params 获取版本
                node_version = (default_params or {}).get('version', '0.0.1')
        
        logger.info(
            f"注册节点类型: {node_class_type} v{node_version}, 类: {cls.__name__}"
        )

        # 2. 注册到本地 Worker Registry（用于运行时实例化）
        local_registry.register_node(node_class_type, cls, default_params)

        # 3. 注册到版本化 Registry（用于版本管理）
        try:
            VersionedNodeRegistry.register(
                node_type=node_class_type,
                version=node_version,
                node_class=cls,
                metadata=default_params
            )
            logger.debug(
                f"✓ 注册到版本系统: {node_class_type} v{node_version}"
            )
        except Exception as e:
            # 如果版本已存在或其他错误，记录警告但不中断注册
            logger.warning(
                f"版本注册警告: {node_class_type} v{node_version} - {str(e)}"
            )

        # 4. 添加类属性，便于反射
        cls.NODE_CLASS_TYPE = node_class_type
        cls.NODE_VERSION = node_version

        # 5. 直接返回类，不要包装，以支持类继承
        return cls

    return decorator
