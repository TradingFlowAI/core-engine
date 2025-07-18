"""节点装饰器，用于自动注册节点类型"""

import logging
from functools import wraps
from typing import Any, Dict

from .node_registry import NodeRegistry

logger = logging.getLogger(__name__)


def register_node_type(node_class_type: str, default_params: Dict[str, Any] = None):
    """
    节点类型注册装饰器

    Args:
        node_class_type: 节点类型标识符
        default_params: 默认参数配置

    Returns:
        装饰器函数
    """
    registry = NodeRegistry.get_instance()

    def decorator(cls):
        logger.info(f"注册节点类型: {node_class_type}, 类: {cls.__name__}")

        # 在本地注册表中注册节点类型
        registry.register_node(node_class_type, cls, default_params)

        # 添加类属性，便于反射
        cls.NODE_CLASS_TYPE = node_class_type

        # 直接返回类，不要包装，以支持类继承
        return cls

    return decorator
