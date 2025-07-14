"""PostgreSQL database manager"""

import logging
from contextlib import contextmanager
from typing import Any, Callable, TypeVar

from sqlalchemy.orm import Session

from tradingflow.common.db.base import get_engine, get_session_factory

logger = logging.getLogger(__name__)

# 全局PostgreSQL管理器实例缓存
_postgres_managers = {}

# 类型变量定义，用于泛型函数返回类型
T = TypeVar('T')


def get_postgres_manager(name: str = "default") -> 'PostgresManager':
    """获取或创建PostgreSQL管理器实例
    
    Args:
        name: 管理器实例名称，用于区分多个数据库连接
        
    Returns:
        PostgresManager: PostgreSQL管理器实例
    """
    global _postgres_managers
    
    if name not in _postgres_managers:
        _postgres_managers[name] = PostgresManager()
        logger.debug(f"创建了PostgreSQL管理器实例: {name}")
        
    return _postgres_managers[name]


def close_all_connections():
    """关闭所有PostgreSQL连接"""
    global _postgres_managers
    
    for name, manager in _postgres_managers.items():
        logger.info(f"关闭PostgreSQL管理器实例: {name}")
        # 这里不需要显式关闭，因为SQLAlchemy的engine会自己管理连接池
        
    _postgres_managers = {}


class PostgresManager:
    """PostgreSQL数据库管理器，用于管理数据库连接和会话"""

    def __init__(self):
        """初始化PostgreSQL管理器"""
        self.engine = get_engine()
        self.session_factory = get_session_factory()

    @contextmanager
    def get_session(self) -> Session:
        """
        获取数据库会话，自动处理提交/回滚

        Returns:
            Session: SQLAlchemy会话
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error("数据库错误: %s", str(e))
            raise
        finally:
            session.close()

    def execute_query(self, query_func: Callable[[Session, ...], T], *args: Any, **kwargs: Any) -> T:
        """
        在会话上下文中执行查询函数

        Args:
            query_func: 接受会话作为第一个参数的查询函数
            *args: 查询函数的额外位置参数
            **kwargs: 查询函数的额外关键字参数

        Returns:
            Any: 查询函数的结果
        """
        with self.get_session() as session:
            return query_func(session, *args, **kwargs)
            
    @staticmethod
    def get_instance(name: str = "default") -> 'PostgresManager':
        """获取PostgreSQL管理器实例
        
        Args:
            name: 管理器实例名称
            
        Returns:
            PostgresManager: PostgreSQL管理器实例
        """
        return get_postgres_manager(name)
