"""PostgreSQL Database Manager"""

import logging
from contextlib import contextmanager
from typing import Any, Callable, TypeVar

from sqlalchemy.orm import Session

from infra.db.base import get_engine, get_session_factory

logger = logging.getLogger(__name__)

# Global PostgreSQL manager instance cache
_postgres_managers = {}

# Type variable definition for generic function return types
T = TypeVar('T')


def get_postgres_manager(name: str = "default") -> 'PostgresManager':
    """Get or create PostgreSQL manager instance.

    Args:
        name: Manager instance name, used to distinguish multiple database connections

    Returns:
        PostgresManager: PostgreSQL manager instance
    """
    global _postgres_managers

    if name not in _postgres_managers:
        _postgres_managers[name] = PostgresManager()
        logger.debug(f"Created PostgreSQL manager instance: {name}")

    return _postgres_managers[name]


def close_all_connections():
    """Close all PostgreSQL connections"""
    global _postgres_managers

    for name, manager in _postgres_managers.items():
        logger.info(f"Closing PostgreSQL manager instance: {name}")
        # No need to explicitly close, SQLAlchemy's engine manages connection pool

    _postgres_managers = {}


class PostgresManager:
    """PostgreSQL database manager for managing database connections and sessions"""

    def __init__(self):
        """Initialize PostgreSQL manager"""
        self.engine = get_engine()
        self.session_factory = get_session_factory()

    @contextmanager
    def get_session(self) -> Session:
        """
        Get database session with automatic commit/rollback handling.

        Returns:
            Session: SQLAlchemy session
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error("Database error: %s", str(e))
            raise
        finally:
            session.close()

    def execute_query(self, query_func: Callable[[Session, ...], T], *args: Any, **kwargs: Any) -> T:
        """
        Execute query function within session context.

        Args:
            query_func: Query function that takes session as first parameter
            *args: Additional positional arguments for query function
            **kwargs: Additional keyword arguments for query function

        Returns:
            Any: Result from query function
        """
        with self.get_session() as session:
            return query_func(session, *args, **kwargs)

    @staticmethod
    def get_instance(name: str = "default") -> 'PostgresManager':
        """Get PostgreSQL manager instance.

        Args:
            name: Manager instance name

        Returns:
            PostgresManager: PostgreSQL manager instance
        """
        return get_postgres_manager(name)
