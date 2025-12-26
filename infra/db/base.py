"""Database Base Configuration"""

import logging
import time
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from infra.config import CONFIG

logger = logging.getLogger(__name__)
sql_logger = logging.getLogger(__name__ + ".sql")

# Create base model class
Base = declarative_base()

# Cache database engines
_engine = None
_SessionFactory = None


def get_engine():
    """Get or create database engine"""
    global _engine
    if _engine is None:
        db_url = CONFIG.get("POSTGRES_URL")
        logger.debug("Creating engine with URL: %s", db_url)
        _engine = create_engine(
            db_url,
            pool_pre_ping=True,  # Auto reconnect
            pool_recycle=3600,  # Recreate connections after one hour
        )

        # Add event listener for before cursor execution
        @event.listens_for(_engine, "before_cursor_execute")
        def before_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            conn.info.setdefault("query_start_time", []).append(time.time())

        # Add event listener for after cursor execution
        @event.listens_for(_engine, "after_cursor_execute")
        def after_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            total_time = time.time() - conn.info["query_start_time"].pop(-1)

    return _engine


def get_session_factory():
    """Get session factory"""
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(
            autocommit=False, autoflush=False, bind=get_engine()
        )
    return _SessionFactory


def get_db():
    """Get database session"""
    factory = get_session_factory()
    return factory()


def get_db_session():
    """Get database session (alias for get_db)"""
    return get_db()


@contextmanager
def db_session():
    """Session context manager, automatically commit or rollback"""
    session = get_db()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.exception("Database session error, rolling back, error: %s", e)
        raise
    finally:
        session.close()


def create_tables():
    """Create tables for all models"""
    # Make sure all models are imported, so metadata contains all tables

    logger.debug("Creating database tables...")
    Base.metadata.create_all(bind=get_engine())
    logger.debug("Tables created (if they didn't exist)")
