"""Flow execution log service"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import and_, desc, func
from sqlalchemy.orm import Session

from tradingflow.depot.db.models.flow_execution_log import FlowExecutionLog
from tradingflow.depot.db.postgres_manager import PostgresManager

logger = logging.getLogger(__name__)


class FlowExecutionLogService:
    """Flow execution log service for database operations"""

    def __init__(self, postgres_manager: PostgresManager = None):
        """
        Initialize service

        Args:
            postgres_manager: PostgreSQL manager instance
        """
        self.postgres_manager = postgres_manager or PostgresManager()

    async def create_log(
        self,
        flow_id: str,
        cycle: int,
        message: str,
        node_id: Optional[str] = None,
        log_level: str = "INFO",
        log_source: str = "node",
        log_metadata: Optional[Dict] = None,
    ) -> FlowExecutionLog:
        """
        Create a new flow execution log

        Args:
            flow_id: Flow identifier
            cycle: Execution cycle number
            message: Log message
            node_id: Node identifier (None for system/user logs)
            log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_source: Log source (node, system, user)
            log_metadata: Additional structured metadata

        Returns:
            Created FlowExecutionLog instance
        """
        try:
            log_entry = FlowExecutionLog(
                flow_id=flow_id,
                cycle=cycle,
                node_id=node_id,
                log_level=log_level.upper(),
                log_source=log_source,
                message=message,
                log_metadata=log_metadata,
            )

            with self.postgres_manager.get_session() as session:
                session.add(log_entry)
                session.commit()
                session.refresh(log_entry)

            logger.debug(
                "Created flow execution log: flow_id=%s, cycle=%s, node_id=%s",
                flow_id,
                cycle,
                node_id,
            )
            return log_entry

        except Exception as e:
            logger.error("Error creating flow execution log: %s", str(e))
            raise

    async def get_logs_by_flow(
        self,
        flow_id: str,
        cycle: Optional[int] = None,
        node_id: Optional[str] = None,
        log_level: Optional[str] = None,
        log_source: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        order_direction: str = "desc",
        db: Optional[Session] = None,
    ) -> List[FlowExecutionLog]:
        """
        Get flow execution logs with filtering

        Args:
            flow_id: Flow identifier
            cycle: Optional cycle filter
            node_id: Optional node filter
            log_level: Optional log level filter
            log_source: Optional log source filter
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Field to order by
            order_direction: Order direction (asc/desc)
            db: Optional database session to use

        Returns:
            List of FlowExecutionLog instances
        """
        try:
            def _execute_query(session):
                query = session.query(FlowExecutionLog).filter(
                    FlowExecutionLog.flow_id == flow_id
                )

                # Apply filters
                if cycle is not None:
                    query = query.filter(FlowExecutionLog.cycle == cycle)

                if node_id is not None:
                    query = query.filter(FlowExecutionLog.node_id == node_id)

                if log_level is not None:
                    query = query.filter(FlowExecutionLog.log_level == log_level.upper())

                if log_source is not None:
                    query = query.filter(FlowExecutionLog.log_source == log_source)

                # Apply ordering
                order_field = getattr(FlowExecutionLog, order_by, FlowExecutionLog.created_at)
                if order_direction.lower() == "desc":
                    query = query.order_by(desc(order_field))
                else:
                    query = query.order_by(order_field)

                # Apply pagination
                query = query.offset(offset).limit(limit)

                return query.all()

            if db is not None:
                # Use provided session
                logs = _execute_query(db)
            else:
                # Use internal session manager
                with self.postgres_manager.get_session() as session:
                    logs = _execute_query(session)

            logger.debug(
                "Retrieved %d flow execution logs for flow_id=%s", len(logs), flow_id
            )
            return logs

        except Exception as e:
            logger.error("Error retrieving flow execution logs: %s", str(e))
            raise

    async def get_log_by_id(self, log_id: int, db: Optional[Session] = None) -> Optional[FlowExecutionLog]:
        """
        Get a specific log by ID

        Args:
            log_id: Log ID
            db: Optional database session to use

        Returns:
            FlowExecutionLog instance or None if not found
        """
        try:
            def _execute_query(session):
                return (
                    session.query(FlowExecutionLog)
                    .filter(FlowExecutionLog.id == log_id)
                    .first()
                )

            if db is not None:
                # Use provided session
                log_entry = _execute_query(db)
            else:
                # Use internal session manager
                with self.postgres_manager.get_session() as session:
                    log_entry = _execute_query(session)

            if log_entry:
                logger.debug("Retrieved flow execution log with id=%d", log_id)
            else:
                logger.debug("Flow execution log with id=%d not found", log_id)

            return log_entry

        except Exception as e:
            logger.error("Error retrieving flow execution log by id: %s", str(e))
            raise

    async def get_logs_count(
        self,
        flow_id: str,
        cycle: Optional[int] = None,
        node_id: Optional[str] = None,
        log_level: Optional[str] = None,
        log_source: Optional[str] = None,
        db: Optional[Session] = None,
    ) -> int:
        """
        Get count of logs matching filters

        Args:
            flow_id: Flow identifier
            cycle: Optional cycle filter
            node_id: Optional node filter
            log_level: Optional log level filter
            log_source: Optional log source filter
            db: Optional database session to use

        Returns:
            Count of matching logs
        """
        try:
            def _execute_query(session):
                query = session.query(func.count(FlowExecutionLog.id)).filter(
                    FlowExecutionLog.flow_id == flow_id
                )

                # Apply filters
                if cycle is not None:
                    query = query.filter(FlowExecutionLog.cycle == cycle)

                if node_id is not None:
                    query = query.filter(FlowExecutionLog.node_id == node_id)

                if log_level is not None:
                    query = query.filter(FlowExecutionLog.log_level == log_level.upper())

                if log_source is not None:
                    query = query.filter(FlowExecutionLog.log_source == log_source)

                return query.scalar()

            if db is not None:
                # Use provided session
                count = _execute_query(db)
            else:
                # Use internal session manager
                with self.postgres_manager.get_session() as session:
                    count = _execute_query(session)

            logger.debug("Flow execution logs count: %d for flow_id=%s", count, flow_id)
            return count

        except Exception as e:
            logger.error("Error counting flow execution logs: %s", str(e))
            raise

    async def delete_logs_by_flow(self, flow_id: str, cycle: Optional[int] = None) -> int:
        """
        Delete logs for a specific flow

        Args:
            flow_id: Flow identifier
            cycle: Optional cycle filter

        Returns:
            Number of deleted records
        """
        try:
            with self.postgres_manager.get_session() as session:
                query = session.query(FlowExecutionLog).filter(
                    FlowExecutionLog.flow_id == flow_id
                )

                if cycle is not None:
                    query = query.filter(FlowExecutionLog.cycle == cycle)

                deleted_count = query.delete()
                session.commit()

            logger.info(
                "Deleted %d flow execution logs for flow_id=%s, cycle=%s",
                deleted_count,
                flow_id,
                cycle,
            )
            return deleted_count

        except Exception as e:
            logger.error("Error deleting flow execution logs: %s", str(e))
            raise

    async def get_cycles_by_flow(self, flow_id: str) -> List[int]:
        """
        Get all cycle numbers for a specific flow

        Args:
            flow_id: Flow identifier

        Returns:
            List of cycle numbers
        """
        try:
            with self.postgres_manager.get_session() as session:
                cycles = (
                    session.query(FlowExecutionLog.cycle)
                    .filter(FlowExecutionLog.flow_id == flow_id)
                    .distinct()
                    .order_by(FlowExecutionLog.cycle)
                    .all()
                )

            cycle_numbers = [cycle[0] for cycle in cycles]
            logger.debug(
                "Retrieved %d cycles for flow_id=%s", len(cycle_numbers), flow_id
            )
            return cycle_numbers

        except Exception as e:
            logger.error("Error retrieving cycles for flow: %s", str(e))
            raise

    async def get_logs_by_flow_cycle_node(
        self,
        flow_id: str,
        cycle: int,
        node_id: Optional[str] = None,
        log_level: Optional[str] = None,
        log_source: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        order_direction: str = "desc",
    ) -> List[FlowExecutionLog]:
        """
        Get logs for a specific flow, cycle and node

        Args:
            flow_id: Flow identifier
            cycle: Cycle number
            node_id: Node identifier (None for system/user logs)
            log_level: Optional log level filter
            log_source: Optional log source filter
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Field to order by
            order_direction: Order direction (asc/desc)

        Returns:
            List of FlowExecutionLog objects
        """
        try:
            with self.postgres_manager.get_session() as session:
                query = session.query(FlowExecutionLog).filter(
                    and_(
                        FlowExecutionLog.flow_id == flow_id,
                        FlowExecutionLog.cycle == cycle,
                    )
                )

                # Add node_id filter
                if node_id is not None:
                    query = query.filter(FlowExecutionLog.node_id == node_id)
                else:
                    # If node_id is None, filter for system/user logs (where node_id is NULL)
                    query = query.filter(FlowExecutionLog.node_id.is_(None))

                # Add optional filters
                if log_level:
                    query = query.filter(FlowExecutionLog.log_level == log_level)

                if log_source:
                    query = query.filter(FlowExecutionLog.log_source == log_source)

                # Add ordering
                if hasattr(FlowExecutionLog, order_by):
                    order_column = getattr(FlowExecutionLog, order_by)
                    if order_direction.lower() == "desc":
                        query = query.order_by(order_column.desc())
                    else:
                        query = query.order_by(order_column.asc())

                # Add pagination
                logs = query.offset(offset).limit(limit).all()

            logger.debug(
                "Retrieved %d logs for flow_id=%s, cycle=%s, node_id=%s",
                len(logs),
                flow_id,
                cycle,
                node_id,
            )
            return logs

        except Exception as e:
            logger.error("Error retrieving logs for flow cycle node: %s", str(e))
            raise

    async def cleanup_old_logs(
        self, flow_id: str, keep_cycles: int = 10
    ) -> int:
        """
        Clean up old logs, keeping only the most recent cycles

        Args:
            flow_id: Flow identifier
            keep_cycles: Number of recent cycles to keep

        Returns:
            Number of deleted records
        """
        try:
            with self.postgres_manager.get_session() as session:
                # Get the most recent cycles to keep
                recent_cycles = (
                    session.query(FlowExecutionLog.cycle)
                    .filter(FlowExecutionLog.flow_id == flow_id)
                    .distinct()
                    .order_by(desc(FlowExecutionLog.cycle))
                    .limit(keep_cycles)
                    .all()
                )

                if not recent_cycles:
                    return 0

                cycles_to_keep = [cycle[0] for cycle in recent_cycles]

                # Delete logs not in the recent cycles
                deleted_count = (
                    session.query(FlowExecutionLog)
                    .filter(
                        and_(
                            FlowExecutionLog.flow_id == flow_id,
                            ~FlowExecutionLog.cycle.in_(cycles_to_keep),
                        )
                    )
                    .delete(synchronize_session=False)
                )

                session.commit()

            logger.info(
                "Cleaned up %d old flow execution logs for flow_id=%s",
                deleted_count,
                flow_id,
            )
            return deleted_count

        except Exception as e:
            logger.error("Error cleaning up old flow execution logs: %s", str(e))
            raise
