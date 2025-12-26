"""Connection Pool Manager"""

import atexit
import logging

from infra.mq.connection_pool import connection_pool

logger = logging.getLogger(__name__)


class ConnectionPoolManager:
    """Connection pool manager for application lifecycle management"""

    def __init__(self):
        self._shutdown_registered = False

    def register_shutdown_handler(self):
        """Register cleanup handler for application shutdown"""
        if not self._shutdown_registered:
            atexit.register(self._cleanup_on_shutdown)
            self._shutdown_registered = True
            logger.info("RabbitMQ connection pool shutdown handler registered")

    def _cleanup_on_shutdown(self):
        """Cleanup function called on application shutdown"""
        try:
            import asyncio

            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(connection_pool.close_all())
            else:
                loop.run_until_complete(connection_pool.close_all())
            logger.info("RabbitMQ connection pool cleaned up on shutdown")
        except Exception as e:
            logger.error("Error during connection pool cleanup: %s", str(e))

    async def get_pool_status(self) -> dict:
        """Get connection pool status"""
        return {
            "connection_count": connection_pool.get_connection_count(),
            "total_connections": len(connection_pool._connections),
        }


# Global manager instance
pool_manager = ConnectionPoolManager()
