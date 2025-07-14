import atexit
import logging

from tradingflow.depot.mq.connection_pool import connection_pool

logger = logging.getLogger(__name__)


class ConnectionPoolManager:
    """连接池管理器，负责应用程序生命周期管理"""

    def __init__(self):
        self._shutdown_registered = False

    def register_shutdown_handler(self):
        """注册应用程序关闭时的清理处理器"""
        if not self._shutdown_registered:
            atexit.register(self._cleanup_on_shutdown)
            self._shutdown_registered = True
            logger.info("RabbitMQ connection pool shutdown handler registered")

    def _cleanup_on_shutdown(self):
        """应用程序关闭时的清理函数"""
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
        """获取连接池状态"""
        return {
            "connection_count": connection_pool.get_connection_count(),
            "total_connections": len(connection_pool._connections),
        }


# 全局管理器实例
pool_manager = ConnectionPoolManager()
