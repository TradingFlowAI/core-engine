"""RabbitMQ Connection Pool"""

import asyncio
import logging
from typing import Dict, Optional

from aio_pika import connect_robust
from aio_pika.abc import AbstractChannel, AbstractConnection

logger = logging.getLogger(__name__)


class RabbitMQConnectionPool:
    """RabbitMQ connection pool singleton"""

    _instance: Optional["RabbitMQConnectionPool"] = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            self._connections: Dict[str, AbstractConnection] = {}
            self._channels: Dict[str, list] = {}  # Channel list for each connection
            self._connection_locks: Dict[str, asyncio.Lock] = {}
            self._initialized = True

    async def get_connection(self, connection_string: str) -> AbstractConnection:
        """Get or create connection"""
        if connection_string not in self._connection_locks:
            self._connection_locks[connection_string] = asyncio.Lock()

        async with self._connection_locks[connection_string]:
            if (
                connection_string not in self._connections
                or self._connections[connection_string].is_closed
            ):
                logger.debug("Creating new RabbitMQ connection: %s", connection_string)
                try:
                    connection = await connect_robust(connection_string)
                    self._connections[connection_string] = connection
                    self._channels[connection_string] = []
                    logger.debug(
                        "RabbitMQ connection established: %s", connection_string
                    )
                except Exception as e:
                    logger.error("Failed to create RabbitMQ connection: %s", str(e))
                    raise

            return self._connections[connection_string]

    async def get_channel(self, connection_string: str) -> AbstractChannel:
        """Get channel"""
        connection = await self.get_connection(connection_string)

        try:
            channel = await connection.channel()
            # Record channel for later cleanup
            if connection_string in self._channels:
                self._channels[connection_string].append(channel)
            logger.debug("Created new channel for connection: %s", connection_string)
            return channel
        except Exception as e:
            logger.error("Failed to create channel: %s", str(e))
            raise

    async def close_connection(self, connection_string: str):
        """Close specific connection"""
        if connection_string in self._connections:
            connection = self._connections[connection_string]
            if not connection.is_closed:
                try:
                    # First close all channels
                    if connection_string in self._channels:
                        for channel in self._channels[connection_string]:
                            if not channel.is_closed:
                                await channel.close()
                        self._channels[connection_string].clear()

                    # Then close connection
                    await connection.close()
                    logger.debug("Closed RabbitMQ connection: %s", connection_string)
                except Exception as e:
                    logger.warning(
                        "Error closing connection %s: %s", connection_string, str(e)
                    )

            # Clean up records
            del self._connections[connection_string]
            if connection_string in self._channels:
                del self._channels[connection_string]
            if connection_string in self._connection_locks:
                del self._connection_locks[connection_string]

    async def close_all(self):
        """Close all connections"""
        connection_strings = list(self._connections.keys())
        for connection_string in connection_strings:
            await self.close_connection(connection_string)
        logger.debug("All RabbitMQ connections closed")

    def get_connection_count(self) -> int:
        """Get current connection count"""
        return len([conn for conn in self._connections.values() if not conn.is_closed])


# Global connection pool instance
connection_pool = RabbitMQConnectionPool()
