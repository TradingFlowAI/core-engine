import asyncio
import logging
from typing import Any, Callable, List, Optional, Union

from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage

from tradingflow.depot.config import CONFIG
from tradingflow.depot.mq.connection_pool import connection_pool
from tradingflow.depot.mq.interfaces import Consumer, Publisher

logger = logging.getLogger(__name__)

RABBITMQ_URL = CONFIG.get("RABBITMQ_URL", "amqp://guest:guest@localhost/")
RABBITMQ_EXCHANGE = CONFIG.get("RABBITMQ_EXCHANGE", "tradingflow")


class AioPikaTopicPublisher(Publisher):
    """aio_pika based message publisher implementation (Topic mode) with connection pool"""

    def __init__(
        self,
        connection_string: str = RABBITMQ_URL,
        exchange: str = RABBITMQ_EXCHANGE,
    ):
        self._connection: Optional[AbstractConnection] = None
        self._channel: Optional[AbstractChannel] = None
        self._connection_string = connection_string
        self._exchange = exchange
        self._exchange_type = "topic"
        self._is_connected = False

    async def connect(
        self, connection_string: str = None, timeout: int = 10, max_retries: int = 3
    ) -> None:
        """Connect to RabbitMQ server using connection pool

        Args:
            connection_string: Connection string, e.g., "amqp://guest:guest@localhost/"
            timeout: Connection timeout in seconds
            max_retries: Maximum number of connection retries
        """
        if connection_string:
            self._connection_string = connection_string

        for attempt in range(max_retries):
            try:
                logger.debug(
                    "Attempting to connect publisher to RabbitMQ (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries,
                    self._connection_string,
                )

                # 使用连接池获取连接和通道
                self._connection = await asyncio.wait_for(
                    connection_pool.get_connection(self._connection_string),
                    timeout=timeout,
                )
                self._channel = await asyncio.wait_for(
                    connection_pool.get_channel(self._connection_string),
                    timeout=timeout,
                )

                self._is_connected = True
                logger.debug(
                    "Publisher connected to RabbitMQ: %s", self._connection_string
                )
                return

            except asyncio.TimeoutError:
                logger.warning(
                    "Publisher RabbitMQ connection timeout (attempt %d/%d)",
                    attempt + 1,
                    max_retries,
                )
                if attempt == max_retries - 1:
                    raise ConnectionError(
                        f"Failed to connect publisher to RabbitMQ after {max_retries} attempts: connection timeout"
                    )
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("Publisher RabbitMQ connection was cancelled")
                raise

            except Exception as e:
                logger.error(
                    "Failed to connect publisher to RabbitMQ (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries,
                    str(e),
                )
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(1)

    async def publish(
        self, message: Any, routing_key: str, persistent: bool = True, **kwargs
    ) -> None:
        """Publish message"""
        if not self._is_connected or self._channel is None:
            raise RuntimeError(
                "Not connected to RabbitMQ, please call connect method first"
            )

        try:
            # Convert message content to bytes
            if isinstance(message, bytes):
                message_body = message
            elif isinstance(message, str):
                message_body = message.encode()
            else:
                message_body = str(message).encode()

            # Create message object
            delivery_mode = (
                DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT
            )
            message_obj = Message(
                body=message_body, delivery_mode=delivery_mode, **kwargs
            )

            topic_logs_exchange = await self._channel.declare_exchange(
                self._exchange, self._exchange_type
            )

            # Publish message
            await topic_logs_exchange.publish(
                message=message_obj, routing_key=routing_key
            )
            logger.debug("Message %s sent: %r", routing_key, message_obj)
        except Exception as e:
            logger.error("Failed to publish message: %s", str(e))
            raise

    async def close(self) -> None:
        """Close channel (connection is managed by pool)"""
        errors = []

        # 只关闭通道，连接由连接池管理
        if self._channel and not self._channel.is_closed:
            try:
                await asyncio.wait_for(self._channel.close(), timeout=5.0)
                logger.debug("Publisher RabbitMQ channel closed")
            except Exception as e:
                logger.warning("Error closing publisher RabbitMQ channel: %s", str(e))
                errors.append(e)

        # 重置状态
        self._channel = None
        self._is_connected = False

        if errors:
            raise errors[0]


class AioPikaTopicConsumer(Consumer):
    """aio_pika based message consumer implementation (Topic mode) with connection pool"""

    def __init__(
        self,
        connection_string: str = RABBITMQ_URL,
        exchange: str = RABBITMQ_EXCHANGE,
    ):
        self._connection: Optional[AbstractConnection] = None
        self._channel: Optional[AbstractChannel] = None
        self._connection_string = connection_string
        self._exchange = exchange
        self._exchange_type = "topic"
        self._is_connected = False

    async def connect(
        self, connection_string: str = None, timeout: int = 10, max_retries: int = 3
    ) -> None:
        """Connect to RabbitMQ server using connection pool

        Args:
            connection_string: Connection string
            timeout: Connection timeout in seconds
            max_retries: Maximum number of connection retries
        """
        if connection_string:
            self._connection_string = connection_string

        for attempt in range(max_retries):
            try:
                logger.debug(
                    "Attempting to connect consumer to RabbitMQ (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries,
                    self._connection_string,
                )

                # 使用连接池获取连接和通道
                self._connection = await asyncio.wait_for(
                    connection_pool.get_connection(self._connection_string),
                    timeout=timeout,
                )
                self._channel = await asyncio.wait_for(
                    connection_pool.get_channel(self._connection_string),
                    timeout=timeout,
                )

                self._is_connected = True
                logger.debug(
                    "Consumer connected to RabbitMQ: %s", self._connection_string
                )
                return

            except asyncio.TimeoutError:
                logger.warning(
                    "Consumer RabbitMQ connection timeout (attempt %d/%d)",
                    attempt + 1,
                    max_retries,
                )
                if attempt == max_retries - 1:
                    raise ConnectionError(
                        f"Failed to connect consumer to RabbitMQ after {max_retries} attempts: connection timeout"
                    )
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("Consumer RabbitMQ connection was cancelled")
                raise

            except Exception as e:
                logger.error(
                    "Failed to connect consumer to RabbitMQ (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries,
                    str(e),
                )
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(1)

    async def consume(
        self,
        queue_name: str,
        binding_keys: Union[str, List[str]],
        callback: Callable[[AbstractIncomingMessage], Any],
        prefetch_count: int = 1,
        durable: bool = True,
    ) -> None:
        """Consume messages"""
        if not self._is_connected or self._channel is None:
            raise RuntimeError(
                "Not connected to RabbitMQ, please call connect method first"
            )

        try:
            # Set prefetch count
            await self._channel.set_qos(prefetch_count=prefetch_count)

            topic_logs_exchange = await self._channel.declare_exchange(
                self._exchange, self._exchange_type
            )

            # Declare queue
            queue = await self._channel.declare_queue(name=queue_name, durable=durable)

            # Handle binding keys
            if isinstance(binding_keys, str):
                binding_keys = [binding_keys]

            for binding_key in binding_keys:
                await queue.bind(topic_logs_exchange, routing_key=binding_key)
                logger.debug(
                    "Queue %s bound to routing key %s", queue_name, binding_key
                )

            # Start consuming messages
            await queue.consume(callback)
            logger.debug("Started consuming messages from queue %s", queue_name)
        except Exception as e:
            logger.error("Failed to set up consumer: %s", str(e))
            raise

    async def close(self) -> None:
        """Close channel (connection is managed by pool)"""
        errors = []

        # 只关闭通道，连接由连接池管理
        if self._channel and not self._channel.is_closed:
            try:
                await asyncio.wait_for(self._channel.close(), timeout=5.0)
                logger.debug("Consumer RabbitMQ channel closed")
            except Exception as e:
                logger.warning("Error closing consumer RabbitMQ channel: %s", str(e))
                errors.append(e)

        # 重置状态
        self._channel = None
        self._is_connected = False

        if errors:
            raise errors[0]
