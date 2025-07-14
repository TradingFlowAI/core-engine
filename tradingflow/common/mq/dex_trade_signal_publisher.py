import asyncio
import json
import logging
import uuid
from typing import Any, Dict, MutableMapping, Optional, Union

from aio_pika import Message, connect
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractIncomingMessage,
    AbstractQueue,
)

from tradingflow.common.config import CONFIG
from tradingflow.common.mq.aio_pika_impl import AioPikaTopicPublisher

logger = logging.getLogger(__name__)


class DexTradeSignalPublisher:
    """DEX Trade Signal Publisher

    Publishes buy/sell signals to specified trading queue for automated trade execution.
    Supports RPC mode for synchronously waiting for execution results.

    Queue name format: chain.{chain_id}.dex.{dex_name}
    Routing key format: signal.swap
    """

    def __init__(
        self,
        chain_id: str,
        dex_name: str,
        vault_address: str,
        exchange_name: str = "trade_signals",
        rabbitmq_url: str = None,
    ):
        """Initialize trade signal publisher

        Args:
            chain_id: Blockchain ID (e.g., "1" for Ethereum mainnet)
            dex_name: DEX name (e.g., "Uniswap")
            vault_address: Vault contract address
            exchange_name: Message exchange name
            rabbitmq_url: RabbitMQ connection URL
        """
        self.chain_id = chain_id
        self.dex_name = dex_name.lower()  # Convert to lowercase for consistency
        self.vault_address = vault_address
        self.exchange_name = exchange_name
        self.rabbitmq_url = rabbitmq_url or CONFIG.get(
            "RABBITMQ_URL", "amqp://guest:guest@localhost/"
        )

        # Set queue name
        self.queue_name = f"chain.{self.chain_id}.dex.{self.dex_name}"

        # Set fixed routing key
        self.routing_key = "signal.swap"

        # Initialize publisher
        self.publisher = AioPikaTopicPublisher(
            connection_string=self.rabbitmq_url,
            exchange=self.exchange_name,
        )

        # RPC mode variables
        self.connection: Optional[AbstractConnection] = None
        self.channel: Optional[AbstractChannel] = None
        self.callback_queue: Optional[AbstractQueue] = None
        self.futures: MutableMapping[str, asyncio.Future] = {}
        self.rpc_timeout = 120  # Default timeout in seconds

        logger.info(
            "DexTradeSignalPublisher initialized: chain_id=%s, dex_name=%s, queue=%s",
            chain_id,
            dex_name,
            self.queue_name,
        )

    async def connect(self):
        """Connect to message queue"""
        # Connect regular publisher
        await self.publisher.connect()

        # Establish connection for RPC mode
        self.connection = await connect(self.rabbitmq_url)
        self.channel = await self.connection.channel()

        # Create callback queue for receiving execution results
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
        await self.callback_queue.consume(self._on_response, no_ack=True)

        # Ensure target queue exists
        await self.channel.declare_queue(self.queue_name, durable=True)

        logger.info(
            "Trade signal publisher connected: chain_id=%s, dex_name=%s",
            self.chain_id,
            self.dex_name,
        )

    async def close(self):
        """Close connections"""
        await self.publisher.close()

        if self.connection:
            await self.connection.close()
            self.connection = None
            self.channel = None
            self.callback_queue = None

        logger.info(
            "Trade signal publisher closed: chain_id=%s, dex_name=%s",
            self.chain_id,
            self.dex_name,
        )

    async def _on_response(self, message: AbstractIncomingMessage) -> None:
        """Handle RPC callback response"""
        if message.correlation_id is None:
            logger.warning("Received message without correlation_id: %r", message)
            return

        if message.correlation_id in self.futures:
            future: asyncio.Future = self.futures.pop(message.correlation_id)

            try:
                # Parse response content
                response_data = json.loads(message.body.decode())
                future.set_result(response_data)
            except Exception as e:
                # If parsing fails, return the original message as result
                logger.error("Failed to parse response message: %s", str(e))
                future.set_exception(e)

    async def publish_buy_signal(
        self,
        token_address: str,
        amount_in: Union[int, str],
        min_amount_out: Union[int, str] = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publish buy signal

        Args:
            token_address: Token contract address to purchase
            amount_in: Base token amount to use for purchase
            min_amount_out: Minimum token amount to receive
            metadata: Additional metadata

        Returns:
            bool: Whether sending was successful
        """
        # Build message
        message = {
            "vault_address": self.vault_address,
            "action": "buy",
            "token_address": token_address,
            "amount_in": str(amount_in),
            "min_amount_out": str(min_amount_out),
        }

        # Add metadata
        if metadata:
            message["metadata"] = metadata

        try:
            # Send message
            await self.publisher.publish(
                message=json.dumps(message), routing_key=self.routing_key
            )

            logger.info(
                "Buy signal published: token=%s, amount=%s", token_address, amount_in
            )
            return True

        except Exception as e:
            logger.exception("Error publishing buy signal: %s", str(e))
            return False

    async def publish_sell_signal(
        self,
        token_address: str,
        amount_in: Optional[Union[int, str]] = None,
        min_amount_out: Union[int, str] = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publish sell signal

        Args:
            token_address: Token contract address to sell
            amount_in: Token amount to sell (None or 0 means sell all)
            min_amount_out: Minimum base token amount to receive
            metadata: Additional metadata

        Returns:
            bool: Whether sending was successful
        """
        # Build message
        message = {
            "vault_address": self.vault_address,
            "action": "sell",
            "token_address": token_address,
            "min_amount_out": str(min_amount_out),
        }

        # If amount specified, add to message
        if amount_in is not None:
            message["amount_in"] = str(amount_in)

        # Add metadata
        if metadata:
            message["metadata"] = metadata

        try:
            # Send message
            await self.publisher.publish(
                message=json.dumps(message), routing_key=self.routing_key
            )

            amount_display = amount_in or "all"
            logger.info(
                "Sell signal published: token=%s, amount=%s",
                token_address,
                amount_display,
            )
            return True

        except Exception as e:
            logger.exception("Error publishing sell signal: %s", str(e))
            return False

    async def publish_buy_signal_and_wait(
        self,
        token_address: str,
        amount_in: Union[int, str],
        min_amount_out: Union[int, str] = 0,
        metadata: Optional[Dict[str, Any]] = None,
        timeout: int = None,
    ) -> Dict[str, Any]:
        """
        Publish buy signal and wait for execution result

        Args:
            token_address: Token contract address to purchase
            amount_in: Base token amount to use for purchase
            min_amount_out: Minimum token amount to receive
            metadata: Additional metadata
            timeout: Timeout in seconds

        Returns:
            Dict: Transaction execution result
        """
        if not self.channel or not self.callback_queue:
            raise RuntimeError(
                "RPC channel not initialized, please call connect() first"
            )

        # Set timeout
        timeout = timeout or self.rpc_timeout

        # Generate unique correlation ID
        correlation_id = str(uuid.uuid4())

        # Create Future object
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.futures[correlation_id] = future

        # Build message
        message_body = {
            "vault_address": self.vault_address,
            "action": "buy",
            "token_address": token_address,
            "amount_in": str(amount_in),
            "min_amount_out": str(min_amount_out),
            "request_type": "rpc",  # Identify this as an RPC request
        }

        # Add metadata
        if metadata:
            message_body["metadata"] = metadata

        try:
            # Create message
            message = Message(
                body=json.dumps(message_body).encode(),
                content_type="application/json",
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name,
            )

            # Send message to target queue
            await self.channel.default_exchange.publish(
                message, routing_key=self.queue_name
            )

            logger.info(
                "Buy signal RPC request sent: token=%s, correlation_id=%s, timeout=%s",
                token_address,
                correlation_id,
                timeout,
            )

            # Wait for response
            try:
                result = await asyncio.wait_for(future, timeout=timeout)
                logger.info(
                    "Received buy signal RPC response: correlation_id=%s",
                    correlation_id,
                )
                return result
            except asyncio.TimeoutError:
                # Timeout handling
                if correlation_id in self.futures:
                    del self.futures[correlation_id]
                logger.error(
                    "Timeout waiting for buy signal execution result: token=%s",
                    token_address,
                )
                return {
                    "success": False,
                    "error": "timeout",
                    "message": "Timeout waiting for execution result",
                }

        except Exception as e:
            # Clean up future
            if correlation_id in self.futures:
                del self.futures[correlation_id]

            logger.exception("Error publishing buy signal RPC request: %s", str(e))
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to send RPC request",
            }

    async def publish_sell_signal_and_wait(
        self,
        token_address: str,
        amount_in: Optional[Union[int, str]] = None,
        min_amount_out: Union[int, str] = 0,
        metadata: Optional[Dict[str, Any]] = None,
        timeout: int = None,
    ) -> Dict[str, Any]:
        """
        Publish sell signal and wait for execution result

        Args:
            token_address: Token contract address to sell
            amount_in: Token amount to sell (None means sell all)
            min_amount_out: Minimum base token amount to receive
            metadata: Additional metadata
            timeout: Timeout in seconds

        Returns:
            Dict: Transaction execution result
        """
        if not self.channel or not self.callback_queue:
            raise RuntimeError(
                "RPC channel not initialized, please call connect() first"
            )

        # Set timeout
        timeout = timeout or self.rpc_timeout

        # Generate unique correlation ID
        correlation_id = str(uuid.uuid4())

        # Create Future object
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.futures[correlation_id] = future

        # Build message
        message_body = {
            "vault_address": self.vault_address,
            "action": "sell",
            "token_address": token_address,
            "min_amount_out": str(min_amount_out),
            "request_type": "rpc",  # Identify this as an RPC request
        }

        # If amount specified, add to message
        if amount_in is not None:
            message_body["amount_in"] = str(amount_in)

        # Add metadata
        if metadata:
            message_body["metadata"] = metadata

        try:
            # Create message
            message = Message(
                body=json.dumps(message_body).encode(),
                content_type="application/json",
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name,
            )

            # Send message to target queue
            await self.channel.default_exchange.publish(
                message, routing_key=self.queue_name
            )

            amount_display = amount_in or "all"
            logger.info(
                "Sell signal RPC request sent: token=%s, amount=%s, correlation_id=%s",
                token_address,
                amount_display,
                correlation_id,
            )

            # Wait for response
            try:
                result = await asyncio.wait_for(future, timeout=timeout)
                logger.info(
                    "Received sell signal RPC response: correlation_id=%s",
                    correlation_id,
                )
                return result
            except asyncio.TimeoutError:
                # Timeout handling
                if correlation_id in self.futures:
                    del self.futures[correlation_id]
                logger.error(
                    "Timeout waiting for sell signal execution result: token=%s",
                    token_address,
                )
                return {
                    "success": False,
                    "error": "timeout",
                    "message": "Timeout waiting for execution result",
                }

        except Exception as e:
            # Clean up future
            if correlation_id in self.futures:
                del self.futures[correlation_id]

            logger.exception("Error publishing sell signal RPC request: %s", str(e))
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to send RPC request",
            }
