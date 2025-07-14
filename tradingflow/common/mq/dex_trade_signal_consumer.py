import json
import logging
from typing import Any, Dict, Optional

from aio_pika import Message, connect
from aio_pika.abc import AbstractIncomingMessage

from tradingflow.bank.services.vault_service import VaultService
from tradingflow.depot.config import CONFIG
from tradingflow.depot.mq.aio_pika_impl import AioPikaTopicConsumer

logger = logging.getLogger(__name__)


class DexTradeSignalConsumer:
    """DEX trade signal consumer

    Monitors trading signals for specified chain and DEX, and executes corresponding trade operations.
    Supports RPC mode to receive requests and return execution results.

    Queue name format: chain.{chain_id}.dex.{dex_name}
    Routing key format: signal.swap
    """

    def __init__(
        self,
        chain_id: str,
        dex_name: str,
        vault_service=None,  # VaultService instance, used for actual trading
        rpc_url: Optional[str] = None,
        exchange_name: str = "trade_signals",
        rabbitmq_url: str = None,
        private_key: Optional[str] = None,
    ):
        self.chain_id = chain_id
        self.dex_name = dex_name.lower()
        self.rpc_url = rpc_url or CONFIG.get("DEFAULT_RPC_URL")
        self.rabbitmq_url = rabbitmq_url or CONFIG.get(
            "RABBITMQ_URL", "amqp://guest:guest@localhost/"
        )

        # Using VaultService instead of VaultManager
        self.vault_service = vault_service or VaultService.get_instance(int(chain_id))

        # If a private key is provided, ensure the VaultService instance uses this key
        if private_key and not hasattr(self.vault_service, "private_key"):
            self.vault_service.private_key = private_key

        # Set queue name
        self.queue_name = f"chain.{self.chain_id}.dex.{self.dex_name}"

        # Set fixed routing key
        self.routing_key = "signal.swap"

        # Initialize message queue consumer
        self.consumer = AioPikaTopicConsumer(
            connection_string=self.rabbitmq_url,
            exchange=exchange_name,
        )

        # RPC mode related variables
        self.connection = None
        self.channel = None
        self.exchange = None

        logger.info(
            "DexTradeSignalConsumer initialized: chain_id=%s, dex_name=%s, queue=%s",
            chain_id,
            dex_name,
            self.queue_name,
        )

    async def start(self):
        """Start the consumer"""
        # Connect to the message queue
        await self.consumer.connect()

        # Start consuming messages
        await self.consumer.consume(
            queue_name=self.queue_name,
            binding_keys=[self.routing_key],
            callback=self._message_handler,
            prefetch_count=1,
            durable=True,
        )

        # Initialize RPC mode
        self.connection = await connect(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.exchange = self.channel.default_exchange

        logger.info(
            "Trade signal consumer started: chain_id=%s, dex_name=%s",
            self.chain_id,
            self.dex_name,
        )

    async def stop(self):
        """Stop the consumer"""
        # Stop the consumer
        await self.consumer.close()

        # Close RPC connection
        if self.connection:
            await self.connection.close()
            self.connection = None
            self.channel = None
            self.exchange = None

        logger.info(
            "Trade signal consumer stopped: chain_id=%s, dex_name=%s",
            self.chain_id,
            self.dex_name,
        )

    async def _message_handler(self, message: AbstractIncomingMessage) -> None:
        """Handle received messages"""
        async with message.process():
            try:
                # Parse message content
                body = message.body.decode()
                try:
                    payload = json.loads(body)
                except json.JSONDecodeError:
                    logger.error("Invalid JSON message: %s", body)
                    return

                # Process trade signal
                await self._process_trade_signal(
                    {
                        "routing_key": message.routing_key,
                        "payload": payload,
                        "properties": {
                            "correlation_id": message.correlation_id,
                            "reply_to": message.reply_to,
                        },
                    }
                )

            except Exception as e:
                logger.exception("Error processing message: %s", str(e))

    async def _process_trade_signal(self, message: Dict[str, Any]) -> None:
        """Process trade signal

        Args:
            message: Message content including trade-related parameters
        """
        try:
            # Get routing key
            routing_key = message.get("routing_key", "")
            logger.info("Received trade signal: routing_key=%s", routing_key)

            # Parse message body
            payload = message.get("payload", {})
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    logger.error("Invalid JSON message body: %s", payload)
                    return

            # Get required parameters
            vault_address = payload.get("vault_address")
            action = payload.get("action")
            token_address = payload.get("token_address")
            amount_in = payload.get("amount_in")
            min_amount_out = payload.get("min_amount_out", "0")

            # Check required parameters
            if not vault_address or not action or not token_address:
                logger.error(
                    "Missing required parameters: vault_address=%s, action=%s, token_address=%s",
                    vault_address,
                    action,
                    token_address,
                )
                return

            # Print portfolio information before trading
            logger.info("Vault portfolio before trade execution:")
            await self.vault_service.print_portfolio(vault_address)

            result = False
            # Execute the appropriate operation based on message type
            if action.lower() == "buy":
                if not amount_in:
                    logger.error("Buy operation missing amount_in parameter")
                    return

                # Convert strings to integers
                amount_in_value = int(amount_in)
                min_amount_out_value = int(min_amount_out)

                # Execute buy operation
                result = await self.vault_service.execute_buy_signal(
                    vault_address=vault_address,
                    token_address=token_address,
                    amount=amount_in_value,
                    min_output_amount=min_amount_out_value,
                    max_allocation_percent=10000,  # Default 100%
                )

                if result:
                    logger.info(
                        "Buy operation successful: vault=%s, token=%s, amount=%s",
                        vault_address,
                        token_address,
                        amount_in,
                    )
                else:
                    logger.error(
                        "Buy operation failed: vault=%s, token=%s, amount=%s",
                        vault_address,
                        token_address,
                        amount_in,
                    )

            elif action.lower() == "sell":
                # For sell operations, amount_in can be empty (indicating sell all)
                amount_in_value = int(amount_in) if amount_in else 0
                min_amount_out_value = int(min_amount_out)

                # Execute sell operation
                result = await self.vault_service.execute_sell_signal(
                    vault_address=vault_address,
                    token_address=token_address,
                    amount=amount_in_value,  # 0 means sell all
                    min_output_amount=min_amount_out_value,
                )

                if result:
                    amount_str = amount_in if amount_in else "all"
                    logger.info(
                        "Sell operation successful: vault=%s, token=%s, amount=%s",
                        vault_address,
                        token_address,
                        amount_str,
                    )
                else:
                    logger.error(
                        "Sell operation failed: vault=%s, token=%s",
                        vault_address,
                        token_address,
                    )
            else:
                logger.error("Unsupported operation type: %s", action)
                result = False

            # After successful trading, print vault portfolio information
            logger.info("Vault portfolio after trade execution:")
            await self.vault_service.print_portfolio(vault_address)

            # Check if this is an RPC request
            if "request_type" in payload and payload["request_type"] == "rpc":
                correlation_id = message.get("properties", {}).get("correlation_id")
                reply_to = message.get("properties", {}).get("reply_to")

                if correlation_id and reply_to:
                    # Build RPC response
                    response = {
                        "success": result,
                        "tx_hash": (
                            self.vault_service.last_tx_hash.hex()
                            if result and self.vault_service.last_tx_hash
                            else None
                        ),
                        "error": (
                            self.vault_service.last_error
                            if not result and self.vault_service.last_error
                            else None
                        ),
                    }

                    # If the transaction was successful, add portfolio information to the response
                    if result:
                        portfolio = self.vault_service.get_portfolio_composition(
                            vault_address
                        )
                        if portfolio and "error" not in portfolio:
                            response["portfolio"] = portfolio

                    # Send RPC response
                    response_message = Message(
                        body=json.dumps(response).encode(),
                        content_type="application/json",
                        correlation_id=correlation_id,
                    )

                    await self.exchange.publish(response_message, routing_key=reply_to)
                    logger.info("RPC response sent: correlation_id=%s", correlation_id)

        except Exception as e:
            logger.exception("Error processing trade signal: %s", str(e))
