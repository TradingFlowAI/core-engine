import asyncio
import logging
import time
import traceback
from typing import Dict, Optional

from web3 import Web3

from tradingflow.depot.python.mq.dex_trade_signal_publisher import DexTradeSignalPublisher
from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus


# Unit conversion function
def to_wei(amount, decimals=18):
    return int(amount * 10**decimals)


# handles
AMOUNT_IN_HANDLE = "amount_in_handle"
DEX_TRADE_ACTION_HANDLE = "trade_action"


@register_node_type(
    "dex_trade_node",
    default_params={
        "chain_id": 1,  # Default Ethereum mainnet
        "dex_name": "uniswap",
        "vault_address": None,  # Required configuration
        "slippage_tolerance": 0.5,  # Slippage tolerance (percentage)
        "signal_timeout": 300,  # Timeout for signal execution (seconds)
        "output_token_address": None,  # Required configuration
        "amount_in": None,  # Required configuration
        "min_amount_out": "0",  # Minimum output amount, default is 0
        "action": "buy",  # Trading action, buy or sell
    },
)
class UniswapV3DEXTradeNode(NodeBase):
    """
    DEX Trading Signal Node - Sends trading signals to message queue and waits for execution results

    Input parameters:
    - chain_id: Chain ID (1=Ethereum mainnet, 56=BSC, etc.)
    - dex_name: DEX name (e.g. "uniswap", "sushiswap")
    - vault_address: Vault contract address
    - slippage_tolerance: Slippage tolerance (percentage)
    - signal_timeout: Timeout for waiting signal execution (seconds)
    - input_token_address: Payment token contract address (can be None for sell)
    - output_token_address: Receiving token contract address (required for buy, for sell it's the token to sell)
    - amount_in: Trading amount (None for sell means sell all)
    - min_amount_out: Minimum amount to receive
    - action: Trading action, "buy" or "sell"

    Output signals:
    - DEX_TRADE_RECEIPT: Signal containing transaction results
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        chain_id: int,
        dex_name: str,
        vault_address: str,
        action: str = "buy",
        output_token_address: str = None,
        amount_in: Optional[str] = None,
        min_amount_out: str = "0",
        slippage_tolerance: float = 0.5,
        signal_timeout: int = 30,
        **kwargs,
    ):
        """Initialize DEX Trading Signal Node"""
        # Initialize base node
        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            **kwargs,
        )

        # Trading configuration
        self.chain_id = str(chain_id)  # Ensure it's a string
        self.dex_name = dex_name.lower()  # Convert to lowercase for consistency
        self.vault_address = Web3.to_checksum_address(vault_address)
        self.action = action.lower()  # buy or sell, convert to lowercase
        self.slippage_tolerance = slippage_tolerance
        self.signal_timeout = signal_timeout

        # Token and amount
        self.token_address = Web3.to_checksum_address(
            output_token_address
        )  # For sell it's the token to sell, for buy it's the token to buy
        self.amount_in = amount_in
        self.min_amount_out = min_amount_out

        # Transaction result
        self.tx_result = None

        # Signal publisher
        self.signal_publisher = None

        # Logging setup
        self.logger = logging.getLogger(f"DEXTradeNode.{node_id}")

    async def initialize_publisher(self) -> bool:
        """Initialize trading signal publisher"""
        try:
            self.signal_publisher = DexTradeSignalPublisher(
                chain_id=self.chain_id,
                dex_name=self.dex_name,
                vault_address=self.vault_address,
            )

            await self.signal_publisher.connect()
            self.logger.info(
                "Trading signal publisher initialized successfully: chain_id=%s, dex_name=%s",
                self.chain_id,
                self.dex_name,
            )
            return True

        except Exception as e:
            self.logger.error(
                "Failed to initialize trading signal publisher: %s", str(e)
            )
            await self.set_status(
                NodeStatus.FAILED,
                f"Failed to initialize trading signal publisher: {str(e)}",
            )
            return False

    async def send_trade_signal(self) -> bool:
        """Send trading signal and wait for execution result"""
        try:
            # Prepare metadata
            metadata = {
                "source": "trading_flow",
                "node_id": self.node_id,
                "flow_id": self.flow_id,
                "component_id": self.component_id,
                "cycle": self.cycle,
                "timestamp": int(time.time()),
            }

            self.logger.info(
                "Preparing to send %s signal: token_address=%s",
                self.action,
                self.token_address,
            )

            # Convert amount to Wei (if provided)
            amount_in_wei = None
            if self.amount_in:
                try:
                    amount_in_wei = str(to_wei(float(self.amount_in)))
                    self.logger.info(
                        "Amount conversion: %s -> %s Wei", self.amount_in, amount_in_wei
                    )
                except ValueError as e:
                    self.logger.error("Failed to convert amount to Wei: %s", str(e))
                    await self.set_status(
                        NodeStatus.FAILED, f"Failed to convert amount to Wei: {str(e)}"
                    )
                    return False

            # Handle minimum output amount
            min_amount_out_wei = "0"
            if self.min_amount_out and self.min_amount_out != "0":
                # User explicitly specified min_amount_out, use directly
                try:
                    min_amount_out_wei = str(to_wei(float(self.min_amount_out)))
                    self.logger.info(
                        "Using specified minimum output amount: %s Wei",
                        min_amount_out_wei,
                    )
                except ValueError as e:
                    self.logger.error(
                        "Failed to convert minimum output amount to Wei: %s", str(e)
                    )
                    await self.set_status(
                        NodeStatus.FAILED,
                        f"Failed to convert minimum output amount to Wei: {str(e)}",
                    )
                    return False
            else:
                # User didn't specify min_amount_out, try to calculate using slippage_tolerance
                # For buy operations, need to get current price of output token
                # For sell operations, can use estimated ETH/USDC output
                try:
                    if self.action == "buy" and amount_in_wei:
                        # Get estimated output amount
                        estimated_out = await self.get_estimated_output(
                            self.token_address, amount_in_wei, is_buy=True
                        )
                        if estimated_out:
                            # Calculate minimum output based on slippage
                            # Assume slippage_tolerance is percentage, e.g. 0.5 means 0.5%
                            min_amount_out_value = int(
                                float(estimated_out)
                                * (1 - self.slippage_tolerance / 100)
                            )
                            min_amount_out_wei = str(min_amount_out_value)
                            self.logger.info(
                                "Calculated minimum output based on slippage: Estimated=%s Wei, Slippage=%s%%, Min Output=%s Wei",
                                estimated_out,
                                self.slippage_tolerance,
                                min_amount_out_wei,
                            )
                    elif self.action == "sell" and (
                        amount_in_wei or self.action == "sell_all"
                    ):
                        # For sell, also need to get estimated output
                        estimated_out = await self.get_estimated_output(
                            self.token_address,
                            amount_in_wei,  # If sell_all, this is None
                            is_buy=False,
                        )
                        if estimated_out:
                            min_amount_out_value = int(
                                float(estimated_out)
                                * (1 - self.slippage_tolerance / 100)
                            )
                            min_amount_out_wei = str(min_amount_out_value)
                            self.logger.info(
                                "Calculated minimum output based on slippage: Estimated=%s Wei, Slippage=%s%%, Min Output=%s Wei",
                                estimated_out,
                                self.slippage_tolerance,
                                min_amount_out_wei,
                            )
                except Exception as e:
                    self.logger.warning(
                        "Failed to calculate minimum output amount, using default 0: %s",
                        str(e),
                    )
                    # If calculation fails, use default value 0
                    min_amount_out_wei = "0"

            # Send different signals based on transaction type
            if self.action == "buy":
                self.tx_result = (
                    await self.signal_publisher.publish_buy_signal_and_wait(
                        token_address=self.token_address,
                        amount_in=amount_in_wei,  # Use Wei amount
                        min_amount_out=min_amount_out_wei,  # Use Wei minimum amount
                        metadata=metadata,
                        timeout=self.signal_timeout,
                    )
                )
                self.logger.info("Buy signal execution result: %s", self.tx_result)

            elif self.action == "sell":
                self.tx_result = (
                    await self.signal_publisher.publish_sell_signal_and_wait(
                        token_address=self.token_address,
                        amount_in=amount_in_wei,  # Use Wei amount, None means sell all
                        min_amount_out=min_amount_out_wei,  # Use Wei minimum amount
                        metadata=metadata,
                        timeout=self.signal_timeout,
                    )
                )
                self.logger.info("Sell signal execution result: %s", self.tx_result)

            else:
                self.logger.error("Unsupported trading action: %s", self.action)
                await self.set_status(
                    NodeStatus.FAILED, f"Unsupported trading action: {self.action}"
                )
                return False

            # Check transaction result
            if not self.tx_result.get("success", False):
                error_msg = self.tx_result.get("message", "Unknown error")
                self.logger.error("Transaction execution failed: %s", error_msg)
                await self.set_status(
                    NodeStatus.FAILED, f"Transaction execution failed: {error_msg}"
                )
                return False

            return True

        except Exception as e:
            self.logger.error("Error sending trading signal: %s", str(e))
            await self.set_status(
                NodeStatus.FAILED, f"Error sending trading signal: {str(e)}"
            )
            return False

    async def get_estimated_output(
        self, token_address: str, amount_in: str, is_buy: bool
    ) -> Optional[str]:
        # TODO: Implement logic to get estimated output amount
        raise NotImplementedError

    def prepare_trade_receipt(self) -> Dict:
        """Prepare transaction receipt signal data"""
        tx_data = self.tx_result
        receipt = {
            "action": self.action.upper(),
            "tx_hash": tx_data.get("tx_hash"),
            "dex_name": self.dex_name,
            "chain_id": self.chain_id,
            "vault_address": self.vault_address,
            "token_address": self.token_address,
            "amount_in": self.amount_in,
            "min_amount_out": self.min_amount_out,
            "amount_out": tx_data.get("amount_out"),
            "executed_at": tx_data.get("timestamp", int(time.time())),
            "gas_used": tx_data.get("gas_used"),
            "gas_price": tx_data.get("gas_price"),
            "status": tx_data.get(
                "status", "SUCCESS" if self.tx_result.get("success") else "FAILED"
            ),
            "block_number": tx_data.get("block_number"),
            "execution_price": tx_data.get("execution_price"),
            # Add complete original result for downstream node reference
            "raw_result": self.tx_result,
        }

        return receipt

    async def execute(self) -> bool:
        """Execute node logic"""
        try:
            self.logger.info(
                "Starting DEX trading node execution: action=%s, token=%s",
                self.action,
                self.token_address,
            )
            await self.set_status(NodeStatus.RUNNING)

            # Initialize trading signal publisher
            if not await self.initialize_publisher():
                return False

            # Send trading signal and wait for execution result
            if not await self.send_trade_signal():
                return False

            # Prepare transaction receipt data
            trade_receipt = self.prepare_trade_receipt()
            self.logger.info("Transaction receipt data: %s", trade_receipt)

            # Send transaction receipt signal to downstream nodes
            if not await self.send_signal(
                "output", SignalType.DEX_TRADE_RECEIPT, payload=trade_receipt
            ):
                self.logger.error("Failed to send transaction receipt signal")
                await self.set_status(
                    NodeStatus.FAILED, "Failed to send transaction receipt signal"
                )
                return False

            # Complete node execution
            await self.set_status(NodeStatus.COMPLETED)
            self.logger.info(
                "DEX trading node execution completed, transaction hash: %s",
                trade_receipt.get("tx_hash"),
            )
            return True

        except asyncio.CancelledError:
            self.logger.info("DEX trading node execution cancelled")
            await self.set_status(NodeStatus.TERMINATED, "Execution cancelled")
            return False

        except Exception as e:
            error_message = f"DEX trading node execution error: {str(e)}"
            self.logger.error("DEX trading node execution error: %s", str(e))
            self.logger.error(traceback.format_exc())
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

        finally:
            # Cleanup resources
            if self.signal_publisher:
                await self.signal_publisher.close()
                self.signal_publisher = None

    def _register_input_handles(self) -> None:
        """注册输入句柄"""
        # 注册交易金额输入句柄
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE,
            data_type=float,
            description="交易金额",
            example=100.50,
        )
        # 注册交易操作类型输入句柄
        self.register_input_handle(
            name=DEX_TRADE_ACTION_HANDLE,
            data_type=str,
            description="交易操作类型（buy/sell）",
            example="buy",
        )

    async def _on_amount_in_handle_received(self, amount_in):
        """Handle received amount_in signal"""
        try:
            self.logger.info("handle amount_in received: %s", amount_in)
            # Additional processing logic can be added here
            self.amount_in = amount_in
            self.logger.info(
                "Received amount_in: %s, type: %s",
                self.amount_in,
                type(self.amount_in).__name__,
            )
            return True
        except Exception as e:
            self.logger.error("Error processing amount_in handle: %s", str(e))
            return False

    async def _on_trade_action_handle_received(self, trade_action):
        """Handle received trade action signal"""
        try:
            self.logger.info("handle trade action received: %s", trade_action)
            # Additional processing logic can be added here
            self.action = trade_action.lower()  # Ensure action is lowercase
            if self.action not in ["buy", "sell"]:
                raise ValueError(f"Unsupported action: {self.action}")
            return True
        except Exception as e:
            self.logger.error("Error processing trade action handle: %s", str(e))
            return False
