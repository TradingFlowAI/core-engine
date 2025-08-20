import asyncio
import logging
import traceback
from decimal import Decimal, getcontext
from typing import Dict, Optional

from tradingflow.station.services.aptos_vault_service import AptosVaultService
from tradingflow.station.utils.token_price_util import (
    get_aptos_monitored_token_info,
    get_aptos_token_address_by_symbol,
    get_aptos_token_price_usd,
)
from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

# input handles
FROM_TOKEN_HANDLE = "from_token"  # Input token symbol
TO_TOKEN_HANDLE = "to_token"  # Output token symbol
AMOUNT_IN_HANDLE = "amount_in"
AMOUNT_IN_HANDLE_HUMAN_READABLE = "amount_in_human_readable"
AMOUNT_IN_HANDLE_PERCENTAGE = "amount_in_percentage"
VAULT_ADDRESS_HANDLE = "vault_address"
SLIPPAGE_TOLERANCE_HANDLE = "slippage_tolerance"
INPUT_TOKEN_ADDRESS_HANDLE = "input_token_address"
OUTPUT_TOKEN_ADDRESS_HANDLE = "output_token_address"
CHAIN_HANDLE = "chain"
SYMBOL_HANDLE = "symbol"
# output
TX_RECEIPT_HANDLE = "trade_receipt"

# Set decimal precision for Decimal operations
getcontext().prec = 50


def calculate_sqrt_price_limit_q64_64(
    input_price: float, output_price: float, slippage_tolerance: float
) -> str:
    """
    Calculate sqrt_price_limit for exact_in scenario (Q64.64 format)

    Args:
        input_price: USD price of input token
        output_price: USD price of output token
        slippage_tolerance: Slippage tolerance (percentage, e.g., 0.5 means 0.5%)

    Returns:
        str: String representation of sqrt_price_limit in Q64.64 format
    """
    try:
        # Calculate current price ratio (output_price / input_price)
        price_ratio = Decimal(str(output_price)) / Decimal(str(input_price))

        # For exact_in, we want to get at least the specified amount of output tokens
        # So the price limit should be more favorable than current price (lower price_ratio)
        slippage_factor = Decimal(str(slippage_tolerance)) / Decimal("100")
        adjusted_price_ratio = price_ratio * (Decimal("1") - slippage_factor)

        # Calculate sqrt(price_ratio)
        sqrt_price_ratio = adjusted_price_ratio.sqrt()

        # Convert to Q64.64 fixed point number
        # Q64.64 means multiply by 2^64
        q64_64_multiplier = Decimal(2) ** 64
        sqrt_price_limit_q64_64 = int(sqrt_price_ratio * q64_64_multiplier)

        return str(sqrt_price_limit_q64_64)

    except Exception as e:
        raise ValueError(f"Failed to calculate sqrt_price_limit: {str(e)}")


@register_node_type(
    "swap_node",
    default_params={
        "chain": "aptos",
        "from_token": None,
        "to_token": None,
        "symbol": "USDT->xBTC",
        "vault_address": None,
        "slippage_tolerance": 1,
        "input_token_address": None,
        "output_token_address": None,
        "amount_in": None,
        "amount_in_percentage": None,
        "amount_in_human_readable": None,
    },
)
class HyperionDEXTradeNode(NodeBase):
    """
    Hyperion DEX Trading Node - Executes token swaps on Hyperion DEX via TradingFlow Vault

    This node handles token swapping operations on the Hyperion DEX through the TradingFlow Vault system.
    It supports multiple ways to specify trade amounts and includes slippage protection.

    Input parameters:
    - input_token_address: Source token contract address (token to swap from)
    - output_token_address: Target token contract address (token to swap to)
    - vault_address: TradingFlow vault contract address (acts as user address)
    - amount_in: Exact trading amount in Wei format (integer string with decimals applied)
    - amount_in_percentage: Trading amount as percentage of current balance (0-100)
    - amount_in_human_readable: Trading amount in human-readable decimal format
    - slippage_tolerance: Maximum acceptable slippage percentage (default: 0.5%)

    Amount specification priority (only one should be provided):
    1. amount_in - Direct Wei format amount (highest priority)
    2. amount_in_human_readable - Decimal amount, will be converted to Wei
    3. amount_in_percentage - Percentage of current token balance

    Trading process:
    1. Determines final trading amount based on input parameters
    2. Fetches current token prices from external sources
    3. Calculates minimum output amount with slippage protection
    4. Computes sqrt_price_limit for DEX price protection
    5. Executes swap via TradingFlow Vault admin function
    6. Returns transaction receipt with execution details

    Output signals:
    - DEX_TRADE_RECEIPT: Signal containing complete transaction results and metadata

    Example configurations:
    ```python
    # Using exact Wei amount
    node = HyperionDEXTradeNode(
        amount_in="1000000000",  # 10 tokens with 8 decimals
        input_token_address="0xa",
        output_token_address="0xbae207...",
        vault_address="0x6a1a233...",
        slippage_tolerance=1.0
    )

    # Using human readable amount
    node = HyperionDEXTradeNode(
        amount_in_human_readable=10.5,  # 10.5 tokens
        input_token_address="0xa",
        output_token_address="0xbae207...",
        vault_address="0x6a1a233...",
        slippage_tolerance=0.5
    )

    # Using percentage of balance
    node = HyperionDEXTradeNode(
        amount_in_percentage=25.0,  # 25% of current balance
        input_token_address="0xa",
        output_token_address="0xbae207...",
        vault_address="0x6a1a233...",
        slippage_tolerance=1.0
    )
    ```
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        chain: str = "aptos",
        from_token: str = None,
        to_token: str = None,
        # NOTE: 实际是swap pair的symbol，如 "USDT->xBTC"
        symbol: str = None,
        input_token_address: str = None,
        output_token_address: str = None,
        vault_address: str = None,
        amount_in: Optional[str] = None,
        amount_in_percentage: Optional[float] = None,
        amount_in_human_readable: Optional[float] = None,
        slippage_tolerance: float = 1.0,
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

        self.chain = chain
        self.from_token = from_token
        self.to_token = to_token
        if from_token and to_token:
            self.swap_pair_symbol = f"{from_token}->{to_token}"
        else:
            self.swap_pair_symbol = symbol
        if self.swap_pair_symbol is None:
            self.input_token_address, self.output_token_address = (
                self._parse_symbol_to_token_addresses(self.swap_pair_symbol)
            )

        # Trading configuration
        self.slippage_tolerance = slippage_tolerance

        # Token and amount
        self.vault_address = vault_address
        self.input_token_address = input_token_address
        self.output_token_address = output_token_address
        self.input_token_info = None
        self.output_token_info = None
        self.input_token_decimals = None
        self.output_token_decimals = None
        if self.input_token_address:
            self.input_token_info = get_aptos_monitored_token_info(input_token_address)
            self.input_token_decimals = self.input_token_info.get("decimals")
        if self.output_token_address:
            self.output_token_info = get_aptos_monitored_token_info(
                output_token_address
            )
            self.output_token_decimals = self.output_token_info.get("decimals")

        self.amount_in = amount_in
        self.amount_in_percentage = amount_in_percentage
        self.amount_in_human_readable = amount_in_human_readable

        # Validate percentage input
        if self.amount_in_percentage is not None:
            if not (0 < self.amount_in_percentage <= 100):
                raise ValueError("amount_in_percentage must be between 0 and 100")

        # Validate human readable amount
        if self.amount_in_human_readable is not None:
            if not (self.amount_in_human_readable > 0):
                raise ValueError("amount_in_human_readable must be greater than 0")

        # Transaction result
        self.tx_result = None

        # Logging setup
        self.logger = logging.getLogger(f"HyperionDEXTradeNode.{node_id}")

        self.vault_service = AptosVaultService.get_instance()

    def _parse_symbol_to_token_addresses(
        self, swap_pair_symbol: str
    ) -> tuple[str, str]:
        """Parse swap pair symbol to input and output token addresses"""
        if not swap_pair_symbol or "->" not in swap_pair_symbol:
            raise ValueError(f"Invalid swap pair symbol format: {swap_pair_symbol}")

        input_symbol, output_symbol = swap_pair_symbol.split("->")
        input_token_address = get_aptos_token_address_by_symbol(input_symbol.strip())
        output_token_address = get_aptos_token_address_by_symbol(output_symbol.strip())

        if not input_token_address or not output_token_address:
            raise ValueError(
                f"Cannot resolve token addresses for symbols: {input_symbol}, {output_symbol}"
            )

        return input_token_address, output_token_address

    async def _is_amount_in_valid(self, amount_in: Optional[str]) -> bool:
        """Check if amount_in is valid (not None, not empty, and > 0)"""
        if amount_in is None or amount_in == "":
            return False

        try:
            amount = int(amount_in)
            return amount > 0
        except (ValueError, TypeError):
            await self.persist_log(f"Invalid amount_in value: {amount_in}", log_level="ERROR")
            return False

    async def get_token_amount_from_holdings(
        self, token_address: str, investor_address: str
    ) -> Optional[Decimal]:
        """Get token balance from vault holdings"""
        if not self.vault_service:
            self.vault_service = AptosVaultService.get_instance()

        # Get investor holdings
        holdings_data = await self.vault_service.get_investor_holdings(investor_address)
        holdings = holdings_data.get("holdings", [])

        # Find the specific token
        for holding in holdings:
            if holding.get("token_address", "").lower() == token_address.lower():
                amount_raw_str = holding.get("amount", "0")
                await self.persist_log(
                    f"Token {token_address} balance: {amount_raw_str} (decimals: {self.input_token_decimals})"
                )
                return Decimal(amount_raw_str)

        await self.persist_log(f"Token not found in holdings: {token_address}", log_level="WARNING")
        raise ValueError(
            f"Token {token_address} not found in holdings for investor {investor_address}"
        )

    async def calculate_amount_in_from_percentage(
        self,
    ) -> str:
        """Calculate actual amount from percentage of balance"""
        investor_address = self.vault_address
        if self.amount_in_percentage is None:
            raise ValueError(
                "amount_in_percentage must be provided for percentage calculation"
            )

        # Get amount_raw balance from holdings
        amount_raw = await self.get_token_amount_from_holdings(
            self.input_token_address, investor_address
        )
        # Calculate amount based on percentage
        calculated_amount_in_str = str(
            int(amount_raw * Decimal(self.amount_in_percentage) / Decimal(100))
        )

        await self.persist_log(
            f"Calculated amount_in from percentage: {calculated_amount_in_str} (percentage: {self.amount_in_percentage}%, balance: {amount_raw})"
        )
        return calculated_amount_in_str

    async def get_final_amount_in(self) -> Optional[str]:
        """Get final amount_in, considering direct amount, human readable amount, and percentage"""
        # Priority 1: Check if direct amount_in is valid (already in Wei format)
        if self._is_amount_in_valid(self.amount_in):
            await self.persist_log(
                f"Using valid direct amount (Wei format): {self.amount_in}"
            )
            return self.amount_in

        # Priority 2: If human readable amount is provided, convert it to Wei format
        if (
            self.amount_in_human_readable is not None
            and self.amount_in_human_readable > 0
        ):

            # Get input token decimals for conversion
            input_decimals = self.input_token_decimals

            # Convert human readable amount to Wei format
            amount_decimal = Decimal(str(self.amount_in_human_readable))
            amount_wei = int(amount_decimal * Decimal(10**input_decimals))
            amount_wei_str = str(amount_wei)
            await self.persist_log(
                f"Using amount_in_human_readable: {self.amount_in_human_readable}, converted to: {amount_wei_str}"
            )
            return amount_wei_str

        # Priority 3: If percentage is provided, calculate from balance
        if self.amount_in_percentage is not None:
            calculated_amount = await self.calculate_amount_in_from_percentage()
            return calculated_amount

        raise ValueError(
            "No valid amount_in provided: either direct amount, human readable amount, or percentage must be specified"
        )

    async def send_trade_signal(self) -> bool:
        """Send trading signal and wait for execution result"""
        try:
            final_amount_in = await self.get_final_amount_in()

            # Handle minimum output amount
            estimated_min_output_amount, sqrt_price_limit = (
                await self.get_estimated_min_output_amount(
                    input_token_address=self.input_token_address,
                    output_token_address=self.output_token_address,
                    amount_in=final_amount_in,
                    slippage=self.slippage_tolerance,
                )
            )

            # swap
            tx_result = await self.vault_service.admin_execute_swap(
                self.vault_address,
                self.input_token_address,
                self.output_token_address,
                int(final_amount_in),
                sqrt_price_limit=sqrt_price_limit,
                amount_out_min=int(estimated_min_output_amount),
            )
            self.tx_result = tx_result
            # Check transaction result
            if not tx_result.get("success", False):
                error_msg = tx_result.get("message", "Unknown error")
                await self.persist_log(f"Transaction execution failed: {error_msg}", log_level="ERROR")
                await self.set_status(
                    NodeStatus.FAILED, f"Transaction execution failed: {error_msg}"
                )

            return True

        except Exception as e:
            await self.persist_log(f"Error sending trading signal: {str(e)}", log_level="ERROR")
            await self.persist_log(f"Traceback: {traceback.format_exc()}", log_level="ERROR")
            await self.set_status(
                NodeStatus.FAILED, f"Error sending trading signal: {str(e)}"
            )
            self.tx_result = {
                "success": False,
                "message": str(e),
                "tx_data": {},
            }
            return False

    def prepare_trade_receipt(self) -> Dict:
        """Prepare transaction receipt signal data"""
        tx_data = self.tx_result.get("transaction_result", {})
        trade_details = tx_data.get("tradeDetails", {})
        # 构建交易收据
        receipt = {
            # 基本交易信息
            "success": self.tx_result.get("success", False),
            "tx_hash": tx_data.get("hash"),
            "status": tx_data.get("status", "Unknown"),
            "message": self.tx_result.get("message", ""),
            # DEX和链信息
            "dex_name": "hyperion",
            "vault_address": self.vault_address,
            # 代币地址信息
            "input_token_address": self.input_token_address,
            "output_token_address": self.output_token_address,
            # 交易金额信息（从tradeDetails获取实际执行的金额）
            "amount_in": trade_details.get("amountIn"),
            "amount_out": trade_details.get("amountOut"),
            "amount_out_min": trade_details.get("amountOutMin"),
            # 交易执行信息
            "timestamp": tx_data.get("timestamp"),
            "gas_used": tx_data.get("gasUsed"),
            # 资产信息（从tradeDetails获取）
            "from_asset": trade_details.get("fromAsset", self.input_token_address),
            "to_asset": trade_details.get("toAsset", self.output_token_address),
            # 滑点信息
            "slippage_tolerance": self.slippage_tolerance,
            # 完整的原始结果供下游节点参考
            "raw_result": self.tx_result,
            # 交易详情
            "trade_details": trade_details,
            "tx_data": tx_data,
        }
        return receipt

    async def get_estimated_min_output_amount(
        self,
        input_token_address: str,
        output_token_address: str,
        amount_in: str,
        slippage: float,
    ) -> tuple[Optional[str], Optional[str]]:
        """
        使用代币价格估算输出金额和计算 sqrt_price_limit (exact_in 场景)

        Args:
            input_token_address: 输入代币地址
            output_token_address: 输出代币地址
            amount_in: 输入金额（以最小单位表示）
            slippage: 滑点百分比

        Returns:
            tuple: (估算的输出金额, sqrt_price_limit字符串)，失败时返回(None, None)
        """
        await self.persist_log(
            f"Estimating output for Hyperion exact_in swap: input={input_token_address}, output={output_token_address}, amount_in={amount_in}, slippage={slippage}%"
        )

        # 获取输入代币和输出代币的价格
        input_price = get_aptos_token_price_usd(input_token_address)
        output_price = get_aptos_token_price_usd(output_token_address)

        if input_price is None:
            raise ValueError(f"Cannot get price for input token: {input_token_address}")

        if output_price is None:
            raise ValueError(
                f"Cannot get price for output token: {output_token_address}"
            )

        if output_price <= 0:
            raise ValueError(
                f"Output token price must be greater than 0: {output_token_address}"
            )

        input_decimals = self.input_token_decimals
        output_decimals = self.output_token_decimals

        await self.persist_log(
            f"Token info: input_decimals={input_decimals}, output_decimals={output_decimals}, input_price=$input_price, output_price=$output_price"
        )

        # 将输入金额从最小单位转换为实际金额
        amount_in_decimal = Decimal(amount_in) / Decimal(10**input_decimals)

        # 计算输入金额的USD价值
        input_value_usd = amount_in_decimal * Decimal(str(input_price))

        # 根据输出代币价格计算能够购买的输出代币数量
        output_amount_decimal = input_value_usd / Decimal(str(output_price))

        # 应用滑点（减少预期输出）
        slippage_factor = Decimal("1") - (Decimal(str(slippage)) / Decimal("100"))
        output_amount_with_slippage = output_amount_decimal * slippage_factor

        # 转换回最小单位
        output_amount_raw = int(
            output_amount_with_slippage * Decimal(10**output_decimals)
        )

        # 计算 sqrt_price_limit (Q64.64) - exact_in 场景
        sqrt_price_limit = calculate_sqrt_price_limit_q64_64(
            input_price=float(input_price),
            output_price=float(output_price),
            slippage_tolerance=slippage,
        )

        await self.persist_log(
            f"Exact_in estimation result: input_amount={amount_in_decimal}, input_value_usd=$input_value_usd, "
            f"output_amount={output_amount_with_slippage}, output_amount_raw={output_amount_raw}, slippage_applied={slippage}%, "
            f"sqrt_price_limit={sqrt_price_limit}"
        )

        return str(output_amount_raw), sqrt_price_limit

    async def execute(self) -> bool:
        """Execute node logic"""
        try:
            await self.persist_log(
                f"Preparing trade receipt with input_token: {self.input_token_address}, output_token: {self.output_token_address}, amount_in: {self.amount_in}"
            )
            await self.set_status(NodeStatus.RUNNING)

            # Send trading signal and wait for execution result
            # if not await self.send_trade_signal():
            #     return False
            await self.send_trade_signal()
            # Prepare transaction receipt data
            trade_receipt = self.prepare_trade_receipt()
            await self.persist_log(f"Transaction receipt data: {trade_receipt}")

            # Send transaction receipt signal to downstream nodes
            if not await self.send_signal(
                TX_RECEIPT_HANDLE, SignalType.DEX_TRADE_RECEIPT, payload=trade_receipt
            ):
                await self.persist_log("Failed to send transaction receipt signal", log_level="ERROR")
                await self.set_status(
                    NodeStatus.FAILED, "Failed to send transaction receipt signal"
                )
                return False

            # Complete node execution
            await self.set_status(NodeStatus.COMPLETED)
            await self.persist_log(
                f"DEX trading node execution completed successfully: {self.input_token_address} -> {self.output_token_address}"
            )
            return True

        except asyncio.CancelledError:
            await self.persist_log("DEX trading node execution cancelled")
            await self.set_status(NodeStatus.TERMINATED, "Execution cancelled")
            return False

        except Exception as e:
            await self.persist_log(f"DEX trading node execution error: {str(e)}", log_level="ERROR")
            await self.persist_log(f"Traceback: {traceback.format_exc()}", log_level="ERROR")
            await self.set_status(NodeStatus.FAILED, f"Error sending trading signal: {str(e)}")
            return False

    def _register_input_handles(self) -> None:
        """Register input handles"""
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE,
            data_type=int,
            description="Transaction amount (in Wei)",
            example=100,
            auto_update_attr="amount_in",
        )
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE_HUMAN_READABLE,
            data_type=float,
            description="Transaction amount (human-readable format)",
            example=10.5,
            auto_update_attr="amount_in_human_readable",
        )
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE_PERCENTAGE,
            data_type=float,
            description="Transaction amount (as a percentage of current balance)",
            example=25.0,
            auto_update_attr="amount_in_percentage",
        )
        # Register Vault address input handle
        self.register_input_handle(
            name=VAULT_ADDRESS_HANDLE,
            data_type=str,
            description="Transaction Vault address (user address)",
            example="0x6a1a233...",
            auto_update_attr="vault_address",
        )
        # Register slippage tolerance input handle
        self.register_input_handle(
            name=SLIPPAGE_TOLERANCE_HANDLE,
            data_type=float,
            description="Slippage tolerance (percentage)",
            example=0.5,
            auto_update_attr="slippage_tolerance",
        )
        # Register input token address handle
        self.register_input_handle(
            name=INPUT_TOKEN_ADDRESS_HANDLE,
            data_type=str,
            description="Input token address (source token for trading)",
            example="0xa",
            auto_update_attr="input_token_address",
        )
        # 注册输出代币地址句柄
        self.register_input_handle(
            name=OUTPUT_TOKEN_ADDRESS_HANDLE,
            data_type=str,
            description="输出代币地址（交易的目标代币）",
            example="0xbae207...",
            auto_update_attr="output_token_address",
        )
        self.register_input_handle(
            name=CHAIN_HANDLE,
            data_type=str,
            description="交易所在的区块链网络",
            example="aptos",
            auto_update_attr="chain",
        )
        self.register_input_handle(
            name=SYMBOL_HANDLE,
            data_type=str,
            description="Trading pair symbol, e.g., 'USDT->xBTC'",
            example="USDT->xBTC",
            auto_update_attr="swap_pair_symbol",
        )
        self.register_input_handle(
            name=FROM_TOKEN_HANDLE,
            data_type=str,
            description="Input token symbol (for symbol parsing)",
            example="USDT",
            auto_update_attr="from_token",
        )
        self.register_input_handle(
            name=TO_TOKEN_HANDLE,
            data_type=str,
            description="Output token symbol (for symbol parsing)",
            example="xBTC",
            auto_update_attr="to_token",
        )

    async def _on_symbol_received(self, symbol: str) -> None:
        await self.persist_log(f"Received symbol update: {symbol}")
        self.swap_pair_symbol = symbol
        # Parse symbol and update token addresses
        self.input_token_address, self.output_token_address = (
            self._parse_symbol_to_token_addresses(symbol)
        )
        # Update input and output token information
        self.input_token_info = get_aptos_monitored_token_info(self.input_token_address)
        self.output_token_info = get_aptos_monitored_token_info(
            self.output_token_address
        )
        if not self.input_token_info or not self.input_token_address:
            raise ValueError(f"Cannot resolve input token address for symbol: {symbol}")
        self.input_token_decimals = self.input_token_info.get("decimals")
        self.output_token_decimals = self.output_token_info.get("decimals")

    async def _on_input_token_address_received(self, amount_in: str) -> None:
        await self.persist_log(f"Received input token address: {amount_in}")
        self.input_token_address = amount_in
        # Update input token information
        self.input_token_info = get_aptos_monitored_token_info(self.input_token_address)
        if not self.input_token_info or not self.input_token_address:
            raise ValueError(
                f"Cannot resolve input token address: {self.input_token_address}"
            )
        self.input_token_decimals = self.input_token_info.get("decimals")

    async def _on_output_token_address_received(self, amount_in: str) -> None:
        await self.persist_log(f"Received output token address: {amount_in}")
        self.output_token_address = amount_in
        # Update output token information
        self.output_token_info = get_aptos_monitored_token_info(
            self.output_token_address
        )
        self.output_token_decimals = self.output_token_info.get("decimals")
        if not self.output_token_info or not self.output_token_address:
            raise ValueError(
                f"Cannot resolve output token address: {self.output_token_address}"
            )

    async def _on_from_token_received(self, from_token: str) -> None:
        await self.persist_log(f"Received from token: {from_token}")
        self.from_token = from_token
        # Update input token address
        self.input_token_address = get_aptos_token_address_by_symbol(from_token)
        self.input_token_info = get_aptos_monitored_token_info(self.input_token_address)
        self.input_token_decimals = self.input_token_info.get("decimals")
        if not self.input_token_address:
            raise ValueError(
                f"Cannot resolve input token address for symbol: {from_token}"
            )

    async def _on_to_token_received(self, to_token: str) -> None:
        await self.persist_log(f"Received to token: {to_token}")
        self.to_token = to_token
        # Update output token address
        self.output_token_address = get_aptos_token_address_by_symbol(to_token)
        self.output_token_info = get_aptos_monitored_token_info(
            self.output_token_address
        )
        self.output_token_decimals = self.output_token_info.get("decimals")
        if not self.output_token_address:
            raise ValueError(
                f"Cannot resolve output token address for symbol: {to_token}"
            )
