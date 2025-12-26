import asyncio
# Removed logging import - using persist_log from NodeBase
import os
import traceback
from decimal import Decimal, getcontext
from typing import Any, Dict, List, Optional, Tuple

import httpx

from services.aptos_vault_service import AptosVaultService
from services.flow_evm_vault_service import FlowEvmVaultService
from utils.token_price_util import (
    get_aptos_monitored_token_info,
    get_aptos_token_address_by_symbol,
    get_aptos_token_price_usd_async,
    get_flow_evm_token_prices_usd,
)
from common.node_decorators import register_node_type
from common.signal_types import Signal, SignalType
from nodes.node_base import NodeBase, NodeStatus
from infra.config import CONFIG
from publishers.trading_signal_publisher import publish_trading_signal

CO_TRADING_ENABLED = os.getenv("ENABLE_CO_TRADING", "false").lower() == "true"

# input handles
FROM_TOKEN_HANDLE = "from_token"
TO_TOKEN_HANDLE = "to_token"
AMOUNT_IN_HANDLE_HUMAN_READABLE = "amount_in_human_readable"
AMOUNT_IN_HANDLE_PERCENTAGE = "amount_in_percentage"
VAULT_HANDLE = "vault"
SLIPPAGE_TOLERANCE_HANDLE = "slippery"

FROM_FIXED_MODES = {"from_fixed", "number", "spend_fixed", "sell_fixed"}
FROM_PERCENT_MODES = {"from_percent", "percentage", "spend_percent", "sell_percent"}
TO_FIXED_MODES = {"to_fixed", "buy_fixed", "receive_fixed"}
TO_PERCENT_MODES = {"to_percent", "buy_percent", "receive_percent"}
# output handles
TX_RECEIPT_HANDLE = "trade_receipt"

getcontext().prec = 50


class SwapSkipException(Exception):
    """Raised when a swap should be skipped without marking the node as failed."""


def calculate_sqrt_price_limit_q64_64(input_price: float, output_price: float, slippage_tolerance: float) -> str:
    """
    Calculate sqrt_price_limit for Hyperion DEX

    Based on Hyperion official docs:
    sqrt_price_limit: a x64 fixed-point number, indicate price impact limit after swap

    This is different from Uniswap's Q64.96 format!
    """
    try:
        # Hyperion uses x64 fixed-point format (multiply by 2^64, not 2^96)
        # This is a price impact limit, not necessarily a complex price ratio

        # Calculate price ratio
        price_ratio = Decimal(str(output_price)) / Decimal(str(input_price))

        # Apply slippage tolerance
        slippage_factor = Decimal(str(slippage_tolerance)) / Decimal("100")
        limit_price_ratio = price_ratio * (Decimal("1") - slippage_factor)

        # Convert to x64 fixed-point format (Hyperion's format)
        sqrt_price = limit_price_ratio.sqrt()
        x64_multiplier = Decimal(2) ** 64
        sqrt_price_x64 = int(sqrt_price * x64_multiplier)

        # Apply reasonable bounds for Hyperion DEX
        # Based on test script value 4295128740 ≈ 2^32
        max_sqrt_price = 2**64 - 1      # x64 format max
        min_sqrt_price = 4295128740      # Known working value from test

        if sqrt_price_x64 > max_sqrt_price:
            sqrt_price_x64 = max_sqrt_price
        elif sqrt_price_x64 < min_sqrt_price:
            sqrt_price_x64 = min_sqrt_price

        return str(sqrt_price_x64)

    except Exception as e:
        # Fallback to known working value
        return "4295128740"


@register_node_type(
    "swap_node",
    default_params={
        "vault": None,
        "from_token": None,
        "to_token": None,
        "slippery": 1.0,
        "amount_in_percentage": None,
        "amount_in_human_readable": None,
    },
)
class SwapNode(NodeBase):
    """
    Multi-Chain Swap Node - Executes token swaps on Aptos and Flow EVM

    Supported Chains:
    - aptos: Aptos blockchain using Hyperion DEX
    - flow_evm: Flow EVM blockchain

    Input parameters:
    - chain: Blockchain network ('aptos' or 'flow_evm')
    - from_token: Source token symbol
    - to_token: Target token symbol
    - vault_address: Vault contract address
    - amount_in_percentage: Trading amount as percentage of balance (0-100)
    - amount_in_human_readable: Trading amount in decimal format
    - slippery: Maximum slippage percentage (default: 1.0%)
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        vault: Optional[Dict] = None,
        from_token: str = None,
        to_token: str = None,
        amount_in_percentage: Optional[float] = None,
        amount_in_human_readable: Optional[float] = None,
        slippery: float = 1.0,
        **kwargs,
    ):
        # Set defaults for SwapNode metadata (but allow BuyNode to override)
        kwargs.setdefault('version', "0.0.2")
        kwargs.setdefault('display_name', "Swap Node")
        kwargs.setdefault('node_category', "instance")

        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            **kwargs,
        )

        # Extract vault information from vault object
        self.vault = vault
        self.chain = vault.get("chain", "aptos") if vault else "aptos"
        self.vault_address = vault.get("address") if vault else None
        self.vault_address = self._normalize_chain_address(self.vault_address)
        self.vault_balance = vault.get("balance") if vault else None
        self.vault_transactions = vault.get("transactions", []) if vault else []

        if self.chain not in ["aptos", "flow_evm"]:
            raise ValueError(f"Unsupported chain: {self.chain}")

        self.from_token = from_token
        self.to_token = to_token
        self.slippery = slippery

        # 🔧 Handle Switch type from frontend (v0.4.1+)
        # Frontend sends: { mode: "...", value: "100" }
        if isinstance(amount_in_human_readable, dict) and "mode" in amount_in_human_readable:
            self.amount_in_percentage = None
            self.amount_in_human_readable = None
            self._apply_amount_switch(amount_in_human_readable)
        else:
            self.amount_in_percentage = amount_in_percentage
            self.amount_in_human_readable = amount_in_human_readable
            self._validate_amount_inputs()

        # Token info
        self.input_token_address = None
        self.output_token_address = None
        self.input_token_decimals = None
        self.output_token_decimals = None
        self.tx_result = None

        # Initialize vault service
        if self.chain == "aptos":
            self.vault_service = AptosVaultService.get_instance()
        elif self.chain == "flow_evm":
            self.vault_service = FlowEvmVaultService.get_instance(545)  # Flow Testnet

        # Initialization log will be handled in execute method

    def _ensure_vault_context(self) -> None:
        """Ensure vault-derived attributes (address/chain) are populated."""
        if isinstance(self.vault, dict):
            vault_chain = self.vault.get("chain")
            if vault_chain:
                self.chain = vault_chain

            vault_addr = self._normalize_chain_address(self.vault.get("address"))
            if vault_addr:
                self.vault_address = vault_addr

        if not self.vault_address:
            raise ValueError("vault_address is required to execute swap node")
        self.vault_address = self._normalize_chain_address(self.vault_address)

    def _normalize_chain_address(self, address: Optional[str]) -> Optional[str]:
        if not address:
            return address
        addr = address.lower()
        if self.chain == "aptos":
            if addr.startswith("0x"):
                addr = addr[2:]
            addr = addr.zfill(64)
            return "0x" + addr[-64:]
        if not addr.startswith("0x"):
            addr = "0x" + addr
        return addr

    def _safe_float(self, value: Optional[float]) -> Optional[float]:
        try:
            return float(value) if value is not None and value != "" else None
        except (ValueError, TypeError):
            return None

    def _apply_amount_switch(self, switch_value: Dict[str, Any]) -> None:
        mode = (switch_value.get("mode") or "").lower()
        parsed_value = self._safe_float(switch_value.get("value"))

        if mode in FROM_FIXED_MODES:
            self.amount_direction = "from"
            self.amount_type = "fixed"
            self.amount_in_human_readable = parsed_value
            self.amount_in_percentage = None
            self.amount_target = None
        elif mode in FROM_PERCENT_MODES:
            self.amount_direction = "from"
            self.amount_type = "percent"
            self.amount_in_percentage = parsed_value
            self.amount_in_human_readable = None
            self.amount_target = None
        elif mode in TO_FIXED_MODES:
            self.amount_direction = "to"
            self.amount_type = "fixed"
            self.amount_target = parsed_value
            self.amount_in_human_readable = None
            self.amount_in_percentage = None
        elif mode in TO_PERCENT_MODES:
            self.amount_direction = "to"
            self.amount_type = "percent"
            self.amount_target = parsed_value
            self.amount_in_human_readable = None
            self.amount_in_percentage = None
        else:
            self.amount_direction = "from"
            self.amount_type = "fixed"
            self.amount_in_human_readable = parsed_value
            self.amount_in_percentage = None
            self.amount_target = None

        self._validate_amount_inputs()

    def _validate_amount_inputs(self) -> None:
        if self.amount_in_percentage is not None:
            if isinstance(self.amount_in_percentage, str):
                self.amount_in_percentage = float(self.amount_in_percentage)
            if not (0 < float(self.amount_in_percentage) <= 100):
                raise ValueError("amount_in_percentage must be between 0 and 100")

        if self.amount_in_human_readable is not None:
            if isinstance(self.amount_in_human_readable, str):
                self.amount_in_human_readable = float(self.amount_in_human_readable)
            if not (float(self.amount_in_human_readable) > 0):
                raise ValueError("amount_in_human_readable must be greater than 0")

        if self.amount_direction == "to" and self.amount_target is not None:
            if self.amount_target is None or self.amount_target <= 0:
                raise ValueError("Target amount must be greater than 0")

    async def _complete_skip(
        self,
        message: str,
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Send a unified skip receipt and mark node as completed."""
        receipt_payload = {
            "success": True,
            "skipped": True,
            "message": message,
            "chain": self.chain,
            "vault_address": self.vault_address,
            "from_token": self.from_token or "",
            "to_token": self.to_token or "",
        }
        if extra_payload:
            receipt_payload.update(extra_payload)

        await self.send_signal(
            TX_RECEIPT_HANDLE,
            SignalType.DEX_TRADE_RECEIPT,
            payload=receipt_payload,
        )
        await self.set_status(NodeStatus.COMPLETED, message)
        await self.persist_log(message, "INFO")
        return True

    def _extract_signal_value(self, value: Any, key: str) -> Optional[str]:
        if isinstance(value, Signal):
            payload = value.payload or {}
            candidate = payload.get(key)
        else:
            candidate = value
        if isinstance(candidate, str):
            candidate = candidate.strip()
        return candidate or None

    def _get_vault_holdings(self) -> List[Dict[str, Any]]:
        if isinstance(self.vault, dict):
            balance = self.vault.get("balance")
            if isinstance(balance, dict) and isinstance(balance.get("holdings"), list):
                return balance["holdings"]
            if isinstance(self.vault.get("holdings"), list):
                return self.vault["holdings"]
        return []

    def _find_vault_holding(self, token_address: Optional[str], token_symbol: Optional[str]) -> Optional[Dict[str, Any]]:
        compare_address = self._normalize_chain_address(token_address)
        token_symbol_norm = (token_symbol or "").lower()
        for holding in self._get_vault_holdings():
            holding_symbol = (holding.get("symbol") or holding.get("token_symbol") or "").lower()
            holding_address = self._normalize_chain_address(holding.get("token_address"))
            if compare_address and holding_address == compare_address:
                return holding
            if token_symbol_norm and holding_symbol == token_symbol_norm:
                return holding
        return None

    def _extract_raw_amount_from_holding(self, holding: Dict[str, Any]) -> Optional[Decimal]:
        amount_raw = holding.get("amount")
        decimals = holding.get("decimals", 0)
        amount_hr = holding.get("amount_human_readable")

        if amount_raw is not None:
            try:
                return Decimal(str(amount_raw))
            except (ValueError, TypeError):
                pass

        if amount_hr is not None and decimals is not None:
            try:
                return Decimal(str(amount_hr)) * Decimal(10) ** int(decimals)
            except (ValueError, TypeError):
                return None
        return None

    def _extract_decimal_amount_from_holding(self, holding: Dict[str, Any]) -> Optional[Decimal]:
        amount_hr = holding.get("amount_human_readable")
        if amount_hr is not None:
            try:
                return Decimal(str(amount_hr))
            except (ValueError, TypeError):
                return None
        amount_raw = holding.get("amount")
        decimals = holding.get("decimals", 0)
        if amount_raw is not None:
            try:
                return Decimal(str(amount_raw)) / (Decimal(10) ** int(decimals))
            except (ValueError, TypeError, ZeroDivisionError):
                return None
        return None

    async def _get_token_price_usd(self, token_address: str) -> Optional[Decimal]:
        normalized_address = self._normalize_chain_address(token_address)
        try:
            if self.chain == "aptos":
                price = await get_aptos_token_price_usd_async(normalized_address)
                return Decimal(str(price)) if price is not None else None
            elif self.chain == "flow_evm":
                prices = await get_flow_evm_token_prices_usd([normalized_address])
                price = prices.get(normalized_address) if prices else None
                return Decimal(str(price)) if price is not None else None
        except Exception as err:
            await self.persist_log(f"Failed to fetch price for {normalized_address}: {err}", "WARNING")
        return None

    async def _prepare_amount_from_target(self) -> None:
        if self.amount_direction != "to" or self.amount_target is None:
            return

        if not self.vault_address and isinstance(self.vault, dict):
            self.vault_address = self._normalize_chain_address(self.vault.get("address"))

        if self.amount_type == "percent":
            target_decimal = await self._calculate_target_amount_from_percent()
            if target_decimal is None:
                # Converted to from-percent fallback
                return
        else:
            target_decimal = Decimal(str(self.amount_target))

        required_input = await self._convert_target_amount_to_input(target_decimal)
        self.amount_in_human_readable = float(required_input)
        self.amount_in_percentage = None
        await self.persist_log(
            f"Target amount {target_decimal} {self.to_token} requires ~{self.amount_in_human_readable} {self.from_token}",
            "INFO",
        )

    async def _calculate_target_amount_from_percent(self) -> Optional[Decimal]:
        percent = Decimal(str(self.amount_target))
        target_holding = self._find_vault_holding(self.output_token_address, self.to_token)
        if target_holding:
            token_amount = self._extract_decimal_amount_from_holding(target_holding) or Decimal(0)
            return token_amount * percent / Decimal(100)

        input_holding = self._find_vault_holding(self.input_token_address, self.from_token)
        if input_holding:
            input_amount = self._extract_raw_amount_from_holding(input_holding)
            if input_amount is not None and input_amount > 0:
                self.amount_direction = "from"
                self.amount_type = "percent"
                self.amount_in_percentage = float(percent)
                self.amount_target = None
                self._validate_amount_inputs()
                await self.persist_log(
                    "Vault has no target token holdings; falling back to from-percent calculation based on input token.",
                    "WARNING",
                )
                return None

        raise ValueError("Cannot calculate target percentage amount – target holdings unavailable.")

    async def _convert_target_amount_to_input(self, target_amount_decimal: Decimal) -> Decimal:
        price_out = await self._get_token_price_usd(self.output_token_address)
        price_in = await self._get_token_price_usd(self.input_token_address)
        if not price_out or not price_in or price_in == 0:
            raise ValueError("Unable to fetch token prices for target conversion")

        required_input_decimal = (target_amount_decimal * price_out) / price_in
        if required_input_decimal <= 0:
            raise ValueError("Calculated required input is invalid")
        return required_input_decimal
    async def _resolve_token_addresses(self) -> None:
        """Resolve token addresses from symbols based on chain"""
        # Note: Empty value check has been moved to execute() method, here we assume token already exists

        if self.chain == "aptos":
            self.input_token_address = get_aptos_token_address_by_symbol(self.from_token)
            self.output_token_address = get_aptos_token_address_by_symbol(self.to_token)

            if not self.input_token_address or not self.output_token_address:
                raise ValueError(f"Cannot resolve Aptos token addresses for symbols: {self.from_token}, {self.to_token}")

            # Get token info for Aptos
            input_info = get_aptos_monitored_token_info(self.input_token_address)
            output_info = get_aptos_monitored_token_info(self.output_token_address)

            # If token info not found in database, try to fetch from monitor API
            if not input_info:
                input_info = await self._fetch_token_metadata_from_monitor(self.input_token_address)
            if not output_info:
                output_info = await self._fetch_token_metadata_from_monitor(self.output_token_address)

            if not input_info or not output_info:
                raise ValueError(f"Cannot get token info for Aptos tokens: {self.from_token}, {self.to_token}")

            self.input_token_decimals = input_info.get("decimals")
            self.output_token_decimals = output_info.get("decimals")
            self.input_token_address = self._normalize_chain_address(self.input_token_address)
            self.output_token_address = self._normalize_chain_address(self.output_token_address)

        elif self.chain == "flow_evm":
            # For Flow EVM, use symbols as addresses (placeholder)
            await self.persist_log("Flow EVM token symbol resolution not implemented, using symbols as addresses", "WARNING")
            self.input_token_address = self.from_token
            self.output_token_address = self.to_token
            self.input_token_decimals = 18  # Default for EVM
            self.output_token_decimals = 18

        await self.persist_log(
            f"Resolved tokens: {self.from_token}({self.input_token_address}) -> {self.to_token}({self.output_token_address})", "INFO"
        )

    async def get_token_balance(self, token_address: str, token_symbol: Optional[str] = None) -> Decimal:
        """Get token balance from vault"""
        holding = self._find_vault_holding(token_address, token_symbol)
        if holding:
            cached_amount = self._extract_raw_amount_from_holding(holding)
            if cached_amount is not None:
                await self.persist_log(
                    f"Using vault signal holdings for {token_symbol or token_address}: {cached_amount}",
                    "DEBUG",
                )
                return cached_amount

        if self.chain == "aptos":
            if not self.vault_address and isinstance(self.vault, dict):
                self.vault_address = self.vault.get("address")
            holdings_data = await self.vault_service.get_investor_holdings(self.vault_address)
            holdings = holdings_data.get("holdings", [])

            for entry in holdings:
                if entry.get("token_address", "").lower() == token_address.lower():
                    amount_raw_str = entry.get("amount", "0")
                    await self.persist_log(f"Found Aptos balance via API: {token_address}={amount_raw_str}", "INFO")
                    return Decimal(amount_raw_str)

            raise SwapSkipException(
                f"No holdings found for {token_symbol or token_address} in vault {self.vault_address}, skipping swap"
            )

        elif self.chain == "flow_evm":
            if not self.vault_address and isinstance(self.vault, dict):
                self.vault_address = self.vault.get("address")
            balance_data = await self.vault_service.get_token_balance(token_address, self.vault_address)
            balance_raw = balance_data.get("balance")
            if balance_raw is None:
                raise SwapSkipException(
                    f"No holdings found for {token_symbol or token_address} in Flow vault {self.vault_address}, skipping swap"
                )
            await self.persist_log(f"Found Flow EVM balance via API: {token_address}={balance_raw}", "INFO")
            return Decimal(balance_raw)

        raise ValueError("Unsupported chain for token balance query")

    async def get_final_amount_in(self) -> int:
        """Get final amount_in from human readable or percentage"""
        if self.amount_in_human_readable is not None and self.amount_in_human_readable > 0:
            amount_decimal = Decimal(str(self.amount_in_human_readable))
            amount_wei = int(amount_decimal * Decimal(10**self.input_token_decimals))
            await self.persist_log(f"Using human readable: {self.amount_in_human_readable} -> {amount_wei}", "INFO")
            return amount_wei

        if self.amount_in_percentage is not None:
            balance_raw = await self.get_token_balance(self.input_token_address, self.from_token)
            if balance_raw <= 0:
                raise SwapSkipException(
                    f"Vault has no spendable balance for {self.from_token}, skipping swap"
                )
            calculated_amount = int(balance_raw * Decimal(self.amount_in_percentage) / Decimal(100))
            if calculated_amount <= 0:
                raise SwapSkipException(
                    f"Calculated spend amount is zero for {self.from_token}, skipping swap"
                )
            await self.persist_log(f"Using percentage: {self.amount_in_percentage}% -> {calculated_amount}", "INFO")
            return calculated_amount

        raise ValueError("No valid amount specified")

    async def get_estimated_min_output_amount_from_pool(self, amount_in: int, slippage: float, pool_data: dict) -> int:
        """
        Use pool exchange rate to calculate estimated min output amount (no price API dependency)

        Args:
            amount_in: Input amount in wei
            slippage: Slippage tolerance percentage (e.g., 0.5 for 0.5%)
            pool_data: Pool information from monitor API

        Returns:
            int: Estimated minimum output amount after slippage
        """
        try:
            pool_info = pool_data.get("pool_info", {})
            current_sqrt_price = int(pool_info.get("sqrtPrice", 0))

            if current_sqrt_price == 0:
                pool_inner = pool_info.get("pool", {})
                current_sqrt_price = int(pool_inner.get("sqrtPrice", 0))

            if current_sqrt_price == 0:
                raise ValueError("No valid sqrtPrice found in pool data")

            await self.persist_log(f"Pool original sqrt_price: {current_sqrt_price}", "DEBUG")

            # Calculate price from sqrt_price using X64 format (confirmed by Aptos docs)
            # sqrt_price_limit: a x64 fixed-point number
            sqrt_price_x64 = Decimal(current_sqrt_price) / Decimal(2**64)
            price_x64 = sqrt_price_x64 ** 2

            await self.persist_log(f"X64 calculation: sqrt_price_raw={current_sqrt_price}, sqrt_price_x64={sqrt_price_x64}, price_x64={price_x64}", "DEBUG")

            # Use X64 format as specified in Aptos documentation
            price_decimal = price_x64

            # Determine trade direction for price conversion
            trade_direction, token0, token1 = self._determine_trade_direction(
                self.input_token_address,
                self.output_token_address
            )

            amount_in_decimal = Decimal(amount_in) / Decimal(10**self.input_token_decimals)

            # Debug detailed pool and token info
            await self.persist_log(
                f"Pool info details: "
                f"sqrt_price={current_sqrt_price}, "
                f"input_token={self.input_token_address} (decimals={self.input_token_decimals}), "
                f"output_token={self.output_token_address} (decimals={self.output_token_decimals}), "
                f"trade_direction={trade_direction}",
                "DEBUG"
            )

            # Raw X64 price calculation
            await self.persist_log(f"Raw X64 price (no decimal adjustment): {price_decimal}", "DEBUG")

            # Adjust price for token decimal differences
            # Pool price is in terms of base units, need to convert to human-readable units
            # If output has more decimals than input, we need to scale up the price
            decimal_adjustment = Decimal(10) ** (self.output_token_decimals - self.input_token_decimals)
            adjusted_price = price_decimal * decimal_adjustment

            await self.persist_log(
                f"Decimal adjustment: input_decimals={self.input_token_decimals}, "
                f"output_decimals={self.output_token_decimals}, "
                f"adjustment_factor={decimal_adjustment}, "
                f"adjusted_price={adjusted_price}",
                "DEBUG"
            )

            price_decimal = adjusted_price

            if trade_direction == "token0_to_token1":
                # token0 -> token1: use price directly
                # price = token1/token0, so token1_amount = token0_amount * price
                output_amount_decimal = amount_in_decimal * price_decimal
                await self.persist_log(f"Trade direction: token0->token1, price={price_decimal}", "DEBUG")
            else:
                # token1 -> token0: use inverse price
                # price = token1/token0, so token0_amount = token1_amount / price
                output_amount_decimal = amount_in_decimal / price_decimal
                await self.persist_log(f"Trade direction: token1->token0, inverse_price={1/price_decimal}", "DEBUG")

            # Apply slippage protection
            slippage_factor = Decimal("1") - (Decimal(str(slippage)) / Decimal("100"))
            output_amount_with_slippage = output_amount_decimal * slippage_factor
            output_amount_raw = int(output_amount_with_slippage * Decimal(10**self.output_token_decimals))

            # Safety check for uint64 overflow
            uint64_max = 2**64 - 1
            if output_amount_raw > uint64_max:
                await self.persist_log(
                    f"ERROR: Calculated amount {output_amount_raw} exceeds uint64 max {uint64_max}. "
                    f"Price calculation may be incorrect.", "ERROR"
                )
                # Try with token decimals adjustment
                await self.persist_log(
                    f"Debug: price_decimal={price_decimal}, amount_in_decimal={amount_in_decimal}, "
                    f"output_token_decimals={self.output_token_decimals}, input_token_decimals={self.input_token_decimals}", "DEBUG"
                )
                raise ValueError(f"Calculated swap amount {output_amount_raw} exceeds uint64 maximum")

            await self.persist_log(
                f"Pool-based calculation: input={amount_in_decimal} -> "
                f"output_before_slippage={output_amount_decimal} -> "
                f"output_after_{slippage}%_slippage={output_amount_raw}",
                "INFO"
            )

            return output_amount_raw

        except Exception as e:
            await self.persist_log(f"Error calculating output from pool: {str(e)}", "ERROR")
            raise ValueError(f"Cannot calculate output from pool data: {str(e)}")

    async def get_estimated_min_output_amount_aptos(self, amount_in: int, slippage: float) -> tuple[int, str]:
        """Estimate Aptos output amount and sqrt_price_limit (DEPRECATED: use pool-based calculation)"""
        input_price = await get_aptos_token_price_usd_async(self.input_token_address)
        output_price = await get_aptos_token_price_usd_async(self.output_token_address)

        if not input_price or not output_price or output_price <= 0:
            raise ValueError("Cannot get valid token prices for Aptos")

        # Calculate output amount
        amount_in_decimal = Decimal(amount_in) / Decimal(10**self.input_token_decimals)
        input_value_usd = amount_in_decimal * Decimal(str(input_price))
        output_amount_decimal = input_value_usd / Decimal(str(output_price))

        # Apply slippage
        slippage_factor = Decimal("1") - (Decimal(str(slippage)) / Decimal("100"))
        output_amount_with_slippage = output_amount_decimal * slippage_factor
        output_amount_raw = int(output_amount_with_slippage * Decimal(10**self.output_token_decimals))

        # Calculate sqrt_price_limit
        sqrt_price_limit = calculate_sqrt_price_limit_q64_64(input_price, output_price, slippage)

        await self.persist_log(f"Aptos estimation: output={output_amount_raw}, sqrt_limit={sqrt_price_limit}", "INFO")
        return output_amount_raw, sqrt_price_limit

    async def get_estimated_min_output_amount_flow_evm(self, amount_in: int, slippage: float) -> int:
        """Estimate Flow EVM output amount"""
        token_addresses = [self.input_token_address, self.output_token_address]
        prices = await get_flow_evm_token_prices_usd(token_addresses)

        input_price = prices.get(self.input_token_address)
        output_price = prices.get(self.output_token_address)

        if not input_price or not output_price or output_price <= 0:
            raise ValueError("Cannot get valid token prices for Flow EVM")

        # Calculate output amount
        amount_in_decimal = Decimal(amount_in) / Decimal(10**self.input_token_decimals)
        input_value_usd = amount_in_decimal * Decimal(str(input_price))
        output_amount_decimal = input_value_usd / Decimal(str(output_price))

        # Apply slippage
        slippage_factor = Decimal("1") - (Decimal(str(slippage)) / Decimal("100"))
        output_amount_with_slippage = output_amount_decimal * slippage_factor
        output_amount_raw = int(output_amount_with_slippage * Decimal(10**self.output_token_decimals))

        await self.persist_log(f"Flow EVM estimation: output={output_amount_raw}", "INFO")
        return output_amount_raw

    async def _fetch_token_metadata_from_monitor(self, token_address: str) -> Optional[Dict[str, any]]:
        """
        Fetch token metadata from monitor service

        Args:
            token_address: Token address

        Returns:
            Optional[Dict[str, any]]: Token metadata, format compatible with get_aptos_monitored_token_info
        """
        try:
            from services.aptos_vault_service import AptosVaultService

            vault_service = AptosVaultService.get_instance()
            metadata = await vault_service.get_token_metadata(token_address)

            if metadata:
                # Convert to format compatible with database query results
                return {
                    "token_address": metadata.get("address", token_address),
                    "name": metadata.get("name"),
                    "symbol": metadata.get("symbol"),
                    "decimals": metadata.get("decimals", 8),
                    "network": "aptos",
                    "network_type": "aptos"
                }
            return None

        except Exception as e:
            await self.persist_log(f"Failed to fetch token metadata from monitor for {token_address}: {e}", "WARNING")
            return None

    async def find_best_pool(self, token1: str, token2: str) -> Optional[Dict[str, any]]:
        """
        Dynamically search for optimal trading pool using new monitor API

        Args:
            token1: First token address
            token2: Second token address

        Returns:
            Optional[Dict]: Optimal pool info, including fee_tier
        """
        if self.chain != "aptos":
            # Currently only supports Aptos chain pool search
            return None

        try:
            monitor_url = CONFIG.get("MONITOR_URL")
            if not monitor_url:
                await self.persist_log("Monitor URL not configured, using default fee tier", "WARNING")
                return None

            # Fee tier priority: 0.05%, 0.3%, 0.01%, 1%
            fee_tiers = "1,2,0,3"

            async with httpx.AsyncClient(timeout=10.0) as client:
                url = f"{monitor_url}/aptos/pools/pair"
                params = {
                    "token1": token1,
                    "token2": token2,
                    "feeTiers": fee_tiers
                }

                await self.persist_log(f"Searching pool: {token1}/{token2} with priority tiers [{fee_tiers}]", "INFO")

                response = await client.get(url, params=params)

                if response.status_code == 200:
                    pool_data = response.json()

                    # Check if API response is successful
                    if pool_data.get("success") and pool_data.get("pool"):
                        pool_info = pool_data["pool"]
                        fee_tier = pool_info.get("feeTier")

                        # Check if pool has sufficient liquidity
                        if self._is_pool_suitable(pool_data):
                            best_pool = {
                                "pool_info": pool_data,
                                "fee_tier": fee_tier,
                                "token1": token1,
                                "token2": token2
                            }
                            await self.persist_log(
                                f"Found optimal pool: fee_tier={fee_tier}, "
                                f"TVL=${pool_data.get('tvlUSD', 'N/A')}, "
                                f"tokens={pool_info.get('token1Info', {}).get('symbol', 'N/A')}-{pool_info.get('token2Info', {}).get('symbol', 'N/A')}",
                                "INFO"
                            )
                            return best_pool
                        else:
                            await self.persist_log(f"Found pool but has insufficient liquidity: TVL=${pool_data.get('tvlUSD', 'N/A')}", "WARNING")
                    else:
                        await self.persist_log("API response indicates no pool found", "WARNING")

                elif response.status_code == 404:
                    error_data = response.json() if response.content else {}
                    await self.persist_log(f"No matching pool found: {error_data.get('error', 'Not found')}", "INFO")
                else:
                    await self.persist_log(f"Pool search failed: {response.status_code} - {response.text}", "WARNING")

        except Exception as e:
            await self.persist_log(f"Pool search failed: {e}", "ERROR")
            return None

        await self.persist_log("No suitable pool found, will use default fee_tier=1", "WARNING")
        return None

    def _is_pool_suitable(self, pool_data: Dict) -> bool:
        """
        Check if pool is suitable for trading

        Args:
            pool_data: Pool data (new API response format)

        Returns:
            bool: Whether suitable for trading
        """
        try:
            # Check basic fields
            sqrt_price = pool_data.get("sqrtPrice")
            if not sqrt_price:
                return False

            # Check liquidity: prefer tvlUSD, fallback to liquidity
            liquidity_value = 0
            if pool_data.get("tvlUSD"):
                try:
                    liquidity_value = float(pool_data.get("tvlUSD"))
                except (ValueError, TypeError):
                    liquidity_value = 0
            elif pool_data.get("liquidity"):
                try:
                    liquidity_value = float(pool_data.get("liquidity"))
                except (ValueError, TypeError):
                    liquidity_value = 0

            # Minimum liquidity requirement: $1000 USD
            if liquidity_value < 1000:
                return False

            # Check if price is reasonable
            try:
                sqrt_price_value = float(sqrt_price)
                if sqrt_price_value <= 0:
                    return False
            except (ValueError, TypeError):
                return False

            return True

        except Exception:
            return False

    def _determine_trade_direction(self, input_token: str, output_token: str) -> tuple[str, str, str]:
        """
        Determine trade direction based on token addresses (Uniswap V3 sorting rule)

        Returns:
            tuple: (trade_direction, token0, token1)
        """
        # In Uniswap V3, token0 < token1 (lexicographic order by address)
        if input_token < output_token:
            token0, token1 = input_token, output_token
            trade_direction = "token0_to_token1"  # Price goes UP
        else:
            token0, token1 = output_token, input_token
            trade_direction = "token1_to_token0"  # Price goes DOWN

        return trade_direction, token0, token1

    async def calculate_dynamic_sqrt_price_limit(self, best_pool: dict, slippage_pct: float = 5.0) -> int:
        """
        Calculate sqrt_price_limit dynamically based on pool token order and trade direction

        Args:
            best_pool: Pool data from monitor API
            slippage_pct: User's slippage tolerance percentage (e.g., 5.0 for 5%)

        Returns:
            int: Correct sqrt_price_limit for the trade
        """
        try:
            # Get current sqrt price from pool
            pool_info = best_pool.get("pool_info", {})
            current_sqrt_price = int(pool_info.get("sqrtPrice", 0))

            if current_sqrt_price == 0:
                # Fallback to pool.sqrtPrice if top-level sqrtPrice is missing
                pool_data = pool_info.get("pool", {})
                current_sqrt_price = int(pool_data.get("sqrtPrice", 0))

            if current_sqrt_price == 0:
                raise ValueError("No valid sqrtPrice found in pool data")

            await self.persist_log(f"Pool original sqrt_price: {current_sqrt_price}", "INFO")

            # Determine trade direction relative to pool order
            trade_direction, token0, token1 = self._determine_trade_direction(
                self.input_token_address,
                self.output_token_address,
            )

            import math
            slippage_decimal = slippage_pct / 100
            if slippage_decimal >= 1:
                raise ValueError("Slippage must be less than 100%")

            # Uniswap/Hyperion semantics:
            # - token0 -> token1 swaps push price DOWN, so limit must be LOWER
            # - token1 -> token0 swaps push price UP, so limit must be HIGHER
            if trade_direction == "token0_to_token1":
                sqrt_multiplier = math.sqrt(1 - slippage_decimal)
                direction = "-"
                protection_type = "token0→token1 (price decreases), lower limit"
            else:
                sqrt_multiplier = math.sqrt(1 + slippage_decimal)
                direction = "+"
                protection_type = "token1→token0 (price increases), upper limit"

            sqrt_price_limit = int(current_sqrt_price * sqrt_multiplier)

            await self.persist_log(
                f"Dynamic sqrt_price_limit: {self.input_token_address}→{self.output_token_address} "
                f"= {trade_direction} → {direction}{slippage_pct}% ({protection_type}) → "
                f"sqrt_multiplier={sqrt_multiplier:.6f} → limit={sqrt_price_limit}",
                "INFO"
            )

            return sqrt_price_limit

        except Exception as e:
            await self.persist_log(f"Failed to calculate dynamic sqrt_price_limit: {e}", "ERROR")
            # Fallback: use conservative 1% in correct direction
            fallback_multiplier = math.sqrt(1.01) if trade_direction == "token0_to_token1" else math.sqrt(0.99)
            fallback_price = int(current_sqrt_price * fallback_multiplier) if current_sqrt_price > 0 else 0
            return fallback_price

    async def execute_swap(self) -> bool:
        """Execute swap transaction based on chain"""
        try:
            self._ensure_vault_context()
            # Resolve token addresses
            await self._resolve_token_addresses()
            await self._prepare_amount_from_target()

            # Get final amount
            final_amount_in = await self.get_final_amount_in()
            if final_amount_in <= 0:
                raise SwapSkipException(
                    f"Calculated final amount is zero for {self.from_token}, skipping swap"
                )

            if self.chain == "aptos":
                # Search for optimal pool
                await self.persist_log(f"Searching for best pool: {self.input_token_address} -> {self.output_token_address}", "INFO")
                best_pool = await self.find_best_pool(self.input_token_address, self.output_token_address)

                # Determine fee_tier to use
                fee_tier = 1  # Default value
                if best_pool:
                    fee_tier = best_pool["fee_tier"]
                    await self.persist_log(f"Using pool with fee_tier={fee_tier}", "INFO")
                else:
                    await self.persist_log(f"Using default fee_tier={fee_tier}", "WARNING")

                # Aptos swap execution - use dynamic sqrt_price_limit calculation
                if best_pool:
                    # Calculate dynamic sqrt_price_limit based on trade direction and user slippage
                    sqrt_price_limit_int = await self.calculate_dynamic_sqrt_price_limit(
                        best_pool,
                        slippage_pct=self.slippery  # User input is already percentage (e.g., 0.5 for 0.5%)
                    )
                    sqrt_price_limit = str(sqrt_price_limit_int)

                    # Use pool-based calculation for estimated output (no price API dependency)
                    estimated_min_output = await self.get_estimated_min_output_amount_from_pool(
                        final_amount_in, self.slippery, best_pool
                    )
                else:
                    # Fallback to old method if no pool found
                    estimated_min_output, sqrt_price_limit = await self.get_estimated_min_output_amount_aptos(
                        final_amount_in, self.slippery
                    )

                tx_result = await self.vault_service.admin_execute_swap(
                    self.vault_address,
                    self.input_token_address,
                    self.output_token_address,
                    final_amount_in,
                    fee_tier=fee_tier,  # Use dynamically searched fee_tier
                    sqrt_price_limit=sqrt_price_limit,
                    amount_out_min=estimated_min_output,
                )

            elif self.chain == "flow_evm":
                # Flow EVM swap execution
                estimated_min_output = await self.get_estimated_min_output_amount_flow_evm(
                    final_amount_in, self.slippery
                )

                tx_result = await self.vault_service.execute_swap(
                    vault_address=self.vault_address,
                    token_in=self.input_token_address,
                    token_out=self.output_token_address,
                    amount_in=final_amount_in,
                    amount_out_min=estimated_min_output,
                )

            self.tx_result = tx_result

            if not tx_result.get("success", False):
                error_msg = tx_result.get("message", "Unknown error")
                await self.persist_log(f"Transaction failed: {error_msg}", "ERROR")
                await self.set_status(NodeStatus.FAILED, f"Transaction failed: {error_msg}")
                return False

            return True

        except SwapSkipException:
            raise
        except ValueError as e:
            error_message = str(e)
            lower_message = error_message.lower()
            if (
                "not found in aptos holdings" in lower_message
                or "not found in holdings" in lower_message
                or "no holdings" in lower_message
            ):
                raise SwapSkipException(error_message)
            raise
        except Exception as e:
            await self.persist_log(f"Error executing swap: {str(e)}", "ERROR")
            await self.persist_log(traceback.format_exc(), "ERROR")
            await self.set_status(NodeStatus.FAILED, f"Error executing swap: {str(e)}")
            self.tx_result = {"success": False, "message": str(e)}
            return False

    def prepare_trade_receipt(self) -> Dict:
        """Prepare transaction receipt signal data"""
        if not self.tx_result:
            return {"success": False, "message": "No transaction result", "chain": self.chain}

        base_receipt = {
            "success": self.tx_result.get("success", False),
            "message": self.tx_result.get("message", ""),
            "chain": self.chain,
            "vault_address": self.vault_address,
            "from_token": self.from_token,
            "to_token": self.to_token,
            "input_token_address": self.input_token_address,
            "output_token_address": self.output_token_address,
            "slippery": self.slippery,
            "raw_result": self.tx_result,
        }

        if self.chain == "aptos":
            tx_data = self.tx_result.get("transaction_result", {})
            trade_details = tx_data.get("tradeDetails", {})
            base_receipt.update({
                "tx_hash": tx_data.get("hash"),
                "status": tx_data.get("status", "Unknown"),
                "dex_name": "hyperion",
                "amount_in": trade_details.get("amountIn"),
                "amount_out": trade_details.get("amountOut"),
                "amount_out_min": trade_details.get("amountOutMin"),
                "timestamp": tx_data.get("timestamp"),
                "gas_used": tx_data.get("gasUsed"),
            })
        elif self.chain == "flow_evm":
            base_receipt.update({
                "tx_hash": self.tx_result.get("tx_hash"),
                "status": self.tx_result.get("status", "Unknown"),
                "dex_name": "flow_evm",
                "amount_in": self.tx_result.get("amount_in"),
                "amount_out": self.tx_result.get("amount_out"),
                "amount_out_min": self.tx_result.get("amount_out_min"),
                "timestamp": self.tx_result.get("timestamp"),
                "gas_used": self.tx_result.get("gas_used"),
            })

        return base_receipt

    async def _publish_trading_signal(self, trade_receipt: Dict):
        """
        Publish trading signal to MQ for co-trading system

        Args:
            trade_receipt: Transaction receipt with trade details
        """
        try:
            if not CO_TRADING_ENABLED:
                await self.persist_log("Co-trading disabled, skipping signal publish", "DEBUG")
                return

            # Only publish if transaction was successful
            if not trade_receipt.get("success", False):
                await self.persist_log("Skipping signal publish - transaction failed", "INFO")
                return

            # Prepare signal data
            signal_data = {
                "fromToken": self.from_token,
                "toToken": self.to_token,
                "amountInPercentage": self.amount_in_percentage,
                "amountInHumanReadable": self.amount_in_human_readable,
                "slippageTolerance": self.slippery,
                "chainType": self.chain
            }

            # Publish to MQ
            success = publish_trading_signal(
                flow_id=self.flow_id,
                user_id=getattr(self, 'user_id', 'unknown'),
                vault_address=self.vault_address,
                signal_type='swap',
                signal_data=signal_data,
                node_id=self.node_id,
                node_type='swap_node'
            )

            if success:
                await self.persist_log(f"Trading signal published to MQ: flow={self.flow_id}", "INFO")
            else:
                await self.persist_log("Failed to publish trading signal to MQ", "WARNING")

        except Exception as e:
            # Don't fail the node if signal publish fails
            await self.persist_log(f"Error publishing trading signal: {str(e)}", "WARNING")

    async def execute(self) -> bool:
        """Execute node logic"""
        try:
            # 🔧 NEW: Support empty value skip trade - for conditional trading control
            if not self.from_token or not self.to_token:
                return await self._complete_skip(
                    "Skipping swap: empty from_token/to_token input, no trade executed",
                    {"reason": "empty_token"},
                )

            await self.persist_log(
                f"Starting SwapNode: chain={self.chain}, {self.from_token}->{self.to_token}, vault={self.vault_address}", "INFO"
            )
            await self.set_status(NodeStatus.RUNNING)

            # Execute swap
            if not await self.execute_swap():
                return False

            # Prepare and send receipt
            trade_receipt = self.prepare_trade_receipt()
            await self.persist_log(f"Transaction receipt: {trade_receipt}", "INFO")

            if not await self.send_signal(TX_RECEIPT_HANDLE, SignalType.DEX_TRADE_RECEIPT, payload=trade_receipt):
                await self.persist_log("Failed to send receipt signal", "ERROR")
                await self.set_status(NodeStatus.FAILED, "Failed to send receipt signal")
                return False

            # Publish trading signal to MQ for co-trading system
            await self._publish_trading_signal(trade_receipt)

            await self.set_status(NodeStatus.COMPLETED)
            await self.persist_log(f"SwapNode completed: tx_hash={trade_receipt.get('tx_hash')}", "INFO")
            return True

        except asyncio.CancelledError:
            await self.persist_log("SwapNode cancelled", "INFO")
            await self.set_status(NodeStatus.TERMINATED, "Execution cancelled")
            return False
        except SwapSkipException as skip_exc:
            await self._complete_skip(str(skip_exc), {"reason": "missing_balance"})
            return True
        except Exception as e:
            error_message = f"SwapNode error: {str(e)}"
            await self.persist_log(error_message, "ERROR")
            await self.persist_log(traceback.format_exc(), "ERROR")
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

    def _register_input_handles(self) -> None:
        """Register input handles"""
        # Register vault input (unified)
        self.register_input_handle(
            name=VAULT_HANDLE,
            data_type=dict,
            description="Vault - Complete vault information from Vault Node",
            example={
                "chain": "aptos",
                "address": "0x6a1a233...",
                "balance": {...},
                "transactions": [...]
            },
            auto_update_attr="vault",
        )
        self.register_input_handle(
            name=FROM_TOKEN_HANDLE,
            data_type=str,
            description="From Token - Source token symbol (e.g., 'USDT')",
            example="USDT",
            auto_update_attr="from_token",
        )
        self.register_input_handle(
            name=TO_TOKEN_HANDLE,
            data_type=str,
            description="To Token - Target token symbol (e.g., 'BTC')",
            example="BTC",
            auto_update_attr="to_token",
        )
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE_PERCENTAGE,
            data_type=float,
            description="Percentage In - Trading amount as percentage of current balance (0-100)",
            example=25.0,
            auto_update_attr="amount_in_percentage",
        )
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE_HUMAN_READABLE,
            data_type=float,
            description="Amount In - Trading amount in human-readable decimal format",
            example=100.0,
            auto_update_attr="amount_in_human_readable",
        )
        self.register_input_handle(
            name=SLIPPAGE_TOLERANCE_HANDLE,
            data_type=float,
            description="Slippery Tolerance - Maximum acceptable slippage percentage",
            example=1.0,
            auto_update_attr="slippery",
        )

    def _register_output_handles(self) -> None:
        """Register output handles"""
        self.register_output_handle(
            name=TX_RECEIPT_HANDLE,
            data_type=dict,
            description="Trade Receipt - Transaction receipt from swap operation",
            example={"tx_hash": "0x123...", "status": "success", "amount_out": "1000000"},
        )


# ============ Instance Node Classes ============

@register_node_type(
    "buy_node",
    default_params={
        "vault": None,
        "buy_token": None,
        "base_token": None,
        # "order_type": "market",  # COMMENTED: Not in frontend
        # "limited_price": None,  # COMMENTED: Not in frontend
        "amount_in_percentage": None,
        "amount_in_human_readable": None,
        "slippery": 1.0,
    },
)
class BuyNode(SwapNode):
    """
    Buy Node - Specialized node instance for buying tokens

    Input parameters:
    - buy_token: Token symbol to buy (string)
    - base_token: Base token symbol for payment (string)
    - chain: Blockchain network (string)
    - vault_address: Vault contract address (string)
    # - order_type: Order type (string) - "market" or "limit" [COMMENTED: Not in frontend]
    # - limited_price: Limit price (number) - only for limit orders [COMMENTED: Not in frontend]
    - amount_in_percentage: Trading amount percentage (number)
    - amount_in_human_readable: Human readable amount (number)
    - slippery: Slippage tolerance (number)

    Output signals:
    - trade_receipt: Transaction receipt (json object)
    """

    def __init__(self, **kwargs):
        # Set buy logic: from_token = base_token, to_token = buy_token
        buy_token = kwargs.get('buy_token')
        base_token = kwargs.get('base_token')

        if buy_token:
            kwargs['to_token'] = buy_token
        if base_token:
            kwargs['from_token'] = base_token

        # Set instance node metadata
        kwargs.setdefault('version', '0.0.2')
        kwargs.setdefault('display_name', 'Buy Node')
        kwargs.setdefault('node_category', 'instance')
        kwargs.setdefault('base_node_type', 'swap_node')
        kwargs.setdefault('description', 'Specialized node for buying tokens')
        kwargs.setdefault('author', 'TradingFlow Team')
        kwargs.setdefault('tags', ['trading', 'buy', 'dex'])

        super().__init__(**kwargs)

        # Save buy specific parameters
        self.buy_token = buy_token
        self.base_token = base_token
        # self.order_type = kwargs.get('order_type', 'market')  # COMMENTED: Not in frontend
        # self.limited_price = kwargs.get('limited_price')  # COMMENTED: Not in frontend

        # Reset logger name
        # Logger removed - using persist_log from NodeBase

    def _register_input_handles(self) -> None:
        """Register buy node specialized input handles"""
        # Register vault input (unified)
        self.register_input_handle(
            name=VAULT_HANDLE,
            data_type=dict,
            description="Vault - Complete vault information from Vault Node",
            example={
                "chain": "aptos",
                "address": "0x6a1a233...",
                "balance": {...},
                "transactions": [...]
            },
            auto_update_attr="vault",
        )
        self.register_input_handle(
            name="buy_token",
            data_type=str,
            description="Token to Buy - Target token symbol to purchase",
            example="BTC",
            auto_update_attr=None,
        )
        self.register_input_handle(
            name="base_token",
            data_type=str,
            description="With Token - Base token symbol used for payment",
            example="USDT",
            auto_update_attr=None,
        )
        # COMMENTED: Not in frontend
        # self.register_input_handle(
        #     name="order_type",
        #     data_type=str,
        #     description="Order Type - Order execution type ('market' or 'limit')",
        #     example="market",
        #     auto_update_attr="order_type",
        # )
        # self.register_input_handle(
        #     name="limited_price",
        #     data_type=float,
        #     description="Limited Price - Maximum price for limit orders",
        #     example=50000.0,
        #     auto_update_attr="limited_price",
        # )
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE_PERCENTAGE,
            data_type=float,
            description="Percentage In - Trading amount as percentage of base token balance (0-100)",
            example=25.0,
            auto_update_attr="amount_in_percentage",
        )
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE_HUMAN_READABLE,
            data_type=float,
            description="Amount In - Trading amount in human-readable decimal format",
            example=100.0,
            auto_update_attr="amount_in_human_readable",
        )
        self.register_input_handle(
            name=SLIPPAGE_TOLERANCE_HANDLE,
            data_type=float,
            description="Slippery Tolerance - Maximum acceptable slippage percentage",
            example=1.0,
            auto_update_attr="slippery",
        )

    async def _on_buy_token_received(self, buy_token: str) -> None:
        token_value = self._extract_signal_value(buy_token, "buy_token")
        self.buy_token = token_value or ""
        self.to_token = token_value
        await self.persist_log(f"Buy token updated to: {self.to_token}", "INFO")

    async def _on_base_token_received(self, base_token: str) -> None:
        token_value = self._extract_signal_value(base_token, "base_token")
        self.base_token = token_value or ""
        self.from_token = token_value
        await self.persist_log(f"Base token updated to: {self.from_token}", "INFO")


@register_node_type(
    "sell_node",
    default_params={
        "vault": None,
        "sell_token": None,
        "base_token": None,
        # "order_type": "market",  # COMMENTED: Not in frontend
        # "limited_price": None,  # COMMENTED: Not in frontend
        "amount_in_percentage": None,
        "amount_in_human_readable": None,
        "slippery": 1.0,
    },
)
class SellNode(SwapNode):
    """
    Sell Node - Specialized node instance for selling tokens

    Input parameters:
    - sell_token: Token symbol to sell (string)
    - base_token: Base token symbol to receive (string)
    - chain: Blockchain network (string)
    - vault_address: Vault contract address (string)
    # - order_type: Order type (string) - "market" or "limit" [COMMENTED: Not in frontend]
    # - limited_price: Limit price (number) - only for limit orders [COMMENTED: Not in frontend]
    - amount_in_percentage: Trading amount percentage (number)
    - amount_in_human_readable: Human readable amount (number)
    - slippery: Slippage tolerance (number)

    Output signals:
    - trade_receipt: Transaction receipt (json object)
    """

    def __init__(self, **kwargs):
        # Set sell logic: from_token = sell_token, to_token = base_token
        sell_token = kwargs.get('sell_token')
        base_token = kwargs.get('base_token')

        if sell_token:
            kwargs['from_token'] = sell_token
        if base_token:
            kwargs['to_token'] = base_token

        # Set instance node metadata
        kwargs.setdefault('version', '0.0.2')
        kwargs.setdefault('display_name', 'Sell Node')
        kwargs.setdefault('node_category', 'instance')
        kwargs.setdefault('base_node_type', 'swap_node')
        kwargs.setdefault('description', 'Specialized node for selling tokens')
        kwargs.setdefault('author', 'TradingFlow Team')
        kwargs.setdefault('tags', ['trading', 'sell', 'dex'])

        super().__init__(**kwargs)

        # Save sell specific parameters
        self.sell_token = sell_token
        self.base_token = base_token
        # self.order_type = kwargs.get('order_type', 'market')  # COMMENTED: Not in frontend
        # self.limited_price = kwargs.get('limited_price')  # COMMENTED: Not in frontend

        # Logger removed - using persist_log from NodeBase

    def _register_input_handles(self) -> None:
        """Register sell node specialized input handles"""
        # Register vault input (unified)
        self.register_input_handle(
            name=VAULT_HANDLE,
            data_type=dict,
            description="Vault - Complete vault information from Vault Node",
            example={
                "chain": "aptos",
                "address": "0x6a1a233...",
                "balance": {...},
                "transactions": [...]
            },
            auto_update_attr="vault",
        )
        self.register_input_handle(
            name="sell_token",
            data_type=str,
            description="Token to Sell - Source token symbol to sell",
            example="BTC",
            auto_update_attr=None,
        )
        self.register_input_handle(
            name="base_token",
            data_type=str,
            description="With Token - Base token symbol to receive",
            example="USDT",
            auto_update_attr=None,
        )
        # COMMENTED: Not in frontend
        # self.register_input_handle(
        #     name="order_type",
        #     data_type=str,
        #     description="Order Type - Order execution type ('market' or 'limit')",
        #     example="market",
        #     auto_update_attr="order_type",
        # )
        # self.register_input_handle(
        #     name="limited_price",
        #     data_type=float,
        #     description="Limited Price - Minimum price for limit orders",
        #     example=45000.0,
        #     auto_update_attr="limited_price",
        # )
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE_PERCENTAGE,
            data_type=float,
            description="Percentage In - Trading amount as percentage of sell token balance (0-100)",
            example=25.0,
            auto_update_attr="amount_in_percentage",
        )
        self.register_input_handle(
            name=AMOUNT_IN_HANDLE_HUMAN_READABLE,
            data_type=float,
            description="Amount In - Trading amount in human-readable decimal format",
            example=0.5,
            auto_update_attr="amount_in_human_readable",
        )
        self.register_input_handle(
            name=SLIPPAGE_TOLERANCE_HANDLE,
            data_type=float,
            description="Slippery Tolerance - Maximum acceptable slippage percentage",
            example=1.0,
            auto_update_attr="slippery",
        )

    async def _on_sell_token_received(self, sell_token: str) -> None:
        token_value = self._extract_signal_value(sell_token, "sell_token")
        self.sell_token = token_value or ""
        self.from_token = token_value
        await self.persist_log(f"Sell token updated to: {self.from_token}", "INFO")

    async def _on_base_token_received(self, base_token: str) -> None:
        token_value = self._extract_signal_value(base_token, "base_token")
        self.base_token = token_value or ""
        self.to_token = token_value
        await self.persist_log(f"Base token updated to: {self.to_token}", "INFO")
