import asyncio
# Removed logging import - using persist_log from NodeBase
import traceback
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple

import httpx

from tradingflow.station.services.aptos_vault_service import AptosVaultService
from tradingflow.station.services.flow_evm_vault_service import FlowEvmVaultService
from tradingflow.station.utils.token_price_util import (
    get_aptos_monitored_token_info,
    get_aptos_token_address_by_symbol,
    get_aptos_token_price_usd_async,
    get_flow_evm_token_prices_usd,
)
from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus
from tradingflow.depot.python.config import CONFIG

# input handles
FROM_TOKEN_HANDLE = "from_token"
TO_TOKEN_HANDLE = "to_token"
AMOUNT_IN_HANDLE_HUMAN_READABLE = "amount_in_human_readable"
AMOUNT_IN_HANDLE_PERCENTAGE = "amount_in_percentage"
VAULT_ADDRESS_HANDLE = "vault_address"
SLIPPAGE_TOLERANCE_HANDLE = "slippery"
CHAIN_HANDLE = "chain"
TX_RECEIPT_HANDLE = "trade_receipt"

getcontext().prec = 50


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
        "chain": "aptos",
        "from_token": None,
        "to_token": None,
        "vault_address": None,
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
        chain: str = "aptos",
        from_token: str = None,
        to_token: str = None,
        vault_address: str = None,
        amount_in_percentage: Optional[float] = None,
        amount_in_human_readable: Optional[float] = None,
        slippery: float = 1.0,
        **kwargs,
    ):
        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            version="0.0.2",
            display_name="Swap Node",
            node_category="instance",
            **kwargs,
        )

        if chain not in ["aptos", "flow_evm"]:
            raise ValueError(f"Unsupported chain: {chain}")

        self.chain = chain
        self.from_token = from_token
        self.to_token = to_token
        self.vault_address = vault_address
        self.slippery = slippery
        self.amount_in_percentage = amount_in_percentage
        self.amount_in_human_readable = amount_in_human_readable

        # Convert and validate inputs
        if self.amount_in_percentage is not None:
            # Convert to float if it's a string
            if isinstance(self.amount_in_percentage, str):
                self.amount_in_percentage = float(self.amount_in_percentage)
            if not (0 < self.amount_in_percentage <= 100):
                raise ValueError("amount_in_percentage must be between 0 and 100")

        if self.amount_in_human_readable is not None:
            # Convert to float if it's a string
            if isinstance(self.amount_in_human_readable, str):
                self.amount_in_human_readable = float(self.amount_in_human_readable)
            if not (self.amount_in_human_readable > 0):
                raise ValueError("amount_in_human_readable must be greater than 0")

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

    async def _resolve_token_addresses(self) -> None:
        """Resolve token addresses from symbols based on chain"""
        if not self.from_token or not self.to_token:
            raise ValueError("Both from_token and to_token must be provided")

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

    async def get_token_balance(self, token_address: str) -> Decimal:
        """Get token balance from vault"""
        if self.chain == "aptos":
            holdings_data = await self.vault_service.get_investor_holdings(self.vault_address)
            holdings = holdings_data.get("holdings", [])

            for holding in holdings:
                if holding.get("token_address", "").lower() == token_address.lower():
                    amount_raw_str = holding.get("amount", "0")
                    await self.persist_log(f"Found Aptos balance: {token_address}={amount_raw_str}", "INFO")
                    return Decimal(amount_raw_str)

            raise ValueError(f"Token {token_address} not found in Aptos holdings")

        elif self.chain == "flow_evm":
            balance_data = await self.vault_service.get_token_balance(token_address, self.vault_address)
            balance_raw = balance_data.get("balance", "0")
            await self.persist_log(f"Found Flow EVM balance: {token_address}={balance_raw}", "INFO")
            return Decimal(balance_raw)

    async def get_final_amount_in(self) -> int:
        """Get final amount_in from human readable or percentage"""
        if self.amount_in_human_readable is not None and self.amount_in_human_readable > 0:
            amount_decimal = Decimal(str(self.amount_in_human_readable))
            amount_wei = int(amount_decimal * Decimal(10**self.input_token_decimals))
            await self.persist_log(f"Using human readable: {self.amount_in_human_readable} -> {amount_wei}", "INFO")
            return amount_wei

        if self.amount_in_percentage is not None:
            balance_raw = await self.get_token_balance(self.input_token_address)
            calculated_amount = int(balance_raw * Decimal(self.amount_in_percentage) / Decimal(100))
            await self.persist_log(f"Using percentage: {self.amount_in_percentage}% -> {calculated_amount}", "INFO")
            return calculated_amount

        raise ValueError("No valid amount specified")

    async def get_estimated_min_output_amount_aptos(self, amount_in: int, slippage: float) -> tuple[int, str]:
        """估算 Aptos 输出金额和 sqrt_price_limit"""
        input_price = await get_aptos_token_price_usd_async(self.input_token_address)
        output_price = await get_aptos_token_price_usd_async(self.output_token_address)

        if not input_price or not output_price or output_price <= 0:
            raise ValueError("Cannot get valid token prices for Aptos")

        # 计算输出金额
        amount_in_decimal = Decimal(amount_in) / Decimal(10**self.input_token_decimals)
        input_value_usd = amount_in_decimal * Decimal(str(input_price))
        output_amount_decimal = input_value_usd / Decimal(str(output_price))

        # 应用滑点
        slippage_factor = Decimal("1") - (Decimal(str(slippage)) / Decimal("100"))
        output_amount_with_slippage = output_amount_decimal * slippage_factor
        output_amount_raw = int(output_amount_with_slippage * Decimal(10**self.output_token_decimals))

        # 计算 sqrt_price_limit
        sqrt_price_limit = calculate_sqrt_price_limit_q64_64(input_price, output_price, slippage)

        await self.persist_log(f"Aptos estimation: output={output_amount_raw}, sqrt_limit={sqrt_price_limit}", "INFO")
        return output_amount_raw, sqrt_price_limit

    async def get_estimated_min_output_amount_flow_evm(self, amount_in: int, slippage: float) -> int:
        """估算 Flow EVM 输出金额"""
        token_addresses = [self.input_token_address, self.output_token_address]
        prices = await get_flow_evm_token_prices_usd(token_addresses)

        input_price = prices.get(self.input_token_address)
        output_price = prices.get(self.output_token_address)

        if not input_price or not output_price or output_price <= 0:
            raise ValueError("Cannot get valid token prices for Flow EVM")

        # 计算输出金额
        amount_in_decimal = Decimal(amount_in) / Decimal(10**self.input_token_decimals)
        input_value_usd = amount_in_decimal * Decimal(str(input_price))
        output_amount_decimal = input_value_usd / Decimal(str(output_price))

        # 应用滑点
        slippage_factor = Decimal("1") - (Decimal(str(slippage)) / Decimal("100"))
        output_amount_with_slippage = output_amount_decimal * slippage_factor
        output_amount_raw = int(output_amount_with_slippage * Decimal(10**self.output_token_decimals))

        await self.persist_log(f"Flow EVM estimation: output={output_amount_raw}", "INFO")
        return output_amount_raw

    async def _fetch_token_metadata_from_monitor(self, token_address: str) -> Optional[Dict[str, any]]:
        """
        从monitor服务获取代币元数据

        Args:
            token_address: 代币地址

        Returns:
            Optional[Dict[str, any]]: 代币元数据，格式与get_aptos_monitored_token_info兼容
        """
        try:
            from tradingflow.station.services.aptos_vault_service import AptosVaultService

            vault_service = AptosVaultService.get_instance()
            metadata = await vault_service.get_token_metadata(token_address)

            if metadata:
                # 转换为与数据库查询结果兼容的格式
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
        动态搜索最优的交易池子，使用新版monitor API

        Args:
            token1: 第一个代币地址
            token2: 第二个代币地址

        Returns:
            Optional[Dict]: 最优池子信息，包含fee_tier
        """
        if self.chain != "aptos":
            # 目前只支持Aptos链的池子搜索
            return None

        try:
            monitor_url = CONFIG.get("MONITOR_URL")
            if not monitor_url:
                await self.persist_log("Monitor URL not configured, using default fee tier", "WARNING")
                return None

            # 费率等级优先级: 0.05%, 0.3%, 0.01%, 1%
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

                    # 检查API返回是否成功
                    if pool_data.get("success") and pool_data.get("pool"):
                        pool_info = pool_data["pool"]
                        fee_tier = pool_info.get("feeTier")

                        # 检查池子是否有足够的流动性
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
        检查池子是否适合交易

        Args:
            pool_data: 池子数据（新版API响应格式）

        Returns:
            bool: 是否适合交易
        """
        try:
            # 检查基本字段
            sqrt_price = pool_data.get("sqrtPrice")
            if not sqrt_price:
                return False

            # 检查流动性：优先使用tvlUSD，其次使用liquidity
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

            # 最小流动性要求：$1000 USD
            if liquidity_value < 1000:
                return False

            # 检查价格是否合理
            try:
                sqrt_price_value = float(sqrt_price)
                if sqrt_price_value <= 0:
                    return False
            except (ValueError, TypeError):
                return False

            return True

        except Exception:
            return False

    def calculate_sqrt_price_limit_from_pool(self, best_pool: dict, is_buy: bool = True, slippage_pct: float = 5.0) -> int:
        """
        Calculate sqrt_price_limit based on pool's current price and trade direction

        Args:
            best_pool: Pool data from monitor API
            is_buy: True for buying token2, False for selling token2
            slippage_pct: Slippage tolerance percentage

        Returns:
            int: Appropriate sqrt_price_limit for the trade
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

            # Calculate slippage multiplier
            slippage_multiplier = 1 + (slippage_pct / 100)

            if is_buy:
                # Buying: allow price to go up by slippage %
                sqrt_price_limit = int(current_sqrt_price * slippage_multiplier)
            else:
                # Selling: allow price to go down by slippage %
                sqrt_price_limit = int(current_sqrt_price / slippage_multiplier)

            return sqrt_price_limit

        except Exception as e:
            # Return reasonable default (current price ± 10%)
            fallback_multiplier = 1.1 if is_buy else 0.9
            fallback_price = int(current_sqrt_price * fallback_multiplier) if current_sqrt_price > 0 else 0
            return fallback_price

    async def execute_swap(self) -> bool:
        """Execute swap transaction based on chain"""
        try:
            # Resolve token addresses
            await self._resolve_token_addresses()

            # Get final amount
            final_amount_in = await self.get_final_amount_in()

            if self.chain == "aptos":
                # 搜索最优池子
                await self.persist_log(f"Searching for best pool: {self.input_token_address} -> {self.output_token_address}", "INFO")
                best_pool = await self.find_best_pool(self.input_token_address, self.output_token_address)

                # 确定使用的fee_tier
                fee_tier = 1  # 默认值
                if best_pool:
                    fee_tier = best_pool["fee_tier"]
                    await self.persist_log(f"Using pool with fee_tier={fee_tier}", "INFO")
                else:
                    await self.persist_log(f"Using default fee_tier={fee_tier}", "WARNING")

                # Aptos swap execution - test different sqrt_price_limit approaches
                if best_pool:
                    pool_info = best_pool.get("pool_info", {})
                    current_sqrt_price = int(pool_info.get("sqrtPrice", "0"))

                    # Test Option 4: Current price - 5% (lower bound for selling)
                    sqrt_price_limit = str(int(current_sqrt_price * 0.95))

                    # Other options to test:
                    # Option 1: sqrt_price_limit = str(int(current_sqrt_price * 1.01))  # +1%
                    # Option 2: sqrt_price_limit = str(int(current_sqrt_price * 0.99))  # -1%
                    # Option 3: sqrt_price_limit = str(int(current_sqrt_price * 1.05))  # +5%
                    # Option 5: sqrt_price_limit = str(current_sqrt_price)              # exact

                    await self.persist_log(
                        f"Testing sqrt_price_limit: current={current_sqrt_price}, limit={sqrt_price_limit} (-5%)",
                        "INFO"
                    )

                    # Still need estimated output for amount_out_min
                    estimated_min_output, _ = await self.get_estimated_min_output_amount_aptos(
                        final_amount_in, self.slippery
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
                    fee_tier=fee_tier,  # 使用动态搜索的fee_tier
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

    async def execute(self) -> bool:
        """Execute node logic"""
        try:
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

            await self.set_status(NodeStatus.COMPLETED)
            await self.persist_log(f"SwapNode completed: tx_hash={trade_receipt.get('tx_hash')}", "INFO")
            return True

        except asyncio.CancelledError:
            await self.persist_log("SwapNode cancelled", "INFO")
            await self.set_status(NodeStatus.TERMINATED, "Execution cancelled")
            return False
        except Exception as e:
            error_message = f"SwapNode error: {str(e)}"
            await self.persist_log(error_message, "ERROR")
            await self.persist_log(traceback.format_exc(), "ERROR")
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

    def _register_input_handles(self) -> None:
        """注册输入句柄"""
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
            name=CHAIN_HANDLE,
            data_type=str,
            description="Chain - Blockchain network ('aptos' or 'flow_evm')",
            example="aptos",
            auto_update_attr="chain",
        )
        self.register_input_handle(
            name=VAULT_ADDRESS_HANDLE,
            data_type=str,
            description="Vault Address - Vault contract address",
            example="0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
            auto_update_attr="vault_address",
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


# ============ 实例节点类 ============

@register_node_type(
    "buy_node",
    default_params={
        "chain": "aptos",
        "buy_token": None,
        "base_token": None,
        "vault_address": None,
        "order_type": "market",
        "limited_price": None,
        "amount_in_percentage": None,
        "amount_in_human_readable": None,
        "slippery": 1.0,
    },
)
class BuyNode(SwapNode):
    """
    Buy Node - 专门用于买入代币的节点实例

    输入参数:
    - buy_token: 要买入的代币符号 (string)
    - base_token: 用于支付的基础代币符号 (string)
    - chain: 区块链网络 (string)
    - vault_address: Vault合约地址 (string)
    - order_type: 订单类型 (string) - "market" 或 "limit"
    - limited_price: 限价 (number) - 仅限价单使用
    - amount_in_percentage: 交易金额百分比 (number)
    - amount_in_human_readable: 人类可读金额 (number)
    - slippery: 滑点容忍度 (number)

    输出信号:
    - trade_receipt: 交易收据 (json object)
    """

    def __init__(self, **kwargs):
        # 设置买入逻辑：from_token = base_token, to_token = buy_token
        buy_token = kwargs.get('buy_token')
        base_token = kwargs.get('base_token')

        if buy_token:
            kwargs['to_token'] = buy_token
        if base_token:
            kwargs['from_token'] = base_token

        # 设置实例节点元数据
        kwargs.setdefault('version', '0.0.2')
        kwargs.setdefault('display_name', 'Buy Node')
        kwargs.setdefault('node_category', 'instance')
        kwargs.setdefault('base_node_type', 'swap_node')
        kwargs.setdefault('description', 'Specialized node for buying tokens')
        kwargs.setdefault('author', 'TradingFlow Team')
        kwargs.setdefault('tags', ['trading', 'buy', 'dex'])

        super().__init__(**kwargs)

        # 保存买入特定参数
        self.buy_token = buy_token
        self.base_token = base_token
        self.order_type = kwargs.get('order_type', 'market')
        self.limited_price = kwargs.get('limited_price')

        # 重新设置日志名称
        # Logger removed - using persist_log from NodeBase

    def _register_input_handles(self) -> None:
        """注册买入节点特化的输入句柄"""
        self.register_input_handle(
            name="buy_token",
            data_type=str,
            description="Token to Buy - Target token symbol to purchase",
            example="BTC",
            auto_update_attr="buy_token",
        )
        self.register_input_handle(
            name="base_token",
            data_type=str,
            description="With Token - Base token symbol used for payment",
            example="USDT",
            auto_update_attr="base_token",
        )
        self.register_input_handle(
            name=CHAIN_HANDLE,
            data_type=str,
            description="Chain - Blockchain network ('aptos' or 'flow_evm')",
            example="aptos",
            auto_update_attr="chain",
        )
        self.register_input_handle(
            name=VAULT_ADDRESS_HANDLE,
            data_type=str,
            description="Vault Address - Vault contract address",
            example="0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
            auto_update_attr="vault_address",
        )
        self.register_input_handle(
            name="order_type",
            data_type=str,
            description="Order Type - Order execution type ('market' or 'limit')",
            example="market",
            auto_update_attr="order_type",
        )
        self.register_input_handle(
            name="limited_price",
            data_type=float,
            description="Limited Price - Maximum price for limit orders",
            example=50000.0,
            auto_update_attr="limited_price",
        )
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
        """处理买入代币更新"""
        await self.persist_log(f"Received buy token: {buy_token}", "INFO")
        self.buy_token = buy_token
        self.to_token = buy_token

    async def _on_base_token_received(self, base_token: str) -> None:
        """Handle base token update"""
        await self.persist_log(f"Received base token: {base_token}", "INFO")
        self.base_token = base_token
        self.from_token = base_token


@register_node_type(
    "sell_node",
    default_params={
        "chain": "aptos",
        "sell_token": None,
        "base_token": None,
        "vault_address": None,
        "order_type": "market",
        "limited_price": None,
        "amount_in_percentage": None,
        "amount_in_human_readable": None,
        "slippery": 1.0,
    },
)
class SellNode(SwapNode):
    """
    Sell Node - 专门用于卖出代币的节点实例

    输入参数:
    - sell_token: 要卖出的代币符号 (string)
    - base_token: 换取的基础代币符号 (string)
    - chain: 区块链网络 (string)
    - vault_address: Vault合约地址 (string)
    - order_type: 订单类型 (string) - "market" 或 "limit"
    - limited_price: 限价 (number) - 仅限价单使用
    - amount_in_percentage: 交易金额百分比 (number)
    - amount_in_human_readable: 人类可读金额 (number)
    - slippery: 滑点容忍度 (number)

    输出信号:
    - trade_receipt: 交易收据 (json object)
    """

    def __init__(self, **kwargs):
        # 设置卖出逻辑：from_token = sell_token, to_token = base_token
        sell_token = kwargs.get('sell_token')
        base_token = kwargs.get('base_token')

        if sell_token:
            kwargs['from_token'] = sell_token
        if base_token:
            kwargs['to_token'] = base_token

        # 设置实例节点元数据
        kwargs.setdefault('version', '0.0.2')
        kwargs.setdefault('display_name', 'Sell Node')
        kwargs.setdefault('node_category', 'instance')
        kwargs.setdefault('base_node_type', 'swap_node')
        kwargs.setdefault('description', 'Specialized node for selling tokens')
        kwargs.setdefault('author', 'TradingFlow Team')
        kwargs.setdefault('tags', ['trading', 'sell', 'dex'])

        super().__init__(**kwargs)

        # 保存卖出特定参数
        self.sell_token = sell_token
        self.base_token = base_token
        self.order_type = kwargs.get('order_type', 'market')
        self.limited_price = kwargs.get('limited_price')

        # Logger removed - using persist_log from NodeBase

    def _register_input_handles(self) -> None:
        """注册卖出节点特化的输入句柄"""
        self.register_input_handle(
            name="sell_token",
            data_type=str,
            description="Token to Sell - Source token symbol to sell",
            example="BTC",
            auto_update_attr="sell_token",
        )
        self.register_input_handle(
            name="base_token",
            data_type=str,
            description="With Token - Base token symbol to receive",
            example="USDT",
            auto_update_attr="base_token",
        )
        self.register_input_handle(
            name=CHAIN_HANDLE,
            data_type=str,
            description="Chain - Blockchain network ('aptos' or 'flow_evm')",
            example="aptos",
            auto_update_attr="chain",
        )
        self.register_input_handle(
            name=VAULT_ADDRESS_HANDLE,
            data_type=str,
            description="Vault Address - Vault contract address",
            example="0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
            auto_update_attr="vault_address",
        )
        self.register_input_handle(
            name="order_type",
            data_type=str,
            description="Order Type - Order execution type ('market' or 'limit')",
            example="market",
            auto_update_attr="order_type",
        )
        self.register_input_handle(
            name="limited_price",
            data_type=float,
            description="Limited Price - Minimum price for limit orders",
            example=45000.0,
            auto_update_attr="limited_price",
        )
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
        """Handle sell token update"""
        await self.persist_log(f"Received sell token: {sell_token}", "INFO")
        self.sell_token = sell_token
        self.from_token = sell_token

    async def _on_base_token_received(self, base_token: str) -> None:
        """Handle base token update"""
        await self.persist_log(f"Received base token: {base_token}", "INFO")
        self.base_token = base_token
        self.to_token = base_token
