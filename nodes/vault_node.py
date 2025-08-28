import asyncio
import traceback
from typing import Dict, Optional

from tradingflow.station.services.aptos_vault_service import AptosVaultService
from tradingflow.station.services.flow_evm_vault_service import FlowEvmVaultService
from tradingflow.station.utils.token_price_util import get_aptos_token_price_usd
from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

# input handles
CHAIN_HANDLE = "chain"
CHAIN_ID_HANDLE = "chain_id"
VAULT_ADDRESS_HANDLE = "vault_address"
# output handles
BALANCE_HANDLE = "vault_balance"


@register_node_type(
    "vault_node",
    default_params={
        "chain": "aptos",  # Supports "aptos" or "flow_evm"
        "chain_id": None,  # Flow EVM chain ID, such as 545 (Flow testnet)
        "vault_address": None,  # User address, required parameter
    },
)
class VaultNode(NodeBase):
    """
    Vault Node - Query user holdings information in TradingFlow Vault

    This node is responsible for querying the asset holdings of specified users in the TradingFlow Vault system.
    Supports multi-chain queries, including Aptos and Flow EVM.

    Input parameters:
    - chain: Blockchain network name (supports "aptos" or "flow_evm")
    - chain_id: Flow EVM chain ID (only required for Flow EVM, default 545)
    - vault_address: User's wallet address or vault address

    Output signals:
    - VAULT_HOLDINGS: Signal containing complete user holdings information

    Holdings information includes:
    - holdings: List of held tokens, each containing token address, amount, value and other information
    - total_value_usd: Total holdings value (USD)
    - timestamp: Query timestamp
    - vault_address: Queried user address

    Example configuration:
    ```python
    # Aptos configuration
    node = VaultNode(
        chain="aptos",
        vault_address="0x6a1a233..."
    )

    # Flow EVM configuration
    node = VaultNode(
        chain="flow_evm",
        chain_id=545,  # Flow testnet
        vault_address="0x1234..."
    )

    # Receive parameters through signals
    node = VaultNode(
        chain="flow_evm"
        # chain_id and vault_address will be received through signals
    )
    ```

    Output data format:
    ```python
    {
        "success": True,
        "vault_address": "0x6a1a233...",
        "chain": "aptos",
        "holdings": [
            {
                "token_address": "0xa",
                "amount": "1000000000",
                "symbol": "APT",
                "decimals": 8,
                "price_usd": 5.23,
                "value_usd": 52.30
            },
            ...
        ],
        "total_value_usd": 1234.56,
        "timestamp": "2024-01-01T12:00:00Z",
        "raw_result": {...}  # Original query result
    }
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
        chain_id: Optional[int] = None,
        vault_address: Optional[str] = None,
        **kwargs,
    ):
        """Initialize Vault Node"""
        # Initialize base node
        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            **kwargs,
        )

        # Configuration
        self.chain = chain
        self.chain_id = chain_id
        self.vault_address = vault_address

        # Validate chain
        if self.chain not in ["aptos", "flow_evm"]:
            # Cannot use await in __init__, validation error will be logged in execute method
            raise ValueError(
                f"Unsupported chain: {self.chain}. Supported chains: 'aptos', 'flow_evm'"
            )

        # Validate chain_id for flow_evm
        if self.chain == "flow_evm" and self.chain_id is None:
            # Get vault information to Flow testnet
            self.chain_id = 545
            # Warning log will be handled in execute method

        # Query result
        self.holdings_result = None

        # Initialize vault service based on chain
        if self.chain == "aptos":
            self.vault_service = AptosVaultService.get_instance()
        elif self.chain == "flow_evm":
            self.vault_service = FlowEvmVaultService.get_instance(self.chain_id)
        else:
            raise ValueError(f"Unsupported chain: {self.chain}")

    async def query_user_holdings(self, vault_address: str) -> Dict:
        """
        Query user holdings information

        Args:
            vault_address: User address

        Returns:
            Dict: Holdings query result
        """
        try:
            await self.persist_log(
                f"Querying holdings for user: {vault_address} on chain: {self.chain} (chain_id: {self.chain_id})",
                "INFO",
            )

            if not self.vault_service:
                if self.chain == "aptos":
                    self.vault_service = AptosVaultService.get_instance()
                elif self.chain == "flow_evm":
                    self.vault_service = FlowEvmVaultService.get_instance(self.chain_id)

            # Get holdings data based on chain type
            if self.chain == "aptos":
                # Aptos uses get_investor_holdings
                holdings_data = await self.vault_service.get_investor_holdings(
                    vault_address
                )
            elif self.chain == "flow_evm":
                # Flow EVM uses get_vault_info_with_prices
                holdings_data = await self.vault_service.get_vault_info_with_prices(
                    vault_address
                )
            else:
                raise ValueError(f"Unsupported chain: {self.chain}")

            await self.persist_log(
                f"Successfully retrieved {self.chain} vault holdings for address: {vault_address}",
                "INFO",
            )
            await self.persist_log(f"Holdings data: {holdings_data}", "DEBUG")

            return {
                "success": True,
                "vault_address": vault_address,
                "chain": self.chain,
                "chain_id": self.chain_id,
                "holdings_data": holdings_data,
                "message": "Holdings retrieved successfully",
            }

        except Exception as e:
            await self.persist_log(
                f"Error querying {self.chain} vault holdings: {str(e)}", "ERROR"
            )
            await self.persist_log(traceback.format_exc(), "DEBUG")

            raise
            # return {
            #     "success": False,
            #     "vault_address": vault_address,
            #     "chain": self.chain,
            #     "holdings_data": None,
            #     "message": f"Error querying {self.chain} vault holdings: {str(e)}",
            #     "error": str(e),
            # }

    async def prepare_holdings_output(self) -> Dict:
        """
        Prepare holdings output data

        Returns:
            Dict: Formatted holdings data
        """
        if not self.holdings_result:
            return {
                "success": False,
                "vault_address": self.vault_address,
                "chain": self.chain,
                "chain_id": self.chain_id,
                "holdings": [],
                "total_value_usd": 0.0,
                "message": "No holdings data available",
            }

        holdings_data = self.holdings_result.get("holdings_data") or {}

        # 处理不同链的数据格式
        if self.chain == "aptos":
            return await self._prepare_aptos_holdings_output(holdings_data)
        elif self.chain == "flow_evm":
            return await self._prepare_flow_evm_holdings_output(holdings_data)
        else:
            raise ValueError(f"Unsupported chain: {self.chain}")

    async def _prepare_aptos_holdings_output(self, holdings_data: Dict) -> Dict:
        """
        准备 Aptos 持仓输出数据
        """
        holdings = holdings_data.get("holdings", [])

        # 计算总价值
        total_value_usd = 0.0
        formatted_holdings = []

        for holding in holdings:
            token_address = holding.get("token_address")
            amount = holding.get("amount", "0")
            decimals = holding.get("decimals", 0)

            # 获取代币价格
            try:
                price_usd = get_aptos_token_price_usd(token_address)
                if price_usd is None:
                    await self.persist_log(
                        f"Failed to get price for token: {token_address}", "WARNING"
                    )
                    price_usd = 0.0
            except Exception as e:
                await self.persist_log(
                    f"Error getting price for token {token_address}: {str(e)}", "ERROR"
                )
                price_usd = 0.0

            # Calculate value
            if amount and decimals is not None and price_usd > 0:
                try:
                    # Convert raw amount to human-readable format
                    amount_decimal = float(amount) / (10**decimals)
                    value_usd = amount_decimal * price_usd
                except (ValueError, TypeError, ZeroDivisionError) as e:
                    await self.persist_log(
                        f"Error calculating value for token {token_address}: {str(e)}", "ERROR"
                    )
                    amount_decimal = 0.0
                    value_usd = 0.0
            else:
                amount_decimal = 0.0
                value_usd = 0.0

            # Format individual holdings data
            formatted_holding = {
                "token_address": token_address,
                "amount": amount,
                "symbol": holding.get("token_symbol", holding.get("symbol")),
                "decimals": decimals,
                "price_usd": price_usd,
                "value_usd": value_usd,
                "amount_human_readable": amount_decimal,
            }
            formatted_holdings.append(formatted_holding)

            # 累加总价值
            total_value_usd += value_usd

        return {
            "success": self.holdings_result.get("success", False),
            "vault_address": self.vault_address,
            "chain": self.chain,
            "chain_id": self.chain_id,
            "holdings": formatted_holdings,
            "total_value_usd": total_value_usd,
            "timestamp": holdings_data.get("timestamp"),
            "message": self.holdings_result.get("message", ""),
            "raw_result": self.holdings_result,  # 保留原始结果供参考
        }

    async def _prepare_flow_evm_holdings_output(self, holdings_data: Dict) -> Dict:
        """
        准备 Flow EVM 持仓输出数据
        """
        # Check if holdings_data is None (connection failed)
        if holdings_data is None:
            return {
                "success": False,
                "vault_address": self.vault_address,
                "chain": self.chain,
                "chain_id": self.chain_id,
                "holdings": [],
                "total_value_usd": 0.0,
                "message": "Failed to retrieve Flow EVM holdings - connection error",
            }

        # Flow EVM service 返回的数据格式已经包含价格信息
        if not holdings_data.get("success"):
            return {
                "success": False,
                "vault_address": self.vault_address,
                "chain": self.chain,
                "chain_id": self.chain_id,
                "holdings": [],
                "total_value_usd": 0.0,
                "message": "Failed to retrieve Flow EVM holdings",
            }

        vault_info = holdings_data.get("vault", {})
        portfolio_composition = vault_info.get("portfolio_composition", [])
        total_value_usd = float(vault_info.get("total_value_usd", "0"))

        # 转换为统一格式
        formatted_holdings = []
        for token_data in portfolio_composition:
            formatted_holding = {
                "token_address": token_data.get("token_address"),
                "amount": token_data.get("amount"),
                "symbol": token_data.get("token_symbol"),
                "decimals": token_data.get("decimals"),
                "price_usd": token_data.get("price_usd"),
                "value_usd": float(token_data.get("value_usd", "0")) if token_data.get("value_usd") else 0.0,
                "amount_human_readable": float(token_data.get("amount", "0")),
                "percentage": token_data.get("percentage"),
            }
            formatted_holdings.append(formatted_holding)

        return {
            "success": True,
            "vault_address": self.vault_address,
            "chain": self.chain,
            "chain_id": self.chain_id,
            "holdings": formatted_holdings,
            "total_value_usd": total_value_usd,
            "token_count": vault_info.get("token_count", len(formatted_holdings)),
            "message": "Flow EVM holdings retrieved successfully",
            "raw_result": self.holdings_result,  # 保留原始结果供参考
        }

    async def execute(self) -> bool:
        """Execute node logic"""
        try:
            await self.persist_log(
                f"Starting vault node execution: chain={self.chain}, vault_address={self.vault_address}",
                "INFO",
            )
            await self.set_status(NodeStatus.RUNNING)

            # 验证必需参数
            if not self.vault_address:
                error_msg = "vault_address is required but not provided"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # 查询用户持仓
            self.holdings_result = await self.query_user_holdings(self.vault_address)

            # 准备输出数据 (现在是async方法)
            holdings_output = await self.prepare_holdings_output()
            await self.persist_log(
                f"Holdings query result: success={holdings_output.get('success')}, result={holdings_output}",
                "INFO",
            )

            # 发送持仓信息信号给下游节点
            if not await self.send_signal(
                BALANCE_HANDLE, SignalType.VAULT_INFO, payload=holdings_output
            ):
                await self.persist_log("Failed to send vault holdings signal", "ERROR")
                await self.set_status(
                    NodeStatus.FAILED, "Failed to send vault holdings signal"
                )
                return False

            # 完成节点执行
            await self.set_status(NodeStatus.COMPLETED)
            await self.persist_log(
                f"Vault node execution completed: user={self.vault_address}, holdings_count={len(holdings_output.get('holdings', []))}, total_value_usd={holdings_output.get('total_value_usd', 0.0)}",
                "INFO",
            )
            return True

        except asyncio.CancelledError:
            await self.persist_log("Vault node execution cancelled", "INFO")
            await self.set_status(NodeStatus.TERMINATED, "Execution cancelled")
            return False

        except Exception as e:
            await self.persist_log(f"Vault node execution error: {str(e)}", "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            await self.set_status(NodeStatus.FAILED, f"Vault node execution error: {str(e)}")
            return False

    def _register_input_handles(self) -> None:
        """注册输入句柄"""
        # 注册网络输入句柄
        self.register_input_handle(
            name=CHAIN_HANDLE,
            data_type=str,
            description="区块链网络名称（支持 'aptos' 或 'flow_evm'）",
            example="aptos",
            auto_update_attr="chain",
        )
        # 注册链ID输入句柄（Flow EVM 使用）
        self.register_input_handle(
            name=CHAIN_ID_HANDLE,
            data_type=int,
            description="Flow EVM 链ID，如 545 (Flow测试网)",
            example=545,
            auto_update_attr="chain_id",
        )
        # 注册用户地址输入句柄
        self.register_input_handle(
            name=VAULT_ADDRESS_HANDLE,
            data_type=str,
            description="用户的钱包地址或 vault 地址",
            example="0x6a1a233...",
            auto_update_attr="vault_address",
        )
