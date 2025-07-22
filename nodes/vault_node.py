import asyncio
import logging
import traceback
from typing import Dict, Optional

from tradingflow.bank.services.aptos_vault_service import AptosVaultService
from tradingflow.bank.services.flow_evm_vault_service import FlowEvmVaultService
from tradingflow.bank.utils.token_price_util import get_aptos_token_price_usd
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
        "chain": "aptos",  # 支持 "aptos" 或 "flow_evm"
        "chain_id": None,  # Flow EVM 链ID，如 545 (Flow测试网)
        "vault_address": None,  # 用户地址，必需参数
    },
)
class VaultNode(NodeBase):
    """
    Vault Node - 查询用户在 TradingFlow Vault 中的持仓信息

    这个节点负责查询指定用户在 TradingFlow Vault 系统中的资产持仓情况。
    支持多链查询，包括 Aptos 和 Flow EVM。

    输入参数:
    - chain: 区块链网络名称（支持 "aptos" 或 "flow_evm"）
    - chain_id: Flow EVM 链ID（仅 Flow EVM 需要，默认 545）
    - vault_address: 用户的钱包地址或 vault 地址

    输出信号:
    - VAULT_HOLDINGS: 包含用户完整持仓信息的信号

    持仓信息包括:
    - holdings: 持仓代币列表，每个包含代币地址、数量、价值等信息
    - total_value_usd: 总持仓价值（美元）
    - timestamp: 查询时间戳
    - vault_address: 查询的用户地址

    示例配置:
    ```python
    # Aptos 配置
    node = VaultNode(
        chain="aptos",
        vault_address="0x6a1a233..."
    )

    # Flow EVM 配置
    node = VaultNode(
        chain="flow_evm",
        chain_id=545,  # Flow 测试网
        vault_address="0x1234..."
    )

    # 通过信号接收参数
    node = VaultNode(
        chain="flow_evm"
        # chain_id 和 vault_address 将通过信号接收
    )
    ```

    输出数据格式:
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
        "raw_result": {...}  # 原始查询结果
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
            raise ValueError(
                f"Unsupported chain: {self.chain}. Supported chains: 'aptos', 'flow_evm'"
            )
        
        # Validate chain_id for flow_evm
        if self.chain == "flow_evm" and self.chain_id is None:
            # 默认使用 Flow 测试网
            self.chain_id = 545
            self.logger.warning("No chain_id provided for flow_evm, using default: 545 (Flow Testnet)")

        # Query result
        self.holdings_result = None

        # Logging setup
        self.logger = logging.getLogger(f"VaultNode.{node_id}")

        # Initialize vault service based on chain
        if self.chain == "aptos":
            self.vault_service = AptosVaultService.get_instance()
        elif self.chain == "flow_evm":
            self.vault_service = FlowEvmVaultService.get_instance(self.chain_id)
        else:
            raise ValueError(f"Unsupported chain: {self.chain}")

    async def query_user_holdings(self, vault_address: str) -> Dict:
        """
        查询用户持仓信息

        Args:
            vault_address: 用户地址

        Returns:
            Dict: 持仓查询结果
        """
        try:
            self.logger.info(
                "Querying holdings for user: %s on chain: %s (chain_id: %s)",
                vault_address,
                self.chain,
                self.chain_id,
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

            self.logger.info(
                "Successfully retrieved holdings for user: %s", vault_address
            )
            self.logger.debug("Holdings data: %s", holdings_data)

            return {
                "success": True,
                "vault_address": vault_address,
                "chain": self.chain,
                "chain_id": self.chain_id,
                "holdings_data": holdings_data,
                "message": "Holdings retrieved successfully",
            }

        except Exception as e:
            error_msg = f"Failed to query holdings for user {vault_address}: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())

            return {
                "success": False,
                "vault_address": vault_address,
                "chain": self.chain,
                "holdings_data": None,
                "message": error_msg,
                "error": str(e),
            }

    async def prepare_holdings_output(self) -> Dict:
        """
        准备持仓输出数据

        Returns:
            Dict: 格式化的持仓数据
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

        holdings_data = self.holdings_result.get("holdings_data", {})
        
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
                    self.logger.warning(
                        f"Failed to get price for token: {token_address}"
                    )
                    price_usd = 0.0
            except Exception as e:
                self.logger.error(
                    f"Error getting price for token {token_address}: {str(e)}"
                )
                price_usd = 0.0

            # 计算价值
            if amount and decimals is not None and price_usd > 0:
                try:
                    # 将原始数量转换为人类可读格式
                    amount_decimal = float(amount) / (10**decimals)
                    value_usd = amount_decimal * price_usd
                except (ValueError, TypeError, ZeroDivisionError) as e:
                    self.logger.error(
                        f"Error calculating value for token {token_address}: {str(e)}"
                    )
                    amount_decimal = 0.0
                    value_usd = 0.0
            else:
                amount_decimal = 0.0
                value_usd = 0.0

            # 格式化单个持仓数据
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
            self.logger.info(
                "Starting vault node execution: chain=%s, vault_address=%s",
                self.chain,
                self.vault_address,
            )
            await self.set_status(NodeStatus.RUNNING)

            # 验证必需参数
            if not self.vault_address:
                error_msg = "vault_address is required but not provided"
                self.logger.error(error_msg)
                await self.set_status(NodeStatus.FAILED, error_msg)
                return False

            # 查询用户持仓
            self.holdings_result = await self.query_user_holdings(self.vault_address)

            # 准备输出数据 (现在是async方法)
            holdings_output = await self.prepare_holdings_output()
            self.logger.info(
                "Holdings query result: success=%s, result=%s",
                holdings_output.get("success"),
                holdings_output,
            )

            # 发送持仓信息信号给下游节点
            if not await self.send_signal(
                BALANCE_HANDLE, SignalType.VAULT_INFO, payload=holdings_output
            ):
                self.logger.error("Failed to send vault holdings signal")
                await self.set_status(
                    NodeStatus.FAILED, "Failed to send vault holdings signal"
                )
                return False

            # 完成节点执行
            await self.set_status(NodeStatus.COMPLETED)
            self.logger.info(
                "Vault node execution completed: user=%s, holdings_count=%d, total_value_usd=%.2f",
                self.vault_address,
                len(holdings_output.get("holdings", [])),
                holdings_output.get("total_value_usd", 0.0),
            )
            return True

        except asyncio.CancelledError:
            self.logger.info("Vault node execution cancelled")
            await self.set_status(NodeStatus.TERMINATED, "Execution cancelled")
            return False

        except Exception as e:
            error_message = f"Vault node execution error: {str(e)}"
            self.logger.error("Vault node execution error: %s", str(e))
            self.logger.error(traceback.format_exc())
            await self.set_status(NodeStatus.FAILED, error_message)
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
