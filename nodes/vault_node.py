import asyncio
import logging
import traceback
from typing import Dict, Optional

from tradingflow.bank.services.aptos_vault_service import AptosVaultService
from tradingflow.bank.utils.token_price_util import get_aptos_token_price_usd
from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

# input handles
CHAIN_HANDLE = "chain"
VAULT_ADDRESS_HANDLE = "vault_address"
# output handles
BALANCE_HANDLE = "vault_balance"


@register_node_type(
    "vault_node",
    default_params={
        "chain": "aptos",  # 目前固定为 aptos
        "vault_address": None,  # 用户地址，必需参数
    },
)
class VaultNode(NodeBase):
    """
    Vault Node - 查询用户在 TradingFlow Vault 中的持仓信息

    这个节点负责查询指定用户在 TradingFlow Vault 系统中的资产持仓情况。
    它通过 AptosVaultService 获取用户的详细持仓数据并输出给下游节点。

    输入参数:
    - chain: 区块链网络名称（目前固定为 "aptos"）
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
    # 基本配置
    node = VaultNode(
        chain="aptos",
        vault_address="0x6a1a233..."
    )

    # 通过信号接收用户地址
    node = VaultNode(
        chain="aptos"
        # vault_address 将通过 address_handle 信号接收
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
        self.vault_address = vault_address

        # Validate chain
        if self.chain != "aptos":
            raise ValueError(
                f"Unsupported chain: {self.chain}. Currently only 'aptos' is supported."
            )

        # Query result
        self.holdings_result = None

        # Logging setup
        self.logger = logging.getLogger(f"VaultNode.{node_id}")

        # Initialize vault service
        self.vault_service = AptosVaultService.get_instance()

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
                "Querying holdings for user: %s on chain: %s",
                vault_address,
                self.chain,
            )

            if not self.vault_service:
                self.vault_service = AptosVaultService.get_instance()

            # Get investor holdings
            holdings_data = await self.vault_service.get_investor_holdings(
                vault_address
            )

            self.logger.info(
                "Successfully retrieved holdings for user: %s", vault_address
            )
            self.logger.debug("Holdings data: %s", holdings_data)

            return {
                "success": True,
                "vault_address": vault_address,
                "vault_address": vault_address,
                "chain": self.chain,
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
                "holdings": [],
                "total_value_usd": 0.0,
                "message": "No holdings data available",
            }

        holdings_data = self.holdings_result.get("holdings_data", {})
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
                "symbol": holding.get("symbol"),
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
            "holdings": formatted_holdings,
            "total_value_usd": total_value_usd,
            "timestamp": holdings_data.get("timestamp"),
            "message": self.holdings_result.get("message", ""),
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
            description="区块链网络名称（目前固定为 aptos）",
            example="aptos",
            auto_update_attr="chain",
        )
        # 注册用户地址输入句柄
        self.register_input_handle(
            name=VAULT_ADDRESS_HANDLE,
            data_type=str,
            description="用户的钱包地址或 vault 地址",
            example="0x6a1a233...",
            auto_update_attr="vault_address",
        )
