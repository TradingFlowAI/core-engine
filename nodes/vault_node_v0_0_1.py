import asyncio
import traceback
from typing import Dict, List, Optional
from datetime import datetime

import httpx

from services.aptos_vault_service import AptosVaultService
from services.flow_evm_vault_service import FlowEvmVaultService
from services.evm_vault_service import EvmVaultService
from utils.token_price_util import get_aptos_token_price_usd
from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus
from infra.config import CONFIG

# input handles
CHAIN_HANDLE = "chain"
VAULT_ADDRESS_HANDLE = "vault_address"
# output handles
VAULT_OUTPUT_HANDLE = "vault"

# Monitor service URL
MONITOR_URL = CONFIG.get("MONITOR_URL", "http://localhost:3000")

# Chain configuration table - chain_id is auto-determined, no user input needed
CHAIN_CONFIG = {
    "aptos": {
        "chain_id": None,  # Aptos doesn't use chain_id
        "description": "Aptos Mainnet",
        "network_type": "aptos",
    },
    "flow_evm": {
        "chain_id": 545,  # Flow EVM Testnet (747 for mainnet when ready)
        "description": "Flow EVM Testnet",
        "network_type": "evm",
    },
    "bsc": {
        "chain_id": 56,  # BSC Mainnet
        "description": "BSC Mainnet",
        "network_type": "evm",
    },
    "bsc-testnet": {
        "chain_id": 97,  # BSC Testnet
        "description": "BSC Testnet",
        "network_type": "evm",
    },
}


@register_node_type(
    "vault_node",
    default_params={
        "chain": "aptos",  # Supports "aptos", "flow_evm", "bsc", "bsc-testnet"
        "vault_address": None,  # User address, required parameter
    },
)
class VaultNode(NodeBase):
    """
    Vault Node - Query user holdings information in TradingFlow Vault

    This node is responsible for querying the asset holdings of specified users in the TradingFlow Vault system.
    Supports multi-chain queries, including Aptos, Flow EVM, BSC, and BSC Testnet.

    Input parameters:
    - chain: Blockchain network name (supports "aptos", "flow_evm", "bsc", "bsc-testnet")
    - vault_address: User's wallet address or vault address

    Note: chain_id is automatically determined from CHAIN_CONFIG table based on chain selection.

    Output signals:
    - VAULT_INFO: Signal containing complete user holdings information

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

    # Flow EVM configuration (chain_id is auto-determined from config)
    node = VaultNode(
        chain="flow_evm",
        vault_address="0x1234..."
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

        # Validate chain and get chain_id from config table
        if self.chain not in CHAIN_CONFIG:
            raise ValueError(
                f"Unsupported chain: {self.chain}. Supported chains: {list(CHAIN_CONFIG.keys())}"
            )

        # Get chain_id and network_type from fixed config table (no user input needed)
        self.chain_id = CHAIN_CONFIG[self.chain]["chain_id"]
        self.network_type = CHAIN_CONFIG[self.chain].get("network_type", "aptos")

        # Query result
        self.holdings_result = None

        # Initialize vault service based on chain
        if self.chain == "aptos":
            self.vault_service = AptosVaultService.get_instance()
        elif self.chain == "flow_evm":
            self.vault_service = FlowEvmVaultService.get_instance(self.chain_id)
        elif self.chain in ["bsc", "bsc-testnet"]:
            self.vault_service = EvmVaultService.get_instance(self.chain_id)
        else:
            raise ValueError(f"Unsupported chain: {self.chain}")

        # HTTP client for monitor API calls
        self._http_client = httpx.AsyncClient(timeout=30.0)

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
                elif self.chain in ["bsc", "bsc-testnet"]:
                    self.vault_service = EvmVaultService.get_instance(self.chain_id)

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
            elif self.chain in ["bsc", "bsc-testnet"]:
                # BSC uses get_vault_info_with_prices (same as Flow EVM)
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

    async def get_recent_transactions(self, vault_address: str, limit: int = 10) -> List[Dict]:
        """
        获取最近的交易历史

        Args:
            vault_address: Vault地址
            limit: 返回的最大交易数量

        Returns:
            List[Dict]: 交易历史列表
        """
        try:
            if self.chain == "aptos":
                # Aptos: /aptos/vault/:address/operations
                url = f"{MONITOR_URL}/aptos/vault/{vault_address}/operations"
                params = {"limit": limit}
            elif self.chain in ["flow_evm", "bsc", "bsc-testnet"]:
                # EVM chains: /evm/vault/:chain_id/:vault_address/operations
                url = f"{MONITOR_URL}/evm/vault/{self.chain_id}/{vault_address}/operations"
                params = {"limit": limit}
            else:
                await self.persist_log(f"Unsupported chain for transactions: {self.chain}", "WARNING")
                return []

            await self.persist_log(f"Fetching transactions from: {url}", "DEBUG")

            response = await self._http_client.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            # Extract operations from response
            if self.chain == "aptos":
                # Aptos response format: {"data": {"events": [...]}}
                operations = data.get("data", {}).get("events", [])
            else:
                # EVM chains (Flow EVM, BSC) response format: {"success": true, "data": [...]}
                operations = data.get("data", [])

            await self.persist_log(f"Retrieved {len(operations)} transactions", "INFO")

            # Format transactions to unified structure
            formatted_transactions = []
            for op in operations[:limit]:
                formatted_tx = {
                    "id": op.get("id"),
                    "operation_type": op.get("operation_type") or op.get("event_type"),
                    "transaction_hash": op.get("transaction_hash") or op.get("tx_hash"),
                    "input_token_address": op.get("input_token_address"),
                    "input_token_amount": op.get("input_token_amount"),
                    "input_token_usd_value": op.get("input_token_usd_value"),
                    "output_token_address": op.get("output_token_address"),
                    "output_token_amount": op.get("output_token_amount"),
                    "output_token_usd_value": op.get("output_token_usd_value"),
                    "gas_used": op.get("gas_used"),
                    "gas_price": op.get("gas_price"),
                    "total_gas_cost_usd": op.get("total_gas_cost_usd"),
                    "created_at": op.get("created_at") or op.get("timestamp"),
                }
                formatted_transactions.append(formatted_tx)

            return formatted_transactions

        except httpx.HTTPError as e:
            await self.persist_log(f"HTTP error fetching transactions: {str(e)}", "WARNING")
            return []
        except Exception as e:
            await self.persist_log(f"Error fetching transactions: {str(e)}", "WARNING")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            return []

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
        elif self.chain in ["flow_evm", "bsc", "bsc-testnet"]:
            # BSC 和 Flow EVM 使用相同的 EVM 格式
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
                "message": f"Failed to retrieve {self.chain} holdings - connection error",
            }

        # EVM service 返回的数据格式已经包含价格信息
        if not holdings_data.get("success"):
            return {
                "success": False,
                "vault_address": self.vault_address,
                "chain": self.chain,
                "chain_id": self.chain_id,
                "holdings": [],
                "total_value_usd": 0.0,
                "message": f"Failed to retrieve {self.chain} holdings",
            }

        vault_info = holdings_data.get("vault", {})
        portfolio_composition = vault_info.get("portfolio_composition", [])
        total_value_usd = float(vault_info.get("total_value_usd", "0"))

        # 转换为统一格式
        formatted_holdings = []
        for token_data in portfolio_composition:
            # 获取原始 wei 值（优先使用 amount_raw）
            amount_raw = token_data.get("amount_raw")
            decimals = token_data.get("decimals", 18)
            
            # 如果有 amount_raw，使用它；否则从 amount 转换
            if amount_raw is not None:
                amount_wei = str(amount_raw)
                try:
                    amount_human = float(amount_raw) / (10 ** decimals)
                except (ValueError, TypeError):
                    amount_human = float(token_data.get("amount", "0"))
            else:
                # 旧格式：amount 是人类可读值
                amount_human = float(token_data.get("amount", "0"))
                try:
                    amount_wei = str(int(amount_human * (10 ** decimals)))
                except (ValueError, TypeError):
                    amount_wei = "0"
            
            formatted_holding = {
                "token_address": token_data.get("token_address"),
                "amount": amount_wei,  # 原始 wei 值，用于百分比计算
                "symbol": token_data.get("token_symbol"),
                "decimals": decimals,
                "price_usd": token_data.get("price_usd"),
                "value_usd": float(token_data.get("value_usd", "0")) if token_data.get("value_usd") else 0.0,
                "amount_human_readable": amount_human,  # 人类可读值
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
            "message": f"{self.chain} holdings retrieved successfully",
            "raw_result": self.holdings_result,  # 保留原始结果供参考
        }

    async def prepare_vault_output(self) -> Dict:
        """
        准备完整的 Vault 输出数据（包含 chain, address, balance, transactions）

        Returns:
            Dict: 完整的 Vault 信息
        """
        # 获取持仓数据
        holdings_output = await self.prepare_holdings_output()

        # 获取最近的交易历史
        transactions = await self.get_recent_transactions(self.vault_address, limit=10)

        # For BSC chains, use the resolved vault contract address if available
        # This is critical for downstream nodes (like swap) that need the actual contract address
        output_address = self.vault_address
        if self.chain in ["bsc", "bsc-testnet"] and hasattr(self, 'resolved_vault_contract') and self.resolved_vault_contract:
            output_address = self.resolved_vault_contract

        # 构建完整的 vault 输出
        vault_output = {
            "chain": self.chain,
            "chain_id": self.chain_id,
            "address": output_address,  # Use resolved contract address for BSC
            "investor_address": getattr(self, 'investor_address', self.vault_address),  # Original user wallet address
            "balance": {
                "holdings": holdings_output.get("holdings", []),
                "total_value_usd": holdings_output.get("total_value_usd", 0.0),
                "token_count": len(holdings_output.get("holdings", [])),
                "timestamp": holdings_output.get("timestamp") or datetime.utcnow().isoformat() + "Z",
            },
            "transactions": transactions,
            "success": holdings_output.get("success", False),
            "message": holdings_output.get("message", ""),
        }

        # Include token_count for Flow EVM if available
        if "token_count" in holdings_output:
            vault_output["balance"]["token_count"] = holdings_output["token_count"]

        return vault_output

    async def _resolve_vault_contract_address(self, investor_address: str) -> Optional[str]:
        """
        For BSC chains, resolve the actual Vault contract address from an investor address.
        Returns the vault contract address if found, or None if not found.
        """
        if self.chain not in ["bsc", "bsc-testnet"]:
            return None  # Only needed for BSC
        
        try:
            # Get factory address from environment
            import os
            factory_address = os.getenv("BSC_FACTORY_ADDRESS") if self.chain == "bsc" else os.getenv("BSC_TESTNET_FACTORY_ADDRESS")
            
            if not factory_address:
                await self.persist_log(f"No factory address configured for {self.chain}", "WARNING")
                return None
            
            # Query the factory contract to get the user's vault address
            url = f"{MONITOR_URL}/evm/vault/investor/{self.chain_id}/{investor_address}?factory_address={factory_address}"
            await self.persist_log(f"Resolving vault contract address via Factory: {url}", "DEBUG")
            
            response = await self._http_client.get(url)
            response.raise_for_status()
            
            data = response.json()
            if data.get("success") and data.get("data", {}).get("vault_address"):
                vault_contract_address = data["data"]["vault_address"]
                # Check if it's not a zero address
                if vault_contract_address != "0x0000000000000000000000000000000000000000":
                    await self.persist_log(
                        f"Resolved investor {investor_address} to vault contract {vault_contract_address}",
                        "INFO"
                    )
                    return vault_contract_address
            
            return None
        except Exception as e:
            await self.persist_log(f"Failed to resolve vault contract address: {e}", "DEBUG")
            return None

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

            # For BSC chains, resolve the actual vault contract address
            # User may have provided their wallet address (investor address) instead of vault contract address
            self.investor_address = self.vault_address  # Store original investor address
            self.resolved_vault_contract = None
            
            if self.chain in ["bsc", "bsc-testnet"]:
                resolved_address = await self._resolve_vault_contract_address(self.vault_address)
                if resolved_address:
                    self.resolved_vault_contract = resolved_address
                    await self.persist_log(
                        f"Using resolved vault contract address: {resolved_address} (original: {self.vault_address})",
                        "INFO"
                    )

            # 查询用户持仓
            self.holdings_result = await self.query_user_holdings(self.vault_address)

            # 准备完整的 vault 输出数据 (包含 balance 和 transactions)
            vault_output = await self.prepare_vault_output()
            await self.persist_log(
                f"Vault query result: success={vault_output.get('success')}, chain={vault_output.get('chain')}, address={vault_output.get('address')}",
                "INFO",
            )

            # 发送完整的 vault 信息信号给下游节点
            if not await self.send_signal(
                VAULT_OUTPUT_HANDLE, SignalType.VAULT_INFO, payload=vault_output
            ):
                await self.persist_log("Failed to send vault info signal", "ERROR")
                await self.set_status(
                    NodeStatus.FAILED, "Failed to send vault info signal"
                )
                return False

            # 完成节点执行
            await self.set_status(NodeStatus.COMPLETED)
            balance_info = vault_output.get('balance', {})
            await self.persist_log(
                f"Vault node execution completed: chain={vault_output.get('chain')}, address={vault_output.get('address')}, "
                f"holdings_count={len(balance_info.get('holdings', []))}, total_value_usd={balance_info.get('total_value_usd', 0.0)}, "
                f"transactions_count={len(vault_output.get('transactions', []))}",
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
        finally:
            # Close HTTP client
            if hasattr(self, '_http_client'):
                await self._http_client.aclose()

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
        # 注册用户地址输入句柄
        self.register_input_handle(
            name=VAULT_ADDRESS_HANDLE,
            data_type=str,
            description="用户的钱包地址或 vault 地址",
            example="0x6a1a233...",
            auto_update_attr="vault_address",
        )

    def _register_output_handles(self) -> None:
        """Register output handles"""
        self.register_output_handle(
            name=VAULT_OUTPUT_HANDLE,
            data_type=dict,
            description="Complete vault information including chain, address, balance, and recent transactions",
            example={
                "chain": "aptos",
                "chain_id": None,
                "address": "0x6a1a233...",
                "balance": {
                    "holdings": [{"token_symbol": "APT", "amount": "10.5", "value_usd": 52.3}],
                    "total_value_usd": 1000.0,
                    "token_count": 2,
                    "timestamp": "2024-01-01T12:00:00Z"
                },
                "transactions": [
                    {
                        "operation_type": "swap",
                        "transaction_hash": "0xabc...",
                        "input_token_address": "0xa",
                        "output_token_address": "0xb",
                        "created_at": "2024-01-01T12:00:00Z"
                    }
                ]
            },
        )
