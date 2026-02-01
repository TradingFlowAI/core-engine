"""
EVM Vault Service - 通用 EVM 链 Vault 服务
支持 Flow-EVM, BSC, Ethereum 等所有 EVM 兼容链
"""

import asyncio
import logging
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

import httpx

from infra.logging_config import setup_logging  # noqa: F401, E402
from infra.config import CONFIG

# Setup logging
setup_logging(CONFIG)
logger = logging.getLogger(__name__)

MONITOR_URL = CONFIG.get("MONITOR_URL")

# API URI 模板
VAULT_API_CREATE_URI = "/evm/vault/create"
VAULT_API_INFO_URI = "/evm/vault/{chain_id}/{vault_address}"
VAULT_API_INVESTOR_URI = "/evm/vault/investor/{chain_id}/{investor_address}"
VAULT_API_PORTFOLIO_URI = "/evm/vault/{chain_id}/{vault_address}/portfolio"
VAULT_API_SWAP_URI = "/evm/vault/swap"
TOKEN_API_INFO_URI = "/evm/tokens/{chain_id}/{token_address}"
TOKEN_API_BATCH_URI = "/evm/tokens/batch"
TOKEN_API_BALANCE_URI = "/evm/tokens/{chain_id}/{token_address}/balance/{holder_address}"
TOKEN_API_BALANCES_URI = "/evm/tokens/balances"
TOKEN_API_PRICE_URI = "/evm/tokens/{chain_id}/{token_address}/price"
TOKEN_API_PRICES_URI = "/evm/tokens/prices"

BASE_PATH = Path(__file__).parent.parent

# EVM 链配置
EVM_CHAIN_CONFIG = {
    # BSC 主网
    56: {
        "name": "bsc",
        "network": "bsc",
        "network_type": "evm",
        "native_symbol": "BNB",
        "wrapped_native": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
        "swap_router": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",  # PancakeSwap V3 Router
        "explorer": "https://bscscan.com",
    },
    # BSC 测试网
    97: {
        "name": "bsc-testnet",
        "network": "bsc-testnet",
        "network_type": "evm",
        "native_symbol": "tBNB",
        "wrapped_native": "0xae13d989daC2f0dEbFf460aC112a837C89BAa7cd",  # WBNB Testnet
        "swap_router": "0x1b81D678ffb9C0263b24A97847620C99d213eB14",  # PancakeSwap V3 Router Testnet
        "explorer": "https://testnet.bscscan.com",
    },
    # Flow EVM 主网
    747: {
        "name": "flow-evm",
        "network": "flow-evm",
        "network_type": "evm",
        "native_symbol": "FLOW",
        "wrapped_native": "0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e",  # WFLOW
        "swap_router": "0x...",  # TODO: 添加实际地址
        "explorer": "https://evm.flowscan.io",
    },
    # Flow EVM 测试网
    545: {
        "name": "flow-evm-testnet",
        "network": "flow-evm-testnet",
        "network_type": "evm",
        "native_symbol": "FLOW",
        "wrapped_native": "0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e",  # WFLOW Testnet
        "swap_router": "0x...",  # TODO: 添加实际地址
        "explorer": "https://evm-testnet.flowscan.io",
    },
    # Ethereum 主网
    1: {
        "name": "ethereum",
        "network": "eth",
        "network_type": "evm",
        "native_symbol": "ETH",
        "wrapped_native": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
        "swap_router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",  # Uniswap V3 Router
        "explorer": "https://etherscan.io",
    },
}


def get_network_name(chain_id: int) -> str:
    """获取链ID对应的网络名称"""
    config = EVM_CHAIN_CONFIG.get(chain_id)
    if config:
        return config["network"]
    return f"evm-{chain_id}"


class EvmVaultService:
    """
    通用 EVM Vault 服务类

    支持所有 EVM 兼容链的 Vault 管理，包括：
    - BSC (主网 & 测试网)
    - Flow EVM (主网 & 测试网)
    - Ethereum
    - 其他 EVM 链

    通过 chain_id 区分不同网络，实现单一服务类管理多链。
    """

    # 类变量用于存储已创建的实例（按 chain_id 分类）
    _instances: Dict[int, "EvmVaultService"] = {}

    @classmethod
    def get_instance(cls, chain_id: int) -> "EvmVaultService":
        """
        获取 EvmVaultService 实例（单例模式，按 chain_id 区分）

        Args:
            chain_id: 链ID
                - 56: BSC 主网
                - 97: BSC 测试网
                - 747: Flow EVM 主网
                - 545: Flow EVM 测试网

        Returns:
            EvmVaultService: 对应链的服务实例
        """
        if chain_id not in cls._instances:
            cls._instances[chain_id] = EvmVaultService(chain_id)
            network_name = get_network_name(chain_id)
            logger.info(f"EvmVaultService instance created for chain_id: {chain_id} ({network_name})")
        return cls._instances[chain_id]

    @classmethod
    def get_bsc_instance(cls, testnet: bool = False) -> "EvmVaultService":
        """获取 BSC Vault 服务实例（便捷方法）"""
        chain_id = 97 if testnet else 56
        return cls.get_instance(chain_id)

    @classmethod
    def get_flow_evm_instance(cls, testnet: bool = True) -> "EvmVaultService":
        """获取 Flow EVM Vault 服务实例（便捷方法）"""
        chain_id = 545 if testnet else 747
        return cls.get_instance(chain_id)

    def __init__(self, chain_id: int, monitor_url: str = MONITOR_URL):
        """
        初始化 EvmVaultService

        Args:
            chain_id: 链ID
            monitor_url: monitor 服务 URL
        """
        self.chain_id = chain_id
        self._monitor_url = monitor_url
        self._client = httpx.AsyncClient(timeout=30.0)

        # 加载链配置
        self.chain_config = EVM_CHAIN_CONFIG.get(chain_id, {})
        self.network = self.chain_config.get("network", f"evm-{chain_id}")
        self.network_type = self.chain_config.get("network_type", "evm")

    @property
    def network_name(self) -> str:
        """获取当前链的网络名称"""
        return self.network

    async def create_vault(
        self,
        investor_address: str,
        swap_router: str,
        wrapped_native: str,
        factory_address: str
    ) -> Dict[str, any]:
        """
        创建新的 EVM Vault

        Args:
            investor_address: 投资者地址
            swap_router: Swap 路由器地址
            wrapped_native: 包装原生代币地址
            factory_address: 工厂合约地址

        Returns:
            Dict[str, any]: 创建结果
        """
        try:
            url = f"{self._monitor_url}{VAULT_API_CREATE_URI}"
            logger.info(f"Creating vault via monitor: {url} (chain_id: {self.chain_id})")

            request_data = {
                "chain_id": self.chain_id,
                "investor_address": investor_address,
                "swap_router": swap_router,
                "wrapped_native": wrapped_native,
                "factory_address": factory_address
            }

            response = await self._client.post(url, json=request_data)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Vault creation response: {data}")

            return data

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error creating vault: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error creating vault: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating vault: {e}")
            raise

    async def get_vault_info(self, vault_address: str) -> Dict[str, any]:
        """
        获取 vault 信息

        Args:
            vault_address: vault 合约地址

        Returns:
            Dict[str, any]: vault 信息
        """
        try:
            url = f"{self._monitor_url}{VAULT_API_INFO_URI.format(chain_id=self.chain_id, vault_address=vault_address)}"
            logger.info(f"Getting vault info from monitor: {url}")

            response = await self._client.get(url)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Retrieved vault info: {data}")

            return data

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting vault info: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting vault info: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting vault info: {e}")
            raise

    async def get_vault_by_investor(self, investor_address: str, factory_address: str) -> Dict[str, any]:
        """
        根据投资者地址获取 vault 信息

        Args:
            investor_address: 投资者地址
            factory_address: 工厂合约地址

        Returns:
            Dict[str, any]: vault 信息
        """
        try:
            url = f"{self._monitor_url}{VAULT_API_INVESTOR_URI.format(chain_id=self.chain_id, investor_address=investor_address)}"
            params = {"factory_address": factory_address}
            logger.info(f"Getting vault by investor from monitor: {url}")

            response = await self._client.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Retrieved vault by investor: {data}")

            return data

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting vault by investor: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting vault by investor: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting vault by investor: {e}")
            raise

    async def get_portfolio_composition(self, vault_address: str) -> Dict[str, any]:
        """
        获取投资组合构成

        Args:
            vault_address: vault 合约地址

        Returns:
            Dict[str, any]: 投资组合信息
        """
        try:
            url = f"{self._monitor_url}{VAULT_API_PORTFOLIO_URI.format(chain_id=self.chain_id, vault_address=vault_address)}"
            logger.info(f"Getting portfolio composition from monitor: {url}")

            response = await self._client.get(url)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Retrieved portfolio composition: {data}")

            return data

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting portfolio composition: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting portfolio composition: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting portfolio composition: {e}")
            raise

    async def get_vault_info_with_prices(self, vault_address: str) -> Dict[str, any]:
        """
        获取 Vault 信息并计算 USD 价值

        Args:
            vault_address: vault 合约地址

        Returns:
            Dict[str, any]: 包含价格信息的完整 Vault 数据
        """
        try:
            from utils.token_price_util import get_multiple_token_prices_usd

            # 获取投资组合数据
            portfolio_data = await self.get_portfolio_composition(vault_address)

            if not portfolio_data or not portfolio_data.get("success"):
                logger.error(f"Failed to get portfolio data for vault {vault_address}")
                raise Exception(f"Failed to get portfolio data for vault {vault_address}")

            portfolio = portfolio_data.get("data", {})

            # 收集需要获取价格的代币地址
            token_addresses = []
            token_balances_map = {}

            # 添加原生代币
            native_balance = portfolio.get("native_balance", {})
            if native_balance:
                native_address = native_balance.get("token_address", "").lower()
                if native_address:
                    token_addresses.append(native_address)
                    token_balances_map[native_address] = native_balance

            # 添加 ERC20 代币
            token_balances = portfolio.get("token_balances", [])
            for token in token_balances:
                token_address = token.get("token_address", "").lower()
                if token_address:
                    token_addresses.append(token_address)
                    token_balances_map[token_address] = token

            # 批量获取所有代币的 USD 价格
            token_prices = {}
            if token_addresses:
                token_prices = get_multiple_token_prices_usd(
                    token_addresses,
                    chain_id=self.chain_id,
                    network_type="evm"
                )
                logger.info(f"Retrieved prices for {len(token_prices)} tokens")

            # 计算每个代币的 USD 价值
            portfolio_composition = []
            total_value_usd = Decimal("0")

            for token_address in token_addresses:
                token_data = token_balances_map[token_address]

                # 获取代币基本信息
                token_amount_raw = token_data.get("balance", "0")
                decimals = int(token_data.get("decimals", 18))
                token_name = token_data.get("token_name", "Unknown")
                token_symbol = token_data.get("token_symbol", "UNKNOWN")
                balance_human = token_data.get("balance_human", "0")

                # 计算实际代币数量
                try:
                    if balance_human and balance_human != "0":
                        token_amount = Decimal(str(balance_human))
                    else:
                        token_amount = Decimal(token_amount_raw) / Decimal(10**decimals)
                except (ValueError, TypeError, ZeroDivisionError):
                    logger.warning(f"Invalid amount or decimals for token {token_address}")
                    token_amount = Decimal("0")

                # 获取代币价格
                token_price = token_prices.get(token_address)

                # 计算 USD 价值
                if token_price is not None and token_amount > 0:
                    token_value_usd = token_amount * Decimal(str(token_price))
                    total_value_usd += token_value_usd
                else:
                    token_value_usd = None
                    if token_price is None:
                        logger.warning(f"Price not found for token {token_address}")

                # 构建代币组合数据
                token_item = {
                    "token_address": token_address,
                    "token_name": token_name,
                    "token_symbol": token_symbol,
                    "amount": str(token_amount),
                    "amount_raw": token_amount_raw,
                    "decimals": decimals,
                    "price_usd": token_price,
                    "value_usd": str(token_value_usd) if token_value_usd is not None else None,
                    "percentage": None,  # 稍后计算
                }
                portfolio_composition.append(token_item)

            # 计算每个代币在投资组合中的占比
            if total_value_usd > 0:
                for token_data in portfolio_composition:
                    if token_data["value_usd"] is not None:
                        token_value = Decimal(token_data["value_usd"])
                        percentage = (token_value / total_value_usd) * 100
                        token_data["percentage"] = float(percentage)

            # 构建响应数据
            vault_data = {
                "success": True,
                "vault": {
                    "vault_address": vault_address,
                    "chain_id": self.chain_id,
                    "network": self.network,
                    "total_value_usd": str(total_value_usd),
                    "token_count": len(portfolio_composition),
                    "portfolio_composition": portfolio_composition,
                }
            }

            return vault_data

        except Exception as e:
            logger.error(f"Error getting vault info with prices for {vault_address}: {e}")
            raise

    async def execute_swap(
        self,
        vault_address: str,
        token_in: str,
        token_out: str,
        amount_in: int,
        amount_out_min: int = 0,
        fee_recipient: Optional[str] = None,
        fee_rate: int = 0,
        # Smart Router 新增参数
        route_type: str = "single",    # "single", "v2", "v3"
        path: Optional[str] = None,     # V3: bytes, V2: JSON array
        fee: int = 2500                 # V3 fee tier (500, 2500, 10000)
    ) -> Dict[str, any]:
        """
        执行 swap 交易 (支持 Smart Router V2/V3 多跳路由)

        Args:
            vault_address: vault 合约地址
            token_in: 输入代币地址
            token_out: 输出代币地址
            amount_in: 输入金额
            amount_out_min: 最小输出金额
            fee_recipient: 费用接收者地址
            fee_rate: 费用率
            route_type: 路由类型 ("single", "v2", "v3")
            path: 编码的路径 (V3: bytes hex, V2: JSON array string)
            fee: V3 单跳 fee tier

        Returns:
            Dict[str, any]: 交易结果
        """
        try:
            url = f"{self._monitor_url}{VAULT_API_SWAP_URI}"
            logger.info(f"Executing swap via monitor: {url} (chain_id: {self.chain_id}, route_type: {route_type})")

            request_data = {
                "chain_id": self.chain_id,
                "vault_address": vault_address,
                "token_in": token_in,
                "token_out": token_out,
                "amount_in": str(amount_in),  # Convert to string to avoid JS number precision issues
                "amount_out_min": str(amount_out_min),  # Convert to string
                "fee_recipient": fee_recipient,
                "fee_rate": fee_rate,
                # Smart Router 新增字段
                "route_type": route_type,
                "path": path,
                "fee": fee
            }

            response = await self._client.post(url, json=request_data)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Swap execution response: {data}")

            return data

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error executing swap: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error executing swap: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing swap: {e}")
            raise

    async def get_route_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        slippage: float = 0.5
    ) -> Dict[str, any]:
        """
        获取最优交易路由报价

        Args:
            token_in: 输入代币地址
            token_out: 输出代币地址
            amount_in: 输入金额
            slippage: 滑点百分比 (默认 0.5%)

        Returns:
            Dict[str, any]: 路由报价信息
        """
        try:
            url = f"{self._monitor_url}/evm/route/quote"
            logger.info(f"Getting route quote from monitor: {url} (chain_id: {self.chain_id})")

            request_data = {
                "chain_id": self.chain_id,
                "token_in": token_in,
                "token_out": token_out,
                "amount_in": str(amount_in),
                "slippage": slippage
            }

            response = await self._client.post(url, json=request_data)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Route quote response: {data}")

            return data.get("data", {})

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting route quote: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting route quote: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting route quote: {e}")
            raise

    async def get_token_info(self, token_address: str) -> Dict[str, any]:
        """
        获取代币信息

        Args:
            token_address: 代币地址

        Returns:
            Dict[str, any]: 代币信息
        """
        try:
            url = f"{self._monitor_url}{TOKEN_API_INFO_URI.format(chain_id=self.chain_id, token_address=token_address)}"
            logger.info(f"Getting token info from monitor: {url}")

            response = await self._client.get(url)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Retrieved token info: {data}")

            return data

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting token info: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting token info: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting token info: {e}")
            raise

    async def get_token_balance(self, token_address: str, holder_address: str) -> Dict[str, any]:
        """
        获取代币余额

        Args:
            token_address: 代币地址
            holder_address: 持有者地址

        Returns:
            Dict[str, any]: 余额信息
        """
        try:
            url = f"{self._monitor_url}{TOKEN_API_BALANCE_URI.format(chain_id=self.chain_id, token_address=token_address, holder_address=holder_address)}"
            logger.info(f"Getting token balance from monitor: {url}")

            response = await self._client.get(url)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Retrieved token balance: {data}")

            return data

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting token balance: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting token balance: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting token balance: {e}")
            raise

    async def get_vault_operations(self, investor_address: str, operation_type: Optional[str] = None) -> Dict[str, any]:
        """
        获取投资者的操作历史记录（从数据库获取）

        Args:
            investor_address: 投资者地址
            operation_type: 操作类型过滤 (deposit, withdraw, swap)

        Returns:
            Dict[str, any]: 操作历史数据
        """
        try:
            from infra.db import db_session
            from infra.db.services.vault_operation_history_service import VaultOperationHistoryService
            from infra.db.models.vault_operation_history import OperationType

            # 处理操作类型过滤
            operation_types = None
            if operation_type:
                operation_type_upper = operation_type.upper()
                if operation_type_upper == "DEPOSIT":
                    operation_types = [OperationType.DEPOSIT]
                elif operation_type_upper == "WITHDRAW":
                    operation_types = [OperationType.WITHDRAW]
                elif operation_type_upper == "SWAP":
                    operation_types = [OperationType.SWAP]
                else:
                    logger.warning(f"Unknown operation type: {operation_type}")

            with db_session() as db:
                # 从数据库获取操作记录
                operations = VaultOperationHistoryService.get_operations_by_investor(
                    db=db,
                    investor_address=investor_address,
                    network=self.network,
                    operation_types=operation_types,
                    limit=1000
                )

                # 转换为前端期望的格式
                operations_data = []
                for op in operations:
                    op_data = {
                        "id": op.id,
                        "vault_contract_id": op.vault_contract_id,
                        "chain_id": op.chain_id,
                        "vault_address": op.vault_address,
                        "transaction_hash": op.transaction_hash,
                        "operation_type": op.operation_type.value,
                        "created_at": op.created_at.isoformat() if op.created_at else None,
                        "updated_at": op.updated_at.isoformat() if op.updated_at else None,
                        # 资产信息
                        "input_token": (
                            {
                                "address": op.input_token_address,
                                "amount": float(op.input_token_amount) if op.input_token_amount else None,
                                "usd_value": float(op.input_token_usd_value) if op.input_token_usd_value else None,
                            }
                            if op.input_token_address
                            else None
                        ),
                        "output_token": (
                            {
                                "address": op.output_token_address,
                                "amount": float(op.output_token_amount) if op.output_token_amount else None,
                                "usd_value": float(op.output_token_usd_value) if op.output_token_usd_value else None,
                            }
                            if op.output_token_address
                            else None
                        ),
                        # Gas 成本
                        "gas": {
                            "used": op.gas_used,
                            "price_wei": str(op.gas_price) if op.gas_price else None,
                            "cost_usd": float(op.total_gas_cost_usd) if op.total_gas_cost_usd else None,
                        },
                    }
                    operations_data.append(op_data)

                logger.info(
                    f"Retrieved {len(operations_data)} operations for investor {investor_address} from database"
                )

                return {
                    "success": True,
                    "investor_address": investor_address,
                    "chain_id": self.chain_id,
                    "network": self.network,
                    "operations": operations_data,
                }

        except Exception as e:
            logger.error(f"Error getting vault operations from database: {e}")
            raise

    async def close(self):
        """关闭 HTTP 客户端"""
        if hasattr(self, "_client"):
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# 为向后兼容保留的别名
FlowEvmVaultService = EvmVaultService
BscVaultService = EvmVaultService


if __name__ == "__main__":
    # 使用示例
    async def main():
        # BSC 主网服务
        bsc_service = EvmVaultService.get_bsc_instance(testnet=False)
        logger.info(f"BSC Service: chain_id={bsc_service.chain_id}, network={bsc_service.network}")

        # BSC 测试网服务
        bsc_testnet_service = EvmVaultService.get_bsc_instance(testnet=True)
        logger.info(f"BSC Testnet Service: chain_id={bsc_testnet_service.chain_id}, network={bsc_testnet_service.network}")

        # Flow EVM 服务
        flow_service = EvmVaultService.get_flow_evm_instance(testnet=True)
        logger.info(f"Flow EVM Service: chain_id={flow_service.chain_id}, network={flow_service.network}")

        # 也可以直接通过 chain_id 获取
        eth_service = EvmVaultService.get_instance(1)
        logger.info(f"Ethereum Service: chain_id={eth_service.chain_id}, network={eth_service.network}")

        await bsc_service.close()
        await bsc_testnet_service.close()
        await flow_service.close()
        await eth_service.close()

    asyncio.run(main())
