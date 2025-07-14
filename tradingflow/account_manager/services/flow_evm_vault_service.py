import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from decimal import Decimal

import httpx

from tradingflow.account_manager.common.logging_config import setup_logging
from tradingflow.common.config import CONFIG

# Setup logging
setup_logging(CONFIG)
logger = logging.getLogger(__name__)

COMPANION_URL = CONFIG.get("COMPANION_URL", "http://localhost:3000")
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


class FlowEvmVaultService:
    """
    Service class for managing Flow EVM vaults.

    Provides complete lifecycle management for Flow EVM Vault contracts through companion service,
    including deployment, transaction execution and query functions. This class implements a singleton
    factory pattern to ensure the same instance is always returned for the same chain_id.
    """

    # 类变量用于存储已创建的实例
    _instances: Dict[int, "FlowEvmVaultService"] = {}

    @classmethod
    def get_instance(cls, chain_id: int = 545) -> "FlowEvmVaultService":
        """
        获取FlowEvmVaultService实例

        Args:
            chain_id: 链ID，默认545（Flow测试网）

        Returns:
            FlowEvmVaultService: FlowEvmVaultService实例
        """
        if chain_id not in cls._instances:
            cls._instances[chain_id] = FlowEvmVaultService(chain_id)
            logger.info(f"FlowEvmVaultService instance created for chain_id: {chain_id}")
        return cls._instances[chain_id]

    def __init__(self, chain_id: int, companion_url: str = COMPANION_URL):
        """
        Initialize FlowEvmVaultService

        Args:
            chain_id: 链ID
            companion_url: companion服务URL
        """
        self.chain_id = chain_id
        self._companion_url = companion_url
        self._client = httpx.AsyncClient(timeout=30.0)

    async def create_vault(
        self,
        investor_address: str,
        swap_router: str,
        wrapped_native: str,
        factory_address: str
    ) -> Dict[str, any]:
        """
        创建新的Flow EVM Vault

        Args:
            investor_address: 投资者地址
            swap_router: Swap路由器地址
            wrapped_native: 包装原生代币地址
            factory_address: 工厂合约地址

        Returns:
            Dict[str, any]: 创建结果

        Raises:
            httpx.HTTPStatusError: HTTP请求失败
            httpx.RequestError: 网络请求错误
        """
        try:
            url = f"{self._companion_url}{VAULT_API_CREATE_URI}"
            logger.info(f"Creating vault via companion: {url}")

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
        获取vault信息

        Args:
            vault_address: vault合约地址

        Returns:
            Dict[str, any]: vault信息
        """
        try:
            url = f"{self._companion_url}{VAULT_API_INFO_URI.format(chain_id=self.chain_id, vault_address=vault_address)}"
            logger.info(f"Getting vault info from companion: {url}")

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
        根据投资者地址获取vault信息

        Args:
            investor_address: 投资者地址
            factory_address: 工厂合约地址

        Returns:
            Dict[str, any]: vault信息
        """
        try:
            url = f"{self._companion_url}{VAULT_API_INVESTOR_URI.format(chain_id=self.chain_id, investor_address=investor_address)}"
            params = {"factory_address": factory_address}
            logger.info(f"Getting vault by investor from companion: {url}")

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
            vault_address: vault合约地址

        Returns:
            Dict[str, any]: 投资组合信息
        """
        try:
            url = f"{self._companion_url}{VAULT_API_PORTFOLIO_URI.format(chain_id=self.chain_id, vault_address=vault_address)}"
            logger.info(f"Getting portfolio composition from companion: {url}")

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
        获取Vault信息并计算USD价值

        Args:
            vault_address: vault合约地址

        Returns:
            Dict[str, any]: 包含价格信息的完整Vault数据
        """
        try:
            from decimal import Decimal
            from tradingflow.account_manager.utils.token_price_util import get_multiple_token_prices_usd

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

            # 添加ERC20代币
            token_balances = portfolio.get("token_balances", [])
            for token in token_balances:
                token_address = token.get("token_address", "").lower()
                if token_address:
                    token_addresses.append(token_address)
                    token_balances_map[token_address] = token

            # 批量获取所有代币的USD价格
            token_prices = {}
            if token_addresses:
                token_prices = get_multiple_token_prices_usd(
                    token_addresses,
                    chain_id=self.chain_id,
                    network_type="evm"
                )
                logger.info(f"Retrieved prices for {len(token_prices)} tokens")

            # 计算每个代币的USD价值
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

                # 计算实际代币数量（使用balance_human或根据decimals计算）
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

                # 计算USD价值
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
        fee_rate: int = 0
    ) -> Dict[str, any]:
        """
        执行swap交易

        Args:
            vault_address: vault合约地址
            token_in: 输入代币地址
            token_out: 输出代币地址
            amount_in: 输入金额
            amount_out_min: 最小输出金额
            fee_recipient: 费用接收者地址
            fee_rate: 费用率

        Returns:
            Dict[str, any]: 交易结果
        """
        try:
            url = f"{self._companion_url}{VAULT_API_SWAP_URI}"
            logger.info(f"Executing swap via companion: {url}")

            request_data = {
                "chain_id": self.chain_id,
                "vault_address": vault_address,
                "token_in": token_in,
                "token_out": token_out,
                "amount_in": amount_in,
                "amount_out_min": amount_out_min,
                "fee_recipient": fee_recipient,
                "fee_rate": fee_rate
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

    async def get_token_info(self, token_address: str) -> Dict[str, any]:
        """
        获取代币信息

        Args:
            token_address: 代币地址

        Returns:
            Dict[str, any]: 代币信息
        """
        try:
            url = f"{self._companion_url}{TOKEN_API_INFO_URI.format(chain_id=self.chain_id, token_address=token_address)}"
            logger.info(f"Getting token info from companion: {url}")

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
            url = f"{self._companion_url}{TOKEN_API_BALANCE_URI.format(chain_id=self.chain_id, token_address=token_address, holder_address=holder_address)}"
            logger.info(f"Getting token balance from companion: {url}")

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

    async def close(self):
        """关闭HTTP客户端"""
        if hasattr(self, "_client"):
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


if __name__ == "__main__":
    # 使用示例
    async def main():
        service = FlowEvmVaultService.get_instance(545)  # Flow测试网

        try:
            # 测试获取代币信息
            # token_info = await service.get_token_info("0x...")
            # logger.info("Token info: %s", token_info)

            logger.info("FlowEvmVaultService initialized successfully")

        except Exception as e:
            logger.error("Error: %s", e)
        finally:
            await service.close()

    asyncio.run(main())
