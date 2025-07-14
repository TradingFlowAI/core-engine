import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import httpx

from tradingflow.depot.config import CONFIG

# 添加日志记录器
logger = logging.getLogger(__name__)

COMPANION_URL = CONFIG.get("COMPANION_URL")
VAULT_API_EVENTS_URI = "/aptos/vault/events/{address}"
VAULT_API_HOLDINGS_URI = "/aptos/vault/holdings/{address}"
VAULT_API_TRADE_SIGNAL_URI = "/aptos/vault/trade-signal"
VAULT_API_BALANCE_MANAGER_URI = "/aptos/vault/balance-manager/{address}"


BASE_PATH = Path(__file__).parent.parent


class AptosVaultService:
    """
    Service class for managing the vault.

    Provides complete lifecycle management for Vault contracts, including deployment,
    transaction execution and query functions. This class implements a singleton factory pattern
    to ensure the same instance is always returned for the same chain_id.
    """

    # 类变量用于存储已创建的实例
    _instances = None

    @classmethod
    def get_instance(
        cls,
    ) -> "AptosVaultService":
        """
        获取AptosVaultService实例

        Returns:
            AptosVaultService: AptosVaultService
        """
        if cls._instances is None:
            cls._instances = AptosVaultService()
            logger.info("AptosVaultService instance created")
        return cls._instances

    def __init__(self, companion_url=COMPANION_URL):
        """
        Initialize AptosVaultService
        """
        self._companion_url = companion_url
        self._client = httpx.AsyncClient(timeout=30.0)

    async def check_balance_manager(self, investor_address: str) -> Dict[str, any]:
        """
        检查投资者是否已创建余额管理器

        Args:
            investor_address: 投资者地址

        Returns:
            Dict[str, any]: 余额管理器状态信息

        Example Response:
        {"address":"0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d","balance_manager_created":true}

        或者如果没有创建：
        {
            "address": "0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
            "balance_manager_created": false,
        }

        Raises:
            httpx.HTTPStatusError: HTTP请求失败
            httpx.RequestError: 网络请求错误
        """
        try:
            url = f"{self._companion_url}{VAULT_API_BALANCE_MANAGER_URI.format(address=investor_address)}"
            logger.info(f"Checking balance manager for investor: {url}")

            response = await self._client.get(url)
            response.raise_for_status()

            data = response.json()
            logger.info(
                f"Balance manager check completed for investor {investor_address}: "
                f"balance_manager_created={data.get('balance_manager_created', False)}"
            )

            return data

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # 404 通常表示没有找到余额管理器
                logger.info(
                    f"Balance manager not found for investor {investor_address}"
                )
                return {
                    "address": investor_address,
                    "has_balance_manager": False,
                    "message": "Balance manager not found for this address",
                }
            else:
                logger.error(
                    f"HTTP error checking balance manager: {e.response.status_code} - {e.response.text}"
                )
                raise
        except httpx.RequestError as e:
            logger.error(f"Request error checking balance manager: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error checking balance manager: {e}")
            raise

    async def has_balance_manager(self, investor_address: str) -> bool:
        """
        简化的检查方法，只返回布尔值表示是否有余额管理器

        Args:
            investor_address: 投资者地址

        Returns:
            bool: True 如果已创建余额管理器，False 如果未创建

        Raises:
            Exception: 如果检查过程中发生错误
        """
        try:
            result = await self.check_balance_manager(investor_address)
            return result.get("balance_manager_created", False)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            else:
                raise
        except Exception:
            raise

    async def get_investor_holdings(self, investor_address: str) -> Dict[str, any]:
        """
        获取指定投资者地址的Vault持有资产

        Args:
            investor_address: 投资者地址

        Returns:
            Dict[str, any]: 持有资产数据，包含 address 和 holdings 列表

        Example Response:
        {
            "address": "0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
            "holdings": [
                {
                    "token_address": "0xa",
                    "token_name": "Aptos Coin",
                    "token_symbol": "APT",
                    "amount": "50000",
                    "decimals": 8
                }
            ]
        }
        """
        try:
            url = f"{self._companion_url}{VAULT_API_HOLDINGS_URI.format(address=investor_address)}"
            logger.info(f"Requesting investor holdings from: {url}")

            response = await self._client.get(url)
            response.raise_for_status()

            data = response.json()
            logger.info(
                f"Retrieved holdings for investor {investor_address}: {len(data.get('holdings', []))} tokens"
            )

            return data

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error getting investor holdings: {e.response.status_code} - {e.response.text}"
            )
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting investor holdings: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting investor holdings: {e}")
            raise

    async def get_investor_events(
        self, investor_address: str, event_type: Optional[str] = None
    ) -> Dict[str, any]:
        """
        获取指定地址的投资者事件

        Args:
            investor_address: 投资者地址
            event_type: 事件类型，可选 (DEPOSIT, WITHDRAW, SWAP)

        Returns:
            Dict[str, any]: 事件数据，包含 address 和 events 列表

        Example Response:
        {
            "address": "0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
            "events": [
                {
                    "vault_address": "0xa7e5235a9546d0498db43aae1f0565df965f7527f6173be448dd22e86a248df4",
                    "transaction_hash": "2799858519",
                    "operation_type": "SWAP",
                    "input_token_address": "0xa",
                    "input_token_amount": "50000",
                    "output_token_address": "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",
                    "output_token_amount": "2322",
                    "created_at": "2025-05-31T10:08:02.560Z",
                    "updated_at": "2025-05-31T10:08:02.560Z"
                }
            ]
        }
        """
        try:
            url = f"{self._companion_url}{VAULT_API_EVENTS_URI.format(address=investor_address)}"

            # 如果指定了事件类型，添加查询参数
            params = {}
            if event_type:
                params["event_type"] = event_type

            logger.info(f"Requesting investor events from: {url} with params: {params}")

            response = await self._client.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            events = data.get("events", [])

            # 如果在客户端过滤事件类型（如果API不支持服务端过滤）
            if event_type and events:
                events = [
                    event
                    for event in events
                    if event.get("operation_type") == event_type.upper()
                ]
                data["events"] = events

            logger.info(
                f"Retrieved events for investor {investor_address}: {len(events)} events"
            )

            return data

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error getting investor events: {e.response.status_code} - {e.response.text}"
            )
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting investor events: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting investor events: {e}")
            raise

    async def get_vault_summary(self, investor_address: str) -> Dict[str, any]:
        """
        获取投资者的完整Vault摘要信息，包括持仓和事件

        Args:
            investor_address: 投资者地址

        Returns:
            Dict[str, any]: 包含持仓和事件的完整信息
        """
        try:
            # 并发获取持仓和事件数据
            holdings_task = self.get_investor_holdings(investor_address)
            events_task = self.get_investor_events(investor_address)

            holdings_data, events_data = await asyncio.gather(
                holdings_task, events_task
            )

            # 整合数据
            summary = {
                "address": investor_address,
                "holdings": holdings_data.get("holdings", []),
                "events": events_data.get("events", []),
                "summary_stats": self._calculate_summary_stats(
                    holdings_data.get("holdings", []), events_data.get("events", [])
                ),
            }

            return summary

        except Exception as e:
            logger.error(f"Error getting vault summary for {investor_address}: {e}")
            raise

    def _calculate_summary_stats(
        self, holdings: List[Dict], events: List[Dict]
    ) -> Dict[str, any]:
        """
        计算摘要统计信息

        Args:
            holdings: 持仓数据
            events: 事件数据

        Returns:
            Dict[str, any]: 统计信息
        """
        stats = {
            "total_tokens": len(holdings),
            "total_events": len(events),
            "event_types": {},
            "unique_vaults": set(),
        }

        # 统计事件类型
        for event in events:
            event_type = event.get("operation_type", "UNKNOWN")
            stats["event_types"][event_type] = (
                stats["event_types"].get(event_type, 0) + 1
            )

            # 收集唯一的vault地址
            vault_address = event.get("vault_address")
            if vault_address:
                stats["unique_vaults"].add(vault_address)

        # 转换set为list以便JSON序列化
        stats["unique_vaults"] = list(stats["unique_vaults"])
        stats["unique_vault_count"] = len(stats["unique_vaults"])

        return stats

    async def get_events_by_type(
        self, investor_address: str, operation_type: str
    ) -> List[Dict[str, any]]:
        """
        根据操作类型获取事件

        Args:
            investor_address: 投资者地址
            operation_type: 操作类型 (DEPOSIT, WITHDRAW, SWAP)

        Returns:
            List[Dict[str, any]]: 指定类型的事件列表
        """
        events_data = await self.get_investor_events(investor_address, operation_type)
        return events_data.get("events", [])

    async def get_token_holdings(
        self, investor_address: str, token_address: Optional[str] = None
    ) -> List[Dict[str, any]]:
        """
        获取特定代币的持仓或所有持仓

        Args:
            investor_address: 投资者地址
            token_address: 代币地址，可选

        Returns:
            List[Dict[str, any]]: 持仓列表
        """
        holdings_data = await self.get_investor_holdings(investor_address)
        holdings = holdings_data.get("holdings", [])

        if token_address:
            holdings = [h for h in holdings if h.get("token_address") == token_address]

        return holdings

    async def close(self):
        """关闭HTTP客户端"""
        if hasattr(self, "_client"):
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def admin_execute_swap(
        self,
        user_address: str,
        from_token_metadata_id: str,
        to_token_metadata_id: str,
        amount_in: int,
        fee_tier: int = 2,
        amount_out_min: int = 0,
        sqrt_price_limit: str = "0",
        deadline: Optional[int] = None,
    ) -> Dict[str, any]:
        """
        管理员执行swap交易

        Args:
            user_address: 用户地址
            from_token_metadata_id: 输入代币元数据ID
            to_token_metadata_id: 输出代币元数据ID
            amount_in: 输入金额（已经乘以decimals的整数）
            fee_tier: 费用等级，默认为2
            amount_out_min: 最小输出金额，默认为0
            sqrt_price_limit: 价格限制，默认为"0"
            deadline: 交易截止时间戳，如果为None则使用当前时间+1小时

        Returns:
            Dict[str, any]: 交易执行结果

        Example Response:
        {
            "success": true,
            "transaction_hash": "0x...",
            "message": "Trade executed successfully"
        }

        Raises:
            ValueError: 参数验证失败
            httpx.HTTPStatusError: HTTP请求失败
            httpx.RequestError: 网络请求错误
        """
        try:
            # 参数验证
            if not user_address:
                raise ValueError("user_address is required")
            if not from_token_metadata_id:
                raise ValueError("from_token_metadata_id is required")
            if not to_token_metadata_id:
                raise ValueError("to_token_metadata_id is required")
            if amount_in <= 0:
                raise ValueError("amount_in must be greater than 0")

            # 如果没有提供deadline，设置为当前时间+1小时（毫秒时间戳）
            if deadline is None:

                deadline = int((time.time() + 3600) * 1000 * 1000)  # 转换为微秒

            # 构建请求数据
            trade_data = {
                "userAddress": user_address,
                "fromTokenMetadataId": from_token_metadata_id,
                "toTokenMetadataId": to_token_metadata_id,
                "feeTier": fee_tier,
                "amountIn": amount_in,
                "amountOutMin": amount_out_min,
                "sqrtPriceLimit": sqrt_price_limit,
                "deadline": deadline,
            }

            url = f"{self._companion_url}{VAULT_API_TRADE_SIGNAL_URI}"
            logger.info(f"Executing admin swap: {url}")
            logger.info(f"Trade data: {trade_data}")

            # 发送POST请求
            response = await self._client.post(url, json=trade_data)
            response.raise_for_status()

            result = response.json()
            logger.info(f"Admin swap executed successfully: {result}")

            return result

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error executing admin swap: {e.response.status_code} - {e.response.text}"
            logger.error(error_msg)
            return {"success": False, "message": error_msg}
        except httpx.RequestError as e:
            error_msg = f"Request error executing admin swap: {e}"
            logger.error(error_msg)
            raise
        except ValueError as e:
            error_msg = f"Validation error: {e}"
            logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Unexpected error executing admin swap: {e}"
            logger.error(error_msg)
            raise

    async def get_contract_address(self) -> Dict[str, any]:
        """
        从companion服务获取TradingFlow Vault合约地址

        Returns:
            Dict[str, any]: 合约地址信息

        Example Response:
        {
            "contract_address": "0x...",
            "network": "aptos",
            "version": "1.0.0"
        }

        Raises:
            httpx.HTTPStatusError: HTTP请求失败
            httpx.RequestError: 网络请求错误
        """
        try:
            url = f"{self._companion_url}/aptos/api/tf_vault/contract-address"
            logger.info(f"Requesting contract address from companion: {url}")

            response = await self._client.get(url)
            response.raise_for_status()

            data = response.json()
            logger.info("Successfully retrieved contract address from companion")

            return data

        except httpx.HTTPStatusError as e:
            error_msg = f"Failed to get contract address from companion. Status: {e.response.status_code}, Error: {e.response.text}"
            logger.error(error_msg)
            raise
        except httpx.RequestError as e:
            logger.error(f"Network error when requesting companion service: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting contract address: {e}")
            raise

    async def get_vault_info_with_prices(self, investor_address: str) -> Dict[str, any]:
        """
        获取投资者Vault信息并计算USD价值

        Args:
            investor_address: 投资者地址

        Returns:
            Dict[str, any]: 包含价格信息的完整Vault数据
        """
        try:
            from decimal import Decimal
            from tradingflow.bank.utils.token_price_util import (
                get_multiple_aptos_token_prices_usd,
            )

            # 检查余额管理器状态
            balance_manager_created = await self.has_balance_manager(investor_address)

            # 获取投资者持仓数据
            holdings_data = await self.get_investor_holdings(investor_address)
            holdings = holdings_data.get("holdings", [])

            # 收集需要获取价格的代币地址
            token_addresses = []
            token_holdings_map = {}

            for holding in holdings:
                token_address = holding.get("token_address", "").lower()
                if token_address:
                    token_addresses.append(token_address)
                    token_holdings_map[token_address] = holding

            # 批量获取所有代币的USD价格
            token_prices = {}
            if token_addresses:
                token_prices = get_multiple_aptos_token_prices_usd(token_addresses)
                logger.info("Retrieved prices for %d tokens", len(token_prices))

            # 计算每个代币的USD价值
            portfolio_composition = []
            total_value_usd = Decimal("0")

            for token_address in token_addresses:
                holding = token_holdings_map[token_address]

                # 获取代币基本信息
                token_amount_raw = holding.get("amount", "0")
                decimals = holding.get("decimals", 8)
                token_name = holding.get("token_name", "Unknown")
                token_symbol = holding.get("token_symbol", "UNKNOWN")

                # 计算实际代币数量（考虑decimals）
                try:
                    token_amount = Decimal(token_amount_raw) / Decimal(10**decimals)
                except (ValueError, TypeError, ZeroDivisionError):
                    logger.warning("Invalid amount or decimals for token %s", token_address)
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
                        logger.warning("Price not found for token %s", token_address)

                # 构建代币组合数据
                token_data = {
                    "token_address": token_address,
                    "token_name": token_name,
                    "token_symbol": token_symbol,
                    "amount": str(token_amount),
                    "amount_raw": token_amount_raw,
                    "decimals": decimals,
                    "price_usd": token_price,
                    "value_usd": (
                        str(token_value_usd) if token_value_usd is not None else None
                    ),
                    "percentage": None,  # 稍后计算
                }
                portfolio_composition.append(token_data)

            # 计算每个代币在投资组合中的占比
            if total_value_usd > 0:
                for token_data in portfolio_composition:
                    if token_data["value_usd"] is not None:
                        token_value = Decimal(token_data["value_usd"])
                        percentage = (token_value / total_value_usd) * 100
                        token_data["percentage"] = float(percentage)

            # 构建响应数据
            vault_data = {
                "investor_address": investor_address,
                "balance_manager_created": balance_manager_created,
                "total_value_usd": str(total_value_usd),
                "token_count": len(portfolio_composition),
                "portfolio_composition": portfolio_composition,
            }

            return vault_data

        except Exception as e:
            logger.error(f"Error getting vault info with prices for {investor_address}: {e}")
            raise

    async def get_vault_operations_with_prices(self, investor_address: str) -> Dict[str, any]:
        """
        获取投资者操作记录并计算USD价值

        Args:
            investor_address: 投资者地址

        Returns:
            Dict[str, any]: 包含价格信息的操作记录
        """
        try:
            from datetime import datetime
            from decimal import Decimal
            from tradingflow.bank.utils.token_price_util import (
                get_aptos_monitored_token_info,
            )
            from tradingflow.depot.db import db_session
            from tradingflow.depot.db.services.token_price_history_service import (
                TokenPriceHistoryService,
            )

            # 获取操作记录
            operations = await self.get_investor_events(investor_address)

            if not operations:
                return None

            # 处理操作记录，添加USD价值计算
            enhanced_operations = operations.copy()
            events = enhanced_operations.get("events", [])
            tolerance_minutes = 120000  # 临时调试用的大容忍时间

            with db_session() as db:
                for event in events:
                    try:
                        # 解析操作时间
                        created_at_str = event.get("created_at")
                        if created_at_str:
                            operation_time = datetime.fromisoformat(
                                created_at_str.replace("Z", "+00:00")
                            )
                        else:
                            logger.warning(
                                "Missing created_at for transaction %s",
                                event.get("transaction_hash"),
                            )
                            continue

                        # 处理input token
                        await self._process_token_data(
                            db, event, "input", operation_time, tolerance_minutes
                        )

                        # 处理output token
                        await self._process_token_data(
                            db, event, "output", operation_time, tolerance_minutes
                        )

                        # 为SWAP操作计算交易摘要
                        self._calculate_swap_summary(event)

                    except Exception as e:
                        logger.error(
                            "Error processing event %s: %s",
                            event.get("transaction_hash"),
                            e,
                        )
                        continue

            return enhanced_operations

        except Exception as e:
            logger.error(f"Error getting vault operations with prices for {investor_address}: {e}")
            raise

    async def _process_token_data(self, db, event: Dict, token_type: str, operation_time, tolerance_minutes: int):
        """
        处理代币数据，计算价格和价值

        Args:
            db: 数据库会话
            event: 事件数据
            token_type: "input" 或 "output"
            operation_time: 操作时间
            tolerance_minutes: 时间容忍度（分钟）
        """
        from decimal import Decimal
        from tradingflow.bank.utils.token_price_util import (
            get_aptos_monitored_token_info,
        )
        from tradingflow.depot.db.services.token_price_history_service import (
            TokenPriceHistoryService,
        )

        token_address = event.get(f"{token_type}_token_address")
        token_amount_raw = event.get(f"{token_type}_token_amount")

        if token_address and token_amount_raw:
            token_address = token_address.lower()

            # 查询token在操作时间的价格
            price = TokenPriceHistoryService.get_price_at_timestamp(
                db=db,
                network="aptos",
                token_address=token_address,
                target_timestamp=operation_time,
                tolerance_minutes=tolerance_minutes,
            )

            # 从数据库获取代币的实际信息
            token_info = get_aptos_monitored_token_info(token_address)
            decimals = token_info.get("decimals", 8) if token_info else 8
            token_symbol = (
                token_info.get("symbol", "UNKNOWN") if token_info else "UNKNOWN"
            )

            # 添加代币符号字段
            event[f"{token_type}_token_symbol"] = token_symbol

            if price is not None:
                amount = Decimal(token_amount_raw) / Decimal(10**decimals)
                value_usd = float(amount * Decimal(str(price)))

                event[f"{token_type}_token_price_usd"] = price
                event[f"{token_type}_token_amount_decimal"] = str(amount)
                event[f"{token_type}_token_value_usd"] = value_usd
                event[f"{token_type}_token_decimals"] = decimals
            else:
                event[f"{token_type}_token_price_usd"] = None
                event[f"{token_type}_token_amount_decimal"] = None
                event[f"{token_type}_token_value_usd"] = None
                event[f"{token_type}_token_decimals"] = decimals
                logger.warning(
                    "Price not found for %s token %s at %s",
                    token_type,
                    token_address,
                    operation_time,
                )
        else:
            # 如果没有token信息，设置为None
            event[f"{token_type}_token_symbol"] = None

    def _calculate_swap_summary(self, event: Dict):
        """
        计算SWAP操作的交易摘要

        Args:
            event: 事件数据
        """
        if (
            event.get("operation_type") == "SWAP"
            and event.get("input_token_value_usd") is not None
            and event.get("output_token_value_usd") is not None
        ):
            input_value = event["input_token_value_usd"]
            output_value = event["output_token_value_usd"]

            # 计算滑点或盈亏
            if input_value > 0:
                price_impact = ((output_value - input_value) / input_value) * 100
                event["price_impact_percent"] = round(price_impact, 4)
                event["value_difference_usd"] = round(output_value - input_value, 6)


if __name__ == "__main__":

    # 添加asyncio导入
    import asyncio

    from tradingflow.bank.utils.geckoterminal_util import (
        simple_get_multi_token_prices,
    )

    # 使用示例
    async def main():
        service = AptosVaultService.get_instance()

        # 测试3: aptos上的代币
        token_addresses = [
            "0xa",  # Aptos Coin
            "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",  # USDC
        ]
        prices = await simple_get_multi_token_prices(
            token_addresses,
            network="aptos",
            use_proxy=True,
        )
        print("获取到的价格: ", prices)

        price_aptos = prices.get("0xa", {}).get("price_usd", "N/A")
        price_usdc = prices.get(
            "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b", {}
        ).get("price_usd", "N/A")
        aptos_decimals = 8
        usdc_decimals = 6

        slippage = 1  # 1% slippage

        # USDC -> Aptos 的 exact_in 交换
        # amount_in = 2000 已经是乘以 decimals 后的最小单位
        amount_in = 2000  # 这是 USDC 的最小单位 (2000 / 10^6 = 0.002 USDC)

        # 计算实际的 USDC 数量（原始单位）
        actual_usdc_amount = amount_in / (10**usdc_decimals)  # 2000 / 10^6 = 0.002 USDC

        print(f"输入: {amount_in} (最小单位) = {actual_usdc_amount} USDC")
        print(f"Aptos Price: ${price_aptos}")
        print(f"USDC Price: ${price_usdc}")

        # 计算预期输出的 Aptos 数量
        usdc_value_usd = actual_usdc_amount * price_usdc  # USDC 总价值（USD）
        expected_aptos_amount = (
            usdc_value_usd / price_aptos
        )  # 预期得到的 Aptos 数量（原始单位）

        # 应用滑点保护，计算最小输出量
        slippage_factor = 1 - (slippage / 100)  # 1% slippage = 0.99
        min_aptos_amount = (
            expected_aptos_amount * slippage_factor
        )  # 最小 Aptos 数量（原始单位）

        # 转换为最小单位
        amount_out_min = int(min_aptos_amount * (10**aptos_decimals))

        # 计算 sqrt_price_limit (Q64.64 格式)
        from decimal import Decimal, getcontext

        # 设置高精度
        getcontext().prec = 50

        def calculate_sqrt_price_limit_q64_64(
            input_price, output_price, slippage_tolerance
        ):
            """计算 exact_in 场景下的 sqrt_price_limit"""
            try:
                # 计算价格比率 (output_price / input_price)
                price_ratio = Decimal(str(output_price)) / Decimal(str(input_price))

                # 对于 exact_in，应用滑点保护
                slippage_factor = Decimal(str(slippage_tolerance)) / Decimal("100")
                adjusted_price_ratio = price_ratio * (Decimal("1") - slippage_factor)

                # 计算 sqrt(price_ratio)
                sqrt_price_ratio = adjusted_price_ratio.sqrt()

                # 转换为 Q64.64 定点数
                q64_64_multiplier = Decimal(2) ** 64
                sqrt_price_limit_q64_64 = int(sqrt_price_ratio * q64_64_multiplier)

                return str(sqrt_price_limit_q64_64)
            except Exception as e:
                raise ValueError(f"计算 sqrt_price_limit 失败: {str(e)}")

        sqrt_price_limit = calculate_sqrt_price_limit_q64_64(
            input_price=price_usdc,
            output_price=price_aptos,
            slippage_tolerance=slippage,
        )

        print("\n=== 交换计算结果 ===")
        print(f"输入代币: USDC")
        print(f"输出代币: Aptos")
        print(f"输入金额 (最小单位): {amount_in}")
        print(f"输入金额 (实际): {actual_usdc_amount} USDC")
        print(f"输入价值: ${usdc_value_usd:.6f} USD")
        print(f"预期输出: {expected_aptos_amount:.8f} Aptos")
        print(f"最小输出: {min_aptos_amount:.8f} Aptos")
        print(f"最小输出 (最小单位): {amount_out_min}")
        print(f"滑点容忍: {slippage}%")
        print(f"sqrt_price_limit: {sqrt_price_limit}")

        print("\n=== 执行交换 ===")

        try:
            result = await service.admin_execute_swap(
                user_address="0x6a1a233e8034ad0cf8d68951864a5a49819b3e9751da4b9fe34618dd41ea9d0d",
                from_token_metadata_id="0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",  # USDC
                to_token_metadata_id="0xa",  # Aptos
                amount_in=amount_in,  # USDC 最小单位 (2000)
                amount_out_min=amount_out_min,  # Aptos 最小单位
                sqrt_price_limit=sqrt_price_limit,
                fee_tier=2,
            )
            print(f"交换结果: {json.dumps(result, indent=2)}")
        except Exception as e:
            print(f"交换执行失败: {str(e)}")

        await service.close()

    asyncio.run(main())
