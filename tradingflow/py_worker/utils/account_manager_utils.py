import logging
from typing import Any, Dict, Optional

import aiohttp

from tradingflow.common.config import CONFIG

logger = logging.getLogger(__name__)

# 构建Account Manager API地址
scheme = "http"  # 默认使用http
host = CONFIG.get("ACCOUNT_MANAGER_HOST", "localhost")
port = CONFIG.get("ACCOUNT_MANAGER_PORT", 7001)
account_manager_base_url = f"{scheme}://{host}:{port}"


async def fetch_vault_portfolio(
    chain_id: str,
    vault_address: str,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    通过Vault地址获取Vault的投资组合信息。

    :param chain_id: 链ID
    :param vault_address: Vault地址
    :param auth_token: 身份验证令牌（可选）
    :return: Vault投资组合信息的字典，格式为 {token_address: amount}
    """
    try:
        # 构建API URL
        url = f"{account_manager_base_url}/evm/vaults/{chain_id}/{vault_address}"

        # 准备请求头
        headers = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        # 发送异步HTTP请求
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                # 检查响应状态码
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        "Failed to fetch vault portfolio: %s, status: %d, response: %s",
                        url,
                        response.status,
                        error_text,
                    )
                    return {}

                # 解析响应JSON
                data = await response.json()

                # 检查API响应格式
                if not data.get("success", False) or "vault" not in data:
                    logger.error("Invalid API response format: %s", str(data))
                    return {}

                # 提取投资组合信息
                vault_data = data["vault"]
                portfolio = vault_data.get("portfolio_composition", {})

                logger.info(
                    "Successfully fetched portfolio for vault %s on chain %s: %d tokens",
                    vault_address,
                    chain_id,
                    len(portfolio),
                )

                return portfolio

    except aiohttp.ClientError as e:
        logger.error("HTTP request error when fetching vault portfolio: %s", str(e))
        return {}
    except Exception as e:
        logger.exception("Unexpected error when fetching vault portfolio: %s", str(e))
        return {}


async def fetch_vault_details(
    chain_id: str,
    vault_address: str,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    获取Vault的详细信息，包括名称、符号、底层资产等

    :param chain_id: 链ID
    :param vault_address: Vault地址
    :param auth_token: 身份验证令牌（可选）
    :return: Vault详细信息的字典
    """
    try:
        # 构建API URL
        url = f"{account_manager_base_url}/evm/vaults/{chain_id}/{vault_address}"

        # 准备请求头
        headers = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        # 发送异步HTTP请求
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                # 检查响应状态码
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        "Failed to fetch vault details: %s, status: %d, response: %s",
                        url,
                        response.status,
                        error_text,
                    )
                    return {}

                # 解析响应JSON
                data = await response.json()

                # 检查API响应格式
                if not data.get("success", False) or "vault" not in data:
                    logger.error("Invalid API response format: %s", str(data))
                    return {}

                # 返回完整的Vault数据
                return data["vault"]

    except aiohttp.ClientError as e:
        logger.error("HTTP request error when fetching vault details: %s", str(e))
        return {}
    except Exception as e:
        logger.exception("Unexpected error when fetching vault details: %s", str(e))
        return {}


async def fetch_vaults_by_investor(
    chain_id: str,
    investor_address: str,
    auth_token: Optional[str] = None,
) -> list:
    """
    通过投资者地址获取其所有的Vault信息

    :param chain_id: 链ID
    :param investor_address: 投资者地址
    :param auth_token: 身份验证令牌（可选）
    :return: 包含Vault信息的列表
    """
    try:
        # 构建API URL
        url = f"{account_manager_base_url}/evm/vaults/investor/{chain_id}/{investor_address}"

        # 准备请求头
        headers = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        # 发送异步HTTP请求
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                # 检查响应状态码
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        "Failed to fetch vaults by investor: %s, status: %d, response: %s",
                        url,
                        response.status,
                        error_text,
                    )
                    return []

                # 解析响应JSON
                data = await response.json()

                # 检查API响应格式
                if not data.get("success", False) or "vaults" not in data:
                    logger.error("Invalid API response format: %s", str(data))
                    return []

                # 返回Vaults列表
                vaults = data["vaults"]
                logger.info(
                    "Successfully fetched %d vaults for investor %s on chain %s",
                    len(vaults),
                    investor_address,
                    chain_id,
                )
                return vaults

    except aiohttp.ClientError as e:
        logger.error("HTTP request error when fetching vaults by investor: %s", str(e))
        return []
    except Exception as e:
        logger.exception(
            "Unexpected error when fetching vaults by investor: %s", str(e)
        )
        return []
