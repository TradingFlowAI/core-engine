import logging
import requests
from typing import Optional, Dict, Any
from decimal import Decimal

from tradingflow.common.config import CONFIG
from tradingflow.common.logging_config import setup_logging

# Setup logging
setup_logging(CONFIG, "aptos_util")
logger = logging.getLogger(__name__)

# 缓存代币信息，避免重复请求
_token_info_cache = {}
_token_decimals_cache = {}


async def get_token_decimals(token_address: str) -> int:
    """
    获取Aptos代币的小数位数

    Args:
        token_address: Aptos代币的元数据对象地址

    Returns:
        代币的小数位数，如果获取失败则返回8（Aptos常用默认值）
    """
    # 检查缓存
    cache_key = token_address.lower()
    if cache_key in _token_decimals_cache:
        return _token_decimals_cache[cache_key]

    try:
        # 获取代币信息
        token_info = await fetch_token_info(token_address)

        if token_info and "decimals" in token_info:
            decimals = token_info["decimals"]
            # 缓存结果
            _token_decimals_cache[cache_key] = decimals
            return decimals
        else:
            logger.warning("未能获取Aptos代币小数位数: token=%s", token_address)
            # Aptos默认使用8位小数
            return 8

    except Exception as e:
        logger.error("获取Aptos代币小数位数时出错: %s - %s", token_address, e)
        return 8


async def fetch_token_info(token_address: str) -> Optional[Dict[str, Any]]:
    """
    从companion服务获取Aptos代币信息

    Args:
        token_address: Aptos代币的元数据对象地址

    Returns:
        包含name、symbol、decimals等信息的字典，如果获取失败则返回None
    """
    # 检查缓存
    cache_key = token_address.lower()
    if cache_key in _token_info_cache:
        return _token_info_cache[cache_key]

    try:
        # 获取companion服务配置
        companion_host = CONFIG.get("COMPANION_HOST", "localhost")
        companion_port = CONFIG.get("COMPANION_PORT", 3000)

        # 构建API URL
        api_url = f"http://{companion_host}:{companion_port}/aptos/tokens/metadata/{token_address}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        logger.debug("获取Aptos代币信息: %s", api_url)

        # 发送请求
        response = requests.get(api_url, headers=headers, timeout=30)

        if response.status_code == 200:
            token_info = response.json()

            # 缓存结果
            _token_info_cache[cache_key] = token_info

            logger.debug("成功获取Aptos代币信息: %s - %s",
                        token_address, token_info.get("symbol", "Unknown"))

            return token_info

        elif response.status_code == 404:
            logger.warning("未找到Aptos代币信息: %s", token_address)
            return None

        else:
            logger.warning("获取Aptos代币信息失败，状态码: %d, 响应: %s",
                         response.status_code, response.text[:200])
            return None

    except requests.exceptions.RequestException as e:
        logger.error("请求companion服务失败: %s - %s", token_address, e)
        return None
    except Exception as e:
        logger.error("获取Aptos代币信息时出错: %s - %s", token_address, e)
        return None


async def get_token_symbol(token_address: str) -> Optional[str]:
    """
    获取Aptos代币的符号

    Args:
        token_address: Aptos代币的元数据对象地址

    Returns:
        代币符号，如果获取失败则返回None
    """
    try:
        token_info = await fetch_token_info(token_address)
        return token_info.get("symbol") if token_info else None
    except Exception as e:
        logger.error("获取Aptos代币符号时出错: %s - %s", token_address, e)
        return None


async def get_token_name(token_address: str) -> Optional[str]:
    """
    获取Aptos代币的名称

    Args:
        token_address: Aptos代币的元数据对象地址

    Returns:
        代币名称，如果获取失败则返回None
    """
    try:
        token_info = await fetch_token_info(token_address)
        return token_info.get("name") if token_info else None
    except Exception as e:
        logger.error("获取Aptos代币名称时出错: %s - %s", token_address, e)
        return None


def normalize_aptos_address(address: str) -> str:
    """
    规范化Aptos地址格式

    Args:
        address: Aptos地址字符串

    Returns:
        规范化的Aptos地址
    """
    if not isinstance(address, str):
        address = str(address)

    # 移除0x前缀（如果有）
    if address.startswith("0x"):
        address = address[2:]

    # 补零到64位（Aptos地址标准长度）
    address = address.zfill(64)

    # 添加0x前缀
    return f"0x{address}"


def is_valid_aptos_address(address: str) -> bool:
    """
    验证Aptos地址格式是否有效

    Args:
        address: 要验证的地址

    Returns:
        如果地址格式有效则返回True，否则返回False
    """
    try:
        if not isinstance(address, str):
            return False

        # 移除0x前缀进行验证
        clean_address = address[2:] if address.startswith("0x") else address

        # 检查是否为有效的十六进制字符串
        int(clean_address, 16)

        # 检查长度（最多64个字符）
        return len(clean_address) <= 64

    except (ValueError, TypeError):
        return False


# 便捷函数，用于从代币数量字符串转换为Decimal
def convert_token_amount(amount_str: str, decimals: int) -> Decimal:
    """
    将代币原始数量转换为可读的Decimal数量

    Args:
        amount_str: 代币原始数量字符串
        decimals: 代币小数位数

    Returns:
        转换后的Decimal数量
    """
    try:
        raw_amount = Decimal(amount_str)
        return raw_amount / Decimal(10 ** decimals)
    except Exception as e:
        logger.error("转换代币数量时出错: amount=%s, decimals=%s, error=%s",
                    amount_str, decimals, e)
        return Decimal("0")


def clear_cache():
    """清理缓存"""
    global _token_info_cache, _token_decimals_cache
    _token_info_cache.clear()
    _token_decimals_cache.clear()
    logger.info("Aptos代币信息缓存已清理")


if __name__ == "__main__":
    # 测试代码
    import asyncio

    async def test():
        # 测试APT代币（示例地址）
        apt_address = "0x1::aptos_coin::AptosCoin"

        decimals = await get_token_decimals(apt_address)
        symbol = await get_token_symbol(apt_address)
        name = await get_token_name(apt_address)

        logger.info("APT代币信息:")
        logger.info("  地址: %s", apt_address)
        logger.info("  名称: %s", name)
        logger.info("  符号: %s", symbol)
        logger.info("  小数位数: %s", decimals)

    asyncio.run(test())
