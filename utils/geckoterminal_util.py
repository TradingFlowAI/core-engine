import asyncio
import logging
import os
from typing import Any, Dict, List

import aiohttp

from tradingflow.depot.python.constants import EVM_CHAIN_ID_NETWORK_MAP
from tradingflow.depot.python.utils.address_util import normalize_token_address

logger = logging.getLogger(__name__)

# 从环境变量获取代理配置
HTTP_PROXY = os.environ.get("HTTP_PROXY")
HTTPS_PROXY = os.environ.get("HTTPS_PROXY")


# API相关配置
BASE_URL = "https://api.geckoterminal.com/api/v2"

HEADERS = {
    "Accept": "application/json;version=20230302",
    "Content-Type": "application/json",
}

SIMPLE_PRICE_URI = "/simple/networks/{network}/token_price/{addresses}"
SUI_NETWORK = "sui-network"
APTOS_NETWORK = "aptos"
SUI_NETWORK_TYPE = "sui"
APTOS_NETWORK_TYPE = "aptos"
EVM_NETWORK_TYPE = "evm"


async def simple_get_multi_token_prices(
    token_addresses: List[str],
    chain_id: int = 1,
    use_proxy: bool = True,
    timeout: int = 30,
    network=None,
) -> Dict[str, Any]:
    """
    通过GeckoTerminal API获取代币价格。
    :param token_addresses: 代币地址列表
    :param chain_id: 链ID (仅用于EVM网络)
    :param use_proxy: 是否使用代理
    :param timeout: 请求超时时间(秒)
    :param network: 网络名称，如果未提供，则根据链ID自动映射
    :return: 代币价格信息的字典，格式为 {token_address: price_usd}
    """
    if not token_addresses:
        return {}

    # 确定网络类型和网络名称
    if network == SUI_NETWORK:
        network_type = SUI_NETWORK_TYPE
        actual_network = SUI_NETWORK
    elif network == APTOS_NETWORK:
        network_type = APTOS_NETWORK_TYPE
        actual_network = APTOS_NETWORK
    elif network and network.lower() in ["aptos"]:
        network_type = APTOS_NETWORK_TYPE
        actual_network = APTOS_NETWORK
    elif network and network.lower() in ["sui", "sui-network"]:
        network_type = SUI_NETWORK_TYPE
        actual_network = SUI_NETWORK
    else:
        # 默认为EVM网络，使用chain_id映射
        network_type = EVM_NETWORK_TYPE
        actual_network = EVM_CHAIN_ID_NETWORK_MAP.get(chain_id)

        if not actual_network:
            logger.error("不支持的EVM链ID: %s", chain_id)
            return {"error": f"不支持的EVM链ID: {chain_id}"}

    # 规范化代币地址
    token_addresses = [
        normalize_token_address(addr, network_type) for addr in token_addresses
    ]

    # 获取代理设置
    proxy = None
    if use_proxy:
        # 按优先级获取代理配置
        proxy = HTTPS_PROXY or HTTP_PROXY
        logger.info("使用代理: %s", proxy)

    # 初始化结果字典
    result = {}

    # 分批处理，每批最多个地址
    batch_size = 30
    for i in range(0, len(token_addresses), batch_size):
        batch = token_addresses[i : i + batch_size]

        # 将地址列表转换为逗号分隔的字符串
        addresses_str = ",".join(batch)

        # 构建URL
        url = f"{BASE_URL}{SIMPLE_PRICE_URI.format(network=actual_network, addresses=addresses_str)}"
        logger.info("请求URL: %s", url)

        try:
            # 发送请求，设置代理和超时
            async with aiohttp.ClientSession() as session:
                # 如果开启代理，则使用代理和超时设置
                async with session.get(
                    url,
                    headers=HEADERS,
                    proxy=proxy if use_proxy else None,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(
                            "API请求失败，状态码: %s, 错误: %s",
                            response.status,
                            error_text,
                        )
                        continue

                    # 解析响应
                    data = await response.json()
                    logger.info("API响应: %s", data)

                    # 提取价格信息
                    if "data" in data and data["data"]:
                        token_prices = data["data"]["attributes"]["token_prices"]
                        # 将价格字符串转换为浮点数
                        for addr, price_str in token_prices.items():
                            price = float(price_str)
                            # 添加到结果字典，带上价格单位和数据源信息
                            result[addr] = {
                                "price_usd": price,
                                "source": "geckoterminal",
                            }
                    else:
                        logger.warning("API返回格式异常: %s", data)

        except aiohttp.ClientError as e:
            logger.error("API请求异常: %s", str(e))
            # 尝试提供更多的错误信息
            if "Cannot connect to host" in str(e) and use_proxy:
                logger.info("连接失败，检查代理设置: %s", proxy)
            elif "Cannot connect to host" in str(e) and not use_proxy:
                logger.info("连接失败，尝试添加--use-proxy参数使用代理")
        except asyncio.TimeoutError:
            logger.error("请求超时，当前超时设置: %s秒", timeout)
        except Exception as e:
            logger.exception("处理价格数据时发生错误: %s", str(e))

    # 如果没有获取到任何价格数据
    if not result:
        logger.warning("未能获取到任何代币价格数据，链ID: %s", chain_id)
        return {"error": "未能获取价格数据"}

    return result


if __name__ == "__main__":
    import argparse

    # 添加命令行参数
    parser = argparse.ArgumentParser(description="测试GeckoTerminal API")
    parser.add_argument("--no-proxy", action="store_true", help="不使用代理")
    parser.add_argument("--test-connection", action="store_true", help="仅测试API连接")
    parser.add_argument("--timeout", type=int, default=30, help="请求超时时间(秒)")
    args = parser.parse_args()

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # 显示环境信息
    print("环境变量HTTP_PROXY: %s", HTTP_PROXY)
    print("环境变量HTTPS_PROXY: %s", HTTPS_PROXY)
    print("使用代理: %s", not args.no_proxy)

    # 测试函数
    async def test():
        # 测试用例1: 知名代币
        token_addresses = [
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        ]
        print("\n测试1: 获取主流代币价格 (USDC, WETH)...")
        prices = await simple_get_multi_token_prices(
            token_addresses, 1, use_proxy=not args.no_proxy, timeout=args.timeout
        )
        print("获取到的价格: ", prices)

        # 测试用例2: sui上的代币
        token_addresses = [
            "0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59::wal::WAL",
            "0x06864a6f921804860930db6ddbe2e16acdf8504495ea7481637a1c8b9a8fe54b::cetus::CETUS",
        ]
        print("\n测试2: 获取Sui上的代币价格...")
        prices = await simple_get_multi_token_prices(
            token_addresses,
            network=SUI_NETWORK,
            use_proxy=not args.no_proxy,
            timeout=args.timeout,
        )
        print("获取到的价格: ", prices)

        # 测试3: aptos上的代币
        token_addresses = [
            "0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387",  # xBtc
            "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b",  # USDT
            "0x1::aptos_coin::AptosCoin",  # Aptos Coin
            "0xa",  # Aptos Coin
            "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",  # USDC
        ]
        prices = await simple_get_multi_token_prices(
            token_addresses,
            network=APTOS_NETWORK,
            use_proxy=not args.no_proxy,
            timeout=args.timeout,
        )
        print("获取到的价格: ", prices)

        return "测试完成"

    result = asyncio.run(test())
    print(result)
