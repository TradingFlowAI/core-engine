import json
import logging
from datetime import datetime, timezone

from tradingflow.bank.utils.geckoterminal_util import (
    simple_get_multi_token_prices,
)
from tradingflow.depot.db import db_session
from tradingflow.depot.db.services.monitored_token_service import MonitoredTokenService
from tradingflow.depot.db.services.token_price_history_service import (
    TokenPriceHistoryService,
)
from tradingflow.depot.utils.redis_manager import RedisManager

logger = logging.getLogger(__name__)


async def fetch_prices_async():
    """
    抓取token价格数据的异步实现，支持EVM和非EVM网络

    Returns:
        价格抓取结果
    """
    try:
        logger.info("开始抓取token价格数据...")

        # 获取当前时间戳
        current_time = datetime.now(timezone.utc)

        # 存储提前获取的代币数据
        tokens_data = {}

        # 从数据库中获取需要监控的代币
        with db_session() as db:
            # 获取所有活跃的监控代币
            tokens = MonitoredTokenService.get_all_active_tokens(db)

            # 在会话关闭前，提取需要的数据并按网络类型和ID分组
            # 使用两种分组方式：1) 按chain_id分组（EVM网络）2) 按network分组（非EVM网络）
            tokens_by_chain_id = {}  # EVM网络
            tokens_by_network = {}  # 非EVM网络

            for token in tokens:
                # 提取共享属性
                token_data = {
                    "id": token.id,
                    "network": token.network,
                    "network_type": token.network_type,
                    "token_address": token.token_address,
                    "symbol": token.symbol or "未知代币",
                    "name": token.name or token.symbol or "未知代币",
                }

                # 根据网络类型进行分组
                if token.network_type == "evm" and token.chain_id is not None:
                    # EVM网络用chain_id分组
                    chain_id = token.chain_id
                    if chain_id not in tokens_by_chain_id:
                        tokens_by_chain_id[chain_id] = []
                    tokens_by_chain_id[chain_id].append(token_data)
                else:
                    # 非EVM网络用network分组
                    network = token.network
                    if network not in tokens_by_network:
                        tokens_by_network[network] = []
                    tokens_by_network[network].append(token_data)

            logger.info(
                "从数据库中获取了 %d 个EVM链和 %d 个非EVM网络上的监控代币，总计 %d 个代币",
                len(tokens_by_chain_id),
                len(tokens_by_network),
                len(tokens),
            )

            # 保存数据以供后续使用
            tokens_data = {"evm": tokens_by_chain_id, "non_evm": tokens_by_network}

        # 初始化新的结果结构
        results = {"evm": {}, "non_evm": {}}

        # 1. 处理EVM网络的代币
        for chain_id, chain_tokens in tokens_data["evm"].items():
            try:
                # 提取该链上所有代币的地址
                token_addresses = [token["token_address"] for token in chain_tokens]

                # 记录当前链上要查询的代币
                token_symbols = [token["symbol"] for token in chain_tokens]
                logger.info(
                    "准备获取EVM链 %s 上的 %d 个代币价格: %s",
                    chain_id,
                    len(token_addresses),
                    ", ".join(token_symbols),
                )

                # 异步调用价格获取函数，传入chain_id
                prices = await simple_get_multi_token_prices(
                    token_addresses, chain_id=chain_id
                )
                results["evm"][str(chain_id)] = prices
                logger.info(
                    "成功获取EVM链 %s 的价格数据: %s 个币对", chain_id, len(prices)
                )
            except Exception as chain_error:
                logger.error(
                    "获取EVM链 %s 价格数据失败: %s",
                    chain_id,
                    str(chain_error),
                    exc_info=True,
                )
                results["evm"][str(chain_id)] = {"error": str(chain_error)}

        # 2. 处理非EVM网络的代币
        for network, network_tokens in tokens_data["non_evm"].items():
            try:
                # 提取该网络上所有代币的地址
                token_addresses = [token["token_address"] for token in network_tokens]

                # 记录当前网络上要查询的代币
                token_symbols = [token["symbol"] for token in network_tokens]
                logger.info(
                    "准备获取网络 %s 上的 %d 个代币价格: %s",
                    network,
                    len(token_addresses),
                    ", ".join(token_symbols),
                )

                # 异步调用价格获取函数，传入network而非chain_id
                prices = await simple_get_multi_token_prices(
                    token_addresses, network=network
                )

                results["non_evm"][network] = prices
                logger.info(
                    "成功获取网络 %s 的价格数据: %s 个币对", network, len(prices)
                )
            except Exception as network_error:
                logger.error(
                    "获取网络 %s 价格数据失败: %s",
                    network,
                    str(network_error),
                    exc_info=True,
                )
                results["non_evm"][network] = {"error": str(network_error)}

        # FIXME: append mock data for local development
        if "31337" not in results["evm"]:
            results["evm"]["31337"] = {
                "0x7314AEeC874A25A1131F49dA9679D05f8d931175": {
                    "price_usd": 0.99,
                    "source": "mock",
                },
                "0xD604C06206f6DeDd82d42F90D1F5bB34a2E7c5dd": {
                    "price_usd": 1.59,
                    "source": "mock",
                },
            }
        else:
            results["evm"]["31337"]["0x7314AEeC874A25A1131F49dA9679D05f8d931175"] = {
                "price_usd": 0.99,
                "source": "mock",
            }
            results["evm"]["31337"]["0xD604C06206f6DeDd82d42F90D1F5bB34a2E7c5dd"] = {
                "price_usd": 1.59,
                "source": "mock",
            }

        # 存储结果到Redis
        store_price_data(results, current_time)

        logger.info(
            "价格数据已存储到Redis缓存: results = %s", json.dumps(results, indent=2)
        )

        # 存储价格数据到数据库
        try:
            with db_session() as db:
                # 批量插入价格记录
                saved_count = TokenPriceHistoryService.batch_insert_records(
                    db, results, current_time
                )
                logger.info("成功保存 %d 条价格记录到数据库", saved_count)

        except Exception as db_error:
            logger.error("保存价格数据到数据库失败: %s", str(db_error), exc_info=True)
            # 数据库保存失败不影响主流程，继续执行

        # 计算处理的币对数量
        processed_count = 0
        evm_count = sum(
            len(prices)
            for prices in results["evm"].values()
            if isinstance(prices, dict) and "error" not in prices
        )
        non_evm_count = sum(
            len(prices)
            for prices in results["non_evm"].values()
            if isinstance(prices, dict) and "error" not in prices
        )
        processed_count = evm_count + non_evm_count

        logger.info("币对价格抓取完成，处理了 %s 个币对", processed_count)

        return {
            "status": "success",
            "timestamp": current_time.isoformat(),
            "chains_processed": len(results["evm"]),
            "networks_processed": len(results["non_evm"]),
            "tokens_processed": processed_count,
        }

    except Exception as e:
        logger.exception("币对价格抓取任务失败: %s", str(e))
        return {"status": "error", "error": str(e)}


def store_price_data(price_results, timestamp):
    """存储价格数据到Redis缓存，支持EVM和非EVM网络

    Args:
        price_results: 价格结果，格式为:
            {
                "evm": {network_id: {token_address: price_info}},
                "non_evm": {network_name: {token_address: price_info}}
            }
        timestamp: 时间戳
    """
    try:
        # 获取Redis客户端
        redis_client = RedisManager.get_client()

        # 获取当前时间
        timestamp_str = timestamp.isoformat()

        # 记录一个全局的最后更新时间
        global_key = RedisManager.get_global_last_updated_key()
        redis_client.set(global_key, timestamp_str)

        # 处理EVM网络
        for network_id, token_prices in price_results.get("evm", {}).items():
            if isinstance(token_prices, dict) and "error" not in token_prices:
                # 记录每个网络的最后更新时间
                last_updated_key = RedisManager.get_network_last_updated_key(
                    "evm", network_id
                )
                redis_client.set(last_updated_key, timestamp_str)

                for token_address, price_data in token_prices.items():
                    # 添加时间戳到价格数据中
                    price_data_with_timestamp = price_data.copy()
                    price_data_with_timestamp["timestamp"] = timestamp_str

                    # 使用RedisManager统一生成key
                    price_key = RedisManager.get_token_price_key(
                        "evm", network_id, token_address
                    )

                    # 存储价格数据，使用JSON序列化
                    redis_client.set(price_key, json.dumps(price_data_with_timestamp))

                    # 设置过期时间，例如1小时
                    redis_client.expire(price_key, 3600)

        # 处理非EVM网络
        for network_name, token_prices in price_results.get("non_evm", {}).items():
            if isinstance(token_prices, dict) and "error" not in token_prices:
                # 记录每个网络的最后更新时间
                last_updated_key = RedisManager.get_network_last_updated_key(
                    "non_evm", network_name
                )
                redis_client.set(last_updated_key, timestamp_str)

                for token_address, price_data in token_prices.items():
                    # 添加时间戳到价格数据中
                    price_data_with_timestamp = price_data.copy()
                    price_data_with_timestamp["timestamp"] = timestamp_str

                    # 使用RedisManager统一生成key
                    price_key = RedisManager.get_token_price_key(
                        "non_evm", network_name, token_address
                    )

                    # 存储价格数据，使用JSON序列化
                    redis_client.set(price_key, json.dumps(price_data_with_timestamp))

                    # 设置过期时间，例如1小时
                    redis_client.expire(price_key, 3600)

        logger.debug("价格数据及时间戳已存储到Redis，时间戳: %s", timestamp_str)

    except Exception as e:
        logger.error("存储价格数据时出错: %s", str(e), exc_info=True)


if __name__ == "__main__":
    import asyncio

    # 运行异步任务
    result = asyncio.run(fetch_prices_async())
    print(result)
