import json
import logging
from datetime import datetime, timezone

from sanic import Blueprint
from sanic.response import json as sanic_json
from sqlalchemy.exc import IntegrityError

from tradingflow.bank.common.middleware import authenticate
from tradingflow.bank.utils.token_price_util import (
    get_aptos_token_price_usd,
    get_multiple_aptos_token_prices_usd,
    get_multiple_token_prices_usd,
    get_sui_token_price_usd,
    get_token_price_usd,
)
from tradingflow.depot.constants import EVM_CHAIN_ID_NETWORK_MAP
from tradingflow.depot.db import db_session
from tradingflow.depot.db.services.monitored_token_service import MonitoredTokenService
from tradingflow.depot.db.services.token_price_history_service import (
    TokenPriceHistoryService,
)
from tradingflow.depot.exceptions import (
    DuplicateResourceException,
    ResourceNotFoundException,
)
from tradingflow.depot.utils import eth_util
from tradingflow.depot.utils.redis_manager import RedisManager

logger = logging.getLogger(__name__)

# 创建蓝图
price_bp = Blueprint("price_api", url_prefix="/price")


@price_bp.route("/<chain_id>/<token_address>", methods=["GET"])
@authenticate
async def get_evm_token_price(request, chain_id, token_address):
    """获取特定代币的价格信息, evm网络 (向后兼容)

    根据链ID和代币地址从Redis中获取价格数据

    URL参数:
        chain_id: 区块链ID
        token_address: 代币合约地址

    查询参数:
        fallback_source: 当主源不可用时的备用数据源，默认为 "coingecko"
        max_age: 价格数据的最大可接受年龄（以秒为单位），默认为 300 秒

    返回:
        JSON对象，包含价格信息或错误消息
    """
    try:
        try:
            max_age = int(request.args.get("max_age", 300))  # 默认5分钟
        except ValueError:
            max_age = 300  # 如果解析失败，使用默认值

        # 规范化地址（转为小写）
        token_address = token_address.lower()

        # 获取Redis客户端
        redis_client = RedisManager.get_client()

        # 从Redis获取价格数据
        key = RedisManager.get_price_key(chain_id, token_address)
        price_data_json = redis_client.get(key)

        if not price_data_json:
            logger.warning(
                "Redis中不存在链 %s 上代币 %s 的价格数据", chain_id, token_address
            )
            return sanic_json(
                {
                    "success": False,
                    "error": "Price data not found",
                    "message": f"链 {chain_id} 上的代币 {token_address} 没有价格数据",
                },
                status=404,
            )

        # 解析价格数据
        price_data = json.loads(price_data_json)

        # 检查数据是否过期
        if "timestamp" in price_data:
            timestamp = datetime.fromisoformat(price_data["timestamp"])
            age = (datetime.now() - timestamp).total_seconds()

            if age > max_age:
                logger.warning(
                    "链 %s 上代币 %s 的价格数据已过期 (%.2f秒前)",
                    chain_id,
                    token_address,
                    age,
                )
                price_data["is_stale"] = True
                price_data["age_seconds"] = age

        # 构造响应
        response = {
            "success": True,
            "chain_id": int(chain_id),
            "token_address": token_address,
            "price_data": price_data,
        }

        # 添加上次更新时间
        if "timestamp" in price_data:
            response["last_updated"] = price_data["timestamp"]
            # 计算数据年龄
            timestamp = datetime.fromisoformat(price_data["timestamp"])
            age = datetime.now() - timestamp
            response["age"] = {
                "seconds": age.total_seconds(),
                "minutes": age.total_seconds() / 60,
                "hours": age.total_seconds() / 3600,
            }

        return sanic_json(response)

    except ValueError as e:
        return sanic_json(
            {"success": False, "error": f"Invalid parameter: {str(e)}"}, status=400
        )
    except Exception as e:
        logger.exception("获取价格数据时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )


@price_bp.route("/<chain_id>/batch", methods=["POST"])
@authenticate
async def batch_get_token_prices(request, chain_id):
    """批量获取多个代币的价格信息 (EVM链，向后兼容)"""
    try:
        # 获取请求体参数
        if not request.json:
            return sanic_json(
                {"success": False, "error": "Missing request body"}, status=400
            )

        token_addresses = request.json.get("token_addresses", [])
        if not token_addresses or not isinstance(token_addresses, list):
            return sanic_json(
                {
                    "success": False,
                    "error": "Invalid or empty token_addresses parameter",
                },
                status=400,
            )

        max_age = request.json.get("max_age", 300)  # 默认5分钟

        # 获取Redis客户端
        redis_client = RedisManager.get_client()

        # 批量获取价格数据
        results = {}
        for token_address in token_addresses:
            token_address = token_address.lower()  # 规范化地址
            key = RedisManager.get_price_key(chain_id, token_address)
            price_data_json = redis_client.get(key)

            if price_data_json:
                price_data = json.loads(price_data_json)

                # 检查数据是否过期
                if "timestamp" in price_data:
                    timestamp = datetime.fromisoformat(price_data["timestamp"])
                    age = (datetime.now() - timestamp).total_seconds()

                    if age > max_age:
                        price_data["is_stale"] = True
                        price_data["age_seconds"] = age

                results[token_address] = price_data
            else:
                results[token_address] = {"error": "Price data not found"}

        # 构造响应
        response = {
            "success": True,
            "chain_id": int(chain_id),
            "price_data": results,
            "tokens_found": len([k for k, v in results.items() if "error" not in v]),
            "tokens_missing": len([k for k, v in results.items() if "error" in v]),
        }

        # 获取全局最后更新时间
        last_updated = redis_client.get(RedisManager.get_global_last_updated_key())
        if last_updated:
            response["global_last_updated"] = last_updated

        # 获取特定链的最后更新时间
        chain_last_updated = redis_client.get(
            RedisManager.get_chain_last_updated_key(chain_id)
        )
        if chain_last_updated:
            response["chain_last_updated"] = chain_last_updated

        return sanic_json(response)

    except Exception as e:
        logger.exception("批量获取价格数据时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )


@price_bp.route("/network/<network_type>/<network>/<token_address>", methods=["GET"])
@authenticate
async def get_token_price_by_network(request, network_type, network, token_address):
    """获取特定网络上代币的价格信息 (支持多链)

    根据网络类型、网络ID和代币地址从Redis中获取价格数据
    支持EVM链和非EVM网络

    URL参数:
        network_type: 网络类型，如 "evm", "aptos", "sui"
        network: 网络名称，对于EVM网络是链名称，对于非EVM网络是网络名称
        token_address: 代币合约地址或标识符

    查询参数:
        max_age: 价格数据的最大可接受年龄（以秒为单位），默认为 300 秒

    返回:
        JSON对象，包含价格信息或错误消息
    """
    try:
        # 解析和验证参数
        try:
            max_age = int(request.args.get("max_age", 300))  # 默认5分钟
        except ValueError:
            max_age = 300  # 如果解析失败，使用默认值

        # 规范化地址（转为小写）
        token_address = token_address.lower()

        # 验证网络类型
        supported_network_types = ["evm", "aptos", "sui"]
        if network_type.lower() not in supported_network_types:
            return sanic_json(
                {
                    "success": False,
                    "error": "Invalid network_type",
                    "message": f"网络类型必须是: {', '.join(supported_network_types)}",
                },
                status=400,
            )

        # 获取价格数据
        price = None
        if network_type.lower() == "evm":
            # 对于EVM网络，需要将网络名称转换为chain_id
            try:
                # 查找chain_id（可能需要实现网络名称到chain_id的映射）
                chain_id = get_chain_id_by_network_name(network)
                if chain_id:
                    price = get_token_price_usd(
                        token_address=token_address,
                        chain_id=chain_id,
                        network_type="evm",
                    )
            except Exception as e:
                logger.warning(f"EVM网络价格获取失败: {e}")
        elif network_type.lower() == "aptos":
            price = get_token_price_usd(
                token_address=token_address, network="aptos", network_type="aptos"
            )
        elif network_type.lower() == "sui":
            price = get_token_price_usd(
                token_address=token_address, network="sui-network", network_type="sui"
            )

        if price is None:
            logger.warning(
                "没有找到 %s 网络 %s 上代币 %s 的价格数据",
                network_type,
                network,
                token_address,
            )
            return sanic_json(
                {
                    "success": False,
                    "error": "Price data not found",
                    "message": f"{network_type} 网络 {network} 上的代币 {token_address} 没有价格数据",
                },
                status=404,
            )

        # 构造响应
        response = {
            "success": True,
            "network_type": network_type.lower(),
            "network": network,
            "token_address": token_address,
            "price_usd": price,
            "timestamp": datetime.now().isoformat(),
        }

        return sanic_json(response)

    except ValueError as e:
        return sanic_json(
            {"success": False, "error": f"Invalid parameter: {str(e)}"}, status=400
        )
    except Exception as e:
        logger.exception("获取价格数据时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )


@price_bp.route("/network/<network_type>/<network>/batch", methods=["POST"])
@authenticate
async def batch_get_token_prices_by_network(request, network_type, network):
    """批量获取特定网络上多个代币的价格信息 (支持多链)

    URL参数:
        network_type: 网络类型，如 "evm", "aptos", "sui"
        network: 网络名称

    请求体参数:
        token_addresses: 代币地址列表
        max_age: 价格数据的最大可接受年龄（以秒为单位），默认为 300 秒

    返回:
        JSON对象，包含批量价格信息
    """
    try:
        # 获取请求体参数
        if not request.json:
            return sanic_json(
                {"success": False, "error": "Missing request body"}, status=400
            )

        token_addresses = request.json.get("token_addresses", [])
        if not token_addresses or not isinstance(token_addresses, list):
            return sanic_json(
                {
                    "success": False,
                    "error": "Invalid or empty token_addresses parameter",
                },
                status=400,
            )

        max_age = request.json.get("max_age", 300)  # 默认5分钟

        # 验证网络类型
        supported_network_types = ["evm", "aptos", "sui"]
        if network_type.lower() not in supported_network_types:
            return sanic_json(
                {
                    "success": False,
                    "error": "Invalid network_type",
                    "message": f"网络类型必须是: {', '.join(supported_network_types)}",
                },
                status=400,
            )

        # 批量获取价格数据
        prices = {}
        if network_type.lower() == "evm":
            # 对于EVM网络
            try:
                chain_id = get_chain_id_by_network_name(network)
                if chain_id:
                    prices = get_multiple_token_prices_usd(
                        token_addresses=token_addresses,
                        chain_id=chain_id,
                        network_type="evm",
                    )
            except Exception as e:
                logger.warning(f"EVM网络批量价格获取失败: {e}")
        elif network_type.lower() == "aptos":
            prices = get_multiple_token_prices_usd(
                token_addresses=token_addresses, network="aptos", network_type="aptos"
            )
        elif network_type.lower() == "sui":
            prices = get_multiple_token_prices_usd(
                token_addresses=token_addresses,
                network="sui-network",
                network_type="sui",
            )

        # 构造响应
        response = {
            "success": True,
            "network_type": network_type.lower(),
            "network": network,
            "price_data": prices,
            "tokens_found": len([k for k, v in prices.items() if v is not None]),
            "tokens_missing": len([k for k, v in prices.items() if v is None]),
            "timestamp": datetime.now().isoformat(),
        }

        return sanic_json(response)

    except Exception as e:
        logger.exception("批量获取价格数据时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )


# Aptos 专用API端点
@price_bp.route("/aptos/<token_address>", methods=["GET"])
@authenticate
async def get_aptos_token_price(request, token_address):
    """获取Aptos代币价格 (便捷接口)

    URL参数:
        token_address: Aptos代币地址

    返回:
        JSON对象，包含价格信息
    """
    try:
        token_address = token_address.lower()
        price = get_aptos_token_price_usd(token_address)

        if price is None:
            return sanic_json(
                {
                    "success": False,
                    "error": "Price data not found",
                    "message": f"Aptos代币 {token_address} 没有价格数据",
                },
                status=404,
            )

        response = {
            "success": True,
            "network_type": "aptos",
            "network": "aptos",
            "token_address": token_address,
            "price_usd": price,
            "timestamp": datetime.now().isoformat(),
        }

        return sanic_json(response)

    except Exception as e:
        logger.exception("获取Aptos代币价格时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )


@price_bp.route("/aptos/batch", methods=["POST"])
@authenticate
async def batch_get_aptos_token_prices(request):
    """批量获取Aptos代币价格 (便捷接口)

    请求体参数:
        token_addresses: Aptos代币地址列表

    返回:
        JSON对象，包含批量价格信息
    """
    try:
        if not request.json:
            return sanic_json(
                {"success": False, "error": "Missing request body"}, status=400
            )

        token_addresses = request.json.get("token_addresses", [])
        if not token_addresses or not isinstance(token_addresses, list):
            return sanic_json(
                {
                    "success": False,
                    "error": "Invalid or empty token_addresses parameter",
                },
                status=400,
            )

        prices = get_multiple_aptos_token_prices_usd(token_addresses)

        response = {
            "success": True,
            "network_type": "aptos",
            "network": "aptos",
            "price_data": prices,
            "tokens_found": len([k for k, v in prices.items() if v is not None]),
            "tokens_missing": len([k for k, v in prices.items() if v is None]),
            "timestamp": datetime.now().isoformat(),
        }

        return sanic_json(response)

    except Exception as e:
        logger.exception("批量获取Aptos代币价格时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )


# Sui 专用API端点
@price_bp.route("/sui/<token_address>", methods=["GET"])
@authenticate
async def get_sui_token_price(request, token_address):
    """获取Sui代币价格 (便捷接口)"""
    try:
        token_address = token_address.lower()
        price = get_sui_token_price_usd(token_address)

        if price is None:
            return sanic_json(
                {
                    "success": False,
                    "error": "Price data not found",
                    "message": f"Sui代币 {token_address} 没有价格数据",
                },
                status=404,
            )

        response = {
            "success": True,
            "network_type": "sui",
            "network": "sui-network",
            "token_address": token_address,
            "price_usd": price,
            "timestamp": datetime.now().isoformat(),
        }

        return sanic_json(response)

    except Exception as e:
        logger.exception("获取Sui代币价格时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )


@price_bp.route("/status", methods=["GET"])
@authenticate
async def get_price_service_status(request):
    """获取价格服务的状态信息 (支持多链)"""
    try:
        # 获取Redis客户端
        redis_client = RedisManager.get_client()

        # 获取全局最后更新时间
        last_updated = redis_client.get(RedisManager.get_global_last_updated_key())

        # 查找所有可用的链和网络
        chain_keys = redis_client.keys("price:*:*:last_updated")
        available_networks = {}

        for key in chain_keys:
            parts = key.split(":")
            if len(parts) >= 4:
                network_flag = parts[1]  # "chain" or "network"
                network_id = parts[2]  # chain_id or network_name

                if network_flag == "chain":
                    # EVM链
                    try:
                        chain_id = int(network_id)
                        if chain_id in EVM_CHAIN_ID_NETWORK_MAP:
                            network_name = EVM_CHAIN_ID_NETWORK_MAP.get(chain_id)
                        else:
                            network_name = f"unknown-chain-{chain_id}"
                        network_type = "evm"
                    except ValueError:
                        continue
                elif network_flag == "network":
                    # 非EVM网络
                    network_name = network_id
                    # 根据网络名称推断类型
                    if "aptos" in network_name.lower():
                        network_type = "aptos"
                    elif "sui" in network_name.lower():
                        network_type = "sui"
                    else:
                        network_type = "unknown"
                else:
                    continue

                timestamp = redis_client.get(key)
                if timestamp:
                    dt = datetime.fromisoformat(timestamp)
                    age = (datetime.now() - dt).total_seconds()
                    available_networks[network_name] = {
                        "network_type": network_type,
                        "network_id": network_id,
                        "last_updated": timestamp,
                        "age_seconds": age,
                        "is_stale": age > 300,  # 5分钟
                    }

        # 构造响应
        response = {
            "success": True,
            "service_status": "operational",
            "global_last_updated": last_updated,
            "available_networks": available_networks,
            "supported_network_types": ["evm", "aptos", "sui"],
        }

        if last_updated:
            dt = datetime.fromisoformat(last_updated)
            age = (datetime.now() - dt).total_seconds()
            response["global_age_seconds"] = age
            response["is_stale"] = age > 300  # 5分钟

        return sanic_json(response)

    except Exception as e:
        logger.exception("获取价格服务状态时出错: %s", str(e))
        return sanic_json(
            {
                "success": False,
                "error": f"Internal server error: {str(e)}",
                "service_status": "error",
            },
            status=500,
        )


@price_bp.route("/tokens", methods=["POST"])
@authenticate
async def add_monitored_token(request):
    """添加需要监控价格的代币 (支持多链)

    请求体参数:
        token_data: 单个代币信息对象或代币信息对象的数组
            必需字段:
                - network: 网络名称 (字符串，如 "ethereum", "aptos", "sui")
                - token_address: 代币合约地址 (字符串)
            可选字段:
                - chain_id: 链ID (整数，仅EVM网络需要)
                - network_type: 网络类型 (字符串，默认根据network推断)
                - name: 代币名称 (字符串)
                - symbol: 代币符号 (字符串)
                - decimals: 代币精度 (整数)
                - primary_source: 主要价格来源，默认"geckoterminal" (字符串)
                - is_active: 是否主动监控，默认true (布尔值)
                - logo_url: 代币Logo URL (字符串)
                - description: 代币描述 (字符串)

    返回:
        成功: JSON对象，包含创建和更新的代币数量
        失败: JSON对象，包含错误信息
    """
    try:
        # 获取请求体数据
        if not request.json:
            return sanic_json(
                {"success": False, "error": "Missing request body"}, status=400
            )

        # 检查是单个代币还是多个代币的批量添加
        token_data = request.json

        # 处理单个代币
        if isinstance(token_data, dict):
            return await process_single_token_multichain(token_data)
        # 处理多个代币
        elif isinstance(token_data, list):
            return await process_multiple_tokens_multichain(token_data)
        else:
            return sanic_json(
                {
                    "success": False,
                    "error": "Invalid request format. Expected object or array",
                },
                status=400,
            )

    except ValueError as e:
        return sanic_json(
            {"success": False, "error": f"Invalid parameter: {str(e)}"}, status=400
        )
    except Exception as e:
        logger.exception("添加监控代币时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )


async def process_single_token_multichain(token_data):
    """处理单个代币的添加或更新请求 (支持多链)"""
    # 验证必要字段
    if "network" not in token_data or "token_address" not in token_data:
        return sanic_json(
            {
                "success": False,
                "error": "Missing required fields: network and token_address",
            },
            status=400,
        )

    # 处理token_address (转换为小写)
    network = token_data["network"]
    token_address = token_data["token_address"].lower()
    token_data["token_address"] = token_address

    # 推断网络类型
    if "network_type" not in token_data:
        token_data["network_type"] = infer_network_type(network)

    # 对于EVM网络，验证chain_id
    if token_data["network_type"] == "evm" and "chain_id" not in token_data:
        chain_id = get_chain_id_by_network_name(network)
        if chain_id:
            token_data["chain_id"] = chain_id
        else:
            return sanic_json(
                {
                    "success": False,
                    "error": f"Unable to determine chain_id for EVM network: {network}",
                },
                status=400,
            )

    # 如果缺少代币信息，尝试获取
    missing_info = not all(key in token_data for key in ["name", "symbol", "decimals"])
    if missing_info and token_data["network_type"] == "evm":
        # 仅对EVM网络尝试从链上获取
        token_info = await eth_util.fetch_token_info(
            token_data.get("chain_id"), token_address
        )
        if token_info:
            for key in ["name", "symbol", "decimals"]:
                if key not in token_data and key in token_info:
                    token_data[key] = token_info[key]

    with db_session() as db:
        try:
            # 检查代币是否已存在
            try:
                existing_token = MonitoredTokenService.get_token_by_network_address(
                    db, network, token_address, token_data.get("chain_id")
                )
                # 更新现有代币
                updated_token = MonitoredTokenService.update_token(
                    db, existing_token.id, token_data
                )
                return sanic_json(
                    {
                        "success": True,
                        "message": "Token updated successfully",
                        "token": format_token_response(updated_token),
                    }
                )
            except ResourceNotFoundException:
                # 创建新代币
                new_token = MonitoredTokenService.create_token(db, token_data)
                return sanic_json(
                    {
                        "success": True,
                        "message": "Token added successfully",
                        "token": format_token_response(new_token),
                    }
                )
        except DuplicateResourceException as e:
            return sanic_json({"success": False, "error": str(e)}, status=409)


async def process_multiple_tokens_multichain(tokens_data):
    """处理多个代币的批量添加或更新请求 (支持多链)"""
    if not tokens_data:
        return sanic_json(
            {"success": False, "error": "Empty token list provided"},
            status=400,
        )

    # 验证和预处理所有代币数据
    for i, token in enumerate(tokens_data):
        if not isinstance(token, dict):
            return sanic_json(
                {
                    "success": False,
                    "error": f"Invalid token data at index {i}",
                },
                status=400,
            )
        if "network" not in token or "token_address" not in token:
            return sanic_json(
                {
                    "success": False,
                    "error": f"Missing required fields at index {i}",
                },
                status=400,
            )

        # 预处理
        token["token_address"] = token["token_address"].lower()

        # 推断网络类型
        if "network_type" not in token:
            token["network_type"] = infer_network_type(token["network"])

        # 对于EVM网络，处理chain_id
        if token["network_type"] == "evm" and "chain_id" not in token:
            chain_id = get_chain_id_by_network_name(token["network"])
            if chain_id:
                token["chain_id"] = chain_id

    # 批量创建或更新代币
    with db_session() as db:
        try:
            result = MonitoredTokenService.batch_create_or_update_tokens_multichain(
                db, tokens_data
            )
            return sanic_json(
                {
                    "success": True,
                    "message": "Tokens processed successfully",
                    "created": result["created"],
                    "updated": result["updated"],
                }
            )
        except IntegrityError as e:
            return sanic_json(
                {
                    "success": False,
                    "error": f"Database integrity error: {str(e)}",
                },
                status=409,
            )


def get_chain_id_by_network_name(network_name: str) -> int:
    """根据网络名称获取chain_id"""
    # 网络名称到chain_id的映射
    network_to_chain_id = {
        "ethereum": 1,
        "goerli": 5,
        "sepolia": 11155111,
        "polygon": 137,
        "mumbai": 80001,
        "bsc": 56,
        "bsc-testnet": 97,
        "avalanche": 43114,
        "fuji": 43113,
        "arbitrum": 42161,
        "arbitrum-goerli": 421613,
        "optimism": 10,
        "optimism-goerli": 420,
        "hardhat": 31337,
        "localhost": 31337,
    }
    return network_to_chain_id.get(network_name.lower())


def infer_network_type(network_name: str) -> str:
    """根据网络名称推断网络类型"""
    if not network_name:
        return "evm"  # 默认为EVM

    network_name_lower = network_name.lower()

    # 明确的非EVM网络
    if network_name_lower in ["aptos"]:
        return "aptos"
    elif network_name_lower in ["sui", "sui-network"]:
        return "sui"
    elif network_name_lower in ["solana"]:
        return "solana"
    elif network_name_lower in ["flow-evm", "flow_evm"]:
        return "evm"  # Flow EVM仍然是EVM类型
    else:
        # 检查是否是已知的EVM网络名称
        evm_networks = [
            "ethereum", "eth", "mainnet",
            "polygon", "matic",
            "bsc", "binance",
            "avalanche", "avax",
            "arbitrum", "arb",
            "optimism", "opt",
            "goerli", "sepolia", "mumbai", "fuji",
            "hardhat", "localhost"
        ]

        for evm_network in evm_networks:
            if evm_network in network_name_lower:
                return "evm"

        # 如果都不匹配，默认为EVM
        logger.warning(f"未知网络类型，默认为EVM: {network_name}")
        return "evm"


def format_token_response(token):
    """格式化代币响应数据"""
    return {
        "id": token.id,
        "network": token.network,
        "network_type": token.network_type,
        "chain_id": token.chain_id,
        "token_address": token.token_address,
        "name": token.name,
        "symbol": token.symbol,
        "decimals": token.decimals,
        "is_active": token.is_active,
    }


# 保留原有的函数以向后兼容
async def process_single_token(token_data):
    """处理单个代币的添加或更新请求 (向后兼容EVM)"""
    return await process_single_token_multichain(token_data)


async def process_multiple_tokens(tokens_data):
    """处理多个代币的批量添加或更新请求 (向后兼容EVM)"""
    return await process_multiple_tokens_multichain(tokens_data)


@price_bp.route("/aptos/<token_address>/closest", methods=["POST"])
@authenticate
async def get_aptos_token_price_closest_to_timestamp(request, token_address):
    """获取距离指定时间戳最近的Aptos代币价格

    URL参数:
        token_address: Aptos代币地址

    请求体参数:
        target_timestamp: 目标时间戳，ISO格式字符串 (如: "2024-01-01T12:00:00")
        max_time_diff_minutes: 最大时间差（分钟），默认60分钟

    返回:
        JSON对象，包含最近的价格信息或错误消息
    """
    try:
        # 获取请求体参数
        if not request.json:
            return sanic_json(
                {"success": False, "error": "Missing request body"}, status=400
            )

        target_timestamp_str = request.json.get("target_timestamp")
        if not target_timestamp_str:
            return sanic_json(
                {"success": False, "error": "Missing target_timestamp parameter"},
                status=400,
            )

        max_time_diff_minutes = request.json.get("max_time_diff_minutes", 60)

        # 解析时间戳
        try:
            target_timestamp_str = target_timestamp_str.strip()

            # 处理各种时间戳格式
            if target_timestamp_str.endswith("Z"):
                # ISO格式，以Z结尾表示UTC
                target_timestamp = datetime.fromisoformat(
                    target_timestamp_str.replace("Z", "+00:00")
                )
            elif "+" in target_timestamp_str:
                # 已包含时区信息
                target_timestamp = datetime.fromisoformat(target_timestamp_str)
            else:
                # 没有时区信息，解析后添加UTC时区
                target_timestamp = datetime.fromisoformat(target_timestamp_str)
                if target_timestamp.tzinfo is None:
                    target_timestamp = target_timestamp.replace(tzinfo=timezone.utc)

        except ValueError as ve:
            return sanic_json(
                {
                    "success": False,
                    "error": "Invalid timestamp format",
                    "message": f"时间戳格式错误: {str(ve)}。请使用ISO格式如 '2024-01-01T12:00:00' 或 '2024-01-01T12:00:00Z'",
                },
                status=400,
            )

        # 规范化地址（转为小写）
        token_address = token_address.lower()

        # 从数据库查询最近的价格记录（Aptos网络固定参数）
        with db_session() as db:
            price_record = TokenPriceHistoryService.get_price_closest_to_timestamp(
                db=db,
                network="aptos",  # 固定为aptos
                token_address=token_address,
                target_timestamp=target_timestamp,
                chain_id=None,  # Aptos没有chain_id
                max_time_diff_minutes=max_time_diff_minutes,
            )

            if not price_record:
                return sanic_json(
                    {
                        "success": False,
                        "error": "No price data found",
                        "message": f"在 {target_timestamp} 前后 {max_time_diff_minutes} 分钟内未找到 Aptos 代币 {token_address} 的价格数据",
                    },
                    status=404,
                )

            # 计算时间差
            record_timestamp = price_record.timestamp
            if record_timestamp.tzinfo is None:
                record_timestamp = record_timestamp.replace(tzinfo=timezone.utc)

            time_diff = abs((record_timestamp - target_timestamp).total_seconds())
            time_diff_minutes = time_diff / 60

            # 构造响应
            response = {
                "success": True,
                "network_type": "aptos",
                "network": "aptos",
                "token_address": token_address,
                "target_timestamp": target_timestamp.isoformat(),
                "price_data": {
                    "price_usd": price_record.price_usd,
                    "source": price_record.source,
                    "timestamp": record_timestamp.isoformat(),
                    "time_difference": {
                        "seconds": time_diff,
                        "minutes": round(time_diff_minutes, 2),
                        "hours": round(time_diff_minutes / 60, 2),
                    },
                },
                "query_params": {
                    "max_time_diff_minutes": max_time_diff_minutes,
                    "actual_time_diff_minutes": round(time_diff_minutes, 2),
                },
            }

            return sanic_json(response)

    except Exception as e:
        logger.exception("查询Aptos代币最近时间币价时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )


@price_bp.route("/aptos/batch/closest", methods=["POST"])
@authenticate
async def batch_get_aptos_token_prices_closest_to_timestamp(request):
    """批量获取距离指定时间戳最近的多个Aptos代币价格

    请求体参数:
        token_addresses: Aptos代币地址列表
        target_timestamp: 目标时间戳，ISO格式字符串
        max_time_diff_minutes: 最大时间差（分钟），默认60分钟

    返回:
        JSON对象，包含批量最近价格信息
    """
    try:
        # 获取请求体参数
        if not request.json:
            return sanic_json(
                {"success": False, "error": "Missing request body"}, status=400
            )

        token_addresses = request.json.get("token_addresses", [])
        if not token_addresses or not isinstance(token_addresses, list):
            return sanic_json(
                {
                    "success": False,
                    "error": "Invalid or empty token_addresses parameter",
                },
                status=400,
            )

        target_timestamp_str = request.json.get("target_timestamp")
        if not target_timestamp_str:
            return sanic_json(
                {"success": False, "error": "Missing target_timestamp parameter"},
                status=400,
            )

        max_time_diff_minutes = request.json.get("max_time_diff_minutes", 60)

        # 解析时间戳
        try:
            target_timestamp_str = target_timestamp_str.strip()

            if target_timestamp_str.endswith("Z"):
                target_timestamp = datetime.fromisoformat(
                    target_timestamp_str.replace("Z", "+00:00")
                )
            elif "+" in target_timestamp_str:
                target_timestamp = datetime.fromisoformat(target_timestamp_str)
            else:
                target_timestamp = datetime.fromisoformat(target_timestamp_str)
                if target_timestamp.tzinfo is None:
                    target_timestamp = target_timestamp.replace(tzinfo=timezone.utc)

        except ValueError as ve:
            return sanic_json(
                {
                    "success": False,
                    "error": "Invalid timestamp format",
                    "message": f"时间戳格式错误: {str(ve)}。请使用ISO格式如 '2024-01-01T12:00:00' 或 '2024-01-01T12:00:00Z'",
                },
                status=400,
            )

        # 批量查询价格数据
        price_results = {}
        with db_session() as db:
            for token_address in token_addresses:
                token_address = token_address.lower()

                price_record = TokenPriceHistoryService.get_price_closest_to_timestamp(
                    db=db,
                    network="aptos",  # 固定为aptos
                    token_address=token_address,
                    target_timestamp=target_timestamp,
                    chain_id=None,  # Aptos没有chain_id
                    max_time_diff_minutes=max_time_diff_minutes,
                )

                if price_record:
                    record_timestamp = price_record.timestamp
                    if record_timestamp.tzinfo is None:
                        record_timestamp = record_timestamp.replace(tzinfo=timezone.utc)

                    time_diff = abs(
                        (record_timestamp - target_timestamp).total_seconds()
                    )
                    time_diff_minutes = time_diff / 60

                    price_results[token_address] = {
                        "price_usd": price_record.price_usd,
                        "source": price_record.source,
                        "timestamp": record_timestamp.isoformat(),
                        "time_difference": {
                            "seconds": time_diff,
                            "minutes": round(time_diff_minutes, 2),
                            "hours": round(time_diff_minutes / 60, 2),
                        },
                    }
                else:
                    price_results[token_address] = {
                        "error": "Price data not found within time range"
                    }

        # 构造响应
        response = {
            "success": True,
            "network_type": "aptos",
            "network": "aptos",
            "target_timestamp": target_timestamp.isoformat(),
            "price_data": price_results,
            "tokens_found": len(
                [k for k, v in price_results.items() if "error" not in v]
            ),
            "tokens_missing": len(
                [k for k, v in price_results.items() if "error" in v]
            ),
            "query_params": {"max_time_diff_minutes": max_time_diff_minutes},
        }

        return sanic_json(response)

    except Exception as e:
        logger.exception("批量查询Aptos代币最近时间币价时出错: %s", str(e))
        return sanic_json(
            {"success": False, "error": f"Internal server error: {str(e)}"}, status=500
        )
