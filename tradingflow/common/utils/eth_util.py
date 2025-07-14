import asyncio
import json
import logging
import os
from typing import Any, Dict, Optional

import requests
from eth_abi.codec import ABICodec
from eth_utils.abi import event_abi_to_log_topic
from web3 import AsyncWeb3, Web3
from web3.providers.rpc import AsyncHTTPProvider

from tradingflow.common.config import CONFIG, TRADING_FLOW_ROOT
from tradingflow.common.constants import VAULT_CONTRACT_FILE_PATH, erc20_abi, get_network_info_by_name, EVM_CHAIN_ID_NETWORK_MAP
from tradingflow.common.logging_config import setup_logging

# Setup logging
setup_logging(CONFIG, "eth_util")
logger = logging.getLogger(__name__)

# Cache loaded ABIs and contracts
_abi_cache = {}
_w3_instances = {}
_contract_interfaces = {}  # Cache contract objects for decoding

# ROLE HASHES
ORACLE_ROLE = f'0x{Web3.keccak(text="ORACLE_ROLE").hex()}'
STRATEGY_MANAGER_ROLE = f'0x{Web3.keccak(text="STRATEGY_MANAGER_ROLE").hex()}'
DEFAULT_ADMIN_ROLE = (
    "0x0000000000000000000000000000000000000000000000000000000000000000"
)

ROLE_HASHES_TO_NAMES = {
    ORACLE_ROLE: "ORACLE_ROLE",
    STRATEGY_MANAGER_ROLE: "STRATEGY_MANAGER_ROLE",
    DEFAULT_ADMIN_ROLE: "DEFAULT_ADMIN_ROLE",
}


def get_w3_instance(chain_id: int) -> AsyncWeb3 | None:
    """Gets a cached Web3 instance for the given chain_id."""
    chain_id_str = str(chain_id)
    rpc_urls = CONFIG.get("CHAIN_RPC_URLS")
    if chain_id_str not in _w3_instances:
        if chain_id_str not in rpc_urls:
            logger.error(f"RPC URL for chain ID {chain_id} not configured.")
            return None

        provider = AsyncHTTPProvider(rpc_urls[chain_id_str])
        w3 = AsyncWeb3(provider)

        # 如果需要PoA中间件，请参考AsyncWeb3的文档添加
        _w3_instances[chain_id_str] = w3

    return _w3_instances[chain_id_str]


def load_abi(abi_name: str) -> list | None:
    """Loads ABI from JSON file."""
    if abi_name not in _abi_cache:
        abi_path = TRADING_FLOW_ROOT / VAULT_CONTRACT_FILE_PATH["EVM"]["path"]
        if not abi_path or not os.path.exists(abi_path):
            logger.error(f"ABI file not found for '{abi_name}' at {abi_path}")
            return None
        try:
            with open(abi_path, "r") as f:
                _abi_cache[abi_name] = json.load(f)["abi"]
        except (json.JSONDecodeError, KeyError, FileNotFoundError) as e:
            logger.error(f"Error loading ABI '{abi_name}': {e}")
            return None
    return _abi_cache[abi_name]


def get_contract_interface(w3: Web3, abi_name: str):
    """Gets a contract object (without address) for event decoding."""
    if abi_name not in _contract_interfaces:
        abi = load_abi(abi_name)
        if not abi:
            return None
        _contract_interfaces[abi_name] = w3.eth.contract(abi=abi)
    return _contract_interfaces[abi_name]


def get_event_topic_map(abi: list) -> dict[bytes, dict]:
    """Creates a map from event topic hash to event ABI."""
    topic_map = {}
    for item in abi:
        if item.get("type") == "event":
            topic = event_abi_to_log_topic(item)
            topic_map[topic] = item
    return topic_map


def decode_event_log(codec: ABICodec, event_abi: dict, log: dict) -> dict:
    """Decodes event log data using ABI."""
    # 获取事件名称，用于日志
    event_name = event_abi.get("name", "UnknownEvent")

    # 分离indexed和non-indexed输入
    indexed_inputs = [inp for inp in event_abi["inputs"] if inp["indexed"]]
    non_indexed_inputs = [inp for inp in event_abi["inputs"] if not inp["indexed"]]

    # 解码indexed topics
    decoded_indexed = {}

    if len(log["topics"]) > 1:  # Topic 0是事件签名
        # 单独解码每个主题，而不是一次性合并解码
        for i, topic in enumerate(log["topics"][1:]):
            if i >= len(indexed_inputs):
                logger.warning(f"Event {event_name}: More topics than indexed inputs")
                break

            inp = indexed_inputs[i]
            topic_type = inp["type"]

            try:
                # 将主题格式化为适当的字节
                if hasattr(topic, "hex"):
                    topic_bytes = bytes.fromhex(topic.hex()[2:])
                else:
                    topic_str = topic[2:] if topic.startswith("0x") else topic
                    topic_bytes = bytes.fromhex(topic_str)

                # 对主题进行校正（某些类型如string、bytes在indexed时处理不同）
                if (
                    topic_type == "string"
                    or topic_type.startswith("bytes")
                    and topic_type != "bytes32"
                ):
                    # 对于string和动态bytes，主题是一个keccak256哈希
                    decoded_indexed[inp["name"]] = topic_bytes
                else:
                    # 对于其他类型，尝试解码
                    try:
                        # 填充到32字节（如果需要）
                        if len(topic_bytes) < 32:
                            padded_bytes = topic_bytes.rjust(32, b"\x00")
                        else:
                            padded_bytes = topic_bytes

                        decoded_value = codec.decode([topic_type], padded_bytes)[0]
                        decoded_indexed[inp["name"]] = decoded_value
                    except Exception as e:
                        logger.debug(
                            f"Event {event_name}: Failed to decode topic {i+1} as {topic_type}: {e}"
                        )
                        # 保存原始字节
                        decoded_indexed[inp["name"]] = topic_bytes
            except Exception as e:
                logger.debug(f"Event {event_name}: Error processing topic {i+1}: {e}")
                decoded_indexed[inp["name"]] = topic

    # 解码non-indexed数据
    decoded_data = {}

    if non_indexed_inputs and log["data"] and log["data"] != "0x":
        data_inputs = [inp["type"] for inp in non_indexed_inputs]

        # 处理data，确保它是字节类型
        if hasattr(log["data"], "hex"):  # HexBytes对象
            data_hex = log["data"].hex()[2:]  # 去掉'0x'前缀
        else:  # 字符串
            data_hex = log["data"][2:] if log["data"].startswith("0x") else log["data"]

        # 解码数据，宽松模式
        try:
            # 宽松模式，使用自定义解码器或容错机制
            decoded_values = codec.decode(data_inputs, bytes.fromhex(data_hex))
            for i, inp in enumerate(non_indexed_inputs):
                decoded_data[inp["name"]] = decoded_values[i]
        except Exception as e:
            logger.debug(f"Event {event_name}: Error decoding data: {e}")
            logger.debug(
                f"Data hex: {data_hex[:64]}{'...' if len(data_hex) > 64 else ''}"
            )
            logger.debug(f"Data inputs: {data_inputs}")

            # 尝试单独解码每个字段
            try:
                # 解析出数据段，按照ABI规则
                data_bytes = bytes.fromhex(data_hex)
                offset = 0
                for i, inp in enumerate(non_indexed_inputs):
                    try:
                        # 固定大小类型直接解码
                        if inp["type"] in ["uint256", "int256", "address", "bool"]:
                            if offset + 32 <= len(data_bytes):
                                field_data = data_bytes[offset : offset + 32]
                                decoded_value = codec.decode([inp["type"]], field_data)[
                                    0
                                ]
                                decoded_data[inp["name"]] = decoded_value
                                offset += 32
                        # 对于其他类型，至少保存原始数据
                        else:
                            decoded_data[inp["name"]] = (
                                f"0x{data_hex[offset*2:(offset+32)*2]}"
                            )
                            offset += 32
                    except Exception:
                        decoded_data[inp["name"]] = None
            except Exception:
                # 如果都失败，保存整个数据
                decoded_data["_rawData"] = f"0x{data_hex}"

    # 合并结果
    event_data = {**decoded_indexed, **decoded_data}

    # 将字节类型转换为十六进制字符串以便于JSON序列化
    for key, value in event_data.items():
        if isinstance(value, bytes):
            event_data[key] = Web3.to_hex(value)

    # 添加元数据
    event_data["_eventName"] = event_name

    return event_data


def normalize_address(address: str, web3_instance=None) -> str:
    """
    将地址规范化为校验和格式

    参数:
        address: 以太坊地址字符串
        web3_instance: Web3实例，如果不提供则使用一个全局Web3实例

    返回:
        规范化的校验和地址
    """
    if web3_instance is None:
        # 创建一个默认的Web3实例
        web3_instance = get_w3_instance(1)  # 使用主网的RPC URL

    # 确保地址是字符串
    if not isinstance(address, str):
        address = str(address)

    # 如果地址已经包含大写字母，则假设它可能已经是校验和格式
    if address.lower() != address:
        # 验证地址是否真的是有效的校验和
        if web3_instance.is_checksum_address(address):
            return address

    # 将地址转换为校验和格式
    return web3_instance.to_checksum_address(address)


def normalize_addresses(addresses: list, web3_instance=None) -> list:
    """批量规范化地址列表"""
    return [normalize_address(addr, web3_instance) for addr in addresses]


async def is_contract_address(chain_id: int, address: str) -> bool:
    """
    检查地址是否是合约地址（即是否有部署过合约代码）

    Args:
        chain_id: 区块链ID
        address: 要检查的地址

    Returns:
        如果地址是合约地址则返回True，否则返回False
    """
    try:
        # 获取Web3实例
        w3 = get_w3_instance(chain_id)
        if not w3:
            logger.error("无法获取链 %s 的Web3实例", chain_id)
            return False

        # 规范化地址
        address = normalize_address(address, w3)

        # 获取地址的代码
        code = await w3.eth.get_code(address)

        # 如果代码长度大于2（"0x"），则说明这是一个合约地址
        # 普通账户地址的代码为 "0x"
        return len(code) > 2

    except Exception as e:
        logger.error("检查合约地址时出错: %s - %s", address, e)
        return False


async def fetch_token_info(chain_id, token_address):
    """
    从区块链获取代币信息

    Args:
        chain_id: 区块链ID
        token_address: 代币合约地址

    Returns:
        包含name、symbol和decimals的字典，如果获取失败则返回None
    """
    try:
        # 获取异步Web3实例
        w3 = get_w3_instance(chain_id)
        if not w3:
            logger.error(f"无法获取链 {chain_id} 的Web3实例")
            raise ValueError("无效的链ID或RPC URL, chain_id: %s", chain_id)
        # 规范化地址
        token_address = normalize_address(token_address, w3)
        # 检查是不是合约地址
        if not await is_contract_address(chain_id, token_address):
            logger.error(f"地址 {token_address} 不是合约地址")
            return None

        # 创建合约实例
        contract = w3.eth.contract(address=token_address, abi=erc20_abi)

        # 批量获取代币信息
        results = await asyncio.gather(
            contract.functions.name().call(),
            contract.functions.symbol().call(),
            contract.functions.decimals().call(),
            return_exceptions=True,
        )

        # 处理结果
        name = results[0] if not isinstance(results[0], Exception) else None
        symbol = results[1] if not isinstance(results[1], Exception) else None
        decimals = (
            results[2] if not isinstance(results[2], Exception) else 18
        )  # 默认18位小数

        token_info = {}
        if name:
            token_info["name"] = name
        if symbol:
            token_info["symbol"] = symbol
        if decimals is not None:
            token_info["decimals"] = decimals

        return token_info
    except Exception as e:
        logger.warning(f"获取代币信息失败: {str(e)}")
        return None


def fetch_vault_data_from_api(
    chain_id: int, vault_address: str, network: str = None, network_type: str = None
) -> Optional[Dict[str, Any]]:
    """
    从API获取Vault数据

    Args:
        chain_id: 链ID
        vault_address: Vault地址
        network: 网络名称（如'aptos', 'ethereum'等）
        network_type: 网络类型（如'evm', 'aptos'等）

    Returns:
        Vault数据字典, 包括持仓信息，金库价值等，
        如果获取失败则返回None
    """
    try:
        # 获取API配置
        from tradingflow.common.config import CONFIG
        from tradingflow.common.constants import get_network_info_by_name, EVM_CHAIN_ID_NETWORK_MAP

        account_manager_host = CONFIG.get("ACCOUNT_MANAGER_HOST", "localhost")
        account_manager_port = CONFIG.get("ACCOUNT_MANAGER_PORT", 7001)
        api_base_url = f"http://{account_manager_host}:{account_manager_port}"

        # 确定网络类型和网络名称
        if not network_type or not network:
            if chain_id in EVM_CHAIN_ID_NETWORK_MAP:
                network = EVM_CHAIN_ID_NETWORK_MAP[chain_id]
                network_type = "evm"
            else:
                logger.warning("无法确定chain_id %s对应的网络类型", chain_id)
                # 默认尝试EVM格式
                network_type = "evm"
                network = f"chain_{chain_id}"
        else:
            # 标准化网络信息
            network, network_type = get_network_info_by_name(network)

        # 根据网络类型构建不同的API URL
        if network_type == "evm":
            url = f"{api_base_url}/evm/vaults/{chain_id}/{vault_address}/portfolio"
        elif network_type == "aptos":
            url = f"{api_base_url}/aptos/vaults/{vault_address}/portfolio"
        elif network_type == "sui":
            url = f"{api_base_url}/sui/vaults/{vault_address}/portfolio"
        elif network_type == "solana":
            url = f"{api_base_url}/solana/vaults/{vault_address}/portfolio"
        else:
            logger.warning("不支持的网络类型: %s，使用默认EVM格式", network_type)
            url = f"{api_base_url}/evm/vaults/{chain_id}/{vault_address}"

        headers = {"Content-Type": "application/json"}

        logger.debug("获取Vault数据: %s (网络: %s, 类型: %s)", url, network, network_type)

        # 发送请求
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("vault"):
                return data["vault"]
            else:
                logger.warning("API返回错误: %s", data.get('error', '未知错误'))
                return None
        else:
            logger.warning("API请求失败，状态码: %d, 响应: %s",
                         response.status_code, response.text[:200])
            return None

    except Exception as e:
        logger.error("从API获取Vault数据时出错: %s", e)
        return None


async def get_transaction_gas_info_async(
    chain_id: int, tx_hash: str
) -> Optional[Dict[str, Any]]:
    """
    异步获取交易的Gas使用信息

    Args:
        chain_id: 区块链ID
        tx_hash: 交易哈希

    Returns:
        包含gas_used、gas_price、total_cost等信息的字典，如果获取失败则返回None
    """
    try:
        # 获取Web3实例
        w3 = get_w3_instance(chain_id)
        if not w3:
            logger.error("无法获取链 %s 的Web3实例", chain_id)
            return None

        # 规范化交易哈希
        if not tx_hash.startswith("0x"):
            tx_hash = f"0x{tx_hash}"

        # 异步获取交易收据
        tx_receipt = await w3.eth.get_transaction_receipt(tx_hash)

        # 异步获取交易信息（获取gas_price）
        tx = await w3.eth.get_transaction(tx_hash)

        if not tx_receipt or not tx:
            logger.warning("找不到交易信息: %s", tx_hash)
            return None

        # 获取gas_price (优先使用effectiveGasPrice，这在EIP-1559中更准确)
        gas_price = getattr(tx_receipt, "effectiveGasPrice", None)
        if gas_price is None:
            gas_price = tx.get("gasPrice")

        # 计算总成本
        gas_used = tx_receipt.gasUsed
        total_cost_wei = gas_used * gas_price
        total_cost_eth = w3.from_wei(total_cost_wei, "ether")

        # 获取基础费和小费（仅EIP-1559交易）
        base_fee_per_gas = None
        max_fee_per_gas = getattr(tx, "maxFeePerGas", None)
        max_priority_fee_per_gas = getattr(tx, "maxPriorityFeePerGas", None)

        # 获取交易所在区块的基础费
        if hasattr(tx_receipt, "blockNumber") and tx_receipt.blockNumber:
            try:
                block = await w3.eth.get_block(tx_receipt.blockNumber)
                base_fee_per_gas = getattr(block, "baseFeePerGas", None)
            except Exception as e:
                logger.debug("获取区块信息失败: %s", e)

        # 构建结果字典
        result = {
            "gas_used": gas_used,
            "gas_price": gas_price,
            "total_cost_wei": total_cost_wei,
            "total_cost_eth": float(total_cost_eth),
            "block_number": tx_receipt.blockNumber,
            "transaction_hash": tx_hash,
        }

        # 添加EIP-1559相关信息（如果有）
        if base_fee_per_gas:
            result["base_fee_per_gas"] = base_fee_per_gas
        if max_fee_per_gas:
            result["max_fee_per_gas"] = max_fee_per_gas
        if max_priority_fee_per_gas:
            result["max_priority_fee_per_gas"] = max_priority_fee_per_gas

        return result

    except Exception as e:
        logger.error("获取交易Gas信息失败: %s", e)
        return None


async def get_native_token_address(chain_id: int) -> str:
    """
    获取链的原生代币地址

    Args:
        chain_id: 区块链ID

    Returns:
        原生代币的地址表示
    """
    # 常见原生代币的表示地址（通常使用零地址或特殊地址表示）
    NATIVE_TOKEN_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

    # 为不同链的原生代币返回正确的表示地址
    CHAIN_NATIVE_TOKENS = {
        # 主要链的原生代币标识
        1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # Ethereum - wETH
        56: NATIVE_TOKEN_ADDRESS,  # Binance Smart Chain - BNB
        137: NATIVE_TOKEN_ADDRESS,  # Polygon - MATIC
        43114: NATIVE_TOKEN_ADDRESS,  # Avalanche - AVAX
        42161: NATIVE_TOKEN_ADDRESS,  # Arbitrum - ETH
        10: NATIVE_TOKEN_ADDRESS,  # Optimism - ETH
        250: NATIVE_TOKEN_ADDRESS,  # Fantom - FTM
        # 测试网
        5: NATIVE_TOKEN_ADDRESS,  # Goerli Testnet - ETH
        80001: NATIVE_TOKEN_ADDRESS,  # Mumbai Testnet - MATIC
        97: NATIVE_TOKEN_ADDRESS,  # BSC Testnet - BNB
        # 本地测试网
        31337: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # Hardhat local network - ETH
    }

    return CHAIN_NATIVE_TOKENS.get(chain_id, NATIVE_TOKEN_ADDRESS)


if __name__ == "__main__":
    # 测试代码
    chain_id = 31337
    transaction_hash = (
        "7bef7b4a945666b42cc49bc6e96f67d2f2ee2c8c7ad67c8abf2af7db8f263723"
    )

    gas_info = asyncio.run(get_transaction_gas_info_async(chain_id, transaction_hash))
    if gas_info:
        logger.info("Gas Info: %s", gas_info)
    else:
        logger.info("Failed to fetch gas info.")
