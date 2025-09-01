import json
import logging
from typing import Dict, List, Optional

from tradingflow.depot.python.db import db_session
from tradingflow.depot.python.db.services.monitored_token_service import MonitoredTokenService
from tradingflow.depot.python.utils.redis_manager import RedisManager

logger = logging.getLogger(__name__)

_aptos_token_symbol_to_address = {
    "aBTC": "0xf599112bc3a5b6092469890d6a2f353f485a6193c9d36626b480704467d3f4c8",
    "amAPT": "0xa259be733b6a759909f92815927fa213904df6540519568692caf0b068fe8e62",
    "AMI": "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451",
    "ANI": "0x9660042a7c01d776938184278381d24c7009ca385d9a59cf9b22691f97615960",
    "APE": "0xfad230e7d9df2baf83a68b6f50217ed3c06da593e766970a885965b43b894a04",
    "APT": "0xa",
    "AURO": "0xbcff91abababee684b194219ff2113c26e63d57c8872e6fdaf25a41a45fb7197",
    "AWUSDt": "0x173f3bc501dfe7e4cac8e4463cb277e205e9a299380bc22a87ce455f014f3fb7",
    "BAPTMAN": "0xe9c6ae7a056ba49901fcc19ab3fcff0938f882cfd7f2cc5a72eea362d29f5b8f",
    "BNB": "0x3fb0cd30095fc85c77eb5cb9edcdbead3cef34e449b1a6f07729282120bc6383",
    "BUBBLES": "0xaca80bdba3a9f58af0c6348f15530e4d891d1c60abca4c2cfb4c1a73bff0f8dd",
    "BUSD": "0xd47b65c45f5260c4f3c5de3f32ddaeabf7eab56c9493a7a955dff7f70ba8afaf",
    "Cake": "0xad18575b0e51dd056e1e082223c0e014cbfe4b13bc55e92f450585884f4cf951",
    "CAPTOS": "0x7fa78d58cccc849363df4ed1acd373b1f09397d1c322450101e3b0a4a7a14d80",
    "CASH": "0xc692943f7b340f02191c5de8dac2f827e0b66b3ed2206206a3526bcb0cae6e40",
    "CATTOS  ": "0xeeb5ba9616292d315edc8ce36a25b921bab879b2a7088d479d12b0c182bd28c8",
    "CELL": "0x2ebb2ccac5e027a87fa0e2e5f656a3a4238d6a48d93ec9b610d570fc0aa0df12",
    "CHEWY": "0x1fe81b3886ff129d42064f7ee934013de7ef968cb8f47adb5f7210192bcd4a23",
    "DAI": "0x59c3a92ab1565a14d4133eb2a3418604341d37da47698a0e853c7bb22c342c55",
    "DONK": "0x41944cf1d4dac152d692644944e2cc49ee81fafdfb37abd541d06388ec3f7eda",
    "doodoo": "0xb27b0c6b60772f0fc804ec1cd3339f552badf9bd1e125a7dd700d8eb11248ef1",
    "ECHO": "0xb2c7780f0a255a6137e5b39733f5a4c85fe093c549de5c359c1232deef57d1b7",
    "EDOG": "0x1ff8bf54987b665fd0aa8b317a22a60f5927675d35021473a85d720e254ed77e",
    "ETERN": "0x570e7e86130982704afcbb0e860eff90b4af97fb00b69bf7f0f71ddeee5697ee",
    "FIABTC": "0x75de592a7e62e6224d13763c392190fda8635ebb79c798a5e9dd0840102f3f93",
    "GUI": "0x0009da434d9b873b5159e8eeed70202ad22dc075867a7793234fbc981b63e119",
    "kAPT": "0x821c94e69bc7ca058c913b7b5e6b0a5c9fd1523d58723a966fb8c1f5ea888105",
    "LOON": "0x41dfe1fb3d33d4d9d0b460f03ce1c0a6af6520dd8bdc0f204583c4987faf81de",
    "MGPT": "0x4c3efb98d8d3662352f331b3465c6df263d1a7e84f002844348519614a5fea30",
    "MKL": "0x878370592f9129e14b76558689a4b570ad22678111df775befbfcbc9fb3d90ab",
    "MOD": "0x94ed76d3d66cb0b6e7a3ab81acf830e3a50b8ae3cfb9edc0abea635a11185ff4",
    "MODENG": "0x5dd9b0b5eea4d2bec9c8cabed2f1db7062fb6ed16471fd52de491426718b0d95",
    "MOVE": "0x96d1ccca420ebc20fc8af6cacb864e44856ca879c6436d4e9be2b0a4b99bf852",
    "MOVEPUMP": "0xa067de5ce739da1400a92945646f719a0265df6412ddab872cd670052c5cc12f",
    "PEPE": "0x08bbc1e07f934be0862be6df1477dbab54d6ccdf594f1569a64fa2941cbfe368",
    "PROPS": "0x6dba1728c73363be1bdd4d504844c40fbb893e368ccbeff1d1bd83497dbc756d",
    "RION": "0x435ad41e7b383cef98899c4e5a22c8dc88ab67b22f95e5663d6c6649298c3a9d",
    "SBTC": "0xef1f3c4126176b1aaff3bf0d460a9344b64ac4bd28ff3e53793d49ded5c2d42f",
    "Sidelined": "0x073992b487d517a8fc710acf953248b2045e381e5eb6fd9a92828db64a269530",
    "SLT": "0xd1c452f47abeae87027758de85520a1957a5e5a82cfd709c8d762e904b6fe043",
    "stAPT": "0xb614bfdf9edc39b330bbf9c3c5bcd0473eee2f6d4e21748629cc367869ece627",
    "sthAPT": "0x0a9ce1bddf93b074697ec5e483bc5050bc64cff2acd31e1ccfd8ac8cae5e4abe",
    "stkAPT": "0x42556039b88593e768c97ab1a3ab0c6a17230825769304482dff8fdebe4c002b",
    "STONE": "0x700e285ee9f4fc9b0e42a6217e329899e1353476bc532a484048008c8bc8e400",
    "sUSDa": "0xda3e4d44156dce34efda861201eab1164c88b18c49c928a97caa2904be985ebf",
    "sUSDe": "0xb30a694a344edee467d9f82330bbe7c3b89f440a1ecd2da1f3bca266560fce69",
    "tAPT": "0x1912eb1a5f8f0d4c72c1687eb199b7f9d2df34da5931ec96830c557f7abaa0ad",
    "thAPT": "0xa0d9d647c5737a5aed08d2cfeb39c31cf901d44bc4aa024eaa7e5e68b804e011",
    "THL": "0x377adc4848552eb2ea17259be928001923efe12271fef1667e2b784f04a7cf3a",
    "TruAPT": "0xaef6a8c3182e076db72d64324617114cacf9a52f28325edc10b483f7f05da0e7",
    "USDa": "0xace541cbd9b5d60f38cf545ac27738353f70b4f9b970c37a54cf7acfd19dad76",
    "USDA": "0x534e4c3dc0f038dab1a8259e89301c4da58779a5d482fb354a41c08147e6b9ec",
    "USDC": "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",
    "USDD": "0xde368b120e750bbb0d8799356ea23511306ff19edd28eed15fe7b6cc72b04388",
    "USDt": "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b",
    "USDT": "0xe568e9322107a5c9ba4cbd05a630a5586aa73e744ada246c3efb0f4ce3e295f3",
    "USDY": "0xf0876baf6f8c37723f0e9d9c1bbad1ccb49324c228bcc906e2f1f5a9e139eda1",
    "WBTC": "0xa64d2d6f5e26daf6a3552f51d4110343b1a8c8046d0a9e72fa4086a337f3236c",
    "WETH": "0xae02f68520afd221a5cd6fda6f5500afedab8d0a2e19a916d6d8bc2b36e758db",
    "wUSDM": "0x08993fd61f611eb186386962edec50c9d72532b3825f1c7d98e883268dbbc501",
    "xBTC": "0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387",
}

# 内存缓存字典
_token_info_cache = {}


def get_aptos_token_address_by_symbol(symbol: str) -> Optional[str]:
    """
    根据Aptos代币符号获取其地址

    Args:
        symbol: 代币符号（如 'APT', 'USDC'）

    Returns:
        成功时返回代币地址，失败时返回None
    """
    return _aptos_token_symbol_to_address.get(symbol, None)


def _get_token_info_cache_key(
    token_address: str, network_type: str, network_or_chain: str
) -> str:
    """生成代币信息缓存键"""
    token_address = token_address.lower()
    return f"{network_type}:{network_or_chain}:{token_address}"


def get_monitored_token_info(
    token_address: str,
    network: Optional[str] = None,
    chain_id: Optional[int] = None,
    network_type: str = "evm",
    use_cache: bool = True,
) -> Optional[Dict]:
    """
    获取监控代币的基本信息（支持内存缓存）

    Args:
        token_address: 代币合约地址
        network: 网络名称（非EVM链使用，如'aptos', 'sui'）
        chain_id: 区块链ID (EVM链使用，如以太坊主网为1)
        network_type: 网络类型，'evm' 或 'aptos' 或 'sui'
        use_cache: 是否使用缓存，默认True

    Returns:
        包含代币基本信息的字典，失败时返回None
    """
    try:
        # 规范化地址（转为小写）
        token_address = token_address.lower()

        # 参数验证
        if network_type.lower() == "evm" and chain_id is None:
            raise ValueError("EVM网络必须提供chain_id")
        elif network_type.lower() != "evm" and network is None:
            raise ValueError(f"非EVM网络({network_type})必须提供network参数")

        # 构建缓存键
        network_or_chain = str(chain_id) if network_type.lower() == "evm" else network
        cache_key = _get_token_info_cache_key(
            token_address, network_type, network_or_chain
        )

        # 尝试从内存缓存获取
        if use_cache and cache_key in _token_info_cache:
            logger.debug(
                f"从内存缓存获取到代币信息 - 网络类型: {network_type}, "
                f"网络: {network_or_chain}, 地址: {token_address}, "
                f"符号: {_token_info_cache[cache_key].get('symbol')}"
            )
            return _token_info_cache[cache_key].copy()  # 返回副本避免被修改

        # 从数据库查询
        with db_session() as db:
            try:
                if network_type.lower() == "evm":
                    token = MonitoredTokenService.get_token_by_evm_address(
                        db, token_address, chain_id
                    )
                else:
                    token = MonitoredTokenService.get_token_by_address(
                        db, token_address, network
                    )

                # 构建返回的基本信息
                token_info = {
                    "id": token.id,
                    "token_address": token.token_address,
                    "network": token.network,
                    "network_type": token.network_type,
                    "chain_id": token.chain_id,
                    "name": token.name,
                    "symbol": token.symbol,
                    "decimals": token.decimals,
                    "description": token.description,
                    "is_active": token.is_active,
                    "created_at": (
                        token.created_at.isoformat() if token.created_at else None
                    ),
                    "updated_at": (
                        token.updated_at.isoformat() if token.updated_at else None
                    ),
                }

                # 存储到内存缓存
                if use_cache:
                    _token_info_cache[cache_key] = token_info.copy()
                    logger.debug(f"代币信息已缓存到内存 - 键: {cache_key}")

                logger.debug(
                    f"从数据库获取到代币信息 - 网络类型: {network_type}, "
                    f"网络: {network_or_chain}, 地址: {token_address}, "
                    f"符号: {token.symbol}, 名称: {token.name}"
                )

                return token_info

            except Exception as e:
                logger.warning(
                    f"未找到监控代币 - 网络类型: {network_type}, "
                    f"网络: {network_or_chain}, 地址: {token_address}, 错误: {e}"
                )
                return None

    except Exception as e:
        logger.exception(f"获取监控代币信息时出错: {e}")
        return None


def get_aptos_monitored_token_info(token_address: str) -> Optional[Dict]:
    """
    获取Aptos监控代币的基本信息

    Args:
        token_address: Aptos代币地址

    Returns:
        包含代币基本信息的字典，失败时返回None
    """
    return get_monitored_token_info(
        token_address=token_address, network="aptos", network_type="aptos"
    )


def get_token_price_usd(
    token_address: str,
    chain_id: Optional[int] = None,
    network: Optional[str] = None,
    network_type: str = "evm",
) -> Optional[float]:
    """
    从Redis获取指定代币的USD价格（支持多链）

    Args:
        token_address: 代币合约地址
        chain_id: 区块链ID (EVM链使用，如以太坊主网为1)
        network: 网络名称（非EVM链使用，如'aptos', 'sui'）
        network_type: 网络类型，'evm' 或 'aptos' 或 'sui'

    Returns:
        成功时返回USD价格（浮点数），失败时返回None
    """
    try:
        # 规范化地址（转为小写）
        token_address = token_address.lower()

        # 获取Redis客户端
        redis_client = RedisManager.get_client()

        # 根据网络类型构建Redis键
        if network_type.lower() == "evm":
            if chain_id is None:
                raise ValueError("EVM网络必须提供chain_id")
            key = RedisManager.get_price_key(chain_id, token_address)
        else:
            if network is None:
                raise ValueError(f"非EVM网络({network_type})必须提供network参数")
            key = RedisManager.get_token_price_key(network_type, network, token_address)

        logger.debug(f"从Redis获取代币价格 - 键: {key}")

        # 从Redis获取数据
        price_data_json = redis_client.get(key)

        if not price_data_json:
            logger.warning(
                f"Redis中没有找到代币价格数据 - 网络类型: {network_type}, "
                f"网络: {network or chain_id}, 地址: {token_address}"
            )
            return None

        # 解析价格数据
        price_data = json.loads(price_data_json)

        # 直接获取price_usd字段
        if "price_usd" in price_data:
            usd_price = float(price_data["price_usd"])
            logger.info(
                f"获取到代币价格 - 网络类型: {network_type}, "
                f"网络: {network or chain_id}, 地址: {token_address}, USD价格: {usd_price}"
            )
            return usd_price

        logger.warning(f"无法从Redis数据中提取price_usd: {price_data}")
        return None

    except Exception as e:
        logger.exception(f"从Redis获取代币价格时出错: {e}")
        return None


def get_multiple_token_prices_usd(
    token_addresses: List[str],
    chain_id: Optional[int] = None,
    network: Optional[str] = None,
    network_type: str = "evm",
) -> Dict[str, Optional[float]]:
    """
    批量从Redis获取多个代币的USD价格（支持多链）

    Args:
        token_addresses: 代币地址列表
        chain_id: 区块链ID (EVM链使用)
        network: 网络名称（非EVM链使用）
        network_type: 网络类型，'evm' 或 'aptos' 或 'sui'

    Returns:
        代币地址到USD价格的映射字典，如果某个代币获取失败，其值为None
    """
    try:
        # 如果列表为空，直接返回空字典
        if not token_addresses:
            return {}

        # 验证参数
        if network_type.lower() == "evm" and chain_id is None:
            raise ValueError("EVM网络必须提供chain_id")
        elif network_type.lower() != "evm" and network is None:
            raise ValueError(f"非EVM网络({network_type})必须提供network参数")

        # 规范化所有地址（转为小写）
        normalized_addresses = [addr.lower() for addr in token_addresses]

        # 获取Redis客户端
        redis_client = RedisManager.get_client()

        # 构建所有Redis键
        if network_type.lower() == "evm":
            keys = [
                RedisManager.get_price_key(chain_id, addr)
                for addr in normalized_addresses
            ]
        else:
            keys = [
                RedisManager.get_token_price_key(network_type, network, addr)
                for addr in normalized_addresses
            ]

        logger.debug(
            f"批量从Redis获取代币价格 - 网络类型: {network_type}, "
            f"网络: {network or chain_id}, 数量: {len(keys)}"
        )

        # 使用Redis pipeline批量获取数据
        pipeline = redis_client.pipeline()
        for key in keys:
            pipeline.get(key)

        # 执行批量获取
        price_data_list = pipeline.execute()

        # 处理结果
        result = {}
        for i, addr in enumerate(normalized_addresses):
            price_data_json = price_data_list[i]

            if not price_data_json:
                logger.warning(
                    f"Redis中没有找到代币价格数据 - 网络类型: {network_type}, "
                    f"网络: {network or chain_id}, 地址: {addr}"
                )
                result[addr] = None
                continue

            try:
                price_data = json.loads(price_data_json)

                # 直接获取price_usd字段
                if "price_usd" in price_data:
                    usd_price = float(price_data["price_usd"])
                    result[addr] = usd_price
                    logger.debug(
                        f"获取到代币价格 - 网络类型: {network_type}, "
                        f"网络: {network or chain_id}, 地址: {addr}, USD价格: {usd_price}"
                    )
                else:
                    logger.warning(f"无法从Redis数据中提取price_usd: {price_data}")
                    result[addr] = None

            except Exception as e:
                logger.warning(f"处理代币 {addr} 的价格数据时出错: {e}")
                result[addr] = None

        return result

    except Exception as e:
        logger.exception(f"批量获取代币价格时出错: {e}")
        return {addr.lower(): None for addr in token_addresses}


# 为了向后兼容，保留原有的单链ID接口
def get_token_price_usd_by_chain(
    token_address: str,
    chain_id: int = 1,
) -> Optional[float]:
    """
    从Redis获取指定代币的USD价格（仅支持EVM链，向后兼容）

    Args:
        token_address: 代币合约地址
        chain_id: 区块链ID (如以太坊主网为1)

    Returns:
        成功时返回USD价格（浮点数），失败时返回None
    """
    return get_token_price_usd(
        token_address=token_address, chain_id=chain_id, network_type="evm"
    )


def get_multiple_token_prices_usd_by_chain(
    chain_id: int,
    token_addresses: List[str],
) -> Dict[str, Optional[float]]:
    """
    批量从Redis获取多个代币的USD价格（仅支持EVM链，向后兼容）

    Args:
        chain_id: 区块链ID
        token_addresses: 代币地址列表

    Returns:
        代币地址到USD价格的映射字典，如果某个代币获取失败，其值为None
    """
    return get_multiple_token_prices_usd(
        token_addresses=token_addresses, chain_id=chain_id, network_type="evm"
    )


# Aptos 专用的便捷函数
def get_aptos_token_price_usd(token_address: str) -> Optional[float]:
    """
    获取Aptos代币的USD价格

    Args:
        token_address: Aptos代币地址

    Returns:
        成功时返回USD价格（浮点数），失败时返回None
    """
    return get_token_price_usd(
        token_address=token_address, network="aptos", network_type="aptos"
    )


def get_multiple_aptos_token_prices_usd(
    token_addresses: List[str],
) -> Dict[str, Optional[float]]:
    """
    批量获取Aptos代币的USD价格

    Args:
        token_addresses: Aptos代币地址列表

    Returns:
        代币地址到USD价格的映射字典
    """
    return get_multiple_token_prices_usd(
        token_addresses=token_addresses, network="aptos", network_type="aptos"
    )


# Sui 专用的便捷函数
def get_sui_token_price_usd(token_address: str) -> Optional[float]:
    """
    获取Sui代币的USD价格

    Args:
        token_address: Sui代币地址

    Returns:
        成功时返回USD价格（浮点数），失败时返回None
    """
    return get_token_price_usd(
        token_address=token_address, network="sui-network", network_type="sui"
    )


def get_multiple_sui_token_prices_usd(
    token_addresses: List[str],
) -> Dict[str, Optional[float]]:
    """
    批量获取Sui代币的USD价格

    Args:
        token_addresses: Sui代币地址列表

    Returns:
        代币地址到USD价格的映射字典
    """
    return get_multiple_token_prices_usd(
        token_addresses=token_addresses, network="sui-network", network_type="sui"
    )


if __name__ == "__main__":
    # python -m tradingflow.station.utils.token_price_util
    # 配置日志
    logging.basicConfig(level=logging.INFO)

    # 测试EVM链（向后兼容）
    print("=== 测试EVM链（以太坊主网）===")
    price = get_token_price_usd(
        token_address="0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
        chain_id=1,
        network_type="evm",
    )
    if price is not None:
        print(f"DAI价格: ${price}")
    else:
        print("无法获取DAI价格")

    # 测试Aptos链
    print("\n=== 测试Aptos链 ===")
    aptos_tokens = [
        "0xa",  # APT
        "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",  # USDC
    ]

    aptos_prices = get_multiple_aptos_token_prices_usd(aptos_tokens)
    for addr, price in aptos_prices.items():
        if price is not None:
            print(f"Aptos代币 {addr} 价格: ${price}")
        else:
            print(f"无法获取Aptos代币 {addr} 的价格")

    # 测试Sui链
    print("\n=== 测试Sui链 ===")
    sui_tokens = [
        "0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59::wal::WAL",
        "0x06864a6f921804860930db6ddbe2e16acdf8504495ea7481637a1c8b9a8fe54b::cetus::CETUS",
    ]

    sui_prices = get_multiple_sui_token_prices_usd(sui_tokens)
    for addr, price in sui_prices.items():
        if price is not None:
            print(f"Sui代币 {addr} 价格: ${price}")
        else:
            print(f"无法获取Sui代币 {addr} 的价格")

    # 测试通用接口
    print("\n=== 测试通用接口 ===")
    # EVM
    evm_price = get_token_price_usd(
        token_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
        chain_id=1,
        network_type="evm",
    )
    print(f"EVM USDC价格: ${evm_price}")

    # Aptos
    aptos_price = get_token_price_usd(
        token_address="0xa", network="aptos", network_type="aptos"  # APT
    )
    print(f"Aptos APT价格: ${aptos_price}")

    # Sui
    sui_price = get_token_price_usd(
        token_address="0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59::wal::WAL",
        network="sui-network",
        network_type="sui",
    )
    print(f"Sui WAL价格: ${sui_price}")

    # get_multiple_token_prices_usd 的测试
    # aptos下
    aptos_tokens = [
        "0xa",  # APT
        "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b",  # USDC
        "0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387",  # xBtc
        "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b",  # USDT
    ]
    aptos_prices = get_multiple_token_prices_usd(
        token_addresses=aptos_tokens, network="aptos", network_type="aptos"
    )
    print("\nAptos批量代币价格:")
    for addr, price in aptos_prices.items():
        if price is not None:
            print(f"Aptos代币 {addr} 价格: ${price}")
        else:
            print(f"无法获取Aptos代币 {addr} 的价格")

    # 测试Aptos代币信息
    aptos_token_info = get_aptos_monitored_token_info("0xa")
    if aptos_token_info:
        print(f"Aptos APT代币信息: {json.dumps(aptos_token_info, indent=2)}")
    else:
        print("无法获取Aptos APT代币信息")
