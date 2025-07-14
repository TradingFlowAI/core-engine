import logging
import os

from redis import ConnectionPool, Redis

logger = logging.getLogger(__name__)

# 从环境变量获取Redis配置
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)
REDIS_URL = os.environ.get("REDIS_URL", None)

# 全局连接池
_connection_pools = {}


def get_connection_pool(db: int = None) -> ConnectionPool:
    """获取或创建Redis连接池

    Args:
        db: Redis数据库编号，如果为None则使用默认配置

    Returns:
        Redis连接池实例
    """
    global _connection_pools

    # 如果未指定db，使用默认值
    if db is None:
        db = REDIS_DB

    # 如果该db的连接池不存在，则创建
    if db not in _connection_pools:
        if REDIS_URL:
            # 如果有完整的URL，直接使用
            _connection_pools[db] = ConnectionPool.from_url(
                REDIS_URL, decode_responses=True
            )
        else:
            # 否则使用分解的配置参数
            _connection_pools[db] = ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=db,
                password=REDIS_PASSWORD,
                decode_responses=True,
            )
        logger.debug(f"创建了Redis数据库{db}的新连接池")

    return _connection_pools[db]


def get_redis_client(db: int = None) -> Redis:
    """获取Redis客户端实例

    Args:
        db: Redis数据库编号，如果为None则使用默认配置

    Returns:
        Redis客户端实例
    """
    pool = get_connection_pool(db)
    return Redis(connection_pool=pool)


def close_all_connections():
    """关闭所有Redis连接池"""
    global _connection_pools

    for db, pool in _connection_pools.items():
        logger.info(f"关闭Redis数据库{db}的连接池")
        pool.disconnect()

    _connection_pools = {}


class RedisManager:
    """Redis管理器类，用于管理Redis连接和操作"""

    @staticmethod
    def get_client(db: int = None) -> Redis:
        """获取Redis客户端"""
        return get_redis_client(db)

    @staticmethod
    def get_price_key(chain_id, token_address: str) -> str:
        """获取EVM链代币价格的Redis键（向后兼容的方法）

        Args:
            chain_id: 区块链ID
            token_address: 代币地址

        Returns:
            str: Redis键名
        """
        return f"price:chain:{chain_id}:{token_address.lower()}"

    @staticmethod
    def get_token_price_key(network_type: str, network: str, token_address: str) -> str:
        """获取任意网络类型代币价格的Redis键

        Args:
            network_type: 网络类型，'evm'或'non-evm'
            network_id: 网络ID（EVM网络为链ID，非EVM网络为网络名称）
            token_address: 代币地址或标识符

        Returns:
            str: Redis键名
        """
        if network_type.lower() == "evm":
            prefix = "chain"
        else:
            prefix = "network"

        return f"price:{prefix}:{network}:{token_address.lower()}"

    @staticmethod
    def get_network_last_updated_key(network_type: str, network: str) -> str:
        """获取网络最后更新时间的Redis键

        Args:
            network_type: 网络类型，'evm'或'non-evm'
            network_id: 网络ID（EVM网络为链ID，非EVM网络为网络名称）

        Returns:
            str: Redis键名
        """
        if network_type.lower() == "evm":
            prefix = "chain"
        else:
            prefix = "network"

        return f"price:{prefix}:{network}:last_updated"

    @staticmethod
    def get_chain_last_updated_key(chain_id) -> str:
        """获取链最后更新时间的Redis键（向后兼容的方法）

        Args:
            chain_id: 区块链ID

        Returns:
            str: Redis键名
        """
        return f"price:chain:{chain_id}:last_updated"

    @staticmethod
    def get_global_last_updated_key() -> str:
        """获取全局最后更新时间的Redis键

        Returns:
            str: Redis键名
        """
        return "price:last_updated"
