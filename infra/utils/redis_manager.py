"""Redis Connection Manager"""

import logging
import os

from redis import ConnectionPool, Redis

from infra.config import CONFIG

logger = logging.getLogger(__name__)

# Get Redis config from CONFIG (password already auto-encoded)
REDIS_URL = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)

# Global connection pools
_connection_pools = {}


def get_connection_pool(db: int = None) -> ConnectionPool:
    """Get or create Redis connection pool.

    Args:
        db: Redis database number, uses default config if None

    Returns:
        Redis connection pool instance
    """
    global _connection_pools

    # Use default if db not specified
    if db is None:
        db = REDIS_DB

    # Create pool if doesn't exist for this db
    if db not in _connection_pools:
        if REDIS_URL:
            # If full URL available, use directly
            _connection_pools[db] = ConnectionPool.from_url(
                REDIS_URL, decode_responses=True
            )
        else:
            # Otherwise use individual config params
            _connection_pools[db] = ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=db,
                password=REDIS_PASSWORD,
                decode_responses=True,
            )
        logger.debug(f"Created new connection pool for Redis database {db}")

    return _connection_pools[db]


def get_redis_client(db: int = None) -> Redis:
    """Get Redis client instance.

    Args:
        db: Redis database number, uses default config if None

    Returns:
        Redis client instance
    """
    pool = get_connection_pool(db)
    return Redis(connection_pool=pool)


def close_all_connections():
    """Close all Redis connection pools"""
    global _connection_pools

    for db, pool in _connection_pools.items():
        logger.info(f"Closing Redis connection pool for database {db}")
        pool.disconnect()

    _connection_pools = {}


class RedisManager:
    """Redis manager class for managing Redis connections and operations"""

    @staticmethod
    def get_client(db: int = None) -> Redis:
        """Get Redis client"""
        return get_redis_client(db)

    @staticmethod
    def get_price_key(chain_id, token_address: str) -> str:
        """Get Redis key for EVM chain token price (backward compatible method).

        Args:
            chain_id: Blockchain ID
            token_address: Token address

        Returns:
            str: Redis key name
        """
        return f"price:chain:{chain_id}:{token_address.lower()}"

    @staticmethod
    def get_token_price_key(network_type: str, network: str, token_address: str) -> str:
        """Get Redis key for any network type token price.

        Args:
            network_type: Network type, 'evm' or 'non-evm'
            network: Network ID (chain ID for EVM, network name for non-EVM)
            token_address: Token address or identifier

        Returns:
            str: Redis key name
        """
        if network_type.lower() == "evm":
            prefix = "chain"
        else:
            prefix = "network"

        return f"price:{prefix}:{network}:{token_address.lower()}"

    @staticmethod
    def get_network_last_updated_key(network_type: str, network: str) -> str:
        """Get Redis key for network last update time.

        Args:
            network_type: Network type, 'evm' or 'non-evm'
            network: Network ID (chain ID for EVM, network name for non-EVM)

        Returns:
            str: Redis key name
        """
        if network_type.lower() == "evm":
            prefix = "chain"
        else:
            prefix = "network"

        return f"price:{prefix}:{network}:last_updated"

    @staticmethod
    def get_chain_last_updated_key(chain_id) -> str:
        """Get Redis key for chain last update time (backward compatible method).

        Args:
            chain_id: Blockchain ID

        Returns:
            str: Redis key name
        """
        return f"price:chain:{chain_id}:last_updated"

    @staticmethod
    def get_global_last_updated_key() -> str:
        """Get Redis key for global last update time.

        Returns:
            str: Redis key name
        """
        return "price:last_updated"
