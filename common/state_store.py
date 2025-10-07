import abc
import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

# Redis客户端依赖
import redis.asyncio as aioredis


class StateStore(abc.ABC):
    """状态存储抽象基类，作为防腐层隔离底层存储实现"""

    @abc.abstractmethod
    async def initialize(self) -> bool:
        """初始化存储连接"""

    @abc.abstractmethod
    async def close(self) -> None:
        """关闭存储连接"""

    @abc.abstractmethod
    async def set_node_task_status(
        self, node_task_id: str, status: str, error_message: Optional[str] = None
    ) -> bool:
        """设置节点状态"""

    @abc.abstractmethod
    async def get_node_task_status(self, node_task_id: str) -> Dict[str, Any]:
        """获取节点状态"""

    @abc.abstractmethod
    async def set_termination_flag(
        self, node_task_id: str, reason: Optional[str] = None
    ) -> bool:
        """设置节点终止标志"""

    @abc.abstractmethod
    async def get_termination_flag(self, node_task_id: str) -> Optional[Dict[str, Any]]:
        """获取节点终止标志"""

    @abc.abstractmethod
    async def clear_termination_flag(self, node_task_id: str) -> bool:
        """清除节点终止标志"""

    # 新增：通用键值存储方法，用于NodeManager
    @abc.abstractmethod
    async def set_value(self, key: str, value: Any) -> bool:
        """设置键值"""

    @abc.abstractmethod
    async def get_value(self, key: str) -> Any:
        """获取键值"""

    @abc.abstractmethod
    async def delete_value(self, key: str) -> bool:
        """删除键值"""

    @abc.abstractmethod
    async def add_to_set(self, key: str, value: Any) -> bool:
        """添加值到集合"""

    @abc.abstractmethod
    async def remove_from_set(self, key: str, value: Any) -> bool:
        """从集合中移除值"""

    @abc.abstractmethod
    async def get_set_members(self, key: str) -> List[Any]:
        """获取集合中的所有成员"""


class RedisStateStore(StateStore):
    """Redis实现的状态存储"""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        key_prefix: str = "trading_flow:",
    ):
        """
        初始化Redis状态存储

        Args:
            redis_url: Redis连接URL
            key_prefix: Redis键前缀
        """
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.redis_client = None
        self.logger = logging.getLogger("RedisStateStore")

    async def initialize(self) -> bool:
        """
        初始化Redis连接

        Returns:
            bool: 是否初始化成功
        """
        try:
            self.redis_client = await aioredis.from_url(self.redis_url)
            self.logger.info("Redis state store initialized")
            return True
        except Exception as e:
            self.logger.error("Failed to initialize Redis state store: %s", str(e))
            return False

    async def close(self) -> None:
        """关闭Redis连接"""
        if self.redis_client:
            await self.redis_client.close()
            self.logger.info("Redis state store connection closed")

    async def set_node_task_status(
        self, node_task_id: str, status: str, error_message: Optional[str] = None
    ) -> bool:
        """
        设置节点状态

        Args:
            node_task_id: 节点ID
            status: 状态值
            error_message: 错误信息（如果有）

        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            # 更新Redis中的节点状态
            node_key = f"{self.key_prefix}node:{node_task_id}"
            node_data = {
                "status": status,
                "updated_at": asyncio.get_event_loop().time(),
            }

            if error_message:
                node_data["error_message"] = error_message

            # 使用哈希存储节点数据
            await self.redis_client.hset(node_key, mapping=node_data)
            # 设置适当的过期时间，例如24小时
            await self.redis_client.expire(node_key, 86400)
            return True
        except Exception as e:
            self.logger.error("Failed to set node status: %s", str(e))
            return False

    async def get_node_task_status(self, node_task_id: str) -> Dict[str, Any]:
        """
        获取节点状态

        Args:
            node_task_id: 节点ID

        Returns:
            Dict[str, Any]: 节点状态数据
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return {}

            node_key = f"{self.key_prefix}node:{node_task_id}"
            node_data = await self.redis_client.hgetall(node_key)

            # 将字节转换为字符串
            result = {}
            for k, v in node_data.items():
                key = k.decode("utf-8") if isinstance(k, bytes) else k
                value = v.decode("utf-8") if isinstance(v, bytes) else v
                result[key] = value

            return result
        except Exception as e:
            self.logger.error("Failed to get node status: %s", str(e))
            return {}

    async def set_termination_flag(
        self, node_task_id: str, reason: Optional[str] = None
    ) -> bool:
        """
        设置节点终止标志

        Args:
            node_task_id: 节点ID
            reason: 终止原因

        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            terminate_key = f"{self.key_prefix}node:{node_task_id}:terminate"
            terminate_data = {
                "reason": reason or "No reason provided",
                "timestamp": asyncio.get_event_loop().time(),
            }

            await self.redis_client.set(
                terminate_key, json.dumps(terminate_data), ex=3600  # 设置1小时过期时间
            )
            return True
        except Exception as e:
            self.logger.error("Failed to set termination flag: %s", str(e))
            return False

    async def get_termination_flag(self, node_task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取节点终止标志

        Args:
            node_task_id: 节点ID

        Returns:
            Optional[Dict[str, Any]]: 终止标志数据，如果不存在则返回None
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return None

            terminate_key = f"{self.key_prefix}node:{node_task_id}:terminate"
            terminate_data = await self.redis_client.get(terminate_key)

            if terminate_data:
                return json.loads(terminate_data)
            return None
        except Exception as e:
            self.logger.error("Failed to get termination flag: %s", str(e))
            return None

    async def clear_termination_flag(self, node_task_id: str) -> bool:
        """
        清除节点终止标志

        Args:
            node_task_id: 节点ID

        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            terminate_key = f"{self.key_prefix}node:{node_task_id}:terminate"
            await self.redis_client.delete(terminate_key)
            return True
        except Exception as e:
            self.logger.error("Failed to clear termination flag: %s", str(e))
            return False

    async def set_value(self, key: str, value: Any) -> bool:
        """
        设置键值

        Args:
            key: 键名
            value: 值（会被JSON序列化）

        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            # 将值序列化为JSON
            value_str = json.dumps(value)
            await self.redis_client.set(key, value_str)
            return True
        except Exception as e:
            self.logger.error(f"Failed to set value for key {key}: {str(e)}")
            return False

    async def get_value(self, key: str) -> Any:
        """
        获取键值

        Args:
            key: 键名

        Returns:
            Any: 值（JSON反序列化后的对象）
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return None

            value = await self.redis_client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            self.logger.error(f"Failed to get value for key {key}: {str(e)}")
            return None

    async def delete_value(self, key: str) -> bool:
        """
        删除键值

        Args:
            key: 键名

        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            await self.redis_client.delete(key)
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete key {key}: {str(e)}")
            return False

    async def add_to_set(self, key: str, value: Any) -> bool:
        """
        添加值到集合

        Args:
            key: 集合键名
            value: 要添加的值

        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            # Redis SADD命令
            await self.redis_client.sadd(key, str(value))
            return True
        except Exception as e:
            self.logger.error(f"Failed to add value to set {key}: {str(e)}")
            return False

    async def remove_from_set(self, key: str, value: Any) -> bool:
        """
        从集合中移除值

        Args:
            key: 集合键名
            value: 要移除的值

        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return False

            # Redis SREM命令
            await self.redis_client.srem(key, str(value))
            return True
        except Exception as e:
            self.logger.error(f"Failed to remove value from set {key}: {str(e)}")
            return False

    async def get_set_members(self, key: str) -> List[Any]:
        """
        获取集合中的所有成员

        Args:
            key: 集合键名

        Returns:
            List[Any]: 集合成员列表
        """
        try:
            if not self.redis_client:
                self.logger.error("Redis client not initialized")
                return []

            # Redis SMEMBERS命令
            members = await self.redis_client.smembers(key)
            # 将字节转换为字符串
            return [
                member.decode("utf-8") if isinstance(member, bytes) else member
                for member in members
            ]
        except Exception as e:
            self.logger.error(f"Failed to get members of set {key}: {str(e)}")
            return []


class InMemoryStateStore(StateStore):
    """内存实现的状态存储，适用于测试和单进程环境"""

    def __init__(self):
        """初始化内存状态存储"""
        self.node_status = {}  # 存储节点状态
        self.termination_flags = {}  # 存储终止标志
        self.key_values = {}  # 存储普通键值对
        self.sets = {}  # 存储集合
        self.logger = logging.getLogger("InMemoryStateStore")

    async def initialize(self) -> bool:
        """
        初始化存储连接

        Returns:
            bool: 始终返回True
        """
        self.logger.info("In-memory state store initialized")
        return True

    async def close(self) -> None:
        """关闭存储连接"""
        self.logger.info("In-memory state store closed")

    async def set_node_task_status(
        self, node_task_id: str, status: str, error_message: Optional[str] = None
    ) -> bool:
        """
        设置节点状态

        Args:
            node_task_id: 节点ID
            status: 状态值
            error_message: 错误信息（如果有）

        Returns:
            bool: 操作是否成功
        """
        try:
            self.node_status[node_task_id] = {
                "status": status,
                "updated_at": asyncio.get_event_loop().time(),
            }

            if error_message:
                self.node_status[node_task_id]["error_message"] = error_message

            return True
        except Exception as e:
            self.logger.error("Failed to set node status: %s", str(e))
            return False

    async def get_node_task_status(self, node_task_id: str) -> Dict[str, Any]:
        """
        获取节点状态

        Args:
            node_task_id: 节点ID

        Returns:
            Dict[str, Any]: 节点状态数据
        """
        return self.node_status.get(node_task_id, {})

    async def set_termination_flag(
        self, node_task_id: str, reason: Optional[str] = None
    ) -> bool:
        """
        设置节点终止标志

        Args:
            node_task_id: 节点ID
            reason: 终止原因

        Returns:
            bool: 操作是否成功
        """
        try:
            self.termination_flags[node_task_id] = {
                "reason": reason or "No reason provided",
                "timestamp": asyncio.get_event_loop().time(),
            }
            return True
        except Exception as e:
            self.logger.error("Failed to set termination flag: %s", str(e))
            return False

    async def get_termination_flag(self, node_task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取节点终止标志

        Args:
            node_task_id: 节点ID

        Returns:
            Optional[Dict[str, Any]]: 终止标志数据，如果不存在则返回None
        """
        return self.termination_flags.get(node_task_id)

    async def clear_termination_flag(self, node_task_id: str) -> bool:
        """
        清除节点终止标志

        Args:
            node_task_id: 节点ID

        Returns:
            bool: 操作是否成功
        """
        try:
            if node_task_id in self.termination_flags:
                del self.termination_flags[node_task_id]
            return True
        except Exception as e:
            self.logger.error("Failed to clear termination flag: %s", str(e))
            return False

    async def set_value(self, key: str, value: Any) -> bool:
        """设置键值"""
        try:
            self.key_values[key] = value
            return True
        except Exception as e:
            self.logger.error(f"Failed to set value for key {key}: {str(e)}")
            return False

    async def get_value(self, key: str) -> Any:
        """获取键值"""
        return self.key_values.get(key)

    async def delete_value(self, key: str) -> bool:
        """删除键值"""
        try:
            if key in self.key_values:
                del self.key_values[key]
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete key {key}: {str(e)}")
            return False

    async def add_to_set(self, key: str, value: Any) -> bool:
        """添加值到集合"""
        try:
            if key not in self.sets:
                self.sets[key] = set()
            self.sets[key].add(str(value))
            return True
        except Exception as e:
            self.logger.error(f"Failed to add value to set {key}: {str(e)}")
            return False

    async def remove_from_set(self, key: str, value: Any) -> bool:
        """从集合中移除值"""
        try:
            if key in self.sets and str(value) in self.sets[key]:
                self.sets[key].remove(str(value))
            return True
        except Exception as e:
            self.logger.error(f"Failed to remove value from set {key}: {str(e)}")
            return False

    async def get_set_members(self, key: str) -> List[Any]:
        """获取集合中的所有成员"""
        try:
            return list(self.sets.get(key, set()))
        except Exception as e:
            self.logger.error(f"Failed to get members of set {key}: {str(e)}")
            return []


# 工厂类创建合适的存储实现
class StateStoreFactory:
    """状态存储工厂类"""

    @staticmethod
    def create(store_type: str = "redis", config: Dict[str, Any] = None) -> StateStore:
        """
        创建状态存储实例

        Args:
            store_type: 存储类型，支持"redis"和"memory"，如果为None则从CONFIG读取
            config: 配置参数，如果为None则从CONFIG读取

        Returns:
            StateStore: 状态存储实例
        """
        # 导入CONFIG
        from weather_depot.config import CONFIG

        # 如果没有提供store_type，从CONFIG读取
        if store_type is None:
            store_type = CONFIG.get("STATE_STORE_TYPE", "redis")

        # 如果没有提供config，从CONFIG读取
        if config is None:
            config = {
                "redis_url": CONFIG.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
                "key_prefix": "trading_flow:",
            }

        if store_type == "redis":
            return RedisStateStore(
                redis_url=config.get("redis_url", CONFIG.get("REDIS_URL", "redis://127.0.0.1:6379/0")),
                key_prefix=config.get("key_prefix", "trading_flow:"),
            )
        elif store_type == "memory":
            return InMemoryStateStore()
        else:
            raise ValueError(f"Unsupported state store type: {store_type}")
