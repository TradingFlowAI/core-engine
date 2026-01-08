"""
Pause Manager: 管理 Flow/Component/Node 的暂停和恢复

支持三级粒度的暂停/恢复：
- Flow 级别：暂停整个 Flow 的执行
- Component 级别：暂停某个连通分量的执行
- Node 级别：暂停单个节点的执行（如等待用户输入）

暂停状态持久化到 Redis，支持服务重启后恢复。
"""

import json
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

from infra.config import CONFIG

logger = logging.getLogger(__name__)


class PauseType(Enum):
    """暂停类型"""
    AWAITING_INPUT = "awaiting_input"  # 等待用户输入（Interactive Node）
    MANUAL = "manual"  # 用户手动暂停
    ERROR = "error"  # 错误导致暂停
    BREAKPOINT = "breakpoint"  # 断点暂停（调试模式）


class PauseLevel(Enum):
    """暂停粒度"""
    FLOW = "flow"
    COMPONENT = "component"
    NODE = "node"


class PauseManager:
    """
    暂停管理器：管理 Flow/Component/Node 的暂停和恢复

    功能：
    1. 暂停执行并持久化状态
    2. 恢复执行从断点继续
    3. 查询暂停状态
    4. 处理超时清理
    """

    _instance = None

    @classmethod
    def get_instance(cls) -> "PauseManager":
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        if PauseManager._instance is not None:
            return

        self.redis: Optional[aioredis.Redis] = None
        self._initialized = False

        # Redis key prefixes
        self._flow_pause_prefix = "pause:flow:"
        self._component_pause_prefix = "pause:component:"
        self._node_pause_prefix = "pause:node:"
        self._paused_set_key = "paused_executions"

    async def initialize(self) -> bool:
        """初始化管理器，连接 Redis"""
        if self._initialized:
            return True

        try:
            redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
            self.redis = await aioredis.from_url(redis_url, decode_responses=True)
            self._initialized = True
            logger.info("PauseManager initialized, Redis connected")
            return True
        except Exception as e:
            logger.exception(f"Error initializing PauseManager: {e}")
            return False

    async def close(self):
        """关闭 Redis 连接"""
        if self.redis:
            await self.redis.close()
            self._initialized = False
            logger.info("PauseManager closed")

    # ==================== Key 生成 ====================

    def _get_flow_pause_key(self, flow_id: str) -> str:
        """生成 Flow 暂停状态的 Redis key"""
        return f"{self._flow_pause_prefix}{flow_id}"

    def _get_component_pause_key(self, flow_id: str, component_id: str) -> str:
        """生成 Component 暂停状态的 Redis key"""
        return f"{self._component_pause_prefix}{flow_id}:{component_id}"

    def _get_node_pause_key(self, flow_id: str, node_id: str) -> str:
        """生成 Node 暂停状态的 Redis key"""
        return f"{self._node_pause_prefix}{flow_id}:{node_id}"

    # ==================== 暂停操作 ====================

    async def pause_flow(
        self,
        flow_id: str,
        pause_type: PauseType = PauseType.MANUAL,
        paused_by: str = "user",
        resume_context: Optional[Dict] = None,
        ttl_seconds: int = 604800,  # 默认 7 天
    ) -> Dict[str, Any]:
        """
        暂停整个 Flow

        Args:
            flow_id: Flow ID
            pause_type: 暂停类型
            paused_by: 暂停发起者 (user/system/node)
            resume_context: 恢复时需要的上下文数据
            ttl_seconds: 暂停状态保留时间

        Returns:
            暂停状态数据
        """
        if not self._initialized:
            await self.initialize()

        pause_key = self._get_flow_pause_key(flow_id)
        pause_data = {
            "level": PauseLevel.FLOW.value,
            "flow_id": flow_id,
            "pause_type": pause_type.value,
            "paused_at": datetime.now(timezone.utc).isoformat(),
            "paused_by": paused_by,
            "resume_context": resume_context or {},
        }

        await self.redis.set(
            pause_key,
            json.dumps(pause_data),
            ex=ttl_seconds
        )
        await self.redis.sadd(self._paused_set_key, pause_key)

        logger.info(f"Flow {flow_id} paused: type={pause_type.value}, by={paused_by}")
        return pause_data

    async def pause_component(
        self,
        flow_id: str,
        component_id: str,
        pause_type: PauseType = PauseType.MANUAL,
        paused_by: str = "user",
        resume_context: Optional[Dict] = None,
        ttl_seconds: int = 604800,
    ) -> Dict[str, Any]:
        """
        暂停某个连通分量

        Args:
            flow_id: Flow ID
            component_id: 连通分量 ID
            pause_type: 暂停类型
            paused_by: 暂停发起者
            resume_context: 恢复时需要的上下文数据
            ttl_seconds: 暂停状态保留时间

        Returns:
            暂停状态数据
        """
        if not self._initialized:
            await self.initialize()

        pause_key = self._get_component_pause_key(flow_id, component_id)
        pause_data = {
            "level": PauseLevel.COMPONENT.value,
            "flow_id": flow_id,
            "component_id": component_id,
            "pause_type": pause_type.value,
            "paused_at": datetime.now(timezone.utc).isoformat(),
            "paused_by": paused_by,
            "resume_context": resume_context or {},
        }

        await self.redis.set(
            pause_key,
            json.dumps(pause_data),
            ex=ttl_seconds
        )
        await self.redis.sadd(self._paused_set_key, pause_key)

        logger.info(
            f"Component {component_id} in flow {flow_id} paused: "
            f"type={pause_type.value}, by={paused_by}"
        )
        return pause_data

    async def pause_node(
        self,
        flow_id: str,
        node_id: str,
        cycle: int,
        pause_type: PauseType = PauseType.AWAITING_INPUT,
        paused_by: str = "node",
        resume_context: Optional[Dict] = None,
        input_request: Optional[Dict] = None,
        ttl_seconds: int = 604800,
    ) -> Dict[str, Any]:
        """
        暂停单个节点

        Args:
            flow_id: Flow ID
            node_id: 节点 ID
            cycle: 当前 cycle
            pause_type: 暂停类型
            paused_by: 暂停发起者
            resume_context: 恢复时需要的上下文数据
            input_request: 用户输入请求信息（用于 AWAITING_INPUT）
            ttl_seconds: 暂停状态保留时间

        Returns:
            暂停状态数据
        """
        if not self._initialized:
            await self.initialize()

        pause_key = self._get_node_pause_key(flow_id, node_id)
        pause_data = {
            "level": PauseLevel.NODE.value,
            "flow_id": flow_id,
            "node_id": node_id,
            "cycle": cycle,
            "pause_type": pause_type.value,
            "paused_at": datetime.now(timezone.utc).isoformat(),
            "paused_by": paused_by,
            "resume_context": resume_context or {},
            "input_request": input_request or {},
        }

        await self.redis.set(
            pause_key,
            json.dumps(pause_data),
            ex=ttl_seconds
        )
        await self.redis.sadd(self._paused_set_key, pause_key)

        logger.info(
            f"Node {node_id} in flow {flow_id} paused: "
            f"type={pause_type.value}, by={paused_by}, cycle={cycle}"
        )
        return pause_data

    # ==================== 恢复操作 ====================

    async def resume_flow(
        self,
        flow_id: str,
        input_data: Optional[Dict] = None,
        skip_paused: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        恢复 Flow 执行

        Args:
            flow_id: Flow ID
            input_data: 用户输入数据（如果有）
            skip_paused: 是否跳过暂停的节点

        Returns:
            恢复上下文数据，如果没有暂停状态则返回 None
        """
        if not self._initialized:
            await self.initialize()

        pause_key = self._get_flow_pause_key(flow_id)
        data_str = await self.redis.get(pause_key)

        if not data_str:
            logger.warning(f"No pause state found for flow {flow_id}")
            return None

        pause_data = json.loads(data_str)
        resume_context = pause_data.get("resume_context", {})

        # 添加恢复信息
        resume_context["resumed_at"] = datetime.now(timezone.utc).isoformat()
        resume_context["input_data"] = input_data
        resume_context["skip_paused"] = skip_paused

        # 清除暂停状态
        await self.redis.delete(pause_key)
        await self.redis.srem(self._paused_set_key, pause_key)

        logger.info(f"Flow {flow_id} resumed")
        return resume_context

    async def resume_component(
        self,
        flow_id: str,
        component_id: str,
        input_data: Optional[Dict] = None,
        skip_paused: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        恢复 Component 执行

        Args:
            flow_id: Flow ID
            component_id: 连通分量 ID
            input_data: 用户输入数据（如果有）
            skip_paused: 是否跳过暂停的节点

        Returns:
            恢复上下文数据
        """
        if not self._initialized:
            await self.initialize()

        pause_key = self._get_component_pause_key(flow_id, component_id)
        data_str = await self.redis.get(pause_key)

        if not data_str:
            logger.warning(
                f"No pause state found for component {component_id} in flow {flow_id}"
            )
            return None

        pause_data = json.loads(data_str)
        resume_context = pause_data.get("resume_context", {})

        resume_context["resumed_at"] = datetime.now(timezone.utc).isoformat()
        resume_context["input_data"] = input_data
        resume_context["skip_paused"] = skip_paused

        await self.redis.delete(pause_key)
        await self.redis.srem(self._paused_set_key, pause_key)

        logger.info(f"Component {component_id} in flow {flow_id} resumed")
        return resume_context

    async def resume_node(
        self,
        flow_id: str,
        node_id: str,
        input_data: Optional[Dict] = None,
        skip_node: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        恢复 Node 执行

        Args:
            flow_id: Flow ID
            node_id: 节点 ID
            input_data: 用户输入数据（如 YesNo 的 decision）
            skip_node: 是否跳过该节点

        Returns:
            恢复上下文数据
        """
        if not self._initialized:
            await self.initialize()

        pause_key = self._get_node_pause_key(flow_id, node_id)
        data_str = await self.redis.get(pause_key)

        if not data_str:
            logger.warning(f"No pause state found for node {node_id} in flow {flow_id}")
            return None

        pause_data = json.loads(data_str)
        resume_context = pause_data.get("resume_context", {})

        resume_context["resumed_at"] = datetime.now(timezone.utc).isoformat()
        resume_context["input_data"] = input_data
        resume_context["skip_node"] = skip_node
        resume_context["cycle"] = pause_data.get("cycle")
        resume_context["input_request"] = pause_data.get("input_request", {})

        await self.redis.delete(pause_key)
        await self.redis.srem(self._paused_set_key, pause_key)

        logger.info(f"Node {node_id} in flow {flow_id} resumed")
        return resume_context

    # ==================== 查询操作 ====================

    async def get_flow_pause_state(self, flow_id: str) -> Optional[Dict[str, Any]]:
        """获取 Flow 的暂停状态"""
        if not self._initialized:
            await self.initialize()

        pause_key = self._get_flow_pause_key(flow_id)
        data_str = await self.redis.get(pause_key)
        return json.loads(data_str) if data_str else None

    async def get_component_pause_state(
        self, flow_id: str, component_id: str
    ) -> Optional[Dict[str, Any]]:
        """获取 Component 的暂停状态"""
        if not self._initialized:
            await self.initialize()

        pause_key = self._get_component_pause_key(flow_id, component_id)
        data_str = await self.redis.get(pause_key)
        return json.loads(data_str) if data_str else None

    async def get_node_pause_state(
        self, flow_id: str, node_id: str
    ) -> Optional[Dict[str, Any]]:
        """获取 Node 的暂停状态"""
        if not self._initialized:
            await self.initialize()

        pause_key = self._get_node_pause_key(flow_id, node_id)
        data_str = await self.redis.get(pause_key)
        return json.loads(data_str) if data_str else None

    async def is_flow_paused(self, flow_id: str) -> bool:
        """检查 Flow 是否暂停"""
        state = await self.get_flow_pause_state(flow_id)
        return state is not None

    async def is_component_paused(self, flow_id: str, component_id: str) -> bool:
        """检查 Component 是否暂停"""
        state = await self.get_component_pause_state(flow_id, component_id)
        return state is not None

    async def is_node_paused(self, flow_id: str, node_id: str) -> bool:
        """检查 Node 是否暂停"""
        state = await self.get_node_pause_state(flow_id, node_id)
        return state is not None

    async def get_all_paused_nodes(self, flow_id: str) -> List[Dict[str, Any]]:
        """获取 Flow 中所有暂停的节点"""
        if not self._initialized:
            await self.initialize()

        paused_nodes = []
        pattern = f"{self._node_pause_prefix}{flow_id}:*"

        async for key in self.redis.scan_iter(match=pattern):
            data_str = await self.redis.get(key)
            if data_str:
                paused_nodes.append(json.loads(data_str))

        return paused_nodes

    async def get_all_paused_executions(self) -> List[Dict[str, Any]]:
        """获取所有暂停的执行（用于服务恢复）"""
        if not self._initialized:
            await self.initialize()

        paused_keys = await self.redis.smembers(self._paused_set_key)
        paused_executions = []

        for key in paused_keys:
            data_str = await self.redis.get(key)
            if data_str:
                paused_executions.append(json.loads(data_str))
            else:
                # 清理已过期的 key
                await self.redis.srem(self._paused_set_key, key)

        return paused_executions

    # ==================== 清理操作 ====================

    async def clear_all_pause_states(self, flow_id: str) -> int:
        """清除 Flow 的所有暂停状态"""
        if not self._initialized:
            await self.initialize()

        cleared = 0

        # 清除 Flow 级别
        flow_key = self._get_flow_pause_key(flow_id)
        if await self.redis.delete(flow_key):
            await self.redis.srem(self._paused_set_key, flow_key)
            cleared += 1

        # 清除 Component 级别
        pattern = f"{self._component_pause_prefix}{flow_id}:*"
        async for key in self.redis.scan_iter(match=pattern):
            await self.redis.delete(key)
            await self.redis.srem(self._paused_set_key, key)
            cleared += 1

        # 清除 Node 级别
        pattern = f"{self._node_pause_prefix}{flow_id}:*"
        async for key in self.redis.scan_iter(match=pattern):
            await self.redis.delete(key)
            await self.redis.srem(self._paused_set_key, key)
            cleared += 1

        logger.info(f"Cleared {cleared} pause states for flow {flow_id}")
        return cleared


# 便捷函数
def get_pause_manager() -> PauseManager:
    """获取 PauseManager 单例"""
    return PauseManager.get_instance()
