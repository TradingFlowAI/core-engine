"""
Interactive Node Manager

管理 Interactive Nodes 的用户交互请求和响应。
支持 awaiting_input 状态和超时处理。
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

from infra.config import CONFIG

logger = logging.getLogger(__name__)


class InteractiveNodeManager:
    """
    Interactive Node Manager: 管理交互式节点的用户交互

    功能:
    1. 注册节点的交互请求
    2. 存储和检索用户响应
    3. 处理交互超时
    4. 通过 Redis Pub/Sub 广播交互事件
    """

    _instance = None

    @classmethod
    def get_instance(cls):
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        if InteractiveNodeManager._instance is not None:
            return

        self.redis: Optional[aioredis.Redis] = None
        self._initialized = False

        # Key prefixes
        self._interaction_prefix = "interaction:"  # 交互请求
        self._response_prefix = "interaction_response:"  # 用户响应
        self._pending_set_key = "pending_interactions"  # 待处理交互列表

    async def initialize(self) -> bool:
        """Initialize manager and connect to Redis."""
        if self._initialized:
            return True

        try:
            redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
            self.redis = await aioredis.from_url(redis_url, decode_responses=True)
            self._initialized = True
            logger.info("InteractiveNodeManager initialized, Redis connected")
            return True
        except Exception as e:
            logger.exception(f"Error initializing InteractiveNodeManager: {e}")
            return False

    async def close(self):
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            self._initialized = False
            logger.info("InteractiveNodeManager closed")

    def _get_interaction_key(self, flow_id: str, cycle: int, node_id: str) -> str:
        """Generate interaction key."""
        return f"{self._interaction_prefix}{flow_id}:{cycle}:{node_id}"

    def _get_response_key(self, flow_id: str, cycle: int, node_id: str) -> str:
        """Generate response key."""
        return f"{self._response_prefix}{flow_id}:{cycle}:{node_id}"

    async def request_interaction(
        self,
        flow_id: str,
        cycle: int,
        node_id: str,
        node_type: str,
        interaction_type: str,
        prompt: str,
        context_data: Optional[Dict] = None,
        timeout_seconds: int = 300,
        timeout_action: str = "reject",
        options: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        注册一个交互请求，等待用户响应

        Args:
            flow_id: Flow ID
            cycle: 当前 cycle
            node_id: 节点 ID
            node_type: 节点类型 (yes_no_node, chatbox_node, etc.)
            interaction_type: 交互类型 (confirmation, input, choice, etc.)
            prompt: 提示信息
            context_data: 上下文数据
            timeout_seconds: 超时时间（秒），0 表示无超时
            timeout_action: 超时动作 (reject, approve, skip)
            options: 额外选项 (如 yes_label, no_label)

        Returns:
            Dict containing interaction request details
        """
        if not self._initialized:
            await self.initialize()

        interaction_key = self._get_interaction_key(flow_id, cycle, node_id)

        interaction_data = {
            "flow_id": flow_id,
            "cycle": cycle,
            "node_id": node_id,
            "node_type": node_type,
            "interaction_type": interaction_type,
            "prompt": prompt,
            "context_data": context_data or {},
            "timeout_seconds": timeout_seconds,
            "timeout_action": timeout_action,
            "options": options or {},
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": (
                datetime.now(timezone.utc).timestamp() + timeout_seconds
                if timeout_seconds > 0
                else None
            ),
        }

        # 存储交互请求
        await self.redis.set(
            interaction_key,
            json.dumps(interaction_data),
            ex=timeout_seconds + 60 if timeout_seconds > 0 else 3600,  # 额外 60 秒缓冲
        )

        # 添加到待处理列表
        await self.redis.sadd(self._pending_set_key, interaction_key)

        # 发布交互请求事件到 Redis Pub/Sub
        await self._publish_interaction_event(
            flow_id=flow_id,
            event_type="interaction_request",
            data=interaction_data,
        )

        logger.info(
            f"Interaction request registered: flow={flow_id}, cycle={cycle}, node={node_id}, type={interaction_type}"
        )

        return interaction_data

    async def submit_response(
        self,
        flow_id: str,
        cycle: int,
        node_id: str,
        response: Any,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        提交用户响应

        Args:
            flow_id: Flow ID
            cycle: Cycle number
            node_id: Node ID
            response: 用户响应数据
            user_id: 提交响应的用户 ID

        Returns:
            Dict containing response confirmation
        """
        if not self._initialized:
            await self.initialize()

        interaction_key = self._get_interaction_key(flow_id, cycle, node_id)
        response_key = self._get_response_key(flow_id, cycle, node_id)

        # 检查交互请求是否存在
        interaction_data_str = await self.redis.get(interaction_key)
        if not interaction_data_str:
            return {
                "success": False,
                "error": "Interaction request not found or expired",
            }

        interaction_data = json.loads(interaction_data_str)

        # 检查是否已超时
        if interaction_data.get("expires_at"):
            if datetime.now(timezone.utc).timestamp() > interaction_data["expires_at"]:
                return {
                    "success": False,
                    "error": "Interaction request has expired",
                }

        # 存储响应
        response_data = {
            "flow_id": flow_id,
            "cycle": cycle,
            "node_id": node_id,
            "response": response,
            "user_id": user_id,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }

        await self.redis.set(response_key, json.dumps(response_data), ex=3600)

        # 更新交互请求状态
        interaction_data["status"] = "responded"
        interaction_data["responded_at"] = datetime.now(timezone.utc).isoformat()
        await self.redis.set(interaction_key, json.dumps(interaction_data), ex=3600)

        # 从待处理列表移除
        await self.redis.srem(self._pending_set_key, interaction_key)

        # 发布响应事件
        await self._publish_interaction_event(
            flow_id=flow_id,
            event_type="interaction_response",
            data={
                **response_data,
                "interaction_type": interaction_data.get("interaction_type"),
            },
        )

        logger.info(
            f"Interaction response submitted: flow={flow_id}, cycle={cycle}, node={node_id}"
        )

        return {
            "success": True,
            "response": response_data,
        }

    async def get_pending_interaction(
        self, flow_id: str, cycle: int, node_id: str
    ) -> Optional[Dict]:
        """获取待处理的交互请求"""
        if not self._initialized:
            await self.initialize()

        interaction_key = self._get_interaction_key(flow_id, cycle, node_id)
        data_str = await self.redis.get(interaction_key)

        if not data_str:
            return None

        return json.loads(data_str)

    async def get_response(
        self, flow_id: str, cycle: int, node_id: str
    ) -> Optional[Dict]:
        """获取用户响应"""
        if not self._initialized:
            await self.initialize()

        response_key = self._get_response_key(flow_id, cycle, node_id)
        data_str = await self.redis.get(response_key)

        if not data_str:
            return None

        return json.loads(data_str)

    async def wait_for_response(
        self,
        flow_id: str,
        cycle: int,
        node_id: str,
        timeout_seconds: int = 300,
        poll_interval: float = 1.0,
    ) -> Optional[Dict]:
        """
        等待用户响应（阻塞式）

        Args:
            flow_id: Flow ID
            cycle: Cycle number
            node_id: Node ID
            timeout_seconds: 超时时间
            poll_interval: 轮询间隔

        Returns:
            用户响应或 None（超时）
        """
        if not self._initialized:
            await self.initialize()

        start_time = datetime.now(timezone.utc).timestamp()
        deadline = start_time + timeout_seconds

        while datetime.now(timezone.utc).timestamp() < deadline:
            response = await self.get_response(flow_id, cycle, node_id)
            if response:
                return response

            await asyncio.sleep(poll_interval)

        # 超时
        logger.warning(
            f"Interaction timeout: flow={flow_id}, cycle={cycle}, node={node_id}"
        )
        return None

    async def get_all_pending_interactions(self, flow_id: str) -> List[Dict]:
        """获取 Flow 的所有待处理交互"""
        if not self._initialized:
            await self.initialize()

        pending_keys = await self.redis.smembers(self._pending_set_key)
        interactions = []

        for key in pending_keys:
            if f":{flow_id}:" in key:
                data_str = await self.redis.get(key)
                if data_str:
                    interactions.append(json.loads(data_str))

        return interactions

    async def cancel_interaction(
        self, flow_id: str, cycle: int, node_id: str, reason: str = "cancelled"
    ) -> bool:
        """取消交互请求"""
        if not self._initialized:
            await self.initialize()

        interaction_key = self._get_interaction_key(flow_id, cycle, node_id)

        # 获取交互数据
        data_str = await self.redis.get(interaction_key)
        if not data_str:
            return False

        interaction_data = json.loads(data_str)
        interaction_data["status"] = "cancelled"
        interaction_data["cancelled_at"] = datetime.now(timezone.utc).isoformat()
        interaction_data["cancel_reason"] = reason

        await self.redis.set(interaction_key, json.dumps(interaction_data), ex=3600)
        await self.redis.srem(self._pending_set_key, interaction_key)

        # 发布取消事件
        await self._publish_interaction_event(
            flow_id=flow_id,
            event_type="interaction_cancelled",
            data=interaction_data,
        )

        logger.info(
            f"Interaction cancelled: flow={flow_id}, cycle={cycle}, node={node_id}, reason={reason}"
        )

        return True

    async def _publish_interaction_event(
        self, flow_id: str, event_type: str, data: Dict
    ):
        """发布交互事件到 Redis Pub/Sub"""
        if not self.redis:
            return

        try:
            event = {
                "type": event_type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                **data,
            }

            # 发布到 flow 特定频道
            channel = f"interaction:flow:{flow_id}"
            await self.redis.publish(channel, json.dumps(event))

            logger.debug(f"Published interaction event: {event_type} to {channel}")
        except Exception as e:
            logger.warning(f"Failed to publish interaction event: {e}")


# 获取单例实例的快捷函数
def get_interactive_manager() -> InteractiveNodeManager:
    return InteractiveNodeManager.get_instance()
