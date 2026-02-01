"""
Constant Node Manager

管理长时间运行的服务节点 (Constant Nodes)。
这些节点在 Flow 启动时运行，不参与常规的 cycle 调度，
而是由 Scheduler 监控其健康状态。

典型的 Constant Node:
- ChatBox Node: 提供对话服务
- WebSocket Listener: 监听外部事件
- Stream Processor: 处理数据流

特点:
1. 在 Flow 启动时启动，在 Flow 停止时停止
2. Scheduler 定期检查健康状态
3. 异常时自动重启
4. 支持水平扩展（通过 Redis 寻址）
5. Station 部署时硬切换重启
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

from infra.config import CONFIG

logger = logging.getLogger(__name__)


class ConstantNodeManager:
    """
    Constant Node Manager: 管理长时间运行的服务节点

    功能:
    1. 注册和跟踪 Constant Nodes
    2. 健康检查和自动重启
    3. 水平扩展支持（Redis 寻址）
    4. 部署时的硬切换
    """

    _instance = None

    @classmethod
    def get_instance(cls):
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        if ConstantNodeManager._instance is not None:
            return

        self.redis: Optional[aioredis.Redis] = None
        self._initialized = False

        # 本地运行的 Constant Nodes
        self._local_nodes: Dict[str, Dict[str, Any]] = {}
        # 本地运行的任务
        self._local_tasks: Dict[str, asyncio.Task] = {}

        # Worker ID（用于水平扩展时的寻址）
        self._worker_id = CONFIG.get("WORKER_ID", "worker-default")

        # Key prefixes
        self._nodes_prefix = "constant_node:"  # 单个节点信息
        self._registry_key = "constant_nodes_registry"  # 全局注册表
        self._worker_nodes_prefix = "worker_constant_nodes:"  # Worker 节点映射
        self._health_prefix = "constant_node_health:"  # 健康状态

        # 健康检查配置
        self._health_check_interval = int(CONFIG.get("CONSTANT_NODE_HEALTH_INTERVAL", 30))
        self._health_check_task: Optional[asyncio.Task] = None

    async def initialize(self) -> bool:
        """Initialize manager and connect to Redis."""
        if self._initialized:
            return True

        try:
            redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
            self.redis = await aioredis.from_url(redis_url, decode_responses=True)
            self._initialized = True
            logger.info(
                f"ConstantNodeManager initialized, worker_id={self._worker_id}"
            )

            # 启动健康检查
            await self._start_health_check()

            return True
        except Exception as e:
            logger.exception(f"Error initializing ConstantNodeManager: {e}")
            return False

    async def close(self):
        """Close manager and cleanup resources."""
        # 停止健康检查
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # 停止所有本地 Constant Nodes
        await self.stop_all_local_nodes()

        if self.redis:
            await self.redis.close()
            self._initialized = False

        logger.info("ConstantNodeManager closed")

    def _get_node_key(self, flow_id: str, node_id: str) -> str:
        """Generate node key."""
        return f"{self._nodes_prefix}{flow_id}:{node_id}"

    def _get_health_key(self, flow_id: str, node_id: str) -> str:
        """Generate health key."""
        return f"{self._health_prefix}{flow_id}:{node_id}"

    async def register_constant_node(
        self,
        flow_id: str,
        node_id: str,
        node_type: str,
        node_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        注册一个 Constant Node

        Args:
            flow_id: Flow ID
            node_id: Node ID
            node_type: 节点类型 (如 chatbox_node)
            node_config: 节点配置

        Returns:
            注册信息
        """
        if not self._initialized:
            await self.initialize()

        node_key = self._get_node_key(flow_id, node_id)

        node_data = {
            "flow_id": flow_id,
            "node_id": node_id,
            "node_type": node_type,
            "config": node_config,
            "worker_id": self._worker_id,
            "status": "registered",
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "started_at": None,
            "last_health_check": None,
            "restart_count": 0,
        }

        # 存储节点信息
        await self.redis.set(node_key, json.dumps(node_data), ex=86400)  # 24 小时过期

        # 添加到全局注册表
        await self.redis.sadd(self._registry_key, node_key)

        # 添加到 Worker 节点映射
        worker_key = f"{self._worker_nodes_prefix}{self._worker_id}"
        await self.redis.sadd(worker_key, node_key)

        # 保存到本地
        local_key = f"{flow_id}:{node_id}"
        self._local_nodes[local_key] = node_data

        logger.info(
            f"Constant node registered: flow={flow_id}, node={node_id}, type={node_type}"
        )

        return node_data

    async def start_constant_node(
        self,
        flow_id: str,
        node_id: str,
        executor_func: Optional[callable] = None,
    ) -> bool:
        """
        启动一个 Constant Node

        Args:
            flow_id: Flow ID
            node_id: Node ID
            executor_func: 可选的执行函数（用于内部进程方式）

        Returns:
            是否成功启动
        """
        if not self._initialized:
            await self.initialize()

        local_key = f"{flow_id}:{node_id}"
        node_data = self._local_nodes.get(local_key)

        if not node_data:
            # 尝试从 Redis 获取
            node_key = self._get_node_key(flow_id, node_id)
            node_data_str = await self.redis.get(node_key)
            if not node_data_str:
                logger.error(f"Constant node not found: {local_key}")
                return False
            node_data = json.loads(node_data_str)
            self._local_nodes[local_key] = node_data

        # 检查是否已在运行
        if local_key in self._local_tasks and not self._local_tasks[local_key].done():
            logger.warning(f"Constant node already running: {local_key}")
            return True

        # 更新状态
        node_data["status"] = "starting"
        node_data["started_at"] = datetime.now(timezone.utc).isoformat()
        await self._update_node_data(flow_id, node_id, node_data)

        # 启动执行器（如果提供）
        if executor_func:
            task = asyncio.create_task(
                self._run_constant_node(flow_id, node_id, executor_func)
            )
            self._local_tasks[local_key] = task

        # 更新状态为运行中
        node_data["status"] = "running"
        await self._update_node_data(flow_id, node_id, node_data)
        await self._update_health(flow_id, node_id, "healthy")

        logger.info(f"Constant node started: flow={flow_id}, node={node_id}")
        return True

    async def _run_constant_node(
        self,
        flow_id: str,
        node_id: str,
        executor_func: callable,
    ):
        """运行 Constant Node 的执行函数"""
        local_key = f"{flow_id}:{node_id}"
        node_data = self._local_nodes.get(local_key)

        try:
            await executor_func(flow_id, node_id, node_data.get("config", {}))
        except asyncio.CancelledError:
            logger.info(f"Constant node cancelled: {local_key}")
        except Exception as e:
            logger.exception(f"Constant node error: {local_key} - {e}")
            # 更新状态为失败
            if node_data:
                node_data["status"] = "failed"
                node_data["error"] = str(e)
                await self._update_node_data(flow_id, node_id, node_data)
                await self._update_health(flow_id, node_id, "unhealthy", str(e))

    async def stop_constant_node(
        self,
        flow_id: str,
        node_id: str,
        reason: str = "stopped",
    ) -> bool:
        """
        停止一个 Constant Node

        Args:
            flow_id: Flow ID
            node_id: Node ID
            reason: 停止原因

        Returns:
            是否成功停止
        """
        if not self._initialized:
            await self.initialize()

        local_key = f"{flow_id}:{node_id}"

        # 取消本地任务
        if local_key in self._local_tasks:
            task = self._local_tasks.pop(local_key)
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # 更新状态
        if local_key in self._local_nodes:
            node_data = self._local_nodes[local_key]
            node_data["status"] = "stopped"
            node_data["stopped_at"] = datetime.now(timezone.utc).isoformat()
            node_data["stop_reason"] = reason
            await self._update_node_data(flow_id, node_id, node_data)

        # 从注册表移除
        node_key = self._get_node_key(flow_id, node_id)
        await self.redis.srem(self._registry_key, node_key)
        worker_key = f"{self._worker_nodes_prefix}{self._worker_id}"
        await self.redis.srem(worker_key, node_key)

        # 从本地移除
        self._local_nodes.pop(local_key, None)

        logger.info(
            f"Constant node stopped: flow={flow_id}, node={node_id}, reason={reason}"
        )
        return True

    async def stop_all_local_nodes(self):
        """停止所有本地 Constant Nodes"""
        local_keys = list(self._local_nodes.keys())
        for local_key in local_keys:
            parts = local_key.split(":", 1)
            if len(parts) == 2:
                flow_id, node_id = parts
                await self.stop_constant_node(flow_id, node_id, "shutdown")

        logger.info(f"Stopped {len(local_keys)} constant nodes")

    async def stop_flow_constant_nodes(self, flow_id: str):
        """停止 Flow 的所有 Constant Nodes"""
        stopped_count = 0
        for local_key in list(self._local_nodes.keys()):
            if local_key.startswith(f"{flow_id}:"):
                parts = local_key.split(":", 1)
                if len(parts) == 2:
                    _, node_id = parts
                    await self.stop_constant_node(flow_id, node_id, "flow_stopped")
                    stopped_count += 1

        logger.info(f"Stopped {stopped_count} constant nodes for flow {flow_id}")
        return stopped_count

    async def restart_constant_node(
        self,
        flow_id: str,
        node_id: str,
        executor_func: Optional[callable] = None,
    ) -> bool:
        """重启一个 Constant Node"""
        local_key = f"{flow_id}:{node_id}"
        node_data = self._local_nodes.get(local_key)

        # 先停止
        await self.stop_constant_node(flow_id, node_id, "restart")

        # 重新注册
        if node_data:
            node_data["restart_count"] = node_data.get("restart_count", 0) + 1
            await self.register_constant_node(
                flow_id=flow_id,
                node_id=node_id,
                node_type=node_data.get("node_type", "unknown"),
                node_config=node_data.get("config", {}),
            )

            # 启动
            return await self.start_constant_node(flow_id, node_id, executor_func)

        return False

    async def get_node_status(self, flow_id: str, node_id: str) -> Optional[Dict]:
        """获取 Constant Node 状态"""
        if not self._initialized:
            await self.initialize()

        node_key = self._get_node_key(flow_id, node_id)
        data_str = await self.redis.get(node_key)

        if not data_str:
            return None

        node_data = json.loads(data_str)

        # 添加健康状态
        health_key = self._get_health_key(flow_id, node_id)
        health_str = await self.redis.get(health_key)
        if health_str:
            node_data["health"] = json.loads(health_str)

        return node_data

    async def get_all_constant_nodes(self, flow_id: Optional[str] = None) -> List[Dict]:
        """获取所有 Constant Nodes"""
        if not self._initialized:
            await self.initialize()

        node_keys = await self.redis.smembers(self._registry_key)
        nodes = []

        for key in node_keys:
            if flow_id and f":{flow_id}:" not in key:
                continue

            data_str = await self.redis.get(key)
            if data_str:
                nodes.append(json.loads(data_str))

        return nodes

    async def _update_node_data(self, flow_id: str, node_id: str, data: Dict):
        """更新节点数据到 Redis"""
        node_key = self._get_node_key(flow_id, node_id)
        await self.redis.set(node_key, json.dumps(data), ex=86400)

    async def _update_health(
        self,
        flow_id: str,
        node_id: str,
        status: str,
        error: Optional[str] = None,
    ):
        """更新健康状态"""
        health_key = self._get_health_key(flow_id, node_id)
        health_data = {
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": error,
        }
        await self.redis.set(health_key, json.dumps(health_data), ex=120)  # 2 分钟过期

        # 发布健康事件
        await self._publish_health_event(flow_id, node_id, health_data)

    async def _publish_health_event(
        self,
        flow_id: str,
        node_id: str,
        health_data: Dict,
    ):
        """发布健康事件到 Redis Pub/Sub"""
        if not self.redis:
            return

        try:
            event = {
                "type": "constant_node_health",
                "flow_id": flow_id,
                "node_id": node_id,
                "worker_id": self._worker_id,
                **health_data,
            }

            channel = f"constant_node:flow:{flow_id}"
            await self.redis.publish(channel, json.dumps(event))
        except Exception as e:
            logger.warning(f"Failed to publish health event: {e}")

    async def _start_health_check(self):
        """启动健康检查任务"""
        if self._health_check_task and not self._health_check_task.done():
            return

        self._health_check_task = asyncio.create_task(self._health_check_loop())
        logger.info("Constant node health check started")

    async def _health_check_loop(self):
        """健康检查循环"""
        while True:
            try:
                await asyncio.sleep(self._health_check_interval)

                for local_key, node_data in list(self._local_nodes.items()):
                    parts = local_key.split(":", 1)
                    if len(parts) != 2:
                        continue

                    flow_id, node_id = parts

                    # 检查任务状态
                    if local_key in self._local_tasks:
                        task = self._local_tasks[local_key]
                        if task.done():
                            # 任务已完成，可能是异常或正常结束
                            try:
                                exc = task.exception()
                                if exc:
                                    logger.error(
                                        f"Constant node task failed: {local_key} - {exc}"
                                    )
                                    await self._update_health(
                                        flow_id, node_id, "unhealthy", str(exc)
                                    )
                            except asyncio.CancelledError:
                                pass
                        else:
                            # 任务仍在运行，更新健康状态
                            await self._update_health(flow_id, node_id, "healthy")
                    else:
                        # 没有运行任务但节点已注册
                        if node_data.get("status") == "running":
                            await self._update_health(
                                flow_id, node_id, "unknown", "No task running"
                            )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")


# 获取单例实例的快捷函数
def get_constant_node_manager() -> ConstantNodeManager:
    return ConstantNodeManager.get_instance()
