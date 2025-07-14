import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .state_store import StateStoreFactory


class NodeTaskManager:
    """
    节点任务管理器：管理所有运行中的节点任务
    设计为在多进程环境下工作，使用共享状态存储

    实现为单例模式，确保在一个进程中共享同一个实例

    Attributes:
        state_store_type: 状态存储类型（如 Redis）
        state_store_config: 状态存储配置
        worker_id: 当前工作进程的唯一标识符
        state_store: 状态存储实例, 是单例模式的实例

        _local_tasks: 本地进程内节点任务映射
        _initialized: 是否已初始化
        _tasks_key_prefix: 存储节点任务详情的键前缀
        _tasks_list_key: 存储所有节点任务列表的键
        _worker_tasks_prefix: 每个worker的节点任务列表前缀
    """

    _instance = None

    @classmethod
    def get_instance(
        cls,
        state_store_type: str = "redis",
        state_store_config: Dict[str, Any] = None,
        worker_id: str = None,
    ):
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls(state_store_type, state_store_config, worker_id)
        return cls._instance

    def __init__(
        self,
        state_store_type: str = "redis",
        state_store_config: Dict[str, Any] = None,
        worker_id: str = None,
    ):
        # 如果已经有实例，返回该实例（单例模式）
        if NodeTaskManager._instance is not None:
            return

        # 其余初始化逻辑保持不变
        self.logger = logging.getLogger(__name__)
        self.state_store_type = state_store_type
        self.state_store_config = state_store_config or {}
        self.worker_id = worker_id
        self.state_store = None
        self._local_tasks = {}  # 本地进程内节点任务映射: task_id -> task_info
        self._initialized = False

        # 定义存储键前缀
        self._tasks_key_prefix = "node_tasks:"  # 单个节点任务详情
        self._tasks_list_key = "node_tasks_list"  # 所有节点任务列表
        self._worker_tasks_prefix = "worker_tasks:"  # 每个worker的节点任务列表

    async def initialize(self) -> bool:
        """初始化节点任务管理器和状态存储"""
        if self._initialized:
            return True

        try:
            # 创建状态存储 - 修正调用方式
            self.state_store = StateStoreFactory.create(
                self.state_store_type, self.state_store_config
            )

            # 初始化状态存储
            initialized = await self.state_store.initialize()
            if not initialized:
                self.logger.error("Failed to initialize state store")
                return False

            self._initialized = True
            self.logger.info(
                f"NodeTaskManager initialized with {self.state_store_type} state store"
            )
            return True

        except Exception as e:
            self.logger.exception(f"Error initializing NodeTaskManager: {str(e)}")
            return False

    async def register_task(self, node_task_id: str, task_info: Dict[str, Any]) -> bool:
        """
        注册节点任务到管理器

        Args:
            node_task_id: 节点任务ID
            task_info: 任务信息 (包含类型、状态、开始时间等)

        Returns:
            bool: 注册是否成功
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        try:
            # 添加worker_id到任务信息
            if self.worker_id:
                task_info["worker_id"] = self.worker_id

            # 设置任务状态和时间戳
            task_info["registered_at"] = datetime.now().isoformat()
            if "status" not in task_info:
                task_info["status"] = "registered"

            # 保存到状态存储
            task_key = f"{self._tasks_key_prefix}{node_task_id}"
            await self.state_store.set_value(task_key, task_info)

            # 添加到任务列表
            await self.state_store.add_to_set(self._tasks_list_key, node_task_id)

            # 如果有worker_id，添加到worker任务列表
            if self.worker_id:
                worker_key = f"{self._worker_tasks_prefix}{self.worker_id}"
                await self.state_store.add_to_set(worker_key, node_task_id)

            # 保存到本地缓存
            self._local_tasks[node_task_id] = task_info

            self.logger.info(f"Node task {node_task_id} registered successfully")
            return True

        except Exception as e:
            self.logger.exception(
                f"Error registering node task {node_task_id}: {str(e)}"
            )
            return False

    async def update_task_status(
        self, node_task_id: str, status: str, additional_info: Dict[str, Any] = None
    ) -> bool:
        """
        更新节点任务状态

        Args:
            node_task_id: 节点任务ID
            status: 新状态
            additional_info: 要更新的额外信息

        Returns:
            bool: 更新是否成功
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        try:
            # 获取当前任务信息
            task_key = f"{self._tasks_key_prefix}{node_task_id}"
            task_info = await self.state_store.get_value(task_key)

            if not task_info:
                self.logger.warning(
                    f"Node task {node_task_id} not found, cannot update status"
                )
                return False

            # 更新状态和时间戳
            task_info["status"] = status
            task_info["updated_at"] = datetime.now().isoformat()

            # 添加额外信息
            if additional_info:
                task_info.update(additional_info)

            # 保存回状态存储
            await self.state_store.set_value(task_key, task_info)

            # 更新本地缓存（如果存在）
            if node_task_id in self._local_tasks:
                self._local_tasks[node_task_id].update(task_info)

            self.logger.info(f"Node task {node_task_id} status updated to {status}")
            return True

        except Exception as e:
            self.logger.exception(
                f"Error updating node task {node_task_id} status: {str(e)}"
            )
            return False

    async def get_task(self, node_task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取节点任务详细信息

        Args:
            node_task_id: 节点任务ID

        Returns:
            Dict 或 None: 任务信息，不存在时返回None
        """
        if not self._initialized:
            if not await self.initialize():
                return None

        try:
            # 优先从本地缓存获取
            if node_task_id in self._local_tasks:
                self.logger.debug("node_task_id in local cache")
                # 但仍然检查状态存储以确保数据是最新的
                task_key = f"{self._tasks_key_prefix}{node_task_id}"
                stored_info = await self.state_store.get_value(task_key)
                self.logger.debug(f"stored_info: {stored_info}")

                if stored_info:
                    # 更新本地缓存
                    self._local_tasks[node_task_id] = stored_info
                    return stored_info
                return self._local_tasks[node_task_id]

            # 从状态存储获取
            task_key = f"{self._tasks_key_prefix}{node_task_id}"
            task_info = await self.state_store.get_value(task_key)
            self.logger.debug(f"task_info: {task_info}")

            if task_info:
                # 缓存到本地
                self._local_tasks[node_task_id] = task_info

            return task_info

        except Exception as e:
            self.logger.exception(f"Error getting node task {node_task_id}: {str(e)}")
            return None

    async def get_all_tasks(self) -> List[Dict[str, Any]]:
        """
        获取所有节点任务信息

        Returns:
            List: 节点任务信息列表
        """
        if not self._initialized:
            if not await self.initialize():
                return []

        try:
            # 获取所有任务ID
            task_ids = await self.state_store.get_set_members(self._tasks_list_key)

            # 批量获取任务信息
            tasks = []
            for task_id in task_ids:
                task_info = await self.get_task(task_id)
                if task_info:
                    tasks.append(task_info)

            return tasks

        except Exception as e:
            self.logger.exception(f"Error getting all node tasks: {str(e)}")
            return []

    async def get_worker_tasks(self, worker_id: str = None) -> List[Dict[str, Any]]:
        """
        获取指定worker的所有节点任务

        Args:
            worker_id: 可选的worker ID，默认使用当前worker_id

        Returns:
            List: 节点任务信息列表
        """
        if not self._initialized:
            if not await self.initialize():
                return []

        worker_id = worker_id or self.worker_id
        if not worker_id:
            self.logger.warning("No worker_id specified for get_worker_tasks")
            return []

        try:
            # 获取worker的任务列表
            worker_key = f"{self._worker_tasks_prefix}{worker_id}"
            task_ids = await self.state_store.get_set_members(worker_key)

            # 批量获取任务信息
            tasks = []
            for task_id in task_ids:
                task_info = await self.get_task(task_id)
                if task_info:
                    tasks.append(task_info)

            return tasks

        except Exception as e:
            self.logger.exception(f"Error getting worker tasks: {str(e)}")
            return []

    async def stop_task(self, node_task_id: str) -> bool:
        """
        停止节点任务执行
        注意: 这个方法只设置终止标志，不实际取消任务，
        取消任务逻辑应该在调用此方法后由节点任务监控代码处理

        Args:
            node_task_id: 节点任务ID

        Returns:
            bool: 是否成功设置终止标志
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        try:
            # 设置终止标志
            await self.state_store.set_termination_flag(
                node_task_id,
                {
                    "reason": "Stopped by NodeTaskManager",
                    "timestamp": datetime.now().isoformat(),
                },
            )

            # 更新任务状态
            await self.update_task_status(
                node_task_id,
                "stopping",
                {"termination_requested_at": datetime.now().isoformat()},
            )

            self.logger.info(f"Stop request sent to node task {node_task_id}")
            return True

        except Exception as e:
            self.logger.exception(f"Error stopping node task {node_task_id}: {str(e)}")
            return False

    async def remove_task(self, node_task_id: str) -> bool:
        """
        从管理器中移除节点任务

        Args:
            node_task_id: 节点任务ID

        Returns:
            bool: 移除是否成功
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        try:
            # 从状态存储删除任务信息
            task_key = f"{self._tasks_key_prefix}{node_task_id}"
            await self.state_store.delete_value(task_key)

            # 从任务列表中移除
            await self.state_store.remove_from_set(self._tasks_list_key, node_task_id)

            # 如果有worker_id，从worker任务列表移除
            if self.worker_id:
                worker_key = f"{self._worker_tasks_prefix}{self.worker_id}"
                await self.state_store.remove_from_set(worker_key, node_task_id)

            # 从本地缓存移除
            if node_task_id in self._local_tasks:
                del self._local_tasks[node_task_id]

            self.logger.info(f"Node task {node_task_id} removed successfully")
            return True

        except Exception as e:
            self.logger.exception(f"Error removing node task {node_task_id}: {str(e)}")
            return False

    async def cleanup_worker_tasks(self, worker_id: str = None) -> bool:
        """
        清理worker的所有节点任务记录

        Args:
            worker_id: 可选的worker ID，默认使用当前worker_id

        Returns:
            bool: 清理是否成功
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        worker_id = worker_id or self.worker_id
        if not worker_id:
            self.logger.warning("No worker_id specified for cleanup_worker_tasks")
            return False

        try:
            # 获取worker的任务列表
            worker_key = f"{self._worker_tasks_prefix}{worker_id}"
            task_ids = await self.state_store.get_set_members(worker_key)

            # 批量更新任务状态为terminated
            for task_id in task_ids:
                await self.update_task_status(
                    task_id,
                    "terminated",
                    {
                        "terminated_reason": "Worker shutdown",
                        "terminated_at": datetime.now().isoformat(),
                    },
                )

            # 清空worker任务列表
            await self.state_store.delete_value(worker_key)

            self.logger.info(f"All node tasks for worker {worker_id} cleaned up")
            return True

        except Exception as e:
            self.logger.exception(f"Error cleaning up worker tasks: {str(e)}")
            return False

    async def close(self):
        """关闭节点任务管理器和状态存储"""
        if self.state_store:
            try:
                await self.state_store.close()
                self._initialized = False
                self.logger.info("NodeTaskManager closed")
            except Exception as e:
                self.logger.exception(f"Error closing NodeTaskManager: {str(e)}")
