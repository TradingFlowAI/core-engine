import abc
from typing import Any, Callable, List, Union

from aio_pika.abc import AbstractIncomingMessage


class Publisher(abc.ABC):
    """消息发布者接口"""

    @abc.abstractmethod
    async def connect(self) -> None:
        """连接到消息队列服务器"""

    @abc.abstractmethod
    async def publish(
        self, message: Any, routing_key: str, persistent: bool = True, **kwargs
    ) -> None:
        """发布消息

        Args:
            message: 消息内容
            routing_key: 路由键
            persistent: 是否持久化
            **kwargs: 其他参数
        """

    @abc.abstractmethod
    async def close(self) -> None:
        """关闭连接"""


class Consumer(abc.ABC):
    """消息消费者接口"""

    @abc.abstractmethod
    async def connect(self, connection_string: str = None) -> None:
        """连接到消息队列服务器

        Args:
            connection_string: 连接字符串，如果为None则使用默认配置
        """

    @abc.abstractmethod
    async def consume(
        self,
        queue_name: str,
        binding_keys: Union[str, List[str]],
        callback: Callable[[AbstractIncomingMessage], Any],
        prefetch_count: int = 1,
        durable: bool = True,
    ) -> None:
        """消费消息

        Args:
            queue_name: 队列名称
            binding_keys: 绑定键或绑定键列表
            callback: 回调函数，处理接收到的消息
            prefetch_count: 预取数量
            durable: 队列是否持久化
        """

    @abc.abstractmethod
    async def close(self) -> None:
        """关闭连接"""
