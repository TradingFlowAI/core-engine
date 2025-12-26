"""Message Queue Interfaces"""

import abc
from typing import Any, Callable, List, Union

from aio_pika.abc import AbstractIncomingMessage


class Publisher(abc.ABC):
    """Message publisher interface"""

    @abc.abstractmethod
    async def connect(self) -> None:
        """Connect to message queue server"""

    @abc.abstractmethod
    async def publish(
        self, message: Any, routing_key: str, persistent: bool = True, **kwargs
    ) -> None:
        """Publish message.

        Args:
            message: Message content
            routing_key: Routing key
            persistent: Whether to persist
            **kwargs: Additional parameters
        """

    @abc.abstractmethod
    async def close(self) -> None:
        """Close connection"""


class Consumer(abc.ABC):
    """Message consumer interface"""

    @abc.abstractmethod
    async def connect(self, connection_string: str = None) -> None:
        """Connect to message queue server.

        Args:
            connection_string: Connection string, uses default config if None
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
        """Consume messages.

        Args:
            queue_name: Queue name
            binding_keys: Binding key or list of binding keys
            callback: Callback function to process received messages
            prefetch_count: Prefetch count
            durable: Whether queue is persistent
        """

    @abc.abstractmethod
    async def close(self) -> None:
        """Close connection"""
