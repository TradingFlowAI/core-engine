import logging
from typing import Any, Callable, Coroutine, Dict, List, Optional

from aio_pika.abc import AbstractIncomingMessage

from tradingflow.common.mq.aio_pika_impl import AioPikaTopicConsumer
from tradingflow.py_worker.common.edge import Edge
from tradingflow.py_worker.common.signal_types import Signal, SignalType

logger = logging.getLogger(__name__)


class NodeSignalConsumer:
    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        input_edges: List[Edge],
        on_signal_handler: Optional[
            Callable[[Signal, str, Dict[str, Any]], Coroutine[Any, Any, None]]
        ] = None,
    ):
        self.flow_id = flow_id
        self.component_id = component_id
        self.cycle = cycle
        self.node_id = node_id
        self.input_edges = input_edges
        self._target_handles = []
        # 检查输入边是否包含目标节点的句柄
        for edge in input_edges:
            if edge.target_node != node_id:
                logger.warning(
                    "Input edge target node %s does not match expected node_id %s",
                    edge.target_node,
                    node_id,
                )
            else:
                if edge.target_node_handle not in self._target_handles:
                    self._target_handles.append(edge.target_node_handle)
        # TODO 参数化
        self.consumer = AioPikaTopicConsumer(exchange="signals_exchange")
        self.queue_name = f"queue.flow.{self.flow_id}.node.{self.node_id}"
        self.upstream_routing_key = self._generate_receive_signal_routing_key()
        self.stop_execution_routing_key = (
            f"signal.{SignalType.STOP_EXECUTION}.cycle.{self.cycle}"
        )

        # 使用自定义处理器或默认处理器
        self._on_signal_handler = on_signal_handler or self._default_on_signal

    async def connect(self):
        await self.consumer.connect()

    async def close(self):
        await self.consumer.close()

    def _generate_receive_signal_routing_key(
        self,
    ) -> str:
        """生成路由键

        格式: component.{component_id}.cycle.{cycle}.from.{source_node}.handle.{output_handle}.to.{target_node}.handle.{input_handle}

        """
        return f"component.{self.component_id}.cycle.{self.cycle}.from.*.handle.*.to.{self.node_id}.handle.*"

    def _parse_routing_key(self, routing_key: str) -> Dict[str, Any]:
        """解析路由键并提取上下文信息

        Args:
            routing_key: 路由键字符串

        Returns:
            Dict[str, Any]: 包含解析出的上下文信息的字典
        """
        try:
            # 检查是否为停止执行信号
            if routing_key == self.stop_execution_routing_key:
                return {
                    "signal_type": "STOP_EXECUTION",
                    "cycle": self.cycle,
                    "routing_key": routing_key,
                    "parsed": True,
                }

            # 解析标准信号路由键
            # 格式: component.{component_id}.cycle.{cycle}.from.{source_node}.handle.{output_handle}.to.{target_node}.handle.{input_handle}
            parts = routing_key.split(".")

            if len(parts) >= 12:
                context = {
                    "signal_type": "STANDARD",
                    "component_id": parts[1],
                    "cycle": parts[3],
                    "source_node": parts[5],
                    "source_handle": parts[7],
                    "target_node": parts[9],
                    "target_handle": parts[11],
                    "routing_key": routing_key,
                    "parsed": True,
                }

                # 尝试转换数字类型
                try:
                    context["component_id"] = int(context["component_id"])
                    context["cycle"] = int(context["cycle"])
                except ValueError:
                    # 保持字符串格式
                    pass

                return context
            else:
                logger.warning("Unexpected routing key format: %s", routing_key)
                return {
                    "signal_type": "UNKNOWN",
                    "routing_key": routing_key,
                    "parsed": False,
                    "raw_parts": parts,
                }

        except Exception as e:
            logger.error("Error parsing routing key %s: %s", routing_key, e)
            return {
                "signal_type": "ERROR",
                "routing_key": routing_key,
                "parsed": False,
                "error": str(e),
            }

    async def consume(self):

        async def wrapper_callback(message: AbstractIncomingMessage) -> None:
            async with message.process():
                body = message.body.decode()
                signal = Signal.from_json(body)
                # 获取routing_key
                routing_key = message.routing_key
                # 获取header
                headers = message.headers

                # 解析路由键获取上下文信息
                signal_context = self._parse_routing_key(routing_key)

                # 添加额外的上下文信息
                signal_context.update(
                    {
                        "headers": headers,
                        "consumer_node_id": self.node_id,
                        "consumer_component_id": self.component_id,
                        "consumer_cycle": self.cycle,
                        "consumer_flow_id": self.flow_id,
                    }
                )

                # 检查是否为停止执行信号
                if signal_context.get("signal_type") == "STOP_EXECUTION":
                    logger.info(
                        "Received STOP_EXECUTION signal for cycle %s, reason: %s",
                        self.cycle,
                        (
                            signal.payload.get("reason", "No reason provided")
                            if signal.payload
                            else "No payload"
                        ),
                    )
                    # 处理停止执行信号
                    await self.on_signal(
                        signal, handle="STOP_EXECUTION", signal_context=signal_context
                    )
                    return

                # 从上下文中获取目标信息
                target_node_id = signal_context.get("target_node")
                target_handle = signal_context.get("target_handle")

                # 也有不指明target handle 的情况
                # 此时 target_handle 为 '*'
                if target_handle == "*":
                    logger.info(
                        "Received message without specific target handle: [key=%s][body=%s][headers -> %s]",
                        routing_key,
                        body,
                        headers,
                    )

                logger.info(
                    "Received message: [key=%s][body=%s][headers -> %s][context -> %s]",
                    routing_key,
                    body,
                    headers,
                    signal_context,
                )

                # 校验 node_id 和 handle
                if target_node_id != self.node_id:
                    logger.warning(
                        "Received message for different node: expected %s, got %s",
                        self.node_id,
                        target_node_id,
                    )
                    # 处理信号
                    await self.on_signal(signal, signal_context=signal_context)
                else:
                    if (
                        target_handle not in self._target_handles
                        and target_handle != "*"
                    ):
                        logger.warning(
                            "Received message for different handle: expected %s, got %s",
                            self._target_handles,
                            target_handle,
                        )
                        # 处理信号
                        await self.on_signal(
                            signal, handle=None, signal_context=signal_context
                        )
                    else:
                        # 处理信号
                        await self.on_signal(
                            signal, handle=target_handle, signal_context=signal_context
                        )

        logger.info(
            "Starting to consume messages from queue %s with routing key %s",
            self.queue_name,
            self.upstream_routing_key,
        )
        logger.info(
            "Also listening for stop execution signals with routing key %s",
            self.stop_execution_routing_key,
        )
        await self.consumer.consume(
            queue_name=self.queue_name,
            binding_keys=[self.upstream_routing_key, self.stop_execution_routing_key],
            callback=wrapper_callback,
        )

    async def on_signal(
        self, signal: Signal, handle: str = None, signal_context: Dict[str, Any] = None
    ) -> None:
        """处理信号的回调函数

        Args:
            signal: 接收到的信号
            handle: 目标句柄名称
            signal_context: 信号上下文，包含从routing key解析的信息
        """
        if signal_context is None:
            signal_context = {}

        await self._on_signal_handler(signal, handle, signal_context)

    async def _default_on_signal(
        self, signal: Signal, handle: str = None, signal_context: Dict[str, Any] = None
    ) -> None:
        """默认的信号处理函数

        Args:
            signal: 接收到的信号
            handle: 目标句柄名称
            signal_context: 信号上下文，包含从routing key解析的信息
        """
        logger.info(
            "Default signal handler: Received signal %s with handle %s and context %s",
            signal.to_json(),
            handle,
            signal_context,
        )
