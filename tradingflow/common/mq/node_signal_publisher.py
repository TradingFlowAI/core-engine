import logging
from typing import Dict, List

from tradingflow.common.mq.aio_pika_impl import AioPikaTopicPublisher
from tradingflow.py_worker.common.edge import Edge
from tradingflow.py_worker.common.signal_types import Signal, SignalType

logger = logging.getLogger(__name__)


class NodeSignalPublisher:
    """节点信号发布者"""

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        output_edges: List[Edge],
    ):
        self.flow_id = flow_id
        self.component_id = component_id
        self.cycle = cycle
        self.node_id = node_id
        self.output_edges = output_edges
        downstream_nodes = set()
        for edge in self.output_edges:
            downstream_nodes.add(edge.target_node)
        self.output_nodes = list(downstream_nodes)
        # 解析成一个map 供后续使用， key为source_handle，value为{target_node: [target_handle1, target_handle2, ...]}
        self.output_edges_map = self._parse_output_edges(output_edges)
        self.publisher = AioPikaTopicPublisher(exchange="signals_exchange")

    def _parse_output_edges(
        self, output_edges: List[Edge]
    ) -> Dict[str, Dict[str, List[str]]]:
        """
        解析 output_edges 成为便于查找的 map 结构

        Args:
            output_edges: 输出边列表

        Returns:
            Dict[str, Dict[str, List[str]]]:
            {
                "source_handle1": {
                    "target_node1": ["target_handle1", "target_handle2"],
                    "target_node2": ["target_handle3"]
                },
                "source_handle2": {
                    "target_node1": ["target_handle4"]
                }
            }
        """
        edges_map = {}

        for edge in output_edges:
            source_handle = edge.source_node_handle
            target_node = edge.target_node
            target_handle = edge.target_node_handle

            # 如果 source_handle 不存在，初始化
            if source_handle not in edges_map:
                edges_map[source_handle] = {}

            # 如果 target_node 不存在，初始化
            if target_node not in edges_map[source_handle]:
                edges_map[source_handle][target_node] = []

            # 添加 target_handle（避免重复）
            if target_handle not in edges_map[source_handle][target_node]:
                edges_map[source_handle][target_node].append(target_handle)

        logger.debug("Parsed output edges map: %s", edges_map)
        return edges_map

    async def connect(self):
        await self.publisher.connect()

    async def close(self):
        await self.publisher.close()

    def _generate_send_signal_routing_key(
        self,
        source_handle: str,
        target_node: str,
        target_handle: str,
    ) -> str:
        """生成路由键

        格式: component.{component_id}.cycle.{cycle}.from.{source_node}.handle.{output_handle}.to.{target_node}.handle.{input_handle}
        """
        return f"component.{self.component_id}.cycle.{self.cycle}.from.{self.node_id}.handle.{source_handle}.to.{target_node}.handle.{target_handle}"

    def _generate_stop_execution_routing_key(self) -> str:
        """生成停止执行信号的路由键

        格式: signal.{SignalType.STOP_EXECUTION}.cycle.{cycle}
        """
        return f"signal.{SignalType.STOP_EXECUTION}.cycle.{self.cycle}"

    async def send_signal(
        self,
        source_handle: str,
        signal: Signal,
    ) -> None:
        """
        发送信号，根据目标句柄数量自动选择发送方式

        Args:
            source_handle: 源句柄名称
            signal: 要发送的信号
        """

        # 从解析好的 map 中获取目标信息
        targets = self.output_edges_map.get(source_handle, {})

        if not targets:
            logger.warning("No targets found for source handle: %s", source_handle)
            return

        # 遍历每个目标节点
        for target_node, target_handles in targets.items():
            if len(target_handles) == 1:
                # 只有一个目标句柄，精确发送
                target_handle = target_handles[0]
                routing_key = self._generate_send_signal_routing_key(
                    source_handle, target_node, target_handle
                )
                await self.publisher.publish(signal.to_json(), routing_key=routing_key)
                logger.debug(
                    "Signal sent (precise): %s:%s -> %s:%s (routing_key: %s)",
                    self.node_id,
                    source_handle,
                    target_node,
                    target_handle,
                    routing_key,
                )
            else:
                # 多个目标句柄，使用通配符发送
                routing_key = self._generate_send_signal_routing_key(
                    source_handle, target_node, "*"
                )
                await self.publisher.publish(signal.to_json(), routing_key=routing_key)
                logger.debug(
                    "Signal sent (wildcard): %s -> %s:* (targets: %s, routing_key: %s)",
                    source_handle,
                    target_node,
                    target_handles,
                    routing_key,
                )

    async def send_stop_execution_signal(
        self, reason: str = "Execution stopped"
    ) -> None:
        """
        发送停止执行信号到当前周期的所有节点

        Args:
            reason: 停止执行的原因
        """
        try:
            # 创建停止执行信号
            stop_signal = Signal(
                signal_type=SignalType.STOP_EXECUTION,
                payload={
                    "reason": reason,
                    "source_node": self.node_id,
                    "timestamp": None,
                },
                timestamp=None,
            )

            # 生成停止执行的路由键
            routing_key = self._generate_stop_execution_routing_key()

            # 发送信号
            await self.publisher.publish(stop_signal.to_json(), routing_key=routing_key)

            logger.info(
                "Stop execution signal sent from node %s to cycle %s, reason: %s",
                self.node_id,
                self.cycle,
                reason,
            )

        except Exception as e:
            logger.error(
                "Failed to send stop execution signal from node %s: %s",
                self.node_id,
                str(e),
            )
            raise
