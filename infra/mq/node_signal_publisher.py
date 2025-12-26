"""Node Signal Publisher"""

import logging
from typing import Dict, List

from infra.mq.aio_pika_impl import AioPikaTopicPublisher
from common.edge import Edge
from common.signal_types import Signal, SignalType

logger = logging.getLogger(__name__)


class NodeSignalPublisher:
    """Publisher for node-to-node signals"""

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
        # Parse into a map for later use, key is source_handle, value is {target_node: [target_handle1, target_handle2, ...]}
        self.output_edges_map = self._parse_output_edges(output_edges)
        self.publisher = AioPikaTopicPublisher(exchange="signals_exchange")

    def _parse_output_edges(
        self, output_edges: List[Edge]
    ) -> Dict[str, Dict[str, List[str]]]:
        """
        Parse output_edges into a map structure for easy lookup.

        Args:
            output_edges: List of output edges

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

            # Initialize source_handle if not exists
            if source_handle not in edges_map:
                edges_map[source_handle] = {}

            # Initialize target_node if not exists
            if target_node not in edges_map[source_handle]:
                edges_map[source_handle][target_node] = []

            # Add target_handle (avoid duplicates)
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
        """Generate routing key.

        Format: flow.{flow_id}.component.{component_id}.cycle.{cycle}.from.{source_node}.handle.{output_handle}.to.{target_node}.handle.{input_handle}
        """
        return f"flow.{self.flow_id}.component.{self.component_id}.cycle.{self.cycle}.from.{self.node_id}.handle.{source_handle}.to.{target_node}.handle.{target_handle}"

    def _generate_stop_execution_routing_key(self) -> str:
        """Generate routing key for stop execution signal.

        Format: flow.{flow_id}.signal.{SignalType.STOP_EXECUTION}.cycle.{cycle}
        """
        return f"flow.{self.flow_id}.signal.{SignalType.STOP_EXECUTION}.cycle.{self.cycle}"

    async def send_signal(
        self,
        source_handle: str,
        signal: Signal,
    ) -> None:
        """
        Send signal, automatically choose send method based on number of target handles.

        Args:
            source_handle: Source handle name
            signal: Signal to send
        """

        # Get target info from parsed map
        targets = self.output_edges_map.get(source_handle, {})

        if not targets:
            logger.warning("No targets found for source handle: %s", source_handle)
            return

        # Iterate each target node
        for target_node, target_handles in targets.items():
            if len(target_handles) == 1:
                # Only one target handle, send precisely
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
                # Multiple target handles, use wildcard
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
        Send stop execution signal to all nodes in current cycle.

        Args:
            reason: Reason for stopping execution
        """
        try:
            # Create stop execution signal
            stop_signal = Signal(
                signal_type=SignalType.STOP_EXECUTION,
                payload={
                    "reason": reason,
                    "source_node": self.node_id,
                    "timestamp": None,
                },
                timestamp=None,
            )

            # Generate stop execution routing key
            routing_key = self._generate_stop_execution_routing_key()

            # Send signal
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
