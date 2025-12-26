"""Node Signal Consumer"""

import logging
from typing import Any, Callable, Coroutine, Dict, List, Optional

from aio_pika.abc import AbstractIncomingMessage

from infra.mq.aio_pika_impl import AioPikaTopicConsumer
from common.edge import Edge
from common.signal_types import Signal, SignalType

logger = logging.getLogger(__name__)


class NodeSignalConsumer:
    """Consumer for node-to-node signals"""

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
        # Check if input edges contain target node handles
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
        # TODO: Make this configurable
        self.consumer = AioPikaTopicConsumer(exchange="signals_exchange")
        # Include cycle in queue name to prevent cross-cycle signal contamination
        self.queue_name = f"queue.flow.{self.flow_id}.cycle.{self.cycle}.node.{self.node_id}"
        self.upstream_routing_key = self._generate_receive_signal_routing_key()
        self.stop_execution_routing_key = (
            f"flow.{self.flow_id}.signal.{SignalType.STOP_EXECUTION}.cycle.{self.cycle}"
        )

        # Use custom handler or default handler
        self._on_signal_handler = on_signal_handler or self._default_on_signal

    async def connect(self):
        await self.consumer.connect()

    async def close(self):
        await self.consumer.close()

    def _generate_receive_signal_routing_key(
        self,
    ) -> str:
        """Generate routing key.

        Format: flow.{flow_id}.component.{component_id}.cycle.{cycle}.from.{source_node}.handle.{output_handle}.to.{target_node}.handle.{input_handle}
        """
        return f"flow.{self.flow_id}.component.{self.component_id}.cycle.{self.cycle}.from.*.handle.*.to.{self.node_id}.handle.*"

    def _parse_routing_key(self, routing_key: str) -> Dict[str, Any]:
        """Parse routing key and extract context information.

        Args:
            routing_key: Routing key string

        Returns:
            Dict[str, Any]: Dictionary containing parsed context information
        """
        try:
            # Check if it's a stop execution signal
            if routing_key == self.stop_execution_routing_key:
                return {
                    "signal_type": "STOP_EXECUTION",
                    "cycle": self.cycle,
                    "routing_key": routing_key,
                    "parsed": True,
                }

            # Parse standard signal routing key
            # Format: flow.{flow_id}.component.{component_id}.cycle.{cycle}.from.{source_node}.handle.{output_handle}.to.{target_node}.handle.{input_handle}
            parts = routing_key.split(".")

            if len(parts) >= 14:
                context = {
                    "signal_type": "STANDARD",
                    "flow_id": parts[1],
                    "component_id": parts[3],
                    "cycle": parts[5],
                    "source_node": parts[7],
                    "source_handle": parts[9],
                    "target_node": parts[11],
                    "target_handle": parts[13],
                    "routing_key": routing_key,
                    "parsed": True,
                }

                # Try to convert numeric types
                try:
                    context["component_id"] = int(context["component_id"])
                    context["cycle"] = int(context["cycle"])
                except ValueError:
                    # Keep string format
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
                # Get routing_key
                routing_key = message.routing_key
                # Get header
                headers = message.headers

                # Parse routing key to get context info
                signal_context = self._parse_routing_key(routing_key)

                # Add extra context info
                signal_context.update(
                    {
                        "headers": headers,
                        "consumer_node_id": self.node_id,
                        "consumer_component_id": self.component_id,
                        "consumer_cycle": self.cycle,
                        "consumer_flow_id": self.flow_id,
                    }
                )

                # Check if it's a stop execution signal
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
                    # Handle stop execution signal
                    await self.on_signal(
                        signal, handle="STOP_EXECUTION", signal_context=signal_context
                    )
                    return

                # Get target info from context
                target_node_id = signal_context.get("target_node")
                target_handle = signal_context.get("target_handle")
                message_cycle = signal_context.get("cycle")

                # Strict cycle validation - reject messages from different cycles
                if message_cycle != self.cycle and message_cycle is not None:
                    logger.warning(
                        "Rejecting message from different cycle: expected %s, got %s [key=%s]",
                        self.cycle,
                        message_cycle,
                        routing_key,
                    )
                    return

                # There are cases without specific target handle
                # In this case target_handle is '*'
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

                # Validate node_id and handle
                if target_node_id != self.node_id:
                    logger.warning(
                        "Received message for different node: expected %s, got %s",
                        self.node_id,
                        target_node_id,
                    )
                    # Handle signal
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
                        # Handle signal
                        await self.on_signal(
                            signal, handle=None, signal_context=signal_context
                        )
                    else:
                        # Handle signal
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
        """Signal callback function.

        Args:
            signal: Received signal
            handle: Target handle name
            signal_context: Signal context with info parsed from routing key
        """
        if signal_context is None:
            signal_context = {}

        await self._on_signal_handler(signal, handle, signal_context)

    async def _default_on_signal(
        self, signal: Signal, handle: str = None, signal_context: Dict[str, Any] = None
    ) -> None:
        """Default signal handler.

        Args:
            signal: Received signal
            handle: Target handle name
            signal_context: Signal context with info parsed from routing key
        """
        logger.info(
            "Default signal handler: Received signal %s with handle %s and context %s",
            signal.to_json(),
            handle,
            signal_context,
        )
