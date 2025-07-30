import abc
import asyncio
import logging
import traceback
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from tradingflow.depot.python.exceptions.tf_exception import NodeStopExecutionException
from tradingflow.depot.python.mq.node_signal_consumer import NodeSignalConsumer
from tradingflow.depot.python.mq.node_signal_publisher import NodeSignalPublisher
from tradingflow.station.common.edge import Edge
from tradingflow.station.common.signal_types import Signal, SignalType
from tradingflow.station.common.state_store import StateStoreFactory

if TYPE_CHECKING:
    from tradingflow.station.common.state_store import StateStore


@dataclass
class InputHandle:
    """Input handle definition"""

    name: str
    data_type: type
    description: str = ""
    example: Any = None
    auto_update_attr: str = None
    is_aggregate: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            "type": self.data_type.__name__,
            "description": self.description,
            "example": self.example,
            "auto_update_attr": self.auto_update_attr,
            "is_aggregate": self.is_aggregate,
        }


@dataclass
class NodeMetadata:
    """Node metadata definition"""

    version: str = "0.0.1"
    display_name: str = ""
    node_category: str = "base"  # "base", "instance", "variant"
    base_node_type: Optional[str] = None
    description: str = ""
    author: str = ""
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            "version": self.version,
            "display_name": self.display_name,
            "node_category": self.node_category,
            "base_node_type": self.base_node_type,
            "description": self.description,
            "author": self.author,
            "tags": self.tags,
        }


class NodeStatus(Enum):
    """Node status enumeration"""

    PENDING = "pending"  # Waiting to execute
    RUNNING = "running"  # Currently executing
    COMPLETED = "completed"  # Execution completed
    FAILED = "failed"  # Execution failed
    SKIPPED = "skipped"  # Execution skipped
    TERMINATED = "terminated"  # Execution terminated


class NodeBase(abc.ABC):
    """Base node class"""

    # Class-level metadata - can be overridden by subclasses
    _metadata: Optional[NodeMetadata] = None

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store: "StateStore" = None,
        # 添加元数据相关参数
        version: str = None,
        display_name: str = None,
        node_category: str = None,
        base_node_type: str = None,
        description: str = None,
        author: str = None,
        tags: List[str] = None,
        **kwargs  # 接收其他参数，保持向后兼容
    ):
        """
        Initialize node

        Args:
            flow_id: Flow ID
            component_id: Component ID in graph
            cycle: Cycle number
            node_id: Node unique identifier
            name: Node name
            input_edges: List of input edges
            output_edges: List of output edges
            state_store: Initialized state store instance (used if provided)
            version: Node version
            display_name: Display name for UI
            node_category: Node category (base/instance/variant)
            base_node_type: Base node type if this is an instance
            description: Node description
            author: Node author
            tags: Node tags
        """
        # Logger setup
        self.logger = logging.getLogger(f"Node.{node_id}")
        self.flow_id = flow_id
        self.component_id = component_id
        self.cycle = cycle
        self.node_id = node_id
        self.name = name
        self._input_edges = input_edges or []
        self._output_edges = output_edges or []

        # 初始化元数据
        self._instance_metadata = NodeMetadata(
            version=version or "0.0.1",
            display_name=display_name or name or self.__class__.__name__,
            node_category=node_category or "base",
            base_node_type=base_node_type,
            description=description or self.__class__.__doc__ or "",
            author=author or "",
            tags=tags or []
        )
        # NOTE: 描述运行时的输入信号有哪些，注意与_input_handles的区别
        self._input_signals = {}

        # Input handles registry
        # NOTE: 描述静态的handle有哪些
        self._input_handles: Dict[str, InputHandle] = {}

        # Register input handles defined by subclass
        self._register_input_handles()

        self.status = NodeStatus.PENDING
        self.error_message = None

        # Message queue configuration
        if self._input_edges:
            self.logger.debug(
                "Node %s has input %d edges: %s",
                self.node_id,
                len(self._input_edges),
                self._input_edges,
            )
            self.node_signal_consumer = NodeSignalConsumer(
                self.flow_id,
                self.component_id,
                self.cycle,
                self.node_id,
                self._input_edges,
                on_signal_handler=self._on_signal_received,
            )
            for edge in self._input_edges:
                if edge.target_node == self.node_id:
                    edge_key = self._get_edge_key(
                        edge.source_node,
                        edge.source_node_handle,
                        edge.target_node_handle,
                    )
                    self._input_signals[edge_key] = None
            self.logger.debug(
                "Node %s registered input signals: %s",
                self.node_id,
                list(self._input_signals.keys()),
            )
        else:
            self.logger.debug("Node %s has no input edges", self.node_id)
            self.node_signal_consumer = None
        self.node_signal_publisher = NodeSignalPublisher(
            self.flow_id,
            self.component_id,
            self.cycle,
            self.node_id,
            self._output_edges,
        )

        # State store configuration
        self.state_store = state_store  # Use provided state_store
        self._owns_state_store = False  # Flag if we created the state_store

        # Signal ready future - used to notify when all required signals are received
        self._signal_ready_future = None

        # 添加停止执行标记
        self._stop_execution_requested = False
        self._stop_execution_reason = None
        self._stop_execution_source = None

        # Initialize flow execution log service
        self._log_service = None

    async def _initialize_log_service(self):
        """Initialize flow execution log service lazily"""
        if self._log_service is None:
            try:
                from tradingflow.depot.python.db.services.flow_execution_log_service import (
                    FlowExecutionLogService,
                )
                self._log_service = FlowExecutionLogService()
            except Exception as e:
                self.logger.warning("Failed to initialize log service: %s", str(e))

    async def persist_log(
        self,
        message: str,
        log_level: str = "INFO",
        log_source: str = "node",
        log_metadata: Optional[Dict] = None,
    ):
        """
        Persist log message to database and also log to console

        Args:
            message: Log message content
            log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_source: Log source (node, system, user)
            log_metadata: Additional structured metadata
        """
        # Log to console first
        log_level_upper = log_level.upper()
        if log_level_upper == "DEBUG":
            self.logger.debug(message)
        elif log_level_upper == "INFO":
            self.logger.info(message)
        elif log_level_upper == "WARNING":
            self.logger.warning(message)
        elif log_level_upper == "ERROR":
            self.logger.error(message)
        elif log_level_upper == "CRITICAL":
            self.logger.critical(message)
        else:
            self.logger.info(message)

        # Persist to database
        try:
            await self._initialize_log_service()
            if self._log_service:
                await self._log_service.create_log(
                    flow_id=self.flow_id,
                    cycle=self.cycle,
                    message=message,
                    node_id=self.node_id,
                    log_level=log_level_upper,
                    log_source=log_source,
                    log_metadata=log_metadata,
                )
        except Exception as e:
            # Don't let logging errors break node execution
            self.logger.warning("Failed to persist log to database: %s", str(e))

    async def initialize_state_store(self) -> bool:
        """
        Initialize state store

        Returns:
            bool: Whether initialization was successful
        """
        # If state_store already exists, use it
        if self.state_store is not None:
            self.logger.info("Using provided state store for node %s", self.node_id)

            # Set initial node status
            await self.set_status(NodeStatus.PENDING)

            # Clear any termination flag that might exist
            await self.state_store.clear_termination_flag(self.node_id)

            return True

        # Otherwise create a new state_store instance
        try:
            # Create state store using factory pattern
            self.state_store = StateStoreFactory.create()
            self._owns_state_store = True  # Mark as self-created

            # Initialize state store
            if not await self.state_store.initialize():
                return False

            # Set initial node status
            await self.set_status(NodeStatus.PENDING)

            # Clear any termination flag that might exist
            await self.state_store.clear_termination_flag(self.node_id)

            self.logger.info("State store initialized for node %s", self.node_id)
            return True
        except Exception as e:
            self.logger.error("Failed to initialize state store: %s", str(e))
            return False

    async def close_state_store(self):
        """Close state store connection, only if we created it"""
        if self.state_store and self._owns_state_store:  # Only close if we created it
            await self.state_store.close()
            self.logger.info("State store connection closed for node %s", self.node_id)

    async def initialize_message_queue(self) -> bool:
        """
        Initialize message queue asynchronously

        Returns:
            bool: Whether initialization was successful
        """
        try:
            await self.node_signal_publisher.connect()

            # Register consumer if there are signals to listen for
            if self._input_edges:
                await self.node_signal_consumer.connect()

            self.logger.info("Message queue initialized for node %s", self.node_id)
            return True

        except Exception as e:
            self.logger.error("Failed to initialize message queue: %s", str(e))
            self.logger.error(traceback.format_exc())
            return False

    def _deduce_input_handler_name(self, handle_name: str) -> str:
        """
        Get standardized handler function name for a given handle

        Args:
            handle_name: The input handle name

        Returns:
            str: The standardized handler function name
        """
        return f"_on_{handle_name}_received"

    def _get_edge_key(
        self, source_node: str, source_handle: str, target_handle: str
    ) -> str:
        """
        Generate edge key for input signals tracking

        Args:
            source_node: Source node ID
            source_handle: Source handle name
            target_handle: Target handle name

        Returns:
            str: Edge key in format "source_node:source_handle->target_handle"
        """
        return f"{source_node}:{source_handle}->{target_handle}"

    def _find_edge_by_handle(self, handle: str) -> Edge:
        """
        Find edge by target handle name

        Args:
            handle: Target handle name

        Returns:
            Edge: First matching edge, or None if not found
        """
        for edge in self._input_edges:
            if edge.target_node_handle == handle:
                return edge
        return None

    async def _on_signal_received(
        self, signal: Signal, handle: str = None, signal_context: Dict[str, Any] = None
    ) -> None:
        """
        Process received signal

        1. Add signal to the signal queue
        2. Check if all required signals have been received
        3. Trigger future if signal_ready_future is set and execution conditions are met

        Args:
            signal: Received signal
            handle: Target handle name
            signal_context: Signal context containing routing key information
        """
        try:
            self.logger.info(
                "Received signal: %s , handle: %s", signal.type.value, handle
            )
            # 处理停止执行信号
            if handle == "STOP_EXECUTION" or signal.type == SignalType.STOP_EXECUTION:
                await self._handle_stop_execution_signal(signal)
                return

            # Add to signal dictionary
            if handle in [edge.target_node_handle for edge in self._input_edges]:
                # 找到对应的edge来构建完整的key
                self.logger.debug("Received signal[signal.payload] %s", signal.payload)

                # 优先从 signal_context 中获取 source 信息
                source_node = None
                source_handle = None

                if signal_context and signal_context.get("parsed", False):
                    # 从 routing key 解析的上下文中获取
                    source_node = signal_context.get("source_node")
                    source_handle = signal_context.get("source_handle")
                    self.logger.debug(
                        "Found source info from signal_context: node=%s, handle=%s",
                        source_node,
                        source_handle,
                    )
                else:
                    # 回退到从 signal.payload 中获取（兼容旧版本）
                    if signal.payload and isinstance(signal.payload, dict):
                        source_node = signal.payload.get("_source_node")
                        source_handle = signal.payload.get("_source_handle")
                        self.logger.debug(
                            "Found source info from signal.payload: node=%s, handle=%s",
                            source_node,
                            source_handle,
                        )

                if source_node and source_handle:
                    edge_key = self._get_edge_key(source_node, source_handle, handle)
                    if edge_key in self._input_signals:
                        # Update signal
                        self._input_signals[edge_key] = signal
                        self.logger.debug("Updated signal for edge: %s", edge_key)
                    else:
                        self.logger.warning(
                            "Edge key not found in input signals: %s", edge_key
                        )

                else:
                    # 如果没有source信息，尝试从edges中推断（兼容旧版本）
                    edge = self._find_edge_by_handle(handle)
                    if edge:
                        edge_key = self._get_edge_key(
                            edge.source_node, edge.source_node_handle, handle
                        )
                        self._input_signals[edge_key] = signal
                        self.logger.debug(
                            f"Updated signal for edge (inferred): {edge_key}"
                        )
                    else:
                        self.logger.warning(
                            "Cannot determine source for handle %s, no matching edge found",
                            handle,
                        )

                # 执行默认的成员变量更新逻辑
                await self._handle_default_signal_processing(handle, signal)

                # 然后执行自定义的处理函数（如果存在）
                handler_name = self._deduce_input_handler_name(handle)
                if hasattr(self, handler_name):
                    # Call specific signal handler function
                    await getattr(self, handler_name)(signal)

            elif handle == "*":
                # Handle wildcard case, update all input signals
                # 拆包signal，根据signal中的key来更新所有输入信号
                self.logger.info("Received wildcard signal, updating all input signals")
                # Get all available input handles
                input_handles = self.get_input_handles()
                self.logger.debug(
                    "Available input handles: %s", list(input_handles.keys())
                )
                payload = signal.payload
                self.logger.debug("Signal payload: %s", payload)

                # 检查 payload 是否为字典类型
                if payload and isinstance(payload, dict):
                    # Process each handle if it exists in the payload
                    for handle_name, handle_obj in input_handles.items():
                        if handle_name in payload:
                            # 对于通配符情况，更新所有匹配该target_handle的edge_key
                            for edge_key in self._input_signals.keys():
                                if edge_key.endswith(f"->{handle_name}"):
                                    self._input_signals[edge_key] = payload[handle_name]

                            # 执行默认的成员变量更新逻辑
                            await self._handle_default_signal_processing(
                                handle_name, payload[handle_name], is_direct_value=True
                            )

                            # Call the specific handler if it exists
                            handler_name = self._deduce_input_handler_name(handle_name)
                            if hasattr(self, handler_name):
                                self.logger.debug(
                                    "Calling handler %s for handle %s",
                                    handler_name,
                                    handle_name,
                                )
                                # Call the handler with the payload for this handle
                                await getattr(self, handler_name)(payload[handle_name])
                            else:
                                self.logger.debug(
                                    "No handler found for handle %s", handle_name
                                )
                else:
                    self.logger.warning(
                        "Wildcard signal payload is not a dictionary: %s", type(payload)
                    )

            else:
                self.logger.warning("Received signal with unknown handle: %s", handle)

            # Check if all required signals are received, trigger future if set
            if self._signal_ready_future and not self._signal_ready_future.done():
                if self.can_execute():
                    self.logger.debug(
                        "All required signals received, marking ready to execute"
                    )
                    self._signal_ready_future.set_result(True)

        except Exception as e:
            self.logger.error("Error processing signal: %s", str(e))
            raise

    async def _handle_default_signal_processing(
        self, handle_name: str, signal_or_value, is_direct_value: bool = False
    ) -> None:
        """
        处理默认的信号处理逻辑：自动更新成员变量

        Args:
            handle_name: 处理句柄名称
            signal_or_value: Signal对象或直接的值
            is_direct_value: 是否为直接值（通配符情况）
        """
        self.logger.debug(
            "Handling default signal processing for handle '%s' with value: %s",
            handle_name,
            signal_or_value,
        )
        if handle_name not in self._input_handles:
            self.logger.debug(
                "Handle '%s' not registered in input handles, skipping auto-update",
                handle_name,
            )
            return

        handle_obj = self._input_handles[handle_name]
        if not handle_obj.auto_update_attr:
            self.logger.debug(
                "Handle '%s' does not have auto_update_attr defined, skipping auto-update",
                handle_name,
            )
            return

        # 获取要更新的值
        if is_direct_value:
            raw_value = signal_or_value
        else:
            raw_value = (
                signal_or_value.payload
                if hasattr(signal_or_value, "payload")
                else signal_or_value
            )

        # 类型检查和值提取
        expected_type = handle_obj.data_type
        final_value = self._extract_typed_value(raw_value, expected_type, handle_name)

        # 更新成员变量
        if hasattr(self, handle_obj.auto_update_attr):
            old_value = getattr(self, handle_obj.auto_update_attr)
            setattr(self, handle_obj.auto_update_attr, final_value)
            self.logger.info(
                "Auto-updated %s: %s -> %s (handle: %s, type: %s)",
                handle_obj.auto_update_attr,
                old_value,
                final_value,
                handle_name,
                expected_type.__name__,
            )
        else:
            self.logger.warning(
                "Auto-update attribute '%s' not found in node (handle: %s)",
                handle_obj.auto_update_attr,
                handle_name,
            )

    def _extract_typed_value(
        self, raw_value: Any, expected_type: type, handle_name: str
    ) -> Any:
        """
        根据期望类型从原始值中提取合适的值

        Args:
            raw_value: 原始接收到的值
            expected_type: 期望的数据类型
            handle_name: 句柄名称

        Returns:
            Any: 提取并转换后的值
        """
        # 如果原始值就是期望类型，直接返回
        if isinstance(raw_value, expected_type):
            return raw_value

        # 定义简单类型
        simple_types = (str, int, float, bool)

        # 如果期望的是简单类型，而接收到的是字典
        if expected_type in simple_types and isinstance(raw_value, dict):
            # 尝试从字典中提取以handle_name为key的值
            if handle_name in raw_value:
                extracted_value = raw_value[handle_name]
                self.logger.debug(
                    "Extracted value '%s' from dict for handle '%s'",
                    extracted_value,
                    handle_name,
                )

                # 尝试类型转换
                try:
                    if expected_type == str:
                        return str(extracted_value)
                    elif expected_type == int:
                        return int(extracted_value)
                    elif expected_type == float:
                        return float(extracted_value)
                    elif expected_type == bool:
                        return bool(extracted_value)
                    else:
                        return extracted_value
                except (ValueError, TypeError) as e:
                    self.logger.warning(
                        "Failed to convert extracted value '%s' to type %s for handle '%s': %s",
                        extracted_value,
                        expected_type.__name__,
                        handle_name,
                        str(e),
                    )
                    return extracted_value
            else:
                self.logger.warning(
                    "Handle '%s' not found in dict payload: %s",
                    handle_name,
                    (
                        list(raw_value.keys())
                        if isinstance(raw_value, dict)
                        else raw_value
                    ),
                )
                return None

        # 如果期望的是复杂类型（dict, list等），但接收到简单类型
        elif expected_type not in simple_types and not isinstance(raw_value, dict):
            self.logger.warning(
                "Expected complex type %s but received simple type %s for handle '%s'",
                expected_type.__name__,
                type(raw_value).__name__,
                handle_name,
            )
            return raw_value

        # 其他情况：尝试直接类型转换
        else:
            try:
                if expected_type in simple_types:
                    if expected_type == str:
                        return str(raw_value)
                    elif expected_type == int:
                        return int(raw_value)
                    elif expected_type == float:
                        return float(raw_value)
                    elif expected_type == bool:
                        return bool(raw_value)

                # 对于复杂类型，直接返回原值
                return raw_value

            except (ValueError, TypeError) as e:
                self.logger.warning(
                    "Failed to convert value '%s' to type %s for handle '%s': %s",
                    raw_value,
                    expected_type.__name__,
                    handle_name,
                    str(e),
                )
                return raw_value

    async def _handle_stop_execution_signal(self, signal: Signal) -> None:
        """
        处理停止执行信号 - 设置停止标记而不是抛异常

        Args:
            signal: 停止执行信号
        """
        reason = "Unknown reason"
        source_node = "Unknown source"

        if signal.payload:
            reason = signal.payload.get("reason", "Unknown reason")
            source_node = signal.payload.get("source_node", "Unknown source")

        self.logger.warning(
            "Received STOP_EXECUTION signal from %s, reason: %s", source_node, reason
        )

        # 设置停止标记
        self._stop_execution_requested = True
        self._stop_execution_reason = reason
        self._stop_execution_source = source_node

        # 如果有等待中的future，取消它
        if self._signal_ready_future and not self._signal_ready_future.done():
            self._signal_ready_future.cancel()
            self.logger.info(
                "Cancelled signal ready future due to stop execution signal"
            )

        # 调用自定义的停止处理方法（如果存在）
        if hasattr(self, "_on_stop_execution"):
            await self._on_stop_execution(signal)

    async def send_signal(
        self,
        source_handle: str,
        signal_type: SignalType,
        payload: Dict[str, Any] = None,
    ) -> bool:
        """
        Send signal asynchronously

        Args:
            signal_type: Signal type
            payload: Signal data payload

        Returns:
            bool: Whether sending was successful
        """
        try:
            # Send signal
            signal = Signal(
                signal_type=signal_type,
                payload=payload,
                timestamp=None,
            )
            await self.node_signal_publisher.send_signal(source_handle, signal)
            return True
        except Exception as e:
            self.logger.error("Failed to send signal: %s", str(e))
            self.logger.error(traceback.format_exc())
            return False

    async def send_stop_execution_signal(
        self, reason: str = "Execution stopped"
    ) -> bool:
        """
        发送停止执行信号

        Args:
            reason: 停止执行的原因

        Returns:
            bool: 是否发送成功
        """
        try:
            await self.node_signal_publisher.send_stop_execution_signal(reason)
            self.logger.info("Stop execution signal sent, reason: %s", reason)
            return True
        except Exception as e:
            self.logger.error("Failed to send stop execution signal: %s", str(e))
            return False

    def can_execute(self) -> bool:
        """
        Check if node can execute

        Returns:
            bool: True if node can execute
        """
        # If no required signals, can execute immediately
        if not self._input_signals:
            return True

        # Check if all required signals have been received
        for edge_key, signal in self._input_signals.items():
            if signal is None:
                self.logger.debug("Missing required signal for edge: %s", edge_key)
                return False
        return True

    @abc.abstractmethod
    async def execute(self) -> bool:
        """
        Execute node logic, must be overridden by subclasses

        Returns:
            bool: Whether execution was successful
        """

    async def set_status(self, status: NodeStatus, error_message: str = None):
        """
        Set node status and update in state store

        Args:
            status: New status
            error_message: Error message (if any)
        """
        self.status = status
        self.error_message = error_message

        try:
            if self.state_store:
                # Update node status in state store
                await self.state_store.set_node_task_status(
                    self.node_id, status.value, error_message
                )
        except Exception as e:
            self.logger.error("Failed to update node status in state store: %s", str(e))

        self.logger.info("Node %s status changed to %s", self.node_id, status.value)

        if status == NodeStatus.FAILED and error_message:
            self.logger.error("Node %s failed: %s", self.node_id, error_message)

    async def start(self) -> bool:
        """
        Start node processing.

        If node needs to receive specific signals before executing, wait for all necessary signals;
        If node doesn't depend on any input signals, execute directly.

        Returns:
            bool: Whether execution was successful
        """
        try:
            # Initialize state store
            self.logger.info("Initializing state store")
            if not await self.initialize_state_store():
                self.logger.error("Failed to initialize state store")
                return False

            # Initialize message queue
            self.logger.info("Initializing message queue")
            if not await self.initialize_message_queue():
                self.logger.error("Failed to initialize message queue")
                return False

            # Set node status to running
            await self.set_status(NodeStatus.RUNNING)

            # Decide execution strategy based on whether signals are needed
            if self._input_edges:
                for edge in self._input_edges:
                    await self.persist_log(
                        f"Node {self.node_id} waiting for signal: {edge.source_node_handle} -> {edge.target_node_handle}",
                        log_level="INFO",
                    )
                # Create a new Future
                self._signal_ready_future = asyncio.Future()

                # Start consuming signals
                await self.node_signal_consumer.consume()
                await self.persist_log(
                    f"Node {self.node_id} now listening for signals",
                    log_level="INFO",
                )

                try:
                    # Wait for all required signals
                    await self._signal_ready_future

                    await self.persist_log(
                        f"Node {self.node_id} received all required signals, starting execution",
                        log_level="INFO",
                    )
                    await self.node_signal_consumer.close()

                    # Execute node logic
                    success = await self.execute()
                    await self.persist_log(
                        f"Node {self.node_id} execution completed, success={success}",
                        log_level="INFO",
                        log_metadata={"success": success},
                    )
                    await self.set_status(
                        NodeStatus.COMPLETED if success else NodeStatus.FAILED
                    )
                    # Auto-forward input signals to output if needed
                    if success:
                        await self._auto_forward_input_handles()
                    else:
                        await self.persist_log(
                            f"Node {self.node_id} execution failed, forwarding input signals skipped",
                            log_level="ERROR",
                            log_metadata={"success": False},
                        )
                        await self.send_stop_execution_signal(
                            reason=f"Node:{self.node_id} execution failed"
                        )
                    return success
                except asyncio.CancelledError:
                    # 处理 Future 被取消的情况
                    await self.persist_log(
                        f"Node {self.node_id} execution was cancelled due to stop signal",
                        log_level="WARNING",
                        log_metadata={"reason": self._stop_execution_reason},
                    )
                    await self.set_status(
                        NodeStatus.TERMINATED, "Execution cancelled by stop signal"
                    )
                    raise NodeStopExecutionException(
                        f"Node execution cancelled: {self._stop_execution_reason}",
                        node_id=self.node_id,
                        reason=self._stop_execution_reason,
                        source_node=self._stop_execution_source,
                    )
                except NodeStopExecutionException:
                    # 重新抛出停止执行异常，让 executor 处理
                    raise

                finally:
                    # Reset Future
                    self._signal_ready_future = None
            else:
                # No need to wait for signals, execute directly
                await self.persist_log(
                    f"Node {self.node_id} requires no signals, executing directly",
                    log_level="INFO",
                )

                success = await self.execute()
                await self.persist_log(
                    f"Node {self.node_id} execution completed, success={success}",
                    log_level="INFO",
                    log_metadata={"success": success},
                )
                await self.set_status(
                    NodeStatus.COMPLETED if success else NodeStatus.FAILED
                )
                # Auto-forward input signals to output if needed
                if success:
                    await self._auto_forward_input_handles()
                else:
                    await self.persist_log(
                        f"Node {self.node_id} execution failed, forwarding input signals skipped",
                        log_level="ERROR",
                        log_metadata={"success": False},
                    )
                    await self.send_stop_execution_signal(
                        reason=f"Node:{self.node_id} execution failed"
                    )
                return success
        except NodeStopExecutionException:
            # 重新抛出停止执行异常，让 executor 处理
            raise

        except Exception as e:
            self.logger.error(
                "Error starting node %s: %s", self.node_id, str(e), exc_info=True
            )
            await self.set_status(NodeStatus.FAILED, str(e))
            return False

    async def cleanup(self):
        """Clean up resources, ensure resource release"""
        try:
            # Close message queue
            await self.node_signal_publisher.close()
            if (
                hasattr(self, "node_signal_consumer")
                and self.node_signal_consumer is not None
            ):
                await self.node_signal_consumer.close()

            # Close state store
            await self.close_state_store()

        except Exception as e:
            self.logger.error("Error during cleanup: %s", str(e))

    async def _auto_forward_input_handles(self) -> None:
        """
        自动转发输入信号到输出

        检查output_edges中的source_handle是否与当前节点的input_handle匹配，
        如果匹配则自动将接收到的输入数据转发给下游节点
        """
        self.logger.debug("Auto-forwarding input handles for node %s", self.node_id)

        input_handle_names = self.get_input_handle_names()

        self.logger.debug(
            "Registered input handles for auto-forwarding: %s",
            input_handle_names,
        )
        self.logger.debug(
            "Output edges for auto-forwarding: %s",
            [edge.to_dict() for edge in self._output_edges],
        )

        for output_edge in self._output_edges:
            source_handle = output_edge.source_node_handle

            # 检查source_handle是否是当前节点的输入handle
            if source_handle in input_handle_names:
                input_handle_data = self.get_input_handle_data(source_handle)
                await self.send_signal(
                    source_handle=source_handle,
                    signal_type=SignalType.ANY,  # FIXME: 现在路由中没有signal_type了，可以使用通用信号类型
                    payload=input_handle_data,
                )

                self.logger.info(
                    "Auto-forwarded signal from input handle '%s' to output edge: %s -> %s",
                    source_handle,
                    output_edge.target_node,
                    output_edge.target_node_handle,
                )

    def get_input_handle_data(self, target_handle: str) -> Any:
        """
        根据注册input handle获取数据

        Args:
            target_handle: 目标句柄名称

        Returns:
            Any: 句柄对应的数据，如果句柄未注册或没有auto_update_attr则返回None
        """
        # 检查句柄是否已注册
        if target_handle not in self._input_handles:
            self.logger.warning("Input handle '%s' not registered", target_handle)
            return None

        handle_obj = self._input_handles[target_handle]

        # 检查是否设置了auto_update_attr
        if not handle_obj.auto_update_attr:
            self.logger.warning(
                "Input handle '%s' has no auto_update_attr specified", target_handle
            )
            return None

        # 检查成员变量是否存在
        if not hasattr(self, handle_obj.auto_update_attr):
            self.logger.warning(
                "Attribute '%s' not found in node (handle: %s)",
                handle_obj.auto_update_attr,
                target_handle,
            )
            return None

        # 返回成员变量的值
        value = getattr(self, handle_obj.auto_update_attr)
        self.logger.debug(
            "Retrieved data for handle '%s' from attribute '%s': %s",
            target_handle,
            handle_obj.auto_update_attr,
            value,
        )
        return value

    def register_input_handle(
        self,
        name: str,
        data_type: type,
        description: str = "",
        example: Any = None,
        auto_update_attr: str = None,
        is_aggregate: bool = False,
    ) -> None:
        """
        Register an input handle

        Args:
            name: Handle name
            data_type: Expected data type
            description: Description of the handle
            example: Example value
            auto_update_attr: Name of the member variable to auto-update when signal is received
            is_aggregate: Whether this handle aggregates signals from multiple sources
        """
        self._input_handles[name] = InputHandle(
            name=name,
            data_type=data_type,
            description=description,
            example=example,
            auto_update_attr=auto_update_attr,
            is_aggregate=is_aggregate,
        )
        self.logger.debug(
            "Registered input handle: %s (%s), aggregate: %s, auto_update_attr: %s",
            name,
            data_type.__name__,
            is_aggregate,
            auto_update_attr,
        )

    def _register_input_handles(self) -> None:
        """
        Register input handles. Should be overridden by subclasses.
        This method is called during initialization.
        """
        # Default implementation does nothing
        # Subclasses should override this method to register their input handles

    def get_input_handles(self) -> Dict[str, InputHandle]:
        """
        Get all registered input handles

        Returns:
            Dict mapping handle names to InputHandle objects
        """
        return self._input_handles.copy()

    def get_input_handle_names(self) -> List[str]:
        """
        Get list of all registered input handle names

        Returns:
            List of handle names
        """
        return list(self._input_handles.keys())

    # ============ 元数据相关方法 ============

    def get_metadata(self) -> NodeMetadata:
        """
        Get node metadata (instance-level)

        Returns:
            NodeMetadata: Node metadata object
        """
        return self._instance_metadata

    @classmethod
    def get_class_metadata(cls) -> Optional[NodeMetadata]:
        """
        Get class-level metadata

        Returns:
            Optional[NodeMetadata]: Class metadata if set, None otherwise
        """
        return cls._metadata

    @classmethod
    def set_class_metadata(cls, metadata: NodeMetadata) -> None:
        """
        Set class-level metadata

        Args:
            metadata: NodeMetadata object to set
        """
        cls._metadata = metadata

    def update_metadata(self, **kwargs) -> None:
        """
        Update instance metadata

        Args:
            **kwargs: Metadata fields to update
        """
        for key, value in kwargs.items():
            if hasattr(self._instance_metadata, key):
                setattr(self._instance_metadata, key, value)
            else:
                self.logger.warning(f"Unknown metadata field: {key}")

    def get_version(self) -> str:
        """
        Get node version

        Returns:
            str: Node version
        """
        return self._instance_metadata.version

    def get_display_name(self) -> str:
        """
        Get node display name

        Returns:
            str: Node display name
        """
        return self._instance_metadata.display_name

    def get_node_category(self) -> str:
        """
        Get node category

        Returns:
            str: Node category (base/instance/variant)
        """
        return self._instance_metadata.node_category

    def is_base_node(self) -> bool:
        """
        Check if this is a base node

        Returns:
            bool: True if this is a base node
        """
        return self._instance_metadata.node_category == "base"

    def is_instance_node(self) -> bool:
        """
        Check if this is an instance node

        Returns:
            bool: True if this is an instance node
        """
        return self._instance_metadata.node_category == "instance"

    def get_base_node_type(self) -> Optional[str]:
        """
        Get base node type (for instance nodes)

        Returns:
            Optional[str]: Base node type if this is an instance node
        """
        return self._instance_metadata.base_node_type
