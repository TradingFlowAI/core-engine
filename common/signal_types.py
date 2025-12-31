import json
import time
import uuid
from enum import Enum
from typing import Any, Dict, Optional

from .signal_formats import SignalFormats


class SignalType(Enum):
    """Signal type enum, can be extended for business needs"""

    DATA_READY = "data_ready"  # Data ready signal
    DATA_PROCESSED = "data_processed"  # Data processed signal
    EXECUTION_COMPLETE = "execution_complete"  # Execution complete signal
    MARKET_EVENT = "market_event"  # Market event signal
    SYSTEM_EVENT = "system_event"  # System event signal
    ERROR = "error"
    PRICE_UPDATE = "price_update"  # Price update signal
    PRICE_CHANGE_ALERT = "price_change_alert"  # Price change alert signal
    AI_RESPONSE = "AI_RESPONSE"  # AI response signal
    PROCESS_COMPLETE = "PROCESS_COMPLETE"  # Process complete signal
    CONTROL = "CONTROL"  # Control signal

    # Useful signal types
    PRICE_DATA = "price_data"  # Price data signal (K-line data)
    DEX_TRADE = "dex_trade"  # DEX trade signal
    DEX_TRADE_RECEIPT = "dex_trade_receipt"  # DEX trade receipt signal
    DATASET = "dataset"  # Dataset signal, for DatasetNode
    TEXT = "text"  # Text signal, for stdout/stderr
    VAULT_INFO = "vault_info"  # Vault info signal
    CODE_OUTPUT = "code_output"  # Code execution output signal
    JSON_DATA = "json_data"  # Generic JSON structure signal (for AI output)

    # Generic signal type
    ANY = "any"  # Generic signal type, can receive any signal

    # Control signals
    STOP_EXECUTION = "stop_execution"  # Stop execution signal


class Signal:
    """Signal class, represents messages passed between nodes"""

    @staticmethod
    def validate_payload(
        signal_type: SignalType, payload: Dict[str, Any]
    ) -> tuple[bool, str]:
        """
        Validate if payload matches signal type format requirements.

        Args:
            signal_type: Signal type
            payload: Signal payload

        Returns:
            tuple[bool, str]: (is_valid, error_message)
        """
        return SignalFormats.validate(signal_type.value, payload)

    def __init__(
        self,
        signal_type: SignalType,
        payload: Dict[str, Any] = None,
        timestamp: float = None,
        validate: bool = False,
        source_node: Optional[str] = None,
        source_handle: Optional[str] = None,
    ):
        """
        Initialize a signal.

        Args:
            signal_type: Signal type
            payload: Data payload carried by signal
            timestamp: Signal timestamp, auto-generated if not provided
            validate: Whether to validate payload format
            source_node: Source node ID that generated this signal
            source_handle: Source handle name from which signal was sent
        """
        self.id = str(uuid.uuid4())
        self.type = signal_type
        self.payload = payload or {}
        self.timestamp = timestamp or time.time()
        
        # Source information - can be set during signal routing
        self.source_node: Optional[str] = source_node
        self.source_handle: Optional[str] = source_handle
        
        # Alias for backward compatibility
        self.source_node_id: Optional[str] = source_node

        # If validation enabled, validate payload format
        if validate:
            is_valid, error_msg = self.validate_payload(signal_type, self.payload)
            if not is_valid:
                raise ValueError(f"Signal payload format does not match requirements: {error_msg}")

    def to_json(self) -> str:
        """Convert signal to JSON string."""
        return json.dumps(
            {
                "id": self.id,
                "type": (
                    self.type.value if isinstance(self.type, SignalType) else self.type
                ),
                "payload": self.payload,
                "timestamp": self.timestamp,
                "source_node": self.source_node,
                "source_handle": self.source_handle,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "Signal":
        """Create signal object from JSON string."""
        data = json.loads(json_str)
        return cls(
            signal_type=SignalType(data["type"]),
            payload=data["payload"],
            timestamp=data["timestamp"],
            source_node=data.get("source_node"),
            source_handle=data.get("source_handle"),
        )
    
    def set_source(self, source_node: str, source_handle: str) -> None:
        """
        Set source information for the signal.
        
        This is typically called when routing signals between nodes.
        
        Args:
            source_node: Source node ID
            source_handle: Source handle name
        """
        self.source_node = source_node
        self.source_handle = source_handle
        self.source_node_id = source_node  # Keep alias in sync

    def __repr__(self):
        return (
            f"Signal(id={self.id}, type={self.type}, "
            f"source={self.source_node}:{self.source_handle}, "
            f"payload={self.payload}, timestamp={self.timestamp})"
        )

    def __str__(self):
        return self.__repr__()


class NodeEdge:
    source_node: str
    source_node_handle: str
    target_node: str
    target_node_handle: str
