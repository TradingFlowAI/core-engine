import asyncio
import json
import uuid
from enum import Enum
from typing import Any, Dict

from .signal_formats import SignalFormats


class SignalType(Enum):
    """信号类型枚举，可根据业务需要扩展"""

    DATA_READY = "data_ready"  # 数据准备完成信号
    DATA_PROCESSED = "data_processed"  # 数据处理完成信号
    EXECUTION_COMPLETE = "execution_complete"  # 执行完成信号
    MARKET_EVENT = "market_event"  # 市场事件信号
    SYSTEM_EVENT = "system_event"  # 系统事件信号
    ERROR = "error"
    PRICE_UPDATE = "price_update"  # 价格更新信号
    PRICE_CHANGE_ALERT = "price_change_alert"  # 价格变化警报信号
    AI_RESPONSE = "AI_RESPONSE"  # AI回复信号
    PROCESS_COMPLETE = "PROCESS_COMPLETE"  # 处理完成信号
    CONTROL = "CONTROL"  # 控制信号

    # 以下是有用的信号类型
    PRICE_DATA = "price_data"  # 价格数据信号 K线数据
    DEX_TRADE = "dex_trade"  # DEX交易信号
    DEX_TRADE_RECEIPT = "dex_trade_receipt"  # DEX交易回执信号
    DATASET = "dataset"  # 数据集信号，用于DatasetNode
    TEXT = "text"  # 文本信号，用于标准输出和错误输出
    VAULT_INFO = "vault_info"  # 金库信息信号
    CODE_OUTPUT = "code_output"  # 代码执行输出信号

    # 通用信号类型
    ANY = "any"  # 通用信号类型，表示可以接收任何类型的信号

    # 控制信号
    STOP_EXECUTION = "stop_execution"  # 停止执行信号


class Signal:
    """信号类，代表节点间传递的消息"""

    @staticmethod
    def validate_payload(
        signal_type: SignalType, payload: Dict[str, Any]
    ) -> tuple[bool, str]:
        """
        验证payload是否符合信号类型的格式要求

        Args:
            signal_type: 信号类型
            payload: 信号载荷

        Returns:
            tuple[bool, str]: (是否有效, 错误信息)
        """
        return SignalFormats.validate(signal_type.value, payload)

    def __init__(
        self,
        signal_type: SignalType,
        payload: Dict[str, Any] = None,
        timestamp: float = None,
        validate: bool = False,
    ):
        """
        初始化一个信号

        Args:
            signal_type: 信号类型
            source_node_id: 发出信号的节点ID
            payload: 信号携带的数据负载
            timestamp: 信号时间戳，如果不提供则自动生成
        """
        self.id = str(uuid.uuid4())
        self.type = signal_type
        self.payload = payload or {}
        self.timestamp = timestamp or asyncio.get_event_loop().time()

        # 如果启用验证，验证payload是否符合格式要求
        if validate:
            is_valid, error_msg = self.validate_payload(signal_type, self.payload)
            if not is_valid:
                raise ValueError(f"信号payload格式不符合要求: {error_msg}")

    def to_json(self) -> str:
        """将信号转换为JSON字符串"""
        return json.dumps(
            {
                "id": self.id,
                "type": (
                    self.type.value if isinstance(self.type, SignalType) else self.type
                ),
                # "source_node_id": self.source_node_id,
                # "target_node_id": self.target_node_id,
                "payload": self.payload,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "Signal":
        """从JSON字符串创建信号对象"""
        data = json.loads(json_str)
        return cls(
            signal_type=SignalType(data["type"]),
            # source_node_id=data["source_node_id"],
            # target_node_id=data["target_node_id"],
            payload=data["payload"],
            timestamp=data["timestamp"],
        )

    def __repr__(self):
        return (
            f"Signal(id={self.id}, type={self.type}, "
            f"payload={self.payload}, timestamp={self.timestamp})"
        )

    def __str__(self):
        return self.__repr__()


class NodeEdge:
    source_node: str
    source_node_handle: str
    target_node: str
    target_node_handle: str
