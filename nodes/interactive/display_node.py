"""
Display Node - Interactive Node

显示上游节点的输出数据。这是一个非阻塞的 Interactive Node，
用于在 Control Deck 中实时展示数据。

特点:
- 非阻塞：不会暂停 Flow 执行
- 直通：将输入数据原样传递给下游
- 格式化：支持多种显示格式（自动、数字、货币、百分比、JSON、文本）
"""

import json
from typing import Any, List

from common.edge import Edge
from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus


# Input handles
DISPLAY_DATA_HANDLE = "display_data"  # 要显示的数据
LABEL_HANDLE = "label"  # 显示标签
FORMAT_HANDLE = "format"  # 格式化类型

# Output handles - 单一 data 输出
DATA_HANDLE = "data"  # 直通输出


@register_node_type(
    "display_node",
    default_params={
        "label": "Display",
        "format": "auto",
    },
)
class DisplayNode(NodeBase):
    """
    Display Node - 在 Control Deck 中显示数据

    Input:
    - display_data: 要显示的数据（任意类型）
    - label: 显示标签
    - format: 格式化类型 (auto, number, currency, percentage, json, text)

    Output:
    - passthrough: 原样传递输入数据

    Credits Cost: 1 credit (最低消耗)
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        label: str = "Display",
        format: str = "auto",
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        # 从 kwargs 中移除 node_type 避免重复参数
        kwargs.pop("node_type", None)
        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            input_edges=input_edges,
            output_edges=output_edges,
            state_store=state_store,
            node_type="display_node",
            **kwargs,
        )

        self.label = label
        self.format = format
        self.display_data = None

    def _register_input_handles(self) -> None:
        """注册 input handles"""
        self.register_input_handle(
            name=DISPLAY_DATA_HANDLE,
            data_type=object,
            description="要显示的数据（任意类型）",
            example={"value": 42, "message": "Hello"},
            auto_update_attr="display_data",
        )
        self.register_input_handle(
            name=LABEL_HANDLE,
            data_type=str,
            description="显示标签",
            example="Display",
            auto_update_attr="label",
        )
        self.register_input_handle(
            name=FORMAT_HANDLE,
            data_type=str,
            description="格式化类型 (auto, number, currency, percentage, json, text)",
            example="auto",
            auto_update_attr="format",
        )

    async def execute(self) -> bool:
        """
        执行 Display Node 逻辑

        1. 等待接收上游数据
        2. 格式化并发布到 Control Deck
        3. 将数据直通传递给下游

        Returns:
            bool: 是否执行成功
        """
        try:
            await self.persist_log(
                f"Executing DisplayNode: label={self.label}, format={self.format}"
            )

            await self.set_status(NodeStatus.RUNNING)

            # 获取输入数据
            display_data = self.get_input_handle_data(DISPLAY_DATA_HANDLE)

            if display_data is None:
                await self.persist_log("No display data received, using empty value", "WARNING")
                display_data = {"value": None, "message": "No data received"}

            self.display_data = display_data

            # 格式化数据用于显示
            formatted_value = self._format_data(display_data)

            await self.persist_log(
                f"Display data received and formatted: {formatted_value[:100] if len(formatted_value) > 100 else formatted_value}"
            )

            # 发布显示数据到 Redis（用于 Control Deck 实时更新）
            await self._publish_display_update(display_data, formatted_value)

            # 直通传递给下游
            if await self.send_signal(
                DATA_HANDLE,
                SignalType.ANY,
                payload=display_data,
            ):
                await self.persist_log("Data passed through to downstream nodes")
            else:
                await self.persist_log("No downstream connections", "DEBUG")

            await self.set_status(NodeStatus.COMPLETED)
            return True

        except Exception as e:
            error_message = f"DisplayNode execution failed: {str(e)}"
            await self.persist_log(error_message, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

    def _format_data(self, data: Any) -> str:
        """格式化数据用于显示"""
        if data is None:
            return "null"

        format_type = self.format.lower()

        try:
            if format_type == "json":
                return json.dumps(data, indent=2, ensure_ascii=False, default=str)
            elif format_type == "number":
                if isinstance(data, (int, float)):
                    return f"{data:,.2f}"
                return str(data)
            elif format_type == "currency":
                if isinstance(data, (int, float)):
                    return f"${data:,.2f}"
                return str(data)
            elif format_type == "percentage":
                if isinstance(data, (int, float)):
                    return f"{data:.2f}%"
                return str(data)
            elif format_type == "text":
                return str(data)
            else:  # auto
                if isinstance(data, dict):
                    return json.dumps(data, indent=2, ensure_ascii=False, default=str)
                elif isinstance(data, (list, tuple)):
                    return json.dumps(data, ensure_ascii=False, default=str)
                elif isinstance(data, (int, float)):
                    return f"{data:,.4f}" if isinstance(data, float) else str(data)
                else:
                    return str(data)
        except Exception:
            return str(data)

    async def _publish_display_update(self, raw_data: Any, formatted_value: str):
        """发布显示更新到 Redis Pub/Sub"""
        try:
            if self.state_store and hasattr(self.state_store, "redis_client"):
                import json as json_lib
                from datetime import datetime, timezone

                update_event = {
                    "type": "display_update",
                    "flow_id": self.flow_id,
                    "cycle": self.cycle,
                    "node_id": self.node_id,
                    "label": self.label,
                    "format": self.format,
                    "raw_data": raw_data,
                    "formatted_value": formatted_value,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                channel = f"interaction:flow:{self.flow_id}"
                await self.state_store.redis_client.publish(
                    channel, json_lib.dumps(update_event, default=str)
                )

                await self.persist_log(
                    f"Published display update to channel {channel}", "DEBUG"
                )
        except Exception as e:
            await self.persist_log(
                f"Failed to publish display update: {e}", "WARNING"
            )

    def calculate_credits_cost(self) -> int:
        """Display Node 的 credits 消耗最低"""
        return 1
