"""
Chart Node - Interactive Node

在 Control Deck 中显示数据可视化图表。
这是一个非阻塞的 Interactive Node，支持多种图表类型。

特点:
- 非阻塞：不会暂停 Flow 执行
- 多图表类型：bar, line, area, pie
- 实时更新：每次执行时更新图表数据
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from common.edge import Edge
from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus


# Input handles
CHART_DATA_HANDLE = "chart_data"  # 图表数据
CHART_TYPE_HANDLE = "chart_type"  # 图表类型
TITLE_HANDLE = "title"  # 图表标题
X_AXIS_KEY_HANDLE = "x_axis_key"  # X 轴数据键
Y_AXIS_KEY_HANDLE = "y_axis_key"  # Y 轴数据键
COLOR_HANDLE = "color"  # 图表颜色

# Output handles - 单一 data 输出
DATA_HANDLE = "data"  # 直通输出


@register_node_type(
    "chart_node",
    default_params={
        "chart_type": "bar",
        "title": "Chart",
        "x_axis_key": "name",
        "y_axis_key": "value",
        "color": "#8884d8",
    },
)
class ChartNode(NodeBase):
    """
    Chart Node - 数据可视化图表

    Input:
    - chart_data: 图表数据（数组格式）
    - chart_type: 图表类型 (bar, line, area, pie)
    - title: 图表标题
    - x_axis_key: X 轴数据键
    - y_axis_key: Y 轴数据键
    - color: 图表颜色

    Output:
    - passthrough: 原样传递输入数据

    Credits Cost: 1 credit
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        chart_type: str = "bar",
        title: str = "Chart",
        x_axis_key: str = "name",
        y_axis_key: str = "value",
        color: str = "#8884d8",
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
            node_type="chart_node",
            **kwargs,
        )

        self.chart_type = chart_type
        self.title = title
        self.x_axis_key = x_axis_key
        self.y_axis_key = y_axis_key
        self.color = color
        self.chart_data = None

    def _register_input_handles(self) -> None:
        """注册 input handles"""
        self.register_input_handle(
            name=CHART_DATA_HANDLE,
            data_type=list,
            description="图表数据（数组格式）",
            example=[{"name": "A", "value": 100}],
            auto_update_attr="chart_data",
        )
        self.register_input_handle(
            name=CHART_TYPE_HANDLE,
            data_type=str,
            description="图表类型 (bar, line, area, pie)",
            example="bar",
            auto_update_attr="chart_type",
        )
        self.register_input_handle(
            name=TITLE_HANDLE,
            data_type=str,
            description="图表标题",
            example="Chart",
            auto_update_attr="title",
        )
        self.register_input_handle(
            name=X_AXIS_KEY_HANDLE,
            data_type=str,
            description="X 轴数据键",
            example="name",
            auto_update_attr="x_axis_key",
        )
        self.register_input_handle(
            name=Y_AXIS_KEY_HANDLE,
            data_type=str,
            description="Y 轴数据键",
            example="value",
            auto_update_attr="y_axis_key",
        )
        self.register_input_handle(
            name=COLOR_HANDLE,
            data_type=str,
            description="图表颜色",
            example="#8884d8",
            auto_update_attr="color",
        )

    async def execute(self) -> bool:
        """
        执行 Chart Node 逻辑

        1. 等待接收上游数据
        2. 处理并格式化图表数据
        3. 发布到 Control Deck
        4. 将数据直通传递给下游

        Returns:
            bool: 是否执行成功
        """
        try:
            await self.persist_log(
                f"Executing ChartNode: type={self.chart_type}, title={self.title}"
            )

            await self.set_status(NodeStatus.RUNNING)

            # 获取输入数据
            chart_data = self.get_input_handle_data(CHART_DATA_HANDLE)

            if chart_data is None:
                await self.persist_log("No chart data received, using empty array", "WARNING")
                chart_data = []

            # 验证和处理数据
            processed_data = self._process_chart_data(chart_data)
            self.chart_data = processed_data

            await self.persist_log(
                f"Chart data processed: {len(processed_data)} data points"
            )

            # 尝试从输入获取动态配置
            input_chart_type = self.get_input_handle_data(CHART_TYPE_HANDLE)
            if input_chart_type and input_chart_type in ["bar", "line", "area", "pie"]:
                self.chart_type = input_chart_type

            input_title = self.get_input_handle_data(TITLE_HANDLE)
            if input_title:
                self.title = str(input_title)

            input_x_key = self.get_input_handle_data(X_AXIS_KEY_HANDLE)
            if input_x_key:
                self.x_axis_key = str(input_x_key)

            input_y_key = self.get_input_handle_data(Y_AXIS_KEY_HANDLE)
            if input_y_key:
                self.y_axis_key = str(input_y_key)

            input_color = self.get_input_handle_data(COLOR_HANDLE)
            if input_color:
                self.color = str(input_color)

            # 发布图表更新到 Redis
            await self._publish_chart_update(processed_data)

            # 直通传递给下游
            if await self.send_signal(
                DATA_HANDLE,
                SignalType.ANY,
                payload=chart_data,
            ):
                await self.persist_log("Data passed through to downstream nodes")
            else:
                await self.persist_log("No downstream connections", "DEBUG")

            await self.set_status(NodeStatus.COMPLETED)
            return True

        except Exception as e:
            error_message = f"ChartNode execution failed: {str(e)}"
            await self.persist_log(error_message, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

    def _process_chart_data(self, data: Any) -> List[Dict]:
        """处理和验证图表数据"""
        if data is None:
            return []

        # 如果是单个数值，转换为数组
        if isinstance(data, (int, float)):
            return [{"name": "Value", "value": data}]

        # 如果是字典，转换为数组
        if isinstance(data, dict):
            # 检查是否已经是正确格式
            if self.x_axis_key in data and self.y_axis_key in data:
                return [data]
            # 将字典转换为数组
            return [
                {"name": k, "value": v}
                for k, v in data.items()
                if isinstance(v, (int, float))
            ]

        # 如果是列表
        if isinstance(data, list):
            processed = []
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    processed.append(item)
                elif isinstance(item, (int, float)):
                    processed.append({"name": f"Item {i + 1}", "value": item})
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    processed.append({"name": str(item[0]), "value": item[1]})
            return processed

        # 其他情况，尝试转换
        try:
            return [{"name": "Value", "value": float(data)}]
        except (ValueError, TypeError):
            return [{"name": "Data", "value": str(data)}]

    async def _publish_chart_update(self, processed_data: List[Dict]):
        """发布图表更新到 Redis Pub/Sub"""
        try:
            if self.state_store and hasattr(self.state_store, "redis_client"):
                update_event = {
                    "type": "chart_update",
                    "flow_id": self.flow_id,
                    "cycle": self.cycle,
                    "node_id": self.node_id,
                    "chart_type": self.chart_type,
                    "title": self.title,
                    "x_axis_key": self.x_axis_key,
                    "y_axis_key": self.y_axis_key,
                    "color": self.color,
                    "data": processed_data,
                    "data_count": len(processed_data),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                channel = f"interaction:flow:{self.flow_id}"
                await self.state_store.redis_client.publish(
                    channel, json.dumps(update_event, default=str)
                )

                await self.persist_log(
                    f"Published chart update to channel {channel}", "DEBUG"
                )
        except Exception as e:
            await self.persist_log(
                f"Failed to publish chart update: {e}", "WARNING"
            )

    def calculate_credits_cost(self) -> int:
        """Chart Node 的 credits 消耗"""
        return 1
