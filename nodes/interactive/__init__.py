"""
Interactive Nodes Package

这些节点允许用户在 Flow 执行过程中进行交互。
它们会在前端的 Control Deck 面板中渲染实际的交互界面。

节点类型:
- display_node: 显示上游数据（只读）
- yes_no_node: 用户确认/拒绝决策（阻塞）
- chart_node: 数据可视化图表
- chatbox_node: 对话交互（Constant Node）
- candleline_node: K线图与阈值设置
"""

from .display_node import DisplayNode
from .yes_no_node import YesNoNode
from .chart_node import ChartNode
from .chatbox_node import ChatBoxNode
from .candleline_node import CandlelineNode

__all__ = [
    "DisplayNode",
    "YesNoNode",
    "ChartNode",
    "ChatBoxNode",
    "CandlelineNode",
]
