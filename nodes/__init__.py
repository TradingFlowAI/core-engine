"""节点包初始化，负责导入所有节点"""

# 导入所有内置节点，确保它们都被注册
from .ai_model_node_v0_0_1 import AIModelNode
from .price_node_v0_0_1 import PriceNode
from .code_node_v0_0_1 import CodeNode
from .gsheet_node_v0_0_1 import GSheetNode  # Google Sheet Input/Output 节点
# from .hyperion_dex_trade_node import HyperionDEXTradeNode
from .rsshub_node_v0_0_1 import RSSHubNode
from .telegram_sender_node_v0_0_1 import TelegramSenderNode
from .x_listener_node_v0_0_1 import XListenerNode
# from .uniswap_dex_trade_node import UniswapV3DEXTradeNode
from .vault_node_v0_0_1 import VaultNode
from .swap_node_v0_0_1 import SwapNode

# Inputs
from .rootdata_node_v0_0_1 import RootDataNode

# Interactive Nodes
from .interactive.display_node import DisplayNode
from .interactive.yes_no_node import YesNoNode
from .interactive.chart_node import ChartNode
from .interactive.chatbox_node import ChatBoxNode
from .interactive.candleline_node import CandlelineNode
