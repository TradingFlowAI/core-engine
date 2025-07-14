import asyncio
import logging
from typing import TYPE_CHECKING, List

from tradingflow.station.common.signal_types import Signal, SignalType
from tradingflow.station.nodes.node_base import NodeBase

if TYPE_CHECKING:
    from tradingflow.station.common.state_store import StateStore

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class UpstreamSimplePrintNode(NodeBase):

    def __init__(
        self,
        flow_id: str,
        cycle: int,
        node_id: str,
        name: str,
        downstream_nodes: List[str] = None,
        state_store: "StateStore" = None,
    ):
        super().__init__(
            flow_id=flow_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            required_signals=[],
            output_signal=SignalType.DATA_READY,
            downstream_nodes=downstream_nodes,
            state_store=state_store,
        )

    async def execute(self) -> bool:
        logger.info(f"节点 {self.node_id} 开始执行")
        await self.send_signal(
            SignalType.DATA_READY,
            payload={"message": "数据准备就绪"},
        )
        await asyncio.sleep(1)
        return True


class DownstreamSimplePrintNode(NodeBase):

    def __init__(
        self,
        flow_id: str,
        cycle: int,
        node_id: str,
        name: str,
        downstream_nodes: List[str] = None,
        state_store: "StateStore" = None,
    ):
        super().__init__(
            flow_id=flow_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            required_signals=[SignalType.DATA_READY],
            output_signal=SignalType.DATA_PROCESSED,
            downstream_nodes=downstream_nodes,
            state_store=state_store,
        )

    async def execute(self) -> bool:
        logger.info(f"节点 {self.node_id} 开始执行")
        for signal in self.signal_queue:
            logger.info(f"节点 {self.node_id} 收到信号: {signal.type}")

        await self.send_signal(
            SignalType.DATA_PROCESSED,
            payload={"message": "数据处理完成"},
        )
        await asyncio.sleep(1)
        return True


# 消息处理函数
# 使用示例
async def custom_signal_handler(signal: Signal) -> None:
    # 自定义信号处理逻辑
    print(f"Custom handling of signal: {signal.type}")
    # 执行特定业务逻辑...


FLOW_ID = "flow1"
CYCLE = 1

# upstream node
UPSTREAM_NODE_ID = "node1"

# downstream node
DOWNSTREAM_NODE_ID = "node2"


async def upstream_node_run() -> None:
    # 创建并连接发布者
    upstream_node = UpstreamSimplePrintNode(
        flow_id=FLOW_ID,
        cycle=CYCLE,
        node_id=UPSTREAM_NODE_ID,
        name="Upstream Node",
        downstream_nodes=[DOWNSTREAM_NODE_ID],
    )

    await upstream_node.start()


async def downstream_node_run() -> None:
    downstream_node = DownstreamSimplePrintNode(
        flow_id=FLOW_ID,
        cycle=CYCLE,
        node_id=DOWNSTREAM_NODE_ID,
        name="Downstream Node",
        downstream_nodes=["node4"],
    )
    await downstream_node.start()


async def run_example():
    await asyncio.gather(
        upstream_node_run(),
        downstream_node_run(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(run_example())
    except KeyboardInterrupt:
        logger.info("程序已被用户中断")
