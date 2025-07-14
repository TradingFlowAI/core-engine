import asyncio
import logging
import sys
import time

from tradingflow.depot.mq.node_signal_consumer import NodeSignalConsumer
from tradingflow.depot.mq.node_signal_publisher import NodeSignalPublisher
from tradingflow.station.common.signal_types import Signal, SignalType

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# 消息处理函数
# 使用示例
async def custom_signal_handler(signal: Signal) -> None:
    # 自定义信号处理逻辑
    print(f"Custom handling of signal: {signal.type}")
    # 执行特定业务逻辑...


class DataProcessor:
    def __init__(self, name: str):
        self.name = name
        self.processed_signals = 0
        self.is_running = True
        # 模拟在node_base中使用方式
        self.consumer = NodeSignalConsumer(
            flow_id="flow1",
            cycle=1,
            node_id="node3",
            signals=[SignalType.DATA_READY, SignalType.DATA_PROCESSED],
            on_signal_handler=self.process_signal,
        )

    async def process_signal(self, signal: Signal) -> None:
        """处理普通信号的方法"""
        self.processed_signals += 1
        print(f"[{self.name}] 处理信号: {signal.type}, 数据: {signal.payload}")
        # 在这里执行特定的业务逻辑
        await asyncio.sleep(0.5)  # 模拟处理时间
        print(f"[{self.name}] 信号处理完成，已处理总数: {self.processed_signals}")


# 发布者示例: 模拟发送不同市场、不同品种的行情数据
async def publisher_example() -> None:
    """模拟行情数据发布者"""
    # 创建并连接发布者
    publisher = NodeSignalPublisher(
        flow_id="flow1",
        cycle=1,
        node_id="node1",
        downstream_nodes=["node2", "node3", "node4"],
    )

    try:
        await publisher.connect()
        logger.info("发布者已连接")
        # 发布一个数据准备信号
        data_ready_signal = Signal(
            signal_type=SignalType.DATA_READY,
            payload={"message": "数据准备就绪"},
            timestamp=time.time(),
        )
        await publisher.send_signal(
            data_ready_signal,
        )

        # 发布一个停止执行信号
        stop_execution_signal = Signal(
            signal_type=SignalType.STOP_EXECUTION,
            payload={"message": "停止执行"},
            timestamp=time.time(),
        )
        stop_execution_routing_key = f"signal.{SignalType.STOP_EXECUTION}.cycle.1"
        await publisher.publisher.publish(
            stop_execution_signal.to_json(),
            routing_key=stop_execution_routing_key,
        )
        logger.info(
            f"已发送停止执行信号: [key={stop_execution_routing_key}] {stop_execution_signal.to_json()}"
        )
    finally:
        await publisher.close()
        logger.info("发布者已关闭连接")


# 消费者示例: 使用不同的绑定模式监听不同的消息
async def consumer_example() -> None:
    """消费者示例，每个消费者关注不同的消息模式"""
    consumers = []

    processor = DataProcessor(name="节点处理器1")

    # signal_consumer = NodeSignalConsumer(
    #     flow_id="flow1",
    #     cycle=1,
    #     node_id="node3",
    #     signals=[SignalType.DATA_READY, SignalType.DATA_PROCESSED],
    #     # on_signal_handler=custom_signal_handler,
    #     # on_stop_execution_handler=custom_stop_handler,
    #     on_signal_handler=processor.process_signal,
    #     on_stop_execution_handler=processor.handle_stop,
    # )
    signal_consumer = processor.consumer
    await signal_consumer.connect()
    await signal_consumer.consume()
    consumers.append(("flow1 cycle1 node3 consumer", signal_consumer))

    # 输出所有已启动的消费者
    for name, _ in consumers:
        logger.info(f"已启动消费者: {name}")

    logger.info("等待消息中... 按 CTRL+C 退出")

    # 保持所有消费者运行
    try:
        await asyncio.Future()
    finally:
        # 关闭所有消费者
        for name, consumer in consumers:
            await consumer.close()
            logger.info(f"已关闭消费者: {name}")


async def run_example():
    """运行示例程序"""
    # 检查是否需要发布消息
    if len(sys.argv) > 1 and sys.argv[1] == "publish":
        await publisher_example()
    else:
        # 启动消费者
        await consumer_example()


if __name__ == "__main__":
    try:
        asyncio.run(run_example())
    except KeyboardInterrupt:
        logger.info("程序已被用户中断")
