import asyncio
import logging
import random
import sys
from datetime import datetime

from aio_pika.abc import AbstractIncomingMessage

from tradingflow.depot.mq.aio_pika_impl import (
    AioPikaTopicConsumer,
    AioPikaTopicPublisher,
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# 消息处理函数
async def process_cn_message(message: AbstractIncomingMessage) -> None:
    """处理中国市场的消息"""
    async with message.process():
        body = message.body.decode()
        routing_key = message.routing_key
        # logger.info(f"处理中国市场消息: [key={routing_key}] {body}")
        # 模拟处理时间
        await asyncio.sleep(0.5)
        logger.info(f"中国市场消息处理完成: [key={routing_key}] {body}")


async def process_stock_message(message: AbstractIncomingMessage) -> None:
    """处理股票市场的消息"""
    async with message.process():
        body = message.body.decode()
        routing_key = message.routing_key
        # logger.info(f"处理股票市场消息: [key={routing_key}] {body}")
        # 模拟处理时间
        await asyncio.sleep(0.5)
        logger.info(f"股票市场消息处理完成: [key={routing_key}] {body}")


async def process_future_message(message: AbstractIncomingMessage) -> None:
    """处理期货市场的消息"""
    async with message.process():
        body = message.body.decode()
        routing_key = message.routing_key
        # logger.info(f"处理期货市场消息: [key={routing_key}] {body}")
        # 模拟处理时间
        await asyncio.sleep(0.5)
        logger.info(f"期货市场消息处理完成: [key={routing_key}] {body}")


# 发布者示例: 模拟发送不同市场、不同品种的行情数据
async def publisher_example() -> None:
    """模拟行情数据发布者"""
    # 创建并连接发布者
    publisher = AioPikaTopicPublisher(
        exchange="market_data",  # 交换机名称  # 交换机类型
    )
    await publisher.connect()
    logger.info("发布者已连接")

    try:
        # 模拟的市场和品种
        markets = ["cn", "us", "hk"]
        instruments = ["stock", "future", "option"]
        actions = ["price", "vol", "depth"]

        # 发布20条消息
        for i in range(20):
            # 随机选择市场、品种和行为
            market = random.choice(markets)
            instrument = random.choice(instruments)
            action = random.choice(actions)

            # 构建路由键: market.instrument.action
            routing_key = f"{market}.{instrument}.{action}"

            # 构建消息内容
            timestamp = datetime.now().isoformat()
            price = round(random.uniform(50, 200), 2)
            message = f"#{i} - {timestamp} - 价格: {price}"

            # 发布消息
            await publisher.publish(message, routing_key=routing_key)
            logger.info(f"已发送: [key={routing_key}] {message}")

            # 等待一小段时间
            await asyncio.sleep(0.2)
    finally:
        await publisher.close()
        logger.info("发布者已关闭连接")


# 消费者示例: 使用不同的绑定模式监听不同的消息
async def consumer_example() -> None:
    """多个消费者示例，每个消费者关注不同的消息模式"""
    # 创建 3 个不同的消费者
    consumers = []

    # 消费者1: 只接收中国市场的所有数据
    cn_consumer = AioPikaTopicConsumer(exchange="market_data")
    await cn_consumer.connect()
    await cn_consumer.consume(
        queue_name="cn_market_queue",
        binding_keys=["cn.#"],  # 所有cn开头的消息
        callback=process_cn_message,
    )
    consumers.append(("中国市场监控", cn_consumer))

    # 消费者2: 接收所有市场的股票价格数据
    stock_price_consumer = AioPikaTopicConsumer(
        exchange="market_data",
    )
    await stock_price_consumer.connect()
    await stock_price_consumer.consume(
        queue_name="stock_price_queue",
        binding_keys=["*.stock.price"],  # 任何市场的股票价格
        callback=process_stock_message,
    )
    consumers.append(("股票价格监控", stock_price_consumer))

    # 消费者3: 接收所有期货数据
    future_consumer = AioPikaTopicConsumer(
        exchange="market_data",
    )
    await future_consumer.connect()
    await future_consumer.consume(
        queue_name="future_data_queue",
        binding_keys=["*.future.*"],  # 任何市场的期货数据
        callback=process_future_message,
    )
    consumers.append(("期货数据监控", future_consumer))

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
