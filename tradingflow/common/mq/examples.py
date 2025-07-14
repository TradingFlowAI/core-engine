import asyncio
import sys

from aio_pika.abc import AbstractIncomingMessage

from tradingflow.common.mq.aio_pika_impl import AioPikaConsumer, AioPikaPublisher


# 消息处理函数
async def process_message(message: AbstractIncomingMessage) -> None:
    async with message.process():
        print(f" [x] 收到消息 {message!r}")
        # 模拟处理延迟，每个点代表1秒
        await asyncio.sleep(message.body.count(b"."))
        print(f"     消息内容：{message.body!r}")


# 发布者示例
async def publisher_example() -> None:
    publisher = AioPikaPublisher()
    await publisher.connect("amqp://guest:guest@localhost/")

    try:
        message = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Hello World!"
        await publisher.publish(message, "task_queue")
        print(f" [x] 已发送消息: {message}")
    finally:
        await publisher.close()


# 消费者示例
async def consumer_example() -> None:
    consumer = AioPikaConsumer()
    await consumer.connect("amqp://guest:guest@localhost/")

    await consumer.consume("task_queue", process_message)

    print(" [*] 等待消息. 按 CTRL+C 退出")
    # 保持运行直到被中断
    await asyncio.Future()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "publish":
        asyncio.run(publisher_example())
    else:
        asyncio.run(consumer_example())
