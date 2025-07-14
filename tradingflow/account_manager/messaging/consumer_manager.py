"""RabbitMQ消费者管理器"""

import logging
from typing import Callable

import aio_pika

logger = logging.getLogger(__name__)


class ConsumerManager:
    """RabbitMQ消费者管理器"""

    def __init__(self):
        self.connection = None
        self.channel = None
        self.consumers = {}
        self.is_running = False

    async def connect(self, rabbitmq_url: str):
        """连接到RabbitMQ服务器"""
        try:
            self.connection = await aio_pika.connect_robust(rabbitmq_url)
            self.channel = await self.connection.channel()
            logger.info("成功连接到RabbitMQ")
        except Exception as e:
            logger.error(f"RabbitMQ连接失败: {e}")
            raise

    async def register_consumer(
        self,
        queue_name: str,
        callback: Callable,
        exchange_name: str = None,
        routing_key: str = None,
        queue_options: dict = None,
        exchange_options: dict = None,
    ):
        """注册一个消费者"""
        if not self.channel:
            raise RuntimeError("尚未连接到RabbitMQ")

        # 设置默认选项
        queue_options = queue_options or {"durable": True}
        exchange_options = exchange_options or {"type": "direct", "durable": True}

        # 声明队列和交换机
        if exchange_name:
            exchange = await self.channel.declare_exchange(
                exchange_name, **exchange_options
            )
            queue = await self.channel.declare_queue(queue_name, **queue_options)
            await queue.bind(exchange, routing_key=routing_key or queue_name)
        else:
            queue = await self.channel.declare_queue(queue_name, **queue_options)

        # 保存消费者信息
        consumer_tag = f"consumer_{queue_name}"
        self.consumers[consumer_tag] = {
            "queue": queue,
            "callback": callback,
            "consumer_tag": consumer_tag,
        }

        logger.info(f"已注册RabbitMQ消费者: {consumer_tag}")
        return consumer_tag

    async def start_consuming(self):
        """启动所有注册的消费者"""
        if self.is_running:
            return

        for tag, consumer in self.consumers.items():
            await consumer["queue"].consume(consumer["callback"], consumer_tag=tag)

        self.is_running = True
        logger.info(f"已启动所有RabbitMQ消费者({len(self.consumers)}个)")

    async def stop_consuming(self):
        """停止所有消费者"""
        if not self.is_running:
            return

        if self.channel:
            for tag in self.consumers:
                await self.channel.cancel(tag)

        self.is_running = False
        logger.info("已停止所有RabbitMQ消费者")

    async def close(self):
        """关闭连接"""
        await self.stop_consuming()

        if self.connection:
            await self.connection.close()
            self.connection = None
            self.channel = None
            logger.info("已关闭RabbitMQ连接")
