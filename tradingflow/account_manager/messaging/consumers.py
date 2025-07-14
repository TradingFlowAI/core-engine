"""RabbitMQ消费者处理函数"""

import json
import logging

from aio_pika import IncomingMessage

logger = logging.getLogger(__name__)


async def handle_account_event(message: IncomingMessage):
    """处理账户相关事件"""
    async with message.process():
        try:
            body = message.body.decode()
            data = json.loads(body)
            logger.info(f"收到账户事件: {data}")
            # 处理业务逻辑
        except Exception as e:
            logger.error(f"处理账户事件失败: {e}")


async def handle_price_update(message: IncomingMessage):
    """处理价格更新事件"""
    async with message.process():
        try:
            body = message.body.decode()
            data = json.loads(body)
            logger.info(f"收到价格更新: {data}")
            # 处理业务逻辑
        except Exception as e:
            logger.error(f"处理价格更新失败: {e}")
