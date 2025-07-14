#!/usr/bin/env python
import asyncio

import pika


async def delete_all_rabbitmq_objects(
    host: str = "localhost",
    port: int = 5672,
    username: str = "guest",
    password: str = "guest",
    virtual_host: str = "/",
):
    """删除RabbitMQ中所有非默认的交换机和队列

    Args:
        host: RabbitMQ主机地址
        port: 端口号
        username: 用户名
        password: 密码
        virtual_host: 虚拟主机
    """
    print(f"连接到RabbitMQ服务器 {host}:{port}...")

    # 创建连接参数
    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(
        host=host, port=port, virtual_host=virtual_host, credentials=credentials
    )

    # 创建连接和通道
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    try:
        # 1. 获取所有队列
        print("获取所有队列...")
        queues_result = channel.queue_declare(queue="", exclusive=True)
        queue_name = queues_result.method.queue

        # 使用rabbitmq的列队管理接口获取所有队列信息
        channel.queue_bind(
            exchange="amq.rabbitmq.event", queue=queue_name, routing_key="queue.*"
        )

        # 获取队列列表
        all_queues = []

        def queue_callback(ch, method, properties, body):
            queue_info = body.decode("utf-8")
            all_queues.append(queue_info)

        channel.basic_consume(
            queue=queue_name, on_message_callback=queue_callback, auto_ack=True
        )

        # 轮询几秒钟来收集队列信息
        print("等待队列信息...")
        for _ in range(3):
            connection.process_data_events(time_limit=1)

        # 2. 获取所有交换机
        print("获取所有交换机...")
        # 通过HTTP API获取更可靠，但这里我们使用命令行获取

        # 默认不应删除的交换机
        default_exchanges = [
            "",  # 默认交换机
            "amq.direct",
            "amq.fanout",
            "amq.headers",
            "amq.match",
            "amq.rabbitmq.event",
            "amq.rabbitmq.trace",
            "amq.topic",
        ]

        # 使用channel.exchange_declare来测试交换机是否存在
        # 这不是获取所有交换机的理想方法，但可以工作
        # 注意：更好的方法是使用RabbitMQ的HTTP API

        # 通过命名模式猜测可能存在的交换机
        possible_exchanges = [f"test_exchange_{i}" for i in range(10)]
        possible_exchanges.extend([f"test_flow_exchange_{i}" for i in range(10)])
        possible_exchanges.extend(
            ["trading_flow", "test_exchange_shared", "fanout_exchange"]
        )

        all_exchanges = []
        for exchange in possible_exchanges:
            try:
                # 尝试被动声明交换机，如果存在则不会报错
                channel.exchange_declare(exchange=exchange, passive=True)
                all_exchanges.append(exchange)
            except pika.exceptions.ChannelClosedByBroker:
                # 如果交换机不存在，会抛出异常，我们需要重新打开通道
                channel = connection.channel()

        # 3. 删除所有非默认队列
        print("删除所有队列...")
        queue_count = 0
        for queue_info in all_queues:
            if "name" in queue_info:
                import json

                queue_data = json.loads(queue_info)
                queue_name = queue_data.get("name")
                if queue_name and not queue_name.startswith("amq."):
                    try:
                        channel.queue_delete(queue=queue_name)
                        print(f"已删除队列: {queue_name}")
                        queue_count += 1
                    except Exception as e:
                        print(f"删除队列 {queue_name} 失败: {str(e)}")

        # 4. 删除所有非默认交换机
        print("删除所有非默认交换机...")
        exchange_count = 0
        for exchange in all_exchanges:
            if exchange not in default_exchanges:
                try:
                    channel.exchange_delete(exchange=exchange)
                    print(f"已删除交换机: {exchange}")
                    exchange_count += 1
                except Exception as e:
                    print(f"删除交换机 {exchange} 失败: {str(e)}")

        print(f"清理完成! 删除了 {queue_count} 个队列和 {exchange_count} 个交换机")

    finally:
        # 关闭连接
        connection.close()
        print("RabbitMQ连接已关闭")


# 更有效的方式是使用RabbitMQ HTTP API
async def delete_all_with_http_api(
    host: str = "localhost",
    port: int = 15672,  # HTTP API端口
    username: str = "guest",
    password: str = "guest",
    virtual_host: str = "/",
):
    """使用HTTP API删除所有非默认的交换机和队列"""

    # 对虚拟主机进行URL编码
    import urllib.parse

    import aiohttp

    encoded_vhost = urllib.parse.quote(virtual_host, safe="")

    # 基本认证
    auth = aiohttp.BasicAuth(username, password)
    base_url = f"http://{host}:{port}/api"

    async with aiohttp.ClientSession(auth=auth) as session:
        # 1. 获取所有队列
        print("获取所有队列...")
        async with session.get(f"{base_url}/queues/{encoded_vhost}") as response:
            if response.status == 200:
                queues = await response.json()
                print(f"找到 {len(queues)} 个队列")
            else:
                print(f"获取队列失败: {response.status}")
                return

        # 2. 获取所有交换机
        print("获取所有交换机...")
        async with session.get(f"{base_url}/exchanges/{encoded_vhost}") as response:
            if response.status == 200:
                exchanges = await response.json()
                print(f"找到 {len(exchanges)} 个交换机")
            else:
                print(f"获取交换机失败: {response.status}")
                return

        # 默认不应删除的交换机
        default_exchanges = [
            "",  # 默认交换机
            "amq.direct",
            "amq.fanout",
            "amq.headers",
            "amq.match",
            "amq.rabbitmq.event",
            "amq.rabbitmq.trace",
            "amq.topic",
        ]

        # 3. 删除所有非默认队列
        print("删除所有队列...")
        queue_count = 0
        for queue in queues:
            queue_name = queue.get("name")
            if queue_name and not queue_name.startswith("amq."):
                try:
                    async with session.delete(
                        f"{base_url}/queues/{encoded_vhost}/{queue_name}"
                    ) as response:
                        if response.status == 204:
                            print(f"已删除队列: {queue_name}")
                            queue_count += 1
                        else:
                            print(f"删除队列 {queue_name} 失败: {response.status}")
                except Exception as e:
                    print(f"删除队列 {queue_name} 失败: {str(e)}")

        # 4. 删除所有非默认交换机
        print("删除所有非默认交换机...")
        exchange_count = 0
        for exchange in exchanges:
            exchange_name = exchange.get("name")
            if exchange_name and exchange_name not in default_exchanges:
                try:
                    async with session.delete(
                        f"{base_url}/exchanges/{encoded_vhost}/{exchange_name}"
                    ) as response:
                        if response.status == 204:
                            print(f"已删除交换机: {exchange_name}")
                            exchange_count += 1
                        else:
                            print(f"删除交换机 {exchange_name} 失败: {response.status}")
                except Exception as e:
                    print(f"删除交换机 {exchange_name} 失败: {str(e)}")

        print(f"清理完成! 删除了 {queue_count} 个队列和 {exchange_count} 个交换机")


if __name__ == "__main__":
    # 设置RabbitMQ连接参数
    rabbitmq_config = {
        "host": "localhost",  # 或者您的RabbitMQ主机
        "port": 5672,  # 默认AMQP端口
        "username": "guest",  # 默认用户名
        "password": "guest",  # 默认密码
        "virtual_host": "/",  # 默认虚拟主机
    }

    # 选择使用哪种方法，HTTP API更推荐
    http_api_config = {
        "host": "localhost",  # 或者您的RabbitMQ主机
        "port": 15672,  # HTTP API端口
        "username": "guest",  # 默认用户名
        "password": "guest",  # 默认密码
        "virtual_host": "/",  # 默认虚拟主机
    }

    # 运行清理脚本
    print("开始清理RabbitMQ中的所有非默认交换机和队列...")

    # 使用HTTP API方法（推荐）
    asyncio.run(delete_all_with_http_api(**http_api_config))

    # 或者使用AMQP方法（有限制）
    # asyncio.run(delete_all_rabbitmq_objects(**rabbitmq_config))
