import asyncio

import httpx


async def execute_nodes():
    url = "http://localhost:7002/nodes/execute"
    # headers = {"Content-Type": "application/json"}

    # 创建价格生成器节点
    price_generator_data = {
        "node_id": "price_gen_1",
        "node_type": "python",
        "config": {
            "node_class_type": "price_generator",
            "name": "BTC价格生成器",
            "symbol": "BTC/USDT",
            "base_price": 50000,
            "volatility": 0.01,
            "interval": 1.0,
            "max_updates": 10,
            "message_queue_type": "memory",
        },
    }

    # 创建价格变化检测器节点
    detector_data = {
        "node_id": "detector_1",
        "node_type": "python",
        "config": {
            "node_class_type": "price_change_detector",
            "name": "BTC价格变化检测器",
            "threshold_percent": 0.5,
            "message_queue_type": "memory",
        },
    }

    async with httpx.AsyncClient() as client:
        # 先启动价格变化检测器
        detector_response = await client.post(url, json=detector_data)
        print(f"Detector response: {detector_response.status_code}")
        print(detector_response.json())

        # 然后启动价格生成器
        generator_response = await client.post(url, json=price_generator_data)
        print(f"Generator response: {generator_response.status_code}")
        print(generator_response.json())

        # 可以添加轮询节点状态的逻辑
        while True:
            # 每秒检查一次节点状态
            await asyncio.sleep(1)

            gen_status = await client.get(
                "http://localhost:7002/nodes/price_gen_1/status"
            )
            det_status = await client.get(
                "http://localhost:7002/nodes/detector_1/status"
            )

            print(f"Generator status: {gen_status.json().get('status')}")
            print(f"Detector status: {det_status.json().get('status')}")

            # 如果两个节点都不再运行，则退出
            if gen_status.json().get("status") not in [
                "running",
                "initializing",
            ] and det_status.json().get("status") not in ["running", "initializing"]:
                break


# 运行异步函数
asyncio.run(execute_nodes())
