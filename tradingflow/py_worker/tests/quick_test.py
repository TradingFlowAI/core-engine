import asyncio
import json

import httpx

WORKER_URL = "http://localhost:7000"


async def quick_test():
    """快速测试 worker 服务的核心功能"""
    async with httpx.AsyncClient() as client:
        # 1. 测试健康检查
        print("🧪 测试健康检查...")
        resp = await client.get(f"{WORKER_URL}/health")
        print(f"✅ 健康检查: {resp.status_code}")
        print(json.dumps(resp.json(), indent=2, ensure_ascii=False))

        # 2. 执行一个节点
        node_id = f"test_node_{int(asyncio.get_event_loop().time())}"
        print(f"\n🧪 执行节点 {node_id}...")
        resp = await client.post(
            f"{WORKER_URL}/nodes/execute",
            json={
                "node_id": node_id,
                "node_type": "python",
                "config": {"test_param": "test_value"},
            },
        )
        print(f"✅ 节点执行: {resp.status_code}")
        print(json.dumps(resp.json(), indent=2, ensure_ascii=False))

        # 3. 轮询检查节点状态
        print(f"\n🧪 监控节点状态 {node_id}...")
        for _ in range(3):
            resp = await client.get(f"{WORKER_URL}/nodes/{node_id}/status")
            status = resp.json()
            print(
                f"✅ 节点状态: {status.get('status')} 进度: {status.get('progress')}%"
            )
            await asyncio.sleep(1)

        # 4. 停止节点
        print(f"\n🧪 停止节点 {node_id}...")
        resp = await client.post(f"{WORKER_URL}/nodes/{node_id}/stop")
        print(f"✅ 节点停止: {resp.status_code}")
        print(json.dumps(resp.json(), indent=2, ensure_ascii=False))

        # 5. 获取资源统计
        print("\n🧪 获取资源统计...")
        resp = await client.get(f"{WORKER_URL}/stats")
        stats = resp.json()
        print(f"✅ 资源统计: {resp.status_code}")
        print(
            f"CPU: {stats['system']['cpu_percent']}%, 内存: {stats['system']['memory_percent']}%"
        )
        print(f"节点总数: {stats['nodes']['total']}")


if __name__ == "__main__":
    print("🚀 开始测试 Worker 服务...")
    asyncio.run(quick_test())
    print("\n✨ 测试完成!")
