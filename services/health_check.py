"""健康检查服务"""

import asyncio
from datetime import datetime

import httpx
from sanic.log import logger

from weather_depot.config import CONFIG
from common.node_task_manager import NodeTaskManager
from core.system_stats import get_system_stats

# 获取配置
WORKER_HOST = CONFIG["WORKER_HOST"]
WORKER_PORT = CONFIG["WORKER_PORT"]
WORKER_ID = CONFIG["WORKER_ID"]
SERVER_URL = f"http://{WORKER_HOST}:{WORKER_PORT}"
HEALTH_CHECK_INTERVAL = CONFIG["HEALTH_CHECK_INTERVAL"]

# 获取共享实例
node_manager = NodeTaskManager.get_instance()


async def health_check_task():
    """定期发送健康状态到主服务器"""
    while True:
        try:
            system_stats = get_system_stats()
            health_data = {
                "worker_id": WORKER_ID,
                "status": "online",
                "timestamp": datetime.now().isoformat(),
                "running_nodes_count": len(
                    [
                        n
                        for n in await node_manager.get_all_tasks()
                        if n.get("status") == "running"
                    ]
                ),
                "system_stats": system_stats,
            }

            async with httpx.AsyncClient() as client:
                await client.post(f"{SERVER_URL}/api/workers/health", json=health_data)
                logger.debug("Health check sent")
        except Exception as e:
            logger.error(f"Health check error: {str(e)}")

        # 使用配置的间隔
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


def setup_health_check(app):
    """设置健康检查服务"""

    @app.listener("after_server_start")
    async def start_health_check(app, loop):
        # 启动健康检查任务
        asyncio.create_task(health_check_task())

    @app.listener("before_server_start")
    async def setup(app, loop):
        # 初始化节点管理器
        success = await node_manager.initialize()
        if not success:
            logger.error("Failed to initialize NodeManager")
        else:
            logger.info("NodeManager initialized")

        app.ctx.start_time = datetime.now()
        app.ctx.uptime = str(datetime.now() - app.ctx.start_time).split(".")[0]

        # 定期更新 uptime
        async def update_uptime():
            while True:
                app.ctx.uptime = str(datetime.now() - app.ctx.start_time).split(".")[0]
                await asyncio.sleep(1)

        # 启动 uptime 更新任务
        asyncio.create_task(update_uptime())
