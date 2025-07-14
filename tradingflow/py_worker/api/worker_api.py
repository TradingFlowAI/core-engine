"""Worker注册和管理API"""

from tradingflow.common.config import CONFIG

# 配置信息
WORKER_ID = CONFIG["WORKER_ID"]
SERVER_URL = CONFIG["SERVER_URL"]


def register_worker_routes(app):
    """注册Worker管理相关路由"""

    # Worker注册与注销行为通过服务启动和停止事件处理，
    # 该模块不定义额外的API路由
