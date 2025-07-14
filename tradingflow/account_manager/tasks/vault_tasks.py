import asyncio
import logging
import os

from celery import Celery
from celery.schedules import timedelta

from python.tradingflow.account_manager.tasks.handle_price_fetch import fetch_prices_async
from python.tradingflow.account_manager.tasks.handle_vault_event import (
    process_vault_events_async,
)
from python.tradingflow.account_manager.tasks.handle_vault_value_update import update_vault_values_async
# from python.tradingflow.account_manager.tasks.handle_vault_deploy import deploy_vault_async
from tradingflow.common.db import db_session

logger = logging.getLogger(__name__)

# 从环境变量获取Redis配置
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)
REDIS_URL = os.environ.get("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

# Redis配置字典
REDIS_CONFIG = {
    "host": REDIS_HOST,
    "port": REDIS_PORT,
    "db": REDIS_DB,
    "password": REDIS_PASSWORD,
}

# 初始化Celery
celery_app = Celery("vault_tasks", broker=REDIS_URL)

# 配置Celery设置
celery_app.conf.update(
    # 任务结果设置
    result_backend=REDIS_URL,
    result_expires=3600,  # 结果保存1小时

    # 重试设置
    task_default_retry_delay=60,  # 默认重试延迟60秒
    task_max_retries=3,  # 最大重试3次

    # 任务路由和序列化
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],

    # 工作进程设置
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=1000,
)

# 配置定时任务
celery_app.conf.beat_schedule = {
    "fetch-prices-every-60s": {
        "task": "tradingflow.account_manager.tasks.vault_tasks.fetch_prices",
        "schedule": timedelta(seconds=60),
        "args": (),
        "kwargs": {},
    },
    "update-vault-value-usd-every-60s": {
        "task": "tradingflow.account_manager.tasks.vault_tasks.update_vault_value_usd",
        "schedule": timedelta(seconds=60),
        "args": (),
        "kwargs": {},
    },
    "process-vault-events-every-60s": {
        "task": "tradingflow.account_manager.tasks.vault_tasks.process_vault_events",
        "schedule": timedelta(seconds=60),
        "args": (),
        "kwargs": {},
    },
}

celery_app.conf.timezone = "UTC"  # 设置时区

# CL Jul 9 2025:
# 暂时注释一下，似乎不用接口了，用户直接发链上交易，Companion 会监听链上事件，然后处理
# @celery_app.task
# def deploy_vault_task(
#     task_id,
#     chain_id,
#     asset_address,
#     investor_address,
#     vault_name=None,
#     vault_symbol=None,
# ):
#     """Celery任务：部署Vault合约"""
#     try:
#         return deploy_vault_async(
#             task_id,
#             chain_id,
#             asset_address,
#             investor_address,
#             vault_name,
#             vault_symbol,
#             REDIS_CONFIG,
#         )
#     except Exception as e:
#         logger.exception("Vault部署失败: %s", e)
#         return {"status": "error", "error": str(e)}


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 60})
def fetch_prices(self):
    """每60秒执行一次的币对价格抓取任务"""
    try:
        logger.info("开始抓取token价格数据...")
        result = asyncio.run(fetch_prices_async())
        logger.info("价格抓取任务完成: %s", result.get('status', 'unknown'))
        return result
    except Exception as e:
        logger.exception("币对价格抓取任务失败: %s", str(e))
        # 如果还有重试次数，不返回错误，让Celery自动重试
        if self.request.retries < self.max_retries:
            logger.warning("价格抓取任务将在60秒后重试 (第%d/%d次)",
                          self.request.retries + 1, self.max_retries)
            raise self.retry(countdown=60, exc=e)
        return {"status": "error", "error": str(e), "retries_exhausted": True}


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 60})
def update_vault_value_usd(self):
    """
    定时任务：更新所有Vault的USD价值记录
    """
    try:
        logger.info("开始更新Vault价值记录")
        with db_session() as db:
            result = asyncio.run(update_vault_values_async(db))
            logger.info("Vault价值更新任务完成: %s", result.get('status', 'unknown'))
            return result
    except Exception as e:
        logger.exception("更新Vault价值记录任务失败: %s", e)
        # 如果还有重试次数，不返回错误，让Celery自动重试
        if self.request.retries < self.max_retries:
            logger.warning("Vault价值更新任务将在60秒后重试 (第%d/%d次)",
                          self.request.retries + 1, self.max_retries)
            raise self.retry(countdown=60, exc=e)
        return {"status": "error", "error": str(e), "retries_exhausted": True}


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 60})
def process_vault_events(self):
    """
    定时任务：处理Vault相关的链上事件，转换为操作记录
    """
    try:
        logger.info("开始处理Vault链上事件")
        with db_session() as db:
            result = asyncio.run(process_vault_events_async(db))
            logger.info("事件处理任务完成: %s", result.get('status', 'unknown'))
            return result
    except Exception as e:
        logger.exception("处理Vault链上事件任务失败: %s", e)
        # 如果还有重试次数，不返回错误，让Celery自动重试
        if self.request.retries < self.max_retries:
            logger.warning("事件处理任务将在60秒后重试 (第%d/%d次)",
                          self.request.retries + 1, self.max_retries)
            raise self.retry(countdown=60, exc=e)
        return {"status": "error", "error": str(e), "retries_exhausted": True}
