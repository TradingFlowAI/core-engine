from tradingflow.account_manager.common.logging_config import setup_logging
from tradingflow.account_manager.tasks.vault_tasks import celery_app
from tradingflow.common.config import CONFIG

# 设置日志
setup_logging(CONFIG)

if __name__ == "__main__":
    celery_app.start()
