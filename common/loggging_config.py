import logging
import logging.handlers
from pathlib import Path


def setup_logging(config):
    """使用配置设置日志系统"""
    # 获取根日志器
    root_logger = logging.getLogger()

    # 设置日志级别
    log_level = getattr(logging, config.get("LOG_LEVEL", "INFO"))
    root_logger.setLevel(log_level)

    # 清除现有处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 创建格式化器
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(processName)s:%(process)d:%(threadName)s] %(name)s (%(filename)s:%(lineno)d): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 添加控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 如果需要，添加文件处理器
    if config.get("LOG_TO_FILE", False):
        # 确保日志目录存在
        log_path = Path(config.get("LOG_FILE_PATH", "logs/worker.log"))
        log_path.parent.mkdir(exist_ok=True)

        # 创建文件处理器
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=config.get("LOG_MAX_BYTES", 10485760),
            backupCount=config.get("LOG_BACKUP_COUNT", 5),
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # 设置一些第三方库的日志级别
    logging.getLogger("aioredis").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("sanic.server").setLevel(logging.WARNING)

    # 返回根日志器
    return root_logger
