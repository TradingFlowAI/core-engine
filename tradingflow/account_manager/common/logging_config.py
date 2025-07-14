"""KMS 服务日志配置"""

import logging
from logging.handlers import RotatingFileHandler


def setup_logging(config):
    """设置日志配置"""
    log_level = getattr(logging, config.get("LOG_LEVEL", "INFO"))
    log_file = config.get("LOG_FILE", "account_manager.log")

    # 创建日志格式
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # 配置根日志
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 添加文件处理器
    file_handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # 添加控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    return root_logger
