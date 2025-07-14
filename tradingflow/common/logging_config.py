"""通用日志配置模块 - 改进的彩色日志支持"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Any, Dict


class ColoredFormatter(logging.Formatter):
    """彩色日志格式化器 - 简化版"""

    # ANSI颜色代码
    COLORS = {
        'DEBUG': '\033[36m',      # 青色
        'INFO': '\033[32m',       # 绿色
        'WARNING': '\033[33m',    # 黄色
        'ERROR': '\033[31m',      # 红色
        'CRITICAL': '\033[35m',   # 紫色
    }
    RESET = '\033[0m'  # 重置颜色
    BOLD = '\033[1m'   # 粗体

    def __init__(self, fmt=None, datefmt=None, use_colors=True):
        super().__init__(fmt, datefmt)
        self.use_colors = use_colors and self._supports_color()

    def _supports_color(self):
        """检查终端是否支持颜色"""
        # 如果设置了强制颜色环境变量
        if os.environ.get('FORCE_COLOR', '').lower() in ('1', 'true', 'yes'):
            return True

        # 如果禁用颜色
        if os.environ.get('NO_COLOR', ''):
            return False

        # 检查是否是真正的TTY
        if not hasattr(sys.stderr, 'isatty') or not sys.stderr.isatty():
            return False

        # 检查终端类型
        term = os.environ.get('TERM', '')
        if term in ('dumb', 'unknown'):
            return False

        return True

    def format(self, record):
        """格式化日志记录，添加颜色"""
        # 先用父类格式化
        message = super().format(record)

        if not self.use_colors:
            return message

        level_name = record.levelname

        # 给整个消息加上颜色
        if level_name in self.COLORS:
            color = self.COLORS[level_name]

            # 对于ERROR和CRITICAL，还要加粗
            if level_name in ('ERROR', 'CRITICAL'):
                color += self.BOLD

            message = f"{color}{message}{self.RESET}"

        return message


def setup_logging(
    config: Dict[str, Any],
    service_name: str = "tradingflow",
    default_log_file: str = None,
) -> logging.Logger:
    """
    设置日志配置 - 支持彩色输出

    Args:
        config: 配置字典
        service_name: 服务名称，用于默认日志文件名
        default_log_file: 默认日志文件名，如果不提供则使用 service_name

    Returns:
        配置好的根日志器
    """
    # 获取根日志器
    root_logger = logging.getLogger()

    # 设置日志级别
    log_level = getattr(logging, config.get("LOG_LEVEL", "INFO"))
    root_logger.setLevel(log_level)

    # 清除现有处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 格式化器
    log_format = "%(asctime)s [%(levelname)s] [%(processName)s:%(process)d:%(threadName)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # 检查是否启用彩色日志（默认启用）
    color_enabled = config.get("LOG_COLOR_ENABLED", True)

    # 添加控制台处理器（有颜色）
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = ColoredFormatter(log_format, datefmt=date_format, use_colors=color_enabled)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # 添加文件处理器（无颜色）
    if config.get("LOG_TO_FILE", True):
        # 确定日志文件路径
        if "LOG_FILE" in config:
            log_file = config["LOG_FILE"]
        elif "LOG_FILE_PATH" in config:
            log_file = config["LOG_FILE_PATH"]
        elif default_log_file:
            log_file = default_log_file
        else:
            log_file = f"{service_name}.log"

        # 确保日志目录存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # 创建文件处理器
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=config.get("LOG_MAX_BYTES", 10485760),  # 10MB
            backupCount=config.get("LOG_BACKUP_COUNT", 5),
        )
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(log_format, datefmt=date_format)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # 设置第三方库的日志级别
    _configure_third_party_loggers(config)

    return root_logger


def _configure_third_party_loggers(config: Dict[str, Any]) -> None:
    """配置第三方库的日志级别"""
    third_party_level = config.get("THIRD_PARTY_LOG_LEVEL", "WARNING")
    third_party_loggers = [
        "aioredis",
        "httpx",
        "asyncio",
        "sanic.server",
        "urllib3",
        "requests",
        "httpcore",
        "aiormq",
        "aio_pika",
        "sqlalchemy",
        "sqlalchemy.engine",
    ]

    for logger_name in third_party_loggers:
        logging.getLogger(logger_name).setLevel(getattr(logging, third_party_level))
