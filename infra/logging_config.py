"""Common Logging Configuration Module - Improved Color Support"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Any, Dict


class ColoredFormatter(logging.Formatter):
    """Colored log formatter - simplified version"""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Purple
    }
    RESET = '\033[0m'  # Reset color
    BOLD = '\033[1m'   # Bold

    def __init__(self, fmt=None, datefmt=None, use_colors=True):
        super().__init__(fmt, datefmt)
        self.use_colors = use_colors and self._supports_color()

    def _supports_color(self):
        """Check if terminal supports color"""
        # If FORCE_COLOR env var is set
        if os.environ.get('FORCE_COLOR', '').lower() in ('1', 'true', 'yes'):
            return True

        # If color is disabled
        if os.environ.get('NO_COLOR', ''):
            return False

        # Check if it's a real TTY
        if not hasattr(sys.stderr, 'isatty') or not sys.stderr.isatty():
            return False

        # Check terminal type
        term = os.environ.get('TERM', '')
        if term in ('dumb', 'unknown'):
            return False

        return True

    def format(self, record):
        """Format log record with color"""
        # First format with parent class
        message = super().format(record)

        if not self.use_colors:
            return message

        level_name = record.levelname

        # Add color to entire message
        if level_name in self.COLORS:
            color = self.COLORS[level_name]

            # Also add bold for ERROR and CRITICAL
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
    Setup logging configuration with color support.

    Args:
        config: Configuration dictionary
        service_name: Service name, used for default log file name
        default_log_file: Default log file name, uses service_name if not provided

    Returns:
        Configured root logger
    """
    # Get root logger
    root_logger = logging.getLogger()

    # Set log level
    log_level = getattr(logging, config.get("LOG_LEVEL", "INFO"))
    root_logger.setLevel(log_level)

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Formatter
    log_format = "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Check if color logging is enabled (default: True)
    color_enabled = config.get("LOG_COLOR_ENABLED", True)

    # Add console handler (with color)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = ColoredFormatter(log_format, datefmt=date_format, use_colors=color_enabled)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Add file handler (no color)
    if config.get("LOG_TO_FILE", True):
        # Determine log file path
        if "LOG_FILE" in config:
            log_file = config["LOG_FILE"]
        elif "LOG_FILE_PATH" in config:
            log_file = config["LOG_FILE_PATH"]
        elif default_log_file:
            log_file = default_log_file
        else:
            log_file = f"{service_name}.log"

        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Create file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=config.get("LOG_MAX_BYTES", 10485760),  # 10MB
            backupCount=config.get("LOG_BACKUP_COUNT", 5),
        )
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(log_format, datefmt=date_format)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Configure third-party library log levels
    _configure_third_party_loggers(config)

    return root_logger


def _configure_third_party_loggers(config: Dict[str, Any]) -> None:
    """Configure log levels for third-party libraries"""
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
