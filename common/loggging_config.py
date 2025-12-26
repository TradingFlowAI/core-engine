import logging
import logging.handlers
from pathlib import Path


def setup_logging(config):
    """Setup logging system with configuration."""
    # Get root logger
    root_logger = logging.getLogger()

    # Set log level
    log_level = getattr(logging, config.get("LOG_LEVEL", "INFO"))
    root_logger.setLevel(log_level)

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(processName)s:%(process)d:%(threadName)s] %(name)s (%(filename)s:%(lineno)d): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Add file handler if needed
    if config.get("LOG_TO_FILE", False):
        # Ensure log directory exists
        log_path = Path(config.get("LOG_FILE_PATH", "logs/worker.log"))
        log_path.parent.mkdir(exist_ok=True)

        # Create file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=config.get("LOG_MAX_BYTES", 10485760),
            backupCount=config.get("LOG_BACKUP_COUNT", 5),
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Set log level for some third-party libraries
    logging.getLogger("aioredis").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("sanic.server").setLevel(logging.WARNING)

    # Return root logger
    return root_logger
