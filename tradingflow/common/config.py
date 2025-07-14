"""
TradingFlow 统一配置文件
为 account_manager 和 py_worker 提供共享配置
"""

import json
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Union

from dotenv import load_dotenv

# 确定项目根目录
TRADING_FLOW_ROOT = Path(__file__).parent.parent.parent.parent
TRADINGFLOW_PYTHON = TRADING_FLOW_ROOT / "python"

# 加载环境变量
# 先尝试从项目根目录加载.env
dotenv_path = TRADINGFLOW_PYTHON / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path)
else:
    # 如果项目根目录没有.env文件，则尝试默认行为（从运行目录加载）
    load_dotenv()

# 基础配置（两个模块共享的配置项）
BASE_CONFIG = {
    # 服务器配置
    "DEBUG": True,
    "LOG_LEVEL": "DEBUG",
    "LOG_TO_FILE": False,
    "LOG_FILE_PATH": "logs/tradingflow.log",
    "LOG_MAX_BYTES": 10485760,  # 10MB
    "LOG_BACKUP_COUNT": 5,
    # Redis配置
    "REDIS_URL": "redis://localhost:6379/0",
    # RabbitMQ配置
    "RABBITMQ_URL": os.environ.get(
        "RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"
    ),
    "RABBITMQ_HOST": os.environ.get("RABBITMQ_HOST", "localhost"),
    "RABBITMQ_PORT": int(os.environ.get("RABBITMQ_PORT", "5672")),
    "RABBITMQ_USERNAME": os.environ.get("RABBITMQ_USERNAME", "guest"),
    "RABBITMQ_PASSWORD": os.environ.get("RABBITMQ_PASSWORD", "guest"),
    "RABBITMQ_VHOST": os.environ.get("RABBITMQ_VHOST", "/"),
    "RABBITMQ_EXCHANGE": os.environ.get("RABBITMQ_EXCHANGE", "tradingflow"),
    "RABBITMQ_EXCHANGE_TYPE": os.environ.get("RABBITMQ_EXCHANGE_TYPE", "topic"),
    # database配置
    "DATABASE_URL": os.environ.get(
        "DATABASE_URL",
        "postgresql://tradingflow:tradingflow123@localhost:5432/tradingflow",
    ),
    # 链配置
    "CHAIN_CONFIGS": [
        # {"chain_id": "1", "dex_name": "uniswap"},  # 以太坊主网Uniswap配置
        {"chain_id": "31337", "dex_name": "uniswap"},  # Hardhat本地网络Uniswap配置
    ],
    "CHAIN_RPC_URLS": {
        "31337": os.environ.get(
            "CHAIN_RPC_URL_31337", "http://localhost:8545"
        ),  # Hardhat本地网络RPC
        "747": os.environ.get(
            "CHAIN_RPC_URL_747", "https://mainnet.evm.nodes.onflow.org"
        ),  # Flow EVM主网RPC
        "1": os.environ.get("CHAIN_RPC_URL_1", "http://localhost:8545"),
    },
    # 交易工作者配置
    "TRADE_WORKER_COUNT": 1,  # 每个链和DEX组合的消费者数量
    # 合约和部署相关的配置
    "DEPLOYER_PRIVATE_KEY_HARDHAT": os.environ.get("DEPLOYER_PRIVATE_KEY_HARDHAT", ""),
    # companion配置
    "COMPANION_URL": os.environ.get(
        "COMPANION_URL", "http://localhost:3000"
    ),
    # Companion服务配置
    "COMPANION_HOST": os.environ.get("COMPANION_HOST", "localhost"),
    "COMPANION_PORT": int(os.environ.get("COMPANION_PORT", 3000)),
}


# 账户管理器特定配置
ACCOUNT_MANAGER_CONFIG = {
    "ACCOUNT_MANAGER_HOST": os.environ.get("ACCOUNT_MANAGER_HOST", "0.0.0.0"),
    "ACCOUNT_MANAGER_PORT": int(os.environ.get("ACCOUNT_MANAGER_PORT", 7001)),
    "LOG_FILE_PATH": "logs/account_manager.log",
}

# Monitor特定配置
MONITOR_CONFIG = {
    "BLOCK_CONFIRMATIONS": os.environ.get("BLOCK_CONFIRMATIONS", 6),
    "LOG_FILE_PATH": "logs/evm_monitor.log",
}
# Worker 特定配置
WORKER_CONFIG = {
    "WORKER_HOST": os.environ.get("WORKER_HOST", "0.0.0.0"),
    "WORKER_PORT": int(os.environ.get("WORKER_PORT", 7000)),
    "WORKER_ID": str(uuid.uuid4()),  # 默认生成随机ID
    "WORKER_API_URL": os.environ.get("WORKER_API_URL", "http://localhost:7000"),
    # 消息队列配置
    "MESSAGE_QUEUE_TYPE": os.environ.get(
        "MESSAGE_QUEUE_TYPE", "memory"
    ),  # 可选: memory, rabbitmq
    # 状态存储配置
    "STATE_STORE_TYPE": "memory",  # 可选: memory, redis
    # 健康检查配置
    "HEALTH_CHECK_INTERVAL": 30,  # 秒
    # API配置
    "BINANCE_API_KEY": os.environ.get("BINANCE_API_KEY", ""),
    "BINANCE_API_SECRET": os.environ.get("BINANCE_API_SECRET", ""),
    "ARK_API_KEY": os.environ.get("ARK_API_KEY", ""),
    "TWITTER_API_KEY": os.environ.get("TWITTER_API_KEY", ""),
    "AI_MODEL_NODE_ENDPOINT": os.environ.get("AI_MODEL_NODE_ENDPOINT", ""),
    # GOOGLE 配置
    "GOOGLE_CREDENTIALS_PATH": os.environ.get("GOOGLE_CREDENTIALS_PATH", ""),
    "LOG_FILE_PATH": "logs/py_worker.log",
}


# 环境特定配置覆盖
def get_env_config(base_config: Dict[str, Any]) -> Dict[str, Any]:
    """根据环境变量获取环境特定配置"""
    environment = os.environ.get("ENVIRONMENT", "development").lower()

    if environment == "production":
        return {
            **base_config,
            "DEBUG": False,
            "LOG_LEVEL": "WARNING",
            "LOG_TO_FILE": True,
            "HEALTH_CHECK_INTERVAL": 60,
        }

    elif environment == "testing":
        return {
            **base_config,
            "DEBUG": False,
            "WORKER_PORT": 9001,
            "STATE_STORE_TYPE": "memory",
        }

    else:  # 默认开发环境
        return {
            **base_config,
            "DEBUG": True,
            "LOG_LEVEL": "DEBUG",
        }


def parse_json_env(value: str) -> Union[Dict, List, str]:
    """
    尝试将字符串解析为JSON对象，如果失败则返回原字符串
    """
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


# 创建服务特定的配置函数
def get_account_manager_config() -> Dict[str, Any]:
    """获取账户管理器配置"""
    config = {
        **BASE_CONFIG,
        **ACCOUNT_MANAGER_CONFIG,
    }
    return get_env_config(config)


def get_py_worker_config() -> Dict[str, Any]:
    """获取 Py Worker 配置"""
    config = {
        **BASE_CONFIG,
        **WORKER_CONFIG,
    }
    return get_env_config(config)


def get_monitor_config() -> Dict[str, Any]:
    """获取 Monitor 配置"""
    config = {
        **BASE_CONFIG,
        **MONITOR_CONFIG,
        "LOG_FILE_PATH": "logs/monitor.log",  # Monitor 特定的日志文件
    }
    return get_env_config(config)


# 创建完整配置
def create_config() -> Dict[str, Any]:
    """创建完整的配置字典，应用环境变量覆盖"""
    # 合并基础配置和特定服务配置
    full_config = {
        **BASE_CONFIG,
        **ACCOUNT_MANAGER_CONFIG,
        **MONITOR_CONFIG,
        **WORKER_CONFIG,
    }

    # 应用环境特定覆盖
    config = get_env_config(full_config)

    # 环境变量覆盖
    for key in config:
        env_value = os.environ.get(key)
        if env_value is not None:
            # 尝试转换数值类型
            try:
                if isinstance(config[key], bool):
                    config[key] = env_value.lower() in ("true", "yes", "1", "t")
                elif isinstance(config[key], int):
                    config[key] = int(env_value)
                elif isinstance(config[key], float):
                    config[key] = float(env_value)
                elif isinstance(config[key], list):
                    # 先尝试解析为JSON，如果失败则按逗号分隔
                    try:
                        config[key] = json.loads(env_value)
                    except json.JSONDecodeError:
                        config[key] = env_value.split(",")
                elif isinstance(config[key], dict):
                    # 尝试解析为JSON
                    try:
                        config[key] = json.loads(env_value)
                    except json.JSONDecodeError:
                        print(
                            f"Warning: Cannot parse {key} as JSON. Using original value."
                        )
                else:
                    # 对于其他类型的值，尝试JSON解析
                    config[key] = parse_json_env(env_value)
            except ValueError:
                print(
                    f"Warning: Unable to convert {key} to its original type. Keeping as string."
                )
                config[key] = env_value

    return config


# 导出配置
CONFIG = create_config()
