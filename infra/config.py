"""TradingFlow Unified Configuration

Provides shared configuration for the station execution engine.
"""

import json
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Union
from urllib.parse import quote

from dotenv import load_dotenv

# Determine project root directory - starting from 03_weather_station
TRADING_FLOW_ROOT = Path(__file__).parent.parent  # 03_weather_station
TRADINGFLOW_PYTHON = TRADING_FLOW_ROOT

# Load environment variables
# override=False: Don't override existing environment variables 
# (allows runtime and Docker-injected env vars to take precedence)
#
# Behavior:
# - Local development: Load variables from .env file into os.environ
# - Cloud deployment: Docker Compose already injected env vars, load_dotenv() won't override
dotenv_path = TRADINGFLOW_PYTHON / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path, override=False)
else:
    # If no .env file in project root, try loading from current working directory
    # Useful for local development, no impact on cloud (env vars already exist)
    load_dotenv(override=False)


def encode_url_password(url: str) -> str:
    """
    Automatically encode the password part of a URL.
    
    Uses string search to extract password, avoiding urlparse failures
    when encountering special characters (like ?!).
    
    Supported URL formats:
    - redis://:password@host:port/db
    - amqp://user:password@host:port/vhost
    - postgresql://user:password@host:port/database
    
    Args:
        url: Original URL string
        
    Returns:
        Encoded URL string
        
    Examples:
        >>> encode_url_password('redis://:pass,word^123@localhost:6379/0')
        'redis://:pass%2Cword%5E123@localhost:6379/0'
        
        >>> encode_url_password('amqp://admin:p@ss?word!@localhost:5672/')
        'amqp://admin:p%40ss%3Fword%21@localhost:5672/'
    """
    if not url or '://' not in url:
        return url
    
    # Find the part after scheme
    scheme_end = url.index('://') + 3
    after_scheme = url[scheme_end:]
    
    # Find the last @ symbol (separates auth info from host info)
    if '@' not in after_scheme:
        # No auth info, return as-is
        return url
    
    last_at = after_scheme.rfind('@')
    auth_part = after_scheme[:last_at]  # user:password or :password
    host_part = after_scheme[last_at:]  # @host:port/path...
    
    # Find colon in auth part
    if ':' not in auth_part:
        # No password, return as-is
        return url
    
    # Find the last colon in auth part (before password)
    last_colon = auth_part.rfind(':')
    user_part = auth_part[:last_colon]  # username or empty string
    password = auth_part[last_colon + 1:]  # password
    
    # Encode password (encode all special characters)
    encoded_password = quote(password, safe='')
    
    # Rebuild URL
    scheme = url[:scheme_end]
    if user_part:
        # Has username: scheme://username:encoded_password@host...
        return f"{scheme}{user_part}:{encoded_password}{host_part}"
    else:
        # No username: scheme://:encoded_password@host... (Redis format)
        return f"{scheme}:{encoded_password}{host_part}"


def build_redis_url() -> str:
    """
    Build Redis URL with automatic password encoding.
    
    Prioritizes REDIS_URL environment variable, otherwise builds from individual config.
    """
    redis_url = os.environ.get("REDIS_URL")
    
    if redis_url:
        # If full URL provided, auto-encode password
        return encode_url_password(redis_url)
    
    # Build from individual config
    host = os.environ.get("REDIS_HOST", "localhost")
    port = os.environ.get("REDIS_PORT", "6379")
    db = os.environ.get("REDIS_DB", "0")
    password = os.environ.get("REDIS_PASSWORD", "")
    
    if password:
        encoded_password = quote(password, safe='')
        return f"redis://:{encoded_password}@{host}:{port}/{db}"
    else:
        return f"redis://{host}:{port}/{db}"


def build_rabbitmq_url() -> str:
    """
    Build RabbitMQ URL with automatic password encoding.
    
    Prioritizes RABBITMQ_URL environment variable, otherwise builds from individual config.
    """
    rabbitmq_url = os.environ.get("RABBITMQ_URL")
    
    if rabbitmq_url:
        # If full URL provided, auto-encode password
        return encode_url_password(rabbitmq_url)
    
    # Build from individual config
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    port = os.environ.get("RABBITMQ_PORT", "5672")
    user = os.environ.get("RABBITMQ_USER", "guest")
    password = os.environ.get("RABBITMQ_PASSWORD", "guest")
    vhost = os.environ.get("RABBITMQ_VHOST", "/")
    
    # Encode username and password
    encoded_user = quote(user, safe='')
    encoded_password = quote(password, safe='')
    
    return f"amqp://{encoded_user}:{encoded_password}@{host}:{port}{vhost}"


def build_postgres_url() -> str:
    """
    Build PostgreSQL URL with automatic password encoding.
    
    Prioritizes POSTGRES_URL environment variable, otherwise builds from individual config.
    """
    postgres_url = os.environ.get("POSTGRES_URL")
    
    if postgres_url:
        # If full URL provided, auto-encode password
        return encode_url_password(postgres_url)
    
    # Build from individual config
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "tradingflow")
    password = os.environ.get("POSTGRES_PASSWORD", "tradingflow123")
    database = os.environ.get("POSTGRES_DB", "tradingflow")
    
    # Encode username and password
    encoded_user = quote(user, safe='')
    encoded_password = quote(password, safe='')
    
    return f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{database}"


# Base configuration (shared config items)
BASE_CONFIG = {
    # Server configuration
    "DEBUG": True,
    "LOG_LEVEL": "DEBUG",
    "LOG_TO_FILE": False,
    "LOG_FILE_PATH": "logs/tradingflow.log",
    "LOG_MAX_BYTES": 10485760,  # 10MB
    "LOG_BACKUP_COUNT": 5,
    # Redis configuration - auto-encode password
    "REDIS_URL": build_redis_url(),
    "REDIS_HOST": os.environ.get("REDIS_HOST", "localhost"),
    "REDIS_PORT": int(os.environ.get("REDIS_PORT", "6379")),
    "REDIS_DB": int(os.environ.get("REDIS_DB", "0")),
    "REDIS_PASSWORD": os.environ.get("REDIS_PASSWORD", ""),
    # RabbitMQ configuration - auto-encode password
    "RABBITMQ_URL": build_rabbitmq_url(),
    "RABBITMQ_HOST": os.environ.get("RABBITMQ_HOST", "localhost"),
    "RABBITMQ_PORT": int(os.environ.get("RABBITMQ_PORT", "5672")),
    "RABBITMQ_MGMT_PORT": int(os.environ.get("RABBITMQ_MGMT_PORT", "15672")),
    "RABBITMQ_USER": os.environ.get("RABBITMQ_USER", "guest"),
    "RABBITMQ_PASSWORD": os.environ.get("RABBITMQ_PASSWORD", "guest"),
    "RABBITMQ_VHOST": os.environ.get("RABBITMQ_VHOST", "/"),
    "RABBITMQ_EXCHANGE": os.environ.get("RABBITMQ_EXCHANGE", "tradingflow"),
    "RABBITMQ_EXCHANGE_TYPE": os.environ.get("RABBITMQ_EXCHANGE_TYPE", "topic"),
    # Database configuration - auto-encode password
    "POSTGRES_URL": build_postgres_url(),
    "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "localhost"),
    "POSTGRES_PORT": int(os.environ.get("POSTGRES_PORT", "5432")),
    "POSTGRES_DB": os.environ.get("POSTGRES_DB", "tradingflow"),
    "POSTGRES_USER": os.environ.get("POSTGRES_USER", "tradingflow"),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
    # MongoDB configuration
    "MONGO_ROOT_USER": os.environ.get("MONGO_ROOT_USER", "admin"),
    "MONGO_ROOT_PASSWORD": os.environ.get("MONGO_ROOT_PASSWORD", ""),
    "MONGO_DB": os.environ.get("MONGO_DB", "tradingflow"),
    "MONGO_PORT": int(os.environ.get("MONGO_PORT", "27017")),
    # Data storage path configuration
    "DB_DATA_PATH": os.environ.get("DB_DATA_PATH", "/opt/tradingflow/data/postgres"),
    "REDIS_DATA_PATH": os.environ.get("REDIS_DATA_PATH", "/opt/tradingflow/data/redis"),
    "RABBITMQ_DATA_PATH": os.environ.get("RABBITMQ_DATA_PATH", "/opt/tradingflow/data/rabbitmq"),
    "PGADMIN_DATA_PATH": os.environ.get("PGADMIN_DATA_PATH", "/opt/tradingflow/data/pgadmin"),
    "MONGO_DATA_PATH": os.environ.get("MONGO_DATA_PATH", "/opt/tradingflow/data/mongodb"),
    # Infrastructure configuration
    "DATA_SERVER_IP": os.environ.get("DATA_SERVER_IP", "localhost"),
    "TAG": os.environ.get("TAG", "dev"),
    # Chain configuration
    "CHAIN_CONFIGS": [
        {"chain_id": "31337", "dex_name": "uniswap"},  # Hardhat local network
    ],
    "CHAIN_RPC_URLS": {
        "31337": os.environ.get("CHAIN_RPC_URL_31337", "http://localhost:8545"),
        "747": os.environ.get("CHAIN_RPC_URL_747", "https://mainnet.evm.nodes.onflow.org"),
        "1": os.environ.get("CHAIN_RPC_URL_1", "http://localhost:8545"),
    },
    # Trade worker configuration
    "TRADE_WORKER_COUNT": 1,
    # Contract and deployment configuration
    "DEPLOYER_PRIVATE_KEY_HARDHAT": os.environ.get("DEPLOYER_PRIVATE_KEY_HARDHAT", ""),
    # Monitor URL
    "MONITOR_URL": os.environ.get("MONITOR_URL", "http://localhost:3000"),
    # Weather Control URL
    "WEATHER_CONTROL_URL": os.environ.get("WEATHER_CONTROL_URL", "http://localhost:3000"),
}

# Monitor specific configuration
MONITOR_CONFIG = {
    "BLOCK_CONFIRMATIONS": os.environ.get("BLOCK_CONFIRMATIONS", 6),
    "LOG_FILE_PATH": "logs/evm_monitor.log",
}

# Worker specific configuration
WORKER_CONFIG = {
    "WORKER_HOST": os.environ.get("WORKER_HOST", "0.0.0.0"),
    "WORKER_PORT": int(os.environ.get("WORKER_PORT", 7002)),
    "WORKER_ID": os.environ.get("WORKER_ID", str(uuid.uuid4())),
    "WORKER_API_URL": os.environ.get("WORKER_API_URL", "http://localhost:7002"),
    "SERVER_URL": os.environ.get("SERVER_URL", "http://0.0.0.0:7002"),
    "ENVIRONMENT": os.environ.get("ENVIRONMENT", "development"),
    # Message queue configuration
    "MESSAGE_QUEUE_TYPE": os.environ.get("MESSAGE_QUEUE_TYPE", "memory"),
    # State store configuration
    "STATE_STORE_TYPE": os.environ.get("STATE_STORE_TYPE", "memory"),
    # Health check configuration
    "HEALTH_CHECK_INTERVAL": int(os.environ.get("HEALTH_CHECK_INTERVAL", "30")),
    # Supported node types
    "SUPPORTED_NODE_TYPES": os.environ.get("SUPPORTED_NODE_TYPES", ""),
    # API configuration
    "BINANCE_API_KEY": os.environ.get("BINANCE_API_KEY", ""),
    "BINANCE_API_SECRET": os.environ.get("BINANCE_API_SECRET", ""),
    "TWITTER_API_KEY": os.environ.get("TWITTER_API_KEY", ""),
    # OpenRouter LLM configuration
    "OPENROUTER_API_KEY": os.environ.get("OPENROUTER_API_KEY", ""),
    # Google configuration
    "GOOGLE_CREDENTIALS_PATH": os.environ.get("GOOGLE_CREDENTIALS_PATH", ""),
    "LOG_FILE_PATH": "logs/station.log",
}


def get_env_config(base_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get environment-specific configuration overrides.
    
    Note: Only applies environment-specific defaults when env vars are not set,
    avoiding overriding already-set environment variables.
    """
    environment = os.environ.get("ENVIRONMENT", "development").lower()
    
    env_overrides = {}
    
    if environment == "production":
        if "DEBUG" not in os.environ:
            env_overrides["DEBUG"] = False
        if "LOG_LEVEL" not in os.environ:
            env_overrides["LOG_LEVEL"] = "WARNING"
        if "LOG_TO_FILE" not in os.environ:
            env_overrides["LOG_TO_FILE"] = True
        if "HEALTH_CHECK_INTERVAL" not in os.environ:
            env_overrides["HEALTH_CHECK_INTERVAL"] = 60

    elif environment == "testing":
        if "DEBUG" not in os.environ:
            env_overrides["DEBUG"] = False
        if "WORKER_PORT" not in os.environ:
            env_overrides["WORKER_PORT"] = 9001
        if "STATE_STORE_TYPE" not in os.environ:
            env_overrides["STATE_STORE_TYPE"] = "memory"

    else:  # Default development environment
        if "DEBUG" not in os.environ:
            env_overrides["DEBUG"] = True
        if "LOG_LEVEL" not in os.environ:
            env_overrides["LOG_LEVEL"] = "DEBUG"
    
    return {**base_config, **env_overrides}


def parse_json_env(value: str) -> Union[Dict, List, str]:
    """
    Try to parse string as JSON object, return original string if failed.
    """
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def get_station_config() -> Dict[str, Any]:
    """Get station (execution engine) configuration."""
    config = {
        **BASE_CONFIG,
        **WORKER_CONFIG,
    }
    return get_env_config(config)


def get_monitor_config() -> Dict[str, Any]:
    """Get monitor configuration."""
    config = {
        **BASE_CONFIG,
        **MONITOR_CONFIG,
        "LOG_FILE_PATH": "logs/monitor.log",
    }
    return get_env_config(config)


def create_config() -> Dict[str, Any]:
    """Create complete configuration dictionary with environment variable overrides."""
    # Merge base and service-specific configs
    full_config = {
        **BASE_CONFIG,
        **MONITOR_CONFIG,
        **WORKER_CONFIG,
    }

    # Apply environment-specific overrides
    config = get_env_config(full_config)

    # Environment variable overrides
    # Note: URL configs are already processed in build_*_url() functions
    # Don't override here to avoid overwriting encoded passwords
    url_keys = {"REDIS_URL", "RABBITMQ_URL", "POSTGRES_URL"}
    
    for key in config:
        if key in url_keys:
            continue
            
        env_value = os.environ.get(key)
        if env_value is not None:
            try:
                if isinstance(config[key], bool):
                    config[key] = env_value.lower() in ("true", "yes", "1", "t")
                elif isinstance(config[key], int):
                    config[key] = int(env_value)
                elif isinstance(config[key], float):
                    config[key] = float(env_value)
                elif isinstance(config[key], list):
                    try:
                        config[key] = json.loads(env_value)
                    except json.JSONDecodeError:
                        config[key] = env_value.split(",")
                elif isinstance(config[key], dict):
                    try:
                        config[key] = json.loads(env_value)
                    except json.JSONDecodeError:
                        print(f"Warning: Cannot parse {key} as JSON. Using original value.")
                else:
                    config[key] = parse_json_env(env_value)
            except ValueError:
                print(f"Warning: Unable to convert {key} to its original type. Keeping as string.")
                config[key] = env_value

    return config


# Export configuration
CONFIG = create_config()
