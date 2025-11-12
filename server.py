"""TradingFlow Worker Service Entry Point"""

import argparse
import logging

from sanic import Sanic


from weather_depot.config import get_station_config
from weather_depot.logging_config import setup_logging  # noqa: F401, E402
from weather_depot.mq.pool_manager import pool_manager
from api import flow_bp, health_bp, node_bp
from common.node_registry import NodeRegistry
from common.node_task_manager import NodeTaskManager
from services import setup_services  # noqa: F401, E402
from mq.activity_publisher import init_activity_publisher, get_activity_publisher

pool_manager.register_shutdown_handler()

# 🔍 调试：在 CONFIG 加载前检查环境变量
print("\n" + "=" * 80)
print("🔍 Debug: Environment Variables (before CONFIG loading)")
print("=" * 80)
import os
redis_url_env = os.environ.get('REDIS_URL', '[NOT SET]')
redis_host_env = os.environ.get('REDIS_HOST', '[NOT SET]')
redis_password_env = os.environ.get('REDIS_PASSWORD', '[NOT SET]')
print(f"ENV REDIS_URL: {redis_url_env[:50]}..." if len(redis_url_env) > 50 else f"ENV REDIS_URL: {redis_url_env}")
print(f"ENV REDIS_HOST: {redis_host_env}")
print(f"ENV REDIS_PASSWORD: {'[SET, length=' + str(len(redis_password_env)) + ']' if redis_password_env != '[NOT SET]' else '[NOT SET]'}")
print("=" * 80 + "\n")

CONFIG = get_station_config()
setup_logging(CONFIG, "station")
logger = logging.getLogger(__name__)

# 降低 pika 和 asyncio 的日志级别，减少 DEBUG 日志
logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)


def log_config_debug():
    """输出关键配置信息用于调试（密码部分遮挡）"""
    def mask_password(url: str) -> str:
        """遮挡 URL 中的密码部分，只显示前后各2个字符"""
        if not url or '://' not in url:
            return url
        try:
            scheme_end = url.index('://') + 3
            after_scheme = url[scheme_end:]
            if '@' not in after_scheme:
                return url
            last_at = after_scheme.rfind('@')
            auth_part = after_scheme[:last_at]
            host_part = after_scheme[last_at:]
            
            if ':' not in auth_part:
                return url
            
            last_colon = auth_part.rfind(':')
            user_part = auth_part[:last_colon]
            password = auth_part[last_colon + 1:]
            
            # 遮挡密码：只显示前后各2个字符
            if len(password) <= 4:
                masked_pwd = '****'
            else:
                masked_pwd = f"{password[:2]}...{password[-2:]}"
            
            scheme = url[:scheme_end]
            if user_part:
                return f"{scheme}{user_part}:{masked_pwd}{host_part}"
            else:
                return f"{scheme}:{masked_pwd}{host_part}"
        except Exception:
            return "[解析失败]"
    
    # 使用 print 确保输出不被过滤（直接输出到 stdout）
    separator = "=" * 80
    print(f"\n{separator}")
    print("📋 CONFIG Debug Information:")
    print(separator)
    print("🔧 Worker Configuration:")
    print(f"  WORKER_ID: {CONFIG.get('WORKER_ID')}")
    print(f"  WORKER_HOST: {CONFIG.get('WORKER_HOST')}")
    print(f"  WORKER_PORT: {CONFIG.get('WORKER_PORT')}")
    print(f"  STATE_STORE_TYPE: {CONFIG.get('STATE_STORE_TYPE')}")
    print("")
    print("🗄️  Redis Configuration:")
    print(f"  REDIS_URL: {mask_password(CONFIG.get('REDIS_URL', ''))}")
    print(f"  REDIS_HOST: {CONFIG.get('REDIS_HOST')}")
    print(f"  REDIS_PORT: {CONFIG.get('REDIS_PORT')}")
    print(f"  REDIS_DB: {CONFIG.get('REDIS_DB')}")
    print(f"  REDIS_PASSWORD length: {len(CONFIG.get('REDIS_PASSWORD', ''))} chars")
    print("")
    print("🐰 RabbitMQ Configuration:")
    print(f"  RABBITMQ_URL: {mask_password(CONFIG.get('RABBITMQ_URL', ''))}")
    print(f"  RABBITMQ_HOST: {CONFIG.get('RABBITMQ_HOST')}")
    print(f"  RABBITMQ_PORT: {CONFIG.get('RABBITMQ_PORT')}")
    print(f"  RABBITMQ_USER: {CONFIG.get('RABBITMQ_USER')}")
    print("")
    print("🐘 PostgreSQL Configuration:")
    print(f"  POSTGRES_URL: {mask_password(CONFIG.get('POSTGRES_URL', ''))}")
    print(f"  POSTGRES_HOST: {CONFIG.get('POSTGRES_HOST')}")
    print(f"  POSTGRES_PORT: {CONFIG.get('POSTGRES_PORT')}")
    print(f"  POSTGRES_DB: {CONFIG.get('POSTGRES_DB')}")
    print(separator)
    print("")
    
    # 同时也记录到日志
    logger.info("CONFIG debug information has been printed to stdout (see above)")
# Read configuration from config file
WORKER_HOST = CONFIG["WORKER_HOST"]
WORKER_PORT = CONFIG["WORKER_PORT"]
WORKER_ID = CONFIG["WORKER_ID"]
REDIS_URL = CONFIG["REDIS_URL"]
STATE_STORE_TYPE = CONFIG["STATE_STORE_TYPE"]

# Create node manager singleton
node_manager = NodeTaskManager.get_instance(
    state_store_type=STATE_STORE_TYPE,
    state_store_config={"url": REDIS_URL},
    worker_id=WORKER_ID,
)

# Initialize application
app = Sanic("WorkerService")
app.blueprint(health_bp)
app.blueprint(node_bp)
app.blueprint(flow_bp)


def init_app():
    """Initialize Sanic application"""
    # Setup services
    print("Setting up services...")
    setup_services(app)

    return app


# Add the following hooks
@app.listener("before_server_start")
async def setup_node_registry(app, loop):
    """Initialize node registry and activity publisher before server starts"""
    # 0. 输出配置信息用于调试
    print("\n" + "🚀" * 40)
    print("🔧 Starting Station Server Initialization...")
    print("🚀" * 40 + "\n")
    log_config_debug()
    
    # 1. Initialize Node Registry
    print("\n📝 Step 1: Initializing Node Registry...")
    print(f"  Worker ID: {CONFIG.get('WORKER_ID')}")
    print(f"  Redis URL: {CONFIG.get('REDIS_URL', '')[:60]}...")
    
    registry = NodeRegistry.get_instance()
    print(f"  Registry instance created: {registry}")
    print(f"  Supported node types count: {len(registry._node_classes)}")

    # Initialize Redis connection
    print("\n🔌 Step 2: Connecting to Redis...")
    redis_init_success = await registry.initialize()
    print(f"  Redis initialization result: {redis_init_success}")
    
    if redis_init_success:
        # Register worker and supported node types
        print("\n📋 Step 3: Registering Worker...")
        register_result = await registry.register_worker()
        print(f"  Worker registration result: {register_result}")

        # Start heartbeat task
        print("\n💓 Step 4: Starting Heartbeat...")
        await registry.start_heartbeat()
        print("  Heartbeat task started")

        app.ctx.node_registry = registry
        print("\n✅ Node registry initialization complete!")
        logger.info("✓ Node registry initialized and heartbeat started")
    else:
        print("\n❌ Redis initialization failed!")
        logger.error("Failed to initialize node registry")
    
    # 2. Initialize Activity Publisher for Quest System
    try:
        import pika
        from weather_depot.config import CONFIG
        
        # 使用 CONFIG 中自动编码密码的 URL
        rabbitmq_url = CONFIG.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        parameters = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        
        publisher = init_activity_publisher(connection)
        app.ctx.activity_publisher = publisher
        logger.info("✓ Activity Publisher initialized for Quest system")
    except Exception as e:
        logger.warning(f"Failed to initialize Activity Publisher: {e}")
        logger.warning("Quest activity events will not be published from Station")


@app.listener("after_server_start")
async def server_ready(app, loop):
    """Log when server is ready to accept requests"""
    logger.info(f"🚀 Station Server is ready at http://{WORKER_HOST}:{WORKER_PORT}")
    logger.info(f"Worker ID: {WORKER_ID}")
    logger.info("Server is running and accepting requests...")


@app.listener("after_server_stop")
async def cleanup_node_registry(app, loop):
    """Close node registry after server stops"""
    if hasattr(app.ctx, "node_registry"):
        registry = app.ctx.node_registry

        # Note: Even if closing fails here, the records in Redis will be automatically deleted when TTL expires
        try:
            # Stop heartbeat task
            await registry.stop_heartbeat()

            # Close Redis connection
            await registry.shutdown()

            logger.info("Node registry has been closed")
        except Exception as e:
            logger.error("Error closing node registry: %s", str(e))


if __name__ == "__main__":
    # Command line argument parsing
    parser = argparse.ArgumentParser(description="TradingFlow Worker Service")
    parser.add_argument("--host", help="Worker service hostname", default=WORKER_HOST)
    parser.add_argument(
        "--port", type=int, help="Worker service port", default=WORKER_PORT
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
        default=CONFIG.get("DEBUG", False),
    )

    args = parser.parse_args()

    # Initialize application
    init_app()

    # Run application
    app.run(host=args.host, port=args.port, debug=args.debug, access_log=args.debug)
