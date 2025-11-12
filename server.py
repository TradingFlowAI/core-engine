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

CONFIG = get_station_config()
setup_logging(CONFIG, "station")
logger = logging.getLogger(__name__)

# 降低 pika 和 asyncio 的日志级别，减少 DEBUG 日志
logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
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
    # 1. Initialize Node Registry
    registry = NodeRegistry.get_instance()

    # Initialize Redis connection
    if await registry.initialize():
        # Register worker and supported node types
        await registry.register_worker()

        # Start heartbeat task
        await registry.start_heartbeat()

        app.ctx.node_registry = registry
        logger.info("✓ Node registry initialized and heartbeat started")
    else:
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
