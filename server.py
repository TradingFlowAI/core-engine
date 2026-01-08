"""TradingFlow Worker Service Entry Point"""

import argparse
import logging

from sanic import Sanic


from infra.config import get_station_config
from infra.logging_config import setup_logging  # noqa: F401, E402
from infra.mq.pool_manager import pool_manager
from api import flow_bp, health_bp, node_bp, pause_bp, partial_run_bp
from common.node_registry import NodeRegistry
from common.node_task_manager import NodeTaskManager
from services import setup_services  # noqa: F401, E402
from publishers.activity_publisher import init_activity_publisher, get_activity_publisher

# Import nodes package to trigger @register_node decorators
import nodes  # noqa: F401

pool_manager.register_shutdown_handler()

CONFIG = get_station_config()
setup_logging(CONFIG, "station")
logger = logging.getLogger(__name__)

# Reduce log level for pika and asyncio to minimize DEBUG noise
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
app.blueprint(pause_bp)
app.blueprint(partial_run_bp)


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

    # 3. Initialize and restart Constant Nodes
    # 当新的 Station 镜像部署后，需要重启所有 Constant Nodes
    try:
        from common.constant_node_manager import ConstantNodeManager

        constant_manager = ConstantNodeManager.get_instance()
        if await constant_manager.initialize():
            app.ctx.constant_node_manager = constant_manager

            # 获取需要重启的 Constant Nodes
            all_nodes = await constant_manager.get_all_constant_nodes()
            if all_nodes:
                logger.info(f"Found {len(all_nodes)} Constant Nodes to restart after deployment")

                # 重启每个 Constant Node
                for node_info in all_nodes:
                    flow_id = node_info.get("flow_id")
                    node_id = node_info.get("node_id")
                    if flow_id and node_id:
                        try:
                            await constant_manager.restart_constant_node(flow_id, node_id)
                            logger.info(f"Restarted Constant Node: {flow_id}/{node_id}")
                        except Exception as e:
                            logger.warning(f"Failed to restart Constant Node {flow_id}/{node_id}: {e}")
            else:
                logger.info("No Constant Nodes to restart")

            logger.info("✓ Constant Node Manager initialized")
        else:
            logger.warning("Failed to initialize Constant Node Manager")
    except Exception as e:
        logger.warning(f"Error initializing Constant Node Manager: {e}")


@app.listener("after_server_stop")
async def cleanup_node_registry(app, loop):
    """Close node registry and constant node manager after server stops"""
    # 1. Stop Constant Nodes
    if hasattr(app.ctx, "constant_node_manager"):
        try:
            constant_manager = app.ctx.constant_node_manager
            await constant_manager.stop_all_local_nodes()
            await constant_manager.close()
            logger.info("Constant Node Manager has been closed")
        except Exception as e:
            logger.error("Error closing Constant Node Manager: %s", str(e))

    # 2. Close Node Registry
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
