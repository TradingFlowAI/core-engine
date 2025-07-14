import argparse
import asyncio
import logging

from sanic import Sanic

from tradingflow.account_manager.api import (
    account_bp,
    aptos_vault_bp,
    evm_vault_bp,
    price_bp,
    token_bp,
    # vault_bp,
)
from tradingflow.account_manager.services import setup_services
from tradingflow.common.config import get_account_manager_config
from tradingflow.common.logging_config import setup_logging
from tradingflow.common.mq.dex_trade_signal_consumer import DexTradeSignalConsumer

# Setup logging
CONFIG = get_account_manager_config()
setup_logging(CONFIG)
logger = logging.getLogger("account_manager.server")

# Create Sanic application
app = Sanic("AccountManager")

# Read configuration from config file
ACCOUNT_MANAGER_HOST = CONFIG.get("ACCOUNT_MANAGER_HOST", "0.0.0.0")
ACCOUNT_MANAGER_PORT = CONFIG.get("ACCOUNT_MANAGER_PORT", 7001)
TRADE_WORKER_COUNT = CONFIG.get(
    "TRADE_WORKER_COUNT", 1
)  # Number of trade consumer instances
CHAIN_CONFIGS = CONFIG.get("CHAIN_CONFIGS")

# Register blueprints
app.blueprint(account_bp)
# app.blueprint(vault_bp)
app.blueprint(price_bp)
app.blueprint(token_bp)
app.blueprint(aptos_vault_bp)
app.blueprint(evm_vault_bp)

# Save trade signal consumer instances
app.ctx.trade_consumers = []


@app.listener("before_server_start")
async def setup_trade_signal_consumers(app, _):
    """Initialize trade signal consumers before server starts"""
    app.ctx.trade_consumers = []

    # Create specified number of consumer instances for each chain and DEX configuration
    for chain_config in CHAIN_CONFIGS:
        chain_id = chain_config.get("chain_id")
        dex_name = chain_config.get("dex_name")

        for i in range(TRADE_WORKER_COUNT):
            consumer = DexTradeSignalConsumer(
                chain_id=chain_id,
                dex_name=dex_name,
                rabbitmq_url=CONFIG.get("RABBITMQ_URL"),
            )

            try:
                await consumer.start()
                app.ctx.trade_consumers.append(consumer)
                logger.info(
                    "Trade signal consumer [%d/%d] started: chain_id=%s, dex_name=%s",
                    i + 1,
                    TRADE_WORKER_COUNT,
                    chain_id,
                    dex_name,
                )
            except Exception as e:
                logger.error("Failed to start trade signal consumer: %s", str(e))

    logger.info(
        "Successfully started %d trade signal consumers", len(app.ctx.trade_consumers)
    )


@app.listener("after_server_stop")
async def cleanup_trade_signal_consumers(app, _):
    """Close trade signal consumers after server stops"""
    if app.ctx.trade_consumers:
        stop_tasks = [consumer.stop() for consumer in app.ctx.trade_consumers]
        await asyncio.gather(*stop_tasks, return_exceptions=True)
        logger.info("Closed all trade signal consumers")
        app.ctx.trade_consumers = []


def init_app():
    """Initialize Sanic application"""
    # Setup services
    logger.info("Setting up Account Manager services...")
    setup_services(app)
    return app


if __name__ == "__main__":
    # Command line argument parsing
    parser = argparse.ArgumentParser(description="TradingFlow AccountManager Service")
    parser.add_argument(
        "--host", help="AccountManager service hostname", default=ACCOUNT_MANAGER_HOST
    )
    parser.add_argument(
        "--port",
        type=int,
        help="AccountManager service port",
        default=ACCOUNT_MANAGER_PORT,
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
