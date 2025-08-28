import asyncio

from tradingflow.depot.python.config import CONFIG
from tradingflow.station.nodes.binance_price_node import BinancePriceNode


def test_integration():
    node = BinancePriceNode(
        node_id="binance_price_node",
        name="Binance Price Node",
        symbol="BTCUSDT",
        interval="1h",
        limit=10,
        api_key=CONFIG["BINANCE_API_KEY"],
        api_secret=CONFIG["BINANCE_API_SECRET"],
    )

    asyncio.run(node.execute())
