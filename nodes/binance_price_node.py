import asyncio
import traceback
from typing import Any, Dict, List, Optional

from binance.client import BaseClient, Client
from binance.exceptions import BinanceAPIException
from infra.config import CONFIG
from common.edge import Edge

from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus

# Output handle - single unified data output
DATA_HANDLE = "data"


@register_node_type(
    "binance_price_node",
    default_params={
        "symbol": "BTCUSDT",
        "interval": "1m",
        "limit": 100,
    },
)
class BinancePriceNode(NodeBase):
    """
    Binance Price Data Node - Fetches K-line data for specified trading pairs

    Input Parameters:
    - symbol: Trading pair symbol, e.g., 'BTCUSDT'
    - interval: K-line time interval, e.g., '1m', '5m', '1h', '1d'
    - limit: Number of K-lines to fetch, default 500, max 1000

    Output Signals:
    - PRICE_DATA: Signal containing K-line data
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        symbol: str = "BTCUSDT",
        interval: str = "1h",
        limit: int = 100,
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        """
        Initialize Binance price node

        Args:
            flow_id: Flow ID
            cycle: Node execution cycle
            node_id: Unique node identifier
            name: Node name
            symbol: Trading pair symbol, e.g., 'BTCUSDT'
            interval: K-line interval, e.g., '1m', '15m', '1h', '1d'
            limit: Number of K-lines to fetch, 1-1000
            api_key: Binance API Key, uses public interface if not provided
            api_secret: Binance API Secret
            **kwargs: Additional parameters passed to base class
        """
        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            input_edges=input_edges,
            output_edges=output_edges,
            state_store=state_store,
            **kwargs,
        )

        # Save parameters
        self.symbol = symbol.upper()  # Convert to uppercase
        self.interval = interval
        self.limit = min(1000, max(1, limit))  # Limit between 1-1000
        self.api_key = CONFIG["BINANCE_API_KEY"]
        self.api_secret = CONFIG["BINANCE_API_SECRET"]

        # Binance client
        self.client = None

    async def initialize_client(self) -> bool:
        """Initialize Binance API client with retry mechanism"""
        max_retries = 3
        retry_count = 0
        base_delay = 1  # Base delay of 1 second

        while retry_count <= max_retries:
            try:
                # Create client based on whether API credentials are provided
                if self.api_key and self.api_secret:
                    self.client = Client(
                        self.api_key,
                        self.api_secret,
                        base_endpoint=BaseClient.BASE_ENDPOINT_1,
                    )
                    await self.persist_log("Binance client initialized with API key")
                else:
                    self.client = Client()
                    await self.persist_log("Binance client initialized without API key")

                # Test connection
                server_time = self.client.get_server_time()
                await self.persist_log(f"Connected to Binance. Server time: {server_time}")
                return True

            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    # Calculate exponential backoff delay: 1s, 2s, 4s
                    delay = base_delay * (2 ** (retry_count - 1))
                    await self.persist_log(
                        f"Failed to initialize Binance client (attempt {retry_count}/{max_retries + 1}): {str(e)}. Retrying in {delay} seconds...",
                        log_level="WARNING"
                    )
                    await asyncio.sleep(delay)
                else:
                    await self.persist_log(
                        f"Failed to initialize Binance client after {max_retries + 1} attempts: {str(e)}",
                        log_level="ERROR"
                    )
                    await self.set_status(
                        NodeStatus.FAILED,
                        f"Binance client initialization failed after {max_retries} attempts: {str(e)}",
                    )
                    return False
        return False

    async def fetch_klines(self) -> Optional[List]:
        """
        Fetch K-line data

        Returns:
            List: K-line data list, returns None if error occurs
        """
        if not self.client:
            await self.persist_log("Binance client not initialized", log_level="ERROR")
            return None

        try:
            # Use thread pool to execute synchronous API call
            loop = asyncio.get_event_loop()
            klines = await loop.run_in_executor(
                None,
                lambda: self.client.get_klines(
                    symbol=self.symbol, interval=self.interval, limit=self.limit
                ),
            )
            await self.persist_log(
                f"Fetched {len(klines)} klines for {self.symbol} with interval {self.interval}",
                log_level="DEBUG"
            )
            await self.persist_log(
                f"Fetched klines: {klines}",  # Debug info, can be commented out in production
                log_level="DEBUG"
            )
            return klines
        except BinanceAPIException as e:
            await self.persist_log(f"Binance API error: {e.message}", log_level="ERROR")
            return None
        except Exception as e:
            await self.persist_log(f"Error fetching klines: {str(e)}", log_level="ERROR")
            return None

    async def get_symbol_ticker(self) -> Optional[Dict]:
        # Use thread pool to execute synchronous API call
        loop = asyncio.get_event_loop()
        ticker = await loop.run_in_executor(
            None,
            lambda: self.client.get_symbol_ticker(symbol=self.symbol),
        )
        return ticker

    def process_klines(self, klines: List, ticker: Dict) -> Dict[str, Any]:
        """
        Process K-line data, convert to more usable format

        Args:
            klines: Raw K-line data from Binance API

        Returns:
            Dict: Processed K-line data
        """
        processed_data = {
            "symbol": self.symbol,
            "interval": self.interval,
            "limit": self.limit,
            "header": [
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
            ],
            "kline_data": klines,
            "current_price": ticker["price"],
        }
        return processed_data

    async def execute(self) -> bool:
        """
        Execute node logic, fetch K-line data and send signal, completes after one data fetch

        Returns:
            bool: Whether execution was successful
        """
        try:
            await self.persist_log(
                f"Executing BinancePriceNode for {self.symbol} with interval {self.interval}"
            )
            # Initialize client
            if not await self.initialize_client():
                return False

            await self.set_status(NodeStatus.RUNNING)

            # Fetch K-line data (only once)
            klines = await self.fetch_klines()
            ticker = await self.get_symbol_ticker()

            if klines and ticker:
                # Process data
                processed_data = self.process_klines(klines, ticker)

                # Send unified data signal containing both current price and kline data
                if await self.send_signal(
                    DATA_HANDLE, SignalType.PRICE_DATA, payload=processed_data
                ):
                    await self.persist_log(
                        f"Successfully sent price data signal for {self.symbol}"
                    )
                    await self.set_status(NodeStatus.COMPLETED)
                    return True
                else:
                    error_message = (
                        f"Failed to send price data signal for {self.symbol}"
                    )
                    await self.persist_log(error_message, log_level="ERROR")
                    await self.set_status(NodeStatus.FAILED, error_message)
                    return False
            else:
                error_message = f"Failed to fetch klines for {self.symbol}"
                await self.persist_log(error_message, log_level="WARNING")
                await self.set_status(NodeStatus.FAILED, error_message)
                return False

        except asyncio.CancelledError:
            # Task was cancelled
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_message = f"Error executing BinancePriceNode: {str(e)}"
            await self.persist_log(error_message, log_level="ERROR")
            await self.set_status(NodeStatus.FAILED, error_message)
            return False
        finally:
            # Clean up resources
            if self.client:
                # Binance client doesn't have explicit close method, but can be set to None
                self.client = None

    def _register_input_handles(self) -> None:
        """Register input handles"""
        # Binance Price Node 没有动态输入句柄，所有参数通过构造函数传入
        pass

    def _register_output_handles(self) -> None:
        """Register output handles"""
        # Single unified data output containing both current price and kline data
        self.register_output_handle(
            name=DATA_HANDLE,
            data_type=dict,
            description="Data - Complete market data including current price and K-line data",
            example={
                "symbol": "BTCUSDT",
                "interval": "1h",
                "current_price": "50000.00",
                "kline_data": [[...]]
            },
        )
