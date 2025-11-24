import asyncio
import traceback
from typing import Any, Dict, List, Optional
import aiohttp

from weather_depot.config import CONFIG
from common.edge import Edge
from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus

# Output handle - single unified data output
DATA_HANDLE = "data"

# Input handles
SOURCE_HANDLE = "source"
DATA_TYPE_HANDLE = "data_type"
SYMBOL_HANDLE = "symbol"


@register_node_type(
    "price_node",
    default_params={
        "source": "coingecko",
        "data_type": "kline",
        "symbol": "bitcoin",
    },
)
class PriceNode(NodeBase):
    """
    Price Data Node - Fetches cryptocurrency price data from various sources

    Input Parameters:
    - source: Data source, currently supports 'coingecko'
    - data_type: Type of data to fetch - 'kline' for OHLC data or 'current_price' for current price only
    - symbol: Cryptocurrency symbol/ID (e.g., 'bitcoin', 'ethereum', 'aptos')

    Output Signals:
    - data: Signal containing price data (current price or OHLC kline data)

    Credits Cost:
    - Current Price: 2 credits
    - OHLC/Kline Data: 5 credits (more expensive due to larger data volume)
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        source: str = "coingecko",
        data_type: str = "kline",
        symbol: str = "bitcoin",
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        """
        Initialize Price node

        Args:
            flow_id: Flow ID
            cycle: Node execution cycle
            node_id: Unique node identifier
            name: Node name
            source: Data source (currently only 'coingecko' is supported)
            data_type: Type of data - 'kline' for OHLC data or 'current_price' for current price
            symbol: Cryptocurrency symbol/ID (e.g., 'bitcoin', 'ethereum')
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
        self.source = source.lower()
        self.data_type = data_type.lower()
        self.symbol = symbol.lower()  # CoinGecko uses lowercase IDs

        # Monitor service URL
        self.monitor_url = CONFIG.get("MONITOR_URL", "http://localhost:3000")

        # Validate parameters
        if self.source != "coingecko":
            raise ValueError(f"Unsupported data source: {self.source}. Currently only 'coingecko' is supported.")

        if self.data_type not in ["kline", "current_price"]:
            raise ValueError(f"Invalid data_type: {self.data_type}. Must be 'kline' or 'current_price'.")

    async def fetch_current_price(self) -> Optional[Dict]:
        """
        Fetch current price from monitor API

        Returns:
            Dict: Current price data, returns None if error occurs
        """
        try:
            url = f"{self.monitor_url}/api/v1/price/simple/{self.symbol}"
            params = {"vs_currency": "usd"}

            await self.persist_log(
                f"Fetching current price for {self.symbol} from {url}",
                log_level="DEBUG"
            )

            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get("success"):
                            data = result.get("data", {})
                            await self.persist_log(
                                f"Successfully fetched current price: ${data.get('current_price')}",
                                log_level="DEBUG"
                            )
                            return data
                        else:
                            await self.persist_log(
                                f"API returned success=false: {result.get('error', 'Unknown error')}",
                                log_level="ERROR"
                            )
                            return None
                    else:
                        error_text = await response.text()
                        await self.persist_log(
                            f"API request failed with status {response.status}: {error_text}",
                            log_level="ERROR"
                        )
                        return None

        except asyncio.TimeoutError:
            await self.persist_log(f"Timeout fetching current price for {self.symbol}", log_level="ERROR")
            return None
        except Exception as e:
            await self.persist_log(f"Error fetching current price: {str(e)}", log_level="ERROR")
            traceback.print_exc()
            return None

    async def fetch_ohlc_data(self, days: int = 30) -> Optional[Dict]:
        """
        Fetch OHLC (kline) data from monitor API

        Args:
            days: Number of days of historical data (default: 30)

        Returns:
            Dict: OHLC data, returns None if error occurs
        """
        try:
            url = f"{self.monitor_url}/api/price/ohlc/{self.symbol}"
            params = {
                "vs_currency": "usd",
                "days": days
            }

            await self.persist_log(
                f"Fetching OHLC data for {self.symbol} (last {days} days) from {url}",
                log_level="DEBUG"
            )

            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get("success"):
                            data = result.get("data", {})
                            ohlc_count = len(data.get("ohlc", []))
                            await self.persist_log(
                                f"Successfully fetched {ohlc_count} OHLC candles",
                                log_level="DEBUG"
                            )
                            return data
                        else:
                            await self.persist_log(
                                f"API returned success=false: {result.get('error', 'Unknown error')}",
                                log_level="ERROR"
                            )
                            return None
                    else:
                        error_text = await response.text()
                        await self.persist_log(
                            f"API request failed with status {response.status}: {error_text}",
                            log_level="ERROR"
                        )
                        return None

        except asyncio.TimeoutError:
            await self.persist_log(f"Timeout fetching OHLC data for {self.symbol}", log_level="ERROR")
            return None
        except Exception as e:
            await self.persist_log(f"Error fetching OHLC data: {str(e)}", log_level="ERROR")
            traceback.print_exc()
            return None

    def calculate_credits_cost(self) -> int:
        """
        Calculate credits cost based on data type

        Returns:
            int: Credits cost
        """
        if self.data_type == "current_price":
            return 2  # Lower cost for simple price query
        else:  # kline
            return 5  # Higher cost for OHLC data (larger dataset)

    async def execute(self) -> bool:
        """
        Execute node logic, fetch price data and send signal

        Returns:
            bool: Whether execution was successful
        """
        try:
            await self.persist_log(
                f"Executing PriceNode: source={self.source}, data_type={self.data_type}, symbol={self.symbol}"
            )

            await self.set_status(NodeStatus.RUNNING)

            # Fetch data based on data_type
            if self.data_type == "current_price":
                data = await self.fetch_current_price()
            else:  # kline
                data = await self.fetch_ohlc_data()

            if data:
                # Prepare output data
                processed_data = {
                    "source": self.source,
                    "data_type": self.data_type,
                    "symbol": self.symbol,
                    "data": data
                }

                # Calculate and deduct credits
                credits_cost = self.calculate_credits_cost()
                await self.persist_log(
                    f"Credits cost for this operation: {credits_cost} credits"
                )

                # TODO: Implement actual credits deduction through credits service
                # For now, just log the cost

                # Send unified data signal
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
                error_message = f"Failed to fetch {self.data_type} data for {self.symbol}"
                await self.persist_log(error_message, log_level="WARNING")
                await self.set_status(NodeStatus.FAILED, error_message)
                return False

        except asyncio.CancelledError:
            # Task was cancelled
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_message = f"Error executing PriceNode: {str(e)}"
            await self.persist_log(error_message, log_level="ERROR")
            traceback.print_exc()
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

    def _register_input_handles(self) -> None:
        """Register input handles"""
        self.register_input_handle(
            name=SOURCE_HANDLE,
            data_type=str,
            description="Data source (currently only 'coingecko' is supported)",
            example="coingecko",
        )

        self.register_input_handle(
            name=DATA_TYPE_HANDLE,
            data_type=str,
            description="Type of data to fetch - 'kline' for OHLC data or 'current_price' for current price",
            example="kline",
        )

        self.register_input_handle(
            name=SYMBOL_HANDLE,
            data_type=str,
            description="Cryptocurrency symbol/ID (e.g., 'bitcoin', 'ethereum', 'aptos')",
            example="bitcoin",
        )

    def _register_output_handles(self) -> None:
        """Register output handles"""
        # Single unified data output containing price data
        self.register_output_handle(
            name=DATA_HANDLE,
            data_type=dict,
            description="Data - Price data (current price or OHLC kline data)",
            example={
                "source": "coingecko",
                "data_type": "kline",
                "symbol": "bitcoin",
                "data": {
                    "coin_id": "bitcoin",
                    "vs_currency": "usd",
                    "ohlc": [[1700000000000, 35000, 36000, 34500, 35800]],
                    "count": 120
                }
            },
        )
