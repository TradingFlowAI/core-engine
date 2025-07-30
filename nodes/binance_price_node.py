import asyncio
import logging
import traceback
from typing import Any, Dict, List, Optional

from binance.client import BaseClient, Client
from binance.exceptions import BinanceAPIException
from tradingflow.depot.python.config import CONFIG
from tradingflow.station.common.edge import Edge

from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

PRICE_DATA_HANDLE = "price_data"


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
    币安价格数据节点 - 用于获取指定交易对的K线数据

    输入参数:
    - symbol: 交易对符号，例如 'BTCUSDT'
    - interval: K线时间间隔，如 '1m', '5m', '1h', '1d'等
    - limit: 获取的K线数量，默认500，最大1000

    输出信号:
    - PRICE_DATA: 包含K线数据的信号
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
        初始化币安价格节点

        Args:
            flow_id: 流程ID
            cycle: 节点执行周期
            node_id: 节点唯一标识符
            name: 节点名称
            symbol: 交易对符号，例如 'BTCUSDT'
            interval: K线间隔，例如 '1m', '15m', '1h', '1d'
            limit: 获取的K线数量，1-1000
            api_key: 币安API Key，如果不提供则使用无需认证的公开接口
            api_secret: 币安API Secret
            **kwargs: 传递给基类的其他参数
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

        # 保存参数
        self.symbol = symbol.upper()  # 转换为大写
        self.interval = interval
        self.limit = min(1000, max(1, limit))  # 限制在1-1000之间
        self.api_key = CONFIG["BINANCE_API_KEY"]
        self.api_secret = CONFIG["BINANCE_API_SECRET"]

        # 币安客户端
        self.client = None

        # 日志设置
        self.logger = logging.getLogger(f"BinancePrice.{node_id}")

    async def initialize_client(self) -> bool:
        """初始化币安API客户端，带有重试机制"""
        max_retries = 3
        retry_count = 0
        base_delay = 1  # 基础延迟为1秒

        while retry_count <= max_retries:
            try:
                # 根据是否提供API凭证创建客户端
                if self.api_key and self.api_secret:
                    self.client = Client(
                        self.api_key,
                        self.api_secret,
                        base_endpoint=BaseClient.BASE_ENDPOINT_1,
                    )
                    self.logger.info("Binance client initialized with API key")
                else:
                    self.client = Client()
                    self.logger.info("Binance client initialized without API key")

                # 测试连接
                server_time = self.client.get_server_time()
                self.logger.info(f"Connected to Binance. Server time: {server_time}")
                return True

            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    # 计算指数退避延迟：1s, 2s, 4s
                    delay = base_delay * (2 ** (retry_count - 1))
                    self.logger.warning(
                        f"Failed to initialize Binance client (attempt {retry_count}/{max_retries}): {str(e)}. "
                        f"Retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(
                        f"Failed to initialize Binance client after {max_retries} attempts: {str(e)}"
                    )
                    await self.set_status(
                        NodeStatus.FAILED,
                        f"Binance client initialization failed after {max_retries} attempts: {str(e)}",
                    )
                    self.logger.debug(traceback.format_exc())
                    return False
        return False

    async def fetch_klines(self) -> Optional[List]:
        """
        获取K线数据

        Returns:
            List: K线数据列表，如果出错则返回None
        """
        if not self.client:
            self.logger.error("Binance client not initialized")
            return None

        try:
            # 使用线程池执行同步API调用
            loop = asyncio.get_event_loop()
            klines = await loop.run_in_executor(
                None,
                lambda: self.client.get_klines(
                    symbol=self.symbol, interval=self.interval, limit=self.limit,
                ),
            )

            self.logger.info(
                "Fetched %s klines for %s with interval %s",
                len(klines),
                self.symbol,
                self.interval,
            )
            self.logger.debug(
                "Fetched klines: %s", klines
            )  # 调试信息，实际使用中可以注释掉
            return klines
        except BinanceAPIException as e:
            self.logger.error("Binance API error: %s", e.message)
            return None
        except Exception as e:
            self.logger.error("Error fetching klines: %s", str(e))
            return None

    async def get_symbol_ticker(self) -> Optional[Dict]:
        # 使用线程池执行同步API调用
        loop = asyncio.get_event_loop()
        ticker = await loop.run_in_executor(
            None,
            lambda: self.client.get_symbol_ticker(symbol=self.symbol),
        )
        return ticker

    def process_klines(self, klines: List, ticker: Dict) -> Dict[str, Any]:
        """
        处理K线数据，转换为更易用的格式

        Args:
            klines: 从币安API获取的原始K线数据

        Returns:
            Dict: 处理后的K线数据
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
        执行节点逻辑，获取K线数据并发送信号，获取一次数据后即完成

        Returns:
            bool: 执行是否成功
        """
        try:
            self.logger.info(
                f"Executing BinancePriceNode for {self.symbol} with interval {self.interval}"
            )
            # 初始化客户端
            if not await self.initialize_client():
                return False

            await self.set_status(NodeStatus.RUNNING)

            # 获取K线数据（只获取一次）
            klines = await self.fetch_klines()
            ticker = await self.get_symbol_ticker()

            if klines and ticker:
                # 处理数据
                processed_data = self.process_klines(klines, ticker)

                # 发送数据信号
                if await self.send_signal(
                    PRICE_DATA_HANDLE, SignalType.PRICE_DATA, payload=processed_data
                ):
                    self.logger.info(
                        "Successfully sent price data signal for %s", self.symbol
                    )
                    await self.set_status(NodeStatus.COMPLETED)
                    return True
                else:
                    error_message = (
                        f"Failed to send price data signal for {self.symbol}"
                    )
                    self.logger.error(error_message)
                    await self.set_status(NodeStatus.FAILED, error_message)
                    return False
            else:
                error_message = f"Failed to fetch klines for {self.symbol}"
                self.logger.warning(error_message)
                await self.set_status(NodeStatus.FAILED, error_message)
                return False

        except asyncio.CancelledError:
            # 任务被取消
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_message = f"Error executing BinancePriceNode: {str(e)}"
            self.logger.error(error_message)
            await self.set_status(NodeStatus.FAILED, error_message)
            return False
        finally:
            # 清理资源
            if self.client:
                # 币安客户端没有明确的关闭方法，但可以设置为None
                self.client = None
