"""
BinanceOperator — CEX operations for Binance.

Provides sync methods for CodeNode users:
- spot_order(): Place spot market/limit orders
- get_balance(): Query account balances
- get_price(): Get ticker prices
- get_open_orders(): List open orders
- cancel_order(): Cancel an order

Uses ccxt library for exchange connectivity.

Security:
- API Key/Secret are never in __repr__/__str__
- Credentials registered with log_sanitizer
"""

import logging
from typing import Any, Dict, List, Optional

from operators.base import BaseOperator
from common.log_sanitizer import register_sensitive_value

logger = logging.getLogger(__name__)


class BinanceOperator(BaseOperator):
    """
    Binance CEX operator.

    Exposes synchronous public methods for CodeNode compatibility.
    Uses ccxt for exchange API calls.
    """

    _operator_type = "binance"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: Optional[str] = None,
        exchange_id: str = "binance",
        sandbox: bool = False,
        loop=None,
    ):
        super().__init__(loop=loop)

        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._exchange_id = exchange_id

        # Register secrets with log sanitizer
        register_sensitive_value(api_key)
        register_sensitive_value(api_secret)
        if passphrase:
            register_sensitive_value(passphrase)

        # Display info (safe — only show masked key preview)
        key_preview = f"{api_key[:4]}****{api_key[-4:]}" if len(api_key) >= 8 else "****"
        self._display_info = f"exchange={exchange_id}, key={key_preview}"

        # Initialize ccxt exchange (lazy — created on first use)
        self._exchange = None
        self._sandbox = sandbox

    def _get_exchange(self):
        """Lazy-initialize the ccxt exchange instance."""
        if self._exchange is None:
            try:
                import ccxt
            except ImportError:
                raise ImportError(
                    "The 'ccxt' library is required for exchange operations. "
                    "Please install it: pip install ccxt"
                )

            exchange_class = getattr(ccxt, self._exchange_id, None)
            if exchange_class is None:
                raise ValueError(f"Unsupported exchange: {self._exchange_id}")

            config = {
                "apiKey": self._api_key,
                "secret": self._api_secret,
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
            }
            if self._passphrase:
                config["password"] = self._passphrase

            self._exchange = exchange_class(config)

            if self._sandbox:
                self._exchange.set_sandbox_mode(True)

        return self._exchange

    # ── Public sync API ──────────────────────────────────────────────
    # 注意：ccxt 同步版本的所有方法都是同步调用。CodeNode 的用户代码已在独立
    # 线程 (threading.Thread) 中执行，所以这里直接用同步调用即可。
    # 不要用 _run_async() 桥接，否则会阻塞 Sanic 的主事件循环。

    def spot_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str = "market",
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Place a spot order.

        Args:
            symbol: Trading pair (e.g., 'BTC/USDT').
            side: 'buy' or 'sell'.
            quantity: Order quantity in base currency.
            order_type: 'market' or 'limit'.
            price: Required for limit orders.

        Returns:
            Dict with order details (orderId, status, fills, etc.)
        """
        exchange = self._get_exchange()
        side = side.lower()
        order_type = order_type.lower()

        if order_type == "market":
            order = exchange.create_order(symbol, "market", side, quantity)
        elif order_type == "limit":
            if price is None:
                raise ValueError("Price is required for limit orders")
            order = exchange.create_order(symbol, "limit", side, quantity, price)
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

        return {
            "orderId": order.get("id"),
            "symbol": order.get("symbol"),
            "side": order.get("side"),
            "type": order.get("type"),
            "status": order.get("status"),
            "price": order.get("price"),
            "amount": order.get("amount"),
            "filled": order.get("filled"),
            "cost": order.get("cost"),
            "timestamp": order.get("timestamp"),
        }

    def get_balance(self, asset: Optional[str] = None) -> Any:
        """
        Get account balance.

        Args:
            asset: Specific asset symbol (e.g., 'USDT'). None for all.

        Returns:
            Dict with balance info. If asset specified, returns
            {'free': float, 'used': float, 'total': float}.
            If None, returns full balance dict.
        """
        exchange = self._get_exchange()
        balance = exchange.fetch_balance()

        if asset:
            asset = asset.upper()
            if asset in balance:
                return {
                    "free": balance[asset].get("free", 0),
                    "used": balance[asset].get("used", 0),
                    "total": balance[asset].get("total", 0),
                }
            else:
                return {"free": 0, "used": 0, "total": 0}

        # Return simplified balance (non-zero assets only)
        result = {}
        for key, val in balance.items():
            if isinstance(val, dict) and val.get("total", 0) > 0:
                result[key] = {
                    "free": val.get("free", 0),
                    "used": val.get("used", 0),
                    "total": val.get("total", 0),
                }
        return result

    def get_price(self, symbol: str) -> Dict[str, Any]:
        """
        Get latest ticker price.

        Args:
            symbol: Trading pair (e.g., 'BTC/USDT').

        Returns:
            Dict with 'last', 'bid', 'ask', 'high', 'low', 'volume', etc.
        """
        exchange = self._get_exchange()
        ticker = exchange.fetch_ticker(symbol)
        return {
            "symbol": ticker.get("symbol"),
            "last": ticker.get("last"),
            "bid": ticker.get("bid"),
            "ask": ticker.get("ask"),
            "high": ticker.get("high"),
            "low": ticker.get("low"),
            "volume": ticker.get("baseVolume"),
            "quoteVolume": ticker.get("quoteVolume"),
            "timestamp": ticker.get("timestamp"),
        }

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get open orders.

        Args:
            symbol: Trading pair. None for all symbols.

        Returns:
            List of order dicts.
        """
        exchange = self._get_exchange()
        orders = exchange.fetch_open_orders(symbol)
        return [
            {
                "orderId": o.get("id"),
                "symbol": o.get("symbol"),
                "side": o.get("side"),
                "type": o.get("type"),
                "price": o.get("price"),
                "amount": o.get("amount"),
                "filled": o.get("filled"),
                "status": o.get("status"),
                "timestamp": o.get("timestamp"),
            }
            for o in orders
        ]

    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Cancel an order.

        Args:
            order_id: The order ID to cancel.
            symbol: Trading pair.

        Returns:
            Dict with cancellation result.
        """
        exchange = self._get_exchange()
        result = exchange.cancel_order(order_id, symbol)
        return {
            "orderId": result.get("id"),
            "status": result.get("status", "cancelled"),
            "symbol": result.get("symbol"),
        }

    def get_markets(self) -> List[Dict[str, Any]]:
        """
        Get available trading markets.

        Returns:
            List of market info dicts.
        """
        exchange = self._get_exchange()
        exchange.load_markets()
        return [
            {
                "symbol": m["symbol"],
                "base": m["base"],
                "quote": m["quote"],
                "active": m.get("active", True),
            }
            for m in exchange.markets.values()
            if m.get("active", True)
        ]

    def get_klines(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 100,
    ) -> List:
        """
        Get K-line / OHLCV data.

        Args:
            symbol: Trading pair.
            timeframe: Candle timeframe ('1m', '5m', '15m', '1h', '4h', '1d').
            limit: Number of candles.

        Returns:
            List of [timestamp, open, high, low, close, volume] arrays.
        """
        exchange = self._get_exchange()
        return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
