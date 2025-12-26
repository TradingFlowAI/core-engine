from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class SignalFormatField:
    """Signal field definition"""

    name: str  # Field name
    type: type  # Field type
    required: bool = True  # Whether required
    description: str = ""  # Field description
    example: Any = None  # Example value


class SignalFormats:
    """Signal format definition class, defines standard fields for various signal types"""

    # AI_RESPONSE signal format
    AI_RESPONSE = [
        SignalFormatField("model_name", str, True, "AI model name", "gpt-3.5-turbo"),
        SignalFormatField("response", str, True, "AI response content", "This is an AI analysis result..."),
        SignalFormatField("input_signals_count", int, False, "Input signals count", 3),
        SignalFormatField(
            "input_signal_types",
            List[str],
            False,
            "Input signal types list",
            ["PRICE_DATA", "MARKET_EVENT"],
        ),
    ]

    # PRICE_DATA signal format
    PRICE_DATA = [
        SignalFormatField("symbol", str, True, "Trading pair symbol", "BTC/USDT"),
        SignalFormatField("timeframe", str, True, "Timeframe", "1h"),
        SignalFormatField("current_price", float, True, "Current price", 50000.0),
        SignalFormatField("open", float, True, "Open price", 49800.0),
        SignalFormatField("high", float, True, "High price", 50200.0),
        SignalFormatField("low", float, True, "Low price", 49700.0),
        SignalFormatField("close", float, True, "Close price", 50000.0),
        SignalFormatField("volume", float, True, "Volume", 1234.56),
        SignalFormatField("timestamp", int, True, "Timestamp", 1651825123),
        SignalFormatField("trend", str, False, "Trend", "bullish"),
        SignalFormatField(
            "support_levels", List[float], False, "Support levels", [49000, 48000]
        ),
        SignalFormatField(
            "resistance_levels", List[float], False, "Resistance levels", [51000, 52000]
        ),
    ]

    # DEX_TRADE signal format
    DEX_TRADE = [
        SignalFormatField("chain_id", str, True, "Chain ID", "eth"),
        SignalFormatField("exchange", str, True, "DEX exchange name", "Uniswap"),
        SignalFormatField("trading_pair", str, True, "Trading pair", "ETH/USDT"),
        SignalFormatField(
            "token_address",
            str,
            True,
            "Token address",
            "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
        ),
        SignalFormatField("action", str, True, "Action type", "buy"),
        SignalFormatField("amount_to_swap", int, True, "Swap amount", 10000),
        SignalFormatField("price", float, False, "Limit price", 3000.0),
        SignalFormatField("slippage", float, False, "Slippage tolerance percentage", 0.5),
        SignalFormatField("reason", str, False, "Trade reason", "Breakout signal after resistance"),
    ]

    # JSON_DATA signal format (generic JSON payload, no enforced fields)
    JSON_DATA = []

    @classmethod
    def get_format(cls, signal_type: str) -> List[SignalFormatField]:
        """Get format definition for specified signal type."""
        signal_type = signal_type.upper()
        if hasattr(cls, signal_type):
            return getattr(cls, signal_type)
        return []

    @classmethod
    def validate(cls, signal_type: str, payload: Dict[str, Any]) -> tuple[bool, str]:
        """Validate if signal payload matches format requirements."""
        format_fields = cls.get_format(signal_type)
        if not format_fields:
            return True, ""  # Signal types without defined format pass validation by default

        # Check if all required fields exist
        for field in format_fields:
            if field.required and field.name not in payload:
                return False, f"Missing required field: {field.name}"

            # Type check
            if field.name in payload and payload[field.name] is not None:
                # Check list type
                if isinstance(field.type, type) and field.type == List:
                    if not isinstance(payload[field.name], list):
                        return False, f"Field {field.name} should be list type"
                # Check basic types
                elif not isinstance(payload[field.name], field.type):
                    return (
                        False,
                        f"Field {field.name} type mismatch, expected {field.type.__name__}",
                    )

        return True, ""

    @classmethod
    def get_example_payload(cls, signal_type: str) -> Dict[str, Any]:
        """Get example payload for specified signal type."""
        format_fields = cls.get_format(signal_type)
        if not format_fields:
            return {}

        example = {}
        for field in format_fields:
            if field.example is not None:
                example[field.name] = field.example
        return example

    @classmethod
    def get_format_description(cls, signal_type: str) -> str:
        """Get format description text for specified signal type."""
        format_fields = cls.get_format(signal_type)
        if not format_fields:
            return f"Signal type {signal_type} has no defined standard format."

        desc = f"{signal_type} signal format requirements:\n"
        for field in format_fields:
            req = "required" if field.required else "optional"
            desc += (
                f"- {field.name}: {field.type.__name__} ({req}) - {field.description}\n"
            )
            if field.example is not None:
                desc += f"  Example: {field.example}\n"
        return desc
