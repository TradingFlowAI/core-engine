from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class SignalFormatField:
    """信号字段定义"""

    name: str  # 字段名称
    type: type  # 字段类型
    required: bool = True  # 是否必需
    description: str = ""  # 字段描述
    example: Any = None  # 示例值


class SignalFormats:
    """信号格式定义类，定义各种信号类型的标准字段"""

    # AI_RESPONSE 信号格式
    AI_RESPONSE = [
        SignalFormatField("model_name", str, True, "AI模型名称", "gpt-3.5-turbo"),
        SignalFormatField("response", str, True, "AI回复内容", "这是一段AI分析结果..."),
        SignalFormatField("input_signals_count", int, False, "输入信号数量", 3),
        SignalFormatField(
            "input_signal_types",
            List[str],
            False,
            "输入信号类型列表",
            ["PRICE_DATA", "MARKET_EVENT"],
        ),
    ]

    # PRICE_DATA 信号格式
    PRICE_DATA = [
        SignalFormatField("symbol", str, True, "交易对符号", "BTC/USDT"),
        SignalFormatField("timeframe", str, True, "时间周期", "1h"),
        SignalFormatField("current_price", float, True, "当前价格", 50000.0),
        SignalFormatField("open", float, True, "开盘价", 49800.0),
        SignalFormatField("high", float, True, "最高价", 50200.0),
        SignalFormatField("low", float, True, "最低价", 49700.0),
        SignalFormatField("close", float, True, "收盘价", 50000.0),
        SignalFormatField("volume", float, True, "交易量", 1234.56),
        SignalFormatField("timestamp", int, True, "时间戳", 1651825123),
        SignalFormatField("trend", str, False, "趋势", "上涨"),
        SignalFormatField(
            "support_levels", List[float], False, "支撑位", [49000, 48000]
        ),
        SignalFormatField(
            "resistance_levels", List[float], False, "阻力位", [51000, 52000]
        ),
    ]

    # DEX_TRADE 信号格式
    DEX_TRADE = [
        SignalFormatField("chain_id", str, True, "链ID", "eth"),
        SignalFormatField("exchange", str, True, "DEX交易所名称", "Uniswap"),
        SignalFormatField("trading_pair", str, True, "交易对", "ETH/USDT"),
        SignalFormatField(
            "token_address",
            str,
            True,
            "代币地址",
            "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
        ),
        SignalFormatField("action", str, True, "操作类型", "buy"),
        SignalFormatField("amount_to_swap", int, True, "交易数量", 10000),
        SignalFormatField("price", float, False, "限价", 3000.0),
        SignalFormatField("slippage", float, False, "允许滑点百分比", 0.5),
        SignalFormatField("reason", str, False, "交易理由", "突破阻力位后的追涨信号"),
    ]

    @classmethod
    def get_format(cls, signal_type: str) -> List[SignalFormatField]:
        """获取指定信号类型的格式定义"""
        signal_type = signal_type.upper()
        if hasattr(cls, signal_type):
            return getattr(cls, signal_type)
        return []

    @classmethod
    def validate(cls, signal_type: str, payload: Dict[str, Any]) -> tuple[bool, str]:
        """验证信号payload是否符合格式要求"""
        format_fields = cls.get_format(signal_type)
        if not format_fields:
            return True, ""  # 没有定义格式的信号类型，默认通过验证

        # 检查所有必需字段是否存在
        for field in format_fields:
            if field.required and field.name not in payload:
                return False, f"缺少必需字段: {field.name}"

            # 类型检查
            if field.name in payload and payload[field.name] is not None:
                # 检查列表类型
                if isinstance(field.type, type) and field.type == List:
                    if not isinstance(payload[field.name], list):
                        return False, f"字段 {field.name} 应该是列表类型"
                # 检查基本类型
                elif not isinstance(payload[field.name], field.type):
                    return (
                        False,
                        f"字段 {field.name} 类型不匹配，应为 {field.type.__name__}",
                    )

        return True, ""

    @classmethod
    def get_example_payload(cls, signal_type: str) -> Dict[str, Any]:
        """获取指定信号类型的示例payload"""
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
        """获取指定信号类型的格式描述文本"""
        format_fields = cls.get_format(signal_type)
        if not format_fields:
            return f"信号类型 {signal_type} 没有定义标准格式。"

        desc = f"{signal_type} 信号格式要求:\n"
        for field in format_fields:
            req = "必需" if field.required else "可选"
            desc += (
                f"- {field.name}: {field.type.__name__} ({req}) - {field.description}\n"
            )
            if field.example is not None:
                desc += f"  示例: {field.example}\n"
        return desc
