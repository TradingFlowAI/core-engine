"""
Candleline Node - Interactive Chart Node

显示 K 线图表，允许用户直接在图表上绘制各种技术分析图形。
本节点仅负责数据展示和收集用户绘图，分析逻辑应由后续的 AI Model Node 处理。

输入:
- price_data: 从 Price Node 接收的数据，格式:
  {
    "source": "coingecko",
    "data_type": "kline",
    "symbol": "bitcoin",
    "data": {
      "coin_id": "bitcoin",
      "vs_currency": "usd",
      "ohlc": [[timestamp, open, high, low, close], ...],
      "count": 120
    }
  }

输出 (单一 data output):
- data: 完整的图表数据，格式:
  {
    "symbol": "bitcoin",
    "timeframe": "auto",  # 根据数据粒度推断
    "current_price": 45000.0,
    "price_stats": {...},
    "ohlc": [...],
    "drawings": [...],
    "drawings_analysis": {...},
    "description": "..."
  }

支持的绘图工具:
- 水平线 (Horizontal Line)
- 趋势线 (Trend Line)
- 平行通道 (Parallel Channel)
- 斐波那契回撤 (Fibonacci Retracement)
- 矩形区域 (Rectangle)
- 椭圆/圆形 (Ellipse)
- 箭头 (Arrow)
- 文字标注 (Text Annotation)
- 价格区间 (Price Range)

特点:
- 非阻塞：持续显示价格数据
- 可交互：用户可以直接在图表上绘图
- 数据透传：将价格数据和绘图信息传递给下游节点
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from common.edge import Edge
from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus


# Input handles
PRICE_DATA_HANDLE = "price_data"  # 从 Price Node 接收的完整数据

# Output handles - 单一输出
DATA_HANDLE = "data"  # 完整的图表输出（价格 + 绘图 + 分析）


@register_node_type(
    "candleline_node",
    default_params={},
)
class CandlelineNode(NodeBase):
    """
    Candleline Node - K 线图表与绘图工具

    接收 Price Node 输出的数据，显示 K 线图表，收集用户绘图信息。
    symbol 和 timeframe 从输入数据中自动提取。

    Input:
    - price_data: Price Node 输出的完整数据（包含 symbol, ohlc 等）

    Output:
    - data: 完整的图表数据（价格 + 用户绘图 + 语义分析）

    Credits Cost: 3 credits
    """

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        # 从 kwargs 中移除 node_type 避免重复参数
        kwargs.pop("node_type", None)
        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            input_edges=input_edges,
            output_edges=output_edges,
            state_store=state_store,
            node_type="candleline_node",
            **kwargs,
        )

        # 从输入数据中动态提取
        self.symbol: str = ""
        self.timeframe: str = "auto"
        self.drawings: List[Dict] = []
        
        # 存储接收到的价格数据 - 用于 auto_update_attr
        self.price_data: Optional[Dict] = None

    def _register_input_handles(self) -> None:
        """注册 input handles"""
        self.register_input_handle(
            name=PRICE_DATA_HANDLE,
            data_type=dict,
            description="从 Price Node 接收的完整数据（包含 symbol, ohlc 等）",
            example={"source": "coingecko", "symbol": "bitcoin", "data": {"ohlc": []}},
            auto_update_attr="price_data",  # 指定接收信号时自动更新的成员变量
        )

    async def execute(self) -> bool:
        """
        执行 Candleline Node 逻辑

        1. 接收 Price Node 输出的数据
        2. 从数据中提取 symbol、timeframe
        3. 加载用户绘图
        4. 发布图表更新到前端
        5. 输出完整数据供下游分析

        Returns:
            bool: 是否执行成功
        """
        try:
            await self.set_status(NodeStatus.RUNNING)

            # 使用 get_input_handle_data 获取价格数据（来自 Price Node）
            price_data = self.get_input_handle_data(PRICE_DATA_HANDLE)

            if price_data is None:
                await self.persist_log("No price data received", "WARNING")
                await self.set_status(NodeStatus.COMPLETED)
                return True

            # 从 Price Node 输出中提取 symbol
            self.symbol = self._extract_symbol(price_data)
            
            # 从数据粒度推断 timeframe
            self.timeframe = self._infer_timeframe(price_data)

            await self.persist_log(
                f"Executing CandlelineNode: symbol={self.symbol}, timeframe={self.timeframe}"
            )

            # 加载用户绘图
            await self._load_drawings()

            # 处理价格数据
            current_price = self._extract_current_price(price_data)
            ohlcv_data = self._extract_ohlcv(price_data)

            await self.persist_log(
                f"Price data received: {self.symbol} @ {current_price}, "
                f"{len(ohlcv_data)} candles, {len(self.drawings)} drawings"
            )

            # 发布图表更新到前端
            await self._publish_chart_update(ohlcv_data, current_price)

            # 构建完整的图表输出
            chart_output = self._build_chart_output(price_data, current_price, ohlcv_data)

            # 发送单一 data 输出（供 AI Model 分析）
            await self.send_signal(
                DATA_HANDLE,
                SignalType.ANY,
                payload=chart_output,
            )
            await self.persist_log("Chart data sent for downstream analysis")

            await self.set_status(NodeStatus.COMPLETED)
            return True

        except Exception as e:
            error_message = f"CandlelineNode execution failed: {str(e)}"
            await self.persist_log(error_message, "ERROR")
            await self.set_status(NodeStatus.FAILED, error_message)
            return False

    def _extract_symbol(self, price_data: Any) -> str:
        """从 Price Node 输出中提取 symbol"""
        try:
            if isinstance(price_data, dict):
                # Price Node 输出格式: {"source": "...", "symbol": "bitcoin", "data": {...}}
                if "symbol" in price_data:
                    return str(price_data["symbol"]).upper()
                if "data" in price_data and isinstance(price_data["data"], dict):
                    if "coin_id" in price_data["data"]:
                        return str(price_data["data"]["coin_id"]).upper()
        except Exception:
            pass
        return "UNKNOWN"

    def _infer_timeframe(self, price_data: Any) -> str:
        """根据数据粒度推断 timeframe"""
        try:
            ohlcv = self._extract_ohlcv(price_data)
            if len(ohlcv) >= 2:
                # 计算相邻蜡烛的时间差
                t1 = ohlcv[0].get("time", 0)
                t2 = ohlcv[1].get("time", 0)
                
                # 如果是毫秒时间戳，转换为秒
                if t1 > 1e12:
                    t1 = t1 / 1000
                if t2 > 1e12:
                    t2 = t2 / 1000
                
                diff_seconds = abs(t2 - t1)
                
                # 根据时间差推断周期
                if diff_seconds < 120:  # < 2 min
                    return "1m"
                elif diff_seconds < 600:  # < 10 min
                    return "5m"
                elif diff_seconds < 1800:  # < 30 min
                    return "15m"
                elif diff_seconds < 3600:  # < 1 hour
                    return "30m"
                elif diff_seconds < 14400:  # < 4 hours
                    return "1h"
                elif diff_seconds < 86400:  # < 1 day
                    return "4h"
                elif diff_seconds < 604800:  # < 1 week
                    return "1d"
                else:
                    return "1w"
        except Exception:
            pass
        return "auto"

    def _build_chart_output(
        self,
        price_data: Any,
        current_price: Optional[float],
        ohlcv_data: List[Dict],
    ) -> Dict[str, Any]:
        """
        构建完整的图表输出

        这个输出设计用于连接到 AI Model Node，包含：
        1. 市场数据（价格、趋势）
        2. 原始 OHLC 数据
        3. 用户绘图信息（支撑/阻力线、趋势线等）
        4. 绘图的语义化描述

        Returns:
            Dict: 完整的图表数据
        """
        # 分析绘图
        drawings_analysis = self._analyze_drawings(current_price)

        # 计算基本技术指标
        price_stats = self._calculate_price_stats(ohlcv_data, current_price)

        return {
            # 基本信息
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "current_price": current_price,
            "timestamp": datetime.now(timezone.utc).isoformat(),

            # 价格统计
            "price_stats": price_stats,

            # 原始 OHLC 数据（供下游节点使用）
            "ohlc": ohlcv_data,
            "ohlc_count": len(ohlcv_data),

            # 用户绘图（原始数据）
            "drawings": self.drawings,
            "drawings_count": len(self.drawings),

            # 绘图分析（语义化）
            "drawings_analysis": drawings_analysis,

            # 自然语言描述（供 AI 理解）
            "description": self._generate_description(
                current_price, price_stats, drawings_analysis
            ),
        }

    def _analyze_drawings(self, current_price: Optional[float]) -> Dict[str, Any]:
        """
        分析用户绘图，生成语义化信息
        
        支持的绘图类型:
        - horizontal_line: 水平线（支撑/阻力）
        - trend_line: 趋势线
        - parallel_channel: 平行通道
        - fibonacci: 斐波那契回撤
        - rectangle: 矩形区域
        - ellipse: 圆/椭圆（时间/价格区间）
        - arrow: 箭头（方向指示）
        - text: 文字标注
        - price_range: 价格范围
        """
        analysis = {
            "horizontal_lines": [],
            "trend_lines": [],
            "parallel_channels": [],
            "fibonacci_levels": [],
            "rectangles": [],
            "ellipses": [],
            "arrows": [],
            "price_ranges": [],
            "annotations": [],
            "summary": [],
        }

        if not current_price:
            return analysis

        for drawing in self.drawings:
            drawing_type = drawing.get("type", "")

            if drawing_type == "horizontal_line":
                price = drawing.get("price", 0)
                distance = current_price - price
                distance_pct = (distance / price * 100) if price else 0
                position = "above" if distance > 0 else "below"

                line_info = {
                    "id": drawing.get("id"),
                    "price": price,
                    "label": drawing.get("label", ""),
                    "distance": round(distance, 4),
                    "distance_pct": round(distance_pct, 2),
                    "position": position,
                }
                analysis["horizontal_lines"].append(line_info)

                if distance_pct < 0:
                    analysis["summary"].append(
                        f"Price is {abs(distance_pct):.1f}% below horizontal line at ${price:.2f}"
                    )
                else:
                    analysis["summary"].append(
                        f"Price is {distance_pct:.1f}% above horizontal line at ${price:.2f}"
                    )

            elif drawing_type == "trend_line":
                start_price = drawing.get("startPrice", 0)
                end_price = drawing.get("endPrice", 0)
                start_time = drawing.get("startTime", 0)
                end_time = drawing.get("endTime", 0)
                slope = "ascending" if end_price > start_price else "descending"
                extended = drawing.get("extended", False)

                analysis["trend_lines"].append({
                    "id": drawing.get("id"),
                    "start_time": start_time,
                    "start_price": start_price,
                    "end_time": end_time,
                    "end_price": end_price,
                    "slope": slope,
                    "extended": extended,
                })
                analysis["summary"].append(
                    f"Trend line: {slope} from ${start_price:.2f} to ${end_price:.2f}"
                )

            elif drawing_type == "parallel_channel":
                start_price = drawing.get("startPrice", 0)
                end_price = drawing.get("endPrice", 0)
                channel_width = drawing.get("channelWidth", 0)
                
                # 计算通道边界
                main_high = max(start_price, end_price)
                main_low = min(start_price, end_price)
                channel_high = main_high + channel_width
                channel_low = main_low
                
                in_channel = channel_low <= current_price <= channel_high

                analysis["parallel_channels"].append({
                    "id": drawing.get("id"),
                    "main_line_start": start_price,
                    "main_line_end": end_price,
                    "channel_width": channel_width,
                    "channel_high": channel_high,
                    "channel_low": channel_low,
                    "price_in_channel": in_channel,
                })
                status = "inside" if in_channel else "outside"
                analysis["summary"].append(
                    f"Parallel channel ${channel_low:.2f} - ${channel_high:.2f} (currently {status})"
                )

            elif drawing_type == "fibonacci":
                start_price = drawing.get("startPrice", 0)
                end_price = drawing.get("endPrice", 0)
                high = max(start_price, end_price)
                low = min(start_price, end_price)
                diff = high - low

                levels = {}
                nearest_level = None
                nearest_distance = float('inf')
                
                for level in drawing.get("levels", [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1]):
                    level_price = high - (diff * level)
                    levels[f"{level * 100:.1f}%"] = round(level_price, 4)
                    
                    # 找最近的斐波那契水平
                    dist = abs(current_price - level_price)
                    if dist < nearest_distance:
                        nearest_distance = dist
                        nearest_level = f"{level * 100:.1f}%"

                analysis["fibonacci_levels"].append({
                    "id": drawing.get("id"),
                    "high": high,
                    "low": low,
                    "levels": levels,
                    "nearest_level": nearest_level,
                    "nearest_level_distance": round(nearest_distance, 4),
                })
                analysis["summary"].append(
                    f"Fibonacci ${low:.2f} - ${high:.2f}, nearest level: {nearest_level}"
                )

            elif drawing_type == "rectangle":
                start_price = drawing.get("startPrice", 0)
                end_price = drawing.get("endPrice", 0)
                start_time = drawing.get("startTime", 0)
                end_time = drawing.get("endTime", 0)
                high = max(start_price, end_price)
                low = min(start_price, end_price)

                in_zone = low <= current_price <= high

                analysis["rectangles"].append({
                    "id": drawing.get("id"),
                    "start_time": start_time,
                    "end_time": end_time,
                    "high": high,
                    "low": low,
                    "price_in_zone": in_zone,
                })
                zone_status = "inside" if in_zone else "outside"
                analysis["summary"].append(
                    f"Rectangle zone ${low:.2f} - ${high:.2f} (currently {zone_status})"
                )

            elif drawing_type == "ellipse":
                center_time = drawing.get("centerTime", 0)
                center_price = drawing.get("centerPrice", 0)
                radius_time = drawing.get("radiusTime", 0)
                radius_price = drawing.get("radiusPrice", 0)
                
                # 计算价格是否在椭圆的价格范围内
                price_low = center_price - radius_price
                price_high = center_price + radius_price
                in_price_range = price_low <= current_price <= price_high

                analysis["ellipses"].append({
                    "id": drawing.get("id"),
                    "center_time": center_time,
                    "center_price": center_price,
                    "radius_time": radius_time,
                    "radius_price": radius_price,
                    "price_range_low": price_low,
                    "price_range_high": price_high,
                    "price_in_range": in_price_range,
                })
                status = "in range" if in_price_range else "out of range"
                analysis["summary"].append(
                    f"Ellipse zone around ${center_price:.2f} ±${radius_price:.2f} (currently {status})"
                )

            elif drawing_type == "arrow":
                start_price = drawing.get("startPrice", 0)
                end_price = drawing.get("endPrice", 0)
                direction = "up" if end_price > start_price else "down"

                analysis["arrows"].append({
                    "id": drawing.get("id"),
                    "start_price": start_price,
                    "end_price": end_price,
                    "direction": direction,
                })
                analysis["summary"].append(
                    f"Arrow pointing {direction} from ${start_price:.2f} to ${end_price:.2f}"
                )

            elif drawing_type == "price_range":
                start_price = drawing.get("startPrice", 0)
                end_price = drawing.get("endPrice", 0)
                high = max(start_price, end_price)
                low = min(start_price, end_price)
                range_size = high - low
                range_pct = (range_size / low * 100) if low else 0
                
                in_range = low <= current_price <= high

                analysis["price_ranges"].append({
                    "id": drawing.get("id"),
                    "high": high,
                    "low": low,
                    "range_size": range_size,
                    "range_pct": round(range_pct, 2),
                    "price_in_range": in_range,
                })
                status = "inside" if in_range else "outside"
                analysis["summary"].append(
                    f"Price range ${low:.2f} - ${high:.2f} ({range_pct:.1f}%, currently {status})"
                )

            elif drawing_type == "text":
                text = drawing.get("text", "")
                price = drawing.get("price", 0)
                
                analysis["annotations"].append({
                    "id": drawing.get("id"),
                    "text": text,
                    "price": price,
                    "time": drawing.get("time", 0),
                })
                if text:
                    analysis["summary"].append(f"Note at ${price:.2f}: \"{text}\"")

        return analysis

    def _calculate_price_stats(
        self, ohlcv_data: List[Dict], current_price: Optional[float]
    ) -> Dict[str, Any]:
        """计算基本价格统计"""
        if not ohlcv_data or not current_price:
            return {}

        try:
            closes = [float(d.get("close", 0)) for d in ohlcv_data if d.get("close")]
            highs = [float(d.get("high", 0)) for d in ohlcv_data if d.get("high")]
            lows = [float(d.get("low", 0)) for d in ohlcv_data if d.get("low")]

            if not closes:
                return {}

            first_close = closes[0]
            change = current_price - first_close
            change_pct = (change / first_close * 100) if first_close else 0

            return {
                "period_high": max(highs) if highs else None,
                "period_low": min(lows) if lows else None,
                "period_open": first_close,
                "price_change": round(change, 4),
                "price_change_pct": round(change_pct, 2),
                "candles_count": len(ohlcv_data),
            }
        except Exception:
            return {}

    def _generate_description(
        self,
        current_price: Optional[float],
        price_stats: Dict,
        drawings_analysis: Dict,
    ) -> str:
        """生成自然语言描述"""
        parts = []

        # 价格信息
        if current_price:
            parts.append(f"{self.symbol} is trading at ${current_price:.4f}")

        # 价格变化
        if price_stats.get("price_change_pct") is not None:
            change_pct = price_stats["price_change_pct"]
            direction = "up" if change_pct > 0 else "down"
            parts.append(f"Price is {direction} {abs(change_pct):.2f}% in this period")

        # 绘图摘要
        summaries = drawings_analysis.get("summary", [])
        if summaries:
            parts.append("User has marked: " + "; ".join(summaries[:5]))

        return ". ".join(parts) + "." if parts else "No data available."

    def _extract_current_price(self, price_data: Any) -> Optional[float]:
        """从价格数据中提取当前价格"""
        try:
            if isinstance(price_data, dict):
                if "data" in price_data and isinstance(price_data["data"], dict):
                    data = price_data["data"]
                    if "current_price" in data:
                        return float(data["current_price"])
                    if "prices" in data and data["prices"]:
                        return float(data["prices"][-1][1])
                if "close" in price_data:
                    return float(price_data["close"])
                if "c" in price_data:
                    return float(price_data["c"])
            elif isinstance(price_data, (list, tuple)):
                if price_data and isinstance(price_data[-1], (list, tuple)):
                    return float(price_data[-1][4])
            elif isinstance(price_data, (int, float)):
                return float(price_data)
        except (ValueError, TypeError, IndexError):
            pass
        return None

    def _extract_ohlcv(self, price_data: Any) -> List[Dict]:
        """从价格数据中提取 OHLCV 数据"""
        ohlcv_list = []

        try:
            if isinstance(price_data, dict):
                if "data" in price_data:
                    data = price_data["data"]
                    if isinstance(data, dict) and "ohlc" in data:
                        return data["ohlc"]
                    if isinstance(data, list):
                        return data
            elif isinstance(price_data, list):
                for item in price_data:
                    if isinstance(item, dict):
                        ohlcv_list.append(item)
                    elif isinstance(item, (list, tuple)) and len(item) >= 5:
                        ohlcv_list.append({
                            "time": item[0],
                            "open": item[1],
                            "high": item[2],
                            "low": item[3],
                            "close": item[4],
                            "volume": item[5] if len(item) > 5 else 0,
                        })
        except Exception:
            pass

        return ohlcv_list

    async def _load_drawings(self) -> None:
        """从 Redis 加载用户绘图"""
        try:
            if self.state_store and hasattr(self.state_store, "redis_client"):
                key = f"candleline:drawings:{self.flow_id}:{self.node_id}"
                data = await self.state_store.redis_client.get(key)
                if data:
                    self.drawings = json.loads(data)
        except Exception as e:
            await self.persist_log(f"Error loading drawings: {e}", "WARNING")

    async def _save_drawings(self) -> None:
        """保存绘图到 Redis"""
        try:
            if self.state_store and hasattr(self.state_store, "redis_client"):
                key = f"candleline:drawings:{self.flow_id}:{self.node_id}"
                await self.state_store.redis_client.set(
                    key,
                    json.dumps(self.drawings),
                    ex=86400 * 30,  # 30 天过期
                )
        except Exception as e:
            await self.persist_log(f"Error saving drawings: {e}", "WARNING")

    async def update_drawings(self, drawings: List[Dict]) -> None:
        """
        更新绘图（由前端调用）

        Args:
            drawings: 新的绘图列表
        """
        self.drawings = drawings
        await self._save_drawings()
        await self.persist_log(f"Drawings updated: {len(drawings)} items")

    async def _publish_chart_update(
        self,
        ohlcv_data: List[Dict],
        current_price: Optional[float],
    ) -> None:
        """发布图表更新到 Redis Pub/Sub"""
        try:
            if self.state_store and hasattr(self.state_store, "redis_client"):
                update_event = {
                    "type": "candleline_update",
                    "flow_id": self.flow_id,
                    "cycle": self.cycle,
                    "node_id": self.node_id,
                    "symbol": self.symbol,
                    "timeframe": self.timeframe,
                    "current_price": current_price,
                    "ohlcv": ohlcv_data[-100:] if len(ohlcv_data) > 100 else ohlcv_data,
                    "drawings": self.drawings,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                channel = f"interaction:flow:{self.flow_id}"
                await self.state_store.redis_client.publish(
                    channel, json.dumps(update_event, default=str)
                )

        except Exception as e:
            await self.persist_log(f"Error publishing chart update: {e}", "WARNING")

    def calculate_credits_cost(self) -> int:
        """Candleline Node 的 credits 消耗"""
        return 3
