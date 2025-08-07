import asyncio
import random
from datetime import datetime
from typing import Any, Dict

from ...common.node_decorators import register_node_type
from ...common.signal_types import SignalType
from ..node_base import NodeBase, NodeStatus


# 添加装饰器
@register_node_type(
    "price_generator",
    default_params={
        "symbol": "BTC/USDT",
        "base_price": 50000.0,
        "volatility": 0.01,
        "interval": 1.0,
        "max_updates": 100,
    },
)
class PriceGenerator(NodeBase):
    """
    价格生成器节点

    该节点不需要输入信号，每秒自动生成一个价格更新信号。
    可用于模拟行情数据源、测试或演示目的。
    """

    def __init__(
        self,
        node_id: str,
        name: str,
        state_store=None,
        symbol: str = "BTC/USDT",
        base_price: float = 50000.0,
        volatility: float = 0.01,  # 价格波动率，百分比
        interval: float = 1.0,  # 价格更新间隔，秒
        max_updates: int = None,  # 最大更新次数，None表示无限
        message_queue_config: Dict[str, Any] = None,
        message_queue_type: str = "memory",
    ):
        """
        初始化价格生成器节点

        Args:
            node_id: 节点唯一标识符
            name: 节点名称
            symbol: 交易对符号
            base_price: 基础价格
            volatility: 价格波动率（百分比）
            interval: 价格更新间隔（秒）
            max_updates: 最大更新次数（None表示无限）
            message_queue_config: 消息队列配置
            message_queue_type: 消息队列类型
        """
        # 价格生成器不需要接收任何信号，但会产生价格更新信号
        required_signals = []
        output_signals = [SignalType.PRICE_UPDATE]

        super().__init__(
            node_id=node_id,
            name=name,
            required_signals=required_signals,
            message_queue_config=message_queue_config,
            message_queue_type=message_queue_type,
            output_signals=output_signals,
            state_store=state_store,
        )

        # 节点特定配置
        self.symbol = symbol
        self.current_price = base_price
        self.volatility = volatility
        self.interval = interval
        self.max_updates = max_updates
        self.running = False
        self.update_count = 0

    def _generate_next_price(self) -> float:
        """
        生成下一个价格

        使用随机游走模型生成有一定波动性的价格序列

        Returns:
            float: 下一个价格
        """
        # 计算价格变化的范围
        price_change_percent = random.uniform(-self.volatility, self.volatility)
        price_change = self.current_price * price_change_percent

        # 计算新价格
        new_price = self.current_price + price_change

        # 确保价格为正
        if new_price <= 0:
            new_price = self.current_price * (1 - self.volatility / 10)

        return round(new_price, 2)

    async def _send_price_update(self) -> bool:
        """
        发送价格更新信号

        Returns:
            bool: 是否发送成功
        """
        # 生成新价格
        self.current_price = self._generate_next_price()

        # 构建信号负载
        payload = {
            "symbol": self.symbol,
            "price": self.current_price,
            "timestamp": datetime.now().timestamp(),
        }

        await self.persist_log(f"Generated price update: {self.symbol} price: {self.current_price:.2f}", "INFO")

        # 发送价格更新信号
        result = await self.send_signal(SignalType.PRICE_UPDATE, payload)
        return result

    async def execute(self) -> bool:
        """
        执行节点逻辑：持续生成价格更新信号

        Returns:
            bool: 执行是否成功
        """
        try:
            await self.set_status(NodeStatus.RUNNING)
            self.logger.info(
                "启动价格生成器，交易对: %s, 初始价格: %.2f",
                self.symbol,
                self.current_price,
            )

            self.running = True
            self.update_count = 0

            # 持续生成价格，直到达到最大更新次数或被停止
            while self.running and (
                self.max_updates is None or self.update_count < self.max_updates
            ):
                if await self.is_terminated():
                    self.logger.info("检测到终止请求，优雅退出")
                    # 修复：设置状态为TERMINATED
                    await self.set_status(NodeStatus.TERMINATED)
                    return True

                # 发送价格更新
                if not await self._send_price_update():
                    self.logger.error("发送价格更新失败")
                    await self.set_status(NodeStatus.FAILED, "发送信号失败")
                    return False

                self.update_count += 1

                # 等待指定间隔
                terminated = await self.wait_for_termination(self.interval)
                if terminated:
                    self.logger.info("等待期间检测到终止请求，优雅退出")
                    # 修复：设置状态为TERMINATED
                    await self.set_status(NodeStatus.TERMINATED)
                    return True

            # 正常完成所有更新
            await self.set_status(NodeStatus.COMPLETED)
            return True

        except asyncio.CancelledError:
            self.logger.info("任务被取消，正在清理...")
            raise  # 重新抛出异常，让调用者知道任务被取消

        except Exception as e:
            error_msg = f"价格生成器执行失败: {str(e)}"
            self.logger.exception(error_msg)
            await self.set_status(NodeStatus.FAILED, error_msg)
            return False

    async def stop(self):
        """停止价格生成器"""
        self.running = False
        await super().stop()
