import asyncio
from typing import Any, Dict

from ...common.node_decorators import register_node_type
from ...common.signal_types import Signal, SignalType
from ..node_base import NodeBase, NodeStatus


# 添加装饰器
@register_node_type("price_change_detector", default_params={"threshold_percent": 0.5})
class PriceChangeDetector(NodeBase):
    """
    价格变化检测节点

    该节点接收价格更新信号，当价格变化超过阈值时发送价格变化信号并结束执行
    """

    def __init__(
        self,
        node_id: str,
        name: str,
        state_store=None,
        threshold_percent: float = 2.0,
        check_interval: float = 0.5,  # 检查信号队列的间隔（秒）
        message_queue_config: Dict[str, Any] = None,
        message_queue_type: str = "memory",
        max_wait_time: float = 300.0,  # 最大等待时间（秒），默认5分钟
    ):
        """
        初始化价格变化检测节点

        Args:
            node_id: 节点唯一标识符
            name: 节点名称
            threshold_percent: 价格变化触发阈值（百分比）
            check_interval: 检查信号队列的间隔（秒）
            message_queue_config: 消息队列配置
            message_queue_type: 消息队列类型
            max_wait_time: 最大等待时间，超过此时间未检测到显著变化则结束执行
        """
        # 定义该节点需要接收的信号类型和可能产生的信号类型
        required_signals = [SignalType.PRICE_UPDATE]
        output_signals = [SignalType.PRICE_CHANGE_ALERT]

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
        self.threshold_percent = threshold_percent
        self.check_interval = check_interval
        self.max_wait_time = max_wait_time
        self.last_price = None
        self.last_symbol = None
        self.running = False
        self.significant_change_detected = False
        self.start_time = None

    async def execute(self) -> bool:
        """
        执行节点逻辑：监控价格变化并在超过阈值时发送信号并结束

        Returns:
            bool: 执行是否成功
        """
        try:
            await self.set_status(NodeStatus.RUNNING)
            self.logger.info(
                "开始监控价格变化，阈值：%s%%，检测到一次显著变化后将结束",
                self.threshold_percent,
            )
            self.running = True
            self.start_time = asyncio.get_event_loop().time()

            # 注册当前任务，以便能够取消
            self._current_task = asyncio.current_task()

            # 监控循环
            while self.running:
                # 检查是否收到终止信号
                if await self.is_terminated():
                    self.logger.info("接收到终止信号，停止价格监控")
                    await self.set_status(NodeStatus.TERMINATED)
                    return True

                # 处理队列中的价格信号
                change_detected = await self._process_price_signals()

                # 如果检测到显著价格变化，结束执行
                if change_detected:
                    self.logger.info("已检测到显著价格变化，完成任务")
                    await self.set_status(NodeStatus.COMPLETED)
                    return True

                # 检查是否超过最大等待时间
                current_time = asyncio.get_event_loop().time()
                elapsed_time = current_time - self.start_time
                if elapsed_time > self.max_wait_time:
                    self.logger.info(
                        "已达到最大等待时间 %.1f 秒，未检测到显著价格变化，结束监控",
                        self.max_wait_time,
                    )
                    await self.set_status(NodeStatus.COMPLETED)
                    return True

                # 等待一段时间，同时监听终止事件
                terminated = await self.wait_for_termination(self.check_interval)
                if terminated:
                    self.logger.info("等待期间接收到终止信号，停止价格监控")
                    await self.set_status(NodeStatus.TERMINATED)
                    return True

            await self.set_status(NodeStatus.COMPLETED)
            return True

        except asyncio.CancelledError:
            self.logger.info("价格监控任务被取消，正在清理...")
            self.running = False
            raise  # 重新抛出异常，让调用者知道任务被取消

        except Exception as e:
            error_msg = f"价格变化检测失败: {str(e)}"
            self.logger.exception(error_msg)
            await self.set_status(NodeStatus.FAILED, error_msg)
            self.running = False
            return False

    async def _process_price_signals(self) -> bool:
        """
        处理队列中的所有价格信号

        Returns:
            bool: 是否检测到显著价格变化
        """
        # 从信号队列中找到价格更新信号
        price_signals = [
            s for s in self.signal_queue if s.type == SignalType.PRICE_UPDATE
        ]

        if not price_signals:
            return False  # 没有价格信号，等待下一个检查周期

        # 按接收顺序处理每一个价格信号，如果检测到显著变化则立即返回
        for signal in price_signals:
            if await self._process_single_price_signal(signal):
                # 清理队列
                self.signal_queue = []
                return True

        # 清理已处理的价格信号
        self.signal_queue = [
            s for s in self.signal_queue if s.type != SignalType.PRICE_UPDATE
        ]

        return False

    async def _process_single_price_signal(self, signal: Signal) -> bool:
        """
        处理单个价格信号

        Args:
            signal: 价格更新信号

        Returns:
            bool: 是否检测到显著价格变化
        """
        # 提取信号数据
        current_price = signal.payload.get("price")
        symbol = signal.payload.get("symbol")
        timestamp = signal.payload.get("timestamp")

        # 验证数据完整性
        if not current_price or not symbol:
            self.logger.warning("价格信号缺少必要数据，忽略此信号")
            return False

        # 如果是新的交易对，重置价格比较
        if self.last_symbol and self.last_symbol != symbol:
            self.logger.info(
                "检测到交易对变更: %s -> %s，重置价格比较", self.last_symbol, symbol
            )
            self.last_price = None

        self.last_symbol = symbol

        # 检查价格变化
        if self.last_price is not None:
            change_percent = ((current_price - self.last_price) / self.last_price) * 100

            self.logger.info(
                "交易对: %s, 上次价格: %.2f, 当前价格: %.2f, 变化: %.2f%%",
                symbol,
                self.last_price,
                current_price,
                change_percent,
            )

            # 如果价格变化超过阈值，发送警报信号
            if abs(change_percent) >= self.threshold_percent:
                self.logger.info(
                    "检测到显著价格变化: %.2f%%，任务将结束", change_percent
                )

                # 构建信号负载
                alert_payload = {
                    "symbol": symbol,
                    "old_price": self.last_price,
                    "new_price": current_price,
                    "change_percent": change_percent,
                    "timestamp": timestamp,
                }

                # 发送价格变化警报信号
                if not await self.send_signal(
                    SignalType.PRICE_CHANGE_ALERT, alert_payload
                ):
                    self.logger.error("发送价格变化警报信号失败")
                    return False

                # 更新最近价格
                self.last_price = current_price

                # 标记检测到显著变化，准备结束执行
                self.significant_change_detected = True
                return True
        else:
            self.logger.info("首次接收到 %s 价格: %.2f", symbol, current_price)

        # 更新最近价格
        self.last_price = current_price
        return False

    async def stop(self):
        """停止价格监控"""
        self.logger.info("停止价格变化检测器")
        self.running = False
        await super().stop()


# 使用示例
async def run_example():
    # 创建价格变化检测节点
    detector = PriceChangeDetector(
        node_id="price_change_detector_001",
        name="BTC价格变化检测器",
        threshold_percent=1.5,  # 1.5%的价格变化阈值
        check_interval=0.5,  # 每0.5秒检查一次信号队列
        max_wait_time=300.0,  # 最多等待5分钟
    )

    # 初始化消息队列
    if not detector.initialize_message_queue():
        print("初始化消息队列失败")
        return

    # 初始化状态存储
    if not await detector.initialize_state_store():
        print("初始化状态存储失败")
        return

    # 运行检测器
    try:
        await detector.execute()
    except KeyboardInterrupt:
        print("手动终止监控")
    finally:
        await detector.stop()


if __name__ == "__main__":
    asyncio.run(run_example())
