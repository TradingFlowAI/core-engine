import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from tradingflow.station.common.signal_types import Signal, SignalType
from tradingflow.station.nodes.examples.price_change_detector import PriceChangeDetector
from tradingflow.station.nodes.node_base import NodeStatus


@pytest.fixture
def detector():
    """创建PriceChangeDetector测试实例"""
    detector = PriceChangeDetector(
        node_id="test_detector",
        name="Test Price Change Detector",
        threshold_percent=2.0,
        check_interval=0.01,  # 使用更短的间隔以加快测试速度
    )

    # 模拟消息队列
    detector.message_queue = MagicMock()
    detector.message_queue.send_signal.return_value = True
    detector.message_queue.connect.return_value = True
    detector.message_queue.register_consumer.return_value = True
    detector.message_queue.start_consuming = MagicMock()

    # 清理信号队列
    detector.signal_queue = []

    # 重置上次价格
    detector.last_price = None
    detector.last_symbol = None

    # 替换wait_for_termination方法，用于控制测试流程
    detector.wait_for_termination = AsyncMock(return_value=False)

    # 替换is_terminated方法，避免测试中终止
    # 修正：使用AsyncMock而不是MagicMock，因为is_terminated是异步函数
    detector.is_terminated = AsyncMock(return_value=False)

    # 模拟set_status方法，避免实际状态存储操作
    detector.set_status = AsyncMock()

    return detector


def test_initialization(detector):
    """测试节点正确初始化"""
    assert detector.node_id == "test_detector"
    assert detector.name == "Test Price Change Detector"
    assert detector.threshold_percent == 2.0
    assert detector.status == NodeStatus.PENDING
    assert detector.required_signals == [SignalType.PRICE_UPDATE]
    assert detector.output_signals == [SignalType.PRICE_CHANGE_ALERT]
    assert detector.last_price is None


@pytest.mark.asyncio
async def test_process_first_price_update(detector):
    """测试处理首次价格更新但不发送警报的情况"""
    # 创建价格更新信号
    signal = Signal(
        signal_type=SignalType.PRICE_UPDATE,
        source_node_id="price_source",
        payload={
            "symbol": "BTC/USDT",
            "price": 30000.0,
            "timestamp": 1617278400.0,
        },
    )
    detector.signal_queue.append(signal)

    # 直接调用处理方法，而不是execute
    await detector._process_price_signals()

    # 验证结果
    assert detector.last_price == 30000.0
    assert detector.last_symbol == "BTC/USDT"
    detector.message_queue.send_signal.assert_not_called()
    assert len(detector.signal_queue) == 0  # 确认信号已被清理


@pytest.mark.asyncio
async def test_process_below_threshold_change(detector):
    """测试价格变化低于阈值的情况"""
    # 设置上次价格
    detector.last_price = 30000.0
    detector.last_symbol = "BTC/USDT"

    # 创建新价格信号，变化小于阈值
    signal = Signal(
        signal_type=SignalType.PRICE_UPDATE,
        source_node_id="price_source",
        payload={
            "symbol": "BTC/USDT",
            "price": 30500.0,  # 1.67% 的变化，低于2%阈值
            "timestamp": 1617278700.0,
        },
    )
    detector.signal_queue.append(signal)

    # 调用处理方法
    await detector._process_price_signals()

    # 验证结果
    assert detector.last_price == 30500.0
    detector.message_queue.send_signal.assert_not_called()


@pytest.mark.asyncio
async def test_process_above_threshold_change(detector):
    """测试价格变化高于阈值的情况"""
    # 设置上次价格
    detector.last_price = 30000.0
    detector.last_symbol = "BTC/USDT"

    # 创建新价格信号，变化大于阈值
    signal = Signal(
        signal_type=SignalType.PRICE_UPDATE,
        source_node_id="price_source",
        payload={
            "symbol": "BTC/USDT",
            "price": 31000.0,  # 3.33% 的变化，高于2%阈值
            "timestamp": 1617279000.0,
        },
    )
    detector.signal_queue.append(signal)

    # 调用处理方法
    await detector._process_price_signals()

    # 验证结果
    assert detector.last_price == 31000.0

    # 验证发送了正确的警报信号
    detector.message_queue.send_signal.assert_called_once()
    call_args = detector.message_queue.send_signal.call_args[0]
    signal_sent = call_args[0]

    assert signal_sent.type == SignalType.PRICE_CHANGE_ALERT
    assert signal_sent.source_node_id == "test_detector"
    assert signal_sent.payload["symbol"] == "BTC/USDT"
    assert signal_sent.payload["old_price"] == 30000.0
    assert signal_sent.payload["new_price"] == 31000.0
    assert abs(signal_sent.payload["change_percent"] - 3.33) < 0.01  # 近似比较


@pytest.mark.asyncio
async def test_process_incomplete_signal(detector):
    """测试信号数据不完整的情况"""
    # 创建不完整的价格信号（缺少价格字段）
    signal = Signal(
        signal_type=SignalType.PRICE_UPDATE,
        source_node_id="price_source",
        payload={
            "symbol": "BTC/USDT",
            # 缺少 price 字段
            "timestamp": 1617279300.0,
        },
    )
    detector.signal_queue.append(signal)

    # 调用处理方法
    await detector._process_price_signals()

    # 验证结果
    assert detector.last_price is None  # 不应该更新价格
    detector.message_queue.send_signal.assert_not_called()


@pytest.mark.asyncio
async def test_execute_with_termination(detector):
    """测试执行过程中的终止功能"""
    # 设置终止检查在第二次循环返回True
    detector.is_terminated.side_effect = [False, True]

    # 调用execute方法
    result = await detector.execute()

    # 验证结果
    assert result is True
    # 验证set_status被调用，状态设置为TERMINATED
    detector.set_status.assert_called_with(NodeStatus.TERMINATED)
    # 确认执行了两次is_terminated检查
    assert detector.is_terminated.call_count == 2
    # 确认至少进入了一个循环
    assert detector.wait_for_termination.call_count >= 1


@pytest.mark.asyncio
async def test_execute_with_termination_during_wait(detector):
    """测试等待期间收到终止信号的情况"""
    # 设置wait_for_termination第二次返回True
    detector.wait_for_termination.side_effect = [False, True]

    # 调用execute方法
    result = await detector.execute()

    # 验证结果
    assert result is True
    # 验证set_status被调用，状态设置为TERMINATED
    detector.set_status.assert_called_with(NodeStatus.TERMINATED)
    assert detector.wait_for_termination.call_count == 2


@pytest.mark.asyncio
async def test_execute_with_exception(detector):
    """测试执行过程中发生异常的情况"""
    # 让_process_price_signals抛出异常
    detector._process_price_signals = AsyncMock(side_effect=Exception("测试异常"))

    # 调用execute方法
    result = await detector.execute()

    # 验证结果
    assert result is False
    # 验证set_status被调用，状态设置为FAILED
    detector.set_status.assert_called_with(
        NodeStatus.FAILED, "价格变化检测失败: 测试异常"
    )
    assert detector.running is False


@pytest.mark.asyncio
async def test_execute_cancelled(detector):
    """测试任务被取消的情况"""
    # 设置第二次循环时抛出CancelledError
    detector._process_price_signals = AsyncMock(
        side_effect=[None, asyncio.CancelledError()]
    )

    with pytest.raises(asyncio.CancelledError):
        await detector.execute()

    # 验证结果
    assert detector.running is False
    assert detector._process_price_signals.call_count == 2


@pytest.mark.asyncio
async def test_process_symbol_change(detector):
    """测试交易对变更的情况"""
    # 设置上次价格和交易对
    detector.last_price = 30000.0
    detector.last_symbol = "BTC/USDT"

    # 创建不同交易对的价格信号
    signal = Signal(
        signal_type=SignalType.PRICE_UPDATE,
        source_node_id="price_source",
        payload={
            "symbol": "ETH/USDT",
            "price": 2000.0,
            "timestamp": 1617279600.0,
        },
    )
    detector.signal_queue.append(signal)

    # 调用处理方法
    await detector._process_price_signals()

    # 验证结果
    assert detector.last_symbol == "ETH/USDT"
    assert detector.last_price == 2000.0
    # 不应该发送警报，因为是新的交易对
    detector.message_queue.send_signal.assert_not_called()


@pytest.mark.asyncio
async def test_multiple_signals_processing(detector):
    """测试处理多个信号"""
    # 添加多个价格信号
    signals = [
        Signal(
            signal_type=SignalType.PRICE_UPDATE,
            source_node_id="price_source",
            payload={
                "symbol": "BTC/USDT",
                "price": 30000.0,
                "timestamp": 1617279900.0,
            },
        ),
        Signal(
            signal_type=SignalType.PRICE_UPDATE,
            source_node_id="price_source",
            payload={
                "symbol": "BTC/USDT",
                "price": 31000.0,  # >2% 变化
                "timestamp": 1617280200.0,
            },
        ),
        Signal(
            signal_type=SignalType.PRICE_UPDATE,
            source_node_id="price_source",
            payload={
                "symbol": "BTC/USDT",
                "price": 31500.0,  # <2% 变化
                "timestamp": 1617280500.0,
            },
        ),
    ]

    detector.signal_queue.extend(signals)

    # 调用处理方法
    await detector._process_price_signals()

    # 验证结果
    assert detector.last_price == 31000.0
    assert (
        detector.message_queue.send_signal.call_count == 1
    )  # 只应该发送一次警报(第一个到第二个的变化)
    assert len(detector.signal_queue) == 0  # 所有信号应该被处理
