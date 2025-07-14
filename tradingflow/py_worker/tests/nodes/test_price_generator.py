import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun import freeze_time
from tradingflow.py_worker.common.signal_types import SignalType
from tradingflow.py_worker.nodes.examples.price_generator import PriceGenerator
from tradingflow.py_worker.nodes.node_base import NodeStatus


@pytest.fixture
def price_generator():
    """创建PriceGenerator测试实例"""
    generator = PriceGenerator(
        node_id="test_generator",
        name="Test Price Generator",
        symbol="BTC/USDT",
        base_price=50000.0,
        volatility=0.01,
        interval=0.01,  # 使用短间隔加速测试
        max_updates=5,  # 限制更新次数以加速测试
    )

    # 模拟消息队列
    generator.message_queue = MagicMock()
    generator.message_queue.send_signal.return_value = True

    # 替换wait_for_termination方法，避免真正的等待
    generator.wait_for_termination = AsyncMock(return_value=False)

    # 替换is_terminated方法，默认不终止
    # 修改：使用AsyncMock替代MagicMock
    generator.is_terminated = AsyncMock(return_value=False)

    # 模拟set_status方法
    generator.set_status = AsyncMock()

    return generator


def test_initialization(price_generator):
    """测试节点初始化"""
    assert price_generator.node_id == "test_generator"
    assert price_generator.name == "Test Price Generator"
    assert price_generator.symbol == "BTC/USDT"
    assert price_generator.current_price == 50000.0
    assert price_generator.volatility == 0.01
    assert price_generator.interval == 0.01
    assert price_generator.max_updates == 5
    assert price_generator.running is False
    assert price_generator.update_count == 0
    assert price_generator.required_signals == []
    assert price_generator.output_signals == [SignalType.PRICE_UPDATE]


def test_generate_next_price(price_generator):
    """测试价格生成逻辑"""
    # 设置随机种子以获得可预测的结果
    with patch("random.uniform", return_value=0.005):  # 模拟0.5%的上涨
        next_price = price_generator._generate_next_price()
        # 预期价格 = 50000 + 50000 * 0.005 = 50250，然后四舍五入到小数点后两位
        assert next_price == 50250.0

    # 测试负价格变化
    with patch("random.uniform", return_value=-0.008):  # 模拟0.8%的下跌
        next_price = price_generator._generate_next_price()
        # 预期价格 = 50000 + 50000 * (-0.008) = 49600，然后四舍五入到小数点后两位
        assert next_price == 49600.0

    # 测试价格保护（防止价格为负）
    price_generator.current_price = 10.0
    with patch("random.uniform", return_value=-0.2):  # 模拟20%的下跌，超过当前价格
        next_price = price_generator._generate_next_price()
        # 应当触发价格保护逻辑
        assert next_price > 0


@pytest.mark.asyncio
@freeze_time("2023-04-01 12:00:00")
async def test_send_price_update(price_generator):
    """测试发送价格更新信号"""
    # 模拟价格生成
    with patch.object(price_generator, "_generate_next_price", return_value=51000.0):
        # 调用方法
        result = await price_generator._send_price_update()

        # 验证结果
        assert result is True
        assert price_generator.current_price == 51000.0

        # 验证发送信号
        price_generator.message_queue.send_signal.assert_called_once()

        # 验证信号内容
        call_args = price_generator.message_queue.send_signal.call_args[0]
        signal = call_args[0]

        assert signal.type == SignalType.PRICE_UPDATE
        assert signal.source_node_id == "test_generator"
        assert signal.payload["symbol"] == "BTC/USDT"
        assert signal.payload["price"] == 51000.0
        # 冻结时间，使得时间戳可预测
        assert (
            signal.payload["timestamp"] == 1680350400.0
        )  # 2023-04-01 12:00:00 的时间戳


@pytest.mark.asyncio
async def test_execute_normal_completion(price_generator):
    """测试正常执行直到达到最大更新次数"""
    # 模拟发送信号成功
    price_generator._send_price_update = AsyncMock(return_value=True)

    # 调用execute方法
    result = await price_generator.execute()

    # 验证结果
    assert result is True
    # 修改：验证set_status被调用，而不是直接检查status属性
    price_generator.set_status.assert_called_with(NodeStatus.COMPLETED)
    assert price_generator._send_price_update.call_count == 5  # 应执行5次更新
    assert price_generator.update_count == 5


@pytest.mark.asyncio
async def test_execute_with_termination_check(price_generator):
    """测试执行过程中通过is_terminated检测到终止"""
    # 设置第二次检查时返回True
    price_generator.is_terminated.side_effect = [False, True]
    price_generator._send_price_update = AsyncMock(return_value=True)

    # 调用execute方法
    result = await price_generator.execute()

    # 验证结果
    assert result is True
    assert price_generator._send_price_update.call_count == 1
    assert price_generator.is_terminated.call_count == 2
    # 修改：验证set_status被调用，而不是直接检查status属性
    price_generator.set_status.assert_called_with(NodeStatus.TERMINATED)


@pytest.mark.asyncio
async def test_execute_with_termination_during_wait(price_generator):
    """测试执行过程中在wait_for_termination期间终止"""
    # 设置第一次发送后，在等待期间终止
    price_generator._send_price_update = AsyncMock(return_value=True)
    price_generator.wait_for_termination = AsyncMock(return_value=True)

    # 调用execute方法
    result = await price_generator.execute()

    # 验证结果
    assert result is True
    assert price_generator._send_price_update.call_count == 1
    assert price_generator.update_count == 1
    price_generator.wait_for_termination.assert_called_once_with(0.01)
    price_generator.set_status.assert_called_with(NodeStatus.TERMINATED)


@pytest.mark.asyncio
async def test_execute_send_signal_failure(price_generator):
    """测试发送信号失败的情况"""
    # 设置发送信号失败
    price_generator._send_price_update = AsyncMock(return_value=False)

    # 调用execute方法
    result = await price_generator.execute()

    # 验证结果
    assert result is False
    # 修改：验证set_status被调用并检查错误消息
    price_generator.set_status.assert_called_with(NodeStatus.FAILED, "发送信号失败")
    assert price_generator._send_price_update.call_count == 1


@pytest.mark.asyncio
async def test_execute_exception(price_generator):
    """测试执行过程中遇到异常的情况"""
    # 设置抛出异常
    expected_error = "价格生成器执行失败: 测试异常"
    price_generator._send_price_update = AsyncMock(side_effect=ValueError("测试异常"))

    # 调用execute方法
    result = await price_generator.execute()

    # 验证结果
    assert result is False
    # 修改：验证set_status被调用并检查错误消息
    price_generator.set_status.assert_called_with(NodeStatus.FAILED, expected_error)


@pytest.mark.asyncio
async def test_execute_cancellation(price_generator):
    """测试取消执行的情况"""
    # 设置取消异常
    price_generator._send_price_update = AsyncMock(
        side_effect=[True, asyncio.CancelledError()]
    )

    # 调用execute方法并期望抛出CancelledError
    with pytest.raises(asyncio.CancelledError):
        await price_generator.execute()

    # 验证结果
    assert price_generator._send_price_update.call_count == 2
    assert price_generator.update_count == 1


@pytest.mark.asyncio
async def test_stop(price_generator):
    """测试停止方法"""
    # 设置初始状态
    price_generator.running = True

    # 调用stop方法
    await price_generator.stop()

    # 验证结果
    assert price_generator.running is False
    price_generator.message_queue.stop_consuming.assert_called_once()


@pytest.mark.asyncio
async def test_unlimited_updates_with_early_termination(price_generator):
    """测试无限更新模式但提前终止的情况"""
    # 设置无限更新
    price_generator.max_updates = None

    # 模拟发送信号成功，但在第3次检查时终止
    price_generator._send_price_update = AsyncMock(return_value=True)
    price_generator.is_terminated.side_effect = [False, False, True]

    # 调用execute方法
    result = await price_generator.execute()

    # 验证结果
    assert result is True
    assert price_generator._send_price_update.call_count == 2
    assert price_generator.update_count == 2


@pytest.mark.asyncio
async def test_volatility_affects_price_range(price_generator):
    """测试不同波动率对价格范围的影响"""
    # 设置两种不同波动率的生成器
    low_volatility_generator = PriceGenerator(
        node_id="low_vol_gen",
        name="Low Volatility Generator",
        base_price=50000.0,
        volatility=0.001,  # 0.1%波动
        interval=0.01,
        max_updates=100,
    )

    high_volatility_generator = PriceGenerator(
        node_id="high_vol_gen",
        name="High Volatility Generator",
        base_price=50000.0,
        volatility=0.05,  # 5%波动
        interval=0.01,
        max_updates=100,
    )

    # 使用真实随机生成100个价格点
    low_vol_prices = []
    high_vol_prices = []

    for _ in range(100):
        low_vol_prices.append(low_volatility_generator._generate_next_price())
        low_volatility_generator.current_price = low_vol_prices[-1]

        high_vol_prices.append(high_volatility_generator._generate_next_price())
        high_volatility_generator.current_price = high_vol_prices[-1]

    # 计算波动范围
    low_vol_range = max(low_vol_prices) - min(low_vol_prices)
    high_vol_range = max(high_vol_prices) - min(high_vol_prices)

    # 高波动率应该产生更大的价格范围
    assert high_vol_range > low_vol_range * 5  # 考虑到随机性，使用宽松的比较


# 使用示例
@pytest.mark.asyncio
async def test_run_example_integration():
    # 创建价格生成器节点
    generator = PriceGenerator(
        node_id="btc_price_generator",
        name="比特币价格生成器",
        symbol="BTC/USDT",
        base_price=50000.0,
        volatility=0.005,  # 0.5%价格波动
        interval=1.0,  # 每秒更新一次
        max_updates=5,  # 运行1分钟
    )

    # 初始化消息队列
    if not generator.initialize_message_queue():
        print("初始化消息队列失败")
        return

    # 创建价格变化检测器
    from tradingflow.py_worker.nodes.examples.price_change_detector import PriceChangeDetector

    detector = PriceChangeDetector(
        node_id="btc_change_detector",
        name="比特币价格变化检测器",
        threshold_percent=0.5,  # 0.5%的价格变化阈值
    )

    # 启动检测器
    await detector.start()

    # 运行生成器
    await generator.execute()

    # 停止节点
    await generator.stop()
    await detector.stop()

    # 验证生成器和检测器的状态， 只做demo不做断言
    # assert generator.status == NodeStatus.TERMINATED
    # assert detector.status == NodeStatus.TERMINATED


if __name__ == "__main__":
    pytest.main(["-xvs", "test_price_generator.py"])
