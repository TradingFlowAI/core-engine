#!/usr/bin/env python3

"""
测试动态 sqrt_price_limit 计算逻辑
"""

import math

def test_dynamic_sqrt_price_limit():
    """测试动态计算逻辑"""
    
    # 模拟池子数据
    pool_sqrt_price = 114938537987309159
    user_slippage_pct = 5.0  # 用户设置的5%滑点
    
    # 你的代币地址
    input_token = "0xa"  # APT
    output_token = "0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387"  # xBTC
    
    print("🧮 动态 sqrt_price_limit 计算:")
    print("=" * 50)
    
    # 1. 判断代币顺序 (Uniswap V3 规则: token0 < token1 按地址排序)
    if input_token < output_token:
        token0, token1 = input_token, output_token
        token0_name, token1_name = "APT", "xBTC"
        trade_direction = "token0 → token1"
        price_direction = "UP (价格上涨)"
        sqrt_direction = "应该设置上限 (+)"
    else:
        token0, token1 = output_token, input_token
        token0_name, token1_name = "xBTC", "APT" 
        trade_direction = "token1 → token0"
        price_direction = "DOWN (价格下跌)"
        sqrt_direction = "应该设置下限 (-)"
    
    print(f"📊 池子分析:")
    print(f"token0: {token0} ({token0_name})")
    print(f"token1: {token1} ({token1_name})")
    print(f"交易方向: {input_token} → {output_token} = {trade_direction}")
    print(f"价格变化: {price_direction}")
    print(f"sqrt_price_limit: {sqrt_direction}")
    
    # 2. 正确的滑点计算
    # 如果用户设置5%价格滑点，sqrt_price应该变化 sqrt(1±0.05)
    slippage_decimal = user_slippage_pct / 100  # 0.05
    
    if trade_direction == "token0 → token1":
        # 价格上涨，设置上限：sqrt_price_limit = current * sqrt(1 + slippage)
        sqrt_multiplier = math.sqrt(1 + slippage_decimal)
        sqrt_price_limit = int(pool_sqrt_price * sqrt_multiplier)
        direction_sign = "+"
    else:
        # 价格下跌，设置下限：sqrt_price_limit = current * sqrt(1 - slippage)  
        sqrt_multiplier = math.sqrt(1 - slippage_decimal)
        sqrt_price_limit = int(pool_sqrt_price * sqrt_multiplier)
        direction_sign = "-"
    
    print(f"\n🔢 滑点计算:")
    print(f"用户滑点: {user_slippage_pct}%")
    print(f"sqrt乘数: sqrt(1 {direction_sign} {slippage_decimal}) = {sqrt_multiplier:.6f}")
    print(f"当前sqrt_price: {pool_sqrt_price:,}")
    print(f"计算后限制: {sqrt_price_limit:,}")
    
    # 3. 验证价格变化
    X64_MULTIPLIER = 2**64
    
    def sqrt_to_price(sqrt_val):
        return (sqrt_val / X64_MULTIPLIER) ** 2
    
    current_price = sqrt_to_price(pool_sqrt_price)
    limit_price = sqrt_to_price(sqrt_price_limit)
    actual_price_change = (limit_price / current_price - 1) * 100
    
    print(f"\n✅ 价格验证:")
    print(f"当前价格: {current_price:.10f}")
    print(f"限制价格: {limit_price:.10f}")
    print(f"实际价格变化: {actual_price_change:+.2f}% (目标: {direction_sign}{user_slippage_pct}%)")
    
    return {
        'trade_direction': trade_direction,
        'sqrt_multiplier': sqrt_multiplier, 
        'sqrt_price_limit': sqrt_price_limit,
        'actual_price_change': actual_price_change
    }

def compare_calculation_methods():
    """对比不同计算方法"""
    print(f"\n🔍 对比计算方法:")
    print("=" * 40)
    
    pool_sqrt_price = 114938537987309159
    slippage_pct = 5.0
    
    # 方法1: 直接乘法 (错误)
    wrong_multiplier = 1 - (slippage_pct/100)  # 0.95
    wrong_result = int(pool_sqrt_price * wrong_multiplier)
    
    # 方法2: 正确的sqrt计算
    correct_multiplier = math.sqrt(1 - (slippage_pct/100))  # sqrt(0.95) ≈ 0.9747
    correct_result = int(pool_sqrt_price * correct_multiplier)
    
    print(f"❌ 错误方法 (直接-5%): {wrong_multiplier:.4f} → {wrong_result:,}")
    print(f"✅ 正确方法 (sqrt(0.95)): {correct_multiplier:.4f} → {correct_result:,}")
    print(f"差异: {abs(correct_result - wrong_result):,}")
    
    # 验证价格差异
    X64_MULTIPLIER = 2**64
    current_price = (pool_sqrt_price / X64_MULTIPLIER) ** 2
    wrong_price = (wrong_result / X64_MULTIPLIER) ** 2
    correct_price = (correct_result / X64_MULTIPLIER) ** 2
    
    wrong_change = (wrong_price / current_price - 1) * 100
    correct_change = (correct_price / current_price - 1) * 100
    
    print(f"\n价格变化对比:")
    print(f"❌ 错误方法: {wrong_change:+.2f}%")
    print(f"✅ 正确方法: {correct_change:+.2f}% (更接近-5%)")

if __name__ == "__main__":
    result = test_dynamic_sqrt_price_limit()
    compare_calculation_methods()
    
    print(f"\n🎯 实现要点:")
    print(f"1. 判断交易方向: {result['trade_direction']}")
    print(f"2. sqrt乘数: {result['sqrt_multiplier']:.6f}")
    print(f"3. 最终sqrt_price_limit: {result['sqrt_price_limit']:,}")
    print(f"4. 实际价格变化: {result['actual_price_change']:+.2f}%")
