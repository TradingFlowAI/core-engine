#!/usr/bin/env python3

"""
分析 sqrt_price_limit 应该加还是减滑点
"""

def analyze_sqrt_price_direction():
    """分析交易方向和sqrt_price_limit的设置"""
    
    # 你的池子数据
    pool_sqrt_price = 114938537987309159
    slippage_pct = 5.0  # 5% 滑点
    
    print("🧪 分析 sqrt_price_limit 的设置方向:")
    print("=" * 60)
    print(f"池子当前 sqrtPrice: {pool_sqrt_price:,}")
    print(f"滑点设置: {slippage_pct}%")
    
    # 计算不同的 sqrt_price_limit 选项
    slippage_multiplier = slippage_pct / 100
    
    # 选项1: 当前价格 + 滑点 (允许价格上涨)
    limit_higher = int(pool_sqrt_price * (1 + slippage_multiplier))
    
    # 选项2: 当前价格 - 滑点 (允许价格下跌)  
    limit_lower = int(pool_sqrt_price * (1 - slippage_multiplier))
    
    # 选项3: 当前价格 (精确匹配)
    limit_exact = pool_sqrt_price
    
    # 选项4: 稍微高一点 (1% 缓冲)
    limit_buffer_up = int(pool_sqrt_price * 1.01)
    
    # 选项5: 稍微低一点 (1% 缓冲)
    limit_buffer_down = int(pool_sqrt_price * 0.99)
    
    print(f"\n📊 不同的 sqrt_price_limit 选项:")
    print(f"1. 当前价格 + 5%:  {limit_higher:,}")
    print(f"2. 当前价格 - 5%:  {limit_lower:,}")
    print(f"3. 当前价格 (精确): {limit_exact:,}")
    print(f"4. 当前价格 + 1%:  {limit_buffer_up:,}")
    print(f"5. 当前价格 - 1%:  {limit_buffer_down:,}")
    
    print(f"\n💡 理论分析:")
    print(f"在 Uniswap V3 中，sqrt_price_limit 用于限制价格滑点:")
    print(f"- 如果我们是 买入 output token (token0 -> token1)")
    print(f"  price 上涨对我们不利，所以 sqrt_price_limit 应该是 上限")
    print(f"- 如果我们是 卖出 input token (token1 -> token0)")  
    print(f"  price 下跌对我们不利，所以 sqrt_price_limit 应该是 下限")
    
    print(f"\n🔍 推荐测试顺序:")
    test_cases = [
        ("当前价格 + 1%", limit_buffer_up),
        ("当前价格 - 1%", limit_buffer_down), 
        ("当前价格 + 5%", limit_higher),
        ("当前价格 - 5%", limit_lower),
        ("当前价格精确", limit_exact)
    ]
    
    for i, (desc, value) in enumerate(test_cases, 1):
        print(f"{i}. {desc}: {value}")
    
    return test_cases

if __name__ == "__main__":
    test_cases = analyze_sqrt_price_direction()
    print(f"\n🎯 建议先试: 当前价格 + 1% = {test_cases[0][1]}")
