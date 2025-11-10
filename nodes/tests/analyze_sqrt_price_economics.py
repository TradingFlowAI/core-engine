#!/usr/bin/env python3

"""
分析 sqrt_price 的实际价格含义和交易方向
"""

def analyze_sqrt_price_economics():
    """分析 sqrt_price 的经济学含义"""
    
    # 你的池子数据
    pool_sqrt_price = 114938537987309159
    minus_5_percent = int(pool_sqrt_price * 0.95)  # 109191611087943696
    plus_5_percent = int(pool_sqrt_price * 1.05)   # 120685464886674608
    
    print("🧮 SqrtPrice 到实际价格的转换 (X64格式):")
    print("=" * 60)
    
    # X64格式转换 (2^64)
    X64_MULTIPLIER = 2**64  # 18446744073709551616
    
    def sqrt_price_to_price(sqrt_price_x64):
        """将 sqrt_price (X64格式) 转换为实际价格"""
        # sqrt_price_x64 = sqrt(price) * 2^64
        # 所以 price = (sqrt_price_x64 / 2^64)^2
        sqrt_price_decimal = sqrt_price_x64 / X64_MULTIPLIER
        actual_price = sqrt_price_decimal ** 2
        return actual_price, sqrt_price_decimal
    
    # 计算实际价格
    current_price, current_sqrt = sqrt_price_to_price(pool_sqrt_price)
    minus5_price, minus5_sqrt = sqrt_price_to_price(minus_5_percent)  
    plus5_price, plus5_sqrt = sqrt_price_to_price(plus_5_percent)
    
    print(f"📊 价格分析:")
    print(f"当前 sqrt_price: {pool_sqrt_price:,}")
    print(f"  -> sqrt值: {current_sqrt:.10f}")
    print(f"  -> 实际价格: {current_price:.15f}")
    
    print(f"\n-5% sqrt_price: {minus_5_percent:,}")
    print(f"  -> sqrt值: {minus5_sqrt:.10f}")
    print(f"  -> 实际价格: {minus5_price:.15f}")
    print(f"  -> 价格变化: {((minus5_price/current_price-1)*100):+.2f}%")
    
    print(f"\n+5% sqrt_price: {plus_5_percent:,}")
    print(f"  -> sqrt值: {plus5_sqrt:.10f}")
    print(f"  -> 实际价格: {plus5_price:.15f}")
    print(f"  -> 价格变化: {((plus5_price/current_price-1)*100):+.2f}%")
    
    print(f"\n🔍 关键发现:")
    print(f"- sqrt_price 变化 ±5% → 实际价格变化 约±10%")
    print(f"- 这是因为 price = sqrt_price²，所以变化被平方放大")
    
    # 分析交易方向
    print(f"\n🔄 交易方向分析:")
    print(f"根据你的代币地址:")
    print(f"  input_token:  0xa (APT)")
    print(f"  output_token: 0x81214a80...87 (xBTC)")
    
    print(f"\n💡 为什么 -5% 能工作而 +5% 不行？")
    print(f"在 Uniswap V3 中，sqrt_price_limit 的作用:")
    print(f"1. 如果是 token0 → token1 (价格上涨)")
    print(f"   sqrt_price_limit 应该是 上限 (防止价格涨太多)")
    print(f"2. 如果是 token1 → token0 (价格下跌)")  
    print(f"   sqrt_price_limit 应该是 下限 (防止价格跌太多)")
    
    # 判断交易方向
    input_addr = "0xa"
    output_addr = "0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387"
    
    print(f"\n🎯 你的交易分析:")
    print(f"APT → xBTC 交易")
    print(f"- 这可能是 token1 → token0 的方向 (因为APT地址更小)")
    print(f"- 所以需要设置 下限 来防止价格跌太多")
    print(f"- 这就是为什么 -5% (更低的价格) 能工作")
    print(f"- 而 +5% (更高的价格) 被拒绝，因为方向错误")
    
    return {
        'current_price': current_price,
        'minus5_price': minus5_price,
        'plus5_price': plus5_price,
        'price_change_minus5': (minus5_price/current_price-1)*100,
        'price_change_plus5': (plus5_price/current_price-1)*100
    }

def analyze_token_order():
    """分析代币顺序和价格报价"""
    print(f"\n📝 代币顺序分析:")
    print(f"=" * 40)
    
    input_addr = "0xa"
    output_addr = "0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387"
    
    # 在Uniswap中，token0 < token1 (按地址排序)
    if input_addr < output_addr:
        token0, token1 = input_addr, output_addr
        token0_symbol, token1_symbol = "APT", "xBTC"
        swap_direction = "token0 → token1"
        price_goes = "UP (价格上涨)"
        limit_should_be = "上限 (max price)"
    else:
        token0, token1 = output_addr, input_addr  
        token0_symbol, token1_symbol = "xBTC", "APT"
        swap_direction = "token1 → token0"
        price_goes = "DOWN (价格下跌)"
        limit_should_be = "下限 (min price)"
    
    print(f"token0: {token0} ({token0_symbol})")
    print(f"token1: {token1} ({token1_symbol})")
    print(f"交易方向: {swap_direction}")
    print(f"价格变化: {price_goes}")
    print(f"sqrt_price_limit 应该设为: {limit_should_be}")
    
    if swap_direction == "token1 → token0":
        print(f"\n✅ 这解释了为什么 -5% 有效:")
        print(f"- APT → xBTC 是 token1 → token0")
        print(f"- 价格会下跌，需要设置下限防止跌太多")
        print(f"- 所以 sqrt_price_limit = current_price - 5% 是正确的")
    else:
        print(f"\n✅ 这解释了为什么 +5% 有效:")
        print(f"- APT → xBTC 是 token0 → token1") 
        print(f"- 价格会上涨，需要设置上限防止涨太多")
        print(f"- 所以 sqrt_price_limit = current_price + 5% 是正确的")

if __name__ == "__main__":
    results = analyze_sqrt_price_economics()
    analyze_token_order()
    
    print(f"\n📊 总结:")
    print(f"当前实际价格: {results['current_price']:.10f}")
    print(f"-5% 实际价格变化: {results['price_change_minus5']:+.2f}%")
    print(f"+5% 实际价格变化: {results['price_change_plus5']:+.2f}%")
