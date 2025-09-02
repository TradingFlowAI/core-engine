#!/usr/bin/env python3

"""
Test using pool's sqrtPrice directly as sqrt_price_limit
"""

def test_direct_sqrt_price():
    """Test the direct sqrtPrice approach"""
    
    # Your pool data
    best_pool = {
        'pool_info': {
            'success': True, 
            'pool': {
                'currentTick': -101570, 
                'feeRate': '500', 
                'feeTier': 1, 
                'poolId': '0xd8609fb7a2446b1e343de45decc9651d4402b967439d352849a422b55327516f', 
                'sqrtPrice': '114938537987309159'
            }, 
            'tvlUSD': '1789400.5542687718376', 
            'sqrtPrice': '114938537987309159'  # Top-level sqrtPrice
        }, 
        'fee_tier': 1
    }
    
    print("🧪 测试直接使用池子的sqrtPrice:")
    print("=" * 50)
    
    # Simulate the new logic
    pool_info = best_pool.get("pool_info", {})
    sqrt_price_limit = pool_info.get("sqrtPrice", "0")
    
    # Convert to string if it's an integer
    if isinstance(sqrt_price_limit, int):
        sqrt_price_limit = str(sqrt_price_limit)
    
    print(f"📊 池子当前sqrtPrice: {sqrt_price_limit}")
    print(f"🎯 直接用作sqrt_price_limit: {sqrt_price_limit}")
    
    # Compare with old problematic values  
    old_calculated = "18446744073709551615"
    old_with_slippage = "120685464886674608"
    
    print(f"\n📈 对比:")
    print(f"  池子原值:     {sqrt_price_limit}")
    print(f"  旧计算值:     {old_calculated} (max uint64)")
    print(f"  加滑点后:     {old_with_slippage} (池子价格+5%)")
    
    print(f"\n💡 分析:")
    print(f"  直接使用池子的sqrtPrice意味着:")
    print(f"  - 交易必须在当前价格点执行")
    print(f"  - 没有价格滑点容忍度")
    print(f"  - 最精确的价格控制")
    
    # Check if this matches the pool's actual sqrtPrice in both locations
    pool_sqrt = best_pool['pool_info']['pool']['sqrtPrice']
    top_sqrt = best_pool['pool_info']['sqrtPrice']
    
    print(f"\n🔍 数据一致性检查:")
    print(f"  pool.sqrtPrice:     {pool_sqrt}")
    print(f"  top-level sqrtPrice: {top_sqrt}")
    print(f"  一致性: {'✅ 一致' if pool_sqrt == top_sqrt else '❌ 不一致'}")
    
    return sqrt_price_limit

if __name__ == "__main__":
    result = test_direct_sqrt_price()
    print(f"\n🎯 最终使用的sqrt_price_limit: {result}")
