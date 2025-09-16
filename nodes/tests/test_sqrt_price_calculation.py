#!/usr/bin/env python3

"""
Test the complete sqrt_price_limit calculation with real pool data
"""

def simulate_sqrt_price_calculation():
    """Simulate the new sqrt_price_limit calculation with user's data"""
    
    # User's real pool data
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
            'sqrtPrice': '114938537987309159'
        }, 
        'fee_tier': 1, 
        'token1': '0xa', 
        'token2': '0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387'
    }
    
    # Simulate the new method
    def calculate_sqrt_price_limit_from_pool(best_pool: dict, is_buy: bool = True, slippage_pct: float = 5.0) -> int:
        try:
            # Get current sqrt price from pool
            pool_info = best_pool.get("pool_info", {})
            current_sqrt_price = int(pool_info.get("sqrtPrice", 0))
            
            if current_sqrt_price == 0:
                # Fallback to pool.sqrtPrice if top-level sqrtPrice is missing
                pool_data = pool_info.get("pool", {})
                current_sqrt_price = int(pool_data.get("sqrtPrice", 0))
            
            if current_sqrt_price == 0:
                raise ValueError("No valid sqrtPrice found in pool data")
            
            # Calculate slippage multiplier
            slippage_multiplier = 1 + (slippage_pct / 100)
            
            if is_buy:
                # Buying: allow price to go up by slippage %
                sqrt_price_limit = int(current_sqrt_price * slippage_multiplier)
            else:
                # Selling: allow price to go down by slippage %
                sqrt_price_limit = int(current_sqrt_price / slippage_multiplier)
                
            return sqrt_price_limit, current_sqrt_price
            
        except Exception as e:
            # Return reasonable default (current price ± 10%)
            fallback_multiplier = 1.1 if is_buy else 0.9
            fallback_price = int(current_sqrt_price * fallback_multiplier) if current_sqrt_price > 0 else 0
            return fallback_price, current_sqrt_price
    
    print("🧪 Testing sqrt_price_limit calculation with real pool data:")
    print("=" * 60)
    
    # Test with different slippage values
    slippage_tests = [1.0, 3.0, 5.0, 10.0]
    
    for slippage in slippage_tests:
        new_limit, current_price = calculate_sqrt_price_limit_from_pool(best_pool, is_buy=True, slippage_pct=slippage)
        
        print(f"\n📊 Slippage {slippage}%:")
        print(f"  Current sqrtPrice: {current_price:,}")
        print(f"  New sqrt_limit:    {new_limit:,}")
        print(f"  Old sqrt_limit:    18,446,744,073,709,551,615 (max uint64)")
        
        # Calculate improvement
        old_limit = 18446744073709551615
        improvement_factor = old_limit / new_limit
        print(f"  Improvement:       {improvement_factor:,.0f}x more reasonable")
    
    # Test buy vs sell
    print(f"\n🔄 Buy vs Sell (5% slippage):")
    buy_limit, current_price = calculate_sqrt_price_limit_from_pool(best_pool, is_buy=True, slippage_pct=5.0)
    sell_limit, _ = calculate_sqrt_price_limit_from_pool(best_pool, is_buy=False, slippage_pct=5.0)
    
    print(f"  Current price: {current_price:,}")
    print(f"  Buy limit:     {buy_limit:,} (+5%)")
    print(f"  Sell limit:    {sell_limit:,} (-5%)")
    
    return current_price, buy_limit, sell_limit

if __name__ == "__main__":
    simulate_sqrt_price_calculation()
