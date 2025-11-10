#!/usr/bin/env python3

"""
Debug script to analyze sqrtPrice usage in swap execution
"""

import math

def analyze_sqrt_prices():
    """Analyze the difference between pool sqrtPrice and calculated sqrt_price_limit"""
    
    # Data from user's variables
    pool_sqrt_price = int('114938537987309159')
    calculated_sqrt_price_limit = int('18446744073709551615')
    
    print("🔍 SqrtPrice Analysis:")
    print("=" * 50)
    print(f"📊 Pool current sqrtPrice:     {pool_sqrt_price:,}")
    print(f"🧮 Calculated sqrt_price_limit: {calculated_sqrt_price_limit:,}")
    
    # Calculate ratio
    ratio = calculated_sqrt_price_limit / pool_sqrt_price
    print(f"📈 Ratio (calculated/pool):     {ratio:,.2f}x")
    
    # Check if calculated value is max uint64
    max_uint64 = 2**64 - 1
    print(f"🔢 Max uint64:                  {max_uint64:,}")
    print(f"❓ Is calculated = max uint64?   {calculated_sqrt_price_limit == max_uint64}")
    
    print("\n💡 Analysis:")
    if calculated_sqrt_price_limit == max_uint64:
        print("✅ Calculated value is max uint64 - likely means 'no price limit'")
        print("⚠️  This might cause issues as it's essentially unlimited slippage")
    
    print("\n🎯 Recommendations:")
    print("1. Use pool's current sqrtPrice as base for slippage calculation")
    print("2. Apply reasonable slippage tolerance (e.g., ±5%)")
    print("3. Calculate appropriate sqrt_price_limit based on trade direction")
    
    return pool_sqrt_price, calculated_sqrt_price_limit

def calculate_proper_sqrt_price_limits(current_sqrt_price: int, slippage_pct: float = 5.0):
    """
    Calculate proper sqrt_price_limit based on current pool price and slippage tolerance
    
    Args:
        current_sqrt_price: Current pool sqrtPrice
        slippage_pct: Slippage tolerance percentage (default 5%)
    """
    print(f"\n🧮 Calculating proper sqrt_price_limits:")
    print(f"📊 Current sqrtPrice: {current_sqrt_price:,}")
    print(f"📉 Slippage tolerance: {slippage_pct}%")
    
    # Calculate slippage multiplier
    slippage_multiplier = 1 + (slippage_pct / 100)
    
    # For buy orders (price can go up to limit)
    sqrt_price_limit_buy = int(current_sqrt_price * slippage_multiplier)
    
    # For sell orders (price can go down to limit)  
    sqrt_price_limit_sell = int(current_sqrt_price / slippage_multiplier)
    
    print(f"📈 Buy limit (max price):  {sqrt_price_limit_buy:,}")
    print(f"📉 Sell limit (min price): {sqrt_price_limit_sell:,}")
    
    return sqrt_price_limit_buy, sqrt_price_limit_sell

def suggest_implementation():
    """Suggest how to implement proper sqrt_price_limit calculation"""
    
    print("\n🛠️  Implementation Suggestion:")
    print("=" * 50)
    
    code_suggestion = '''
def calculate_sqrt_price_limit_from_pool(self, best_pool: dict, is_buy: bool = True, slippage_pct: float = 5.0) -> int:
    """
    Calculate sqrt_price_limit based on pool's current price and trade direction
    
    Args:
        best_pool: Pool data from monitor API
        is_buy: True for buying token2, False for selling token2  
        slippage_pct: Slippage tolerance percentage
        
    Returns:
        int: Appropriate sqrt_price_limit for the trade
    """
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
            
        await self.persist_log(
            f"Calculated sqrt_price_limit: {sqrt_price_limit:,} "
            f"(current: {current_sqrt_price:,}, slippage: {slippage_pct}%, "
            f"direction: {'buy' if is_buy else 'sell'})", "INFO"
        )
        
        return sqrt_price_limit
        
    except Exception as e:
        await self.persist_log(f"Failed to calculate sqrt_price_limit from pool: {e}", "WARNING")
        # Return reasonable default (current price ± 10%)
        fallback_multiplier = 1.1 if is_buy else 0.9
        return int(current_sqrt_price * fallback_multiplier)
'''
    
    print(code_suggestion)

if __name__ == "__main__":
    # Run analysis
    pool_sqrt_price, calculated_limit = analyze_sqrt_prices()
    
    # Calculate proper limits
    calculate_proper_sqrt_price_limits(pool_sqrt_price)
    
    # Show implementation suggestion
    suggest_implementation()
