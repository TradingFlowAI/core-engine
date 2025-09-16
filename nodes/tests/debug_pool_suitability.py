#!/usr/bin/env python3

"""
Debug script to test pool suitability logic with real API response data
"""

def debug_is_pool_suitable(pool_data: dict) -> bool:
    """
    Debug version of _is_pool_suitable method with detailed logging
    """
    print("🔍 Debugging pool suitability check...")
    print(f"📊 Pool data keys: {list(pool_data.keys())}")
    
    # Check basic fields
    sqrt_price = pool_data.get("sqrtPrice")
    liquidity = pool_data.get("liquidity")
    tvl_usd = pool_data.get("tvlUSD")
    
    print(f"💰 sqrtPrice: {sqrt_price}")
    print(f"💧 liquidity: {liquidity}")
    print(f"💵 tvlUSD: {tvl_usd}")
    
    if not sqrt_price:
        print("❌ Missing sqrtPrice")
        return False
    
    if not liquidity:
        print("❌ Missing liquidity field")
        print("💡 Checking if we should use tvlUSD instead...")
        
        if tvl_usd:
            try:
                tvl_value = float(tvl_usd)
                print(f"✅ Found tvlUSD: ${tvl_value:,.2f}")
                
                # Use TVL as liquidity proxy (minimum $1000)
                if tvl_value < 1000:
                    print(f"❌ TVL too low: ${tvl_value:,.2f} < $1000")
                    return False
                else:
                    print(f"✅ TVL sufficient: ${tvl_value:,.2f} >= $1000")
            except (ValueError, TypeError) as e:
                print(f"❌ Invalid tvlUSD format: {e}")
                return False
        else:
            print("❌ No liquidity or tvlUSD data available")
            return False
    else:
        try:
            liquidity_value = float(liquidity)
            print(f"✅ Found liquidity: {liquidity_value}")
            if liquidity_value < 1000:
                print(f"❌ Liquidity too low: {liquidity_value} < 1000")
                return False
        except (ValueError, TypeError) as e:
            print(f"❌ Invalid liquidity format: {e}")
            return False
    
    # Check price
    try:
        sqrt_price_value = float(sqrt_price)
        if sqrt_price_value <= 0:
            print(f"❌ Invalid sqrtPrice: {sqrt_price_value}")
            return False
        else:
            print(f"✅ Valid sqrtPrice: {sqrt_price_value}")
    except (ValueError, TypeError) as e:
        print(f"❌ Invalid sqrtPrice format: {e}")
        return False
    
    print("✅ Pool is suitable for trading")
    return True


def test_with_real_data():
    """Test with the actual API response data from user's log"""
    
    # Real API response from user's log
    pool_data = {
        'success': True, 
        'pool': {
            'currentTick': -101556, 
            'feeRate': '500', 
            'feeTier': 1, 
            'poolId': '0xd8609fb7a2446b1e343de45decc9651d4402b967439d352849a422b55327516f', 
            'senderAddress': '0xc34cad27bfc7b2a9c217a8bad840bf3dc34d1aaaeca3e94f05c66e5336bfdb56', 
            'sqrtPrice': '115024257946338202', 
            'token1': '0x000000000000000000000000000000000000000000000000000000000000000a', 
            'token2': '0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387', 
            'token1Info': {
                'assetType': '0x000000000000000000000000000000000000000000000000000000000000000a', 
                'symbol': 'APT', 
                'name': 'Aptos Coin'
            }, 
            'token2Info': {
                'assetType': '0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387', 
                'symbol': 'xBTC', 
                'name': 'OKX Wrapped BTC'
            }
        }, 
        'tvlUSD': '1782521.4178187764829', 
        'dailyVolumeUSD': '3835534.4491768726175', 
        'feeTier': 1, 
        'sqrtPrice': '115024257946338202',
        'token1Info': {
            'assetType': '0x000000000000000000000000000000000000000000000000000000000000000a', 
            'symbol': 'APT', 
            'name': 'Aptos Coin',
            'decimals': 8
        }, 
        'token2Info': {
            'assetType': '0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387', 
            'symbol': 'xBTC', 
            'name': 'OKX Wrapped BTC',
            'decimals': 8
        }
    }
    
    print("🧪 Testing with real API response data:")
    print("="*50)
    
    result = debug_is_pool_suitable(pool_data)
    print(f"\n🎯 Final result: {'✅ SUITABLE' if result else '❌ NOT SUITABLE'}")
    
    return result


if __name__ == "__main__":
    test_with_real_data()
