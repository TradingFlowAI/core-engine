#!/usr/bin/env python3

"""
Test the fixed _is_pool_suitable method with real API response data
"""

def _is_pool_suitable_fixed(pool_data: dict) -> bool:
    """
    Fixed version of _is_pool_suitable method that handles new API response format
    """
    try:
        # 检查基本字段
        sqrt_price = pool_data.get("sqrtPrice")
        if not sqrt_price:
            return False
            
        # 检查流动性：优先使用tvlUSD，其次使用liquidity
        liquidity_value = 0
        if pool_data.get("tvlUSD"):
            try:
                liquidity_value = float(pool_data.get("tvlUSD"))
            except (ValueError, TypeError):
                liquidity_value = 0
        elif pool_data.get("liquidity"):
            try:
                liquidity_value = float(pool_data.get("liquidity"))
            except (ValueError, TypeError):
                liquidity_value = 0
        
        # 最小流动性要求：$1000 USD
        if liquidity_value < 1000:
            return False
            
        # 检查价格是否合理
        try:
            sqrt_price_value = float(sqrt_price)
            if sqrt_price_value <= 0:
                return False
        except (ValueError, TypeError):
            return False
            
        return True
        
    except Exception:
        return False


def test_with_user_data():
    """Test with the exact data from user's log"""
    
    # User's real API response
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
    
    print("🧪 Testing pool suitability with user's data:")
    print("="*50)
    print(f"📊 Pool: {pool_data['token1Info']['symbol']}/{pool_data['token2Info']['symbol']}")
    print(f"💰 TVL: ${float(pool_data['tvlUSD']):,.2f}")
    print(f"📈 Daily Volume: ${float(pool_data['dailyVolumeUSD']):,.2f}")
    print(f"⚙️  Fee Tier: {pool_data['feeTier']}")
    print(f"📊 Sqrt Price: {pool_data['sqrtPrice']}")
    
    result = _is_pool_suitable_fixed(pool_data)
    print(f"\n🎯 Pool Suitability: {'✅ SUITABLE' if result else '❌ NOT SUITABLE'}")
    
    if result:
        print("✅ Fix successful - pool will now be accepted by SwapNode")
    else:
        print("❌ Fix failed - additional debugging needed")
    
    return result


if __name__ == "__main__":
    test_with_user_data()
