#!/usr/bin/env python3

"""
验证Aptos池子是否按照Uniswap V3规则排序 (token1 < token2)
"""

import json
import os

def verify_pool_sorting():
    """验证池子中的代币排序"""
    
    # 读取pools.json
    pools_file = "/Users/cleopatra/Desktop/TradingFlow/tradingflow-codebase/4_weather_vault/aptos/ts-scripts/utils/pools.json"
    
    if not os.path.exists(pools_file):
        print(f"❌ 文件不存在: {pools_file}")
        return False
    
    try:
        with open(pools_file, 'r', encoding='utf-8') as f:
            pools_data = json.load(f)
    except Exception as e:
        print(f"❌ 读取JSON失败: {e}")
        return False
    
    print("🔍 验证Aptos池子代币排序规则:")
    print("=" * 60)
    
    total_pools = len(pools_data)
    valid_pools = 0
    sorted_correctly = 0
    sorting_violations = []
    
    print(f"📊 总池子数量: {total_pools}")
    
    for i, pool_data in enumerate(pools_data):
        pool_info = pool_data.get("pool", {})
        
        # 跳过空池子
        if not pool_info:
            continue
            
        valid_pools += 1
        token1 = pool_info.get("token1", "")
        token2 = pool_info.get("token2", "")
        pool_id = pool_info.get("poolId", "")
        
        # 检查排序
        if token1 and token2:
            is_sorted_correctly = token1 < token2
            
            if is_sorted_correctly:
                sorted_correctly += 1
            else:
                sorting_violations.append({
                    'pool_index': i,
                    'pool_id': pool_id[:20] + "..." if len(pool_id) > 20 else pool_id,
                    'token1': token1[:20] + "..." if len(token1) > 20 else token1,
                    'token2': token2[:20] + "..." if len(token2) > 20 else token2,
                    'token1_symbol': pool_info.get("token1Info", {}).get("symbol", "Unknown"),
                    'token2_symbol': pool_info.get("token2Info", {}).get("symbol", "Unknown")
                })
    
    print(f"✅ 有效池子数量: {valid_pools}")
    print(f"✅ 正确排序池子: {sorted_correctly}")
    print(f"❌ 排序违规池子: {len(sorting_violations)}")
    print(f"📈 正确率: {(sorted_correctly/valid_pools*100):.2f}%" if valid_pools > 0 else "N/A")
    
    # 显示违规的池子
    if sorting_violations:
        print(f"\n⚠️  排序违规的池子:")
        print("-" * 60)
        for violation in sorting_violations[:10]:  # 只显示前10个
            print(f"池子 #{violation['pool_index']}: {violation['pool_id']}")
            print(f"  token1: {violation['token1']} ({violation['token1_symbol']})")
            print(f"  token2: {violation['token2']} ({violation['token2_symbol']})")
            print(f"  问题: token1 > token2 (违反Uniswap V3规则)")
            print()
        
        if len(sorting_violations) > 10:
            print(f"... 还有 {len(sorting_violations) - 10} 个违规池子")
    
    # 分析几个具体的例子
    print(f"\n🔍 具体例子分析:")
    print("-" * 40)
    
    examples_shown = 0
    for i, pool_data in enumerate(pools_data[:20]):  # 检查前20个
        pool_info = pool_data.get("pool", {})
        if not pool_info:
            continue
            
        token1 = pool_info.get("token1", "")
        token2 = pool_info.get("token2", "") 
        token1_symbol = pool_info.get("token1Info", {}).get("symbol", "Unknown")
        token2_symbol = pool_info.get("token2Info", {}).get("symbol", "Unknown")
        
        if token1 and token2:
            is_correct = token1 < token2
            status = "✅" if is_correct else "❌"
            
            print(f"{status} 池子 #{i}: {token1_symbol}/{token2_symbol}")
            print(f"   token1: {token1[:30]}...")
            print(f"   token2: {token2[:30]}...")
            print(f"   排序: {'正确' if is_correct else '错误'}")
            print()
            
            examples_shown += 1
            if examples_shown >= 5:
                break
    
    return len(sorting_violations) == 0

def analyze_apt_pools():
    """专门分析包含APT的池子"""
    
    pools_file = "/Users/cleopatra/Desktop/TradingFlow/tradingflow-codebase/4_weather_vault/aptos/ts-scripts/utils/pools.json"
    
    try:
        with open(pools_file, 'r', encoding='utf-8') as f:
            pools_data = json.load(f)
    except Exception as e:
        print(f"❌ 读取JSON失败: {e}")
        return
    
    print(f"\n🪙 APT相关池子分析:")
    print("=" * 40)
    
    apt_address = "0x000000000000000000000000000000000000000000000000000000000000000a"  # APT地址
    apt_pools = []
    
    for i, pool_data in enumerate(pools_data):
        pool_info = pool_data.get("pool", {})
        if not pool_info:
            continue
            
        token1 = pool_info.get("token1", "")
        token2 = pool_info.get("token2", "")
        token1_symbol = pool_info.get("token1Info", {}).get("symbol", "Unknown")
        token2_symbol = pool_info.get("token2Info", {}).get("symbol", "Unknown")
        
        if apt_address in [token1, token2]:
            apt_pools.append({
                'index': i,
                'token1': token1,
                'token2': token2,
                'token1_symbol': token1_symbol,
                'token2_symbol': token2_symbol,
                'apt_is_token1': token1 == apt_address,
                'is_sorted': token1 < token2
            })
    
    print(f"找到 {len(apt_pools)} 个包含APT的池子:")
    
    for pool in apt_pools[:10]:  # 显示前10个
        apt_position = "token1" if pool['apt_is_token1'] else "token2"
        other_symbol = pool['token2_symbol'] if pool['apt_is_token1'] else pool['token1_symbol']
        status = "✅" if pool['is_sorted'] else "❌"
        
        print(f"{status} APT/{other_symbol} - APT作为{apt_position} - {'排序正确' if pool['is_sorted'] else '排序错误'}")
        
        if not pool['is_sorted']:
            print(f"    问题: token1={pool['token1'][:30]}...")
            print(f"          token2={pool['token2'][:30]}...")
    
    # 统计APT位置
    apt_as_token1 = sum(1 for p in apt_pools if p['apt_is_token1'])
    apt_as_token2 = len(apt_pools) - apt_as_token1
    
    print(f"\n📊 APT位置统计:")
    print(f"APT作为token1: {apt_as_token1}")
    print(f"APT作为token2: {apt_as_token2}")
    
    if apt_as_token1 > 0:
        print(f"\n💡 分析: APT地址 0xa 非常小，按字典序应该总是token1")
        print(f"实际: APT在 {apt_as_token1}/{len(apt_pools)} 个池子中作为token1")

if __name__ == "__main__":
    is_correct = verify_pool_sorting()
    analyze_apt_pools()
    
    print(f"\n🎯 结论:")
    if is_correct:
        print("✅ 所有池子都遵循Uniswap V3排序规则 (token1 < token2)")
        print("✅ 我们的swap_node逻辑应该是正确的")
    else:
        print("⚠️  存在排序违规，需要检查Aptos的具体实现")
        print("⚠️  可能需要调整swap_node的逻辑")
