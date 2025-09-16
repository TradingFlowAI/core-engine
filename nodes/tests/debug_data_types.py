#!/usr/bin/env python3

"""
检查 sqrt_price_limit 的数据类型和范围
"""

import sys

def check_data_types():
    """检查各种数值的数据类型和范围"""
    
    # 池子的 sqrtPrice
    pool_sqrt_price = 114938537987309159
    
    # uint64 最大值
    uint64_max = 18446744073709551615
    
    # 各种计算结果
    plus_1_pct = int(pool_sqrt_price * 1.01)
    minus_1_pct = int(pool_sqrt_price * 0.99)
    plus_5_pct = int(pool_sqrt_price * 1.05)
    minus_5_pct = int(pool_sqrt_price * 0.95)
    
    print("🧪 数据类型和范围检查:")
    print("=" * 60)
    
    values = [
        ("池子 sqrtPrice", pool_sqrt_price),
        ("uint64 最大值", uint64_max),
        ("当前 +1%", plus_1_pct),
        ("当前 -1%", minus_1_pct),
        ("当前 +5%", plus_5_pct),
        ("当前 -5%", minus_5_pct),
    ]
    
    for name, value in values:
        # 检查是否在 uint64 范围内
        in_uint64 = value <= uint64_max
        # 获取位数
        bit_length = value.bit_length()
        # Python int 大小 (理论上无限)
        py_int_max = sys.maxsize
        
        print(f"\n📊 {name}:")
        print(f"  值:           {value:,}")
        print(f"  十六进制:     0x{value:x}")
        print(f"  位长度:       {bit_length} bits")
        print(f"  uint64范围:   {'✅ 是' if in_uint64 else '❌ 否'}")
        print(f"  Python int:   {'✅ 支持' if value <= py_int_max else '❌ 超限'}")
    
    print(f"\n🔍 关键发现:")
    print(f"- Python int 可以处理任意大小的整数")
    print(f"- 所有计算值都在 uint64 范围内")
    print(f"- 问题可能不在数据类型，而在 Aptos 合约的验证逻辑")
    
    # 检查字符串转换
    print(f"\n🔧 字符串转换测试:")
    test_values = [pool_sqrt_price, plus_1_pct, minus_1_pct]
    for val in test_values:
        str_val = str(val)
        back_to_int = int(str_val)
        print(f"  {val} -> '{str_val}' -> {back_to_int} ({'✅ 正确' if val == back_to_int else '❌ 错误'})")
    
    return values

if __name__ == "__main__":
    check_data_types()
