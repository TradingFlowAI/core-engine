#!/usr/bin/env python3
"""
测试Aptos代币工具函数

使用方法:
python test_aptos_util.py
"""

import asyncio
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'tradingflow'))

from tradingflow.common.utils import aptos_util
from tradingflow.common.logging_config import setup_logging
from tradingflow.common.config import CONFIG

# 设置日志
setup_logging(CONFIG, "test_aptos_util")
import logging
logger = logging.getLogger(__name__)


async def test_aptos_token_functions():
    """测试Aptos代币相关函数"""
    print("=" * 60)
    print("测试 Aptos 代币工具函数")
    print("=" * 60)

    # 测试代币地址 (示例地址，可能需要根据实际情况调整)
    test_addresses = [
        "0x1::aptos_coin::AptosCoin",  # APT代币
        "0xa", # 简短地址测试
        "0x0000000000000000000000000000000000000000000000000000000000000001::aptos_coin::AptosCoin",  # 完整地址
    ]

    for i, token_address in enumerate(test_addresses, 1):
        print(f"\n测试 #{i}: {token_address}")
        print("-" * 40)

        try:
            # 测试获取代币信息
            print("获取代币完整信息...")
            token_info = await aptos_util.fetch_token_info(token_address)
            if token_info:
                print(f"✅ 代币信息: {token_info}")
            else:
                print("❌ 未找到代币信息")

            # 测试获取小数位数
            print("获取代币小数位数...")
            decimals = await aptos_util.get_token_decimals(token_address)
            print(f"✅ 小数位数: {decimals}")

            # 测试获取符号
            print("获取代币符号...")
            symbol = await aptos_util.get_token_symbol(token_address)
            if symbol:
                print(f"✅ 符号: {symbol}")
            else:
                print("❌ 未找到符号")

            # 测试获取名称
            print("获取代币名称...")
            name = await aptos_util.get_token_name(token_address)
            if name:
                print(f"✅ 名称: {name}")
            else:
                print("❌ 未找到名称")

            # 测试数量转换
            if token_info and "decimals" in token_info:
                test_amount = "1000000000"  # 10亿基础单位
                converted = aptos_util.convert_token_amount(test_amount, token_info["decimals"])
                print(f"✅ 数量转换: {test_amount} -> {converted}")

        except Exception as e:
            print(f"❌ 测试失败: {e}")
            logger.error(f"测试地址 {token_address} 时出错", exc_info=True)

    print("\n" + "=" * 60)
    print("测试地址工具函数")
    print("=" * 60)

    # 测试地址工具函数
    test_addresses_validation = [
        "0x1",
        "0xa",
        "0x1::aptos_coin::AptosCoin",
        "invalid_address",
        "0x0000000000000000000000000000000000000000000000000000000000000001",
    ]

    for addr in test_addresses_validation:
        normalized = aptos_util.normalize_aptos_address(addr)
        is_valid = aptos_util.is_valid_aptos_address(addr)
        print(f"地址: {addr}")
        print(f"  规范化: {normalized}")
        print(f"  有效性: {'✅' if is_valid else '❌'}")
        print()


def test_companion_connection():
    """测试companion服务连接"""
    print("=" * 60)
    print("测试 Companion 服务连接")
    print("=" * 60)

    import requests
    from tradingflow.common.config import CONFIG

    companion_host = CONFIG.get("COMPANION_HOST", "localhost")
    companion_port = CONFIG.get("COMPANION_PORT", 3000)

    # 测试健康检查端点
    try:
        url = f"http://{companion_host}:{companion_port}/health"
        print(f"尝试连接: {url}")

        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            print("✅ Companion 服务运行正常")
            return True
        else:
            print(f"❌ Companion 服务响应异常: {response.status_code}")
            return False

    except requests.exceptions.ConnectionError:
        print(f"❌ 无法连接到 Companion 服务 ({companion_host}:{companion_port})")
        print("请确保 Companion 服务正在运行")
        return False
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return False


async def main():
    """主测试函数"""
    print("开始测试 Aptos 工具函数")
    print("请确保 Companion 服务正在运行 (默认端口: 3000)")
    print()

    # 首先测试连接
    connection_ok = test_companion_connection()

    if connection_ok:
        print("\n继续进行代币函数测试...")
        await test_aptos_token_functions()
    else:
        print("\n❌ 由于无法连接到 Companion 服务，跳过代币函数测试")
        print("启动 Companion 服务后重新运行此测试")

    print("\n测试完成!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n测试被用户中断")
    except Exception as e:
        print(f"\n测试过程中发生错误: {e}")
        logger.error("测试失败", exc_info=True)
