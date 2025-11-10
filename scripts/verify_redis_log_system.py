#!/usr/bin/env python3
"""
验证 Redis 日志系统

快速验证异步 Redis 日志发布器是否正常工作

使用方式:
    python scripts/verify_redis_log_system.py
"""

import asyncio
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def test_basic_functionality():
    """测试基本功能"""
    print("\n" + "=" * 60)
    print("🧪 测试 1: 基本功能")
    print("=" * 60)
    
    from core.redis_log_publisher_async import publish_log_async, get_publisher_stats
    
    try:
        # 发布测试日志
        print("\n📤 发布测试日志...")
        success = await publish_log_async(
            flow_id="test_flow",
            cycle=1,
            log_entry={
                "node_id": "test_node",
                "level": "info",
                "message": "Verification test log",
                "log_source": "verification_script",
            },
            max_retries=3
        )
        
        if success:
            print("✅ 日志发布成功")
        else:
            print("❌ 日志发布失败")
            return False
        
        # 获取统计信息
        print("\n📊 获取统计信息...")
        stats = await get_publisher_stats()
        print(f"  ├─ 连接状态: {'✅ 已连接' if stats['connected'] else '❌ 未连接'}")
        print(f"  ├─ 总请求数: {stats['total_count']}")
        print(f"  ├─ 成功次数: {stats['success_count']}")
        print(f"  ├─ 失败次数: {stats['failure_count']}")
        print(f"  ├─ 成功率: {stats['success_rate']}%")
        print(f"  └─ 平均延迟: {stats['avg_publish_time_ms']} ms")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_batch_publishing():
    """测试批量发布"""
    print("\n" + "=" * 60)
    print("🧪 测试 2: 批量发布")
    print("=" * 60)
    
    from core.redis_log_publisher_async import publish_log_async
    
    batch_size = 10
    print(f"\n📤 批量发布 {batch_size} 条日志...")
    
    success_count = 0
    failure_count = 0
    
    for i in range(batch_size):
        success = await publish_log_async(
            flow_id="batch_test_flow",
            cycle=1,
            log_entry={
                "node_id": "batch_test_node",
                "level": "info",
                "message": f"Batch test log #{i+1}",
            }
        )
        
        if success:
            success_count += 1
            print(f"  ✅ 日志 #{i+1} 发布成功")
        else:
            failure_count += 1
            print(f"  ❌ 日志 #{i+1} 发布失败")
    
    print(f"\n📊 批量发布结果:")
    print(f"  ├─ 成功: {success_count}/{batch_size}")
    print(f"  ├─ 失败: {failure_count}/{batch_size}")
    print(f"  └─ 成功率: {success_count/batch_size*100:.1f}%")
    
    return success_count == batch_size


async def test_error_handling():
    """测试错误处理"""
    print("\n" + "=" * 60)
    print("🧪 测试 3: 错误处理")
    print("=" * 60)
    
    from core.redis_log_publisher_async import AsyncRedisLogPublisher
    
    print("\n🔌 测试连接失败处理...")
    
    # 创建一个使用错误配置的发布器
    old_redis_host = os.getenv('REDIS_HOST')
    os.environ['REDIS_HOST'] = 'invalid_host_12345'
    
    try:
        publisher = AsyncRedisLogPublisher()
        try:
            await publisher.connect()
            print("❌ 应该抛出连接错误，但没有")
            return False
        except Exception as e:
            print(f"✅ 正确捕获连接错误: {type(e).__name__}")
            return True
    finally:
        # 恢复环境变量
        if old_redis_host:
            os.environ['REDIS_HOST'] = old_redis_host
        else:
            os.environ.pop('REDIS_HOST', None)


async def test_metrics():
    """测试 Metrics"""
    print("\n" + "=" * 60)
    print("🧪 测试 4: Prometheus Metrics")
    print("=" * 60)
    
    from core.metrics import is_metrics_enabled
    
    if is_metrics_enabled():
        print("✅ Prometheus metrics 已启用")
        print("  可以通过以下方式查看:")
        print("    1. 调用 start_metrics_server(port=9090)")
        print("    2. 访问 http://localhost:9090/metrics")
        return True
    else:
        print("⚠️  Prometheus metrics 未启用")
        print("  安装方式: pip install prometheus-client")
        return False


async def test_stats_api():
    """测试统计 API"""
    print("\n" + "=" * 60)
    print("🧪 测试 5: 统计 API")
    print("=" * 60)
    
    from core.redis_log_publisher_async import get_publisher_stats
    
    try:
        stats = await get_publisher_stats()
        print("\n📊 统计信息结构:")
        print(f"  ├─ connected: {type(stats.get('connected')).__name__}")
        print(f"  ├─ success_count: {type(stats.get('success_count')).__name__}")
        print(f"  ├─ failure_count: {type(stats.get('failure_count')).__name__}")
        print(f"  ├─ total_count: {type(stats.get('total_count')).__name__}")
        print(f"  ├─ success_rate: {type(stats.get('success_rate')).__name__}")
        print(f"  └─ avg_publish_time_ms: {type(stats.get('avg_publish_time_ms')).__name__}")
        
        required_keys = [
            'connected', 'success_count', 'failure_count',
            'total_count', 'success_rate', 'avg_publish_time_ms'
        ]
        
        all_keys_present = all(key in stats for key in required_keys)
        
        if all_keys_present:
            print("\n✅ 统计 API 结构正确")
            return True
        else:
            missing_keys = [key for key in required_keys if key not in stats]
            print(f"\n❌ 缺少必需的键: {missing_keys}")
            return False
            
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """主测试函数"""
    print("\n" + "=" * 60)
    print("🚀 Redis 日志系统验证")
    print("=" * 60)
    
    print("\n📋 环境信息:")
    print(f"  ├─ Python 版本: {sys.version.split()[0]}")
    print(f"  ├─ Redis 主机: {os.getenv('REDIS_HOST', 'localhost')}")
    print(f"  └─ Redis 端口: {os.getenv('REDIS_PORT', '6379')}")
    
    # 运行所有测试
    tests = [
        ("基本功能", test_basic_functionality),
        ("批量发布", test_batch_publishing),
        ("错误处理", test_error_handling),
        ("Prometheus Metrics", test_metrics),
        ("统计 API", test_stats_api),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            result = await test_func()
            results[test_name] = result
        except Exception as e:
            print(f"\n❌ 测试 '{test_name}' 异常: {e}")
            import traceback
            traceback.print_exc()
            results[test_name] = False
    
    # 打印总结
    print("\n" + "=" * 60)
    print("📊 测试总结")
    print("=" * 60)
    
    total = len(results)
    passed = sum(1 for r in results.values() if r)
    failed = total - passed
    
    for test_name, result in results.items():
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  {status} - {test_name}")
    
    print(f"\n🎯 总计: {passed}/{total} 通过")
    
    if passed == total:
        print("\n✅ 所有测试通过！Redis 日志系统工作正常。")
        return 0
    else:
        print(f"\n⚠️  有 {failed} 个测试失败，请检查配置和连接。")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  测试被用户中断")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n❌ 验证脚本异常: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
