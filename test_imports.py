#!/usr/bin/env python3
"""
测试 Station 服务的所有关键导入
确保没有循环依赖和导入错误
"""

import sys
sys.path.insert(0, '.')

def test_imports():
    """测试所有关键导入"""
    try:
        # 1. 测试 core 模块
        print("Testing core modules...")
        from core.node_registry import NodeRegistry
        from core.community_node import CommunityNode
        from core.version_manager import VersionManager
        print("✅ Core modules imported successfully")
        
        # 2. 测试 nodes 模块
        print("\nTesting nodes modules...")
        from nodes.node_base import NodeBase, NodeStatus
        from nodes.ai_model_node import AIModelNode
        from nodes.code_node import CodeNode
        print("✅ Nodes modules imported successfully")
        
        # 3. 测试 common 模块
        print("\nTesting common modules...")
        from common.node_decorators import register_node_type
        from common.node_registry import NodeRegistry as LocalNodeRegistry
        print("✅ Common modules imported successfully")
        
        # 4. 测试 server 主模块
        print("\nTesting server module...")
        import server
        print("✅ Server module imported successfully")
        
        # 5. 验证节点注册
        print("\nVerifying node registrations...")
        registered_nodes = NodeRegistry.get_all_node_types()
        print(f"✅ Total registered nodes: {len(registered_nodes)}")
        for node_type in sorted(registered_nodes):
            versions = NodeRegistry.get_all_versions(node_type)
            print(f"  - {node_type}: {versions}")
        
        print("\n" + "="*50)
        print("🎉 ALL TESTS PASSED! Station imports are working correctly.")
        print("="*50)
        return True
        
    except Exception as e:
        print(f"\n❌ IMPORT TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_imports()
    sys.exit(0 if success else 1)
