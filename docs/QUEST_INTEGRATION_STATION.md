# Station Quest Integration Guide
# Station层Quest系统集成指南

本文档说明如何在Station (Python后端) 中集成Quest活动事件发布。

---

## 📦 导入

```python
from mq.activity_publisher import get_activity_publisher, publish_activity
```

---

## 🎯 使用方法

### 方法一：使用全局函数（推荐）

```python
from mq.activity_publisher import publish_activity

# 在任何地方发布事件
publish_activity(
    user_id='user_123',
    event_type='RUN_FLOW',
    metadata={
        'flowId': flow_id,
        'cycle': cycle,
        'nodeCount': len(nodes)
    }
)
```

### 方法二：使用Publisher实例

```python
from mq.activity_publisher import get_activity_publisher

publisher = get_activity_publisher()
if publisher:
    publisher.publish(
        user_id='user_123',
        event_type='RUN_NODE',
        metadata={'nodeType': 'swap_node', 'flowId': flow_id}
    )
```

---

## 🔄 Flow执行集成

### 在flow/executor.py中

```python
from mq.activity_publisher import publish_activity

class FlowExecutor:
    async def execute_flow(self, flow_id: str, user_id: str, cycle: int):
        """执行Flow"""
        try:
            # ... 执行逻辑 ...
            
            # 发布Flow运行事件
            publish_activity(
                user_id=user_id,
                event_type='RUN_FLOW',
                metadata={
                    'flowId': flow_id,
                    'cycle': cycle,
                    'timestamp': datetime.utcnow().isoformat()
                }
            )
            
            # 执行所有节点
            for node in nodes:
                result = await self.execute_node(node)
                
                # 发布节点运行事件
                publish_activity(
                    user_id=user_id,
                    event_type='RUN_NODE',
                    metadata={
                        'nodeType': node.type,
                        'flowId': flow_id,
                        'nodeId': node.id,
                        'success': result.success
                    }
                )
            
            # Flow执行完成
            success = all(results)
            publish_activity(
                user_id=user_id,
                event_type='COMPLETE_FLOW',
                metadata={
                    'flowId': flow_id,
                    'cycle': cycle,
                    'success': success,
                    'executionTime': execution_time
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f'Flow execution error: {e}')
            raise
```

---

## 🎮 API路由集成

### 在api/flow_api.py中

```python
from sanic import Blueprint, response
from mq.activity_publisher import publish_activity

flow_bp = Blueprint('flow', url_prefix='/flow')

@flow_bp.post('/execute')
async def execute_flow(request):
    """执行Flow API"""
    try:
        flow_id = request.json.get('flowId')
        user_id = request.ctx.user_id  # 从认证中间件获取
        
        # 执行Flow
        result = await executor.execute_flow(flow_id, user_id, cycle=0)
        
        # 发布活动事件（可选，如果executor中已发布可省略）
        if not hasattr(executor, 'publishes_events'):
            publish_activity(
                user_id=user_id,
                event_type='RUN_FLOW',
                metadata={'flowId': flow_id, 'success': result.success}
            )
        
        return response.json({
            'success': True,
            'result': result.to_dict()
        })
        
    except Exception as e:
        logger.error(f'Error executing flow: {e}')
        return response.json({
            'success': False,
            'error': str(e)
        }, status=500)
```

---

## 📡 支持的事件类型

### Flow相关
- `RUN_FLOW` - 运行Flow
- `COMPLETE_FLOW` - 完成Flow
- `RUN_NODE` - 运行节点

### 便捷方法

Activity Publisher提供了一些便捷方法：

```python
from mq.activity_publisher import get_activity_publisher

publisher = get_activity_publisher()

# Flow运行
publisher.publish_flow_run(
    user_id='user_123',
    flow_id='flow_abc',
    cycle=5,
    metadata={'nodeCount': 10}
)

# Flow完成
publisher.publish_flow_complete(
    user_id='user_123',
    flow_id='flow_abc',
    cycle=5,
    success=True,
    metadata={'executionTime': 1.5}
)

# 节点运行
publisher.publish_node_run(
    user_id='user_123',
    node_type='swap_node',
    flow_id='flow_abc',
    metadata={'success': True}
)
```

---

## 🔍 调试

### 检查Publisher是否初始化

```python
from mq.activity_publisher import get_activity_publisher

publisher = get_activity_publisher()
if publisher:
    print("✓ Activity Publisher is initialized")
else:
    print("✗ Activity Publisher not initialized")
```

### 查看日志

发布成功时会看到：
```
INFO: Activity published: RUN_FLOW for user user_123 (routing_key: user.flow.run)
```

发布失败时会看到：
```
ERROR: Failed to publish activity: [error details]
```

---

## ⚠️ 注意事项

1. **非阻塞**: 事件发布失败不应阻塞主业务逻辑
2. **错误处理**: `publish_activity()` 返回bool，但失败不会抛出异常
3. **元数据**: 提供足够的上下文信息便于Quest进度追踪
4. **性能**: 事件发布是轻量操作，但避免在循环中大量发布

---

## 🧪 测试

### 单元测试

```python
import pytest
from unittest.mock import Mock, patch
from mq.activity_publisher import ActivityPublisher

def test_publish_activity():
    mock_connection = Mock()
    publisher = ActivityPublisher(mock_connection)
    
    # 测试发布
    result = publisher.publish(
        user_id='test_user',
        event_type='RUN_FLOW',
        metadata={'flowId': 'test_flow'}
    )
    
    assert result == True
    # 验证channel.basic_publish被调用
    mock_connection.channel.return_value.basic_publish.assert_called_once()
```

### 集成测试

```python
# 在实际环境中测试
from mq.activity_publisher import publish_activity

# 发布测试事件
result = publish_activity(
    user_id='test_user_123',
    event_type='RUN_FLOW',
    metadata={
        'flowId': 'test_flow',
        'cycle': 0,
        'test': True
    }
)

print(f"Publish result: {result}")
```

---

## 📋 集成清单

Station层需要在以下位置集成：

- [ ] **Flow Executor** (`flow/executor.py`)
  - [ ] RUN_FLOW - Flow开始执行
  - [ ] COMPLETE_FLOW - Flow执行完成
  - [ ] RUN_NODE - 节点执行
  
- [ ] **Flow API** (`api/flow_api.py`)
  - [ ] Flow执行端点

---

## 🔗 相关文档

- [Quest System Implementation](../docs/QUESTS_SYSTEM_IMPLEMENTATION.md)
- [Control Quest Integration](../02_weather_control/QUEST_INTEGRATION_GUIDE.md)
- [Activity Publisher API](./mq/activity_publisher.py)

---

**维护者**: TradingFlow Development Team  
**最后更新**: 2025-01-09
