# Node Credits 扣费集成文档

## 📋 概述

在 Python station 的 node 执行代码中集成了 credits 扣费功能，通过 **RabbitMQ 消息队列**将扣费请求发送到 `weather_control` 后端进行处理。

---

## 🏗️ 架构设计

### 消息流程

```
┌─────────────────┐
│  NodeBase       │
│  (Python)       │
└────────┬────────┘
         │ 1. execute() 执行成功
         ↓
┌─────────────────┐
│  charge_credits │
│  ()             │
└────────┬────────┘
         │ 2. 发送 MQ 消息
         ↓
┌─────────────────┐
│  RabbitMQ       │
│  Exchange:      │
│  credits_       │
│  exchange       │
└────────┬────────┘
         │ 3. 路由: credits.node
         ↓
┌─────────────────┐
│  weather_       │
│  control        │
│  Credits        │
│  QueueListener  │
└────────┬────────┘
         │ 4. 处理扣费
         ↓
┌─────────────────┐
│  CreditsService │
│  .deductCredits │
│  ()             │
└─────────────────┘
```

---

## 📦 新增文件

### 1. CreditsPublisher (`05_weather_depot/python/mq/credits_publisher.py`)

**功能**: 发布 credits 消息到 RabbitMQ

**关键方法**:
- `connect()`: 连接到 RabbitMQ
- `publish_node_credits()`: 发布单个节点的扣费消息
- `publish_flow_credits()`: 发布整个 flow 的扣费消息
- `close()`: 关闭连接

**消息格式**:
```python
{
    "userId": "user_id_123",
    "nodeType": "code_node" | "regular_node",
    "metadata": {
        "flowId": "flow_id",
        "nodeId": "node_id",
        "nodeName": "node_name",
        "nodeType": "ai_model_node",
        "cycle": 0,
        "componentId": 1
    }
}
```

**RabbitMQ 配置**:
- **Exchange**: `credits_exchange`
- **Routing Key**: `credits.node`
- **持久化**: 是

---

## 🔧 修改的文件

### NodeBase (`03_weather_station/nodes/node_base.py`)

#### 新增构造函数参数

```python
def __init__(
    self,
    # ... 原有参数
    user_id: str = None,           # 用户 ID（必需）
    enable_credits: bool = True,   # 是否启用扣费
    **kwargs
):
```

#### 新增实例属性

```python
# Credits tracking
self.user_id = user_id                    # 用户 ID
self.enable_credits = enable_credits       # 是否启用
self._credits_publisher = None             # Publisher 实例
```

#### 修改 `initialize_message_queue()` 方法

```python
async def initialize_message_queue(self) -> bool:
    # ... 原有逻辑
    
    # 初始化 credits publisher
    if self.enable_credits and self.user_id:
        try:
            from weather_depot.mq.credits_publisher import get_credits_publisher
            self._credits_publisher = await get_credits_publisher()
            self.logger.info("Credits publisher initialized")
        except Exception as e:
            self.logger.warning(f"Failed to init credits publisher: {e}")
            self._credits_publisher = None
```

#### 新增 `charge_credits()` 方法

```python
async def charge_credits(self) -> bool:
    """
    发送 credits 扣费消息到 weather_control
    
    根据节点类型自动判断扣费标准：
    - code_node: 20 credits
    - 普通 node: 10 credits
    """
    if not self.enable_credits or not self.user_id:
        return True
    
    # 判断节点类型
    node_type = self.__class__.__name__.lower()
    is_code_node = 'code' in node_type or getattr(self, 'type', None) == 'code_node'
    
    # 发送扣费消息
    success = await self._credits_publisher.publish_node_credits(
        user_id=self.user_id,
        node_type='code_node' if is_code_node else 'regular_node',
        flow_id=self.flow_id,
        node_id=self.node_id,
        cycle=self.cycle,
        metadata={...}
    )
    
    return success
```

#### 修改 `start()` 方法

在节点执行成功后自动扣费：

```python
async def start(self) -> bool:
    # ... 执行节点逻辑
    success = await self.execute()
    
    # 设置状态
    await self.set_status(
        NodeStatus.COMPLETED if success else NodeStatus.FAILED
    )
    
    # ✨ 成功后扣费
    if success:
        await self.charge_credits()
        await self._auto_forward_input_handles()
    else:
        # 失败不扣费
        await self.send_stop_execution_signal(...)
```

---

## 💰 扣费规则

| 节点类型 | Credits 消耗 | 判断条件 |
|---------|-------------|---------|
| **Code Node** | 20 credits | 类名包含 'code' 或 `type='code_node'` |
| **Regular Node** | 10 credits | 其他所有节点 |

---

## 🔄 扣费时机

### 1. **成功执行后扣费**
- 节点 `execute()` 返回 `True`
- 状态设置为 `COMPLETED`
- **然后**调用 `charge_credits()`

### 2. **失败不扣费**
- 节点 `execute()` 返回 `False`
- 状态设置为 `FAILED`
- **不调用** `charge_credits()`

### 3. **异常不扣费**
- 节点执行抛出异常
- 异常被捕获，不会调用 `charge_credits()`

---

## 🚀 使用方式

### 方式 1: 自动扣费（推荐）

在创建 node 实例时传入 `user_id`：

```python
node = AIModelNode(
    flow_id="flow_123",
    component_id=1,
    cycle=0,
    node_id="node_ai_1",
    name="AI Model",
    user_id="user_abc123",      # 传入用户 ID
    enable_credits=True,         # 启用扣费（默认）
    # ... 其他参数
)

await node.start()
# 执行成功后自动扣费
```

### 方式 2: 禁用扣费

```python
node = SwapNode(
    # ... 参数
    user_id="user_abc123",
    enable_credits=False,        # ✨ 禁用扣费
)
```

### 方式 3: 无用户 ID（不扣费）

```python
node = BinanceNode(
    # ... 参数
    user_id=None,                # ✨ 无用户 ID，不扣费
)
```

---

## 🔍 日志示例

### 成功扣费

```
INFO - Credits publisher initialized for node node_ai_1
INFO - Node node_ai_1 execution completed, success=True
INFO - Credits charge message sent: user=user_abc123, node=node_ai_1, type=aimodelnode, cost=10
```

### 扣费跳过

```
WARNING - No user_id provided, skipping credits charge
```

### 扣费失败

```
ERROR - Failed to send credits charge message
ERROR - Error charging credits: Connection refused
```

---

## 📡 RabbitMQ 消息格式

### Exchange 配置

```yaml
Exchange Name: credits_exchange
Type: topic
Durable: true
Auto Delete: false
```

### Queue 配置（在 weather_control）

```yaml
Queue Name: credits.node
Binding Key: credits.node
Durable: true
```

### 消息示例

```json
{
  "userId": "507f1f77bcf86cd799439011",
  "nodeType": "code_node",
  "metadata": {
    "flowId": "flow_abc123",
    "nodeId": "node_code_1",
    "nodeName": "Python Code Executor",
    "nodeType": "codeexecutornode",
    "cycle": 0,
    "componentId": 1
  }
}
```

---

## 🔐 安全特性

### 1. **容错设计**
- Publisher 初始化失败不影响节点执行
- 扣费失败只记录日志，不中断流程

### 2. **幂等性**
- 单例模式确保只有一个 Publisher 实例
- 消息持久化，避免丢失

### 3. **权限验证**
- `user_id` 由外部传入（来自 API 请求）
- weather_control 端再次验证用户身份

### 4. **错误隔离**
- Credits 错误不会导致节点执行失败
- 详细日志记录，便于追踪问题

---

## 🧪 测试方式

### 单元测试

```python
import asyncio
from nodes.ai_model_node_v0_0_1 import AIModelNode

async def test_credits():
    node = AIModelNode(
        flow_id="test_flow",
        component_id=1,
        cycle=0,
        node_id="test_node",
        name="Test",
        user_id="test_user_123",
        enable_credits=True
    )
    
    await node.initialize_message_queue()
    
    # 手动调用扣费
    success = await node.charge_credits()
    
    print(f"Credits charged: {success}")

asyncio.run(test_credits())
```

### 集成测试

1. **启动 RabbitMQ**:
   ```bash
   docker run -d --name rabbitmq \
     -p 5672:5672 -p 15672:15672 \
     rabbitmq:3-management
   ```

2. **启动 weather_control**:
   ```bash
   cd 02_weather_control
   npm start
   ```

3. **运行节点**:
   ```bash
   cd 03_weather_station
   python -m nodes.test_node
   ```

4. **验证结果**:
   - 检查 RabbitMQ 管理界面 (http://localhost:15672)
   - 查看 `credits.node` 队列消息
   - 检查 weather_control 日志
   - 验证数据库中的 credits 交易记录

---

## 🐛 常见问题

### Q1: Credits publisher 初始化失败？

**原因**: RabbitMQ 连接失败

**解决方案**:
1. 检查 `RABBITMQ_URL` 环境变量
2. 确认 RabbitMQ 服务正在运行
3. 查看防火墙设置

### Q2: 扣费消息未被处理？

**原因**: weather_control 未启动或未监听队列

**解决方案**:
1. 确认 weather_control 正在运行
2. 检查 `CreditsQueueListener` 是否已初始化
3. 查看 RabbitMQ 队列消费者列表

### Q3: 节点执行成功但未扣费？

**原因**: 
- `enable_credits=False`
- `user_id` 未传入
- Publisher 初始化失败

**解决方案**:
1. 检查构造函数参数
2. 查看日志中的警告信息
3. 验证 RabbitMQ 连接

### Q4: 重复扣费？

**原因**: 节点被重复执行

**解决方案**:
- weather_control 端需要实现幂等性检查
- 使用 `flowId + nodeId + cycle` 组合作为唯一标识
- 在一定时间窗口内避免重复扣费

---

## 📊 监控指标

### 推荐监控项

1. **扣费成功率**: `credits_charge_success / total_executions`
2. **扣费失败率**: `credits_charge_failed / total_executions`
3. **平均扣费延迟**: MQ 消息发送到处理完成的时间
4. **队列堆积**: `credits.node` 队列中的消息数量

### 告警规则

- 扣费失败率 > 5%
- 队列堆积 > 1000 条消息
- Publisher 连接失败次数 > 10

---

## 🔮 未来优化

### 1. **批量扣费**
- 收集多个节点的扣费请求
- 批量发送到 MQ，减少网络开销

### 2. **本地缓存**
- 在 station 端缓存用户余额
- 预先判断是否有足够 credits
- 减少不必要的节点执行

### 3. **异步回调**
- 扣费结果通过 MQ 返回给 station
- 支持扣费失败的重试逻辑

### 4. **性能优化**
- 使用连接池管理 RabbitMQ 连接
- 消息压缩，减少带宽占用

---

## ✅ 总结

### 实现特点

✅ **解耦设计**: Station 和 Control 通过 MQ 通信，互不依赖  
✅ **自动扣费**: 节点执行成功后自动触发，无需手动调用  
✅ **容错机制**: 扣费失败不影响节点执行，降低耦合度  
✅ **灵活配置**: 支持启用/禁用扣费，适应不同场景  
✅ **详细日志**: 完整的执行轨迹，便于问题定位  
✅ **类型识别**: 自动区分 code_node 和 regular_node  

### 扣费流程

1. 节点初始化时接收 `user_id` 和 `enable_credits` 参数
2. 初始化 MQ 时创建 `CreditsPublisher` 单例
3. 节点执行 `execute()` 方法
4. 执行成功后调用 `charge_credits()`
5. 判断节点类型（code_node: 20, regular: 10）
6. 发送 JSON 消息到 RabbitMQ `credits_exchange`
7. weather_control 的 `CreditsQueueListener` 消费消息
8. 调用 `CreditsService.deductCredits()` 扣除用户 credits
9. 记录交易到 `CreditsTransaction` 集合

这个设计确保了 **高可用性、低耦合、易维护** 的 credits 扣费系统！🎉
