# 信号系统设计文档

## 概述

TradingFlow信号系统是一个基于RabbitMQ构建的健壮、多用户、多周期消息传递架构。它使交易工作流中的节点能够异步通信，同时确保不同用户和执行周期之间的适当隔离。

## 架构组件

### 核心组件

1. **NodeSignalPublisher** - 从节点发布信号
2. **NodeSignalConsumer** - 为节点消费信号  
3. **NodeBase** - 带有信号处理逻辑的基类
4. **Signal** - 表示信号的数据结构
5. **RabbitMQ Topic Exchange** - 消息路由基础设施

### 关键设计原则

- **多用户隔离**: 使用`flow_id`完全隔离每个用户的信号
- **多周期隔离**: 每个执行周期都有专用队列以防止跨周期干扰
- **灵活路由**: Topic交换机支持使用通配符的复杂路由模式
- **类型安全**: 信号载荷的自动类型转换和验证
- **错误恢复**: 全面的错误处理和信号验证

## 信号路由架构

### 路由键格式

所有信号都使用分层路由键格式：

```
flow.{flow_id}.component.{component_id}.cycle.{cycle}.from.{source_node}.handle.{source_handle}.to.{target_node}.handle.{target_handle}
```

**示例:**
```
flow.user123.component.comp456.cycle.1.from.node_a.handle.output.to.node_b.handle.input
```

### 队列命名约定

队列命名确保周期级隔离：

```
queue.flow.{flow_id}.cycle.{cycle}.node.{node_id}
```

**示例:**
```
queue.flow.user123.cycle.1.node.swap_node_001
```

### 路由键组件

| 组件 | 描述 | 示例 |
|------|------|------|
| `flow_id` | 用户/会话的唯一标识符 | `user123` |
| `component_id` | 交易组件标识符 | `comp456` |
| `cycle` | 执行周期号 | `1`, `2`, `3` |
| `source_node` | 发送信号的节点 | `price_node` |
| `source_handle` | 源节点的输出句柄 | `price_output` |
| `target_node` | 接收信号的节点 | `swap_node` |
| `target_handle` | 目标节点的输入句柄 | `price_input` |

## Topic Exchange 与通配符设计

### Topic Exchange 工作原理

TradingFlow使用RabbitMQ的**Topic Exchange**实现灵活的消息路由。Topic Exchange根据路由键模式匹配来路由消息，支持使用通配符进行复杂的路由配置。

#### Exchange 声明
```python
# 在 aio_pika_impl.py 中
self._exchange_type = "topic"

# 声明 topic exchange
topic_logs_exchange = await self._channel.declare_exchange(
    self._exchange, self._exchange_type
)
```

#### Topic Exchange 特性
1. **路由键匹配**: 基于点分隔的路由键进行精确或模式匹配
2. **通配符支持**: 支持 `*` (单词匹配) 和 `#` (多词匹配)
3. **多对多路由**: 一个消息可以路由到多个队列
4. **动态绑定**: 队列可以动态绑定到不同的路由键模式

### 通配符模式设计

#### 1. 单词通配符 (`*`)
`*` 匹配恰好一个单词（点分隔的段）

**发布端通配符使用：**
```python
# 在 node_signal_publisher.py 中
# 发送到多个目标句柄时使用通配符
routing_key = self._generate_send_signal_routing_key(
    source_handle, target_node, "*"  # 目标句柄使用通配符
)
```

**消费端通配符使用：**
```python
# 在 node_signal_consumer.py 中
# 接收来自任意源的信号
def _generate_receive_signal_routing_key(self):
    return f"flow.{self.flow_id}.component.{self.component_id}.cycle.{self.cycle}.from.*.handle.*.to.{self.node_id}.handle.*"
```

#### 2. 通配符匹配示例

| 路由键模式 | 匹配示例 | 不匹配示例 |
|-----------|----------|------------|
| `flow.user123.*.*.cycle.1.from.*.*` | `flow.user123.component.comp1.cycle.1.from.node_a.handle.output` | `flow.user456.component.comp1.cycle.1.from.node_a.handle.output` |
| `flow.*.component.*.cycle.*.from.price_node.handle.*` | `flow.user123.component.comp1.cycle.1.from.price_node.handle.output` | `flow.user123.component.comp1.cycle.1.from.swap_node.handle.output` |
| `*.*.*.*.*.*.to.swap_node.handle.*` | `flow.user123.component.comp1.cycle.1.from.price_node.handle.output.to.swap_node.handle.input` | `flow.user123.component.comp1.cycle.1.from.price_node.handle.output.to.calc_node.handle.input` |

#### 3. 实际路由场景

**场景1: 广播信号**
```python
# 发布者：向所有监听节点广播价格更新
routing_key = "flow.user123.component.trading.cycle.1.from.price_node.handle.broadcast.to.*.handle.price_input"

# 多个消费者绑定模式：
# 节点A: "flow.user123.component.trading.cycle.1.from.*.handle.*.to.swap_node.handle.price_input"
# 节点B: "flow.user123.component.trading.cycle.1.from.*.handle.*.to.calc_node.handle.price_input"
```

**场景2: 聚合信号**
```python
# 消费者：接收来自多个价格源的信号
binding_key = "flow.user123.component.trading.cycle.1.from.*.handle.*.to.aggregator.handle.price_input"

# 匹配的发布者：
# "flow.user123.component.trading.cycle.1.from.binance_price.handle.output.to.aggregator.handle.price_input"
# "flow.user123.component.trading.cycle.1.from.coinbase_price.handle.output.to.aggregator.handle.price_input"
```

**场景3: 条件路由**
```python
# 发布者：根据条件发送到不同处理节点
if trade_type == "spot":
    routing_key = "flow.user123.component.trading.cycle.1.from.router.handle.output.to.spot_handler.handle.input"
else:
    routing_key = "flow.user123.component.trading.cycle.1.from.router.handle.output.to.futures_handler.handle.input"

# 消费者使用通配符接收：
binding_key = "flow.user123.component.trading.cycle.1.from.router.handle.*.to.*.handle.input"
```

### 队列绑定机制

#### 动态队列绑定
```python
# 在 aio_pika_impl.py 中
for binding_key in binding_keys:
    await queue.bind(topic_logs_exchange, routing_key=binding_key)
    logger.debug(
        "Queue %s bound to routing key %s", queue_name, binding_key
    )
```

#### 多绑定支持
```python
# 一个队列可以绑定多个路由键模式
binding_keys = [
    "flow.user123.*.*.cycle.1.from.*.handle.*.to.node_a.handle.*",
    "flow.user123.*.*.cycle.1.from.emergency.handle.*.to.*.handle.*"
]
```

### 性能优化设计

#### 1. 路由键缓存
```python
# 避免重复生成路由键
class NodeSignalPublisher:
    def __init__(self):
        self._routing_key_cache = {}
    
    def _get_cached_routing_key(self, source_handle, target_node, target_handle):
        cache_key = f"{source_handle}:{target_node}:{target_handle}"
        if cache_key not in self._routing_key_cache:
            self._routing_key_cache[cache_key] = self._generate_send_signal_routing_key(
                source_handle, target_node, target_handle
            )
        return self._routing_key_cache[cache_key]
```

#### 2. 批量绑定优化
```python
# 批量绑定减少网络开销
async def batch_bind_queues(self, queue, binding_patterns):
    for pattern in binding_patterns:
        await queue.bind(self.exchange, routing_key=pattern)
```

## 信号流程

### 1. 信号发布

```python
# 在 NodeBase.send_signal() 中
signal = Signal(
    signal_type=signal_type,
    payload=payload,
    timestamp=None,
)
await self.node_signal_publisher.send_signal(source_handle, signal)
```

**发布者路由键生成：**
```python
def _generate_send_signal_routing_key(self, source_handle, target_node, target_handle):
    return f"flow.{self.flow_id}.component.{self.component_id}.cycle.{self.cycle}.from.{self.node_id}.handle.{source_handle}.to.{target_node}.handle.{target_handle}"
```

### 2. 信号消费

**消费者路由键模式：**
```python
def _generate_receive_signal_routing_key(self):
    return f"flow.{self.flow_id}.component.{self.component_id}.cycle.{self.cycle}.from.*.handle.*.to.{self.node_id}.handle.*"
```

**队列创建：**
```python
def _generate_queue_name(self):
    return f"queue.flow.{self.flow_id}.cycle.{self.cycle}.node.{self.node_id}"
```

### 3. 信号处理管道

1. **消息到达** → RabbitMQ投递到节点的专用队列
2. **路由键解析** → 提取flow_id、cycle、源/目标信息
3. **周期验证** → 拒绝来自不同周期的消息
4. **信号反序列化** → 将消息转换为Signal对象
5. **类型转换** → 将载荷转换为预期的数据类型
6. **句柄映射** → 将信号路由到适当的输入句柄
7. **聚合** → 如果句柄需要聚合则收集信号
8. **执行触发** → 当接收到所有必需信号时触发节点执行

## 多用户与多周期隔离

### 通过 flow_id 实现用户隔离

每个用户会话都有唯一的 `flow_id`：
- 用户A: `flow.userA.component.comp1.cycle.1...`  
- 用户B: `flow.userB.component.comp1.cycle.1...`

路由键确保完全分离 - 用户A的信号无法到达用户B的节点。

### 通过专用队列实现周期隔离

每个执行周期创建全新的队列：
- 周期 1: `queue.flow.user123.cycle.1.node.swap_node`
- 周期 2: `queue.flow.user123.cycle.2.node.swap_node`

**周期验证：**
```python
async def _handle_message(self, message):
    routing_key_parts = message.routing_key.split('.')
    message_cycle = int(routing_key_parts[5])  # 从路由键提取周期
    
    if message_cycle != self.cycle:
        self.logger.warning(f"忽略来自不同周期的消息: {message_cycle} != {self.cycle}")
        await message.ack()
        return
```

## 输入/输出句柄系统

### 句柄注册

节点使用元数据注册输入句柄：

```python
def register_input_handle(self, name: str, data_type: type, description: str = "", 
                         example: Any = None, auto_update_attr: str = None, 
                         is_aggregate: bool = False):
    self._input_handles[name] = InputHandle(
        name=name,
        data_type=data_type, 
        description=description,
        example=example,
        auto_update_attr=auto_update_attr,
        is_aggregate=is_aggregate,
    )
```

### 信号聚合

对于标记为 `is_aggregate=True` 的句柄：
- 从多个源收集信号
- 在触发执行之前等待所有预期的信号
- 支持基于键和基于列表的聚合

### 自动转发

输入句柄可以自动转发到输出：
```python
async def _auto_forward_input_handles(self):
    for output_edge in self._output_edges:
        source_handle = output_edge.source_node_handle
        if source_handle in self.get_input_handle_names():
            input_handle_data = self.get_input_handle_data(source_handle)
            await self.send_signal(
                source_handle=source_handle,
                signal_type=SignalType.ANY,
                payload=input_handle_data,
            )
```

## 类型转换系统

信号系统支持自动类型转换：

### 支持的类型
- **简单类型**: `str`, `int`, `float`, `bool`
- **复杂类型**: `dict`, `list`, 自定义对象
- **JSON字符串**: 自动解析和字段提取

### 转换过程
```python
def _convert_signal_data_by_type(self, raw_value, expected_type, handle_name):
    # 1. 直接类型匹配 - 原样返回
    if isinstance(raw_value, expected_type):
        return raw_value
    
    # 2. 复杂类型处理
    if expected_type not in [str, int, float, bool]:
        return raw_value
    
    # 3. JSON字符串解析
    if isinstance(raw_value, str):
        try:
            parsed_json = json.loads(raw_value)
            # 如果JSON字典包含句柄名则提取字段
            if isinstance(parsed_json, dict) and handle_name in parsed_json:
                return self._convert_to_type(parsed_json[handle_name], expected_type)
        except json.JSONDecodeError:
            pass
    
    # 4. 直接类型转换
    return self._convert_to_type(raw_value, expected_type)
```

## 错误处理与恢复性

### 停止执行信号

优雅终止的特殊系统：
```python
async def send_stop_execution_signal(self, reason: str = "执行停止"):
    await self.node_signal_publisher.send_stop_execution_signal(reason)
```

**停止信号路由键：**
```
flow.{flow_id}.component.{component_id}.cycle.{cycle}.stop_execution
```

### 错误恢复

1. **消息确认**: 仅在成功处理后确认消息
2. **周期验证**: 优雅地拒绝非当前周期的消息  
3. **类型转换回退**: 如果转换失败则返回原始值
4. **连接恢复**: 通过连接池实现RabbitMQ的自动重连

## 性能考虑

### 连接池
- 跨节点共享RabbitMQ连接
- 高效的通道管理
- 自动重连处理

### 消息TTL
- 可配置的消息过期时间
- 防止失败周期造成队列堆积

### 队列生命周期
- 每个周期创建队列
- 周期完成后自动清理
- 防止资源泄露

## 使用示例

### 基本信号发送
```python
class PriceNode(NodeBase):
    async def execute(self):
        price_data = await self.fetch_price()
        await self.send_signal(
            source_handle="price_output",
            signal_type=SignalType.DATA,
            payload={"price": price_data, "timestamp": time.time()}
        )
```

### 带聚合的信号消费
```python
class SwapNode(NodeBase):
    def _register_input_handles(self):
        self.register_input_handle(
            name="price_input",
            data_type=dict,
            description="来自价格节点的价格数据",
            auto_update_attr="price_data",
            is_aggregate=False
        )
        
        self.register_input_handle(
            name="pool_data",
            data_type=dict, 
            description="池信息",
            auto_update_attr="pool_info",
            is_aggregate=True  # 等待多个池源
        )
```

### 通配符路由
```python
# 发布者发送到多个目标
routing_key = f"flow.{flow_id}.component.{component_id}.cycle.{cycle}.from.{node_id}.handle.broadcast.to.*.handle.notification"

# 消费者监听任意源
routing_key = f"flow.{flow_id}.component.{component_id}.cycle.{cycle}.from.*.handle.*.to.{node_id}.handle.notification"
```

## 最佳实践

### 1. 句柄命名
- 使用描述性、一致的名称
- 遵循 `snake_case` 约定
- 在描述中包含数据类型提示

### 2. 信号载荷设计
- 保持载荷轻量级
- 对复杂信息使用结构化数据（字典）
- 为时间相关数据包含时间戳

### 3. 错误处理
- 处理前始终验证信号数据
- 为缺失信号实现优雅降级
- 记录信号流用于调试

### 4. 资源管理
- 在节点清理方法中清理资源
- 对RabbitMQ使用连接池
- 监控队列大小和消息速率

### 5. 测试
- 使用不同flow_ids测试多用户场景
- 验证重叠执行的周期隔离
- 测试错误条件和恢复路径

## 故障排除

### 常见问题

1. **跨周期信号泄露**
   - **症状**: 节点接收到来自前一周期的信号
   - **解决方案**: 验证队列命名包含周期号

2. **多用户干扰** 
   - **症状**: 用户看到彼此的信号
   - **解决方案**: 确保路由键中正确包含flow_id

3. **信号类型不匹配**
   - **症状**: 日志中出现类型转换错误
   - **解决方案**: 验证句柄注册的数据类型与信号载荷匹配

4. **信号丢失**
   - **症状**: 节点无限期等待信号
   - **解决方案**: 检查路由键模式和边配置

### 调试工具

1. **信号日志**: 启用调试日志进行信号流跟踪
2. **RabbitMQ管理界面**: 监控队列、交换机和消息速率
3. **Redis检查**: 检查节点状态和信号存储
4. **路由键验证**: 验证路由键生成逻辑

## 未来增强

### 计划改进
1. **消息TTL配置**: 每种信号类型可配置的消息过期时间
2. **队列生命周期管理**: 自动清理未使用的队列
3. **信号指标**: 全面的监控和告警
4. **批量信号处理**: 高效处理大量信号
5. **信号重放**: 为调试和测试重放信号的能力

### 性能优化
1. **连接池增强**: 更好的资源利用
2. **路由键缓存**: 减少字符串生成开销
3. **并行信号处理**: 并发处理独立信号
4. **消息压缩**: 减少大载荷的网络开销

---

本文档提供了信号系统设计的全面概述。有关实现细节，请参考 `node_base.py`、`node_signal_publisher.py` 和 `node_signal_consumer.py` 中的源代码。
