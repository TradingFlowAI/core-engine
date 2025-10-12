# Credits 同步扣费实现指南

> 本文档说明如何使用同步 HTTP 扣费机制，以及如何测试余额不足时停止 flow 的功能。

---

## 📋 目录

1. [实现概览](#实现概览)
2. [架构变更](#架构变更)
3. [配置说明](#配置说明)
4. [测试指南](#测试指南)
5. [故障排查](#故障排查)

---

## 实现概览

### ✅ 已完成的改动

#### 1. **新增异常类型** - `InsufficientCreditsException`

```python
# 05_weather_depot/python/exceptions/tf_exception.py
class InsufficientCreditsException(NodeExecutionException):
    """余额不足异常 - 用于在 credits 余额不足时停止 flow 执行"""
    
    def __init__(
        self,
        message: str,
        node_id: str = None,
        user_id: str = None,
        required_credits: int = None,
        current_balance: int = None,
    ):
        super().__init__(message, node_id, "insufficient_credits")
        self.user_id = user_id
        self.required_credits = required_credits
        self.current_balance = current_balance
```

#### 2. **weather_control 同步扣费 API**

```javascript
// POST /api/v1/credits/charge
// 请求示例
{
  "userId": "user123",
  "amount": 10,
  "nodeId": "node_abc",
  "nodeType": "regular_node",
  "flowId": "flow_xyz",
  "cycle": 1,
  "metadata": {
    "nodeName": "My Node",
    "nodeType": "binance_price_node"
  }
}

// 成功响应 (200 OK)
{
  "success": true,
  "data": {
    "charged": 10,
    "balance": 990,
    "remainingCredits": 990
  }
}

// 余额不足响应 (402 Payment Required)
{
  "success": false,
  "error": "Insufficient credits. Required: 10, Available: 5",
  "balance": 5,
  "code": "INSUFFICIENT_CREDITS"
}
```

#### 3. **node_base.py 同步扣费**

```python
async def _charge_credits_sync(self) -> None:
    """
    同步扣费 - 调用 weather_control HTTP API
    
    Raises:
        InsufficientCreditsException: 余额不足时抛出
    """
    # 调用 HTTP API
    response = await client.post(
        f"{weather_control_url}/api/v1/credits/charge",
        json={...}
    )
    
    # 检查余额不足
    if response.status_code == 402:
        raise InsufficientCreditsException(...)
```

**执行顺序变更**：
```python
async def start(self):
    # 1. 初始化
    await self.initialize_state_store()
    await self.initialize_message_queue()
    
    # 2. 等待信号（如果需要）
    if self._input_edges:
        await self._signal_ready_future
    
    # 3. 先扣费（新增）
    await self._charge_credits_sync()  # ⭐ 在执行前扣费
    
    # 4. 再执行
    success = await self.execute()
    
    # 5. 转发信号
    if success:
        await self._auto_forward_input_handles()
```

#### 4. **node_executor.py 异常处理**

```python
except InsufficientCreditsException as e:
    # 标记为 TERMINATED
    await _update_node_status(
        node_task_id,
        NodeStatus.TERMINATED,
        f"Insufficient credits: {e.message}",
        {...}
    )
    
    # 发送停止信号到整个 component
    if node_instance:
        await node_instance.send_stop_execution_signal(
            reason="insufficient_credits",
            metadata={...}
        )
```

---

## 架构变更

### **之前：异步 MQ 扣费** ❌

```
Node 执行完成 → 发送消息到 MQ → weather_control 消费 → 扣费
          ↓ (不等待)
      已经执行完 ❌ 无法阻止后续 node
```

**问题**：
- 无法实时知道余额是否充足
- Node 执行完才扣费（事后扣费）
- 无法在余额不足时停止 flow

### **现在：同步 HTTP 扣费** ✅

```
Node 开始 → 先扣费（HTTP 同步调用）→ 检查余额 → 扣费成功 → 执行 node
        ↓                              ↓
     等待结果                     余额不足 ❌
                                      ↓
                        抛 InsufficientCreditsException
                                      ↓
                          node_executor 捕获异常
                                      ↓
                         标记为 TERMINATED + 发送停止信号
                                      ↓
                         后续 node 收到停止信号，跳过执行 ✅
```

**优点**：
- ✅ 实时反馈扣费结果
- ✅ 余额不足时立即停止
- ✅ 架构更简单（不需要 RabbitMQ）

---

## 配置说明

### 1. **环境变量配置**

在 weather_station 的配置中添加：

```bash
# 03_weather_station/.env 或 config.py
WEATHER_CONTROL_URL=http://localhost:8000
```

### 2. **Node 执行时传递 user_id**

确保在创建 node 时传递 `user_id` 参数：

```python
# flow/scheduler.py - _execute_node 方法
node_data = {
    "flow_id": flow_id,
    "component_id": component_id,
    "cycle": cycle,
    "node_id": node_id,
    "node_type": node_type,
    "input_edges": input_edges,
    "output_edges": output_edges,
    "config": {
        **node_config,
        "user_id": flow_config.get("user_id"),  # ⭐ 传递 user_id
        "enable_credits": True
    }
}
```

### 3. **扣费标准**

| Node 类型 | Credits 费用 |
|----------|-------------|
| 普通 Node | 10 credits |
| Code Node | 20 credits |

判断逻辑：
- 类名包含 `code` 或 `type` 属性为 `code_node` → Code Node (20 credits)
- 其他 → 普通 Node (10 credits)

---

## 测试指南

### 测试场景 1: 正常扣费流程

**测试步骤**：

1. **准备测试用户**
   ```bash
   # 确保用户有足够余额（如 1000 credits）
   ```

2. **创建简单 Flow**
   ```javascript
   {
     "user_id": "test_user_123",
     "interval": "0",  // 只执行一次
     "nodes": [
       {
         "id": "node_a",
         "type": "binance_price_node",
         "config": {...}
       },
       {
         "id": "node_b",
         "type": "regular_node",
         "config": {...}
       }
     ],
     "edges": [
       {"source": "node_a", "target": "node_b"}
     ]
   }
   ```

3. **执行 Flow**
   ```bash
   # 启动 weather_station
   python main.py
   
   # 注册并启动 flow
   curl -X POST http://localhost:5000/api/flows/register \
     -H "Content-Type: application/json" \
     -d @flow_config.json
   ```

4. **验证结果**
   - ✅ Node A 执行前扣费 10 credits
   - ✅ Node B 执行前扣费 10 credits
   - ✅ 总共扣费 20 credits
   - ✅ 用户余额：1000 - 20 = 980 credits

---

### 测试场景 2: 余额不足时停止 Flow ⭐

**测试步骤**：

1. **准备测试用户**
   ```bash
   # 设置用户余额为 15 credits（只够执行 1 个 node）
   ```

2. **创建包含 3 个 Node 的 Flow**
   ```javascript
   {
     "user_id": "test_user_123",
     "interval": "0",
     "nodes": [
       {
         "id": "node_a",
         "type": "regular_node",  // 10 credits
         "config": {...}
       },
       {
         "id": "node_b",
         "type": "regular_node",  // 10 credits
         "config": {...}
       },
       {
         "id": "node_c",
         "type": "regular_node",  // 10 credits - 会失败
         "config": {...}
       }
     ],
     "edges": [
       {"source": "node_a", "target": "node_b"},
       {"source": "node_b", "target": "node_c"}
     ]
   }
   ```

3. **执行 Flow**

4. **验证结果**
   - ✅ Node A: 扣费 10 credits，执行成功，余额 = 5
   - ✅ Node B: 扣费 10 credits **失败**（余额不足）
   - ✅ Node B: 抛出 `InsufficientCreditsException`
   - ✅ Node B: 状态 = `TERMINATED`
   - ✅ Node B: 发送停止信号到 component
   - ✅ Node C: 收到停止信号，状态 = `SKIPPED`
   - ✅ Flow 停止执行

5. **检查日志**
   ```
   [INFO] Node node_a: Credits charged successfully: cost=10, remaining=5
   [INFO] Node node_a execution completed, success=True
   [ERROR] Node node_b: Insufficient credits: required=10, balance=5
   [ERROR] Node node_b terminated due to insufficient credits
   [INFO] Stop signal sent for component due to insufficient credits
   [INFO] Component XXX has been marked as stopped, skipping execution of node node_c
   ```

---

### 测试场景 3: Code Node 扣费

**测试步骤**：

1. **创建包含 Code Node 的 Flow**
   ```javascript
   {
     "user_id": "test_user_123",
     "nodes": [
       {
         "id": "code_node_1",
         "type": "code_node",  // 20 credits
         "config": {
           "node_class_type": "code_node",
           "code": "print('hello')"
         }
       }
     ]
   }
   ```

2. **执行并验证**
   - ✅ 扣费 20 credits（而不是 10）

---

### 测试场景 4: 并发执行多个 Node

**测试步骤**：

1. **创建并行执行的 Flow**
   ```javascript
   {
     "user_id": "test_user_123",
     "nodes": [
       {"id": "node_a"},
       {"id": "node_b"},  // 与 node_a 并行
       {"id": "node_c"}   // 与 node_a, node_b 并行
     ],
     "edges": []  // 无依赖，并行执行
   }
   ```

2. **验证**
   - ✅ 3 个 node 同时扣费
   - ✅ 总共扣费 30 credits
   - ✅ 不会重复扣费

---

## 故障排查

### 问题 1: `WEATHER_CONTROL_URL not configured`

**症状**：
```
[ERROR] Error charging credits: Connection error
```

**解决方案**：
```bash
# 检查配置
echo $WEATHER_CONTROL_URL

# 或在 config.py 中设置
WEATHER_CONTROL_URL = "http://localhost:8000"
```

---

### 问题 2: `Credits service timeout`

**症状**：
```
[ERROR] Timeout charging credits: HTTPTimeout
```

**原因**：
- weather_control 服务未启动
- 网络延迟超过 5 秒

**解决方案**：
```bash
# 1. 确保 weather_control 运行
cd 02_weather_control
npm run dev

# 2. 检查连接
curl http://localhost:8000/api/v1/credits/costs

# 3. 调整超时时间（如果需要）
# node_base.py
async with httpx.AsyncClient(timeout=10.0) as client:  # 改为 10 秒
```

---

### 问题 3: Node 执行了但没有扣费

**检查清单**：

1. **是否传递了 user_id？**
   ```python
   # 检查 node config
   config = {
       "user_id": "...",  # 必须有
       "enable_credits": True
   }
   ```

2. **enable_credits 是否为 True？**
   ```python
   # node_base.py
   if not self.enable_credits:
       return  # 不会扣费
   ```

3. **检查日志**
   ```
   [DEBUG] Credits tracking is disabled for node XXX
   [WARNING] No user_id provided, skipping credits charge
   ```

---

### 问题 4: 余额不足但 Flow 没有停止

**检查**：

1. **异常是否被正确抛出？**
   ```python
   # node_base.py - _charge_credits_sync
   if response.status_code == 402:
       raise InsufficientCreditsException(...)  # 确保抛出
   ```

2. **node_executor 是否捕获异常？**
   ```python
   # node_executor.py
   except InsufficientCreditsException as e:
       # 应该有这个处理逻辑
   ```

3. **停止信号是否发送？**
   ```
   [INFO] Stop signal sent for component due to insufficient credits
   ```

4. **后续 node 是否检查停止状态？**
   ```python
   # scheduler.py - _execute_node
   if await self.is_component_stopped(flow_id, cycle, component_id):
       return {"status": "skipped", "reason": "component_stopped"}
   ```

---

## 总结

### ✅ 改动清单

- [x] 新增 `InsufficientCreditsException` 异常类型
- [x] weather_control 添加 `POST /api/v1/credits/charge` API
- [x] CreditsService 添加 `chargeNodeExecution` 方法
- [x] node_base.py 实现 `_charge_credits_sync` 方法
- [x] node_base.py 在执行前调用同步扣费
- [x] node_executor.py 添加 `InsufficientCreditsException` 处理
- [x] 删除 `credits_publisher.py`（MQ 组件）
- [x] 删除 `CreditsQueueListener.js`（MQ 消费者）
- [x] 更新 `subscription/index.js` 移除 MQ 引用

### 🎯 核心逻辑

**余额充足**：
```
扣费成功 → 执行 node → 转发信号 → 后续 node 继续
```

**余额不足**：
```
扣费失败 → 抛异常 → 标记 TERMINATED → 发送停止信号 → 后续 node 跳过 ✅
```

---

最后更新：2025-10-08
