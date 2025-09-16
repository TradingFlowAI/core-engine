# SwapNode 交换节点文档

## 概述

`swap_node.py` 实现了 TradingFlow 系统的多链代币交换功能，支持 Aptos 和 Flow EVM 两条区块链。该文件包含三个主要节点类：基础交换节点 `SwapNode` 和两个专用实例节点 `BuyNode`、`SellNode`。

## 核心特性

### 🚀 多链支持

- **Aptos**: 集成 Hyperion DEX，支持动态池子搜索
- **Flow EVM**: 支持 EVM 兼容的交换协议

### 🎯 动态池子搜索 (新增功能)

- 自动查找最优交易池子，替代硬编码费率等级
- 按流动性和价格合理性筛选池子
- 支持多费率等级智能选择

### 📊 智能价格估算

- 实时获取代币价格进行交换预估
- 自动计算滑点保护和最小输出金额
- Aptos 链支持 sqrt_price_limit 计算

---

## 1. 基础工具函数

### `calculate_sqrt_price_limit_q64_64()`

**功能**: 计算 Hyperion DEX 的价格影响限制参数

**输入参数**:

- `input_price` (float): 输入代币价格
- `output_price` (float): 输出代币价格
- `slippage_tolerance` (float): 滑点容忍度百分比

**输出**:

- `str`: x64 定点格式的 sqrt_price_limit 值

**关键逻辑**:

1. 计算价格比率并应用滑点
2. 转换为 Hyperion 的 x64 定点格式
3. 应用合理边界限制 (最小值: 4295128740)

---

## 2. SwapNode (基础交换节点)

### 节点信息

- **节点类型**: `swap_node`
- **版本**: `0.0.2`
- **显示名称**: "Swap Node"
- **节点类别**: `instance`

### 输入参数

| 参数名                     | 类型   | 描述                   | 示例                |
| -------------------------- | ------ | ---------------------- | ------------------- |
| `chain`                    | string | 区块链网络             | "aptos", "flow_evm" |
| `from_token`               | string | 源代币符号             | "USDT"              |
| `to_token`                 | string | 目标代币符号           | "BTC"               |
| `vault_address`            | string | Vault 合约地址         | "0x6a1a233e..."     |
| `amount_in_percentage`     | number | 交易金额百分比 (0-100) | 25.0                |
| `amount_in_human_readable` | number | 人类可读金额           | 100.0               |
| `slippery`                 | number | 滑点容忍度百分比       | 1.0                 |

### 输出信号

- **`trade_receipt`**: 交易收据 (JSON 对象)

### 核心方法

#### `_resolve_token_addresses()`

**功能**: 根据代币符号解析具体地址

- Aptos: 通过符号查找映射地址
- Flow EVM: 使用符号作为地址占位符
- 获取代币元数据（精度等信息）

#### `find_best_pool()` 🆕 动态池子搜索

**功能**: 为 Aptos 链动态查找最优交易池子

**搜索策略**:

```
费率优先级: [1, 2, 0, 3] → [0.05%, 0.3%, 0.01%, 1%]
```

**流程**:

1. 调用 Monitor API `/aptos/pools/pair` 查询池子
2. 检查池子流动性和价格合理性 (`_is_pool_suitable()`)
3. 返回第一个符合条件的池子信息

**输出**:

```json
{
  "pool_info": {
    /* 池子详细信息 */
  },
  "fee_tier": 1,
  "token1": "token_address_1",
  "token2": "token_address_2"
}
```

#### `_is_pool_suitable()`

**功能**: 验证池子是否适合交易

**检查项**:

- 流动性 > 1000 (最小要求)
- sqrtPrice > 0 且合理
- 必需字段完整性

#### `execute_swap()`

**功能**: 执行实际的代币交换

**Aptos 链流程**:

1. 🔍 动态搜索最优池子
2. 📊 价格估算和 sqrt_price_limit 计算
3. 🚀 调用 `admin_execute_swap()` 执行交换

**Flow EVM 链流程**:

1. 📊 EVM 价格估算
2. 🚀 调用 `execute_swap()` 执行交换

#### `prepare_trade_receipt()`

**功能**: 生成统一格式的交易收据

**基础字段**:

- `success`, `message`, `chain`
- `vault_address`, `from_token`, `to_token`
- `slippery`, `raw_result`

**链特定字段**:

- Aptos: `tx_hash`, `dex_name: "hyperion"`, `gas_used`
- Flow EVM: `tx_hash`, `dex_name: "flow_evm"`, `gas_used`

### 执行流程

```
开始 → 解析代币地址 → 搜索最优池子(Aptos) → 计算交换金额
→ 价格估算 → 执行交换 → 生成收据 → 发送信号 → 完成
```

---

## 3. BuyNode (买入节点)

### 节点信息

- **节点类型**: `buy_node`
- **基础节点**: `swap_node`
- **显示名称**: "Buy Node"
- **专用用途**: 代币买入操作

### 参数映射逻辑

```
from_token = base_token  (支付代币)
to_token = buy_token     (买入代币)
```

### 专用输入参数

| 参数名          | 类型   | 描述               | 示例              |
| --------------- | ------ | ------------------ | ----------------- |
| `buy_token`     | string | 要买入的代币符号   | "BTC"             |
| `base_token`    | string | 用于支付的基础代币 | "USDT"            |
| `order_type`    | string | 订单类型           | "market", "limit" |
| `limited_price` | number | 限价订单价格       | 50000.0           |

### 特化功能

- 自动处理买入逻辑的参数映射
- 支持市价单和限价单
- 基于基础代币余额的百分比计算

---

## 4. SellNode (卖出节点)

### 节点信息

- **节点类型**: `sell_node`
- **基础节点**: `swap_node`
- **显示名称**: "Sell Node"
- **专用用途**: 代币卖出操作

### 参数映射逻辑

```
from_token = sell_token  (卖出代币)
to_token = base_token    (换取代币)
```

### 专用输入参数

| 参数名          | 类型   | 描述             | 示例              |
| --------------- | ------ | ---------------- | ----------------- |
| `sell_token`    | string | 要卖出的代币符号 | "BTC"             |
| `base_token`    | string | 换取的基础代币   | "USDT"            |
| `order_type`    | string | 订单类型         | "market", "limit" |
| `limited_price` | number | 限价订单价格     | 45000.0           |

### 特化功能

- 自动处理卖出逻辑的参数映射
- 支持市价单和限价单
- 基于卖出代币余额的百分比计算

---

## 5. 关键依赖和服务

### 外部服务

- **AptosVaultService**: Aptos 链 Vault 操作
- **FlowEvmVaultService**: Flow EVM 链 Vault 操作
- **Monitor API**: 池子信息查询服务

### 价格服务

- `get_aptos_token_price_usd_async()`: Aptos 代币价格
- `get_flow_evm_token_prices_usd()`: Flow EVM 代币价格

### 配置依赖

- `MONITOR_URL`: Monitor 服务地址 (动态池子搜索必需)

---

## 6. 错误处理和容错

### 网络容错

- HTTP 请求 10 秒超时保护
- 池子搜索失败时回退到默认 fee_tier=1
- API 错误时自动尝试下一个费率等级

### 参数验证

- 代币地址解析失败检查
- 金额范围验证 (百分比: 0-100, 金额: >0)
- 支持的链类型验证

### 日志记录

- 完整的执行过程日志
- 错误堆栈跟踪
- 池子搜索详细信息

---

## 7. 使用示例

### 基础 SwapNode 使用

```python
# 创建交换节点
swap_node = SwapNode(
    flow_id="flow_123",
    component_id=1,
    cycle=1,
    node_id="swap_1",
    name="USDT->BTC Swap",
    chain="aptos",
    from_token="USDT",
    to_token="BTC",
    vault_address="0x6a1a233e...",
    amount_in_percentage=25.0,
    slippery=1.0
)

# 执行交换
success = await swap_node.execute()
```

### BuyNode 使用

```python
# 创建买入节点
buy_node = BuyNode(
    flow_id="flow_123",
    component_id=2,
    cycle=1,
    node_id="buy_1",
    name="Buy BTC",
    buy_token="BTC",
    base_token="USDT",
    chain="aptos",
    vault_address="0x6a1a233e...",
    amount_in_percentage=50.0,
    order_type="market"
)
```

### SellNode 使用

```python
# 创建卖出节点
sell_node = SellNode(
    flow_id="flow_123",
    component_id=3,
    cycle=1,
    node_id="sell_1",
    name="Sell BTC",
    sell_token="BTC",
    base_token="USDT",
    chain="aptos",
    vault_address="0x6a1a233e...",
    amount_in_percentage=30.0,
    order_type="limit",
    limited_price=45000.0
)
```

---

## 8. 技术架构

### 设计模式

- **继承模式**: BuyNode/SellNode 继承 SwapNode
- **工厂模式**: 根据链类型选择对应的 VaultService
- **策略模式**: 不同链使用不同的价格估算策略

### 异步处理

- 全异步 API 调用
- 并发安全的信号处理
- 异步日志记录

### 扩展性

- 新链集成: 添加对应的 VaultService 和价格服务
- 新 DEX 支持: 扩展池子搜索和交换逻辑
- 新订单类型: 扩展 BuyNode/SellNode 的订单处理

---

## 9. 版本历史

### v0.0.2 (当前版本)

- ✅ 添加动态池子搜索功能
- ✅ 支持 Aptos 和 Flow EVM 多链
- ✅ 实现 BuyNode 和 SellNode 实例节点
- ✅ 完整的错误处理和日志系统
- ✅ 统一的交易收据格式

### 主要改进

- 从硬编码 fee_tier 升级为动态池子发现
- 集成 Monitor API 实现实时池子查询
- 提供专用的买卖节点简化用户操作

---

## 10. 注意事项

### 配置要求

- 确保 `MONITOR_URL` 环境变量正确配置
- Vault 地址必须是有效的合约地址
- 代币符号需要在系统中已注册

### 性能考虑

- 动态池子搜索会增加延迟，但提高交换成功率
- 建议在生产环境中启用池子信息缓存
- HTTP 超时设置为 10 秒，适合大多数网络环境

### 安全建议

- 滑点设置不宜过大，推荐 1-3%
- 大额交易前建议先进行小额测试
- 定期验证 Vault 合约的安全性

#### CICD Tests
