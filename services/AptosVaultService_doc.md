# AptosVaultService 类文档

## 概述

`AptosVaultService` 是 TradingFlow 系统中用于管理 Aptos 区块链上 Vault 合约的核心服务类。该类提供了完整的 Vault 生命周期管理功能，包括部署、交易执行和查询功能。采用单例工厂模式确保每个链ID始终返回相同的实例。

## 主要功能

- 🏦 **余额管理器检查**: 验证投资者是否已创建余额管理器
- 💰 **资产持仓查询**: 获取投资者在 Vault 中的代币持仓情况
- 📊 **事件历史记录**: 查询投资者的存款、提款、交换等操作记录  
- 🔄 **管理员交换执行**: 管理员代表用户执行代币交换交易
- 🏷️ **代币元数据获取**: 从 monitor 服务获取代币基本信息
- 💲 **价格集成**: 集成价格数据计算 USD 价值
- 📋 **投资组合分析**: 计算投资组合构成和统计信息

## 类属性

- `_instances`: 类变量，存储已创建的实例（单例模式）
- `_monitor_url`: Monitor 服务的基础 URL
- `_client`: 异步 HTTP 客户端实例

## API 端点常量

```python
VAULT_API_EVENTS_URI = "/aptos/vault/events/{address}"
VAULT_API_HOLDINGS_URI = "/aptos/vault/holdings/{address}"  
VAULT_API_TRADE_SIGNAL_URI = "/aptos/vault/trade-signal"
VAULT_API_BALANCE_MANAGER_URI = "/aptos/vault/balance-manager/{address}"
```

## 方法详解

### 工厂方法

#### `get_instance() -> AptosVaultService`
**说明**: 获取 AptosVaultService 单例实例

**返回值**: 
- `AptosVaultService`: 服务实例

**特点**: 
- 单例模式，确保全局只有一个实例
- 线程安全

---

### 初始化方法

#### `__init__(monitor_url=MONITOR_URL)`
**说明**: 初始化 AptosVaultService 实例

**参数**:
- `monitor_url` (str): Monitor 服务的基础 URL

**功能**:
- 初始化 HTTP 客户端
- 设置 30 秒超时

---

### 余额管理器相关

#### `check_balance_manager(investor_address: str) -> Dict[str, any]`
**说明**: 检查投资者是否已创建余额管理器

**参数**:
- `investor_address` (str): 投资者地址

**返回值**:
```json
{
    "address": "0x...",
    "balance_manager_created": true/false
}
```

**异常**:
- `httpx.HTTPStatusError`: HTTP 请求失败
- `httpx.RequestError`: 网络请求错误

#### `has_balance_manager(investor_address: str) -> bool`
**说明**: 简化的检查方法，只返回布尔值

**参数**:
- `investor_address` (str): 投资者地址

**返回值**:
- `bool`: True 表示已创建余额管理器

---

### 资产持仓查询

#### `get_investor_holdings(investor_address: str) -> Dict[str, any]`
**说明**: 获取指定投资者地址的 Vault 持有资产

**参数**:
- `investor_address` (str): 投资者地址

**返回值**:
```json
{
    "address": "0x...",
    "holdings": [
        {
            "token_address": "0xa",
            "token_name": "Aptos Coin", 
            "token_symbol": "APT",
            "amount": "50000",
            "decimals": 8
        }
    ]
}
```

#### `get_token_holdings(investor_address: str, token_address: Optional[str] = None) -> List[Dict[str, any]]`
**说明**: 获取特定代币的持仓或所有持仓

**参数**:
- `investor_address` (str): 投资者地址
- `token_address` (Optional[str]): 代币地址，可选

**返回值**:
- `List[Dict[str, any]]`: 持仓列表

---

### 事件历史记录

#### `get_investor_events(investor_address: str, event_type: Optional[str] = None) -> Dict[str, any]`
**说明**: 获取指定地址的投资者事件（从数据库获取）

**参数**:
- `investor_address` (str): 投资者地址
- `event_type` (Optional[str]): 事件类型，可选 (DEPOSIT, WITHDRAW, SWAP)

**返回值**:
```json
{
    "address": "0x...",
    "events": [
        {
            "vault_address": "0x...",
            "transaction_hash": "2799858519",
            "operation_type": "SWAP",
            "input_token_address": "0xa",
            "input_token_amount": "50000",
            "output_token_address": "0x...",
            "output_token_amount": "2322",
            "created_at": "2025-05-31T10:08:02.560Z",
            "updated_at": "2025-05-31T10:08:02.560Z"
        }
    ]
}
```

#### `get_events_by_type(investor_address: str, operation_type: str) -> List[Dict[str, any]]`
**说明**: 根据操作类型获取事件

**参数**:
- `investor_address` (str): 投资者地址
- `operation_type` (str): 操作类型 (DEPOSIT, WITHDRAW, SWAP)

**返回值**:
- `List[Dict[str, any]]`: 指定类型的事件列表

---

### 综合数据查询

#### `get_vault_summary(investor_address: str) -> Dict[str, any]`
**说明**: 获取投资者的完整 Vault 摘要信息，包括持仓和事件

**参数**:
- `investor_address` (str): 投资者地址

**返回值**:
```json
{
    "address": "0x...",
    "holdings": [...],
    "events": [...],
    "summary_stats": {
        "total_tokens": 3,
        "total_events": 10,
        "event_types": {"SWAP": 5, "DEPOSIT": 3, "WITHDRAW": 2},
        "unique_vaults": ["0x..."],
        "unique_vault_count": 1
    }
}
```

---

### 交易执行

#### `admin_execute_swap(...) -> Dict[str, any]`
**说明**: 管理员执行 swap 交易

**参数**:
- `user_address` (str): 用户地址
- `from_token_metadata_id` (str): 输入代币元数据ID
- `to_token_metadata_id` (str): 输出代币元数据ID
- `amount_in` (int): 输入金额（已经乘以decimals的整数）
- `fee_tier` (int): 费用等级，默认为1
- `amount_out_min` (int): 最小输出金额，默认为0
- `sqrt_price_limit` (str): 价格限制，默认为"0"
- `deadline` (Optional[int]): 交易截止时间戳

**返回值**:
```json
{
    "success": true,
    "transaction_hash": "0x...",
    "message": "Trade executed successfully"
}
```

**异常**:
- `ValueError`: 参数验证失败
- `httpx.HTTPStatusError`: HTTP 请求失败

---

### 代币信息

#### `get_token_metadata(token_address: str) -> Optional[Dict[str, any]]`
**说明**: 从 monitor 服务获取代币元数据

**参数**:
- `token_address` (str): 代币地址

**返回值**:
```json
{
    "name": "Aptos Coin",
    "symbol": "APT", 
    "decimals": 8,
    "address": "0x1::aptos_coin::AptosCoin"
}
```

---

### 合约信息

#### `get_contract_address() -> Dict[str, any]`
**说明**: 从 monitor 获取 Vault 合约地址

**返回值**:
```json
{
    "contract_address": "0x...",
    "network": "aptos",
    "version": "1.0.0"
}
```

---

### 价格集成功能

#### `get_vault_info_with_prices(investor_address: str) -> Dict[str, any]`
**说明**: 获取投资者 Vault 信息并计算 USD 价值

**参数**:
- `investor_address` (str): 投资者地址

**返回值**:
```json
{
    "investor_address": "0x...",
    "balance_manager_created": true,
    "total_value_usd": "1250.75",
    "token_count": 3,
    "portfolio_composition": [
        {
            "token_address": "0xa",
            "token_name": "Aptos Coin",
            "token_symbol": "APT",
            "amount": "100.0",
            "amount_raw": "10000000000",
            "decimals": 8,
            "price_usd": 8.45,
            "value_usd": "845.0",
            "percentage": 67.6
        }
    ]
}
```

#### `get_vault_operations_with_prices(investor_address: str) -> Dict[str, any]`
**说明**: 获取投资者操作记录并计算 USD 价值

**功能**:
- 获取历史操作记录
- 查询操作时间点的代币价格
- 计算每笔操作的 USD 价值
- 计算 SWAP 操作的价格影响和滑点

---

### 内部辅助方法

#### `_calculate_summary_stats(holdings: List[Dict], events: List[Dict]) -> Dict[str, any]`
**说明**: 计算摘要统计信息

#### `_process_token_data(db, event: Dict, token_type: str, operation_time, tolerance_minutes: int)`
**说明**: 处理代币数据，计算价格和价值

#### `_calculate_swap_summary(event: Dict)`
**说明**: 计算 SWAP 操作的交易摘要，包括价格影响和盈亏

---

### 资源管理

#### `close()`
**说明**: 关闭 HTTP 客户端

#### `__aenter__()` / `__aexit__(...)`
**说明**: 异步上下文管理器支持

---

## 使用示例

```python
# 获取服务实例
service = AptosVaultService.get_instance()

# 检查余额管理器
has_manager = await service.has_balance_manager("0x...")

# 获取持仓信息
holdings = await service.get_investor_holdings("0x...")

# 获取带价格的完整信息
vault_info = await service.get_vault_info_with_prices("0x...")

# 执行交换
result = await service.admin_execute_swap(
    user_address="0x...",
    from_token_metadata_id="0xa",
    to_token_metadata_id="0x...",
    amount_in=1000000,
    fee_tier=2
)

# 关闭服务
await service.close()
```

## 依赖项

- `httpx`: 异步 HTTP 客户端
- `tradingflow.depot.python.db`: 数据库服务
- `tradingflow.station.utils.token_price_util`: 价格工具
- `tradingflow.depot.python.config`: 配置管理

## 注意事项

1. **单例模式**: 使用 `get_instance()` 获取实例，不要直接实例化
2. **异步操作**: 所有方法都是异步的，需要使用 `await`
3. **错误处理**: 大多数方法会抛出 `httpx` 相关异常，需要适当处理
4. **资源管理**: 使用完毕后记得调用 `close()` 或使用异步上下文管理器
5. **价格数据**: 价格相关功能需要 monitor 服务和数据库支持
