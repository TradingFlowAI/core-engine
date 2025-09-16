# BinancePriceNode 节点概述

## 概述
`BinancePriceNode` 是 TradingFlow Station 中用于获取 Binance 交易所价格数据的核心节点。该节点通过 Binance API 实时获取指定交易对的 K 线数据，为交易策略提供市场数据支持。

## 节点信息
- **节点类型**: `binance_price_node`
- **基类**: `NodeBase` 
- **注册装饰器**: `@register_node_type`
- **文件路径**: `tradingflow/station/nodes/binance_price_node.py`

## 核心功能

### 1. 数据获取
- **K 线数据获取**: 支持获取指定交易对的历史 K 线数据
- **实时价格**: 获取当前市场价格 ticker 信息
- **异步执行**: 使用异步编程模式，不阻塞主线程

### 2. API 集成
- **Binance API**: 集成官方 Binance Python SDK
- **认证支持**: 支持 API Key/Secret 认证（可选，默认使用公共接口）
- **连接测试**: 初始化时自动测试与 Binance 服务器的连接

### 3. 容错机制
- **重试机制**: 客户端初始化失败时支持最多 3 次重试
- **指数退避**: 重试延迟采用指数退避算法（1s, 2s, 4s）
- **异常处理**: 完整的异常捕获和错误日志记录

## 输入参数

### 默认参数 (default_params)
```python
{
    "symbol": "BTCUSDT",      # 交易对符号
    "interval": "1m",         # K 线时间间隔  
    "limit": 100,             # 获取数量限制
}
```

### 构造函数参数
| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `symbol` | str | "BTCUSDT" | 交易对符号（自动转换为大写） |
| `interval` | str | "1h" | K 线时间间隔（1m, 5m, 15m, 1h, 1d 等） |
| `limit` | int | 100 | K 线数据数量（1-1000 之间） |
| `flow_id` | str | - | 流程 ID |
| `component_id` | int | - | 组件 ID |
| `cycle` | int | - | 执行周期 |
| `node_id` | str | - | 节点唯一标识符 |
| `name` | str | - | 节点名称 |

### 环境变量配置
- `BINANCE_API_KEY`: Binance API 密钥（可选）
- `BINANCE_API_SECRET`: Binance API 密码（可选）

## 输出数据

### 信号类型
- **信号句柄**: `PRICE_DATA_HANDLE` ("price_data")
- **信号类型**: `SignalType.PRICE_DATA`

### 输出数据结构
```python
{
    "symbol": "BTCUSDT",           # 交易对符号
    "interval": "1h",              # K 线间隔
    "limit": 100,                  # 数据数量
    "header": [                    # 数据字段说明
        "open_time",               # 开盘时间
        "open",                    # 开盘价
        "high",                    # 最高价
        "low",                     # 最低价
        "close",                   # 收盘价
        "volume",                  # 成交量
        "close_time",              # 收盘时间
        "quote_volume",            # 报价资产成交量
        "count",                   # 成交笔数
        "taker_buy_volume",        # 主动买入成交量
        "taker_buy_quote_volume",  # 主动买入报价资产成交量
    ],
    "kline_data": [...],           # K 线原始数据数组
    "current_price": "43250.50"    # 当前价格
}
```

## 核心方法

### 1. `initialize_client()` - 客户端初始化
```python
async def initialize_client() -> bool
```
- **功能**: 初始化 Binance API 客户端
- **重试机制**: 支持最多 3 次重试，指数退避延迟
- **返回值**: 初始化成功返回 True，失败返回 False

### 2. `fetch_klines()` - 获取 K 线数据
```python
async def fetch_klines() -> Optional[List]
```
- **功能**: 异步获取指定交易对的 K 线数据
- **线程池**: 使用 `run_in_executor` 执行同步 API 调用
- **返回值**: K 线数据列表，失败返回 None

### 3. `get_symbol_ticker()` - 获取价格信息
```python
async def get_symbol_ticker() -> Optional[Dict]
```
- **功能**: 获取指定交易对的当前价格信息
- **返回值**: 包含当前价格的字典

### 4. `process_klines()` - 数据处理
```python
def process_klines(klines: List, ticker: Dict) -> Dict[str, Any]
```
- **功能**: 将原始 K 线数据处理为标准化格式
- **返回值**: 结构化的价格数据字典

### 5. `execute()` - 执行节点逻辑
```python
async def execute() -> bool
```
- **功能**: 节点主执行逻辑
- **流程**: 初始化客户端 → 获取数据 → 处理数据 → 发送信号
- **返回值**: 执行成功返回 True

## 执行流程

1. **初始化阶段**
   - 设置交易对符号、时间间隔、数据量限制
   - 从配置文件读取 API 密钥（如果有）
   - 创建日志器

2. **执行阶段**
   - 初始化 Binance 客户端（带重试机制）
   - 设置节点状态为 RUNNING
   - 异步获取 K 线数据和当前价格
   - 处理数据为标准格式

3. **输出阶段**
   - 通过信号系统发送处理后的价格数据
   - 设置节点状态为 COMPLETED
   - 清理资源

4. **异常处理**
   - 捕获并记录所有异常
   - 设置适当的节点状态（FAILED/TERMINATED）
   - 清理 Binance 客户端资源

## 状态管理
- **RUNNING**: 正在获取数据
- **COMPLETED**: 成功获取并发送数据
- **FAILED**: 获取数据失败或发送信号失败
- **TERMINATED**: 任务被取消

## 日志记录
- 使用 `persist_log` 方法记录执行日志
- 支持不同日志级别：DEBUG, INFO, WARNING, ERROR
- 记录客户端初始化、数据获取、错误信息等关键事件

## 依赖库
- `binance`: Binance 官方 Python SDK
- `asyncio`: 异步编程支持
- `tradingflow.station.*`: TradingFlow 框架组件

## 使用场景
1. **市场数据源**: 为交易策略提供实时和历史价格数据
2. **技术分析**: K 线数据可用于各种技术指标计算
3. **价格监控**: 监控特定交易对的价格变化
4. **策略回测**: 历史 K 线数据支持策略回测分析

## 扩展性
- **多交易对支持**: 可以同时部署多个节点监控不同交易对
- **时间间隔灵活**: 支持 Binance 提供的所有 K 线时间间隔
- **数据量可配置**: 可根据需求调整获取的 K 线数量

## 注意事项
1. **API 限制**: 注意 Binance API 的请求频率限制
2. **网络连接**: 需要稳定的网络连接访问 Binance API
3. **数据延迟**: 数据获取可能存在轻微延迟
4. **资源清理**: 执行完成后自动清理客户端资源
5. **一次性执行**: 节点执行一次获取数据后即完成，不是持续监控模式
