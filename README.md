# TradingFlow Python Worker

## 概述

TradingFlow Python Worker 是一个高性能、可扩展的任务处理框架，用于构建和执行数据流图(DAG)中的节点。该框架专为交易系统设计，支持实时数据处理、信号生成与传递，以及各种自定义任务的执行。

## 主要功能

- **节点系统**: 提供灵活的节点抽象，每个节点可以执行特定的逻辑
- **信号机制**: 节点间通过信号通信，实现数据和事件的传递
- **消息队列**: 支持多种消息队列后端，包括内存队列和 RabbitMQ
- **异步处理**: 基于异步 IO 设计，保证高性能和低延迟
- **REST API**: 提供 HTTP 接口用于节点管理和监控
- **资源监控**: 实时跟踪系统资源和节点状态

## 系统架构

```
+----------------+     +----------------+     +----------------+
|     Node A     |---->|     Node B     |---->|     Node C     |
+----------------+     +----------------+     +----------------+
        |                     ^                      ^
        |                     |                      |
        v                     |                      |
      +-----------------------------------------------+
      |                 Message Queue                 |
      +-----------------------------------------------+
```

### 核心组件

- **NodeBase**: 节点基类，提供节点生命周期管理和信号处理
- **MessageQueue**: 消息队列抽象
- **Signal**: 节点间传递的信号，包含类型和有效负载
- **Worker API**: 提供 HTTP 接口用于管理和监控节点

## API 接口

### HTTP 端点

| 端点                      | 方法 | 描述             |
| ------------------------- | ---- | ---------------- |
| `/health`                 | GET  | 健康检查         |
| `/nodes/execute`          | POST | 执行一个节点     |
| `/nodes/{node_id}/status` | GET  | 获取节点状态     |
| `/nodes/{node_id}/stop`   | POST | 停止节点执行     |
| `/stats`                  | GET  | 获取资源使用统计 |

### 执行节点示例

```bash
# 执行一个价格数据节点
curl -X POST http://localhost:7002/nodes/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "example_flow",
    "component_id": 1,
    "cycle": 1,
    "node_id": "binance_price_node_1",
    "node_type": "binance_price_node",
    "input_edges": [
    ],
    "output_edges": [
      {
        "source": "binance_price_node_1",
        "source_handle": "price_data_handle",
        "target": "data_processor_2",
        "target_handle": "price_data_handle"
      }
    ],
    "config": {
      "node_class_type": "binance_price_node",
      "symbol": "BTCUSDT",
      "interval": "1h",
      "limit": 10
    }
  }'
# 执行一个价格trade节点
curl -X POST http://localhost:7002/nodes/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "trading_flow",
    "component_id": 2,
    "cycle": 1,
    "node_id": "uniswap_trade_node1",
    "node_type": "dex_trade_node",
    "input_edges": [],
    "output_edges": [
      {
        "source": "uniswap_trade_node1",
        "source_handle": "output",
        "target": "trade_notification_node",
        "target_handle": "transaction_receipt"
      }
    ],
    "config": {
      "node_class_type": "dex_trade_node",
      "chain_id": 31337,
      "dex_name": "uniswap",
      "vault_address": "0xfDD930c22708c7572278cf74D64f3721Eedc18Ad",
      "action": "buy",
      "output_token_address": "0x88D3CAaD49fC2e8E38C812c5f4ACdd0a8B065F66",
      "amount_in": "1.5",
      "min_amount_out": "0",
      "slippage_tolerance": 0.5,
      "signal_timeout": 10
    }
  }'
```

### 执行 flow 示例

基于你的 Flow API 和调度器实现，我来为你提供三个 curl 示例：执行 Flow、查询 Flow 状态以及停止 Flow。

#### 1. 执行 Flow 的 curl 示例

这个示例调用 `/flows/execute` 接口来注册并执行一个新的 Flow：

```bash
curl -X POST http://localhost:7002/flows/execute \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "example_flow",
    "cycle_interval": "1m",
    "flow_json": {
      "nodes": [
        {
          "id": "binance_price",
          "type": "binance_price_node",
          "config": {
            "node_class_type": "binance_price_node",
            "symbol": "BTCUSDT",
            "interval": "1h",
            "limit": 10
          }
        },
        {
          "id": "ai_model_node",
          "type": "ai_model_node",
          "config": {
            "node_class_type": "ai_model_node",
            "operation": "rolling_avg",
            "window": 5
          }
        },
        {


        }
      ],
      "edges": [
        {
          "source": "binance_price",
          "target": "data_processor"
        }
      ]
    }
  }'
```

#### 2. 查询 Flow 状态的 curl 示例

这个示例调用 `/flows/{flow_id}/status` 接口来获取指定 Flow 的状态信息：

```bash
curl -X GET http://localhost:8000/flows/example_flow/status \
  -H "Content-Type: application/json"
```

如果你想查询特定周期的状态：

```bash
curl -X GET http://localhost:8000/flows/example_flow/cycles/0 \
  -H "Content-Type: application/json"
```

#### 3. 停止 Flow 的 curl 示例

这个示例调用 `/flows/{flow_id}/stop` 接口来停止 Flow 的执行：

```bash
curl -X POST http://localhost:8000/flows/example_flow/stop \
  -H "Content-Type: application/json"
```

#### 额外示例：手动触发执行一个周期

如果你想手动触发执行一个新的周期：

```bash
curl -X POST http://localhost:8000/flows/example_flow/cycles \
  -H "Content-Type: application/json"
```

或指定特定的周期号：

```bash
curl -X POST http://localhost:8000/flows/example_flow/cycles \
  -H "Content-Type: application/json" \
  -d '{"cycle": 5}'
```

#### 额外示例：停止特定组件的执行

如果你想停止 Flow 中特定组件在特定周期的执行：

```bash
curl -X POST http://localhost:8000/flows/example_flow/cycles/0/components/0/stop \
  -H "Content-Type: application/json"
```

这些 curl 命令假设你的 Sanic 应用运行在 localhost 的 8000 端口上。如果你的应用运行在其他地址或端口，请相应地调整 URL。

## 中间件服务准备

在运行 TradingFlow Python Worker 之前，需要准备相关的中间件服务，主要包括 RabbitMQ 和 Redis。以下是使用 Docker 启动这些服务的示例。

### 启动 RabbitMQ

```bash
# 拉取 RabbitMQ 镜像
docker pull rabbitmq:3-management

# 启动 RabbitMQ 容器
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  rabbitmq:3-management

# 验证 RabbitMQ 是否正常运行
# 访问管理界面：http://localhost:15672
# 默认用户名和密码都是 guest
```

### 启动 Redis

```bash
# 拉取 Redis 镜像
docker pull redis

# 启动 Redis 容器
docker run -d --name redis \
  -p 6379:6379 \
  redis

# 验证 Redis 是否正常运行
docker exec -it redis redis-cli ping
# 应返回 PONG
```

## 使用方法

### 安装依赖

```bash
pip install -r requirements.txt
```

### 启动服务

```bash
# 使用默认配置运行
python station/server.py
```

## 测试

运行单元测试：

```bash
pytest station/tests/
```

运行快速功能测试：

```bash
python -m station.quick_test
```

## 扩展

### 添加新的节点类型

1. 在 nodes 目录下创建新的节点类
2. 继承 `NodeBase` 类并实现 `execute()` 方法
3. 在 `node_factory.py` 中注册新的节点类型

### 添加新的消息队列实现

1. 在 message_queue 目录下创建新的队列实现类
2. 继承 `MessageQueueBase` 类并实现所有抽象方法

## 注意事项

- 当使用内存队列时，通信仅限于单进程内
- 复杂工作流程建议使用 RabbitMQ 或其他持久化消息队列
- 节点的 `execute()` 方法应当是异步的，避免阻塞主线程
- 信号处理应考虑幂等性，确保多次处理相同信号不会产生副作用

如有问题或需要支持，请联系 TradingFlow 开发团队。

---

## 版权声明

代码归 TradingFlow Company，未经 TradingFlow Company 和 TradingFlow DAO 授权，不得私自传播给第三方，违者要追究法律责任。
