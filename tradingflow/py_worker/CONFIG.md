# TradingFlow Worker 配置指南

本文档描述了 TradingFlow Worker 服务的配置选项。

## 配置方法

配置可以通过以下方式提供（按优先级从高到低）：

1. 环境变量
2. 命令行参数
3. 配置文件 (`config.py`)
4. 默认值

## 环境变量

环境变量会覆盖配置文件中的同名设置。

## 基本配置

| 配置名称    | 类型    | 默认值                    | 说明                  |
| ----------- | ------- | ------------------------- | --------------------- |
| SERVER_URL  | string  | http://localhost:8000     | 主服务器 URL          |
| WORKER_HOST | string  | localhost                 | Worker 服务绑定主机名 |
| WORKER_PORT | integer | 7000                      | Worker 服务绑定端口   |
| WORKER_ID   | string  | auto (自动生成)           | Worker 唯一标识       |
| DEBUG       | boolean | true (开发), false (生产) | 是否启用调试模式      |

## 节点配置

| 配置名称             | 类型 | 默认值                                    | 说明           |
| -------------------- | ---- | ----------------------------------------- | -------------- |
| SUPPORTED_NODE_TYPES | list | ["python", "data_processing", "ml_model"] | 支持的节点类型 |

## 存储配置

| 配置名称         | 类型   | 默认值                   | 说明                        |
| ---------------- | ------ | ------------------------ | --------------------------- |
| REDIS_URL        | string | redis://localhost:6379/0 | Redis 连接 URL              |
| STATE_STORE_TYPE | string | memory                   | 状态存储类型 (memory/redis) |

## 日志配置

| 配置名称         | 类型    | 默认值          | 说明                                         |
| ---------------- | ------- | --------------- | -------------------------------------------- |
| LOG_LEVEL        | string  | INFO            | 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL) |
| LOG_TO_FILE      | boolean | false           | 是否将日志写入文件                           |
| LOG_FILE_PATH    | string  | logs/worker.log | 日志文件路径                                 |
| LOG_MAX_BYTES    | integer | 10485760 (10MB) | 日志文件最大大小                             |
| LOG_BACKUP_COUNT | integer | 5               | 保留的日志备份数量                           |

## 健康检查配置

| 配置名称              | 类型    | 默认值 | 说明               |
| --------------------- | ------- | ------ | ------------------ |
| HEALTH_CHECK_INTERVAL | integer | 30     | 健康检查间隔（秒） |

## 币安配置

| 配置名称           | 类型   | 默认值 | 说明          |
| ------------------ | ------ | ------ | ------------- |
| BINANCE_API_KEY    | string | ""     | 币安 API 密钥 |
| BINANCE_API_SECRET | string | ""     | 币安 API 密钥 |

## 环境选择

可以通过 `ENVIRONMENT` 环境变量选择预定义的环境配置：

-   `development`：开发环境（默认）
-   `testing`：测试环境
-   `production`：生产环境
