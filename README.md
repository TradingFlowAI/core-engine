# TradingFlow Python 后端

## 项目概述

TradingFlow 是一个高性能、可扩展的交易流程自动化平台，支持加密货币交易策略的设计、测试和执行。Python 后端提供了核心服务，包括账户管理、交易执行、数据处理和系统监控等功能。

## 系统架构

TradingFlow Python 后端由以下主要组件组成：

### 1. Account Manager 服务

账户管理服务负责用户认证、资产管理、交易历史记录和用户偏好设置等功能。

- **核心功能**：
  - 用户账户管理与认证
  - 钱包集成与签名验证
  - Vault 合约部署与管理
  - 代币信息管理
  - 定时任务调度

- **技术栈**：
  - FastAPI Web 框架
  - Celery 任务队列
  - PostgreSQL 数据库
  - Redis 缓存

### 2. Python Worker 服务

Python Worker 是一个高性能任务处理框架，用于构建和执行数据流图(DAG)中的节点。

- **核心功能**：
  - 节点系统：提供灵活的节点抽象，每个节点执行特定逻辑
  - 信号机制：节点间通过信号通信，实现数据和事件传递
  - 异步处理：基于异步IO设计，保证高性能和低延迟
  - REST API：提供HTTP接口用于节点管理和监控

- **技术栈**：
  - FastAPI
  - AsyncIO
  - RabbitMQ 消息队列

### 3. Monitor 服务

监控服务负责监听区块链事件，并将相关数据存储到数据库中。

- **核心功能**：
  - 区块链事件监听
  - 合约事件解码与存储
  - 动态合约发现
  - 事件通知

- **技术栈**：
  - web3.py
  - asyncio
  - PostgreSQL

## 部署架构

TradingFlow Python 后端采用容器化部署方式，支持灵活的部署选项：

### 1. 服务分离部署

系统被分为两个主要的服务组：

- **Account Services**：包含 Account Manager、Celery 服务和 Monitor
- **Python Worker**：独立的 Worker 服务

### 2. 数据服务

所有服务依赖的数据存储和消息队列组件：

- **PostgreSQL**：主数据库，存储用户数据、交易记录等
- **Redis**：缓存和 Celery 后端
- **RabbitMQ**：消息队列，用于服务间通信

## 部署指南

### 环境要求

- Docker 和 Docker Compose
- Python 3.10+
- 足够的磁盘空间用于数据持久化

### 配置文件

1. 复制环境变量模板：
   ```bash
   cp .env.example .env
   ```

2. 编辑 `.env` 文件，配置必要的环境变量：
   - 数据库连接信息
   - Redis 和 RabbitMQ 连接信息
   - 服务端口和主机设置
   - 数据存储路径

### 部署选项

#### 选项 1：单机部署（所有服务）

```bash
docker-compose -f docker-compose-account-and-worker.yml -f docker-compose-db-redis-mq.yml up -d
```

#### 选项 2：分离部署

1. 在数据服务器上部署数据服务：
   ```bash
   docker-compose -f docker-compose-db-redis-mq.yml up -d
   ```

2. 在应用服务器上部署应用服务：
   ```bash
   docker-compose -f docker-compose-account-and-worker.yml up -d
   ```

### 服务端口

- Account Manager: 7000
- Python Worker: 7001
- PostgreSQL: 5432
- Redis: 6379
- RabbitMQ: 5672 (管理界面: 15672)

## 开发指南

### 本地开发环境设置

1. 克隆代码库：
   ```bash
   git clone <repository-url>
   cd TradingFlow
   ```

2. 创建并激活虚拟环境：
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # 或
   venv\Scripts\activate  # Windows
   ```

3. 安装依赖：
   ```bash
   pip install -r python/requirements.txt
   ```

4. 启动本地数据服务：
   ```bash
   cd python
   docker-compose -f docker-compose-db-redis-mq.yml up -d
   ```

5. 运行服务：
   ```bash
   # 启动 Account Manager
   python -m tradingflow.account_manager.server

   # 启动 Celery Beat
   celery -A tradingflow.account_manager.celery_worker beat --loglevel=info

   # 启动 Celery Worker
   celery -A tradingflow.account_manager.celery_worker worker --loglevel=info

   # 启动 Python Worker
   python -m tradingflow.py_worker.server
   ```

### 使用脚本

项目提供了几个便捷脚本用于服务管理：

- `start_account_services.sh`: 启动 Account Manager 相关服务
- `stop_account_services.sh`: 停止 Account Manager 相关服务
- `start_worker.sh`: 启动 Python Worker 服务
- `stop_worker.sh`: 停止 Python Worker 服务

## CI/CD 流程

TradingFlow 使用 GitHub Actions 进行持续集成和部署：

1. **Python Account Services 工作流**：
   - 构建和部署 Account Manager、Celery 服务和 Monitor
   - 触发条件：推送到 `stg` 分支且修改了相关文件

2. **Python Worker 工作流**：
   - 构建和部署 Python Worker 服务
   - 触发条件：推送到 `stg` 分支且修改了相关文件

## 数据持久化

所有关键数据都通过 Docker 卷进行持久化存储：

- PostgreSQL 数据：`/opt/tradingflow/data/postgres`
- Redis 数据：`/opt/tradingflow/data/redis`
- RabbitMQ 数据：`/opt/tradingflow/data/rabbitmq`
- 日志文件：`./logs` (映射到容器内的 `/app/logs`)

## 故障排除

### 常见问题

1. **服务无法启动**
   - 检查环境变量配置
   - 检查数据库连接
   - 查看日志文件

2. **数据库连接问题**
   - 确认 PostgreSQL 服务正在运行
   - 验证数据库凭据
   - 检查网络连接和防火墙设置

3. **消息队列问题**
   - 确认 RabbitMQ 服务正在运行
   - 验证 RabbitMQ 凭据和虚拟主机设置
   - 检查队列状态和消息积压

### 日志位置

- 容器日志：`docker logs <container-name>`
- 应用日志：`./logs/` 目录下的相应日志文件

## 联系与支持

如有问题或需要支持，请联系 TradingFlow 开发团队。

---

## 版权声明

代码归 TradingFlow Company，未经 TradingFlow Company 和 TradingFlow DAO 授权，不得私自传播给第三方，违者要追究法律责任。
