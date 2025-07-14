**1. 技术选型:**

*   **语言:** Python (与你现有项目一致，利用 `web3.py`)。
*   **核心库:**
    *   `web3.py`: 用于连接以太坊节点 (RPC)，过滤和解码事件日志。
    *   `asyncio`: 强烈推荐使用 `asyncio` 来处理并发的网络请求（监听多个合约/链）和定时任务，提高效率。
    *   数据库驱动: 如 `psycopg2` (PostgreSQL), `mysql-connector-python` (MySQL), 或 ORM 如 `SQLAlchemy` (推荐，更易于管理数据库交互)。
*   **数据库:** PostgreSQL 是一个非常好的选择，因为它对 JSONB 数据类型的支持很好（可以方便地存储事件参数），并且查询功能强大。MySQL 也可以。

**2. 核心架构:**

*   **独立进程/服务:** 将监听器作为一个单独的 Python 应用程序运行，与你的 Web 服务或 Celery Worker 分开。可以使用 `systemd`, Docker, Supervisor 或类似工具来管理这个后台进程。
*   **配置管理:** 需要读取配置，包括：
    *   链 ID 和对应的 RPC URL。
    *   要监听的合约 ABI (Vault, Price Oracle, ERC20 等)。
    *   数据库连接信息。
    *   初始要监听的固定合约地址（如 Price Oracle）。
*   **动态合约发现:** 需要一种机制让监听器知道新部署的 Vault 合约地址。

**3. 实现步骤:**

*   **a. 数据库设计:**
    *   **`monitored_contracts` 表:** 用于存储需要监听的合约信息。
        *   `id` (PK)
        *   `contract_address` (TEXT, UNIQUE)
        *   `chain_id` (INTEGER)
        *   `contract_type` (TEXT, e.g., 'Vault', 'PriceOracle')
        *   `abi_name` (TEXT, 指向需要使用的 ABI)
        *   `added_at` (TIMESTAMP)
    *   **`contract_events` 表:** 用于存储捕获到的事件。
        *   `id` (PK)
        *   `transaction_hash` (TEXT, indexed)
        *   `log_index` (INTEGER) - 区分同一交易中的多个事件
        *   `block_number` (BIGINT, indexed)
        *   `block_timestamp` (TIMESTAMP, indexed)
        *   `chain_id` (INTEGER, indexed)
        *   `contract_address` (TEXT, indexed)
        *   `event_name` (TEXT, indexed)
        *   `parameters` (JSONB or TEXT) - 存储解码后的事件参数字典
        *   `user_address` (TEXT, indexed, optional) - 如果事件有用户地址且经常查询，可以冗余存储
        *   ... 其他可能用于索引的字段 ...
    *   **`listener_state` 表:** 用于存储监听器进度。
        *   `chain_id` (INTEGER, PK)
        *   `last_processed_block` (BIGINT)

*   **b. 修改 `VaultService.deploy_vault`:**
    *   在成功部署并获取 `vault_address` 后，将该地址、`chain_id`、类型 ('Vault') 等信息插入到 `monitored_contracts` 表中。确保这个数据库操作是可靠的（例如，在 Celery 任务成功后执行）。

*   **c. 实现监听器进程 (`event_listener.py`):**
    *   **初始化:**
        *   加载配置。
        *   连接数据库。
        *   从 `monitored_contracts` 加载初始的合约列表。
        *   从 `listener_state` 获取每个 `chain_id` 的 `last_processed_block`，如果不存在，可以从一个合理的起点开始（例如，当前区块号减去少量区块，或者项目上线时的区块号）。
    *   **主循环 (`asyncio`):**
        *   **定期检查新合约:** 定期查询 `monitored_contracts` 表，看是否有新的合约地址被添加进来，更新内存中的监听列表。
        *   **轮询事件:** 对每个支持的 `chain_id`：
            *   获取当前最新区块号 (`w3.eth.block_number`)。
            *   确定要查询的区块范围 (`from_block = last_processed_block + 1`, `to_block = latest_block`)。注意处理 `from_block > to_block` 的情况。
            *   获取当前 `chain_id` 下所有需要监听的合约地址列表。
            *   使用 `w3.eth.get_logs` 查询这个区块范围内的事件。**关键优化:**
                *   **地址过滤:** 在 `get_logs` 中传入 `address` 列表（所有要监听的该链上的合约）。
                *   **主题过滤:** 在 `get_logs` 中传入 `topics` 参数，只获取你关心的事件签名哈希（例如 `Deposit`, `Withdraw`, `PriceUpdated` 的 Keccak 哈希）。这能极大减少从 RPC 节点传输的数据量。
            *   **处理日志:**
                *   遍历返回的 `logs`。
                *   根据 `log.address` 找到对应的合约 ABI。
                *   使用 ABI 解码 `log.topics` 和 `log.data` 得到事件名称和参数。
                *   获取区块时间戳 (`w3.eth.get_block(log.blockNumber).timestamp`)。
                *   将解析后的信息（交易哈希, log index, 区块号, 时间戳, 合约地址, 事件名, 参数等）存入 `contract_events` 数据库表。**注意处理数据库事务和唯一性约束** (e.g., `(transaction_hash, log_index)` 应该是唯一的)。
            *   **更新状态:** 在成功处理完 `to_block` 的所有事件后，将该 `chain_id` 的 `last_processed_block` 更新为 `to_block`，并存入 `listener_state` 表。**这步必须在数据写入成功后执行，保证原子性或至少是幂等性。**
        *   **休眠:** `await asyncio.sleep(interval)` (e.g., 5-15 秒)，然后重复轮询。
    *   **错误处理:**
        *   对 RPC 调用 (`get_logs`, `get_block`, etc.) 和数据库操作进行健壮的错误处理（`try...except`）。
        *   实现重试逻辑（例如，使用 `tenacity` 库）来应对临时的 RPC 节点或网络问题。
        *   记录详细日志。

*   **d. 部署和运行:**
    *   将监听器脚本打包（可能包含依赖）。
    *   使用进程管理工具（如 `supervisor` 或 `systemd`）或容器化（Docker）来部署和运行，确保它能在后台稳定运行并在失败时自动重启。

**4. 关键考虑点:**

*   **RPC 节点:** 需要稳定可靠的 RPC 节点。自建节点或使用 Alchemy, Infura 等服务。注意它们的请求限制。
*   **效率:** `eth_getLogs` 结合地址和主题过滤是最高效的方式。避免在循环中对每个合约单独调用 `get_logs`。
*   **延迟:** 轮询会有一定的延迟（取决于你的轮询间隔）。如果需要极低延迟，可以研究 `eth_subscribe` (WebSocket)，但它通常更复杂且可能不如轮询稳定。
*   **链重组 (Reorgs):** 简单的轮询可能受链重组影响。一个常见的缓解方法是只处理比最新区块稍早一些的区块（例如，`latest_block - N`，N 通常取 6-12）。如果需要更严格的一致性，需要实现更复杂的重组检测逻辑。
*   **历史数据回填:** 如果需要处理服务启动前的历史事件，需要编写一个单独的脚本或模式来扫描过去的区块。

这个方案提供了一个健壮且可扩展的方式来收集所有相关的链上活动，为后续的查询和分析打下基础。


"""
# cmd docker run postgres
docker run --name tradingflow-postgres \
  -e POSTGRES_USER=tradingflow \
  -e POSTGRES_PASSWORD=tradingflow123 \
  -e POSTGRES_DB=tradingflow \
  -p 5432:5432 \
  -d postgres


python -m monitor.event_listener
"""


"""json
# 这是一个周期下的和余额有关的事件，需要根据这个来计算持仓postion？
{
	"deposit": {
		"owner": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"assets": 1000000000000000000000,
		"sender": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"shares": 1000000000000000000000
	},
	"trade1": {
		"amount": 300000000000000000000,
		"result": 191572227986640717216,
		"signalType": 0,
		"tokenAddress": "0xF47e3B0A1952A81F1afc41172762CB7CE8700133"
	},
	"trade2": {
		"amount": 191572227986640717216,
		"result": 298392864028697125989,
		"signalType": 1,
		"tokenAddress": "0xF47e3B0A1952A81F1afc41172762CB7CE8700133"
	},
	"withdraw": {
		"owner": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"assets": 998392864028697125989,
		"sender": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"shares": 1000000000000000000000,
		"receiver": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
	}
}
"""