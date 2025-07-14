-- TradingFlow 数据库初始化脚本 - 支持多链网络架构
-- 创建时间: 2025-01-08 13:36:17
-- 版本: v2.0 - 添加多链网络支持
-- 可反复运行，完全清理重建

-- ==========================================
-- Step 1: 设置 schema（使用 public）
-- ==========================================
-- 确保 public schema 存在（通常默认存在）
-- CREATE SCHEMA IF NOT EXISTS public;

-- 设置当前会话使用 public schema
SET search_path TO public;

-- ==========================================
-- Step 2: 清理所有对象
-- ==========================================

-- 删除触发器函数
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;
DROP FUNCTION IF EXISTS update_event_processor_state_updated_at() CASCADE;

-- 删除所有表
DROP TABLE IF EXISTS flow_execution_logs CASCADE;
DROP TABLE IF EXISTS token_price_history CASCADE;
DROP TABLE IF EXISTS vault_operation_history CASCADE;
DROP TABLE IF EXISTS event_processor_state CASCADE;
DROP TABLE IF EXISTS vault_value_history CASCADE;
DROP TABLE IF EXISTS monitored_tokens CASCADE;
DROP TABLE IF EXISTS vault_contracts CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS listener_state CASCADE;
DROP TABLE IF EXISTS monitored_contracts CASCADE;
DROP TABLE IF EXISTS contract_events CASCADE;

-- ==========================================
-- Step 3: 创建表（支持多链网络架构）
-- ==========================================

-- contract_events表 - 合约事件记录
CREATE TABLE contract_events (
    id TEXT PRIMARY KEY,
    transaction_hash TEXT NOT NULL,
    log_index INT NOT NULL,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    network VARCHAR(50) NOT NULL,  -- 网络类型: aptos, ethereum, flow-evm, solana, sui, hardhat, evm 等
    chain_id INT NOT NULL,
    contract_address TEXT NOT NULL,
    event_name TEXT NOT NULL,
    parameters JSONB NOT NULL,
    user_address TEXT,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_event_log UNIQUE (transaction_hash, log_index)
);

-- listener_state表 - 区块链监听状态
CREATE TABLE listener_state (
    network VARCHAR(50) NOT NULL,  -- 网络类型
    chain_id INT NOT NULL,
    last_processed_block BIGINT NOT NULL,
    last_update TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (network, chain_id)
);

-- monitored_contracts表 - 监控的合约
CREATE TABLE monitored_contracts (
    id TEXT PRIMARY KEY,                    -- 使用TEXT ID匹配companion服务逻辑
    network VARCHAR(50) NOT NULL,          -- 网络类型
    contract_address TEXT NOT NULL,
    chain_id INT NOT NULL,
    contract_type TEXT NOT NULL,
    abi_name TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    added_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uix_contract_network_chain_address UNIQUE (network, chain_id, contract_address)
);

-- users表 - 用户信息（支持多链长地址）
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL UNIQUE,
    nickname VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMPTZ
);

-- vault_contracts表 - 金库合约信息（支持多链架构）
CREATE TABLE vault_contracts (
    id SERIAL PRIMARY KEY,
    contract_address VARCHAR(255) NOT NULL,

    -- 网络信息 - 支持多链
    network VARCHAR(50) NOT NULL COMMENT '网络标识符，如 ethereum、aptos、sui、flow-evm等',
    chain_id INT NOT NULL,

    transaction_hash VARCHAR(255),
    vault_name VARCHAR(255),
    vault_symbol VARCHAR(255),
    asset_address VARCHAR(255),
    deployer_address VARCHAR(255),
    investor_address VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    deployment_block INT,
    deployment_cost_eth NUMERIC(36, 18),

    -- 确保每个网络和链上的合约地址唯一
    CONSTRAINT uix_vault_contract_network_chain_address UNIQUE (network, chain_id, contract_address)
);

-- monitored_tokens表 - 监控的代币
CREATE TABLE monitored_tokens (
    id SERIAL PRIMARY KEY,
    network VARCHAR NOT NULL,
    network_type VARCHAR DEFAULT 'evm',
    chain_id INT,
    token_address VARCHAR NOT NULL,
    name VARCHAR,
    symbol VARCHAR,
    decimals INT DEFAULT 18,
    primary_source VARCHAR DEFAULT 'geckoterminal',
    is_active BOOLEAN DEFAULT true,
    logo_url VARCHAR,
    description VARCHAR,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ,
    CONSTRAINT uix_network_token UNIQUE (network, token_address)
);

-- vault_value_history表 - 金库价值历史
CREATE TABLE vault_value_history (
    id BIGSERIAL PRIMARY KEY,
    vault_contract_id INTEGER NOT NULL,
    chain_id INTEGER NOT NULL,
    vault_address VARCHAR(255) NOT NULL,
    total_value_usd NUMERIC(36, 18) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_vault_value_history_vault_time UNIQUE (vault_contract_id, updated_at)
);

-- event_processor_state表 - 事件处理器状态
CREATE TABLE event_processor_state (
    processor_id VARCHAR PRIMARY KEY,
    last_processed_event_id INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processed_count INTEGER NOT NULL DEFAULT 0,
    last_status VARCHAR
);

-- vault_operation_history表 - 金库操作历史
CREATE TABLE vault_operation_history (
    id BIGSERIAL PRIMARY KEY,
    vault_contract_id INT NOT NULL,
    network VARCHAR NOT NULL,
    network_type VARCHAR NOT NULL,
    chain_id INT,
    vault_address VARCHAR(255) NOT NULL,
    transaction_hash VARCHAR(255),
    operation_type VARCHAR(20) NOT NULL,
    input_token_address VARCHAR(255),
    input_token_amount NUMERIC(36, 18),
    input_token_usd_value NUMERIC(36, 18),
    output_token_address VARCHAR(255),
    output_token_amount NUMERIC(36, 18),
    output_token_usd_value NUMERIC(36, 18),
    gas_used BIGINT,
    gas_price NUMERIC(36, 18),
    total_gas_cost_usd NUMERIC(36, 18),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- token_price_history表 - 代币价格历史
CREATE TABLE token_price_history (
    id SERIAL PRIMARY KEY,
    network VARCHAR NOT NULL,
    chain_id INT,
    token_address VARCHAR NOT NULL,
    price_usd DOUBLE PRECISION NOT NULL,
    source VARCHAR NOT NULL DEFAULT 'geckoterminal',
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- flow_execution_logs表 - 流程执行日志
CREATE TABLE flow_execution_logs (
    id SERIAL PRIMARY KEY,
    flow_id VARCHAR(255) NOT NULL,
    cycle INTEGER NOT NULL,
    node_id VARCHAR(255),
    log_level VARCHAR(20) NOT NULL DEFAULT 'INFO',
    log_source VARCHAR(50) NOT NULL DEFAULT 'node',
    message TEXT NOT NULL,
    log_metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- Step 4: 创建索引 - 优化多链网络查询
-- ==========================================

-- contract_events表索引
CREATE INDEX idx_contract_events_network ON contract_events (network);
CREATE INDEX idx_contract_events_network_chain ON contract_events (network, chain_id);
CREATE INDEX idx_contract_events_network_block ON contract_events (network, block_number);
CREATE INDEX idx_contract_events_block_number ON contract_events (block_number);
CREATE INDEX idx_contract_events_block_timestamp ON contract_events (block_timestamp);
CREATE INDEX idx_contract_events_chain_id ON contract_events (chain_id);
CREATE INDEX idx_contract_events_contract_address ON contract_events (contract_address);
CREATE INDEX idx_contract_events_event_name ON contract_events (event_name);
CREATE INDEX idx_contract_events_tx_hash ON contract_events (transaction_hash);
CREATE INDEX idx_contract_events_user_address ON contract_events (user_address);
CREATE INDEX idx_contract_events_processed ON contract_events (processed);

-- listener_state表索引（主键已提供网络索引）
CREATE INDEX idx_listener_state_network ON listener_state (network);
CREATE INDEX idx_listener_state_last_block ON listener_state (last_processed_block);

-- monitored_contracts表索引
CREATE INDEX idx_monitored_contracts_network ON monitored_contracts (network);
CREATE INDEX idx_monitored_contracts_network_chain ON monitored_contracts (network, chain_id);
CREATE INDEX idx_monitored_contracts_chain_id ON monitored_contracts (chain_id);
CREATE INDEX idx_monitored_contracts_contract_type ON monitored_contracts (contract_type);
CREATE INDEX idx_monitored_contracts_is_active ON monitored_contracts (is_active);

-- users表索引
CREATE INDEX idx_users_email ON users (email);
CREATE INDEX idx_users_wallet_address ON users (wallet_address);

-- vault_contracts表索引
CREATE INDEX idx_vault_asset_address ON vault_contracts (asset_address);
CREATE INDEX idx_vault_chain_id ON vault_contracts (chain_id);
CREATE INDEX idx_vault_contract_address ON vault_contracts (contract_address);
CREATE INDEX idx_vault_deployer_address ON vault_contracts (deployer_address);
CREATE INDEX idx_vault_investor_address ON vault_contracts (investor_address);
CREATE INDEX idx_vault_transaction_hash ON vault_contracts (transaction_hash);

-- vault_contracts表网络相关索引（多链支持）
CREATE INDEX idx_vault_contracts_network ON vault_contracts (network);
CREATE INDEX idx_vault_contracts_network_chain ON vault_contracts (network, chain_id);
CREATE INDEX idx_vault_contracts_network_address ON vault_contracts (network, contract_address);

-- monitored_tokens表索引
CREATE INDEX idx_monitored_tokens_network ON monitored_tokens (network);
CREATE INDEX ix_monitored_tokens_chain_id ON monitored_tokens (chain_id);
CREATE INDEX ix_monitored_tokens_id ON monitored_tokens (id);
CREATE INDEX ix_monitored_tokens_token_address ON monitored_tokens (token_address);

-- vault_value_history表索引
CREATE INDEX idx_vault_value_history_vault_id ON vault_value_history (vault_contract_id);
CREATE INDEX idx_vault_value_history_vault_time ON vault_value_history (vault_contract_id, updated_at);
CREATE INDEX idx_vault_value_history_chain_time ON vault_value_history (chain_id, updated_at);
CREATE INDEX idx_vault_value_history_address_time ON vault_value_history (vault_address, updated_at);

-- vault_operation_history表索引
CREATE INDEX idx_vault_operation_network ON vault_operation_history (network);
CREATE INDEX idx_vault_operation_network_address ON vault_operation_history (network, vault_address);
CREATE INDEX idx_vault_operation_network_type ON vault_operation_history (network_type);
CREATE INDEX idx_vault_operation_type_vault ON vault_operation_history (operation_type, vault_contract_id);
CREATE INDEX idx_vault_operation_vault_id ON vault_operation_history (vault_contract_id);
CREATE INDEX idx_vault_operation_vault_time ON vault_operation_history (vault_contract_id, created_at);

-- token_price_history表索引
CREATE INDEX idx_chain_timestamp ON token_price_history (chain_id, timestamp);
CREATE INDEX idx_network_address_timestamp ON token_price_history (network, token_address, timestamp);
CREATE INDEX idx_timestamp ON token_price_history (timestamp);
CREATE INDEX ix_token_price_history_chain_id ON token_price_history (chain_id);
CREATE INDEX ix_token_price_history_id ON token_price_history (id);
CREATE INDEX ix_token_price_history_network ON token_price_history (network);
CREATE INDEX ix_token_price_history_token_address ON token_price_history (token_address);

-- flow_execution_logs表索引
CREATE INDEX idx_flow_execution_logs_flow_id ON flow_execution_logs (flow_id);
CREATE INDEX idx_flow_execution_logs_cycle ON flow_execution_logs (cycle);
CREATE INDEX idx_flow_execution_logs_node_id ON flow_execution_logs (node_id);
CREATE INDEX idx_flow_execution_logs_flow_cycle ON flow_execution_logs (flow_id, cycle);
CREATE INDEX idx_flow_execution_logs_flow_cycle_node ON flow_execution_logs (flow_id, cycle, node_id);
CREATE INDEX idx_flow_execution_logs_created_at ON flow_execution_logs (created_at);
CREATE INDEX idx_flow_execution_logs_log_level ON flow_execution_logs (log_level);
CREATE INDEX idx_flow_execution_logs_log_source ON flow_execution_logs (log_source);

-- ==========================================
-- Step 5: 创建函数和触发器
-- ==========================================

-- 更新updated_at字段的触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- 更新event_processor_state的updated_at字段的触发器函数
CREATE OR REPLACE FUNCTION update_event_processor_state_updated_at()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- 创建触发器
CREATE TRIGGER trigger_update_vault_value_history_updated_at
BEFORE UPDATE ON vault_value_history
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_update_event_processor_state_updated_at
BEFORE UPDATE ON event_processor_state
FOR EACH ROW
EXECUTE FUNCTION update_event_processor_state_updated_at();

-- ==========================================
-- Step 6: 添加表注释和字段注释
-- ==========================================

-- 表注释
COMMENT ON TABLE contract_events IS '合约事件记录表 - 支持多链网络';
COMMENT ON TABLE listener_state IS '区块链监听状态表 - 按网络和链ID分别跟踪';
COMMENT ON TABLE monitored_contracts IS '监控合约表 - 支持多链网络架构';

-- 字段注释 - 重点说明多链支持
COMMENT ON COLUMN contract_events.network IS '网络类型: aptos, ethereum, flow-evm, solana, sui, hardhat, evm 等';
COMMENT ON COLUMN listener_state.network IS '网络类型: aptos, ethereum, flow-evm, solana, sui, hardhat, evm 等';
COMMENT ON COLUMN monitored_contracts.network IS '网络类型: aptos, ethereum, flow-evm, solana, sui, hardhat, evm 等';

COMMENT ON COLUMN contract_events.chain_id IS '链ID: Aptos=1, Ethereum=1, Flow-EVM=747, Hardhat=31337等';
COMMENT ON COLUMN listener_state.chain_id IS '链ID: Aptos=1, Ethereum=1, Flow-EVM=747, Hardhat=31337等';
COMMENT ON COLUMN monitored_contracts.chain_id IS '链ID: Aptos=1, Ethereum=1, Flow-EVM=747, Hardhat=31337等';

-- ==========================================
-- Step 7: 插入初始数据示例
-- ==========================================

-- 示例监控合约（如果有环境变量配置）
-- INSERT INTO monitored_contracts (id, network, chain_id, contract_address, contract_type, abi_name, is_active, added_at)
-- VALUES
--   ('flow_evm_factory_mainnet', 'flow-evm', 747, 'FLOW_EVM_FACTORY_ADDRESS', 'FACTORY', 'PersonalVaultFactoryUniV2', true, NOW()),
--   ('aptos_vault_mainnet', 'aptos', 1, 'APTOS_VAULT_ADDRESS', 'VAULT', 'AptosVault', true, NOW());

-- ==========================================
-- 完成
-- ==========================================

-- 验证创建结果
SELECT
    'TradingFlow数据库v2.0初始化完成！' as status,
    'Network字段已添加，支持多链架构：aptos, ethereum, flow-evm, solana, sui等' as features;

-- 显示表结构信息
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name IN ('contract_events', 'listener_state', 'monitored_contracts')
    AND column_name IN ('network', 'chain_id')
ORDER BY table_name, ordinal_position;
