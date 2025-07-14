-- 数据库迁移脚本：修复monitored_contracts表的id字段类型
-- 创建时间: 2025-01-12
-- 描述: 将monitored_contracts表的id字段从integer改为TEXT，支持字符串ID

-- 设置当前schema
SET search_path TO public;

-- ==========================================
-- 修复monitored_contracts表的id字段类型
-- ==========================================

-- 1. 首先备份现有数据
CREATE TEMP TABLE monitored_contracts_backup AS
SELECT * FROM monitored_contracts;

-- 2. 删除现有表的约束和索引
DROP INDEX IF EXISTS idx_monitored_contracts_chain_id;
DROP INDEX IF EXISTS idx_monitored_contracts_contract_type;
DROP INDEX IF EXISTS idx_monitored_contracts_is_active;
DROP INDEX IF EXISTS idx_monitored_contracts_network;
DROP INDEX IF EXISTS idx_monitored_contracts_network_chain;
DROP INDEX IF EXISTS uix_contract_network_chain_address;

-- 3. 重新创建表结构（使用TEXT类型的id）
DROP TABLE monitored_contracts;

CREATE TABLE monitored_contracts (
    id TEXT PRIMARY KEY,
    network VARCHAR(50) NOT NULL,
    contract_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL,
    contract_type TEXT NOT NULL,
    abi_name TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. 重新创建索引
CREATE INDEX idx_monitored_contracts_chain_id ON monitored_contracts (chain_id);
CREATE INDEX idx_monitored_contracts_contract_type ON monitored_contracts (contract_type);
CREATE INDEX idx_monitored_contracts_is_active ON monitored_contracts (is_active);
CREATE INDEX idx_monitored_contracts_network ON monitored_contracts (network);
CREATE INDEX idx_monitored_contracts_network_chain ON monitored_contracts (network, chain_id);
CREATE UNIQUE INDEX uix_contract_network_chain_address ON monitored_contracts (network, chain_id, contract_address);

-- 5. 恢复数据（如果有的话），生成字符串ID
INSERT INTO monitored_contracts (id, network, contract_address, chain_id, contract_type, abi_name, is_active, added_at)
SELECT
    CASE
        WHEN contract_type = 'FACTORY' THEN
            network || '_' || chain_id || '_factory_' || LOWER(contract_address)
        ELSE
            network || '_' || chain_id || '_vault_' || LOWER(contract_address)
    END as id,
    network,
    contract_address,
    chain_id,
    contract_type,
    abi_name,
    is_active,
    added_at
FROM monitored_contracts_backup;

-- 6. 清理临时表
DROP TABLE monitored_contracts_backup;

-- ==========================================
-- 验证修改结果
-- ==========================================

-- 显示新的表结构
\d monitored_contracts

-- 显示恢复的数据
SELECT COUNT(*) as total_contracts FROM monitored_contracts;
SELECT contract_type, COUNT(*) as count FROM monitored_contracts GROUP BY contract_type;

-- 输出完成信息
SELECT 'monitored_contracts表id字段类型修复完成' as status;
