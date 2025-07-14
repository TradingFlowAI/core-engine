-- 数据库迁移脚本：修复地址字段长度支持多链
-- 创建时间: 2025-01-11
-- 目的: 将VARCHAR(42)地址字段扩展为VARCHAR(255)以支持Aptos、Sui等长地址
-- 受影响表: users, vault_contracts, vault_value_history

-- ==========================================
-- 步骤1: 添加vault_contracts表的network字段
-- ==========================================

-- 检查network字段是否存在，如果不存在则添加
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'vault_contracts'
        AND column_name = 'network'
    ) THEN
        ALTER TABLE vault_contracts
        ADD COLUMN network VARCHAR(50);

        -- 为现有记录设置默认network值
        UPDATE vault_contracts SET network = CASE
            WHEN chain_id = 1 THEN 'ethereum'
            WHEN chain_id = 747 THEN 'flow-evm'
            WHEN chain_id = 31337 THEN 'hardhat'
            ELSE 'unknown'
        END WHERE network IS NULL;

        -- 设置network字段为NOT NULL
        ALTER TABLE vault_contracts
        ALTER COLUMN network SET NOT NULL;

        -- 添加注释
        COMMENT ON COLUMN vault_contracts.network IS '网络标识符，如 ethereum、aptos、sui、flow-evm等';
    END IF;
END
$$;

-- ==========================================
-- 步骤2: 扩展地址字段长度
-- ==========================================

-- 修复users表的wallet_address字段
ALTER TABLE users
ALTER COLUMN wallet_address TYPE VARCHAR(255);

-- 修复vault_contracts表的地址字段
ALTER TABLE vault_contracts
ALTER COLUMN deployer_address TYPE VARCHAR(255);

ALTER TABLE vault_contracts
ALTER COLUMN investor_address TYPE VARCHAR(255);

-- 修复vault_value_history表的vault_address字段
ALTER TABLE vault_value_history
ALTER COLUMN vault_address TYPE VARCHAR(255);

-- ==========================================
-- 步骤3: 更新或创建唯一约束
-- ==========================================

-- 检查并更新vault_contracts表的唯一约束
DO $$
BEGIN
    -- 删除旧的唯一约束（如果存在）
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'vault_contracts_contract_address_chain_id_key'
        AND table_name = 'vault_contracts'
    ) THEN
        ALTER TABLE vault_contracts
        DROP CONSTRAINT vault_contracts_contract_address_chain_id_key;
    END IF;

    -- 添加新的包含network的唯一约束
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'uix_vault_contract_network_chain_address'
        AND table_name = 'vault_contracts'
    ) THEN
        ALTER TABLE vault_contracts
        ADD CONSTRAINT uix_vault_contract_network_chain_address
        UNIQUE (network, chain_id, contract_address);
    END IF;
END
$$;

-- ==========================================
-- 步骤4: 创建或更新索引
-- ==========================================

-- vault_contracts表的网络相关索引
CREATE INDEX IF NOT EXISTS idx_vault_contracts_network
ON vault_contracts (network);

CREATE INDEX IF NOT EXISTS idx_vault_contracts_network_chain
ON vault_contracts (network, chain_id);

CREATE INDEX IF NOT EXISTS idx_vault_contracts_network_address
ON vault_contracts (network, contract_address);

-- ==========================================
-- 步骤5: 验证修复结果
-- ==========================================

-- 验证字段类型修改
SELECT
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
FROM information_schema.columns
WHERE (table_name = 'users' AND column_name = 'wallet_address')
   OR (table_name = 'vault_contracts' AND column_name IN ('network', 'deployer_address', 'investor_address'))
   OR (table_name = 'vault_value_history' AND column_name = 'vault_address')
ORDER BY table_name, column_name;

-- 检查约束
SELECT
    constraint_name,
    table_name,
    constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'vault_contracts'
AND constraint_type = 'UNIQUE'
ORDER BY constraint_name;

-- 最终状态报告
SELECT
    '地址字段长度修复完成' as status,
    'VARCHAR(42) -> VARCHAR(255)，支持多链长地址' as changes,
    'vault_contracts表已添加network字段和相应约束' as network_support;
