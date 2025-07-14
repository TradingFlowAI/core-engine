-- 数据库迁移脚本：为vault_contracts表添加network字段
-- 创建时间: 2025-01-10
-- 目的: 支持多链架构，确保vault_contracts表包含network信息
-- 适用于: 现有生产数据库的增量迁移

-- ==========================================
-- Step 1: 添加network字段到vault_contracts表
-- ==========================================

-- 检查字段是否已存在，如果不存在则添加
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'vault_contracts'
        AND column_name = 'network'
        AND table_schema = 'public'
    ) THEN
        -- 添加network字段，初始设为可空
        ALTER TABLE vault_contracts
        ADD COLUMN network VARCHAR(50);

        -- 添加注释
        COMMENT ON COLUMN vault_contracts.network IS '网络标识符，如 ethereum、aptos、sui、flow-evm等';

        RAISE NOTICE 'Added network column to vault_contracts table';
    ELSE
        RAISE NOTICE 'Network column already exists in vault_contracts table';
    END IF;
END $$;

-- ==========================================
-- Step 2: 根据chain_id填充network字段
-- ==========================================

-- 为现有记录根据chain_id推断network值
UPDATE vault_contracts SET network = CASE
    WHEN chain_id = 1 THEN 'eth'                    -- Ethereum主网
    WHEN chain_id = 56 THEN 'bsc'                   -- BSC
    WHEN chain_id = 137 THEN 'polygon'              -- Polygon
    WHEN chain_id = 10 THEN 'optimism'              -- Optimism
    WHEN chain_id = 42161 THEN 'arbitrum'           -- Arbitrum
    WHEN chain_id = 43114 THEN 'avalanche'          -- Avalanche
    WHEN chain_id = 747 THEN 'flow-evm'             -- Flow EVM
    WHEN chain_id = 31337 THEN 'eth'                -- Hardhat本地测试网
    WHEN chain_id = 1337 THEN 'eth'                 -- 本地测试网
    ELSE CONCAT('chain_', chain_id)                 -- 未知链ID，使用chain_前缀
END
WHERE network IS NULL;

-- 显示更新结果
DO $$
DECLARE
    updated_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO updated_count FROM vault_contracts WHERE network IS NOT NULL;
    RAISE NOTICE 'Updated % records with network values', updated_count;
END $$;

-- ==========================================
-- Step 3: 设置network字段为NOT NULL
-- ==========================================

-- 确保所有记录都有network值后，将字段设为NOT NULL
ALTER TABLE vault_contracts
ALTER COLUMN network SET NOT NULL;

-- ==========================================
-- Step 4: 处理唯一约束
-- ==========================================

-- 删除旧的唯一约束（如果存在）
DO $$
BEGIN
    -- 检查旧约束是否存在
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'uix_vault_contract_chain'
        AND table_name = 'vault_contracts'
        AND table_schema = 'public'
    ) THEN
        -- 删除旧的唯一约束
        ALTER TABLE vault_contracts
        DROP CONSTRAINT uix_vault_contract_chain;
        RAISE NOTICE 'Dropped old constraint uix_vault_contract_chain';
    END IF;
END $$;

-- 创建新的唯一约束，包含network
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'uix_vault_contract_network_chain_address'
        AND table_name = 'vault_contracts'
        AND table_schema = 'public'
    ) THEN
        ALTER TABLE vault_contracts
        ADD CONSTRAINT uix_vault_contract_network_chain_address
        UNIQUE (network, chain_id, contract_address);
        RAISE NOTICE 'Added new constraint uix_vault_contract_network_chain_address';
    ELSE
        RAISE NOTICE 'Constraint uix_vault_contract_network_chain_address already exists';
    END IF;
END $$;

-- ==========================================
-- Step 5: 创建新的索引
-- ==========================================

-- 为network字段创建索引
CREATE INDEX IF NOT EXISTS idx_vault_contracts_network
ON vault_contracts (network);

-- 创建复合索引：network + contract_address
CREATE INDEX IF NOT EXISTS idx_vault_contracts_network_address
ON vault_contracts (network, contract_address);

-- 创建复合索引：network + chain_id
CREATE INDEX IF NOT EXISTS idx_vault_contracts_network_chain
ON vault_contracts (network, chain_id);

-- ==========================================
-- Step 6: 验证迁移结果
-- ==========================================

-- 显示迁移结果统计
DO $$
DECLARE
    total_records INTEGER;
    unique_networks INTEGER;
    networks_list TEXT;
BEGIN
    SELECT COUNT(*) INTO total_records FROM vault_contracts;
    SELECT COUNT(DISTINCT network) INTO unique_networks FROM vault_contracts;
    SELECT string_agg(DISTINCT network, ', ' ORDER BY network) INTO networks_list FROM vault_contracts;

    RAISE NOTICE '=== 迁移完成统计 ===';
    RAISE NOTICE 'Total vault_contracts records: %', total_records;
    RAISE NOTICE 'Unique networks: %', unique_networks;
    RAISE NOTICE 'Networks found: %', COALESCE(networks_list, 'None');
END $$;

-- 显示字段信息
SELECT
    'vault_contracts表network字段迁移完成' as status,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'vault_contracts'
AND table_schema = 'public'
AND column_name IN ('network', 'chain_id', 'contract_address')
ORDER BY ordinal_position;

-- 显示约束信息
SELECT
    constraint_name,
    constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'vault_contracts'
AND table_schema = 'public'
AND constraint_type = 'UNIQUE';
