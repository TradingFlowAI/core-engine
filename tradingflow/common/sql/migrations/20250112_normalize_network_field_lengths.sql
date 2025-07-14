-- 数据库迁移脚本：统一网络字段长度
-- 创建时间: 2025-01-12
-- 描述: 统一所有表中network和network_type字段的长度为VARCHAR(50)

-- 设置当前schema
SET search_path TO public;

-- ==========================================
-- 统一网络字段长度为VARCHAR(50)
-- ==========================================

-- 修改monitored_tokens表的network和network_type字段
ALTER TABLE monitored_tokens
ALTER COLUMN network TYPE VARCHAR(50),
ALTER COLUMN network_type TYPE VARCHAR(50);

-- 修改vault_operation_history表的network和network_type字段
ALTER TABLE vault_operation_history
ALTER COLUMN network TYPE VARCHAR(50),
ALTER COLUMN network_type TYPE VARCHAR(50);

-- 修改token_price_history表的network字段
ALTER TABLE token_price_history
ALTER COLUMN network TYPE VARCHAR(50);

-- ==========================================
-- 验证修改结果
-- ==========================================

-- 查询所有表的网络字段定义
SELECT
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
    AND column_name IN ('network', 'network_type')
    AND table_name IN (
        'contract_events',
        'monitored_contracts',
        'vault_contracts',
        'monitored_tokens',
        'vault_operation_history',
        'token_price_history'
    )
ORDER BY table_name, column_name;

-- 显示完成信息
SELECT 'Network字段长度统一完成：所有network和network_type字段现在都是VARCHAR(50)' as status;
