-- TradingFlow 数据库完全清理并重建脚本
-- 解决schema混乱问题

-- ==========================================
-- 方案1: 强制清理并使用 public schema（推荐）
-- ==========================================

-- 删除所有可能的表和函数
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;
DROP FUNCTION IF EXISTS update_event_processor_state_updated_at() CASCADE;

-- 清理所有表
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

-- 强制删除所有匹配的表（以防万一）
DO $$
DECLARE
    r RECORD;
BEGIN
    -- 删除所有以这些名称开头的表
    FOR r IN SELECT tablename FROM pg_tables WHERE tablename IN (
        'flow_execution_logs', 'token_price_history', 'vault_operation_history',
        'event_processor_state', 'vault_value_history', 'monitored_tokens',
        'vault_contracts', 'users', 'listener_state', 'monitored_contracts', 'contract_events'
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;

-- 现在在public schema中重新创建所有表
-- 注意: 这里使用您原来的create-tradingflow-schema.sql内容，现在已经统一使用public schema
