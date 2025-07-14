-- 初始化 TradingFlow 数据库
-- 此脚本将在 PostgreSQL 容器首次启动时执行

-- 创建扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- 确保 public schema 存在（通常默认存在）
-- CREATE SCHEMA IF NOT EXISTS public;

-- 设置搜索路径到 public schema
ALTER DATABASE tradingflow SET search_path TO public;

-- 设置权限
GRANT ALL PRIVILEGES ON SCHEMA public TO tradingflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO tradingflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO tradingflow;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO tradingflow;

-- 注意：实际的表创建和数据迁移应该通过应用程序的迁移工具完成
-- 这个脚本只是设置基本的数据库环境
