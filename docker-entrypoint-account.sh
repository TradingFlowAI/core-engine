#!/bin/bash

# 创建日志目录
LOG_DIR="/app/logs"
mkdir -p $LOG_DIR

# 设置日志文件路径
SERVER_LOG="$LOG_DIR/account_manager_server.log"
CELERY_BEAT_LOG="$LOG_DIR/celery_beat.log"
CELERY_WORKER_LOG="$LOG_DIR/celery_worker.log"
MONITOR_LOG="$LOG_DIR/monitor.log"

echo "==== Starting TradingFlow Account Services ===="

# 1. 启动 account_manager 服务器
echo "[1/4] Starting Account Manager service..."
python -m tradingflow.account_manager.server > $SERVER_LOG 2>&1 &
SERVER_PID=$!
echo "✓ Account Manager service started, PID: $SERVER_PID"

# 等待服务器启动
sleep 3
echo "Account Manager initialized"

# 2. 启动 celery beat 服务
echo "[2/4] Starting Celery Beat..."
celery -A tradingflow.account_manager.celery_worker beat --loglevel=info > $CELERY_BEAT_LOG 2>&1 &
BEAT_PID=$!
echo "✓ Celery Beat started, PID: $BEAT_PID"

# 3. 启动 celery worker 服务
echo "[3/4] Starting Celery Worker..."
celery -A tradingflow.account_manager.celery_worker worker --loglevel=info > $CELERY_WORKER_LOG 2>&1 &
WORKER_PID=$!
echo "✓ Celery Worker started, PID: $WORKER_PID"

# 4. 启动 monitor 服务 (CL移动到Companion JS去了)
# echo "[4/4] Starting Event Listener..."
# python -m tradingflow.monitor.event_listener > $MONITOR_LOG 2>&1 &
# MONITOR_PID=$!
# echo "✓ Event Listener started, PID: $MONITOR_PID"

# 记录所有 PID
echo "$SERVER_PID $BEAT_PID $WORKER_PID $MONITOR_PID" > $LOG_DIR/service_pids.txt

echo "==== All Services Started ===="
echo "Account Manager Server: PID $SERVER_PID"
echo "Celery Beat:           PID $BEAT_PID"
echo "Celery Worker:         PID $WORKER_PID"
# echo "Event Monitor:         PID $MONITOR_PID"

# 保持容器运行
echo "Services running. Press CTRL+C to stop..."
tail -f $SERVER_LOG $CELERY_BEAT_LOG $CELERY_WORKER_LOG $MONITOR_LOG
