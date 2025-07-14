#!/bin/bash

# 创建日志目录
LOG_DIR="/app/logs"
mkdir -p $LOG_DIR

# 设置日志文件路径
WORKER_LOG="$LOG_DIR/py_worker_server.log"

echo "==== Starting TradingFlow Python Worker ===="

# 启动 py_worker 服务器
echo "[1/1] Starting Python Worker service..."
python -m tradingflow.py_worker.server > $WORKER_LOG 2>&1 &
WORKER_PID=$!
echo "✓ Python Worker service started, PID: $WORKER_PID"

# 等待 worker 启动
sleep 3
echo "Python Worker initialized"

# 记录 PID 到文件
echo "$WORKER_PID" > $LOG_DIR/py_worker_pids.txt

# 显示启动的服务
echo "==== Python Worker Started ===="
echo "Python Worker Server: PID $WORKER_PID"

# 保持容器运行
echo "Worker service running. Press CTRL+C to stop..."
tail -f $WORKER_LOG
