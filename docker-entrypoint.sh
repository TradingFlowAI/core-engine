#!/bin/bash

# 创建日志目录
LOG_DIR="/app/logs"
mkdir -p $LOG_DIR

# 设置日志文件路径
STATION_LOG="$LOG_DIR/py_station_server.log"

echo "==== Starting TradingFlow Python Station ===="

# 启动 station 服务器
echo "[1/1] Starting Python Station service..."
python -m tradingflow.station.server > $STATION_LOG 2>&1 &
STATION_PID=$!
echo "✓ Python Station service started, PID: $STATION_PID"

# 等待 station 启动
sleep 3
echo "Python Station initialized"

# 记录 PID 到文件
echo "$STATION_PID" > $LOG_DIR/py_station_pids.txt

# 显示启动的服务
echo "==== Python Station Started ===="
echo "Python Station Server: PID $STATION_PID"

# 保持容器运行
echo "Station service running. Press CTRL+C to stop..."
tail -f $STATION_LOG
