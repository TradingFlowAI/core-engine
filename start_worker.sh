#!/bin/bash

# Create log directory
LOG_DIR="logs"
mkdir -p $LOG_DIR

# Set log file paths
WORKER_LOG="$LOG_DIR/py_worker_server_stdout.log"

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}==== Starting TradingFlow Python Worker ====${NC}"

# Start py_worker server
echo -e "${BLUE}[1/1]${NC} Starting Python Worker service..."
python -m tradingflow.station.server > $WORKER_LOG 2>&1 &
WORKER_PID=$!
echo -e "${GREEN}✓${NC} Python Worker service started, PID: $WORKER_PID, log: $WORKER_LOG"

# Wait for worker to start
sleep 2
echo "Waiting for worker to fully initialize..."
sleep 3

# Record PID to file for later termination
echo "$WORKER_PID" > $LOG_DIR/py_worker_pids.txt

# Display started service
echo -e "\n${GREEN}==== Python Worker Started ====${NC}"
echo -e "Python Worker Server: PID $WORKER_PID"

echo -e "\n${YELLOW}==== Service Information ====${NC}"
echo -e "Log file: $WORKER_LOG"
echo -e "PID file: $LOG_DIR/py_worker_pids.txt"

echo -e "\n${YELLOW}==== Useful Commands ====${NC}"
echo -e "View logs:     ${BLUE}tail -f $WORKER_LOG${NC}"
echo -e "Check status:  ${BLUE}ps aux | grep py_worker${NC}"
echo -e "Stop service:  ${BLUE}kill $WORKER_PID${NC}"
