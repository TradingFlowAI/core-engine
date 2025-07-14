#!/bin/bash
# filepath: /Users/fudata/work/github/TradingFlow/start_account_services.sh

# Create log directory
LOG_DIR="logs"
mkdir -p $LOG_DIR

# Set log file paths
SERVER_LOG="$LOG_DIR/account_manager_server_stdout.log"
CELERY_BEAT_LOG="$LOG_DIR/celery_beat_stdout.log"
CELERY_WORKER_LOG="$LOG_DIR/celery_worker_stdout.log"
MONITOR_LOG="$LOG_DIR/monitor_stdout.log"

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}==== Starting TradingFlow Services ====${NC}"

# 1. Start account_manager server
echo -e "${BLUE}[1/4]${NC} Starting Account Manager service..."
python -m tradingflow.bank.server > $SERVER_LOG 2>&1 &
SERVER_PID=$!
echo -e "${GREEN}✓${NC} Account Manager service started, PID: $SERVER_PID, log: $SERVER_LOG"

# Wait for server to start
sleep 2
echo "Waiting for server to fully initialize..."
sleep 3

# 2. Start celery beat service
echo -e "${BLUE}[2/4]${NC} Starting Celery Beat..."
celery -A tradingflow.bank.celery_worker beat --loglevel=info > $CELERY_BEAT_LOG 2>&1 &
BEAT_PID=$!
echo -e "${GREEN}✓${NC} Celery Beat started, PID: $BEAT_PID, log: $CELERY_BEAT_LOG"

# 3. Start celery worker service
echo -e "${BLUE}[3/4]${NC} Starting Celery Worker..."
celery -A tradingflow.bank.celery_worker worker --loglevel=info > $CELERY_WORKER_LOG 2>&1 &
WORKER_PID=$!
echo -e "${GREEN}✓${NC} Celery Worker started, PID: $WORKER_PID, log: $CELERY_WORKER_LOG"

# 4. Start monitor service
# echo -e "${BLUE}[4/4]${NC} Starting Event Listener..."
# python -m tradingflow.monitor.event_listener > $MONITOR_LOG 2>&1 &
# MONITOR_PID=$!
# echo -e "${GREEN}✓${NC} Event Listener started, PID: $MONITOR_PID, log: $MONITOR_LOG"

# Record all PIDs to file for later termination
echo "$SERVER_PID $BEAT_PID $WORKER_PID $MONITOR_PID" > $LOG_DIR/service_pids.txt

# Display all started services
echo -e "\n${GREEN}==== All Services Started ====${NC}"
echo -e "Account Manager Server: PID $SERVER_PID"
echo -e "Celery Beat:           PID $BEAT_PID"
echo -e "Celery Worker:         PID $WORKER_PID"
# echo -e "Event Monitor:         PID $MONITOR_PID"

echo -e "\n${YELLOW}==== Service Information ====${NC}"
echo -e "Log files:"
echo -e "  Account Manager: $SERVER_LOG"
echo -e "  Celery Beat:     $CELERY_BEAT_LOG"
echo -e "  Celery Worker:   $CELERY_WORKER_LOG"
# echo -e "  Event Monitor:   $MONITOR_LOG"
echo -e "PID file: $LOG_DIR/service_pids.txt"

echo -e "\n${YELLOW}==== Useful Commands ====${NC}"
echo -e "View individual logs:"
echo -e "  Account Manager: ${BLUE}tail -f $SERVER_LOG${NC}"
echo -e "  Celery Beat:     ${BLUE}tail -f $CELERY_BEAT_LOG${NC}"
echo -e "  Celery Worker:   ${BLUE}tail -f $CELERY_WORKER_LOG${NC}"
# echo -e "  Event Monitor:   ${BLUE}tail -f $MONITOR_LOG${NC}"

echo -e "\nView all logs together:"
echo -e "  ${BLUE}tail -f $SERVER_LOG $CELERY_BEAT_LOG $CELERY_WORKER_LOG${NC}"

echo -e "\nOther useful commands:"
echo -e "  Check status:    ${BLUE}ps aux | grep -E 'account_manager|celery'${NC}"
echo -e "  Stop services:   ${BLUE}./stop_account_services.sh${NC}"
