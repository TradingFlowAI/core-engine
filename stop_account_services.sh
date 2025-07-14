#!/bin/bash
# filepath: /Users/fudata/work/github/TradingFlow/stop_account_services.sh

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

LOG_DIR="logs"
PID_FILE="$LOG_DIR/service_pids.txt"

echo -e "${RED}==== Stopping TradingFlow Services ====${NC}"

# Check if PID file exists
if [ -f "$PID_FILE" ]; then
    # Read PIDs from file
    read SERVER_PID BEAT_PID WORKER_PID MONITOR_PID < "$PID_FILE"

    # Function to stop a process with a specific name for logging
    stop_process() {
        local pid=$1
        local name=$2

        echo -e "${BLUE}Stopping $name (PID: $pid)...${NC}"

        # Check if process is still running
        if ps -p $pid > /dev/null; then
            kill $pid
            sleep 1

            # If still running, force kill
            if ps -p $pid > /dev/null; then
                echo -e "${YELLOW}Process $pid is still running, force killing...${NC}"
                kill -9 $pid
                sleep 1
            fi

            # Check if successfully killed
            if ! ps -p $pid > /dev/null; then
                echo -e "${GREEN}✓${NC} $name stopped"
            else
                echo -e "${RED}✗${NC} Failed to stop $name!"
            fi
        else
            echo -e "${GREEN}✓${NC} $name is not running"
        fi
    }

    # Stop each service
    stop_process $MONITOR_PID "Event Monitor"
    stop_process $WORKER_PID "Celery Worker"
    stop_process $BEAT_PID "Celery Beat"
    stop_process $SERVER_PID "Account Manager Server"

    # Remove PID file
    rm -f "$PID_FILE"
    echo -e "${GREEN}==== All Services Stopped ====${NC}"

    # Optional: Check for any remaining processes matching our services
    REMAINING=$(ps aux | grep -E "python.account_manager|celery|python.monitor" | grep -v grep | grep -v "stop_account_services")
    if [ ! -z "$REMAINING" ]; then
        echo -e "${YELLOW}Warning: Some related processes might still be running:${NC}"
        echo "$REMAINING"
        echo -e "${YELLOW}You may need to stop them manually.${NC}"
    fi
else
    echo -e "${RED}PID file not found. Services may not be running or were started differently.${NC}"

    # Alternative approach: Find and kill by process names
    echo -e "${YELLOW}Attempting to find and stop services by name...${NC}"

    # Find and kill Account Manager
    pkill -f "python.account_manager.server"

    # Find and kill Celery processes
    pkill -f "celery -A python.account_manager.celery_worker"

    # Find and kill Monitor
    # pkill -f "python.monitor.event_listener"

    echo -e "${GREEN}Done. Any matching processes should be stopped.${NC}"
fi

# Final cleanup - check for zombie celery processes which can sometimes remain
CELERY_ZOMBIES=$(ps aux | grep celery | grep -v grep)
if [ ! -z "$CELERY_ZOMBIES" ]; then
    echo -e "${YELLOW}Checking for remaining celery processes...${NC}"
    echo "$CELERY_ZOMBIES"
    echo -e "${BLUE}Attempting to stop remaining celery processes...${NC}"
    pkill -f celery
    sleep 1
    pkill -9 -f celery
    echo -e "${GREEN}Cleanup complete.${NC}"
fi

echo -e "${GREEN}==== Service Shutdown Complete ====${NC}"
