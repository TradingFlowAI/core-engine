#!/bin/bash
# filepath: /Users/fudata/work/github/TradingFlow/python/stop_worker.sh

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

echo -e "${YELLOW}Stopping Python Worker...${NC}"

# Check if PID file exists
if [ -f "logs/py_worker_pids.txt" ]; then
    PID=$(cat logs/py_worker_pids.txt)

    if ps -p $PID > /dev/null; then
        kill $PID
        echo -e "${GREEN}✓ Python Worker (PID: $PID) stopped${NC}"
        rm logs/py_worker_pids.txt
    else
        echo -e "${YELLOW}Python Worker process not found${NC}"
    fi
else
    echo -e "${YELLOW}PID file not found, trying to find process...${NC}"

    # Try to find and kill by process name
    PIDS=$(pgrep -f "tradingflow.station.server")
    if [ ! -z "$PIDS" ]; then
        echo $PIDS | xargs kill
        echo -e "${GREEN}✓ Found and stopped Python Worker processes${NC}"
    else
        echo -e "${RED}No Python Worker processes found${NC}"
    fi
fi
