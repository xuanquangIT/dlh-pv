#!/bin/bash
# Spark Submit Wrapper for PV Lakehouse ETL Jobs
# Usage: ./spark-submit.sh <script_path> [args...]
# Example: ./spark-submit.sh src/pv_lakehouse/etl/bronze/load_facilities.py --mode incremental

set -euo pipefail

# Configuration
COMPOSE_FILE="${COMPOSE_FILE:-docker/docker-compose.yml}"
CONTAINER="${SPARK_CONTAINER:-spark-master}"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-/opt/spark/bin/spark-submit}"
WORKDIR="${WORKDIR:-/opt/workdir}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Usage message
usage() {
    echo -e "${BLUE}Usage:${NC} $0 <script_path> [arguments...]"
    echo ""
    echo -e "${BLUE}Examples:${NC}"
    echo "  $0 src/pv_lakehouse/etl/bronze/load_facilities.py --mode incremental"
    echo "  $0 src/pv_lakehouse/etl/bronze/load_facility_timeseries.py --mode backfill --date-start 2025-10-01T00:00:00"
    echo "  $0 src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode incremental"
    echo ""
    echo -e "${BLUE}Environment Variables:${NC}"
    echo "  COMPOSE_FILE     Docker compose file path (default: docker/docker-compose.yml)"
    echo "  SPARK_CONTAINER  Container name (default: spark-master)"
    exit 1
}

# Check arguments
if [ $# -lt 1 ]; then
    echo -e "${RED}Error: Script path required${NC}"
    usage
fi

SCRIPT_PATH="$1"
shift  # Remove first argument, rest are script arguments

# Validate script exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo -e "${RED}Error: Script not found: $SCRIPT_PATH${NC}"
    exit 1
fi

# Check if container is running
if ! docker compose -f "$COMPOSE_FILE" ps | grep -q "$CONTAINER.*Up"; then
    echo -e "${RED}Error: $CONTAINER container is not running${NC}"
    echo -e "${YELLOW}Start services with: docker compose -f $COMPOSE_FILE up -d${NC}"
    exit 1
fi

# Build spark-submit command
SCRIPT_FULL_PATH="$WORKDIR/$SCRIPT_PATH"
SCRIPT_ARGS="$@"

echo -e "${GREEN}===========================================  ========${NC}"
echo -e "${GREEN}Spark Submit${NC}"
echo -e "${GREEN}===================================================${NC}"
echo -e "${BLUE}Container:${NC} $CONTAINER"
echo -e "${BLUE}Script:${NC}    $SCRIPT_PATH"
if [ -n "$SCRIPT_ARGS" ]; then
    echo -e "${BLUE}Arguments:${NC} $SCRIPT_ARGS"
fi
echo -e "${GREEN}===================================================${NC}"
echo ""

# Execute spark-submit in container
docker compose -f "$COMPOSE_FILE" exec "$CONTAINER" bash -lc "
    cd $WORKDIR && \
    PYTHONPATH=$WORKDIR/src \
    $SPARK_SUBMIT_BIN $SCRIPT_FULL_PATH $SCRIPT_ARGS
"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}===================================================${NC}"
    echo -e "${GREEN}Job completed successfully${NC}"
    echo -e "${GREEN}===================================================${NC}"
else
    echo -e "${RED}===================================================${NC}"
    echo -e "${RED}Job failed with exit code $EXIT_CODE${NC}"
    echo -e "${RED}===================================================${NC}"
fi

exit $EXIT_CODE
