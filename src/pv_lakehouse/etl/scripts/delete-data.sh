#!/bin/bash
# Wrapper script to delete data from PV Lakehouse using spark-submit
# This script provides a convenient interface to the delete_data.py script

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
DELETE_SCRIPT="src/pv_lakehouse/etl/scripts/delete_data.py"
SPARK_SUBMIT_WRAPPER="$PROJECT_ROOT/src/pv_lakehouse/etl/scripts/spark-submit.sh"

# Usage message
usage() {
    cat << EOF
${BLUE}PV Lakehouse Data Deletion Tool${NC}

${GREEN}Usage:${NC} $0 [OPTIONS]

${GREEN}Options:${NC}
  --start-datetime DATETIME    Start datetime (ISO format: YYYY-MM-DDTHH:MM:SS)
                               Required unless --delete-table is Y
  
  --end-datetime DATETIME      End datetime (ISO format: YYYY-MM-DDTHH:MM:SS)
                               If not provided, uses current datetime
  
  --layers LAYERS              Comma-separated layers (bronze,silver,gold)
                               Default: all layers
  
  --tables TABLES              Comma-separated table names
                               Default: all tables in selected layers
  
  --delete-table Y|N           Delete entire table(s) (Y/N)
                               Default: N (delete data only)
  
  --dry-run                    Show what would be deleted without actually deleting
  
  --max-workers N              Maximum parallel deletion threads
                               Default: 4
  
  --help                       Show this help message

${GREEN}Examples:${NC}

  # Dry-run: See what would be deleted from all layers since 2025-10-01
  $0 --start-datetime "2025-10-01T00:00:00" --dry-run

  # Delete data from bronze layer in a specific date range
  $0 --start-datetime "2025-10-01T00:00:00" \\
     --end-datetime "2025-10-31T23:59:59" \\
     --layers bronze

  # Delete data from specific tables
  $0 --start-datetime "2025-10-01T00:00:00" \\
     --tables raw_facilities,clean_facility_master

  # Delete specific table completely (drop table)
  $0 --delete-table Y --layers bronze --tables raw_facilities

  # Delete all data from silver layer (all time)
  $0 --start-datetime "2020-01-01T00:00:00" --layers silver

${YELLOW}Safety Features:${NC}
  - Dry-run preview before execution
  - User must type 'DELETE' to confirm
  - Validates inputs before proceeding
  - Thread-safe parallel execution
  - Complete cleanup across all systems

${YELLOW}Note:${NC} This script uses spark-submit and requires the Docker stack to be running.

EOF
    exit 0
}

# Check if help is requested
if [[ "${1:-}" == "--help" ]] || [[ "${1:-}" == "-h" ]]; then
    usage
fi

# Check if Docker stack is running
echo -e "${BLUE}Checking Docker stack...${NC}"
if ! docker compose -f "$PROJECT_ROOT/docker/docker-compose.yml" ps | grep -q "spark-master.*Up"; then
    echo -e "${RED}Error: Spark master container is not running${NC}"
    echo -e "${YELLOW}Start the stack with: cd docker && docker compose --profile core up -d${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker stack is running${NC}\n"

# Check if script exists
if [ ! -f "$PROJECT_ROOT/$DELETE_SCRIPT" ]; then
    echo -e "${RED}Error: Delete script not found: $DELETE_SCRIPT${NC}"
    exit 1
fi

# Check if spark-submit wrapper exists
if [ ! -f "$SPARK_SUBMIT_WRAPPER" ]; then
    echo -e "${RED}Error: Spark submit wrapper not found: $SPARK_SUBMIT_WRAPPER${NC}"
    exit 1
fi

# Pass all arguments to the Python script via spark-submit wrapper
echo -e "${BLUE}Launching data deletion tool...${NC}\n"
cd "$PROJECT_ROOT"

# Execute via spark-submit wrapper
bash "$SPARK_SUBMIT_WRAPPER" "$DELETE_SCRIPT" "$@"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "\n${GREEN}✓ Data deletion completed successfully${NC}"
else
    echo -e "\n${RED}✗ Data deletion failed with exit code $EXIT_CODE${NC}"
fi

exit $EXIT_CODE
