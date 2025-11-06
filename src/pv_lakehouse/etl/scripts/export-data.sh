#!/bin/bash
# Export Iceberg tables (Bronze/Silver/Gold) to CSV format
# Each table is saved as a separate CSV file

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
EXPORT_DIR="${PROJECT_ROOT}/src/pv_lakehouse/exported_data"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}PV Lakehouse - CSV Export Tool${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Parse arguments
LAYERS="${1:-all}"
OUTPUT_DIR="${2:-$EXPORT_DIR}"

# Validate layers argument
if [[ ! "$LAYERS" =~ ^(bronze|silver|gold|all)$ ]]; then
    echo -e "${RED}Error: Invalid layer. Must be: bronze, silver, gold, or all${NC}"
    exit 1
fi

# Show configuration
echo -e "${YELLOW}Configuration:${NC}"
echo "  Layers to export: $LAYERS"
echo "  Output directory: $OUTPUT_DIR"
echo "  Script location: $SCRIPT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Run the Python script
echo -e "${YELLOW}Starting export...${NC}"
echo ""

# Use /tmp inside Docker for write operations
DOCKER_OUTPUT_DIR="/tmp/exported_data"

cd "$PROJECT_ROOT"
docker exec -it spark-master bash -c "cd /opt/workdir && \
    export PYTHONPATH=/opt/workdir/src:\$PYTHONPATH && \
    python3 /opt/workdir/src/pv_lakehouse/etl/scripts/export_to_csv.py \
    --output-dir '$DOCKER_OUTPUT_DIR' \
    --layers $LAYERS"

RESULT=$?

# Copy files from Docker to host
if [ $RESULT -eq 0 ]; then
    echo ""
    echo -e "${YELLOW}Copying files from Docker...${NC}"
    rm -rf "$OUTPUT_DIR"
    docker cp spark-master:$DOCKER_OUTPUT_DIR "$OUTPUT_DIR" || {
        echo -e "${RED}Warning: Could not copy files from Docker${NC}"
    }
fi

echo ""
if [ $RESULT -eq 0 ]; then
    echo -e "${GREEN}✓ Export completed successfully!${NC}"
    echo ""
    echo -e "${YELLOW}CSV files location:${NC}"
    echo "  $OUTPUT_DIR"
    echo ""
    echo -e "${YELLOW}Available files:${NC}"
    ls -lh "$OUTPUT_DIR"/*.csv 2>/dev/null || echo "  (No CSV files found)"
else
    echo -e "${RED}✗ Export failed with exit code $RESULT${NC}"
    exit $RESULT
fi
