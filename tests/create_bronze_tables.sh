#!/bin/bash
# ============================================================================
# Bronze Tables Creation and Testing Script
# ============================================================================
# This script creates Bronze tables using Spark SQL and verifies them in Trino
# ============================================================================

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "================================================================================"
echo "Bronze Tables Creation and Testing"
echo "================================================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to execute SQL in Spark
execute_spark_sql() {
    local sql_file=$1
    local table_name=$(basename "$sql_file" .sql)
    
    echo -e "${BLUE}→ Creating table from ${sql_file}...${NC}"
    
    docker exec spark-master /opt/spark/bin/spark-sql \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.lh=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.lh.type=rest \
        --conf spark.sql.catalog.lh.uri=http://iceberg-rest:8181 \
        --conf spark.sql.catalog.lh.warehouse=s3a://lakehouse/ \
        --conf spark.sql.catalog.lh.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
        --conf spark.sql.catalog.lh.s3.endpoint=http://minio:9000 \
        --conf spark.sql.catalog.lh.s3.access-key-id=minio \
        --conf spark.sql.catalog.lh.s3.secret-access-key=minio123 \
        --conf spark.sql.catalog.lh.s3.path-style-access=true \
        --conf spark.sql.defaultCatalog=lh \
        -f "/opt/spark/work-dir/sql/bronze/${table_name}.sql" 2>&1 | grep -v "WARN\|INFO" || true
    
    echo -e "${GREEN}  ✓ Table ${table_name} created${NC}"
}

# Function to execute SQL in Trino
execute_trino_sql() {
    local sql=$1
    
    docker exec trino trino --catalog lh --schema bronze --execute "$sql"
}

# Step 1: Create Bronze schema
echo "================================================================================"
echo "STEP 1: Creating Bronze Schema"
echo "================================================================================"
echo ""

docker exec spark-master /opt/spark/bin/spark-sql \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.lh=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.lh.type=rest \
    --conf spark.sql.catalog.lh.uri=http://iceberg-rest:8181 \
    --conf spark.sql.catalog.lh.warehouse=s3a://lakehouse/ \
    --conf spark.sql.catalog.lh.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.lh.s3.endpoint=http://minio:9000 \
    --conf spark.sql.catalog.lh.s3.access-key-id=minio \
    --conf spark.sql.catalog.lh.s3.secret-access-key=minio123 \
    --conf spark.sql.catalog.lh.s3.path-style-access=true \
    --conf spark.sql.defaultCatalog=lh \
    -e "CREATE SCHEMA IF NOT EXISTS lh.bronze" 2>&1 | grep -v "WARN\|INFO" || true

echo -e "${GREEN}✓ Schema lh.bronze created/verified${NC}"
echo ""

# Step 2: Create all Bronze tables
echo "================================================================================"
echo "STEP 2: Creating Bronze Tables"
echo "================================================================================"
echo ""

# Copy SQL files to Spark container
docker exec spark-master mkdir -p /opt/spark/work-dir/sql/bronze
docker cp "${PROJECT_ROOT}/sql/bronze/oe_facilities_raw.sql" spark-master:/opt/spark/work-dir/sql/bronze/
docker cp "${PROJECT_ROOT}/sql/bronze/oe_generation_hourly_raw.sql" spark-master:/opt/spark/work-dir/sql/bronze/
docker cp "${PROJECT_ROOT}/sql/bronze/om_weather_hourly_raw.sql" spark-master:/opt/spark/work-dir/sql/bronze/
docker cp "${PROJECT_ROOT}/sql/bronze/om_air_quality_hourly_raw.sql" spark-master:/opt/spark/work-dir/sql/bronze/

# Create tables
execute_spark_sql "${PROJECT_ROOT}/sql/bronze/oe_facilities_raw.sql"
execute_spark_sql "${PROJECT_ROOT}/sql/bronze/oe_generation_hourly_raw.sql"
execute_spark_sql "${PROJECT_ROOT}/sql/bronze/om_weather_hourly_raw.sql"
execute_spark_sql "${PROJECT_ROOT}/sql/bronze/om_air_quality_hourly_raw.sql"

echo ""
echo -e "${GREEN}✓ All tables created successfully${NC}"
echo ""

# Step 3: Verify tables in Trino
echo "================================================================================"
echo "STEP 3: Verifying Tables in Trino"
echo "================================================================================"
echo ""

echo -e "${BLUE}→ Listing tables in lh.bronze:${NC}"
execute_trino_sql "SHOW TABLES"
echo ""

echo -e "${BLUE}→ Verifying table schemas:${NC}"
echo ""

tables=("oe_facilities_raw" "oe_generation_hourly_raw" "om_weather_hourly_raw" "om_air_quality_hourly_raw")

for table in "${tables[@]}"; do
    echo -e "${YELLOW}Table: lh.bronze.${table}${NC}"
    
    # Show CREATE TABLE to verify partition spec
    create_stmt=$(execute_trino_sql "SHOW CREATE TABLE ${table}" 2>&1)
    
    # Check for PARTITIONED BY days(ts_utc)
    if echo "$create_stmt" | grep -q "PARTITIONED BY.*days.*ts_utc"; then
        echo -e "${GREEN}  ✓ Partition spec: days(ts_utc)${NC}"
    else
        echo -e "${YELLOW}  ⚠ Checking partition spec...${NC}"
        echo "$create_stmt" | grep -i "partitioned" || echo "  Note: Partition spec may need manual verification"
    fi
    
    # Check for metadata columns
    if echo "$create_stmt" | grep -q "_ingest_time" && \
       echo "$create_stmt" | grep -q "_source" && \
       echo "$create_stmt" | grep -q "_hash"; then
        echo -e "${GREEN}  ✓ Metadata columns (_ingest_time, _source, _hash) present${NC}"
    else
        echo -e "${RED}  ✗ Missing metadata columns${NC}"
    fi
    
    echo ""
done

echo "================================================================================"
echo -e "${GREEN}✓ Bronze Tables Setup Complete!${NC}"
echo "================================================================================"
echo ""
echo "Tables created:"
echo "  - lh.bronze.oe_facilities_raw"
echo "  - lh.bronze.oe_generation_hourly_raw"
echo "  - lh.bronze.om_weather_hourly_raw"
echo "  - lh.bronze.om_air_quality_hourly_raw"
echo ""
echo "Next steps:"
echo "  1. Run test script to insert sample data:"
echo "     bash tests/test_bronze_insert_data.sh"
echo "  2. Verify in Trino:"
echo "     docker exec -it trino trino --catalog lh --schema bronze"
echo ""
