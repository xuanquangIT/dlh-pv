#!/bin/bash
# Data Lakehouse Health Check
# Comprehensive validation of all services, configurations, and connectivity

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0

echo ""
echo "=========================================="
echo "   Data Lakehouse Health Check"
echo "=========================================="
echo ""

check_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASS++))
}

check_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((FAIL++))
}

check_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
    ((WARN++))
}

section() {
    echo ""
    echo -e "${BLUE}=== $1 ===${NC}"
}

# 1. Docker Services Health
section "1. Docker Services"

REQUIRED_SERVICES=(postgres minio trino spark-master)
for service in "${REQUIRED_SERVICES[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${service}$"; then
        HEALTH=$(docker inspect --format='{{.State.Health.Status}}' "$service" 2>/dev/null || echo "no-healthcheck")
        if [ "$HEALTH" = "healthy" ] || [ "$HEALTH" = "no-healthcheck" ]; then
            check_pass "Service '$service' is running"
        else
            check_fail "Service '$service' is unhealthy (status: $HEALTH)"
        fi
    else
        check_fail "Service '$service' is not running"
    fi
done

# Check optional services
OPTIONAL_SERVICES=(mlflow spark-worker pgadmin)
for service in "${OPTIONAL_SERVICES[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${service}$"; then
        HEALTH=$(docker inspect --format='{{.State.Health.Status}}' "$service" 2>/dev/null || echo "no-healthcheck")
        if [ "$HEALTH" = "healthy" ] || [ "$HEALTH" = "no-healthcheck" ]; then
            check_pass "Optional service '$service' is running"
        else
            check_warn "Optional service '$service' is unhealthy"
        fi
    fi
done

# 2. PostgreSQL Databases
section "2. PostgreSQL Databases"

REQUIRED_DBS=(iceberg_catalog mlflow)
for db in "${REQUIRED_DBS[@]}"; do
    if docker exec postgres psql -U pvlakehouse -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" 2>/dev/null | grep -q 1; then
        check_pass "Database '$db' exists"
    else
        check_fail "Database '$db' is missing"
    fi
done

# 3. Iceberg Catalog Tables
section "3. Iceberg Catalog Schema"

ICEBERG_TABLES=(iceberg_tables iceberg_namespace_properties)
for table in "${ICEBERG_TABLES[@]}"; do
    if docker exec postgres psql -U pvlakehouse -d iceberg_catalog -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='$table'" 2>/dev/null | grep -q 1; then
        check_pass "Table '$table' exists in iceberg_catalog"
    else
        check_fail "Table '$table' is missing from iceberg_catalog"
    fi
done

# 4. Trino Connectivity
section "4. Trino Connectivity"

# Check Trino can execute queries
if docker exec -i trino trino --execute "SELECT 1;" 2>&1 | grep -v WARNING | grep -q '"1"'; then
    check_pass "Trino can execute basic queries"
else
    check_fail "Trino cannot execute queries"
fi

# Check Iceberg catalog
if docker exec -i trino trino --execute "SHOW CATALOGS;" 2>&1 | grep -v WARNING | grep -q '"iceberg"'; then
    check_pass "Trino can see 'iceberg' catalog"
else
    check_fail "Trino cannot see 'iceberg' catalog"
fi

# Check schemas
SCHEMAS=$(docker exec -i trino trino --execute "SHOW SCHEMAS FROM iceberg;" 2>&1 | grep -v WARNING)
if echo "$SCHEMAS" | grep -q '"lh"'; then
    check_pass "Schema 'lh' exists in Iceberg catalog"
else
    check_warn "Schema 'lh' does not exist - will be created on first use"
fi

# 5. MinIO Storage
section "5. MinIO Storage"

# Check buckets
REQUIRED_BUCKETS=(lakehouse mlflow)
for bucket in "${REQUIRED_BUCKETS[@]}"; do
    if docker exec minio sh -c "mc alias set tmp http://localhost:9000 \$MINIO_ROOT_USER \$MINIO_ROOT_PASSWORD >/dev/null 2>&1 && mc ls tmp/$bucket >/dev/null 2>&1"; then
        check_pass "Bucket '$bucket' exists in MinIO"
    else
        check_fail "Bucket '$bucket' is missing from MinIO"
    fi
done

# Check service users
SERVICE_USERS=(spark_svc trino_svc mlflow_svc)
for user in "${SERVICE_USERS[@]}"; do
    if docker exec minio sh -c "mc alias set tmp http://localhost:9000 \$MINIO_ROOT_USER \$MINIO_ROOT_PASSWORD >/dev/null 2>&1 && mc admin user list tmp/ 2>/dev/null" | grep -q "$user"; then
        check_pass "Service user '$user' exists in MinIO"
    else
        check_fail "Service user '$user' is missing from MinIO"
    fi
done

# Check policies
REQUIRED_POLICIES=(lakehouse-rw mlflow-rw)
for policy in "${REQUIRED_POLICIES[@]}"; do
    if docker exec minio sh -c "mc alias set tmp http://localhost:9000 \$MINIO_ROOT_USER \$MINIO_ROOT_PASSWORD >/dev/null 2>&1 && mc admin policy list tmp/ 2>/dev/null" | grep -q "$policy"; then
        check_pass "Policy '$policy' exists in MinIO"
    else
        check_fail "Policy '$policy' is missing from MinIO"
    fi
done

# 6. End-to-End S3A Test
section "6. End-to-End S3A Test"

# Test Trino can write to MinIO via S3A
TEST_TABLE="iceberg.lh.health_check_$(date +%s)"
if docker exec -i trino trino --execute "CREATE TABLE $TEST_TABLE (id INT); INSERT INTO $TEST_TABLE VALUES (1); DROP TABLE $TEST_TABLE;" 2>&1 | grep -v WARNING | grep -q "DROP TABLE"; then
    check_pass "Trino can write to MinIO via S3A"
else
    check_warn "Could not verify S3A write access (schema may not exist yet)"
fi

# 7. Service Endpoints
section "7. Service Endpoints"

ENDPOINTS=(
    "MinIO_API:http://localhost:9000/minio/health/ready"
    "MinIO_Console:http://localhost:9001"
    "Trino_UI:http://localhost:8081/v1/info"
    "Spark_Master:http://localhost:4040"
)

for endpoint in "${ENDPOINTS[@]}"; do
    IFS=':' read -r name url <<< "$endpoint"
    url="${url#*:}"
    url="http:$url"
    
    if curl -sf "$url" > /dev/null 2>&1; then
        check_pass "$name is responding"
    else
        check_warn "$name is not responding"
    fi
done

# Check optional endpoints
OPTIONAL_ENDPOINTS=(
    "MLflow:http://localhost:5000/"
)

for endpoint in "${OPTIONAL_ENDPOINTS[@]}"; do
    IFS=':' read -r name url <<< "$endpoint"
    url="${url#*:}"
    url="http:$url"
    
    if curl -sf "$url" > /dev/null 2>&1; then
        check_pass "Optional: $name is responding"
    fi
done

# 8. Configuration Files
section "8. Configuration Files"

CONFIG_FILES=(
    "./trino/catalog/iceberg.properties:Trino Iceberg catalog config"
    "../infra/minio/policies/lakehouse-rw.json:MinIO lakehouse-rw policy"
    "../infra/minio/policies/mlflow-rw.json:MinIO mlflow-rw policy"
    "./postgres/postgres-init.sql:PostgreSQL init script"
    "./spark/core-site.xml:Spark Hadoop config"
)

for config in "${CONFIG_FILES[@]}"; do
    IFS=':' read -r file desc <<< "$config"
    if [ -f "$file" ]; then
        check_pass "$desc exists"
    else
        check_fail "$desc NOT found: $file"
    fi
done

# Summary
section "Summary"

echo ""
echo "Results:"
echo -e "  ${GREEN}PASS: $PASS${NC}"
echo -e "  ${YELLOW}WARN: $WARN${NC}"
echo -e "  ${RED}FAIL: $FAIL${NC}"
echo ""

if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}All critical checks passed!${NC}"
    echo ""
    echo "Stack Status: HEALTHY"
    echo ""
    echo "Next steps:"
    echo "  1. Run smoke test: cd /home/pvlakehouse/dlh-pv/src/tools/spark_smoke && ./smoke_test.sh"
    echo "  2. Access Trino UI: http://localhost:8081"
    echo "  3. Access MinIO Console: http://localhost:9001 (pvlakehouse/pvlakehouse)"
    echo "  4. Access Spark UI: http://localhost:4040"
    echo ""
    
    if [ $WARN -gt 0 ]; then
        echo -e "${YELLOW}Note: $WARN warnings found - non-critical issues or optional services${NC}"
        echo ""
    fi
    
    exit 0
else
    echo -e "${RED}Health check FAILED!${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "  - Check logs: docker compose logs <service-name>"
    echo "  - Restart services: docker compose restart"
    echo "  - View status: docker compose ps"
    echo "  - Fresh start: docker compose down && docker compose --profile core up -d"
    echo ""
    exit 1
fi
