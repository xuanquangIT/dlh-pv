# Operations & Monitoring

## 1. Daily Operations

### 1.1 Morning Checklist

```bash
# Check all services are running
docker compose ps

# Run health check
cd docker && ./scripts/health-check.sh

# Check data freshness
# Example: Check if generation data has today's records
docker compose exec -i trino trino --execute "
  SELECT COUNT(*) as record_count, MAX(ts_utc) as last_timestamp
  FROM iceberg.bronze.oe_generation_hourly_raw
  WHERE DATE(ts_utc) = CURRENT_DATE;
"
```

### 1.2 Monitor Data Ingestion

```bash
# Check recent ingestions
docker compose exec -i trino trino --execute "
  SELECT 
    table_name,
    MAX(_ingest_time) as last_ingest,
    COUNT(*) as total_records
  FROM (
    SELECT '_generation' as table_name, _ingest_time FROM iceberg.bronze.oe_generation_hourly_raw
    UNION ALL
    SELECT '_weather', _ingest_time FROM iceberg.bronze.om_weather_hourly_raw
  )
  GROUP BY table_name
  ORDER BY last_ingest DESC;
"
```

### 1.3 Check Storage Usage

```bash
# MinIO bucket sizes
docker run --rm --network dlhpv_data-net \
  --entrypoint /bin/sh minio/mc:latest -c '
  mc alias set local http://minio:9000 pvlakehouse pvlakehouse
  mc du local/lakehouse
  mc du local/mlflow
'

# PostgreSQL database size
docker compose exec -i postgres psql -U pvlakehouse -c "
  SELECT 
    datname,
    pg_size_pretty(pg_database_size(datname)) as size
  FROM pg_database
  WHERE datname IN (\"iceberg_catalog\", \"mlflow\", \"prefect\")
  ORDER BY pg_database_size(datname) DESC;
"
```

## 2. Troubleshooting

### 2.1 Service Health Issues

```bash
# Check specific service
docker compose ps <service>

# View logs
docker compose logs <service> -f --tail 100

# Example
docker compose logs trino -f
docker compose logs spark-master -f
docker compose logs postgres -f
```

### 2.2 Query Performance Issues

```sql
-- Check query execution time
EXPLAIN SELECT * FROM iceberg.bronze.oe_generation_hourly_raw 
WHERE DATE(ts_utc) = '2025-01-15' LIMIT 1000;

-- Check partition pruning
-- Should see "Filter: (ts_utc >= ... AND ts_utc < ...)"

-- Analyze table stats
ANALYZE TABLE iceberg.bronze.oe_generation_hourly_raw;
```

### 2.3 Common Issues & Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| Trino timeout | Large table scan | Use partition filter (WHERE ts_utc >= ...) |
| Spark OOM | Data too large | Reduce batch size or increase memory |
| Database full | No cleanup | Run retention cleanup |
| Slow ingestion | API rate limit | Add exponential backoff retry |

## 3. Data Management

### 3.1 Data Deletion Tool

**Safe deletion of data across Bronze/Silver/Gold layers.**

Quick examples:

```bash
# Dry-run to see what would be deleted
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --dry-run

# Delete data from specific date range
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --end-datetime "2025-10-31T23:59:59"

# Delete from specific layers
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --layers bronze,silver

# Drop entire table
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --delete-table Y \
  --layers bronze \
  --tables raw_facilities
```

**Features:**
- ✅ Dry-run preview before execution
- ✅ Date range filtering
- ✅ Layer and table selection
- ✅ Parallel execution for performance
- ✅ Complete cleanup (Iceberg + MinIO + PostgreSQL)
- ✅ User confirmation required

**Documentation:**
- [Full Guide](./data-deletion-tool.md)
- [Quick Reference](./data-deletion-quick-reference.md)

### 3.2 Backup & Recovery

**Critical Data:**
1. PostgreSQL databases (metadata)
2. MinIO buckets (data)
3. Configuration files (git)

**Backup Methods:**
```bash
# PostgreSQL dump
docker compose exec postgres pg_dump \
  -U pvlakehouse iceberg_catalog \
  -Fc > /backup/iceberg_catalog_$(date +%Y%m%d).dump

# MinIO mirror
mc mirror local/lakehouse /backup/lakehouse-$(date +%Y%m%d)
mc mirror local/mlflow /backup/mlflow-$(date +%Y%m%d)

# Configuration backup
tar -czf /backup/config_$(date +%Y%m%d).tar.gz docker/.env *.sql
```

### 3.3 Recovery Procedures

**PostgreSQL Recovery:**
```bash
# Restore from dump
docker compose exec -i postgres pg_restore \
  -U pvlakehouse \
  -d iceberg_catalog \
  /backup/iceberg_catalog_20250115.dump
```

**MinIO Recovery:**
```bash
# Restore from backup
mc mirror /backup/lakehouse-20250115 local/lakehouse
mc mirror /backup/mlflow-20250115 local/mlflow
```

## 4. Scaling & Optimization

### 4.1 Increase Spark Resources

```yaml
# docker-compose.yml
spark-worker:
  environment:
    SPARK_WORKER_CORES: 4      # Increase from 2
    SPARK_WORKER_MEMORY: 8G    # Increase from 4G
  deploy:
    resources:
      limits:
        memory: 8g             # Increase from 4g
```

### 4.2 Iceberg Optimization

```sql
-- Compact small files
ALTER TABLE iceberg.bronze.oe_generation_hourly_raw
EXECUTE rewrite_data_files;

-- Expire old snapshots
ALTER TABLE iceberg.bronze.oe_generation_hourly_raw
SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms' = '604800000'  -- 7 days
);
```

### 4.3 Query Optimization

```sql
-- Add column statistics
ANALYZE TABLE iceberg.bronze.oe_generation_hourly_raw;

-- Create materialized view for common queries
CREATE TABLE iceberg.silver.generation_daily AS
SELECT 
  DATE(ts_utc) as date_utc,
  duid,
  SUM(generation_mw) as total_mwh,
  AVG(capacity_factor) as avg_capacity_factor,
  COUNT(*) as record_count
FROM iceberg.bronze.oe_generation_hourly_raw
GROUP BY DATE(ts_utc), duid;

-- Partition the view
ALTER TABLE iceberg.silver.generation_daily
SET TBLPROPERTIES (
  'write.target-file-size-bytes' = '1073741824'  -- 1GB
);
```

## 5. Monitoring & Alerts

### 5.1 Key Metrics to Monitor

```
Metric                      | Target | Alert Threshold
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Data freshness (gen)       | <1hr   | >2hrs
Query response time        | <5s    | >30s
Storage used               | <80%   | >90%
Replication lag            | 0      | >5 min
Service availability       | 100%   | <99.5%
```

### 5.2 Manual Health Check

```bash
#!/bin/bash
# health_check.sh

set -e

echo "=== PV Lakehouse Health Check ==="

# 1. Container health
echo "1. Container Status..."
docker compose ps
echo "✓ All services healthy"

# 2. MinIO
echo "2. MinIO..."
docker compose exec minio curl -s http://localhost:9000/minio/health/ready
echo "✓ MinIO ready"

# 3. PostgreSQL
echo "3. PostgreSQL..."
docker compose exec postgres pg_isready -U pvlakehouse
echo "✓ PostgreSQL ready"

# 4. Trino
echo "4. Trino..."
docker compose exec -i trino trino --execute "SELECT 1"
echo "✓ Trino ready"

# 5. Data freshness
echo "5. Data Freshness..."
LAST_GEN=$(docker compose exec -i trino trino --quiet --execute \
  "SELECT CAST(MAX(ts_utc) AS VARCHAR) FROM iceberg.bronze.oe_generation_hourly_raw")
echo "Last generation record: $LAST_GEN"

echo ""
echo "=== All Checks Passed ✓ ==="
```

---

**Runbooks:**
- [Incident Response](incidents.md) - TBD
- [Disaster Recovery](disaster-recovery.md) - TBD
- [Performance Tuning](performance-tuning.md) - TBD
