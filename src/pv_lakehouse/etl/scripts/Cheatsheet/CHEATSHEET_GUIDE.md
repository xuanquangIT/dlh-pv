# 🚀 PV Lakehouse - ETL Operations Guide

> **Quick Reference** for running ETL pipelines in the PV Lakehouse platform.

---

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Architecture Overview](#-architecture-overview)
- [Bronze Layer (Raw Data)](#-bronze-layer---raw-data-ingestion)
- [Silver Layer (Cleaned Data)](#-silver-layer---data-transformation)
- [Gold Layer (Analytics)](#-gold-layer---analytics--reporting)
- [Data Verification](#-data-verification)
- [Data Management](#-data-management)
- [Troubleshooting](#-troubleshooting)

---

## ⚡ Quick Start

### Prerequisites

```bash
# 1. Start Docker services
cd docker && docker compose --profile core up -d

# 2. Verify services are healthy
./scripts/health-check.sh

# 3. Check Spark is ready
docker compose exec spark-master spark-submit --version
```

### Run Complete Pipeline (1 Year of Data)

```bash
# Set date range
START_DATE="2025-01-01"
END_DATE="2025-12-31"

# Run full pipeline (Bronze → Silver → Gold)
./src/pv_lakehouse/etl/scripts/spark-submit.sh \
  src/pv_lakehouse/etl/bronze/load_facilities.py

./src/pv_lakehouse/etl/scripts/spark-submit.sh \
  src/pv_lakehouse/etl/bronze/load_facility_timeseries.py \
  --mode backfill --date-start ${START_DATE}T00:00:00 --date-end ${END_DATE}T23:59:59

# Continue with remaining steps...
```

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MEDALLION ARCHITECTURE                              │
├───────────────────┬───────────────────┬─────────────────────────────────────┤
│    🥉 BRONZE      │    🥈 SILVER      │           🥇 GOLD                   │
│    (Raw Data)     │   (Cleaned)       │       (Analytics)                   │
├───────────────────┼───────────────────┼─────────────────────────────────────┤
│ raw_facilities    │ clean_facility_   │ dim_facility                        │
│ raw_facility_     │   master          │ dim_date                            │
│   timeseries      │ clean_hourly_     │ dim_time                            │
│ raw_facility_     │   energy          │ dim_aqi_category                    │
│   weather         │ clean_hourly_     │ fact_solar_environmental            │
│ raw_facility_     │   weather         │                                     │
│   air_quality     │ clean_hourly_     │                                     │
│                   │   air_quality     │                                     │
└───────────────────┴───────────────────┴─────────────────────────────────────┘

Data Flow: Bronze → Silver → Gold (strict order required)
```

### Key Principles

| Principle | Description |
|-----------|-------------|
| **Strict Order** | Always load Bronze → Silver → Gold sequentially |
| **Dimensions First** | Load all `dim_*` tables before `fact_*` tables |
| **Deduplication** | Silver layer removes duplicates automatically |
| **Data Quality** | Gold layer includes `completeness_pct` and `quality_flag` |
| **API Resilience** | Scripts auto-skip inaccessible facilities (403/416 errors) |

---

## 🥉 Bronze Layer - Raw Data Ingestion

Bronze layer stores raw data exactly as received from external APIs.

### Step 1: Load Facility Metadata

```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facilities.py
```

### Step 2: Load Timeseries Data (Energy)

**Backfill Mode** (historical data):
```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py \
  --mode backfill \
  --date-start 2025-01-01T00:00:00 \
  --date-end 2025-12-31T23:59:59
```

**Incremental Mode** (daily updates):
```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py \
  --mode incremental
```

### Step 3: Load Weather Data

**Backfill Mode**:
```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py \
  --mode backfill --start 2025-01-01 --end 2025-12-31
```

**Incremental Mode**:
```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py \
  --mode incremental
```

### Step 4: Load Air Quality Data

**Backfill Mode**:
```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py \
  --mode backfill --start 2025-01-01 --end 2025-12-31
```

**Incremental Mode**:
```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py \
  --mode incremental
```

---

## 🥈 Silver Layer - Data Transformation

Silver layer cleans, validates, and deduplicates Bronze data.

### Facility Master

```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py facility_master --mode full
```

### Hourly Energy

```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode full
```

### Hourly Weather

```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_weather --mode full
```

### Hourly Air Quality

```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_air_quality --mode full
```

---

## 🥇 Gold Layer - Analytics & Reporting

Gold layer creates dimensional model for analytics.

### Step 1: Load Dimensions (Required First)

```bash
# Facility Dimension
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_facility --mode full

# Date Dimension
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_date --mode full

# Time Dimension
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_time --mode full

# AQI Category Dimension
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_aqi_category --mode full
```

### Step 2: Load Fact Table

```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental --mode full
```

---

## 📊 Data Verification

### Check Row Counts (All Layers)

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 'Bronze - raw_facilities' as table_name, COUNT(*) as cnt FROM iceberg.bronze.raw_facilities
UNION ALL SELECT 'Bronze - raw_facility_timeseries', COUNT(*) FROM iceberg.bronze.raw_facility_timeseries
UNION ALL SELECT 'Bronze - raw_facility_weather', COUNT(*) FROM iceberg.bronze.raw_facility_weather
UNION ALL SELECT 'Bronze - raw_facility_air_quality', COUNT(*) FROM iceberg.bronze.raw_facility_air_quality
UNION ALL SELECT 'Silver - clean_facility_master', COUNT(*) FROM iceberg.silver.clean_facility_master
UNION ALL SELECT 'Silver - clean_hourly_energy', COUNT(*) FROM iceberg.silver.clean_hourly_energy
UNION ALL SELECT 'Silver - clean_hourly_weather', COUNT(*) FROM iceberg.silver.clean_hourly_weather
UNION ALL SELECT 'Silver - clean_hourly_air_quality', COUNT(*) FROM iceberg.silver.clean_hourly_air_quality
UNION ALL SELECT 'Gold - dim_facility', COUNT(*) FROM iceberg.gold.dim_facility
UNION ALL SELECT 'Gold - dim_date', COUNT(*) FROM iceberg.gold.dim_date
UNION ALL SELECT 'Gold - dim_time', COUNT(*) FROM iceberg.gold.dim_time
UNION ALL SELECT 'Gold - dim_aqi_category', COUNT(*) FROM iceberg.gold.dim_aqi_category
UNION ALL SELECT 'Gold - fact_solar_environmental', COUNT(*) FROM iceberg.gold.fact_solar_environmental
ORDER BY table_name"
```

### Check Data Quality (Gold Fact)

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 
  COUNT(*) as total_rows,
  SUM(CASE WHEN quality_flag = 'GOOD' THEN 1 ELSE 0 END) as good_rows,
  SUM(CASE WHEN quality_flag = 'WARNING' THEN 1 ELSE 0 END) as warning_rows,
  SUM(CASE WHEN quality_flag = 'BAD' THEN 1 ELSE 0 END) as bad_rows,
  ROUND(AVG(completeness_pct), 2) as avg_completeness_pct,
  MIN(date_key) as min_date,
  MAX(date_key) as max_date
FROM iceberg.gold.fact_solar_environmental"
```

### Check for Duplicates (Silver Layer)

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 'clean_hourly_energy' as tbl, MAX(cnt) as max_dup
FROM (SELECT facility_code, date_hour, COUNT(*) as cnt FROM iceberg.silver.clean_hourly_energy GROUP BY 1,2)
UNION ALL
SELECT 'clean_hourly_weather', MAX(cnt)
FROM (SELECT facility_code, date_hour, COUNT(*) as cnt FROM iceberg.silver.clean_hourly_weather GROUP BY 1,2)
UNION ALL
SELECT 'clean_hourly_air_quality', MAX(cnt)
FROM (SELECT facility_code, date_hour, COUNT(*) as cnt FROM iceberg.silver.clean_hourly_air_quality GROUP BY 1,2)"
```

Expected: `max_dup = 1` for all tables (no duplicates).

---

## 🗑️ Data Management

### Delete All Data (Keep Tables)

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
DELETE FROM iceberg.gold.fact_solar_environmental;
DELETE FROM iceberg.gold.dim_aqi_category;
DELETE FROM iceberg.gold.dim_time;
DELETE FROM iceberg.gold.dim_date;
DELETE FROM iceberg.gold.dim_facility;
DELETE FROM iceberg.silver.clean_hourly_air_quality;
DELETE FROM iceberg.silver.clean_hourly_weather;
DELETE FROM iceberg.silver.clean_hourly_energy;
DELETE FROM iceberg.silver.clean_facility_master;
DELETE FROM iceberg.bronze.raw_facility_air_quality;
DELETE FROM iceberg.bronze.raw_facility_weather;
DELETE FROM iceberg.bronze.raw_facility_timeseries;
DELETE FROM iceberg.bronze.raw_facilities"
```

### Drop All Tables (Destructive)

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
DROP TABLE IF EXISTS iceberg.gold.fact_solar_environmental;
DROP TABLE IF EXISTS iceberg.gold.dim_aqi_category;
DROP TABLE IF EXISTS iceberg.gold.dim_time;
DROP TABLE IF EXISTS iceberg.gold.dim_date;
DROP TABLE IF EXISTS iceberg.gold.dim_facility;
DROP TABLE IF EXISTS iceberg.silver.clean_hourly_air_quality;
DROP TABLE IF EXISTS iceberg.silver.clean_hourly_weather;
DROP TABLE IF EXISTS iceberg.silver.clean_hourly_energy;
DROP TABLE IF EXISTS iceberg.silver.clean_facility_master;
DROP TABLE IF EXISTS iceberg.bronze.raw_facility_air_quality;
DROP TABLE IF EXISTS iceberg.bronze.raw_facility_weather;
DROP TABLE IF EXISTS iceberg.bronze.raw_facility_timeseries;
DROP TABLE IF EXISTS iceberg.bronze.raw_facilities"
```

---

## ⚠️ Troubleshooting

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `OutOfMemoryError` | Insufficient Spark memory | Increase `SPARK_EXECUTOR_MEMORY` in `.env` |
| `Connection refused` | Docker not running | Run `docker compose --profile core up -d` |
| `403 Forbidden` | API access denied | Facility auto-skipped, check API key |
| `416 Range Not Satisfiable` | No data in date range | Try different date range |
| `ModuleNotFoundError: pydantic` | Missing dependency | Rebuild: `docker compose up -d --build` |

### Check Service Status

```bash
# View all services
docker compose -f docker/docker-compose.yml ps

# Check Spark logs
docker compose -f docker/docker-compose.yml logs spark-master --tail 100

# Check Spark worker logs
docker compose -f docker/docker-compose.yml logs spark-worker --tail 100
```

### Memory Configuration

Edit `docker/.env` to adjust Spark memory:

```env
# For 16GB system
SPARK_WORKER_MEMORY=6G
SPARK_EXECUTOR_MEMORY=4g
SPARK_DRIVER_MEMORY=3g

# For 32GB+ system
SPARK_WORKER_MEMORY=12G
SPARK_EXECUTOR_MEMORY=8g
SPARK_DRIVER_MEMORY=4g
```

Then restart:
```bash
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml --profile core up -d
```

---

## 📚 Additional Resources

- **Architecture Guide**: `doc/architecture/`
- **Schema Definitions**: `doc/schema/`
- **ETL Source Code**: `src/pv_lakehouse/etl/`
- **Docker Setup**: `docker/README-SETUP.md`

---

*Last updated: January 2026*
