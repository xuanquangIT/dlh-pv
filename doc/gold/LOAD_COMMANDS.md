# Gold Layer Load Commands

Complete guide to load data from Silver layer into Gold layer (Star Schema).

## Overview

**Gold Layer Tables:**
- 4 Dimension Tables (Static/Slowly-Changing)
- 1 Fact Table (Hourly granularity)

**Data Volume:**
```
dim_facility:              5 rows
dim_date:                 93 rows
dim_time:                 24 rows
dim_aqi_category:          6 rows
fact_solar_environmental: 11,085 rows (5 facilities × 93 dates × 24 hours = 11,160 potential, filtered to 11,085)
```

**Data Quality:**
- Completeness: 100% (all 3 data sources present for all records)
- Validity: 100% (all records are valid)
- Quality Flag: All records marked as "GOOD"

---

## Prerequisites

### 1. Verify Docker Stack is Running

```bash
docker compose -f docker/docker-compose.yml ps
```

Expected services running:
- spark-master ✓
- spark-worker ✓
- trino ✓
- minio ✓
- postgres ✓

### 2. Verify Silver Layer Tables Exist

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'silver' AND TABLE_CATALOG = 'iceberg'
ORDER BY TABLE_NAME
"
```

Expected tables:
- clean_facility_master ✓
- clean_hourly_air_quality ✓
- clean_hourly_energy ✓
- clean_hourly_weather ✓

---

## Step-by-Step Loading

### Step 1: Clean Gold Layer (Optional - only if reloading)

Drop all existing Gold tables:

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
DROP TABLE IF EXISTS iceberg.gold.dim_facility;
DROP TABLE IF EXISTS iceberg.gold.dim_date;
DROP TABLE IF EXISTS iceberg.gold.dim_time;
DROP TABLE IF EXISTS iceberg.gold.dim_aqi_category;
DROP TABLE IF EXISTS iceberg.gold.fact_solar_environmental;
"
```

### Step 2: Load Dimension Tables (Order matters!)

#### 2.1 Load dim_facility

**Purpose:** Static dimension with facility metadata

**Command:**
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_facility --mode full"
```

**Expected Output:**
```
[GOLD] Running loader: dim_facility
[GOLD]   Mode: full
[GOLD]   Strategy: merge
[GOLD] Loader 'dim_facility' completed: 5 rows written
```

**Verify:**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT COUNT(*) as row_count FROM iceberg.gold.dim_facility"
```

Expected: `5`

---

#### 2.2 Load dim_date

**Purpose:** Dimension with date hierarchy (year, month, day, week, quarter, season, weekend flag)

**Command:**
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_date --mode full"
```

**Expected Output:**
```
[GOLD] Running loader: dim_date
[GOLD]   Mode: full
[GOLD]   Strategy: merge
[GOLD] Loader 'dim_date' completed: 93 rows written
```

**Verify:**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT COUNT(*) as row_count FROM iceberg.gold.dim_date"
```

Expected: `93`

---

#### 2.3 Load dim_time

**Purpose:** Static dimension with time hierarchy (hour, minute, time_of_day, is_peak_hour, daylight_period)

**Command:**
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_time --mode full"
```

**Expected Output:**
```
[GOLD] Running loader: dim_time
[GOLD]   Mode: full
[GOLD]   Strategy: merge
[GOLD] Loader 'dim_time' completed: 24 rows written
```

**Verify:**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT COUNT(*) as row_count FROM iceberg.gold.dim_time"
```

Expected: `24`

---

#### 2.4 Load dim_aqi_category

**Purpose:** Static dimension with EPA/WHO AQI standards (6 categories from "Good" to "Hazardous")

**Command:**
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_aqi_category --mode full"
```

**Expected Output:**
```
[GOLD] Running loader: dim_aqi_category
[GOLD]   Mode: full
[GOLD]   Strategy: merge
[GOLD] Loader 'dim_aqi_category' completed: 6 rows written
```

**Verify:**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT COUNT(*) as row_count FROM iceberg.gold.dim_aqi_category"
```

Expected: `6`

---

### Step 3: Load Fact Table

#### 3.1 Load fact_solar_environmental (Full Mode)

**Purpose:** Fact table combining solar energy, weather, and air quality data at hourly granularity

**Command:**
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode full"
```

**Expected Output:**
```
[GOLD] Running loader: fact_solar_environmental
[GOLD]   Mode: full
[GOLD]   Strategy: merge
[GOLD] Loader 'fact_solar_environmental' completed: 11085 rows written
```

**Runtime:** ~30-60 seconds (depending on cluster load)

**Verify:**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT COUNT(*) as row_count FROM iceberg.gold.fact_solar_environmental"
```

Expected: `11085`

---

### Step 4: Comprehensive Verification

#### 4.1 Verify All Table Row Counts

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 'dim_facility' as table_name, COUNT(*) as row_count FROM iceberg.gold.dim_facility
UNION ALL
SELECT 'dim_date', COUNT(*) FROM iceberg.gold.dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM iceberg.gold.dim_time
UNION ALL
SELECT 'dim_aqi_category', COUNT(*) FROM iceberg.gold.dim_aqi_category
UNION ALL
SELECT 'fact_solar_environmental', COUNT(*) FROM iceberg.gold.fact_solar_environmental
ORDER BY table_name
"
```

**Expected Output:**
```
"dim_aqi_category","6"
"dim_date","93"
"dim_facility","5"
"dim_time","24"
"fact_solar_environmental","11085"
```

---

#### 4.2 Check Data Quality Metrics

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT facility_key) as facilities,
  COUNT(DISTINCT date_key) as dates,
  COUNT(DISTINCT time_key) as times,
  COUNT(CASE WHEN energy_mwh IS NOT NULL THEN 1 END) as with_energy,
  COUNT(CASE WHEN shortwave_radiation IS NOT NULL THEN 1 END) as with_weather,
  COUNT(CASE WHEN pm2_5 IS NOT NULL THEN 1 END) as with_air_quality,
  COUNT(CASE WHEN is_valid = true THEN 1 END) as valid_records,
  ROUND(AVG(completeness_pct), 2) as avg_completeness_pct
FROM iceberg.gold.fact_solar_environmental
"
```

**Expected Output:**
```
"11085","5","93","24","11085","11085","11085","11085","100.00"
```

---

#### 4.3 View Sample Data

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 
  facility_key, date_key, time_key, aqi_category_key,
  energy_mwh, shortwave_radiation, temperature_2m, pm2_5, aqi_value,
  completeness_pct, is_valid, quality_flag
FROM iceberg.gold.fact_solar_environmental 
LIMIT 10
"
```

---

#### 4.4 Check Schema of fact_solar_environmental

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "DESCRIBE iceberg.gold.fact_solar_environmental"
```

---

## Incremental Loading (Optional)

For incremental loads after initial full load:

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode incremental"
```

**Behavior:**
- Auto-detects the last loaded timestamp from Silver source tables
- Loads only new/updated data since that timestamp
- Maintains data consistency and avoids duplicates

**Example Output:**
```
[GOLD INCREMENTAL] Auto-detected last loaded timestamps from Silver sources:
[GOLD INCREMENTAL]   lh.silver.clean_hourly_energy: 2025-11-01 09:53:24.087585
[GOLD INCREMENTAL]   lh.silver.clean_hourly_weather: 2025-11-01 09:54:33.058542
[GOLD INCREMENTAL]   lh.silver.clean_hourly_air_quality: 2025-11-01 09:55:46.021789
[GOLD INCREMENTAL]   Using earliest: 2025-11-01 09:53:24.087585
[GOLD INCREMENTAL]   Will load from: 2025-11-01 10:53:24.087585
[GOLD] Loader 'fact_solar_environmental' completed: 0 rows written
```

---

## Automated Script (One-liner)

Load all tables in sequence:

```bash
#!/bin/bash
set -e

cd /home/pvlakehouse/dlh-pv

echo "Loading Gold layer tables..."

echo "1. Loading dim_facility..."
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_facility --mode full" | grep "completed"

echo "2. Loading dim_date..."
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_date --mode full" | grep "completed"

echo "3. Loading dim_time..."
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_time --mode full" | grep "completed"

echo "4. Loading dim_aqi_category..."
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_aqi_category --mode full" | grep "completed"

echo "5. Loading fact_solar_environmental..."
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode full" | grep "completed"

echo ""
echo "Verifying all tables..."
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 'dim_facility' as table_name, COUNT(*) as row_count FROM iceberg.gold.dim_facility
UNION ALL
SELECT 'dim_date', COUNT(*) FROM iceberg.gold.dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM iceberg.gold.dim_time
UNION ALL
SELECT 'dim_aqi_category', COUNT(*) FROM iceberg.gold.dim_aqi_category
UNION ALL
SELECT 'fact_solar_environmental', COUNT(*) FROM iceberg.gold.fact_solar_environmental
ORDER BY table_name
"

echo ""
echo "✓ Gold layer loading completed successfully!"
```

---

## Troubleshooting

### Issue: Import Error - ModuleNotFoundError

**Cause:** `__init__.py` contains imports for non-existent loaders

**Solution:**
```bash
# Edit /home/pvlakehouse/dlh-pv/src/pv_lakehouse/etl/gold/__init__.py
# Remove imports for:
#   - dim_model_version
#   - dim_performance_issue
#   - dim_weather_condition
#   - fact_air_quality_impact
#   - fact_solar_forecast
```

### Issue: Ambiguous Column Reference

**Cause:** Multiple tables have same column names (e.g., `date_key`, `full_date`)

**Solution:** Already fixed in current implementation - columns are aliased during joins

### Issue: Decimal Type Error

**Cause:** Float values passed to DecimalType column

**Solution:** Convert values to Decimal type before creating DataFrame

### Issue: Timeout

**Cause:** Fact table load takes too long

**Solution:** Increase timeout or run on larger cluster

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && timeout 600 python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode full"
```

---

## Schema Reference

### fact_solar_environmental Columns

**Keys:**
- `facility_key` (BIGINT) - Foreign key to dim_facility
- `date_key` (INT) - Foreign key to dim_date (format: YYYYMMDD)
- `time_key` (INT) - Foreign key to dim_time (format: HHMM)
- `aqi_category_key` (BIGINT) - Foreign key to dim_aqi_category

**Energy Metrics:**
- `energy_mwh` (DECIMAL(12,6)) - Solar energy in MWh
- `power_avg_mw` (DECIMAL(12,6)) - Average power in MW
- `intervals_count` (INT) - Number of 5-minute intervals

**Weather Metrics:**
- `shortwave_radiation` (DECIMAL(10,4)) - W/m²
- `direct_radiation` (DECIMAL(10,4)) - W/m²
- `diffuse_radiation` (DECIMAL(10,4)) - W/m²
- `direct_normal_irradiance` (DECIMAL(10,4)) - W/m²
- `temperature_2m` (DECIMAL(6,2)) - °C
- `dew_point_2m` (DECIMAL(6,2)) - °C
- `humidity_2m` (DECIMAL(5,2)) - % (calculated from dew point)
- `cloud_cover` (DECIMAL(5,2)) - %
- `precipitation` (DECIMAL(8,4)) - mm
- `wind_speed_10m` (DECIMAL(6,2)) - m/s

**Air Quality Metrics:**
- `pm2_5` (DECIMAL(8,4)) - µg/m³
- `pm10` (DECIMAL(8,4)) - µg/m³
- `dust` (DECIMAL(8,4)) - µg/m³
- `nitrogen_dioxide` (DECIMAL(8,4)) - µg/m³
- `ozone` (DECIMAL(8,4)) - µg/m³
- `sulphur_dioxide` (DECIMAL(8,4)) - µg/m³
- `carbon_monoxide` (DECIMAL(8,4)) - µg/m³
- `uv_index` (DECIMAL(5,2)) - Index
- `aqi_value` (INT) - AQI value (0-500)

**Data Quality Metrics:**
- `completeness_pct` (DECIMAL(5,2)) - % (33.33% per source, 100% when all present)
- `is_valid` (BOOLEAN) - True if all quality checks pass
- `quality_flag` (STRING) - "GOOD", "WARNING", or "BAD"

---

## Performance Considerations

### Broadcast Joins
- `dim_facility` (5 rows) - Broadcasted
- `dim_date` (93 rows) - Broadcasted
- `dim_time` (24 rows) - Broadcasted
- `dim_aqi_category` (6 rows) - Broadcasted for cross-join

**Benefit:** 5-10x faster than shuffle joins for small dimensions

### Partitioning
- `fact_solar_environmental` partitioned by `date_key`
- Enables efficient time-based queries and incremental loads

### Spark Configuration
- `spark.sql.shuffle.partitions = 8` (reduced from default 200)
- `spark.sql.autoBroadcastJoinThreshold = 10MB`

---

## Next Steps

1. **Query Gold Layer:**
   ```sql
   SELECT 
     f.facility_key,
     d.full_date,
     t.time_of_day,
     f.energy_mwh,
     w.temperature_2m,
     a.aqi_value
   FROM iceberg.gold.fact_solar_environmental f
   JOIN iceberg.gold.dim_facility f ON ...
   JOIN iceberg.gold.dim_date d ON ...
   JOIN iceberg.gold.dim_time t ON ...
   ```

2. **Set up Incremental Scheduling:**
   - Configure Prefect workflows to run incremental loads hourly/daily
   - Monitor auto-detected timestamps for data consistency

3. **Build Analytics:**
   - Create materialized views for business reporting
   - Set up dashboards using Tableau/Power BI

---

## Last Updated

**Date:** November 6, 2025  
**Status:** ✅ All tables loaded and verified  
**Data Volume:** 11,085 fact records + 128 dimension records  
**Data Quality:** 100% completeness, 100% validity  

