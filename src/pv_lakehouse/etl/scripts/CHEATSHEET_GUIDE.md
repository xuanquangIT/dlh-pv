# 🚀 PV Lakehouse - Quick Start Guide

## ⚡ Spark Memory Config (FIX for OutOfMemoryError)

Nếu gặp lỗi `OutOfMemoryError: Java heap space`, chạy các lệnh này trước:

```bash
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_DRIVER_MEMORY=2g
export SPARK_SHUFFLE_PARTITIONS=8
export SPARK_BROADCAST_THRESHOLD=10485760
```

---

## 📦 Bronze Layer - Load Historical Data

### 1. Load Facilities (metadata)
```bash
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facilities.py --mode backfill
```

### 2. Load Timeseries (Energy data: 2024-01-01 → 2025-11-08)
```bash
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_timeseries.py --mode backfill --date-start 2024-01-01T00:00:00 --date-end 2025-11-08T23:59:59
```

### 3. Load Weather data (2024-01-01 → 2025-11-08)
```bash
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_weather.py --mode backfill --start 2024-01-01 --end 2025-11-08
```

### 4. Load Air Quality data (2024-01-01 → 2025-11-08)
```bash
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_air_quality.py --mode backfill --start 2024-01-01 --end 2025-11-08
```

---

## ✨ Silver Layer - Transform Data

Run these commands in order (each processes all Bronze data in full mode):

```bash
# 1. Process facility metadata
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py facility_master --mode full

# 2. Transform energy data
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode full

# 3. Transform weather data
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_weather --mode full

# 4. Transform air quality data
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_air_quality --mode full
```

---

## 🏆 Gold Layer - Build Dimensions & Facts

### Load Dimensions (in this order):

```bash
# 1. Load facility dimension
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py dim_facility --mode full

# 2. Load date dimension
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py dim_date --mode full

# 3. Load time dimension
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py dim_time --mode full

# 4. Load AQI category dimension
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py dim_aqi_category --mode full
```

### Load Fact Table (after dimensions):

```bash
# ⚠️ Set memory BEFORE running this:
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_DRIVER_MEMORY=2g
export SPARK_SHUFFLE_PARTITIONS=8

# Then load the fact table with date range:
# For backfill specific period: 2024-01-01 → 2024-06-30
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental --mode full --start 2024-01-01T00:00:00 --end 2024-06-30T23:59:59

# For full load (all data):
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental --mode full
```

---

## 📊 Verify Data Quality

### Bronze Layer - Check data ranges:
```bash
docker exec -it trino trino --execute \
  "SELECT 'timeseries' as source, MIN(interval_ts) as min_ts, MAX(interval_ts) as max_ts, COUNT(DISTINCT interval_ts) as unique_hours, COUNT(*) as total_rows FROM iceberg.bronze.raw_facility_timeseries UNION ALL SELECT 'weather', MIN(weather_timestamp), MAX(weather_timestamp), COUNT(DISTINCT weather_timestamp), COUNT(*) FROM iceberg.bronze.raw_facility_weather UNION ALL SELECT 'air_quality', MIN(air_timestamp), MAX(air_timestamp), COUNT(DISTINCT air_timestamp), COUNT(*) FROM iceberg.bronze.raw_facility_air_quality;"
```

### Silver Layer - Check duplicates (should be max_dup=1.0):
```bash
docker exec -it trino trino --execute \
  "SELECT 'clean_hourly_energy' as table_name, COUNT(*) as total_rows, MAX(cnt) as max_dup, AVG(CAST(cnt AS DOUBLE)) as avg_dup FROM (SELECT facility_code, date_hour, COUNT(*) as cnt FROM iceberg.silver.clean_hourly_energy GROUP BY facility_code, date_hour) UNION ALL SELECT 'clean_hourly_weather', COUNT(*), MAX(cnt), AVG(CAST(cnt AS DOUBLE)) FROM (SELECT facility_code, date_hour, COUNT(*) as cnt FROM iceberg.silver.clean_hourly_weather GROUP BY facility_code, date_hour) UNION ALL SELECT 'clean_hourly_air_quality', COUNT(*), MAX(cnt), AVG(CAST(cnt AS DOUBLE)) FROM (SELECT facility_code, date_hour, COUNT(*) as cnt FROM iceberg.silver.clean_hourly_air_quality GROUP BY facility_code, date_hour);"
```

### Gold Layer - Fact table summary:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT COUNT(*) as total_records, COUNT(DISTINCT facility_key) as facilities, COUNT(DISTINCT date_key) as dates, COUNT(DISTINCT time_key) as times, ROUND(AVG(completeness_pct), 2) as avg_completeness FROM iceberg.gold.fact_solar_environmental;"
```

### Gold Layer - Check for duplicates (should be max_dup=1.0):
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT COUNT(*) as total_rows, MAX(cnt) as max_duplicates, AVG(CAST(cnt AS DOUBLE)) as avg_duplicates FROM (SELECT facility_key, date_key, time_key, COUNT(*) as cnt FROM iceberg.gold.fact_solar_environmental GROUP BY facility_key, date_key, time_key);"
```

### Gold Layer - Sample data (10 rows):
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT facility_key, date_key, time_key, energy_mwh, irr_kwh_m2_hour, sunshine_hours, yr_weighted_kwh, shortwave_radiation, temperature_2m, humidity_2m, pm2_5, completeness_pct, is_valid FROM iceberg.gold.fact_solar_environmental LIMIT 10;"
```

---

## 🗑️ Delete/Clean Data

### Delete Bronze layer:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM bronze.raw_facility_timeseries;"
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM bronze.raw_facility_weather;"
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM bronze.raw_facility_air_quality;"
```

### Delete Silver layer:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM silver.clean_hourly_energy;"
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM silver.clean_hourly_weather;"
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM silver.clean_hourly_air_quality;"
```

### Delete Gold layer:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM gold.dim_facility;"
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM gold.dim_date;"
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM gold.dim_time;"
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM gold.dim_aqi_category;"
docker compose -f docker/docker-compose.yml exec trino trino --catalog iceberg --schema lh --execute "DELETE FROM gold.fact_solar_environmental;"
```

---

## 📝 Complete Backfill Workflow

Run these steps in order (copy-paste từng cái):

```bash
# Step 1: Bronze Layer
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facilities.py --mode backfill
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_timeseries.py --mode backfill --date-start 2024-01-01T00:00:00 --date-end 2025-11-08T23:59:59
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_weather.py --mode backfill --start 2024-01-01 --end 2025-11-08
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_air_quality.py --mode backfill --start 2024-01-01 --end 2025-11-08

# Step 2: Silver Layer
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py facility_master --mode full
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode full
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_weather --mode full
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_air_quality --mode full

# Step 3: Gold Dimensions
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py dim_facility --mode full
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py dim_date --mode full
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py dim_time --mode full
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py dim_aqi_category --mode full

# Step 4: Gold Fact (Set memory first!)
export SPARK_EXECUTOR_MEMORY=4g && export SPARK_DRIVER_MEMORY=2g && export SPARK_SHUFFLE_PARTITIONS=8
# Backfill for specific date range (2024-01-01 → 2024-06-30)
bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental --mode full
```

---

## ⚠️ Troubleshooting

### OutOfMemoryError: Java heap space
**Solution:** Set Spark memory before running:
```bash
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_DRIVER_MEMORY=2g
export SPARK_SHUFFLE_PARTITIONS=8
```

### API Errors (403 Forbidden)
**Solution:** Check API credentials and network connectivity. Some APIs may be rate-limited.

### Duplicates in Silver/Gold tables
**Verify:** Run duplicate check queries from section "Verify Data Quality"
Expected: `max_dup=1.0`, `avg_dup=1.0`

---

## 📚 Documentation

- **Architecture:** `doc/architecture/medallion-design.md`
- **ETL Scripts:** `src/pv_lakehouse/etl/{bronze,silver,gold}/`
- **Spark Config:** `src/pv_lakehouse/etl/utils/spark_utils.py`
- **Cheat Sheet:** `src/pv_lakehouse/etl/scripts/cheat-sheet.sh`

---

## 🔑 Key Points

✅ **Bronze → Silver → Gold** (strict order)
✅ **Dimensions BEFORE Facts** (Gold layer)
✅ **Set memory for fact_solar_environmental** (large data)
✅ **No duplicates** (check with max_dup=1.0)
✅ **Backfill from 2024-01-01 to 2025-11-08** (entire date range)

## ✨ Recent Updates to fact_solar_environmental

### New Columns Added (Nov 2025)

1. **`irr_kwh_m2_hour`** - Irradiance converted to kWh/m²·h
   - Formula: `shortwave_radiation / 1000.0`
   - Used for Performance Ratio (PR) calculation in Power BI

2. **`sunshine_hours`** - Total sunshine duration in hours
   - Formula: `sunshine_duration / 3600.0` (convert seconds to hours)
   - Helps BI reports track daily/hourly solar exposure

3. **`yr_weighted_kwh`** - Weighted irradiance by facility capacity
   - Formula: `irr_kwh_m2_hour × total_capacity_mw × 1000`
   - Key denominator for Performance Ratio weighted calculations

4. **`humidity_2m`** - Relative Humidity with clamping
   - Formula: Magnus formula with RH clamping to [0, 100]%
   - Fixed: Previous version had no clamping, caused invalid values

### Key Fixes Applied

- ✅ Timestamp standardization: All joins use `date_hour_ts_utc` (UTC consistent)
- ✅ Date/Time joins: `to_date(date_hour_ts_utc)` for date matching, `hour(date_hour_ts_utc)` for time
- ✅ RH calculation: Proper Magnus formula with bounds checking
- ✅ Irradiance conversion: W/m² → kWh/m²·h (divide by 1000, not multiply by 3600)
- ✅ Schema update: Dropped old table & recreated with new columns

