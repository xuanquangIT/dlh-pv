# 🚀 PV Lakehouse - Quick Start Guide

## 📋 Overview

The PV Lakehouse follows a medallion architecture (Bronze → Silver → Gold):
- **Bronze**: Raw data ingestion from APIs
- **Silver**: Cleaned, deduplicated, normalized data
- **Gold**: Fact tables and dimensions for analytics

---

## ⚡ Docker & Spark Setup

All commands run inside Docker containers for consistency. No need to set environment variables.

### Memory Configuration (Built-in)
```bash
--driver-memory 2g       # Driver heap size
--executor-memory 4g     # Executor heap size
```

### Verify Docker Status
```bash
docker compose -f docker/docker-compose.yml ps
```

---

## 🔄 Complete ETL Workflow (Recommended)

Run these steps in order. Copy-paste each command to execute:

### Step 1: Load Facilities Metadata
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facilities.py --mode backfill
```

### Step 2: Load Bronze Layer (Raw Data)

**Timeseries (Energy Data)** - Fetches from OpenElectricity API
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py \
  --mode backfill --date-start 2024-01-01T00:00:00 --date-end 2025-11-17T23:59:59
```

**Weather Data** - Fetches from OpenMeteo API
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py \
  --mode backfill --start 2024-01-01 --end 2025-11-17
```

**Air Quality Data** - Fetches from OpenMeteo API
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py \
  --mode backfill --start 2024-01-01 --end 2025-11-17
```

**Note on API Errors:**
- `403 Forbidden` → Facility not accessible via API, script auto-skips
- `416 Range Not Satisfiable` → No data for facility in date range, script auto-skips
- All accessible facilities will be loaded successfully

### Step 3: Transform Silver Layer (Deduplication & Normalization)

**Facility Master** - Process facility metadata
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py facility_master --mode full
```

**Hourly Energy** - Clean and normalize energy data
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode full
```

**Hourly Weather** - Clean and normalize weather data
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_weather --mode full
```

**Hourly Air Quality** - Clean and normalize air quality data
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_air_quality --mode full
```

### Step 4: Build Gold Layer Dimensions (Required before fact tables)

**Facility Dimension**
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_facility --mode full
```

**Date Dimension** (Calendar table with fiscal information)
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_date --mode full
```

**Time Dimension** (Hour-of-day table with peak indicators)
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_time --mode full
```

**AQI Category Dimension** (Air Quality Index ranges)
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_aqi_category --mode full
```

### Step 5: Build Gold Fact Table

**Fact Solar Environmental** - Combines energy, weather, and air quality by hour at facility level
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental --mode full
```

Optionally, backfill specific period:
```bash
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental --mode full \
  --start 2024-01-01T00:00:00 --end 2024-06-30T23:59:59
```

---

## 📊 Data Quality Verification

### Bronze Layer - Check data availability
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT 
     'timeseries' as source, 
     COUNT(*) as row_count,
     COUNT(DISTINCT facility_code) as facilities,
     MIN(interval_ts) as min_date, 
     MAX(interval_ts) as max_date
   FROM iceberg.bronze.raw_facility_timeseries
   UNION ALL
   SELECT 'weather', COUNT(*), COUNT(DISTINCT facility_code), MIN(weather_timestamp), MAX(weather_timestamp)
   FROM iceberg.bronze.raw_facility_weather
   UNION ALL
   SELECT 'air_quality', COUNT(*), COUNT(DISTINCT facility_code), MIN(air_timestamp), MAX(air_timestamp)
   FROM iceberg.bronze.raw_facility_air_quality;"
```

### Silver Layer - Verify no duplicates
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT 
     'clean_hourly_energy' as table_name, 
     COUNT(*) as total_rows, 
     MAX(cnt) as max_dup,
     AVG(CAST(cnt AS DOUBLE)) as avg_dup
   FROM (SELECT facility_code, date_hour, COUNT(*) as cnt FROM iceberg.silver.clean_hourly_energy GROUP BY facility_code, date_hour)
   UNION ALL
   SELECT 'clean_hourly_weather', COUNT(*), MAX(cnt), AVG(CAST(cnt AS DOUBLE))
   FROM (SELECT facility_code, date_hour, COUNT(*) as cnt FROM iceberg.silver.clean_hourly_weather GROUP BY facility_code, date_hour)
   UNION ALL
   SELECT 'clean_hourly_air_quality', COUNT(*), MAX(cnt), AVG(CAST(cnt AS DOUBLE))
   FROM (SELECT facility_code, date_hour, COUNT(*) as cnt FROM iceberg.silver.clean_hourly_air_quality GROUP BY facility_code, date_hour);"
```

**Expected result:** `max_dup = 1.0` (no duplicates), `avg_dup = 1.0`

### Gold Layer - Fact table summary
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT 
     COUNT(*) as total_records, 
     COUNT(DISTINCT facility_key) as facilities, 
     COUNT(DISTINCT date_key) as dates, 
     COUNT(DISTINCT time_key) as times, 
     ROUND(AVG(completeness_pct), 2) as avg_completeness,
     COUNT_IF(quality_flag = 'GOOD') as good_records,
     COUNT_IF(quality_flag = 'WARNING') as warning_records,
     COUNT_IF(quality_flag = 'BAD') as bad_records
   FROM iceberg.gold.fact_solar_environmental;"
```

### Gold Layer - Sample data (10 rows)
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT 
     facility_key, date_key, time_key, energy_mwh, irr_kwh_m2_hour, sunshine_hours, 
     yr_weighted_kwh, shortwave_radiation, temperature_2m, humidity_2m, pm2_5, 
     completeness_pct, quality_flag, is_valid
   FROM iceberg.gold.fact_solar_environmental 
   LIMIT 10;"
```

---

## 🗑️ Data Cleanup & Reset

### Delete Data (Reversible - keeps table structure)

**Bronze Layer Only**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "DELETE FROM iceberg.bronze.raw_facility_timeseries;
   DELETE FROM iceberg.bronze.raw_facility_weather;
   DELETE FROM iceberg.bronze.raw_facility_air_quality;
   DELETE FROM iceberg.bronze.raw_facilities;"
```

**Silver Layer Only**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "DELETE FROM iceberg.silver.clean_hourly_energy;
   DELETE FROM iceberg.silver.clean_hourly_weather;
   DELETE FROM iceberg.silver.clean_hourly_air_quality;
   DELETE FROM iceberg.silver.clean_facility_master;"
```

**Gold Layer Only**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "DELETE FROM iceberg.gold.dim_facility;
   DELETE FROM iceberg.gold.dim_date;
   DELETE FROM iceberg.gold.dim_time;
   DELETE FROM iceberg.gold.dim_aqi_category;
   DELETE FROM iceberg.gold.fact_solar_environmental;"
```

### Backfill Workflow (included from local changes)
Run the full backfill workflow (Bronze → Silver → Gold) in order:

```bash
# Step 1: Bronze Layer
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facilities.py --mode backfill

docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py \
  --mode backfill --date-start 2025-10-01T00:00:00 --date-end 2025-11-01T23:59:59

docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py \
  --mode backfill --start 2025-10-01 --end 2025-11-01

docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py \
  --mode backfill --start 2025-10-01 --end 2025-11-01

# Step 2: Silver Layer
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py facility_master --mode full --load-strategy overwrite

docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode full

docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_weather --mode full

docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_air_quality --mode full

# Step 3: Gold Dimensions
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_facility --mode full

docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_date --mode full

docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_time --mode full

docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_aqi_category --mode full

# Step 4: Gold Fact
docker compose -f docker/docker-compose.yml exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client --driver-memory 2g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental --mode full
```
```

### Drop Tables (Irreversible - removes table structure)

**All Tables**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "DROP TABLE IF EXISTS iceberg.bronze.raw_facility_timeseries;
   DROP TABLE IF EXISTS iceberg.bronze.raw_facility_weather;
   DROP TABLE IF EXISTS iceberg.bronze.raw_facility_air_quality;
   DROP TABLE IF EXISTS iceberg.bronze.raw_facilities;
   DROP TABLE IF EXISTS iceberg.silver.clean_hourly_energy;
   DROP TABLE IF EXISTS iceberg.silver.clean_hourly_weather;
   DROP TABLE IF EXISTS iceberg.silver.clean_hourly_air_quality;
   DROP TABLE IF EXISTS iceberg.silver.clean_facility_master;
   DROP TABLE IF EXISTS iceberg.gold.dim_facility;
   DROP TABLE IF EXISTS iceberg.gold.dim_date;
   DROP TABLE IF EXISTS iceberg.gold.dim_time;
   DROP TABLE IF EXISTS iceberg.gold.dim_aqi_category;
   DROP TABLE IF EXISTS iceberg.gold.fact_solar_environmental;"
```

**Bronze Layer Only**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "DROP TABLE IF EXISTS iceberg.bronze.raw_facility_timeseries;
   DROP TABLE IF EXISTS iceberg.bronze.raw_facility_weather;
   DROP TABLE IF EXISTS iceberg.bronze.raw_facility_air_quality;
   DROP TABLE IF EXISTS iceberg.bronze.raw_facilities;"
```

**Silver Layer Only**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "DROP TABLE IF EXISTS iceberg.silver.clean_hourly_energy;
   DROP TABLE IF EXISTS iceberg.silver.clean_hourly_weather;
   DROP TABLE IF EXISTS iceberg.silver.clean_hourly_air_quality;
   DROP TABLE IF EXISTS iceberg.silver.clean_facility_master;"
```

**Gold Layer Only**
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "DROP TABLE IF EXISTS iceberg.gold.dim_facility;
   DROP TABLE IF EXISTS iceberg.gold.dim_date;
   DROP TABLE IF EXISTS iceberg.gold.dim_time;
   DROP TABLE IF EXISTS iceberg.gold.dim_aqi_category;
   DROP TABLE IF EXISTS iceberg.gold.fact_solar_environmental;"
```

---

## ⚠️ Troubleshooting

### OutOfMemoryError: Java heap space
**Cause:** Spark driver or executor out of memory
**Solution:** Increase memory flags:
```bash
--driver-memory 4g --executor-memory 8g  # Double the default
```

### Cannot connect to Spark master (Connection refused)
**Cause:** Docker containers not running
**Solution:** Start containers:
```bash
docker compose -f docker/docker-compose.yml up -d
# Wait 30 seconds for services to initialize
docker compose -f docker/docker-compose.yml ps
```

### API Errors - 403 Forbidden
**Cause:** Facility not accessible via API key or API down
**Solution:** Script auto-skips these facilities. Check which loaded:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute \
  "SELECT DISTINCT facility_code FROM iceberg.bronze.raw_facility_timeseries ORDER BY facility_code;"
```

### API Errors - 416 Range Not Satisfiable
**Cause:** No data for facility in specified date range
**Solution:** Script auto-skips. Try smaller date range or different facility codes.

### Duplicates in Silver/Gold tables
**Verify:** Run data quality checks above. Expected: `max_dup = 1.0`
**If found:** Issue is upstream; check Bronze layer for duplicates first.

---

## 📚 Documentation References

- **Architecture & Design:** `doc/architecture/medallion-design.md`
- **ETL Scripts:** `src/pv_lakehouse/etl/{bronze,silver,gold}/`
- **Spark Configuration:** `src/pv_lakehouse/etl/utils/spark_utils.py`
- **Client Libraries:** `src/pv_lakehouse/etl/clients/`

---

## 🔑 Key Principles

✅ **Strict Order:** Bronze → Silver → Gold (no skipping)
✅ **Dimensions First:** Load all dim_* tables before fact tables
✅ **Deduplication:** Silver layer removes duplicates via MERGE/INSERT OVERWRITE
✅ **Data Quality:** Gold layer includes completeness_pct and quality_flag
✅ **Date Ranges:** Use ISO format with time: `2024-01-01T00:00:00 → 2025-11-17T23:59:59`
✅ **API Resilience:** Script auto-skips inaccessible facilities (403, 416 errors)

---

## 📊 Gold Layer - Fact Table Details

### fact_solar_environmental (1 row = 1 hour at 1 facility)

**Dimensions:**
- `facility_key` → Facility reference
- `date_key` → Calendar date reference
- `time_key` → Hour-of-day reference
- `aqi_category_key` → Air quality category reference

**Energy Metrics:**
- `energy_mwh` → Energy produced (MWh)
- `intervals_count` → Valid 30-min intervals in hour
- `irr_kwh_m2_hour` → Irradiance (kWh/m²·h) - converted from W/m²

**Weather Metrics:**
- `shortwave_radiation, direct_radiation, diffuse_radiation` - (W/m²)
- `direct_normal_irradiance` (DNI - W/m²)
- `temperature_2m, dew_point_2m, humidity_2m` - (°C, °C, %)
- `cloud_cover, cloud_cover_low/mid/high` - (%)
- `precipitation, wind_speed_10m, wind_direction_10m, wind_gusts_10m`
- `pressure_msl` - (hPa)
- `sunshine_hours` - (hours) - converted from seconds
- `yr_weighted_kwh` - Capacity-weighted irradiance for PR calculations

**Air Quality Metrics:**
- `pm2_5, pm10, dust` - (µg/m³)
- `nitrogen_dioxide, ozone, sulphur_dioxide, carbon_monoxide` - (ppb)
- `uv_index, uv_index_clear_sky` - (dimensionless)
- `aqi_value` - (0-500)

**Data Quality:**
- `completeness_pct` - (0-100%) - Based on energy/weather/AQ availability
- `quality_flag` - GOOD (≥90%), WARNING (≥50%), BAD (<50%)
- `is_valid` - TRUE if energy present and ≥ 0

**Audit:**
- `created_at, updated_at` - (timestamp) - Load and update times

