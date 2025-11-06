# Individual Load Commands - Copy & Paste Ready

## Prerequisites

Check Docker stack:
```bash
docker compose -f docker/docker-compose.yml ps
```

Check Silver tables:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'silver' AND TABLE_CATALOG = 'iceberg'
ORDER BY TABLE_NAME
"
```

---

## Clean Gold Layer (Optional)

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
DROP TABLE IF EXISTS iceberg.gold.dim_facility;
DROP TABLE IF EXISTS iceberg.gold.dim_date;
DROP TABLE IF EXISTS iceberg.gold.dim_time;
DROP TABLE IF EXISTS iceberg.gold.dim_aqi_category;
DROP TABLE IF EXISTS iceberg.gold.fact_solar_environmental;
"
```

---

## Load dim_facility (5 rows)

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_facility --mode full"
```

Verify:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT COUNT(*) as row_count FROM iceberg.gold.dim_facility
"
```

---

## Load dim_date (93 rows)

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_date --mode full"
```

Verify:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT COUNT(*) as row_count FROM iceberg.gold.dim_date
"
```

---

## Load dim_time (24 rows)

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_time --mode full"
```

Verify:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT COUNT(*) as row_count FROM iceberg.gold.dim_time
"
```

---

## Load dim_aqi_category (6 rows)

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_aqi_category --mode full"
```

Verify:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT COUNT(*) as row_count FROM iceberg.gold.dim_aqi_category
"
```

---

## Load fact_solar_environmental (11,085 rows - FULL MODE)

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode full"
```

Verify:
```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT COUNT(*) as row_count FROM iceberg.gold.fact_solar_environmental
"
```

---

## Load fact_solar_environmental (INCREMENTAL MODE)

Auto-detect timestamp from Silver tables and load only new data:

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode incremental"
```

---

## Verify All Tables

Single query to check all table sizes:

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

Expected:
```
dim_aqi_category,6
dim_date,93
dim_facility,5
dim_time,24
fact_solar_environmental,11085
```

---

## Verify Data Quality

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

Expected:
```
11085,5,93,24,11085,11085,11085,11085,100.00
```

---

## Sample Queries

### View Sample Data

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

### Check Schema

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
DESCRIBE iceberg.gold.fact_solar_environmental
"
```

### Data by Facility

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 
  f.facility_key,
  COUNT(*) as record_count,
  ROUND(AVG(energy_mwh), 4) as avg_energy_mwh,
  ROUND(AVG(temperature_2m), 2) as avg_temp,
  ROUND(AVG(pm2_5), 2) as avg_pm2_5
FROM iceberg.gold.fact_solar_environmental fact
JOIN iceberg.gold.dim_facility f ON fact.facility_key = f.facility_key
GROUP BY f.facility_key
ORDER BY f.facility_key
"
```

### Data by Date

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 
  d.full_date,
  COUNT(*) as record_count,
  ROUND(SUM(energy_mwh), 4) as total_energy_mwh,
  ROUND(AVG(temperature_2m), 2) as avg_temp,
  COUNT(CASE WHEN is_valid = true THEN 1 END) as valid_records
FROM iceberg.gold.fact_solar_environmental fact
JOIN iceberg.gold.dim_date d ON fact.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date DESC
LIMIT 10
"
```

### Quality Metrics by Hour

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 
  t.hour,
  COUNT(*) as record_count,
  COUNT(DISTINCT CAST(fact.date_key AS VARCHAR)) as distinct_dates,
  COUNT(CASE WHEN is_valid = true THEN 1 END) as valid_records,
  ROUND(AVG(completeness_pct), 2) as avg_completeness
FROM iceberg.gold.fact_solar_environmental fact
JOIN iceberg.gold.dim_time t ON fact.time_key = t.time_key
GROUP BY t.hour
ORDER BY t.hour
"
```

---

## Quick Commands (One-liners)

**Load everything sequentially:**
```bash
cd /home/pvlakehouse/dlh-pv && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_facility --mode full" && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_date --mode full" && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_time --mode full" && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_aqi_category --mode full" && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode full"
```

**Check all tables exist:**
```bash
cd /home/pvlakehouse/dlh-pv && \
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT COUNT(DISTINCT table_name) as gold_tables FROM information_schema.tables WHERE table_schema = 'gold' AND table_catalog = 'iceberg'
"
```

**Count all records:**
```bash
cd /home/pvlakehouse/dlh-pv && \
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 
  (SELECT COUNT(*) FROM iceberg.gold.dim_facility) + 
  (SELECT COUNT(*) FROM iceberg.gold.dim_date) +
  (SELECT COUNT(*) FROM iceberg.gold.dim_time) +
  (SELECT COUNT(*) FROM iceberg.gold.dim_aqi_category) +
  (SELECT COUNT(*) FROM iceberg.gold.fact_solar_environmental) as total_rows
"
```

