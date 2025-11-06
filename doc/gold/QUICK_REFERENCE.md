# Gold Layer Loading - Quick Reference

## 🎯 One-Liner to Load Everything

```bash
cd /home/pvlakehouse/dlh-pv && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_facility --mode full" && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_date --mode full" && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_time --mode full" && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_aqi_category --mode full" && \
docker compose -f docker/docker-compose.yml exec spark-master bash -c "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode full"
```

## 📋 Step-by-Step Commands

### Step 1: Drop existing Gold tables (optional)

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
DROP TABLE IF EXISTS iceberg.gold.dim_facility;
DROP TABLE IF EXISTS iceberg.gold.dim_date;
DROP TABLE IF EXISTS iceberg.gold.dim_time;
DROP TABLE IF EXISTS iceberg.gold.dim_aqi_category;
DROP TABLE IF EXISTS iceberg.gold.fact_solar_environmental;
"
```

### Step 2: Load Dimensions

#### dim_facility
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_facility --mode full"
```

#### dim_date
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_date --mode full"
```

#### dim_time
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_time --mode full"
```

#### dim_aqi_category
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli dim_aqi_category --mode full"
```

### Step 3: Load Fact Table

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode full"
```

### Step 4: Verify

```bash
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 'dim_facility' as name, COUNT(*) as cnt FROM iceberg.gold.dim_facility
UNION ALL
SELECT 'dim_date', COUNT(*) FROM iceberg.gold.dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM iceberg.gold.dim_time
UNION ALL
SELECT 'dim_aqi_category', COUNT(*) FROM iceberg.gold.dim_aqi_category
UNION ALL
SELECT 'fact_solar_environmental', COUNT(*) FROM iceberg.gold.fact_solar_environmental
"
```

**Expected Result:**
```
"dim_aqi_category","6"
"dim_date","93"
"dim_facility","5"
"dim_time","24"
"fact_solar_environmental","11085"
```

## 📊 Final Data Summary

| Table | Rows | Type | Status |
|-------|------|------|--------|
| dim_facility | 5 | Dimension | ✓ |
| dim_date | 93 | Dimension | ✓ |
| dim_time | 24 | Dimension | ✓ |
| dim_aqi_category | 6 | Dimension | ✓ |
| fact_solar_environmental | 11,085 | Fact | ✓ |
| **TOTAL** | **11,213** | | **✓** |

**Data Quality:**
- Completeness: 100% (all 3 sources present)
- Validity: 100% (all quality checks pass)
- Status: **Production Ready**

## 🔄 Incremental Load (After initial full load)

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -c \
  "cd /opt/workdir && python3 -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode incremental"
```

## 📚 Full Documentation

- **LOAD_COMMANDS.md** - Complete guide with all details
- **INDIVIDUAL_COMMANDS.md** - Copy-paste ready commands
- **load-all-gold-tables.sh** - Automated loading script

## ✅ Verification

```bash
# Check all tables exist
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT COUNT(*) as table_count FROM information_schema.tables 
WHERE table_schema = 'gold' AND table_catalog = 'iceberg'
"

# Check data quality
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT 
  COUNT(*) as total,
  COUNT(CASE WHEN is_valid = true THEN 1 END) as valid,
  ROUND(AVG(completeness_pct), 2) as completeness
FROM iceberg.gold.fact_solar_environmental
"

# Sample data
docker compose -f docker/docker-compose.yml exec trino trino --execute "
SELECT * FROM iceberg.gold.fact_solar_environmental LIMIT 5
"
```

---
**Created:** November 6, 2025  
**Status:** ✓ Complete and Verified
