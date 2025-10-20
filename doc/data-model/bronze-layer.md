# Bronze Layer - Định Nghiệu & Triển Khai

## 1. Tổng Quan Bronze Layer

Bronze Layer là layer thứ nhất trong Medallion Architecture, chứa dữ liệu **thô từ các nguồn bên ngoài** với minimal transformation.

**Đặc điểm:**
- ✅ Dữ liệu ingested as-is từ APIs
- ✅ Schema định nghiệu trước (strict schema)
- ✅ Partition by `days(ts_utc)` cho time-series
- ✅ Metadata columns: `_ingest_time`, `_source`, `_hash`
- ✅ Format: Apache Iceberg v2
- ✅ Write mode: Append (upsert khi needed)

**Mục Đích:**
- 🔍 Audit trail đầy đủ
- 📊 Source data integrity verification
- 🔄 Nhanh khôi phục nếu Silver/Gold có bug
- 📈 Tính data volume & growth metrics

## 2. Current Bronze Tables

### 2.1 `oe_facilities_raw` - OpenNEM Facilities

**Nguồn:** OpenNEM API - Solar Facilities Registry

**Mục Đích:** Master data cho tất cả solar facilities

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS lh.bronze.oe_facilities_raw (
    -- Primary identification
    facility_code STRING NOT NULL,
    facility_name STRING NOT NULL,
    
    -- Location (critical for weather matching)
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    
    -- Network & region
    network_id STRING,              -- NEM, WEM
    network_region STRING,          -- NSW1, VIC1, QLD1, SA1
    
    -- Capacity information
    total_capacity_mw DOUBLE,
    total_capacity_registered_mw DOUBLE,
    total_capacity_maximum_mw DOUBLE,
    
    -- Technology
    fuel_technology STRING,         -- solar_utility, solar_rooftop
    unit_count INT,
    
    -- Status
    operational_status STRING,      -- operating, commissioning, retired
    
    -- Timestamps
    facility_created_at TIMESTAMP,
    facility_updated_at TIMESTAMP,
    
    -- Unit mapping
    unit_codes STRING,              -- DUIDs: AVLSF1 or NEWENSF1,NEWENSF2
    
    -- Partition & Metadata
    ts_utc TIMESTAMP NOT NULL,
    _ingest_time TIMESTAMP NOT NULL,
    _source STRING NOT NULL,
    _hash STRING
)
USING iceberg
PARTITIONED BY (days(ts_utc))
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.metrics.default' = 'full'
)
```

**Data Characteristics:**
- ~100 solar facilities in Australia
- Schema snapshot (no time series)
- Typically ingest daily or on-demand
- Dedup by facility_code

**File Location:**
```
s3a://lakehouse/warehouse/lh/oe_facilities_raw/
├── year=2025/month=01/day=15/
│   └── 00000-xyz.parquet
```

**Example Records:**
```json
{
  "facility_code": "AVLSF",
  "facility_name": "Avonlie Solar Farm",
  "latitude": -36.1234,
  "longitude": 142.5678,
  "network_region": "VIC1",
  "total_capacity_mw": 50.0,
  "fuel_technology": "solar_utility",
  "operational_status": "operating",
  "ts_utc": "2025-01-15 10:00:00",
  "_ingest_time": "2025-01-15 10:15:30",
  "_source": "opennem_api_v1",
  "_hash": "a3f2e1c9b8d7f6e5..."
}
```

### 2.2 `oe_generation_hourly_raw` - OpenNEM Generation

**Nguồn:** OpenNEM API - Solar Generation Time Series

**Mục Đích:** Hourly solar generation outputs

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS lh.bronze.oe_generation_hourly_raw (
    -- Time & identification
    ts_utc TIMESTAMP NOT NULL,      -- Settlement hour
    duid STRING NOT NULL,           -- Individual unit ID
    
    -- Generation data
    generation_mw DOUBLE,           -- Actual power output
    capacity_factor DOUBLE,         -- generation/capacity ratio
    
    -- Quality indicators
    data_quality_code STRING,       -- ACTUAL, ESTIMATED, SUBSTITUTED
    data_source STRING,             -- AEMO_SCADA, OPENNEM
    
    -- Partition & Metadata
    _ingest_time TIMESTAMP NOT NULL,
    _source STRING NOT NULL,
    _hash STRING
)
USING iceberg
PARTITIONED BY (days(ts_utc))
TBLPROPERTIES (...)
```

**Data Characteristics:**
- Hourly granularity (00:00 - 23:00 UTC)
- Real-time updates (may have corrections)
- ~800 hourly records/day for ~2 major facilities
- Upsert by (duid, ts_utc) to handle updates

**Ingestion Pattern:**
```
Hour 0 (00:00 UTC): Available by 00:30 UTC
Hour 1 (01:00 UTC): Available by 01:30 UTC
...
Hour 23 (23:00 UTC): Available by next day 00:30 UTC
```

**Lookback:** 48 hours (handles delayed updates)

**Example Records:**
```json
{
  "ts_utc": "2025-01-15 10:00:00",
  "duid": "AVLSF1",
  "generation_mw": 35.5,
  "capacity_factor": 0.71,
  "data_quality_code": "ACTUAL",
  "_ingest_time": "2025-01-15 10:15:30",
  "_source": "opennem_api_v1",
  "_hash": "b4g3f2d1c0e9f8a7..."
}
```

### 2.3 `om_weather_hourly_raw` - Open-Meteo Weather

**Nguồn:** Open-Meteo Weather API

**Mục Đích:** Hourly weather observations for solar forecasting

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS lh.bronze.om_weather_hourly_raw (
    -- Time & location
    ts_utc TIMESTAMP NOT NULL,
    latitude DOUBLE NOT NULL,       -- 0.25° grid resolution
    longitude DOUBLE NOT NULL,
    
    -- Temperature & humidity
    temperature_2m DOUBLE,          -- °C
    relative_humidity_2m DOUBLE,    -- %
    pressure_msl DOUBLE,            -- hPa
    
    -- Wind
    wind_speed_10m DOUBLE,          -- m/s
    wind_direction_10m DOUBLE,      -- degrees (0-360)
    
    -- Solar radiation (CRITICAL)
    shortwave_radiation DOUBLE,     -- W/m² (GHI)
    direct_normal_irradiance DOUBLE,-- W/m² (DNI)
    diffuse_radiation DOUBLE,       -- W/m²
    
    -- Cloud cover
    cloud_cover DOUBLE,             -- %
    cloud_cover_low DOUBLE,         -- %
    
    -- Precipitation
    precipitation DOUBLE,           -- mm
    
    -- Data quality
    data_completeness_score DOUBLE, -- 0-1
    api_source_model STRING,        -- ERA5, ERA5-Land, etc
    api_request_id STRING,
    
    -- Partition & Metadata
    _ingest_time TIMESTAMP NOT NULL,
    _source STRING NOT NULL,
    _hash STRING
)
USING iceberg
PARTITIONED BY (days(ts_utc))
```

**Data Characteristics:**
- Hourly observations
- Multiple grid points covering facility locations
- 0.25° resolution (~28 km)
- Historical + forecast data

**Grid Coverage:**
- NSW facilities: Centered at -33.5°, 149.0°
- QLD facilities: Centered at -27.5°, 151.0°

**Solar-Specific Fields:**
| Field | Importance | Use Case |
|-------|-----------|----------|
| `shortwave_radiation` | 🔴 Critical | Main irradiance measure (GHI) |
| `direct_normal_irradiance` | 🟡 High | DNI for tracking systems |
| `cloud_cover` | 🟡 High | Strong correlation with generation |
| `diffuse_radiation` | 🟢 Medium | Fixed tilt system modeling |
| `temperature_2m` | 🟢 Medium | Module efficiency correction |

### 2.4 `om_air_quality_hourly_raw` - Open-Meteo Air Quality

**Nguồn:** Open-Meteo Air Quality API (CAMS)

**Mục Đích:** Air quality data affecting solar irradiance

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS lh.bronze.om_air_quality_hourly_raw (
    -- Time & location
    ts_utc TIMESTAMP NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    
    -- Particulate matter
    pm2_5 DOUBLE,                  -- μg/m³
    pm10 DOUBLE,                   -- μg/m³
    
    -- Aerosols (CRITICAL for solar)
    aerosol_optical_depth DOUBLE,  -- Dimensionless
    dust DOUBLE,                   -- μg/m³
    
    -- Gases
    nitrogen_dioxide DOUBLE,       -- μg/m³
    ozone DOUBLE,                  -- μg/m³
    
    -- AQI
    european_aqi INT,
    
    -- Data quality
    data_completeness_score DOUBLE,
    cams_domain STRING,            -- cams_global, cams_europe
    api_request_id STRING,
    
    -- Partition & Metadata
    _ingest_time TIMESTAMP NOT NULL,
    _source STRING NOT NULL,
    _hash STRING
)
USING iceberg
PARTITIONED BY (days(ts_utc))
```

**Data Characteristics:**
- Hourly observations
- Global CAMS dataset
- Aerosol Optical Depth (AOD) is key metric
- AOD directly reduces solar irradiance

**Impact on Solar:**
```
AOD vs GHI Reduction:
- AOD = 0.1  →  -2% GHI
- AOD = 0.3  →  -6% GHI
- AOD = 0.5  →  -12% GHI
```

## 3. Bronze Table Creation

### 3.1 Create All Tables at Once

```bash
# From project root
cd tests

# Option 1: Using Python/PySpark
python test_bronze_tables_complete.py --create

# Option 2: Using Bash script
bash create_bronze_tables.sh

# Option 3: Using Trino (requires DDL adaptation)
docker compose exec -i trino trino < ../sql/bronze/oe_facilities_raw.sql
```

### 3.2 Verify Table Creation

```bash
# Connect to Trino
docker compose exec -it trino trino --catalog iceberg --schema bronze

# List tables
SHOW TABLES;

# Check table structure
DESCRIBE oe_facilities_raw;
DESCRIBE oe_generation_hourly_raw;
DESCRIBE om_weather_hourly_raw;
DESCRIBE om_air_quality_hourly_raw;

# Check row count
SELECT COUNT(*) FROM oe_facilities_raw;
SELECT COUNT(*) FROM oe_generation_hourly_raw;
```

### 3.3 Verify Partitioning

```sql
-- Check partition structure
SELECT *
FROM oe_generation_hourly_raw
WHERE ts_utc >= '2025-01-15' AND ts_utc < '2025-01-16'
LIMIT 10;

-- Verify days(ts_utc) partitioning
-- Should scan only day partition, not full table
```

## 4. Data Loading Process

### 4.1 Manual Ingestion (Development)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder \
    .appName("bronze-ingest") \
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lh.type", "jdbc") \
    .config("spark.sql.catalog.lh.uri", "jdbc:postgresql://postgres/iceberg_catalog") \
    .getOrCreate()

# Example: Load generation data
df_generation = spark.read \
    .option("header", "true") \
    .csv("s3a://lakehouse/raw/generation.csv")

# Add metadata columns
from pyspark.sql.functions import lit, sha2, col, concat_ws, current_timestamp
from hashlib import sha256

df_with_metadata = df_generation.withColumn(
    "_ingest_time", current_timestamp()
).withColumn(
    "_source", lit("opennem_api_v1")
).withColumn(
    "_hash", sha2(concat_ws("|", col("duid"), col("ts_utc")), 256)
)

# Write to Iceberg (upsert)
df_with_metadata.writeTo("lh.bronze.oe_generation_hourly_raw") \
    .using("iceberg") \
    .mode("append") \
    .saveAsTable("lh.bronze.oe_generation_hourly_raw")
```

### 4.2 Automated Ingestion (via Prefect)

```python
# flows/ingest_bronze.py
from prefect import task, flow
from datetime import datetime, timedelta
import httpx

@task
def fetch_opennem_generation():
    """Fetch generation data from OpenNEM API"""
    url = "https://api.opennem.org.au/v3/data/generation/hourly"
    params = {
        "from": (datetime.utcnow() - timedelta(days=2)).isoformat(),
        "to": datetime.utcnow().isoformat(),
    }
    response = httpx.get(url, params=params)
    return response.json()

@task
def load_to_bronze(data):
    """Load to Bronze layer"""
    from pyspark.sql import SparkSession
    # ... conversion & write logic
    pass

@flow(name="ingest_bronze_hourly")
def ingest_bronze():
    raw_data = fetch_opennem_generation()
    load_to_bronze(raw_data)

# Schedule: every hour at :15 minutes past
if __name__ == "__main__":
    ingest_bronze.serve(cron="15 * * * *")
```

## 5. Bronze Data Quality

### 5.1 Validation Checks

```sql
-- 1. Schema validation (run after ingestion)
SELECT COUNT(*) as record_count,
       COUNT(DISTINCT facility_code) as unique_facilities,
       MIN(ts_utc) as first_record,
       MAX(ts_utc) as last_record
FROM oe_facilities_raw
WHERE DATE(ts_utc) = CURRENT_DATE;

-- Expected: should see records from today

-- 2. Null checks
SELECT 
  COUNT(*) as total_records,
  COUNT(facility_code) as non_null_facility_code,
  COUNT(generation_mw) as non_null_generation,
  COUNT(ts_utc) as non_null_timestamp
FROM oe_generation_hourly_raw
WHERE DATE(ts_utc) = CURRENT_DATE;

-- Expected: all counts should be equal

-- 3. Range validation
SELECT COUNT(*) as out_of_range
FROM oe_generation_hourly_raw
WHERE generation_mw < -1 
   OR generation_mw > (SELECT MAX(total_capacity_mw) FROM oe_facilities_raw)
AND DATE(ts_utc) = CURRENT_DATE;

-- Expected: should be 0

-- 4. Dedup detection
SELECT facility_code, ts_utc, COUNT(*) as dup_count
FROM oe_generation_hourly_raw
WHERE DATE(ts_utc) = CURRENT_DATE
GROUP BY facility_code, ts_utc
HAVING COUNT(*) > 1;

-- Expected: should be empty
```

### 5.2 Freshness Monitoring

```sql
-- Check ingestion freshness
SELECT 
  table_name,
  MAX(_ingest_time) as last_ingest,
  CURRENT_TIMESTAMP() - MAX(_ingest_time) as age_minutes
FROM (
  SELECT '_generation_raw' as table_name, _ingest_time FROM oe_generation_hourly_raw
  UNION ALL
  SELECT '_weather_raw', _ingest_time FROM om_weather_hourly_raw
)
GROUP BY table_name;

-- Expected: age_minutes < 60 (within 1 hour)
```

## 6. Retention & Cleanup

### 6.1 Retention Policy

| Tier | Retention | Use Case |
|------|-----------|----------|
| **Hot (Current)** | 7 days | Active queries & joins |
| **Warm (Recent)** | 90 days | Typical analysis window |
| **Cold (Archive)** | 1 year | Compliance & history |

### 6.2 Cleanup Script

```sql
-- Cleanup old snapshots (keep only recent)
-- Iceberg automatically manages data files
-- You can remove old snapshots via:

ALTER TABLE lh.bronze.oe_generation_hourly_raw
SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms' = '259200000'  -- 3 days
);

-- Or manually expire old data
-- SET_CURRENT_SNAPSHOT procedure (version 1.5+)
```

## 7. Related Documentation

- 📊 [Medallion Architecture Design](../architecture/medallion-design.md)
- 🗂️ [Data Lineage](data-lineage.md)
- 📈 [Silver Layer](silver-layer.md)
- 🔍 [Queries & Analytics](queries.md)

---

**File Locations:**
- DDL: `/sql/bronze/*.sql`
- Ingest Code: `/src/pv_lakehouse/etl/bronze_ingest.py`
- Tests: `/tests/test_bronze_tables_complete.py`
- Flows: `/flows/ingest_bronze.py`

**Status:** ✅ Complete & Ready for Production
