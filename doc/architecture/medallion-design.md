# Thiết Kế Medallion Architecture

## 1. Giới Thiệu Mô Hình Medallion

Medallion Architecture (còn gọi là Delta Lake Architecture) chia data lakehouse thành 3 layers, mỗi layer có mục đích cụ thể:

```
┌─────────────────────────────────────────────────────┐
│           GOLD LAYER (Analytics)                    │
│   - Business-ready, aggregated data                 │
│   - Optimized for reporting & BI                    │
│   - ~5-10% volume so as Bronze                      │
│   - Tables: dim_*, fact_*, metrics_*                │
└─────────────────────────────────────────────────────┘
                         ↑
           Transformations & Aggregations
                         │
┌─────────────────────────────────────────────────────┐
│         SILVER LAYER (Cleansed)                     │
│   - Normalized, deduplicated data                   │
│   - Business rules applied                          │
│   - ~20-30% volume so as Bronze                     │
│   - Tables: *_normalized, *_cleansed                │
└─────────────────────────────────────────────────────┘
                         ↑
              ETL Transformations
                         │
┌─────────────────────────────────────────────────────┐
│         BRONZE LAYER (Raw)                          │
│   - Ingested data as-is from sources                │
│   - Minimal transformations                         │
│   - Includes technical metadata                     │
│   - Tables: *_raw suffix                            │
└─────────────────────────────────────────────────────┘
                         ↑
             External Data Sources
```

## 2. Bronze Layer (Raw Data)

### 2.1 Mục Đích

- **Ingestion tối thiểu**: Ghi dữ liệu nguyên bản từ nguồn
- **Audit Trail**: Giữ bản ghi gốc cho compliance & debugging
- **Lineage Tracking**: Biết dữ liệu từ đâu, khi nào
- **Quick Recovery**: Nếu layer cao hơn có bug, dễ dàng reset

### 2.2 Đặc Điểm Thiết Kế

| Đặc Điểm | Mô Tả |
|---------|-------|
| **Naming** | `<source>_<entity>_raw` (oe_generation_hourly_raw) |
| **Format** | Apache Iceberg v2 |
| **Partition** | `days(ts_utc)` - hàng ngày theo UTC |
| **Schema** | Nguyên bản từ source API |
| **Metadata Columns** | `_ingest_time`, `_source`, `_hash` |
| **Write Mode** | Append (thêm từng batch) |
| **Cleanup** | Giữ 90 ngày (configurable) |

### 2.3 Current Bronze Tables

#### 2.3.1 `oe_facilities_raw`

**Source:** OpenNEM API - Facilities Registry

**Schema:**
```sql
facility_code          STRING      -- DUID (AVLSF, BERYLSF)
facility_name          STRING      -- Full name
latitude               DOUBLE      -- Geographic location
longitude              DOUBLE      -- Geographic location
network_id             STRING      -- NEM, WEM
network_region         STRING      -- NSW1, VIC1, QLD1, SA1, WEM
total_capacity_mw      DOUBLE      -- Nameplate capacity
fuel_technology        STRING      -- solar_utility, solar_rooftop
operational_status     STRING      -- operating, commissioning, retired
ts_utc                 TIMESTAMP   -- Partition key (snapshot time)
_ingest_time           TIMESTAMP   -- Ingestion time (metadata)
_source                STRING      -- opennem_api_v1
_hash                  STRING      -- Dedup hash
```

**Partitioning:**
```
s3a://lakehouse/warehouse/lh/oe_facilities_raw/
├── year=2025/month=01/day=15/
│   ├── 00000.parquet
│   ├── 00001.parquet
│   └── ...
```

**Ingestion Logic:**
1. Call OpenNEM facilities endpoint
2. Extract all active solar facilities
3. Add ingestion timestamp
4. Calculate hash from (facility_code, ts_utc)
5. Write to Iceberg with APPEND mode
6. Upsert by (facility_code) to avoid duplicates

#### 2.3.2 `oe_generation_hourly_raw`

**Source:** OpenNEM API - Generation Time Series

**Schema:**
```sql
ts_utc                 TIMESTAMP   -- Observation hour (partition key)
duid                   STRING      -- Individual unit ID
generation_mw          DOUBLE      -- MW output
capacity_factor        DOUBLE      -- generation/registered_capacity
data_quality_code      STRING      -- ACTUAL, ESTIMATED
ts_utc                 TIMESTAMP   -- Partition key
_ingest_time           TIMESTAMP   -- Ingestion time
_source                STRING      -- opennem_api_v1
_hash                  STRING      -- Dedup hash
```

**Ingestion Logic:**
1. Query OpenNEM generation endpoint for past 48 hours
2. Filter to active solar units only
3. Calculate capacity_factor from metadata
4. Append to existing data

**Data Freshness:**
- Ingest every hour (typically 30-60 min after generation window closes)
- 48-hour lookback window (handles delayed updates)
- Upsert by (duid, ts_utc) to update with final values

#### 2.3.3 `om_weather_hourly_raw`

**Source:** Open-Meteo Weather API

**Schema:**
```sql
ts_utc                        TIMESTAMP   -- Observation time
latitude                      DOUBLE      -- Grid location (0.25° resolution)
longitude                     DOUBLE      -- Grid location
temperature_2m                DOUBLE      -- °C
relative_humidity_2m          DOUBLE      -- %
pressure_msl                  DOUBLE      -- hPa
wind_speed_10m                DOUBLE      -- m/s
wind_direction_10m            DOUBLE      -- degrees
shortwave_radiation           DOUBLE      -- W/m² (GHI - critical for solar)
direct_normal_irradiance      DOUBLE      -- W/m² (DNI)
diffuse_radiation             DOUBLE      -- W/m²
cloud_cover                   DOUBLE      -- %
cloud_cover_low               DOUBLE      -- %
precipitation                 DOUBLE      -- mm
data_completeness_score       DOUBLE      -- 0-1 scale
api_source_model              STRING      -- ERA5, ERA5-Land, etc
_ingest_time                  TIMESTAMP   -- Ingestion time
_source                       STRING      -- openmeteo_api_v1
_hash                         STRING      -- Dedup hash
```

**Data Location Grid:**
- Major solar facilities in NSW/QLD
- Grid resolution: 0.25° (approx 28 km at equator)
- Multiple forecast runs per day (historical data)

#### 2.3.4 `om_air_quality_hourly_raw`

**Source:** Open-Meteo Air Quality API

**Schema:**
```sql
ts_utc                      TIMESTAMP   -- Observation time
latitude                    DOUBLE      -- CAMS grid location
longitude                   DOUBLE      -- CAMS grid location
pm2_5                       DOUBLE      -- μg/m³ (Fine particulates)
pm10                        DOUBLE      -- μg/m³ (Coarse particulates)
aerosol_optical_depth       DOUBLE      -- Dimensionless (critical for solar)
dust                        DOUBLE      -- μg/m³
nitrogen_dioxide            DOUBLE      -- μg/m³
ozone                       DOUBLE      -- μg/m³
european_aqi                INT         -- AQI index
data_completeness_score     DOUBLE      -- 0-1 scale
cams_domain                 STRING      -- cams_global, cams_europe
_ingest_time                TIMESTAMP   -- Ingestion time
_source                     STRING      -- openmeteo_airquality_api_v1
_hash                       STRING      -- Dedup hash
```

**Importance for Solar:**
- Aerosol Optical Depth (AOD) is strong predictor of irradiance
- PM2.5 reduces solar irradiance by ~2-3% per 100 μg/m³
- Critical for accurate solar generation forecasting

### 2.4 Bronze Table Properties

**Iceberg Format Version:** 2

**Table Properties:**
```sql
TBLPROPERTIES (
  'format-version' = '2',
  'write.metadata.metrics.default' = 'full'
)
```

**Write Characteristics:**
- Append-only initially
- Upsert when updates needed (via Merge Into)
- Snapshot isolation prevents dirty reads
- Compaction job removes old snapshots periodically

### 2.5 Retention Policy

| Layer | Retention | Reason |
|-------|-----------|--------|
| **Recent** | 90 days | Hot storage, frequent access |
| **Archive** | 1 year | Cold storage, compliance |
| **Snapshots** | 7 days | Time travel capability |

## 3. Silver Layer (Normalized Data)

### 3.1 Mục Đích

- **Cleansing**: Remove nulls, handle outliers
- **Deduplication**: Ensure unique records
- **Normalization**: Consistent types & formats
- **Enrichment**: Join with reference data (facilities)
- **Business Rules**: Apply domain logic
- **Performance**: Optimized for common queries

### 3.2 Thiết Kế Chung

| Đặc Điểm | Mô Tả |
|---------|-------|
| **Naming** | `<entity>_normalized` or `<entity>_cleansed` |
| **Format** | Apache Iceberg v2 |
| **Partition** | `days(ts_utc)` - consistent với Bronze |
| **Schema** | Standardized, documented |
| **Materialized** | Fully materialized for performance |
| **SLO** | Available within 2 hours of ingestion |

### 3.3 Example Silver Table: `generation_normalized`

**Source:** Bronze `oe_generation_hourly_raw` + `oe_facilities_raw`

**Schema:**
```sql
-- Time dimension
ts_utc                TIMESTAMP NOT NULL
hour_utc              INT            -- Hour of day (0-23)
date_utc              DATE           -- Date partition friendly

-- Facility dimension
facility_code         STRING NOT NULL
facility_name         STRING
network_region        STRING
latitude              DOUBLE
longitude             DOUBLE

-- Generation metrics (cleansed)
generation_mw         DOUBLE
capacity_factor       DOUBLE
forecast_type         STRING         -- ACTUAL, FORECASTED, ESTIMATED
quality_score         DOUBLE         -- 0-1, data quality indicator

-- Operational state
is_operational        BOOLEAN
is_within_normal_range BOOLEAN       -- Outlier detection

-- Lineage
bronze_source_time    TIMESTAMP      -- When bronze record was created
silver_created_time   TIMESTAMP      -- When normalized record created

PARTITIONED BY (days(ts_utc))
```

**Transformation Logic:**
```sql
WITH deduplicated AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY facility_code, ts_utc ORDER BY _ingest_time DESC) as rn
  FROM bronze.oe_generation_hourly_raw
)
SELECT
  d.ts_utc,
  HOUR(d.ts_utc) as hour_utc,
  CAST(d.ts_utc AS DATE) as date_utc,
  d.duid as facility_code,
  f.facility_name,
  f.network_region,
  f.latitude,
  f.longitude,
  COALESCE(d.generation_mw, 0) as generation_mw,
  CASE 
    WHEN d.generation_mw < 0 THEN 0  -- Fix negative generation
    WHEN d.generation_mw > f.total_capacity_mw THEN f.total_capacity_mw  -- Cap at capacity
    ELSE d.generation_mw
  END as generation_mw_cleansed,
  CASE 
    WHEN d.capacity_factor BETWEEN 0 AND 1 THEN d.capacity_factor
    ELSE SAFE_DIVIDE(d.generation_mw, f.total_capacity_mw)
  END as capacity_factor,
  d.data_quality_code as forecast_type,
  CASE 
    WHEN d.data_quality_code = 'ACTUAL' THEN 0.95
    WHEN d.data_quality_code = 'ESTIMATED' THEN 0.75
    ELSE 0.50
  END as quality_score,
  f.operational_status = 'operating' as is_operational,
  BETWEEN(d.generation_mw, f.total_capacity_mw * -0.1, f.total_capacity_mw * 1.1) as is_within_normal_range,
  d._ingest_time as bronze_source_time,
  CURRENT_TIMESTAMP() as silver_created_time
FROM deduplicated d
LEFT JOIN bronze.oe_facilities_raw f
  ON d.duid = f.facility_code
  AND DATE(d.ts_utc) = DATE(f.ts_utc)
WHERE d.rn = 1  -- Keep latest version only
```

### 3.4 Other Silver Tables (Planned)

```
silver/
├── generation_normalized        # Cleaned generation time series
├── weather_normalized           # Cleaned weather observations
├── air_quality_normalized       # Cleaned air quality
├── facilities_dimension         # Facility master data
└── calendar_dimension           # Time dimension
```

## 4. Gold Layer (Analytics/Business)

### 4.1 Mục Đích

- **Aggregation**: Pre-calculated metrics
- **Analytics**: KPI, trends, dashboards
- **Performance**: Optimized for BI tools
- **Semantics**: Business-friendly naming

### 4.2 Thiết Kế Chung

| Đặc Điểm | Mô Tả |
|---------|-------|
| **Naming** | `fact_*`, `dim_*`, or `metrics_*` |
| **Format** | Apache Iceberg v2 or Parquet views |
| **Aggregation Level** | Daily, hourly, monthly |
| **Freshness** | Daily (near-real-time optional) |
| **SLO** | Available by 6am for previous day |

### 4.3 Example Gold Tables

#### `fact_daily_generation`

**Aggregation from:** `silver.generation_normalized`

**Schema:**
```sql
date_utc                DATE
facility_code           STRING
facility_name           STRING
network_region          STRING
latitude                DOUBLE
longitude               DOUBLE

-- Daily aggregates
total_generation_mwh    DOUBLE      -- Sum of hourly MW
avg_capacity_factor     DOUBLE      -- Average CF
peak_generation_mw      DOUBLE      -- Max hourly output
generation_hours        INT         -- Hours with generation > 0
downtime_hours          INT         -- Hours offline

-- Data quality
data_completeness_pct   DOUBLE      -- % of hours with data
quality_score           DOUBLE      -- Average quality

PARTITIONED BY (date_utc)
```

#### `metrics_generation_forecast`

**For Dashboard/Reporting**

```sql
date_utc                DATE
forecast_date           DATE        -- Date being forecasted
network_region          STRING
method                  STRING      -- persistence, ml_model, ensemble
forecasted_generation_mwh DOUBLE
actual_generation_mwh   DOUBLE
mae                     DOUBLE      -- Mean Absolute Error
rmse                    DOUBLE      -- Root Mean Squared Error
mape                    DOUBLE      -- Mean Absolute Percentage Error
```

#### `dim_weather_summary`

**Daily Weather Summary for reporting**

```sql
date_utc                DATE
location_name           STRING      -- Major facility location
avg_temperature         DOUBLE
avg_solar_radiation     DOUBLE      -- GHI (most important for solar)
avg_cloud_cover         DOUBLE
total_precipitation     DOUBLE
weather_quality_score   DOUBLE      -- 0-1 data availability

PARTITIONED BY (date_utc)
```

### 4.4 Gold Layer SLO

- **Availability**: 99.5% uptime
- **Freshness**: 6am UTC daily
- **Accuracy**: Validated daily with source systems
- **Documentation**: Schema, lineage, owner documented

## 5. Data Lineage & Governance

### 5.1 Lineage Tracking

```
External Sources
├─ OpenNEM API (facilities, generation)
├─ Open-Meteo API (weather)
└─ Open-Meteo API (air quality)
    │
    ↓ (Ingestion via Spark)
    │
Bronze Layer
├─ oe_facilities_raw
├─ oe_generation_hourly_raw
├─ om_weather_hourly_raw
└─ om_air_quality_hourly_raw
    │
    ↓ (ETL transformations)
    │
Silver Layer
├─ generation_normalized (from oe_generation + oe_facilities)
├─ weather_normalized
├─ facilities_dimension
└─ calendar_dimension
    │
    ↓ (Aggregations & business logic)
    │
Gold Layer
├─ fact_daily_generation
├─ fact_hourly_metrics
├─ metrics_generation_forecast
└─ dim_weather_summary
    │
    ↓ (Consumption)
    │
BI Tools / Dashboards / Reports
```

### 5.2 Metadata Tracking

**Each record carries:**
- `_ingest_time` - When entered Bronze
- `_source` - Source system identifier
- `_hash` - For deduplication
- Iceberg snapshot versioning - Full history

**Each transformation adds:**
- Timestamp
- Operator/User
- Version
- Configuration parameters

## 6. Quality Assurance

### 6.1 Bronze Layer QA

- ✅ Schema validation on ingestion
- ✅ Not-null checks for key columns
- ✅ Range validation (e.g., generation 0-capacity)
- ✅ Deduplication detection
- ✅ Freshness checks (within SLA window)

### 6.2 Silver Layer QA

- ✅ Deduplication validation
- ✅ Null imputation
- ✅ Outlier detection & flagging
- ✅ Join completeness checks
- ✅ Data type consistency

### 6.3 Gold Layer QA

- ✅ Aggregation correctness
- ✅ Total row matching (if applicable)
- ✅ No unexpected nulls
- ✅ Freshness SLA
- ✅ Comparison with previous period (TBD)

## 7. Best Practices

✅ **Naming Conventions:**
- Bronze: `<source>_<entity>_raw`
- Silver: `<entity>_normalized` or `<entity>_cleansed`
- Gold: `fact_*`, `dim_*`, or `metrics_*`

✅ **Partitioning:**
- Always partition Bronze by `days(ts_utc)`
- Mirror partitioning in Silver/Gold for performance
- Enables efficient retention policies

✅ **Metadata:**
- Include `_ingest_time`, `_source`, `_hash` in Bronze
- Add lineage columns in Silver/Gold
- Document all transformations

✅ **Performance:**
- Coalesce small files before querying
- Use Iceberg compaction jobs
- Collect statistics on partitions

✅ **Versioning:**
- Use Iceberg snapshots for time travel
- Tag important versions
- Document breaking schema changes

---

**Next Steps:**
- Review [Bronze Layer Implementation](../data-model/bronze-layer.md)
- Design Silver layer tables for your domain
- Set up Gold layer dashboards

**References:**
- [Databricks Medallion Architecture](https://www.databricks.com/blog/2022/06/24/etl-pipeline-design-with-delta-lake.html)
- [Iceberg Partitioning](https://iceberg.apache.org/docs/latest/spec/#partitioning)
