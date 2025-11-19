# Silver Layer Design Document

## Overview

The **Silver Layer** is the cleansing and normalization layer of the data lakehouse. It transforms raw Bronze data into consistent, high-quality datasets by removing duplicates, validating schemas, handling missing values, and applying business rules.

### Core Purpose

- **Data Cleansing**: Remove duplicates, validate data types, standardize formats
- **Data Enrichment**: Add calculated columns, apply transformations, compute metrics
- **Data Quality**: Implement quality flags, completeness tracking, anomaly detection
- **Business Logic**: Apply domain rules (facility grouping, regional classifications)
- **Incremental Processing**: Efficiently update only changed records from Bronze

---

## Architecture

### Data Flow

```
┌──────────────────────────────┐
│  Bronze Layer Tables         │
│  - raw_facility_timeseries   │
│  - raw_facility_weather      │
│  - raw_facility_air_quality  │
│  - raw_facilities            │
└──────────────────┬───────────┘
                   │ Spark SQL
                   ▼
┌────────────────────────────────────────┐
│  Silver Layer Transformations          │
│  - Deduplication                       │
│  - Schema Normalization                │
│  - Data Validation                     │
│  - Quality Flagging                    │
│  - Business Rule Application           │
└────────────────────┬───────────────────┘
                     │
                     ▼
┌────────────────────────────────────────┐
│  Silver Layer Tables (Iceberg)         │
│  - lh.silver.clean_facility_master     │
│  - lh.silver.clean_hourly_energy       │
│  - lh.silver.clean_hourly_weather      │
│  - lh.silver.clean_hourly_air_quality  │
└────────────────────────────────────────┘
```

### Design Principles

1. **Idempotent Transforms**: Same input always produces same output (safe re-runs)
2. **Data Quality Tracking**: Maintain quality flags and completeness metrics
3. **Slowly-Changing Dimensions**: Handle facility metadata updates (SCD Type 1)
4. **Partition Pruning**: Organize by date for efficient incremental processing
5. **Schema Versioning**: Document all transformations for lineage tracking

---

## Tables Specification

### `clean_facility_master` (Facility Dimension)
**Source**: `lh.bronze.raw_facilities`  
**Grain**: One row per facility (current version only)  
**Partitioning**: None (static reference data, ~50 rows)  
**Update Frequency**: Weekly or on-demand  
**Row Count**: ~5-50 facilities

| Column | Type | Purpose |
|--------|------|---------|
| **Business Keys** |
| facility_code | STRING | Unique facility identifier (from Bronze) |
| facility_name | STRING | Cleaned/standardized facility name |
| **Location** |
| location_lat | DECIMAL(10,7) | Validated latitude (WGS84) |
| location_lng | DECIMAL(10,7) | Validated longitude (WGS84) |
| location_altitude | DECIMAL(8,2) | Altitude (meters) |
| timezone | STRING | IANA timezone (standardized) |
| **Capacity** |
| total_capacity_mw | DECIMAL(10,2) | Capacity (MWh) with null handling |
| total_capacity_registered_mw | DECIMAL(10,2) | Registered capacity (MWh) |
| total_capacity_maximum_mw | DECIMAL(10,2) | Maximum capacity (MWh) |
| **Status** |
| effective_from | DATE | When facility became active |
| effective_to | DATE | Decommission date (NULL if active) |
| is_current | BOOLEAN | Current version flag (SCD Type 1) |
| **Data Quality** |
| quality_flag | STRING | "good", "incomplete", "suspect" |
| quality_issues | STRING | Comma-separated list of issues found |
| completeness_pct | DECIMAL(5,2) | Completeness percentage (0-100) |
| **Audit** |
| created_at | TIMESTAMP | When record was first created |
| updated_at | TIMESTAMP | When record was last updated |
| bronze_ingestion_timestamp | TIMESTAMP | Source Bronze ingestion time |

**Transformation Logic**:

1. **Deduplication**: 
   - Deduplicate on `facility_code`
   - Use latest `bronze_ingestion_timestamp` for conflicts
   - Keep only `is_current=true` or `effective_to IS NULL`

2. **Null Handling**:
   ```sql
   COALESCE(facility_name, 'UNKNOWN') as facility_name,
   COALESCE(total_capacity_mw, 0) as total_capacity_mw,
   ```

3. **Data Validation**:
   - Latitude: -90 to 90
   - Longitude: -180 to 180
   - Capacity: > 0
   - Timezone: IANA format (e.g., "Asia/Ho_Chi_Minh")

4. **Quality Flagging**:
   ```
   Good:       All fields populated, valid ranges
   Incomplete: Missing capacity or timezone
   Suspect:    Coordinates outside expected region
   ```

---

### `clean_hourly_energy` (Energy Facts)
**Source**: `lh.bronze.raw_facility_timeseries`  
**Grain**: One row per hour per facility  
**Partitioning**: `year`, `month` (for partition elimination)  
**Update Frequency**: Incremental hourly or daily  
**Row Count**: ~8.76M per year (24h × 365d × 1000 fac)

| Column | Type | Purpose |
|--------|------|---------|
| **Keys** |
| facility_code | STRING | Facility identifier |
| facility_name | STRING | Facility name (denormalized for readability) |
| network_code | STRING | Electrical network identifier |
| network_region | STRING | Geographic region (standardized) |
| **Timestamp** |
| date_hour | TIMESTAMP | Hour UTC (normalized, no timezone) |
| **Energy Measures** |
| energy_mwh | DECIMAL(12,6) | Energy produced (MWh), NULL handling applied |
| intervals_count | INTEGER | Number of data intervals in hour |
| **Data Quality** |
| quality_flag | STRING | "good", "partial", "poor", "estimated" |
| quality_issues | STRING | Comma-separated issues (duplicates, gaps, nulls) |
| completeness_pct | DECIMAL(5,2) | Percentage of hour with valid data |
| **Audit** |
| created_at | TIMESTAMP | When row was first created |
| updated_at | TIMESTAMP | Last update timestamp |
| bronze_ingestion_timestamp | TIMESTAMP | Source Bronze time |

**Transformation Logic**:

1. **Deduplication**:
   ```sql
   Row_Number() Over (
       Partition By facility_code, date_hour 
       Order By bronze_ingestion_timestamp DESC
   ) = 1
   ```
   - Keep latest by ingestion timestamp

2. **Timestamp Normalization**:
   ```sql
   date_hour = CAST(CAST(date_hour AS DATE) AS TIMESTAMP)
   -- Removes any timezone offset, ensures UTC
   ```

3. **Null Handling**:
   ```sql
   energy_mwh = CASE 
       WHEN energy_mwh IS NULL THEN NULL
       WHEN energy_mwh < 0 THEN NULL  -- Invalid values
       WHEN energy_mwh > capacity_mw * 2 THEN NULL  -- Outliers
       ELSE energy_mwh
   END
   ```

4. **Quality Flagging**:
   - **good**: No data quality issues
   - **partial**: 50-99% completeness
   - **poor**: < 50% completeness
   - **estimated**: Value imputed from adjacent hours

5. **Completeness Calculation**:
   ```sql
   completeness_pct = (
       CASE WHEN energy_mwh IS NOT NULL THEN 100 ELSE 0 END +
       CASE WHEN intervals_count > 0 THEN 0 ELSE -50 END
   ) / 1.5
   ```

---

### `clean_hourly_weather` (Weather Facts)
**Source**: `lh.bronze.raw_facility_weather`  
**Grain**: One row per hour per facility  
**Partitioning**: `year`, `month`  
**Update Frequency**: Incremental hourly  
**Row Count**: ~8.76M per year per facility

| Column | Type | Purpose |
|--------|------|---------|
| **Keys** |
| facility_code | STRING | Facility identifier |
| facility_name | STRING | Facility name (denormalized) |
| **Timestamp** |
| date_hour | TIMESTAMP | Hour UTC (normalized) |
| date | DATE | Date extract (for aggregations) |
| **Solar Radiation** |
| shortwave_radiation | DECIMAL(10,2) | GHI (W/m²), range-validated |
| direct_radiation | DECIMAL(10,2) | Direct beam (W/m²) |
| diffuse_radiation | DECIMAL(10,2) | Diffuse (W/m²) |
| direct_normal_irradiance | DECIMAL(10,2) | DNI (W/m²) |
| **Temperature & Humidity** |
| temperature_2m | DECIMAL(5,2) | Air temp (°C), outlier-cleaned |
| dew_point_2m | DECIMAL(5,2) | Dew point (°C) |
| **Cloud & Precipitation** |
| cloud_cover | DECIMAL(5,1) | Cloud coverage (%), range 0-100 |
| cloud_cover_low | DECIMAL(5,1) | Low clouds (%) |
| cloud_cover_mid | DECIMAL(5,1) | Mid clouds (%) |
| cloud_cover_high | DECIMAL(5,1) | High clouds (%) |
| precipitation | DECIMAL(8,2) | Precipitation (mm), >= 0 |
| sunshine_duration | INTEGER | Sunshine duration (seconds) |
| **Wind** |
| wind_speed_10m | DECIMAL(6,2) | Wind speed (m/s), >= 0 |
| wind_direction_10m | INTEGER | Wind direction (0-359°) |
| wind_gusts_10m | DECIMAL(6,2) | Wind gust speed (m/s) |
| **Pressure** |
| pressure_msl | DECIMAL(8,1) | Sea level pressure (hPa) |
| **Data Quality** |
| quality_flag | STRING | "good", "partial", "poor" |
| quality_issues | STRING | Issues: nulls, outliers, inconsistencies |
| completeness_pct | DECIMAL(5,2) | % of fields with valid data |
| **Audit** |
| created_at | TIMESTAMP | Creation time |
| updated_at | TIMESTAMP | Last update |
| bronze_ingestion_timestamp | TIMESTAMP | Bronze source time |

**Transformation Logic**:

1. **Deduplication**: Keep latest by `bronze_ingestion_timestamp`

2. **Radiation Validation**:
   ```sql
   -- GHI cannot exceed ~1367 W/m² (solar constant)
   shortwave_radiation = CASE
       WHEN shortwave_radiation < 0 THEN NULL
       WHEN shortwave_radiation > 1500 THEN NULL
       ELSE shortwave_radiation
   END
   ```

3. **Temperature Range Checks**:
   ```sql
   -- Reasonable range for Earth: -50°C to +60°C
   temperature_2m = CASE
       WHEN temperature_2m < -50 OR temperature_2m > 60 THEN NULL
       ELSE temperature_2m
   END
   ```

4. **Cloud Cover Normalization**:
   ```sql
   -- Ensure 0-100% range
   cloud_cover = CASE
       WHEN cloud_cover IS NULL THEN NULL
       WHEN cloud_cover < 0 THEN 0
       WHEN cloud_cover > 100 THEN 100
       ELSE cloud_cover
   END
   ```

5. **Wind Speed Validation**:
   ```sql
   -- Hurricane max ~100 m/s; ignore outliers
   wind_speed_10m = CASE
       WHEN wind_speed_10m < 0 OR wind_speed_10m > 100 THEN NULL
       ELSE wind_speed_10m
   END
   ```

6. **Quality Flagging**:
   - **good**: All fields present, valid ranges
   - **partial**: 1-2 missing fields
   - **poor**: > 2 missing or invalid fields

---

### `clean_hourly_air_quality` (Air Quality Facts)
**Source**: `lh.bronze.raw_facility_air_quality`  
**Grain**: One row per hour per facility  
**Partitioning**: `year`, `month`  
**Update Frequency**: Incremental hourly  
**Row Count**: ~8.76M per year

| Column | Type | Purpose |
|--------|------|---------|
| **Keys** |
| facility_code | STRING | Facility identifier |
| facility_name | STRING | Facility name (denormalized) |
| **Timestamp** |
| date_hour | TIMESTAMP | Hour UTC (normalized) |
| date | DATE | Date extract |
| **Particulate Matter** |
| pm2_5 | DECIMAL(8,3) | PM2.5 (µg/m³), WHO guideline 5 µg/m³ |
| pm10 | DECIMAL(8,3) | PM10 (µg/m³), WHO guideline 15 µg/m³ |
| dust | DECIMAL(8,3) | Dust (µg/m³) |
| **Gases** |
| nitrogen_dioxide | DECIMAL(8,3) | NO2 (ppb) |
| ozone | DECIMAL(8,3) | O3 (ppb) |
| sulphur_dioxide | DECIMAL(8,3) | SO2 (ppb) |
| carbon_monoxide | DECIMAL(8,3) | CO (ppm) |
| **UV Index** |
| uv_index | DECIMAL(5,2) | UV Index (0-16+) |
| uv_index_clear_sky | DECIMAL(5,2) | UV Index clear sky |
| **Air Quality Index** |
| aqi_value | DECIMAL(8,2) | AQI (0-500+) |
| **Data Quality** |
| quality_flag | STRING | "good", "partial", "poor" |
| quality_issues | STRING | Issues detected |
| completeness_pct | DECIMAL(5,2) | Completeness % |
| **Audit** |
| created_at | TIMESTAMP | Creation time |
| updated_at | TIMESTAMP | Last update |
| bronze_ingestion_timestamp | TIMESTAMP | Bronze source time |

**Transformation Logic**:

1. **Deduplication**: Keep latest by ingestion timestamp

2. **PM2.5 Validation**:
   ```sql
   -- WHO guideline: 5 µg/m³; allow up to 2000 µg/m³ (extreme events)
   pm2_5 = CASE
       WHEN pm2_5 < 0 THEN NULL
       WHEN pm2_5 > 2000 THEN NULL  -- Suspect values
       ELSE pm2_5
   END
   ```

3. **Gas Concentration Validation**:
   ```sql
   -- NO2 typically < 100 ppb in air quality data
   nitrogen_dioxide = CASE
       WHEN nitrogen_dioxide < 0 OR nitrogen_dioxide > 500 THEN NULL
       ELSE nitrogen_dioxide
   END
   ```

4. **AQI Range Validation**:
   ```sql
   -- EPA AQI range: 0-500+ (can exceed 500 in hazardous conditions)
   aqi_value = CASE
       WHEN aqi_value < 0 THEN NULL
       WHEN aqi_value > 1000 THEN NULL  -- Suspect extreme values
       ELSE aqi_value
   END
   ```

5. **Quality Flagging**:
   - **good**: All AQI fields present, valid ranges
   - **partial**: 1-3 missing fields
   - **poor**: > 3 missing or invalid fields

---

## Loading Process

### Orchestration Flow

```
┌────────────────────────────────┐
│  Scheduled Trigger             │
│  (Hourly or Daily)             │
└────────────────┬───────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│  1. Silver Facility Master Load     │
│     - Read Bronze raw_facilities    │
│     - Deduplicate, validate         │
│     - Insert/Update lh.silver...    │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│  2. Silver Energy Load              │
│     - Read Bronze timeseries        │
│     - Increment date range          │
│     - Apply quality rules           │
│     - Write lh.silver.clean_hourly_ │
│       energy (partition overwrite)  │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│  3. Silver Weather Load             │
│     - Read Bronze weather           │
│     - Apply radiation/temp rules    │
│     - Write lh.silver.clean_hourly_ │
│       weather (partition overwrite) │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│  4. Silver Air Quality Load         │
│     - Read Bronze air quality       │
│     - Apply PM/gas validation       │
│     - Write lh.silver.clean_hourly_ │
│       air_quality (partition)       │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│  Data Quality Verification          │
│  - Check for quality flags          │
│  - Alert on > 10% poor records      │
│  - Log completeness metrics         │
└────────────────────────────────────┘
```

### Loading Strategy

**Idempotent Merge/Overwrite**:
```sql
-- For hourly tables (facts)
MERGE INTO lh.silver.clean_hourly_energy tgt
USING (SELECT ... FROM bronze WHERE date_hour >= '2024-01-01') src
ON tgt.facility_code = src.facility_code 
  AND tgt.date_hour = src.date_hour
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Partition overwrite for efficiency
INSERT OVERWRITE TABLE lh.silver.clean_hourly_energy
PARTITION (year=2024, month=1)
SELECT ... FROM bronze WHERE YEAR(date_hour)=2024 AND MONTH(date_hour)=1;
```

**Auto-detect Date Range**:
1. Query `MAX(date_hour)` from Silver table
2. Start from `MAX(date_hour)` + 1 hour
3. Include 24-hour lookback for corrections
4. Process only that window

---

## Data Quality Framework

### Quality Rules by Table

| Table | Rule | Action |
|-------|------|--------|
| clean_hourly_energy | energy_mwh < 0 | NULL out value, flag "poor" |
| clean_hourly_energy | energy_mwh > capacity × 2 | NULL out, flag "suspect" |
| clean_hourly_weather | shortwave_radiation > 1500 | NULL out, flag "poor" |
| clean_hourly_weather | temperature < -50 or > 60 | NULL out, flag "poor" |
| clean_hourly_air_quality | pm2_5 > 2000 | NULL out, flag "poor" |
| clean_hourly_air_quality | aqi_value < 0 | NULL out, flag "poor" |
| clean_facility_master | capacity_mw < 0 | NULL out, flag "incomplete" |

### Completeness Metrics

```sql
-- Example: Energy completeness
completeness_pct = (
    CASE WHEN energy_mwh IS NOT NULL THEN 1 ELSE 0 END * 0.5 +
    CASE WHEN intervals_count > 0 THEN 1 ELSE 0 END * 0.3 +
    CASE WHEN quality_flag IS NOT NULL THEN 1 ELSE 0 END * 0.2
) * 100
```

### Anomaly Detection

```sql
-- Detect sudden drops in generation (potential sensor failure)
SELECT facility_code, date_hour, energy_mwh,
    LAG(energy_mwh) OVER (
        PARTITION BY facility_code 
        ORDER BY date_hour
    ) as prev_mwh
FROM lh.silver.clean_hourly_energy
WHERE ABS(energy_mwh - prev_mwh) / NULLIF(prev_mwh, 0) > 0.5
AND quality_flag = 'good';
```

---

## Running Silver Layer

### CLI Usage

```bash
# Load all Silver tables incrementally
python -m pv_lakehouse.etl.silver.cli \
  --mode incremental \
  --date-start 2024-01-01

# Load specific table
python -m pv_lakehouse.etl.silver.cli clean_hourly_energy \
  --mode incremental
```

### Orchestration

```python
# Prefect Flow example
@flow(name="silver-incremental-load")
def silver_load_flow(limit_per_source: int = 50000):
    # Load dimension first
    load_facility_master()
    
    # Then load facts in parallel
    load_energy_data(limit_per_source)
    load_weather_data(limit_per_source)
    load_air_quality_data(limit_per_source)
    
    # Quality checks
    verify_data_quality()
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Duplicates in Silver | Overlap in date range | Run deduplication SQL with Row_Number |
| Nulls increased | Stricter validation rules | Review thresholds; may be correct |
| Partition explosion | Too many year-month combos | Archive old partitions to cold storage |
| Slow incremental load | Full table scan instead of partition prune | Add partition filter to WHERE clause |

### Debugging Queries

```sql
-- Check latest load time
SELECT MAX(updated_at) FROM lh.silver.clean_hourly_energy;

-- Count by quality flag
SELECT quality_flag, COUNT(*) 
FROM lh.silver.clean_hourly_energy
WHERE DATE(date_hour) = CURRENT_DATE
GROUP BY quality_flag;

-- Find nulls in critical fields
SELECT facility_code, COUNT(*) null_count
FROM lh.silver.clean_hourly_energy
WHERE date_hour >= DATE_SUB(CURRENT_DATE, 7)
AND energy_mwh IS NULL
GROUP BY facility_code
ORDER BY null_count DESC;
```

---

## References

- **Data Quality Dimensions**: Dimension and Fact Table Design (Ralph Kimball)
- **Slowly-Changing Dimensions**: https://en.wikipedia.org/wiki/Slowly_changing_dimension
- **Apache Iceberg**: https://iceberg.apache.org/
- **WHO Air Quality Guidelines**: https://www.who.int/publications/i/item/9789240034228
