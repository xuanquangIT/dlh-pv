# Bronze Layer Design Document

## Overview

The **Bronze Layer** is the raw data ingestion layer of the data lakehouse, responsible for capturing data from external sources in its original form with minimal transformation. It acts as a historical record of all ingested data.

### Core Purpose

- **Raw Data Capture**: Ingest data from external APIs and data sources with minimal transformation
- **Historical Archive**: Maintain a complete audit trail of all data ingestion events
- **Landing Zone**: Serve as the staging area before cleansing and transformation in Silver layer
- **Data Provenance**: Track source systems, ingestion timestamps, and data quality metadata
- **Incremental Loading**: Support efficient delta ingestion for time-series data

---

## Architecture

### Data Flow

```
┌──────────────────────────────┐
│  External Data Sources       │
│  - OpenMeteo (Weather API)   │
│  - OpenElectricity (Energy)  │
│  - Open-Meteo (Air Quality)  │
└──────────────────┬───────────┘
                   │ HTTP/REST API
                   ▼
┌──────────────────────────────────────┐
│  Bronze Layer Loaders                │
│  - load_facility_timeseries.py       │
│  - load_facility_weather.py          │
│  - load_facility_air_quality.py      │
└──────────────────┬───────────────────┘
                   │ Apache Spark
                   ▼
┌──────────────────────────────────────┐
│  Bronze Layer Tables (Iceberg)       │
│  - lh.bronze.raw_facilities          │
│  - lh.bronze.raw_facility_timeseries │
│  - lh.bronze.raw_facility_weather    │
│  - lh.bronze.raw_facility_air_quality│
└──────────────────────────────────────┘
```

### Design Principles

1. **Schema-on-Read**: Minimal schema validation; capture data as-is from source
2. **Immutable Append**: Only append/insert; never update or delete raw records
3. **Partitioned by Time**: All tables partitioned by ingestion date for efficient querying
4. **Source Metadata**: Capture ingestion timestamp, source system, and data quality flags
5. **Deduplication Prevention**: Include source transaction ID to detect duplicates

---

## Tables Specification

### `raw_facilities`
**Source**: OpenMeteo Facilities API  
**Grain**: One row per facility  
**Partitioning**: None (static reference data)  
**Update Frequency**: Manual or weekly refresh  
**Row Count**: ~5-50 facilities

| Column | Type | Purpose |
|--------|------|---------|
| **Business Keys** |
| facility_code | STRING | Unique facility identifier from source |
| facility_name | STRING | Official facility name |
| **Location** |
| location_lat | DECIMAL(10,7) | Latitude (WGS84) |
| location_lng | DECIMAL(10,7) | Longitude (WGS84) |
| location_altitude | DECIMAL(8,2) | Altitude above sea level (meters) |
| timezone | STRING | IANA timezone (e.g., "Asia/Ho_Chi_Minh") |
| **Capacity** |
| total_capacity_mw | DECIMAL(10,2) | Total installed capacity (MW) |
| total_capacity_registered_mw | DECIMAL(10,2) | Registered capacity (MW) |
| total_capacity_maximum_mw | DECIMAL(10,2) | Maximum theoretical capacity (MW) |
| **Metadata** |
| effective_from | DATE | When facility became operational |
| effective_to | DATE | When facility was decommissioned (NULL if active) |
| is_current | BOOLEAN | Whether facility is currently active |
| **Audit** |
| ingestion_timestamp | TIMESTAMP | When record was ingested |
| source_system | STRING | "openmeteo" or other source identifier |
| raw_json | STRING | Complete JSON response from source API |
| data_hash | STRING | MD5/SHA256 hash for duplicate detection |

**Loading Pattern**:
- Full refresh from OpenMeteo facilities endpoint
- No incremental loading needed (reference data)
- Executed manually or on weekly schedule

---

### `raw_facility_timeseries`
**Source**: OpenMeteo Historical Timeseries API  
**Grain**: One row per hour per facility  
**Partitioning**: `ingestion_date` (date when record was loaded)  
**Update Frequency**: Incremental hourly or daily  
**Row Count**: ~8.76M per year (24 hours × 365 days × 1000 facilities)

| Column | Type | Purpose |
|--------|------|---------|
| **Keys** |
| facility_code | STRING | Foreign key to `raw_facilities` |
| facility_name | STRING | Facility name for reference |
| network_code | STRING | Electrical network/grid identifier |
| network_region | STRING | Geographic region of network |
| **Timestamp** |
| date_hour | TIMESTAMP | Data hour (UTC) |
| **Energy Measures** |
| energy_mwh | DECIMAL(12,6) | Actual energy production (MWh) |
| intervals_count | INTEGER | Number of data intervals in hour |
| **Data Quality** |
| completeness_pct | DECIMAL(5,2) | Data completeness (0-100%) |
| quality_flag | STRING | "good", "partial", "poor" |
| quality_issues | STRING | Comma-separated issues detected |
| **Metadata** |
| ingestion_timestamp | TIMESTAMP | When record was ingested |
| ingestion_date | DATE | Date of ingestion (partition column) |
| source_system | STRING | "openelectricity" or identifier |
| data_hash | STRING | MD5/SHA256 hash for duplicate detection |
| raw_json | STRING | Original JSON from API (optional, for debugging) |

**Partitioning**:
```sql
PARTITIONED BY (ingestion_date)
```

**Loading Pattern**:
- Incremental: Load only new hours since last load
- Auto-detect start time from `MAX(date_hour)` in existing Bronze table
- Typical refresh: Hourly with 24-48 hour lookback for corrections
- Deduplication: Use `(facility_code, date_hour, source_system)` as composite key

---

### `raw_facility_weather`
**Source**: OpenMeteo Weather API (historical/forecast)  
**Grain**: One row per hour per facility  
**Partitioning**: `ingestion_date`  
**Update Frequency**: Incremental hourly  
**Row Count**: ~8.76M per year per facility

| Column | Type | Purpose |
|--------|------|---------|
| **Keys** |
| facility_code | STRING | Foreign key to `raw_facilities` |
| facility_name | STRING | Facility name for reference |
| **Timestamp** |
| date_hour | TIMESTAMP | Weather observation hour (UTC) |
| **Solar Radiation** |
| shortwave_radiation | DECIMAL(10,2) | Global horizontal irradiance (W/m²) |
| direct_radiation | DECIMAL(10,2) | Direct beam radiation (W/m²) |
| diffuse_radiation | DECIMAL(10,2) | Diffuse radiation (W/m²) |
| direct_normal_irradiance | DECIMAL(10,2) | Direct normal irradiance (W/m²) |
| **Temperature & Humidity** |
| temperature_2m | DECIMAL(5,2) | Air temperature at 2m height (°C) |
| dew_point_2m | DECIMAL(5,2) | Dew point temperature (°C) |
| **Cloud & Precipitation** |
| cloud_cover | DECIMAL(5,1) | Total cloud coverage (%) |
| cloud_cover_low | DECIMAL(5,1) | Low cloud coverage (%) |
| cloud_cover_mid | DECIMAL(5,1) | Mid-level cloud coverage (%) |
| cloud_cover_high | DECIMAL(5,1) | High cloud coverage (%) |
| precipitation | DECIMAL(8,2) | Precipitation amount (mm) |
| sunshine_duration | INTEGER | Duration of sunshine (seconds) |
| **Wind** |
| wind_speed_10m | DECIMAL(6,2) | Wind speed at 10m height (m/s) |
| wind_direction_10m | INTEGER | Wind direction (0-359 degrees) |
| wind_gusts_10m | DECIMAL(6,2) | Wind gust speed at 10m (m/s) |
| **Pressure** |
| pressure_msl | DECIMAL(8,1) | Sea level pressure (hPa) |
| **Metadata** |
| ingestion_timestamp | TIMESTAMP | When record was ingested |
| ingestion_date | DATE | Date of ingestion (partition column) |
| source_system | STRING | "openmeteo" |
| data_hash | STRING | MD5/SHA256 hash |
| raw_json | STRING | Original JSON response |

**Partitioning**:
```sql
PARTITIONED BY (ingestion_date)
```

**Loading Pattern**:
- Incremental: Load past 48 hours + forecast 7 days ahead
- Lookback: OpenMeteo allows corrections to recent data
- Deduplication: `(facility_code, date_hour, source_system)`

---

### `raw_facility_air_quality`
**Source**: Open-Meteo Air Quality API  
**Grain**: One row per hour per facility  
**Partitioning**: `ingestion_date`  
**Update Frequency**: Incremental hourly  
**Row Count**: ~8.76M per year per facility

| Column | Type | Purpose |
|--------|------|---------|
| **Keys** |
| facility_code | STRING | Foreign key to `raw_facilities` |
| facility_name | STRING | Facility name for reference |
| **Timestamp** |
| date_hour | TIMESTAMP | Air quality observation hour (UTC) |
| **Particulate Matter** |
| pm2_5 | DECIMAL(8,3) | PM2.5 concentration (µg/m³) |
| pm10 | DECIMAL(8,3) | PM10 concentration (µg/m³) |
| dust | DECIMAL(8,3) | Dust concentration (µg/m³) |
| **Gases** |
| nitrogen_dioxide | DECIMAL(8,3) | NO2 concentration (ppb) |
| ozone | DECIMAL(8,3) | O3 concentration (ppb) |
| sulphur_dioxide | DECIMAL(8,3) | SO2 concentration (ppb) |
| carbon_monoxide | DECIMAL(8,3) | CO concentration (ppm) |
| **UV Index** |
| uv_index | DECIMAL(5,2) | UV index (0-16+) |
| uv_index_clear_sky | DECIMAL(5,2) | UV index clear sky scenario |
| **Air Quality Index** |
| aqi_value | DECIMAL(8,2) | Overall AQI (0-500+) |
| **Metadata** |
| ingestion_timestamp | TIMESTAMP | When record was ingested |
| ingestion_date | DATE | Date of ingestion (partition column) |
| source_system | STRING | "open-meteo" |
| data_hash | STRING | MD5/SHA256 hash |
| raw_json | STRING | Original JSON response |

**Partitioning**:
```sql
PARTITIONED BY (ingestion_date)
```

**Loading Pattern**:
- Incremental: Load past 48 hours + forecast 7 days ahead
- Deduplication: `(facility_code, date_hour, source_system)`

---

## Loading Process

### Orchestration Flow

```
┌─────────────────────────────────┐
│  Scheduled Trigger (Hourly)     │
│  - Prefect Flow                 │
│  - or Apache Airflow DAG        │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  For each facility:                         │
│  1. load_facility_timeseries.py             │
│     - Query OpenElectricity API             │
│     - Load last 48 hours of data            │
│     - Insert into raw_facility_timeseries   │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  For each facility:                         │
│  2. load_facility_weather.py                │
│     - Query OpenMeteo Weather API           │
│     - Load past 48h + forecast 7 days       │
│     - Insert into raw_facility_weather      │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  For each facility:                         │
│  3. load_facility_air_quality.py            │
│     - Query Open-Meteo AQ API               │
│     - Load past 48h + forecast 7 days       │
│     - Insert into raw_facility_air_quality  │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  Data Quality Checks                        │
│  - Check for nulls in critical fields       │
│  - Verify date_hour continuity              │
│  - Detect schema drift                      │
└────────────────────────────────────────────┘
```

### Loading Strategy

**Append-Only Pattern**:
- Bronze layer only appends new records
- Never updates or deletes existing rows
- Preserves complete history of data ingestion
- Supports duplicate detection via `data_hash`

**Incremental Loading**:
1. Query `MAX(ingestion_timestamp)` from Bronze table
2. Request data from source since that timestamp
3. Include lookback window (24-48 hours) to capture corrections
4. Deduplicate on `(facility_code, date_hour, source_system, data_hash)`

**Example: Daily Weather Load**
```python
# Pseudo-code
max_ingested_time = get_max_timestamp(bronze_weather_table)
start_time = max_ingested_time - 48.hours  # Lookback for corrections
end_time = now() + 7.days  # Forecast horizon

weather_data = openmeteo_api.fetch_weather(
    facilities=all_facility_codes,
    start_time=start_time,
    end_time=end_time
)

# Deduplicate before insert
deduplicated = weather_data.dedup_on(
    facility_code, date_hour, source_system, data_hash
)

insert_or_ignore(bronze_weather_table, deduplicated)
```

---

## Data Quality & Governance

### Quality Checks

1. **Schema Validation**:
   - All required columns present
   - Data types match expected schema
   - Detect schema evolution from source APIs

2. **Completeness**:
   - No unexpected NULL values in key fields
   - Row count matches API response
   - Timestamp continuity (no gaps > 1 hour)

3. **Duplicate Detection**:
   - Use MD5/SHA256 hash of entire row
   - Flag on `(facility_code, date_hour, source_system)`
   - Track duplicate rate for monitoring

4. **Range Checks**:
   - Temperature: -50°C to +60°C (reasonable range)
   - Wind speed: 0 to 300 m/s (max hurricane speed)
   - PM2.5: 0 to 2000 µg/m³ (WHO max is 500)
   - AQI: 0 to 500+ (valid range)

### Audit Columns

All Bronze tables include:
- `ingestion_timestamp`: Exact time of ingestion (for audit trail)
- `ingestion_date`: Partition column for efficient querying
- `source_system`: Identify data source (for multi-source scenarios)
- `data_hash`: Detect exact duplicates across ingestions
- `raw_json`: Optional: preserve original source format for debugging

### Data Lineage

```
OpenMeteo API v1
    │
    ├──→ Facilities Endpoint
    │        └→ raw_facilities
    │
    ├──→ Weather Endpoint
    │        └→ raw_facility_weather
    │
    └──→ Air Quality Endpoint
             └→ raw_facility_air_quality

OpenElectricity API v1
    │
    └──→ Timeseries Endpoint
             └→ raw_facility_timeseries
```

---

## Performance & Storage

### Partitioning Strategy

| Table | Partition | Benefit |
|-------|-----------|---------|
| raw_facility_timeseries | ingestion_date | Enable partition pruning; typical queries scan recent 30-90 days |
| raw_facility_weather | ingestion_date | Same; weather data corrected for past 48h |
| raw_facility_air_quality | ingestion_date | Same |
| raw_facilities | None | Static reference; small table (~50 rows) |

### Estimated Storage

```
raw_facility_timeseries:    ~250 GB per year (24h × 365d × 1000 fac × 100 bytes/row)
raw_facility_weather:       ~400 GB per year (more columns)
raw_facility_air_quality:   ~300 GB per year
raw_facilities:             ~10 KB (minimal)

Total Bronze Layer:         ~950 GB per year (with 3-year retention = 2.85 TB)
```

### Query Performance

Typical queries:
```sql
-- Query recent data (fast, partition-pruned)
SELECT * FROM lh.bronze.raw_facility_weather
WHERE ingestion_date >= CURRENT_DATE - INTERVAL 7 DAYS
AND facility_code = 'FAC001';
-- Result: Scans ~5 GB (1 week × 700MB/day)

-- Query all weather for a facility (medium, full table scan)
SELECT COUNT(*) FROM lh.bronze.raw_facility_weather
WHERE facility_code = 'FAC001';
-- Result: Scans 400 GB (full year)
```

---

## Running Bronze Layer Loads

### CLI Usage

```bash
# Load facilities (static reference, usually manual)
python -m pv_lakehouse.etl.bronze.load_facilities

# Load timeseries for all facilities (hourly incremental)
python -m pv_lakehouse.etl.bronze.load_facility_timeseries --mode incremental

# Load weather for all facilities (hourly incremental)
python -m pv_lakehouse.etl.bronze.load_facility_weather --mode incremental

# Load air quality for all facilities (hourly incremental)
python -m pv_lakehouse.etl.bronze.load_facility_air_quality --mode incremental
```

### Scheduled Orchestration

Using Prefect (see `src/pv_lakehouse/prefect/bronze_silver_hourly_incremental.py`):

```python
# Runs hourly; each load is idempotent
@flow(name="bronze-incremental-load")
def bronze_load_flow():
    load_timeseries()
    load_weather()
    load_air_quality()
    verify_data_quality()
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Duplicate rows | API returned same data | Check `data_hash`; run deduplication SQL |
| Missing hours | API temporary outage | Re-run with extended lookback window |
| High NULL rates | Schema drift from API | Inspect `raw_json` column; update schema |
| Partition explosion | Too many ingestion dates | Archive old partitions to cold storage |
| API rate limit | Too many concurrent requests | Add exponential backoff; reduce concurrent facilities |

### Debugging

```sql
-- Check latest ingestion
SELECT MAX(ingestion_timestamp) FROM lh.bronze.raw_facility_weather;

-- Count records by facility
SELECT facility_code, COUNT(*) 
FROM lh.bronze.raw_facility_weather
WHERE ingestion_date = CURRENT_DATE
GROUP BY facility_code
ORDER BY COUNT(*) DESC;

-- Detect duplicates
SELECT facility_code, date_hour, COUNT(*) 
FROM lh.bronze.raw_facility_weather
WHERE ingestion_date >= CURRENT_DATE - 1
GROUP BY facility_code, date_hour
HAVING COUNT(*) > 1;
```

---

## References

- **OpenMeteo API**: https://open-meteo.com/en/docs
- **OpenElectricity**: https://openelectricity.org/
- **Apache Iceberg**: https://iceberg.apache.org/
- **Medallion Architecture**: https://www.databricks.com/glossary/medallion-architecture
