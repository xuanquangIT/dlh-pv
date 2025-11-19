# Gold Layer Design Document

## Overview

The **Gold Layer** is the presentation layer of the data lakehouse, implementing a **Dimensional Star Schema** optimized for reporting and analytics. It transforms cleansed Silver-layer data into business-ready fact and dimension tables.

### Core Purpose

- **Dimensional Star Schema**: Organize data for fast BI queries through denormalized fact tables joined with conformed dimensions
- **Business Semantics**: Map technical Silver tables into domain-specific analytical concepts (facilities, dates, times, environmental conditions)
- **Performance Optimization**: Pre-computed aggregations and optimized partitioning for downstream BI tools
- **Incremental Loading**: Support both full refreshes and efficient incremental updates
- **Data Quality**: Maintain audit columns and data lineage

---

## Architecture

### Design Pattern: Dimensional Star Schema

```
┌────────────────────────────────────────────────────────────────┐
│                        FACT TABLES (Center)                    │
│                                                                │
│  fact_solar_environmental (1 row per hour per facility)       │
│  fact_energy_forecast (1 row per forecast per hour per facility)
│                                                                │
└────────────────────────────────────────────────────────────────┘
         │               │                │                 │
         ▼               ▼                ▼                 ▼
    ┌─────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────────┐
    │dim_date │    │dim_time  │    │dim_facility   │ dim_forecast_model│
    │         │    │          │    │          │    │ _version         │
    │- date_key    │-time_key │    │-facility_key  │                  │
    │- month   │    │- hour    │    │- code    │    │- model_version_id│
    │- quarter │    │-day_part │    │- capacity    │- algorithm   │
    │- season  │    │          │    │          │    │- performance │
    └─────────┘    └──────────┘    └──────────┘    └──────────────────┘

    ┌──────────────────┐    ┌────────────────────┐
    │dim_aqi_category  │    │dim_air_quality_category
    │                  │    │
    │-aqi_category_key │    │-air_quality_category_key
    │- category_name   │    │- category_name
    │- range_min/max   │    │- range_min/max
    └──────────────────┘    └────────────────────┘
```

### Loading Strategy

The Gold layer uses a **declarative, registry-based loading system**:

1. **CLI-driven Execution**: Run loaders via command line with flexible options
2. **Loader Registry**: Centralized mapping of dataset names to loader classes
3. **Dependency Management**: Dimensions load before facts; static dimensions load first
4. **Mode Selection**: Choose between `full` (rebuild all) or `incremental` (delta only) loading

---

## Tables Specification

### Dimension Tables

#### `dim_date` (Date Dimension)
**Source**: Extracted from Silver hourly tables (`clean_hourly_energy`, `clean_hourly_weather`, `clean_hourly_air_quality`)  
**Grain**: One row per unique calendar date  
**Partitioning**: `year`, `month`  
**Row Count**: ~1,825 (5 years)

| Column | Type | Purpose |
|--------|------|---------|
| date_key | INTEGER (YYYYMMDD format) | Surrogate key for joins |
| full_date | DATE | Actual calendar date |
| year | INTEGER | Extract for analysis |
| quarter | INTEGER | Q1-Q4 for quarterly reporting |
| month | INTEGER | 1-12 for monthly analysis |
| month_name | STRING | "January", "February", etc. |
| week | INTEGER | ISO week number |
| day_of_month | INTEGER | 1-31 |
| day_of_week | INTEGER | Monday=1, Sunday=7 |
| day_name | STRING | "Monday", "Tuesday", etc. |

**Load Characteristics**:
- Full refresh on every load (small table, ~1825 rows)
- Auto-detects date range from Silver sources' max timestamps
- Deduplicates to prevent duplicates across source tables

---

#### `dim_time` (Time Dimension)
**Source**: Static (generated in code)  
**Grain**: One row per hour of day  
**Row Count**: 24 (fixed)

| Column | Type | Purpose |
|--------|------|---------|
| time_key | INTEGER (HHMM format: 0-2300) | Surrogate key |
| hour | INTEGER | 0-23 hour of day |
| time_of_day | STRING | Morning/Afternoon/Evening/Night |

**Business Rules**:
- Morning: 6-11
- Afternoon: 12-16
- Evening: 17-20
- Night: 21-5

**Load Characteristics**:
- Static, regenerated every load (24 rows)
- No dependencies on Silver data
- Used for hourly-grain fact table joins

---

#### `dim_facility` (Facility Dimension)
**Source**: `lh.silver.clean_facility_master`  
**Grain**: One row per facility (current version only)  
**Partitioning**: None (small reference table)

| Column | Type | Purpose |
|--------|------|---------|
| facility_key | LONG | Surrogate key |
| facility_code | STRING | Business key (e.g., "FAC001") |
| facility_name | STRING | Display name |
| location_lat | DECIMAL(10,7) | Latitude for mapping |
| location_lng | DECIMAL(10,7) | Longitude for mapping |
| total_capacity_mw | DECIMAL(10,2) | Installed capacity (MW) |
| total_capacity_registered_mw | DECIMAL(10,2) | Registered capacity |
| total_capacity_maximum_mw | DECIMAL(10,2) | Maximum possible capacity |
| effective_from | DATE | When facility became active |
| effective_to | DATE | When facility was retired (NULL if current) |
| is_current | BOOLEAN | Current version flag |
| created_at | TIMESTAMP | Audit: creation time |
| updated_at | TIMESTAMP | Audit: last update time |

**Load Characteristics**:
- **Slow-Changing Dimension (Type 1)**: Overwrites with latest version
- Incremental check: loads only if Silver `updated_at` > previous load
- Filters to `is_current=true` or `effective_to IS NULL`

---

#### `dim_aqi_category` (Air Quality Index Category Dimension)
**Source**: Static dimension (EPA/WHO standards)  
**Grain**: One row per AQI category level  
**Row Count**: 6 (fixed)

| Column | Type | Purpose |
|--------|------|---------|
| aqi_category_key | LONG | Surrogate key (1-6) |
| aqi_category | STRING | Category name |
| aqi_range_min | INTEGER | Minimum AQI value for category |
| aqi_range_max | INTEGER | Maximum AQI value for category |
| pm2_5_min_ug_m3 | DECIMAL(8,3) | PM2.5 minimum (µg/m³) |
| pm2_5_max_ug_m3 | DECIMAL(8,3) | PM2.5 maximum (µg/m³) |
| created_at | TIMESTAMP | Audit: creation time |

**AQI Standards** (EPA):
- **Good** (1): 0-50 AQI, 0-12.0 PM2.5
- **Moderate** (2): 51-100 AQI, 12.1-35.0 PM2.5
- **Unhealthy for Sensitive Groups** (3): 101-150 AQI, 35.1-55.0 PM2.5
- **Unhealthy** (4): 151-200 AQI, 55.1-150.0 PM2.5
- **Very Unhealthy** (5): 201-300 AQI, 150.1-250.0 PM2.5
- **Hazardous** (6): 301-500 AQI, 250.1-500.0 PM2.5

**Load Characteristics**:
- Static, regenerated every load
- No dependencies on Silver data
- Used for AQI value range-based categorization in fact tables

---

#### `dim_forecast_model_version` (Model Dimension)
**Source**: Static/manually-curated (future: MLflow tracking)  
**Grain**: One row per trained model version  
**Partitioning**: None (small reference table)

| Column | Type | Purpose |
|--------|------|---------|
| model_version_id | STRING | Unique model ID (e.g., "lr_baseline_v1") |
| model_name | STRING | Model class name |
| model_type | STRING | "classification", "regression", etc. |
| model_algorithm | STRING | Algorithm used (e.g., "LogisticRegression") |
| train_start_date | STRING | Training data start date |
| train_end_date | STRING | Training data end date |
| hyperparameters | STRING | JSON serialized hyperparameters |
| feature_columns | STRING | Comma-separated feature names |
| target_column | STRING | Target variable name |
| performance_metrics | STRING | JSON serialized metrics (AUC, accuracy, etc.) |
| is_active | BOOLEAN | Whether model is currently in use |
| model_description | STRING | Human-readable description |
| created_at | TIMESTAMP | Audit: creation time |
| updated_at | TIMESTAMP | Audit: last update time |

**Load Characteristics**:
- Static baseline version (can be extended with MLflow integration)
- Updated when new models are trained
- Current implementation hardcodes one baseline model

---

### Fact Tables

#### `fact_solar_environmental` (Solar Energy & Environmental Conditions)
**Sources**: 
- Energy: `lh.silver.clean_hourly_energy`
- Weather: `lh.silver.clean_hourly_weather`
- Air Quality: `lh.silver.clean_hourly_air_quality`
- Dimensions: `dim_facility`, `dim_date`, `dim_time`, `dim_aqi_category`

**Grain**: **1 row = 1 hour at 1 facility** (e.g., FAC001 on 2024-01-01 at 14:00)  
**Partitioning**: `date_key` (enables efficient range scans)  
**Row Count**: ~8.76M per year (24 hours × 365 days × ~1000 facilities)

| Column | Type | Purpose |
|--------|------|---------|
| **Keys** |
| facility_key | LONG | Foreign key to `dim_facility` |
| date_key | INTEGER (YYYYMMDD) | Foreign key to `dim_date` |
| time_key | INTEGER (HHMM) | Foreign key to `dim_time` |
| aqi_category_key | LONG | Foreign key to `dim_aqi_category` |
| **Energy Measures** |
| energy_mwh | DECIMAL(12,6) | Actual energy produced (MWh) |
| quality_flag | STRING | Data quality indicator |
| quality_issues | STRING | Comma-separated quality issues |
| completeness_pct | DECIMAL(5,2) | Completeness percentage (0-100) |
| intervals_count | INTEGER | Number of data intervals in hour |
| **Weather Measures** |
| shortwave_radiation | DECIMAL(10,2) | Solar irradiance (W/m²) |
| direct_radiation | DECIMAL(10,2) | Direct component (W/m²) |
| diffuse_radiation | DECIMAL(10,2) | Diffuse component (W/m²) |
| direct_normal_irradiance | DECIMAL(10,2) | DNI (W/m²) |
| temperature_2m | DECIMAL(5,2) | Air temperature (°C) |
| dew_point_2m | DECIMAL(5,2) | Dew point (°C) |
| cloud_cover | DECIMAL(5,1) | Cloud coverage (%) |
| cloud_cover_low | DECIMAL(5,1) | Low cloud coverage (%) |
| cloud_cover_mid | DECIMAL(5,1) | Mid-level cloud (%) |
| cloud_cover_high | DECIMAL(5,1) | High cloud coverage (%) |
| precipitation | DECIMAL(8,2) | Precipitation (mm) |
| sunshine_duration | INTEGER | Duration of sunshine (seconds) |
| wind_speed_10m | DECIMAL(6,2) | Wind speed at 10m (m/s) |
| wind_direction_10m | INTEGER | Wind direction (0-359 degrees) |
| wind_gusts_10m | DECIMAL(6,2) | Wind gust speed (m/s) |
| pressure_msl | DECIMAL(8,1) | Sea level pressure (hPa) |
| **Air Quality Measures** |
| pm2_5 | DECIMAL(8,3) | PM2.5 concentration (µg/m³) |
| pm10 | DECIMAL(8,3) | PM10 concentration (µg/m³) |
| dust | DECIMAL(8,3) | Dust concentration (µg/m³) |
| nitrogen_dioxide | DECIMAL(8,3) | NO2 concentration (ppb) |
| ozone | DECIMAL(8,3) | O3 concentration (ppb) |
| sulphur_dioxide | DECIMAL(8,3) | SO2 concentration (ppb) |
| carbon_monoxide | DECIMAL(8,3) | CO concentration (ppm) |
| uv_index | DECIMAL(5,2) | UV index (0-16+) |
| uv_index_clear_sky | DECIMAL(5,2) | UV index clear sky (0-16+) |
| aqi_value | DECIMAL(8,2) | Air Quality Index value (0-500+) |
| **Audit** |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Row last update time |

**Load Characteristics**:
- Incremental by `date_key` partition
- Join base (hourly_energy) with weather and air_quality on `facility_code`, `date_hour`
- Broadcast small dimensions (dim_facility, dim_date, dim_time) to avoid shuffles
- Validate required dimensions exist before loading
- Default partitions/broadcast thresholds:
  - Shuffle partitions: 8 (instead of default 200)
  - Broadcast threshold: 10MB for small dims

---

#### `fact_energy_forecast` (Energy Production Forecasts)
**Sources**: 
- Predictions: S3 Parquet from ML training pipeline
- Dimensions: `dim_facility`, `dim_date`, `dim_time`, `dim_forecast_model_version`

**Grain**: **1 row = 1 forecast hour for 1 facility at 1 model version**  
**Partitioning**: `date_key`  
**Row Count**: Depends on forecast horizon (e.g., 24-hour lookhead = same as fact_solar_environmental)

| Column | Type | Purpose |
|--------|------|---------|
| **Keys** |
| facility_key | LONG | Foreign key to `dim_facility` |
| date_key | INTEGER (YYYYMMDD) | Foreign key to `dim_date` |
| time_key | INTEGER (HHMM) | Foreign key to `dim_time` |
| model_version_id | STRING | Foreign key to `dim_forecast_model_version` |
| **Measures** |
| forecast_energy_mwh | DECIMAL(12,6) | Predicted energy (MWh) |
| forecast_score | DECIMAL(10,6) | Prediction score/confidence (0-1) |
| **Audit** |
| forecast_timestamp | TIMESTAMP | When forecast was generated |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Row last update time |

**Load Characteristics**:
- Reads pre-trained model predictions from S3 Parquet path
- Joins with dimension tables for star schema
- Partition pruning: only loads forecasts for new date ranges
- Validates all required dimensions are available

---

## Loading Process

### Execution Flow

```
┌────────────────────────────────────────┐
│  CLI Invocation                        │
│  python -m pv_lakehouse.etl.gold.cli  │
│    dim_facility --mode full            │
└────────────────────┬───────────────────┘
                     ▼
        ┌────────────────────────────┐
        │ 1. Parse Arguments & Options│
        │    - Dataset to load       │
        │    - Mode (full/incr)      │
        │    - Date range            │
        │    - Load strategy         │
        └────────────────┬───────────┘
                         ▼
        ┌────────────────────────────┐
        │ 2. Create Loader Instance  │
        │    Get from registry       │
        └────────────────┬───────────┘
                         ▼
        ┌────────────────────────────┐
        │ 3. Initialize Spark Session│
        │    Apply Gold configs      │
        │    (partitions, broadcast) │
        └────────────────┬───────────┘
                         ▼
        ┌────────────────────────────┐
        │ 4. Read Source Tables      │
        │    From Iceberg/Silver     │
        │    Apply filters (date)    │
        └────────────────┬───────────┘
                         ▼
        ┌────────────────────────────┐
        │ 5. Transform (dim/fact)    │
        │    User-defined logic      │
        │    Enrich, join, compute   │
        └────────────────┬───────────┘
                         ▼
        ┌────────────────────────────┐
        │ 6. Materialize Results     │
        │    Count rows, deduplicate │
        └────────────────┬───────────┘
                         ▼
        ┌────────────────────────────┐
        │ 7. Write to Gold Layer     │
        │    Via merge or overwrite  │
        │    Iceberg format          │
        └────────────────┬───────────┘
                         ▼
        ┌────────────────────────────┐
        │ 8. Return Status           │
        │    Total rows loaded       │
        └────────────────────────────┘
```

### Incremental Loading Strategy

The Gold layer employs **smart incremental loading**:

1. **Auto-detect Start Time**:
   - Check Gold table's `MAX(date_key)` → start from next day
   - If Gold table is empty: check Silver tables' `MAX(updated_at)` → start from next hour
   - Falls back to processing all data if no existing records

2. **Date Range Filtering**:
   - Apply `start` and `end` parameters to source table queries
   - Partition pruning for efficiency (only read relevant data)

3. **Load Strategy Modes**:
   - **merge** (default): Overwrite only affected partitions (faster, requires partitioning)
   - **overwrite**: Full table replacement (slower, used for dimensions)

4. **Deduplication**:
   - Critical for facts with multiple source tables (prevent cartesian products)
   - Ensure 1:1 relationship: `dropDuplicates(facility_code, date_hour)`

---

### Mode Comparison

| Aspect | Full | Incremental |
|--------|------|-------------|
| **Use Case** | Initial load, backfill, emergency refresh | Daily/hourly scheduled runs |
| **Data Processed** | All Silver data | Only new/changed records |
| **Duration** | Hours to days | Minutes to hours |
| **Storage Cost** | High (process all) | Low (process delta only) |
| **SQL** | No date filter | WHERE date_hour > {start_time} |
| **Idempotency** | Yes (overwrites) | Yes (partition overwrite) |

---

## Performance Optimization

### Spark Configuration for Gold Layer

```python
# Smaller shuffle partitions for smaller datasets
spark.sql.shuffle.partitions = 8  # (vs default 200)

# Enable broadcast for dimensions < 10MB
spark.sql.autoBroadcastJoinThreshold = 10485760  # 10MB

# Other configs
spark.sql.adaptive.enabled = true  # Adaptive query execution
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.shuffle.partitions.preferWindowOver.shuffle = true
```

### Join Strategy

**Dimension Tables** (small, static):
- Always broadcast to driver memory
- Avoids expensive shuffle joins
- Performance: 5-10x faster than shuffle joins

**Fact-to-Fact Joins**:
- Use partition-aware joins where possible
- Shuffle on foreign keys (date_key, facility_key)
- Minimize data movement via partition pruning

### Partitioning Strategy

| Table | Partition | Benefit |
|-------|-----------|---------|
| fact_solar_environmental | date_key | Enables partition pruning; typical query filters by date |
| fact_energy_forecast | date_key | Same; forecast queries typically bounded by date |
| dim_date | year, month | Optional; enables partition elimination on date lookups |
| dim_facility | (none) | Small table; no partitioning needed |
| dim_aqi_category | (none) | Static; typically filtered in memory |
| dim_forecast_model_version | (none) | Small; usually filtered to is_active=true |

---

## Data Quality & Governance

### Quality Checks

1. **Source Validation** (in `_read_sources`):
   - Required columns exist in source tables
   - Non-empty data (at least 1 row)

2. **Transform Validation**:
   - AQI category mapping: 100% of rows match a category
   - No `NULL` values in foreign keys (dimension keys)
   - Grain preservation: 1 energy row + 1 weather row + 1 AQI row = 1 fact row

3. **Write Validation**:
   - Row counts match or exceed source counts (no unexpected filtering)
   - Iceberg table structure matches schema

### Audit Columns

All Gold tables include:
- `created_at`: Timestamp when row was first inserted (set once)
- `updated_at`: Timestamp of most recent update (updated on each load)

**Purpose**: Enable change tracking and data lineage analysis

### Foreign Key Integrity

**Fact → Dimension Relationships**:
```
fact_solar_environmental
  → dim_facility (on facility_key)
  → dim_date (on date_key)
  → dim_time (on time_key)
  → dim_aqi_category (on aqi_category_key)

fact_energy_forecast
  → dim_facility (on facility_key)
  → dim_date (on date_key)
  → dim_time (on time_key)
  → dim_forecast_model_version (on model_version_id)
```

All foreign key references must have matching dimensions before fact loads.

---

## Running the Gold Layer

### CLI Usage

#### Load a single dataset (full refresh)
```bash
python -m pv_lakehouse.etl.gold.cli dim_facility --mode full
```

#### Load incrementally (default)
```bash
python -m pv_lakehouse.etl.gold.cli fact_solar_environmental
```

#### Specify date range
```bash
python -m pv_lakehouse.etl.gold.cli fact_solar_environmental \
  --mode incremental \
  --start 2024-01-01 \
  --end 2024-01-31
```

#### Using load strategy
```bash
python -m pv_lakehouse.etl.gold.cli fact_solar_environmental \
  --mode incremental \
  --load-strategy merge
```

#### Load all Gold tables in order (dimensions first, then facts)
```bash
# Dimensions (no dependencies)
python -m pv_lakehouse.etl.gold.cli dim_aqi_category --mode full
python -m pv_lakehouse.etl.gold.cli dim_date --mode full
python -m pv_lakehouse.etl.gold.cli dim_time --mode full

# Facility dimension (from Silver)
python -m pv_lakehouse.etl.gold.cli dim_facility --mode incremental

# Model dimension (static)
python -m pv_lakehouse.etl.gold.cli dim_forecast_model_version --mode full

# Facts (depend on all dimensions)
python -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode incremental
python -m pv_lakehouse.etl.gold.cli fact_energy_forecast --mode incremental
```

### Orchestration (Prefect, Airflow, etc.)

The Gold layer loaders integrate with orchestration platforms:

```python
# Pseudo-code: orchestration example
def gold_etl_dag():
    # Static dimensions
    load_dim_aqi = run_gold_loader("dim_aqi_category", mode="full")
    load_dim_time = run_gold_loader("dim_time", mode="full")
    load_dim_date = run_gold_loader("dim_date", mode="incremental")
    
    # Wait for all dimensions
    wait_for([load_dim_aqi, load_dim_time, load_dim_date])
    
    # Facility dimension
    load_dim_facility = run_gold_loader("dim_facility", mode="incremental")
    load_model_version = run_gold_loader("dim_forecast_model_version", mode="full")
    
    wait_for([load_dim_facility, load_model_version])
    
    # Facts (now all dimensions are ready)
    load_fact_env = run_gold_loader("fact_solar_environmental", mode="incremental")
    load_fact_forecast = run_gold_loader("fact_energy_forecast", mode="incremental")
    
    return [load_fact_env, load_fact_forecast]
```

---

## Developer Guide

### Adding a New Gold Table

1. **Create Loader Class**:
   ```python
   from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
   
   class GoldDimMyTableLoader(BaseGoldLoader):
       source_tables = {
           "source_name": SourceTableConfig(
               table_name="lh.silver.my_table",
               timestamp_column="updated_at",
               required_columns=["col1", "col2"]
           )
       }
       
       gold_tables = {
           "dim_my_table": GoldTableConfig(
               iceberg_table="lh.gold.dim_my_table",
               s3_base_path="s3a://lakehouse/gold/dim_my_table"
           )
       }
       
       def transform(self, sources):
           df = sources["source_name"]
           # Your transformation logic
           return {"dim_my_table": result}
   ```

2. **Register Loader** in `cli.py`:
   ```python
   _LOADER_REGISTRY["dim_my_table"] = GoldDimMyTableLoader
   ```

3. **Export Class** in `__init__.py`:
   ```python
   from .dim_my_table import GoldDimMyTableLoader
   __all__ = [..., "GoldDimMyTableLoader"]
   ```

### Key Base Class Methods

- **`spark`**: Get or create Spark session with Gold configs
- **`run()`**: Execute full ETL: read → transform → write → return count
- **`close()`**: Clean up Spark resources
- **`_read_sources()`**: Load source tables with auto-detected date filtering
- **`_write_outputs()`**: Write fact/dimension tables to Iceberg

### Common Helpers (in `common.py`)

- **`broadcast_small_dim()`**: Broadcast dimension to driver
- **`is_empty()`**: Check if DataFrame is null or has no rows
- **`compute_date_key()`**: Convert date to YYYYMMDD integer
- **`build_hourly_fact_base()`**: Add date_key and time_key to hourly data
- **`require_sources()`**: Validate all required sources are available
- **`classify_weather()`**: Categorize weather conditions
- **`classify_air_quality()`**: Categorize AQI values

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `ValueError: dim_facility must be available` | Dimension not loaded | Load dimensions first: `dim_facility --mode full` |
| Slow performance | Large shuffle partitions | Check `spark.sql.shuffle.partitions` is 8 |
| Memory errors | Broadcast too large | Increase driver memory or disable broadcasting |
| Duplicate rows in fact | Multiple source records per join key | Add `dropDuplicates()` in transform |
| Old data in incremental load | Start time not auto-detected | Verify Gold table has rows; check `date_key` format |

### Debugging

Enable detailed logging:
```bash
python -m pv_lakehouse.etl.gold.cli dim_facility \
  --mode incremental \
  2>&1 | tee gold_load.log
```

Check Spark logs:
```bash
tail -f spark-logs/executor.log
```

Query Gold table status:
```sql
SELECT COUNT(*), MAX(date_key), MAX(updated_at)
FROM lh.gold.dim_facility;
```

---

## Future Enhancements

1. **MLflow Integration**: Auto-populate `dim_forecast_model_version` from MLflow tracking
2. **Data Quality Framework**: Implement Great Expectations validation
3. **Slowly-Changing Dimensions**: Support SCD Type 2 (track history)
4. **Aggregation Mart**: Pre-aggregate to daily/monthly summaries
5. **Schema Versioning**: Track column additions/removals over time
6. **Metadata Catalog**: Integration with Apache Atlas or Collibra
7. **Performance Monitoring**: Automated query performance tracking

---

## References

- **Apache Iceberg**: https://iceberg.apache.org/
- **Apache Spark**: https://spark.apache.org/
- **Dimensional Modeling**: Ralph Kimball's Data Warehouse Toolkit
- **EPA AQI Standards**: https://www.epa.gov/outdoor-air-quality-data/air-quality-index-daily-values-report

