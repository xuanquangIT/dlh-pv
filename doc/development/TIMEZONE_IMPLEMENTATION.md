# Timezone Implementation - Bronze & Silver Layer

**Date:** November 11, 2025  
**Status:** Implemented  
**Scope:** Bronze & Silver layers only (NOT Gold)

## Overview

Mỗi trạm solar được set lại theo múi giờ của chính cái trạm đó đang ở. Timestamps được convert từ UTC sang local time của từng trạm để phù hợp với khái niệm "business time" (giờ địa phương nơi trạm ở).

## Network Timezone Mapping

| Network | Timezone ID | UTC Offset | Region |
|---------|-------------|------------|--------|
| NEM | Australia/Brisbane | UTC+10/+11 | Eastern Australia |
| WEM | Australia/Perth | UTC+8 | Western Australia |

## Changes Made

### 1. Bronze Layer - Facilities Metadata

**File:** `src/pv_lakehouse/etl/bronze/load_facilities.py`

- ✅ Added `timezone_id` column to `raw_facilities` table
- Maps `network_id` to IANA timezone ID (NEM → Australia/Brisbane, WEM → Australia/Perth)
- Timezone is determined at ingestion time and persisted in Bronze

```python
NETWORK_TIMEZONE_MAPPING = {
    "NEM": "Australia/Brisbane",
    "WEM": "Australia/Perth",
}

# In transform:
spark_df = spark_df.withColumn(
    "timezone_id",
    F.when(F.col("network_id") == "NEM", F.lit("Australia/Brisbane"))
     .when(F.col("network_id") == "WEM", F.lit("Australia/Perth"))
     .otherwise(F.lit("Australia/Brisbane"))
)
```

### 2. Bronze Layer - Timeseries (Energy Data)

**File:** `src/pv_lakehouse/etl/bronze/load_facility_timeseries.py`

- ✅ Timestamps converted from UTC → local facility time
- Join with `raw_facilities` to get timezone for each facility
- Use `F.from_utc_timestamp()` to convert

```python
# Join with facility timezone
facilities_tz = spark.sql("""
    SELECT facility_code, timezone_id FROM lh.bronze.raw_facilities
""")
spark_df = spark_df.join(facilities_tz, on="facility_code", how="left")

# Convert UTC → local time
spark_df = spark_df.withColumn(
    "interval_ts_local",
    F.from_utc_timestamp(
        F.col("interval_ts"),
        F.coalesce(F.col("timezone_id"), F.lit("Australia/Brisbane"))
    )
)

# Replace timestamp
spark_df = spark_df.drop("interval_ts").withColumnRenamed("interval_ts_local", "interval_ts")
```

### 3. Bronze Layer - Weather Data

**File:** `src/pv_lakehouse/etl/bronze/load_facility_weather.py`

- ✅ Timestamps converted from UTC → local facility time
- Same approach as timeseries: join + convert + replace

```python
# Join with facility timezone
facilities_tz = spark.sql("SELECT facility_code, timezone_id FROM lh.bronze.raw_facilities")
weather_spark_df = weather_spark_df.join(facilities_tz, on="facility_code", how="left")

# Convert & replace weather_timestamp with local time
```

### 4. Bronze Layer - Air Quality Data

**File:** `src/pv_lakehouse/etl/bronze/load_facility_air_quality.py`

- ✅ Timestamps converted from UTC → local facility time
- Same approach as timeseries and weather

```python
# Join with facility timezone
facilities_tz = spark.sql("SELECT facility_code, timezone_id FROM lh.bronze.raw_facilities")
air_spark_df = air_spark_df.join(facilities_tz, on="facility_code", how="left")

# Convert & replace air_timestamp with local time
```

### 5. Silver Layer - Facility Master

**File:** `src/pv_lakehouse/etl/silver/facility_master.py`

- ✅ Added `timezone_id` to output columns
- Inherits timezone from Bronze `raw_facilities`
- Propagated to all dependent tables for reference

```python
_output_columns = [
    "facility_code",
    "facility_name",
    "network_id",
    "network_region",
    "timezone_id",  # ← NEW
    # ... rest of columns
]
```

### 6. Silver Layer - Timeseries, Weather, Air Quality

**Files:**
- `src/pv_lakehouse/etl/silver/hourly_energy.py`
- `src/pv_lakehouse/etl/silver/hourly_weather.py`
- `src/pv_lakehouse/etl/silver/hourly_air_quality.py`

- ✅ **NO CHANGES NEEDED** - Already use converted timestamps from Bronze
- `date_trunc("hour", interval_ts)` uses local time automatically
- `to_date(weather_timestamp)` uses local date automatically

Since Bronze layer timestamps are already in local time, Silver layer aggregations work correctly:
- Hour 0-23 represents local hours (not UTC)
- Date boundaries align with facility's local midnight

## Data Flow Example

**Scenario:** Brisbane facility (NEM, UTC+10) on 2025-10-01

```
┌─────────────────────────────────────────────────────────┐
│ Raw API Data (UTC)                                      │
│ interval_ts: 2025-09-30 14:00:00 UTC                  │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ Bronze Timeseries (After Convert)                       │
│ interval_ts: 2025-10-01 00:00:00 +10:00 (local)       │
│ timezone_id: Australia/Brisbane                         │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ Silver Hourly Energy                                    │
│ date_hour: 2025-10-01 00:00:00 (local midnight hour)  │
│ hour: 0 (local midnight)                               │
│ date: 2025-10-01 (local date)                          │
└─────────────────────────────────────────────────────────┘
```

## Verification & Testing

### Check timezone was added to Bronze facilities:
```sql
SELECT facility_code, network_id, timezone_id 
FROM lh.bronze.raw_facilities 
LIMIT 5;
```

Expected output:
```
facility_code  | network_id | timezone_id
---------------|------------|---------------------
AGLHAL | NEM | Australia/Brisbane
BRKWAL | NEM | Australia/Brisbane
CWFPER | WEM | Australia/Perth
```

### Check timestamps were converted in Bronze timeseries:
```sql
-- Sample UTC timestamp from API (before) vs Bronze local timestamp
-- The hour should match local hour (0-23 for local day)
SELECT 
  facility_code,
  interval_ts,
  HOUR(interval_ts) as hour_local,
  DAYOFWEEK(interval_ts) as day_local
FROM lh.bronze.raw_facility_timeseries 
WHERE facility_code = 'AGLHAL'
LIMIT 10;
```

### Check Silver uses local dates:
```sql
-- Date-hour aggregation should use local dates
SELECT 
  facility_code,
  date_hour,
  EXTRACT(HOUR FROM date_hour) as hour_local,
  COUNT(*) as records
FROM lh.silver.clean_hourly_energy
WHERE facility_code = 'AGLHAL'
GROUP BY facility_code, date_hour
LIMIT 10;
```

## Handling Daylight Saving Time (DST)

Spark's `from_utc_timestamp()` function automatically handles DST transitions:
- When DST starts/ends in a timezone, the conversion correctly applies
- No manual DST handling required
- Works for both Australia/Brisbane and Australia/Perth

**Note:** Western Australia (WEM) does NOT observe DST, so timezone is always UTC+8.

## Backward Compatibility

- **New columns added:** `timezone_id` (Bronze facilities, Silver facility_master)
- **Existing column behavior:** Timestamp columns (`interval_ts`, `weather_timestamp`, `air_timestamp`) now contain local time instead of UTC
- **Silver layer consumers:** Must be aware that:
  - `date_hour` now represents local hours, not UTC hours
  - Date boundaries align with facility's local midnight
  - All aggregations are now in local time

## Migration Notes

If backfilling data:

1. Run Bronze loaders FIRST:
   ```bash
   # Loads facilities with timezone_id
   bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facilities.py --mode backfill
   
   # Timeseries with UTC → local conversion
   bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_timeseries.py --mode backfill
   
   # Weather with UTC → local conversion
   bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_weather.py --mode backfill
   
   # Air Quality with UTC → local conversion
   bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_air_quality.py --mode backfill
   ```

2. Then run Silver loaders:
   ```bash
   bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py facility_master --mode full
   bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode full
   bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_weather --mode full
   bash src/pv_lakehouse/etl/scripts/spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_air_quality --mode full
   ```

## Implementation Details

### Timezone Conversion Function

From `pv_lakehouse.etl.clients.openelectricity`:

```python
def get_timezone_offset_hours(network_id: str) -> int:
    """Get timezone offset in hours for a network."""
    return NETWORK_FALLBACK_OFFSETS.get(network_id, 10)

def get_timezone_id(network_id: str) -> str:
    """Get IANA timezone ID for a network."""
    return NETWORK_TIMEZONE_IDS.get(network_id, "Australia/Brisbane")
```

### Spark Conversion

Using Spark SQL function `from_utc_timestamp()`:

```python
F.from_utc_timestamp(
    utc_timestamp_column,
    timezone_id_column  # "Australia/Brisbane" or "Australia/Perth"
)
```

This function:
- Takes UTC timestamp
- Converts to specified timezone
- Handles DST automatically
- Returns local timestamp (without timezone info)

## Related Files

- Bronze Facilities: `src/pv_lakehouse/etl/bronze/load_facilities.py`
- Bronze Timeseries: `src/pv_lakehouse/etl/bronze/load_facility_timeseries.py`
- Bronze Weather: `src/pv_lakehouse/etl/bronze/load_facility_weather.py`
- Bronze Air Quality: `src/pv_lakehouse/etl/bronze/load_facility_air_quality.py`
- Silver Facility Master: `src/pv_lakehouse/etl/silver/facility_master.py`
- Silver Timeseries: `src/pv_lakehouse/etl/silver/hourly_energy.py`
- Silver Weather: `src/pv_lakehouse/etl/silver/hourly_weather.py`
- Silver Air Quality: `src/pv_lakehouse/etl/silver/hourly_air_quality.py`

## Testing Checklist

- [ ] Load facilities with `load_facilities.py` - should populate `timezone_id`
- [ ] Load timeseries with `load_facility_timeseries.py` - check timestamps are local
- [ ] Load weather with `load_facility_weather.py` - check timestamps are local
- [ ] Load air quality with `load_facility_air_quality.py` - check timestamps are local
- [ ] Load Silver layers - verify date_hour uses local dates
- [ ] Run heatmap notebook - verify hours align with local time (0-23 for day)
- [ ] Test DST transitions (if applicable)
