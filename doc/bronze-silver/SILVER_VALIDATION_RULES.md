# Silver Layer Validation Rules - Quick Reference

## WEATHER DATA VALIDATION

### Numeric Bounds (Hard Limits - REJECT if violated)

| Column | Min | Max | Unit | Reason |
|--------|-----|-----|------|--------|
| shortwave_radiation | 0.0 | 1150.0 | W/m² | P99.5=1045, Australian extreme |
| direct_radiation | 0.0 | 1050.0 | W/m² | Australian extreme events |
| diffuse_radiation | 0.0 | 520.0 | W/m² | Measurement variation |
| direct_normal_irradiance | 0.0 | 1060.0 | W/m² | Extreme events max |
| temperature_2m | -10.0 | 50.0 | °C | P99.5=38.5°C, actual max=43.7°C |
| dew_point_2m | -20.0 | 30.0 | °C | Expanded for extreme conditions |
| wet_bulb_temperature_2m | -5.0 | 40.0 | °C | Bounded by air temperature |
| cloud_cover | 0.0 | 100.0 | % | Physical percentage bounds |
| cloud_cover_low | 0.0 | 100.0 | % | Physical percentage bounds |
| cloud_cover_mid | 0.0 | 100.0 | % | Physical percentage bounds |
| cloud_cover_high | 0.0 | 100.0 | % | Physical percentage bounds |
| precipitation | 0.0 | 1000.0 | mm | Extreme event bound |
| sunshine_duration | 0.0 | 3600.0 | seconds | 1 hour max per hourly period |
| total_column_integrated_water_vapour | 0.0 | 100.0 | kg/m² | Typical atmospheric bound |
| wind_speed_10m | 0.0 | 50.0 | m/s | Australian cyclones max=47.2 |
| wind_direction_10m | 0.0 | 360.0 | degrees | Perfect bounds |
| wind_gusts_10m | 0.0 | 120.0 | m/s | Extreme weather bound |
| pressure_msl | 985.0 | 1050.0 | hPa | P99=1033 hPa |

### Logical Checks (Soft Limits - CAUTION if violated)

#### 1. Night-time Radiation Spike
```python
is_night = (hour >= 22) | (hour < 6)
if is_night and shortwave_radiation > 100:
    flag = "CAUTION"  # Physical impossibility
    issue = "NIGHT_RADIATION_SPIKE"
```

#### 2. Radiation Consistency
```python
if (direct_radiation + diffuse_radiation) > (shortwave_radiation * 1.05):
    flag = "CAUTION"
    issue = "RADIATION_INCONSISTENCY"
    reason = "Direct + Diffuse should ≤ Shortwave by physics"
```

#### 3. Cloud Cover & Radiation Mismatch
```python
is_peak_sun = (hour >= 10) & (hour <= 14)
high_cloud = cloud_cover > 98  # Very high, not just 95%
low_radiation = shortwave_radiation < 600

if is_peak_sun and high_cloud and low_radiation:
    flag = "CAUTION"
    issue = "CLOUD_MEASUREMENT_INCONSISTENCY"
    reason = "98% cloud should have minimal radiation"
```
**Why 98% not 95%?** 95% cloud still allows ~5% direct light.
Reduces false positives by 95% compared to 95% threshold.

#### 4. Extreme Temperature
```python
if temperature_2m < -10 or temperature_2m > 45:
    flag = "CAUTION"
    issue = "EXTREME_TEMPERATURE"
```

### Quality Flag Rules

```
IF any column OUT OF BOUNDS:
    → quality_flag = "REJECT"
    
ELSE IF night_radiation_spike OR radiation_inconsistency:
    → quality_flag = "REJECT"
    
ELSE IF radiation_inconsistency OR high_cloud_peak OR extreme_temp:
    → quality_flag = "CAUTION"
    
ELSE:
    → quality_flag = "GOOD"
```

---

## ENERGY TIMESERIES VALIDATION

### Hard Physical Bounds

| Metric | Min | Max | Reason |
|--------|-----|-----|--------|
| energy_mwh | 0.0 | ∞ | Non-negative (REJECT if negative) |

### Logical Checks (Soft Limits - CAUTION if violated)

#### 1. Night-time Energy Anomaly
```python
is_night = (hour >= 22) | (hour < 6)
if is_night and energy_mwh > 1.0:
    flag = "CAUTION"
    issue = "NIGHT_ENERGY_ANOMALY"
    reason = "Solar cannot generate at night"
```

#### 2. Daytime Zero Energy
```python
is_daytime = (hour >= 8) & (hour <= 17)
if is_daytime and energy_mwh == 0.0:
    flag = "CAUTION"
    issue = "DAYTIME_ZERO_ENERGY"
    reason = "Should have generation during day"
```

#### 3. Peak Hour Equipment Downtime
```python
is_peak = (hour >= 10) & (hour <= 14)
if is_peak and energy_mwh == 0.0:
    flag = "CAUTION"
    issue = "EQUIPMENT_DOWNTIME"
    reason = "Zero during peak = failure"
```

#### 4. Transition Hour Low Energy (formerly RAMP_ANOMALY)
```python
PEAK_REFERENCE_MWH = 85.0

is_sunrise = (hour >= 6) & (hour < 8)
is_early_morning = (hour >= 8) & (hour < 10)
is_sunset = (hour >= 17) & (hour < 19)

# Energy should ramp gradually during transitions
thresholds = {
    'sunrise': 0.05 * PEAK_REFERENCE_MWH,        # 4.25 MWh
    'early_morning': 0.08 * PEAK_REFERENCE_MWH, # 6.8 MWh
    'sunset': 0.10 * PEAK_REFERENCE_MWH,        # 8.5 MWh
}

if is_transition and 0.01 < energy_mwh < threshold:
    flag = "CAUTION"
    issue = "TRANSITION_HOUR_LOW_ENERGY"
    reason = "Sudden drop suggests measurement issue"
```

#### 5. Peak Hour Low Energy (Efficiency Issue)
```python
is_peak = (hour >= 10) & (hour <= 14)
peak_average = 85.0  # Reference from analysis

if is_peak and 0.5 < energy_mwh < (peak_average * 0.50):
    flag = "CAUTION"
    issue = "PEAK_HOUR_LOW_ENERGY"
    reason = "Efficiency issue or partial failure"
```

### Quality Flag Rules

```
IF energy_mwh < 0:
    → quality_flag = "REJECT"
    
ELSE IF night_anomaly OR daytime_zero OR equipment_downtime 
        OR transition_low OR efficiency_anomaly:
    → quality_flag = "CAUTION"
    
ELSE:
    → quality_flag = "GOOD"
```

---

## AIR QUALITY DATA VALIDATION

### Numeric Bounds (Hard Limits)

| Column | Min | Max | Unit | Reason |
|--------|-----|-----|------|--------|
| pm2_5 | 0.0 | 500.0 | µg/m³ | Physical range |
| pm10 | 0.0 | 500.0 | µg/m³ | Physical range |
| dust | 0.0 | 500.0 | µg/m³ | Physical range |
| nitrogen_dioxide | 0.0 | 500.0 | µg/m³ | Physical range |
| ozone | 0.0 | 500.0 | µg/m³ | Physical range |
| sulphur_dioxide | 0.0 | 500.0 | µg/m³ | Physical range |
| carbon_monoxide | 0.0 | 500.0 | µg/m³ | Physical range |
| uv_index | 0.0 | 15.0 | - | Physical scale |
| uv_index_clear_sky | 0.0 | 15.0 | - | Physical scale |

### AQI Calculation (from PM2.5)

```python
def calculate_aqi(pm25):
    if pm25 <= 12.0:
        return scale(pm25, 0.0, 12.0, 0, 50)           # GOOD
    elif pm25 <= 35.4:
        return scale(pm25, 12.1, 35.4, 51, 100)        # MODERATE
    elif pm25 <= 55.4:
        return scale(pm25, 35.5, 55.4, 101, 150)       # UNHEALTHY
    elif pm25 <= 150.4:
        return scale(pm25, 55.5, 150.4, 151, 200)      # UNHEALTHY (sensitive)
    elif pm25 <= 250.4:
        return scale(pm25, 150.5, 250.4, 201, 300)     # VERY UNHEALTHY
    else:
        return scale(min(pm25, 500), 250.5, 500, 301, 500)  # HAZARDOUS
```

### Quality Flag Rules

```
IF any column OUT OF BOUNDS:
    → quality_flag = "CAUTION"
    
ELSE IF AQI invalid:
    → quality_flag = "CAUTION"
    
ELSE:
    → quality_flag = "GOOD"
```

---

## TRANSFORMATION PIPELINE

### Step 1: Type Casting
```python
.select(
    F.col("value").cast("double").alias("metric_value"),
    F.col("timestamp").cast("timestamp").alias("timestamp_ts")
)
```

### Step 2: Handle NaN/NULL
```python
# Replace NaN with NULL using Spark (distributed processing)
.withColumn(
    "column_name",
    F.when(F.isnan(F.col("column_name")), F.lit(None))
     .otherwise(F.col("column_name"))
)

# Forward-fill within groups
window = Window.partitionBy("facility_code", "date").orderBy("timestamp")
.withColumn(
    "column_name",
    F.coalesce(
        F.col("column_name"),
        F.last(F.col("column_name"), ignorenulls=True).over(window)
    )
)
```

### Step 3: Round to 4 Decimals
```python
.select(
    F.round(F.col("column"), 4).alias("column")
)
```

### Step 4: Timezone Conversion (if needed)
```python
# For energy data (UTC → local)
.withColumn(
    "timestamp_local",
    F.from_utc_timestamp(F.col("timestamp_utc"), facility_tz)
)
```

### Step 5: Temporal Aggregation
```python
.withColumn(
    "date_hour",
    F.date_trunc("hour", F.col("timestamp_local"))
)
.groupBy("facility_code", "date_hour")
.agg(
    F.sum(F.when(...)).alias("metric_sum"),
    F.count(...).alias("record_count")
)
```

### Step 6: Apply Bounds & Quality Flags
```python
# Compute bounds checks
is_valid = (column >= min_bound) & (column <= max_bound)

# Build quality issues string
.withColumn(
    "quality_issues",
    F.concat_ws("|",
        F.when(~is_valid, F.lit("OUT_OF_BOUNDS")),
        F.when(logical_issue1, F.lit("ISSUE_1")),
        F.when(logical_issue2, F.lit("ISSUE_2"))
    )
)

# Assign quality flag
.withColumn(
    "quality_flag",
    F.when(hard_violation, F.lit("REJECT"))
     .when(soft_violation, F.lit("CAUTION"))
     .otherwise(F.lit("GOOD"))
)
```

---

## Decision Matrix

### When to use HARD BOUNDS (REJECT)
- Physical impossibility: negative energy, negative radiation
- Cannot be measurement error: impossible combinations

### When to use SOFT CHECKS (CAUTION)
- Possible measurement error but plausible
- Logical inconsistencies (needs investigation)
- Extreme but real events (heat waves, droughts)

### When to allow records through as GOOD
- Within bounds AND passes logical checks
- Conservative approach: let models decide

---

## Performance & Scalability

### Processing Strategy
- **Chunking**: Process in 3-7 day chunks to limit memory
- **Partitioning**: Store by date_hour for efficient queries
- **File size**: 8-128 MB per partition (tunable)
- **Deduplication**: Keep latest record per (facility, timestamp, metric)

### Quality Flag Distribution Expected
- **GOOD**: 85-98% (depends on source quality)
- **CAUTION**: 1-15% (anomalies for investigation)
- **REJECT**: < 0.1% (rare hard violations)

This distribution is desired - too many CAUTION flags = loose bounds,
too few = overly strict bounds.
