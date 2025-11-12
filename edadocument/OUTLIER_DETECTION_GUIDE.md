# Silver Layer - Outlier Detection & Data Quality Guide

## Overview

This guide explains how to handle outlier data in the Silver layer transformation using the EDA analysis and quality validation utilities.

## Quick Start

### 1. Run the EDA Analysis

```bash
jupyter notebook src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb
```

This notebook provides:
- **Physical bounds validation** for each variable
- **Temporal anomaly detection** (null/future timestamps, duplicates)
- **Statistical outlier detection** (IQR, Z-score methods)
- **Diurnal pattern validation** (solar-specific - zero at night, peak at midday)
- **Data quality visualization** with charts and heatmaps
- **Quality flagging** combining all validation checks

### 2. Integrate Outlier Handler in Silver Loaders

The `outlier_handler.py` module provides utilities to implement quality checks in Silver layer transformations.

#### Example: Energy Loader with Quality Flags

```python
from pv_lakehouse.etl.silver.outlier_handler import (
    add_physical_bounds_flag,
    add_temporal_flags,
    add_diurnal_pattern_flags,
    add_composite_quality_flag,
    get_quality_summary,
)

# In SilverHourlyEnergyLoader.transform():
def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
    # ... existing transform logic ...
    
    result = prepared  # Your current transformation result
    
    # 1. Add physical bounds check
    result = add_physical_bounds_flag(result, bounds={'energy_mwh': (0.0, None)})
    
    # 2. Add temporal flags
    result = add_temporal_flags(result, timestamp_col='date_hour')
    
    # 3. Add diurnal pattern validation (solar-specific)
    result = add_diurnal_pattern_flags(
        result,
        timestamp_col='date_hour',
        energy_col='energy_mwh'
    )
    
    # 4. Add statistical outlier detection
    from pyspark.sql import Window
    result = add_statistical_outlier_flags(result, value_col='energy_mwh', method='tukey')
    
    # 5. Combine into composite quality flag
    result = add_composite_quality_flag(
        result,
        timestamp_col='date_hour',
        value_col='energy_mwh',
        include_diurnal=True
    )
    
    # 6. Get quality summary for logging
    summary = get_quality_summary(result)
    print(f"Quality Summary: {summary}")
    
    # 7. Optionally filter to keep only GOOD records for production
    # from pyspark.sql.outlier_handler import filter_by_quality
    # result = filter_by_quality(result, keep_good=True, keep_caution=False)
    
    return result
```

## Physical Bounds Reference

### Energy Variables
| Variable | Min | Max | Unit | Notes |
|----------|-----|-----|------|-------|
| energy_mwh | 0.0 | None | MWh | Must be non-negative |
| power_mw | 0.0 | None | MW | Must be non-negative |

### Weather Variables - Radiation
| Variable | Min | Max | Unit | Notes |
|----------|-----|-----|------|-------|
| shortwave_radiation | 0.0 | 1500.0 | W/m² | Solar radiation |
| direct_radiation | 0.0 | 1500.0 | W/m² | Direct component |
| diffuse_radiation | 0.0 | 1500.0 | W/m² | Diffuse component |
| direct_normal_irradiance | 0.0 | 1500.0 | W/m² | DNI for concentrated PV |

### Weather Variables - Temperature
| Variable | Min | Max | Unit | Notes |
|----------|-----|-----|------|-------|
| temperature_2m | -50.0 | 60.0 | °C | At 2m height |
| dew_point_2m | -50.0 | 60.0 | °C | Relative humidity indicator |
| wet_bulb_temperature_2m | -50.0 | 60.0 | °C | Heat stress indicator |

### Weather Variables - Other
| Variable | Min | Max | Unit | Notes |
|----------|-----|-----|------|-------|
| cloud_cover | 0.0 | 100.0 | % | Reduces irradiance |
| precipitation | 0.0 | 1000.0 | mm | Rare extreme values |
| pressure_msl | 800.0 | 1100.0 | hPa | Sea level pressure |
| wind_speed_10m | 0.0 | 60.0 | m/s | At 10m height |
| wind_direction_10m | 0.0 | 360.0 | degrees | 0=N, 90=E, 180=S, 270=W |

### Air Quality Variables
| Variable | Min | Max | Unit | Notes |
|----------|-----|-----|------|-------|
| pm2_5 | 0.0 | 500.0 | μg/m³ | Fine particulates |
| pm10 | 0.0 | 500.0 | μg/m³ | Coarse particulates |
| uv_index | 0.0 | 15.0 | Index | Extreme is >11 |

## Quality Flags Explained

### Flag Levels

1. **GOOD** (✅ Use in production)
   - Passes all physical bounds checks
   - Valid timestamps (not null, not in future)
   - No statistical outliers or diurnal anomalies

2. **CAUTION** (⚠️ Review & flag for analysis)
   - Passes bounds and timestamps
   - BUT has statistical anomalies or unusual patterns
   - Example: High PM2.5 during low-wind conditions (plausible but noteworthy)

3. **REJECT** (❌ Exclude from production)
   - Out-of-bounds values
   - Invalid/missing/future timestamps
   - Data integrity issues

### Quality Issues Codes

| Code | Meaning | Action |
|------|---------|--------|
| OUT_OF_BOUNDS | Value outside physical range | Reject |
| INVALID_TIMESTAMP | Null or future timestamp | Reject |
| STATISTICAL_OUTLIER | IQR or Z-score anomaly | Flag as CAUTION |
| NIGHT_HIGH_ENERGY | High generation at night (solar) | Flag as CAUTION |
| PEAK_LOW_ENERGY | Low generation at peak hours (solar) | Flag as CAUTION |
| DIURNAL_ANOMALY | Inverted or flat daily pattern | Flag as CAUTION |

## Diurnal Pattern Rules (Solar)

For solar PV facilities, energy generation should follow a predictable daily pattern:

```python
DIURNAL_RULES = {
    'night_hours': (22, 6),          # 22:00 - 06:00
    'peak_hours': (10, 15),          # 10:00 - 15:00
    'max_night_energy_mwh': 0.1,     # Very low at night
    'min_peak_energy_mwh': 1.0,      # Should be significant at peak
}
```

### Flagging Logic

```
Night hours (22:00-06:00):
  ├─ energy > 0.1 MWh? → CAUTION (night_high_energy)
  └─ Often indicates equipment malfunction or data error

Peak hours (10:00-15:00):
  ├─ energy < 1.0 MWh? → CAUTION (peak_low_energy)
  └─ May indicate cloud cover or system downtime
```

## Statistical Outlier Detection

### Tukey's IQR Method (Recommended)

```
Q1 = 25th percentile
Q3 = 75th percentile
IQR = Q3 - Q1
Lower fence = Q1 - 1.5 × IQR
Upper fence = Q3 + 1.5 × IQR

Outliers: values outside [Lower fence, Upper fence]
```

**Interpretation**: Catches ~0.7% of normal data as outliers
- Works well for solar energy (asymmetric daytime distribution)
- Resistant to extreme values

### Z-Score Method (3-sigma)

```
Z = |value - mean| / std_dev
Outliers: Z > 3.0 (99.7% confidence)
```

**Interpretation**: Catches values beyond 3 standard deviations
- Works for normally distributed data
- May miss solar patterns (non-normal distribution)

## Implementation Strategies

### Strategy 1: Strict (Exclude All Issues)

```python
# Keep only GOOD records
result_strict = filter_by_quality(
    df_with_flags,
    keep_good=True,
    keep_caution=False,
    keep_reject=False
)

# Result: ~85-95% of data retained
# Use for: Production metrics, financial reporting
```

### Strategy 2: Moderate (Flag But Keep)

```python
# Keep GOOD + CAUTION, flag reject
result_moderate = filter_by_quality(
    df_with_flags,
    keep_good=True,
    keep_caution=True,
    keep_reject=False
)

# Add note to CAUTION records
result_moderate = result_moderate.withColumn(
    "quality_note",
    F.when(
        F.col("quality_flag") == "CAUTION",
        "Review data quality"
    ).otherwise(F.lit(None))
)

# Result: ~95-99% of data retained
# Use for: Analysis, dashboards
```

### Strategy 3: Permissive (Keep All, Flag Issues)

```python
# Keep all records, let downstream decide
# Use for: Historical analysis, research

# Just ensure quality_flag and quality_issues columns exist
summary = get_quality_summary(df_with_flags)
print(f"Data quality report: {summary}")
```

## Implementing in Silver Layer

### Option A: Add Quality Columns Only (Recommended First Step)

In `SilverHourlyEnergyLoader.transform()`:

```python
def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
    # ... existing logic ...
    
    # Add all quality validations
    result = prepared
    result = add_physical_bounds_flag(result)
    result = add_temporal_flags(result, timestamp_col='date_hour')
    result = add_diurnal_pattern_flags(result, timestamp_col='date_hour', energy_col='energy_mwh')
    result = add_composite_quality_flag(result, timestamp_col='date_hour', value_col='energy_mwh')
    
    # Keep ALL records but include quality flags
    return result.select(
        "facility_code", "facility_name", "date_hour", "energy_mwh",
        "quality_flag", "quality_issues",
        "is_within_bounds", "valid_timestamp", "statistical_outlier",
        "night_high_energy", "peak_low_energy",
        # ... other columns ...
    )
```

**Advantages**:
- Trace data provenance
- Analyze quality trends over time
- Adjust filtering criteria later without re-running

### Option B: Filter in Transform

```python
def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
    # ... add quality flags ...
    result = add_composite_quality_flag(result, ...)
    
    # Exclude REJECT records, keep GOOD + CAUTION
    result = filter_by_quality(result, keep_good=True, keep_caution=True)
    
    # Clean up quality columns for Silver layer
    return result.drop("is_within_bounds", "valid_timestamp", "statistical_outlier")
```

## Monitoring & Alerting

### Track Quality Metrics

```python
from pv_lakehouse.etl.silver.outlier_handler import get_quality_summary

summary = get_quality_summary(result)

print(f"""
Quality Report:
  Total: {summary['total']:,}
  ✅ GOOD: {summary['GOOD']:,} ({summary['GOOD_pct']:.2f}%)
  ⚠️  CAUTION: {summary['CAUTION']:,} ({summary['CAUTION_pct']:.2f}%)
  ❌ REJECT: {summary['REJECT']:,} ({summary['REJECT_pct']:.2f}%)
""")

# Alert if rejection rate exceeds threshold
if summary['REJECT_pct'] > 5.0:
    print("⚠️ WARNING: Rejection rate > 5%")
```

### Track by Facility

```python
facility_summary = result.groupBy("facility_code", "quality_flag").count()
facility_summary.show()
```

## Examples of Data Quality Issues

### Example 1: Night Energy Generation

**Symptom**: High energy values at 22:00-06:00
**Root causes**:
- Equipment malfunction (inverter stuck on)
- Measurement error
- Data transmission error

**Action**: Flag as CAUTION or REJECT based on magnitude

### Example 2: Negative Energy

**Symptom**: Negative or extremely high values
**Root causes**:
- Sensor failure
- Data encoding error
- Grid feedback (exported energy)

**Action**: REJECT - outside physical bounds

### Example 3: Constant Zero for 24h

**Symptom**: Zero energy throughout entire day
**Root causes**:
- System downtime/maintenance
- Inverter failure
- Grid connection issue

**Action**: CAUTION - unusual pattern, may be valid maintenance window

### Example 4: Extreme Temperature

**Symptom**: Temperature > 60°C or < -50°C
**Root causes**:
- Sensor malfunction
- Data corruption
- Extreme weather (rare)

**Action**: CAUTION - statistical outlier, may be valid extreme weather

### Example 5: PM2.5 Spike During Wind

**Symptom**: High particulates during high wind speeds
**Root causes**:
- Sensor error (wind vibration)
- Extreme weather event
- Dust storm

**Action**: CAUTION - unusual but plausible

## Troubleshooting

### Q: Why is my quality rejection rate so high?

**A**: Check physical bounds:
- Are the min/max values appropriate for your region/equipment?
- Have you validated against known good data?
- Are timestamp columns in correct timezone?

### Q: How do I adjust quality thresholds?

**A**: Edit `PHYSICAL_BOUNDS` and `DIURNAL_RULES` in `outlier_handler.py`:

```python
# Example: Allow higher nighttime generation (biogas backup)
DIURNAL_RULES['max_night_energy_mwh'] = 5.0

# Example: Allow colder temperatures (alpine site)
PHYSICAL_BOUNDS['temperature_2m'] = (-60.0, 60.0)
```

### Q: Should I exclude CAUTION records?

**A**: Depends on use case:
- **Analytics/Research**: Keep them, flag for review
- **Financial Reporting**: Consider excluding them
- **Real-time Monitoring**: Flag but analyze separately

### Q: Can I use different thresholds per facility?

**A**: Yes, create facility-specific rules:

```python
FACILITY_BOUNDS = {
    'NYNGAN': {'energy_mwh': (0.0, 200.0)},
    'GANNSF': {'energy_mwh': (0.0, 100.0)},
}

# In transform():
facility_code = df.select('facility_code').first()[0]
bounds = FACILITY_BOUNDS.get(facility_code, PHYSICAL_BOUNDS)
result = add_physical_bounds_flag(result, bounds=bounds)
```

## References

- EDA Notebook: `src/pv_lakehouse/etl/scripts/notebooks/eda_outlier_detection.ipynb`
- Outlier Handler: `src/pv_lakehouse/etl/silver/outlier_handler.py`
- Tukey's Method: https://en.wikipedia.org/wiki/Outlier#Tukey's_fences
- Z-Score Method: https://en.wikipedia.org/wiki/Standard_score
- Solar PV Diurnal Patterns: https://pvoutput.org

---

**Last Updated**: 2025-11-12
**Version**: 1.0
