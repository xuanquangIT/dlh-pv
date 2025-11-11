# 🎯 Silver Layer Outlier Detection & Handling
## Implementation Documentation

**Date**: November 11, 2025  
**Status**: ✅ Complete (All 3 loaders updated)  
**Backward Compatibility**: ✅ 100% (No existing code removed)

---

## 📋 Overview

Outlier detection and handling được thêm vào **Silver Layer transformation** cho tất cả 3 loaders:
- `hourly_energy.py` - Energy generation data
- `hourly_weather.py` - Weather/climate data  
- `hourly_air_quality.py` - Air quality pollution data

**Goal**: Identify and flag anomalous values để:
1. Prevent data quality issues từ lan vào downstream analytics
2. Maintain data lineage (không xóa, chỉ flag)
3. Enable operational insights về equipment/data issues
4. Support root cause analysis

---

## 🔧 Implementation Details

### General Strategy

**Không xóa dữ liệu (Zero Deletion Policy)**:
- ✅ All data retained in Silver table
- ✅ Outliers flagged with detailed metadata
- ✅ Operators can decide action (keep, investigate, or remove)
- ✅ Full audit trail maintained

**Detection Methods**: 2 complementary approaches mỗi loader

#### Method 1: Z-Score Detection
- **Formula**: `Z = |value - mean| / stddev`
- **Threshold**: > 3 (3 sigma rule)
- **Sensitivity**: High (catches extreme deviations)
- **Use Case**: Detect genuinely anomalous measurements

#### Method 2: Interquartile Range (IQR) Method
- **Formula**: Outlier if value < Q1 - 1.5×IQR OR value > Q3 + 1.5×IQR
- **Sensitivity**: Medium (more robust to extreme values)
- **Use Case**: Detect unusual but plausible measurements

#### Method 3 (Energy only): Deviation from Typical Pattern
- **Formula**: `Deviation% = |value - median_hourly| / median_hourly × 100`
- **Threshold**: > 95% deviation from typical hour
- **Use Case**: Catch equipment malfunctions (zero/near-zero energy during sunny hours)

---

## 📊 Implementation per Loader

### 1. hourly_energy.py

**New Columns Added**:
```
has_outliers_zscore         : Boolean - Z-score outlier detected
outliers_zscore_reason      : String  - Z-score value & threshold
has_outliers_deviation      : Boolean - Deviation from pattern detected
outliers_deviation_pct      : Float   - Deviation percentage (0-100%)
has_outliers_iqr            : Boolean - IQR outlier detected
outliers_iqr_reason         : String  - IQR bounds [lower, upper]
```

**Detection Methods**:

1. **Z-Score Detection** (per facility + hour-of-day)
   ```
   Z = |energy_mwh - hourly_mean| / hourly_stddev
   Flag if Z > 3.0
   ```
   - Groups by: `facility_code` + `hour(date_hour)`
   - Reason: Catch hour-specific anomalies (e.g., 9 AM ramp-up anomalies)

2. **Deviation Pattern Detection** (per facility + hour-of-day)
   ```
   Deviation% = |energy - typical_hourly_energy| / typical_hourly_energy × 100
   Flag if Deviation% > 95%
   ```
   - Uses median as "typical" value
   - Reason: Catch partial/full facility shutdowns or equipment issues
   - Example: If 8 AM usually generates 50 MWh, but today shows 2 MWh = 96% deviation

3. **IQR Detection** (per facility + hour-of-day)
   ```
   Q1, Q3, IQR = percentiles at facility-hour level
   Lower = Q1 - 1.5 × IQR
   Upper = Q3 + 1.5 × IQR
   Flag if energy < Lower OR energy > Upper
   ```
   - More conservative than Z-score
   - Reason: Catch systematic equipment limits or realistic constraints

**Output Example**:
```
facility_code | date_hour | energy_mwh | has_outliers_zscore | outliers_zscore_reason | has_outliers_deviation | outliers_deviation_pct | has_outliers_iqr | outliers_iqr_reason
GANNSF        | 2024-10-22 08:00:00 | 0.5 | true | Z-score=4.2(threshold=3.0) | true | 98.5 | true | IQR:[25.3,48.7]
```

---

### 2. hourly_weather.py

**New Columns Added**:
```
has_outliers_zscore     : Boolean - Z-score outlier detected
outliers_zscore_cols    : String  - Comma-separated column names with outliers
has_outliers_iqr        : Boolean - IQR outlier detected  
outliers_iqr_cols       : String  - Comma-separated column names with outliers
```

**Detection Methods**:

1. **Z-Score Detection** (per facility + hour-of-day)
   ```
   Applied to all 17 numeric columns:
   - shortwave_radiation, direct_radiation, diffuse_radiation
   - temperature_2m, dew_point_2m, wet_bulb_temperature_2m
   - cloud_cover, cloud_cover_low/mid/high
   - precipitation, sunshine_duration, water_vapour
   - wind_speed_10m, wind_direction_10m, wind_gusts_10m, pressure_msl
   
   Z = |value - facility_hour_mean| / facility_hour_stddev
   Flag if Z > 3.0
   ```

2. **IQR Detection** (per facility)
   ```
   Applied to all 17 numeric columns
   Q1, Q3, IQR = percentiles at facility level
   Lower = Q1 - 1.5 × IQR
   Upper = Q3 + 1.5 × IQR
   Flag if value < Lower OR value > Upper
   ```

**Output Example**:
```
facility_code | date_hour | temperature_2m | cloud_cover | has_outliers_zscore | outliers_zscore_cols | has_outliers_iqr | outliers_iqr_cols
BNGSF1        | 2024-06-15 14:00:00 | 48.2 | 15 | true | temperature_2m;cloud_cover | false |
```

**Partition Strategy**: 
- Window partitioning by `facility_code` + `hour(date_hour)` for Z-score
- Facility-level for IQR (more stable bounds)
- 3-day chunks to manage memory during Spark processing

---

### 3. hourly_air_quality.py

**New Columns Added**:
```
has_outliers_zscore     : Boolean - Z-score outlier detected
outliers_zscore_cols    : String  - Comma-separated column names with outliers
has_outliers_iqr        : Boolean - IQR outlier detected
outliers_iqr_cols       : String  - Comma-separated column names with outliers
```

**Detection Methods**:

1. **Z-Score Detection** (per facility)
   ```
   Applied to all 9 numeric columns:
   - pm2_5, pm10, dust, nitrogen_dioxide, ozone
   - sulphur_dioxide, carbon_monoxide, uv_index, uv_index_clear_sky
   
   Z = |value - facility_mean| / facility_stddev
   Flag if Z > 3.0
   ```
   - Facility-level (not hour-specific) because pollution varies slowly

2. **IQR Detection** (per facility)
   ```
   Applied to all 9 numeric columns
   Q1, Q3, IQR = percentiles at facility level
   Lower = Q1 - 1.5 × IQR
   Upper = Q3 + 1.5 × IQR
   Flag if value < Lower OR value > Upper
   ```

**Output Example**:
```
facility_code | date_hour | pm2_5 | ozone | has_outliers_zscore | outliers_zscore_cols | has_outliers_iqr | outliers_iqr_cols
CLARESF       | 2024-10-12 10:00:00 | 42.5 | 65.3 | true | pm2_5 | false |
```

**Partition Strategy**: 
- Facility-level windows (3-day chunks for memory efficiency)
- AQI category calculation still applied before outlier detection

---

## 🎯 Threshold Configuration

### Default Thresholds (Configurable):

| Parameter | Value | Description | Rationale |
|-----------|-------|-------------|-----------|
| `_outlier_zscore_threshold` | 3.0 | Z-score threshold | 3-sigma rule catches ~0.3% extreme values |
| `_iqr_multiplier` | 1.5 | IQR multiplier | Standard statistical method |
| `_outlier_energy_deviation_pct` | 95% | Energy deviation threshold | Catches 95%+ underperformance (maintenance/shutdown) |

**To adjust thresholds** (in future):
```python
# In each loader class:
_outlier_zscore_threshold = 2.5  # More sensitive (2.5 sigma)
_iqr_multiplier = 2.0  # More conservative IQR
_outlier_energy_deviation_pct = 80.0  # Catch 80%+ deviations
```

---

## 📈 Expected Outlier Rates

**Based on Phase 1 Analysis**:

| Metric | Energy | Weather | Air Quality |
|--------|--------|---------|-------------|
| Expected Outlier Rate (Z-score) | 0.3-1% | 0.5-2% | 0.2-0.5% |
| Expected Outlier Rate (IQR) | 2-5% | 3-7% | 1-3% |
| Combined Detection Rate | 2-6% | 3-8% | 1-3% |

**Interpretation**:
- Energy: High variation due to equipment issues + weather = higher outlier %
- Weather: Natural variation in atmospheric conditions = moderate %
- Air Quality: Typically stable (good conditions in Vietnam 2024-25) = low %

---

## 🔄 Data Flow

```
Bronze Data
    ↓
[Read Bronze] 
    ↓
[Basic Validation] (Range checks)
    ↓
[Numeric Column Rounding] (4 decimals)
    ↓
[Z-Score Detection] → Add has_outliers_zscore column
    ↓
[IQR Detection] → Add has_outliers_iqr column
    ↓
(Energy only) [Deviation Pattern Detection] → Add has_outliers_deviation column
    ↓
[Drop Intermediate Calculation Columns]
    ↓
[Add Metadata] (created_at, updated_at, quality_flag)
    ↓
[Select & Rename Final Columns]
    ↓
[Write to Silver (Iceberg Table)]
    ↓
Silver Output with Outlier Flags
```

**Memory Optimization**:
- Intermediate calculation columns (mean, stddev, zscore, q1, q3, iqr, bounds) deleted after use
- Only final flag columns retained
- 3-day chunking prevents memory overflow on large datasets

---

## 📋 Code Changes Summary

### Files Modified (No Deletions)

1. **hourly_energy.py**
   - Added imports: `Window` from `pyspark.sql`
   - Added class attributes: 3 threshold parameters
   - Added methods:
     - `_detect_outliers_zscore()` - 39 lines
     - `_detect_outliers_deviation_pattern()` - 25 lines
     - `_detect_outliers_iqr()` - 35 lines
   - Modified `transform()`: Added 3 method calls (no code deletion)
   - Modified output columns: Added 6 new columns to SELECT

2. **hourly_weather.py**
   - Added imports: `Window` from `pyspark.sql`
   - Added class attributes: 2 threshold parameters
   - Added methods:
     - `_detect_outliers_zscore()` - 59 lines
     - `_detect_outliers_iqr()` - 60 lines
   - Modified `transform()`: Added 2 method calls (no code deletion)
   - Modified output columns: Added 4 new columns to SELECT

3. **hourly_air_quality.py**
   - Added imports: `Window` from `pyspark.sql`
   - Added class attributes: 2 threshold parameters
   - Added methods:
     - `_detect_outliers_zscore()` - 59 lines
     - `_detect_outliers_iqr()` - 60 lines
   - Modified `transform()`: Added 2 method calls (no code deletion)
   - Modified output columns: Added 4 new columns to SELECT

**Total New Code**: ~400 lines (all methods, no deletions)

---

## ✅ Backward Compatibility

**✅ 100% Compatible with Existing Code**:

1. **No code removed** - All existing logic preserved
2. **Only additions** - New detection methods called after existing validations
3. **New output columns** - Doesn't break existing column references
4. **Configurable** - Thresholds can be tuned without code changes
5. **Optional flags** - Downstream can ignore outlier columns if not needed

**Existing Features Still Work**:
- ✅ Numeric column rounding (4 decimals)
- ✅ Range validation (min/max checks)
- ✅ Quality flags (GOOD / OUT_OF_RANGE)
- ✅ Metadata timestamps (created_at, updated_at)
- ✅ Energy completeness % calculation
- ✅ AQI category calculation
- ✅ Negative energy flagging

---

## 🎯 Usage Example

### Reading Silver Tables with Outlier Flags

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("analytics").getOrCreate()

# Energy with outlier flags
energy_df = spark.table("lh.silver.clean_hourly_energy")
outliers = energy_df.filter(energy_df.has_outliers_zscore | energy_df.has_outliers_iqr)
print(f"Found {outliers.count()} energy records with outliers")

# Weather with outlier details
weather_df = spark.table("lh.silver.clean_hourly_weather")
weather_outliers = weather_df.filter(weather_df.has_outliers_zscore)
weather_outliers.select(
    "facility_code", "date_hour", "outliers_zscore_cols"
).show(truncate=False)

# Air quality with both detection methods
aq_df = spark.table("lh.silver.clean_hourly_air_quality")
aq_outliers = aq_df.filter(aq_df.has_outliers_iqr)
print(f"Air Quality IQR outliers in: {aq_outliers.select('outliers_iqr_cols').distinct().collect()}")
```

### Analysis Query: Facilities with Most Outliers

```python
# Find facilities with high outlier frequency
from pyspark.sql import functions as F

energy_df = spark.table("lh.silver.clean_hourly_energy")
outlier_summary = (
    energy_df
    .groupBy("facility_code")
    .agg(
        F.count(F.when(energy_df.has_outliers_zscore | energy_df.has_outliers_iqr, 1)).alias("outlier_count"),
        F.count("*").alias("total_count")
    )
    .withColumn("outlier_pct", (F.col("outlier_count") / F.col("total_count")) * 100)
    .sort(F.desc("outlier_pct"))
)
outlier_summary.show()
```

---

## 🔍 Monitoring & Alerts (Recommended)

**Suggested Alerts** (for operations team):

```python
# Alert 1: Unusually high outlier rate for facility
outlier_rate = outlier_count / total_records
if outlier_rate > 10%:  # Threshold
    alert("Facility {facility} has {outlier_rate}% outliers - potential data quality issue")

# Alert 2: Specific column outliers
if "energy_mwh" in outliers_zscore_cols:
    alert("Energy zscore outlier detected - possible equipment malfunction")

# Alert 3: Weather anomalies affecting energy
if "cloud_cover" in weather_outliers_zscore_cols:
    alert("Unusual cloud cover detected - may explain energy anomalies")
```

---

## 📚 Reference Tables

### Column Name Reference

**Energy Outlier Columns**:
- `has_outliers_zscore` - Z-score detection flag
- `outliers_zscore_reason` - "Z-score=4.2(threshold=3.0)"
- `has_outliers_deviation` - Deviation from pattern flag
- `outliers_deviation_pct` - Deviation percentage
- `has_outliers_iqr` - IQR detection flag
- `outliers_iqr_reason` - "IQR:[25.3,48.7]"

**Weather/AQ Outlier Columns**:
- `has_outliers_zscore` - Z-score detection flag
- `outliers_zscore_cols` - "temperature_2m;cloud_cover"
- `has_outliers_iqr` - IQR detection flag
- `outliers_iqr_cols` - "pm2_5;ozone"

---

## ✅ Validation Checklist

- [x] No existing code removed
- [x] All data retained (zero deletion policy)
- [x] Window partitioning correctly specified
- [x] Intermediate columns dropped (memory efficient)
- [x] Outlier flags properly named and typed
- [x] Descriptive reason/cols columns added
- [x] Methods follow existing code style
- [x] Comments added for clarity
- [x] Compatible with 3-day chunking strategy
- [x] Backward compatible with existing columns

---

## 🎯 Next Steps

### For Operators:
1. Deploy updated loaders to silver zone
2. Monitor outlier rates for baseline establishment
3. Create alerts for unusual outlier spikes
4. Use outlier flags for operational insights

### For Data Scientists:
1. Analyze flagged outliers for patterns
2. Adjust thresholds based on operational feedback
3. Integrate outlier insights into forecasting models
4. Cross-reference with maintenance logs

### For Future Enhancement:
1. Add seasonal/temporal adjustments to thresholds
2. Implement facility-specific threshold tuning
3. Add machine learning-based anomaly detection
4. Create outlier dashboard for visualization

---

## 📞 Support & Questions

**Configuration Questions**:
- How to adjust outlier thresholds? → Modify class attributes
- How to disable specific detection? → Comment out method call in `transform()`
- How to export outlier statistics? → Query Silver tables and aggregate

**Operational Questions**:
- What should I do with flagged outliers? → Use as investigation leads
- Can I trust data with outlier flags? → Yes, it passed range validation
- Should I remove outlier records? → No, keep for audit trail; flag for action

---

*Implementation Complete: November 11, 2025*  
*All 3 Silver Loaders Updated*  
*Zero Code Deletion, 100% Backward Compatible*
