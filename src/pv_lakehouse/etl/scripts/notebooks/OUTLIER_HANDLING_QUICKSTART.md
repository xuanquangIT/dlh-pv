# 🚀 Outlier Handling in Silver Layer - QUICK START

## What Was Added?

✅ **Outlier detection** added to all 3 Silver loaders  
✅ **No code removed** (100% backward compatible)  
✅ **2-3 detection methods** per loader  
✅ **Flags only** (data retained, never deleted)  

---

## By The Numbers

| Aspect | Energy | Weather | Air Quality |
|--------|--------|---------|-------------|
| Detection Methods | 3 | 2 | 2 |
| New Output Columns | 6 | 4 | 4 |
| Code Added (lines) | 99 | 119 | 119 |
| Code Deleted | 0 | 0 | 0 |
| **Backward Compatible** | ✅ | ✅ | ✅ |

---

## New Columns in Silver Tables

### Energy (`lh.silver.clean_hourly_energy`)
```
has_outliers_zscore      : Boolean (Z-score detection)
outliers_zscore_reason   : String  (e.g., "Z-score=4.2(threshold=3.0)")
has_outliers_deviation   : Boolean (Pattern deviation detection)
outliers_deviation_pct   : Float   (e.g., 98.5 = 98.5% deviation)
has_outliers_iqr         : Boolean (IQR detection)
outliers_iqr_reason      : String  (e.g., "IQR:[25.3,48.7]")
```

### Weather (`lh.silver.clean_hourly_weather`)
```
has_outliers_zscore   : Boolean
outliers_zscore_cols  : String (e.g., "temperature_2m;cloud_cover")
has_outliers_iqr      : Boolean
outliers_iqr_cols     : String (e.g., "wind_speed_10m;pressure_msl")
```

### Air Quality (`lh.silver.clean_hourly_air_quality`)
```
has_outliers_zscore   : Boolean
outliers_zscore_cols  : String (e.g., "pm2_5;ozone")
has_outliers_iqr      : Boolean
outliers_iqr_cols     : String (e.g., "dust;nitrogen_dioxide")
```

---

## Detection Methods Explained (Simple)

### 1️⃣ Z-Score (Statistical Method)
**What**: Catches values WAY different from normal  
**How**: If value is >3× away from average = outlier  
**Sensitivity**: HIGH (strict)  
**Example**: Energy usually 50 MWh → today 5 MWh = outlier

### 2️⃣ IQR Method (Quartile Method)
**What**: Catches values outside normal range  
**How**: If value outside Q1-1.5×IQR to Q3+1.5×IQR = outlier  
**Sensitivity**: MEDIUM (balanced)  
**Example**: Energy range usually 30-70 MWh → today 2 MWh = outlier

### 3️⃣ Energy Deviation (Pattern Method - Energy Only)
**What**: Catches when facility generates way less than usual  
**How**: If energy <5% of typical hourly value = outlier  
**Sensitivity**: HIGH (for equipment issues)  
**Example**: 8 AM usually 50 MWh, today 0.5 MWh = 99% deviation = OUTLIER

---

## How to Query Outliers

### Find All Energy Outliers
```python
df = spark.table("lh.silver.clean_hourly_energy")
outliers = df.filter(df.has_outliers_zscore | df.has_outliers_iqr | df.has_outliers_deviation)
print(f"Found {outliers.count()} outlier records")
```

### See Outlier Details
```python
df.filter(df.has_outliers_zscore).select(
    "facility_code", "date_hour", "energy_mwh", 
    "outliers_zscore_reason"
).show()
```

### Which Facilities Have Most Outliers?
```python
df.filter(df.has_outliers_zscore | df.has_outliers_iqr).groupBy("facility_code").count().show()
```

### Weather Outliers by Type
```python
weather = spark.table("lh.silver.clean_hourly_weather")
weather.filter(weather.has_outliers_zscore).select(
    "facility_code", "date_hour", "outliers_zscore_cols"
).distinct().show(truncate=False)
```

---

## Configuration (If You Need to Adjust)

**In Python code, edit these values**:

```python
# In hourly_energy.py
_outlier_zscore_threshold = 3.0       # Change to 2.5 for more sensitive
_iqr_multiplier = 1.5                 # Standard, rarely changes
_outlier_energy_deviation_pct = 95.0  # Change to 80.0 for less sensitive

# In hourly_weather.py
_outlier_zscore_threshold = 3.0       # Same as above
_iqr_multiplier = 1.5

# In hourly_air_quality.py
_outlier_zscore_threshold = 3.0
_iqr_multiplier = 1.5
```

**Common Adjustments**:
- **More lenient** (fewer flags): Increase zscore to 3.5, increase IQR to 2.0
- **Stricter** (more flags): Decrease zscore to 2.5, decrease IQR to 1.0

---

## Key Facts

✅ **All data is KEPT** - no deletion  
✅ **Data passes basic validation** - only then checked for outliers  
✅ **3 methods run independently** - more comprehensive detection  
✅ **Results are additive** - if Z-score finds outlier, it's flagged (same for IQR)  
✅ **Reason columns** - explain WHY something was flagged  
✅ **Facility-specific** - thresholds adapt per facility  

---

## Example: Finding Energy Equipment Issues

```python
energy = spark.table("lh.silver.clean_hourly_energy")

# Find days with zero energy during sunny hours (likely equipment issue)
equipment_issues = energy.filter(
    (energy.has_outliers_deviation) &  # High deviation from pattern
    (energy.energy_mwh < 1)  # Near-zero generation
)

equipment_issues.select(
    "facility_code", "date_hour", "energy_mwh", "outliers_deviation_pct"
).show()

# Results: Likely indicate maintenance or equipment failure
```

---

## Example: Finding Weather Anomalies

```python
weather = spark.table("lh.silver.clean_hourly_weather")

# Find unusual weather patterns
unusual_weather = weather.filter(weather.has_outliers_zscore)

unusual_weather.select(
    "facility_code", "date_hour", "temperature_2m", "cloud_cover", 
    "outliers_zscore_cols"
).show(truncate=False)

# Results: Might explain unexpected energy drops
```

---

## Expected Outlier Frequency

**Normal rates** (based on statistical properties):

| Dataset | Z-Score | IQR | Both | Meaning |
|---------|---------|-----|------|---------|
| **Energy** | 0.3-1% | 2-5% | 2-6% | Small % is normal |
| **Weather** | 0.5-2% | 3-7% | 3-8% | Weather varies naturally |
| **Air Quality** | 0.2-0.5% | 1-3% | 1-3% | Usually stable |

**If you see >15% outliers**: Something might be wrong with data quality or sensor.

---

## Troubleshooting

### Too Many False Positives?
→ Increase `_outlier_zscore_threshold` to 3.5  
→ Increase `_iqr_multiplier` to 1.75

### Missing Real Outliers?
→ Decrease `_outlier_zscore_threshold` to 2.5  
→ Decrease `_iqr_multiplier` to 1.25

### Confused by Reason Strings?
Example: `"Z-score=4.2(threshold=3.0)"` means:
- **4.2** = How many standard deviations away
- **3.0** = Our threshold
- **Verdict**: 4.2 > 3.0 → Outlier ✗

Example: `"IQR:[25.3,48.7]"` means:
- **25.3** = Lower acceptable bound
- **48.7** = Upper acceptable bound
- **Verdict**: If your value is outside this range → Outlier ✗

---

## Files Modified

```
✅ silver/hourly_energy.py          (99 lines added, 0 deleted)
✅ silver/hourly_weather.py         (119 lines added, 0 deleted)
✅ silver/hourly_air_quality.py     (119 lines added, 0 deleted)
📄 OUTLIER_DETECTION_IMPLEMENTATION.md (full documentation)
📄 OUTLIER_HANDLING_QUICKSTART.md (this file)
```

---

## What About Downstream? (Gold Layer & Analytics)

✅ **No breaking changes** - your existing queries still work  
✅ **New columns optional** - only use if you care about outliers  
✅ **Full backward compatible** - can ignore outlier flags  

**But we recommend**:
1. Document where outlier flags are used
2. Create alerts for high outlier rates
3. Track operational issues using outlier patterns
4. Cross-reference with maintenance logs

---

## Summary

| Question | Answer |
|----------|--------|
| Will my data break? | ❌ No, fully compatible |
| Was code deleted? | ❌ No, 100% additions |
| Do I have to use outlier flags? | ❌ No, optional |
| Are outliers removed from data? | ❌ No, all data kept |
| Can I adjust sensitivity? | ✅ Yes, easy config |
| Do I get detailed info about outliers? | ✅ Yes, reason columns |

---

**Status**: ✅ Ready to Deploy  
**Date**: November 11, 2025  
**Compatibility**: 100% Backward Compatible
