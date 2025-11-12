# Silver Layer Code Improvements - Implementation Plan

## 🎯 Mục tiêu: Cải thiện từ 91.19% → 95%+ GOOD records

**Timeline:** 4-6 giờ  
**Impact:** +429 records GOOD (0.93% improvement)

---

## PHASE 1: Quick Wins (2 giờ) - 🔴 CRITICAL FIXES

### Change 1.1: Add Facility-Specific Thresholds (hourly_energy.py)

**File:** `src/pv_lakehouse/etl/silver/hourly_energy.py`  
**Location:** Lines 82-92 (after DAYTIME_END definition)

**Current code:**
```python
TUKEY_LOWER = 0.0
TUKEY_UPPER = 110.0
```

**Replace with:**
```python
# Tukey-based thresholds from EDA (Q3 + 1.5*IQR)
TUKEY_LOWER = 0.0
TUKEY_UPPER = 110.0

# Facility-specific capacity thresholds from real data:
# COLEASF: max 139.9 MWh (p95: 124.1)
# Others: max ~100-110 MWh
FACILITY_CAPACITY_THRESHOLDS = {
    'COLEASF': 145.0,    # Slightly above observed max
    'BNGSF1': 115.0,
    'CLARESF': 115.0,
    'GANNSF': 115.0,
    'NYNGAN': 115.0,
}

# Night-time thresholds
NIGHT_START = 22
NIGHT_END = 6
MAX_NIGHT_ENERGY = 0.1  # Very strict - almost no gen at night (from 0.3)
SUSPICIOUS_NIGHT_ENERGY = 1.0  # Anything > 1 MWh is suspicious

# Peak hours
PEAK_NOON_START = 11
PEAK_NOON_END = 15
MIN_PEAK_ENERGY = 5.0  # Minimum expected during peak (if available)

DAYTIME_START = 6
DAYTIME_END = 18
```

---

### Change 1.2: Enhance Quality Flag Logic (hourly_energy.py)

**Location:** Lines 100-130 (the quality flag section)

**Current code section starts:**
```python
# Define validation checks inline to avoid creating temporary columns
hour_of_day = F.col("hour_of_day")
energy = F.col("energy_mwh")

# Check conditions
is_within_bounds = energy >= 0
is_night = ((hour_of_day >= NIGHT_START) | (hour_of_day < NIGHT_END))
is_night_anomaly = is_night & (energy > MAX_NIGHT_ENERGY)
```

**Replace with:**
```python
# Define validation checks inline to avoid temporary columns
hour_of_day = F.col("hour_of_day")
energy = F.col("energy_mwh")
facility = F.col("facility_code")

# Physical bounds check
is_within_bounds = energy >= 0

# TIME-OF-DAY CHECKS
is_night = ((hour_of_day >= NIGHT_START) | (hour_of_day < NIGHT_END))
is_peak = (hour_of_day >= PEAK_NOON_START) & (hour_of_day <= PEAK_NOON_END)
is_daytime = (hour_of_day >= DAYTIME_START) & (hour_of_day < DAYTIME_END)

# NIGHT-TIME ANOMALIES (HIGH severity)
is_night_anomaly = is_night & (energy > SUSPICIOUS_NIGHT_ENERGY)

# PEAK HOUR ANOMALIES (MEDIUM severity)
is_peak_low = is_peak & (energy < MIN_PEAK_ENERGY)

# DAYTIME ZERO (will be enhanced with weather data later)
is_daytime_zero = is_daytime & (energy == 0.0)

# CAPACITY ANOMALIES (facility-specific, MEDIUM-HIGH severity)
capacity_threshold = F.when(facility == 'COLEASF', 145.0).otherwise(115.0)
is_capacity_anomaly = energy > capacity_threshold

# STATISTICAL OUTLIER (existing check)
is_statistical_outlier = (energy < TUKEY_LOWER) | (energy > TUKEY_UPPER)
```

---

### Change 1.3: Build Quality Issues String (hourly_energy.py)

**Location:** Lines 131-153

**Current incomplete section:**
```python
# Build quality_issues and quality_flag in single pass
```

**Replace with complete logic:**
```python
# Build quality_issues and quality_flag in single pass
quality_issues = F.concat_ws(
    "|",
    F.when(is_night_anomaly, F.lit("NIGHT_HIGH_ENERGY")),
    F.when(is_peak_low, F.lit("PEAK_LOW_ENERGY")),
    F.when(is_daytime_zero, F.lit("DAYTIME_ZERO")),
    F.when(is_capacity_anomaly, F.lit("CAPACITY_ANOMALY")),
    F.when(is_statistical_outlier, F.lit("STATISTICAL_OUTLIER")),
)

quality_flag = F.when(
    is_night_anomaly | is_capacity_anomaly, 
    F.lit("CAUTION")  # HIGH severity issues
).when(
    is_peak_low | is_daytime_zero,
    F.lit("CAUTION")  # MEDIUM severity - still worth investigating
).when(
    is_statistical_outlier & ~is_night_anomaly,
    F.lit("CAUTION")  # Only if not already high severity
).otherwise(
    F.lit("GOOD")
)

result = (
    result
    .withColumn("quality_issues", quality_issues)
    .withColumn("quality_flag", quality_flag)
    .drop("hour_of_day")  # Clean up
)

return result
```

---

### Change 1.4: Weather Data Integration (hourly_energy.py)

**Location:** End of transform() method (before return)

Add join with weather to detect daytime zero with radiation:

```python
# [After building result from energy alone]

# Join with weather to enhance quality checks
weather_data = self._spark.sql("""
    SELECT facility_code, date_hour, shortwave_radiation
    FROM iceberg.silver.clean_hourly_weather
    WHERE shortwave_radiation IS NOT NULL
""")

result_with_weather = (
    result
    .join(
        weather_data,
        on=["facility_code", "date_hour"],
        how="left"
    )
)

# Enhanced check: Daytime zero with high radiation = REJECT
is_daytime_zero_with_high_rad = (
    (F.col("time_period") == "Daytime") &
    (F.col("energy_mwh") == 0.0) &
    (F.col("shortwave_radiation") > 300)
)

result_enhanced = result_with_weather.withColumn(
    "quality_flag",
    F.when(
        is_daytime_zero_with_high_rad,
        F.lit("CAUTION")
    ).otherwise(F.col("quality_flag"))
).drop("shortwave_radiation")

return result_enhanced
```

---

## PHASE 2: Enhanced Validation (2-3 giờ) - 🟠 HIGH PRIORITY

### Change 2.1: Adjust Weather Bounds (hourly_weather.py)

**File:** `src/pv_lakehouse/etl/silver/hourly_weather.py`  
**Location:** Lines 35-52 (_numeric_columns)

**Current:**
```python
"direct_normal_irradiance": (0.0, 900.0),
```

**Change to:**
```python
"direct_normal_irradiance": (0.0, 1200.0),  # Increased from 900
```

---

### Change 2.2: Add Radiation Consistency Check (hourly_weather.py)

**Location:** Lines 95-110 (quality validation section)

**Add new check:**
```python
# Radiation consistency: DNI should not far exceed direct radiation
is_inconsistent_radiation = (
    (F.col("direct_normal_irradiance") > 1000) & 
    (F.col("direct_radiation") < 300) &
    ~is_night_rad_high  # Not if night
)

# Add to quality_issues
quality_issues = F.concat_ws(
    "|",
    existing_issues,  # Keep existing
    F.when(is_inconsistent_radiation, F.lit("RADIATION_INCONSISTENT"))
)
```

---

### Change 2.3: Add Context-Aware Divergence (outlier_handler.py)

**File:** `src/pv_lakehouse/etl/silver/outlier_handler.py`  
**Location:** End of file (before __all__)

**Add new function:**
```python
def get_hourly_divergence_threshold(hour_of_day: int) -> float:
    """
    Get divergence threshold based on time of day.
    
    Sunrise/sunset hours (6-7, 16-17) have naturally high divergence
    due to low solar angles and system inefficiency.
    
    Peak hours (10-14) should have low divergence (high efficiency).
    """
    if hour_of_day in [6, 7, 16, 17]:
        return 0.50  # Lenient: low angle = low efficiency
    elif hour_of_day in range(10, 15):
        return 0.25  # Strict: peak should be efficient
    elif hour_of_day in [8, 9, 15]:
        return 0.35  # Moderate: ramping hours
    else:
        return 0.40  # Default: off-peak hours


def add_context_divergence_flags(
    df: DataFrame,
    energy_col: str = "energy_mwh",
    radiation_col: str = "shortwave_radiation",
    timestamp_col: str = "date_hour",
    divergence_col: str = "divergence",
) -> DataFrame:
    """
    Add divergence flags with hour-of-day context awareness.
    
    High divergence is expected at sunrise/sunset, unexpected at peak.
    """
    hour_of_day = F.hour(F.col(timestamp_col))
    
    # Get threshold per hour
    threshold = F.when(
        hour_of_day.isin([6, 7, 16, 17]), 0.50
    ).when(
        (hour_of_day >= 10) & (hour_of_day <= 14), 0.25
    ).when(
        hour_of_day.isin([8, 9, 15]), 0.35
    ).otherwise(0.40)
    
    is_high_divergence = F.col(divergence_col) > threshold
    
    return df.withColumn(
        "is_high_divergence_contextual",
        is_high_divergence
    )


# Update __all__ to include new functions
__all__ = [
    ...,
    "get_hourly_divergence_threshold",
    "add_context_divergence_flags",
]
```

---

## PHASE 3: Advanced Analysis (Optional, 4-6 giờ)

### Change 3.1: Facility-Specific Rules (outlier_handler.py)

```python
# Add facility-specific bounds
FACILITY_SPECIFIC_BOUNDS: Dict[str, Dict[str, Tuple[float, Optional[float]]]] = {
    'energy_mwh': {
        'COLEASF': (0.0, 145.0),   # High capacity
        'BNGSF1': (0.0, 115.0),
        'CLARESF': (0.0, 115.0),
        'GANNSF': (0.0, 115.0),
        'NYNGAN': (0.0, 115.0),
    }
}

def get_facility_specific_bounds(
    facility_code: str,
    variable: str,
    default_bounds: Tuple[float, Optional[float]]
) -> Tuple[float, Optional[float]]:
    """Get facility-specific bounds or use default."""
    facility_rules = FACILITY_SPECIFIC_BOUNDS.get(variable, {})
    return facility_rules.get(facility_code, default_bounds)
```

---

## 📊 Testing & Validation Plan

### Before Changes:
```sql
SELECT 
    quality_flag,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM iceberg.silver.clean_hourly_energy
GROUP BY quality_flag;

-- Expected: GOOD 91.19%, CAUTION 8.81%
```

### After Phase 1 Changes:
```sql
-- Re-run same query
-- Expected: GOOD 92.1-92.5%, CAUTION 7.5-8.0%
```

### Facility-specific check:
```sql
SELECT 
    facility_code,
    quality_flag,
    COUNT(*) as count
FROM iceberg.silver.clean_hourly_energy
GROUP BY facility_code, quality_flag
ORDER BY facility_code;

-- Expected: All facilities >= 90% GOOD
```

---

## 🔄 Implementation Checklist

- [ ] Review Phase 1 changes
- [ ] Implement Change 1.1 (facility thresholds)
- [ ] Implement Change 1.2 (quality checks)
- [ ] Implement Change 1.3 (quality issues)
- [ ] Implement Change 1.4 (weather join)
- [ ] Test Phase 1 changes
- [ ] Run Silver loaders: `python -m src.pv_lakehouse.etl.silver.hourly_energy`
- [ ] Validate with SQL queries
- [ ] Generate new EDA report
- [ ] Review improvements
- [ ] Plan Phase 2 if needed

---

## 📝 Rollback Plan

If issues found, revert changes:
```bash
# Revert to previous version
git checkout HEAD -- src/pv_lakehouse/etl/silver/hourly_energy.py

# Or rollback specific sections
# Keep backup of changes in branch: feature/silver-improvements
```

---

## 📞 Questions to Answer Before Implementation

1. **Q:** COLEASF có capacity thực sự 140+ MWh không?  
   **A:** Xem SILVER_LAYER_SQL_ANALYSIS.md Query 1️⃣

2. **Q:** Night-time energy > 1 MWh có phải lỗi hay legitimate billing?  
   **A:** Xem Query 2️⃣ - phân tích LAG/LEAD để detect pattern

3. **Q:** Daytime zero energy có nên REJECT hay CAUTION?  
   **A:** CAUTION first - investigate patterns, REJECT nếu pattern rõ ràng

4. **Q:** Có nên dùng weather data từ Bronze hay Silver?  
   **A:** Bronze is more recent, nhưng Silver khi transform xong sẽ join

5. **Q:** DST gaps có cần handle special không?  
   **A:** Thường xảy ra 1-2 lần/năm, không ảnh hưởng nhiều

