# Silver Layer Data Quality Improvements

**Phân tích dựa trên:** EDA với 81,354 records từ 2024-01-01 đến 2025-11-08

## 📊 Tổng Quan Hiện Tại

```
✅ GOOD Records:    74,185 (91.19%)
⚠️  CAUTION Records: 7,169 (8.81%)
❌ REJECT Records:  0 (0.00%)
```

**Mục tiêu:** Cải thiện từ 91.19% → 95%+ GOOD quality

---

## 🔴 CRITICAL ISSUES (Cần sửa ngay)

### 1. Night-time High Energy (121 records - HIGH severity)
**Vấn đề:** Energy > 1 MWh tại 22-6h (không có mặt trời)

```
Energy range: 1.01 - 12.05 MWh
Radiation: 0.00 W/m² (xác nhận là đêm)
```

**Root cause:** 
- Meter lag/billing adjustments
- Equipment errors
- Data entry mistakes

**Cách sửa:**
- Flag as CAUTION hoặc REJECT dựa trên magnitude
- Kiểm tra meter reset logic trong Bronze layer

**Độ ưu tiên:** 🔴 CRITICAL
**Impact:** +121 records GOOD (nếu exclude)

---

### 2. Daytime Zero Energy with High Radiation (231 records - HIGH severity)
**Vấn đề:** Energy = 0 MWh nhưng radiation > 300 W/m² (đang có ánh sáng)

```
Time: 6-18h
Radiation: 300 - 1052 W/m²
```

**Root cause:**
- Equipment offline/maintenance
- Inverter failure
- Communication gaps
- Data quality flags already set?

**Cách sửa:**
```sql
-- Kiểm tra nếu đã có quality_issues
SELECT facility_code, date_hour, quality_issues 
FROM silver.clean_hourly_energy 
WHERE time_period = 'Daytime' AND energy_mwh = 0

-- Recommend: Flag as CAUTION or mark as equipment_issue
```

**Độ ưu tiên:** 🔴 CRITICAL  
**Impact:** +231 records GOOD (nếu exclude)

---

### 3. Statistical Outliers - Energy (6,244 records - MEDIUM severity)
**Vấn đề:** Energy > 110 MWh (Q3 + 1.5×IQR boundary)

```
Mean: 21.73 MWh
Median: 0.087 MWh
Q1-Q3 range: [0, 88.75] MWh

Outliers (>110 MWh): 6,244 (7.68%)
Skewness: 1.5796 (right-skewed)
```

**Root cause:**
- High-output days (legitimate)
- BUT high divergence from radiation suggests anomalies
- COLEASF facility shows abnormally high peak generation

**Analysis per facility:**
```
Need to segment: 
- COLEASF: Different pattern/capacity?
- Others: Actual anomalies?
```

**Cách sửa:**
```python
# In hourly_energy.py transform():
# Add facility-specific thresholds
FACILITY_THRESHOLDS = {
    'COLEASF': 140.0,   # Higher capacity
    'BNGSF1': 110.0,
    'CLARESF': 110.0,
    'GANNSF': 110.0,
    'NYNGAN': 110.0,
}

# Apply per-facility validation
for facility, threshold in FACILITY_THRESHOLDS.items():
    result = result.withColumn(
        'quality_flag',
        F.when(
            (F.col('facility_code') == facility) & 
            (F.col('energy_mwh') > threshold),
            'CAUTION'
        ).otherwise(F.col('quality_flag'))
    )
```

**Độ ưu tiên:** 🟠 HIGH  
**Impact:** Better facility-specific thresholds

---

## 🟠 HIGH PRIORITY ISSUES

### 4. Peak Hour Low Energy (1,277 records - MEDIUM severity)
**Vấn đề:** Energy < 1 MWh tại 10-15h (peak hours) nhưng radiation > 500 W/m²

```
Count: 1,277 (1.57%)
Expected: 40-60+ MWh during peak
Actual: < 1 MWh
```

**Root cause:**
- Partial outage
- System maintenance windows
- Weather events (cloud cover)
- Inverter issues

**Cách sửa:**
```python
# Already in code but maybe needs refinement
is_peak_anomaly = (
    (hour_of_day >= 10) & (hour_of_day <= 15) & 
    (energy_mwh < 5.0)
)

# Enhanced: Check against radiation to distinguish anomalies
is_peak_low_with_high_rad = (
    (hour_of_day >= 10) & (hour_of_day <= 15) & 
    (energy_mwh < 5.0) &
    (shortwave_radiation > 500)  # High radiation but no output
)
```

**Độ ưu tiên:** 🟠 HIGH  
**Impact:** +1,277 potential CAUTION flags

---

### 5. High Divergence Energy-Radiation (11,123 records - MEDIUM severity)
**Vấn đề:** abs(energy_norm - radiation_norm) > 0.3

```
Mean divergence: 0.2186
High divergence (>0.3): 11,123 (13.67%)
Correlation: 0.6398 (moderate)

Interpretation:
- 13.67% records have energy-radiation mismatch
- Likely: Weather events, shading, inverter efficiency
```

**Root cause:**
- **Cloudy periods:** High radiation variance
- **Early/late hours:** Lower system efficiency
- **Equipment degradation:** Efficiency drops
- **Seasonal effects:** Temp impacts efficiency

**Cách sửa:**
```python
# Add time-of-day weighted divergence thresholds
def get_divergence_threshold(hour_of_day):
    if hour_of_day in [6, 7, 16, 17]:  # Sunrise/sunset
        return 0.5  # More lenient (inefficient angles)
    elif hour_of_day in [10, 11, 12, 13, 14]:  # Peak hours
        return 0.25  # Stricter (should be efficient)
    else:
        return 0.35  # Default

# Apply in transform:
threshold = F.when(
    (F.col('hour_of_day').isin([6, 7, 16, 17])), 0.5
).when(
    (F.col('hour_of_day').isin([10, 11, 12, 13, 14])), 0.25
).otherwise(0.35)
```

**Độ ưu tiên:** 🟠 HIGH  
**Impact:** Better context-aware quality flags

---

## 🟡 MEDIUM PRIORITY ISSUES

### 6. Physical Limit Violations (79 records - LOW severity)
**Vấn đề:** Direct Normal Irradiance (DNI) out of bounds

```
Violations: 79 (0.10%)
Current bounds: 0 - 1000 W/m²

Likely: DNI > 1000 during clear sky conditions
or measurement errors
```

**Cách sửa:**
```python
# In hourly_weather.py - adjust DNI bounds
_numeric_columns = {
    ...
    "direct_normal_irradiance": (0.0, 1200.0),  # Increased from 900
    ...
}
```

**Độ ưu tiên:** 🟡 MEDIUM  
**Impact:** +79 records resolve violations

---

### 7. Temporal Anomalies (1 irregular gap per facility)
**Vấn đề:** Mỗi facility có ~1 gap non-hourly

```
BNGSF1:    1 irregular gap
CLARESF:   1 irregular gap
COLEASF:   1 irregular gap
GANNSF:    1 irregular gap
NYNGAN:    1 irregular gap
```

**Root cause:**
- Likely: DST (Daylight Saving Time) transitions
- Or: Single missed hour during data import

**Cách sửa:**
```python
# In transform: Add gap detection and interpolation
# Check for DST transitions
def handle_dst_gaps(df):
    """Handle gaps from DST transitions"""
    return df  # Mark dates with gaps for investigation

# For now: Mark with CAUTION
is_temporal_gap = (F.col('expected_hour') != F.col('actual_hour'))
```

**Độ ưu tiên:** 🟡 MEDIUM  
**Impact:** Better temporal consistency

---

## 🔧 IMPLEMENTATION ROADMAP

### Phase 1: Quick Wins (1-2 hours)
- [x] Identify facility-specific thresholds
- [ ] Add COLEASF higher capacity threshold (140 MWh)
- [ ] Flag night-time high energy → CAUTION
- [ ] Flag daytime zero-energy with radiation → CAUTION

### Phase 2: Enhanced Validation (2-3 hours)
- [ ] Implement time-of-day divergence thresholds
- [ ] Add peak-hour low energy detection
- [ ] Adjust physical bounds (DNI → 1200)
- [ ] Add quality_issues descriptive text

### Phase 3: Advanced Analysis (4-6 hours)
- [ ] Facility-specific efficiency curves
- [ ] Temperature-dependent efficiency
- [ ] Cloud cover correlation analysis
- [ ] Machine learning anomaly detection

---

## 📝 CODE CHANGES SUMMARY

### `hourly_energy.py` - Priority Changes

**1. Add facility-specific thresholds:**
```python
# Line ~110 - After TUKEY_UPPER definition
FACILITY_THRESHOLDS = {
    'COLEASF': 140.0,
    'BNGSF1': 110.0,
    'CLARESF': 110.0,
    'GANNSF': 110.0,
    'NYNGAN': 110.0,
}

# Apply in quality checks
energy_upper = F.when(
    F.col('facility_code') == 'COLEASF', 140.0
).otherwise(110.0)
```

**2. Enhance night-time detection:**
```python
# Line ~95 - Make night energy threshold more aggressive
MAX_NIGHT_ENERGY = 0.1  # Changed from 0.3
SUSPICIOUS_NIGHT_ENERGY = 1.0  # Flag anything > 1 as HIGH severity
```

**3. Add peak-hour divergence check:**
```python
# New constant
PEAK_HOUR_DIVERGENCE_THRESHOLD = 0.25
NORMAL_DIVERGENCE_THRESHOLD = 0.35

# Use in quality logic
is_high_divergence = F.when(
    (F.col('hour_of_day') >= 10) & (F.col('hour_of_day') <= 14),
    F.col('divergence') > 0.25
).otherwise(F.col('divergence') > 0.35)
```

### `hourly_weather.py` - Priority Changes

**1. Increase DNI bounds:**
```python
# Line ~40
_numeric_columns = {
    ...
    "direct_normal_irradiance": (0.0, 1200.0),  # from 900
    ...
}
```

**2. Add radiation consistency check:**
```python
# Line ~90 - New validation
is_dni_dni_inconsistent = (
    (F.col("direct_normal_irradiance") > 1000) & 
    (F.col("direct_radiation") < 300)
)
```

### `outlier_handler.py` - Updates Needed

**1. Add facility-specific rules:**
```python
FACILITY_SPECIFIC_BOUNDS = {
    'energy_mwh': {
        'COLEASF': (0.0, 140.0),
        'default': (0.0, 110.0),
    }
}
```

**2. Add context-aware divergence:**
```python
def get_divergence_threshold(hour_of_day: int) -> float:
    """Hour-dependent divergence tolerance"""
    if hour_of_day in [6, 7, 16, 17]:  # Sunrise/sunset
        return 0.5
    elif hour_of_day in range(10, 15):  # Peak hours
        return 0.25
    else:
        return 0.35
```

---

## 📊 Expected Impact After Improvements

| Issue | Current | After Fix | Gain |
|-------|---------|-----------|------|
| Night high energy | 121 flagged | 121 REJECT | +121 |
| Daytime zero | 231 flagged | 231 REJECT | +231 |
| Physical violations | 79 unfixed | 0 | +79 |
| **Total** | **7,169 CAUTION** | **~6,740 CAUTION** | **+429 GOOD** |
| **New Quality %** | **91.19%** | **92.12%** | **+0.93%** |

**Target:** 95%+ achievable with Phase 2+3 implementation

---

## 🎯 NEXT STEPS

1. ✅ **Review current EDA findings** (Done)
2. 📋 **Prioritize fixes** (This doc)
3. 🔧 **Implement Phase 1 changes** (2 hours)
4. ✔️ **Re-run Silver layer loaders**
5. 📊 **Validate improvements with new EDA**
6. 🚀 **Deploy to production**

