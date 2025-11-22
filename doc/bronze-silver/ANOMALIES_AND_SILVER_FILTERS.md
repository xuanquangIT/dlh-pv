# Bronze Anomalies & Silver Layer Bounds/Filters

**Mục đích:** Tài liệu này chi tiết hóa các vấn đề chất lượng dữ liệu (anomalies) được phát hiện ở Bronze layer và cách chúng được xử lý thông qua validation bounds và quality filters ở Silver layer.

---

## 📋 Tổng Quan

### Ba Loại Dữ Liệu Chính

| Layer | Bảng Bronze | Bảng Silver | Nguồn |
|-------|-----------|-----------|--------|
| **Weather** | `lh.bronze.raw_facility_weather` | `lh.silver.clean_hourly_weather` | Open-Meteo API |
| **Energy** | `lh.bronze.raw_facility_timeseries` | `lh.silver.clean_hourly_energy` | OpenElectricity API |
| **Air Quality** | `lh.bronze.raw_facility_air_quality` | `lh.silver.clean_hourly_air_quality` | Open-Meteo API |

### Ba Mức Quality Flags

```
GOOD    → 🟢 Dữ liệu sạch, vượt tất cả validation (85-98% dữ liệu)
CAUTION → 🟡 Phát hiện anomalies, cần điều tra thêm (1-15% dữ liệu)
REJECT  → 🔴 Vi phạm hard bounds, loại bỏ khỏi training (<0.1% dữ liệu)
```

---

## 🌦️ WEATHER LAYER - Bất Thường & Xử Lý

### 1. Radiation Anomalies

#### **Bất Thường ở Bronze**

| Anomaly | Mô Tả | Tần Suất | Nguyên Nhân |
|---------|-------|---------|-----------|
| **Night-time Radiation** | Phát hiện shortwave radiation > 100 W/m² vào ban đêm (22:00-6:00) | < 0.1% | Sensor malfunction, data transmission error |
| **Radiation Inconsistency** | Tổng (Direct + Diffuse) > Shortwave × 1.05 | < 1% | Sensor calibration mismatch, API aggregation error |
| **Extreme Peak Radiation** | Shortwave > 1120 W/m² | < 0.01% | Measurement error, sensor reflection |
| **High Cloud + High Radiation** | Cloud cover > 98% nhưng shortwave > 600 W/m² tại giờ peak (10-14) | < 0.5% | Sensor drift, localized weather patterns |

#### **Bounds & Filters ở Silver**

```python
# FILE: hourly_weather.py

_numeric_columns = {
    "shortwave_radiation": (0.0, 1150.0),      # Hard bound: P99.5 = 1045 + 5% margin
    "direct_radiation": (0.0, 1050.0),          # Hard bound: Actual max = 1009
    "diffuse_radiation": (0.0, 520.0),          # Hard bound: Actual max = 520
    "direct_normal_irradiance": (0.0, 1060.0), # Hard bound: Actual max = 1057
}

# Validation Logic:
is_night = (hour_of_day < 6) | (hour_of_day >= 22)
is_night_rad_high = is_night & (shortwave_radiation > 100)
                    → FLAG: CAUTION (soft check)

radiation_inconsistency = (direct + diffuse) > (shortwave * 1.05)
                         → FLAG: CAUTION (soft check)

high_cloud_peak = is_peak_sun & (cloud_cover > 98) & (shortwave < 600)
                 → FLAG: CAUTION (soft check)

# Quality Flag Assignment:
quality_flag = "REJECT" if out_of_bounds else (
    "CAUTION" if night_rad_high or radiation_inconsistency or high_cloud_peak
    else "GOOD"
)
```

**Lý Thuyết Thiết Kế:**
- **Hard Bounds (REJECT):** Chỉ với giá trị vật lý không thể (< 0 hoặc > 1150)
- **Soft Checks (CAUTION):** 
  - Night radiation > 100: Rõ ràng là lỗi
  - Radiation sum inconsistency: Chỉ ra calibration issue
  - High cloud + peak sun: Rare but valid (extreme weather) → raised threshold to 98% to reduce false positives by ~90%

---

### 2. Temperature Anomalies

#### **Bất Thường ở Bronze**

| Anomaly | Mô Tả | Tần Suất | Nguyên Nhân |
|---------|-------|---------|-----------|
| **Extreme Temperature** | T < -10°C hoặc T > 45°C | < 0.1% | Sensor malfunction, extreme heatwave/coldwave |
| **Invalid Dew Point** | Dew point > temperature | < 0.01% | API calculation error |
| **Wet Bulb Logic** | Wet bulb > temperature (impossible) | < 0.05% | Measurement inconsistency |

#### **Bounds & Filters ở Silver**

```python
_numeric_columns = {
    "temperature_2m": (-10.0, 50.0),           # P99.5 = 38.5°C, actual max = 43.7°C
    "dew_point_2m": (-20.0, 30.0),             # Expanded for extreme conditions
    "wet_bulb_temperature_2m": (-5.0, 40.0),   # Bounded by air temperature logic
}

# Temperature Anomaly Check:
extreme_temp = (temperature < -10) | (temperature > 45)
             → FLAG: CAUTION (soft check)
```

**Lý Thuyết Thiết Kế:**
- Bounds dựa trên **P99.5 percentile** (99.5% dữ liệu hợp lệ nằm trong giới hạn)
- Cho phép **extreme weather events** (Australian heatwaves, cold snaps)
- Lower bound -10°C: Xử lý extreme winter
- Upper bound 50°C: Cho phép heatwaves (actual max: 43.7°C), thêm 6.3°C để có margin

---

### 3. Cloud Cover & Pressure Anomalies

#### **Bất Thường ở Bronze**

| Anomaly | Mô Tả | Tần Suất |
|---------|-------|---------|
| **Out-of-range Cloud Cover** | Cloud cover < 0% hoặc > 100% | < 0.01% |
| **Invalid Pressure** | MSL pressure < 985 hPa hoặc > 1050 hPa | < 0.1% |
| **Wind Speed Extremes** | Wind > 50 m/s | < 0.01% |

#### **Bounds & Filters ở Silver**

```python
_numeric_columns = {
    "cloud_cover": (0.0, 100.0),               # Perfect bounds, no outliers
    "cloud_cover_low": (0.0, 100.0),
    "cloud_cover_mid": (0.0, 100.0),
    "cloud_cover_high": (0.0, 100.0),
    "pressure_msl": (985.0, 1050.0),           # P99 = 1033 hPa
    "wind_speed_10m": (0.0, 50.0),             # Increased for Australian cyclones
    "wind_gusts_10m": (0.0, 120.0),            # Extreme weather bound
}

# Hard Bounds Check:
for column, (min_val, max_val) in numeric_columns.items():
    if value < min_val or value > max_val:
        → FLAG: REJECT (hard check)
```

---

### 4. Precipitation & Sunshine Duration

#### **Bất Thường ở Bronze**

| Anomaly | Mô Tả | Tần Suất |
|---------|-------|---------|
| **Extreme Precipitation** | Hourly rain > 1000 mm | < 0.01% |
| **Sunshine > 1 hour** | Sunshine > 3600 sec trong 1 giờ | < 0.1% |

#### **Bounds & Filters ở Silver**

```python
_numeric_columns = {
    "precipitation": (0.0, 1000.0),            # Extreme event bound
    "sunshine_duration": (0.0, 3600.0),        # 1 hour max per hourly period
}
```

---

## ⚡ ENERGY LAYER - Bất Thường & Xử Lý

### 1. Negative Energy (Hard Violation)

#### **Bất Thường ở Bronze**

```
Bronze Raw Data:
  facility_code | interval_ts          | energy | power
  AVLSF        | 2025-11-01 08:00:00 | -5.2   | -520
  AVLSF        | 2025-11-01 09:00:00 | -0.3   | -30
```

| Anomaly | Mô Tả | Tần Suất | Nguyên Nhân |
|---------|-------|---------|-----------|
| **Negative Energy** | energy_mwh < 0 | < 0.05% | API data transmission error, database corruption |

#### **Bounds & Filters ở Silver**

```python
# FILE: hourly_energy.py

ENERGY_LOWER = 0.0  # Physical constraint: Solar cannot generate negative energy
PEAK_REFERENCE_MWH = 85.0  # Facility-level reference capacity

# Validation:
is_within_bounds = energy_mwh >= ENERGY_LOWER

quality_flag = (
    "REJECT" if not is_within_bounds else  # Hard violation
    "CAUTION" if [anomalies detected] else
    "GOOD"
)
```

**Impact:** Loại bỏ **100%** records với energy < 0

---

### 2. Night-time Energy Anomalies

#### **Bất Thường ở Bronze**

```
Bronze Anomaly Pattern:
  hour | expected | observed | reason
  23:00 | 0 MWh  | 2.1 MWh | sensor malfunction
  03:00 | 0 MWh  | 1.5 MWh | data transmission lag
  22:00 | 0 MWh  | 3.2 MWh | equipment issue
```

| Anomaly | Mô Tả | Tần Suất | Nguyên Nhân |
|---------|-------|---------|-----------|
| **Night-time Generation** | Energy > 1.0 MWh giữa 22:00-6:00 | 0.2-0.5% | Solar equipment malfunction, sensor failure, data transmission error |

#### **Bounds & Filters ở Silver**

```python
is_night = ((hour >= 22) | (hour < 6))
is_night_anomaly = is_night & (energy_mwh > 1.0)
                 → FLAG: CAUTION (soft check)
                 → quality_issues: "NIGHT_ENERGY_ANOMALY"

# Threshold Design:
# - 1.0 MWh allows for sensor noise/calibration offset
# - > 1.0 is clearly anomalous (solar panels don't generate at night)
```

**Pattern Recognition:**
- Systematic night generation: Sensor calibration issue
- Random spikes: Data transmission lag
- Always at same hour: Equipment timer issue

---

### 3. Daytime Zero Energy

#### **Bất Thường ở Bronze**

```
Bronze Anomaly Pattern:
  hour | expected | observed | reason
  09:00 | 40 MWh | 0 MWh    | maintenance window
  12:00 | 80 MWh | 0 MWh    | equipment downtime
  15:00 | 70 MWh | 0 MWh    | grid disconnection
```

| Anomaly | Mô Tả | Tần Suất | Nguyên Nhân |
|---------|-------|---------|-----------|
| **Daytime Zero (Non-peak)** | Energy = 0 MWh giữa 8:00-17:00 (non-peak) | 1-3% | Maintenance, equipment downtime |
| **Equipment Downtime (Peak)** | Energy = 0 MWh tại peak hours (10:00-14:00) | 0.5-1% | Critical equipment failure, grid issues |

#### **Bounds & Filters ở Silver**

```python
is_daytime_zero = (hour >= 8) & (hour <= 17) & (energy_mwh == 0.0)
                → FLAG: CAUTION (soft check)
                → quality_issues: "DAYTIME_ZERO_ENERGY"

is_peak = (hour >= 10) & (hour <= 14)
is_equipment_downtime = is_peak & (energy_mwh == 0.0)
                      → FLAG: CAUTION (soft check)
                      → quality_issues: "EQUIPMENT_DOWNTIME"
```

**Phân Loại:**
- **Daytime Zero (8-17):** Có thể là maintenance → CAUTION
- **Peak Hour Zero (10-14):** Critical issue → CAUTION (more severe)

---

### 4. Transition Hour Low Energy (Ramp Issues)

#### **Bất Thường ở Bronze**

```
Bronze Anomaly Pattern - Sunrise (6:00-8:00):
  hour | expected_pct | observed | reason
  06:00 | 5-15%       | 0.2 MWh  | slow ramp-up (should be ~4.25-12.75 MWh if peak=85)
  07:00 | 10-30%      | 0.5 MWh  | inverter delay
  08:00 | 20-50%      | 1.0 MWh  | wiring issue

Bronze Anomaly Pattern - Sunset (17:00-19:00):
  hour | expected_pct | observed | reason
  17:00 | 10-50%      | 1.2 MWh  | sensor lag
  18:00 | 5-20%       | 0.8 MWh  | grid ramping issue
  19:00 | 1-5%        | 0.1 MWh  | normal (acceptable)
```

| Anomaly | Mô Tả | Tần Suất | Threshold | Nguyên Nhân |
|---------|-------|---------|-----------|-----------|
| **Sunrise Low Energy** | Energy tại 6:00-8:00 < 5% peak | 2-5% | < 4.25 MWh | Slow ramp-up, inverter delay |
| **Early Morning Low Energy** | Energy tại 8:00-10:00 < 8% peak | 1-3% | < 6.8 MWh | Morning shading, equipment warmup |
| **Sunset Low Energy** | Energy tại 17:00-19:00 < 10% peak | 1-2% | < 8.5 MWh | Sensor lag, grid ramping |

#### **Bounds & Filters ở Silver**

```python
PEAK_REFERENCE_MWH = 85.0

# Sunrise (6:00-8:00): Only 5% threshold
is_sunrise = (hour >= 6) & (hour < 8)
threshold = 0.05 * PEAK_REFERENCE_MWH  # = 4.25 MWh
is_sunrise_low = is_sunrise & (energy > 0.01) & (energy < 4.25)
               → FLAG: CAUTION (soft check)
               → quality_issues: "TRANSITION_HOUR_LOW_ENERGY"

# Early Morning (8:00-10:00): 8% threshold
is_early_morning = (hour >= 8) & (hour < 10)
threshold = 0.08 * PEAK_REFERENCE_MWH  # = 6.8 MWh
is_early_morning_low = is_early_morning & (energy > 0.01) & (energy < 6.8)
                     → FLAG: CAUTION (soft check)
                     → quality_issues: "TRANSITION_HOUR_LOW_ENERGY"

# Sunset (17:00-19:00): 10% threshold
is_sunset = (hour >= 17) & (hour < 19)
threshold = 0.10 * PEAK_REFERENCE_MWH  # = 8.5 MWh
is_sunset_low = is_sunset & (energy > 0.01) & (energy < 8.5)
              → FLAG: CAUTION (soft check)
              → quality_issues: "TRANSITION_HOUR_LOW_ENERGY"

# Combined Check:
is_transition_hour_low_energy = (
    is_sunrise_low | is_early_morning_low | is_sunset_low
)
```

**Design Rationale:**
- **Sunrise stricter (5%):** Morning ramping is most predictable
- **Early morning loose (8%):** Weather variability at 8-10
- **Sunset looser (10%):** Unpredictable cloud patterns
- **Energy > 0.01:** Avoid flagging already-zero energy

---

### 5. Peak Hour Low Energy Efficiency

#### **Bất Thường ở Bronze**

```
Bronze Anomaly Pattern - Peak Hours (10:00-14:00):
  hour | expected | observed | efficiency | reason
  10:00 | 80 MWh | 25 MWh   | 31%        | equipment malfunction
  11:00 | 85 MWh | 30 MWh   | 35%        | inverter clipping issue
  13:00 | 85 MWh | 20 MWh   | 24%        | thermal throttling
```

| Anomaly | Mô Tả | Tần Suất | Threshold | Nguyên Nhân |
|---------|-------|---------|-----------|-----------|
| **Peak Hour Low Efficiency** | Energy tại 10:00-14:00 < 50% peak | 0.5-2% | < 42.5 MWh | Equipment malfunction, inverter clipping |

#### **Bounds & Filters ở Silver**

```python
is_peak = (hour >= 10) & (hour <= 14)
PEAK_REFERENCE_MWH = 85.0
efficiency_threshold = 0.50 * PEAK_REFERENCE_MWH  # = 42.5 MWh

is_efficiency_anomaly = (
    is_peak & 
    (energy > 0.5) &  # Not zero (already flagged)
    (energy < 42.5)
)
                      → FLAG: CAUTION (soft check)
                      → quality_issues: "PEAK_HOUR_LOW_ENERGY"
```

**Design Rationale:**
- 50% threshold: Catches significant efficiency loss
- At peak sun, < 50% indicates equipment/system issue
- Excludes zero (already flagged separately)

---

## 💨 AIR QUALITY LAYER - Bất Thường & Xử Lý

### 1. Pollutant Concentration Anomalies

#### **Bất Thường ở Bronze**

| Anomaly | Mô Tả | Tần Suất | Nguyên Nhân |
|---------|-------|---------|-----------|
| **Out-of-range PM2.5** | PM2.5 < 0 hoặc > 500 µg/m³ | < 0.01% | Sensor malfunction, API error |
| **Out-of-range PM10** | PM10 < 0 hoặc > 500 µg/m³ | < 0.01% | Sensor malfunction, API error |
| **Extreme NO₂/O₃/SO₂** | Any pollutant > 500 ppb | < 0.05% | Rare extreme pollution event or sensor error |

#### **Bounds & Filters ở Silver**

```python
# FILE: hourly_air_quality.py

_numeric_columns = {
    "pm2_5": (0.0, 500.0),                    # Hard bound: 0-500 µg/m³
    "pm10": (0.0, 500.0),
    "dust": (0.0, 500.0),
    "nitrogen_dioxide": (0.0, 500.0),        # Hard bound: 0-500 ppb
    "ozone": (0.0, 500.0),
    "sulphur_dioxide": (0.0, 500.0),
    "carbon_monoxide": (0.0, 500.0),
    "uv_index": (0.0, 15.0),                 # Hard bound: 0-15
    "uv_index_clear_sky": (0.0, 15.0),
}

# Validation:
is_valid_bounds = True
for column, (min_val, max_val) in numeric_columns.items():
    if not (min_val <= value <= max_val):
        → FLAG: CAUTION (soft check - no REJECT for air quality)
        → quality_issues: f"{column}_OUT_OF_BOUNDS"

quality_flag = "GOOD" if is_valid else "CAUTION"
```

---

### 2. AQI Calculation Anomalies

#### **Bất Thường ở Bronze**

| Anomaly | Mô Tả | Tần Suất |
|---------|-------|---------|
| **Invalid AQI** | Calculated AQI outside 0-500 range | < 0.01% |
| **Missing PM2.5** | PM2.5 = NULL (cannot calculate AQI) | ~5-10% |

#### **Bounds & Filters ở Silver**

```python
def _aqi_from_pm25(pm25_value):
    """Calculate AQI from PM2.5 using EPA breakpoints"""
    # EPA AQI Breakpoints for PM2.5 (µg/m³):
    # 0-12       → AQI 0-50     (Good)
    # 12-35.4    → AQI 51-100   (Moderate)
    # 35.5-55.4  → AQI 101-150  (Unhealthy for Sensitive)
    # 55.5-150.4 → AQI 151-200  (Unhealthy)
    # 150.5-250.4→ AQI 201-300  (Very Unhealthy)
    # > 250.4    → AQI > 300    (Hazardous)
    
    breakpoints_pm25 = [12, 35.4, 55.4, 150.4, 250.4]
    breakpoints_aqi = [50, 100, 150, 200, 300, 500]
    
    # Perform linear interpolation
    for i, bp in enumerate(breakpoints_pm25):
        if pm25_value <= bp:
            return linear_interpolation(pm25_value, bp_low, bp_high, aqi_low, aqi_high)

# Validation:
aqi_value = calculate_aqi(pm2_5)
aqi_valid = (aqi_value >= 0) & (aqi_value <= 500)
           → If invalid: FLAG: CAUTION, quality_issues: "AQI_OUT_OF_RANGE"

aqi_category = (
    "Good" if 0 <= aqi <= 50 else
    "Moderate" if 51 <= aqi <= 100 else
    "Unhealthy" if 101 <= aqi <= 200 else
    "Hazardous"
)

quality_flag = "GOOD" if aqi_valid else "CAUTION"
```

**AQI Categories:**
- **Good (0-50):** AQI từ PM2.5 ≤ 12 µg/m³
- **Moderate (51-100):** AQI từ PM2.5 = 12-35.4 µg/m³
- **Unhealthy (101-200):** AQI từ PM2.5 = 35.5-55.4 µg/m³
- **Hazardous (201-500):** AQI từ PM2.5 > 55.5 µg/m³

---

## 📊 Bảng Tổng Hợp: Anomalies vs Silver Filters

### Weather Layer Summary

| Anomaly | Bronze Detection | Silver Filter | Flag |
|---------|-----------------|---------------|------|
| Night radiation > 100 W/m² | Manual check | `is_night & (shortwave > 100)` | CAUTION |
| Radiation inconsistency | Manual check | `(direct + diffuse) > (shortwave * 1.05)` | CAUTION |
| High cloud + peak sun | Manual check | `peak & (cloud > 98) & (shortwave < 600)` | CAUTION |
| Extreme temperature | Manual check | `(temp < -10) \| (temp > 45)` | CAUTION |
| Out of bounds (any metric) | Manual check | Hard bounds check | REJECT |
| **Data Quality** | ~0% anomalies | **95-100% GOOD** | ✅ |

### Energy Layer Summary

| Anomaly | Bronze Detection | Silver Filter | Flag |
|---------|-----------------|---------------|------|
| Negative energy | Manual check | `energy < 0` | REJECT |
| Night-time generation | Manual check | `night & (energy > 1.0)` | CAUTION |
| Daytime zero (8-17) | Manual check | `daytime & (hour 8-17) & (energy == 0)` | CAUTION |
| Equipment downtime (peak) | Manual check | `peak & (energy == 0)` | CAUTION |
| Sunrise low (< 5% peak) | Manual check | `sunrise & (0.01 < energy < 4.25)` | CAUTION |
| Early morning low (< 8%) | Manual check | `early_morning & (0.01 < energy < 6.8)` | CAUTION |
| Sunset low (< 10%) | Manual check | `sunset & (0.01 < energy < 8.5)` | CAUTION |
| Peak efficiency low (< 50%) | Manual check | `peak & (0.5 < energy < 42.5)` | CAUTION |
| **Data Quality** | ~0.5-1% anomalies | **85-95% GOOD, 5-15% CAUTION** | ⚠️ |

### Air Quality Layer Summary

| Anomaly | Bronze Detection | Silver Filter | Flag |
|---------|-----------------|---------------|------|
| Out of bounds (any pollutant) | Manual check | Hard bounds check | CAUTION |
| AQI calculation error | Manual check | `(aqi < 0) \| (aqi > 500)` | CAUTION |
| Invalid AQI category | Manual check | EPA breakpoint validation | CAUTION |
| **Data Quality** | ~0% anomalies | **98-100% GOOD** | ✅ |

---

## 🎯 Design Principles Behind Silver Filters

### Nguyên Tắc 1: Physics-Based Hard Bounds
```
Chỉ REJECT khi:
  - Vật lý không thể (Solar energy < 0, temperature < -100°C)
  - Hardware vô lý (Night radiation > 1000 W/m²)
  
Không REJECT khi:
  - Có thể xảy ra trong thực tế (Extreme heatwave, equipment downtime)
```

### Nguyên Tắc 2: P99.5 Percentile + Safety Margin
```
Threshold = P99.5 + 5% margin

Ví dụ: Shortwave Radiation
  - P99.5 percentile: 1045 W/m²
  - Safety margin: +5% = +52 W/m²
  - Final bound: 1150 W/m² (covers 99.5% dữ liệu)
  - Catches extreme anomalies mà không reject legitimate peaks
```

### Nguyên Tắc 3: Soft Checks Reduce False Positives
```
Threshold tuning to reduce false positives:

Cloud cover check:
  - Old threshold: 95% → 30% false positive rate
  - New threshold: 98% → 1% false positive rate
  - Improvement: 95% reduction in false positives!

Energy transition hours:
  - Different thresholds for sunrise (5%), early morning (8%), sunset (10%)
  - Accounts for varying weather patterns and system behavior
```

### Nguyên Tắc 4: Quality Flags Enable Smart Training
```python
# Sử dụng quality_flag trong model training:

for record in training_data:
    if record.quality_flag == "GOOD":
        weight = 1.0  # Full confidence
    elif record.quality_flag == "CAUTION":
        weight = 0.5  # Half confidence (edge case or anomaly)
    elif record.quality_flag == "REJECT":
        skip_record()  # No confidence (hard violation)
```

### Nguyên Tắc 5: Context-Aware Thresholds
```
Energy thresholds depend on facility context:
  - PEAK_REFERENCE_MWH = 85 MWh (facility-level average)
  - Sunrise threshold: 5% × 85 = 4.25 MWh
  - Peak efficiency: 50% × 85 = 42.5 MWh
  
Áp dụng cho tất cả facilities regardless of size!
```

---

## 📈 Expected Data Quality Distribution

### After Silver Layer Processing

```
Weather Data:
  ├─ GOOD: 95-100% (excellent API data)
  ├─ CAUTION: 0-5% (radiation/temperature anomalies)
  └─ REJECT: 0% (rare hard violations)

Energy Data:
  ├─ GOOD: 85-95% (mostly clean)
  ├─ CAUTION: 5-15% (maintenance windows, ramp issues)
  └─ REJECT: <0.1% (negative values only)

Air Quality Data:
  ├─ GOOD: 98-100% (excellent API data)
  ├─ CAUTION: 0-2% (rare bounds violations)
  └─ REJECT: 0% (no hard violations for air quality)
```

### Model Training Recommendation

```python
# For Solar Energy Forecasting:
train_data = silver_data.filter(
    (quality_flag == "GOOD") | 
    (quality_flag == "CAUTION" & weight_caution_records)
)

# Weight by quality:
record_weight = (
    1.0 if quality_flag == "GOOD" else
    0.5 if quality_flag == "CAUTION" else
    0.0  # REJECT
)

# Benefits:
# ✅ GOOD records: Clean signal for model learning
# ✅ CAUTION records: Edge cases help model robustness
# ✅ REJECT records: Excluded, prevent overfitting to errors
```

---

## 🔍 Debugging Anomalies

### Quick Diagnostics

```sql
-- Find records with CAUTION flags
SELECT 
    facility_code, date_hour, energy_mwh, quality_flag, quality_issues
FROM silver.clean_hourly_energy
WHERE quality_flag = 'CAUTION'
ORDER BY date_hour DESC
LIMIT 100;

-- Analyze anomaly distribution by type
SELECT 
    quality_issues, COUNT(*) as count, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
FROM silver.clean_hourly_energy
WHERE quality_flag = 'CAUTION'
GROUP BY quality_issues
ORDER BY count DESC;

-- Find NIGHT_ENERGY_ANOMALY patterns
SELECT 
    facility_code, date_hour, energy_mwh, 
    HOUR(date_hour) as hour, quality_issues
FROM silver.clean_hourly_energy
WHERE quality_issues LIKE '%NIGHT_ENERGY%'
ORDER BY facility_code, date_hour;
```

### Common Issues & Fixes

| Issue | Symptom | Root Cause | Fix |
|-------|---------|-----------|-----|
| Too many CAUTION flags | > 30% CAUTION | Bounds too strict | Adjust percentile margin |
| Missing real anomalies | Few CAUTION, RMSE high | Bounds too loose | Decrease percentile margin |
| False positives | CAUTION but data looks good | Cloud threshold too low | Increase to 98% |
| False negatives | GOOD but data looks wrong | Physics check missing | Add new validation logic |

---

## 📚 Reference Files

- **Source Code:** `src/pv_lakehouse/etl/silver/hourly_*.py`
- **Notebook Analysis:** `src/pv_lakehouse/etl/notebooks/bronze_silver_analysis.ipynb`
- **Validation Rules:** `SILVER_VALIDATION_RULES.md`
- **Bounds Summary:** `ANALYSIS_SUMMARY.md`

---

**Document Version:** 1.0  
**Last Updated:** 2025-11-22  
**Focus:** Anomaly detection, bounds justification, filter logic
