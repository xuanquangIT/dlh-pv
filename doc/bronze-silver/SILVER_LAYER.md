# 🟪 SILVER LAYER - Chi Tiết Toàn Bộ

**Tác Giả:** Data Engineering Team  
**Cập Nhật:** 2025-11-22  
**Phiên Bản:** 1.0

---

## 📌 Giới Thiệu Silver Layer

Silver layer là lớp **dữ liệu sạch và được xác thực (cleaned & validated data)**. Dữ liệu ở đây đã được xử lý từ Bronze, loại bỏ anomalies, áp dụng bounds checks, và gắn quality flags.

### Đặc Điểm Silver Layer

| Đặc Điểm | Mô Tả |
|---------|-------|
| **Nguồn Dữ Liệu** | Bronze layer (dữ liệu thô) |
| **Tính Chất** | Sạch, xác thực, có quality flags |
| **Quality** | Đã validate với hard bounds & soft checks |
| **Format** | Tiêu chuẩn hoá, chuẩn bị cho Gold |
| **Lưu Trữ** | Iceberg tables (merge writes) |
| **Chỉ Số** | Partition theo date_hour (local timezone) |
| **Ý Nghĩa** | Dùng cho ML training, analytics |

### So Sánh Bronze vs Silver

```
BRONZE Layer:
  ├─ Data: Thô từ API
  ├─ Quality Flags: None
  ├─ Validation: None
  ├─ Anomalies: 0.5-5% (chưa xử lý)
  └─ Use Case: Audit, history

SILVER Layer:
  ├─ Data: Sạch, xác thực
  ├─ Quality Flags: GOOD/CAUTION/REJECT
  ├─ Validation: Hard bounds + soft checks
  ├─ Anomalies: Đã flagged (0-15%)
  └─ Use Case: ML training, analytics
```

---

## 🌦️ Silver Weather (`lh.silver.clean_hourly_weather`)

### Mục Đích & Transformation

**Mục đích:** Chuyển đổi dữ liệu thời tiết từ Bronze sang dạng sạch, hourly, với quality flags.

**Transformation:**
```
Bronze raw_facility_weather (multiple records per hour from API)
    ↓
1. Filter: Only records with valid facility_code & timestamp
2. Round: All numeric columns to 4 decimal places
3. Aggregate: Group by date_hour (hourly)
4. Validate: Check hard bounds + soft logic checks
5. Flag: Assign quality_flag (GOOD/CAUTION/REJECT)
    ↓
Silver clean_hourly_weather (one record per facility per hour)
```

### Schema Chi Tiết

```
Column Name                         | Type      | Mô Tả
------------------------------------|-----------|-------
facility_code                       | string    | Mã facility
facility_name                       | string    | Tên facility
timestamp_local                     | timestamp | Thời gian (local timezone)
date_hour                           | timestamp | Giờ tròn (partition key)
date                                | date      | Ngày (YYYY-MM-DD)
shortwave_radiation                 | double    | Bức xạ sóng ngắn (W/m²) - ROUNDED
direct_radiation                    | double    | Bức xạ trực tiếp (W/m²) - ROUNDED
diffuse_radiation                   | double    | Bức xạ khuếch tán (W/m²) - ROUNDED
direct_normal_irradiance            | double    | DNI (W/m²) - ROUNDED
temperature_2m                      | double    | Nhiệt độ (°C) - ROUNDED
dew_point_2m                        | double    | Điểm sương (°C) - ROUNDED
wet_bulb_temperature_2m             | double    | Nhiệt độ bóng ướt (°C) - ROUNDED
cloud_cover                         | double    | Mây (%) - ROUNDED
cloud_cover_low                     | double    | Mây tầng thấp (%) - ROUNDED
cloud_cover_mid                     | double    | Mây tầng trung bình (%) - ROUNDED
cloud_cover_high                    | double    | Mây tầng cao (%) - ROUNDED
precipitation                       | double    | Mưa (mm) - ROUNDED
sunshine_duration                   | double    | Thời gian nắng (s) - ROUNDED
total_column_integrated_water_vapour| double    | Hơi nước (kg/m²) - ROUNDED (forward-filled)
wind_speed_10m                      | double    | Tốc độ gió (m/s) - ROUNDED
wind_direction_10m                  | double    | Hướng gió (°) - ROUNDED
wind_gusts_10m                      | double    | Gió giật (m/s) - ROUNDED
pressure_msl                        | double    | Áp suất (hPa) - ROUNDED
is_valid                            | boolean   | Vượt tất cả validations?
quality_issues                      | string    | Pipe-separated anomalies (VD: "NIGHT_RAD|EXTREME_TEMP")
quality_flag                        | string    | GOOD | CAUTION | REJECT
created_at                          | timestamp | Thời gian tạo record
updated_at                          | timestamp | Thời gian cập nhật
```

### Validation Rules (Bounds & Checks)

#### Hard Bounds (REJECT)

```python
# Hard bounds dựa trên giá trị vật lý không thể:
_numeric_columns = {
    "shortwave_radiation": (0.0, 1150.0),      # P99.5 = 1045, +5% = 1150
    "direct_radiation": (0.0, 1050.0),          # Actual max = 1009
    "diffuse_radiation": (0.0, 520.0),          # Actual max = 520
    "direct_normal_irradiance": (0.0, 1060.0), # Actual max = 1057
    "temperature_2m": (-10.0, 50.0),           # Extreme weather bounds
    "dew_point_2m": (-20.0, 30.0),             # Physical limits
    "wet_bulb_temperature_2m": (-5.0, 40.0),   # Bounded by air temp
    "cloud_cover": (0.0, 100.0),               # Perfect bounds
    "cloud_cover_low": (0.0, 100.0),
    "cloud_cover_mid": (0.0, 100.0),
    "cloud_cover_high": (0.0, 100.0),
    "precipitation": (0.0, 1000.0),            # Extreme event
    "sunshine_duration": (0.0, 3600.0),        # Max 1 hour/hour
    "total_column_integrated_water_vapour": (0.0, 100.0),
    "wind_speed_10m": (0.0, 50.0),             # Australian cyclone
    "wind_direction_10m": (0.0, 360.0),        # Perfect
    "wind_gusts_10m": (0.0, 120.0),            # Extreme weather
    "pressure_msl": (985.0, 1050.0),           # P99 = 1033
}

# Validation:
if value < min or value > max:
    quality_flag = "REJECT"
    quality_issues += f"{column}_OUT_OF_BOUNDS"
```

#### Soft Checks (CAUTION)

```python
# Soft checks phát hiện anomalies nhưng cho phép edge cases:

1. Night-time Radiation
   hour_of_day = HOUR(timestamp_local)
   is_night = (hour_of_day < 6) | (hour_of_day >= 22)
   is_night_rad_high = is_night & (shortwave_radiation > 100)
   → quality_issues += "NIGHT_RAD_ANOMALY"

2. Radiation Inconsistency
   (direct_radiation + diffuse_radiation) > (shortwave_radiation * 1.05)
   → quality_issues += "RADIATION_INCONSISTENCY"
   
3. High Cloud + Peak Sun (RELAXED threshold: 98% instead of 95%)
   is_peak_sun = (hour_of_day >= 10) & (hour_of_day <= 14)
   high_cloud_peak = (
       is_peak_sun & 
       (cloud_cover > 98) & 
       (shortwave_radiation < 600)
   )
   → quality_issues += "HIGH_CLOUD_LOW_RADIATION"
   
4. Extreme Temperature
   (temperature_2m < -10) | (temperature_2m > 45)
   → quality_issues += "EXTREME_TEMPERATURE"

# Quality Flag Assignment:
quality_flag = (
    "REJECT" if is_valid_bounds == False else
    "CAUTION" if any(soft_checks_failed) else
    "GOOD"
)
```

### Data Processing Pipeline

```python
# File: src/pv_lakehouse/etl/silver/hourly_weather.py

def transform(bronze_df: DataFrame) -> Optional[DataFrame]:
    """
    Transform Bronze weather to Silver clean_hourly_weather
    """
    # Step 1: Select + Filter
    prepared = bronze_df.select(
        "facility_code", "facility_name",
        F.col("weather_timestamp").cast("timestamp").alias("timestamp_local"),
        *[F.col(col) for col in numeric_columns.keys() if col in bronze_df.columns]
    ).where(F.col("facility_code").isNotNull())
    
    # Step 2: Aggregate to hourly (already in local timezone from Bronze)
    prepared = prepared.withColumn(
        "date_hour", F.date_trunc("hour", F.col("timestamp_local"))
    )
    
    # Step 3: Round numeric columns to 4 decimals
    for column in numeric_columns.keys():
        prepared = prepared.withColumn(
            column,
            F.round(F.col(column), 4)
        )
    
    # Step 4: Forward-fill missing total_column_integrated_water_vapour
    # (This column has 73% nulls but is not critical)
    window = Window.partitionBy("facility_code", "date").orderBy("timestamp_local")
    prepared = prepared.withColumn(
        "total_column_integrated_water_vapour",
        F.coalesce(
            F.col("total_column_integrated_water_vapour"),
            F.last(F.col("total_column_integrated_water_vapour"), ignorenulls=True).over(window)
        )
    )
    
    # Step 5: Validate + Flag
    hour_of_day = F.hour(F.col("timestamp_local"))
    is_night = (hour_of_day < 6) | (hour_of_day >= 22)
    
    is_night_rad_high = is_night & (F.col("shortwave_radiation") > 100)
    radiation_inconsistency = (
        (F.col("direct_radiation") + F.col("diffuse_radiation")) > 
        (F.col("shortwave_radiation") * 1.05)
    )
    high_cloud_peak = (
        is_peak_sun & 
        (F.col("cloud_cover") > 98) & 
        (F.col("shortwave_radiation") < 600)
    )
    extreme_temp = (F.col("temperature_2m") < -10) | (F.col("temperature_2m") > 45)
    
    result = result.withColumn(
        "quality_flag",
        F.when(
            ~is_valid_bounds, F.lit("REJECT")
        ).when(
            is_night_rad_high | radiation_inconsistency | high_cloud_peak | extreme_temp,
            F.lit("CAUTION")
        ).otherwise(F.lit("GOOD"))
    )
    
    return result
```

### Dữ Liệu Mẫu

```json
{
  "facility_code": "AVLSF",
  "facility_name": "Alexandria Solar Farm",
  "timestamp_local": "2025-11-22 14:00:00",
  "date_hour": "2025-11-22 14:00:00",
  "date": "2025-11-22",
  "shortwave_radiation": 845.2,
  "direct_radiation": 620.5,
  "diffuse_radiation": 224.7,
  "direct_normal_irradiance": 750.1,
  "temperature_2m": 28.5,
  "dew_point_2m": 15.3,
  "wet_bulb_temperature_2m": 21.8,
  "cloud_cover": 35.0,
  "cloud_cover_low": 10.0,
  "cloud_cover_mid": 15.0,
  "cloud_cover_high": 10.0,
  "precipitation": 0.0,
  "sunshine_duration": 3600.0,
  "total_column_integrated_water_vapour": 45.2,
  "wind_speed_10m": 8.5,
  "wind_direction_10m": 230.0,
  "wind_gusts_10m": 15.2,
  "pressure_msl": 1013.5,
  "is_valid": true,
  "quality_issues": "",
  "quality_flag": "GOOD",
  "created_at": "2025-11-22 14:05:00",
  "updated_at": "2025-11-22 14:05:00"
}
```

### Expected Data Quality

```
GOOD Records (95-100%): 
  ✅ All bounds checks pass
  ✅ No soft check anomalies
  → Use directly in model training

CAUTION Records (0-5%):
  ⚠️ Soft checks detected (e.g., high cloud + low radiation)
  ⚠️ Edge case but possibly valid
  → Use with reduced weight (0.5x) in training

REJECT Records (0%):
  ❌ Hard bounds violation (e.g., negative radiation)
  ❌ Physically impossible
  → Exclude from training
```

---

## ⚡ Silver Energy (`lh.silver.clean_hourly_energy`)

### Mục Đích & Transformation

**Mục đích:** Chuyển đổi dữ liệu năng lượng từ Bronze (5-15 min granularity) sang hourly aggregated, với quality flags.

**Transformation:**
```
Bronze raw_facility_timeseries (multiple 5-15min records)
    ↓
1. Filter: Only "energy" metrics, valid facility/timestamp
2. Convert: UTC → Facility local timezone
3. Aggregate: Group by hour (shifted +1 hour for label)
4. Validate: Check bounds + anomaly detection
5. Flag: Assign quality_flag + quality_issues
    ↓
Silver clean_hourly_energy (one record per facility per hour)
```

### Schema Chi Tiết

```
Column Name         | Type      | Mô Tả
--------------------|-----------|-------
facility_code       | string    | Mã facility
facility_name       | string    | Tên facility
network_code        | string    | Mã thị trường (NEM)
network_region      | string    | Vùng (NSW1)
date_hour           | timestamp | Giờ tròn (local timezone, PARTITION KEY)
energy_mwh          | double    | Năng lượng sinh ra (MWh)
intervals_count     | int       | Số khoảng thời gian trong giờ (expected: 4 for 15min or 12 for 5min)
completeness_pct    | double    | % completeness (currently always 100.0)
quality_flag        | string    | GOOD | CAUTION | REJECT
quality_issues      | string    | Pipe-separated issues (VD: "NIGHT_ENERGY_ANOMALY|PEAK_HOUR_LOW_ENERGY")
created_at          | timestamp | Thời gian tạo record
updated_at          | timestamp | Thời gian cập nhật
```

### Validation Rules

#### Hard Bounds (REJECT)

```python
ENERGY_LOWER = 0.0  # Solar cannot generate negative energy
PEAK_REFERENCE_MWH = 85.0  # Facility-level reference capacity

# Hard bounds check:
is_within_bounds = energy_mwh >= ENERGY_LOWER

if not is_within_bounds:
    quality_flag = "REJECT"
    quality_issues += "OUT_OF_BOUNDS"
```

#### Soft Checks (CAUTION)

```python
# 1. Night-time Energy Anomaly
is_night = ((hour >= 22) | (hour < 6))
is_night_anomaly = is_night & (energy_mwh > 1.0)
→ quality_issues += "NIGHT_ENERGY_ANOMALY"
REASON: Solar generates 0 at night, > 1 is clearly anomalous

# 2. Daytime Zero Energy (non-peak)
is_daytime_zero = (hour >= 8) & (hour <= 17) & (energy_mwh == 0.0)
→ quality_issues += "DAYTIME_ZERO_ENERGY"
REASON: Equipment downtime, maintenance

# 3. Equipment Downtime (peak hours)
is_peak = (hour >= 10) & (hour <= 14)
is_equipment_downtime = is_peak & (energy_mwh == 0.0)
→ quality_issues += "EQUIPMENT_DOWNTIME"
REASON: Critical during peak - indicates system failure

# 4. Transition Hour Low Energy (Ramp Issues)
# Sunrise (6:00-8:00): expect > 5% of peak
is_sunrise = (hour >= 6) & (hour < 8)
is_sunrise_low = is_sunrise & (energy > 0.01) & (energy < 0.05 * PEAK_REFERENCE_MWH)
→ quality_issues += "TRANSITION_HOUR_LOW_ENERGY"

# Early Morning (8:00-10:00): expect > 8% of peak
is_early_morning = (hour >= 8) & (hour < 10)
is_early_morning_low = is_early_morning & (energy > 0.01) & (energy < 0.08 * PEAK_REFERENCE_MWH)
→ quality_issues += "TRANSITION_HOUR_LOW_ENERGY"

# Sunset (17:00-19:00): expect > 10% of peak
is_sunset = (hour >= 17) & (hour < 19)
is_sunset_low = is_sunset & (energy > 0.01) & (energy < 0.10 * PEAK_REFERENCE_MWH)
→ quality_issues += "TRANSITION_HOUR_LOW_ENERGY"

# 5. Peak Hour Low Efficiency
is_peak_efficiency = (
    is_peak & 
    (energy > 0.5) & 
    (energy < 0.50 * PEAK_REFERENCE_MWH)
)
→ quality_issues += "PEAK_HOUR_LOW_ENERGY"
REASON: Efficiency < 50% at peak indicates equipment issue

# Quality Flag Assignment:
quality_flag = (
    "REJECT" if not is_within_bounds else
    "CAUTION" if any(soft_checks_failed) else
    "GOOD"
)
```

### Threshold Justification

```
PEAK_REFERENCE_MWH = 85.0 (facility average)

Sunrise (6:00-8:00): 5% threshold = 4.25 MWh
  → Morning ramp most predictable
  → Stricter threshold catches ramping issues

Early Morning (8:00-10:00): 8% threshold = 6.8 MWh
  → Weather variability increases
  → Looser threshold reduces false positives

Sunset (17:00-19:00): 10% threshold = 8.5 MWh
  → Unpredictable cloud patterns
  → Loosest threshold handles weather volatility

Peak Hour Efficiency: 50% threshold = 42.5 MWh
  → Significant efficiency loss likely indicates equipment problem
  → Conservative threshold avoids false positives
```

### Timezone & Hour Shifting

```python
# CRITICAL: Hour Shift for Energy Labels

# Bronze interval_ts: 2025-11-22 04:00 UTC (start of period)
#   Energy during [04:00-04:15), [04:15-04:30), etc.
#   Total: 49.76 MWh

# Aggregate: Sum all intervals in UTC hour [04:00-05:00)
#   = 12.45 + 12.38 + 12.52 + 12.41 = 49.76 MWh

# Convert to Facility Timezone (Sydney +10):
#   UTC 04:00 = Sydney 14:00

# Silver Label: date_hour = 2025-11-22 05:00 UTC (shifted +1 hour)
#   Why? Energy [04:00-05:00) represents work done BY 05:00
#   Aligns with end-of-period semantics in energy markets

# Result in Silver (LOCAL TIMEZONE):
#   date_hour = 2025-11-22 15:00+10:00 (Sydney)
#   energy_mwh = 49.76
```

### Data Processing Pipeline

```python
# File: src/pv_lakehouse/etl/silver/hourly_energy.py

def transform(bronze_df: DataFrame) -> Optional[DataFrame]:
    """Transform Bronze timeseries to Silver clean_hourly_energy"""
    
    # Step 1: Filter + Cast
    filtered = bronze_df.select(
        "facility_code", "facility_name", "network_code", "network_region",
        F.col("value").cast("double").alias("metric_value"),
        F.col("interval_ts").cast("timestamp").alias("interval_ts"),
    ).where(F.col("facility_code").isNotNull())\
     .where(F.col("metric").isin("energy", "power"))
    
    # Step 2: Convert UTC → Local timezone
    # Build per-facility timezone expression
    tz_expr = F.from_utc_timestamp(F.col("interval_ts"), DEFAULT_TIMEZONE)
    for code, tz in FACILITY_TIMEZONES.items():
        tz_expr = F.when(
            F.col("facility_code") == code,
            F.from_utc_timestamp(F.col("interval_ts"), tz)
        ).otherwise(tz_expr)
    
    # Step 3: Aggregate to hourly
    hourly = filtered.withColumn("timestamp_local", tz_expr)\
        .withColumn(
            "date_hour",
            F.date_trunc("hour", F.expr("timestamp_local + INTERVAL 1 HOUR"))
        )\
        .groupBy("facility_code", "facility_name", "network_code", "network_region", "date_hour")\
        .agg(
            F.sum(F.when(F.col("metric") == "energy", F.col("metric_value"))).alias("energy_mwh"),
            F.count(F.when(F.col("metric") == "energy", F.lit(1))).alias("intervals_count")
        )
    
    # Step 4: Validate + Flag (as described above)
    
    return result
```

### Dữ Liệu Mẫu

```json
{
  "facility_code": "AVLSF",
  "facility_name": "Alexandria Solar Farm",
  "network_code": "NEM",
  "network_region": "NSW1",
  "date_hour": "2025-11-22 14:00:00",
  "energy_mwh": 49.76,
  "intervals_count": 4,
  "completeness_pct": 100.0,
  "quality_flag": "GOOD",
  "quality_issues": "",
  "created_at": "2025-11-22 14:05:00",
  "updated_at": "2025-11-22 14:05:00"
}
```

### Expected Data Quality

```
GOOD Records (85-95%):
  ✅ Energy within bounds (>= 0)
  ✅ No anomalies detected
  → Use directly in model training (weight: 1.0)

CAUTION Records (5-15%):
  ⚠️ One or more soft checks failed
  ⚠️ Examples: night anomaly, daytime zero, low efficiency
  → Use with reduced weight (weight: 0.5)
  → Worth investigating patterns

REJECT Records (< 0.1%):
  ❌ Negative energy (hard bounds violation)
  ❌ Data corruption or sensor failure
  → Exclude from training
```

---

## 💨 Silver Air Quality (`lh.silver.clean_hourly_air_quality`)

### Mục Đích & Transformation

**Mục đích:** Chuyển đổi dữ liệu chất lượng không khí từ Bronze sang hourly, tính AQI, và gắn quality flags.

### Schema Chi Tiết

```
Column Name              | Type      | Mô Tả
------------------------|-----------|-------
facility_code           | string    | Mã facility
facility_name           | string    | Tên facility
timestamp               | timestamp | Thời gian (local timezone)
date_hour               | timestamp | Giờ tròn (PARTITION KEY)
date                    | date      | Ngày
pm2_5                   | double    | PM2.5 (µg/m³)
pm10                    | double    | PM10 (µg/m³)
dust                    | double    | Bụi tổng (µg/m³)
nitrogen_dioxide        | double    | NO₂ (ppb)
ozone                   | double    | O₃ (ppb)
sulphur_dioxide         | double    | SO₂ (ppb)
carbon_monoxide         | double    | CO (ppb)
uv_index                | double    | UV index
uv_index_clear_sky      | double    | UV clear sky
aqi_value               | int       | AQI (0-500+)
aqi_category            | string    | Good | Moderate | Unhealthy | Hazardous
is_valid                | boolean   | Vượt validations?
quality_flag            | string    | GOOD | CAUTION
quality_issues          | string    | Pipe-separated issues
created_at              | timestamp | Thời gian tạo
updated_at              | timestamp | Thời gian cập nhật
```

### Validation Rules

#### Hard Bounds (Check only, no REJECT)

```python
_numeric_columns = {
    "pm2_5": (0.0, 500.0),                    # 0-500 µg/m³
    "pm10": (0.0, 500.0),
    "dust": (0.0, 500.0),
    "nitrogen_dioxide": (0.0, 500.0),        # 0-500 ppb
    "ozone": (0.0, 500.0),
    "sulphur_dioxide": (0.0, 500.0),
    "carbon_monoxide": (0.0, 500.0),
    "uv_index": (0.0, 15.0),                 # 0-15
    "uv_index_clear_sky": (0.0, 15.0),
}

# Note: No REJECT flag for air quality (too strict)
# Out-of-bounds → CAUTION only
```

#### AQI Calculation (EPA Standard)

```python
def aqi_from_pm25(pm25: float) -> int:
    """Calculate EPA AQI from PM2.5 concentration"""
    
    # EPA Breakpoints for PM2.5 (µg/m³)
    breakpoints_pm25 = [12, 35.4, 55.4, 150.4, 250.4]
    breakpoints_aqi = [50, 100, 150, 200, 300, 500]
    
    if pm25 is None:
        return None
    
    if pm25 <= 12:
        # Good (0-50)
        return (50 / 12) * pm25
    elif pm25 <= 35.4:
        # Moderate (51-100)
        return 50 + (50 / (35.4 - 12)) * (pm25 - 12)
    elif pm25 <= 55.4:
        # Unhealthy for Sensitive (101-150)
        return 100 + (50 / (55.4 - 35.4)) * (pm25 - 35.4)
    elif pm25 <= 150.4:
        # Unhealthy (151-200)
        return 150 + (50 / (150.4 - 55.4)) * (pm25 - 55.4)
    elif pm25 <= 250.4:
        # Very Unhealthy (201-300)
        return 200 + (100 / (250.4 - 150.4)) * (pm25 - 150.4)
    else:
        # Hazardous (301+)
        return 300 + (200 / 1000) * (pm25 - 250.4)

# AQI Categories:
aqi_category = (
    "Good" if aqi <= 50 else
    "Moderate" if aqi <= 100 else
    "Unhealthy" if aqi <= 200 else
    "Hazardous"
)
```

#### Quality Flag Logic

```python
# Validation:
is_valid_bounds = True
for column, (min_val, max_val) in numeric_columns.items():
    if value < min_val or value > max_val:
        is_valid_bounds = False
        quality_issues += f"{column}_OUT_OF_BOUNDS"

aqi_valid = (aqi_value >= 0) & (aqi_value <= 500)
is_valid = is_valid_bounds & aqi_valid

quality_flag = "GOOD" if is_valid else "CAUTION"
# Note: No REJECT for air quality
```

### Dữ Liệu Mẫu

```json
{
  "facility_code": "AVLSF",
  "facility_name": "Alexandria Solar Farm",
  "timestamp": "2025-11-22 14:00:00",
  "date_hour": "2025-11-22 14:00:00",
  "date": "2025-11-22",
  "pm2_5": 12.5,
  "pm10": 18.2,
  "dust": 20.1,
  "nitrogen_dioxide": 15.3,
  "ozone": 45.2,
  "sulphur_dioxide": 2.1,
  "carbon_monoxide": 0.5,
  "uv_index": 8.2,
  "uv_index_clear_sky": 9.1,
  "aqi_value": 51,
  "aqi_category": "Moderate",
  "is_valid": true,
  "quality_flag": "GOOD",
  "quality_issues": "",
  "created_at": "2025-11-22 14:05:00",
  "updated_at": "2025-11-22 14:05:00"
}
```

### Expected Data Quality

```
GOOD Records (98-100%):
  ✅ All bounds valid
  ✅ AQI calculation valid
  → Use directly in analytics

CAUTION Records (0-2%):
  ⚠️ Out-of-bounds value or AQI error
  ⚠️ Rare occurrence
  → Investigate manually

REJECT Records (0%):
  (Air quality doesn't use REJECT)
```

---

## 🏗️ Silver Layer Architecture

### Base Loader Class

```python
# File: src/pv_lakehouse/etl/silver/base.py

class BaseSilverLoader:
    """Base class for all Silver loaders"""
    
    bronze_table: str           # Source table
    silver_table: str           # Target table
    timestamp_column: str       # Bronze timestamp for filtering
    partition_cols: tuple       # Partition columns (e.g., ("date_hour",))
    
    def run(self) -> int:
        """Main orchestration method"""
        bronze_df = self._read_bronze()
        rows_written = self._process_in_chunks(bronze_df, chunk_days=7)
        return rows_written
    
    def transform(self, bronze_df: DataFrame) -> DataFrame:
        """Override in subclasses"""
        raise NotImplementedError()
    
    def _read_bronze(self) -> DataFrame:
        """Read Bronze table with incremental date filter"""
        # Auto-detect start date from last loaded
        if mode == "incremental":
            max_ts = spark.sql(f"SELECT MAX({timestamp_column}) FROM {silver_table}").collect()[0][0]
            start_date = max_ts + 1 day if max_ts else default_date
        else:
            start_date = user_specified_date
        
        return spark.sql(f"SELECT * FROM {bronze_table} WHERE {timestamp_column} >= {start_date}")
    
    def _process_in_chunks(self, bronze_df: DataFrame, chunk_days: int) -> int:
        """Process in time chunks to limit concurrent partition writers"""
        # Break data into 7-day chunks
        # Transform each chunk
        # Write with merge strategy
        # Total rows written = sum of all chunks
```

### Load Options

```python
@dataclass
class LoadOptions:
    mode: str = "incremental"              # full | incremental
    start: Optional[datetime] = None       # Manual start date (backfill)
    end: Optional[datetime] = None         # Manual end date (backfill)
    load_strategy: str = "merge"           # overwrite | merge
    app_name: str = "silver-loader"
    target_file_size_mb: int = 128         # Iceberg file size
    max_records_per_file: int = 250_000    # Records per file
```

### Write Strategy

```
INCREMENTAL MODE (Default):
  1. Read last_loaded_timestamp from Silver table
  2. Fetch Bronze data AFTER last_loaded_timestamp
  3. Transform + validate
  4. MERGE write (append new, update existing)
  5. Result: Idempotent, can rerun safely

BACKFILL MODE:
  1. User specifies --start YYYY-MM-DD --end YYYY-MM-DD
  2. Fetch all Bronze data in range
  3. Transform + validate
  4. OVERWRITE write (delete old, write new)
  5. Result: Rebuild specific period

MERGE Strategy:
  - Used for incremental loads
  - Iceberg merges duplicates by primary key
  - Preserves historical accuracy
```

---

## 📁 Silver Layer File Structure

```
src/pv_lakehouse/etl/silver/
├── base.py                      # BaseSilverLoader + LoadOptions
├── hourly_weather.py            # SilverHourlyWeatherLoader
├── hourly_energy.py             # SilverHourlyEnergyLoader
├── hourly_air_quality.py        # SilverHourlyAirQualityLoader
├── facility_master.py           # SilverFacilityMasterLoader
├── cli.py                       # Command-line interface
├── __init__.py
└── __pycache__/
```

### Key Files

**1. `base.py` - Base Infrastructure**
- `BaseSilverLoader`: Base class for all loaders
- `LoadOptions`: Configuration dataclass
- Incremental date detection
- Chunk processing logic
- Write strategy (merge vs overwrite)

**2. `hourly_weather.py` - Weather Transformation**
- 17 numeric columns with bounds
- Soft checks (night radiation, inconsistency, etc.)
- Forward-fill for water vapor column
- Quality flags

**3. `hourly_energy.py` - Energy Transformation**
- UTC → Local timezone conversion
- 7-day chunk processing
- 5 types of anomaly detection
- Quality flags (GOOD/CAUTION/REJECT)

**4. `hourly_air_quality.py` - Air Quality Transformation**
- 10 metrics with bounds
- EPA AQI calculation
- AQI category assignment
- Quality flags (GOOD/CAUTION)

**5. `facility_master.py` - Master Dimension**
- Load facility reference data
- Coordinates, timezone, capacity
- Dimension table for joins

**6. `cli.py` - CLI Interface**
```python
@click.group()
def cli():
    """Silver layer loader CLI"""
    pass

@cli.command()
@click.option("--mode", default="incremental", type=click.Choice(["incremental", "backfill"]))
@click.option("--start", type=click.DateTime())
@click.option("--end", type=click.DateTime())
def weather(mode, start, end):
    """Load Silver weather"""
    loader = SilverHourlyWeatherLoader(LoadOptions(mode=mode, start=start, end=end))
    rows = loader.run()
    print(f"Loaded {rows} weather records")

@cli.command()
def energy(mode, start, end):
    """Load Silver energy"""
    # Similar

@cli.command()
def air_quality(mode, start, end):
    """Load Silver air quality"""
    # Similar
```

---

## 🚀 Chạy Silver Load Jobs

### Command Line Interface

```bash
# 1. Load Weather (Incremental - default)
python -m pv_lakehouse.etl.silver.cli weather

# 2. Load Weather (Backfill specific period)
python -m pv_lakehouse.etl.silver.cli weather \
  --mode backfill \
  --start 2025-01-01 \
  --end 2025-01-31

# 3. Load Energy (Incremental)
python -m pv_lakehouse.etl.silver.cli energy

# 4. Load Air Quality (Backfill)
python -m pv_lakehouse.etl.silver.cli air-quality \
  --mode backfill \
  --start 2025-11-01 \
  --end 2025-11-22

# 5. Load All in Order
python -m pv_lakehouse.etl.silver.cli weather && \
python -m pv_lakehouse.etl.silver.cli energy && \
python -m pv_lakehouse.etl.silver.cli air-quality
```

### Prefect Orchestration

```python
# Via Prefect (workflow orchestration):
prefect deployment run silver-weather-load
prefect deployment run silver-energy-load --interval=1h
```

---

## 📊 Performance & Scalability

### Typical Load Times

| Data Type | Date Range | Records | Time | Rate |
|-----------|-----------|---------|------|------|
| Weather | 30 days | 17,280 | 2 min | 144 rec/sec |
| Energy | 30 days | 69,120 | 3 min | 384 rec/sec |
| Air Quality | 30 days | 17,280 | 2 min | 144 rec/sec |

### Memory Usage

```
Weather (7-day chunk):
  - Input: 17,280 records × 20 bytes = 346 KB
  - Spark DF (serialized): ~5 MB
  - Memory peak: ~50 MB (with overhead)

Energy (7-day chunk):
  - Input: 69,120 records × 8 bytes = 553 KB
  - Spark DF (serialized): ~8 MB
  - Memory peak: ~80 MB (with overhead)

Air Quality (7-day chunk):
  - Input: 17,280 records × 16 bytes = 276 KB
  - Spark DF (serialized): ~4 MB
  - Memory peak: ~40 MB (with overhead)
```

### Storage Size

```
Silver Layer (2 years, 20 facilities):

Weather:
  Records: 2×365×24×20 = 350,400
  Size: ~400 MB (compressed Iceberg + quality flags)

Energy:
  Records: 2×365×24×20 = 350,400
  Size: ~200 MB (compressed Iceberg + quality flags)

Air Quality:
  Records: 2×365×24×20 = 350,400
  Size: ~300 MB (compressed Iceberg + quality flags)

Total Silver: ~900 MB
```

---

## ✅ Best Practices

1. **Always Start with Incremental**
   - Faster, uses fewer resources
   - Only backfill when rebuilding

2. **Monitor Quality Flags**
   - Track % CAUTION vs % GOOD
   - Alert if CAUTION > 20%

3. **Test Transforms Locally**
   - Use --start YYYY-MM-01 --end YYYY-MM-02 first
   - Verify output before full load

4. **Merge vs Overwrite**
   - Incremental: Use merge (preserves history)
   - Backfill: Use overwrite (rebuild period)

5. **Quality Validation**
   - Check record counts: Bronze vs Silver
   - Verify all quality_flags are assigned
   - Validate no unexpected NULL values

6. **Timezone Verification**
   - Confirm all timestamps in local timezone
   - Energy should be UTC → local (done in Silver)
   - Weather should already be local (from Bronze)

---

## 🔍 Monitoring & Debugging

### Health Checks

```sql
-- Check data freshness
SELECT 
    'Weather' as layer,
    MAX(date_hour) as last_update,
    CURRENT_TIMESTAMP - MAX(date_hour) as lag
FROM silver.clean_hourly_weather
UNION ALL
SELECT 
    'Energy',
    MAX(date_hour),
    CURRENT_TIMESTAMP - MAX(date_hour)
FROM silver.clean_hourly_energy
UNION ALL
SELECT 
    'Air Quality',
    MAX(date_hour),
    CURRENT_TIMESTAMP - MAX(date_hour)
FROM silver.clean_hourly_air_quality;

-- Check quality flag distribution
SELECT 
    quality_flag,
    COUNT(*) as record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
FROM silver.clean_hourly_energy
WHERE date_hour >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY quality_flag;

-- Find anomalies
SELECT 
    facility_code, date_hour, energy_mwh, quality_flag, quality_issues
FROM silver.clean_hourly_energy
WHERE quality_flag IN ('CAUTION', 'REJECT')
ORDER BY date_hour DESC
LIMIT 100;
```

---

## 📞 Liên Hệ & Hỗ Trợ

**Documentation:** See files in `src/pv_lakehouse/etl/silver/`  
**Issues:** Check Silver layer logs  
**Questions:** Ask Data Engineering team

---

**Document Version:** 1.0  
**Last Updated:** 2025-11-22  
**Scope:** Complete Silver layer architecture, transformation, validation, and operations
