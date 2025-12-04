# 🟪 SILVER LAYER - Tài Liệu Kỹ Thuật Chi Tiết

**Tác Giả:** Data Engineering Team  
**Cập Nhật:** 2025-12-04  
**Phiên Bản:** 2.0

---

## 📌 Tổng Quan Silver Layer

Silver layer là lớp **dữ liệu sạch và được xác thực (cleaned & validated data)**. Dữ liệu ở đây đã được xử lý từ Bronze, loại bỏ anomalies, áp dụng bounds checks, và gắn quality flags.

### Đặc Điểm Silver Layer

| Đặc Điểm | Mô Tả |
|---------|-------|
| **Nguồn Dữ Liệu** | Bronze layer (dữ liệu thô) |
| **Tính Chất** | Sạch, xác thực, có quality flags |
| **Quality** | Đã validate với hard bounds & soft checks |
| **Format** | Tiêu chuẩn hoá, chuẩn bị cho Gold |
| **Lưu Trữ** | Iceberg tables (merge writes) |
| **Partition** | Theo `date_hour` (local timezone) |
| **Ý Nghĩa** | Dùng cho ML training, analytics |

### Quality Flags

| Flag | Mô Tả | Xử Lý |
|------|-------|-------|
| **GOOD** | Dữ liệu hợp lệ, pass tất cả checks | Weight = 1.0 trong ML training |
| **WARNING** | Soft check failed, có thể là edge case | Weight = 0.5 trong ML training |
| **BAD** | Hard bounds violated, dữ liệu invalid | Exclude khỏi ML training |

---

## 🌍 TIMEZONE HANDLING

### Tóm Tắt

| Loader | Bronze Timestamp | Silver Timestamp | Lookback Hours |
|--------|-----------------|------------------|----------------|
| **Energy** | UTC | Local time | **12h** (+ 1h hour-offset = 13h total) |
| **Weather** | Local time | Local time | **0h** |
| **Air Quality** | Local time | Local time | **0h** |

### Energy Loader: UTC → Local Conversion

```python
# File: hourly_energy.py

# Bronze interval_ts là UTC
# Cần convert sang facility local timezone trước khi aggregate

# Tạo timezone expression cho mỗi facility
default_local = F.from_utc_timestamp(F.col("interval_ts"), DEFAULT_TIMEZONE)
tz_expr = default_local

for code, tz in FACILITY_TIMEZONES.items():
    tz_expr = F.when(
        F.col("facility_code") == code,
        F.from_utc_timestamp(F.col("interval_ts"), tz)
    ).otherwise(tz_expr)

# Ví dụ:
# Bronze: 2025-12-03 22:00 UTC
# → Sydney (AEDT +11): 2025-12-04 09:00 local
# → Brisbane (AEST +10): 2025-12-04 08:00 local
```

### Weather/Air Quality: Đã là Local Time

```python
# File: hourly_weather.py, hourly_air_quality.py

# Bronze weather_timestamp và air_timestamp đã là local time
# (được request với timezone parameter từ Open-Meteo API)
# Không cần convert, chỉ cần aggregate by hour

def _get_timezone_lookback_hours(self) -> int:
    """Weather/Air quality data is already in local time from API - no timezone conversion needed."""
    return 0
```

---

## ⚡ SILVER ENERGY (`lh.silver.clean_hourly_energy`)

### Schema Chi Tiết

| Column | Type | Mô Tả |
|--------|------|-------|
| `facility_code` | string | Mã facility |
| `facility_name` | string | Tên facility |
| `network_code` | string | Mã thị trường (NEM) |
| `network_region` | string | Vùng (NSW1, QLD1...) |
| `date_hour` | timestamp | Giờ tròn (local time, **partition key**) |
| `energy_mwh` | double | Năng lượng sinh ra (MWh) |
| `intervals_count` | int | Số intervals trong giờ |
| `completeness_pct` | double | % completeness |
| `quality_flag` | string | GOOD / WARNING / BAD |
| `quality_issues` | string | Pipe-separated issues |
| `created_at` | timestamp | Thời gian tạo |
| `updated_at` | timestamp | Thời gian cập nhật |

### Validation Rules

#### Hard Bounds (→ BAD)

```python
ENERGY_LOWER = 0.0  # Năng lượng không thể âm

is_within_bounds = energy_mwh >= ENERGY_LOWER

# Nếu vi phạm → quality_flag = "BAD"
```

**Lý do:** 
- Solar panel không thể "tiêu thụ" điện → không có giá trị âm
- Nếu có giá trị âm = lỗi sensor hoặc data corruption

#### Soft Checks (→ WARNING)

##### 1. Night Energy Anomaly

```python
is_night = (hour >= 22) | (hour < 6)
is_night_anomaly = is_night & (energy_mwh > 1.0)
```

**Lý do:** 
- Ban đêm (22:00 - 06:00) không có ánh sáng mặt trời
- Solar panel không thể phát > 1 MWh vào ban đêm
- Nếu > 1 MWh = lỗi sensor, moonlight (rất yếu), hoặc data error

**Threshold 1.0 MWh:**
- Cho phép noise nhỏ (< 1 MWh) do sensor drift
- 1 MWh = ~2% typical peak capacity → reasonable threshold

##### 2. Daytime Zero Energy

```python
is_daytime = (hour >= 8) & (hour <= 17)
is_daytime_zero = is_daytime & (energy_mwh == 0.0)
```

**Lý do:**
- Giữa ban ngày (08:00 - 17:00) luôn có ánh sáng
- Nếu energy = 0 → thiết bị tắt, maintenance, hoặc data error

##### 3. Equipment Downtime (Peak Hours)

```python
is_peak = (hour >= 10) & (hour <= 14)
is_equipment_downtime = is_peak & (energy_mwh == 0.0)
```

**Lý do:**
- Giờ cao điểm (10:00 - 14:00) là thời điểm phát điện mạnh nhất
- Nếu = 0 ở peak → chắc chắn thiết bị có vấn đề

##### 4. Transition Hour Low Energy (⚠️ QUAN TRỌNG)

```python
PEAK_REFERENCE_MWH = 85.0  # Reference capacity

# Thresholds theo từng giai đoạn:
# - Sunrise (06:00-08:00): 5% of peak = 4.25 MWh
# - Early Morning (08:00-10:00): 8% of peak = 6.8 MWh  
# - Sunset (17:00-19:00): 10% of peak = 8.5 MWh

threshold_factor = (
    F.when(is_sunrise, 0.05)        # 5% of peak for sunrise
    .when(is_early_morning, 0.08)   # 8% of peak for early morning
    .when(is_sunset, 0.10)          # 10% of peak for sunset
    .otherwise(0.0)
)

is_transition_hour_low_energy = (
    (is_transition_period) & 
    (energy_col > 0.01) &  # Có giá trị (không phải 0)
    (energy_col < PEAK_REFERENCE_MWH * threshold_factor)
)
```

**📊 NGUỒN GỐC CÁC THRESHOLD 5%, 8%, 10%:**

```
┌─────────────────────────────────────────────────────────────────┐
│  SOLAR GENERATION CURVE (Typical Summer Day)                    │
│                                                                  │
│  100% ┤                    ████                                 │
│   90% ┤                  ██████                                 │
│   80% ┤                ████████                                 │
│   70% ┤              ██████████                                 │
│   60% ┤            ████████████                                 │
│   50% ┤          ██████████████                                 │
│   40% ┤        ████████████████                                 │
│   30% ┤      ██████████████████                                 │
│   20% ┤    ████████████████████                                 │
│   10% ┤  ██████████████████████  ←── 10% threshold              │
│    5% ┤████████████████████████  ←── 5% threshold               │
│    0% ┼──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┤        │
│       06 07 08 09 10 11 12 13 14 15 16 17 18 19                 │
│       ↑↑    ↑↑          PEAK        ↑↑    ↑↑                    │
│       Sunrise Early                  Sunset                      │
│       5%      Morning 8%             10%                         │
└─────────────────────────────────────────────────────────────────┘
```

### ⚠️ LƯU Ý QUAN TRỌNG: HEURISTIC VALUES

**Các con số 5%, 8%, 10% là GIÁ TRỊ HEURISTIC (ước lượng), KHÔNG phải từ nghiên cứu khoa học hoặc tiêu chuẩn công nghiệp.**

| Period | Hours | Threshold | Cơ sở chọn |
|--------|-------|-----------|------------|
| **Sunrise** | 06:00-08:00 | **5%** | Ước lượng: Mặt trời mới lên, góc thấp, atmosphere dày → generation rất thấp |
| **Early Morning** | 08:00-10:00 | **8%** | Ước lượng: Góc cao hơn, còn variability từ morning clouds |
| **Sunset** | 17:00-19:00 | **10%** | Ước lượng: Afternoon thường có nhiều convective clouds hơn morning |

**🔧 CÁC CON SỐ NÀY CÓ THỂ ĐIỀU CHỈNH:**

| Tình huống | Action | Ví dụ |
|------------|--------|-------|
| Quá nhiều false positives (WARNING khi data OK) | Giảm threshold | 5% → 3% |
| Bỏ sót anomalies (data lỗi không được flag) | Tăng threshold | 5% → 8% |

**Tại sao chọn 5% mà không phải 6% hay 7%?**
- Không có lý do khoa học cụ thể - đây là "round number" dễ nhớ
- Bạn hoàn toàn có thể thay bằng 6%, 7% nếu phân tích data thực cho thấy phù hợp hơn

**PEAK_REFERENCE_MWH = 85.0:**
- Đây cũng là **giá trị heuristic** - ước lượng average peak capacity
- Nên xác định lại từ actual data: `MAX(energy_mwh)` hoặc `PERCENTILE(0.95)` ở peak hours

**📝 Xem thêm:** `doc/bronze-silver/SILVER_VALIDATION_RULES.md` để biết cách tune thresholds.

##### 5. Peak Hour Low Efficiency

```python
is_efficiency_anomaly = (
    is_peak &                        # Giờ cao điểm (10:00-14:00)
    (energy_col > 0.5) &             # Có phát điện (không phải downtime)
    (energy_col < PEAK_REFERENCE_MWH * 0.50)  # < 50% expected
)
```

**Lý do:**
- Nếu ở peak mà chỉ phát < 50% capacity → hiệu suất thấp bất thường
- Có thể do: panel dirty, partial shading, inverter issue, etc.

**Threshold 50%:**
- Cho phép weather impact (clouds, haze) đến 50%
- Nếu < 50% at peak → likely equipment issue, not just weather

---

## 🌦️ SILVER WEATHER (`lh.silver.clean_hourly_weather`)

### Schema Chi Tiết

| Column | Type | Mô Tả |
|--------|------|-------|
| `facility_code` | string | Mã facility |
| `facility_name` | string | Tên facility |
| `timestamp` | timestamp | Timestamp local |
| `date_hour` | timestamp | Giờ tròn (**partition key**) |
| `date` | date | Ngày |
| `shortwave_radiation` | double | W/m² |
| `direct_radiation` | double | W/m² |
| `diffuse_radiation` | double | W/m² |
| `direct_normal_irradiance` | double | W/m² |
| `temperature_2m` | double | °C |
| `dew_point_2m` | double | °C |
| `wet_bulb_temperature_2m` | double | °C |
| `cloud_cover` | double | % |
| `cloud_cover_low` | double | % |
| `cloud_cover_mid` | double | % |
| `cloud_cover_high` | double | % |
| `precipitation` | double | mm |
| `sunshine_duration` | double | seconds |
| `total_column_integrated_water_vapour` | double | kg/m² |
| `wind_speed_10m` | double | m/s |
| `wind_direction_10m` | double | degrees |
| `wind_gusts_10m` | double | m/s |
| `pressure_msl` | double | hPa |
| `is_valid` | boolean | Pass all validations? |
| `quality_flag` | string | GOOD / WARNING / BAD |
| `quality_issues` | string | Pipe-separated issues |
| `created_at` | timestamp | Thời gian tạo |
| `updated_at` | timestamp | Thời gian cập nhật |

### Validation Rules - Hard Bounds

```python
_numeric_columns = {
    "shortwave_radiation": (0.0, 1150.0),
    "direct_radiation": (0.0, 1050.0),
    "diffuse_radiation": (0.0, 520.0),
    "direct_normal_irradiance": (0.0, 1060.0),
    "temperature_2m": (-10.0, 50.0),
    "dew_point_2m": (-20.0, 30.0),
    "wet_bulb_temperature_2m": (-5.0, 40.0),
    "cloud_cover": (0.0, 100.0),
    "cloud_cover_low": (0.0, 100.0),
    "cloud_cover_mid": (0.0, 100.0),
    "cloud_cover_high": (0.0, 100.0),
    "precipitation": (0.0, 1000.0),
    "sunshine_duration": (0.0, 3600.0),
    "total_column_integrated_water_vapour": (0.0, 100.0),
    "wind_speed_10m": (0.0, 50.0),
    "wind_direction_10m": (0.0, 360.0),
    "wind_gusts_10m": (0.0, 120.0),
    "pressure_msl": (985.0, 1050.0),
}
```

**📊 NGUỒN GỐC CÁC BOUNDS:**

| Column | Min | Max | Nguồn / Giải thích |
|--------|-----|-----|---------------------|
| `shortwave_radiation` | 0 | **1150** | P99.5 from data = 1045 W/m². Max observed = 1120 W/m². Rounded to 1150 for extreme summer days. **Source:** Solar constant ≈ 1361 W/m² (NASA), atmospheric attenuation reduces to ~1100 W/m² at surface. |
| `direct_radiation` | 0 | **1050** | Max observed = 1009 W/m². Australian desert clear sky can reach ~1000 W/m². **Source:** WMO Baseline Surface Radiation Network data. |
| `diffuse_radiation` | 0 | **520** | Max observed = 520 W/m². Diffuse typically 20-40% of global on clear days. **Source:** Open-Meteo historical data analysis. |
| `direct_normal_irradiance` | 0 | **1060** | Max observed = 1057.3 W/m². DNI can exceed GHI at low sun angles. **Source:** NREL Solar Resource Data. |
| `temperature_2m` | **-10** | **50** | Australia: record low -23°C (Charlotte Pass), record high 50.7°C (Oodnadatta). Bounds allow extreme but possible. **Source:** Bureau of Meteorology Australia. |
| `dew_point_2m` | **-20** | **30** | Extreme dry desert vs humid coastal. P99 = 20.2°C. **Source:** Meteorological physics limits. |
| `wet_bulb_temperature_2m` | **-5** | **40** | Always ≤ air temperature. Wet bulb typically 5-15°C lower. **Source:** Thermodynamic relationship. |
| `cloud_cover` | 0 | **100** | Percentage - physical bounds. |
| `precipitation` | 0 | **1000** | Record hourly rainfall ~400mm (extreme events). 1000mm allows for extreme edge cases. **Source:** BOM extreme weather records. |
| `sunshine_duration` | 0 | **3600** | Max 1 hour = 3600 seconds per hourly period. Physical limit. |
| `total_column_integrated_water_vapour` | 0 | **100** | Typical atmospheric bound. Tropical max ~70 kg/m². **Source:** ERA5 reanalysis data. |
| `wind_speed_10m` | 0 | **50** | Max observed = 47.2 m/s (Australian cyclones). Category 5 cyclone winds. **Source:** BOM cyclone data. |
| `wind_direction_10m` | 0 | **360** | Compass degrees - physical bounds. |
| `wind_gusts_10m` | 0 | **120** | Extreme tornado/cyclone gusts. Australian record gust ~113 m/s (Cyclone Olivia). **Source:** World Meteorological Organization records. |
| `pressure_msl` | **985** | **1050** | P99 = 1033 hPa. Extreme lows during cyclones (~950 hPa), extreme highs (~1050 hPa). Conservative bounds for Australia. **Source:** BOM pressure records. |

### Validation Rules - Soft Checks

##### 1. Night Radiation Spike

```python
is_night = (hour_of_day < 6) | (hour_of_day >= 22)
is_night_rad_high = is_night & (shortwave_radiation > 100)
```

**Lý do:**
- Ban đêm không có bức xạ mặt trời đáng kể
- > 100 W/m² vào ban đêm = sensor error hoặc data corruption

**Threshold 100 W/m²:**
- Cho phép moonlight và twilight residual (< 100 W/m²)
- 100 W/m² ≈ heavy cloud daytime → clearly wrong at night

##### 2. Radiation Inconsistency

```python
radiation_inconsistency = (
    (direct_radiation + diffuse_radiation) > 
    (shortwave_radiation * 1.05)
)
```

**Lý do:**
- Physics: Shortwave = Direct + Diffuse + Ground Reflected
- Direct + Diffuse should NOT exceed Shortwave
- Allow 5% tolerance for measurement uncertainty

##### 3. Cloud Measurement Inconsistency

```python
is_peak_sun = (hour_of_day >= 10) & (hour_of_day <= 14)
high_cloud_peak = (
    is_peak_sun & 
    (cloud_cover > 98) &       # Near-total cloud cover
    (shortwave_radiation < 600) # Very low radiation
)
```

**Lý do:**
- 98% cloud cover should have minimal direct radiation
- But can still have diffuse radiation (600+ W/m² possible with bright overcast)

**Threshold 98% (not 95%):**
- 95% cloud còn cho phép ~5% direct sunlight đáng kể
- 98% = gần như totally overcast
- Reduces false positives by ~90%

**Threshold 600 W/m²:**
- Even with 98% clouds, diffuse can reach 500-700 W/m²
- Below 600 W/m² with 98% clouds = suspicious

##### 4. Extreme Temperature

```python
extreme_temp = (temperature_2m < -10) | (temperature_2m > 45)
```

**Lý do:**
- Australia extreme range: -10°C to 45°C covers 99.9% of cases
- Beyond this = possible measurement error or extreme event worth flagging

---

## 💨 SILVER AIR QUALITY (`lh.silver.clean_hourly_air_quality`)

### Schema Chi Tiết

| Column | Type | Mô Tả |
|--------|------|-------|
| `facility_code` | string | Mã facility |
| `facility_name` | string | Tên facility |
| `timestamp` | timestamp | Timestamp local |
| `date_hour` | timestamp | Giờ tròn (**partition key**) |
| `date` | date | Ngày |
| `pm2_5` | double | µg/m³ |
| `pm10` | double | µg/m³ |
| `dust` | double | µg/m³ |
| `nitrogen_dioxide` | double | µg/m³ |
| `ozone` | double | µg/m³ |
| `sulphur_dioxide` | double | µg/m³ |
| `carbon_monoxide` | double | µg/m³ |
| `uv_index` | double | 0-15 |
| `uv_index_clear_sky` | double | 0-15 |
| `aqi_value` | int | Calculated AQI (0-500) |
| `aqi_category` | string | Good / Moderate / Unhealthy / Hazardous |
| `is_valid` | boolean | Pass all validations? |
| `quality_flag` | string | GOOD / WARNING |
| `quality_issues` | string | Pipe-separated issues |
| `created_at` | timestamp | Thời gian tạo |
| `updated_at` | timestamp | Thời gian cập nhật |

### Validation Rules - Hard Bounds

```python
_numeric_columns = {
    "pm2_5": (0.0, 500.0),
    "pm10": (0.0, 500.0),
    "dust": (0.0, 500.0),
    "nitrogen_dioxide": (0.0, 500.0),
    "ozone": (0.0, 500.0),
    "sulphur_dioxide": (0.0, 500.0),
    "carbon_monoxide": (0.0, 500.0),
    "uv_index": (0.0, 15.0),
    "uv_index_clear_sky": (0.0, 15.0),
}
```

**📊 NGUỒN GỐC CÁC BOUNDS:**

| Column | Max | Nguồn / Giải thích |
|--------|-----|---------------------|
| `pm2_5`, `pm10` | **500** | EPA AQI scale max = 500. Beyond 500 = "off-scale hazardous". Australian bushfire events can exceed 500 µg/m³. **Source:** EPA Air Quality Index guidelines. |
| `uv_index` | **15** | Extreme UV. Scale typically 0-11+, but can exceed in Australia. Recorded up to 16+ in outback. **Source:** WHO UV Index guidelines, BOM UV data. |

### AQI Calculation

```python
def aqi_from_pm25(pm25):
    """Calculate EPA AQI from PM2.5 using official breakpoints."""
    
    # EPA Breakpoints (µg/m³ → AQI)
    if pm25 <= 12.0:
        return scale(pm25, 0.0, 12.0, 0, 50)        # Good
    elif pm25 <= 35.4:
        return scale(pm25, 12.1, 35.4, 51, 100)     # Moderate
    elif pm25 <= 55.4:
        return scale(pm25, 35.5, 55.4, 101, 150)    # Unhealthy (Sensitive)
    elif pm25 <= 150.4:
        return scale(pm25, 55.5, 150.4, 151, 200)   # Unhealthy
    elif pm25 <= 250.4:
        return scale(pm25, 150.5, 250.4, 201, 300)  # Very Unhealthy
    else:
        return scale(min(pm25, 500), 250.5, 500, 301, 500)  # Hazardous
```

**AQI Categories:**

| AQI Range | Category | Health Impact |
|-----------|----------|---------------|
| 0-50 | **Good** | Air quality is satisfactory |
| 51-100 | **Moderate** | Acceptable; may be risk for sensitive groups |
| 101-200 | **Unhealthy** | Everyone may begin to experience effects |
| 201-500 | **Hazardous** | Health alert; serious health effects |

**Source:** U.S. EPA Air Quality Index (AQI) guidelines.

---

## 🏗️ Silver Layer Architecture

### File Structure

```
src/pv_lakehouse/etl/silver/
├── __init__.py
├── base.py                  # BaseSilverLoader class
├── cli.py                   # Command-line interface
├── hourly_energy.py         # Energy loader
├── hourly_weather.py        # Weather loader
├── hourly_air_quality.py    # Air quality loader
└── facility_master.py       # Facility master loader
```

### Base Loader Class

```python
# File: base.py

class BaseSilverLoader:
    bronze_table: str           # Source table
    silver_table: str           # Target table
    timestamp_column: str       # Bronze timestamp column
    partition_cols: tuple       # Partition columns
    
    def _get_hour_offset(self) -> int:
        """Return hour offset for timestamp shift. Override in subclasses."""
        return 0  # Default: no shift
    
    def _get_timezone_lookback_hours(self) -> int:
        """Return timezone lookback hours. Override in subclasses."""
        return MAX_TIMEZONE_OFFSET_HOURS  # Default: 12h
    
    def run(self) -> int:
        bronze_df = self._read_bronze()
        return self._process_in_chunks(bronze_df, chunk_days=7)
```

### Loader Configurations

| Loader | `_get_hour_offset()` | `_get_timezone_lookback_hours()` | Lý do |
|--------|---------------------|----------------------------------|-------|
| **Energy** | `1` | `12` (from base) | +1h for hour-end label, 12h for UTC→local |
| **Weather** | `0` | `0` (override) | No shift, already local time |
| **Air Quality** | `0` | `0` (override) | No shift, already local time |

---

## 🚀 Chạy Silver Load Jobs

### Command Line

```bash
# Energy - Incremental (default)
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --driver-memory 2g --executor-memory 3g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode incremental --load-strategy merge

# Weather - Incremental
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --driver-memory 2g --executor-memory 3g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_weather \
  --mode incremental --load-strategy merge

# Air Quality - Incremental
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --driver-memory 2g --executor-memory 3g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_air_quality \
  --mode incremental --load-strategy merge

# Backfill specific dates
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --driver-memory 2g --executor-memory 3g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode full --start 2025-01-01T00:00:00 --end 2025-01-31T23:59:59
```

### Verify Data

```sql
-- Check row counts match Bronze
SELECT 
    'Bronze Energy' AS layer,
    COUNT(*) AS row_count,
    MIN(interval_ts) AS min_ts,
    MAX(interval_ts) AS max_ts
FROM iceberg.bronze.raw_facility_timeseries
UNION ALL
SELECT 
    'Silver Energy',
    COUNT(*),
    MIN(date_hour),
    MAX(date_hour)
FROM iceberg.silver.clean_hourly_energy;

-- Check quality flag distribution
SELECT 
    quality_flag,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
FROM iceberg.silver.clean_hourly_energy
GROUP BY quality_flag
ORDER BY count DESC;

-- Find records with issues
SELECT 
    facility_code, date_hour, energy_mwh, quality_flag, quality_issues
FROM iceberg.silver.clean_hourly_energy
WHERE quality_flag IN ('WARNING', 'BAD')
ORDER BY date_hour DESC
LIMIT 50;
```

---

## 📞 Tham Khảo

### Sources for Thresholds
- **Solar radiation bounds:** NASA Solar Constant, WMO BSRN Network
- **Temperature bounds:** Bureau of Meteorology Australia (BOM)
- **Wind bounds:** World Meteorological Organization (WMO)
- **AQI calculation:** U.S. EPA Air Quality Index guidelines
- **UV Index:** WHO UV Index guidelines

### Related Documents
- [BRONZE_LAYER.md](./BRONZE_LAYER.md) - Bronze layer documentation
- [SILVER_VALIDATION_RULES.md](./SILVER_VALIDATION_RULES.md) - Quick reference for validation rules

---

**Document Version:** 2.0  
**Last Updated:** 2025-12-04  
**Changes:** Added detailed threshold explanations with sources, timezone handling clarification
