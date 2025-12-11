# 🟫 BRONZE LAYER - Tài Liệu Kỹ Thuật Chi Tiết

**Tác Giả:** Data Engineering Team  
**Cập Nhật:** 2025-12-04  
**Phiên Bản:** 2.0

---

## 📌 Tổng Quan Bronze Layer

Bronze layer là lớp **dữ liệu thô (raw data)** trực tiếp từ các nguồn API bên ngoài. Dữ liệu ở đây chưa được xử lý, chưa được làm sạch, và có thể chứa các anomalies, missing values, hoặc dữ liệu không hợp lệ.

### Đặc Điểm Bronze Layer

| Đặc Điểm | Mô Tả |
|---------|-------|
| **Nguồn Dữ Liệu** | API bên ngoài (Open-Meteo, OpenElectricity) |
| **Tính Chất** | Thô, không xử lý |
| **Quality** | Không được validate, có thể có lỗi |
| **Format** | Dữ liệu gốc từ API (JSON/CSV) |
| **Lưu Trữ** | Iceberg tables (append-only) |
| **Partition** | Theo ngày |
| **Ý Nghĩa** | Dùng làm backup, audit trail |

---

## 🌍 TIMEZONE - KIẾN THỨC QUAN TRỌNG

### Tóm Tắt Timezone Handling

| Data Type | Bronze Timestamp | Timezone Format | Cần Convert ở Silver? |
|-----------|-----------------|-----------------|----------------------|
| **Energy** | `interval_ts` | **UTC** | ✅ Cần convert sang local |
| **Weather** | `weather_timestamp` | **Local time** | ❌ Không cần |
| **Air Quality** | `air_timestamp` | **Local time** | ❌ Không cần |

### Giải Thích Chi Tiết

#### 1. Energy Data (UTC)

**Nguồn:** OpenElectricity API trả về timestamp ở **UTC**.

```
API Response: "interval_ts": "2025-12-03T22:00:00Z"  ← UTC
                                              ↑
                                              Ký hiệu Z = Zulu = UTC
```

**Tại sao UTC?**
- OpenElectricity là API toàn cầu, dùng UTC để đồng nhất
- Thị trường điện (NEM - National Electricity Market) ghi nhận theo UTC

**Chuyển đổi sang Local (trong Silver):**
```python
# Australia timezones:
# - AEST (Australian Eastern Standard Time) = UTC+10
# - AEDT (Australian Eastern Daylight Time) = UTC+11 (mùa hè)
# - ACST = UTC+9:30 (Adelaide)
# - AWST = UTC+8 (Perth)

# Ví dụ:
Bronze: 2025-12-03 22:00 UTC
→ Sydney (AEDT): 2025-12-04 09:00 local (+11h)
→ Brisbane (AEST): 2025-12-04 08:00 local (+10h)
```

#### 2. Weather & Air Quality Data (Local Time)

**Nguồn:** Open-Meteo API cho phép request với timezone cụ thể.

```python
# Request gửi đi:
params = {
    "latitude": -33.86,
    "longitude": 151.21,
    "timezone": "Australia/Sydney",  # ← Chỉ định timezone
    ...
}

# Response trả về đã là local time:
"time": "2025-12-04T09:00"  # Sydney local time (không có Z)
```

**Tại sao Local Time?**
- Open-Meteo hỗ trợ timezone trong request
- Code Bronze loader đã request với facility timezone
- Giảm complexity khi transform

### Timezone Lookback trong Incremental Mode

```
┌─────────────────────────────────────────────────────────────────┐
│  ENERGY LOADER: Silver incremental cần lookback 13h            │
│                                                                  │
│  Vấn đề:                                                        │
│  - Silver lưu timestamp local time (VD: 2025-12-04 09:00 AEDT)  │
│  - Bronze lưu timestamp UTC (VD: 2025-12-03 22:00 UTC)          │
│  - Khi incremental, query Bronze theo max(Silver) sẽ miss data │
│                                                                  │
│  Giải pháp:                                                     │
│  total_lookback = hour_offset(1) + timezone_lookback(12) = 13h  │
│                                                                  │
│  - hour_offset = 1: Energy shift +1h cho hour-end labeling      │
│  - timezone_lookback = 12: Max UTC offset (AEDT=+11, buffer=+1) │
│                                                                  │
│  WEATHER/AIR QUALITY: Không cần timezone lookback (đã local)    │
│  _get_timezone_lookback_hours() return 0                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Ba Loại Dữ Liệu Bronze

### 1️⃣ Bronze Weather (`lh.bronze.raw_facility_weather`)

#### Nguồn Dữ Liệu
- **API:** Open-Meteo (https://open-meteo.com)
- **Endpoint:** Historical + Forecast API
- **Rate Limit:** 10,000 requests/day (free tier)
- **Timezone:** **Local time** (được chỉ định trong request)
- **Frequency:** Hourly
- **Coverage:** 1940-present (historical), 16 days forecast

#### Schema Chi Tiết

| Column | Type | Mô Tả | Đơn Vị |
|--------|------|-------|--------|
| `facility_code` | string | Mã facility (VD: NYNGAN) | - |
| `facility_name` | string | Tên facility | - |
| `weather_timestamp` | timestamp | **Local time** từ API | - |
| `shortwave_radiation` | double | Bức xạ sóng ngắn tổng | W/m² |
| `direct_radiation` | double | Bức xạ trực tiếp (beam) | W/m² |
| `diffuse_radiation` | double | Bức xạ khuếch tán (scattered) | W/m² |
| `direct_normal_irradiance` | double | DNI - Bức xạ pháp tuyến | W/m² |
| `temperature_2m` | double | Nhiệt độ tại 2m | °C |
| `dew_point_2m` | double | Điểm sương | °C |
| `wet_bulb_temperature_2m` | double | Nhiệt độ bóng ướt | °C |
| `cloud_cover` | double | Mây tổng | % |
| `cloud_cover_low` | double | Mây tầng thấp (<2km) | % |
| `cloud_cover_mid` | double | Mây tầng trung (2-6km) | % |
| `cloud_cover_high` | double | Mây tầng cao (>6km) | % |
| `precipitation` | double | Lượng mưa | mm |
| `sunshine_duration` | double | Thời gian nắng trong giờ | seconds |
| `total_column_integrated_water_vapour` | double | Hơi nước cột tổng | kg/m² |
| `wind_speed_10m` | double | Tốc độ gió tại 10m | m/s |
| `wind_direction_10m` | double | Hướng gió (0=N, 90=E) | degrees |
| `wind_gusts_10m` | double | Gió giật | m/s |
| `pressure_msl` | double | Áp suất mực nước biển | hPa |

#### Dữ Liệu Mẫu

```json
{
  "facility_code": "NYNGAN",
  "facility_name": "Nyngan Solar Plant",
  "weather_timestamp": "2025-12-04 12:00:00",
  "shortwave_radiation": 892.5,
  "direct_radiation": 650.2,
  "diffuse_radiation": 242.3,
  "direct_normal_irradiance": 780.1,
  "temperature_2m": 32.5,
  "cloud_cover": 25.0,
  "wind_speed_10m": 12.3
}
```

---

### 2️⃣ Bronze Energy (`lh.bronze.raw_facility_timeseries`)

#### Nguồn Dữ Liệu
- **API:** OpenElectricity (https://openelectricity.org.au)
- **Market:** NEM (National Electricity Market) - Australia
- **Granularity:** 5-minute intervals
- **Timezone:** **UTC** (cần convert)
- **Coverage:** 2+ years historical
- **Metrics:** Energy (MWh), Power (MW)

#### Schema Chi Tiết

| Column | Type | Mô Tả | Đơn Vị |
|--------|------|-------|--------|
| `facility_code` | string | Mã facility | - |
| `facility_name` | string | Tên facility | - |
| `network_code` | string | Mã thị trường (NEM) | - |
| `network_region` | string | Vùng (NSW1, QLD1, VIC1...) | - |
| `metric` | string | Loại: "energy" hoặc "power" | - |
| `value` | double | Giá trị số | MWh hoặc MW |
| `interval_ts` | timestamp | **UTC timestamp** | - |

#### Giải Thích Energy vs Power

```
┌─────────────────────────────────────────────────────────────────┐
│  ENERGY (MWh) vs POWER (MW)                                     │
│                                                                  │
│  Power (MW): Công suất tức thời                                 │
│  - Tại thời điểm t, facility đang phát 50 MW                    │
│  - Đo: Điện áp × Dòng điện                                      │
│                                                                  │
│  Energy (MWh): Năng lượng tích lũy trong khoảng thời gian       │
│  - Trong 5 phút [t, t+5min), facility phát 4.17 MWh             │
│  - Energy = Power × Time = 50 MW × (5/60) h = 4.17 MWh          │
│                                                                  │
│  Quan hệ: Energy = ∫ Power dt                                   │
└─────────────────────────────────────────────────────────────────┘
```

#### Hour-End Labeling Convention

```
┌─────────────────────────────────────────────────────────────────┐
│  INTERVAL LABELING CONVENTION                                   │
│                                                                  │
│  Bronze interval_ts = "start" of interval                       │
│                                                                  │
│  interval_ts     | energy  | meaning                            │
│  2025-12-03 04:00| 12.45   | Energy generated during [04:00-04:05)
│  2025-12-03 04:05| 12.38   | Energy generated during [04:05-04:10)
│  ...             | ...     | ...                                │
│  2025-12-03 04:55| 12.41   | Energy generated during [04:55-05:00)
│                                                                  │
│  Silver (hourly aggregation with +1h shift):                    │
│  date_hour = 2025-12-03 05:00 (hour-END label)                  │
│  energy_mwh = SUM([04:00, 04:05, ..., 04:55]) = 49.76 MWh       │
│                                                                  │
│  Tại sao shift +1h?                                             │
│  - Energy [04:00-05:00) đại diện công việc hoàn thành BY 05:00  │
│  - Thị trường điện thường dùng hour-end convention              │
│  - VD: "5 giờ sáng có 49.76 MWh" = năng lượng tích lũy đến 5h   │
└─────────────────────────────────────────────────────────────────┘
```

#### Dữ Liệu Mẫu

```json
{
  "facility_code": "NYNGAN",
  "facility_name": "Nyngan Solar Plant",
  "network_code": "NEM",
  "network_region": "NSW1",
  "metric": "energy",
  "value": 12.45,
  "interval_ts": "2025-12-03T22:00:00Z"
}
```

---

### 3️⃣ Bronze Air Quality (`lh.bronze.raw_facility_air_quality`)

#### Nguồn Dữ Liệu
- **API:** Open-Meteo Air Quality API
- **Timezone:** **Local time** (được chỉ định trong request)
- **Frequency:** Hourly
- **Coverage:** 2+ years historical, 5 days forecast

#### Schema Chi Tiết

| Column | Type | Mô Tả | Đơn Vị |
|--------|------|-------|--------|
| `facility_code` | string | Mã facility | - |
| `facility_name` | string | Tên facility | - |
| `air_timestamp` | timestamp | **Local time** từ API | - |
| `pm2_5` | double | Bụi mịn PM2.5 | µg/m³ |
| `pm10` | double | Bụi PM10 | µg/m³ |
| `dust` | double | Bụi tổng | µg/m³ |
| `nitrogen_dioxide` | double | NO₂ | µg/m³ |
| `ozone` | double | O₃ | µg/m³ |
| `sulphur_dioxide` | double | SO₂ | µg/m³ |
| `carbon_monoxide` | double | CO | mg/m³ |
| `uv_index` | double | Chỉ số UV | 0-11+ |
| `uv_index_clear_sky` | double | UV khi trời quang | 0-11+ |

#### Dữ Liệu Mẫu

```json
{
  "facility_code": "NYNGAN",
  "facility_name": "Nyngan Solar Plant",
  "air_timestamp": "2025-12-04 12:00:00",
  "pm2_5": 8.5,
  "pm10": 15.2,
  "ozone": 45.3,
  "uv_index": 9.2
}
```

---

## 🔧 Bronze Load Architecture

### File Structure

```
src/pv_lakehouse/etl/bronze/
├── __init__.py
├── facilities.js               # Hardcoded facility coordinates
├── facility_timezones.py       # Timezone mapping per facility
├── load_facilities.py          # Load facility master data
├── load_facility_weather.py    # Weather ingestion job
├── load_facility_timeseries.py # Energy ingestion job
├── load_facility_air_quality.py# Air quality ingestion job
└── openmeteo_common.py         # Shared Open-Meteo utilities
```

### Facility Timezone Configuration

```python
# File: facility_timezones.py

# Map facility codes to their local timezone
FACILITY_TIMEZONES: Dict[str, str] = {
    # Commented out - currently using default for all
    # "NYNGAN": "Australia/Sydney",      # NSW - UTC+10 (or +11 DST)
    # "BNGSF1": "Australia/Adelaide",    # SA - UTC+9:30 (or +10:30 DST)
}

# Default timezone if facility not found
DEFAULT_TIMEZONE = "Australia/Brisbane"  # Queensland - UTC+10 (no DST)
```

**Australian Timezones:**

| State | Timezone | UTC Offset | DST? |
|-------|----------|------------|------|
| NSW, VIC, TAS | `Australia/Sydney` | +10 / +11 | Yes |
| QLD | `Australia/Brisbane` | +10 | No |
| SA | `Australia/Adelaide` | +9:30 / +10:30 | Yes |
| WA | `Australia/Perth` | +8 | No |
| NT | `Australia/Darwin` | +9:30 | No |

### Incremental vs Backfill Mode

```
┌─────────────────────────────────────────────────────────────────┐
│  INCREMENTAL MODE (Default)                                     │
│                                                                  │
│  1. Query: SELECT MAX(timestamp) FROM bronze.table              │
│  2. If max_ts = NULL → start = today - 1 day (first run)        │
│  3. If max_ts < today → start = max_ts (continue from last)     │
│  4. If max_ts >= today → reload today (update latest hours)     │
│  5. Fetch [start, today] → append to Iceberg                    │
│                                                                  │
│  ✓ Ưu: Chỉ load dữ liệu mới, tiết kiệm API calls                │
│  ✗ Nhược: Phải duy trì state                                    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  BACKFILL MODE                                                  │
│                                                                  │
│  1. User chỉ định: --start YYYY-MM-DD --end YYYY-MM-DD          │
│  2. Fetch tất cả dữ liệu trong range                            │
│  3. Write append hoặc overwrite (tùy option)                    │
│                                                                  │
│  ✓ Ưu: Rebuild toàn bộ dữ liệu lịch sử                          │
│  ✗ Nhược: Tốn API calls, tốn thời gian                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Chạy Bronze Load Jobs

### Command Line

```bash
# Weather - Incremental (default)
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py

# Weather - Backfill specific dates
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py \
  --start 2025-01-01 --end 2025-01-31 --mode backfill

# Energy - Incremental
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py

# Air Quality - Incremental
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py
```

### Verify Data

```sql
-- Check row counts and date ranges
SELECT 
    'Bronze Weather' AS table_name,
    COUNT(*) AS row_count,
    MIN(weather_timestamp) AS min_ts,
    MAX(weather_timestamp) AS max_ts
FROM iceberg.bronze.raw_facility_weather
UNION ALL
SELECT 
    'Bronze Energy',
    COUNT(*),
    MIN(interval_ts),
    MAX(interval_ts)
FROM iceberg.bronze.raw_facility_timeseries
UNION ALL
SELECT 
    'Bronze Air Quality',
    COUNT(*),
    MIN(air_timestamp),
    MAX(air_timestamp)
FROM iceberg.bronze.raw_facility_air_quality;
```

---

## 📞 Tham Khảo

### API Documentation
- **Open-Meteo Weather:** https://open-meteo.com/en/docs
- **Open-Meteo Air Quality:** https://open-meteo.com/en/docs/air-quality-api
- **OpenElectricity:** https://openelectricity.org.au/docs

### Related Documents
- [SILVER_LAYER.md](./SILVER_LAYER.md) - Chi tiết Silver layer transformation
- [SILVER_VALIDATION_RULES.md](./SILVER_VALIDATION_RULES.md) - Validation rules chi tiết

---

**Document Version:** 2.0  
**Last Updated:** 2025-12-04  
**Changes:** Added detailed timezone explanation, hour-end labeling convention, updated examples
