# Timezone Handling & Record Count Analysis

**Tác Giả:** Data Engineering Team  
**Cập Nhật:** 2025-12-09  
**Phiên Bản:** 1.0

---

## 📌 Tổng Quan

Tài liệu này giải thích chi tiết:
1. Cách xử lý timezone từ API đến Bronze và Silver layers
2. Tại sao Energy có ít hơn Weather/Air Quality 8 records

---

## 📊 Kết Quả Query Thực Tế

### Bronze Layer - Timestamp Ranges

```sql
-- Query thực hiện ngày 2025-12-09
```

| Table | Total Records | Min Timestamp (UTC) | Max Timestamp (UTC) | Facilities |
|-------|---------------|---------------------|---------------------|------------|
| **Bronze Energy** | 65,696 | `2024-12-31 14:00:00` | `2025-12-08 18:00:00` | 8 |
| **Bronze Weather** | 65,704 | `2025-01-01 00:00:00` | `2025-12-09 04:00:00` | 8 |
| **Bronze Air Quality** | 65,704 | `2025-01-01 00:00:00` | `2025-12-09 04:00:00` | 8 |

### Silver Layer - Timestamp Ranges

| Table | Total Records | Min date_hour | Max date_hour | Facilities |
|-------|---------------|---------------|---------------|------------|
| **Silver Energy** | 65,696 | `2025-01-01 01:00:00` | `2025-12-09 05:00:00` | 8 |
| **Silver Weather** | 65,704 | `2025-01-01 00:00:00` | `2025-12-09 04:00:00` | 8 |
| **Silver Air Quality** | 65,704 | `2025-01-01 00:00:00` | `2025-12-09 04:00:00` | 8 |

### Chênh Lệch Record Count

```
Weather Records:     65,704
Air Quality Records: 65,704
Energy Records:      65,696
─────────────────────────────
Chênh lệch:              8 records
```

---

## 🌐 Timezone Handling từ API

### 1. OpenElectricity API (Energy Data)

```
┌─────────────────────────────────────────────────────────────────┐
│  OPENELECTRICITY API → BRONZE ENERGY                            │
│                                                                  │
│  API Response:                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ interval_ts: "2024-12-31T14:00:00Z"  ← LUÔN LÀ UTC       │   │
│  │ energy: 45.2                                              │   │
│  │ facility_code: "AVLSF"                                    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Bronze Storage:                                                 │
│  → interval_ts stored as UTC timestamp                          │
│  → No timezone conversion at Bronze layer                       │
└─────────────────────────────────────────────────────────────────┘
```

**Đặc điểm:**
- API trả về timestamp **luôn ở UTC** (format ISO 8601 với "Z" suffix)
- Bronze layer lưu trữ **nguyên bản UTC**
- `interval_ts` đại diện cho **điểm bắt đầu** của khoảng thời gian (interval start)

### 2. Open-Meteo API (Weather & Air Quality Data)

```
┌─────────────────────────────────────────────────────────────────┐
│  OPEN-METEO API → BRONZE WEATHER/AIR QUALITY                    │
│                                                                  │
│  API Request:                                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ timezone=Australia/Brisbane  ← Request với local timezone│   │
│  │ latitude=-27.5                                            │   │
│  │ longitude=153.0                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  API Response:                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ time: "2025-01-01T00:00"  ← ĐÃ LÀ LOCAL TIME            │   │
│  │ shortwave_radiation: 0.0                                  │   │
│  │ temperature_2m: 24.5                                      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Bronze Storage:                                                 │
│  → weather_timestamp / air_timestamp stored as local time       │
│  → No conversion needed (already in local timezone)             │
└─────────────────────────────────────────────────────────────────┘
```

**Đặc điểm:**
- API được request với **facility timezone** (ví dụ: `Australia/Brisbane`)
- API trả về timestamp **đã ở local time**
- Bronze layer lưu trữ **nguyên bản local time**

---

## 🔄 Timezone Transformation ở Silver Layer

### Energy: UTC → Local + Hour-End Labeling

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE ENERGY → SILVER ENERGY (Timezone Conversion)            │
│                                                                  │
│  Step 1: UTC → Local Time Conversion                            │
│  ─────────────────────────────────────────────────────────────  │
│  Bronze interval_ts (UTC):  2024-12-31 14:00:00 UTC             │
│                                    ↓                             │
│  Convert to Brisbane (+10):  2025-01-01 00:00:00 AEST           │
│                                                                  │
│  Step 2: Hour-End Labeling (+1 Hour)                            │
│  ─────────────────────────────────────────────────────────────  │
│  Energy measured trong [00:00, 01:00) được label tại 01:00      │
│  2025-01-01 00:00:00 AEST + 1 hour = 2025-01-01 01:00:00        │
│                                    ↓                             │
│  Silver date_hour:          2025-01-01 01:00:00                 │
└─────────────────────────────────────────────────────────────────┘
```

**Code thực hiện (hourly_energy.py):**

```python
# Step 1: Convert UTC to local timezone per facility
from pv_lakehouse.etl.bronze.facility_timezones import FACILITY_TIMEZONES, DEFAULT_TIMEZONE

default_local = F.from_utc_timestamp(F.col("interval_ts"), DEFAULT_TIMEZONE)
tz_expr = default_local

for code, tz in FACILITY_TIMEZONES.items():
    tz_expr = F.when(
        F.col("facility_code") == code, 
        F.from_utc_timestamp(F.col("interval_ts"), tz)
    ).otherwise(tz_expr)

# Step 2: Hour-end labeling (+1 hour)
.withColumn("date_hour", F.date_trunc("hour", F.expr("timestamp_local + INTERVAL 1 HOUR")))
```

### Weather/Air Quality: No Conversion Needed

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE WEATHER/AIR → SILVER (No Timezone Conversion)           │
│                                                                  │
│  Bronze weather_timestamp:   2025-01-01 00:00:00 (local)        │
│                                    ↓                             │
│  Silver date_hour:           2025-01-01 00:00:00 (local)        │
│                                                                  │
│  ✅ Không cần convert vì API đã trả về local time               │
│  ✅ Chỉ cần truncate to hour nếu có sub-hourly data             │
└─────────────────────────────────────────────────────────────────┘
```

**Code thực hiện (hourly_weather.py):**

```python
def _get_timezone_lookback_hours(self) -> int:
    """Weather data is already in local time from API - no timezone conversion needed."""
    return 0

# No UTC conversion, just truncate to hour
.withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_local")))
```

---

## ❓ Giải Thích: Tại Sao Energy Ít Hơn 8 Records?

### Phân Tích Ngày Đầu Tiên (2025-01-01)

```
┌─────────────────────────────────────────────────────────────────┐
│  NGÀY 2025-01-01: SO SÁNH HOURS                                 │
│                                                                  │
│  Weather/Air Quality:                                            │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Hour 00 │ Hour 01 │ Hour 02 │ ... │ Hour 22 │ Hour 23 │   │   │
│  │    ✅   │    ✅   │    ✅   │     │    ✅   │    ✅   │   │   │
│  └──────────────────────────────────────────────────────────┘   │
│  Total: 24 hours × 8 facilities = 192 records                   │
│                                                                  │
│  Energy:                                                         │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Hour 00 │ Hour 01 │ Hour 02 │ ... │ Hour 22 │ Hour 23 │   │   │
│  │    ❌   │    ✅   │    ✅   │     │    ✅   │    ✅   │   │   │
│  └──────────────────────────────────────────────────────────┘   │
│  Total: 23 hours × 8 facilities = 184 records                   │
│                                                                  │
│  CHÊNH LỆCH: 192 - 184 = 8 records                              │
└─────────────────────────────────────────────────────────────────┘
```

### Query Xác Nhận

```sql
-- Silver Energy ngày 2025-01-01: Thiếu hour 00
SELECT HOUR(date_hour), COUNT(DISTINCT facility_code)
FROM iceberg.silver.clean_hourly_energy
WHERE CAST(date_hour AS DATE) = DATE '2025-01-01'
GROUP BY HOUR(date_hour) ORDER BY 1;

-- Result: Hours 1-23 (thiếu hour 0)
-- Mỗi hour có 8 facilities
```

```sql
-- Silver Weather ngày 2025-01-01: Đầy đủ 24 hours
SELECT HOUR(date_hour), COUNT(DISTINCT facility_code)  
FROM iceberg.silver.clean_hourly_weather
WHERE CAST(date_hour AS DATE) = DATE '2025-01-01'
GROUP BY HOUR(date_hour) ORDER BY 1;

-- Result: Hours 0-23 (đầy đủ)
-- Mỗi hour có 8 facilities
```

### Nguyên Nhân Chi Tiết

```
┌─────────────────────────────────────────────────────────────────┐
│  TẠI SAO ENERGY THIẾU HOUR 00:00 NGÀY 2025-01-01?               │
│                                                                  │
│  1. Bronze Energy bắt đầu từ: 2024-12-31 14:00:00 UTC           │
│                                                                  │
│  2. Để có Silver Energy hour 00:00 local cần:                   │
│     - Bronze interval_ts = 2024-12-30 13:00:00 UTC              │
│       (với Brisbane +10 và +1h hour-end labeling)               │
│     - NHƯNG Bronze chỉ bắt đầu từ 2024-12-31 14:00:00 UTC       │
│                                                                  │
│  3. Record đầu tiên trong Bronze Energy:                        │
│     - Bronze: 2024-12-31 14:00:00 UTC                           │
│     - Convert to Brisbane (+10): 2025-01-01 00:00:00 AEST       │
│     - +1h hour-end labeling: 2025-01-01 01:00:00                │
│     → Silver date_hour đầu tiên = 01:00:00 (KHÔNG PHẢI 00:00)   │
│                                                                  │
│  4. Weather/Air bắt đầu từ: 2025-01-01 00:00:00 (đã local)      │
│     → Có đầy đủ hour 00:00 ngay từ đầu                          │
└─────────────────────────────────────────────────────────────────┘
```

### Công Thức Tính Toán

```
Chênh lệch = (Weather records) - (Energy records)
           = 65,704 - 65,696
           = 8 records

Giải thích: 
- Energy thiếu 1 hour (00:00) vào ngày đầu tiên
- 1 hour × 8 facilities = 8 records
```

---

## 📐 Tóm Tắt Timezone Flow

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        TIMEZONE FLOW DIAGRAM                              │
│                                                                           │
│  ┌─────────────┐                    ┌─────────────┐                      │
│  │   Energy    │                    │  Weather/   │                      │
│  │    API      │                    │  Air API    │                      │
│  └──────┬──────┘                    └──────┬──────┘                      │
│         │                                   │                             │
│         │ Returns UTC                       │ Request with                │
│         │                                   │ local timezone              │
│         ↓                                   ↓                             │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                         BRONZE LAYER                                │ │
│  │  interval_ts (UTC)          │    weather_timestamp (LOCAL)          │ │
│  │  2024-12-31 14:00:00 UTC    │    2025-01-01 00:00:00 AEST          │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│         │                                   │                             │
│         │ UTC → Local + 1h                  │ No conversion               │
│         │ (hour-end labeling)               │ (already local)             │
│         ↓                                   ↓                             │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                         SILVER LAYER                                │ │
│  │  date_hour (LOCAL)           │    date_hour (LOCAL)                 │ │
│  │  2025-01-01 01:00:00         │    2025-01-01 00:00:00               │ │
│  │  (starts at 01:00)           │    (starts at 00:00)                 │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ⚠️ CHÊNH LỆCH: Energy thiếu hour 00:00 = 8 records (1 hour × 8 fac)    │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## 📝 Lưu Ý Quan Trọng

### 1. Hour-End Labeling (Energy Only)

Energy data sử dụng **hour-end labeling**:
- Năng lượng đo trong khoảng [00:00, 01:00) được gán nhãn tại **01:00**
- Điều này tuân theo convention của OpenElectricity API

### 2. Facility Timezone

```python
# Default timezone cho tất cả facilities
DEFAULT_TIMEZONE = "Australia/Brisbane"  # UTC+10

# Có thể override per facility nếu cần
FACILITY_TIMEZONES = {
    # "NYNGAN": "Australia/Sydney",  # UTC+10 or +11 during DST
}
```

### 3. DST (Daylight Saving Time)

- **Brisbane (QLD):** Không có DST → UTC+10 quanh năm
- **Sydney (NSW):** Có DST → UTC+10 (winter) hoặc UTC+11 (summer)
- Current setup sử dụng Brisbane timezone cho tất cả facilities

### 4. Impact on ML Training

- Energy và Weather/Air data được join trên `facility_code` và `date_hour`
- Record đầu tiên (2025-01-01 00:00:00) sẽ không có Energy data
- ML pipeline cần handle NULL values hoặc filter out incomplete records

---

## 🔍 Query Verification

### Kiểm Tra Bronze Timestamps

```sql
-- Bronze Energy: UTC timestamps
SELECT 
    MIN(interval_ts) as earliest_utc,
    MAX(interval_ts) as latest_utc
FROM iceberg.bronze.raw_facility_timeseries;
-- Result: 2024-12-31 14:00:00 UTC → 2025-12-08 18:00:00 UTC

-- Bronze Weather: Local timestamps
SELECT 
    MIN(weather_timestamp) as earliest_local,
    MAX(weather_timestamp) as latest_local
FROM iceberg.bronze.raw_facility_weather;
-- Result: 2025-01-01 00:00:00 → 2025-12-09 04:00:00
```

### Kiểm Tra Silver Timestamps

```sql
-- Silver Energy: Local timestamps (after conversion + hour-end)
SELECT 
    MIN(date_hour) as earliest,
    MAX(date_hour) as latest
FROM iceberg.silver.clean_hourly_energy;
-- Result: 2025-01-01 01:00:00 → 2025-12-09 05:00:00

-- Silver Weather: Local timestamps (no conversion)
SELECT 
    MIN(date_hour) as earliest,
    MAX(date_hour) as latest
FROM iceberg.silver.clean_hourly_weather;
-- Result: 2025-01-01 00:00:00 → 2025-12-09 04:00:00
```

### Xác Nhận 8 Record Difference

```sql
-- Count records per table
SELECT 
    'Silver Energy' as table_name, COUNT(*) as records
FROM iceberg.silver.clean_hourly_energy
UNION ALL
SELECT 
    'Silver Weather' as table_name, COUNT(*) as records
FROM iceberg.silver.clean_hourly_weather;

-- Result:
-- Silver Energy:  65,696
-- Silver Weather: 65,704
-- Difference: 8 records
```

---

## 📚 Tham Khảo Code

| Component | File | Description |
|-----------|------|-------------|
| Energy Bronze Loader | `src/pv_lakehouse/etl/bronze/load_facility_timeseries.py` | Load từ OpenElectricity API (UTC) |
| Weather Bronze Loader | `src/pv_lakehouse/etl/bronze/load_facility_weather.py` | Load từ Open-Meteo API (local TZ) |
| Energy Silver Loader | `src/pv_lakehouse/etl/silver/hourly_energy.py` | UTC → Local + hour-end conversion |
| Weather Silver Loader | `src/pv_lakehouse/etl/silver/hourly_weather.py` | No conversion needed |
| Facility Timezones | `src/pv_lakehouse/etl/bronze/facility_timezones.py` | Timezone mapping per facility |

---

**Last Updated:** 2025-12-09  
**Verified Against:** Production data as of December 9, 2025
