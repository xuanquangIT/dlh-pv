# 🟫 BRONZE LAYER - Chi Tiết Toàn Bộ

**Tác Giả:** Data Engineering Team  
**Cập Nhật:** 2025-11-22  
**Phiên Bản:** 1.0

---

## 📌 Giới Thiệu Bronze Layer

Bronze layer là lớp **dữ liệu thô (raw data)** trực tiếp từ các nguồn API bên ngoài. Dữ liệu ở đây chưa được xử lý, chưa được làm sạch, và có thể chứa các anomalies, missing values, hoặc dữ liệu không hợp lệ.

### Đặc Điểm Bronze Layer

| Đặc Điểm | Mô Tả |
|---------|-------|
| **Nguồn Dữ Liệu** | API bên ngoài (Open-Meteo, OpenElectricity) |
| **Tính Chất** | Thô, không xử lý |
| **Quality** | Không được validate, có thể có lỗi |
| **Format** | Dữ liệu gốc từ API (JSON/CSV) |
| **Lưu Trữ** | Iceberg tables (append-only) |
| **Chỉ Số** | Có partition theo ngày |
| **Ý Nghĩa** | Dùng làm backup, audit trail |

---

## 🌐 Ba Loại Dữ Liệu Bronze

### 1️⃣ Bronze Weather (`lh.bronze.raw_facility_weather`)

#### Mục Đích
Lưu trữ dữ liệu thời tiết theo giờ từ Open-Meteo API cho mỗi facility.

#### Nguồn Dữ Liệu
- **API:** Open-Meteo (https://open-meteo.com)
- **Endpoint:** Realtime + Archive API
- **Timezone:** Địa phương của facility (request được gửi với timezone)
- **Frequency:** Cập nhật hàng giờ
- **Coverage:** Lịch sử 6 năm, dự báo 7 ngày

#### Schema Chi Tiết

```
Column Name                          | Type      | Mô Tả
------------------------------------|-----------|-------
facility_code                       | string    | Mã facility (VD: AVLSF)
facility_name                       | string    | Tên facility
weather_timestamp                   | timestamp | Thời gian (đã là local timezone)
shortwave_radiation                 | double    | Bức xạ sóng ngắn (W/m²)
direct_radiation                    | double    | Bức xạ trực tiếp (W/m²)
diffuse_radiation                   | double    | Bức xạ khuếch tán (W/m²)
direct_normal_irradiance            | double    | Bức xạ pháp tuyến trực tiếp (DNI) (W/m²)
temperature_2m                      | double    | Nhiệt độ ở độ cao 2m (°C)
dew_point_2m                        | double    | Điểm sương ở độ cao 2m (°C)
wet_bulb_temperature_2m             | double    | Nhiệt độ bóng ướt ở độ cao 2m (°C)
cloud_cover                         | double    | Mây toàn bộ (%)
cloud_cover_low                     | double    | Mây tầng thấp (%)
cloud_cover_mid                     | double    | Mây tầng trung bình (%)
cloud_cover_high                    | double    | Mây tầng cao (%)
precipitation                       | double    | Lượng mưa (mm)
sunshine_duration                   | double    | Thời gian nắng (giây)
total_column_integrated_water_vapour| double    | Hơi nước cột tổng (kg/m²)
wind_speed_10m                      | double    | Tốc độ gió ở độ cao 10m (m/s)
wind_direction_10m                  | double    | Hướng gió ở độ cao 10m (°)
wind_gusts_10m                      | double    | Gió giật ở độ cao 10m (m/s)
pressure_msl                        | double    | Áp suất mực nước biển (hPa)
```

#### Dữ Liệu Mẫu

```json
{
  "facility_code": "AVLSF",
  "facility_name": "Alexandria Solar Farm",
  "weather_timestamp": "2025-11-22 14:00:00+10:00",
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
  "pressure_msl": 1013.5
}
```

#### Quy Trình Load Weather

```
┌─────────────────────────────────┐
│  Parse Arguments (mode, dates)  │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  Load Facility List             │
│  (coordinates, timezone)        │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  Parallel Fetch from Open-Meteo │
│  (4 workers, 30 req/min limit)  │
│  - Per facility                 │
│  - 30-day chunks                │
│  - Local timezone               │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  Merge DataFrames               │
│  (Pandas concat)                │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  Convert to Spark DF            │
│  Append to Iceberg Table        │
└─────────────────────────────────┘
```

#### Độ Chính Xác & Timezone

**Important:** Timestamp từ Open-Meteo đã được request với facility timezone!

```python
# VD: Request cho Alexandria (Sydney)
facility_tz = get_facility_timezone("AVLSF")  # "Australia/Sydney"
response = openmeteo.fetch_weather(
    latitude=...,
    longitude=...,
    timezone="Australia/Sydney"  # ← Timezone đã được chỉ định
)
# Response sẽ có timestamp trong timezone này
# VD: 2025-11-22 14:00 (Sydney time)
```

---

### 2️⃣ Bronze Timeseries Energy (`lh.bronze.raw_facility_timeseries`)

#### Mục Đích
Lưu trữ dữ liệu năng lượng/công suất theo từng khoảng thời gian từ OpenElectricity API.

#### Nguồn Dữ Liệu
- **API:** OpenElectricity (https://openelectricity.org)
- **Granularity:** 5-15 phút (tuỳ facility)
- **Metrics:** Energy (MWh), Power (MW)
- **Timezone:** UTC (cần convert sang facility local time)
- **Coverage:** 2 năm lịch sử
- **Frequency:** Cập nhật thực tế

#### Schema Chi Tiết

```
Column Name      | Type      | Mô Tả
-----------------|-----------|-------
facility_code    | string    | Mã facility
facility_name    | string    | Tên facility
network_code     | string    | Mã thị trường điện (VD: NEM - Australia)
network_region   | string    | Vùng thị trường (VD: NSW1 - New South Wales)
metric           | string    | Loại metric: "energy" hoặc "power"
value            | double    | Giá trị số (MWh hoặc MW)
interval_ts      | timestamp | Thời gian bắt đầu khoảng (UTC)
```

#### Dữ Liệu Mẫu

```json
{
  "facility_code": "AVLSF",
  "facility_name": "Alexandria Solar Farm",
  "network_code": "NEM",
  "network_region": "NSW1",
  "metric": "energy",
  "value": 12.45,
  "interval_ts": "2025-11-22T04:00:00Z"
}
```

#### Giải Thích Dữ Liệu

**Energy vs Power:**
- **Energy (MWh):** Năng lượng sinh ra trong khoảng thời gian (tích phân)
  - VD: Energy từ 4:00-4:15 = 12.45 MWh (khoảng 15 phút)
- **Power (MW):** Công suất tức thời (tỉ lệ)
  - VD: Power tại 4:00 = 49.8 MW (công suất ngay lúc đó)

**Timezone:**
```
Bronze: interval_ts = 2025-11-22 04:00:00 UTC
→ Cần convert sang facility timezone (VD: Sydney +10:00)
→ Silver: date_hour = 2025-11-22 14:00:00+10:00 (local)
```

#### Quy Trình Load Timeseries

```
┌──────────────────────────────────┐
│  Parse Arguments (mode, dates)   │
└────────────┬─────────────────────┘
             │
             ▼
┌──────────────────────────────────┐
│  Detect Last Loaded Date         │
│  (Query existing Bronze data)    │
└────────────┬─────────────────────┘
             │
             ▼
┌──────────────────────────────────┐
│  Set Start/End Dates             │
│  Incremental: day after last     │
│  Backfill: user-specified        │
└────────────┬─────────────────────┘
             │
             ▼
┌──────────────────────────────────┐
│  Parallel Fetch from OpenElec    │
│  (per facility, 1-day chunks)    │
└────────────┬─────────────────────┘
             │
             ▼
┌──────────────────────────────────┐
│  Filter: metric IN (energy, power)
│  Parse: value AS double          │
└────────────┬─────────────────────┘
             │
             ▼
┌──────────────────────────────────┐
│  Convert to Spark DF             │
│  Append to Iceberg Table         │
└──────────────────────────────────┘
```

#### Interval vs Timestamp

**Important Detail:**
- `interval_ts` = thời điểm bắt đầu khoảng thời gian
- Energy = tích lũy từ `interval_ts` đến `interval_ts + 15min`

```
interval_ts       | energy | meaning
2025-11-22 04:00 | 12.45  | Energy generated during [04:00, 04:15)
2025-11-22 04:15 | 12.38  | Energy generated during [04:15, 04:30)
2025-11-22 04:30 | 12.52  | Energy generated during [04:30, 04:45)
2025-11-22 04:45 | 12.41  | Energy generated during [04:45, 05:00)
```

**Aggregation to Hourly:**
```
Silver hourly energy = sum([04:00, 04:15, 04:30, 04:45])
                     = 12.45 + 12.38 + 12.52 + 12.41
                     = 49.76 MWh for hour starting at 04:00 UTC

But shifted to 05:00 UTC (timestamp +1 hour) for label
Why? Because energy [04:00-05:00) represents work done BY 05:00
```

---

### 3️⃣ Bronze Air Quality (`lh.bronze.raw_facility_air_quality`)

#### Mục Đích
Lưu trữ dữ liệu chất lượng không khí theo giờ từ Open-Meteo API.

#### Nguồn Dữ Liệu
- **API:** Open-Meteo Air Quality (https://open-meteo.com)
- **Variables:** PM2.5, PM10, NO₂, O₃, SO₂, CO, UV Index
- **Timezone:** Địa phương của facility
- **Frequency:** Cập nhật hàng giờ
- **Coverage:** 2 năm lịch sử, dự báo 7 ngày

#### Schema Chi Tiết

```
Column Name                    | Type      | Mô Tả
-------------------------------|-----------|-------
facility_code                  | string    | Mã facility
facility_name                  | string    | Tên facility
air_timestamp                  | timestamp | Thời gian (local timezone)
pm2_5                          | double    | Bụi mịn PM2.5 (µg/m³)
pm10                           | double    | Bụi PM10 (µg/m³)
dust                           | double    | Bụi tổng (µg/m³)
nitrogen_dioxide               | double    | NO₂ (ppb)
ozone                          | double    | O₃ (ppb)
sulphur_dioxide                | double    | SO₂ (ppb)
carbon_monoxide                | double    | CO (ppb)
uv_index                       | double    | Chỉ số UV hiện tại
uv_index_clear_sky             | double    | Chỉ số UV khi trời quang
```

#### Dữ Liệu Mẫu

```json
{
  "facility_code": "AVLSF",
  "facility_name": "Alexandria Solar Farm",
  "air_timestamp": "2025-11-22 14:00:00+10:00",
  "pm2_5": 12.5,
  "pm10": 18.2,
  "dust": 20.1,
  "nitrogen_dioxide": 15.3,
  "ozone": 45.2,
  "sulphur_dioxide": 2.1,
  "carbon_monoxide": 0.5,
  "uv_index": 8.2,
  "uv_index_clear_sky": 9.1
}
```

#### Quy Trình Load Air Quality

Tương tự Weather, nhưng dữ liệu ít hơn (chỉ 10 metrics vs 20 metrics).

```
┌──────────────────────────────────┐
│  Parallel Fetch per Facility     │
│  (4 workers, 30 req/min limit)   │
│  - Local timezone                │
│  - 30-day chunks                 │
└────────────┬─────────────────────┘
             │
             ▼
┌──────────────────────────────────┐
│  Merge DataFrames                │
│  Convert to Spark DF             │
│  Append to Iceberg Table         │
└──────────────────────────────────┘
```

---

## 🔧 Bronze Load Architecture

### Incremental vs Backfill Mode

#### **Incremental Mode** (Mặc định)
```
Mục đích: Cập nhật dữ liệu mới hàng ngày
Quy trình:
  1. Query: SELECT MAX(weather_timestamp) FROM bronze.raw_facility_weather
  2. Nếu max_ts = NULL → start = today - 1 day
  3. Nếu max_ts < today → start = max_ts + 1 day
  4. Nếu max_ts >= today → reload today (update latest hours)
  5. Fetch từ [start, today] → ghi append vào Iceberg
  
Ưu điểm: Chỉ load dữ liệu mới, tiết kiệm API calls
Nhược điểm: Phải duy trì state (last loaded timestamp)
```

#### **Backfill Mode**
```
Mục đích: Load toàn bộ dữ liệu lịch sử (khôi phục)
Quy trình:
  1. User chỉ định --start YYYY-MM-DD --end YYYY-MM-DD
  2. Fetch tất cả dữ liệu trong range
  3. Write overwrite (xóa cũ, ghi mới)
  
Ưu điểm: Có thể rebuild toàn bộ dữ liệu lịch sử
Nhược điểm: Tốn API calls, tốn thời gian
```

### Rate Limiting

```python
# Open-Meteo API: 30 requests/minute (free tier)
limiter = RateLimiter(30.0)

# Multi-threading: 4 concurrent workers
max_workers = 4

# Scheduling: Ensure ~4 facilities fetch concurrently
# Each facility = 1 API request per date range
# Rate limiter ensures: sum(requests) ≤ 30/min
```

### Retry Logic

```
Default settings:
  - max_retries = 3
  - retry_backoff = 2.0 (exponential)
  
Retry sequence:
  Request 1 → Fail
  Wait 2 seconds
  Request 2 → Fail
  Wait 4 seconds
  Request 3 → Fail
  Raise exception
```

---

## 📊 Bronze Data Characteristics

### Data Quality

| Aspect | Weather | Energy | Air Quality |
|--------|---------|--------|------------|
| **Completeness** | 95%+ | 90%+ | 98%+ |
| **Timeliness** | Real-time (1h delay) | Real-time (5min delay) | Real-time (1h delay) |
| **Anomalies** | Rare (<1%) | Common (2-5%) | Very Rare (<0.5%) |
| **Source Reliability** | High | High | High |
| **Missing Values** | Some columns | Rare | Some columns |

### Timezone Handling

```python
# Weather & Air Quality: Already local timezone from API
weather_timestamp = "2025-11-22 14:00:00+10:00"  # Sydney local

# Energy: UTC, needs conversion
interval_ts = "2025-11-22T04:00:00Z"  # UTC
→ Convert to Sydney: 2025-11-22 14:00:00+10:00 (in Silver layer)
```

### Partition Strategy

Bronze tables partition by date for efficient scanning:

```sql
-- Partition: date_part('year', weather_timestamp), date_part('month', weather_timestamp)
-- Efficient range scans:
SELECT * FROM bronze.raw_facility_weather
WHERE weather_timestamp >= '2025-11-01' AND weather_timestamp < '2025-11-08'
-- Query only 8 partitions
```

---

## 📁 Bronze Layer File Structure

```
src/pv_lakehouse/etl/bronze/
├── load_facility_weather.py       # Weather ingestion job
├── load_facility_timeseries.py    # Energy ingestion job
├── load_facility_air_quality.py   # Air quality ingestion job
├── load_facilities.py              # Load facility master data
├── facility_timezones.py           # Timezone mapping
├── openmeteo_common.py             # Common utilities
├── facilities.js                   # Facility coordinates (hardcoded)
└── __init__.py
```

### Key Files

**1. `load_facility_weather.py`**
- Entry point: `main()` → Parse args → Fetch → Load
- Function: `collect_weather_data()` → Parallel fetch per facility
- Configuration: 4 workers, 30 req/min, 30-day chunks

**2. `load_facility_timeseries.py`**
- Entry point: `main()` → Parse args → Fetch → Load
- Function: `collect_timeseries_data()` → Parallel fetch per facility
- Configuration: 4 workers, 1-day chunks

**3. `load_facility_air_quality.py`**
- Similar to weather
- Configuration: 4 workers, 30 req/min, 30-day chunks

**4. `facility_timezones.py`**
```python
FACILITY_TIMEZONES = {
    "AVLSF": "Australia/Sydney",
    "BARC": "Australia/Brisbane",
    # ... etc
}

DEFAULT_TIMEZONE = "Australia/Sydney"
```

---

## 🚀 Chạy Bronze Load Jobs

### Command Line Interface

```bash
# 1. Load Weather (Incremental - default)
python -m pv_lakehouse.etl.bronze.load_facility_weather

# 2. Load Weather (Specific dates)
python -m pv_lakehouse.etl.bronze.load_facility_weather \
  --start 2025-01-01 \
  --end 2025-01-31 \
  --mode backfill

# 3. Load Energy (Incremental)
python -m pv_lakehouse.etl.bronze.load_facility_timeseries

# 4. Load Air Quality (Specific facility)
python -m pv_lakehouse.etl.bronze.load_facility_air_quality \
  --facility-codes AVLSF,BARC \
  --start 2025-11-01 \
  --end 2025-11-22

# 5. Load Facilities Master
python -m pv_lakehouse.etl.bronze.load_facilities
```

### Docker/Spark Execution

```bash
# Submit to Spark cluster
spark-submit \
  --master spark://localhost:7077 \
  --class pv_lakehouse.etl.bronze.load_facility_weather \
  --driver-memory 2g \
  --executor-memory 2g \
  dist/pv-lakehouse.jar

# Via Prefect (orchestration)
prefect deployment run bronze-weather-load
```

---

## 🔍 Troubleshooting Bronze Issues

### Common Issues

| Issue | Triệu Chứng | Nguyên Nhân | Giải Pháp |
|-------|----------|-----------|----------|
| **API Quota Exceeded** | 429 Too Many Requests | Exceed 30 req/min | Reduce workers, wait |
| **Missing Data** | NULL values in columns | API down, network issue | Retry, check API status |
| **Timezone Error** | Wrong timestamps | Incorrect timezone conversion | Verify facility_timezones.py |
| **Duplicate Data** | Same records twice | Incremental mode re-fetched same day | Use merge write strategy |
| **Parse Error** | Type casting failed | Unexpected data format from API | Check API schema changes |

### Debug Queries

```sql
-- Check latest data
SELECT MAX(weather_timestamp) as last_weather,
       MAX(interval_ts) as last_energy,
       MAX(air_timestamp) as last_air_quality
FROM bronze.raw_facility_weather,
     bronze.raw_facility_timeseries,
     bronze.raw_facility_air_quality;

-- Find NULL values
SELECT facility_code, COUNT(*) as null_count
FROM bronze.raw_facility_weather
WHERE shortwave_radiation IS NULL
GROUP BY facility_code;

-- Check for negative values (shouldn't exist)
SELECT * FROM bronze.raw_facility_timeseries
WHERE value < 0;
```

---

## 📈 Performance Metrics

### Typical Load Times

| Data Type | Records | Time | Rate |
|-----------|---------|------|------|
| Weather (30 days) | 30 × 24 × 20 facilities = 14,400 | ~5 min | 48 records/sec |
| Energy (30 days) | 30 × 96 × 20 facilities = 57,600 | ~10 min | 96 records/sec |
| Air Quality (30 days) | 30 × 24 × 20 facilities = 14,400 | ~5 min | 48 records/sec |

### Storage Size (Approximate)

```
Weather (2 years, 20 facilities):
  - Records: 2×365×24×20 = 350,400
  - Size: ~200 MB (19 numeric columns)

Energy (2 years, 20 facilities):
  - Records: 2×365×96×20 = 1,401,600
  - Size: ~150 MB (sparse, 4 numeric columns)

Air Quality (2 years, 20 facilities):
  - Records: 2×365×24×20 = 350,400
  - Size: ~100 MB (10 numeric columns)

Total Bronze: ~450 MB
```

---

## ✅ Best Practices

1. **Always Run Incremental First**
   - Only use backfill when needed (data corruption, recovery)
   - Incremental is faster and uses fewer API calls

2. **Monitor API Quotas**
   - Keep logs of API request counts
   - Alert if approaching limits

3. **Verify Timestamps**
   - Confirm weather/air timestamps are in local timezone
   - Energy timestamps should be UTC (converted in Silver)

4. **Handle Missing Data**
   - Don't fail entire job if one facility fails
   - Use try-catch, log error, continue with others

5. **Archive Old Data**
   - Bronze keeps ALL history (audit trail)
   - Consider archiving data > 2 years to reduce costs

6. **Test with Small Date Range**
   - Run --start 2025-01-01 --end 2025-01-02 first
   - Verify data quality before full backfill

---

## 📞 Liên Hệ & Hỗ Trợ

**Issues:** Check `bronze/` directory logs  
**Questions:** Ask Data Engineering team  
**API Status:** Check Open-Meteo, OpenElectricity status pages

---

**Document Version:** 1.0  
**Last Updated:** 2025-11-22  
**Scope:** Complete Bronze layer architecture and operations
