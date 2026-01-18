# 📋 BÁO CÁO CHI TIẾT CÔNG VIỆC CỦA CHUNG QUANG ĐĂNG KHOA

**Dự Án:** PV Lakehouse - Hệ Thống Data Lakehouse cho Năng Lượng Mặt Trời  
**Sinh Viên:** Chung Quang Đăng Khoa  
**Tỉ lệ hoàn thành:** 100%

---

## 📌 TỔNG QUAN PHÂN CÔNG

| STT | Nội dung công việc |
|-----|-------------------|
| 1 | Cài đặt và cấu hình công nghệ/hệ thống theo kiến trúc Data Lakehouse |
| 2 | Nghiên cứu nguồn dữ liệu đầu vào của đề tài |
| 3 | Thực hiện nạp dữ liệu vào lớp Bronze |
| 4 | Xử lý, làm sạch và chuẩn hoá dữ liệu ở lớp Silver |

---

## 1️⃣ CÀI ĐẶT VÀ CẤU HÌNH CÔNG NGHỆ/HỆ THỐNG

### 🏗️ Kiến Trúc Data Lakehouse

Hệ thống được xây dựng theo kiến trúc **Medallion (Bronze → Silver → Gold)** với các công nghệ:

### 💻 Stack Công Nghệ

| Thành phần | Công nghệ | Mục đích |
|-----------|-----------|----------|
| **Object Storage** | MinIO | Lưu trữ S3-compatible cho data lake |
| **Table Format** | Apache Iceberg v2 | ACID transactions, time travel, schema evolution |
| **Batch Processing** | Apache Spark 3.5 | Engine xử lý dữ liệu phân tán |
| **Query Engine** | Trino | SQL analytics engine |
| **Orchestration** | Prefect | Workflow orchestration |
| **ML Tracking** | MLflow 2.4 | Experiment tracking & model registry |
| **Metadata Store** | PostgreSQL | Iceberg catalog & application metadata |
| **Container** | Docker + Docker Compose | Containerization & orchestration |

### 📁 Cấu Trúc Docker

**File cấu hình chính:** `docker/docker-compose.yml`

#### Services được cấu hình:

```yaml
services:
  minio:          # Object storage S3-compatible
  mc:             # MinIO client để setup buckets/policies
  postgres:       # Database cho Iceberg catalog
  pgadmin:        # PostgreSQL admin UI
  spark-master:   # Spark Master node
  spark-worker:   # Spark Worker node
  trino:          # SQL query engine
  mlflow:         # ML experiment tracking
```

### 🌐 Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | `pvlakehouse` / `pvlakehouse` |
| Spark Master UI | http://localhost:8080 | — |
| Trino UI | http://localhost:8081 | — |
| MLflow UI | http://localhost:5000 | — |
| pgAdmin | http://localhost:5050 | `admin@admin.com` / `pvlakehouse` |

---

## 2️⃣ NGHIÊN CỨU NGUỒN DỮ LIỆU ĐẦU VÀO

Hệ thống sử dụng **3 nguồn dữ liệu API** từ bên ngoài:

### 📡 Nguồn 1: Open-Meteo Weather API

| Đặc điểm | Chi tiết |
|----------|----------|
| **Website** | https://open-meteo.com |
| **Endpoint** | Historical + Forecast API |
| **Rate Limit** | 10,000 requests/day (free tier) |
| **Timezone** | Local time (configurable) |
| **Frequency** | Hourly |

**Các biến thời tiết được thu thập:**
- shortwave_radiation, direct_radiation, diffuse_radiation
- temperature_2m, dew_point_2m, cloud_cover
- precipitation, sunshine_duration, wind_speed_10m, pressure_msl

### 📡 Nguồn 2: Open-Meteo Air Quality API

| Đặc điểm | Chi tiết |
|----------|----------|
| **Endpoint** | https://air-quality-api.open-meteo.com/v1/air-quality |
| **Frequency** | Hourly |
| **Coverage** | 2+ years historical, 5 days forecast |

**Các biến chất lượng không khí:**
- pm2_5, pm10, dust, nitrogen_dioxide, ozone
- sulphur_dioxide, carbon_monoxide, uv_index

### 📡 Nguồn 3: OpenElectricity API (NEM Australia)

| Đặc điểm | Chi tiết |
|----------|----------|
| **Website** | https://openelectricity.org.au |
| **Market** | NEM (National Electricity Market) - Australia |
| **Granularity** | Supports 5m/1h/1d/... (we use **1h**) |
| **Timezone** | **UTC** (cần convert) |
| **Metrics** | Energy (MWh), Power (MW) |

### 🕐 Timezone Handling

| Data Type | Bronze Timestamp | Cần Convert ở Silver? |
|-----------|-----------------|----------------------|
| **Energy** | UTC | ✅ Cần convert sang local |
| **Weather** | Local time | ❌ Không cần |
| **Air Quality** | Local time | ❌ Không cần |

---

## 3️⃣ THỰC HIỆN NẠP DỮ LIỆU VÀO LỚP BRONZE

### 📂 Cấu Trúc Thư Mục Bronze

```
src/pv_lakehouse/etl/bronze/
├── load_facilities.py          # Load facility master data
├── load_facility_weather.py    # Weather ingestion job
├── load_facility_timeseries.py # Energy ingestion job
├── load_facility_air_quality.py# Air quality ingestion job
└── openmeteo_common.py         # Shared Open-Meteo utilities
```

### 📊 Bronze Tables

| Table | Source | Columns |
|-------|--------|---------|
| `lh.bronze.raw_facility_weather` | Open-Meteo | ~20 columns |
| `lh.bronze.raw_facility_timeseries` | OpenElectricity | ~8 columns |
| `lh.bronze.raw_facility_air_quality` | Open-Meteo | ~12 columns |

### 🔧 Kỹ Thuật Xử Lý

#### ThreadPoolExecutor - Xử Lý Song Song
```python
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(fetch_for_facility, facility): facility 
               for facility in facilities}
```

#### Rate Limiter - Chống API Throttling
```python
limiter = RateLimiter(30.0)  # 30 requests/minute
```

#### MERGE INTO - Upsert và Deduplicate
```sql
MERGE INTO lh.bronze.raw_facility_weather AS target
USING (
    SELECT * FROM (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY facility_code, weather_timestamp 
                          ORDER BY ingest_timestamp DESC) as rn
        FROM weather_source
    ) WHERE rn = 1
) AS source
ON target.facility_code = source.facility_code 
   AND target.weather_timestamp = source.weather_timestamp
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 🔄 Incremental vs Backfill Mode

| Mode | Mô tả |
|------|-------|
| **Incremental** | Query MAX(timestamp), chỉ load dữ liệu mới |
| **Backfill** | User chỉ định --start/--end, rebuild toàn bộ |

---

## 4️⃣ XỬ LÝ, LÀM SẠCH VÀ CHUẨN HOÁ DỮ LIỆU Ở LỚP SILVER

### 📂 Cấu Trúc Thư Mục Silver

```
src/pv_lakehouse/etl/silver/
├── base.py                  # BaseSilverLoader class
├── cli.py                   # Command-line interface
├── hourly_energy.py         # Energy loader
├── hourly_weather.py        # Weather loader  
├── hourly_air_quality.py    # Air quality loader
└── facility_master.py       # Facility master loader
```

### 🎯 Quality Flags System

| Flag | Mô Tả | Xử Lý |
|------|-------|-------|
| **GOOD** | Dữ liệu hợp lệ, pass tất cả checks | Weight = 1.0 trong ML training |
| **WARNING** | Soft check failed, có thể là edge case | Weight = 0.5 trong ML training |
| **BAD** | Hard bounds violated, dữ liệu invalid | Exclude khỏi ML training |

### ⚡ Silver Hourly Energy

**Source:** `lh.bronze.raw_facility_timeseries`  
**Target:** `lh.silver.clean_hourly_energy`

#### Timezone Conversion (UTC → Local):
```python
default_local = F.from_utc_timestamp(F.col("interval_ts"), DEFAULT_TIMEZONE)
for code, tz in FACILITY_TIMEZONES.items():
    tz_expr = F.when(F.col("facility_code") == code,
        F.from_utc_timestamp(F.col("interval_ts"), tz)).otherwise(tz_expr)
```

#### Validation Rules:

**Hard Bounds (→ BAD):**
```python
ENERGY_LOWER = 0.0  # Năng lượng không thể âm
```

**Soft Checks (→ WARNING):**
| Check | Condition |
|-------|-----------|
| NIGHT_ENERGY_ANOMALY | hour ∈ [22, 6) AND energy > 1.0 MWh |
| DAYTIME_ZERO_ENERGY | hour ∈ [8, 17] AND energy == 0 |
| EQUIPMENT_DOWNTIME | hour ∈ [10, 14] AND energy == 0 |

### 🌦️ Silver Hourly Weather

**Source:** `lh.bronze.raw_facility_weather`  
**Target:** `lh.silver.clean_hourly_weather`

#### Validation Bounds:
```python
_numeric_columns = {
    "shortwave_radiation": (0.0, 1150.0),    # W/m²
    "temperature_2m": (-10.0, 50.0),         # °C
    "cloud_cover": (0.0, 100.0),             # %
    "wind_speed_10m": (0.0, 50.0),           # m/s
}
```

### 💨 Silver Hourly Air Quality

**Source:** `lh.bronze.raw_facility_air_quality`  
**Target:** `lh.silver.clean_hourly_air_quality`

#### AQI Calculation (EPA Standard):
```python
def _aqi_from_pm25(self, column):
    return (
        F.when(column <= 12.0, scale(column, 0.0, 12.0, 0, 50))
        .when(column <= 35.4, scale(column, 12.1, 35.4, 51, 100))
        .when(column <= 55.4, scale(column, 35.5, 55.4, 101, 150))
        .otherwise(...)
    )
```

#### AQI Categories:

| AQI Range | Category | Health Impact |
|-----------|----------|---------------|
| 0-50 | **Good** | Air quality is satisfactory |
| 51-100 | **Moderate** | Acceptable; may be risk for sensitive groups |
| 101-200 | **Unhealthy** | Everyone may begin to experience effects |
| 201-500 | **Hazardous** | Health alert; serious health effects |

---

## 📝 KẾT LUẬN

Chung Quang Đăng Khoa đã thực hiện đầy đủ các công việc được phân công:

1. ✅ **Cài đặt và cấu hình hệ thống:** Thiết lập Docker Compose với MinIO, Spark, Trino, PostgreSQL, MLflow
2. ✅ **Nghiên cứu nguồn dữ liệu:** Nghiên cứu 3 API sources (Open-Meteo Weather, Air Quality, OpenElectricity)
3. ✅ **Nạp dữ liệu Bronze:** Xây dựng 3 Bronze loaders với incremental/backfill modes, rate limiting, error handling
4. ✅ **Xử lý dữ liệu Silver:** Xây dựng 3 Silver loaders với timezone conversion, validation, quality flags, AQI calculation

**Tỉ lệ hoàn thành: 100%**
