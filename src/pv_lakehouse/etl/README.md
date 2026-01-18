# 📚 ETL SOURCE CODE DOCUMENTATION

Chi tiết đầy đủ về các file ETL trong PV Lakehouse project.

---

## 📂 TỔNG QUAN CẤU TRÚC

```
src/pv_lakehouse/etl/
├── clients/                     # API Clients
│   ├── openelectricity.py      # OpenElectricity API (NEM Australia)
│   └── openmeteo.py            # Open-Meteo Weather/Air Quality API
├── bronze/                      # Bronze Layer Loaders
│   ├── facilities.js           # Danh sách facility codes
│   ├── facility_timezones.py   # Mapping facility → timezone
│   ├── load_facilities.py      # Load metadata nhà máy
│   ├── load_facility_weather.py      # Load thời tiết
│   ├── load_facility_timeseries.py   # Load năng lượng
│   ├── load_facility_air_quality.py  # Load chất lượng k.khí
│   └── openmeteo_common.py     # Shared utilities
└── silver/                      # Silver Layer Loaders
    ├── base.py                 # Base class cho Silver loaders
    ├── cli.py                  # Command-line interface
    ├── facility_master.py      # Facility master loader
    ├── hourly_energy.py        # Energy loader
    ├── hourly_weather.py       # Weather loader
    └── hourly_air_quality.py   # Air quality loader
```

---

## 🔌 API CLIENTS

### 📡 openelectricity.py

**Mục đích:** Client gọi OpenElectricity API để lấy dữ liệu năng lượng từ thị trường NEM Australia.

| Hàm | Đầu vào | Đầu ra | Ý nghĩa |
|-----|---------|--------|---------|
| `load_default_facility_codes()` | Path file JS (optional) | `List[str]` | Đọc danh sách facility codes từ `facilities.js` |
| `load_api_key(cli_key)` | API key (optional) | `str` | Lấy API key từ CLI, ENV, hoặc `.env` file |
| `fetch_facilities_dataframe()` | api_key, networks, statuses, fueltechs | `pd.DataFrame` | Lấy metadata của các nhà máy |
| `fetch_facility_timeseries_dataframe()` | facility_codes, metrics, interval, date_start, date_end | `pd.DataFrame` | Lấy dữ liệu timeseries (energy/power) |

**Constants quan trọng:**
```python
SUPPORTED_INTERVALS = {"5m", "1h", "1d", "7d", "1M", "3M", "season", "1y", "fy"}
NETWORK_TIMEZONE_IDS = {"NEM": "Australia/Brisbane", "WEM": "Australia/Perth"}
```

---

### 🌦️ openmeteo.py

**Mục đích:** Client gọi Open-Meteo API để lấy dữ liệu thời tiết và chất lượng không khí.

| Hàm | Đầu vào | Đầu ra | Ý nghĩa |
|-----|---------|--------|---------|
| `RateLimiter(max_requests_per_minute)` | float | `RateLimiter` | Rate limiter để tránh API throttling |
| `fetch_weather_dataframe()` | facility, start, end, timezone, limiter | `pd.DataFrame` | Lấy dữ liệu thời tiết theo giờ |
| `fetch_air_quality_dataframe()` | facility, start, end, timezone, limiter | `pd.DataFrame` | Lấy dữ liệu chất lượng không khí theo giờ |

**Biến thời tiết mặc định:**
```python
DEFAULT_WEATHER_VARS = (
    "shortwave_radiation,direct_radiation,diffuse_radiation,..."
    "temperature_2m,cloud_cover,precipitation,wind_speed_10m,..."
)
```

**Dataclass:**
```python
@dataclass
class FacilityLocation:
    code: str       # VD: "NYNGAN"
    name: str       # Tên đầy đủ
    latitude: float
    longitude: float
```

---

## 🥉 BRONZE LAYER

### 📁 facilities.js

**Mục đích:** Danh sách hardcoded các facility codes sẽ được load.

```javascript
export const DEFAULT_FACILITY_CODES = [
  "WRSF1", "AVLSF", "BOMENSF", "YATSF1", "LIMOSF2", 
  "FINLEYSF", "EMERASF", "DARLSF"
];
```

---

### 🕐 facility_timezones.py

**Mục đích:** Mapping từ facility code → IANA timezone.

| Hàm | Đầu vào | Đầu ra | Ý nghĩa |
|-----|---------|--------|---------|
| `get_facility_timezone(facility_code)` | `str` | `str` | Trả về timezone của facility |

```python
DEFAULT_TIMEZONE = "Australia/Brisbane"

def get_facility_timezone(facility_code: str) -> str:
    return FACILITY_TIMEZONES.get(facility_code.upper(), DEFAULT_TIMEZONE)
```

---

### 🏭 load_facilities.py

**Mục đích:** Load metadata các nhà máy vào Bronze table.

| Thành phần | Giá trị |
|------------|---------|
| **Table** | `lh.bronze.raw_facilities` |
| **Mode** | Overwrite (master data) |
| **Source** | OpenElectricity API |

**Flow:**
```
1. Parse args (--mode, --facility-codes, --api-key)
2. Gọi openelectricity.fetch_facilities_dataframe()
3. Thêm cột: ingest_mode, ingest_timestamp, ingest_date
4. Write Iceberg table (mode=overwrite)
```

---

### 🌤️ load_facility_weather.py

**Mục đích:** Load dữ liệu thời tiết từ Open-Meteo vào Bronze.

| Thành phần | Giá trị |
|------------|---------|
| **Table** | `lh.bronze.raw_facility_weather` |
| **Source** | Open-Meteo Archive/Forecast API |
| **Key columns** | facility_code, weather_timestamp |

**Kỹ thuật:**
```python
# 1. ThreadPoolExecutor - Parallel fetching
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(fetch_for_facility, f): f for f in facilities}

# 2. Rate Limiter
limiter = RateLimiter(30.0)  # 30 requests/minute

# 3. MERGE INTO - Upsert với deduplication
MERGE INTO table AS target
USING (SELECT *, ROW_NUMBER() OVER (...) as rn FROM source WHERE rn=1) AS source
ON target.facility_code = source.facility_code 
   AND target.weather_timestamp = source.weather_timestamp
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

---

### ⚡ load_facility_timeseries.py

**Mục đích:** Load dữ liệu năng lượng từ OpenElectricity vào Bronze.

| Thành phần | Giá trị |
|------------|---------|
| **Table** | `lh.bronze.raw_facility_timeseries` |
| **Source** | OpenElectricity API |
| **Interval** | 1h (hourly) |
| **Key columns** | facility_code, interval_ts, metric |

**Schema:**

| Column | Type | Mô tả |
|--------|------|-------|
| `facility_code` | string | Mã nhà máy |
| `interval_ts` | timestamp | Thời gian (UTC) |
| `metric` | string | "energy" hoặc "power" |
| `value` | double | Giá trị (MWh hoặc MW) |

---

### 💨 load_facility_air_quality.py

**Mục đích:** Load dữ liệu chất lượng không khí từ Open-Meteo vào Bronze.

| Thành phần | Giá trị |
|------------|---------|
| **Table** | `lh.bronze.raw_facility_air_quality` |
| **Source** | Open-Meteo Air Quality API |
| **Key columns** | facility_code, air_timestamp |

**Các biến được load:**
- pm2_5, pm10, dust
- nitrogen_dioxide, ozone, sulphur_dioxide, carbon_monoxide
- uv_index, uv_index_clear_sky

---

### 🔧 openmeteo_common.py

**Mục đích:** Shared utilities cho các Open-Meteo loaders.

| Hàm | Đầu vào | Đầu ra | Ý nghĩa |
|-----|---------|--------|---------|
| `parse_csv(value)` | `str` | `List[str]` | Split chuỗi theo dấu phẩy |
| `parse_date(value)` | `str` | `date` | Parse "YYYY-MM-DD" → date |
| `resolve_facility_codes(codes)` | `str` | `List[str]` | Lấy facility codes |
| `load_facility_locations(codes, api_key)` | `List[str]`, `str` | `List[FacilityLocation]` | Lấy metadata + coordinates |
| `write_dataset(spark_df, ...)` | DataFrame, table, mode | `None` | Ghi vào Iceberg với MERGE |

---

## 🥈 SILVER LAYER

### 🏗️ base.py

**Mục đích:** Base class cho tất cả Silver loaders.

**Dataclass LoadOptions:**
```python
@dataclass
class LoadOptions:
    mode: str = "incremental"     # "full" hoặc "incremental"
    start: Optional[datetime]     # Lower bound
    end: Optional[datetime]       # Upper bound
    load_strategy: str = "merge"  # "overwrite" hoặc "merge"
```

**Class BaseSilverLoader:**

| Method | Ý nghĩa |
|--------|---------|
| `run()` | Entry point chính - đọc Bronze, transform, write Silver |
| `transform(bronze_df)` | Abstract - subclass phải override |
| `_read_bronze()` | Đọc Bronze table với time filters |
| `_write_outputs(df)` | Ghi vào Silver Iceberg table |
| `_process_in_chunks(df, chunk_days)` | Xử lý từng chunk để tiết kiệm memory |
| `_get_hour_offset()` | Override nếu cần shift timestamp |
| `_get_timezone_lookback_hours()` | Override nếu cần timezone lookback |

---

### 🖥️ cli.py

**Mục đích:** Command-line interface để chạy Silver loaders.

**Usage:**
```bash
spark-submit cli.py <dataset> [--mode] [--load-strategy] [--start] [--end]
```

**Datasets:**
- `facility_master`
- `hourly_energy`
- `hourly_weather`
- `hourly_air_quality`

---

### 🏭 facility_master.py

**Mục đích:** SCD Type 2 cho facility metadata.

| Thành phần | Giá trị |
|------------|---------|
| **Bronze** | `lh.bronze.raw_facilities` |
| **Silver** | `lh.silver.clean_facility_master` |
| **Partition** | `facility_code` |

**Output schema:**
- facility_code, facility_name, network_id, network_region
- location_lat, location_lng, total_capacity_mw
- effective_from, effective_to, is_current
- is_valid, quality_flag

---

### ⚡ hourly_energy.py

**Mục đích:** Làm sạch và validate dữ liệu năng lượng.

| Thành phần | Giá trị |
|------------|---------|
| **Bronze** | `lh.bronze.raw_facility_timeseries` |
| **Silver** | `lh.silver.clean_hourly_energy` |
| **Partition** | `date_hour` |

**Transform logic:**
```python
# 1. Timezone conversion (UTC → Local)
tz_expr = F.from_utc_timestamp(F.col("interval_ts"), tz)

# 2. Hour-End Labeling (+1 giờ)
.withColumn("date_hour", F.date_trunc("hour", 
    F.expr("timestamp_local + INTERVAL 1 HOUR")))

# 3. Aggregate by hour
.groupBy("facility_code", "date_hour")
.agg(F.sum("energy_mwh"), F.count("intervals_count"))
```

**Validation rules:**

| Check | Condition | Flag |
|-------|-----------|------|
| OUT_OF_BOUNDS | energy < 0 | BAD |
| NIGHT_ENERGY_ANOMALY | 22:00-06:00 AND energy > 1.0 | WARNING |
| DAYTIME_ZERO_ENERGY | 08:00-17:00 AND energy == 0 | WARNING |
| EQUIPMENT_DOWNTIME | 10:00-14:00 AND energy == 0 | WARNING |
| TRANSITION_HOUR_LOW_ENERGY | Sunrise/sunset với energy thấp | WARNING |
| PEAK_HOUR_LOW_ENERGY | Peak hours với energy < 50% expected | WARNING |

---

### 🌦️ hourly_weather.py

**Mục đích:** Làm sạch và validate dữ liệu thời tiết.

| Thành phần | Giá trị |
|------------|---------|
| **Bronze** | `lh.bronze.raw_facility_weather` |
| **Silver** | `lh.silver.clean_hourly_weather` |
| **Partition** | `date_hour` |

**Validation bounds (_numeric_columns):**
```python
_numeric_columns = {
    "shortwave_radiation": (0.0, 1150.0),  # W/m²
    "direct_radiation": (0.0, 1050.0),
    "temperature_2m": (-10.0, 50.0),        # °C
    "cloud_cover": (0.0, 100.0),            # %
    "wind_speed_10m": (0.0, 50.0),          # m/s
    "pressure_msl": (985.0, 1050.0),        # hPa
}
```

**Soft checks:**
- NIGHT_RADIATION_SPIKE: Ban đêm nhưng radiation > 100
- RADIATION_INCONSISTENCY: Direct + Diffuse > Shortwave × 1.05
- CLOUD_MEASUREMENT_INCONSISTENCY: Peak sun + cloud > 98% + radiation < 600
- EXTREME_TEMPERATURE: temp < -10°C hoặc > 45°C

---

### 💨 hourly_air_quality.py

**Mục đích:** Làm sạch và tính AQI từ dữ liệu chất lượng không khí.

| Thành phần | Giá trị |
|------------|---------|
| **Bronze** | `lh.bronze.raw_facility_air_quality` |
| **Silver** | `lh.silver.clean_hourly_air_quality` |
| **Partition** | `date_hour` |

**AQI Calculation (EPA Standard):**
```python
def _aqi_from_pm25(self, column):
    return (
        F.when(column <= 12.0, scale(column, 0, 12, 0, 50))
        .when(column <= 35.4, scale(column, 12.1, 35.4, 51, 100))
        .when(column <= 55.4, scale(column, 35.5, 55.4, 101, 150))
        .when(column <= 150.4, scale(column, 55.5, 150.4, 151, 200))
        .when(column <= 250.4, scale(column, 150.5, 250.4, 201, 300))
        .otherwise(scale(column, 250.5, 500, 301, 500))
    )
```

**AQI Categories:**

| Range | Category | Ý nghĩa |
|-------|----------|---------|
| 0-50 | Good | Không khí tốt |
| 51-100 | Moderate | Chấp nhận được |
| 101-200 | Unhealthy | Ảnh hưởng sức khỏe |
| 201-500 | Hazardous | Nguy hiểm |

---

## 🚀 COMMANDS CHẠY ETL

### Bronze Jobs

```bash
# Weather
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py

# Energy  
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py

# Air Quality
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py
```

### Silver Jobs

```bash
# Energy
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode incremental --load-strategy merge

# Weather
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_weather \
  --mode incremental --load-strategy merge

# Air Quality
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_air_quality \
  --mode incremental --load-strategy merge
```

---

## 📊 DIAGRAM LUỒNG DỮ LIỆU

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐                    │
│  │OpenMeteo   │    │OpenMeteo   │    │OpenElec.   │                    │
│  │Weather API │    │Air Quality │    │API         │                    │
│  └─────┬──────┘    └─────┬──────┘    └─────┬──────┘                    │
│        │                 │                 │                            │
│        ▼                 ▼                 ▼                            │
│  ┌────────────────────────────────────────────────────────────┐        │
│  │                    BRONZE LAYER                             │        │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐   │        │
│  │  │raw_facility │ │raw_facility │ │raw_facility         │   │        │
│  │  │_weather     │ │_air_quality │ │_timeseries          │   │        │
│  │  │(LOCAL time) │ │(LOCAL time) │ │(UTC time)           │   │        │
│  │  └──────┬──────┘ └──────┬──────┘ └──────────┬──────────┘   │        │
│  └─────────┼───────────────┼───────────────────┼──────────────┘        │
│            │               │                   │                        │
│            ▼               ▼                   ▼                        │
│  ┌────────────────────────────────────────────────────────────┐        │
│  │                    SILVER LAYER                             │        │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐   │        │
│  │  │clean_hourly │ │clean_hourly │ │clean_hourly         │   │        │
│  │  │_weather     │ │_air_quality │ │_energy              │   │        │
│  │  │+ validation │ │+ AQI calc   │ │+ UTC→Local convert  │   │        │
│  │  │+ quality_   │ │+ quality_   │ │+ quality_flag       │   │        │
│  │  │  flag       │ │  flag       │ │+ completeness_pct   │   │        │
│  │  └─────────────┘ └─────────────┘ └─────────────────────┘   │        │
│  └────────────────────────────────────────────────────────────┘        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```
