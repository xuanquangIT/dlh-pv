# Bronze-Silver ELT Operations Guide

**Tác Giả:** Data Engineering Team  
**Cập Nhật:** 2025-12-04  
**Phiên Bản:** 2.0

---

## 📌 Tổng Quan

### ELT vs ETL - Hệ thống này là **ELT**!

```
┌─────────────────────────────────────────────────────────────────┐
│  ELT (Extract - Load - Transform)                               │
│                                                                  │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐      │
│  │ Extract │ -> │  Load   │ -> │Transform│ -> │  Load   │      │
│  │  (API)  │    │ (Bronze)│    │ (Spark) │    │ (Silver)│      │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘      │
│                                                                  │
│  1. EXTRACT: Gọi API (OpenElectricity, Open-Meteo)              │
│  2. LOAD:    Ghi raw data vào Bronze (Iceberg)                  │
│  3. TRANSFORM: Spark đọc Bronze, clean, validate, aggregate     │
│  4. LOAD:    Ghi transformed data vào Silver (Iceberg)          │
│                                                                  │
│  ⚠️ TRANSFORM xảy ra SAU khi LOAD vào Bronze                    │
│  → Đây là ELT, KHÔNG phải ETL                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Tại sao chọn ELT thay vì ETL?**
| Aspect | ETL (Transform trước Load) | ELT (Transform sau Load) |
|--------|---------------------------|--------------------------|
| Raw data | ❌ Mất nếu transform có bug | ✅ Luôn giữ trong Bronze |
| Re-process | ❌ Phải gọi lại API | ✅ Đọc từ Bronze |
| Debug | ❌ Khó trace nguồn gốc | ✅ So sánh Bronze vs Silver |
| Schema evolution | ❌ Phức tạp | ✅ Bronze giữ original schema |

---

## 🔄 CHẾ ĐỘ CHẠY: INCREMENTAL vs FULL vs BACKFILL

### Tổng quan 3 Modes

```
┌─────────────────────────────────────────────────────────────────┐
│  SO SÁNH 3 MODES                                                │
│                                                                  │
│  ┌───────────────┬───────────────┬───────────────┐              │
│  │  INCREMENTAL  │     FULL      │   BACKFILL    │              │
│  ├───────────────┼───────────────┼───────────────┤              │
│  │ Chỉ data mới  │ Toàn bộ data  │ Specific range│              │
│  │ merge strategy│ overwrite all │ overwrite range│             │
│  │ --mode incr   │ --mode full   │ --mode full   │              │
│  │               │               │ + --start/end │              │
│  │ Daily/hourly  │ First setup   │ Fix specific  │              │
│  │ operations    │ or rebuild    │ date range    │              │
│  └───────────────┴───────────────┴───────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

### 1. Incremental Mode (Mặc định - Recommended)

**Mục đích:** Cập nhật dữ liệu mới hàng ngày/giờ.

```
┌─────────────────────────────────────────────────────────────────┐
│  INCREMENTAL MODE - Logic Flow                                  │
│                                                                  │
│  1. Query max timestamp từ table hiện tại                       │
│     SELECT MAX(timestamp) FROM table                            │
│                                                                  │
│  2. Xác định start date:                                        │
│     - Nếu max_ts = NULL → First run, start = earliest data      │
│     - Nếu max_ts < current_hour → start = max_ts - lookback     │
│     - Nếu max_ts >= current_hour → reload from current - lookback│
│                                                                  │
│  3. Fetch data từ [start, now]                                  │
│                                                                  │
│  4. Transform và Write (merge strategy)                         │
│     - Partition overwrite: chỉ ghi đè partitions bị ảnh hưởng   │
│     - Không xóa data cũ ngoài range                             │
└─────────────────────────────────────────────────────────────────┘
```

**Command:**
```bash
# Bronze - Incremental (default)
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py

# Silver - Incremental  
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --driver-memory 2g --executor-memory 3g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode incremental --load-strategy merge
```

**Ưu điểm:**
- ✅ Nhanh - chỉ xử lý data mới
- ✅ Tiết kiệm API calls (Bronze)
- ✅ Tiết kiệm compute resources (Silver)
- ✅ Idempotent - có thể rerun an toàn

**Nhược điểm:**
- ❌ Cần maintain state (last loaded timestamp)
- ❌ Không thể sửa historical data errors

---

### 2. Full Mode (Rebuild toàn bộ)

**Mục đích:** Rebuild toàn bộ Silver table từ Bronze.

```
┌─────────────────────────────────────────────────────────────────┐
│  FULL MODE - Logic Flow                                         │
│                                                                  │
│  1. Đọc TOÀN BỘ Bronze data (không filter by timestamp)         │
│                                                                  │
│  2. Transform tất cả data                                       │
│                                                                  │
│  3. Write với overwrite strategy:                               │
│     - Option 1: Xóa toàn bộ Silver table, ghi mới               │
│     - Option 2: Partition overwrite (giữ structure)             │
│                                                                  │
│  ⚠️ CẢNH BÁO: Tốn thời gian và resources cho large datasets!    │
└─────────────────────────────────────────────────────────────────┘
```

**Command:**
```bash
# Silver - Full rebuild
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --driver-memory 2g --executor-memory 3g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode full --load-strategy overwrite
```

**Ưu điểm:**
- ✅ Rebuild từ Bronze (source of truth)
- ✅ Fix tất cả data errors
- ✅ Apply new transformation logic
- ✅ Không phụ thuộc state

**Nhược điểm:**
- ❌ Tốn thời gian (process all data)
- ❌ Có thể ảnh hưởng downstream (Gold, ML)
- ❌ Không cần thiết cho daily operations

---

### 3. Backfill Mode (Specific Date Range)

**Mục đích:** Rebuild một period cụ thể (fix data cho specific dates).

```
┌─────────────────────────────────────────────────────────────────┐
│  BACKFILL MODE - Logic Flow                                     │
│                                                                  │
│  1. User chỉ định date range:                                   │
│     --start 2025-01-01T00:00:00 --end 2025-01-31T23:59:59       │
│                                                                  │
│  2. Filter Bronze data trong range                              │
│                                                                  │
│  3. Transform và Write (overwrite strategy)                     │
│     - Chỉ xóa/ghi đè partitions trong range                     │
│     - Giữ nguyên data ngoài range                               │
│                                                                  │
│  ⚠️ QUAN TRỌNG: Dùng --mode full + --start/--end                │
└─────────────────────────────────────────────────────────────────┘
```

**Command:**
```bash
# Bronze - Backfill specific dates (gọi lại API)
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py \
  --start 2025-01-01 --end 2025-01-31 --mode backfill

# Silver - Backfill specific dates (transform từ Bronze)
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --driver-memory 2g --executor-memory 3g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode full \
  --start 2025-01-01T00:00:00 --end 2025-01-31T23:59:59 \
  --load-strategy overwrite
```

**Ưu điểm:**
- ✅ Có thể rebuild historical data
- ✅ Fix data errors
- ✅ Không phụ thuộc state

**Nhược điểm:**
- ❌ Tốn API calls (Bronze)
- ❌ Tốn thời gian và resources
- ❌ Có thể ảnh hưởng downstream (Gold, ML models)

---

### 4. Khi nào dùng mode nào?

| Scenario | Mode | Command Flag | Lý do |
|----------|------|--------------|-------|
| Daily/hourly update | **Incremental** | `--mode incremental` | Chỉ load data mới, nhanh |
| First time setup | **Full** | `--mode full` | Cần toàn bộ historical |
| After code change | **Full** | `--mode full` | Apply new logic to all data |
| Fix specific dates | **Backfill** | `--mode full --start X --end Y` | Rebuild only affected range |
| API returned wrong data | **Backfill** | Bronze backfill + Silver backfill | Reload from API then transform |
| Schema change | **Full** | `--mode full --load-strategy overwrite` | Rebuild với schema mới |
| Normal daily ops | **Incremental** | `--mode incremental` | Production default |

---

## 📊 SỐ LƯỢNG RECORDS: ENERGY vs WEATHER/AIR QUALITY

### Kết quả thực tế từ hệ thống (2025-12-04):

```
┌────────────────────────┬─────────────────┬─────────────────┐
│ Table                  │ Bronze Records  │ Silver Records  │
├────────────────────────┼─────────────────┼─────────────────┤
│ Energy (Timeseries)    │     64,768      │     64,768      │
│ Weather                │     64,776      │     64,776      │
│ Air Quality            │     64,776      │     64,776      │
└────────────────────────┴─────────────────┴─────────────────┘

CHÊNH LỆCH: Weather/Air có HƠN Energy 8 records
```

### ⚠️ TẠI SAO ENERGY CÓ ÍT HƠN 8 RECORDS?

**Nguyên nhân: API Data Range khác nhau + Hour-End Labeling**

```
┌─────────────────────────────────────────────────────────────────┐
│  PHÂN TÍCH CHI TIẾT                                             │
│                                                                  │
│  📊 BRONZE DATA RANGE:                                          │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Energy (OpenElectricity API):                              │ │
│  │   Start: 2024-12-31 14:00:00 UTC                          │ │
│  │   End:   2025-12-03 22:00:00 UTC                          │ │
│  │   → API bắt đầu từ giữa ngày Dec 31, 2024                 │ │
│  │                                                            │ │
│  │ Weather/Air Quality (Open-Meteo API):                      │ │
│  │   Start: 2025-01-01 00:00:00 UTC                          │ │
│  │   End:   2025-12-04 08:00:00 UTC                          │ │
│  │   → API bắt đầu đầy đủ từ 00:00 ngày Jan 1                │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  📊 SILVER NGÀY ĐẦU TIÊN (2025-01-01):                          │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Energy:                                                    │ │
│  │   Min hour: 2025-01-01 01:00:00 (thiếu 00:00!)            │ │
│  │   Max hour: 2025-01-01 23:00:00                           │ │
│  │   Records: 184 (= 8 facilities × 23 hours)                │ │
│  │                                                            │ │
│  │ Weather:                                                   │ │
│  │   Min hour: 2025-01-01 00:00:00 (đầy đủ)                  │ │
│  │   Max hour: 2025-01-01 23:00:00                           │ │
│  │   Records: 192 (= 8 facilities × 24 hours)                │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  CHÊNH LỆCH: 192 - 184 = 8 records (đúng = 8 facilities)       │
└─────────────────────────────────────────────────────────────────┘
```

### Giải thích Hour-End Labeling Effect:

```
┌─────────────────────────────────────────────────────────────────┐
│  HOUR-END LABELING (Energy data)                                │
│                                                                  │
│  OpenElectricity API trả về:                                    │
│  - interval_start = thời điểm BẮT ĐẦU của interval             │
│  - Energy measured over [interval_start, interval_start + 1h)   │
│                                                                  │
│  Hour-End Convention:                                            │
│  - Energy của [00:00, 01:00) được gán label 01:00               │
│  - Energy của [23:00, 00:00) được gán label 00:00 (ngày sau)    │
│                                                                  │
│  BRONZE:                     SILVER (after +1h shift):          │
│  2024-12-31 14:00 UTC   →   2025-01-01 01:00 local (+10+1)     │
│  2025-01-01 00:00 UTC   →   2025-01-01 11:00 local (+10+1)     │
│                                                                  │
│  ⚠️ Không có Bronze data cho 2024-12-31 13:00 UTC trở về trước │
│  → Không thể tạo Silver record cho 2025-01-01 00:00 local       │
│  → Thiếu 8 records (1 hour × 8 facilities)                      │
└─────────────────────────────────────────────────────────────────┘
```

### Weather/Air Quality KHÔNG bị ảnh hưởng vì:

```
┌─────────────────────────────────────────────────────────────────┐
│  WEATHER/AIR QUALITY - Local Time từ API                        │
│                                                                  │
│  Open-Meteo API request:                                        │
│    timezone: "Australia/Brisbane" (hoặc facility timezone)      │
│                                                                  │
│  Response đã là LOCAL TIME:                                     │
│    2025-01-01 00:00 (local) → Silver 2025-01-01 00:00          │
│    2025-01-01 01:00 (local) → Silver 2025-01-01 01:00          │
│    ...                                                          │
│                                                                  │
│  → Không có timezone conversion                                 │
│  → Không có hour-end shift                                      │
│  → Mỗi ngày đều đủ 24 hours × 8 facilities = 192 records       │
└─────────────────────────────────────────────────────────────────┘
```

### ✅ KẾT LUẬN

| Metric | Giá trị | Giải thích |
|--------|---------|------------|
| Chênh lệch | 8 records | = 1 hour × 8 facilities |
| Nguyên nhân | API data range | Energy API bắt đầu muộn hơn |
| Hour bị thiếu | 2025-01-01 00:00 | Do không có Bronze data trước đó |
| Có mất data? | **KHÔNG** | Đây là giới hạn của source data |

---

## 🔧 LOOKBACK HOURS TRONG INCREMENTAL MODE

### Tại sao cần Lookback?

```
┌─────────────────────────────────────────────────────────────────┐
│  VẤN ĐỀ: Silver incremental bỏ sót data                         │
│                                                                  │
│  Silver max(date_hour) = 2025-12-04 09:00 local                 │
│  Nếu query Bronze từ 2025-12-04 09:00:                          │
│  → Miss Bronze data 2025-12-03 22:00 UTC (= 2025-12-04 08:00 local)
│                                                                  │
│  GIẢI PHÁP: Lookback thêm giờ                                   │
│                                                                  │
│  Energy: total_lookback = 1 (hour shift) + 12 (timezone) = 13h  │
│  Weather/Air: total_lookback = 0 (no conversion needed)         │
└─────────────────────────────────────────────────────────────────┘
```

### Configuration trong Code

```python
# File: src/pv_lakehouse/etl/silver/base.py
MAX_TIMEZONE_OFFSET_HOURS = 12  # Max UTC offset for Australia

# File: src/pv_lakehouse/etl/silver/hourly_energy.py
def _get_hour_offset(self) -> int:
    return 1  # Hour-end labeling shift

def _get_timezone_lookback_hours(self) -> int:
    return 12  # From base class (MAX_TIMEZONE_OFFSET_HOURS)

# Total: 1 + 12 = 13 hours lookback

# File: src/pv_lakehouse/etl/silver/hourly_weather.py
def _get_timezone_lookback_hours(self) -> int:
    return 0  # Override - no timezone conversion needed
```

---

## 🚀 COMMAND REFERENCE

### Bronze Layer

```bash
# Weather - Incremental
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py

# Weather - Backfill
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py \
  --start 2025-01-01 --end 2025-01-31

# Energy - Incremental
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py

# Air Quality - Incremental
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_air_quality.py
```

### Silver Layer

```bash
# Energy - Incremental (recommended)
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --driver-memory 2g --executor-memory 3g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode incremental --load-strategy merge

# Energy - Backfill specific dates
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --driver-memory 2g --executor-memory 3g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode full --start 2025-01-01T00:00:00 --end 2025-01-31T23:59:59 \
  --load-strategy overwrite

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

# Run all Silver in sequence
docker compose -f docker/docker-compose.yml exec spark-master bash -c "
  spark-submit --driver-memory 2g --executor-memory 3g \
    /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
    --mode incremental --load-strategy merge && \
  spark-submit --driver-memory 2g --executor-memory 3g \
    /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_weather \
    --mode incremental --load-strategy merge && \
  spark-submit --driver-memory 2g --executor-memory 3g \
    /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_air_quality \
    --mode incremental --load-strategy merge
"
```

---

## 🔍 TROUBLESHOOTING

### Problem: Silver có ít records hơn Bronze

**Check 1:** Verify row counts
```sql
SELECT 
    'Bronze' AS layer, COUNT(*) AS rows
FROM iceberg.bronze.raw_facility_timeseries
UNION ALL
SELECT 'Silver', COUNT(*)
FROM iceberg.silver.clean_hourly_energy;
```

**Check 2:** Nếu Silver < Bronze, run incremental với manual start
```bash
# Force reload từ specific date
spark-submit ... --mode incremental --start 2025-12-01T00:00:00
```

### Problem: Missing data for specific days

**Solution:** Backfill specific range
```bash
spark-submit ... --mode full \
  --start 2025-12-01T00:00:00 --end 2025-12-05T23:59:59 \
  --load-strategy overwrite
```

### Problem: Duplicate records

**Check:** Count by primary key
```sql
SELECT facility_code, date_hour, COUNT(*)
FROM iceberg.silver.clean_hourly_energy
GROUP BY facility_code, date_hour
HAVING COUNT(*) > 1;
```

**Solution:** Backfill with overwrite to deduplicate

---

**Version:** 2.0  
**Last Updated:** 2025-01-16
