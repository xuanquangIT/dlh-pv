# 📊 SQL QUERIES REFERENCE

Tất cả SQL queries được sử dụng trong Bronze và Silver ETL pipelines.

---

## 🥉 BRONZE LAYER QUERIES

### 1. load_facility_timeseries.py

#### Query 1: Detect Incremental Start (Line 52)
```sql
SELECT MAX(interval_ts) FROM lh.bronze.raw_facility_timeseries
```
**Mục đích:** Tìm timestamp mới nhất để load incremental

---

#### Query 2: Backfill với Deduplication (Line 148-155)
```sql
INSERT OVERWRITE TABLE lh.bronze.raw_facility_timeseries
SELECT * FROM (
    SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY facility_code, interval_ts, metric 
        ORDER BY ingest_timestamp DESC
    ) as rn
    FROM timeseries_source
) WHERE rn = 1
```
**Mục đích:** Overwrite toàn bộ table, chỉ giữ bản ghi mới nhất cho mỗi key

---

#### Query 3: Incremental MERGE với Deduplication (Line 168-182)
```sql
MERGE INTO lh.bronze.raw_facility_timeseries AS target
USING (
    SELECT * FROM (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY facility_code, interval_ts, metric 
            ORDER BY ingest_timestamp DESC
        ) as rn
        FROM timeseries_source
    ) WHERE rn = 1
) AS source
ON target.facility_code = source.facility_code 
    AND target.interval_ts = source.interval_ts 
    AND target.metric = source.metric
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```
**Mục đích:** UPSERT với deduplication - giữ bản ghi mới nhất

---

### 2. load_facility_weather.py

#### Query 1: Detect Incremental Start (Line 83)
```sql
SELECT MAX(weather_timestamp) FROM lh.bronze.raw_facility_weather
```
**Mục đích:** Tìm timestamp mới nhất để load incremental

---

#### Query 2: MERGE Weather Data (Line 145-157)
```sql
MERGE INTO lh.bronze.raw_facility_weather AS target
USING (
    SELECT * FROM (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY facility_code, weather_timestamp 
            ORDER BY ingest_timestamp DESC
        ) as rn
        FROM weather_source
    ) WHERE rn = 1
) AS source
ON target.facility_code = source.facility_code 
   AND target.weather_timestamp = source.weather_timestamp
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```
**Mục đích:** UPSERT weather data với deduplication

---

### 3. load_facility_air_quality.py

#### Query 1: Detect Incremental Start (Line 82)
```sql
SELECT MAX(air_timestamp) FROM lh.bronze.raw_facility_air_quality
```
**Mục đích:** Tìm timestamp mới nhất để load incremental

---

#### Query 2: MERGE Air Quality Data (Line 131-143)
```sql
MERGE INTO lh.bronze.raw_facility_air_quality AS target
USING (
    SELECT * FROM (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY facility_code, air_timestamp 
            ORDER BY ingest_timestamp DESC
        ) as rn
        FROM air_source
    ) WHERE rn = 1
) AS source
ON target.facility_code = source.facility_code 
   AND target.air_timestamp = source.air_timestamp
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```
**Mục đích:** UPSERT air quality data với deduplication

---

### 4. openmeteo_common.py

#### Query: Generic MERGE (Line 119-128)
```sql
MERGE INTO {iceberg_table} AS target
USING {temp_view} AS source
ON {merge_keys}
WHEN MATCHED THEN
    UPDATE SET {update_set}
WHEN NOT MATCHED THEN
    INSERT ({insert_cols})
    VALUES ({insert_vals})
```
**Mục đích:** Fallback MERGE utility cho Open-Meteo data

**Merge keys được xác định tự động:**
- Weather: `facility_code` + `weather_timestamp`
- Air Quality: `facility_code` + `air_timestamp`

---

## 🥈 SILVER LAYER QUERIES

### 1. base.py (BaseSilverLoader)

#### Query 1: Get Silver MAX Timestamp (Line 225-228)
```sql
SELECT MAX({silver_timestamp_column}) as max_ts
FROM {silver_table}
```
**Mục đích:** Incremental load - tìm timestamp mới nhất đã load

**Ví dụ thực tế:**
```sql
SELECT MAX(date_hour) as max_ts FROM lh.silver.clean_hourly_energy
SELECT MAX(date_hour) as max_ts FROM lh.silver.clean_hourly_weather
```

---

#### Query 2: Get Bronze MIN Timestamp (Line 237-240)
```sql
SELECT MIN(CAST({timestamp_column} AS TIMESTAMP)) as min_ts
FROM {bronze_table}
```
**Mục đích:** First-run detection - tìm timestamp sớm nhất trong Bronze

**Ví dụ thực tế:**
```sql
SELECT MIN(CAST(interval_ts AS TIMESTAMP)) as min_ts 
FROM lh.bronze.raw_facility_timeseries

SELECT MIN(CAST(weather_timestamp AS TIMESTAMP)) as min_ts 
FROM lh.bronze.raw_facility_weather
```

---

## 📝 TỔNG KẾT

### Loại queries sử dụng

| Loại Query | Số lần | Mục đích |
|------------|--------|----------|
| `SELECT MAX(timestamp)` | 6 | Incremental detection |
| `SELECT MIN(timestamp)` | 1 | First-run detection |
| `MERGE INTO ... USING` | 4 | UPSERT với deduplication |
| `INSERT OVERWRITE` | 1 | Backfill mode |
| `ROW_NUMBER() OVER (...)` | 4 | Deduplication |

---

### Pattern chung: MERGE với Deduplication

**Template:**
```sql
MERGE INTO {target_table} AS target
USING (
    -- Deduplication subquery
    SELECT * FROM (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {key_columns}
            ORDER BY ingest_timestamp DESC
        ) as rn
        FROM {temp_view}
    ) WHERE rn = 1
) AS source
ON {match_condition}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Tại sao cần deduplication?**
- API có thể trả về duplicate records
- Chỉ giữ bản ghi mới nhất (`ORDER BY ingest_timestamp DESC`)
- `ROW_NUMBER() ... WHERE rn = 1` đảm bảo chỉ lấy 1 record/key

---

### Window Functions

#### ROW_NUMBER() OVER (...)

**Cú pháp:**
```sql
ROW_NUMBER() OVER (
    PARTITION BY {group_columns}  -- Nhóm theo cột nào
    ORDER BY {sort_column} DESC   -- Sắp xếp như thế nào
) as rn
```

**Ví dụ:**
```sql
ROW_NUMBER() OVER (
    PARTITION BY facility_code, interval_ts, metric
    ORDER BY ingest_timestamp DESC
) as rn
```

| facility_code | interval_ts | metric | ingest_ts | rn |
|---------------|-------------|--------|-----------|-----|
| NYNGAN | 2025-01-01 10:00 | energy | 2025-01-02 08:00 | **1** ← Mới nhất |
| NYNGAN | 2025-01-01 10:00 | energy | 2025-01-01 08:00 | 2 |

→ `WHERE rn = 1` giữ lại chỉ row đầu tiên (mới nhất)

---

## 🔍 GHI CHÚ

1. **PySpark DataFrame API:**
   - Silver loaders (hourly_energy, hourly_weather, hourly_air_quality) sử dụng PySpark DataFrame API
   - Không dùng raw SQL mà dùng: `.select()`, `.withColumn()`, `.groupBy()`, `.agg()`
   
2. **SQL chỉ dùng cho:**
   - MERGE INTO operations (Bronze layer)
   - Incremental detection queries (Silver layer)
   - Metadata lookups

3. **Iceberg Benefits:**
   - `MERGE INTO` hỗ trợ ACID transactions
   - `UPDATE SET *` cập nhật tất cả cột
   - Không cần explicit locking
