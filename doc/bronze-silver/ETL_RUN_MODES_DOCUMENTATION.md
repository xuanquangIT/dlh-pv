# ETL Run Modes & Strategies Documentation

**Tac Gia:** Data Engineering Team  
**Cap Nhat:** 2025-01  
**Phien Ban:** 1.0

---

## Muc Luc

1. [Tong Quan](#1-tong-quan)
2. [Cac Che Do Chay (Modes)](#2-cac-che-do-chay-modes)
   - [Incremental Mode](#21-incremental-mode)
   - [Full Mode](#22-full-mode)
   - [Backfill Mode](#23-backfill-mode)
3. [Cac Chien Luoc Ghi (Load Strategies)](#3-cac-chien-luoc-ghi-load-strategies)
   - [Merge Strategy](#31-merge-strategy)
   - [Overwrite Strategy](#32-overwrite-strategy)
   - [Append Mode](#33-append-mode)
   - [Upsert Mode](#34-upsert-mode-chua-implement)
4. [Ket Hop Mode va Strategy](#4-ket-hop-mode-va-strategy)
5. [Auto-Detection Logic](#5-auto-detection-logic)
6. [Chunked Processing](#6-chunked-processing)
7. [CLI Usage](#7-cli-usage)
8. [Bang So Sanh](#8-bang-so-sanh)
9. [Best Practices](#9-best-practices)

---

## 1. Tong Quan

He thong ETL cua PV Lakehouse su dung 2 tham so chinh de dieu khien cach doc va ghi du lieu:

| Tham So | Giai Thich | Gia Tri |
|---------|------------|---------|
| `mode` | Dieu khien PHAM VI du lieu doc tu nguon (Bronze/Silver) | `full`, `incremental` |
| `load_strategy` | Dieu khien CACH THUC ghi du lieu vao bang dich | `merge`, `overwrite` |

```
+-------------------+       +-------------------+       +-------------------+
|   Bronze Layer    |  -->  |   Silver Layer    |  -->  |    Gold Layer     |
+-------------------+       +-------------------+       +-------------------+
       |                           |                           |
       |  mode: backfill           |  mode: full/incremental   |  mode: full/incremental
       |  (API specific)           |  load_strategy: merge/    |  load_strategy: merge/
       |                           |                overwrite  |                overwrite
```

---

## 2. Cac Che Do Chay (Modes)

### 2.1 Incremental Mode

**Muc dich:** Chi xu ly du lieu MOI tu lan chay truoc. Day la mode mac dinh cho daily/hourly operations.

**Logic hoat dong:**

```
+-------------------------------------------------------------------+
|  INCREMENTAL MODE FLOW                                            |
|                                                                   |
|  1. Query MAX(timestamp) tu bang dich hien tai                    |
|     SELECT MAX(loaded_at_utc) FROM silver.clean_hourly_energy     |
|                                                                   |
|  2. Xac dinh start_time:                                          |
|     - Neu bang rong (NULL) -> lay tu EARLIEST_START               |
|     - Neu co max_ts -> start = max_ts - LOOKBACK_HOURS            |
|                                                                   |
|  3. Filter Bronze data: timestamp >= start_time                   |
|                                                                   |
|  4. Transform va Write voi merge strategy                         |
|     - Chi overwrite partitions bi anh huong                       |
|     - Giu nguyen du lieu cu ngoai range                           |
+-------------------------------------------------------------------+
```

**Code Reference (BaseSilverLoader):**

```python
@dataclass
class LoadOptions:
    mode: str = "incremental"      # "full" | "incremental"
    load_strategy: str = "merge"   # "merge" | "overwrite"
    start: Optional[str] = None    # Override start time
    end: Optional[str] = None      # Override end time
```

**Command:**

```bash
# Silver - Incremental (default)
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode incremental --load-strategy merge

# Gold - Incremental
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental \
  --mode incremental
```

**Uu diem:**
- Nhanh - chi xu ly data moi
- Tiet kiem compute resources
- Idempotent - co the rerun an toan
- Phu hop cho daily operations

**Nhuoc diem:**
- Phu thuoc vao state (last loaded timestamp)
- Khong sua duoc historical data errors

---

### 2.2 Full Mode

**Muc dich:** Rebuild TOAN BO du lieu tu Bronze. Dung cho first-time setup hoac khi can apply new transformation logic.

**Logic hoat dong:**

```
+-------------------------------------------------------------------+
|  FULL MODE FLOW                                                   |
|                                                                   |
|  1. Doc TOAN BO Bronze data (khong filter by timestamp)           |
|     df = spark.table("bronze.raw_facility_weather")               |
|     # KHONG co .filter(timestamp >= ...)                          |
|                                                                   |
|  2. Transform tat ca data                                         |
|                                                                   |
|  3. Write voi load_strategy:                                      |
|     - merge: overwritePartitions() - chi ghi de partitions        |
|     - overwrite: overwritePartitions() - tuong tu merge           |
|                                                                   |
|  Luu y: Ca merge va overwrite deu dung overwritePartitions()      |
|         trong Iceberg de tranh xoa het data ngoai range           |
+-------------------------------------------------------------------+
```

**Command:**

```bash
# Silver - Full rebuild
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode full --load-strategy overwrite

# Gold - Full rebuild
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_facility \
  --mode full
```

**Uu diem:**
- Rebuild tu Bronze (source of truth)
- Fix tat ca data errors
- Apply new transformation logic
- Khong phu thuoc state

**Nhuoc diem:**
- Ton thoi gian (process all data)
- Co the anh huong downstream (Gold, ML)

---

### 2.3 Backfill Mode

**Muc dich:** Reload du lieu cho mot KHOANG THOI GIAN CU THE. Dung de fix data errors cho specific dates.

**Logic hoat dong:**

```
+-------------------------------------------------------------------+
|  BACKFILL MODE FLOW                                               |
|                                                                   |
|  1. User chi dinh date range:                                     |
|     --start 2025-01-01T00:00:00 --end 2025-01-31T23:59:59         |
|                                                                   |
|  2. Filter Bronze data trong range                                |
|     df.filter(timestamp >= start).filter(timestamp <= end)        |
|                                                                   |
|  3. Transform va Write                                            |
|     - Chi xoa/ghi de partitions trong range                       |
|     - Giu nguyen data ngoai range                                 |
|                                                                   |
|  QUAN TRONG: Backfill = --mode full + --start/--end               |
+-------------------------------------------------------------------+
```

**Command:**

```bash
# Bronze - Backfill specific dates (goi lai API)
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_weather.py \
  --mode backfill --start 2025-01-01 --end 2025-01-31

# Silver - Backfill specific dates
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode full \
  --start 2025-01-01T00:00:00 --end 2025-01-31T23:59:59 \
  --load-strategy overwrite

# Gold - Backfill specific dates
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental \
  --mode full \
  --start 2025-01-01T00:00:00 --end 2025-01-31T23:59:59
```

**Uu diem:**
- Co the rebuild historical data
- Fix data errors cho specific period
- Khong anh huong data ngoai range

**Nhuoc diem:**
- Ton API calls (Bronze layer)
- Can biet chinh xac date range can fix

---

## 3. Cac Chien Luoc Ghi (Load Strategies)

### 3.1 Merge Strategy

**Muc dich:** Ghi du lieu moi ma KHONG xoa du lieu cu ngoai pham vi.

**Iceberg Implementation:**

```python
# spark_utils.py - write_iceberg_table()
if mode == "overwrite":
    # Dung overwritePartitions() - CHI ghi de partitions co trong DataFrame
    writer.overwritePartitions()
```

**Dac diem:**
- Su dung Iceberg `overwritePartitions()`
- Chi ghi de partitions co data moi
- Giu nguyen partitions khong co trong DataFrame
- An toan cho incremental loads

**Vi du:**

```
Truoc khi chay (Silver table):
+------------+----------+-------+
| date_hour  | facility | value |
+------------+----------+-------+
| 2025-01-01 | F001     | 100   |
| 2025-01-02 | F001     | 200   |
| 2025-01-03 | F001     | 300   |
+------------+----------+-------+

DataFrame moi (chi co 2025-01-02):
+------------+----------+-------+
| date_hour  | facility | value |
+------------+----------+-------+
| 2025-01-02 | F001     | 250   |  <-- Updated value
+------------+----------+-------+

Sau khi MERGE:
+------------+----------+-------+
| date_hour  | facility | value |
+------------+----------+-------+
| 2025-01-01 | F001     | 100   |  <-- Giu nguyen
| 2025-01-02 | F001     | 250   |  <-- Updated
| 2025-01-03 | F001     | 300   |  <-- Giu nguyen
+------------+----------+-------+
```

---

### 3.2 Overwrite Strategy

**Muc dich:** Ghi de TOAN BO du lieu trong pham vi partition.

**Iceberg Implementation:**

```python
# spark_utils.py - write_iceberg_table()
if mode == "overwrite":
    writer.overwritePartitions()  # Giong voi merge trong implementation hien tai
```

**Luu y quan trong:**
> Trong implementation hien tai, `merge` va `overwrite` DEU su dung `overwritePartitions()`.
> Dieu nay co nghia la behavior giong nhau - chi ghi de partitions co data trong DataFrame.

---

### 3.3 Append Mode

**Muc dich:** Them du lieu moi ma KHONG kiem tra duplicates.

**Iceberg Implementation:**

```python
# spark_utils.py - write_iceberg_table()
if mode == "append":
    writer.append()
```

**Canh bao:**
- Khong kiem tra duplicates
- Co the tao ra duplicate records
- Chi dung khi chac chan data khong trung lap

---

### 3.4 Upsert Mode (Chua Implement)

**Muc dich:** Update neu ton tai, Insert neu chua co. Day la true MERGE operation.

**Trang thai hien tai:**

```python
# spark_utils.py - write_iceberg_table()
elif mode == "upsert":
    raise NotImplementedError("Upsert mode is not implemented. Use 'append' or 'overwrite'.")
```

**Ke hoach tuong lai:**
- Implement bang Iceberg MERGE INTO
- Can dinh nghia merge keys (vd: facility_code + date_hour)
- Ho tro update existing + insert new trong 1 operation

---

## 4. Ket Hop Mode va Strategy

### Bang Ket Hop Khuyen Nghi

| Use Case | Mode | Strategy | Ghi Chu |
|----------|------|----------|---------|
| Daily incremental load | `incremental` | `merge` | Mac dinh, an toan nhat |
| First time setup | `full` | `overwrite` | Tao toan bo tu dau |
| Fix specific dates | `full` + `--start/--end` | `overwrite` | Backfill pattern |
| After code change | `full` | `overwrite` | Apply new logic |
| Schema change | `full` | `overwrite` | Rebuild voi schema moi |
| Re-process all | `full` | `overwrite` | Complete rebuild |

### Behavior Matrix

```
+-------------+------------------+----------------------------------------+
|    Mode     |    Strategy      |              Behavior                  |
+-------------+------------------+----------------------------------------+
| incremental | merge            | Doc data moi, ghi de partitions moi    |
| incremental | overwrite        | Doc data moi, ghi de partitions moi    |
| full        | merge            | Doc all data, ghi de all partitions    |
| full        | overwrite        | Doc all data, ghi de all partitions    |
| full+range  | merge/overwrite  | Doc data trong range, ghi de range     |
+-------------+------------------+----------------------------------------+

Luu y: merge va overwrite hien tai co behavior giong nhau do dung overwritePartitions()
```

---

## 5. Auto-Detection Logic

### Silver Layer Auto-Detection

Khi chay `--mode incremental` ma KHONG chi dinh `--start`:

```python
# base.py - BaseSilverLoader._read_bronze()

def _auto_detect_start_time(self) -> Optional[datetime]:
    """Query max timestamp tu Silver table de xac dinh start."""
    
    # 1. Doc max loaded_at_utc tu Silver
    max_ts_query = f"SELECT MAX(loaded_at_utc) as max_ts FROM {self.target_table}"
    max_ts = spark.sql(max_ts_query).first()["max_ts"]
    
    # 2. Neu NULL (table rong) -> return None (se dung EARLIEST_START)
    if max_ts is None:
        return None
    
    # 3. Neu co max_ts -> lookback LOOKBACK_HOURS gio
    return max_ts - timedelta(hours=self.LOOKBACK_HOURS)
```

### Gold Layer Auto-Detection

```python
# base.py - BaseGoldLoader._auto_detect_start_time()

def _auto_detect_start_time(self) -> Optional[datetime]:
    """Auto-detect start time cho incremental mode."""
    
    # 1. Thu doc tu Gold table (date_key)
    max_date_query = f"SELECT MAX(date_key) as max_date FROM {target_table}"
    max_date = spark.sql(max_date_query).first()["max_date"]
    
    if max_date:
        # Convert date_key (YYYYMMDD) -> datetime
        return datetime.strptime(str(max_date), "%Y%m%d") - timedelta(days=lookback_days)
    
    # 2. Neu Gold rong, thu doc tu Silver sources
    for source in self.source_tables:
        max_ts_query = f"SELECT MAX({source.timestamp_col}) FROM {source.table_name}"
        # ...
    
    # 3. Neu van NULL -> return None (first run)
    return None
```

### Lookback Configuration

```python
# Trong BaseSilverLoader
LOOKBACK_HOURS = 48  # Mac dinh lookback 48 gio

# Trong BaseGoldLoader  
LOOKBACK_DAYS = 3    # Mac dinh lookback 3 ngay
```

**Tai sao can Lookback?**

1. **Late-arriving data:** Data co the den tre hon expected
2. **Timezone effects:** Doi mui gio co the lam shift data
3. **Data corrections:** API co the sua data gan day
4. **Safe overlap:** Dam bao khong miss bat ky data nao

---

## 6. Chunked Processing

### Khi nao can Chunked Processing?

- Dataset lon (nhieu thang/nam data)
- Memory constraints
- Tranh timeout khi process lau

### Implementation

```python
# base.py - BaseSilverLoader._process_in_chunks()

def _process_in_chunks(self, chunk_days: int = 30):
    """Xu ly data theo tung chunk thoi gian."""
    
    current_start = self.options.start
    final_end = self.options.end
    
    while current_start < final_end:
        chunk_end = min(current_start + timedelta(days=chunk_days), final_end)
        
        # Process 1 chunk
        chunk_options = LoadOptions(
            mode="full",
            load_strategy="merge",
            start=current_start.isoformat(),
            end=chunk_end.isoformat()
        )
        
        self._process_single_chunk(chunk_options)
        
        current_start = chunk_end
```

### Usage

```bash
# Process 1 nam data theo chunks 30 ngay
docker compose exec spark-master spark-submit \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy \
  --mode full \
  --start 2024-01-01T00:00:00 --end 2024-12-31T23:59:59 \
  --chunk-days 30
```

---

## 7. CLI Usage

### Silver Layer CLI

```bash
python cli.py <loader_name> [options]

Arguments:
  loader_name          Ten loader: facility_master, hourly_energy, 
                       hourly_weather, hourly_air_quality

Options:
  --mode               "full" | "incremental" (default: incremental)
  --load-strategy      "merge" | "overwrite" (default: merge)
  --start              Start datetime (ISO format)
  --end                End datetime (ISO format)
  --chunk-days         So ngay moi chunk (optional)
  --app-name           Ten Spark application (optional)
```

**Examples:**

```bash
# Incremental (default)
python cli.py hourly_energy

# Full rebuild
python cli.py hourly_energy --mode full --load-strategy overwrite

# Backfill specific range
python cli.py hourly_energy --mode full \
  --start 2025-01-01T00:00:00 --end 2025-01-31T23:59:59

# Chunked processing
python cli.py hourly_energy --mode full \
  --start 2024-01-01T00:00:00 --end 2024-12-31T23:59:59 \
  --chunk-days 30
```

### Gold Layer CLI

```bash
python cli.py <loader_name> [options]

Arguments:
  loader_name          Ten loader: dim_facility, dim_date, dim_time,
                       dim_aqi_category, fact_solar_environmental,
                       fact_solar_forecast_regression

Options:
  --mode               "full" | "incremental" (default: incremental)
  --load-strategy      "merge" | "overwrite" (default: merge)
  --start              Start datetime (ISO format)
  --end                End datetime (ISO format)
```

**Examples:**

```bash
# Dimension tables (thuong dung full mode)
python cli.py dim_facility --mode full
python cli.py dim_date --mode full

# Fact table incremental
python cli.py fact_solar_environmental --mode incremental

# Fact table backfill
python cli.py fact_solar_environmental --mode full \
  --start 2025-01-01T00:00:00 --end 2025-03-31T23:59:59
```

---

## 8. Bang So Sanh

### So Sanh Cac Mode

| Tinh Nang | Incremental | Full | Backfill |
|-----------|-------------|------|----------|
| Doc du lieu | Chi data moi | Tat ca data | Data trong range |
| Toc do | Nhanh | Cham | Trung binh |
| Resources | It | Nhieu | Trung binh |
| State dependency | Co | Khong | Khong |
| Fix historical | Khong | Co | Co |
| Daily ops | Recommended | Khong | Khi can thiet |

### So Sanh Cac Strategy

| Tinh Nang | Merge | Overwrite | Append | Upsert |
|-----------|-------|-----------|--------|--------|
| Giu data cu | Co | Co* | Co | Co |
| Kiem tra dup | Khong | Khong | Khong | Co |
| Iceberg operation | overwritePartitions | overwritePartitions | append | MERGE INTO |
| Implemented | Co | Co | Co | Chua |

*Trong implementation hien tai, overwrite cung dung overwritePartitions() nen giu data ngoai range.

---

## 9. Best Practices

### Do's

1. **Daily operations:** Dung `--mode incremental --load-strategy merge`
2. **First setup:** Dung `--mode full --load-strategy overwrite`
3. **Fix data:** Dung backfill voi specific date range
4. **Large datasets:** Dung chunked processing
5. **After code change:** Full rebuild de apply new logic

### Don'ts

1. **Khong dung full mode cho daily ops:** Ton resources
2. **Khong dung append cho time-series:** Co the duplicate
3. **Khong skip Bronze backfill:** Neu can fix tu API
4. **Khong ignore lookback:** Co the miss late data

### Recommended Workflow

```
+-------------------------------------------------------------------+
|  RECOMMENDED DAILY WORKFLOW                                       |
|                                                                   |
|  1. Bronze: Khong can chay manual (co scheduler)                  |
|                                                                   |
|  2. Silver: Incremental mode                                      |
|     python cli.py hourly_energy --mode incremental                |
|     python cli.py hourly_weather --mode incremental               |
|     python cli.py hourly_air_quality --mode incremental           |
|                                                                   |
|  3. Gold: Incremental mode                                        |
|     python cli.py fact_solar_environmental --mode incremental     |
|                                                                   |
|  4. Verify: Kiem tra data quality                                 |
|     SELECT COUNT(*), MAX(date_hour) FROM silver.clean_hourly_*    |
+-------------------------------------------------------------------+
```

---

## Appendix: Code References

### LoadOptions Dataclass (Silver)

```python
# src/pv_lakehouse/etl/silver/base.py

@dataclass
class LoadOptions:
    """Configuration options for Silver layer loading."""
    mode: str = "incremental"       # "full" | "incremental"
    load_strategy: str = "merge"    # "merge" | "overwrite"
    start: Optional[str] = None     # ISO datetime string
    end: Optional[str] = None       # ISO datetime string
    app_name: Optional[str] = None  # Spark application name
```

### GoldLoadOptions Dataclass (Gold)

```python
# src/pv_lakehouse/etl/gold/base.py

@dataclass
class GoldLoadOptions:
    """Configuration options for Gold layer loading."""
    mode: str = "incremental"       # "full" | "incremental"
    load_strategy: str = "merge"    # "merge" | "overwrite"
    start: Optional[str] = None     # ISO datetime string
    end: Optional[str] = None       # ISO datetime string
    app_name: Optional[str] = None  # Spark application name
```

### write_iceberg_table Function

```python
# src/pv_lakehouse/etl/utils/spark_utils.py

def write_iceberg_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",  # "append" | "overwrite" | "upsert"
    partition_by: Optional[List[str]] = None
) -> None:
    """Write DataFrame to Iceberg table."""
    
    writer = df.writeTo(table_name)
    
    if partition_by:
        writer = writer.partitionedBy(*partition_by)
    
    if mode == "overwrite":
        writer.overwritePartitions()
    elif mode == "append":
        writer.append()
    elif mode == "upsert":
        raise NotImplementedError("Upsert mode is not implemented.")
```

---

*Document generated from source code analysis of PV Lakehouse ETL framework.*
