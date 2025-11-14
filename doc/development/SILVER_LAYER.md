# Silver Layer - Làm Sạch & Xác Thực Chất Lượng

## Mục Đích
- Làm sạch & chuẩn hóa dữ liệu bronze
- Xác thực chất lượng với business rules
- Tạo quality flags (GOOD/CAUTION/REJECT)
- Sẵn sàng cho gold layer & ML training

---

## Kiến Trúc Silver

### Base Class: `BaseSilverLoader` (base.py)

**Trách Nhiệm Chung**:
- Load bronze data theo time range
- Chunking: Xử lý 3-7 ngày/lần (tránh memory leak)
- Write incremental/merge to Iceberg
- Logging & error handling

**Thông Số**:
```python
LoadOptions(
    mode="incremental",              # full | incremental
    target_file_size_mb=128,         # File size target
    max_records_per_file=250_000,    # Record limit/file
    partition_cols=("date_hour",),   # Partition key
)
```

---

## Các Bảng Silver

### 1. `clean_facility_master` (Metadata Làm Sạch)
**File**: `facility_master.py`

**Mục Đích**: Tham chiếu chính cho cơ sở, cập nhật slowly changing dimensions

**Từ Bronze**: `raw_facilities`

**Transform**:
```
1. Select: Các cột bắt buộc + optional metadata
2. Deduplicate: Lấy record mới nhất (SCD Type 1)
3. Validate: Check non-null facility_code, capacity > 0
4. Quality Flags:
   - GOOD: Đủ thông tin, capacity hợp lý
   - REJECT: Missing facility_code, capacity ≤ 0
5. Add audit: effective_from, effective_to, is_current
```

**Cột Output**:
```
facility_code                → Mã (PK)
facility_name                → Tên cơ sở
network_id                   → NEM/WEM
network_region               → Bang/vùng
location_lat / location_lng  → Tọa độ
total_capacity_mw            → Công suất (MW)
unit_count                   → Số unit
quality_flag                 → GOOD | REJECT
created_at / updated_at      → Audit timestamp
```

**Partition**: `facility_code` (2 partition = 2 cơ sở)

---

### 2. `clean_hourly_energy` (Năng Lượng Làm Sạch)
**File**: `hourly_energy.py`

**Từ Bronze**: `raw_facility_timeseries` (metric = "energy")

#### Step 1: Aggregation
```
Group by: facility_code, facility_name, date_hour
Aggregate:
  - SUM(energy) → energy_mwh
  - COUNT(*) → intervals_count (bao nhiêu records/giờ)
```

#### Step 2: Quality Rules (7 loại kiểm tra)

| Loại Kiểm Tra | Điều Kiện | Flag | Giải Thích |
|---|---|---|---|
| **OUT_OF_BOUNDS** | energy < 0 | REJECT | Không thể âm (vật lý) |
| **STATISTICAL_OUTLIER** | energy > capacity_threshold | CAUTION | Vượt công suất định mức |
| **NIGHT_ENERGY_ANOMALY** | 22:00-05:59 AND energy > 1 MWh | CAUTION | Phát điện vào ban đêm (bất thường) |
| **DAYTIME_ZERO_ENERGY** | 08:00-17:59 AND energy == 0 | CAUTION | Zero vào ban ngày (lỗi) |
| **EQUIPMENT_DOWNTIME** | 10:00-14:59 AND energy == 0 | CAUTION | Downtime vào peak hours |
| **TRANSITION_HOUR_LOW_ENERGY** | Sunrise/morning/sunset AND energy < threshold% | CAUTION | Ramp-up/down quá thấp |
| **PEAK_HOUR_LOW_ENERGY** | 10:00-14:59 AND 0.5 < energy < 50% peak | CAUTION | Hiệu suất thấp peak |

#### Step 3: Facility-Specific Thresholds

```python
# Công suất tối đa (P99.5 từ 679 ngày)
GANNSF:  capacity = 52 MWh    (50 MW × 1.04 margin)
NYNGAN:  capacity = 105 MWh   (102 MW × 1.03 margin)

# Peak threshold (P95)
GANNSF:  peak = 40 MWh        (Conservative)
NYNGAN:  peak = 95 MWh        (95th percentile)

# Transition hour thresholds (% of peak)
Sunrise  (06:00-08:00): 5% of peak
Morning  (08:00-10:00): 8% of peak
Sunset   (17:00-19:00): 10% of peak
```

#### Step 4: Quality Flag Logic
```
IF energy < 0:
    flag = "REJECT"
ELSE IF bất kỳ CAUTION rule nào:
    flag = "CAUTION"
ELSE:
    flag = "GOOD"
```

**Cột Output**:
```
facility_code      → Mã cơ sở
facility_name      → Tên
date_hour          → Timestamp (UTC hour)
energy_mwh         → Năng lượng (MWh)
intervals_count    → Số 5min records/giờ
quality_flag       → GOOD | CAUTION | REJECT
quality_issues     → Pipe-delimited: "NIGHT_ANOMALY|PEAK_LOW"
completeness_pct   → Always 100.0 (hourly agg)
created_at / updated_at
```

**Partition**: `date_hour` (theo ngày)

**Chunk Size**: 7 ngày (7 × 24 × 2 facilities ≈ 336 rows/chunk)

---

### 3. `clean_hourly_weather` (Thời Tiết Làm Sạch)
**File**: `hourly_weather.py`

**Từ Bronze**: `raw_facility_weather`

#### Step 1: Type Casting & Rounding
```
shortwave_radiation        → float, round to 1 decimal
direct_radiation           → float, round to 1 decimal
temperature_2m             → float, round to 2 decimals
... (15 cột số)
```

#### Step 2: Range Validation (Bounds Check)

| Cột | Min | Max | Giải Thích |
|---|---|---|---|
| shortwave_radiation | 0 | 1150 | W/m² (P99.5 = 1045) |
| direct_radiation | 0 | 1050 | W/m² |
| temperature_2m | -10 | 50 | °C (P99 = 38.5) |
| wind_speed_10m | 0 | 50 | m/s (P99 = 47.2, cyclones) |
| pressure_msl | 985 | 1050 | hPa (P99 = 1033) |
| cloud_cover | 0 | 100 | % |
| **night radiation** | 0 | 100 | W/m² (22:00-06:00) → **REJECT** |

#### Step 3: Quality Flags
```
REJECT:
  - Negative radiation/temperature
  - Night radiation > 100 W/m² (22:00-06:00)
  - Out of bounds

CAUTION:
  - Minor outliers (P99.5 threshold)
  - Extreme but plausible values

GOOD:
  - In-range, realistic
```

**Cột Output**:
```
facility_code              → Mã cơ sở
facility_name              → Tên
date_hour                  → Timestamp
shortwave_radiation        → W/m²
direct_radiation           → W/m²
diffuse_radiation          → W/m²
temperature_2m             → °C
dew_point_2m               → °C
wind_speed_10m             → m/s
wind_direction_10m         → °
wind_gusts_10m             → m/s
cloud_cover                → %
precipitation              → mm
... (15 cột tổng cộng)
quality_flag               → GOOD | CAUTION | REJECT
quality_issues             → Pipe-delimited issues
completeness_pct           → % non-null fields
created_at / updated_at
```

**Partition**: `date_hour`

**Chunk Size**: 7 ngày

---

### 4. `clean_hourly_air_quality` (Chất Lượng Không Khí Làm Sạch)
**File**: `hourly_air_quality.py`

**Từ Bronze**: `raw_facility_air_quality`

#### Step 1: Type Casting & Rounding
```
pm2_5              → float, round to 1 decimal (µg/m³)
pm10               → float, round to 1 decimal
nitrogen_dioxide   → float, round to 2 decimals (ppb)
ozone              → float, round to 2 decimals (ppb)
uv_index           → float, round to 1 decimal
... (9 cột chính)
```

#### Step 2: Range Validation

| Cột | Min | Max | Giải Thích |
|---|---|---|---|
| pm2_5 | 0 | 500 | µg/m³ |
| pm10 | 0 | 500 | µg/m³ |
| nitrogen_dioxide | 0 | 500 | ppb |
| ozone | 0 | 500 | ppb |
| uv_index | 0 | 15 | Chỉ số UV |

#### Step 3: Quality Flags
```
REJECT:
  - Negative values
  - Out of bounds (extreme)

CAUTION:
  - Minor outliers

GOOD:
  - In-range
  - Realistic values
```

**Cột Output**:
```
facility_code          → Mã cơ sở
facility_name          → Tên
date_hour              → Timestamp
pm2_5                  → µg/m³
pm10                   → µg/m³
dust                   → µg/m³
nitrogen_dioxide       → ppb
ozone                  → ppb
sulphur_dioxide        → ppb
carbon_monoxide        → ppm
uv_index               → Chỉ số
quality_flag           → GOOD | CAUTION | REJECT
quality_issues         → Pipe-delimited issues
completeness_pct       → % non-null fields
created_at / updated_at
```

**Partition**: `date_hour`

**Chunk Size**: 7 ngày

---

## Quality Metrics (Đánh Giá Chất Lượng)

### Số Liệu Hiện Tại (679 ngày, 2 cơ sở)

| Bảng | Records | GOOD | CAUTION | REJECT | Quality |
|---|---|---|---|---|---|
| **clean_hourly_energy** | 32,542 | 85.46% | 14.54% | 0% | ⚠️ CAUTION |
| **clean_hourly_weather** | 32,544 | 96.11% | 3.89% | 0% | ✅ GOOD |
| **clean_hourly_air_quality** | 32,544 | 100% | 0% | 0% | ✅ EXCELLENT |

### Readiness Score (Sẵn Sàng Cho Gold/ML)

```
Completeness:          23.7/25 (94.7%)  ✅
Data Quality:          23.5/25 (93.9%)  ✅
Referential Integrity: 25.0/25 (100%)   ✅
Time Coverage:         25.0/25 (100%)   ✅
────────────────────────────────────────────
FINAL SCORE:           97.1/100         ✅ READY FOR PRODUCTION
```

**Verdict**: Dữ liệu silver đã sạch sẽ, sẵn sàng cho gold layer & ML training.

---

## CLI Commands

```bash
# Load tất cả silver tables (incremental)
python -m pv_lakehouse.etl.silver load_all

# Load riêng facility master
python -m pv_lakehouse.etl.silver load_facility_master

# Load hourly energy
python -m pv_lakehouse.etl.silver load_hourly_energy --mode incremental

# Load thời tiết
python -m pv_lakehouse.etl.silver load_hourly_weather

# Load chất lượng không khí
python -m pv_lakehouse.etl.silver load_hourly_air_quality

# Full backfill (tái tải toàn bộ)
python -m pv_lakehouse.etl.silver load_all --mode full --date-start 2024-01-01

# Load từ ngày cụ thể
python -m pv_lakehouse.etl.silver load_hourly_energy --start 2025-01-01 --end 2025-11-14
```

---

## Troubleshooting

### Silver Quality Thấp

#### Energy GOOD < 85%
```
Problem: Năng lượng quality score quá thấp
Root Cause: 
  • Threshold assumptions không đúng (peak, capacity)
  • Thiếu training data lịch sử
  • Seasonal patterns không capture được

Solution:
  • Review hourly_energy.py thresholds
  • Kiểm tra facility_expected_peak_threshold (cài quá thấp?)
  • Kiểm tra transition hour thresholds (sunrise/sunset)
```

#### Weather GOOD < 90%
```
Problem: Thời tiết quality score thấp
Common Causes:
  • Radiation bounds quá chặt
  • Night radiation check (> 100 W/m²)
  • Timezone handling issue

Solution:
  • Kiểm tra night radiation filter (line 163)
  • Kiểm tra radiation bounds trong hourly_weather.py
  • Verify timezone conversion (local vs UTC)
```

### Performance Issues

#### Silver Load Quá Chậm
```
Problem: Load time quá lâu (>30 min)
Solution:
  • Giảm target_file_size_mb: 128 → 64
  • Giảm max_records_per_file: 250K → 100K
  • Tăng chunk_days (nhưng risk OOM): 7 → 14 (test trước)

Command:
python -m pv_lakehouse.etl.silver load_hourly_energy \
  --target-file-size-mb 64 \
  --max-records-per-file 100000
```

#### Out of Memory During Load
```
Problem: OOM error khi load
Solution:
  • Giảm chunk_days (default 7 → 3)
  • Kiểm tra bronze table size (SELECT COUNT(*))
  • Thử mode="full" thay vì incremental (simpler)
  • Chạy trên machine với RAM lớn hơn
```

#### Iceberg Merge Conflicts
```
Problem: Write conflicts khi merge
Solution:
  • Switch load_strategy: "merge" → "overwrite"
  • Rebuild partitions từ đầu (mode="full")
  • Check Iceberg version compatibility
```

### Data Quality Issues

#### Unexpected REJECT Records
```
Problem: Quá nhiều REJECT flags
Debug:
  • Check quality_issues column để xem lý do
  • Ví dụ: "OUT_OF_BOUNDS|STATISTICAL_OUTLIER"
  • Phân tích MAX/MIN của energy_mwh per facility

Solution:
  • Adjust capacity_threshold nếu cần
  • Verify data đúng từ bronze
```

#### Missing Values (NULL/NaN)
```
Problem: Quá nhiều NULL trong quality_issues
Reason: Khi không có issues → NULL không được tạo

Solution:
  • Treat NULL = no issues found (GOOD record)
  • Kiểm tra completeness_pct (bao nhiêu non-null)
  • Filter notna() trước khi split quality_issues
```

---

## Tệp Tham Khảo

- **Entry Point**: `/src/pv_lakehouse/etl/silver/cli.py`
- **Base Class**: `/src/pv_lakehouse/etl/silver/base.py`
- **Loaders**:
  - `facility_master.py` → clean_facility_master
  - `hourly_energy.py` → clean_hourly_energy
  - `hourly_weather.py` → clean_hourly_weather
  - `hourly_air_quality.py` → clean_hourly_air_quality
- **Test Report**: `/notebooks/silver_readiness_for_gold.ipynb`
- **Exported Data**: `/src/pv_lakehouse/exported_data/*.csv` (679 ngày sample)

---

## Tiếp Theo (Roadmap)

- ✅ Bronze ingestion (DONE)
- ✅ Silver cleaning (DONE - 97.1/100 score)
- ⏳ Gold layer (PENDING) - Fact/Dimension tables filtered GOOD only
- ⏳ ML training pipeline (PENDING) - Stratified sampling, normalization
- ⏳ Power BI integration (PENDING)
- ⏳ Data monitoring & alerting (PENDING)
