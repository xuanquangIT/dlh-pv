# Bronze Layer - Thu Thập Dữ Liệu Thô

## Mục Đích
- Thu thập dữ liệu thô từ các API bên ngoài
- Lưu nguyên trạng, không xử lý logic
- Tạo audit trail cho dữ liệu lịch sử

---

## Các Bảng Bronze

### 1. `raw_facilities` (Metadata Cơ Sở)
**File**: `load_facilities.py`

**Nguồn**: OpenElectricity API
- Thông tin tĩnh: tên cơ sở, vị trí, công suất
- Cập nhật: Tuần tính

**Cột chính**:
```
facility_code      → GANNSF, NYNGAN (mã định danh)
facility_name      → Tên cơ sở (ví dụ: "Gannawarra")
network_id         → NEM (quốc gia)
network_region     → VIC (bang/vùng)
location_lat       → Vĩ độ
location_lng       → Kinh độ
total_capacity_mw  → Công suất định mức (MW)
unit_count         → Số unit quang điện
unit_status_summary → Trạng thái (operating)
```

**Mode Load**:
- `backfill`: Tải toàn bộ dữ liệu từ đầu
- `incremental`: Cập nhật metadata mới

---

### 2. `raw_facility_timeseries` (Chuỗi Thời Gian Năng Lượng)
**File**: `load_facility_timeseries.py`

**Nguồn**: OpenElectricity API
- Dữ liệu công suất/năng lượng theo giờ
- Cập nhật: Mỗi giờ (incremental)

**Cột chính**:
```
facility_code    → Mã cơ sở
metric           → "energy" hoặc "power"
value            → Giá trị (MWh hoặc MW)
interval_ts      → Timestamp (UTC)
network_code     → NEM/WEM
network_region   → VIC/NSW/QLD/...
```

**Xử Lý**:
- Mode `incremental`: Tự động tìm dữ liệu mới từ `MAX(interval_ts)`
- Chunk 7 ngày để tránh memory overflow
- Xác thực cơ bản: non-null facility_code, interval_ts

---

### 3. `raw_facility_weather` (Dữ Liệu Thời Tiết)
**File**: `load_facility_weather.py`

**Nguồn**: Open-Meteo API (miễn phí)
- Bức xạ, nhiệt độ, độ ẩm, tốc độ gió
- Cập nhật: Mỗi ngày (incremental)
- Phạm vi: 50 năm lịch sử + dự báo

**Cột chính**:
```
facility_code              → Mã cơ sở
shortwave_radiation        → Bức xạ sóng ngắn (W/m²)
direct_radiation           → Bức xạ trực tiếp
diffuse_radiation          → Bức xạ khuếch tán
temperature_2m             → Nhiệt độ (°C)
dew_point_2m               → Điểm sương
wind_speed_10m             → Tốc độ gió (m/s)
wind_direction_10m         → Hướng gió (°)
cloud_cover                → Độ che phủ mây (%)
precipitation              → Lượng mưa (mm)
pressure_msl               → Áp suất mực nước biển (hPa)
weather_timestamp          → Timestamp (múi giờ địa phương)
```

**Đặc Điểm**:
- Concurrent fetch: 4 threads (tránh rate limit)
- Rate limit: 30 request/phút
- Auto chunk: 30 ngày/request (archive API limit)
- Timezone cụ thể: Dữ liệu đã trong múi giờ địa phương

---

### 4. `raw_facility_air_quality` (Chất Lượng Không Khí)
**File**: `load_facility_air_quality.py`

**Nguồn**: Open-Meteo API
- PM2.5, PM10, NO₂, O₃, SO₂, CO, UV index
- Cập nhật: Mỗi ngày (incremental)
- Giới hạn: 14 ngày/API call (Open-Meteo limit)

**Cột chính**:
```
facility_code              → Mã cơ sở
pm2_5                      → Hạt siêu nhỏ (µg/m³)
pm10                       → Hạt nhỏ
nitrogen_dioxide           → NO₂ (ppb)
ozone                      → O₃ (ppb)
sulphur_dioxide            → SO₂ (ppb)
carbon_monoxide            → CO (ppm)
uv_index                   → Chỉ số UV
air_timestamp              → Timestamp (múi giờ địa phương)
```

**Đặc Điểm**:
- Dữ liệu đã trong múi giờ địa phương
- Không cần transform timezone
- 4 threads concurrent fetch

---

## Quy Trình Bronze Load

```
1. Phân tích args (facility codes, date range, mode)
2. Fetch từ API ngoài (concurrent threads)
3. Xác thực non-null, type casting cơ bản
4. Write to Iceberg table:
   - Mode "backfill": Overwrite toàn bộ partition
   - Mode "incremental": Merge với dữ liệu cũ
5. Format: Parquet + Iceberg v2 + partitioning
```

---

## CLI Commands

```bash
# Load metadata cơ sở
python -m pv_lakehouse.etl.bronze load_facilities

# Load năng lượng (incremental mặc định)
python -m pv_lakehouse.etl.bronze load_timeseries --facility-codes GANNSF,NYNGAN

# Load thời tiết (7 ngày gần đây)
python -m pv_lakehouse.etl.bronze load_weather --mode incremental

# Load chất lượng không khí
python -m pv_lakehouse.etl.bronze load_air_quality --start 2025-01-01

# Full backfill năng lượng từ đầu
python -m pv_lakehouse.etl.bronze load_timeseries --mode backfill --date-start 2020-01-01
```

---

## Troubleshooting

### Rate Limit Exceeded
```
Problem: Open-Meteo 30 request/phút bị exceed
Solution: Giảm --max-workers từ 4 → 2
Command: python -m pv_lakehouse.etl.bronze load_weather --max-workers 2
```

### Out of Memory
```
Problem: Chunk quá lớn → memory leak
Solution: 
  • File load_*.py sử dụng chunk_days=7 (mặc định)
  • Nếu vẫn OOM, giảm chunk_days → 3
```

### Incremental Mode Không Tải Dữ Liệu Mới
```
Problem: Không có dữ liệu mới sau load
Solution:
  • Check MAX(timestamp) trong bronze table:
    SELECT MAX(interval_ts) FROM lh.bronze.raw_facility_timeseries
  • Kiểm tra API có dữ liệu mới chưa
  • Thử mode="backfill" để reset
```

### API Connection Timeout
```
Problem: Không kết nối được API (network issue)
Solution:
  • Retry với --max-retries (built-in exponential backoff)
  • Kiểm tra internet connection
  • Kiểm tra API key (--api-key ARG)
```

---

## Tệp Tham Khảo

- **Entry Point**: `/src/pv_lakehouse/etl/bronze/cli.py`
- **Loaders**: 
  - `load_facilities.py` → raw_facilities
  - `load_facility_timeseries.py` → raw_facility_timeseries
  - `load_facility_weather.py` → raw_facility_weather
  - `load_facility_air_quality.py` → raw_facility_air_quality
- **Client**: 
  - `clients/openelectricity.py` → API integration
  - `clients/openmeteo.py` → Open-Meteo integration
- **Utils**: `bronze/openmeteo_common.py`, `bronze/facility_timezones.py`
