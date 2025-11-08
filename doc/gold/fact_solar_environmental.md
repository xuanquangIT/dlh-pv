# README — `fact_solar_environmental` (Gold Layer)

> **Mục tiêu**: mô tả chi tiết từng field của bảng fact, nêu **nguồn lấy dữ liệu** (từ Silver), **nơi tính toán** (ETL PySpark trong Gold hay DAX trong Power BI) và **công thức thô** để tái lập. Các KPI theo ngữ cảnh lọc (facility/date/time/AQI) được để **DAX**; các chuẩn hoá/row-level tính tại **ETL**.

**Grain**: 1 hàng = 1 giờ tại 1 nhà máy (facility, date_key, time_key).  
**Nguồn tổng**: hợp nhất 3 bảng Silver
- `lh_silver_clean_hourly_energy.csv` → năng lượng/công suất theo giờ
- `lh_silver_clean_hourly_weather.csv` → bức xạ & thời tiết theo giờ
- `lh_silver_clean_hourly_air_quality.csv` → AQI & thành phần ô nhiễm theo giờ

---

## 1) Khóa & metadata
| Column | Type | Nguồn | Tính ở đâu | Ghi chú |
|---|---|---|---|---|
| `facility_key` | BIGINT (FK) | `dim_facility` | ETL | Surrogate key của nhà máy |
| `date_key` | INT (FK) | `dim_date` | ETL | Định dạng `YYYYMMDD` |
| `time_key` | INT (FK) | `dim_time` | ETL | 0–23 |
| `aqi_category_key` | BIGINT (FK, nullable) | `dim_aqi_category` | **ETL** | Map từ `aqi_value` theo bảng chuẩn (EPA/WHO) |
| `created_at`, `updated_at` | TIMESTAMP | hệ thống | ETL | Dấu vết pipeline |
| `is_valid` | BOOLEAN | tổng hợp | **ETL** | `true` nếu toàn bộ field quan trọng không null & qua rule chất lượng |
| `quality_flag` | VARCHAR(16) | tổng hợp | **ETL** | Mã hoá lỗi (e.g., `MISSING_WEATHER`, `SUSPECT_ENERGY`) |
| `completeness_pct` | DECIMAL(5,2) | tổng hợp | **ETL** | % số sub-interval hợp lệ trong giờ |
| `intervals_count` | INT | energy | **ETL** | Số điểm đo hợp lệ trong giờ (ví dụ 12×5 phút) |

---

## 2) Năng lượng (từ Silver *hourly_energy*)
| Column | Type | Nguồn | Tính ở đâu | Công thức |
|---|---|---|---|---|
| `energy_mwh` | DECIMAL(12,6) | Silver.energy_mwh | ETL (copy/aggregate) | SUM năng lượng trong giờ nếu Silver là sub-interval |
| `power_avg_mw` | DECIMAL(12,6) | Silver.power_avg_mw | ETL (copy/avg) | AVERAGE công suất trong giờ |

---

## 3) Thời tiết & bức xạ (từ Silver *hourly_weather*)
> Tất cả bức xạ ở Silver lưu **W/m² (giá trị trung bình theo giờ)**. Trong Gold, bổ sung field chuẩn hoá **`irr_kwh_m2_hour`** để dùng cho KPI theo IEC 61724 (yields).

| Column | Type | Nguồn | Tính ở đâu | Công thức |
|---|---|---|---|---|
| `shortwave_radiation` | DECIMAL(10,4) | Silver.shortwave_radiation | ETL | W/m² (TB giờ) |
| `direct_radiation` | DECIMAL(10,4) | Silver.direct_radiation | ETL | W/m² |
| `diffuse_radiation` | DECIMAL(10,4) | Silver.diffuse_radiation | ETL | W/m² |
| `direct_normal_irradiance` | DECIMAL(10,4) | Silver.direct_normal_irradiance | ETL | W/m² |
| **`irr_kwh_m2_hour`** | DECIMAL(10,6) | từ `shortwave_radiation` | **ETL (Gold)** | `irr_kwh_m2_hour = shortwave_radiation * 3600 / 1000`  *(đổi W/m² TB giờ → kWh/m²-giờ)* |
| `temperature_2m` | DECIMAL(6,2) | Silver.temperature_2m | ETL | °C |
| `dew_point_2m` | DECIMAL(6,2) | Silver.dew_point_2m | ETL | °C |
| `humidity_2m` | DECIMAL(5,2) | **Tính toán từ** dew_point & temperature | **ETL (Gold)** | RH = 100 * exp((17.625*TD)/(243.04+TD)) / exp((17.625*T)/(243.04+T)) |
| `cloud_cover`, `cloud_cover_low/mid/high` | DECIMAL(5,2) | Silver.cloud_* | ETL | % |
| `precipitation` | DECIMAL(8,3) | Silver.precipitation | ETL | mm |
| `sunshine_duration` | DECIMAL(10,2) | Silver.sunshine_duration | ETL | giây |
| `wind_speed_10m`, `wind_gusts_10m` | DECIMAL(6,2) | Silver.wind_* | ETL | m/s |
| `wind_direction_10m` | DECIMAL(6,2) | Silver.wind_direction_10m | ETL | độ (0–360) |
| `pressure_msl` | DECIMAL(8,1) | Silver.pressure_msl | ETL | hPa |

---

## 4) Không khí (từ Silver *hourly_air_quality*)
| Column | Type | Nguồn | Tính ở đâu | Công thức |
|---|---|---|---|---|
| `pm2_5`, `pm10`, `ozone`, `nitrogen_dioxide`, `sulphur_dioxide`, `carbon_monoxide`, `dust` | DECIMAL | Silver tương ứng | ETL | μg/m³ (hoặc ppm theo nguồn), quy đổi về μg/m³ nếu cần |
| `uv_index`, `uv_index_clear_sky` | DECIMAL(6,2) | Silver.uv_* (từ air_quality) | ETL | chỉ số UV |
| `aqi_value` | INT | Silver.aqi_value | ETL | Chỉ số AQI tích hợp |
| `aqi_category_key` | BIGINT (FK) | từ `aqi_value` | **ETL** | Join `dim_aqi_category` theo khoảng (EPA/WHO) |

---

## 5) KPI & Measures (tính **DAX** trong Power BI)

> Các KPI là **ratio-of-sums** để tuân thủ ngữ cảnh filter. PR/SEY theo IEC 61724; CF theo định nghĩa EIA.

### 5.1 Tổng năng lượng
```DAX
Total Energy (MWh) = SUM(fact_solar_environmental[energy_mwh])
```

### 5.2 Performance Ratio (PR, %)
- **Ý tưởng**: PR = Final Yield / Reference Yield.  
- **Final Yield** = năng lượng AC / công suất danh định (kWh/kWp).  
- **Reference Yield** = tổng bức xạ POA quy đổi về “giờ @ 1 kW/m²” (kWh/m² ÷ 1 kW/m²).

Khuyến nghị dùng `irr_kwh_m2_hour` đã chuẩn hoá trong ETL.

```DAX
PR (%) =
VAR E  = [Total Energy (MWh)]
VAR PkW = SUMX(DISTINCT(fact_solar_environmental[facility_key]), RELATED(dim_facility[total_capacity_mw])*1000)
VAR Yf = DIVIDE(E*1000, PkW)                           -- kWh/kWp
VAR Yr = SUM(fact_solar_environmental[irr_kwh_m2_hour]) -- kWh/m² ≡ kWh/kWp@1kW/m²
RETURN IF(Yr>0, DIVIDE(Yf, Yr) * 100, BLANK())
```

### 5.3 Capacity Factor (CF, %)
- **Định nghĩa EIA**: tỷ lệ giữa năng lượng thực tế và năng lượng tối đa nếu chạy full công suất liên tục trong cùng kỳ.

```DAX
CF Calendar (%) =
VAR E = [Total Energy (MWh)]
VAR P = SUMX(DISTINCT(fact_solar_environmental[facility_key]), RELATED(dim_facility[total_capacity_mw]))
VAR H = DISTINCTCOUNT(fact_solar_environmental[date_key]) * DISTINCTCOUNT(fact_solar_environmental[time_key])
RETURN DIVIDE(E, P * H) * 100
```

**Tuỳ chọn** – phiên bản *Observed* (không phạt thiếu dữ liệu): thay `H` bằng **số giờ thực có bản ghi**.

```DAX
CF Observed (%) =
VAR E = [Total Energy (MWh)]
VAR P = SUMX(DISTINCT(fact_solar_environmental[facility_key]), RELATED(dim_facility[total_capacity_mw]))
VAR H = DISTINCTCOUNT(fact_solar_environmental[date_key] & fact_solar_environmental[time_key])
RETURN DIVIDE(E, P * H) * 100
```

### 5.4 Specific Energy Yield (SEY, kWh/kWp)
```DAX
SEY (kWh/kWp) =
DIVIDE(
    [Total Energy (MWh)] * 1000,
    SUMX(DISTINCT(fact_solar_environmental[facility_key]), RELATED(dim_facility[total_capacity_mw]) * 1000)
)
```

### 5.5 Solar Efficiency (%)
Hiệu suất hệ thống quy đổi theo bức xạ tới (ước lượng):
```DAX
Solar Efficiency (%) =
VAR E  = [Total Energy (MWh)]
VAR Irr = SUM(fact_solar_environmental[irr_kwh_m2_hour])   -- kWh/m²
VAR Area_kW = 1.0                                          -- quy ước 1 kW/m²
RETURN IF(Irr>0, DIVIDE(E*1000, Irr*3600) * 100, BLANK())
```
> Lưu ý: Công thức trên phù hợp khi coi PR và Efficiency liên hệ tuyến tính qua bức xạ chuẩn; nếu có **POA Irradiance** riêng, thay `irr_kwh_m2_hour` bằng POA để chính xác hơn.

### 5.6 Chỉ số AQI (tham khảo trên dashboard)
```DAX
Current PM2_5 (µg/m³) = AVERAGE(fact_solar_environmental[pm2_5])
Current AQI           = AVERAGE(fact_solar_environmental[aqi_value])
```

---

## 6) Tính toán **ETL PySpark** quan trọng trong Gold

### 6.1 Chuẩn hoá bức xạ → `irr_kwh_m2_hour`
```python
from pyspark.sql import functions as F

# Calculate irr_kwh_m2_hour: Convert W/m² (average hourly) to kWh/m²-hour
# Formula: irr_kwh_m2_hour = shortwave_radiation * 3600 / 1000
fact = fact.withColumn(
    "irr_kwh_m2_hour",
    F.when(
        F.col("shortwave_radiation").isNotNull(),
        F.col("shortwave_radiation") * F.lit(3600.0) / F.lit(1000.0)
    ).otherwise(F.lit(None))
)
```

### 6.2 Tính Relative Humidity từ Dew Point và Temperature
```python
# Calculate relative humidity from dew point and temperature
# Formula: RH = 100 * (exp((17.625*TD)/(243.04+TD))/exp((17.625*T)/(243.04+T)))
# where TD is dew point and T is temperature in Celsius
weather = weather.withColumn(
    "humidity_2m",
    F.when(
        F.col("dew_point_2m").isNotNull() & F.col("temperature_2m").isNotNull(),
        100.0 * F.exp((17.625 * F.col("dew_point_2m")) / (243.04 + F.col("dew_point_2m"))) /
        F.exp((17.625 * F.col("temperature_2m")) / (243.04 + F.col("temperature_2m")))
    ).otherwise(F.lit(None))
)
```

### 6.3 Map AQI → `aqi_category_key`
```python
# Using build_aqi_lookup helper function
# This performs a range join: aqi_value BETWEEN aqi_range_min AND aqi_range_max
aqi_lookup = build_aqi_lookup(hourly_air_quality, dim_aqi_category)

# Join to get aqi_category_key for each fact row
fact = fact.join(
    aqi_lookup.select("facility_code", "date_hour_ts", "aqi_category_key", ...),
    on=["facility_code", "date_hour_ts"],
    how="left"
)
```

### 6.4 Data Quality Metrics
```python
# Completeness based on presence of energy, weather, and air quality data
fact = fact.withColumn(
    "completeness_pct",
    (
        F.when(F.col("has_energy"), F.lit(33.33)).otherwise(F.lit(0.0)) +
        F.when(F.col("has_weather"), F.lit(33.33)).otherwise(F.lit(0.0)) +
        F.when(F.col("has_air_quality"), F.lit(33.34)).otherwise(F.lit(0.0))
    ).cast(dec(5, 2))
)

# Validation: Record is valid if energy data is valid AND present
fact = fact.withColumn(
    "is_valid",
    F.col("has_energy") & 
    (F.col("energy_mwh") >= F.lit(0.0)) &
    (F.col("power_avg_mw").isNull() | (F.col("power_avg_mw") >= F.lit(0.0)))
)
```

---

## 7) Nguồn dữ liệu Silver → cột Gold (tóm tắt)

- **Energy** (từ `clean_hourly_energy`): `energy_mwh`, `power_avg_mw`, `intervals_count`, `is_valid`, `quality_flag`, `completeness_pct`
- **Weather** (từ `clean_hourly_weather`): `shortwave_radiation`, `direct_radiation`, `diffuse_radiation`, `direct_normal_irradiance`, `temperature_2m`, `dew_point_2m`, `cloud_cover[_low|_mid|_high]`, `precipitation`, `sunshine_duration`, `wind_*`, `pressure_msl`
  - **Tính toán trong ETL**: `irr_kwh_m2_hour` (từ shortwave_radiation), `humidity_2m` (từ dew_point & temperature)
- **Air Quality** (từ `clean_hourly_air_quality`): `pm2_5`, `pm10`, `dust`, `ozone`, `nitrogen_dioxide`, `sulphur_dioxide`, `carbon_monoxide`, `uv_index`, `uv_index_clear_sky`, `aqi_value`
  - **Tính toán trong ETL**: `aqi_category_key` (map từ aqi_value theo dim_aqi_category)
- **Dimensions** (từ Gold dims): `facility_key` (dim_facility), `date_key` (dim_date), `time_key` (dim_time)

---

## 8) Kiểm định & thống nhất đơn vị
- Bức xạ: Silver lưu **W/m² (TB giờ)** → Gold thêm **kWh/m²-giờ** để dùng cho yield/PR.  
- Nhiệt độ (°C), gió (m/s), mưa (mm), áp suất (hPa) — giữ nguyên.  
- AQI & PM (µg/m³): giữ nguyên; nếu nguồn có ppm, đổi về µg/m³ theo tiêu chuẩn cảm biến.

---

## 9) Tài liệu tham khảo (khoa học/chuẩn)

- **PR (IEC 61724-1) & Yield khái niệm:** Sandia PV Performance Modeling Collaborative — *Performance Ratio (IEC 61724)*: https://pvpmc.sandia.gov/modeling-guide/5-ac-system-output/pv-performance-metrics/performance-ratio/  
- **Yield/Normalised index:** PVSyst help — *Normalised performance index (Yf, Yr)*: https://www.pvsyst.com/help/project-design/results/normalised-performance-index.html  
- **Capacity Factor (định nghĩa chuẩn):** U.S. EIA Glossary — *Capacity factor*: https://www.eia.gov/tools/glossary/index.php?id=Capacity_factor  
- **AQI chính thức (dải & màu):** U.S. EPA / AirNow — *AQI Basics*: https://www.airnow.gov/aqi/aqi-basics  ; & *AQI Fact Sheet (PM updates 2024)*: https://www.epa.gov/system/files/documents/2024-02/pm-naaqs-air-quality-index-fact-sheet.pdf  
- **WHO AQG 2021 (PM2.5/PM10/NO2/O3/SO2/CO):** WHO guideline: https://www.who.int/publications/i/item/9789240034228  
- **POA Irradiance & chuyển đổi W/m² ↔ kWh/m²:** NREL & PV Education  
  - NREL — *POA Irradiance* (Xie 2021): https://docs.nrel.gov/docs/fy21osti/80260.pdf  
  - NREL — *POA for One-Axis* (Vignola 2018): https://docs.nrel.gov/docs/fy18osti/67882.pdf  
  - PV Education — *Average Solar Radiation / Insolation*: https://www.pveducation.org/pvcdrom/properties-of-sunlight/average-solar-radiation  
  - PennState EME812 — *Irradiance vs daily irradiation (area under curve)*: https://courses.ems.psu.edu/eme812/node/644

---

### Phụ lục A — Lý do phân bổ
- **ETL (PySpark)**: chuẩn hoá đơn vị, tạo `irr_kwh_m2_hour`, mapping AQI, cờ chất lượng → tránh sai lệch khi tổng hợp trên BI.  
- **DAX**: các KPI **ratio-of-sums** thay đổi đúng theo filter (facility/date/time/AQI).

> Tài liệu nội bộ liên quan: “Datalakehouse-Analysis-Problem-Complete.pdf” (thiết kế schema, measure) và “PowerBI-Complete-Implementation-Guide.pdf” (kịch bản dashboard).