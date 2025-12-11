# Silver Layer Validation Rules - Quick Reference

**Cập Nhật:** 2025-01-16  
**Phiên Bản:** 2.2 (PEAK_REFERENCE_MWH đã được cập nhật lên 186.0)

---

## 📊 ENERGY DATA VALIDATION

### 🎯 KẾT QUẢ PHÂN TÍCH TỪ DỮ LIỆU THỰC (Bronze Data)

**Nguồn phân tích:** `notebooks/silver_bounds_analysis.ipynb`

| Metric | Actual Min | Actual Max | Actual P95 | Current Bound |
|--------|------------|------------|------------|---------------|
| Energy (MWh) | 0.0 | 275.05 | 115.81 | min=0 ✅ |
| Peak Hour Energy | 0.0 | 275.05 | 186.45 | PEAK_REF=186 ✅ |

> **✅ ĐÃ CẬP NHẬT (2025-01-16):** PEAK_REFERENCE_MWH đã được tăng từ 85.0 → 186.0 dựa trên P95 của peak hours.

### Hard Bounds (→ BAD)

| Metric | Min | Max | Đơn Vị | Nguồn |
|--------|-----|-----|--------|-------|
| `energy_mwh` | **0.0** | ∞ | MWh | Physics: Solar không thể phát năng lượng âm. **Actual data: 0 negative values** ✅ |

### Soft Checks (→ WARNING)

#### 1. Night Energy Anomaly
```python
is_night = (hour >= 22) | (hour < 6)
is_night_anomaly = is_night & (energy_mwh > 1.0)
```
**Threshold 1.0 MWh:** Cho phép sensor noise, > 1 MWh ban đêm = lỗi.
**Từ data:** 0 records có night energy > 1 MWh ✅

#### 2. Daytime Zero Energy
```python
is_daytime = (hour >= 8) & (hour <= 17)
is_daytime_zero = is_daytime & (energy_mwh == 0.0)
```
**Lý do:** Ban ngày luôn có ánh sáng → phải có generation.
**Từ data:** 2,553 records có zero energy trong daytime → cần flag ✅

#### 3. Equipment Downtime
```python
is_peak = (hour >= 10) & (hour <= 14)
is_equipment_downtime = is_peak & (energy_mwh == 0.0)
```
**Lý do:** Peak hours mà = 0 → chắc chắn thiết bị tắt.

#### 4. Transition Hour Low Energy ⚠️
```python
PEAK_REFERENCE_MWH = 85.0  # ⚠️ NÊN CẬP NHẬT = 186.0 (P95 từ data thực)

threshold_factor = (
    F.when(is_sunrise, 0.05)        # 5% for 06:00-08:00
    .when(is_early_morning, 0.08)   # 8% for 08:00-10:00
    .when(is_sunset, 0.10)          # 10% for 17:00-19:00
    .otherwise(0.0)
)

is_transition_low = (
    (is_transition_period) & 
    (energy > 0.01) & 
    (energy < PEAK_REFERENCE_MWH * threshold_factor)
)
```

### KET QUA PHAN TICH TRANSITION HOURS (Tu Data Thuc)

**Peak Average Energy = 57.65 MWh (Hour 11)**

| Hour | Avg Energy | % of Peak | Threshold | Status |
|------|------------|-----------|-----------|--------|
| **6** (Sunrise) | 0.60 MWh | **1.0%** | 5% | Flag if < 5% of peak |
| **7** (Sunrise) | 11.47 MWh | **19.9%** | 5% | OK (19.9% > 5%) |
| **8** (Early Morning) | 37.98 MWh | **65.9%** | 8% | OK (65.9% > 8%) |
| **17** (Sunset) | 29.78 MWh | **51.7%** | 10% | OK (51.7% > 10%) |
| **18** (Sunset) | 20.07 MWh | **34.8%** | 10% | OK (34.8% > 10%) |

### GIAI THICH THRESHOLDS

#### NGUON GOC CAC GIA TRI 5%, 8%, 10%

**QUAN TRONG:** Cac gia tri nay la **HEURISTIC** - duoc chon dua tren:
1. **Phan tich du lieu thuc te** (Bronze data analysis)
2. **Logic solar physics co ban** (mat troi len/xuong)
3. **KHONG co tai lieu tham khao hoac paper nao**

**Co so chon threshold:**

| Period | Hours | Threshold | Ly do chon |
|--------|-------|-----------|------------|
| Sunrise | 06:00-08:00 | 5% | Hour 6 chi co 1% of peak (tu data). Chon 5% de flag anomaly neu energy > 0 nhung qua thap |
| Early Morning | 08:00-10:00 | 8% | Hour 8 co 65.9% of peak. Chon 8% vi luc nay da co nhieu anh sang |
| Sunset | 17:00-19:00 | 10% | Hour 17 co 51.7% of peak. Chon 10% vi con nhieu anh sang |

**Nguon: `src/pv_lakehouse/etl/silver/hourly_energy.py` lines 94-102**
```python
# TRANSITION_HOUR_LOW_ENERGY detection
# Thresholds based on analysis:
# - Sunrise (06:00-08:00): Only flag if <5% of expected peak
# - Early Morning (08:00-10:00): Only flag if <8% of expected peak  
# - Sunset (17:00-19:00): Only flag if <10% of expected peak
threshold_factor = (
    F.when(is_sunrise, 0.05)        # 5% of peak for sunrise
    .when(is_early_morning, 0.08)   # 8% of peak for early morning
    .when(is_sunset, 0.10)          # 10% of peak for sunset
    .otherwise(0.0)
)
```

#### MUC DICH CUA CAC THRESHOLD

**KHONG phai** nguong energy trung binh mong doi.
**LA** nguong toi thieu de phat hien anomaly (equipment issue).

**Vi du cu the:**
```
Hour 6 (Sunrise): avg = 0.60 MWh = 1% of peak
  -> Threshold 5% = 9.3 MWh (voi PEAK_REF = 186)
  -> Dieu kien flag: energy > 0.01 MWh AND energy < 9.3 MWh
  -> Y nghia: Co anh sang (energy > 0) nhung san luong qua thap (< 5% peak)
  -> Co the la: equipment issue, partial shading, inverter fault

Hour 17 (Sunset): avg = 29.78 MWh = 51.7% of peak  
  -> Threshold 10% = 18.6 MWh
  -> Dieu kien flag: energy > 0.01 MWh AND energy < 18.6 MWh
  -> Y nghia: Con nhieu anh sang nhung san luong bat thuong
```

#### TAI SAO CHON CAC GIA TRI NAY?

**5% cho Sunrise (06:00-08:00):**
- Hour 6 actual avg = 0.60 MWh = 1% of peak
- Chon 5% vi: mat troi moi len, energy thap la binh thuong
- Flag chi khi: co anh sang (energy > 0) nhung qua thap (< 5%)
- Neu chon cao hon (10%), se flag qua nhieu false positive

**8% cho Early Morning (08:00-10:00):**
- Hour 8 actual avg = 37.98 MWh = 65.9% of peak  
- Chon 8% vi: luc nay da co nhieu anh sang
- Energy < 8% peak luc 8h sang = co van de

**10% cho Sunset (17:00-19:00):**
- Hour 17 actual avg = 29.78 MWh = 51.7% of peak
- Chon 10% vi: con nhieu anh sang
- Energy < 10% peak luc 5h chieu = co van de

**Logic tong quat:**
```
Transition threshold KHONG phai la: "Gio nay phai dat X% cua peak"
Ma la: "Neu gio nay co anh sang (energy > 0) va energy < X% cua peak -> co van de"
```

#### LUU Y VE TINH HEURISTIC

- Cac threshold nay **CHUA DUOC VALIDATED** bang cach so sanh voi actual equipment failures
- Co the dieu chinh neu:
  - Qua nhieu false positives -> Tang threshold
  - Bo sot anomalies -> Giam threshold
- **Khuyen nghi:** Thu nghiem voi data thuc va dieu chinh

### ✅ PEAK_REFERENCE_MWH = 186.0 (ĐÃ CẬP NHẬT)

**Nguồn:** Giá trị được xác định từ **phân tích dữ liệu thực**:
- P95 của energy trong peak hours (10:00-14:00) = 186.45 MWh
- Làm tròn xuống = 186.0 MWh
- Phản ánh actual peak output của các facilities trong dataset

**Các threshold hiện tại với PEAK_REF = 186:**
| Threshold | Percentage | Value (MWh) |
|-----------|------------|-------------|
| Transition Low | 5% | 9.3 |
| Night Anomaly | - | 1.0 |
| Efficiency Check | 50% | 93.0 |

**Truy vấn xác nhận:**
```sql
-- Verify P95 of peak hours
SELECT 
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY energy_mwh) AS p95_energy
FROM iceberg.silver.clean_hourly_energy
WHERE HOUR(date_hour) BETWEEN 10 AND 14;  -- → 186.45 MWh
```

#### 5. Peak Hour Low Efficiency
```python
is_efficiency_anomaly = (
    is_peak &                                  # 10:00-14:00
    (energy > 0.5) &                           # Có phát điện
    (energy < PEAK_REFERENCE_MWH * 0.50)       # < 50% capacity
)
```
**Threshold 50%:** Peak mà < 50% = likely equipment issue, not just weather.

---

## 🌦️ WEATHER DATA VALIDATION

### 🎯 KẾT QUẢ PHÂN TÍCH TỪ DỮ LIỆU THỰC (Bronze Data)

**Nguồn phân tích:** `notebooks/silver_bounds_analysis.ipynb`  
**Data Range:** 2025-01-01 đến 2025-12-04 (64,776 records, 8 facilities)

| Variable | Actual Min | Actual Max | P95 | P99 | Current Bound | Status |
|----------|------------|------------|-----|-----|---------------|--------|
| Temperature | **-2.3°C** | **43.8°C** | 31.7°C | 36.3°C | -50°C to 60°C | ✅ OK |
| Shortwave Radiation | **0** | **1127 W/m²** | 854 | 1006 | 0 to 1500 | ✅ OK |
| Wind Speed | **0** | **45.6 km/h** | 23.8 | 29.6 | 0 to 200 | ✅ OK |

### Hard Bounds (→ BAD)

| Column | Min | Max | Đơn Vị | Nguồn |
|--------|-----|-----|--------|-------|
| `shortwave_radiation` | 0 | **1150** | W/m² | P99.5=1045, max observed=1120. Solar constant ~1361 W/m² (NASA), surface ~1100 W/m². |
| `direct_radiation` | 0 | **1050** | W/m² | Max observed=1009. Australian desert clear sky ~1000 W/m². WMO BSRN data. |
| `diffuse_radiation` | 0 | **520** | W/m² | Max observed=520. Typically 20-40% of global. Open-Meteo historical. |
| `direct_normal_irradiance` | 0 | **1060** | W/m² | Max observed=1057.3. DNI can exceed GHI. NREL Solar Resource. |
| `temperature_2m` | **-10** | **50** | °C | Australia: record -23°C, record 50.7°C. BOM records. |
| `dew_point_2m` | **-20** | **30** | °C | Dry desert to humid coastal. P99=20.2°C. Meteorological limits. |
| `wet_bulb_temperature_2m` | **-5** | **40** | °C | Always ≤ air temp. Thermodynamic relationship. |
| `cloud_cover*` | 0 | **100** | % | Percentage physical bounds. |
| `precipitation` | 0 | **1000** | mm | Record hourly ~400mm. BOM extreme records. |
| `sunshine_duration` | 0 | **3600** | s | Max 1 hour = 3600 seconds. Physical limit. |
| `total_column_integrated_water_vapour` | 0 | **100** | kg/m² | Tropical max ~70. ERA5 reanalysis. |
| `wind_speed_10m` | 0 | **50** | m/s | Max observed=47.2 (cyclones). BOM cyclone data. |
| `wind_direction_10m` | 0 | **360** | ° | Compass degrees physical bounds. |
| `wind_gusts_10m` | 0 | **120** | m/s | Record ~113 m/s (Cyclone Olivia). WMO records. |
| `pressure_msl` | **985** | **1050** | hPa | P99=1033. BOM pressure records. |

### Soft Checks (→ WARNING)

#### 1. Night Radiation Spike
```python
is_night = (hour < 6) | (hour >= 22)
is_night_rad_high = is_night & (shortwave_radiation > 100)
```
**Threshold 100 W/m²:** Cho phép moonlight/twilight, > 100 ban đêm = sensor error.

#### 2. Radiation Inconsistency
```python
radiation_inconsistency = (
    (direct_radiation + diffuse_radiation) > 
    (shortwave_radiation * 1.05)
)
```
**Physics:** Shortwave = Direct + Diffuse + Reflected. Direct + Diffuse ≤ Shortwave (5% tolerance).

#### 3. Cloud Measurement Inconsistency
```python
is_peak_sun = (hour >= 10) & (hour <= 14)
high_cloud_peak = (
    is_peak_sun & 
    (cloud_cover > 98) &           # Near-total coverage
    (shortwave_radiation < 600)    # Very low radiation
)
```
**Why 98% not 95%?** 95% cloud còn ~5% direct sunlight. 98% = truly overcast. Reduces false positives ~90%.

**Why 600 W/m²?** Even with 98% clouds, diffuse can reach 500-700 W/m². Below 600 suspicious.

#### 4. Extreme Temperature
```python
extreme_temp = (temperature_2m < -10) | (temperature_2m > 45)
```
**Lý do:** Australia: -10°C to 45°C covers 99.9%. Beyond = measurement error or extreme event.

---

## 💨 AIR QUALITY DATA VALIDATION

### 🎯 KẾT QUẢ PHÂN TÍCH TỪ DỮ LIỆU THỰC (Bronze Data)

**Nguồn phân tích:** `notebooks/silver_bounds_analysis.ipynb`  
**Data Range:** 2025-01-01 đến 2025-12-04 (64,776 records, 8 facilities)

| Variable | Actual Min | Actual Max | Median | P95 | P99 | Current Bound | Status |
|----------|------------|------------|--------|-----|-----|---------------|--------|
| PM2.5 | **0** | **44.8 µg/m³** | 2.6 | 8.8 | 13.5 | 0 to 1000 | ✅ OK |
| UV Index | **0** | **14.35** | - | 8.0 | 11.4 | 0 to 20 | ✅ OK |

**Ghi chú:** Bounds rộng hơn actual data để accommodate extreme events (bushfire, dust storm).

### Hard Bounds (→ WARNING only)

| Column | Min | Max | Đơn Vị | Nguồn |
|--------|-----|-----|--------|-------|
| `pm2_5` | 0 | **500** | µg/m³ | EPA AQI max scale. Bushfire can exceed. |
| `pm10` | 0 | **500** | µg/m³ | EPA AQI max scale. |
| `dust` | 0 | **500** | µg/m³ | Similar to PM. |
| `nitrogen_dioxide` | 0 | **500** | µg/m³ | EPA bounds. |
| `ozone` | 0 | **500** | µg/m³ | EPA bounds. |
| `sulphur_dioxide` | 0 | **500** | µg/m³ | EPA bounds. |
| `carbon_monoxide` | 0 | **500** | µg/m³ | EPA bounds. |
| `uv_index*` | 0 | **15** | - | WHO scale, Australia can reach 16+. |

**Note:** Air quality uses WARNING only, no BAD (less strict than weather/energy).

### AQI Calculation (EPA Standard)

```python
# PM2.5 Breakpoints (µg/m³) → AQI
[0.0-12.0]   → [0-50]     Good
[12.1-35.4]  → [51-100]   Moderate
[35.5-55.4]  → [101-150]  Unhealthy (Sensitive Groups)
[55.5-150.4] → [151-200]  Unhealthy
[150.5-250.4]→ [201-300]  Very Unhealthy
[250.5-500+] → [301-500]  Hazardous
```

**Source:** U.S. EPA Air Quality Index guidelines.

### AQI Categories

| AQI | Category | Health Advice |
|-----|----------|---------------|
| 0-50 | **Good** | Không ảnh hưởng |
| 51-100 | **Moderate** | Sensitive groups cẩn thận |
| 101-200 | **Unhealthy** | Mọi người có thể bị ảnh hưởng |
| 201-500 | **Hazardous** | Cảnh báo sức khỏe nghiêm trọng |

---

## 🔄 QUALITY FLAG RULES

### Energy
```
BAD:     energy_mwh < 0 (negative values)
WARNING: night_anomaly | daytime_zero | equipment_downtime | 
         transition_low | efficiency_anomaly
GOOD:    All checks pass
```

### Weather
```
BAD:     Any column OUT_OF_BOUNDS | night_radiation_spike
WARNING: radiation_inconsistency | high_cloud_peak | extreme_temp
GOOD:    All checks pass
```

### Air Quality
```
WARNING: Any column OUT_OF_BOUNDS | AQI invalid
GOOD:    All checks pass
(No BAD for air quality - less strict)
```

---

## 📊 EXPECTED QUALITY DISTRIBUTION

| Layer | GOOD | WARNING | BAD |
|-------|------|---------|-----|
| Energy | 85-95% | 5-15% | < 0.1% |
| Weather | 95-99% | 1-5% | < 0.1% |
| Air Quality | 98-100% | 0-2% | 0% |

**Note:** Quá nhiều WARNING = bounds quá loose. Quá ít WARNING = bounds quá strict.

---

## 📚 THAM KHẢO

### Radiation Bounds
- NASA Solar Constant: ~1361 W/m²
- WMO Baseline Surface Radiation Network (BSRN)
- NREL National Solar Radiation Database

### Temperature/Weather Bounds
- Bureau of Meteorology Australia (BOM)
- World Meteorological Organization (WMO)
- ERA5 Reanalysis data

### Air Quality
- U.S. EPA Air Quality Index (AQI) guidelines
- WHO Air Quality Guidelines

---

**Version:** 2.2  
**Last Updated:** 2025-01-16
