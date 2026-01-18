# 📊 Silver Layer Validation Rules - Evidence-Based Analysis

**Tác Giả:** Data Engineering Team  
**Cập Nhật:** 2025-12-16  
**Phiên Bản:** 1.0  
**Phương pháp:** Phân tích dữ liệu thực tế từ Bronze layer (58,552 energy records)

---

## 🎯 Mục Đích Tài Liệu

Tài liệu này **CHỨNG MINH CỐ CẢ** rằng các validation rules ở Silver layer có cơ sở từ dữ liệu thực:
- ✅ Từ đâu mỗi con số threshold được chọn?
- ✅ Những gì trong dữ liệu chứng minh điều đó?
- ✅ Liệu có quá nghiêm ngặt hay quá lỏng lẻo?

---

## 📈 PHÂN TÍCH ENERGY DATA (Bronze Layer)

### Dữ liệu Phân Tích

```
Nguồn:     iceberg.bronze.raw_facility_timeseries (metric = 'energy')
Thời kỳ:   2025-01-01 đến 2025-12-09
Facilities: 8 (AVLSF, BOMENSF, DARLSF, EMERASF, FINLEYSF, LIMOSF2, WRSF1, YATSF1)
Records:   58,552 (5-minute intervals)
```

### 1️⃣ BOUNDS CỦA ENERGY - CHỨNG MINH

#### A. Min Bound: Energy >= 0.0 MWh

**Kết quả phân tích:**
```
Min energy in data:           0.0 MWh       ✅
Number of negative values:    0             ✅ Zero negatives!
```

**Chứng minh:**
| Metric | Giá trị | Ý nghĩa |
|--------|--------|---------|
| Min | 0.0 MWh | Thấp nhất là 0, không có giá trị âm |
| P1 | 0.0 MWh | 1% dữ liệu = 0 (ban đêm) |
| P5 | 0.0 MWh | 5% dữ liệu = 0 (thời gian tối) |

**Kết luận:** ✅ **ENERGY_LOWER = 0.0 MWh là CHÍNH XÁC**
- Vật lý solar không thể có energy âm
- Dữ liệu thực: 0 records âm (hoàn hảo!)

---

#### B. PEAK_REFERENCE_MWH - CHỨNG MINH & KHUYẾN NGHỊ CẬP NHẬT

**Kết quả phân tích (Giờ cao điểm 10:00-14:00):**
```
MAX energy (peak hours):        275.05 MWh
P95 energy (peak hours):        185.50 MWh ← CÓ NÊN DÙNG ĐÂY
P99 energy (peak hours):        255.81 MWh
AVG energy (peak hours):         56.63 MWh

Current PEAK_REFERENCE_MWH:     85.0 MWh   ← QUAAAA THẤP!
```

**Chứng minh:**
| Metric | Giá trị | Lý do chọn |
|--------|--------|-----------|
| **MAX (actual max capacity)** | 275.05 MWh | Chỉ xảy ra vài lần/năm (0.1%) |
| **P95 (95th percentile)** | 185.50 MWh | 95% thời gian < 185.5, 5% > 185.5 |
| **P99 (99th percentile)** | 255.81 MWh | Quá cao, sẽ miss 1% extreme events |
| **Current (85.0 MWh)** | ❌ | Quá thấp! Là P50-P60 chứ không phải P95 |

**Phân tích chi tiết:**
```
Energy Distribution (Peak Hours 10:00-14:00):
┌──────────────────────────────────────────────────────────┐
│  0%         25%        50%        75%        95%  100%   │
│  |----------|----------|----------|----------|--------| │
│  0          ...        ...        ...       185.5   275  │
│                                              ↑            │
│                                         SHOULD BE HERE    │
│                                         (P95)            │
│  Current 85 = P50-60 (too low!)                          │
└──────────────────────────────────────────────────────────┘
```

**🚨 KHUYẾN NGHỊ:**
```
❌ CURRENT:  PEAK_REFERENCE_MWH = 85.0
✅ UPDATED:  PEAK_REFERENCE_MWH = 185.5 (hoặc làm tròn = 186.0)

Lý do:
1. P95 phản ánh "sức mạnh bình thường" của facility
2. Threshold 85 quá thấp → flag quá nhiều false positives
3. Threshold 186 phù hợp với dữ liệu thực (95% dữ liệu cấp dưới)
```

**File cần cập nhật:**
```python
# src/pv_lakehouse/etl/silver/hourly_energy.py

# CURRENT (SAI):
PEAK_REFERENCE_MWH = 85.0

# UPDATED (ĐÚNG):
PEAK_REFERENCE_MWH = 186.0  # Dựa trên P95 từ Bronze analysis
```

---

### 2️⃣ SOFT CHECKS - NIGHT ENERGY ANOMALY

#### Threshold: Energy > 1.0 MWh Ban Đêm (22:00-06:00)

**Kết quả phân tích:**
```
Time period:              22:00-06:00 (8 giờ tối)
Average energy:           0.0 MWh (virtually zero)
Records > 1.0 MWh:        0 ❌ (zero violations!)
Max night energy:         0.35 MWh (once, at 22:00)
```

**Chứng minh:**
| Hour | Avg (MWh) | Max (MWh) | Count | Ý nghĩa |
|------|-----------|-----------|-------|---------|
| 22 | 0.0003 | 0.35 | 1 | Rất thấp (dusk) |
| 23-05 | 0.0 | 0.0 | 0 | Hoàn toàn tối |
| 6 | 0.33 | 18.11 | 2,440 | Sunrise bắt đầu |

**Kết luận:** ✅ **Threshold 1.0 MWh là CHÍNH XÁC**
- Dữ liệu thực: 0 records > 1 MWh ban đêm
- Threshold 1.0 = cách an toàn để phát hiện sensor error
- Cho phép noise nhỏ (sensor drift) nhưng flag error lớn

---

### 3️⃣ SOFT CHECKS - DAYTIME ZERO ENERGY

#### Threshold: Energy = 0 MWh Trong Giờ 08:00-17:00

**Kết quả phân tích:**
```
Time period:          08:00-17:00 (10 giờ sáng)
Total records:        24,400 (10 hours × 2,440 records/hour)
Zero energy records:  2,449 (10.0% of daytime!)
```

**Phân tích chi tiết:**
| Hour | Avg (MWh) | % Zero | Đánh giá |
|------|-----------|--------|---------|
| 8 | 37.18 | ~3% | Bình thường (sunrise) |
| 9 | 53.35 | ~1% | Tốt |
| 10-14 (peak) | 56.9 | ~0% | Rất tốt |
| 15 | 53.05 | ~1% | Tốt |
| 16 | 42.20 | ~2% | Bình thường |
| 17 | 27.48 | ~5% | Có vấn đề (sunset) |

**Chứng minh:**
- 2,449 records = 10% dữ liệu ban ngày có zero energy
- Đó là **có vấn đề** (equipment failure, maintenance, etc.)
- Nếu không flag: ML model sẽ học sai logic ("ban ngày = không phát điện")

**Kết luận:** ✅ **Daytime zero energy PHẢI được flag**

---

### 4️⃣ TRANSITION HOUR THRESHOLDS - CHI TIẾT CHỨNG MINH

Đây là phần **MỌI NGƯỜI CÓ NGHI NGỜ NHẤT**. Hãy xem dữ liệu thực:

#### A. Hour 6 (Sunrise) - Threshold 5% of Peak

**Dữ liệu thực:**
```
Hour 6 average:         0.33 MWh
Peak average (hour 11): 57.20 MWh  (từ P95=185.5, average=56.63)
Percentage:             0.33 / 57.2 = 0.58% ← Chỉ 0.6% of peak!
```

**Threshold logic:**
```
Current threshold:      5% of PEAK_REFERENCE (85 MWh)
                        = 5% × 85 = 4.25 MWh

New threshold:          5% of PEAK_REFERENCE (186 MWh)
                        = 5% × 186 = 9.3 MWh

Actual hour 6:          0.33 MWh << 9.3 MWh ✅ (rất dưới threshold)
```

**Ý nghĩa của threshold 5%:**
```
┌────────────────────────────────────────────────────┐
│  Hour 6 (Sunrise)  Logic:                          │
│                                                    │
│  Nếu hour 6 >= 5% peak (9.3 MWh) → GOOD ✓        │
│  Nếu 0 < hour 6 < 5% peak → WARNING ⚠️            │
│  Nếu hour 6 = 0 → GOOD ✓ (dusk side)              │
│                                                    │
│  KHÔNG phải: "Hour 6 should be exactly X"         │
│  MÀ là: "If hour 6 has output, it should be >= X" │
└────────────────────────────────────────────────────┘
```

**Tại sao 5% không phải 3% hoặc 10%?**

```
3% threshold = 5.6 MWh
  → Quá thấp, flag quá nhiều false positives
  → Hour 7 avg = 9.88 MWh > 5.6 (OK)
  → Nhưng hour 8 avg = 37.18 MWh (rõ ràng khác)
  
5% threshold = 9.3 MWh (BEST)
  → Reasonable, phát hiện anomaly hiệu quả
  → Hour 6 < Hour 7 << Hour 8 (rõ gradient)
  → Allow flexibility nhưng catch real issues
  
10% threshold = 18.6 MWh
  → Quá cao, miss anomalies ở sunrise
  → Hour 7 = 9.88 < 18.6 (false negative!)
```

#### B. Hour 8 (Early Morning) - Threshold 8% of Peak

**Dữ liệu thực:**
```
Hour 8 average:         37.18 MWh
Peak average:           57.20 MWh
Percentage:             37.18 / 57.2 = 65.0% ✅ (rất cao, tốt)

Threshold 8% = 8% × 186 = 14.88 MWh
Actual hour 8 = 37.18 MWh >> 14.88 (rất dưới threshold)
```

**Tại sao 8%?**
```
Hour 8 = 65% of peak → nhật nhiều mặt trời
Threshold 8% = 14.88 MWh = về 26% of hour 8 avg

Nếu hour 8 < 8% = rõ ràng có vấn đề equipment
  VD: < 14.88 MWh lúc 8 sáng = sensor error/maintenance
```

#### C. Hour 17 (Sunset) - Threshold 10% of Peak

**Dữ liệu thực:**
```
Hour 17 average:        27.48 MWh
Peak average:           57.20 MWh  
Percentage:             27.48 / 57.2 = 48.0% ✅ (còn nhiều sáng)

Threshold 10% = 10% × 186 = 18.6 MWh
Actual hour 17 = 27.48 MWh > 18.6 (OK)
```

**Tại sao 10%?**
```
Hour 17 = 48% of peak → còn rất nhiều sáng
Threshold 10% = 18.6 MWh = ~68% of hour 17 avg

Nếu hour 17 < 10% = đặc biệt lạ (sunset vẫn sáng)
```

---

## 📊 TÓMON: TRANSITION HOUR THRESHOLDS

| Hour | Period | Avg (MWh) | % Peak | Threshold | Chứng minh |
|------|--------|-----------|--------|-----------|-----------|
| **6** | Sunrise | 0.33 | 0.6% | 5% (9.3) | 0.33 < 9.3: OK, rất sớm |
| **7** | Sunrise | 9.88 | 17.3% | 5% (9.3) | 9.88 ≈ 9.3: Borderline sunrise |
| **8** | Early morning | 37.18 | 65.0% | 8% (14.9) | 37.18 >> 14.9: Good, sáng rồi |
| **17** | Sunset | 27.48 | 48.0% | 10% (18.6) | 27.48 > 18.6: OK, còn sáng |
| **18** | Sunset | 17.78 | 31.1% | 10% (18.6) | 17.78 ≈ 18.6: Borderline |

---

## 🌦️ PHÂN TÍCH WEATHER DATA (Bronze Layer)

### Dữ liệu Phân Tích

```
Nguồn:     iceberg.bronze.raw_facility_weather
Thời kỳ:   2025-01-01 đến 2025-12-09
Facilities: 8
Records:   65,704 (hourly)
```

### Key Metrics & Bounds

#### A. Temperature (°C)

**Kết quả phân tích:**
```
Actual Min:       -2.3°C
Actual Max:      43.8°C
Actual P95:      31.6°C
Actual P99:      36.3°C
Current Bounds:  -10°C to 50°C ← ✅ Rất thoải mái!
```

**Chứng minh:**
| Metric | Giá trị | Ý nghĩa |
|--------|--------|---------|
| Min | -2.3°C | Mùa đông ở Brisbane (hiếm gặp) |
| Max | 43.8°C | Ngày nóng (extreme nhưng khả thi) |
| P99 | 36.3°C | 99% thời gian < 36.3°C |

**Kết luận:** ✅ **Temperature bounds -10°C to 50°C là CHÍNH XÁC**
- Dữ liệu thực nằm trong bounds (43.8 < 50)
- Còn buffer cho extreme events

#### B. Shortwave Radiation (W/m²)

**Kết quả phân tích:**
```
Actual Min:       0.0 W/m²
Actual Max:    1119.0 W/m²
Actual P95:     826.4 W/m²
Actual P99:     985.4 W/m²
Current Bounds: 0 to 1150 W/m² ← ✅ Chính xác!
```

**Chứng minh:**
| Metric | Giá trị | Ý nghĩa |
|--------|--------|---------|
| Max | 1119.0 | Bức xạ mặt trời cực đại (hiếm) |
| P95 | 826.4 | 95% thời gian < 826 W/m² |
| Current Bound | 1150 | Buffer 31 W/m² (2.8%) |

**Kết luận:** ✅ **Radiation bounds 0 to 1150 W/m² là TỐT**
- Max thực = 1119 < 1150 (trong bounds)
- Cho phép rare extremes

#### C. Wind Speed (m/s)

**Kết quả phân tích:**
```
Actual Min:       0.0 m/s
Actual Max:      42.9 m/s (154 km/h)
Actual P95:      23.0 m/s
Actual P99:      29.2 m/s
Current Bounds: 0 to 50 m/s ← ✅ OK!
```

**Chứng minh:**
| Metric | Giá trị | Ý nghĩa |
|--------|--------|---------|
| Max | 42.9 m/s | Gió bão (rare but realistic) |
| P99 | 29.2 m/s | Gió mạnh |
| Buffer | 7.1 m/s | 16% margin để extreme |

**Kết luận:** ✅ **Wind speed bounds 0 to 50 m/s là CÓ CƠ SỞ**

---

## 🌫️ PHÂN TÍCH AIR QUALITY DATA (Bronze Layer)

### Dữ liệu Phân Tích

```
Nguồn:     iceberg.bronze.raw_facility_air_quality
Thời kỳ:   2025-01-01 đến 2025-12-09
Facilities: 8
Records:   65,704 (hourly)
```

### Key Metrics & Bounds

#### A. PM2.5 (µg/m³)

**Kết quả phân tích:**
```
Actual Min:       0.0 µg/m³
Actual Max:      44.8 µg/m³
Actual Median:    2.7 µg/m³
Actual P95:       8.85 µg/m³
Actual P99:      13.51 µg/m³
Current Bounds: 0 to 500 µg/m³ ← ✅ Rất thoải mái
```

**Chứng minh:**
| Metric | Giá trị | Ý nghĩa |
|--------|--------|---------|
| Max | 44.8 | Mức "Không tốt" theo AQI (rất hiếm) |
| P99 | 13.51 | 99% < 13.5 µg/m³ (sạch sẽ) |
| Median | 2.7 | Số liệu điển hình rất thấp |

**Kết luận:** ✅ **PM2.5 bounds 0 to 500 µg/m³ là TỐT**
- Dữ liệu thực chỉ đến 44.8 (rất sạch!)
- Buffer rất lớn cho pollution events

#### B. UV Index

**Kết quả phân tích:**
```
Actual Min:       0.0
Actual Max:      14.35
Actual P95:       7.61
Actual P99:      11.29
Current Bounds: 0 to 15 ← ✅ Rất chặt!
```

**Chứng minh:**
| Metric | Giá trị | Ý nghĩa |
|--------|--------|---------|
| Max | 14.35 | UV "Extreme" (rare summer peak) |
| P99 | 11.29 | 99% < 11.3 (strong UV) |
| Bound | 15 | Buffer chỉ 0.65 (4%) |

**Kết luận:** ✅ **UV Index bounds 0 to 15 là CHÍNH XÁC**
- Max thực = 14.35 < 15 (tight bound)
- Phản ánh dữ liệu Australia tốt

---

## 🎯 TỔNG HỢP: WEATHER & AIR BOUNDS

| Metric | Unit | Min | Max | Actual Min | Actual Max | P95 | Status |
|--------|------|-----|-----|-----------|-----------|-----|--------|
| **Temperature** | °C | -10 | 50 | -2.3 | 43.8 | 31.6 | ✅ Good buffer |
| **Radiation** | W/m² | 0 | 1150 | 0 | 1119 | 826.4 | ✅ 2.8% margin |
| **Wind Speed** | m/s | 0 | 50 | 0 | 42.9 | 23.0 | ✅ 16% margin |
| **PM2.5** | µg/m³ | 0 | 500 | 0 | 44.8 | 8.85 | ✅ Large buffer |
| **UV Index** | - | 0 | 15 | 0 | 14.35 | 7.61 | ✅ Tight fit |

**KẾT LUẬN:** ✅ Tất cả Weather & Air Quality bounds đều có cơ sở từ dữ liệu thực!

---

## 🔍 KẾT LUẬN - CỦA CẢ THẢY TẤT CẢ

### ✅ Các Thresholds Được CHỨNG MINH:

| Layer | Metric | Current | Actual Data | Evidence | Status |
|-------|--------|---------|-------------|----------|--------|
| **Energy** | Energy >= 0 | 0.0 MWh | Min=0, Max=275 | 0 negative values | ✅ |
| **Energy** | PEAK_REFERENCE | 85.0 | P95=185.5 | Too low! | ⚠️ **UPDATE** |
| **Energy** | Night > 1 MWh | 1.0 | Max=0.35 (22h) | 0 violations | ✅ |
| **Energy** | Daytime = 0 | Flag | 2,449 records | 10% of daytime | ✅ |
| **Energy** | Transition 5% (h6) | 5% | Actual=0.6% | Hour 6 = 0.33 MWh | ✅ |
| **Energy** | Transition 8% (h8) | 8% | Actual=65% | Hour 8 = 37.18 MWh | ✅ |
| **Energy** | Transition 10% (h17) | 10% | Actual=48% | Hour 17 = 27.48 MWh | ✅ |
| **Weather** | Temperature | -10 to 50°C | -2.3 to 43.8 | P99=36.3°C | ✅ |
| **Weather** | Radiation | 0 to 1150 W/m² | 0 to 1119 | Max=1119 < 1150 | ✅ |
| **Weather** | Wind Speed | 0 to 50 m/s | 0 to 42.9 | Max=42.9 < 50 | ✅ |
| **Air Quality** | PM2.5 | 0 to 500 µg/m³ | 0 to 44.8 | P99=13.5 | ✅ |
| **Air Quality** | UV Index | 0 to 15 | 0 to 14.35 | Max=14.35 < 15 | ✅ |

### 🚨 CẦN CẬP NHẬT:

```python
# File: src/pv_lakehouse/etl/silver/hourly_energy.py
# Dòng: ~29-30

# HIỆN TẠI (QUAAAA THẤP):
PEAK_REFERENCE_MWH = 85.0

# CẬP NHẬT THEO DỮ LIỆU THỰC:
PEAK_REFERENCE_MWH = 186.0  # P95 from peak hours (10:00-14:00)

# Reasoning:
# - P95 = 185.50 MWh (làm tròn = 186.0)
# - Current 85.0 = P50-60, quá thấp
# - 186.0 phản ánh "normal peak output" tốt hơn
# - Reduces false positives trong transition hour checks
```

### 📚 Tài Liệu Tham Khảo:

**Notebook phân tích:** `/src/pv_lakehouse/etl/notebooks/silver_bounds_analysis.ipynb`
- Cell: Basic Energy Statistics → P95 = 113.35 (overall)
- Cell: Peak Hour Statistics → P95 = 185.50 (peak hours only) ← ĐÂY
- Cell: Anomaly Analysis → 0 negative values, 2,449 daytime zeros
- Cell: Energy by Hour → Transition hour analysis

**Datasets:**
```sql
-- Query: Xác minh PEAK_REFERENCE
SELECT 
    APPROX_PERCENTILE(value, 0.95) as p95_peak
FROM iceberg.bronze.raw_facility_timeseries
WHERE metric = 'energy'
  AND HOUR(interval_ts AT TIME ZONE 'Australia/Sydney') >= 10
  AND HOUR(interval_ts AT TIME ZONE 'Australia/Sydney') <= 14;
  
-- Result: 185.50 MWh
```

---

## ⚠️ LIMITATION CỦA PHÂN TÍCH NÀY

1. **Dữ liệu chỉ từ Jan-Dec 2025**
   - Mùa hè (Jan, Dec) có solar generation cao hơn
   - Mùa đông (Jun-Jul) có solar generation thấp hơn
   - Nên phân tích lại vào tháng 7 (mùa đông)

2. **Thresholds có thể thay đổi theo facility**
   - AVLSF (Solar Farm) khác BOMENSF (Biomass)
   - Facility-level analysis nên được thực hiện

3. **Seasonal Patterns Chưa Được Phân Tích**
   - Peak hours có thể khác giữa mùa (9-15 vs 10-14)
   - Cần per-season analysis

4. **Heuristic Thresholds (5%, 8%, 10%)**
   - Chưa validate với actual equipment failures
   - Có thể điều chỉnh nếu false positive rate cao

---

**Created:** 2025-12-16  
**Last Updated:** 2025-12-16  
**Status:** Ready for Implementation  
**Next Steps:** Update PEAK_REFERENCE_MWH to 186.0 and re-run Silver transforms
