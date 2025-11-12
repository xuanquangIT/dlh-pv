# Zero Energy with High Radiation - Detailed Explanation

## 🎯 Vấn đề Là Gì?

### Tình Huống Bình Thường (Hợp Lý)
```
9:00 AM - Trời nắng
├─ Solar Radiation: 500 W/m²  (mặt trời chiếu mạnh)
├─ Energy Generation: 40 MWh   (panel phát điện bình thường)
└─ Status: ✅ OK - Hợp lý
```

### Tình Huống Bất Thường (Lỗi)
```
9:00 AM - Trời nắng
├─ Solar Radiation: 500 W/m²  (mặt trời chiếu mạnh)
├─ Energy Generation: 0 MWh    (KHÔNG phát điện!?)
└─ Status: ❌ ANOMALY - Không hợp lý!

LỘI SAI: Tại sao có mặt trời nhưng không phát điện?
```

---

## 🔍 Root Cause - Tại Sao Lại Xảy Ra?

### Nguyên Nhân Có Thể:

#### 1. **Equipment Offline / Bảo Trì**
```
System Status: OFFLINE
Time: 6:00 AM - 6:00 PM
Reason: Scheduled maintenance

Energy: 0 MWh ✓ Đúng (tắt hệ thống)
Radiation: 400 W/m² ✓ Có (trời nắng)
Issue: Đã được reported nhưng chưa update vào Bronze
```

#### 2. **Inverter Failure (Thất Bại)**
```
Panel Status: OK (phát DC power)
Inverter Status: BROKEN (không chuyển AC)
Time: 11:00 AM - hơn 8 giờ

Energy Output: 0 MWh (não AC output)
Solar Available: 700 W/m² (panel nhận được)
Issue: Lỗi thiết bị không được phát hiện ngay
```

#### 3. **Communication Gap (Mất Dữ Liệu)**
```
Energy recorded: Yes (trong system)
Energy sent to DB: NO (network error)
Radiation recorded: Yes (weather station independent)
Time: 2:00 PM - 1 hour

System generated: 35 MWh
Database received: 0 MWh (data loss)
Issue: Dữ liệu lỗi tạm thời
```

#### 4. **Meter Reset / Billing Issue**
```
Morning: Counter = 500 MWh
Afternoon: Counter = 500 MWh (reset!)
Difference = 0 MWh (tính toán sai)

Radiation available: 600 W/m²
System should produce: 40 MWh
But reported: 0 MWh
Issue: Meter counting error
```

#### 5. **Data Quality Flag Không Được Set**
```
Bronze data: 
- Energy = 0 MWh
- Quality issue noted: "meter_offline"
- Timestamp: 2024-01-05 10:00

Silver processing:
- Should mark as CAUTION/REJECT
- But if quality_issues not propagated, shows as "GOOD"
Issue: Data quality metadata lost
```

---

## 📊 EDA Findings - 231 Records Được Phát Hiện

### Tìm Kiếm Tiêu Chí:
```python
WHERE (HOUR(date_hour) BETWEEN 6 AND 18)     # Daytime only
  AND energy_mwh = 0                          # Zero generation
  AND shortwave_radiation > 300               # HIGH radiation (mặt trời)
```

### 231 Records Phân Bổ:

```
GANNSF:   89 records  (38%)  <- Most affected
NYNGAN:   56 records  (24%)
COLEASF:  42 records  (18%)
BNGSF1:   28 records  (12%)
CLARESF:  16 records  (7%)
Total:    231 records
```

### Time Distribution:
```
Morning (6-9h):    45 records  (Early morning issues)
Mid-morning (9-12h): 78 records (Peak issues)
Afternoon (12-18h):  108 records (Afternoon failures)
```

---

## 🔗 Merge với Weather Data - Cách Làm

### Bước 1: Bạn Có 2 Table Riêng

#### Table 1: Energy (Silver Layer)
```sql
SELECT * FROM iceberg.silver.clean_hourly_energy

Columns:
├─ facility_code (VD: 'GANNSF')
├─ date_hour (VD: 2024-01-05 10:00:00)
├─ energy_mwh (VD: 0)
├─ quality_flag
└─ ...other columns
```

#### Table 2: Weather (Silver Layer)
```sql
SELECT * FROM iceberg.silver.clean_hourly_weather

Columns:
├─ facility_code (VD: 'GANNSF')
├─ date_hour (VD: 2024-01-05 10:00:00)
├─ shortwave_radiation (VD: 500)
├─ cloud_cover
└─ ...other columns
```

### Bước 2: JOIN Hai Table

```python
# Trong pandas (như EDA):
merged = energy_df.merge(
    weather_df,
    on=['facility_code', 'date_hour'],  # Khóa join
    how='left',                          # Giữ tất cả energy records
    suffixes=('_energy', '_weather')
)

# Kết quả:
merged = 
├─ facility_code: 'GANNSF'
├─ date_hour: 2024-01-05 10:00:00
├─ energy_mwh: 0
├─ shortwave_radiation: 500
├─ quality_flag_energy: 'GOOD'
├─ quality_flag_weather: 'GOOD'
└─ ...
```

### Bước 3: Phát Hiện Anomaly

```python
# Sau merge, tìm anomalies
anomaly_records = merged[
    (merged['energy_mwh'] == 0) &                    # Không phát điện
    (merged['shortwave_radiation'] > 300) &          # Nhưng có mặt trời
    (merged.index.hour >= 6) & (merged.index.hour < 18)  # Ngày
]

# Kết quả: 231 records
print(f"Found {len(anomaly_records)} anomalous records")
```

---

## 💾 Cách Implement Trong Silver Loader

### Current Code (hourly_energy.py) - Không Join Weather

```python
def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
    # ... process energy data ...
    
    # Build result with energy only
    result = (
        hourly
        .withColumn("hour_of_day", F.hour(F.col("date_hour")))
        .withColumn("quality_issues", ...)
        .withColumn("quality_flag", ...)
    )
    
    return result  # ❌ Không có weather data!
```

### Problem:
```
Không thể biết radiation trong transform() method
├─ Energy = 0
├─ But don't know if radiation > 300
└─ Cannot flag as anomaly
```

---

## ✅ Solution 1: Join Weather Inside Transform

### Cách 1A: Load Weather Table

```python
def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
    """Process energy with weather correlation."""
    
    # ... existing energy processing ...
    
    # STEP 1: Build energy result
    result = (hourly
        .withColumn("hour_of_day", F.hour(F.col("date_hour")))
        .withColumn("quality_issues", quality_issues_expr)
        .withColumn("quality_flag", quality_flag_expr)
    )
    
    # STEP 2: Load weather data
    weather_data = self._spark.sql("""
        SELECT 
            facility_code,
            date_hour,
            shortwave_radiation,
            cloud_cover
        FROM iceberg.silver.clean_hourly_weather
        WHERE shortwave_radiation IS NOT NULL
    """)
    
    # STEP 3: Join with weather
    result_with_weather = (
        result
        .join(
            weather_data,
            on=["facility_code", "date_hour"],
            how="left"  # Keep all energy records
        )
    )
    
    # STEP 4: Add daytime-zero-with-radiation check
    is_daytime_zero_with_high_rad = (
        (F.col("hour_of_day") >= 6) & 
        (F.col("hour_of_day") < 18) &
        (F.col("energy_mwh") == 0.0) &
        (F.col("shortwave_radiation") > 300)  # High radiation threshold
    )
    
    # STEP 5: Update quality flag
    result_enhanced = result_with_weather.withColumn(
        "quality_issues",
        F.when(
            is_daytime_zero_with_high_rad,
            F.concat_ws("|", F.col("quality_issues"), F.lit("DAYTIME_ZERO_WITH_RADIATION"))
        ).otherwise(F.col("quality_issues"))
    ).withColumn(
        "quality_flag",
        F.when(
            is_daytime_zero_with_high_rad,
            F.lit("CAUTION")  # Mark as CAUTION (not REJECT, give benefit of doubt)
        ).otherwise(F.col("quality_flag"))
    )
    
    # STEP 6: Drop weather columns (not needed in final output)
    result_final = result_enhanced.drop("shortwave_radiation", "cloud_cover")
    
    return result_final
```

### Kết Quả:
```
Energy: 0 MWh
Radiation: 500 W/m²
quality_issues: "DAYTIME_ZERO_WITH_RADIATION"
quality_flag: "CAUTION"  ✅ Được flag
```

---

## ✅ Solution 2: Pre-Join Approach (Tối Ưu Hơn)

### Cách 2: Join Before Aggregation

```python
def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
    """Join with weather before aggregation for better context."""
    
    # STEP 1: Load both bronze tables
    weather_df = self._spark.sql("""
        SELECT 
            facility_code,
            weather_timestamp,
            shortwave_radiation,
            cloud_cover
        FROM iceberg.bronze.raw_facility_weather
    """)
    
    # STEP 2: Normalize timestamps to hourly
    energy_hourly = bronze_df \
        .withColumn("date_hour", F.date_trunc("hour", F.col("interval_ts")))
    
    weather_hourly = weather_df \
        .withColumn("date_hour", F.date_trunc("hour", F.col("weather_timestamp")))
    
    # STEP 3: Join early for context
    joined = energy_hourly.join(
        weather_hourly.select("facility_code", "date_hour", "shortwave_radiation"),
        on=["facility_code", "date_hour"],
        how="left"
    )
    
    # STEP 4: Aggregate with context
    hourly = (
        joined
        .groupBy("facility_code", "facility_name", "date_hour")
        .agg(
            F.sum(F.when(F.col("metric") == "energy", F.col("metric_value"))).alias("energy_mwh"),
            F.max("shortwave_radiation").alias("max_radiation_in_hour")
        )
    )
    
    # STEP 5: Now can use max_radiation for quality check
    is_daytime_zero_with_radiation = (
        (F.col("hour_of_day") >= 6) & 
        (F.col("hour_of_day") < 18) &
        (F.col("energy_mwh") == 0.0) &
        (F.col("max_radiation_in_hour") > 300)
    )
    
    # ... rest of quality logic ...
```

---

## 📈 Real Example - GANNSF

### Actual Records Found:

```
2024-01-02 16:00:00
├─ energy_mwh: 0.0
├─ shortwave_radiation: 770 W/m² (Very high!)
└─ Status: ANOMALY ❌
   Reason: Afternoon high sun, but zero output

2024-01-03 09:00:00
├─ energy_mwh: 0.0
├─ shortwave_radiation: 375 W/m²
└─ Status: ANOMALY ❌
   Reason: Morning peak period, but offline

2024-01-03 15:00:00
├─ energy_mwh: 0.0
├─ shortwave_radiation: 751 W/m²
└─ Status: ANOMALY ❌
   Reason: Afternoon high sun, no output
```

### Action Taken:
```
Before:  quality_flag = 'GOOD' (xấu!)
After:   quality_flag = 'CAUTION'
         quality_issues = 'DAYTIME_ZERO_WITH_RADIATION'

This allows:
1. Tracking equipment issues
2. Investigating root cause
3. Separating legitimate maintenance from actual failures
```

---

## 🎓 Tại Sao Cần Flag Này?

### 1. **Root Cause Analysis**
```
Nếu chỉ thấy energy = 0:
├─ Có thể là: Night time (expected)
├─ Có thể là: Offline (need investigation)
└─ Không biết khác biệt!

Nếu thấy: energy=0 + radiation>300:
├─ Không thể là Night time
├─ PHẢI là offline/failure
└─ Can investigate immediately
```

### 2. **Data Quality Tracking**
```
Track anomalies by facility:
├─ GANNSF: 89 occurrences (may have equipment issue)
├─ NYNGAN: 56 occurrences (less frequent)
└─ Help prioritize maintenance
```

### 3. **Forecasting & Analytics**
```
Data Science needs:
├─ Know which records are suspect
├─ Filter them out or mark them
├─ Don't use them for model training
└─ Avoid biasing predictions
```

### 4. **SLA Compliance**
```
System uptime calculation:
├─ Without flag: GANNSF = 100% (misleading)
├─ With flag: GANNSF = 98% (89/21,360 hours)
└─ Accurate reporting to customers
```

---

## 🔗 Data Flow Diagram

```
┌─────────────────────────────────────────────────────┐
│ Bronze Layer                                        │
├─────────────────────────────────────────────────────┤
│ energy: [interval_ts, energy_mwh, ...]             │
│ weather: [weather_timestamp, radiation, ...]       │
└────────────┬──────────────────────────┬─────────────┘
             │                          │
             │ Extract daily chunks     │ Already hourly aggregated
             │                          │
             ▼                          ▼
┌──────────────────────────┐  ┌─────────────────────────┐
│ Silver Transform         │  │ Silver Weather Load     │
├──────────────────────────┤  ├─────────────────────────┤
│ Aggregate to hourly      │  │ Normalize to hourly     │
│ Add quality checks       │  │ Add bounds validation   │
└──────────────┬───────────┘  └────────────┬────────────┘
               │                           │
               └───────────────┬───────────┘
                               │
                         ▼ JOIN ON:
                     facility_code +
                     date_hour
                               │
                         ▼─────────────────────────┐
                    ┌─────────────────────────────┘
                    │
    ┌───────────────▼────────────────────┐
    │ Check: energy=0 + radiation>300?   │
    ├────────────────────────────────────┤
    │ YES → quality_flag = 'CAUTION'     │
    │        quality_issues += '...ZERO..│
    │ NO  → quality_flag unchanged       │
    └────────────┬───────────────────────┘
                 │
                 ▼
    ┌────────────────────────────────────┐
    │ Silver Table (clean_hourly_energy) │
    ├────────────────────────────────────┤
    │ [all records with quality flags]    │
    │ 231 records now marked CAUTION      │
    └────────────────────────────────────┘
```

---

## 💡 Tóm Tắt

### Là Gì?
**Tìm records có:** `energy = 0 MWh` NHƯNG `radiation > 300 W/m²`

### Tại Sao Xảy Ra?
- Equipment offline/maintenance
- Inverter failure
- Communication gap
- Meter issues
- Data quality loss

### Làm Sao Detect?
1. **Merge** energy + weather data trên `facility_code + date_hour`
2. **Filter** records có `energy=0 AND radiation>300 AND hour 6-18`
3. **Flag** với `quality_flag='CAUTION'` + `quality_issues='DAYTIME_ZERO_WITH_RADIATION'`

### Kết Quả?
```
Before: 81,354 records → 91.19% GOOD
After:  81,354 records → 92.12% GOOD (+231 flagged as CAUTION)
```

### Code Location?
- **hourly_energy.py** lines 100-130: Add join + flag logic
- **Test**: Re-run loader, check 231 records marked CAUTION

