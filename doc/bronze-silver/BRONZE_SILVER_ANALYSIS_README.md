# Bronze to Silver Data Analysis Notebook

## 📋 Overview

Notebook phân tích chi tiết quá trình chuyển đổi dữ liệu từ **Bronze** (dữ liệu thô từ API) sang **Silver** (dữ liệu sạch và được xác thực).

**Vị trí:** `/src/pv_lakehouse/etl/notebooks/bronze_silver_analysis.ipynb`

## 🎯 Mục Đích Chính

1. **Hiểu vấn đề chất lượng dữ liệu** trong lớp Bronze
2. **Khám phá validation rules & bounds** áp dụng ở Silver
3. **Học cách làm sạch và filter** dữ liệu
4. **Hiểu tại sao** cần các quy tắc này

## 📊 Nội Dung Notebook

### Phần 1: Load Bronze Data Sources
- Tải dữ liệu từ 3 bảng Bronze chính:
  - `lh.bronze.raw_facility_weather` (từ Open-Meteo API)
  - `lh.bronze.raw_facility_air_quality` (từ Open-Meteo API)
  - `lh.bronze.raw_facility_timeseries` (từ OpenElectricity API)
- Hiển thị schema, row counts, sample records

### Phần 2: Explore Data Quality Issues
- **Weather**: NaN values, radiation anomalies, temperature extremes, night-time spikes
- **Energy**: Negative values, night-time anomalies, daytime zeros, peak hour issues
- **Air Quality**: Missing values, bounds violations, AQI anomalies

### Phần 3: Silver Layer Validation Rules
#### Weather Bounds:
```python
shortwave_radiation: [0.0, 1150.0] W/m²
temperature_2m: [-10.0, 50.0] °C
wind_speed_10m: [0.0, 50.0] m/s
cloud_cover: [0.0, 100.0] %
... (11 metrics)
```

**Quality Flags:**
- **REJECT**: Hard bounds violations (impossible values)
- **CAUTION**: Logical inconsistencies (radiation sum > total, etc.)
- **GOOD**: All validations pass

#### Energy Bounds:
- **Min**: 0.0 MWh (non-negative)
- **Logical checks**: Night anomalies, daytime zeros, peak hour issues
- **Transition hours**: Gradual ramp-up/down detection

#### Air Quality Bounds:
- PM2.5, PM10, pollutants: [0.0, 500.0] µg/m³
- UV Index: [0.0, 15.0]
- AQI calculated from PM2.5 breakpoints

### Phần 4: Data Cleaning Transformations
1. **Type Casting**: String → Double/Decimal
2. **Handle NaN**: Replace with NULL, forward-fill
3. **Bounds Checking**: Apply min/max validation
4. **Timezone Normalization**: Convert to facility local time
5. **Temporal Aggregation**: Group by hour
6. **Rounding**: 4 decimal places for consistency

### Phần 5: Bound Violations Analysis
- **Weather**: Detailed analysis of bounds violations per column
- **Energy**: Quality flags distribution by facility
- **Air Quality**: Out-of-bounds statistics

### Phần 6: Summary & Decision Logic
- Data flow architecture (Bronze → Silver → Gold)
- Quality metrics summary
- Key insights for forecasting models

### Phần 7: Quick Reference
- Decision tree for bound determination
- Comparison of different threshold choices
- Why specific bounds were selected

## 🔍 Key Insights

### Tại Sao Bounds Cần Thiết?

1. **Phát hiện lỗi đo lường**
   - Negative energy (vật lý không thể)
   - Radiation at night (không có nguồn)

2. **Phát hiện bất thường**
   - High cloud cover + high radiation (mâu thuẫn)
   - Zero energy during peak hours (failure)

3. **Calibration**
   - Bounds dựa trên P99.5 percentile + safety margin
   - Cho phép extreme events thực tế (drought, heat wave, cyclone)

### Tại Sao Specific Values?

**Example: Cloud Cover Threshold = 98% (not 95%)?**
```
95% cloud cover + low radiation: Có thể xảy ra (30% of cases)
→ False positive rate: 30% (quá cao!)

98% cloud cover + low radiation: Hiếm xảy ra (1% of cases)
→ False positive rate: 1% (acceptable!)

Benefit: Reduces false positives by 95% compared to 95% threshold
Trade-off: Might miss 1 in 1000 subtle errors (acceptable for forecasting)
```

**Example: Transition Hour Energy Thresholds**
- Sunrise (06:00-08:00): < 5% of peak reference
- Early morning (08:00-10:00): < 8% of peak reference
- Sunset (17:00-19:00): < 10% of peak reference

**Why?** Solar farms ramp gradually. Sudden drops = equipment issues.

## 📈 Quality Flag Distribution

### Weather
- **GOOD**: ~95-98% (bounds well-calibrated)
- **CAUTION**: ~1-3% (radiation inconsistencies, extreme temps)
- **REJECT**: < 0.1% (rare severe anomalies)

### Energy
- **GOOD**: ~85-95% (mostly valid)
- **CAUTION**: ~5-15% (night anomalies, transition issues)
- **REJECT**: < 0.1% (negative values very rare)

### Air Quality
- **GOOD**: ~98%+ (API is stable)
- **CAUTION**: < 2% (out-of-bounds anomalies)

## 🛠️ Cách Sử Dụng Notebook

1. **Mở notebook** trong VS Code
2. **Run cells sequentially** từ trên xuống
3. **Kiểm tra output** để hiểu vấn đề chất lượng dữ liệu
4. **Tham khảo** validation rules từ Phần 3
5. **Hiểu** tại sao transformations là cần thiết

## 📚 Tham Khảo Code

Silver Loaders implementation:
- `src/pv_lakehouse/etl/silver/hourly_weather.py`
- `src/pv_lakehouse/etl/silver/hourly_energy.py`
- `src/pv_lakehouse/etl/silver/hourly_air_quality.py`

Bounds definitions:
```python
# Weather (line 51-69 in hourly_weather.py)
_numeric_columns = {
    'shortwave_radiation': (0.0, 1150.0),
    'temperature_2m': (-10.0, 50.0),
    ...
}

# Energy (line 29-30 in hourly_energy.py)
ENERGY_LOWER = 0.0
PEAK_REFERENCE_MWH = 85.0
```

## 🎓 Learning Outcomes

Sau khi hoàn thành notebook, bạn sẽ hiểu:

✅ Vấn đề chất lượng dữ liệu phổ biến trong sensor/API data  
✅ Cách thiết kế validation bounds (không quá strict, không quá loose)  
✅ Phân biệt REJECT vs CAUTION flags  
✅ Tại sao cần timezone normalization cho energy data  
✅ Cách aggregation giảm noise  
✅ Impact của quality flags trên ML models  

## 📞 Notes

- Notebook sử dụng **exported data** (CSV files) thay vì live Iceberg tables
- Data từ 2 ngày (2025-11-02, 2025-11-03) cho facility AVLSF
- Có thể mở rộng để phân tích toàn bộ facilities
- Cell outputs có thể lớn - scroll để xem đầy đủ

---

**Created:** November 22, 2025  
**Purpose:** Data quality analysis & validation rules documentation  
**Target Audience:** Data engineers, ML engineers, analysts
