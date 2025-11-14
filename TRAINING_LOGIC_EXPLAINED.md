# Logic Train Pipeline - Chi tiết từ Silver đến Gold

## 📊 TỔNG QUAN LUỒNG DỮ LIỆU

```
SILVER → Feature Engineering → Training → Predictions → GOLD
```

---

## BƯỚC 1: ĐẦU VÀO TỪ SILVER LAYER

### 1.1. Nguồn dữ liệu
```python
# Đọc từ bảng Silver
silver_df = spark.table("lh.silver.clean_hourly_energy")

# Chọn các cột cơ bản
filtered = silver_df.select(
    "facility_code",           # Mã nhà máy (BNGSF1, CLRSF1, ...)
    "date_hour",               # Timestamp giờ (2024-01-01 00:00:00)
    "energy_mwh",              # Năng lượng thực tế (MWh)
    "intervals_count",         # Số interval có data (0-12)
    "completeness_pct",        # % data đầy đủ (0-100)
)
```

**Ý nghĩa các cột:**
- **energy_mwh**: Năng lượng thực tế phát ra trong 1 giờ (target để dự đoán)
- **intervals_count**: Số lần đo trong 1 giờ (mỗi giờ có 12 interval 5 phút)
- **completeness_pct**: Độ tin cậy của dữ liệu (100% = đủ 12 intervals)

### 1.2. Feature Engineering
```python
# Tạo feature mới: giờ trong ngày
.withColumn("hour_of_day", F.hour("date_hour").cast("double"))
```

**Giải thích:**
- Extract giờ từ timestamp: `2024-01-01 14:30:00` → `14.0`
- Quan trọng vì năng lượng mặt trời phụ thuộc giờ (12h nhiều hơn 18h)

---

## BƯỚC 2: TẠO NHÃN (LABEL) CHO BÀI TOÁN CLASSIFICATION

### 2.1. Tính ngưỡng phân loại
```python
# Tính percentile 60% của energy_mwh
quantiles = subset.approxQuantile("energy_mwh", [0.6], 0.05)
threshold = float(quantiles[0])  # VD: threshold = 5.707 MWh
```

**Ý nghĩa:**
- Lấy giá trị ở vị trí 60% trong phân phối năng lượng
- VD: Nếu 60% giờ có energy < 5.707 MWh → threshold = 5.707
- **Mục đích**: Phân loại giờ "năng lượng cao" vs "năng lượng thấp"

### 2.2. Gắn nhãn
```python
labelled = subset.withColumn(
    "energy_high_flag",  # LABEL_COLUMN
    F.when(F.col("energy_mwh") >= threshold, 1.0)  # Cao = 1
     .otherwise(0.0)                                 # Thấp = 0
)
```

**Kết quả:**

| energy_mwh | threshold | energy_high_flag |
|------------|-----------|------------------|
| 12.5       | 5.707     | 1.0 (cao)        |
| 3.2        | 5.707     | 0.0 (thấp)       |
| 8.9        | 5.707     | 1.0 (cao)        |

---

## BƯỚC 3: CHUẨN BỊ FEATURES CHO TRAINING

### 3.1. Chọn feature columns
```python
FEATURE_COLUMNS = [
    "intervals_count",    # Feature 1: Số lần đo (0-12)
    "completeness_pct",   # Feature 2: % hoàn thiện (0-100)
    "hour_of_day"         # Feature 3: Giờ trong ngày (0-23)
]
```

### 3.2. Tạo feature vector
```python
assembler = VectorAssembler(
    inputCols=FEATURE_COLUMNS, 
    outputCol="features"
)
# Input:  intervals_count=12, completeness_pct=100, hour_of_day=14
# Output: features = [12.0, 100.0, 14.0]  (dense vector)
```

---

## BƯỚC 4: TRAINING MODEL

### 4.1. Chia train/test
```python
train_df, test_df = dataset.randomSplit([0.7, 0.3], seed=42)
# 70% dùng train, 30% dùng test
```

### 4.2. Định nghĩa model
```python
lr = LogisticRegression(
    labelCol="energy_high_flag",  # Dự đoán: cao (1) hay thấp (0)
    featuresCol="features",       # Input: [intervals_count, completeness_pct, hour_of_day]
    maxIter=100                   # Số vòng lặp tối đa
)
```

**Logistic Regression học:**
```
P(energy_high=1) = sigmoid(w1*intervals_count + w2*completeness_pct + w3*hour_of_day + bias)
```

### 4.3. Pipeline và training
```python
pipeline = Pipeline(stages=[assembler, lr])
model = pipeline.fit(train_df)  # Train trên 70% data
```

---

## BƯỚC 5: DỰ ĐOÁN (PREDICTIONS)

### 5.1. Chạy model trên test set
```python
predictions = model.transform(test_df)
```

**Output columns tự động thêm:**

| Column         | Ý nghĩa                                      | VD giá trị         |
|----------------|----------------------------------------------|--------------------|
| **prediction** | Nhãn dự đoán (0=thấp, 1=cao)                | 1.0                |
| **probability**| Vector xác suất [P(thấp), P(cao)]            | [0.35, 0.65]       |
| **rawPrediction** | Logits trước sigmoid                      | [-0.62, 0.62]      |

### 5.2. Extract probability
```python
prob_udf = F.udf(lambda v: float(v[1]) if v else None, DoubleType())
enriched = predictions.withColumn("prob_positive", prob_udf(F.col("probability")))
```

**Giải thích:**
- `probability = [0.35, 0.65]` → lấy phần tử thứ 2 (index=1) → `0.65`
- **prob_positive = 0.65** = Xác suất model tin rằng đây là giờ "năng lượng cao"

---

## BƯỚC 6: TẠO CÁC CỘT GOLD FACT

### 6.1. Keys và Metadata
```python
.withColumn("forecast_id", F.row_number().over(window))  # ID duy nhất
.withColumn("date_key", F.date_format("date_hour", "yyyyMMdd").cast("int"))  # 20240101
.withColumn("time_key", (F.hour("date_hour")*100 + F.minute("date_hour")).cast("int"))  # 1430
.withColumn("model_version_key", F.lit(model_version_key))  # FK → dim_model_version
.withColumn("weather_condition_key", F.lit(None).cast("int"))  # Chưa có weather
```

### 6.2. Actual vs Predicted Energy
```python
.withColumn("actual_energy_mwh", F.col("energy_mwh"))  # Năng lượng thực tế

# Dự đoán năng lượng = xác suất * năng lượng thực tế
.withColumn("predicted_energy_mwh", 
    (F.col("prob_positive") * F.col("energy_mwh")).cast("double")
)
```

**Ví dụ cụ thể:**

| actual_energy_mwh | prob_positive | predicted_energy_mwh | Giải thích                          |
|-------------------|---------------|----------------------|-------------------------------------|
| 12.5              | 0.88          | 11.0                 | Model tin 88% là cao → dự đoán 11   |
| 3.2               | 0.25          | 0.8                  | Model tin 25% là cao → dự đoán 0.8  |

**Logic:**
- Nếu model tin "cao" (prob_positive cao) → predicted gần bằng actual
- Nếu model tin "thấp" (prob_positive thấp) → predicted giảm xuống

### 6.3. Metrics đánh giá
```python
# Sai số tuyệt đối
.withColumn("forecast_error_mwh", 
    F.col("actual_energy_mwh") - F.col("predicted_energy_mwh")
)

# % sai số tuyệt đối (MAPE)
.withColumn("absolute_percentage_error",
    F.when(F.col("actual_energy_mwh") == 0, 0.0)
     .otherwise(F.abs(F.col("forecast_error_mwh") / F.col("actual_energy_mwh")) * 100.0)
)

# MAE (Mean Absolute Error)
.withColumn("mae_metric", F.abs(F.col("forecast_error_mwh")))

# RMSE placeholder (thường tính aggregate)
.withColumn("rmse_metric", F.abs(F.col("forecast_error_mwh")))

# R² placeholder
.withColumn("r2_score", F.lit(1.0))
```

**Ví dụ tính toán:**

| actual | predicted | error | abs % error | mae  |
|--------|-----------|-------|-------------|------|
| 12.5   | 11.0      | 1.5   | 12%         | 1.5  |
| 3.2    | 0.8       | 2.4   | 75%         | 2.4  |

### 6.4. Join với Dimension
```python
facility_dim = spark.table("lh.gold.dim_facility").select("facility_code", "facility_key")
enriched = enriched.join(facility_dim, on="facility_code", how="left")
```

**Kết quả:**

| facility_code | facility_key | Giải thích              |
|---------------|--------------|-------------------------|
| BNGSF1        | 1            | FK → dim_facility       |
| CLRSF1        | 2            | FK → dim_facility       |

---

## BƯỚC 7: SCHEMA CUỐI CÙNG - GOLD FACT TABLE

### 7.1. Columns được select
```python
gold_df = enriched.select(
    "forecast_id",                  # PK: ID duy nhất cho mỗi forecast
    "date_key",                     # FK: dim_date (20240101)
    "time_key",                     # FK: dim_time (1430)
    "facility_key",                 # FK: dim_facility (1, 2, ...)
    "weather_condition_key",        # FK: dim_weather (NULL nếu chưa có)
    "model_version_key",            # FK: dim_model_version (1)
    "actual_energy_mwh",            # Năng lượng thực tế (từ Silver)
    "predicted_energy_mwh",         # Năng lượng dự đoán (prob * actual)
    "forecast_error_mwh",           # Sai số (actual - predicted)
    "absolute_percentage_error",    # % sai số (MAPE)
    "mae_metric",                   # Mean Absolute Error
    "rmse_metric",                  # Root Mean Squared Error
    "r2_score",                     # R² coefficient (placeholder)
    "forecast_timestamp",           # Thời gian forecast (= date_hour)
    "created_at",                   # Audit: thời gian tạo record
)
```

---

## 🎯 TÓM TẮT LUỒNG DỮ LIỆU

```
┌─────────────────────────────────────────────────────────────────┐
│ SILVER INPUT (lh.silver.clean_hourly_energy)                    │
├─────────────────────────────────────────────────────────────────┤
│ facility_code | date_hour           | energy_mwh | intervals  │
│ BNGSF1        | 2024-01-01 14:00:00 | 12.5       | 12         │
│ BNGSF1        | 2024-01-01 15:00:00 | 3.2        | 11         │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ FEATURE ENGINEERING                                              │
├─────────────────────────────────────────────────────────────────┤
│ + hour_of_day = 14.0, 15.0                                      │
│ + energy_high_flag = 1 if energy_mwh >= threshold else 0       │
│ Features: [intervals_count, completeness_pct, hour_of_day]     │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ TRAINING (Logistic Regression)                                   │
├─────────────────────────────────────────────────────────────────┤
│ Input:  features = [12, 100, 14]                                │
│ Label:  energy_high_flag = 1                                    │
│ Learn:  weights [w1, w2, w3] to predict probability             │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ PREDICTIONS                                                      │
├─────────────────────────────────────────────────────────────────┤
│ prediction = 1.0  (model dự đoán: cao)                          │
│ probability = [0.12, 0.88]  → prob_positive = 0.88              │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ GOLD FACT ENRICHMENT                                             │
├─────────────────────────────────────────────────────────────────┤
│ actual_energy_mwh = 12.5                                        │
│ predicted_energy_mwh = 0.88 * 12.5 = 11.0                       │
│ forecast_error_mwh = 12.5 - 11.0 = 1.5                          │
│ absolute_percentage_error = |1.5/12.5| * 100 = 12%             │
│ + Join dim_facility → facility_key = 1                          │
│ + Join dim_date → date_key = 20240101                           │
│ + Join dim_time → time_key = 1400                               │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ GOLD OUTPUT (lh.gold.fact_solar_forecast)                       │
├─────────────────────────────────────────────────────────────────┤
│ forecast_id=1 | date_key=20240101 | time_key=1400 | ...        │
│ facility_key=1 | actual=12.5 | predicted=11.0 | error=1.5      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📌 ĐIỂM QUAN TRỌNG

### 1. Bài toán Classification chuyển thành Regression
- **Train**: Dự đoán xác suất "năng lượng cao" (0-1)
- **Output**: Nhân xác suất với actual → predicted energy (MWh)

### 2. Tại sao nhân `prob_positive * energy_mwh`?
- Nếu model tin 88% là "cao" → dự đoán năng lượng = 88% của actual
- Đây là cách đơn giản để chuyển classification probability thành regression forecast

### 3. Metrics đánh giá forecast
- **MAE**: Trung bình sai số tuyệt đối
- **MAPE**: % sai số tuyệt đối
- **RMSE**: Root mean squared error (thường tính aggregate)

### 4. Dimension joins
- Facility, Date, Time → tạo Star Schema chuẩn trong Data Warehouse
- Giúp query dễ dàng: "forecasts của facility X trong tháng Y"

---

## 🔄 LUỒNG THỰC HIỆN

1. **Silver → Features**: Extract `hour_of_day`, tính `energy_high_flag`
2. **Features → Training**: Logistic Regression học weights
3. **Training → Predictions**: Model output `probability` vector
4. **Predictions → Gold**: Extract `prob_positive`, tính `predicted_energy_mwh`, join dimensions
5. **Gold → Analytics**: Query từ Trino với Star Schema

---

## 📊 KẾT QUẢ CUỐI CÙNG

Sau khi chạy pipeline, bạn có thể query:

```sql
SELECT 
    fac.facility_name,
    d.full_date,
    t.hour,
    f.actual_energy_mwh,
    f.predicted_energy_mwh,
    f.forecast_error_mwh,
    f.absolute_percentage_error
FROM lh.gold.fact_solar_forecast f
LEFT JOIN lh.gold.dim_facility fac ON f.facility_key = fac.facility_key
LEFT JOIN lh.gold.dim_date d ON f.date_key = d.date_key
LEFT JOIN lh.gold.dim_time t ON f.time_key = t.time_key
ORDER BY d.full_date DESC, t.hour
LIMIT 20;
```

**Output mẫu:**

| facility_name | full_date  | hour | actual | predicted | error | % error |
|---------------|------------|------|--------|-----------|-------|---------|
| Bungala One   | 2024-11-10 | 14   | 12.5   | 11.0      | 1.5   | 12%     |
| Clare         | 2024-11-10 | 14   | 8.3    | 7.9       | 0.4   | 5%      |
| Nyngan        | 2024-11-10 | 0    | 0.0    | 0.0       | 0.0   | 0%      |
