# Solar Energy Forecast Pipeline

Quy trình hoàn chỉnh từ training model đến lưu trữ kết quả trong Gold layer.

## Tổng quan kiến trúc

```
┌─────────────────┐
│  Silver Layer   │
│  (clean data)   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Training Script                    │
│  train_forecast_model.py            │
│  - Load Silver energy data          │
│  - Train logistic regression        │
│  - Generate predictions             │
│  - Save to S3 Parquet               │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Gold Layer Loaders                 │
│  1. dim_forecast_model_version      │
│  2. fact_energy_forecast            │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────┐
│  Gold Layer     │
│  (star schema)  │
└─────────────────┘
```

## Cấu trúc Gold Tables

### dim_forecast_model_version
Dimension table lưu metadata về các phiên bản model:

```sql
CREATE TABLE lh.gold.dim_forecast_model_version (
    model_version_id VARCHAR,      -- e.g., "lr_baseline_v1"
    model_name VARCHAR,             -- e.g., "logistic_regression"
    model_type VARCHAR,             -- e.g., "classification"
    model_algorithm VARCHAR,        -- e.g., "LogisticRegression"
    train_start_date VARCHAR,
    train_end_date VARCHAR,
    hyperparameters VARCHAR,        -- JSON string
    feature_columns VARCHAR,        -- Comma-separated
    target_column VARCHAR,
    performance_metrics VARCHAR,    -- JSON string
    is_active BOOLEAN,
    model_description VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

### fact_energy_forecast
Fact table chứa predictions:

```sql
CREATE TABLE lh.gold.fact_energy_forecast (
    -- Dimension keys
    facility_key INTEGER,
    date_key INTEGER,
    time_key INTEGER,
    model_version_id VARCHAR,
    
    -- Measures
    forecast_energy_mwh DECIMAL(12,6),
    forecast_score DECIMAL(10,6),
    
    -- Timestamps
    forecast_timestamp TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITIONED BY (date_key);
```

## Cách chạy

### 1. Chạy toàn bộ pipeline (khuyến nghị)

```bash
./run_forecast_pipeline.sh --limit 5000
```

Pipeline này sẽ:
1. Train model trên Silver data (5000 rows)
2. Load dim_forecast_model_version
3. Load fact_energy_forecast
4. Query và hiển thị kết quả

### 2. Chạy từng bước riêng lẻ

#### Bước 1: Training model

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -lc \
  'export AWS_ACCESS_KEY_ID=mlflow_svc \
   AWS_SECRET_ACCESS_KEY=pvlakehouse_mlflow \
   MLFLOW_S3_ENDPOINT_URL=http://minio:9000 \
   AWS_REGION=us-east-1 \
   MLFLOW_TRACKING_URI=http://mlflow:5000; \
   cd /opt/workdir && \
   python3 src/pv_lakehouse/mlflow/train_forecast_model.py --limit 5000'
```

**Output**: Predictions được lưu tại `s3a://lakehouse/silver/energy_forecast_predictions`

#### Bước 2: Load model version dimension

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -lc \
  'cd /opt/workdir && \
   python3 -m pv_lakehouse.etl.gold.cli dim_forecast_model_version --mode full'
```

#### Bước 3: Load forecast fact table

```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -lc \
  'cd /opt/workdir && \
   python3 -m pv_lakehouse.etl.gold.cli fact_energy_forecast --mode full'
```

### 3. Query kết quả

#### Sử dụng script có sẵn:

```bash
./query_forecast.sh 100
```

#### Hoặc query trực tiếp với Trino:

```bash
# Đếm tổng số forecasts
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "SELECT COUNT(*) FROM fact_energy_forecast"

# Xem top 200 forecasts mới nhất
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "SELECT * FROM fact_energy_forecast ORDER BY forecast_timestamp DESC LIMIT 200"

# Join với dimensions để xem chi tiết
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "
    SELECT 
      fac.facility_code,
      fac.facility_name,
      d.full_date,
      t.hour,
      f.model_version_id,
      f.forecast_energy_mwh,
      f.forecast_score,
      f.forecast_timestamp
    FROM fact_energy_forecast f
    LEFT JOIN dim_facility fac ON f.facility_key = fac.facility_key
    LEFT JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_time t ON f.time_key = t.time_key
    ORDER BY f.forecast_timestamp DESC
    LIMIT 100
  "
```

## Chi tiết kỹ thuật

### Training Script: `train_forecast_model.py`

**Đầu vào**: 
- Silver table: `lh.silver.clean_hourly_energy`
- Features: `intervals_count`, `completeness_pct`, `hour_of_day`
- Target: `energy_high_flag` (binary classification dựa trên 60th percentile)

**Model**: 
- Logistic Regression (PySpark MLlib)
- Train/test split: 70/30
- Metrics: AUC, Accuracy

**Đầu ra**:
- Predictions: `s3a://lakehouse/silver/energy_forecast_predictions` (Parquet)
- Sample CSV: `s3a://lakehouse/silver/predictions_sample_100rows.csv`
- MLflow tracking: Metrics, parameters, model artifact

### Gold Loaders

#### `GoldDimForecastModelVersionLoader`
- **Source**: Static data (hardcoded baseline version)
- **Transformation**: Create dimension records with model metadata
- **Output**: `lh.gold.dim_forecast_model_version`
- **Mode**: Full refresh

#### `GoldFactEnergyForecastLoader`
- **Source**: Predictions Parquet from S3
- **Joins**: 
  - `dim_facility` (facility_code)
  - `dim_date` (date from timestamp)
  - `dim_time` (hour from timestamp)
  - `dim_forecast_model_version` (model_version_id)
- **Output**: `lh.gold.fact_energy_forecast`
- **Partitioning**: By `date_key`

## Monitoring & Debugging

### Kiểm tra MLflow
```bash
# Mở MLflow UI
open http://localhost:5000
```

### Kiểm tra predictions trên S3
```bash
docker compose -f docker/docker-compose.yml exec spark-master bash -lc \
  'cd /opt/workdir && python3 -c "
from pyspark.sql import SparkSession
from pv_lakehouse.etl.utils.spark_utils import create_spark_session
spark = create_spark_session(\"check-predictions\")
df = spark.read.parquet(\"s3a://lakehouse/silver/energy_forecast_predictions\")
print(f\"Total predictions: {df.count()}\")
df.show(10)
"'
```

### Xem logs
```bash
# Training logs
docker compose -f docker/docker-compose.yml logs spark-master

# Gold loader logs
docker compose -f docker/docker-compose.yml logs spark-master | grep GOLD
```

## Tích hợp với BI Tools

Sau khi có dữ liệu trong Gold layer, có thể kết nối Power BI/Tableau với Trino:

```
Host: localhost
Port: 8080
Catalog: iceberg
Schema: gold
Tables: fact_energy_forecast, dim_forecast_model_version
```

Tham khảo: `doc/power-bi/TRINO_CONNECTION_GUIDE.md`

## Troubleshooting

### Lỗi: "Table not found"
- Chạy lại Silver loaders trước: `python -m pv_lakehouse.etl.silver.cli hourly_energy`
- Kiểm tra Iceberg namespace: `SHOW SCHEMAS FROM iceberg`

### Lỗi: "No predictions found"
- Training script chưa chạy hoặc failed
- Kiểm tra S3 path: `s3a://lakehouse/silver/energy_forecast_predictions`

### Lỗi: "Dimension table missing"
- Load dimensions trước khi load facts:
  ```bash
  python -m pv_lakehouse.etl.gold.cli dim_facility --mode full
  python -m pv_lakehouse.etl.gold.cli dim_date --mode full
  python -m pv_lakehouse.etl.gold.cli dim_time --mode full
  ```

## Files tạo ra

```
src/pv_lakehouse/
├── mlflow/
│   └── train_forecast_model.py          # Training script
├── etl/gold/
│   ├── dim_forecast_model_version.py    # Model version dimension loader
│   ├── fact_energy_forecast.py          # Forecast fact loader
│   └── cli.py                           # Updated registry

# Pipeline automation scripts
run_forecast_pipeline.sh                 # End-to-end pipeline
query_forecast.sh                        # Query forecast results

# Documentation
FORECAST_PIPELINE.md                     # This file
```
