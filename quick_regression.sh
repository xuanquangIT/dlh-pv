#!/bin/bash

# Quick Regression Pipeline
# Runs RandomForest regression model training and loads predictions to Gold layer

set -e

echo "=========================================="
echo "🚀 REGRESSION MODEL PIPELINE"
echo "=========================================="
echo ""

# Step 1: Train RandomForest Regression Model
# (Training script tự động ghi predictions vào Gold layer)
echo "📊 Step 1/2: Training RandomForest Regression Model..."
echo "ℹ️  Using ALL data (no limit) for production training"
echo "ℹ️  Predictions will be saved directly to Gold layer"
echo "----------------------------------------"
docker compose -f docker/docker-compose.yml exec spark-master bash -lc '
  export AWS_ACCESS_KEY_ID=mlflow_svc AWS_SECRET_ACCESS_KEY=pvlakehouse_mlflow MLFLOW_S3_ENDPOINT_URL=http://minio:9000 AWS_REGION=us-east-1;
  cd /opt/workdir && python3 src/pv_lakehouse/mlflow/train_regression_model.py --limit 0
'

if [ $? -eq 0 ]; then
    echo "✅ Model training completed & predictions saved to Gold"
else
    echo "❌ Model training failed"
    exit 1
fi

echo ""
echo "----------------------------------------"
echo ""

# Step 2: Query Results
echo "📊 Step 2/2: Querying results from Gold table..."
echo "----------------------------------------"

echo ""
echo "1️⃣ Total predictions:"
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "
    SELECT 
        COUNT(*) as total_predictions,
        COUNT(DISTINCT facility_key) as facilities,
        ROUND(AVG(mae_metric), 2) as avg_mae,
        ROUND(AVG(r2_score), 4) as avg_r2,
        MIN(forecast_timestamp) as earliest,
        MAX(forecast_timestamp) as latest
    FROM fact_solar_forecast_regression
  "

echo ""
echo "2️⃣ Top 10 best predictions:"
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "
    SELECT 
        facility_key,
        CAST(forecast_timestamp AS VARCHAR) as time,
        ROUND(actual_energy_mwh, 2) as actual,
        ROUND(predicted_energy_mwh, 2) as predicted,
        ROUND(absolute_percentage_error, 2) as mape
    FROM fact_solar_forecast_regression
    WHERE actual_energy_mwh > 1
    ORDER BY absolute_percentage_error ASC
    LIMIT 10
  "

echo ""
echo "3️⃣ Predictions per facility:"
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "
    SELECT 
        facility_key,
        COUNT(*) as predictions,
        ROUND(AVG(mae_metric), 2) as avg_mae,
        ROUND(AVG(r2_score), 4) as avg_r2
    FROM fact_solar_forecast_regression
    GROUP BY facility_key
    ORDER BY facility_key
  "

echo ""
echo "=========================================="
echo "✅ REGRESSION PIPELINE COMPLETED"
echo "=========================================="
echo ""
echo "📋 Summary:"
echo "  - Model: RandomForestRegressor (25 trees, depth 10)"
echo "  - Features: 22 (incl. 3 LAG features)"
echo "  - Training Data: ALL available rows (no limit)"
echo "  - Gold Table: lh.gold.fact_solar_forecast_regression"
echo "  - MLflow UI: http://localhost:5002"
echo ""
echo "🔍 Kiểm tra kết quả:"
echo "  ./check_regression_results.sh"
echo ""
