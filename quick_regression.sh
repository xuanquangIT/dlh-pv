#!/bin/bash

# Quick Regression Pipeline
# Runs GBT regression model training and loads predictions to Gold layer

set -e

echo "=========================================="
echo "🚀 REGRESSION MODEL PIPELINE"
echo "=========================================="
echo ""

# Step 1: Train GBT Regression Model
# (Training script tự động ghi predictions vào Gold layer)
echo "📊 Step 1/3: Training GBT Regression Model..."
echo "ℹ️  Using ALL data (no limit) for production training"
echo "ℹ️  Predictions will be saved directly to Gold layer"
echo "----------------------------------------"
docker cp /home/pvlakehouse/work/dlh-pv/src/pv_lakehouse/mlflow/train_regression_model.py spark-master:/opt/spark/work-dir/train_regression_model.py
docker exec -it spark-master spark-submit --master spark://spark-master:7077 train_regression_model.py --limit 0 

if [ $? -eq 0 ]; then
    echo "✅ Model training completed & predictions saved to Gold"
else
    echo "❌ Model training failed"
    exit 1
fi

echo ""
echo "----------------------------------------"
echo ""

# Step 2: Query Prediction Results
echo "📊 Step 2/3: Querying prediction results from Gold table..."
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
echo "----------------------------------------"
echo ""

# Step 3: Query Feature Importance
echo "📊 Step 3/3: Top 15 Most Important Features..."
echo "----------------------------------------"

echo ""
echo "🔝 Feature Importance Rankings:"
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "
    SELECT 
        rank_overall as rank,
        feature_name,
        feature_category as category,
        ROUND(importance_percentage, 2) as importance_pct
    FROM dim_feature_importance
    WHERE is_top_15 = true
    ORDER BY rank_overall
  "

echo ""
echo "📊 Feature Importance by Category:"
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "
    SELECT 
        feature_category as category,
        COUNT(*) as total_features,
        COUNT(*) FILTER (WHERE is_top_15 = true) as in_top_15,
        ROUND(SUM(importance_percentage), 2) as total_importance_pct
    FROM dim_feature_importance
    GROUP BY feature_category
    ORDER BY total_importance_pct DESC
  "

echo ""
echo "=========================================="
echo "✅ REGRESSION PIPELINE COMPLETED"
echo "=========================================="
echo ""
echo "📋 Summary:"
echo "  - Model: GBTRegressor (maxIter=120, depth=6, stepSize=0.1)"
echo "  - Features: 27 (incl. LAG, Non-linear, Interaction features)"
echo "  - Training Data: ALL (~61,766 rows after filters: energy >= 5 MWh, sunlight hours)"
echo "  - Performance: Test MAE ~10.66 MWh, R² ~85%, Poor predictions ~45%"
echo "  - Gold Table: lh.gold.fact_solar_forecast_regression"
echo "  - MLflow UI: http://localhost:5002"
echo ""
echo "🔍 Kiểm tra kết quả:"
echo "  ./check_regression_results.sh"
echo ""
