#!/bin/bash
# Kiểm tra kết quả Regression Model
set -e

echo "🔍 KIỂM TRA KẾT QUẢ REGRESSION"
echo ""

# 1. Gold metrics tổng quan
echo "📊 Performance Metrics:"
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "
    SELECT 
        COUNT(*) as total_predictions,
        COUNT(DISTINCT facility_key) as facilities,
        ROUND(AVG(absolute_percentage_error), 2) as avg_mape,
        ROUND(AVG(mae_metric), 2) as avg_mae,
        ROUND(AVG(r2_score), 4) as avg_r2
    FROM fact_solar_forecast_regression
  "

echo ""
echo "📈 Top 10 Best Predictions:"
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
    ORDER BY absolute_percentage_error ASC LIMIT 10
  "

echo ""
echo "🏭 Performance by Facility:"
docker compose -f docker/docker-compose.yml exec trino trino \
  --server http://trino:8080 --catalog iceberg --schema gold \
  --execute "
    SELECT 
        facility_key,
        COUNT(*) as predictions,
        ROUND(AVG(absolute_percentage_error), 2) as avg_mape,
        ROUND(AVG(mae_metric), 2) as avg_mae
    FROM fact_solar_forecast_regression
    WHERE actual_energy_mwh > 1
    GROUP BY facility_key
    ORDER BY avg_mape ASC
  "

echo ""
echo "✅ Done | MLflow: http://localhost:5002"
