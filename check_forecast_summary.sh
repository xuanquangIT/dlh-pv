#!/bin/bash

# Quick summary of forecast data
# Usage: ./check_forecast_summary.sh

set -e

TRINO_CMD="docker compose -f docker/docker-compose.yml exec trino trino --server http://trino:8080 --catalog iceberg --schema gold"

echo "=========================================="
echo "FORECAST DATA SUMMARY"
echo "=========================================="
echo ""

echo "📊 Total Forecasts:"
$TRINO_CMD --execute "SELECT COUNT(*) as total FROM fact_energy_forecast" 2>/dev/null
echo ""

echo "📅 Date Range:"
$TRINO_CMD --execute "
SELECT 
    MIN(d.full_date) as earliest_date,
    MAX(d.full_date) as latest_date,
    COUNT(DISTINCT d.full_date) as total_days
FROM fact_energy_forecast f
LEFT JOIN dim_date d ON f.date_key = d.date_key
" 2>/dev/null
echo ""

echo "🏭 Facilities:"
$TRINO_CMD --execute "
SELECT 
    COUNT(DISTINCT facility_key) as total_facilities,
    COUNT(*) / COUNT(DISTINCT facility_key) as avg_forecasts_per_facility
FROM fact_energy_forecast
" 2>/dev/null
echo ""

echo "🤖 Models:"
$TRINO_CMD --execute "
SELECT 
    model_version_id,
    COUNT(*) as forecast_count,
    ROUND(AVG(forecast_energy_mwh), 2) as avg_forecast_mwh
FROM fact_energy_forecast
GROUP BY model_version_id
" 2>/dev/null
echo ""

echo "⚠️  Data Quality Issues:"
$TRINO_CMD --execute "
SELECT 
    SUM(CASE WHEN forecast_energy_mwh IS NULL THEN 1 ELSE 0 END) as null_forecasts,
    SUM(CASE WHEN forecast_energy_mwh < 0 THEN 1 ELSE 0 END) as negative_forecasts,
    SUM(CASE WHEN forecast_score IS NULL THEN 1 ELSE 0 END) as null_scores
FROM fact_energy_forecast
" 2>/dev/null
echo ""
