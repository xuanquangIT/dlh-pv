#!/bin/bash
# Query forecast results from Gold layer
# Usage: ./query_forecast.sh [LIMIT]

LIMIT=${1:-100}
COMPOSE_FILE="docker/docker-compose.yml"
TRINO_CONTAINER="trino"

echo "=========================================="
echo "  Query Forecast Results"
echo "=========================================="
echo ""

# Count total forecasts
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Total forecast count:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" exec "$TRINO_CONTAINER" trino \
  --server http://trino:8080 \
  --catalog iceberg \
  --schema gold \
  --execute "SELECT COUNT(*) as total_forecasts FROM fact_energy_forecast"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Latest $LIMIT forecasts:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" exec "$TRINO_CONTAINER" trino \
  --server http://trino:8080 \
  --catalog iceberg \
  --schema gold \
  --execute "
    SELECT 
      fac.facility_code,
      fac.facility_name,
      d.full_date,
      t.hour,
      f.model_version_id,
      ROUND(f.forecast_energy_mwh, 4) as forecast_mwh,
      ROUND(f.forecast_score, 4) as confidence_score,
      f.forecast_timestamp
    FROM fact_energy_forecast f
    LEFT JOIN dim_facility fac ON f.facility_key = fac.facility_key
    LEFT JOIN dim_date d ON f.date_key = d.date_key
    LEFT JOIN dim_time t ON f.time_key = t.time_key
    ORDER BY f.forecast_timestamp DESC
    LIMIT $LIMIT
  "

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Forecast statistics by facility:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" exec "$TRINO_CONTAINER" trino \
  --server http://trino:8080 \
  --catalog iceberg \
  --schema gold \
  --execute "
    SELECT 
      fac.facility_code,
      fac.facility_name,
      COUNT(*) as forecast_count,
      ROUND(AVG(f.forecast_energy_mwh), 3) as avg_forecast_mwh,
      ROUND(STDDEV(f.forecast_energy_mwh), 3) as stddev_mwh,
      ROUND(MIN(f.forecast_energy_mwh), 3) as min_forecast_mwh,
      ROUND(MAX(f.forecast_energy_mwh), 3) as max_forecast_mwh,
      ROUND(AVG(f.forecast_score), 3) as avg_confidence
    FROM fact_energy_forecast f
    LEFT JOIN dim_facility fac ON f.facility_key = fac.facility_key
    GROUP BY fac.facility_code, fac.facility_name
    ORDER BY forecast_count DESC
  "

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Model version information:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" exec "$TRINO_CONTAINER" trino \
  --server http://trino:8080 \
  --catalog iceberg \
  --schema gold \
  --execute "
    SELECT 
      model_version_id,
      model_name,
      model_algorithm,
      is_active,
      performance_metrics,
      created_at
    FROM dim_forecast_model_version
    ORDER BY created_at DESC
  "

echo ""
echo "=========================================="
echo "  ✓ Query Complete"
echo "=========================================="
echo ""
