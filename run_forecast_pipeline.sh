#!/bin/bash
# End-to-end pipeline: Train model → Load to Gold → Query results
# Usage: ./run_forecast_pipeline.sh [--limit ROWS]

set -e  # Exit on any error

LIMIT=${1:-5000}
if [[ "$1" == "--limit" ]]; then
    LIMIT=${2:-5000}
fi

COMPOSE_FILE="docker/docker-compose.yml"
SPARK_CONTAINER="spark-master"
TRINO_CONTAINER="trino"

echo "=========================================="
echo "  Solar Energy Forecast Pipeline"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  - Sample limit: $LIMIT rows"
echo "  - Compose file: $COMPOSE_FILE"
echo ""

# Step 1: Train the model
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 1: Training forecast model..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" exec "$SPARK_CONTAINER" bash -lc \
  "export AWS_ACCESS_KEY_ID=mlflow_svc \
   AWS_SECRET_ACCESS_KEY=pvlakehouse_mlflow \
   MLFLOW_S3_ENDPOINT_URL=http://minio:9000 \
   AWS_REGION=us-east-1 \
   MLFLOW_TRACKING_URI=http://mlflow:5000; \
   cd /opt/workdir && \
   python3 src/pv_lakehouse/mlflow/train_forecast_model.py --limit $LIMIT"

if [ $? -ne 0 ]; then
    echo "✗ Training failed!"
    exit 1
fi
echo "✓ Training complete"
echo ""

# Step 2: Load model version dimension
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 2: Loading dim_forecast_model_version..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" exec "$SPARK_CONTAINER" bash -lc \
  "cd /opt/workdir && \
   python3 -m pv_lakehouse.etl.gold.cli dim_forecast_model_version --mode full"

if [ $? -ne 0 ]; then
    echo "✗ Dimension load failed!"
    exit 1
fi
echo "✓ Model version dimension loaded"
echo ""

# Step 3: Load forecast fact table
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 3: Loading fact_energy_forecast..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" exec "$SPARK_CONTAINER" bash -lc \
  "cd /opt/workdir && \
   python3 -m pv_lakehouse.etl.gold.cli fact_energy_forecast --mode full"

if [ $? -ne 0 ]; then
    echo "✗ Fact table load failed!"
    exit 1
fi
echo "✓ Forecast fact table loaded"
echo ""

# Step 4: Query results
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 4: Querying results from Gold..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo "--- Row count ---"
docker compose -f "$COMPOSE_FILE" exec "$TRINO_CONTAINER" trino \
  --server http://trino:8080 \
  --catalog iceberg \
  --schema gold \
  --execute "SELECT COUNT(*) as forecast_count FROM fact_energy_forecast"

echo ""
echo "--- Sample forecasts (top 10) ---"
docker compose -f "$COMPOSE_FILE" exec "$TRINO_CONTAINER" trino \
  --server http://trino:8080 \
  --catalog iceberg \
  --schema gold \
  --execute "
    SELECT 
      facility_key,
      date_key,
      time_key,
      model_version_id,
      ROUND(forecast_energy_mwh, 3) as forecast_mwh,
      ROUND(forecast_score, 3) as score,
      forecast_timestamp
    FROM fact_energy_forecast
    ORDER BY forecast_timestamp DESC
    LIMIT 10
  "

echo ""
echo "--- Forecast summary by facility ---"
docker compose -f "$COMPOSE_FILE" exec "$TRINO_CONTAINER" trino \
  --server http://trino:8080 \
  --catalog iceberg \
  --schema gold \
  --execute "
    SELECT 
      f.facility_key,
      fac.facility_name,
      COUNT(*) as forecast_count,
      ROUND(AVG(f.forecast_energy_mwh), 3) as avg_forecast_mwh,
      ROUND(MIN(f.forecast_energy_mwh), 3) as min_forecast_mwh,
      ROUND(MAX(f.forecast_energy_mwh), 3) as max_forecast_mwh
    FROM fact_energy_forecast f
    LEFT JOIN dim_facility fac ON f.facility_key = fac.facility_key
    GROUP BY f.facility_key, fac.facility_name
    ORDER BY forecast_count DESC
  "

echo ""
echo "=========================================="
echo "  ✓ Pipeline Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  - View MLflow UI: http://localhost:5000"
echo "  - Query Trino: docker compose -f docker/docker-compose.yml exec trino trino --server http://trino:8080 --catalog iceberg --schema gold"
echo "  - Explore predictions: SELECT * FROM fact_energy_forecast LIMIT 100"
echo ""
