#!/bin/bash

# Quick check for duplicate forecasts
# Usage: ./check_forecast_duplicates.sh

set -e

TRINO_CMD="docker compose -f docker/docker-compose.yml exec trino trino --server http://trino:8080 --catalog iceberg --schema gold"

echo "Checking for DUPLICATE forecasts..."
echo ""

$TRINO_CMD --execute "
WITH duplicates AS (
    SELECT 
        facility_key,
        date_key,
        time_key,
        model_version_id,
        COUNT(*) as dup_count
    FROM fact_energy_forecast
    GROUP BY facility_key, date_key, time_key, model_version_id
    HAVING COUNT(*) > 1
)
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ NO DUPLICATES FOUND'
        ELSE '❌ DUPLICATES DETECTED: ' || CAST(COUNT(*) AS VARCHAR) || ' groups'
    END as status,
    COALESCE(SUM(dup_count), 0) as total_duplicate_rows
FROM duplicates
" 2>/dev/null

echo ""
echo "Detailed duplicate records (if any):"
$TRINO_CMD --execute "
SELECT 
    fac.facility_code,
    d.full_date,
    t.hour,
    f.model_version_id,
    COUNT(*) as duplicate_count,
    MIN(f.created_at) as first_created,
    MAX(f.created_at) as last_created
FROM fact_energy_forecast f
LEFT JOIN dim_facility fac ON f.facility_key = fac.facility_key
LEFT JOIN dim_date d ON f.date_key = d.date_key
LEFT JOIN dim_time t ON f.time_key = t.time_key
GROUP BY fac.facility_code, d.full_date, t.hour, f.model_version_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20
" 2>/dev/null
