# Bronze Layer Tables - Testing Guide

## Overview

This directory contains DDL files and test scripts for the Bronze layer tables in the lakehouse.

## Tables

1. **lh.bronze.oe_facilities_raw** - OpenNEM facilities registry
2. **lh.bronze.oe_generation_hourly_raw** - OpenNEM solar generation hourly time series
3. **lh.bronze.om_weather_hourly_raw** - Open-Meteo weather hourly observations
4. **lh.bronze.om_air_quality_hourly_raw** - Open-Meteo air quality hourly observations

## Table Specifications

All tables follow the Bronze layer standard:

- **Partition Strategy**: `days(ts_utc)` for all tables
- **Metadata Columns**: 
  - `_ingest_time` - Timestamp when data landed in lakehouse
  - `_source` - Data source identifier (e.g., `opennem_api_v3`)
  - `_hash` - SHA256 hash for deduplication
- **Format**: Iceberg v2
- **Catalog**: `lh` (Iceberg REST catalog)

## DDL Files

Located in `/sql/bronze/`:

- `oe_facilities_raw.sql`
- `oe_generation_hourly_raw.sql`
- `om_weather_hourly_raw.sql`
- `om_air_quality_hourly_raw.sql`

## Testing

### Quick Test with Python Script

The Python script provides a complete testing workflow:

```bash
# Create tables and insert sample data
python tests/test_bronze_tables_complete.py --create

# Verify tables and data
python tests/test_bronze_tables_complete.py --verify

# Run all steps (create + verify)
python tests/test_bronze_tables_complete.py --all

# Cleanup test data
python tests/test_bronze_tables_complete.py --cleanup
```

### Alternative: SQL Script

For manual testing with Spark SQL or Trino:

```bash
# Using spark-sql (if available)
spark-sql -f tests/test_bronze_tables_complete.sql

# Or execute sections manually in Trino/Spark SQL client
```

## Verification Checklist

After running the test, verify:

- ✅ All 4 tables are created and visible in Trino
- ✅ Partition spec is `days(ts_utc)` for all tables
- ✅ All metadata columns (`_ingest_time`, `_source`, `_hash`) are present
- ✅ Sample data can be inserted and queried
- ✅ No partition evolution warnings

### Check Tables in Trino

```sql
-- List all bronze tables
SHOW TABLES IN lh.bronze;

-- Verify partition spec
SHOW CREATE TABLE lh.bronze.oe_facilities_raw;
SHOW CREATE TABLE lh.bronze.oe_generation_hourly_raw;
SHOW CREATE TABLE lh.bronze.om_weather_hourly_raw;
SHOW CREATE TABLE lh.bronze.om_air_quality_hourly_raw;

-- Count records
SELECT 'oe_facilities_raw' AS table_name, COUNT(*) AS count FROM lh.bronze.oe_facilities_raw
UNION ALL
SELECT 'oe_generation_hourly_raw', COUNT(*) FROM lh.bronze.oe_generation_hourly_raw
UNION ALL
SELECT 'om_weather_hourly_raw', COUNT(*) FROM lh.bronze.om_weather_hourly_raw
UNION ALL
SELECT 'om_air_quality_hourly_raw', COUNT(*) FROM lh.bronze.om_air_quality_hourly_raw;
```

## Sample Data

The test scripts insert sample data for **2025-01-15** (8 hours):

- **Facilities**: 2 solar farms (AVLSF, BERYLSF)
- **Generation**: 12 hourly records
- **Weather**: 8 hourly observations
- **Air Quality**: 8 hourly observations

All sample data includes proper metadata columns for testing.

## Expected Results

### Record Counts

| Table | Records |
|-------|---------|
| oe_facilities_raw | 2 |
| oe_generation_hourly_raw | 12 |
| om_weather_hourly_raw | 8 |
| om_air_quality_hourly_raw | 8 |

### Metadata Verification

All tables should show:
- `total_records` = `has_ingest_time` = `has_source`
- `has_hash` may be less if some records have NULL hash (acceptable)

## Cleanup

After testing, cleanup the sample data:

```bash
# Using Python script (interactive confirmation)
python tests/test_bronze_tables_complete.py --cleanup

# Or manually in Trino/Spark SQL
DELETE FROM lh.bronze.oe_facilities_raw 
WHERE _source = 'opennem_api_v3' 
AND facility_code IN ('AVLSF', 'BERYLSF');

DELETE FROM lh.bronze.oe_generation_hourly_raw 
WHERE _source = 'opennem_api_v3' 
AND ts_utc >= TIMESTAMP '2025-01-15 00:00:00' 
AND ts_utc < TIMESTAMP '2025-01-16 00:00:00';

DELETE FROM lh.bronze.om_weather_hourly_raw 
WHERE _source = 'openmeteo_api_v1' 
AND ts_utc >= TIMESTAMP '2025-01-15 00:00:00' 
AND ts_utc < TIMESTAMP '2025-01-16 00:00:00';

DELETE FROM lh.bronze.om_air_quality_hourly_raw 
WHERE _source = 'openmeteo_airquality_api_v1' 
AND ts_utc >= TIMESTAMP '2025-01-15 00:00:00' 
AND ts_utc < TIMESTAMP '2025-01-16 00:00:00';
```

## Acceptance Criteria ✅

- [x] DDLs (Spark SQL) checked in under `sql/bronze/*.sql`
- [x] Tables created and visible in Trino
- [x] Partition spec = `days(ts_utc)` for all tables
- [x] Metadata columns (`_ingest_time`, `_source`, `_hash`) included
- [x] No hidden partition evolution warnings
- [x] Test script with sample data and cleanup provided

## Troubleshooting

### "Catalog not found" error
Ensure Iceberg REST catalog is running and accessible at `http://iceberg-rest:8181`

### "Schema does not exist" error
Create the schema first:
```sql
CREATE SCHEMA IF NOT EXISTS lh.bronze;
```

### Partition verification
Check the CREATE TABLE statement output includes:
```sql
PARTITIONED BY (days(ts_utc))
```

## Next Steps

After successful testing:

1. Review and commit DDL files
2. Update bronze ingestion ETL to use these tables
3. Set up data quality checks
4. Monitor partition pruning efficiency
