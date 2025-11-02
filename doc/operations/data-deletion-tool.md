# Data Deletion Tool Documentation

## Overview

The Data Deletion Tool provides a safe, efficient, and comprehensive way to delete data from the PV Lakehouse across Bronze, Silver, and Gold layers. It ensures complete cleanup across all systems (Iceberg catalog, MinIO storage, PostgreSQL metadata).

## Features

### ✨ Key Capabilities

- **Flexible Date Range Filtering**: Delete data by specific datetime ranges
- **Layer Selection**: Choose one or more layers (bronze, silver, gold)
- **Table Selection**: Target specific tables or all tables in a layer
- **Full Table Deletion**: Option to drop entire tables
- **Dry-Run Mode**: Preview all operations before execution
- **User Confirmation**: Requires explicit 'DELETE' confirmation
- **Thread-Optimized**: Parallel execution for performance
- **Complete Cleanup**: Removes data from Iceberg, MinIO, and PostgreSQL
- **Safe Operations**: Extensive validation to prevent accidental data loss
- **Detailed Reporting**: Comprehensive statistics and error reporting

### 🛡️ Safety Features

1. **Dry-run by default**: Always shows what will be deleted first
2. **Explicit confirmation**: User must type 'DELETE' (all caps) to proceed
3. **Input validation**: Validates all parameters before execution
4. **Date column detection**: Automatically finds the best date column for filtering
5. **Error handling**: Graceful handling of missing tables or metadata
6. **No cascading deletes**: Only deletes what's explicitly specified

## Architecture

### System Integration

The deletion tool integrates with:

```
┌─────────────────────────────────────────────────────┐
│              Data Deletion Tool                     │
├─────────────────────────────────────────────────────┤
│  • Spark Session (with Iceberg extensions)          │
│  • Thread Pool Executor (parallel operations)       │
│  • Date/Time Range Filtering                        │
│  • SQL Generation & Execution                       │
└─────────────────────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
┌──────────────┐ ┌─────────────┐ ┌─────────────┐
│   Iceberg    │ │  PostgreSQL │ │    MinIO    │
│   Catalog    │ │  Metadata   │ │   Storage   │
│   (Spark)    │ │   Tables    │ │  (S3 API)   │
└──────────────┘ └─────────────┘ └─────────────┘
```

### Deletion Flow

1. **Input Validation**: Validate all parameters
2. **Table Discovery**: Query catalog to find existing tables
3. **Plan Creation**: Build deletion plans with SQL statements
4. **Dry-Run Display**: Show detailed preview to user
5. **User Confirmation**: Wait for explicit 'DELETE' confirmation
6. **Parallel Execution**: Execute deletions using thread pool
7. **Progress Tracking**: Display real-time progress
8. **Summary Report**: Show statistics and any errors

## Usage

### Command-Line Interface

```bash
# General syntax
./src/pv_lakehouse/etl/scripts/delete-data.sh [OPTIONS]

# Or call Python script directly via spark-submit
./src/pv_lakehouse/etl/scripts/spark-submit.sh \
  src/pv_lakehouse/etl/scripts/delete_data.py [OPTIONS]
```

### Available Options

| Option | Type | Description | Required |
|--------|------|-------------|----------|
| `--start-datetime` | ISO DateTime | Start of date range | Yes* |
| `--end-datetime` | ISO DateTime | End of date range | No |
| `--layers` | Comma-separated | Layers to delete from | No |
| `--tables` | Comma-separated | Specific tables | No |
| `--delete-table` | Y/N | Drop entire tables | No |
| `--dry-run` | Flag | Preview without deleting | No |
| `--max-workers` | Integer | Parallel threads | No |

*Required unless `--delete-table Y`

### Date Format

All datetime values must be in ISO 8601 format:
```
YYYY-MM-DDTHH:MM:SS
```

Examples:
- `2025-10-01T00:00:00`
- `2025-10-31T23:59:59`
- `2025-11-01T12:30:45`

## Examples

### Example 1: Dry-Run Preview

**See what would be deleted without actually deleting:**

```bash
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --dry-run
```

Output:
```
================================================================================
DRY-RUN MODE - NO DATA WILL BE DELETED
================================================================================

Configuration:
  Layers:        bronze, silver, gold
  Delete Table:  N
  Date Range:    2025-10-01T00:00:00 to 2025-11-01T15:30:00
  Max Workers:   4

Planned Operations: 11
--------------------------------------------------------------------------------

1. Layer: bronze | Table: raw_facilities
   Full Name: lh.bronze.raw_facilities
   Action:    DELETE rows in date range
   Date Col:  ingest_timestamp
   Range:     2025-10-01T00:00:00 to 2025-11-01T15:30:00
   Condition: `ingest_timestamp` BETWEEN TIMESTAMP '2025-10-01 00:00:00' 
              AND TIMESTAMP '2025-11-01 15:30:00'
   Estimated: ~1,234 rows affected
   SQL:       DELETE FROM lh.bronze.raw_facilities WHERE ...

...

--------------------------------------------------------------------------------
Total Estimated Rows to Delete: ~15,678
Total Tables Affected: 11
================================================================================
```

### Example 2: Delete Data from Specific Date Range

**Delete all data from October 2025:**

```bash
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --end-datetime "2025-10-31T23:59:59"
```

### Example 3: Delete from Specific Layers

**Delete data from bronze and silver layers only:**

```bash
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --layers bronze,silver
```

### Example 4: Delete from Specific Tables

**Delete data from specific tables across all layers:**

```bash
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --tables raw_facilities,clean_facility_master
```

### Example 5: Delete Entire Table

**Drop a table completely (data + metadata):**

```bash
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --delete-table Y \
  --layers bronze \
  --tables raw_facilities
```

**⚠️ Warning**: This permanently removes the table. You'll need to recreate it.

### Example 6: Delete All Data (All Time)

**Delete all historical data from silver layer:**

```bash
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2020-01-01T00:00:00" \
  --layers silver
```

### Example 7: Parallel Deletion

**Use more threads for faster deletion:**

```bash
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --max-workers 8
```

## How It Works

### 1. Table Discovery

The tool automatically discovers all tables in the specified layers:

```python
# For each layer
SHOW TABLES IN lh.bronze
SHOW TABLES IN lh.silver
SHOW TABLES IN lh.gold
```

### 2. Date Column Detection

Automatically finds the best date column using priority:

1. `ingest_date`, `ingest_timestamp`
2. `weather_date`, `air_date`
3. `date`, `full_date`
4. `event_date`, `event_timestamp`
5. `created_at`, `updated_at`
6. `date_hour`, `date_key`, `time_key`
7. Any other date/timestamp column

### 3. SQL Generation

Based on column type, generates appropriate SQL:

**For Timestamp columns:**
```sql
DELETE FROM lh.bronze.raw_facilities 
WHERE `ingest_timestamp` BETWEEN 
      TIMESTAMP '2025-10-01 00:00:00' AND 
      TIMESTAMP '2025-10-31 23:59:59'
```

**For Date columns:**
```sql
DELETE FROM lh.silver.clean_facility_master 
WHERE `date` BETWEEN 
      DATE '2025-10-01' AND 
      DATE '2025-10-31'
```

**For Integer date keys (YYYYMMDD):**
```sql
DELETE FROM lh.gold.dim_date 
WHERE `date_key` BETWEEN 20251001 AND 20251031
```

### 4. Parallel Execution

Uses Python's `ThreadPoolExecutor` to delete from multiple tables simultaneously:

```python
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(delete, plan) for plan in plans]
    for future in as_completed(futures):
        result = future.result()
        # Track progress
```

### 5. Complete Cleanup

Iceberg automatically handles:
- Removing data files from MinIO
- Updating metadata in PostgreSQL
- Maintaining transaction history
- Cleaning up snapshots

## Layer & Table Reference

### Bronze Layer Tables

| Table Name | Description |
|------------|-------------|
| `raw_facilities` | Facility master data |
| `raw_facility_air_quality` | Air quality measurements |
| `raw_facility_weather` | Weather data |
| `raw_facility_timeseries` | Energy generation timeseries |

### Silver Layer Tables

| Table Name | Description |
|------------|-------------|
| `clean_facility_master` | Cleaned facility master |
| `clean_hourly_air_quality` | Hourly air quality |
| `clean_hourly_weather` | Hourly weather |
| `clean_hourly_energy` | Hourly energy generation |

### Gold Layer Tables

**Dimension Tables:**
- `dim_air_quality_category`
- `dim_date`
- `dim_equipment_status`
- `dim_facility`
- `dim_model_version`
- `dim_performance_issue`
- `dim_time`
- `dim_weather_condition`

**Fact Tables:**
- `fact_air_quality_impact`
- `fact_kpi_performance`
- `fact_root_cause_analysis`
- `fact_solar_forecast`
- `fact_weather_impact`

## Performance Optimization

### Thread Configuration

Choose `--max-workers` based on:

- **Small deletions (<5 tables)**: Use 2-4 workers
- **Medium deletions (5-10 tables)**: Use 4-8 workers
- **Large deletions (>10 tables)**: Use 8-12 workers

**Note**: Too many workers can overwhelm the database. Start with 4.

### Spark Configuration

The tool automatically optimizes Spark settings:

```python
{
    "spark.sql.shuffle.partitions": "100",  # Fewer partitions for deletes
    "spark.sql.adaptive.enabled": "true",   # Adaptive query execution
}
```

### Best Practices

1. **Always dry-run first**: Check what will be deleted
2. **Delete in batches**: For very large datasets, delete in smaller date ranges
3. **Off-peak hours**: Run during low-traffic periods
4. **Monitor resources**: Watch CPU and memory usage
5. **Backup critical data**: Before major deletions

## Error Handling

### Common Errors

#### 1. Table Not Found

```
Warning: No existing tables found in layer 'bronze' matching: ['unknown_table']
```

**Solution**: Check table name spelling or use `--dry-run` to see available tables.

#### 2. No Date Column Found

```
Warning: No suitable date column found in lh.bronze.raw_facilities. 
Skipping date-based deletion.
```

**Solution**: Either use `--delete-table Y` or manually delete the table.

#### 3. Spark Container Not Running

```
Error: Spark master container is not running
Start the stack with: cd docker && docker compose --profile core up -d
```

**Solution**: Start the Docker stack first.

#### 4. Invalid Date Format

```
Error: Invalid datetime '2025-10-01'. Expected ISO format: YYYY-MM-DDTHH:MM:SS
```

**Solution**: Use full ISO format with time: `2025-10-01T00:00:00`

### Recovery

If deletion fails midway:

1. Check the summary report for failed tables
2. Re-run with `--dry-run` to see current state
3. Run again with just the failed tables
4. Check Spark and Trino logs for details:
   ```bash
   docker logs spark-master
   docker logs trino
   ```

## Verification

### After Deletion

**1. Check row counts:**
```bash
docker exec -it trino trino

# In Trino
SELECT COUNT(*) FROM lh.bronze.raw_facilities;
SELECT COUNT(*) FROM lh.silver.clean_facility_master;
```

**2. Check date ranges:**
```bash
# In Trino
SELECT 
    MIN(ingest_timestamp) as first_record,
    MAX(ingest_timestamp) as last_record,
    COUNT(*) as total_rows
FROM lh.bronze.raw_facilities;
```

**3. Check table metadata:**
```bash
docker exec postgres psql -U pvlakehouse -d iceberg_catalog \
  -c "SELECT table_name, metadata_location FROM iceberg_tables WHERE table_namespace = 'bronze';"
```

**4. Check MinIO storage:**
```bash
docker exec minio mc ls minio/lakehouse/iceberg/warehouse/bronze/
```

## Safety Checklist

Before running deletions:

- [ ] Docker stack is running
- [ ] Ran with `--dry-run` first
- [ ] Reviewed the deletion plan
- [ ] Verified date range is correct
- [ ] Confirmed layer selection
- [ ] Checked table names
- [ ] Have backup if needed
- [ ] Notified team members
- [ ] Running during off-peak hours

## Troubleshooting

### Debugging Mode

Enable detailed Spark logging:

```bash
export SPARK_LOG_LEVEL=DEBUG

./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --dry-run
```

### Check Spark Logs

```bash
docker logs spark-master --tail 100 --follow
```

### Manual SQL Verification

Connect to Trino and verify tables:

```bash
docker exec -it trino trino

# Show all bronze tables
SHOW TABLES FROM iceberg.bronze;

# Check table schema
DESCRIBE iceberg.bronze.raw_facilities;

# Count rows
SELECT COUNT(*) FROM iceberg.bronze.raw_facilities;

# Check date range
SELECT 
    MIN(ingest_timestamp),
    MAX(ingest_timestamp) 
FROM iceberg.bronze.raw_facilities;
```

## Limitations

1. **Date Column Required**: For date-based deletion, table must have a date/timestamp column
2. **Iceberg Only**: Only works with Iceberg tables in the `lh` catalog
3. **Serial per Table**: Each table deleted sequentially (but multiple tables in parallel)
4. **No Undo**: Deletions are permanent (though Iceberg keeps snapshots for time travel)

## Advanced Usage

### Using Time Travel (Before Deletion)

Check Iceberg snapshots before deleting:

```sql
-- See all snapshots
SELECT * FROM iceberg.bronze."raw_facilities$snapshots";

-- Query old version
SELECT * FROM iceberg.bronze.raw_facilities 
FOR VERSION AS OF 12345;
```

### Selective Table Deletion

Delete only specific tables across layers:

```bash
./src/pv_lakehouse/etl/scripts/delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --tables raw_facility_weather,clean_hourly_weather,fact_weather_impact
```

This will find and delete from these tables in any layer they exist.

## Support

For issues or questions:

1. Check this documentation
2. Review error messages and summaries
3. Enable debug logging
4. Check Docker logs
5. Open a GitHub issue

## License

MIT License - see LICENSE file for details
