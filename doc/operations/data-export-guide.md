# Data Export Tool - Usage Guide

## Overview

The Data Export Tool allows you to export data from Bronze, Silver, and Gold layers of the PV Lakehouse to CSV files. This is useful for:

- **Data Analysis**: Export specific datasets for external analysis
- **Reporting**: Generate CSV reports for stakeholders
- **Backup**: Create backups of lakehouse data
- **Integration**: Export data for integration with other systems
- **Testing**: Extract test datasets

## Quick Start

### Export All Data (All Layers)
```bash
cd src/pv_lakehouse/etl/scripts
./export-data.sh
```

This will export all tables from bronze, silver, and gold layers to the `exported-data/` folder.

## Usage Examples

### 1. Export Specific Layers

Export only Bronze and Silver layers:
```bash
./export-data.sh --layers bronze,silver
```

Export only Gold layer:
```bash
./export-data.sh --layers gold
```

### 2. Export Specific Tables

Export specific tables from a layer:
```bash
./export-data.sh --layers bronze --tables raw_facilities,raw_facility_weather
```

Export specific tables from multiple layers:
```bash
./export-data.sh --layers silver,gold --tables clean_facility_master,dim_facility
```

### 3. Date Range Filtering

Export Silver data for January 2025:
```bash
./export-data.sh \
  --layers silver \
  --start-date "2025-01-01" \
  --end-date "2025-01-31"
```

The tool automatically detects date/timestamp columns and applies filtering.

### 4. Compression

Export with gzip compression (smaller files):
```bash
./export-data.sh --compression gzip
```

Available compression codecs:
- `none` (default): No compression
- `gzip`: Better compression ratio
- `bzip2`: Maximum compression

### 5. Custom Output Directory

Export to a specific location:
```bash
./export-data.sh --output-dir /mnt/backups/lakehouse-export
```

Default: `./exported-data/`

### 6. Multiple Output Files

For large tables, split into multiple files:
```bash
./export-data.sh --coalesce 4
```

This creates 4 output files per table instead of 1 combined file.

## Complete Examples

### Example 1: Monthly Data Export with Compression

Export all silver layer data for a specific month with compression:
```bash
./export-data.sh \
  --layers silver \
  --start-date "2025-01-01" \
  --end-date "2025-01-31" \
  --compression gzip \
  --output-dir /backups/jan-2025-export
```

Result:
```
/backups/jan-2025-export/
├── silver/
│   ├── clean_facility_master/
│   │   └── data.csv.gz
│   ├── clean_hourly_energy/
│   │   └── data.csv.gz
│   ├── clean_hourly_weather/
│   │   └── data.csv.gz
│   └── clean_hourly_air_quality/
│       └── data.csv.gz
```

### Example 2: Backup All Gold Dimensions

Export all dimension tables from Gold layer:
```bash
./export-data.sh --layers gold --tables \
  dim_facility,dim_date,dim_time,dim_equipment_status,\
  dim_air_quality_category,dim_weather_condition,dim_model_version,\
  dim_performance_issue
```

### Example 3: Export Recent Energy Data

Export the latest energy data:
```bash
./export-data.sh \
  --layers silver \
  --tables clean_hourly_energy \
  --start-date "2025-01-15" \
  --output-dir ./energy-export
```

### Example 4: Full Backup of All Layers

Create a complete backup with compression:
```bash
./export-data.sh \
  --layers bronze,silver,gold \
  --compression gzip \
  --output-dir ./backup-$(date +%Y%m%d)
```

## Output Structure

After export, the data is organized as follows:

```
exported-data/
├── bronze/
│   ├── raw_facilities/
│   │   └── data.csv          # or data.csv.gz if compressed
│   ├── raw_facility_air_quality/
│   │   └── data.csv
│   ├── raw_facility_weather/
│   │   └── data.csv
│   └── raw_facility_timeseries/
│       └── data.csv
├── silver/
│   ├── clean_facility_master/
│   │   └── data.csv
│   ├── clean_hourly_air_quality/
│   │   └── data.csv
│   ├── clean_hourly_energy/
│   │   └── data.csv
│   └── clean_hourly_weather/
│       └── data.csv
└── gold/
    ├── dim_air_quality_category/
    │   └── data.csv
    ├── dim_date/
    │   └── data.csv
    ├── dim_equipment_status/
    │   └── data.csv
    ├── dim_facility/
    │   └── data.csv
    ├── dim_model_version/
    │   └── data.csv
    ├── dim_performance_issue/
    │   └── data.csv
    ├── dim_time/
    │   └── data.csv
    ├── dim_weather_condition/
    │   └── data.csv
    ├── fact_air_quality_impact/
    │   └── data.csv
    ├── fact_kpi_performance/
    │   └── data.csv
    ├── fact_root_cause_analysis/
    │   └── data.csv
    ├── fact_solar_forecast/
    │   └── data.csv
    └── fact_weather_impact/
        └── data.csv
```

## Available Tables by Layer

### Bronze Layer (Raw Data)
- `raw_facilities` - Raw facility metadata
- `raw_facility_air_quality` - Raw air quality data
- `raw_facility_weather` - Raw weather data
- `raw_facility_timeseries` - Raw timeseries data

### Silver Layer (Cleaned Data)
- `clean_facility_master` - Cleaned facility master data
- `clean_hourly_air_quality` - Cleaned hourly air quality
- `clean_hourly_weather` - Cleaned hourly weather
- `clean_hourly_energy` - Cleaned hourly energy data

### Gold Layer (Aggregated Data)
- `dim_air_quality_category` - Air quality category dimension
- `dim_date` - Date dimension
- `dim_time` - Time dimension
- `dim_equipment_status` - Equipment status dimension
- `dim_facility` - Facility dimension
- `dim_model_version` - Model version dimension
- `dim_performance_issue` - Performance issue dimension
- `dim_weather_condition` - Weather condition dimension
- `fact_air_quality_impact` - Air quality impact facts
- `fact_kpi_performance` - KPI performance facts
- `fact_root_cause_analysis` - Root cause analysis facts
- `fact_solar_forecast` - Solar forecast facts
- `fact_weather_impact` - Weather impact facts

## Command Reference

### All Options
```
./export-data.sh [OPTIONS]

--layers LAYERS              Comma-separated layers (default: bronze,silver,gold)
--tables TABLES              Comma-separated table names (optional)
--start-date DATE            Start date (YYYY-MM-DD format)
--end-date DATE              End date (YYYY-MM-DD format)
--compression CODEC          Codec: none, gzip, bzip2 (default: none)
--output-dir PATH            Output directory (default: ./exported-data)
--coalesce N                 Output files per table (default: 1)
--help                       Show help message
```

## Performance Considerations

### Export Time
- **Small tables** (< 1M rows): ~1-2 seconds
- **Medium tables** (1-100M rows): ~10-30 seconds
- **Large tables** (> 100M rows): ~1-5 minutes

Varies based on:
- Table size
- Network speed
- Disk write performance
- Compression codec used

### Disk Space

Estimate CSV file size (uncompressed):
- Bronze layers: ~500 MB - 2 GB
- Silver layers: ~200 MB - 1 GB
- Gold layers: ~100 MB - 500 MB

With gzip compression: ~10-30% of original size

### Tips for Large Exports
1. Use `--coalesce N` to create multiple files
2. Use compression (`--compression gzip`) to reduce disk usage
3. Run during off-peak hours
4. Check available disk space: `df -h`
5. Export specific date ranges for time-series data

## Troubleshooting

### Script Fails with "Module not found"
```
Error: ModuleNotFoundError: No module named 'pv_lakehouse'
```

**Solution**: Use the wrapper script or ensure PYTHONPATH is set:
```bash
cd src/pv_lakehouse/etl/scripts
./export-data.sh          # Use wrapper (recommended)
```

### Empty CSV Files
```
Issue: Exported CSV has headers but no data rows
```

**Possible Causes**:
- Table is empty
- Date filtering excluded all data

**Solution**:
- Check if table has data: `./spark-submit.sh ... | grep "0 rows"`
- Adjust date range filters
- Try without date filters first

### Permission Denied
```
Error: permission denied: ./export-data.sh
```

**Solution**:
```bash
chmod +x export-data.sh
```

### Out of Memory
```
Error: Java heap space or Spark memory error
```

**Solution**: The Spark driver/executor configuration may need adjustment. Contact DevOps team.

### Docker Not Running
```
Error: Connection refused
```

**Solution**:
```bash
cd docker
docker compose --profile core up -d
```

## Verification

After export, verify the data:

```bash
# Count rows in exported CSV
wc -l exported-data/silver/clean_facility_master/data.csv

# Verify data structure
head -5 exported-data/silver/clean_facility_master/data.csv

# Compare with source table (requires spark-submit)
./spark-submit.sh - <<EOF
spark.sql("SELECT COUNT(*) FROM lh.silver.clean_facility_master").show()
EOF
```

## Advanced Usage

### Export with Custom Spark Configuration

The tool uses the default Spark configuration. For custom configurations, edit `src/pv_lakehouse/etl/utils/spark_utils.py` or pass environment variables:

```bash
SPARK_LOCAL_IP=127.0.0.1 ./export-data.sh
```

### Schedule Regular Exports

Create a cron job for daily exports:

```bash
# Add to crontab (crontab -e)
0 2 * * * cd /home/pvlakehouse/dlh-pv/src/pv_lakehouse/etl/scripts && \
  ./export-data.sh --layers silver --compression gzip --output-dir /backups/daily-$(date +%Y%m%d)
```

## Support

For issues or questions:

1. Check the troubleshooting section above
2. Review the script logs
3. Check Docker health: `docker compose ps`
4. Check Spark logs: `docker logs spark-master --tail 100`
5. Open an issue on GitHub with:
   - Command used
   - Error message
   - Output logs
   - Docker status

## Related Documentation

- [ETL Development Guide](../../../doc/development/etl-development.md)
- [Operations Guide](../../../doc/operations/operations.md)
- [Data Model](../../../doc/data-model/)
- [Architecture Overview](../../../doc/architecture/)
