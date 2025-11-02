# ETL Scripts Directory

This directory contains utility scripts for managing the PV Lakehouse ETL operations.

## Available Scripts

### 🗑️ Data Deletion Tool

**Purpose**: Safely delete data from Bronze/Silver/Gold layers with comprehensive validation.

**Script**: `delete-data.sh` (wrapper) and `delete_data.py` (main implementation)

**Quick Start**:
```bash
# Dry-run to preview
./delete-data.sh --start-datetime "2025-10-01T00:00:00" --dry-run

# Execute deletion
./delete-data.sh --start-datetime "2025-10-01T00:00:00"
```

**Documentation**: 
- [Full Guide](../../../doc/operations/data-deletion-tool.md)
- [Quick Reference](../../../doc/operations/data-deletion-quick-reference.md)

**Features**:
- ✅ Date range filtering
- ✅ Layer and table selection  
- ✅ Full table deletion option
- ✅ Dry-run preview
- ✅ User confirmation required
- ✅ Parallel execution
- ✅ Complete cleanup (Iceberg + MinIO + PostgreSQL)

---

### 🚀 Spark Submit Wrapper

**Purpose**: Submit Spark jobs to the cluster with proper configuration.

**Script**: `spark-submit.sh`

**Usage**:
```bash
./spark-submit.sh <script_path> [args...]

# Examples
./spark-submit.sh src/pv_lakehouse/etl/bronze/load_facilities.py --mode incremental
./spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode incremental
```

**Features**:
- Automatically configures PYTHONPATH
- Sets up Spark environment
- Handles container execution
- Color-coded output
- Error handling

---

### 🧹 Purge Lakehouse Layers

**Purpose**: Legacy purge script (use `delete-data.sh` instead for new operations).

**Script**: `purge_lakehouse_layers.py`

**Note**: This is the older purge script. The new `delete_data.py` provides more features and better UX.

---

### 📝 Cheat Sheet

**Purpose**: Quick reference for common Spark and Iceberg commands.

**Script**: `cheat-sheet.sh`

**Usage**:
```bash
cat cheat-sheet.sh
```

Contains examples for:
- Spark submit commands
- Iceberg table operations
- Query examples
- Debugging commands

---

### 🧪 Test Suite

**Purpose**: Validate the data deletion tool.

**Script**: `test-delete-tool.sh`

**Usage**:
```bash
./test-delete-tool.sh
```

**Tests**:
- Help command
- Dry-run functionality
- Date format validation
- Layer validation
- Table selection
- Delete table mode
- Output completeness
- Python syntax
- Dependencies

---

## Common Workflows

### Delete Data by Date Range

```bash
# 1. Preview what will be deleted
./delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --end-datetime "2025-10-31T23:59:59" \
  --dry-run

# 2. Execute deletion
./delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --end-datetime "2025-10-31T23:59:59"

# 3. Verify deletion
docker exec -it trino trino
SELECT COUNT(*) FROM lh.bronze.raw_facilities;
```

### Delete Specific Tables

```bash
# Preview
./delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --tables raw_facilities,clean_facility_master \
  --dry-run

# Execute
./delete-data.sh \
  --start-datetime "2025-10-01T00:00:00" \
  --tables raw_facilities,clean_facility_master
```

### Drop Tables Completely

```bash
# Preview
./delete-data.sh \
  --delete-table Y \
  --layers bronze \
  --tables raw_facilities \
  --dry-run

# Execute
./delete-data.sh \
  --delete-table Y \
  --layers bronze \
  --tables raw_facilities
```

### Run ETL Jobs

```bash
# Bronze layer ingestion
./spark-submit.sh src/pv_lakehouse/etl/bronze/load_facilities.py --mode incremental
./spark-submit.sh src/pv_lakehouse/etl/bronze/load_facility_weather.py --mode incremental

# Silver layer transformation
./spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_weather --mode incremental
./spark-submit.sh src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode incremental

# Gold layer aggregation
./spark-submit.sh src/pv_lakehouse/etl/gold/run_loader.py
```

## Environment Requirements

All scripts require:
- Docker stack running (`cd docker && docker compose --profile core up -d`)
- Spark master container accessible
- PostgreSQL Iceberg catalog initialized
- MinIO storage configured

**Pre-flight check**:
```bash
cd docker
./scripts/health-check.sh
```

## Troubleshooting

### Script not executable
```bash
chmod +x delete-data.sh
chmod +x spark-submit.sh
chmod +x test-delete-tool.sh
```

### Docker stack not running
```bash
cd docker
docker compose --profile core up -d
docker compose ps
```

### Python module not found
```bash
# Ensure you're using spark-submit wrapper
./spark-submit.sh src/pv_lakehouse/etl/scripts/delete_data.py --help

# Don't run Python directly (PYTHONPATH won't be set)
```

### Check logs
```bash
docker logs spark-master --tail 50
docker logs trino --tail 50
docker logs postgres --tail 50
```

## Best Practices

1. **Always use dry-run first** for deletions
2. **Use spark-submit wrapper** for Spark jobs (sets up PYTHONPATH)
3. **Check health** before running operations
4. **Backup critical data** before major deletions
5. **Run during off-peak** for large operations
6. **Monitor logs** during execution
7. **Verify results** after completion

## Documentation Links

- [Data Deletion Tool Guide](../../../doc/operations/data-deletion-tool.md)
- [Data Deletion Quick Reference](../../../doc/operations/data-deletion-quick-reference.md)
- [Operations Guide](../../../doc/operations/operations.md)
- [ETL Development Guide](../../../doc/development/etl-development.md)

## Support

For issues:
1. Check script documentation
2. Review error messages
3. Enable debug logging
4. Check Docker logs
5. Open GitHub issue with details
