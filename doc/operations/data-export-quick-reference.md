# Data Export - Quick Reference

## Most Common Commands

### Export Everything
```bash
./export-data.sh
```

### Export One Layer
```bash
./export-data.sh --layers silver
```

### Export Specific Tables
```bash
./export-data.sh --tables raw_facilities,clean_facility_master
```

### Export with Date Range
```bash
./export-data.sh \
  --layers silver \
  --start-date "2025-01-01" \
  --end-date "2025-01-31"
```

### Export with Compression
```bash
./export-data.sh --compression gzip
```

## Command Cheat Sheet

| Task | Command |
|------|---------|
| **Export all** | `./export-data.sh` |
| **Export one layer** | `./export-data.sh --layers bronze` |
| **Export one table** | `./export-data.sh --layers silver --tables clean_facility_master` |
| **Export by date** | `./export-data.sh --start-date "2025-01-01" --end-date "2025-01-31"` |
| **Compressed export** | `./export-data.sh --compression gzip` |
| **Custom output dir** | `./export-data.sh --output-dir /custom/path` |
| **Multiple output files** | `./export-data.sh --coalesce 4` |
| **Help/options** | `./export-data.sh --help` |

## Layers & Tables

### Bronze (Raw Data)
- raw_facilities
- raw_facility_air_quality  
- raw_facility_weather
- raw_facility_timeseries

### Silver (Cleaned Data)
- clean_facility_master
- clean_hourly_air_quality
- clean_hourly_weather
- clean_hourly_energy

### Gold (Aggregated Data)
- dim_* (dimensions: air_quality_category, date, time, equipment_status, facility, model_version, performance_issue, weather_condition)
- fact_* (facts: air_quality_impact, kpi_performance, root_cause_analysis, solar_forecast, weather_impact)

## Output Location
```
exported-data/
├── bronze/
│   ├── raw_facilities/data.csv
│   └── ...
├── silver/
│   ├── clean_facility_master/data.csv
│   └── ...
└── gold/
    ├── dim_facility/data.csv
    └── ...
```

## Examples

### Backup Everything with Compression
```bash
./export-data.sh --compression gzip
```

### Export January 2025 Energy Data
```bash
./export-data.sh \
  --layers silver \
  --tables clean_hourly_energy \
  --start-date "2025-01-01" \
  --end-date "2025-01-31" \
  --compression gzip
```

### Export All Gold Dimensions
```bash
./export-data.sh --layers gold \
  --tables dim_facility,dim_date,dim_time,dim_equipment_status,dim_air_quality_category,dim_weather_condition,dim_model_version,dim_performance_issue
```

### Export with Large Output Files
```bash
./export-data.sh --coalesce 4
```

## Tips

- 🔍 Check data before exporting: `SELECT COUNT(*) FROM lh.silver.clean_hourly_energy`
- 💾 Check disk space: `df -h exported-data/`
- 📊 View sample data: `head -10 exported-data/silver/clean_facility_master/data.csv`
- 🗜️ Use `--compression gzip` to save disk space
- ⏰ For large exports, use date ranges to split into smaller files
- 🔗 Compressed files work directly: `zcat data.csv.gz | wc -l`

## Help
```bash
./export-data.sh --help
```

---

Full guide: [Data Export Guide](./data-export-guide.md)
