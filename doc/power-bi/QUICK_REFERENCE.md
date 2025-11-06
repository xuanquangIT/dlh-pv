# Power BI - Quick Reference Card

## 🎯 Essential Information at a Glance

### Connection Settings
```
Host:     localhost (or your server IP)
Port:     8081
Catalog:  iceberg
Schema:   gold
User:     trino
Password: (leave blank)
Driver:   Trino ODBC Driver / PostgreSQL
```

### Main Fact Table
```
iceberg.gold.fact_solar_environmental
- 11,085 rows
- Date range: 2024-10-09 to 2024-12-10 (93 days)
- 5 facilities × 24 hours/day × 93 days = 11,085 hourly records
```

### Key Dimensions for Joins
```
iceberg.gold.dim_facility      → 5 rows
iceberg.gold.dim_date          → 93 rows
iceberg.gold.dim_time          → 24 rows
iceberg.gold.dim_aqi_category  → 6 rows
```

---

## 🔑 Primary Key Fields (For Joins)

| Join Field | Fact Table | Dimension Table |
|-----------|-----------|-----------------|
| `facility_key` | fact_solar_environmental | dim_facility |
| `date_key` | fact_solar_environmental | dim_date |
| `time_key` | fact_solar_environmental | dim_time |
| `aqi_category_key` | fact_solar_environmental | dim_aqi_category |

---

## ⚡ Critical Fields for Power BI

### Energy (Main KPIs)
- `energy_mwh` - Energy generation (Measure)
- `power_avg_mw` - Average power (Measure)

### Weather (Context)
- `temperature_2m` - Temperature °C
- `shortwave_radiation` - Solar radiation W/m²
- `humidity_2m` - Humidity %
- `cloud_cover` - Cloud coverage %

### Air Quality (Secondary KPI)
- `pm2_5` - PM2.5 µg/m³
- `pm10` - PM10 µg/m³
- `aqi_value` - Air Quality Index
- `aqi_category` - Category name (from join)

### Time Dimension (Pivots)
- `full_date` - Date
- `hour` - Hour (0-23)
- `time_of_day` - Morning/Afternoon/Evening/Night
- `daylight_period` - Sunrise/Morning/Noon/Afternoon/Sunset/Dusk/Night
- `is_peak_hour` - Peak hour flag

### Facility (Filters)
- `facility_code` - Facility ID
- `facility_name` - Facility name
- `network_region` - Region

### Quality
- `is_valid` - Always filter WHERE is_valid = true
- `completeness_pct` - Data completeness %

---

## 🔗 Standard Join Query

```sql
SELECT 
  d.full_date,
  t.hour,
  t.time_of_day,
  fac.facility_code,
  f.energy_mwh,
  f.power_avg_mw,
  f.temperature_2m,
  f.shortwave_radiation,
  f.pm2_5,
  aqi.aqi_category
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_facility fac 
  ON f.facility_key = fac.facility_key
LEFT JOIN iceberg.gold.dim_date d 
  ON f.date_key = d.date_key
LEFT JOIN iceberg.gold.dim_time t 
  ON f.time_key = t.time_key
LEFT JOIN iceberg.gold.dim_aqi_category aqi 
  ON f.aqi_category_key = aqi.aqi_category_key
WHERE f.is_valid = true
```

---

## 📊 Common Aggregations

### Daily Summary
```sql
SELECT 
  d.full_date,
  fac.facility_name,
  SUM(f.energy_mwh) as daily_energy,
  AVG(f.power_avg_mw) as avg_power,
  AVG(f.temperature_2m) as avg_temp
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_facility fac ON f.facility_key = fac.facility_key
LEFT JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
WHERE f.is_valid = true
GROUP BY d.full_date, fac.facility_name
```

### Hourly Trend
```sql
SELECT 
  t.hour,
  AVG(f.energy_mwh) as avg_energy,
  AVG(f.temperature_2m) as avg_temp,
  AVG(f.shortwave_radiation) as avg_radiation
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_time t ON f.time_key = t.time_key
WHERE f.is_valid = true
GROUP BY t.hour
ORDER BY t.hour
```

### By Facility
```sql
SELECT 
  fac.facility_code,
  fac.facility_name,
  COUNT(*) as record_count,
  SUM(f.energy_mwh) as total_energy,
  AVG(f.energy_mwh) as avg_hourly_energy
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_facility fac ON f.facility_key = fac.facility_key
WHERE f.is_valid = true
GROUP BY fac.facility_code, fac.facility_name
```

---

## 🎨 Recommended Visualizations

| Metric | Visualization | Dimension |
|--------|---------------|-----------|
| Energy vs Date | Line Chart | full_date |
| Energy by Hour | Bar Chart | hour |
| Temp vs Energy | Scatter Plot | temperature_2m vs energy_mwh |
| AQI Categories | Pie Chart | aqi_category |
| Facility Comparison | Column Chart | facility_name |
| Heatmap | Matrix | facility × hour |

---

## ⏱️ Data Refresh Frequency

**Recommended**: Daily refresh at off-peak hours (e.g., 2 AM)

```
Option 1 (Scheduled): Enable Power BI Service refresh
Option 2 (Manual): Refresh from Power BI Desktop before viewing
Option 3 (Real-time): Use Direct Query (slower, but always fresh)
```

---

## 🚨 Important Notes

1. ✅ **Filter Always**: Use `WHERE is_valid = true` in all queries
2. ✅ **Date Range**: Data available from 2024-10-09 to 2024-12-10 only
3. ✅ **Granularity**: Each row = 1 facility + 1 hour
4. ✅ **Unique Key**: (facility_key + date_key + time_key) has no duplicates
5. ✅ **Dimensions**: Import into Power BI for better performance
6. ✅ **Fact Table**: Consider importing for faster dashboard loads

---

## 🔧 Setup Checklist

- [ ] Trino ODBC driver installed
- [ ] Power BI connection configured
- [ ] Test connection successful
- [ ] Can query `iceberg.gold.fact_solar_environmental`
- [ ] Can see dimension tables
- [ ] Joins working correctly
- [ ] Data visible in Power BI

---

## 📞 Test Queries (Copy & Run)

```sql
-- Test 1: Connection
SELECT 1 as connection_test;

-- Test 2: Table exists and has data
SELECT COUNT(*) as total_rows FROM iceberg.gold.fact_solar_environmental;

-- Test 3: Date range
SELECT 
  MIN(d.full_date) as earliest,
  MAX(d.full_date) as latest
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key;

-- Test 4: Facilities
SELECT DISTINCT facility_name FROM iceberg.gold.dim_facility;

-- Test 5: Sample data (should return 10 rows)
SELECT * FROM iceberg.gold.fact_solar_environmental LIMIT 10;
```

---

## 📋 Field Data Types (For Power BI Formatting)

### Numbers (Measures)
- `energy_mwh` → Number, 2 decimals
- `power_avg_mw` → Number, 2 decimals
- `temperature_2m` → Number, 1 decimal
- `shortwave_radiation` → Number, 2 decimals
- `pm2_5` → Number, 2 decimals

### Text (Dimensions)
- `facility_code` → Text
- `facility_name` → Text
- `month_name` → Text
- `day_name` → Text
- `time_of_day` → Text
- `aqi_category` → Text

### Boolean (Filters)
- `is_valid` → Boolean
- `is_peak_hour` → Boolean
- `is_weekend` → Boolean

### Dates
- `full_date` → Date
- `hour` → Whole Number

---

**Last Updated**: November 6, 2025  
**Status**: ✅ Ready to Use  
**Data Quality**: 100% Valid
