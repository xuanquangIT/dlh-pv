# Gold Layer Fields for Power BI

## 📊 Trino Connection Details

**Catalog**: `iceberg`
**Schemas Available**: 
- `bronze` - Raw data layer
- `silver` - Cleaned & validated data
- `gold` - Analytics-ready data (for Power BI)

**Main Table for Power BI**: `iceberg.gold.fact_solar_environmental`

---

## 📈 FACT TABLE: fact_solar_environmental

**Purpose**: Main analytical facts table with hourly solar, weather, and air quality data
**Grain**: 1 row = 1 facility + 1 date + 1 hour
**Row Count**: 11,085 records
**Date Range**: 2024-10-09 to 2024-12-10 (93 days × 5 facilities × 24 hours)

### Primary Keys / Dimensions References
| Field | Type | Description | Usage |
|-------|------|-------------|-------|
| `facility_key` | bigint | Foreign key → dim_facility | Join with dim_facility for facility details |
| `date_key` | integer | Foreign key → dim_date | Join with dim_date for date details |
| `time_key` | integer | Foreign key → dim_time | Join with dim_time for time details |
| `aqi_category_key` | bigint | Foreign key → dim_aqi_category | Join with dim_aqi_category for AQI info |

### Energy Metrics (Main KPIs)
| Field | Type | Precision | Description | Unit |
|-------|------|-----------|-------------|------|
| `energy_mwh` | decimal(12,6) | 1-6 decimals | Energy generation per hour | MWh |
| `power_avg_mw` | decimal(12,6) | 1-6 decimals | Average power during hour | MW |
| `intervals_count` | integer | - | Number of 5-min intervals with data | Count |

### Solar Radiation Metrics
| Field | Type | Precision | Description | Unit |
|-------|------|-----------|-------------|------|
| `shortwave_radiation` | decimal(10,4) | 1-4 decimals | Total solar radiation | W/m² |
| `direct_radiation` | decimal(10,4) | 1-4 decimals | Direct solar component | W/m² |
| `diffuse_radiation` | decimal(10,4) | 1-4 decimals | Diffuse solar component | W/m² |
| `direct_normal_irradiance` | decimal(10,4) | 1-4 decimals | Direct normal irradiance | W/m² |

### Weather Metrics
| Field | Type | Precision | Description | Unit |
|-------|------|-----------|-------------|------|
| `temperature_2m` | decimal(6,2) | 1-2 decimals | Temperature at 2m height | °C |
| `dew_point_2m` | decimal(6,2) | 1-2 decimals | Dew point temperature | °C |
| `humidity_2m` | decimal(5,2) | 1-2 decimals | Relative humidity | % |
| `cloud_cover` | decimal(5,2) | 1-2 decimals | Total cloud cover | % |
| `cloud_cover_low` | decimal(5,2) | 1-2 decimals | Low cloud cover | % |
| `cloud_cover_mid` | decimal(5,2) | 1-2 decimals | Mid-level cloud cover | % |
| `cloud_cover_high` | decimal(5,2) | 1-2 decimals | High cloud cover | % |
| `precipitation` | decimal(8,3) | 1-3 decimals | Precipitation amount | mm |
| `sunshine_duration` | decimal(10,2) | 1-2 decimals | Sunshine duration | minutes |
| `wind_speed_10m` | decimal(6,2) | 1-2 decimals | Wind speed at 10m | m/s |
| `wind_direction_10m` | decimal(6,2) | 1-2 decimals | Wind direction | degrees |
| `wind_gusts_10m` | decimal(6,2) | 1-2 decimals | Wind gust speed | m/s |
| `pressure_msl` | decimal(8,1) | 1-1 decimals | Mean sea level pressure | hPa |
| `uv_index` | decimal(6,2) | 1-2 decimals | UV index | Index |
| `uv_index_clear_sky` | decimal(6,2) | 1-2 decimals | UV index clear sky | Index |

### Air Quality Metrics (Pollutants)
| Field | Type | Precision | Description | Unit |
|-------|------|-----------|-------------|------|
| `pm2_5` | decimal(8,3) | 1-3 decimals | PM2.5 concentration | µg/m³ |
| `pm10` | decimal(8,3) | 1-3 decimals | PM10 concentration | µg/m³ |
| `dust` | decimal(8,3) | 1-3 decimals | Dust concentration | µg/m³ |
| `nitrogen_dioxide` | decimal(8,3) | 1-3 decimals | NO₂ concentration | µg/m³ |
| `ozone` | decimal(8,3) | 1-3 decimals | O₃ concentration | µg/m³ |
| `sulphur_dioxide` | decimal(8,3) | 1-3 decimals | SO₂ concentration | µg/m³ |
| `carbon_monoxide` | decimal(10,3) | 1-3 decimals | CO concentration | µg/m³ |
| `aqi_value` | integer | - | Air quality index value | 0-500+ |

### Quality & Metadata Fields
| Field | Type | Description | Values |
|-------|------|-------------|--------|
| `is_valid` | boolean | Data validity flag | true/false |
| `quality_flag` | varchar | Quality control notes | 'OK', 'LOW_COVERAGE', etc |
| `completeness_pct` | decimal(5,2) | Data completeness percentage | 0-100 |
| `created_at` | timestamp | Record creation time | ISO 8601 |
| `updated_at` | timestamp | Last update time | ISO 8601 |

---

## 🏢 DIMENSION TABLE: dim_facility

**Purpose**: Facility master data
**Row Count**: 5 facilities

| Field | Type | Description |
|-------|------|-------------|
| `facility_key` | integer | **Primary Key** |
| `facility_code` | varchar | Unique facility identifier |
| `facility_name` | varchar | Facility name |
| `network_id` | varchar | Network identifier |
| `network_region` | varchar | Geographic region |
| `location_lat` | decimal(10,6) | Latitude |
| `location_lng` | decimal(10,6) | Longitude |
| `total_capacity_mw` | decimal(10,4) | Total capacity (MW) |
| `total_capacity_registered_mw` | decimal(10,4) | Registered capacity (MW) |
| `total_capacity_maximum_mw` | decimal(10,4) | Maximum capacity (MW) |
| `unit_fueltech_summary` | varchar | Fuel technology summary |
| `unit_status_summary` | varchar | Unit status summary |

**Sample Query**:
```sql
SELECT facility_key, facility_code, facility_name, network_region, location_lat, location_lng
FROM iceberg.gold.dim_facility
```

---

## 📅 DIMENSION TABLE: dim_date

**Purpose**: Date dimension for easy temporal analysis
**Row Count**: 93 days
**Range**: 2024-10-09 to 2024-12-10

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `date_key` | integer | **Primary Key** | 20241009 |
| `full_date` | date | Full date | 2024-10-09 |
| `year` | integer | Year | 2024 |
| `quarter` | integer | Quarter | 4 |
| `month` | integer | Month number | 10 |
| `month_name` | varchar | Month name | October |
| `week` | integer | Week number | 41 |
| `day_of_month` | integer | Day of month | 9 |
| `day_of_week` | integer | Day of week (1=Mon, 7=Sun) | 3 |
| `day_name` | varchar | Day name | Wednesday |
| `is_weekend` | boolean | Weekend flag | false |
| `is_holiday` | boolean | Holiday flag | false |
| `season` | varchar | Season | Fall |

**Sample Query**:
```sql
SELECT date_key, full_date, month_name, day_name, is_weekend
FROM iceberg.gold.dim_date
WHERE full_date >= DATE '2024-10-01'
ORDER BY full_date
```

---

## ⏰ DIMENSION TABLE: dim_time

**Purpose**: Time dimension for hourly analysis
**Row Count**: 24 hours

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `time_key` | integer | **Primary Key** | 1 (for 01:00) |
| `hour` | integer | Hour (0-23) | 1 |
| `minute` | integer | Minute (always 0) | 0 |
| `time_of_day` | varchar | Time period | Morning, Afternoon, Evening, Night |
| `is_peak_hour` | boolean | Peak hour flag | false |
| `daylight_period` | varchar | Daylight period | Sunrise, Morning, Noon, Afternoon, Sunset, Dusk, Night |

**Sample Query**:
```sql
SELECT time_key, hour, time_of_day, daylight_period, is_peak_hour
FROM iceberg.gold.dim_time
ORDER BY time_key
```

**Hours Reference**:
- **Night**: 0-5 (00:00-05:59)
- **Morning**: 6-11 (06:00-11:59)
- **Afternoon**: 12-17 (12:00-17:59)
- **Evening**: 18-23 (18:00-23:59)

---

## 🌍 DIMENSION TABLE: dim_aqi_category

**Purpose**: Air Quality Index standards and thresholds (EPA/WHO)
**Row Count**: 6 categories

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `aqi_category_key` | bigint | **Primary Key** | 1 |
| `aqi_category` | varchar | Category name | Good |
| `aqi_range_min` | integer | Min AQI value | 0 |
| `aqi_range_max` | integer | Max AQI value | 50 |
| `health_advisory` | varchar | Health advice | Air quality is satisfactory |
| `color_code` | varchar | Visualization color | #00e400 (Green) |
| `pm2_5_min_ug_m3` | decimal(8,3) | Min PM2.5 threshold | 0.000 |
| `pm2_5_max_ug_m3` | decimal(8,3) | Max PM2.5 threshold | 12.000 |

**AQI Categories**:
1. **Good** (0-50): Green - Air quality is satisfactory
2. **Satisfactory** (51-100): Yellow - Acceptable air quality
3. **Moderately Polluted** (101-200): Orange - Mild health effects
4. **Poor** (201-300): Red - Significant health effects
5. **Very Poor** (301-400): Purple - Severe health effects
6. **Severe** (401-500+): Maroon - Health alert

**Sample Query**:
```sql
SELECT aqi_category_key, aqi_category, aqi_range_min, aqi_range_max, color_code
FROM iceberg.gold.dim_aqi_category
ORDER BY aqi_range_min
```

---

## 🔗 JOIN PATTERN FOR POWER BI

**Complete denormalized view** - Join all dimensions to fact table:

```sql
SELECT 
  -- Fact table keys
  f.facility_key,
  f.date_key,
  f.time_key,
  f.aqi_category_key,
  
  -- Facility Details
  fac.facility_code,
  fac.facility_name,
  fac.network_region,
  fac.location_lat,
  fac.location_lng,
  fac.total_capacity_mw,
  
  -- Date Details
  d.full_date,
  d.year,
  d.month,
  d.month_name,
  d.day_name,
  d.is_weekend,
  d.season,
  
  -- Time Details
  t.hour,
  t.time_of_day,
  t.daylight_period,
  t.is_peak_hour,
  
  -- AQI Category Details
  aqi.aqi_category,
  aqi.color_code,
  
  -- Energy Metrics
  f.energy_mwh,
  f.power_avg_mw,
  
  -- Weather Metrics
  f.temperature_2m,
  f.humidity_2m,
  f.cloud_cover,
  f.wind_speed_10m,
  f.shortwave_radiation,
  
  -- Air Quality Metrics
  f.pm2_5,
  f.pm10,
  f.aqi_value,
  
  -- Quality Flags
  f.is_valid,
  f.quality_flag,
  f.completeness_pct
  
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_facility fac ON f.facility_key = fac.facility_key
LEFT JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
LEFT JOIN iceberg.gold.dim_time t ON f.time_key = t.time_key
LEFT JOIN iceberg.gold.dim_aqi_category aqi ON f.aqi_category_key = aqi.aqi_category_key
WHERE f.is_valid = true
  AND d.full_date >= DATE '2024-10-09'
  AND d.full_date <= DATE '2024-12-10'
ORDER BY d.full_date, t.hour, fac.facility_code
```

---

## ✅ Data Validation & Quality

| Metric | Value |
|--------|-------|
| **Total Records** | 11,085 |
| **Date Range** | 2024-10-09 to 2024-12-10 (93 days) |
| **Facilities** | 5 |
| **Valid Records** | 11,085 (100%) |
| **Completeness** | 100% avg |
| **Duplicates** | None (Unique key: facility_key + date_key + time_key) |

---

## 🔍 Sample Queries for Power BI

### 1. Daily Energy Summary
```sql
SELECT 
  d.full_date,
  fac.facility_name,
  ROUND(SUM(f.energy_mwh), 2) as daily_energy_mwh,
  ROUND(AVG(f.power_avg_mw), 2) as avg_power_mw,
  ROUND(AVG(f.temperature_2m), 2) as avg_temperature,
  ROUND(AVG(f.shortwave_radiation), 2) as avg_radiation
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_facility fac ON f.facility_key = fac.facility_key
LEFT JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
WHERE f.is_valid = true
GROUP BY d.full_date, fac.facility_name
ORDER BY d.full_date DESC, fac.facility_name
```

### 2. Hourly Data with Context
```sql
SELECT 
  d.full_date,
  t.hour,
  fac.facility_code,
  f.energy_mwh,
  f.temperature_2m,
  f.pm2_5,
  aqi.aqi_category,
  f.completeness_pct
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_facility fac ON f.facility_key = fac.facility_key
LEFT JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
LEFT JOIN iceberg.gold.dim_time t ON f.time_key = t.time_key
LEFT JOIN iceberg.gold.dim_aqi_category aqi ON f.aqi_category_key = aqi.aqi_category_key
WHERE f.is_valid = true
  AND d.full_date >= DATE '2024-11-01'
ORDER BY d.full_date DESC, t.hour DESC, fac.facility_code
LIMIT 1000
```

### 3. Air Quality Analysis
```sql
SELECT 
  aqi.aqi_category,
  COUNT(*) as record_count,
  ROUND(AVG(f.pm2_5), 2) as avg_pm2_5,
  ROUND(AVG(f.pm10), 2) as avg_pm10,
  ROUND(AVG(f.energy_mwh), 2) as avg_energy_during_category
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_aqi_category aqi ON f.aqi_category_key = aqi.aqi_category_key
WHERE f.is_valid = true
GROUP BY aqi.aqi_category, aqi.aqi_range_min
ORDER BY aqi.aqi_range_min
```

---

## 🚀 Power BI Connection String

### Trino ODBC Connection
```
Driver: Trino ODBC Driver
Host: localhost (or your Trino server IP)
Port: 8080
Catalog: iceberg
Schema: gold
```

### Direct SQL Connection
```sql
-- Change 'localhost' to your Trino server IP if remote
Host: localhost
Port: 8080
User: trino (or your configured user)
```

---

## 📝 Notes for Power BI Users

1. **Primary Table**: Use `iceberg.gold.fact_solar_environmental` as your main fact table
2. **Relationships**:
   - fact_solar_environmental.facility_key → dim_facility.facility_key
   - fact_solar_environmental.date_key → dim_date.date_key
   - fact_solar_environmental.time_key → dim_time.time_key
   - fact_solar_environmental.aqi_category_key → dim_aqi_category.aqi_category_key

3. **Date Range**: 2024-10-09 to 2024-12-10 (93 days of historical data)

4. **Key Metrics**:
   - `energy_mwh`: Primary KPI for energy generation
   - `power_avg_mw`: Average power during each hour
   - `temperature_2m`, `shortwave_radiation`: Key weather drivers
   - `pm2_5`, `aqi_value`: Air quality metrics

5. **Data Quality**: All records have `is_valid = true` and 100% completeness

---

**Document Created**: November 6, 2025
**Status**: ✅ Production Ready
