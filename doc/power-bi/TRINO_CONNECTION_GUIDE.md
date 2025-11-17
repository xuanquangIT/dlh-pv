# Trino Connection Guide for Power BI

## 🟢 Trino Status

**Status**: ✅ **HEALTHY** (Running since ~2 hours)

```
Container: trino (trinodb/trino:latest)
Port: 8081 (mapped to internal 8080)
Health Check: Passing
Uptime: 2 hours
```

---

## 🔧 Connection Details

### 1. Local Development (Same Machine)
```
Host: localhost
Port: 8081
Catalog: iceberg
```

### 2. Remote Connection (Different Machine)
```
Host: <your-server-ip>  # IP address of the machine running Docker
Port: 8081
Catalog: iceberg
Schema: gold
User: trino (default)
```

**Example**: If server IP is `192.168.1.100`
```
Host: 192.168.1.100
Port: 8081
```

---

## 🔌 Power BI Connection Methods

### Method 1: Using Trino ODBC Driver (Recommended)

#### Step 1: Install Trino ODBC Driver
- **Windows**: Download from [Trino website](https://trino.io/download.html)
- **Mac**: `brew install trino-odbc`
- **Linux**: Use your package manager

#### Step 2: Configure ODBC Connection in Power BI
1. Open Power BI Desktop
2. Click: **Get Data** → **More...** → Search for **ODBC**
3. Click **ODBC** and then **Connect**
4. Configure with details above
5. Click **OK**

### Method 2: Using PostgreSQL Connector (Alternative)

Since Trino has PostgreSQL wire protocol compatibility:

1. Open Power BI Desktop
2. Click: **Get Data** → **More...** → Search for **PostgreSQL**
3. Configure:
   ```
   Server: localhost (or your IP)
   Port: 8081
   Database: iceberg/gold
   User: trino
   Password: (leave blank for default)
   ```

### Method 3: Using SQL Server ODBC Driver

If you have SQL Server ODBC installed, it may work with Trino:

1. **Get Data** → **ODBC**
2. Create connection with Trino details
3. Query directly

---

## 📊 Sample Power BI Queries

### Query 1: Simple Daily Summary
```sql
SELECT 
  d.full_date,
  fac.facility_name,
  SUM(f.energy_mwh) as daily_energy_mwh,
  AVG(f.temperature_2m) as avg_temp,
  AVG(f.shortwave_radiation) as avg_radiation
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_facility fac ON f.facility_key = fac.facility_key
LEFT JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
WHERE f.is_valid = true
GROUP BY d.full_date, fac.facility_name
```

### Query 2: Hourly Breakdown
```sql
SELECT 
  d.full_date,
  t.hour,
  t.time_of_day,
  fac.facility_code,
  f.energy_mwh,
  f.temperature_2m,
  f.pm2_5,
  aqi.aqi_category
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_facility fac ON f.facility_key = fac.facility_key
LEFT JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
LEFT JOIN iceberg.gold.dim_time t ON f.time_key = t.time_key
LEFT JOIN iceberg.gold.dim_aqi_category aqi ON f.aqi_category_key = aqi.aqi_category_key
WHERE f.is_valid = true
```

### Query 3: Air Quality Trends
```sql
SELECT 
  d.full_date,
  fac.facility_name,
  aqi.aqi_category,
  AVG(f.pm2_5) as avg_pm2_5,
  AVG(f.pm10) as avg_pm10,
  COUNT(*) as measurement_count
FROM iceberg.gold.fact_solar_environmental f
LEFT JOIN iceberg.gold.dim_facility fac ON f.facility_key = fac.facility_key
LEFT JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
LEFT JOIN iceberg.gold.dim_aqi_category aqi ON f.aqi_category_key = aqi.aqi_category_key
GROUP BY d.full_date, fac.facility_name, aqi.aqi_category
```

---

## 🎯 Recommended Data Model for Power BI

### Main Table (Import into Power BI)
```
Table: FACT_SOLAR_ENVIRONMENTAL_DENORMALIZED

Key columns:
- facility_code
- full_date
- hour
- energy_mwh (Main Measure)
- temperature_2m
- shortwave_radiation
- pm2_5
- aqi_value
- is_valid
```

### Approach: Direct Query vs Import

**Option A: Direct Query (Real-time)**
- Pros: Always fresh data
- Cons: Slower for large dashboards
- Recommended for: Small datasets, specific reports

**Option B: Import (Cached)**
- Pros: Fast dashboards, better performance
- Cons: Data not real-time
- Recommended for: Daily/hourly refresh, large dashboards

**Option C: Hybrid (Mixed)**
- Import small dimensions (facility, date)
- Direct query fact table
- Best performance/freshness balance

---

## 🚀 Quick Test Connection

### Via Command Line (Test Connectivity)
```bash
# From your machine running Docker
curl http://localhost:8081/v1/info

# From another machine, replace localhost with server IP
curl http://<your-server-ip>:8081/v1/info
```

**Expected Response**:
```json
{
  "nodeVersion": {
    "version": "...",
    "apiVersion": "...",
    ...
  }
}
```

### Via Trino CLI
```bash
# Connect to Trino
trino --server http://localhost:8081 --catalog iceberg --schema gold

# Simple test query
SELECT COUNT(*) as row_count FROM fact_solar_environmental;
```

---

## 🔍 Troubleshooting

### Issue 1: Connection Refused
```
Error: Connection to localhost:8081 refused
```
**Solution**:
1. Check Trino container is running: `docker ps | grep trino`
2. Check port mapping: `docker ps` should show `0.0.0.0:8081->8080`
3. Restart if needed: `docker compose -f docker/docker-compose.yml restart trino`

### Issue 2: Authentication Failed
```
Error: authentication failed
```
**Solution**:
1. Trino runs without authentication by default
2. Leave username/password blank or use 'trino'
3. Check Trino configuration in `/docker/` folder

### Issue 3: Cannot Find ODBC Driver
```
Error: ODBC driver not installed
```
**Solution**:
1. Install Trino ODBC driver for your OS
2. Or use PostgreSQL driver as fallback
3. Or use direct SQL query method

### Issue 4: Slow Query Performance
**Solutions**:
1. Import dimension tables to Power BI for faster joins
2. Pre-aggregate data in Power BI queries
3. Use date filters to limit data range
4. Consider creating materialized views in Trino

### Issue 5: Data Not Updating
**Solution**:
1. If using Import: Set up scheduled refresh in Power BI
2. If using Direct Query: Check Trino data freshness
3. Verify data load jobs are running (see `doc/gold/` folder)

---

## 📋 Connection String Examples

### ODBC Connection String
```
Driver={Trino ODBC Driver};Server=localhost;Port=8081;Catalog=iceberg;Schema=gold;User=trino
```

### Connection String with Special Characters
```
Driver={Trino ODBC Driver};Server=localhost;Port=8081;Catalog=iceberg;Schema=gold;User=trino;SSL=false
```

---

## ✅ Data Availability

| Property | Value |
|----------|-------|
| **Total Records** | 11,085 |
| **Date Range** | 2024-10-09 to 2024-12-10 |
| **Facilities** | 5 |
| **Hourly Granularity** | 24 hours/day |
| **Data Quality** | 100% valid |
| **Last Updated** | Today |

---

## 📞 Support

### For Trino Issues
- Check container logs: `docker logs trino`
- Trino UI: http://localhost:8081/ui/
- Check connectivity: `telnet localhost 8081`

### For Power BI Issues
1. Verify connection string format
2. Test ODBC driver separately
3. Check Power BI logs in: `%APPDATA%\Microsoft\Power BI Desktop\`

### Test Queries (Copy & Paste Ready)
```sql
-- Test 1: Connection test
SELECT 1 as test;

-- Test 2: Count records
SELECT COUNT(*) FROM iceberg.gold.fact_solar_environmental;

-- Test 3: Facilities
SELECT DISTINCT facility_name FROM iceberg.gold.dim_facility;

-- Test 4: Date range
SELECT MIN(full_date) as min_date, MAX(full_date) as max_date 
FROM iceberg.gold.dim_date;
```

---

**Last Updated**: November 6, 2025  
**Status**: ✅ Production Ready  
**Trino Status**: ✅ Healthy (Running)
