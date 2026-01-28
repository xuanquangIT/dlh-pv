docker compose --profile core --profile ml --profile orchestrate up -d

1️⃣ Bronze Layer (Raw Ingestion)

# Navigate to docker folder
cd /home/pvlakehouse/dlh-pv/docker

# 1. Load Facilities (master data)
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/cli.py facilities

# 2. Load Facility Energy (timeseries data)
# Note: Energy API uses UTC. Adjust for AEST (UTC+10) to match Weather/Air Quality
# Local: 2025-01-01 00:00 AEST = 2024-12-31 14:00 UTC
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/cli.py energy \
  --mode backfill \
  --date-start 2024-12-31T14:00:00 \
  --date-end 2025-01-08T09:59:59

# 3. Load Facility Weather
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/cli.py weather \
  --mode backfill \
  --start 2025-01-01 --end 2025-01-07

# 4. Load Facility Air Quality
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/cli.py air_quality \
  --mode backfill \
  --start 2025-01-01 --end 2025-01-07
  


2️⃣ Silver Layer (Cleansing)


# 1. Clean Facility Master
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py facility_master --mode full

# 2. Clean Hourly Energy
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode full

# 3. Clean Hourly Weather
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_weather --mode full

# 4. Clean Hourly Air Quality
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_air_quality --mode full
  
  3️⃣ Gold Layer (Dimensional Model) 
  
  
  # === DIMENSIONS (run first) ===

# 1. dim_facility
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_facility --mode full

# 2. dim_aqi_category
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_aqi_category --mode full

# 3. dim_date
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_date --mode full

# 4. dim_time
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_time --mode full

# === FACTS (run after dimensions) ===

# 5. fact_solar_environmental
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental --mode full




cd /home/pvlakehouse/dlh-pv && bash src/pv_lakehouse/etl/scripts/export-data.sh bronze silver



# Incremental
# 2. Load Facility Energy (timeseries data)
# Note: Adjust timestamps for AEST timezone (UTC+10)
# Local: 2025-01-03 00:00 AEST = 2025-01-02 14:00 UTC
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/cli.py energy \
  --mode incremental \
  --date-start 2025-01-02T14:00:00 \
  --date-end 2025-01-07T09:59:59

# 3. Load Facility Weather
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/cli.py weather \
  --mode incremental \
  --start 2025-01-03 --end 2025-01-06

# 4. Load Facility Air Quality
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/cli.py air_quality \
  --mode incremental \
  --start 2025-01-03 --end 2025-01-06



Bronze Energy:       2024-12-31 14:00 UTC → 2025-01-08 09:59 UTC
Bronze Weather:      2025-01-01 00:00 AEST → 2025-01-07 23:00 AEST
Bronze Air Quality:  2025-01-01 00:00 AEST → 2025-01-07 23:00 AEST

(All covering the same real-world time period)


Based on the code analysis, OpenElectricity API DOES return timestamps in local network time, just like OpenMeteo:

Key Evidence:
Network Timezone Mapping (parsers.py:1-100):

API Documentation in Code (parsers.py:50-80):

parse_naive_datetime(): "Datetime values must be timezone naive (network local time)"
Input/output are both in local network time, not UTC
Default Date Range (client.py:520-535):

API Request Format (client.py:570-585):

Conclusion:
✅ OpenElectricity API returns local network time (AEST UTC+10 for NEM facilities)
✅ OpenMeteo API also returns local time when configured with facility timezone
✅ No timezone conversion needed - both APIs already use the same timezone

The previous CHEATSHEET adjustments (subtracting 10 hours) were incorrect. The current data mismatch likely has a different cause - possibly:

Original Energy data was loaded with wrong date parameters
Transform logic has timezone issues
Data quality/availability gaps in one of the APIs
You should revert the timezone adjustments in the CHEATSHEET and investigate the root cause of the data misalignment.