1️⃣ Bronze Layer (Raw Ingestion)

# Navigate to docker folder
cd /home/pvlakehouse/dlh-pv/docker

# 1. Load Facilities (master data)
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/cli.py facilities

# 2. Load Facility Energy (timeseries data)
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/cli.py energy \
  --mode backfill \
  --date-start 2025-01-01T00:00:00 \
  --date-end 2025-01-07T23:59:59

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