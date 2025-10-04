# PV Lakehouse (Starter)

A minimal, batteries‑included starter for building a small lakehouse ETL on your laptop or a single VM. It uses open components and clear conventions so you can evolve from **raw → normalized → curated** data with confidence.

> **Stack**: MinIO (S3‑compatible object store) · Apache Iceberg (REST Catalog) · Spark (batch) · Trino (SQL) · Prefect (orchestration) · MLflow (tracking) · Postgres (metastore/auth)
> **Medallion**: **Bronze** (raw) → **Silver** (normalized) → **Gold** (curated/KPI)

---

## What’s inside

* **docker/etl-runner** — containerized Python runner (ETL image)
* **docker/minio/init** — MinIO init scripts (buckets/policies)
* **infra/** — infra‑as‑code (compose, catalogs, network)
* **flows/** — sample Prefect flows (ingest & transform)
* **sql/trino/** — DDL for schemas/tables
* **src/pv_lakehouse/etl/** — ETL modules (Spark/PySpark utilities)
* **tests/** — basic smoke tests (pytest)

---

## Quick start

These steps assume Linux/macOS/WSL2 with Python ≥ **3.11** and Docker.

### Option A — Local Python (no Docker)

```bash
# 1) Clone and enter
git clone https://github.com/<you>/pv-lakehouse-starter.git
cd pv-lakehouse-starter

# 2) Create env
python -m venv .venv
source .venv/bin/activate

# 3) Install deps
pip install -r docker/etl-runner/requirements.txt

# 4) Run tests
pytest -q
```

### Option B — Dockerized

```bash
# Build the ETL runner image
docker build -t pv/etl-runner:dev ./docker/etl-runner
```

### Bring up the services (Compose)

If you use Compose files in `infra/` (recommended):

```bash
# Copy environment template and edit placeholders
cp .env.example .env
# Start the stack (MinIO, Postgres, Iceberg REST, Trino, Spark, Prefect, MLflow)
docker compose -f infra/compose.yaml up -d
```

**Service UIs (defaults, change in `.env`/compose):**

* MinIO Console → [http://localhost:9001](http://localhost:9001)
* Trino Web UI → [http://localhost:8080](http://localhost:8080)
* Prefect UI → [http://localhost:4200](http://localhost:4200)
* MLflow UI → [http://localhost:5001](http://localhost:5001)

### Initialize MinIO buckets

```bash
# Run the init container/script to create buckets and policies
./docker/minio/init/create-buckets.sh  # or: docker compose run --rm minio-init
```

### Create schemas (Trino)

```sql
-- Using your SQL client or Trino UI
CREATE SCHEMA IF NOT EXISTS iceberg.lh WITH (location = 's3a://lakehouse/lh/');
CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3a://lakehouse/lh/bronze/');
CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3a://lakehouse/lh/silver/');
CREATE SCHEMA IF NOT EXISTS iceberg.gold   WITH (location = 's3a://lakehouse/lh/gold/');
```

### Run a sample flow (Prefect)

```bash
# Start Prefect server/agent if not already
prefect server start &
prefect worker start --pool default-agent &

# From the project root, run a flow locally (example)
python -m pv_lakehouse.etl.flows.ingest_oe_bronze --start 2024-01-01 --end 2024-12-31
```

### Query data (Trino)

```sql
SHOW SCHEMAS FROM iceberg;            -- verify catalogs
SHOW TABLES FROM iceberg.bronze;      -- see raw tables
SELECT * FROM iceberg.gold.daily_plant_kpi ORDER BY ds DESC LIMIT 10;  -- sanity
```

---

## Configuration

Use **`.env.example`** as your source of truth. Copy it to `.env` and fill in values. **Never commit real secrets.**

### Required environment variables (typical)

```ini
# MinIO / S3A
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=<minio_access_key>
MINIO_SECRET_KEY=<minio_secret_key>
S3_REGION=us-east-1
S3_PATH_STYLE=true

# Iceberg REST Catalog
ICEBERG_REST_URI=http://iceberg-rest:8181
ICEBERG_WAREHOUSE=s3a://lakehouse/
ICEBERG_CATALOG=iceberg

# Trino
TRINO_HOST=trino
TRINO_PORT=8080
TRINO_USER=admin

# Spark
SPARK_MASTER_URL=spark://spark:7077
SPARK_APP_NAME=pv-lakehouse-etl

# Prefect
PREFECT_API_URL=http://prefect:4200/api
PREFECT_WORK_POOL=default-agent

# MLflow
MLFLOW_TRACKING_URI=http://mlflow:5001

# Postgres (for tracking/metadata if used)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=pv
POSTGRES_USER=pv
POSTGRES_PASSWORD=<postgres_password>

# Optional external APIs
OPEN_NEM_API_URL=https://api.openelectricity.org.au/v4
OPEN_NEM_PRIMARY=PRIMARY_KEY_HERE
OPEN_NEM_SECONDARY=BACKUP_KEY_HERE

OPENMETEO_WEATHER_API_URL=https://archive-api.open-meteo.com/v1/archive
OPENMETEO_AIRQUALITY_API_URL=https://air-quality-api.open-meteo.com/v1/air-quality
```

### Spark configuration tips

* Use **S3A path‑style**: `-Dfs.s3a.path.style.access=true` when needed.
* Supply endpoint/credentials via env or Hadoop XMLs (mounted into the Spark driver/executors).
* Prefer **partition by day** to keep file counts manageable.

---

## Data model & conventions

* **Time**: store canonical timestamps in **UTC** as `ts_utc`.
* **Partition**: `PARTITIONED BY (days(ts_utc))` for all hourly facts.
* **Sort/order**: by `(plant_id, ts_utc)` where applicable.
* **Metadata**: include `_ingest_time`, `_source`, `_hash` in Bronze.

### Bronze (raw)

* `bronze.oe_facilities_raw`
* `bronze.oe_generation_hourly_raw`
* `bronze.om_weather_hourly_raw`

### Silver (normalized)

* `silver.dim_plant` — deduplicated facility/plant metadata
* `silver.fact_generation_hourly` — normalized energy/power per plant per hour
* `silver.fact_weather_hourly` — normalized GHI/DNI/DHI/temp/wind/etc.

### Gold (curated)

* `gold.plant_weather_hourly` — joined generation + weather
* `gold.daily_plant_kpi` — daily KPIs: `energy_mwh`, capacity factor `cf`, missingness, latency

---

## ETL flows (Prefect)

> Flows live under `flows/` (thin registration) and `src/pv_lakehouse/etl/` (logic).

* **`ingest_oe_bronze`** — fetch OpenElectricity facilities + hourly generation (chunked), store to Bronze.
* **`ingest_om_bronze`** — fetch hourly weather for plant coordinates, store to Bronze.
* **`silver_normalize_*`** — transform Bronze → Silver (types, timezones, dedup, units).
* **`gold_build_*`** — join and compute daily KPIs.

### Scheduling (example)

```bash
# Register and schedule deployments (cron examples)
prefect deployment build flows/ingest_oe_bronze.py:flow -n ingest-oe -q default
prefect deployment apply ingest_oe_bronze-deployment.yaml
prefect deployment set-schedule ingest-oe "0 * * * *"     # hourly top of hour

prefect deployment build flows/ingest_om_bronze.py:flow -n ingest-om -q default
prefect deployment apply ingest_om_bronze-deployment.yaml
prefect deployment set-schedule ingest-om "5 * * * *"     # hourly +5m

prefect deployment build flows/silver_to_gold.py:flow -n s2g -q default
prefect deployment apply silver_to_gold-deployment.yaml
prefect deployment set-schedule s2g "10 * * * *"          # hourly +10m
```

---

## Development

### Code style

* Python: **Black** + **Ruff** (optional) and type hints. Add a `pyproject.toml` to enforce.
* Commit hooks: consider `pre-commit` for format/lint on commit.

### Tests

```bash
pytest -q            # fast smoke tests
pytest -k silver     # run subset
```

---

## CI/CD (optional)

* **GitHub Actions**: run tests and build the ETL image on each PR/push.
* **GHCR**: push images to `ghcr.io/<org>/pv-lakehouse:<tag>`.
* Pin base images by **digest** (`@sha256:…`) for reproducibility.

Example job steps (pseudocode): checkout → setup Python → install deps → pytest → login GHCR → buildx → push.

---

## Security & secrets

* Keep real credentials **out of git**. Only commit `.env.sample` with placeholders.
* Use Docker/Compose secrets or CI secret stores for runtime.
* Scope access keys to the MinIO bucket and rotate regularly.

---

## Troubleshooting

* **Trino can’t see tables**: ensure Iceberg REST Catalog is reachable; check `catalog` properties and schema locations.
* **S3A/MinIO errors**: verify endpoint URL and `S3_PATH_STYLE=true`. Check network/ports.
* **Spark write failures**: confirm credentials on both driver & executors; avoid tiny files (enable compaction later).
* **Timezones**: always convert to UTC on ingest; persist local time in a separate column if needed.

---

## Roadmap

* Add compaction/expire manifests tasks (weekly).
* Add data quality checks (Great Expectations or custom checks).
* Add Power BI/ODBC example against Trino.
* Add MLflow example notebook and model registry demo.

---

## License

Choose a license that fits your use (e.g., Apache‑2.0 or MIT). Add `LICENSE` at the repo root.

---

## Acknowledgements

This starter uses only open‑source, cloud‑agnostic components so you can run locally or on any VM.
