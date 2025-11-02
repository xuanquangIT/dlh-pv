# Hướng Dẫn Cài Đặt Nhanh

## 🚀 Quick Start (5 phút)

### 1. Yêu Cầu

```bash
# Check Docker
docker --version        # 20.10+
docker compose version  # 2.0+

# Check Python (for development)
python3 --version      # 3.11+
```

### 2. Clone & Setup

```bash
# Clone repository
git clone https://github.com/xuanquangIT/dlh-pv.git
cd dlh-pv

# Copy environment file
cp .env.example docker/.env

# View default config (optional)
cat docker/.env
```

### 3. Start Services

```bash
# Navigate to docker folder
cd docker

# Start core services (MinIO, PostgreSQL, Iceberg, Trino)
docker compose --profile core up -d

# Wait for health checks (30-60 seconds)
./scripts/health-check.sh

# You should see: ✓ PASS: All services healthy
```

### 4. Verify Installation

```bash
# List running containers
docker compose ps

# Check MinIO
open http://localhost:9001
# Username: pvlakehouse
# Password: pvlakehouse

# Check Trino UI
open http://localhost:8081

# Quick test query
docker compose exec -i trino trino --execute "SELECT 1"
```

## 📋 Service Status

| Service | URL | Status |
|---------|-----|--------|
| MinIO Console | http://localhost:9001 | ✅ Ready |
| Trino UI | http://localhost:8081 | ✅ Ready |
| PostgreSQL | localhost:5432 | ✅ Ready |
| Spark Master | http://localhost:8080 | ✅ Ready |
| pgAdmin | http://localhost:5050 | ✅ Ready |

## ⚙️ Available Profiles

```bash
# Core services only (default)
docker compose --profile core up -d

# With Spark cluster
docker compose --profile core --profile spark up -d

# With ML tools (MLflow)
docker compose --profile core --profile ml up -d

# With Orchestration (Prefect)
docker compose --profile core --profile orchestrate up -d

# Everything (full stack)
docker compose --profile core --profile spark --profile ml --profile orchestrate up -d
```

## 🔑 Default Credentials

```bash
# MinIO
Username: pvlakehouse
Password: pvlakehouse

# PostgreSQL
Username: pvlakehouse
Password: pvlakehouse
Database: iceberg_catalog

# pgAdmin
Email: pvlakehouse@localhost
Password: pvlakehouse
```

⚠️ **Production**: Change all credentials in `docker/.env`

## 📊 First Steps After Setup

### 1. Check MinIO Buckets

```bash
# Connect to MinIO CLI
docker run --rm --network dlhpv_data-net \
  --entrypoint /bin/sh \
  minio/mc:latest -c '
  mc alias set local http://minio:9000 pvlakehouse pvlakehouse
  mc ls local
'

# Expected output:
# [2025-01-15 10:30:00 UTC]     0B lakehouse/
# [2025-01-15 10:30:00 UTC]     0B mlflow/
```

### 2. Query Trino Directly

```bash
# Connect to Trino CLI
docker compose exec -it trino trino --catalog iceberg --schema bronze

# List tables (if any exist)
trino:bronze> SHOW TABLES;

# Test query
trino:bronze> SELECT 1 as test_value;

# Exit
trino:bronze> exit
```

### 3. View PostgreSQL Catalog

```bash
# Connect to PostgreSQL
docker compose exec -it postgres psql -U pvlakehouse -d iceberg_catalog

# Check Iceberg tables
SELECT * FROM iceberg_tables LIMIT 5;

# Exit
\q
```

### 4. Access pgAdmin

1. Open http://localhost:5050
2. Login with: pvlakehouse@localhost / pvlakehouse
3. Add server (if needed):
   - Host: postgres
   - Port: 5432
   - Username: pvlakehouse
   - Password: pvlakehouse

## 🐛 Common Issues & Fixes

### Issue: Containers won't start

```bash
# Check logs
docker compose logs minio
docker compose logs postgres

# Ensure ports are available
lsof -i :9000   # MinIO API
lsof -i :5432   # PostgreSQL
lsof -i :8081   # Trino
```

### Issue: Health check fails

```bash
# Wait longer (sometimes first startup takes longer)
sleep 60
./scripts/health-check.sh

# Or restart a specific service
docker compose restart postgres
docker compose restart minio
```

### Issue: Cannot access MinIO console

```bash
# Check if service is healthy
docker compose ps minio

# Check status inside container
docker compose exec minio curl -f http://localhost:9000/minio/health/ready

# If unhealthy, check logs
docker compose logs minio
```

### Issue: Out of memory

```bash
# Stop everything
docker compose down

# Increase Docker Desktop memory to 8GB+
# Restart Docker daemon
# Start services again
docker compose --profile core up -d
```

## 🔄 Next Steps

### Option 1: Create Bronze Tables

```bash
# Navigate to tests folder
cd ../../tests

# Run table creation
python test_bronze_tables_complete.py --create

# Verify tables in Trino
cd ../docker
docker compose exec -it trino trino --catalog iceberg --schema bronze

# In Trino CLI:
trino:bronze> SHOW TABLES;
trino:bronze> SELECT * FROM oe_facilities_raw LIMIT 5;
```

### Option 2: Explore Documentation

- 📖 [Architecture Overview](../architecture/overview.md)
- 🏗️ [System Architecture](../architecture/system-architecture.md)
- 🛠️ [Infrastructure Setup](../infrastructure/overview.md)
- 📊 [Data Model](../data-model/bronze-layer.md)

### Option 3: Start Development

- 👨‍💻 [Development Guide](../development/development.md)
- 🐍 [ETL Development](../development/etl-development.md)

## 📝 Environment Configuration

### Key Environment Variables

```bash
# Authentication
PV_USER=pvlakehouse
PV_PASSWORD=pvlakehouse

# Storage
S3_WAREHOUSE_BUCKET=lakehouse
S3_MLFLOW_BUCKET=mlflow

# Service Ports
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
TRINO_PORT=8081
PREFECT_PORT=4200
MLFLOW_PORT=5000

# Database
POSTGRES_USER=pvlakehouse
POSTGRES_PASSWORD=pvlakehouse
POSTGRES_DB=iceberg_catalog

# Service Credentials
SPARK_SVC_ACCESS_KEY=spark_svc
SPARK_SVC_SECRET_KEY=spark_secret_change_me
TRINO_SVC_ACCESS_KEY=trino_svc
TRINO_SVC_SECRET_KEY=trino_secret_change_me
MLFLOW_SVC_ACCESS_KEY=mlflow_svc
MLFLOW_SVC_SECRET_KEY=mlflow_secret_change_me

# Images
MINIO_IMAGE=minio/minio:latest
POSTGRES_IMAGE=postgres:15
TRINO_IMAGE=trinodb/trino:latest
MLFLOW_IMAGE=ghcr.io/mlflow/mlflow:latest
```

### Customization

Edit `docker/.env` to:
- Change credentials
- Modify ports (if conflicts)
- Adjust resource limits
- Pin specific versions

## 🛑 Stopping Services

```bash
# Stop all services (keep volumes)
docker compose down

# Stop and remove volumes (full cleanup)
docker compose down -v

# Stop specific profile
docker compose --profile core down
```

## ✅ Verification Checklist

- [x] Docker & Docker Compose installed
- [x] `.env` file copied
- [x] Services started with `docker compose up`
- [x] Health checks passing
- [x] MinIO console accessible
- [x] Trino responds to queries
- [x] PostgreSQL accessible
- [x] Ready for data loading

## 📚 Related Documentation

- [Detailed Setup Guide](detailed-setup.md)
- [Environment Configuration](environment-configuration.md)
- [Troubleshooting](troubleshooting.md)
- [Infrastructure Setup](../infrastructure/overview.md)

---

**Need Help?**
- 📖 Check [Troubleshooting Guide](troubleshooting.md)
- 💬 Review [Docker Setup Guide](../../docker/README-SETUP.md)
- 🐛 Check container logs: `docker compose logs -f <service>`

**Next:** Read [Detailed Setup Guide](detailed-setup.md) for production deployment steps.
