# 🐳 PV Lakehouse - Docker Setup Guide

> Complete guide to deploying and managing the PV Lakehouse platform using Docker.

---

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Architecture](#-architecture)
- [Service Profiles](#-service-profiles)
- [Configuration](#-configuration)
- [Running ETL Pipelines](#-running-etl-pipelines)
- [Web Interfaces](#-web-interfaces)
- [Troubleshooting](#-troubleshooting)
- [Production Considerations](#-production-considerations)

---

## ⚡ Quick Start

### Prerequisites

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| Docker | 20.10+ | 24.0+ |
| Docker Compose | 2.0+ | 2.20+ |
| RAM | 8GB | 16GB+ |
| Disk Space | 20GB | 50GB+ |
| CPU | 4 cores | 8+ cores |

### 1. Clone & Configure

```bash
# Clone repository
git clone https://github.com/yourusername/dlh-pv.git
cd dlh-pv

# Setup environment
cp docker/.env.example docker/.env

# Create symlink for root access (optional)
ln -sf docker/.env .env
```

### 2. Start Services

```bash
cd docker

# Start core services (data engineering)
docker compose --profile core up -d

# Verify all services are healthy
./scripts/health-check.sh
```

### 3. Verify Installation

```bash
# Check container status
docker compose ps

# Test Trino connection
docker exec -it trino trino --execute "SHOW CATALOGS"

# Expected output: iceberg, system
```

**That's it!** 🎉 Your lakehouse is ready.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PV LAKEHOUSE PLATFORM                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌───────────┐  │
│   │   MinIO     │    │ PostgreSQL  │    │    Trino    │    │  pgAdmin  │  │
│   │   (S3)      │    │  (Catalog)  │    │   (Query)   │    │   (UI)    │  │
│   │  :9000/9001 │    │    :5432    │    │    :8081    │    │   :5050   │  │
│   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └───────────┘  │
│          │                  │                  │                          │
│          └──────────────────┴──────────────────┘                          │
│                             │                                              │
│   ┌─────────────────────────┴─────────────────────────┐                   │
│   │              Apache Iceberg (Table Format)         │                   │
│   └─────────────────────────┬─────────────────────────┘                   │
│                             │                                              │
│   ┌─────────────┐    ┌──────┴──────┐                                      │
│   │ Spark Master│◄───│Spark Worker │                                      │
│   │    :4040    │    │   (N pods)  │                                      │
│   └─────────────┘    └─────────────┘                                      │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│   OPTIONAL SERVICES (--profile ml / --profile orchestrate)                  │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                   │
│   │   MLflow    │    │   Prefect   │    │   Prefect   │                   │
│   │  (ML Ops)   │    │   Server    │    │   Agent     │                   │
│   │    :5000    │    │    :4200    │    │             │                   │
│   └─────────────┘    └─────────────┘    └─────────────┘                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
External APIs ──► Bronze (Raw) ──► Silver (Clean) ──► Gold (Analytics)
     │                │                │                    │
     │                ▼                ▼                    ▼
     │           MinIO S3         MinIO S3             MinIO S3
     │           (Parquet)        (Parquet)            (Parquet)
     │                │                │                    │
     └────────────────┴────────────────┴────────────────────┘
                              │
                              ▼
                    PostgreSQL (Iceberg Catalog)
```

---

## 🎯 Service Profiles

| Profile | Services | Use Case |
|---------|----------|----------|
| `core` | MinIO, PostgreSQL, Spark, Trino, pgAdmin | Data engineering |
| `ml` | MLflow | Machine learning tracking |
| `orchestrate` | Prefect Server, Prefect Agent | Workflow automation |

### Start Commands

```bash
# Core only (recommended for development)
docker compose --profile core up -d

# Core + ML tracking
docker compose --profile core --profile ml up -d

# Everything
docker compose --profile core --profile ml --profile orchestrate up -d
```

### Stop Commands

```bash
# Stop all services
docker compose down

# Stop and remove volumes (⚠️ deletes all data)
docker compose down -v
```

---

## ⚙️ Configuration

### Environment Variables

All configuration is in `docker/.env`. Key sections:

| Section | Variables | Description |
|---------|-----------|-------------|
| **Credentials** | `PV_USER`, `PV_PASSWORD` | Global defaults |
| **PostgreSQL** | `POSTGRES_*` | Database settings |
| **MinIO** | `MINIO_*`, `S3_*` | Object storage |
| **Spark** | `SPARK_*` | Performance tuning |
| **Ports** | `*_PORT` | Service ports |

### Spark Tuning Guide

Adjust based on your system:

| System RAM | `SPARK_WORKER_MEMORY` | `SPARK_EXECUTOR_MEMORY` | `SPARK_SHUFFLE_PARTITIONS` |
|------------|----------------------|------------------------|---------------------------|
| 8GB | 2G | 2g | 16 |
| 16GB | 6G | 4g | 32 |
| 32GB | 12G | 8g | 64 |
| 64GB+ | 24G | 16g | 128-200 |

---

## 🚀 Running ETL Pipelines

### Using Docker Compose Exec

```bash
# Basic format
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/<layer>/<script>.py [args]
```

### Using Helper Script

```bash
# From project root
./src/pv_lakehouse/etl/scripts/spark-submit.sh \
  src/pv_lakehouse/etl/bronze/load_facilities.py
```

### Example: Full Pipeline

```bash
# 1. Bronze Layer
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facilities.py

docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facility_timeseries.py \
  --mode backfill --date-start 2025-01-01T00:00:00 --date-end 2025-12-31T23:59:59

# 2. Silver Layer
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/silver/cli.py hourly_energy --mode full

# 3. Gold Layer
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py dim_date --mode full

docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/gold/cli.py fact_solar_environmental --mode full
```

---

## 🌐 Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | `pvlakehouse` / `pvlakehouse` |
| **Spark Master UI** | http://localhost:4040 | — |
| **Trino UI** | http://localhost:8081 | — |
| **MLflow UI** | http://localhost:5000 | — |
| **pgAdmin** | http://localhost:5050 | `admin@example.com` / `pvlakehouse` |
| **Prefect UI** | http://localhost:4200 | — |

---

## 🔧 Troubleshooting

### Service Status

```bash
# Check all containers
docker compose ps

# View logs for specific service
docker compose logs spark-master --tail 100 -f

# Check health status
./scripts/health-check.sh
```

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Container exits immediately | Port conflict | Check `docker compose logs <service>` |
| Spark job OOM | Insufficient memory | Increase `SPARK_EXECUTOR_MEMORY` |
| Cannot connect to Trino | Service not ready | Wait 30s, run health check |
| MinIO access denied | Wrong credentials | Verify `.env` settings |

### Reset Everything

```bash
# Stop and remove all data
docker compose down -v

# Remove dangling images
docker image prune -f

# Fresh start
docker compose --profile core up -d --build
```

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs spark-master -f
docker compose logs trino -f
docker compose logs postgres -f
```

---

## 🔒 Production Considerations

### Security Checklist

- [ ] Change all default passwords in `.env`
- [ ] Use Docker secrets instead of environment variables
- [ ] Enable TLS/SSL for all services
- [ ] Configure network policies
- [ ] Set up proper backup procedures
- [ ] Enable audit logging

### Backup Procedures

```bash
# Backup PostgreSQL (Iceberg catalog)
docker compose exec postgres pg_dump -U pvlakehouse iceberg_catalog > backup.sql

# Backup MinIO data
mc mirror minio/lakehouse ./backup/lakehouse/
```

### Monitoring

- **Spark**: Monitor via Spark UI (`:4040`)
- **Trino**: Monitor via Trino UI (`:8081`)
- **MinIO**: Monitor via MinIO Console (`:9001`)
- **Logs**: Use `docker compose logs` or ELK stack

---

## 📁 File Structure

```
docker/
├── docker-compose.yml       # Main orchestration file
├── .env                     # Environment configuration (DO NOT COMMIT)
├── .env.example            # Template for .env
├── README-SETUP.md         # This file
│
├── postgres/
│   ├── postgres-init.sh    # Database initialization
│   └── postgres-init.sql   # Schema creation
│
├── spark/
│   ├── Dockerfile          # Custom Spark image with S3A
│   ├── requirements.txt    # Python dependencies
│   └── core-site.xml       # Hadoop configuration
│
├── trino/
│   └── catalog/
│       └── iceberg.properties  # Iceberg catalog config
│
└── scripts/
    ├── health-check.sh     # Comprehensive health check
    └── create-sample-data.sh
```

---

## 📚 Additional Resources

- **ETL Operations**: See `src/pv_lakehouse/etl/scripts/CHEATSHEET_GUIDE.md`
- **Schema Design**: See `doc/schema/`
- **Architecture Docs**: See `doc/architecture/`
- **Main README**: See project root `README.md`

---

*Last updated: January 2026*
