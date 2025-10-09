# PV Lakehouse

A production-ready data lakehouse platform for building ETL pipelines on your laptop or a single VM. Built with open-source components and clear conventions for evolving from raw → normalized → curated data with confidence.

**Stack**: MinIO (S3-compatible object store) • Apache Iceberg (table format) • Spark (batch processing) • Trino (SQL engine) • Prefect (orchestration) • MLflow (ML tracking) • PostgreSQL (metadata)

**Medallion Architecture**: Bronze (raw) → Silver (normalized) → Gold (curated/analytics)

---

## 📁 Repository Structure

```
dlh-pv/
├── docker/                      # Docker Compose stack & services
│   ├── docker-compose.yml       # Main compose file with profiles
│   ├── .env.example             # Environment variables template
│   ├── postgres-init.sh         # DB initialization script
│   ├── trino/catalog/           # Trino catalog configurations
│   │   └── iceberg.properties   # Iceberg catalog config
│   ├── scripts/
│   │   ├── stack-health.ps1     # Health check script (Windows)
│   │   ├── stack-health.sh      # Health check script (Linux/Mac)
│   │   ├── verify-ac-simple.sh  # MinIO AC verification
│   │   └── quick-test.sh        # Quick setup test
│   ├── SETUP_GUIDE.md           # Comprehensive setup documentation
│   └── gravitino/               # Gravitino Iceberg REST build
├── infra/minio/                 # MinIO infrastructure
│   ├── policies/                # Bucket policies (committed to repo)
│   │   ├── lakehouse-rw.json    # Lakehouse bucket RW policy
│   │   └── mlflow-rw.json       # MLflow bucket RW policy
│   ├── setup_buckets.sh         # Bucket setup script
│   └── README.md                # MinIO documentation
├── src/pv_lakehouse/            # Python package
│   └── etl/                     # ETL modules
│       └── bronze_ingest.py     # Bronze layer ingestion
├── flows/                       # Prefect orchestration flows
│   └── bronze_to_silver.py      # Sample flow
├── sql/trino/                   # SQL DDL scripts
│   └── create_schemas.sql       # Schema definitions
├── AC_STATUS_REPORT.md          # Acceptance criteria status
├── BRANCH_README.md             # Branch documentation
├── .env.example                 # Environment template (copy to docker/.env)
├── Makefile                     # Development & Docker commands
├── pyproject.toml               # Python project config
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## 🚀 Quick Start

### Prerequisites

- **Docker** 20.10+ & **Docker Compose** V2
- **Python** 3.11+ (for development)
- **Git**

### 1. Clone Repository

```bash
git clone https://github.com/xuanquangIT/dlh-pv.git
cd dlh-pv
```

### 2. Setup Environment

```bash
# Copy environment template
cp .env.example docker/.env

# Edit docker/.env if needed
# Default values work for local development
# For production, update credentials:
#   - Service user passwords (SPARK_SVC_SECRET_KEY, etc.)
#   - PV_PASSWORD
#   - Database passwords
```

### 3. Start Services

**Option A: Core services only** (MinIO, Postgres, Iceberg, Trino)

```bash
make up
# or: cd docker && docker compose --profile core up -d
```

This automatically:
- ✅ Creates MinIO buckets: `lakehouse`, `mlflow`
- ✅ Sets up bucket policies from `infra/minio/policies/`
- ✅ Creates service users: `spark_svc`, `trino_svc`, `mlflow_svc`
- ✅ Configures least-privilege access control

**Option B: All services** (includes Spark, MLflow, Prefect)

```bash
make up-all
# or: cd docker && docker compose --profile core --profile spark --profile ml --profile orchestrate up -d
```

### 4. Verify Setup

```bash
# Quick health check
make health

# Or run comprehensive verification
cd docker && ./scripts/verify-ac-simple.sh

# Or quick test
cd docker && ./scripts/quick-test.sh
```

Expected output:
```
✓ PASS: Buckets exist: lakehouse, mlflow
✓ PASS: Policies exist: lakehouse-rw, mlflow-rw
✓ PASS: Service users configured correctly
```

### 5. Access Services

| Service | URL | Default Credentials |
|---------|-----|---------------------|
| MinIO Console | http://localhost:9001 | pvlakehouse / pvlakehouse |
| Trino | http://localhost:8081 | - |
| MLflow | http://localhost:5000 | - |
| Prefect | http://localhost:4200 | - |
| Spark Master UI | http://localhost:4040 | - |
| pgAdmin | http://localhost:5050 | configured in `docker/.env` |

Notes:
- The Iceberg REST catalog uses Apache Gravitino and is exposed at `http://localhost:8181/iceberg/v1/` (we build a small custom image `gravitino-iceberg-rest-postgres:latest` that includes the PostgreSQL JDBC driver).

---

## 🛠️ Development Workflow

---

## 📋 Docker Compose Profiles

The stack uses profiles to control which services start:

- **`core`** - Essential services (MinIO, Postgres, Iceberg REST, Trino)
- **`spark`** - Spark master + worker for batch processing
- **`ml`** - MLflow for ML tracking and model registry
- **`orchestrate`** - Prefect server + agent for workflow orchestration

**Examples:**

```bash
# Core only
docker compose --profile core up -d

# Core + Spark
docker compose --profile core --profile spark up -d

# Everything
docker compose --profile core --profile spark --profile ml --profile orchestrate up -d

# Stop all (use wildcard profile)
docker compose --profile "*" down
```

---

## ⚙️ Configuration

All configuration is managed through environment variables in `docker/.env`.

Key variables:

```bash
# Authentication
PV_USER=pvlakehouse                  # Default username for all services
PV_PASSWORD=pvlakehouse              # Default password (change in production!)

# Storage
S3_WAREHOUSE_BUCKET=lakehouse        # Iceberg data bucket
S3_MLFLOW_BUCKET=mlflow              # MLflow artifacts bucket

# Service Ports
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
TRINO_PORT=8081
MLFLOW_PORT=5000
PREFECT_PORT=4200

# Images (pin versions for production)
MINIO_IMAGE=minio/minio:latest
POSTGRES_IMAGE=postgres:15
TRINO_IMAGE=trinodb/trino:latest
```

**⚠️ Security**: Never commit secrets to git. Use `docker/.env` (gitignored) for actual values.

---

<!-- CI and pre-commit hooks removed from this repository. -->

---

<!-- Tests and pytest configuration removed from this repository. -->

---

## 🔐 MinIO Security & Bucket Setup

The project implements **least-privilege access control** for MinIO with service-specific users and policies.

### Automatic Setup (Recommended)

When you run `docker compose --profile core up -d`, the setup happens automatically:

1. **Buckets Created**: `lakehouse` (Iceberg data) and `mlflow` (ML artifacts)
2. **Policies Applied**: Read/write policies from `infra/minio/policies/`
3. **Service Users**: `spark_svc`, `trino_svc`, `mlflow_svc` with least-privilege access
4. **Security**: Private buckets with versioning enabled

### Verify MinIO Setup

```bash
# Quick verification
cd docker && ./scripts/verify-ac-simple.sh

# Check buckets
docker run --rm --network dlhpv_data-net \
  --entrypoint /bin/sh \
  minio/mc:latest -c '
  mc alias set local http://minio:9000 pvlakehouse pvlakehouse;
  mc ls local;
  '

# Check policies
docker run --rm --network dlhpv_data-net \
  --entrypoint /bin/sh \
  minio/mc:latest -c '
  mc alias set local http://minio:9000 pvlakehouse pvlakehouse;
  mc admin policy ls local;
  '
```

### Service User Credentials

Configured in `docker/.env`:

```bash
# Default credentials (CHANGE IN PRODUCTION!)
SPARK_SVC_ACCESS_KEY=spark_svc
SPARK_SVC_SECRET_KEY=spark_secret_change_me
TRINO_SVC_ACCESS_KEY=trino_svc
TRINO_SVC_SECRET_KEY=trino_secret_change_me
MLFLOW_SVC_ACCESS_KEY=mlflow_svc
MLFLOW_SVC_SECRET_KEY=mlflow_secret_change_me
```

### Policy Files

Bucket policies are version-controlled in the repository:

- `infra/minio/policies/lakehouse-rw.json` - Lakehouse bucket read/write
- `infra/minio/policies/mlflow-rw.json` - MLflow bucket read/write

Each policy grants minimal required permissions (ListBucket, GetObject, PutObject, DeleteObject).

### Documentation

- **[MinIO README](infra/minio/README.md)** - Complete setup guide and troubleshooting
- **[AC Status Report](AC_STATUS_REPORT.md)** - Acceptance criteria verification

---

## 📚 Additional Documentation

- **[Docker Setup Guide](docker/SETUP_GUIDE.md)** - Comprehensive deployment guide
- **[MinIO Infrastructure](infra/minio/README.md)** - Storage security & policies
- **[AC Status Report](AC_STATUS_REPORT.md)** - Acceptance criteria verification
- **[Branch README](BRANCH_README.md)** - Current branch documentation

---

## 🐛 Troubleshooting

### Services won't start

```bash
# Check container status
make ps
# or: cd docker && docker compose ps

# View logs
make logs
# or: cd docker && docker compose logs

# Check specific service
cd docker && docker compose logs minio -f
cd docker && docker compose logs mc

# Verify MC setup completed
cd docker && docker compose logs mc | grep "MinIO setup complete"
```

### Health checks failing

```bash
# Run health check script
make health

# Check individual service health
docker exec minio curl -f http://localhost:9000/minio/health/ready
docker exec postgres pg_isready -U pvlakehouse
```

### Port conflicts

If ports are already in use, edit `docker/.env`:

```bash
MINIO_API_PORT=9010      # Change from 9000
TRINO_PORT=8082          # Change from 8081
```

### Reset everything

```bash
# Stop and remove all containers + volumes
make clean

# Start fresh
make up
```

### Common issues

1. **MinIO buckets not created**: Check `mc` service logs with `docker compose logs mc`
2. **Import errors**: Ensure `src/` is in PYTHONPATH when running local scripts
3. **Docker out of memory**: Increase Docker Desktop memory to 8GB+
4. **S3A connection errors**: 
   - Inside containers use: `http://minio:9000`
   - From host use: `http://localhost:9000`
   - Verify service user credentials in `docker/.env`
5. **Permission denied errors**: Service users may not have correct policies attached
   - Run: `cd docker && ./scripts/verify-ac-simple.sh`
   - Re-run: `docker compose up mc` to reapply policies

---

## 🤝 Contributing

Contributions welcome! Please:

1. Ensure code style follows ruff (run `make lint`)
2. Add necessary checks and documentation for new features
3. Update documentation as needed
4. Follow existing code style (enforced by ruff)

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

---

## 🔗 Links

- **Repository**: https://github.com/xuanquangIT/dlh-pv
- **Issues**: https://github.com/xuanquangIT/dlh-pv/issues
- **Apache Iceberg**: https://iceberg.apache.org
- **Trino**: https://trino.io
- **MinIO**: https://min.io
- **Prefect**: https://prefect.io
- **MLflow**: https://mlflow.org

