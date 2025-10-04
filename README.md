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
│   ├── .env.sample              # Environment variables template
│   ├── postgres-init.sh         # DB initialization script
│   ├── iceberg-rest.yml         # Iceberg catalog config
│   ├── trino/catalog/           # Trino catalog configurations
│   ├── scripts/
│   │   ├── stack-health.ps1     # Health check script (Windows)
│   │   └── stack-health.sh      # Health check script (Linux/Mac)
│   ├── SETUP_GUIDE.md           # Comprehensive setup documentation
│   └── STATUS_REPORT.md         # Implementation status
├── src/pv_lakehouse/            # Python package
│   └── etl/                     # ETL modules
│       └── bronze_ingest.py     # Bronze layer ingestion
├── flows/                       # Prefect orchestration flows
│   └── bronze_to_silver.py      # Sample flow
├── sql/trino/                   # SQL DDL scripts
│   └── create_schemas.sql       # Schema definitions
<!-- tests and pytest removed -->
├── scripts/                     # Utility scripts
│   └── smoke.sh                 # Quick smoke test
├── .env.example                 # Environment template (copy to docker/.env)
├── Makefile                     # Development & Docker commands
├── pyproject.toml               # Python project config
├── requirements-dev.txt         # Dev dependencies
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

# Edit docker/.env and update passwords/secrets as needed
# The defaults work for local development
```

### 3. Start Services

**Option A: Core services only** (MinIO, Postgres, Iceberg, Trino)

```bash
make up
# or: cd docker && docker compose --profile core up -d
```

**Option B: All services** (includes Spark, MLflow, Prefect)

```bash
make up-all
# or: cd docker && docker compose --profile core --profile spark --profile ml --profile orchestrate up -d
```

### 4. Verify Health

```bash
make health
# or: pwsh docker/scripts/stack-health.ps1
```

### 5. Access Services

| Service | URL | Default Credentials |
|---------|-----|---------------------|
| MinIO Console | http://localhost:9001 | pvlakehouse / pvlakehouse |
| Trino | http://localhost:8081 | - |
| MLflow | http://localhost:5000 | - |
| Prefect | http://localhost:4200 | - |
| Spark Master UI | http://localhost:4040 | - |

---

## 🛠️ Development Workflow

### Setup Development Environment

```bash
# Install dev dependencies and setup hooks
make init

# This will:
# - Install ruff and black (dev linters/formatters)
```

### Run Linting

```bash
make lint        # Run ruff linter and formatter checks
make smoke       # Quick smoke test (start compose + verify)
```

### Makefile Targets

```bash
make help        # Show all available commands
make up          # Start core services
make up-all      # Start all services (core + spark + ml + orchestrate)
make down        # Stop all services
make ps          # Show running containers
make logs        # Tail logs from all services
make health      # Run health check
make clean       # Remove all volumes and cleanup
```

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

## 📚 Additional Documentation

- **[Docker Setup Guide](docker/SETUP_GUIDE.md)** - Comprehensive deployment guide
- **[Status Report](docker/STATUS_REPORT.md)** - Implementation checklist
- **[Contributing](CONTRIBUTING.md)** - Contribution guidelines

---

## 🐛 Troubleshooting

### Services won't start

```bash
# Check container status
make ps

# View logs
make logs

# Check specific service
cd docker && docker compose logs minio -f
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

1. **Import errors**: Ensure `src/` is in PYTHONPATH when running local scripts
3. **Docker out of memory**: Increase Docker Desktop memory to 8GB+
4. **S3A connection errors**: Verify `MINIO_ENDPOINT` uses `http://minio:9000` (inside containers) or `http://localhost:9000` (from host)

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

