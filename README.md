# PV Lakehouse

**A production-ready data lakehouse platform** for building ETL pipelines on your laptop or a single VM.

Built with **open-source components** and clear conventions for evolving from raw → normalized → curated data with confidence.

**Technology Stack**:
- 🗄️ **MinIO** - S3-compatible object store
- 🗂️ **Apache Iceberg** - Open table format v2
- ⚡ **Apache Spark** - Batch processing
- 📊 **Trino** - SQL query engine
- 🔄 **Prefect** - Workflow orchestration
- 🧠 **MLflow** - ML model tracking
- 🐘 **PostgreSQL** - Metadata storage

**Medallion Architecture**: Bronze (raw) → Silver (normalized) → Gold (curated/analytics)

---

## 📁 Repository Structure

```
dlh-pv/
├── doc/                         # 📚 Comprehensive documentation
│   ├── architecture/            # System design & architecture
│   ├── setup/                   # Installation & deployment guides
│   ├── data-model/              # Data layer schemas
│   ├── development/             # ETL development guides
│   ├── operations/              # Operations & monitoring
│   ├── infrastructure/          # Infrastructure setup
│   └── README.md                # Documentation index
│
├── docker/                      # 🐳 Docker Compose services
│   ├── docker-compose.yml       # Main compose file with profiles
│   ├── .env.example             # Environment variables template
│   ├── postgres/                # PostgreSQL initialization
│   ├── trino/catalog/           # Trino Iceberg catalog config
│   ├── spark/                   # Spark Dockerfile & config
│   ├── scripts/                 # Utility scripts
│   └── README-SETUP.md          # Quick setup guide
│
├── infra/minio/                 # 🗄️ MinIO infrastructure
│   ├── policies/                # Bucket policies (version controlled)
│   └── README.md                # MinIO setup docs
│
├── src/pv_lakehouse/            # 🐍 Python package
│   ├── etl/                     # ETL modules
│   │   ├── bronze_ingest.py    # Bronze ingestion
│   │   └── utils/               # Helper utilities
│   └── config/                  # Configuration
│
├── flows/                       # 🔄 Prefect workflows
│   └── bronze_to_silver.py      # Example transformation
│
├── sql/                         # 📊 DDL scripts
│   ├── bronze/                  # Bronze layer tables
│   ├── silver/                  # Silver layer tables (TBD)
│   └── gold/                    # Gold layer tables (TBD)
│
├── tests/                       # ✅ Test scripts
│   ├── test_bronze_tables_complete.py
│   └── create_bronze_tables.sh
│
├── pyproject.toml               # Python project config
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment template
└── README.md                    # This file
```

---

## 🚀 Quick Start

### Prerequisites

- **Docker** 20.10+ & **Docker Compose** 2.0+
- **Python** 3.11+ (for development)
- **Git**

### 1. Clone & Setup

```bash
# Clone repository
git clone https://github.com/xuanquangIT/dlh-pv.git
cd dlh-pv

# Copy environment file
cp .env.example docker/.env
```

### 2. Start Services

```bash
cd docker

# Core services only (MinIO, PostgreSQL, Trino, Spark)
docker compose --profile core up -d

# Wait for startup (~30-60 seconds)
./scripts/health-check.sh
```

### 3. Access Services

| Service | URL |
|---------|-----|
| MinIO Console | http://localhost:9001 |
| Trino UI | http://localhost:8081 |
| Spark Master | http://localhost:8080 |
| pgAdmin | http://localhost:5050 |

**Credentials**: `pvlakehouse` / `pvlakehouse`

### 4. Create Bronze Tables (Optional)

```bash
cd ../tests

# Create all Bronze tables with sample data
python test_bronze_tables_complete.py --create

# Verify in Trino
docker compose exec -it trino trino --catalog iceberg --schema bronze
trino:bronze> SHOW TABLES;
```

## 📚 Documentation

**Start here**: [📖 Documentation Index](doc/README.md)

### Key Documents

- **[Architecture Overview](doc/architecture/overview.md)** - System design
- **[Quick Start Guide](doc/setup/quick-start.md)** - Detailed setup (5 min)
- **[Medallion Design](doc/architecture/medallion-design.md)** - 3-layer architecture
- **[Bronze Layer](doc/data-model/bronze-layer.md)** - Raw data tables
- **[ETL Development](doc/development/etl-development.md)** - Writing ETL code
- **[Operations Guide](doc/operations/operations.md)** - Daily operations
- **[MinIO Setup](doc/infrastructure/minio-setup.md)** - Storage configuration

## � Reading Paths

**For everyone (5 min)**:
1. [Overview](doc/architecture/overview.md)
2. [Quick Start](doc/setup/quick-start.md)

**For Data Engineers (30 min)**:
1. [Medallion Design](doc/architecture/medallion-design.md)
2. [Bronze Layer](doc/data-model/bronze-layer.md)
3. [ETL Development](doc/development/etl-development.md)

**For DevOps/Infra (30 min)**:
1. [System Architecture](doc/architecture/system-architecture.md)
2. [MinIO Setup](doc/infrastructure/minio-setup.md)
3. [Operations](doc/operations/operations.md)

