<p align="center">
  <h1 align="center">🏠 PV Lakehouse</h1>
  <p align="center">
    <strong>A production-ready data lakehouse platform for building modern ETL pipelines</strong>
  </p>
  <p align="center">
    <a href="#-quick-start">Quick Start</a> •
    <a href="#-features">Features</a> •
    <a href="#-architecture">Architecture</a> •
    <a href="#-documentation">Documentation</a>
  </p>
</p>

---

## 📋 Overview

**PV Lakehouse** is a complete data lakehouse solution designed to run on your laptop or a single VM. Built entirely with open-source components, it provides a robust foundation for evolving data from raw ingestion through normalization to curated analytics.

### ✨ Key Highlights

- 🏗️ **Medallion Architecture** — Bronze → Silver → Gold data layers with clear conventions
- 🐳 **Docker-native** — One-command deployment with Docker Compose profiles
- 🔌 **Open Standards** — Apache Iceberg table format for interoperability
- 📊 **SQL-first** — Query data directly with Trino's ANSI SQL engine
- 🤖 **ML-ready** — Integrated MLflow for experiment tracking and model management
- 🔄 **Workflow Orchestration** — Prefect-powered ETL pipelines

---

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Object Storage** | MinIO | S3-compatible storage for data lake |
| **Table Format** | Apache Iceberg v2 | Open table format with ACID transactions |
| **Batch Processing** | Apache Spark 3.5 | Distributed data processing engine |
| **Query Engine** | Trino | Fast SQL analytics over lakehouse |
| **Orchestration** | Prefect | Modern workflow orchestration |
| **ML Tracking** | MLflow 2.4 | Experiment tracking & model registry |
| **Metadata Store** | PostgreSQL | Iceberg catalog & application metadata |
| **Admin UI** | pgAdmin | Database management interface |

---

## 🏛️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              MEDALLION LAYERS                           │
├───────────────────┬───────────────────┬───────────────────────────────┤
│    🥉 BRONZE      │    🥈 SILVER      │         🥇 GOLD               │
│    (Raw Data)     │   (Normalized)    │    (Curated/Analytics)        │
│                   │                   │                               │
│  • Raw ingestion  │  • Cleaned data   │  • Aggregations               │
│  • Schema-on-read │  • Validated      │  • Business metrics           │
│  • Full fidelity  │  • Deduplicated   │  • ML feature tables          │
└───────────────────┴───────────────────┴───────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           INFRASTRUCTURE                                │
│                                                                         │
│   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│   │  MinIO  │  │  Spark  │  │  Trino  │  │ MLflow  │  │Postgres │     │
│   │   S3    │  │ Cluster │  │  Query  │  │   ML    │  │ Catalog │     │
│   └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```
dlh-pv/
├── doc/                         # 📚 Comprehensive documentation
│   ├── bronze-silver/           # Data layer specifications
│   ├── schema/                  # Schema definitions
│   └── power-bi/                # BI integration guides
│
├── docker/                      # 🐳 Docker Compose services
│   ├── docker-compose.yml       # Main compose file with profiles
│   ├── postgres/                # PostgreSQL initialization scripts
│   ├── spark/                   # Spark Dockerfile & configuration
│   ├── trino/                   # Trino catalog configuration
│   └── scripts/                 # Utility & health-check scripts
│
├── infra/                       # 🗄️ Infrastructure configuration
│   └── minio/policies/          # MinIO bucket policies
│
├── src/pv_lakehouse/            # 🐍 Python package
│   ├── etl/                     # ETL modules (bronze, silver, gold)
│   │   ├── bronze/              # Raw data ingestion
│   │   ├── silver/              # Data transformation
│   │   ├── gold/                # Analytics & aggregations
│   │   ├── clients/             # External API clients
│   │   └── notebooks/           # Jupyter notebooks
│   ├── ml_pipeline/             # Machine learning pipelines
│   └── mlflow/                  # MLflow integration
│
├── pyproject.toml               # Python project configuration
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## 🚀 Quick Start

### Prerequisites

| Requirement | Version |
|-------------|---------|
| Docker | 20.10+ |
| Docker Compose | 2.0+ |
| Python | 3.11+ (for development) |
| Git | Latest |

### 1. Clone & Configure

```bash
# Clone the repository
git clone https://github.com/xuanquangIT/dlh-pv.git
cd dlh-pv

# Copy environment template
cp docker/.env.example docker/.env
```

### 2. Start Services

```bash
cd docker

# Start core services (MinIO, PostgreSQL, Trino, Spark)
docker compose --profile core up -d

# Optionally, start ML services (MLflow)
docker compose --profile ml up -d

# Verify all services are healthy
./scripts/health-check.sh
```

### 3. Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | [localhost:9001](http://localhost:9001) | `pvlakehouse` / `pvlakehouse` |
| **Spark Master UI** | [localhost:8080](http://localhost:8080) | — |
| **Trino UI** | [localhost:8081](http://localhost:8081) | — |
| **MLflow UI** | [localhost:5000](http://localhost:5000) | — |
| **pgAdmin** | [localhost:5050](http://localhost:5050) | `admin@admin.com` / `pvlakehouse` |

### 4. Run Your First Query

```bash
# Connect to Trino CLI
docker exec -it trino trino --catalog iceberg --schema bronze

# Show available tables
trino:bronze> SHOW TABLES;

# Query sample data
trino:bronze> SELECT * FROM your_table LIMIT 10;
```

---

## 📚 Documentation

Comprehensive documentation is available in the [`doc/`](doc/) directory:

### Data Layer Guides

| Document | Description |
|----------|-------------|
| [Bronze Layer](doc/bronze-silver/BRONZE_LAYER.md) | Raw data ingestion specifications |
| [Silver Layer](doc/bronze-silver/SILVER_LAYER.md) | Data transformation & validation |
| [Silver Validation Rules](doc/bronze-silver/SILVER_VALIDATION_RULES.md) | Data quality checks |
| [ETL Operations Guide](doc/bronze-silver/ETL_OPERATIONS_GUIDE.md) | Running ETL pipelines |

### Analysis & Troubleshooting

| Document | Description |
|----------|-------------|
| [Bronze-Silver Analysis](doc/bronze-silver/BRONZE_SILVER_ANALYSIS_README.md) | Data flow analysis |
| [Anomalies & Filters](doc/bronze-silver/ANOMALIES_AND_SILVER_FILTERS.md) | Data quality patterns |
| [Timezone Analysis](doc/bronze-silver/TIMEZONE_AND_RECORD_COUNT_ANALYSIS.md) | Temporal data handling |

---

## 🧪 Development

### Setting Up Local Environment

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .
pip install -r requirements.txt
```

### Running Tests

```bash
# Run pytest suite
pytest tests/

# Run specific test
pytest tests/test_bronze_tables_complete.py -v
```

### Project Configuration

The project uses modern Python tooling:

- **Build System**: setuptools with `pyproject.toml`
- **Linting**: Ruff (line-length: 100, Python 3.11+)
- **Testing**: pytest

---

## 🔧 Docker Compose Profiles

The platform uses Docker Compose profiles for flexible deployment:

| Profile | Services | Use Case |
|---------|----------|----------|
| `core` | MinIO, PostgreSQL, Spark, Trino, pgAdmin | Data engineering workloads |
| `ml` | MLflow | Machine learning workflows |

```bash
# Start specific profile
docker compose --profile core up -d

# Start multiple profiles
docker compose --profile core --profile ml up -d

# Stop all services
docker compose --profile core --profile ml down
```

---

## 📊 Use Cases

PV Lakehouse is designed for:

- 🔬 **Data Engineering Learning** — Hands-on experience with modern lakehouse architecture
- 🧪 **Prototype Development** — Quickly validate ETL pipelines before production
- 📈 **Analytics Workloads** — SQL-based analysis with Trino
- 🤖 **ML Experiments** — Track experiments and models with MLflow
- 🏠 **Local Development** — Full lakehouse stack on a single machine

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

---

## 📄 License

This project is open source. See the repository for license details.

---

<p align="center">
  <sub>Built with ❤️ using open-source technologies</sub>
</p>
