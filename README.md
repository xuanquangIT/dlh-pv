<p align="center">
  <img src="doc/assets/logo.png" alt="PV Lakehouse Logo" width="120" height="120">
  <h1 align="center">🏠 PV Lakehouse</h1>
  <p align="center">
    <strong>A production-ready data lakehouse platform for solar energy analytics</strong>
  </p>
  <p align="center">
    <a href="#-quick-start">Quick Start</a> •
    <a href="#-features">Features</a> •
    <a href="#-architecture">Architecture</a> •
    <a href="#-documentation">Documentation</a> •
    <a href="#-contributing">Contributing</a>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/python-3.11+-blue.svg" alt="Python">
    <img src="https://img.shields.io/badge/spark-3.5-orange.svg" alt="Spark">
    <img src="https://img.shields.io/badge/iceberg-1.5-green.svg" alt="Iceberg">
    <img src="https://img.shields.io/badge/docker-ready-blue.svg" alt="Docker">
    <img src="https://img.shields.io/badge/license-MIT-green.svg" alt="License">
  </p>
</p>

---

## 📋 Overview

**PV Lakehouse** is a complete, open-source data lakehouse solution designed for solar energy analytics. Built with modern data engineering best practices, it provides end-to-end capabilities from raw data ingestion to ML-ready feature stores.

### ✨ Key Highlights

| Feature | Description |
|---------|-------------|
| 🏗️ **Medallion Architecture** | Bronze → Silver → Gold data layers with clear data contracts |
| 🐳 **Docker-native** | One-command deployment with Docker Compose profiles |
| 🔌 **Open Standards** | Apache Iceberg table format for interoperability |
| 📊 **SQL-first** | Query data directly with Trino's ANSI SQL engine |
| 🤖 **ML-ready** | Integrated MLflow for experiment tracking and model management |
| ⚡ **High Performance** | Optimized Spark configurations for batch processing |
| 🔄 **ELT Pattern** | Extract-Load-Transform for data lineage & reproducibility |

---

## 🛠️ Technology Stack

| Layer | Technology | Version | Purpose |
|-------|------------|---------|---------|
| **Storage** | MinIO | Latest | S3-compatible object storage |
| **Table Format** | Apache Iceberg | 1.5 | ACID transactions, schema evolution |
| **Processing** | Apache Spark | 3.5 | Distributed batch processing |
| **Query Engine** | Trino | Latest | Interactive SQL analytics |
| **Orchestration** | Prefect | 2.x | Workflow automation |
| **ML Tracking** | MLflow | 2.4 | Experiment tracking & model registry |
| **Catalog** | PostgreSQL | 15 | Iceberg metadata store |

---

## 🏛️ Architecture

### Medallion Data Layers

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MEDALLION ARCHITECTURE                            │
├───────────────────────┬───────────────────────┬─────────────────────────────┤
│      🥉 BRONZE        │      🥈 SILVER        │         🥇 GOLD             │
│      (Raw Data)       │     (Cleaned)         │      (Analytics)            │
├───────────────────────┼───────────────────────┼─────────────────────────────┤
│ • Schema-on-read      │ • Schema enforcement  │ • Star schema               │
│ • Full fidelity       │ • Deduplication       │ • Pre-aggregated            │
│ • Append-only         │ • Data validation     │ • Business metrics          │
│ • Audit trail         │ • Type casting        │ • ML features               │
├───────────────────────┼───────────────────────┼─────────────────────────────┤
│ raw_facilities        │ clean_facility_master │ dim_facility                │
│ raw_facility_         │ clean_hourly_energy   │ dim_date                    │
│   timeseries          │ clean_hourly_weather  │ dim_time                    │
│ raw_facility_weather  │ clean_hourly_         │ dim_aqi_category            │
│ raw_facility_         │   air_quality         │ fact_solar_environmental    │
│   air_quality         │                       │                             │
└───────────────────────┴───────────────────────┴─────────────────────────────┘
```

### Infrastructure Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INFRASTRUCTURE                                    │
│                                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│   │   MinIO     │  │ PostgreSQL  │  │    Trino    │  │   Spark     │      │
│   │   (S3)      │  │  (Catalog)  │  │   (Query)   │  │  (Process)  │      │
│   │  :9000/9001 │  │    :5432    │  │    :8081    │  │    :4040    │      │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘      │
│                                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                       │
│   │   MLflow    │  │   Prefect   │  │   pgAdmin   │                       │
│   │   (ML Ops)  │  │   (Orch)    │  │    (UI)     │                       │
│   │    :5000    │  │    :4200    │  │    :5050    │                       │
│   └─────────────┘  └─────────────┘  └─────────────┘                       │
└─────────────────────────────────────────────────────────────────────────────┘
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
git clone https://github.com/yourusername/dlh-pv.git
cd dlh-pv

# Setup environment configuration
cp docker/.env.example docker/.env

# Create symlink (optional, for convenience)
ln -sf docker/.env .env
```

### 2. Start Services

```bash
cd docker

# Start core services
docker compose --profile core up -d

# Verify health
./scripts/health-check.sh
```

### 3. Run Your First Pipeline

```bash
# Load facility metadata
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client --driver-memory 3g --executor-memory 4g \
  /opt/workdir/src/pv_lakehouse/etl/bronze/load_facilities.py

# Query the data
docker exec -it trino trino --execute \
  "SELECT * FROM iceberg.bronze.raw_facilities LIMIT 5"
```

### 4. Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | `pvlakehouse` / `pvlakehouse` |
| **Spark Master UI** | http://localhost:4040 | — |
| **Trino UI** | http://localhost:8081 | — |
| **MLflow UI** | http://localhost:5000 | — |
| **pgAdmin** | http://localhost:5050 | `admin@example.com` / `pvlakehouse` |

---

## 📁 Project Structure

```
dlh-pv/
├── 📂 docker/                    # Docker Compose configuration
│   ├── docker-compose.yml        # Service definitions
│   ├── .env.example              # Environment template
│   ├── README-SETUP.md           # Docker setup guide
│   ├── postgres/                 # PostgreSQL init scripts
│   ├── spark/                    # Spark Dockerfile & config
│   └── trino/                    # Trino catalog config
│
├── 📂 src/pv_lakehouse/          # Main Python package
│   ├── config/                   # Configuration management
│   │   ├── settings.py           # Pydantic settings
│   │   └── spark_config.yaml     # Spark configuration
│   ├── etl/                      # ETL modules
│   │   ├── bronze/               # Raw data ingestion
│   │   ├── silver/               # Data transformation
│   │   ├── gold/                 # Analytics layer
│   │   ├── clients/              # API clients
│   │   ├── utils/                # Shared utilities
│   │   └── scripts/              # Helper scripts
│   └── ml_pipeline/              # ML training pipelines
│
├── 📂 tests/                     # Test suite
│   ├── config/                   # Config tests
│   ├── etl/                      # ETL tests
│   └── conftest.py               # Pytest fixtures
│
├── 📂 doc/                       # Documentation
│   ├── schema/                   # Schema definitions
│   └── power-bi/                 # BI integration guides
│
├── 📂 dashboard/                 # Power BI dashboards
├── 📂 config/                    # ML configuration
├── pyproject.toml                # Python project config
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

---

## 📚 Documentation

### Guides

| Document | Description |
|----------|-------------|
| [Docker Setup](docker/README-SETUP.md) | Complete Docker deployment guide |
| [ETL Operations](src/pv_lakehouse/etl/scripts/CHEATSHEET_GUIDE.md) | ETL pipeline operations |
| [Gold Layer Design](doc/schema/GOLD_LAYER_DESIGN.md) | Analytics schema design |
| [Trino Connection](doc/power-bi/TRINO_CONNECTION_GUIDE.md) | BI tool integration |

### Data Layers

| Layer | Table | Description |
|-------|-------|-------------|
| **Bronze** | `raw_facilities` | Solar facility metadata |
| | `raw_facility_timeseries` | Energy generation data |
| | `raw_facility_weather` | Weather observations |
| | `raw_facility_air_quality` | Air quality metrics |
| **Silver** | `clean_facility_master` | Validated facility data |
| | `clean_hourly_energy` | Hourly energy aggregates |
| | `clean_hourly_weather` | Hourly weather data |
| | `clean_hourly_air_quality` | Hourly air quality |
| **Gold** | `dim_*` | Dimension tables |
| | `fact_solar_environmental` | Main fact table |

---

## 🧪 Development

### Local Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or: .venv\Scripts\activate  # Windows

# Install dependencies
pip install -e .
pip install -r requirements.txt
```

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src/pv_lakehouse --cov-report=html

# Run specific test file
pytest tests/config/test_settings.py -v
```

### Code Quality

```bash
# Format code
ruff format src/ tests/

# Lint
ruff check src/ tests/

# Type checking
mypy src/pv_lakehouse/
```

---

## 🔧 Configuration

### Environment Variables

All configuration is managed via `docker/.env`:

| Category | Key Variables |
|----------|---------------|
| **Credentials** | `PV_USER`, `PV_PASSWORD` |
| **PostgreSQL** | `POSTGRES_HOST`, `POSTGRES_PORT` |
| **MinIO** | `MINIO_ENDPOINT`, `S3_WAREHOUSE_BUCKET` |
| **Spark** | `SPARK_WORKER_MEMORY`, `SPARK_EXECUTOR_MEMORY` |
| **API Keys** | `OPENELECTRICITY_API_KEY` |

### Spark Tuning

Adjust in `.env` based on your system:

```env
# For 16GB RAM system
SPARK_WORKER_MEMORY=6G
SPARK_EXECUTOR_MEMORY=4g
SPARK_DRIVER_MEMORY=3g
SPARK_SHUFFLE_PARTITIONS=32
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Coding Standards

- Follow [PEP 8](https://pep8.org/) style guide
- Use type hints for all functions
- Write docstrings (Google style)
- Maintain test coverage > 80%
- Use `ruff` for formatting

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- [Apache Iceberg](https://iceberg.apache.org/) - Table format
- [Apache Spark](https://spark.apache.org/) - Processing engine
- [Trino](https://trino.io/) - Query engine
- [MinIO](https://min.io/) - Object storage
- [OpenElectricity](https://openelectricity.org.au/) - Data source

---

<p align="center">
  <sub>Built with ❤️ for the solar energy community</sub>
</p>
