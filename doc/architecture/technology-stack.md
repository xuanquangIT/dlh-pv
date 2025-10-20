# Technology Stack & Components

## 1. Complete Technology Stack

```
┌─────────────────────────────────────────────────────────────┐
│                     Technology Stack                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  STORAGE & COMPUTE                                          │
│  ├─ MinIO v2024.x           S3-compatible object storage   │
│  ├─ Apache Spark 3.5.1        Distributed batch processing  │
│  ├─ Trino v428+              Distributed SQL query engine   │
│  └─ Apache Iceberg v1.4+      Open table format            │
│                                                              │
│  METADATA & CATALOG                                         │
│  ├─ PostgreSQL 15             Relational database           │
│  ├─ Iceberg JDBC Catalog      Table metadata repository     │
│  └─ pgAdmin 4                 Database admin UI             │
│                                                              │
│  ORCHESTRATION & WORKFLOW                                   │
│  ├─ Prefect 2.9+              Workflow orchestration        │
│  └─ Prefect Server            Scheduling & monitoring      │
│                                                              │
│  ML & TRACKING                                              │
│  ├─ MLflow 2.4.0              Experiment tracking           │
│  └─ MLflow UI                 Model registry               │
│                                                              │
│  DEVELOPMENT & RUNTIME                                      │
│  ├─ Python 3.11+              Language runtime             │
│  ├─ PySpark 3.5.1             Python Spark API            │
│  ├─ Prefect 2.9+              Python workflow SDK         │
│  └─ Docker & Docker Compose   Containerization             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## 2. Core Components

### 2.1 Storage: MinIO

**Version:** Latest stable (v2024.x)

**Role:**
- S3-compatible object storage (drop-in S3 replacement)
- Hosts all data files (Parquet format)
- Stores MLflow artifacts

**Key Features:**
- Multi-tenant bucket policies
- Versioning & lifecycle management
- Server-side encryption (optional)
- YAML-configurable

**Configuration:**
```yaml
MINIO_ROOT_USER: pvlakehouse
MINIO_ROOT_PASSWORD: pvlakehouse  # Change in production!
MINIO_API_PORT: 9000
MINIO_CONSOLE_PORT: 9001
```

**Buckets:**
- `lakehouse` - Iceberg data warehouse
- `mlflow` - ML artifacts

**Service Users:**
- `spark_svc` - Spark S3A access (lakehouse-rw policy)
- `trino_svc` - Trino S3A access (lakehouse-rw policy)
- `mlflow_svc` - MLflow artifacts (mlflow-rw policy)

### 2.2 Table Format: Apache Iceberg

**Version:** 1.4+

**Why Iceberg?**
- ACID transactions
- Schema evolution
- Time travel queries
- Partition pruning optimization
- Concurrent writes handling

**Format Version:** 2 (latest features)

**Key Concepts:**

| Concept | Description |
|---------|-------------|
| **Table** | Versioned dataset with schema & partitions |
| **Snapshot** | Point-in-time immutable view |
| **Manifest** | List of files in a snapshot |
| **Partition** | Logical grouping (e.g., by day) |
| **Catalog** | Maps table names to locations & metadata |

**Metadata Storage:**
- PostgreSQL JDBC catalog (not REST)
- Advantages:
  - ✅ No external service dependency
  - ✅ No Hadoop version conflicts
  - ✅ Direct query access to metadata
  - ✅ Better debugging capabilities

### 2.3 Compute: Apache Spark

**Version:** 3.5.1

**Role:**
- Batch ETL processing
- Data ingestion from APIs
- Transformations & aggregations
- Iceberg table creation/updates

**Key Configuration:**

```python
spark = SparkSession.builder \
    .appName("etl-app") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lh.type", "jdbc") \
    .config("spark.sql.catalog.lh.uri", "jdbc:postgresql://postgres/iceberg_catalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "spark_svc") \
    .config("spark.hadoop.fs.s3a.secret.key", "spark_secret") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()
```

**Resource Allocation:**
- Master: 1 executor
- Worker: 2 CPU, 4GB memory per default
- Configurable via docker-compose

**S3A Integration:**
- JAR: `hadoop-aws-3.4.1.jar`
- JAR: `aws-java-sdk-bundle-2.28.19.jar`
- Handles retry logic & connection pooling

### 2.4 Query Engine: Trino

**Version:** Latest stable (428+)

**Role:**
- Interactive SQL queries on Iceberg tables
- Query optimization & planning
- Federated queries (multiple catalogs)

**Configuration:**

```properties
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://postgres:5432/iceberg_catalog
iceberg.jdbc-catalog.connection-user=pvlakehouse
iceberg.jdbc-catalog.default-warehouse-dir=s3a://lakehouse/warehouse
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
```

**Key Features:**
- ANSI SQL support
- Distributed query execution
- Query planning & optimization
- Web UI on port 8081

**Use Cases:**
- Ad-hoc data exploration
- Data quality validation
- Quick ETL testing

### 2.5 Metadata: PostgreSQL + JDBC Catalog

**PostgreSQL Version:** 15

**Databases:**
```
iceberg_catalog/        # Iceberg table metadata
├─ iceberg_tables
├─ iceberg_namespace_properties
├─ iceberg_snapshots
├─ iceberg_manifest_lists
└─ iceberg_files

trino/                  # Trino configuration
mlflow/                 # MLflow experiment tracking
prefect/                # Prefect workflow state
```

**Iceberg JDBC Driver:**
- Version: org.postgresql:postgresql:42.7.x
- Connection pooling: HikariCP
- Thread-safe for concurrent access

**Key Tables:**
```sql
-- Iceberg table registry
iceberg_tables (
  catalog_name,
  table_namespace,
  table_name,
  metadata_location       -- Points to metadata.json in S3
)

-- Snapshot history
iceberg_snapshots (
  table_id,
  snapshot_id,
  parent_id,
  operation,
  timestamp_ms
)

-- Manifest tracking
iceberg_manifest_lists (
  snapshot_id,
  manifest_list_location  -- Avro manifest list file
)
```

### 2.6 Orchestration: Prefect

**Version:** 2.9+

**Role:**
- Schedule ETL jobs
- Monitor execution
- Handle retries & failures
- Dependency management

**Key Features:**
- Declarative flow definitions
- Task-based execution
- Dynamic scheduling
- UI dashboard on port 4200

**Example Flow:**

```python
from prefect import flow, task

@task
def ingest_bronze():
    """Ingest raw data to Bronze layer"""
    # Spark job to fetch from API
    pass

@task
def transform_silver():
    """Transform Bronze to Silver"""
    pass

@flow
def daily_etl():
    ingest_bronze()
    transform_silver()
    
# Deploy & schedule
daily_etl.serve(cron="0 6 * * *")  # 6am daily
```

**Components:**
- **Prefect Server**: Central orchestration
- **Prefect Agent**: Executes flows
- **Database**: PostgreSQL (prefect schema)
- **UI**: http://localhost:4200

### 2.7 ML Tracking: MLflow

**Version:** 2.4.0

**Role:**
- Experiment tracking
- Model versioning
- Parameters & metrics logging
- Model registry

**Key Features:**
- Artifact storage in MinIO
- Metadata in PostgreSQL
- Web UI on port 5000

**Example:**

```python
import mlflow

mlflow.start_run(experiment_name="solar_forecast")
mlflow.log_params({"model": "xgboost", "depth": 5})
mlflow.log_metrics({"rmse": 0.15, "mae": 0.12})
mlflow.sklearn.log_model(model, "solar_forecast_model")
mlflow.end_run()
```

**Model Registry:**
- Track model versions
- Stage transitions (Staging → Production)
- Model metadata & documentation

## 3. Development Stack

### 3.1 Python Environment

**Version:** 3.11+

**Key Dependencies:**

```
# Core orchestration
prefect==2.9.0
mlflow==2.4.0

# Data processing
pandas==2.2.2
pyarrow==11.0.0
pyspark==3.5.1

# Storage
boto3==1.26.149

# Database
psycopg2-binary==2.9.7

# HTTP
requests==2.31.0

# Testing
pytest==7.4.0
```

### 3.2 Code Organization

```
src/
└── pv_lakehouse/
    ├── __init__.py
    └── etl/
        ├── __init__.py
        ├── bronze_ingest.py       # Bronze layer ingestion
        ├── silver_transform.py    # Silver transformations (TBD)
        ├── gold_aggregate.py      # Gold layer aggregations (TBD)
        └── utils/
            ├── spark_utils.py     # Spark helpers
            └── data_utils.py      # Data validation

flows/
├── bronze_to_silver.py    # Prefect orchestration
└── scheduled_etl.py       # Production flows

sql/
├── bronze/
│   ├── oe_facilities_raw.sql
│   ├── oe_generation_hourly_raw.sql
│   ├── om_weather_hourly_raw.sql
│   └── om_air_quality_hourly_raw.sql
├── silver/               # (TBD)
└── gold/                 # (TBD)
```

### 3.3 Docker Containerization

**Base Images:**
- `apache/spark:3.5.1` - Spark runtime
- `trinodb/trino:latest` - Trino runtime
- `postgres:15` - PostgreSQL
- `minio/minio:latest` - MinIO
- `prefect:2.9-latest` - Prefect
- `mlflow:latest` - MLflow

**Docker Compose Profiles:**
```yaml
core:         # Essential (MinIO, Postgres, Trino, Spark)
spark:        # Spark cluster (master + worker)
ml:           # MLflow server
orchestrate:  # Prefect server
```

**Resource Limits:**
```yaml
spark-worker:
  deploy:
    resources:
      limits:
        memory: 4g  # Configurable per environment
```

## 4. Network & Connectivity

### 4.1 Service Ports

| Service | Port | Purpose |
|---------|------|---------|
| MinIO API | 9000 | S3 operations |
| MinIO Console | 9001 | Web UI |
| Trino | 8081 | SQL queries |
| PostgreSQL | 5432 | Database |
| Spark Master UI | 8080 | Cluster monitoring |
| Spark Worker UI | 8081+ | Worker monitoring |
| Prefect UI | 4200 | Workflow dashboard |
| MLflow UI | 5000 | Experiment tracking |
| pgAdmin | 5050 | DB admin |

### 4.2 Connectivity Matrix

```
Service       │ MinIO │ Postgres │ Trino │ Spark │ Prefect │ MLflow
──────────────┼───────┼──────────┼───────┼───────┼─────────┼────────
MinIO         │   ✓   │    ✓     │   ✓   │   ✓   │         │   ✓
Postgres      │       │    ✓     │   ✓   │   ✓   │    ✓    │   ✓
Trino         │   ✓   │    ✓     │   ✓   │       │         │
Spark         │   ✓   │    ✓     │       │   ✓   │         │   ✓
Prefect       │       │    ✓     │       │   ✓   │    ✓    │
MLflow        │   ✓   │    ✓     │       │       │         │   ✓
```

## 5. Security Components

### 5.1 Authentication

**MinIO:**
- Root user/password (admin)
- Service users with individual credentials
- Access key/secret pairs

**PostgreSQL:**
- Default user: `pvlakehouse`
- Change password in production
- Service-specific credentials (optional)

**Trino:**
- No built-in auth (in this setup)
- Optionally: LDAP/SASL (TBD)

### 5.2 Authorization

**MinIO Policies:**
```json
lakehouse-rw.json      // Read/write lakehouse bucket
mlflow-rw.json         // Read/write mlflow bucket
```

**Database:**
- All services: pvlakehouse user
- Schema-level permissions (future)

### 5.3 Data Encryption

**At Rest:**
- MinIO: Optional server-side encryption (TBD)
- PostgreSQL: Optional encryption (TBD)

**In Transit:**
- Docker network: Isolated container network
- Production: Enable SSL/TLS (TBD)

## 6. Monitoring & Observability

### 6.1 Health Checks

```bash
# Health check components
docker-compose exec minio curl -f http://localhost:9000/minio/health/ready
docker-compose exec postgres pg_isready -U pvlakehouse
docker-compose exec trino trino --version
docker-compose exec spark-master curl -f http://localhost:8080
```

### 6.2 Logging

**Docker Logging:**
- Driver: json-file
- Max size: 10MB
- Max files: 3 rotation

**Application Logs:**
- Spark: stderr/stdout in container
- Trino: Logs directory in container
- Prefect: Centralized in database

**Access:**
```bash
docker-compose logs -f <service>
docker-compose logs --tail=100 spark-master
```

### 6.3 Metrics (Future)

- Prometheus metrics collection (TBD)
- Grafana dashboards (TBD)
- Alert thresholds (TBD)

## 7. Version Compatibility

### 7.1 Tested Combinations

| Component | Version | Status |
|-----------|---------|--------|
| Spark | 3.5.1 | ✅ Tested |
| Iceberg | 1.4+ | ✅ Tested |
| Trino | 428+ | ✅ Tested |
| PostgreSQL | 15 | ✅ Tested |
| MinIO | 2024.x | ✅ Tested |
| Prefect | 2.9+ | ✅ Tested |
| MLflow | 2.4.0 | ✅ Tested |
| Python | 3.11+ | ✅ Tested |

### 7.2 Upgrade Path

- **Minor updates**: Safe (2.4.0 → 2.4.1)
- **Major updates**: Test in staging first
- **Dependencies**: Pin versions in requirements.txt

## 8. Performance Characteristics

### 8.1 Query Performance

| Operation | Typical Time | Notes |
|-----------|-------------|-------|
| Simple SELECT * | <1s | Partition pruning enabled |
| Aggregation (1 day) | 1-5s | Depends on data size |
| Join (2 tables) | 5-30s | Shuffle operation |
| Full table scan | 10-60s | No filtering |

### 8.2 Ingestion Performance

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| Parquet write | 100-500 MB/s | Network I/O limited |
| Spark job startup | 10-30s | JVM startup + config |
| Daily ingest (all sources) | <5 min | Typical SLA |

### 8.3 Scalability

- **Data Volume**: Petabytes (with proper infrastructure)
- **Concurrent Users**: Trino (100+), limited by resources
- **File Count**: Millions (Iceberg optimizes automatically)

## 9. Disaster Recovery

### 9.1 Backup Strategy

**Critical Data:**
- PostgreSQL: Daily backups
- MinIO buckets: Versioning enabled
- Configuration files: In git repository

**Recovery Options:**
1. Restore PostgreSQL dump
2. Restart MinIO with existing volumes
3. Rerun ETL for specific date range

### 9.2 RTO/RPO

- **Recovery Time Objective (RTO)**: <1 hour
- **Recovery Point Objective (RPO)**: <24 hours

---

**References:**
- [Apache Iceberg Docs](https://iceberg.apache.org/)
- [Trino Documentation](https://trino.io/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/)
- [MinIO Quickstart Guide](https://min.io/docs/minio/linux/index.html)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
