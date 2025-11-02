# Kiến Trúc Hệ Thống Chi Tiết

## 1. Tổng Quan Kiến Trúc

PV Lakehouse sử dụng kiến trúc **layered** với phân tách rõ ràng giữa các tier:

```
                    ┌─────────────────┐
                    │  BI Tools       │
                    │  Dashboards     │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Gold Layer     │
                    │  (Analytics)    │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Silver Layer   │
                    │  (Normalized)   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Bronze Layer   │
                    │  (Raw)          │
                    └────────┬────────┘
                             │
              ┌──────────────▼──────────────┐
              │  Apache Iceberg Catalog     │
              │  (PostgreSQL JDBC)          │
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │  MinIO S3 Object Storage    │
              │  (Parquet Files)            │
              └─────────────────────────────┘
```

## 2. Chi Tiết Từng Thành Phần

### 2.1 Storage Layer: MinIO

**Chức Năng:**
- Lưu trữ dữ liệu dạng object (S3-compatible)
- Lưu các file Parquet của Iceberg tables
- Lưu artifacts của MLflow

**Cấu Trúc Buckets:**
```
minio/
├── lakehouse/              # Iceberg data warehouse
│   └── warehouse/
│       └── lh/             # Namespace "lh"
│           ├── bronze/     # Bronze tables
│           ├── silver/     # Silver tables
│           └── gold/       # Gold tables
└── mlflow/                 # MLflow artifacts
    └── <experiment-id>/
```

**Service Users:**
- `spark_svc` - Read/Write to lakehouse bucket (lakehouse-rw policy)
- `trino_svc` - Read/Write to lakehouse bucket (lakehouse-rw policy)
- `mlflow_svc` - Read/Write to mlflow bucket (mlflow-rw policy)

**Versioning & Retention:**
- Versioning enabled trên cả 2 buckets
- Iceberg handles file cleanup qua garbage collection

### 2.2 Catalog Layer: PostgreSQL + JDBC Catalog

**Chức Năng:**
- Lưu trữ metadata của Iceberg tables
- Track table schemas, partitions, snapshots
- Enable schema evolution & time travel

**Databases:**
```sql
-- Iceberg catalog
iceberg_catalog (
  - iceberg_tables: Lưu table locations & metadata
  - iceberg_namespace_properties: Lưu namespace properties
)

-- Trino metadata
trino (
  - config: Trino configuration
)

-- Prefect orchestration
prefect (
  - flows, runs, task runs
)

-- MLflow tracking
mlflow (
  - experiments, runs, metrics, parameters
)
```

**JDBC Catalog Config:**
```properties
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://postgres:5432/iceberg_catalog
iceberg.jdbc-catalog.default-warehouse-dir=s3a://lakehouse/warehouse
```

**Lợi Ích so với REST Catalog:**
- ✅ Không cần service riêng (Gravitino)
- ✅ Tránh version conflicts của Hadoop
- ✅ Direct access to metadata cho debugging
- ✅ Better performance (no REST overhead)

### 2.3 Compute Layer

#### 2.3.1 Trino (SQL Query Engine)

**Chức Năng:**
- Interactive SQL queries trên Iceberg tables
- Distributed query execution
- Catalog: `iceberg` (configured via JDBC)

**Configuration:**
```properties
connector.name=iceberg
iceberg.catalog.type=jdbc
# ... JDBC connection details
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.aws-access-key=trino_svc
```

**Use Cases:**
- Ad-hoc data exploration
- Verify data quality
- Quick validations

#### 2.3.2 Apache Spark (Batch Processing)

**Chức Năng:**
- Data ingestion từ external sources
- ETL transformations
- Aggregations & feature engineering

**Configuration:**
```bash
# S3A configuration
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=spark_svc
spark.hadoop.fs.s3a.secret.key=spark_svc_secret
spark.hadoop.fs.s3a.path.style.access=true
```

**Iceberg Integration:**
- Spark reads/writes Iceberg tables
- Supports schema evolution
- Partition management

### 2.4 Metadata Layer: Iceberg

**Chức Năng:**
- Open table format cho structured data
- ACID transactions
- Schema evolution
- Time travel queries
- Partition management

**Format Version:**
- Version 2 (latest, recommended)
- Better performance & features

**Partitioning Strategy:**
```sql
PARTITIONED BY (days(ts_utc))
-- Efficient for time-series data
-- Enables daily cleanup policies
```

**Metadata Columns (Bronze):**
```sql
_ingest_time TIMESTAMP    -- When data landed
_source STRING            -- Data source identifier
_hash STRING              -- Dedup hash (SHA256)
```

## 3. Data Flow Architecture

### 3.1 Ingestion Flow

```
External Source
(OpenNEM API)
    ↓
[Spark Job]
    ├─ Extract data via HTTP
    ├─ Transform & validate
    ├─ Convert to Parquet
    └─ Write to MinIO + Iceberg metadata
    ↓
PostgreSQL
(Update iceberg_tables)
    ↓
Bronze Table
(lh.bronze.oe_generation_hourly_raw)
    ├─ Partition: days(ts_utc)
    ├─ Format: Apache Iceberg v2
    └─ Schema: Time-series + metadata
```

### 3.2 Query Flow

```
User Query (SQL)
    ↓
[Trino]
    ├─ Parse & plan
    ├─ Consult PostgreSQL Catalog
    │  (resolve table location)
    └─ Fetch schema & partitions
    ↓
[S3A Connector]
    ├─ Connect to MinIO
    │  (credentials: trino_svc)
    └─ Read Parquet files from
       s3a://lakehouse/warehouse/...
    ↓
[Execution]
    ├─ Filter rows (partition pruning)
    ├─ Project columns
    └─ Return results
```

### 3.3 Transformation Flow (ETL)

```
Bronze Tables
(Raw data)
    ↓
[Spark SQL/PySpark]
    ├─ Join multiple sources
    ├─ Apply business rules
    ├─ Deduplication
    ├─ Type conversion
    └─ Add calculated fields
    ↓
Silver Tables
(Normalized data)
    ├─ Partition: days(ts_utc)
    ├─ Format: Iceberg v2
    └─ Quality: deduplicated
    ↓
[Aggregation & Analytics]
    ├─ Hourly/daily aggregates
    ├─ KPI calculations
    └─ Derived metrics
    ↓
Gold Tables
(Analytics-ready)
    └─ Dashboard & report consumption
```

## 4. Network & Service Architecture

### 4.1 Container Network

```
┌──────────────────────────────────────────────────────┐
│               Docker Network: data-net                │
├──────────────────────────────────────────────────────┤
│                                                        │
│  ┌─────────┐    ┌──────────┐    ┌─────────┐         │
│  │  minio  │────│    mc    │    │ postgres│         │
│  │ :9000   │    │  setup   │    │  :5432  │         │
│  └─────────┘    └──────────┘    └─────────┘         │
│                                                        │
│  ┌──────────────┐    ┌─────────────┐                 │
│  │spark-master  │────│ spark-worker│                 │
│  │   :7077      │    │  cluster    │                 │
│  └──────────────┘    └─────────────┘                 │
│                                                        │
│  ┌─────────────────┐    ┌──────────────┐             │
│  │     trino       │    │   prefect    │             │
│  │    :8081        │    │   :4200      │             │
│  └─────────────────┘    └──────────────┘             │
│                                                        │
│  ┌──────────────┐    ┌──────────────┐                │
│  │   mlflow     │    │   pgadmin    │                │
│  │   :5000      │    │   :5050      │                │
│  └──────────────┘    └──────────────┘                │
│                                                        │
└──────────────────────────────────────────────────────┘
```

### 4.2 Service Connectivity

```
Trino
├─ PostgreSQL: JDBC → iceberg_catalog DB
├─ MinIO: S3A → lakehouse bucket
└─ Health: HTTP :8081

Spark Master/Worker
├─ MinIO: S3A → lakehouse bucket
├─ PostgreSQL: JDBC (optional)
└─ Health: HTTP :8080 / :8081

Prefect Server
├─ PostgreSQL: prefect DB
└─ Health: HTTP :4200

MLflow Server
├─ PostgreSQL: mlflow DB
├─ MinIO: S3A → mlflow bucket
└─ Health: HTTP :5000
```

## 5. Security & Access Control

### 5.1 MinIO Policies

**lakehouse-rw.json** (for spark_svc, trino_svc):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::lakehouse/*",
        "arn:aws:s3:::lakehouse"
      ]
    }
  ]
}
```

**mlflow-rw.json** (for mlflow_svc):
```json
{
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": ["arn:aws:s3:::mlflow/*", "arn:aws:s3:::mlflow"]
    }
  ]
}
```

### 5.2 Database Access

- PostgreSQL: All services connect as `pvlakehouse` user
- Production: Use separate credentials per service
- Iceberg JDBC: Read-only for query engines, write for ETL

## 6. Deployment Architecture

### 6.1 Docker Compose Profiles

```yaml
profiles:
  core:          # MinIO, PostgreSQL, Trino, Iceberg
  spark:         # Spark master & workers
  ml:            # MLflow
  orchestrate:   # Prefect
```

### 6.2 Resource Requirements

**Minimum (core only):**
- Memory: 4GB
- CPU: 2 cores
- Disk: 10GB

**Recommended (full stack):**
- Memory: 8GB
- CPU: 4 cores
- Disk: 20GB

## 7. Monitoring & Health Checks

### 7.1 Health Check Components

1. Container status
2. Database connectivity
3. S3 bucket verification
4. Service user permissions
5. Iceberg catalog verification
6. Trino query test
7. End-to-end write test

### 7.2 Logging

- Docker: JSON logging driver
- Max log file: 10MB, 3 file rotation
- Service logs: `docker compose logs <service>`

## 8. Extension Points

### 8.1 Adding New Data Sources

1. Create ingestion script in `src/pv_lakehouse/etl/`
2. Define Bronze DDL in `sql/bronze/`
3. Add Prefect flow in `flows/`
4. Update documentation

### 8.2 Custom Transformations

1. Create Silver/Gold tables in `sql/silver/` or `sql/gold/`
2. Write transformation logic in `src/pv_lakehouse/etl/`
3. Use Spark SQL or PySpark
4. Test with Trino queries

### 8.3 Integration with BI Tools

- Trino endpoint: `localhost:8081`
- Use Trino connectors from Tableau, Looker, etc.
- Create views on Gold tables for reporting

---

**References:**
- [Apache Iceberg](https://iceberg.apache.org/docs/)
- [Trino Documentation](https://trino.io/docs/)
- [MinIO Documentation](https://min.io/docs/)
- [Apache Spark](https://spark.apache.org/docs/)
