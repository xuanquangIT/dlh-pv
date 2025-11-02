# Infrastructure - MinIO Setup & Configuration

## 1. MinIO Overview

MinIO là S3-compatible object storage dùng để lưu trữ dữ liệu Iceberg (Parquet files) và MLflow artifacts.

**Đặc Điểm:**
- ✅ S3-compatible API
- ✅ Bucket policies & access control
- ✅ Versioning & lifecycle management
- ✅ Multi-tenant support
- ✅ High performance

## 2. Architecture

```
┌─────────────────────────────────────┐
│       MinIO Server                  │
│                                     │
│  ┌──────────────┬──────────────┐   │
│  │  lakehouse   │   mlflow     │   │
│  │   bucket     │   bucket     │   │
│  └──────────────┴──────────────┘   │
│                                     │
│  Service Users:                     │
│  ├─ spark_svc (lakehouse-rw)       │
│  ├─ trino_svc (lakehouse-rw)       │
│  └─ mlflow_svc (mlflow-rw)         │
└─────────────────────────────────────┘
         │
         ↓ (S3A Protocol)
┌─────────────────────────────────────┐
│   Compute Services                  │
│   ├─ Spark                          │
│   ├─ Trino                          │
│   └─ MLflow                         │
└─────────────────────────────────────┘
```

## 3. Setup Process

### 3.1 Automatic Setup (Docker Compose)

```yaml
# docker-compose.yml
services:
  mc:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    volumes:
      - ../infra/minio/policies:/policies:ro
    environment:
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: pvlakehouse
      MINIO_ROOT_PASSWORD: pvlakehouse
      SPARK_SVC_ACCESS_KEY: spark_svc
      SPARK_SVC_SECRET_KEY: spark_secret_change_me
      # ... more env vars
    entrypoint: >
      /bin/sh -c "
      mc alias set local ${MINIO_ENDPOINT} ... ;
      mc mb -p local/${S3_WAREHOUSE_BUCKET};
      mc mb -p local/${S3_MLFLOW_BUCKET};
      mc admin policy create local lakehouse-rw /policies/lakehouse-rw.json;
      mc admin user add local ${SPARK_SVC_ACCESS_KEY} ${SPARK_SVC_SECRET_KEY};
      mc admin policy attach local lakehouse-rw --user ${SPARK_SVC_ACCESS_KEY};
      "
    profiles: [core]
```

**Steps Performed:**
1. Create buckets: `lakehouse` & `mlflow`
2. Set buckets to private
3. Enable versioning
4. Create policies from JSON files
5. Create service users
6. Attach policies to users

### 3.2 Manual Setup (Development)

```bash
# Install mc client
docker run --rm minio/mc:latest --version

# Alias
docker run --rm -it --entrypoint /bin/bash minio/mc:latest

# Inside container:
mc alias set local http://minio:9000 pvlakehouse pvlakehouse

# Create buckets
mc mb local/lakehouse
mc mb local/mlflow

# Set to private
mc anonymous set private local/lakehouse
mc anonymous set private local/mlflow

# Enable versioning
mc version enable local/lakehouse
mc version enable local/mlflow

# Create policies
mc admin policy create local lakehouse-rw /policies/lakehouse-rw.json

# Create service users
mc admin user add local spark_svc spark_secret_change_me
mc admin user add local trino_svc trino_secret_change_me
mc admin user add local mlflow_svc mlflow_secret_change_me

# Attach policies
mc admin policy attach local lakehouse-rw --user spark_svc
mc admin policy attach local lakehouse-rw --user trino_svc
mc admin policy attach local mlflow-rw --user mlflow_svc

# Verify
mc admin policy ls local
mc admin user ls local
```

## 4. Bucket Policies

### 4.1 Lakehouse Policy

**File:** `infra/minio/policies/lakehouse-rw.json`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketVersioning",
        "s3:GetBucketObjectLockConfiguration"
      ],
      "Resource": "arn:aws:s3:::lakehouse"
    },
    {
      "Sid": "ReadWriteObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion"
      ],
      "Resource": "arn:aws:s3:::lakehouse/*"
    },
    {
      "Sid": "ReadWriteMetadata",
      "Effect": "Allow",
      "Action": [
        "s3:GetObjectTagging",
        "s3:PutObjectTagging",
        "s3:ListBucketVersions"
      ],
      "Resource": [
        "arn:aws:s3:::lakehouse",
        "arn:aws:s3:::lakehouse/*"
      ]
    }
  ]
}
```

**Permissions:**
- ✅ ListBucket - List objects
- ✅ GetObject - Read data files
- ✅ PutObject - Write Iceberg data
- ✅ DeleteObject - Cleanup old files
- ✅ GetBucketVersioning - Track versions

### 4.2 MLflow Policy

**File:** `infra/minio/policies/mlflow-rw.json`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "FullAccessMLflow",
      "Effect": "Allow",
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::mlflow",
        "arn:aws:s3:::mlflow/*"
      ]
    }
  ]
}
```

## 5. Service Users & Credentials

### 5.1 Service Users

| User | Access Key | Secret Key | Policy | Used By |
|------|-----------|-----------|--------|---------|
| `spark_svc` | `spark_svc` | `spark_secret_change_me` | `lakehouse-rw` | Spark cluster |
| `trino_svc` | `trino_svc` | `trino_secret_change_me` | `lakehouse-rw` | Trino engine |
| `mlflow_svc` | `mlflow_svc` | `mlflow_secret_change_me` | `mlflow-rw` | MLflow server |

### 5.2 Credential Management

**Environment Variables:**
```bash
# docker/.env
SPARK_SVC_ACCESS_KEY=spark_svc
SPARK_SVC_SECRET_KEY=spark_secret_change_me

TRINO_SVC_ACCESS_KEY=trino_svc
TRINO_SVC_SECRET_KEY=trino_secret_change_me

MLFLOW_SVC_ACCESS_KEY=mlflow_svc
MLFLOW_SVC_SECRET_KEY=mlflow_secret_change_me
```

**Production Recommendations:**
- ✅ Use strong passwords (32+ chars)
- ✅ Store in secret management (AWS Secrets, HashiCorp Vault)
- ✅ Rotate credentials quarterly
- ✅ Audit access logs
- ✅ Use different credentials per environment

## 6. Bucket Structure

### 6.1 Lakehouse Bucket

```
lakehouse/
├── warehouse/               # Iceberg warehouse location
│   └── lh/
│       ├── bronze/
│       │   ├── oe_facilities_raw-<uuid>/
│       │   ├── oe_generation_hourly_raw-<uuid>/
│       │   ├── om_weather_hourly_raw-<uuid>/
│       │   └── om_air_quality_hourly_raw-<uuid>/
│       │
│       ├── silver/          # (Future)
│       │   ├── generation_normalized-<uuid>/
│       │   └── ...
│       │
│       └── gold/            # (Future)
│           ├── fact_daily_generation-<uuid>/
│           └── ...
│
└── metadata/                # Optional: Iceberg metadata exports
    └── backups/
```

### 6.2 MLflow Bucket

```
mlflow/
├── artifacts/
│   ├── <exp-id>/
│   │   └── <run-id>/
│   │       ├── model/
│   │       ├── metrics/
│   │       └── params/
│   └── ...
│
└── backups/                 # Optional
```

## 7. Data Storage & Performance

### 7.1 File Format

- **Format**: Parquet (columnar)
- **Compression**: Snappy (default)
- **Block Size**: 128MB

```bash
# Example S3A path
s3a://lakehouse/warehouse/lh/bronze/oe_generation_hourly_raw-abcd1234/
  year=2025/
  month=01/
  day=15/
    00000-xyz.parquet      # ~100MB
    00001-abc.parquet
```

### 7.2 Versioning & Retention

**Versioning Enabled:**
- Keeps old versions of files
- Enables rollback if needed

**Lifecycle Policy (Optional):**
```bash
# Keep current version + 7 days of history
# Older versions → delete

mc ilm rule add local/lakehouse \
  --id "archive-old-versions" \
  --noncurrent-version-expiration-days 7
```

## 8. Access & Security

### 8.1 External Access (Development)

**MinIO API:** `http://localhost:9000`
**MinIO Console:** `http://localhost:9001`

**Credentials:**
```bash
User: pvlakehouse
Password: pvlakehouse
```

### 8.2 Internal Access (Containers)

**Spark/Trino/MLflow:**
```bash
Endpoint: http://minio:9000  # Internal network
Access Key: <service_user>
Secret Key: <service_secret>
```

**S3A Configuration:**
```properties
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=spark_svc
s3.aws-secret-key=spark_secret_change_me
```

### 8.3 Encryption (Future)

```bash
# Enable server-side encryption at rest (TBD)
mc ilm rule add local/lakehouse \
  --encrypt SSE-S3 \
  --encrypt-key <key-id>
```

## 9. Monitoring & Maintenance

### 9.1 Bucket Info

```bash
# Get bucket info
docker run --rm --network dlhpv_data-net \
  --entrypoint /bin/sh minio/mc:latest -c '
  mc alias set local http://minio:9000 pvlakehouse pvlakehouse
  mc du local/lakehouse
  mc du local/mlflow
'

# Example output:
# lakehouse       : 125 GiB
# mlflow          : 32 GiB
```

### 9.2 Health Checks

```bash
# MinIO health
docker compose exec minio curl -f http://localhost:9000/minio/health/ready

# Expected: 200 OK

# Policy verification
docker run --rm --network dlhpv_data-net \
  --entrypoint /bin/sh minio/mc:latest -c '
  mc alias set local http://minio:9000 pvlakehouse pvlakehouse
  mc admin policy info local lakehouse-rw
'
```

### 9.3 Backup

```bash
# Backup buckets to local disk
docker run --rm --network dlhpv_data-net \
  -v /backup:/backup \
  --entrypoint /bin/sh minio/mc:latest -c '
  mc alias set local http://minio:9000 pvlakehouse pvlakehouse
  mc mirror local/lakehouse /backup/lakehouse
  mc mirror local/mlflow /backup/mlflow
'
```

## 10. Troubleshooting

### Issue: Cannot create buckets

```bash
# Check MinIO logs
docker compose logs minio

# Check mc container
docker compose logs mc

# Verify connectivity
docker compose exec mc mc ls local
```

### Issue: Permission denied

```bash
# Verify policy attached
docker run --rm --network dlhpv_data-net \
  --entrypoint /bin/sh minio/mc:latest -c '
  mc alias set local http://minio:9000 pvlakehouse pvlakehouse
  mc admin user info local spark_svc
'

# Check policy content
mc admin policy info local lakehouse-rw
```

### Issue: Out of disk space

```bash
# Check bucket sizes
mc du local/

# Remove old versions
mc rm --recursive --force local/lakehouse --versions

# Or delete specific old snapshots (handled by Iceberg)
```

## 11. Reference

- [MinIO Documentation](https://min.io/docs/)
- [S3 API Reference](https://docs.aws.amazon.com/s3/latest/API/)
- [Iceberg Configuration](https://iceberg.apache.org/docs/latest/getting-started/)

---

**Related:**
- [PostgreSQL Catalog Setup](postgresql-catalog.md)
- [Trino Configuration](trino-configuration.md)
- [Spark Setup](spark-setup.md)

**Status:** ✅ Configured & Operational
