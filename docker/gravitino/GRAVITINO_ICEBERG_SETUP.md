# Gravitino Iceberg REST Catalog - Setup Guide

## ✅ Successfully Fixed!

The Gravitino Iceberg REST catalog is now fully operational with PostgreSQL backend and MinIO S3 storage.

## Problem & Solution

### Issue
The Apache Gravitino Iceberg REST image (`apache/gravitino-iceberg-rest:latest`) doesn't include the PostgreSQL JDBC driver by default, only SQLite. This caused errors when trying to use PostgreSQL as the catalog backend.

### Solution
Created a custom Docker image that extends the base Gravitino image and adds the PostgreSQL JDBC driver.

**File:** `docker/gravitino/Dockerfile`
```dockerfile
FROM apache/gravitino-iceberg-rest:latest

# Download PostgreSQL JDBC driver
USER root
RUN cd /root/gravitino-iceberg-rest-server/libs && \
    curl -L -O https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    chmod 644 postgresql-42.7.3.jar

USER root
```

## Configuration

### Environment Variables
The Gravitino Iceberg REST server uses the following environment variables (configured in `docker-compose.yml`):

- `GRAVITINO_URI`: JDBC connection URL to PostgreSQL
- `GRAVITINO_JDBC_DRIVER`: `org.postgresql.Driver`
- `GRAVITINO_JDBC_USER`: Database username
- `GRAVITINO_JDBC_PASSWORD`: Database password
- `GRAVITINO_WAREHOUSE`: S3 warehouse location (`s3a://lakehouse/warehouse`)
- `GRAVITINO_S3_ENDPOINT`: MinIO endpoint
- `GRAVITINO_S3_ACCESS_KEY`: MinIO access key
- `GRAVITINO_S3_SECRET_KEY`: MinIO secret key
- `GRAVITINO_S3_REGION`: S3 region

## API Endpoints

The Gravitino Iceberg REST API is available at:

**Base URL:** `http://localhost:8181/iceberg/v1/`

Note: Gravitino uses the `/iceberg/v1/` prefix, not just `/v1/`

Note about the container image
------------------------------
We build a small custom image `gravitino-iceberg-rest-postgres:latest` (see `docker/gravitino/Dockerfile`) which
adds the PostgreSQL JDBC driver to the upstream `apache/gravitino-iceberg-rest` image so the server can connect
to a PostgreSQL catalog. If you prefer a different image you can change `ICEBERG_REST_IMAGE` in `docker/.env` or
modify the compose service to use `image:` instead of `build:`.

pgAdmin
-------
pgAdmin is also available in the stack to make exploring the PostgreSQL metadata easier. Access it at:

- http://localhost:5050 (login with pgAdmin credentials from `docker/.env` — `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD`)


### Common Endpoints

- **Config:** `GET http://localhost:8181/iceberg/v1/config`
- **List Namespaces:** `GET http://localhost:8181/iceberg/v1/namespaces`
- **Create Namespace:** `POST http://localhost:8181/iceberg/v1/namespaces`
- **List Tables:** `GET http://localhost:8181/iceberg/v1/namespaces/{namespace}/tables`
- **Create Table:** `POST http://localhost:8181/iceberg/v1/namespaces/{namespace}/tables`

## Testing

### 1. Check API Config
```bash
curl -s http://localhost:8181/iceberg/v1/config | jq
```

### 2. List Namespaces
```bash
curl -s http://localhost:8181/iceberg/v1/namespaces | jq
```

### 3. Create a Namespace
```bash
curl -X POST http://localhost:8181/iceberg/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": ["my_database"],
    "properties": {
      "description": "My test database"
    }
  }' | jq
```

### 4. Verify in PostgreSQL
```bash
docker exec postgres psql -U pvlakehouse -d iceberg -c "SELECT * FROM iceberg_namespace_properties;"
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Lakehouse Stack                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────┐    ┌──────────────────┐    ┌──────────┐     │
│  │  Trino   │───▶│ Iceberg REST API │───▶│PostgreSQL│     │
│  │  :8081   │    │   (Gravitino)    │    │  :5432   │     │
│  └──────────┘    │     :8181        │    └──────────┘     │
│                  └──────────────────┘           │          │
│                           │                     │          │
│                           ▼                     │          │
│                     ┌──────────┐                │          │
│                     │  MinIO   │                │          │
│                     │  S3 API  │                │          │
│                     │  :9000   │                │          │
│                     └──────────┘                │          │
│                           │                     │          │
│                           ▼                     ▼          │
│                   s3a://lakehouse/     Catalog Metadata   │
│                       warehouse/                           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Data Storage

- **Metadata:** Stored in PostgreSQL database `iceberg` (tables: `iceberg_namespace_properties`, `iceberg_tables`)
- **Data Files:** Stored in MinIO bucket `lakehouse` under `/warehouse/` path
- **Warehouse Root:** `s3a://lakehouse/warehouse`

## Status

All core services are running and healthy:

| Service | Status | Port | Purpose |
|---------|--------|------|---------|
| **iceberg-rest** | ✅ Healthy | 8181 | Iceberg REST Catalog (Gravitino) |
| **postgres** | ✅ Healthy | 5432 | Catalog metadata storage |
| **minio** | ✅ Healthy | 9000, 9001 | S3-compatible object storage |
| **trino** | ✅ Healthy | 8081 | Query engine |
| **spark-master** | ✅ Healthy | 4040, 7077 | Spark cluster master |
| **spark-worker** | ✅ Running | - | Spark worker node |

## Next Steps

1. **Connect from Trino:**
   - Trino should auto-connect via the iceberg catalog configuration
   - Test: `docker exec -it trino trino`

2. **Create Iceberg tables:**
   - Use the REST API or connect via Spark/Trino
   - Tables will be stored in MinIO and tracked in PostgreSQL

3. **Query data:**
   - Use Trino to query Iceberg tables
   - Access Trino UI at http://localhost:8081

## Troubleshooting

### Check Gravitino Logs
```bash
docker logs iceberg-rest
docker exec iceberg-rest cat /root/gravitino-iceberg-rest-server/logs/gravitino-iceberg-rest-server.log
```

### Verify PostgreSQL JDBC Driver
```bash
docker exec iceberg-rest ls -lh /root/gravitino-iceberg-rest-server/libs/ | grep postgres
```

### Test Database Connection
```bash
docker exec postgres psql -U pvlakehouse -d iceberg -c "\dt"
```

### Rebuild Custom Image
If you need to rebuild:
```bash
docker compose --project-name dlhpv build iceberg-rest
docker compose --project-name dlhpv up -d iceberg-rest
```

## References

- [Apache Gravitino](https://gravitino.apache.org/)
- [Apache Iceberg REST Catalog Specification](https://iceberg.apache.org/docs/latest/api/)
- [Gravitino Iceberg REST Documentation](https://gravitino.apache.org/docs/latest/iceberg-rest-service/)
