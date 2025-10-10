# Quick Start

## For New Team Members

**Get the entire lakehouse platform running in 3 commands:**

```bash
cd docker
docker compose --profile core --profile ml --profile orchestrate up -d --build
./scripts/health-check.sh
```

That's it!

## What Gets Set Up Automatically

- ✅ MinIO object storage with buckets: `lakehouse`, `mlflow`
- ✅ PostgreSQL with databases: `iceberg`, `iceberg_catalog`, `mlflow`, `prefect`
- ✅ **Apache Iceberg JDBC Catalog** (PostgreSQL-based, no Gravitino needed)
- ✅ Trino SQL query engine with Iceberg catalog configured
- ✅ Apache Spark cluster with S3A support
- ✅ MLflow experiment tracking
- ✅ Prefect workflow orchestration
- ✅ Service users with least-privilege bucket policies
- ✅ All networking and healthchecks configured
- ✅ Trino Iceberg schemas: `lh`

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | pvlakehouse / pvlakehouse |
| Trino UI | http://localhost:8081 | - |
| MLflow UI | http://localhost:5000 | - |
| Prefect UI | http://localhost:4200 | - |
| Spark Master UI | http://localhost:4040 | - |

## Common Commands

**Start everything:**
```bash
docker compose --profile core --profile ml --profile orchestrate up -d
```

**Stop everything:**
```bash
docker compose down
```

**View logs:**
```bash
docker compose logs -f <service-name>
# Example: docker compose logs -f spark-master
```

**Restart a service:**
```bash
docker compose restart <service-name>
```

**Run health check:**
```bash
./scripts/health-check.sh
```

**Clean up (⚠️ removes all data):**
```bash
docker compose down -v
```

## Health Check Script

The `health-check.sh` script validates:

1. All containers are healthy
2. MinIO buckets exist
3. MinIO policies are created  
4. Service users exist with correct permissions
5. PostgreSQL databases exist
6. Iceberg catalog tables exist
7. Trino Iceberg catalog is configured correctly
   - SHOW CATALOGS lists `iceberg`
   - SHOW SCHEMAS FROM iceberg returns schemas
   - SELECT 1 query works
   - DDL permissions verified (schema creation)
   - Schema `lh` exists in Iceberg catalog
8. All service endpoints respond
9. Configuration files exist in repository
10. End-to-end S3A write test (Trino -> MinIO)

## Troubleshooting

**Containers not starting?**
```bash
docker compose logs <service-name>
docker compose ps
```

**Need to rebuild?**
```bash
docker compose down
docker compose up -d --build
```

**Port conflicts?**
```bash
# Check what's using a port
sudo netstat -tlnp | grep <port>
# Or on macOS:
lsof -i :<port>
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Data Lakehouse                      │
├─────────────┬──────────────┬─────────────┬─────────────┤
│   Storage   │   Catalog    │   Compute   │     ML      │
├─────────────┼──────────────┼─────────────┼─────────────┤
│   MinIO     │  PostgreSQL  │    Trino    │   MLflow    │
│  (S3 API)   │ JDBC Catalog │   (SQL)     │ (Tracking)  │
│             │  (Iceberg    │             │             │
│             │  metadata)   │    Spark    │   Prefect   │
│             │              │  (Python)   │ (Workflows) │
└─────────────┴──────────────┴─────────────┴─────────────┘
```

**Note:** We use Iceberg's JDBC catalog with PostgreSQL instead of REST catalog.
This eliminates Hadoop library compatibility issues and simplifies the architecture.

## Trino Iceberg Catalog

Trino is configured with an Iceberg catalog using **PostgreSQL JDBC catalog** with S3A storage on MinIO.

**Verify Trino Iceberg Catalog:**
```bash
# Connect to Trino CLI
docker exec -it trino trino

# In the Trino CLI, run:
SHOW CATALOGS;                    -- Should list: iceberg, system
SHOW SCHEMAS FROM iceberg;        -- Should list: information_schema, lh, system
SELECT 1;                          -- Basic query test
```

**Create tables in Iceberg:**
```sql
-- Create a test table
CREATE TABLE iceberg.lh.test_table (
    id INT,
    name VARCHAR
) WITH (
    format = 'PARQUET'
);

-- Insert data
INSERT INTO iceberg.lh.test_table VALUES (1, 'test');

-- Query data
SELECT * FROM iceberg.lh.test_table;

-- View metadata in PostgreSQL
-- Run from host:
docker exec postgres psql -U pvlakehouse -d iceberg_catalog -c "SELECT * FROM iceberg_tables;"
```

**Configuration Details:**
- Catalog type: `jdbc` (PostgreSQL JDBC)
- Database: `iceberg_catalog` in PostgreSQL
- Warehouse location: `s3a://lakehouse/warehouse`
- S3 endpoint: `http://minio:9000`
- Path-style access: `true`
- Service user: `trino_svc` with `lakehouse-rw` policy

**Metadata Tables:**
- `iceberg_tables` - Stores table locations and metadata
- `iceberg_namespace_properties` - Stores schema/namespace properties

**Note:** Schema tables are automatically created by `postgres-init.sql` on first startup.

Configuration file: `docker/trino/catalog/iceberg.properties`

**Benefits of JDBC Catalog:**
- ✅ No Hadoop version conflicts
- ✅ Simpler architecture (no REST catalog service needed)
- ✅ Direct PostgreSQL access for debugging
- ✅ Better performance (no REST overhead)
- ✅ Production-ready and stable

## What's Different from Standard Setup?

1. **Custom Spark Image**: Built with hadoop-aws and AWS SDK for S3A support
2. **Auto-initialization**: MinIO buckets, policies, and users created on startup
3. **Service Users**: Spark, Trino, and MLflow use dedicated service accounts (not root)
4. **Least Privilege**: Each service has minimal required permissions
5. **Hadoop Config**: Custom core-site.xml to fix duration-string parsing issues
6. **Trino Iceberg Catalog**: Pre-configured with PostgreSQL JDBC catalog (no REST catalog/Gravitino needed)
7. **Iceberg Metadata**: Catalog schema tables automatically created in PostgreSQL on first startup

## Files Structure

```
docker/
├── docker-compose.yml          # Main orchestration file
├── .env                        # Environment variables (DO NOT COMMIT)
├── .env.example               # Template for .env
├── postgres/
│   ├── postgres-init.sh       # Auto-creates databases
│   └── postgres-init.sql      # SQL initialization
├── spark/
│   ├── Dockerfile             # Custom Spark image with S3A
│   └── core-site.xml          # Hadoop configuration
├── trino/
│   └── catalog/
│       ├── iceberg.properties         # Trino-Iceberg catalog configuration
│       └── iceberg.properties.template # Template with env vars
├── gravitino/
│   └── Dockerfile             # Iceberg REST catalog image
└── scripts/
    └── health-check.sh        # Comprehensive health check script
```

## For More Details

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for:
- Detailed architecture
- Configuration options
- Advanced troubleshooting
- Production considerations
- Backup and recovery

## Security Notes (⚠️ Important for Production)

**Current configuration uses default credentials for LOCAL DEVELOPMENT only.**

For production:
1. Copy `.env.example` to `.env`
2. Change all passwords and access keys
3. Use secrets management (e.g., Docker secrets, Vault)
4. Enable TLS/SSL for all services
5. Configure firewall rules
6. Use separate service accounts for each component

## Support

If verification fails:
1. Check the script output for specific failures
2. View service logs: `docker compose logs <service>`
3. Ensure no port conflicts
4. Verify Docker has sufficient resources (8GB+ RAM recommended)
5. Check the SETUP_GUIDE.md troubleshooting section

---

**Happy Data Engineering!** 🎉
