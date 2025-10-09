# 🚀 Quick Start

## For New Team Members

**Get the entire lakehouse platform running in 3 commands:**

```bash
cd docker
docker compose --profile core --profile ml --profile orchestrate up -d --build
./scripts/verify-setup.sh
```

That's it! ✅

## What Gets Set Up Automatically

- ✅ MinIO object storage with buckets: `lakehouse`, `mlflow`
- ✅ PostgreSQL with databases: `iceberg`, `mlflow`, `prefect`
- ✅ Apache Iceberg REST catalog
- ✅ Trino SQL query engine (connected to Iceberg)
- ✅ Apache Spark cluster with S3A support
- ✅ MLflow experiment tracking
- ✅ Prefect workflow orchestration
- ✅ Service users with least-privilege bucket policies
- ✅ All networking and healthchecks configured

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

**Run verification:**
```bash
./scripts/verify-setup.sh
```

**Clean up (⚠️ removes all data):**
```bash
docker compose down -v
```

## Verification Script

The `verify-setup.sh` script checks:

1. ✅ All containers are healthy
2. ✅ MinIO buckets exist
3. ✅ MinIO policies are created  
4. ✅ Service users exist with correct permissions
5. ✅ PostgreSQL databases exist
6. ✅ Spark can write/read to S3A
7. ✅ All service endpoints respond
8. ✅ Policy files exist in repository

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
│   MinIO     │   Iceberg    │    Trino    │   MLflow    │
│  (S3 API)   │    REST      │   (SQL)     │ (Tracking)  │
│             │              │             │             │
│             │  PostgreSQL  │    Spark    │   Prefect   │
│             │  (Metadata)  │  (Python)   │ (Workflows) │
└─────────────┴──────────────┴─────────────┴─────────────┘
```

## What's Different from Standard Setup?

1. **Custom Spark Image**: Built with hadoop-aws and AWS SDK for S3A support
2. **Auto-initialization**: MinIO buckets, policies, and users created on startup
3. **Service Users**: Spark, Trino, and MLflow use dedicated service accounts (not root)
4. **Least Privilege**: Each service has minimal required permissions
5. **Hadoop Config**: Custom core-site.xml to fix duration-string parsing issues

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
│       └── iceberg.properties # Trino-Iceberg connection
├── gravitino/
│   └── Dockerfile             # Iceberg REST catalog image
└── scripts/
    └── verify-setup.sh        # Comprehensive verification script
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
