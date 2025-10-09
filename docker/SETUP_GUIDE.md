# Data Lakehouse Platform - Setup Guide

This guide provides comprehensive instructions for setting up, deploying, and managing the Data Lakehouse platform using Docker Compose.

## 🚀 Quick Start (New Team Members)

**If you just cloned this project and want to get everything running:**

1. **Navigate to the docker directory:**
   ```bash
   cd docker
   ```

2. **Start all services:**
   ```bash
   docker compose --profile core --profile ml --profile orchestrate up -d --build
   ```
   
   This will:
   - Build the custom Spark image with S3A support
   - Start MinIO, PostgreSQL, Trino, Iceberg REST, Spark, MLflow, and Prefect
   - Auto-create databases: `iceberg`, `mlflow`, `prefect`
   - Auto-create S3 buckets: `lakehouse`, `mlflow`
   - Set up service users with least-privilege policies

3. **Verify the setup:**
   ```bash
   # Run the comprehensive verification script
   ./scripts/verify-setup.sh
   ```

4. **Access the services:**
   - MinIO Console: http://localhost:9001 (pvlakehouse/pvlakehouse)
   - Trino UI: http://localhost:8081
   - MLflow UI: http://localhost:5000
   - Prefect UI: http://localhost:4200
   - Spark Master UI: http://localhost:4040

**That's it!** 🎉 Jump to [Verification](#verification) to test your setup.

---

## Table of Contents

- [Quick Start](#-quick-start-new-team-members)
- [Prerequisites](#prerequisites)
- [Architecture Overview](#architecture-overview)
- [Detailed Setup](#detailed-setup)
- [Verification](#verification)
- [Service Access](#service-access)
- [Troubleshooting](#troubleshooting)
- [Stopping and Cleaning Up](#stopping-and-cleaning-up)

## Prerequisites

### System Requirements

- **OS**: Linux, macOS, or Windows 10/11 with WSL2
- **CPU**: Minimum 4 cores (8 cores recommended)
- **RAM**: Minimum 8GB (16GB recommended)
- **Disk**: Minimum 20GB free space

### Software Dependencies

- **Docker**: Version 20.10 or higher
- **Docker Compose**: Version 2.0 or higher (V2 CLI)

Verify installation:
```bash
docker --version
docker compose version
```

## Architecture Overview

The platform consists of the following components:

### Core Services (Profile: `core`)

| Service | Description | Port | Technology |
|---------|-------------|------|------------|
| **MinIO** | S3-compatible object storage | 9000 (API), 9001 (Console) | MinIO |
| **PostgreSQL** | Relational metastore | 5432 | PostgreSQL 15 |
| **Iceberg REST** | Table catalog service | 8181 | Apache Iceberg + Gravitino |
| **Trino** | Distributed SQL query engine | 8081 | Trino |
| **Spark Master** | Spark cluster master | 7077, 4040 | Apache Spark 4.x + S3A |
| **Spark Worker** | Spark cluster worker | - | Apache Spark 4.x + S3A |
| **pgAdmin** | Postgres GUI | 5050 | pgAdmin |
| **mc (init)** | MinIO setup (runs once) | - | MinIO Client |

### ML Services (Profile: `ml`)

| Service | Description | Port | Technology |
|---------|-------------|------|------------|
| **MLflow** | ML tracking and model registry | 5000 | MLflow |

### Orchestration Services (Profile: `orchestrate`)

| Service | Description | Port | Technology |
|---------|-------------|------|------------|
| **Prefect** | Workflow orchestration server | 4200 | Prefect 2.x |
| **Prefect Agent** | Workflow execution agent | - | Prefect 2.x |

## Detailed Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd dlh-pv/docker
```

### 2. Environment Configuration

The repository includes a `.env` file with sensible defaults for local development. 

**⚠️ For production:** Copy `.env.example` to `.env` and update credentials:

```bash
cp .env.example .env
# Edit .env and change passwords/keys
```

**Default credentials (for local development only):**
- User: `pvlakehouse`
- Password: `pvlakehouse`

### 3. Start Services

**Start all services (recommended):**
```bash
docker compose --profile core --profile ml --profile orchestrate up -d --build
```

**Or start specific profiles:**

```bash
# Core only (MinIO, Postgres, Trino, Spark, Iceberg)
docker compose --profile core up -d --build

# Core + ML
docker compose --profile core --profile ml up -d --build

# All services
docker compose --profile core --profile ml --profile orchestrate up -d --build
```

**First-time startup takes 2-5 minutes** to:
- Build custom Spark image with hadoop-aws and AWS SDK
- Initialize databases
- Create MinIO buckets and policies
- Set up service users

### 4. Verify Setup

Run the verification script:
```bash
./scripts/verify-setup.sh
```

## Verification

### Automated Verification Script

The repository includes a comprehensive verification script that tests all components.

**Run the full verification:**
```bash
cd docker
./scripts/verify-setup.sh
```

**What it checks:**
1. ✅ All containers are running and healthy
2. ✅ MinIO buckets (`lakehouse`, `mlflow`) exist
3. ✅ MinIO policies (`lakehouse-rw`, `mlflow-rw`) are created
4. ✅ Service users (`spark_svc`, `trino_svc`, `mlflow_svc`) exist
5. ✅ PostgreSQL databases (`iceberg`, `mlflow`, `prefect`) exist
6. ✅ Spark can write/read to S3A (s3a://lakehouse/tmp_check/)
7. ✅ All service endpoints are responding

**Expected output:**
```
=== Data Lakehouse Setup Verification ===

1. Container Health Status:
  ✓ minio: healthy
  ✓ postgres: healthy
  ✓ iceberg-rest: healthy
  ✓ trino: healthy
  ✓ spark-master: healthy
  ✓ mlflow: healthy
  ✓ prefect: healthy

2. MinIO Buckets:
  ✓ Bucket 'lakehouse' exists
  ✓ Bucket 'mlflow' exists

3. MinIO Policies:
  ✓ Policy 'lakehouse-rw' exists
  ✓ Policy 'mlflow-rw' exists

4. MinIO Service Users:
  ✓ User 'spark_svc' exists with policy 'lakehouse-rw'
  ✓ User 'trino_svc' exists with policy 'lakehouse-rw'
  ✓ User 'mlflow_svc' exists with policy 'mlflow-rw'

5. PostgreSQL Databases:
  ✓ Database 'iceberg' exists
  ✓ Database 'mlflow' exists
  ✓ Database 'prefect' exists

6. Spark S3A Capability:
  ✓ Spark can write to s3a://lakehouse/tmp_check/
  ✓ Spark can read from s3a://lakehouse/tmp_check/

7. Service Endpoints:
  ✓ MinIO API: http://localhost:9000
  ✓ Trino: http://localhost:8081
  ✓ MLflow: http://localhost:5000
  ✓ Prefect: http://localhost:4200
  ✓ Spark UI: http://localhost:4040

=== All Checks Passed! ✅ ===
```

### Manual Verification

**Check container status:**
```bash
docker compose ps
```

**Check specific service logs:**
```bash
docker compose logs minio
docker compose logs postgres
docker compose logs spark-master
```

**Test MinIO buckets:**
```bash
docker run --rm --network dlhpv_data-net \
  -e MC_HOST_local=http://pvlakehouse:pvlakehouse@minio:9000 \
  minio/mc:latest ls local
```

**Test Spark S3A:**
```bash
# Inside spark-master container
docker exec spark-master /opt/spark/bin/spark-submit --master local[1] \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples*.jar 10
```

**Connect to Trino:**
```bash
docker exec -it trino trino
# Then run: SHOW CATALOGS;
```

**Check Postgres databases:**
```bash
docker exec postgres psql -U pvlakehouse -c "\l"
```

## Service Access

### Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | User: `pvlakehouse`<br>Password: `pvlakehouse` |
| **MLflow UI** | http://localhost:5000 | No authentication |
| **Prefect UI** | http://localhost:4200 | No authentication |
| **Trino UI** | http://localhost:8081 | No authentication |

### Database Connections

**PostgreSQL:**
```
Host: localhost
Port: 5432
User: pvlakehouse
Password: pvlakehouse
Databases: iceberg, mlflow, prefect, postgres
```

Connect via psql:
```bash
docker exec -it postgres psql -U pvlakehouse -d iceberg
```

**MinIO S3 API:**
```
Endpoint: http://localhost:9000
Access Key: pvlakehouse
Secret Key: pvlakehouse
Region: us-east-1
```

### Trino Query Interface

Connect to Trino CLI:
```bash
docker exec -it trino trino
```

Example queries:
```sql
-- Show catalogs
SHOW CATALOGS;

-- Show schemas in iceberg catalog
SHOW SCHEMAS IN iceberg;

-- Create a namespace
CREATE SCHEMA iceberg.bronze;

-- List tables
SHOW TABLES IN iceberg.bronze;
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Services Not Starting

**Problem**: Containers exit immediately or fail to start

**Solution**:
- Check logs: `docker compose logs <service-name>`
- Verify environment variables in `.env` file
- Ensure sufficient system resources
- Check port conflicts: `netstat -an | grep <port>`

#### 2. Healthcheck Failures

**Problem**: Containers show as "unhealthy"

**Solution**:
- Wait for services to fully initialize (can take 1-2 minutes)
- Check service logs for errors
- Verify dependencies are healthy first (e.g., postgres before prefect)
- Increase healthcheck retries or interval if needed

#### 3. Database Connection Errors

**Problem**: Services cannot connect to PostgreSQL

**Solution**:
- Verify postgres is healthy: `docker compose ps postgres`
- Check databases exist: `docker exec postgres psql -U pvlakehouse -c "\l"`
- Verify credentials in `.env` file
- If databases missing, restart postgres or recreate volume:
  ```bash
  docker compose down
  docker volume rm docker_postgres-data
  docker compose up -d postgres
  ```

#### 4. MinIO Access Issues

**Problem**: Cannot access MinIO or buckets

**Solution**:
- Verify MinIO is healthy: `docker compose ps minio`
- Check buckets exist: Run the mc container manually
- Verify credentials match in `.env`
- Check MinIO logs: `docker compose logs minio`

#### 5. Prefect Agent Restart Loop

**Problem**: prefect-agent keeps restarting

**Solution**:
- This is expected if work pool doesn't exist
- Create work pool manually:
  ```bash
  docker exec prefect prefect work-pool create default-pool
  ```

#### 6. Trino Cannot Query Iceberg Tables

**Problem**: Trino shows connection errors to Iceberg REST

**Solution**:
- Verify iceberg-rest is healthy
- Check catalog configuration in `trino/catalog/iceberg.properties`
- Verify network connectivity:
  ```bash
  docker exec trino curl http://iceberg-rest:8181/v1/config
  ```

### Viewing Logs

**All services:**
```bash
docker compose logs
```

**Specific service:**
```bash
docker compose logs <service-name>
```

**Follow logs:**
```bash
docker compose logs -f <service-name>
```

**Last N lines:**
```bash
docker compose logs --tail 50 <service-name>
```

### Resource Monitoring

**Container resource usage:**
```bash
docker stats
```

**Container processes:**
```bash
docker compose top
```

## Stopping and Cleaning Up

### Stop Services

**Stop all services:**
```bash
docker compose down
```

**Stop specific profile:**
```bash
docker compose --profile core down
```

**Keep services running in background:**
```bash
# Stop following logs but leave containers running
# Press Ctrl+C when viewing logs
```

### Clean Up Resources

**Remove containers and networks (preserve volumes):**
```bash
docker compose down
```

**Remove everything including volumes:**
```bash
docker compose down -v
```

**Remove specific volume:**
```bash
docker volume rm docker_postgres-data
docker volume rm docker_minio-data
```

**Clean up unused Docker resources:**
```bash
docker system prune -a
```

**View volume usage:**
```bash
docker volume ls
docker system df
```

### Restart Services

**Restart all services:**
```bash
docker compose restart
```

**Restart specific service:**
```bash
docker compose restart <service-name>
```

**Recreate containers (apply config changes):**
```bash
docker compose up -d --force-recreate
```

## Best Practices

### Development Workflow

1. **Start with core services first:**
   ```bash
   docker compose --profile core up -d
   ```

2. **Add ML services when needed:**
   ```bash
   docker compose --profile ml up -d
   ```

3. **Add orchestration for workflows:**
   ```bash
   docker compose --profile orchestrate up -d
   ```

### Production Considerations

1. **Change default credentials** in `.env` file
2. **Use secrets management** instead of plaintext passwords
3. **Configure resource limits** in docker-compose.yml
4. **Set up backup** for postgres-data and minio-data volumes
5. **Use external PostgreSQL** and S3 for production
6. **Enable TLS/SSL** for all services
7. **Implement monitoring** with Prometheus and Grafana
8. **Configure log aggregation** (e.g., ELK stack)

### Backup and Recovery

**Backup PostgreSQL:**
```bash
docker exec postgres pg_dumpall -U pvlakehouse > backup.sql
```

**Restore PostgreSQL:**
```bash
cat backup.sql | docker exec -i postgres psql -U pvlakehouse
```

**Backup MinIO data:**
```bash
docker run --rm --volumes-from minio -v $(pwd):/backup ubuntu tar czf /backup/minio-backup.tar.gz /data
```

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Trino Documentation](https://trino.io/docs/)
- [MLflow Documentation](https://mlflow.org/docs/)
- [Prefect Documentation](https://docs.prefect.io/)
- [MinIO Documentation](https://min.io/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## Support

For issues and questions:
- Check the [Troubleshooting](#troubleshooting) section
- Review service logs
- Consult official documentation for each service
- Open an issue in the repository

---

**Last Updated**: October 4, 2025
**Version**: 1.0.0
