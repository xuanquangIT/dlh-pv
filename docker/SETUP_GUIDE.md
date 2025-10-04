# Data Lakehouse Platform - Setup Guide

This guide provides comprehensive instructions for setting up, deploying, and managing the Data Lakehouse platform using Docker Compose.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Architecture Overview](#architecture-overview)
- [Installation](#installation)
- [Configuration](#configuration)
- [Building and Starting Services](#building-and-starting-services)
- [Health Checks](#health-checks)
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
| **Iceberg REST** | Table catalog service | 8181 | Apache Iceberg |
| **Trino** | Distributed SQL query engine | 8081 | Trino |

### ML Services (Profile: `ml`)

| Service | Description | Port | Technology |
|---------|-------------|------|------------|
| **MLflow** | ML tracking and model registry | 5000 | MLflow |

### Orchestration Services (Profile: `orchestrate`)

| Service | Description | Port | Technology |
|---------|-------------|------|------------|
| **Prefect** | Workflow orchestration server | 4200 | Prefect 2.x |
| **Prefect Agent** | Workflow execution agent | - | Prefect 2.x |

### Spark Services (Profile: `spark`)

| Service | Description | Port | Technology |
|---------|-------------|------|------------|
| **Spark Master** | Spark cluster master | 7077, 8080 | Apache Spark |
| **Spark Worker** | Spark cluster worker | - | Apache Spark |

## Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd dlh-pv/docker
```

### 2. Environment Configuration

Create or verify the `.env` file in the `docker` directory:

```bash
# User credentials (used across all services)
PV_USER=pvlakehouse
PV_PASSWORD=pvlakehouse

# PostgreSQL configuration
POSTGRES_USER=${PV_USER}
POSTGRES_PASSWORD=${PV_PASSWORD}
POSTGRES_DB=postgres

# Database names
ICEBERG_DB=iceberg
MLFLOW_DB=mlflow
PREFECT_DB=prefect

# MinIO configuration
MINIO_ROOT_USER=${PV_USER}
MINIO_ROOT_PASSWORD=${PV_PASSWORD}
MINIO_ENDPOINT=http://minio:9000
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001

# S3 Buckets
S3_WAREHOUSE_BUCKET=lakehouse
S3_MLFLOW_BUCKET=mlflow
S3_REGION=us-east-1

# Service users
ICEBERG_USER=${PV_USER}
ICEBERG_PASSWORD=${PV_PASSWORD}
MLFLOW_USER=${PV_USER}
MLFLOW_PASSWORD=${PV_PASSWORD}

# Service ports
TRINO_PORT=8081
MLFLOW_PORT=5000
PREFECT_PORT=4200
SPARK_UI_PORT=8082

# Prefect configuration
PREFECT_API_URL=http://prefect:4200/api
PREFECT_WORK_POOL=default-pool

# Docker images
MINIO_IMAGE=minio/minio:latest
MC_IMAGE=minio/mc:latest
POSTGRES_IMAGE=postgres:15
ICEBERG_REST_IMAGE=tabulario/iceberg-rest:latest
TRINO_IMAGE=trinodb/trino:latest
MLFLOW_BASE_IMAGE=python:3.11-slim
PREFECT_IMAGE=prefecthq/prefect:2-latest
SPARK_IMAGE=bitnami/spark:3.5
```

### 3. Initialize Database Setup Script

Ensure the PostgreSQL initialization script exists:

**File**: `docker/postgres-init.sh`

```bash
#!/bin/bash
set -e

echo "Initializing application databases..."

# Create databases idempotently
psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d postgres <<-EOSQL
    SELECT 'CREATE DATABASE iceberg OWNER ${POSTGRES_USER}' 
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'iceberg')\gexec
    
    SELECT 'CREATE DATABASE mlflow OWNER ${POSTGRES_USER}' 
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec
    
    SELECT 'CREATE DATABASE prefect OWNER ${POSTGRES_USER}' 
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'prefect')\gexec
EOSQL

echo "Application databases created successfully"
```

Make it executable (Linux/macOS):
```bash
chmod +x postgres-init.sh
```

## Configuration

### Trino Catalog Configuration

Create the Iceberg catalog configuration for Trino:

**File**: `docker/trino/catalog/iceberg.properties`

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
```

## Building and Starting Services

### Start All Services

To start all services across all profiles:

```bash
docker compose up -d
```

### Start Specific Profiles

#### Core Services Only
```bash
docker compose --profile core up -d
```

#### Core + ML Services
```bash
docker compose --profile core --profile ml up -d
```

#### Core + ML + Orchestration
```bash
docker compose --profile core --profile ml --profile orchestrate up -d
```

#### All Services Including Spark
```bash
docker compose --profile core --profile ml --profile orchestrate --profile spark up -d
```

### Verify Services Are Starting

```bash
docker compose ps
```

Expected output:
```
NAME            IMAGE                          STATUS
iceberg-rest    tabulario/iceberg-rest:latest  Up (healthy)
mlflow          python:3.11-slim               Up (healthy)
minio           minio/minio:latest             Up (healthy)
postgres        postgres:15                    Up (healthy)
prefect         prefecthq/prefect:2-latest     Up (healthy)
prefect-agent   prefecthq/prefect:2-latest     Up
trino           trinodb/trino:latest           Up (healthy)
```

## Health Checks

### Automated Health Check Script

Create a PowerShell script to check all services:

**File**: `docker/scripts/stack-health.ps1`

```powershell
Write-Host "`n=== Data Lakehouse Health Check ===" -ForegroundColor Cyan

# Check container status
Write-Host "`n1. Container Status:" -ForegroundColor Yellow
docker ps --format "table {{.Names}}\t{{.Status}}" | Where-Object { $_ -match '(minio|postgres|iceberg|mlflow|prefect|trino|NAMES)' }

# Test endpoints
Write-Host "`n2. Service Endpoints:" -ForegroundColor Yellow
$endpoints = @{
    'MinIO API'    = 'http://localhost:9000/minio/health/ready'
    'Iceberg REST' = 'http://localhost:8181/v1/config'
    'MLflow'       = 'http://localhost:5000/'
    'Prefect API'  = 'http://localhost:4200/api/health'
    'Trino'        = 'http://localhost:8081/v1/info'
}

$endpoints.GetEnumerator() | ForEach-Object {
    try {
        $response = Invoke-WebRequest -Uri $_.Value -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
        Write-Host "  ✓ $($_.Key): OK (HTTP $($response.StatusCode))" -ForegroundColor Green
    } catch {
        Write-Host "  ✗ $($_.Key): FAILED" -ForegroundColor Red
    }
}

# Check databases
Write-Host "`n3. PostgreSQL Databases:" -ForegroundColor Yellow
docker exec postgres psql -U pvlakehouse -d postgres -c "\l" | Select-String -Pattern '(iceberg|mlflow|prefect)'

# Check S3 buckets
Write-Host "`n4. MinIO Buckets:" -ForegroundColor Yellow
docker run --rm --network docker_data-net --entrypoint sh minio/mc:latest -c "mc alias set local http://minio:9000 pvlakehouse pvlakehouse && mc ls local"

Write-Host "`n=== Health Check Complete ===" -ForegroundColor Cyan
```

For Linux/macOS, create `docker/scripts/stack-health.sh`:

```bash
#!/bin/bash

echo "=== Data Lakehouse Health Check ==="

# Check container status
echo -e "\n1. Container Status:"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E '(minio|postgres|iceberg|mlflow|prefect|trino|NAMES)'

# Test endpoints
echo -e "\n2. Service Endpoints:"
endpoints=(
    "MinIO:http://localhost:9000/minio/health/ready"
    "Iceberg:http://localhost:8181/v1/config"
    "MLflow:http://localhost:5000/"
    "Prefect:http://localhost:4200/api/health"
    "Trino:http://localhost:8081/v1/info"
)

for endpoint in "${endpoints[@]}"; do
    IFS=':' read -r name url <<< "$endpoint"
    if curl -sf "$url" > /dev/null 2>&1; then
        echo "  ✓ $name: OK"
    else
        echo "  ✗ $name: FAILED"
    fi
done

# Check databases
echo -e "\n3. PostgreSQL Databases:"
docker exec postgres psql -U pvlakehouse -d postgres -c "\l" | grep -E '(iceberg|mlflow|prefect)'

# Check S3 buckets
echo -e "\n4. MinIO Buckets:"
docker run --rm --network docker_data-net --entrypoint sh minio/mc:latest -c \
    "mc alias set local http://minio:9000 pvlakehouse pvlakehouse && mc ls local"

echo -e "\n=== Health Check Complete ==="
```

### Run Health Check

**Windows (PowerShell):**
```powershell
.\scripts\stack-health.ps1
```

**Linux/macOS:**
```bash
chmod +x scripts/stack-health.sh
./scripts/stack-health.sh
```

### Manual Health Verification

#### Check Container Status
```bash
docker compose ps
```

#### Check Service Logs
```bash
# View logs for a specific service
docker compose logs <service-name>

# Follow logs in real-time
docker compose logs -f <service-name>

# View last 100 lines
docker compose logs --tail 100 <service-name>
```

#### Test Individual Endpoints

**MinIO:**
```bash
curl http://localhost:9000/minio/health/ready
```

**Iceberg REST:**
```bash
curl http://localhost:8181/v1/config
```

**MLflow:**
```bash
curl http://localhost:5000/
```

**Prefect:**
```bash
curl http://localhost:4200/api/health
```

**Trino:**
```bash
curl http://localhost:8081/v1/info
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
