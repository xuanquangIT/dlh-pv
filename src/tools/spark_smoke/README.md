# Iceberg Smoke Test

End-to-end validation of the Iceberg data lakehouse stack using PostgreSQL JDBC catalog.

## Overview

This smoke test validates:
- Trino SQL engine
- PostgreSQL JDBC catalog (Iceberg metadata)
- MinIO S3-compatible storage
- Parquet file format
- Complete CRUD operations

## Architecture

```
Trino (SQL) -> PostgreSQL JDBC Catalog -> MinIO (S3A)
     |              |                           |
     |              |                           |
   Query      Store metadata              Store data
              (iceberg_tables)          (Parquet files)
```

## Quick Start

```bash
cd /home/pvlakehouse/dlh-pv/src/tools/spark_smoke
./smoke_test.sh
```

## What It Tests

The smoke test performs 5 steps:

1. **Prepare** - Clean any existing test table
2. **Create** - Create `iceberg.lh.tmp_smoke` table with Parquet format
3. **Write** - Insert 3 test rows via Trino
4. **Read** - Query and validate data
5. **Cleanup** - Drop table (can be skipped with `CLEANUP=no`)

## Acceptance Criteria

All criteria must pass:

- [x] Command runs without credential/SSL/path-style errors
- [x] Table `lh.tmp_smoke` created and readable
- [x] Data readable from Trino
- [x] Clean-up job drops table successfully
- [x] Script and config documented

## Usage

### Run with cleanup (default)
```bash
./smoke_test.sh
```

### Run without cleanup (keep table for inspection)
```bash
CLEANUP=no ./smoke_test.sh
```

### Manual table inspection
```bash
# Connect to Trino
docker exec -it trino trino

# Query the table
SELECT * FROM iceberg.lh.tmp_smoke;

# Check metadata in PostgreSQL
docker exec postgres psql -U pvlakehouse -d iceberg_catalog \
  -c "SELECT * FROM iceberg_tables WHERE table_name='tmp_smoke';"
```

## Configuration

### Trino Catalog
File: `docker/trino/catalog/iceberg.properties`

```properties
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://postgres:5432/iceberg_catalog
iceberg.jdbc-catalog.connection-user=pvlakehouse
iceberg.jdbc-catalog.default-warehouse-dir=s3a://lakehouse/warehouse
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=trino_svc
s3.aws-secret-key=pvlakehouse_trino
```

### PostgreSQL Schema
Database: `iceberg_catalog`

Tables:
- `iceberg_tables` - Stores table metadata locations
- `iceberg_namespace_properties` - Stores namespace properties

## Expected Output

```
Iceberg Stack Smoke Test
=========================

Table: iceberg.lh.tmp_smoke
Cleanup: yes

[STEP 1/5] Preparing clean environment
PASS: Environment ready

[STEP 2/5] Creating Iceberg table
PASS: Table created successfully

[STEP 3/5] Writing test rows
PASS: 3 rows written successfully

[STEP 4/5] Reading data from Trino
PASS: Data readable from Trino

Sample output:
"1","test_row_1","100.5"
"2","test_row_2","200.7"
"3","test_row_3","300.9"

[STEP 5/5] Cleanup
PASS: Table dropped successfully

=============================
ACCEPTANCE CRITERIA
=============================

PASS: Command runs without credential/SSL/path-style errors
PASS: Table lh.tmp_smoke created and readable
PASS: Data readable from Trino
PASS: Clean-up job drops table successfully
PASS: Script and config documented

All tests passed!
```

## Troubleshooting

### Test fails at step 2 (Create table)

**Check schema exists:**
```bash
docker exec -i trino trino --execute "SHOW SCHEMAS FROM iceberg;"
```

If `lh` schema is missing:
```bash
docker exec -i trino trino --execute "CREATE SCHEMA iceberg.lh;"
```

### Test fails at step 3 (Write data)

**Check S3 credentials:**
```bash
docker exec minio mc admin user list minio/ | grep trino_svc
```

**Check MinIO policy:**
```bash
docker exec minio mc admin policy info minio/ lakehouse-rw
```

### Test fails at step 4 (Read data)

**Check metadata in PostgreSQL:**
```bash
docker exec postgres psql -U pvlakehouse -d iceberg_catalog \
  -c "SELECT * FROM iceberg_tables;"
```

**Check Trino logs:**
```bash
docker logs trino | tail -50
```

### General issues

**Verify all services are running:**
```bash
cd /home/pvlakehouse/dlh-pv/docker
docker compose ps
```

**Run full health check:**
```bash
cd /home/pvlakehouse/dlh-pv/docker
./scripts/health-check.sh
```

## Technical Details

### Data Flow

1. **CREATE TABLE**: Trino -> JDBC catalog -> PostgreSQL (metadata) + MinIO (warehouse location)
2. **INSERT**: Trino -> Write Parquet files to MinIO -> Update metadata in PostgreSQL
3. **SELECT**: Trino -> Read metadata from PostgreSQL -> Read Parquet from MinIO -> Return results
4. **DROP TABLE**: Trino -> Delete metadata from PostgreSQL -> Delete files from MinIO

### Storage Layout

MinIO path structure:
```
s3a://lakehouse/warehouse/
  lh/
    tmp_smoke-<uuid>/
      metadata/
        00001-<uuid>.metadata.json
        snap-<id>.avro
      data/
        00000-<id>.parquet
```

PostgreSQL metadata:
```sql
SELECT catalog_name, table_namespace, table_name, metadata_location 
FROM iceberg_tables;
```

## Files

- `smoke_test.sh` - Main test script
- `README.md` - This file
- `TECHNICAL.md` - Detailed technical documentation

## References

- Iceberg JDBC Catalog: https://iceberg.apache.org/docs/latest/jdbc/
- Trino Iceberg Connector: https://trino.io/docs/current/connector/iceberg.html
- Setup Guide: `/home/pvlakehouse/dlh-pv/docker/README-SETUP.md`
- Migration Details: `/home/pvlakehouse/dlh-pv/MIGRATION_TO_JDBC_CATALOG.md`
