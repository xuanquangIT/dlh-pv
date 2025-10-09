# MinIO Setup - Acceptance Criteria Status

## ✅ Completed Requirements

### 1. Bucket Creation ✅
- ✅ `S3_WAREHOUSE_BUCKET=lakehouse` in `.env`
- ✅ `S3_MLFLOW_BUCKET=mlflow` in `.env`
- ✅ Buckets created via `mc mb` command in `mc` service
- ✅ Buckets set to private access
- ✅ Versioning enabled on both buckets

**Verification:**
```bash
docker run --rm --network dlhpv_data-net \
  --entrypoint /bin/sh \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ROOT_USER=pvlakehouse \
  -e MINIO_ROOT_PASSWORD=pvlakehouse \
  minio/mc:latest -c '
  mc alias set local $MINIO_ENDPOINT $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD;
  mc ls local;
  '
```

**Result:**
```
[2025-10-04 11:07:17 UTC]     0B lakehouse/
[2025-10-04 11:07:17 UTC]     0B mlflow/
```

### 2. Policy Files in Repository ✅
- ✅ `infra/minio/policies/lakehouse-rw.json` created
- ✅ `infra/minio/policies/mlflow-rw.json` created
- ✅ `infra/minio/setup_buckets.sh` created (optional script)

**Verification:**
```bash
test -f infra/minio/policies/lakehouse-rw.json && echo OK || echo MISSING
test -f infra/minio/policies/mlflow-rw.json && echo OK || echo MISSING
```

### 3. Policy Creation & Assignment ✅
- ✅ Policies created via `mc admin policy create`
- ✅ Service users created: `spark_svc`, `trino_svc`, `mlflow_svc`
- ✅ Policies attached to appropriate users:
  - `spark_svc` → `lakehouse-rw`
  - `trino_svc` → `lakehouse-rw`
  - `mlflow_svc` → `mlflow-rw`

**Verification:**
```bash
docker run --rm --network dlhpv_data-net \
  --entrypoint /bin/sh \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ROOT_USER=pvlakehouse \
  -e MINIO_ROOT_PASSWORD=pvlakehouse \
  -e SPARK_SVC_ACCESS_KEY=spark_svc \
  -e TRINO_SVC_ACCESS_KEY=trino_svc \
  -e MLFLOW_SVC_ACCESS_KEY=mlflow_svc \
  minio/mc:latest -c '
  mc alias set local $MINIO_ENDPOINT $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD;
  mc admin policy info local lakehouse-rw;
  mc admin policy info local mlflow-rw;
  mc admin user info local $SPARK_SVC_ACCESS_KEY;
  mc admin user info local $TRINO_SVC_ACCESS_KEY;
  mc admin user info local $MLFLOW_SVC_ACCESS_KEY;
  '
```

**Result:**
```
AccessKey: spark_svc
Status: enabled
PolicyName: lakehouse-rw

AccessKey: trino_svc
Status: enabled
PolicyName: lakehouse-rw

AccessKey: mlflow_svc
Status: enabled
PolicyName: mlflow-rw
```

### 4. Service Users in Runtime ✅
- ✅ Spark master uses `SPARK_SVC_ACCESS_KEY/SECRET_KEY`
- ✅ Spark worker uses `SPARK_SVC_ACCESS_KEY/SECRET_KEY`
- ✅ MLflow uses `MLFLOW_SVC_ACCESS_KEY/SECRET_KEY`
- ✅ Trino catalog configured with `TRINO_SVC_ACCESS_KEY/SECRET_KEY`

## ⚠️ Known Issue

### Spark S3A Test - Requires Additional Configuration

**Issue:** Apache Spark image (`apache/spark:latest`) does not include Hadoop AWS libraries needed for S3A filesystem support.

**Error:** `ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found`

**Solution Options:**

1. **Use Bitnami Spark Image (Recommended):**
   ```yaml
   SPARK_IMAGE=bitnami/spark:latest
   ```
   Bitnami's Spark includes Hadoop AWS libraries by default.

2. **Build Custom Spark Image:**
   Create `docker/spark/Dockerfile`:
   ```dockerfile
   FROM apache/spark:latest
   
   USER root
   RUN apt-get update && \
       apt-get install -y wget && \
       wget -P /opt/spark/jars/ \
         https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
   
   USER spark
   ```

3. **Add JARs at Runtime:**
   ```bash
   docker exec spark-master bash -c '
   wget -P /opt/spark/jars/ \
     https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
     https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
   '
   ```

## Definition of Done

- [x] `.env` has `S3_WAREHOUSE_BUCKET=lakehouse`
- [x] `.env` has `S3_MLFLOW_BUCKET=mlflow`
- [x] Repo has `infra/minio/policies/lakehouse-rw.json`
- [x] Repo has `infra/minio/policies/mlflow-rw.json`
- [x] Service `mc` creates buckets successfully
- [x] Service `mc` sets buckets to private
- [x] Service `mc` enables versioning
- [x] Service `mc` creates policies from JSON files
- [x] Service `mc` creates service users
- [x] Service `mc` attaches policies to users
- [x] Spark uses service user credentials (not root)
- [x] Trino uses service user credentials (not root)
- [x] MLflow uses service user credentials (not root)
- [ ] Spark can write/read s3a://lakehouse/tmp_check/ (requires S3A library fix)

## Quick Start

1. **Start the stack:**
   ```bash
   cd docker
   docker compose --profile core up -d
   ```

2. **Verify setup:**
   ```bash
   # Check mc logs
   docker compose logs mc
   
   # Should see:
   # ✅ MinIO setup complete!
   ```

3. **Test access (once Spark S3A is configured):**
   ```bash
   # Will work after adding S3A libraries
   docker exec -i spark-master bash -c '
   /opt/spark/bin/spark-shell \
     --master spark://spark-master:7077 \
     --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
     --conf spark.hadoop.fs.s3a.path.style.access=true \
     --conf spark.hadoop.fs.s3a.access.key=spark_svc \
     --conf spark.hadoop.fs.s3a.secret.key=spark_secret_change_me <<EOF
   import java.time._, java.time.format.DateTimeFormatter
   val ts = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(Instant.now().atZone(ZoneOffset.UTC))
   spark.createDataFrame(Seq(("ok", ts))).toDF("status","ts")
     .write.mode("overwrite").parquet(s"s3a://lakehouse/tmp_check/ts=\$ts")
   val df = spark.read.parquet("s3a://lakehouse/tmp_check/*")
   df.show(5,false)
   System.exit(0)
   EOF
   '
   ```

## Security Notes

⚠️ **IMPORTANT:** The current `.env` file contains example secrets. In production:

1. Generate strong passwords:
   ```bash
   openssl rand -base64 32
   ```

2. Update `.env` with real secrets:
   ```properties
   SPARK_SVC_SECRET_KEY=<strong-random-password>
   TRINO_SVC_SECRET_KEY=<strong-random-password>
   MLFLOW_SVC_SECRET_KEY=<strong-random-password>
   ```

3. Add `.env` to `.gitignore` (already done)

4. Use `.env.example` for documentation:
   ```bash
   cp .env .env.example
   # Edit .env.example to remove real secrets
   git add .env.example
   ```
