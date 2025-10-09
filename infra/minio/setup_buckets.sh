#!/bin/bash
set -e

# MinIO Bucket Setup Script
# This script creates buckets, policies, and service users for MinIO

echo "Setting up MinIO alias..."
mc alias set local ${MINIO_ENDPOINT} ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

echo "Creating buckets..."
mc mb -p local/${S3_WAREHOUSE_BUCKET} || true
mc mb -p local/${S3_MLFLOW_BUCKET} || true

echo "Setting bucket access to private..."
mc anonymous set private local/${S3_WAREHOUSE_BUCKET} || true
mc anonymous set private local/${S3_MLFLOW_BUCKET} || true

echo "Enabling versioning..."
mc version enable local/${S3_WAREHOUSE_BUCKET} || true
mc version enable local/${S3_MLFLOW_BUCKET} || true

echo "Creating policies..."
mc admin policy create local lakehouse-rw /policies/lakehouse-rw.json || true
mc admin policy create local mlflow-rw /policies/mlflow-rw.json || true

echo "Creating service users..."
mc admin user add local ${SPARK_SVC_ACCESS_KEY} ${SPARK_SVC_SECRET_KEY} || true
mc admin user add local ${TRINO_SVC_ACCESS_KEY} ${TRINO_SVC_SECRET_KEY} || true
mc admin user add local ${MLFLOW_SVC_ACCESS_KEY} ${MLFLOW_SVC_SECRET_KEY} || true

echo "Attaching policies to users..."
mc admin policy attach local lakehouse-rw --user ${SPARK_SVC_ACCESS_KEY}
mc admin policy attach local lakehouse-rw --user ${TRINO_SVC_ACCESS_KEY}
mc admin policy attach local mlflow-rw --user ${MLFLOW_SVC_ACCESS_KEY}

echo "MinIO setup complete!"
