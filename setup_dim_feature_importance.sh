#!/bin/bash

set -e

docker cp /home/pvlakehouse/work/dlh-pv/src/pv_lakehouse/etl/gold/dim_feature_importance.py \
  spark-master:/opt/spark/work-dir/dim_feature_importance.py

docker cp /home/pvlakehouse/work/dlh-pv/src/pv_lakehouse/etl/utils/spark_utils.py \
  spark-master:/opt/spark/work-dir/spark_utils.py

docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=rest \
  --conf spark.sql.catalog.iceberg.uri=http://gravitino:8090/iceberg/ \
  --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse/ \
  --conf spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.iceberg.s3.access-key-id=minio \
  --conf spark.sql.catalog.iceberg.s3.secret-access-key=minio123 \
  --conf spark.sql.catalog.iceberg.s3.path-style-access=true \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.20.18 \
  /opt/spark/work-dir/dim_feature_importance.py

if [ $? -eq 0 ]; then
    docker compose -f docker/docker-compose.yml exec trino trino \
      --server http://trino:8080 --catalog iceberg --schema gold \
      --execute "DESCRIBE lh.gold.dim_feature_importance"
fi
