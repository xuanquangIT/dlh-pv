"""Spark helpers for PV Lakehouse ETL jobs."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from typing import Any, Dict, Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException


def _ensure_namespace_exists(spark: SparkSession, table_name: str) -> None:
    parts = table_name.split(".")
    if len(parts) < 3:
        return

    catalog, *namespace_parts, _ = parts
    if not namespace_parts:
        return

    namespace = ".".join(namespace_parts)
    fq_namespace = f"{catalog}.{namespace}"

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {fq_namespace}")
    except AnalysisException as error:  # pragma: no cover - defensive guard
        LOGGER.warning("Failed creating namespace %s: %s", fq_namespace, error)

LOGGER = logging.getLogger(__name__)

DEFAULT_SPARK_CONFIG: Dict[str, str] = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lh": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lh.type": "jdbc",
    "spark.sql.catalog.lh.uri": "jdbc:postgresql://postgres:5432/iceberg_catalog",
    "spark.sql.catalog.lh.jdbc.user": "pvlakehouse",
    "spark.sql.catalog.lh.jdbc.password": "pvlakehouse",
    "spark.sql.catalog.lh.jdbc.catalog-name": "lh",
    "spark.sql.catalog.lh.warehouse": "s3a://lakehouse/iceberg/warehouse",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "spark_svc",
    "spark.hadoop.fs.s3a.secret.key": "pvlakehouse_spark",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored": "true",
    "spark.sql.iceberg.compression-codec": "snappy",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.session.timeZone": "UTC",
}


def cleanup_spark_staging(prefix: str = "spark-") -> None:
    """Delete leftover Spark staging directories in the system temp path."""

    tmp_dir = tempfile.gettempdir()
    removed = 0
    for name in os.listdir(tmp_dir):
        if not name.startswith(prefix):
            continue

        path = os.path.join(tmp_dir, name)
        try:
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
                removed += 1
        except Exception as error:  # pragma: no cover - best effort cleanup
            LOGGER.debug("Failed to remove staging directory %s: %s", path, error)

    if removed:
        LOGGER.info("Removed %d Spark staging directories under %s", removed, tmp_dir)


def create_spark_session(app_name: str, *, extra_conf: Optional[Dict[str, Any]] = None) -> SparkSession:
    """Create a Spark session configured for Iceberg + MinIO usage."""

    cleanup_spark_staging()
    builder = SparkSession.builder.appName(app_name)
    config_items = dict(DEFAULT_SPARK_CONFIG)
    builder = builder.config(
        "spark.jars.packages",
        ",".join(
            [
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
                "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
                "org.postgresql:postgresql:42.7.4",
            ]
        ),
    )
    if extra_conf:
        config_items.update({k: str(v) for k, v in extra_conf.items()})

    for key, value in config_items.items():
        builder = builder.config(key, value)

    LOGGER.info("Creating Spark session '%s' with %d config entries", app_name, len(config_items))
    return builder.getOrCreate()


def write_iceberg_table(
    dataframe: DataFrame,
    table_name: str,
    *,
    mode: str = "append",
    partition_cols: Optional[Iterable[str]] = None,
) -> None:
    """Write a DataFrame to an Iceberg table using DataFrameWriterV2."""

    row_count = dataframe.count()
    _ensure_namespace_exists(dataframe.sparkSession, table_name)
    writer = dataframe.writeTo(table_name)

    if partition_cols:
        for column in partition_cols:
            writer = writer.partitionedBy(column)

    LOGGER.info("Writing %d rows to Iceberg table %s [%s]", row_count, table_name, mode)

    try:
        if mode == "overwrite":
            writer.overwritePartitions()
        elif mode == "append":
            writer.append()
        elif mode == "upsert":  # pragma: no cover - placeholder until MERGE helpers added
            raise NotImplementedError("Upsert mode is not implemented. Use append or overwrite.")
        else:
            raise ValueError(f"Unsupported Iceberg write mode: {mode}")
    except AnalysisException as error:
        error_msg = getattr(error, "desc", str(error))
        if "TABLE_OR_VIEW_NOT_FOUND" not in error_msg and "Table does not exist" not in error_msg:
            raise
        LOGGER.info("Iceberg table %s missing; creating it via createOrReplace", table_name)
        create_writer = dataframe.writeTo(table_name)
        if partition_cols:
            for column in partition_cols:
                create_writer = create_writer.partitionedBy(column)
        create_writer.createOrReplace()


def register_iceberg_table_with_trino(spark: SparkSession, table_name: str) -> None:
    parts = table_name.split(".")
    if len(parts) < 3:
        LOGGER.warning("Cannot register Iceberg table %s: expected catalog.namespace.table", table_name)
        return

    catalog = parts[0]
    namespace = ".".join(parts[1:-1])
    table = parts[-1]
    identifier = f"{namespace}.{table}"

    try:
        snapshots_df = spark.sql(
            f"SELECT file AS metadata_file FROM {table_name}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1"
        )
    except AnalysisException as error:
        LOGGER.warning("Unable to query snapshots for %s: %s", table_name, error)
        return

    rows = snapshots_df.collect()
    if not rows:
        LOGGER.warning("No snapshots found for %s; skipping Trino registration", table_name)
        return

    metadata_location = rows[0][0]
    if not metadata_location:
        LOGGER.warning("Snapshot metadata missing for %s; skipping Trino registration", table_name)
        return

    try:
        spark.sql(
            f"CALL {catalog}.system.register_table('{identifier}', '{metadata_location}')"
        )
        LOGGER.info(
            "Registered Iceberg table %s with Trino (metadata=%s)",
            table_name,
            metadata_location,
        )
        return
    except Exception as error:  # pylint: disable=broad-except
        message = str(error)
        if "Already exists" not in message and "AlreadyExists" not in message:
            LOGGER.warning("Failed registering %s with Trino: %s", table_name, message)
            return

    try:
        spark.sql(
            f"CALL {catalog}.system.set_table_metadata('{identifier}', '{metadata_location}')"
        )
        LOGGER.info(
            "Updated Trino metadata for %s (metadata=%s)",
            table_name,
            metadata_location,
        )
    except Exception as error:  # pylint: disable=broad-except
        LOGGER.warning(
            "Failed updating Trino metadata for %s: %s",
            table_name,
            error,
        )


__all__ = ["create_spark_session", "write_iceberg_table", "register_iceberg_table_with_trino"]
