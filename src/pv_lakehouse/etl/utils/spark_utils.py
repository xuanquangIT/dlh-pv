"""Spark helpers for PV Lakehouse ETL jobs."""

from __future__ import annotations

import logging
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from pv_lakehouse.config.settings import get_settings

LOGGER = logging.getLogger(__name__)

# Cache loaded config to avoid repeated YAML parsing
_CACHED_SPARK_CONFIG: Optional[Dict[str, str]] = None
_config_lock = threading.Lock()


def _ensure_namespace_exists(spark: SparkSession, table_name: str) -> None:
    """Ensure Iceberg namespace exists for table creation."""
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


def load_spark_config_from_yaml() -> Dict[str, str]:
    """Load Spark configuration from YAML and merge with runtime settings.

    This function reads the static configuration structure from spark_config.yaml
    and injects secrets/dynamic values from the Settings singleton.
    Uses thread-safe caching to avoid repeated YAML parsing.

    Returns:
        Dictionary of Spark configuration key-value pairs ready to pass to SparkSession.

    Raises:
        FileNotFoundError: If spark_config.yaml is missing.
        yaml.YAMLError: If YAML file is malformed.
    """
    global _CACHED_SPARK_CONFIG

    # First check without lock (fast path)
    if _CACHED_SPARK_CONFIG is not None:
        return _CACHED_SPARK_CONFIG.copy()

    # Acquire lock for loading
    with _config_lock:
        # Double-check after acquiring lock
        if _CACHED_SPARK_CONFIG is not None:
            return _CACHED_SPARK_CONFIG.copy()

        LOGGER.info("Loading Spark configuration from YAML")

        config_path = Path(__file__).parent.parent.parent / "config" / "spark_config.yaml"

        if not config_path.exists():
            raise FileNotFoundError(f"Spark config YAML not found: {config_path}")

        with open(config_path, encoding="utf-8") as f:
            yaml_config = yaml.safe_load(f)

        # Build flat Spark config dict from YAML structure
        spark_config: Dict[str, str] = {}

        # Iceberg Catalog Configuration (with runtime secret injection)
        catalog = yaml_config.get("catalog", {})
        spark_config["spark.sql.extensions"] = catalog.get("extensions", "")
        spark_config["spark.sql.catalog.lh"] = catalog.get("catalog_impl", "")
        spark_config["spark.sql.catalog.lh.type"] = catalog.get("catalog_type", "")
        spark_config["spark.sql.catalog.lh.jdbc.catalog-name"] = catalog.get("catalog_name", "lh")

        # Inject from settings (secrets not in YAML)
        settings = get_settings()
        spark_config["spark.sql.catalog.lh.uri"] = settings.jdbc_url
        spark_config["spark.sql.catalog.lh.jdbc.user"] = settings.database.postgres_user
        spark_config["spark.sql.catalog.lh.jdbc.password"] = settings.database.postgres_password
        spark_config["spark.sql.catalog.lh.warehouse"] = settings.s3_warehouse_path

        # S3/MinIO Configuration (secrets injected from settings)
        hadoop = yaml_config.get("hadoop", {})
        fs = hadoop.get("fs", {})
        s3a = fs.get("s3a", {})
        spark_config["spark.hadoop.fs.s3a.path.style.access"] = s3a.get("path_style_access", "true")
        spark_config["spark.hadoop.fs.s3a.endpoint"] = settings.s3.minio_endpoint
        spark_config["spark.hadoop.fs.s3a.access.key"] = settings.s3.spark_svc_access_key
        spark_config["spark.hadoop.fs.s3a.secret.key"] = settings.s3.spark_svc_secret_key

        # MapReduce Configuration
        mapreduce = hadoop.get("mapreduce", {}).get("fileoutputcommitter", {})
        spark_config["spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version"] = mapreduce.get(
            "algorithm_version", "2"
        )
        spark_config["spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored"] = mapreduce.get(
            "cleanup_failures_ignored", "true"
        )

    # Performance Configuration
    perf = yaml_config.get("performance", {})
    adaptive = perf.get("adaptive", {})
    spark_config["spark.sql.adaptive.enabled"] = adaptive.get("enabled", "true")
    spark_config["spark.sql.adaptive.coalescePartitions.enabled"] = adaptive.get(
        "coalesce_partitions_enabled", "true"
    )
    spark_config["spark.sql.adaptive.advisoryPartitionSizeInBytes"] = adaptive.get(
        "advisory_partition_size", "64MB"
    )

    files = perf.get("files", {})
    spark_config["spark.sql.files.maxPartitionBytes"] = files.get("max_partition_bytes", "128MB")

    memory = perf.get("memory", {})
    spark_config["spark.memory.fraction"] = settings.spark.spark_memory_fraction
    spark_config["spark.memory.storageFraction"] = settings.spark.spark_memory_storage_fraction

    # Shuffle partitions from settings
    spark_config["spark.sql.shuffle.partitions"] = str(settings.spark.spark_shuffle_partitions)

    # Iceberg Specific
    iceberg = yaml_config.get("iceberg", {})
    spark_config["spark.sql.iceberg.compression-codec"] = iceberg.get("compression_codec", "snappy")

    # Session Configuration
    session = yaml_config.get("session", {})
    spark_config["spark.sql.session.timeZone"] = session.get("timezone", "UTC")

    # Cache for subsequent calls
    _CACHED_SPARK_CONFIG = spark_config.copy()

    LOGGER.info("Loaded Spark config from YAML with %d entries", len(spark_config))
    return spark_config


def cleanup_spark_staging(prefix: str = "spark-") -> None:
    """Delete leftover Spark staging directories in the system temp path.

    Args:
        prefix: Directory name prefix to match for cleanup.
    """
    tmp_dir = Path(tempfile.gettempdir())
    
    if not tmp_dir.exists():
        LOGGER.debug("Temp directory %s does not exist, skipping cleanup", tmp_dir)
        return
    
    removed = 0
    for name in tmp_dir.iterdir():
        if not name.name.startswith(prefix):
            continue

        try:
            if name.is_dir():
                shutil.rmtree(name, ignore_errors=True)
                removed += 1
        except Exception as error:  # pragma: no cover - best effort cleanup
            LOGGER.debug("Failed to remove staging directory %s: %s", name, error)

    if removed:
        LOGGER.info("Removed %d Spark staging directories under %s", removed, tmp_dir)


def create_spark_session(app_name: str, *, extra_conf: Optional[Dict[str, Any]] = None) -> SparkSession:
    """Create a Spark session configured for Iceberg + MinIO usage.

    Configuration is loaded from:
    1. spark_config.yaml (static structure)
    2. Settings singleton (secrets and dynamic values)
    3. extra_conf parameter (runtime overrides)

    Args:
        app_name: Application name for Spark UI identification.
        extra_conf: Additional Spark configuration overrides.

    Returns:
        Configured SparkSession instance.

    Raises:
        FileNotFoundError: If spark_config.yaml is missing.
        ValidationError: If required settings are not configured.

    Example:
        >>> spark = create_spark_session("bronze-loader")
        >>> spark.sql("SELECT 1").show()
    """
    cleanup_spark_staging()
    builder = SparkSession.builder.appName(app_name)

    # Load configuration from YAML + Settings
    config_items = load_spark_config_from_yaml()

    # Maven packages for Iceberg, S3, and PostgreSQL
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
        "org.postgresql:postgresql:42.7.4",
    ]
    builder = builder.config("spark.jars.packages", ",".join(packages))

    # Apply extra configuration overrides
    if extra_conf:
        config_items.update({k: str(v) for k, v in extra_conf.items()})

    # Set all configuration items
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
    """Write a DataFrame to an Iceberg table using DataFrameWriterV2.

    Args:
        dataframe: PySpark DataFrame to write.
        table_name: Fully qualified table name (e.g., 'lh.bronze.raw_facilities').
        mode: Write mode - 'append', 'overwrite', or 'upsert'.
        partition_cols: Optional list of column names to partition by.

    Raises:
        ValueError: If mode is unsupported.
        AnalysisException: If table operation fails.
    """
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
    """Register an Iceberg table with Trino catalog.

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified table name.
    """
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


__all__ = [
    "create_spark_session",
    "write_iceberg_table",
    "register_iceberg_table_with_trino",
    "load_spark_config_from_yaml",
]
