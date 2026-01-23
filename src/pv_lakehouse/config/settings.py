"""Centralized configuration management using Pydantic Settings.

This module provides type-safe, validated configuration loading from environment
variables and .env files. All secrets and configuration should be managed here
instead of hardcoding in application code.

Example:
    >>> from pv_lakehouse.config import settings
    >>> print(settings.database.postgres_user)
    >>> print(settings.jdbc_url)
"""

from __future__ import annotations

import logging
import threading
from typing import Optional

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LOGGER = logging.getLogger(__name__)


class DatabaseSettings(BaseSettings):
    """PostgreSQL database configuration for Iceberg catalog and MLflow."""

    postgres_user: str = Field(
        default="pvlakehouse",
        description="PostgreSQL superuser username",
    )
    postgres_password: str = Field(
        ...,
        description="PostgreSQL superuser password (REQUIRED)",
    )
    postgres_host: str = Field(
        default="postgres",
        description="PostgreSQL hostname",
    )
    postgres_port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        description="PostgreSQL port number",
    )
    postgres_db: str = Field(
        default="postgres",
        description="Default PostgreSQL database",
    )
    iceberg_catalog_db: str = Field(
        default="iceberg_catalog",
        description="Iceberg catalog database name",
    )


class S3Settings(BaseSettings):
    """MinIO/S3 storage configuration."""

    minio_endpoint: str = Field(
        default="http://minio:9000",
        description="MinIO endpoint URL",
    )
    spark_svc_access_key: str = Field(
        default="spark_svc",
        description="Spark service account access key for MinIO",
    )
    spark_svc_secret_key: str = Field(
        ...,
        description="Spark service account secret key for MinIO (REQUIRED)",
    )
    trino_svc_access_key: str = Field(
        default="trino_svc",
        description="Trino service account access key for MinIO",
    )
    trino_svc_secret_key: str = Field(
        default="pvlakehouse_trino",
        description="Trino service account secret key for MinIO",
    )
    mlflow_svc_access_key: str = Field(
        default="mlflow_svc",
        description="MLflow service account access key for MinIO",
    )
    mlflow_svc_secret_key: str = Field(
        default="pvlakehouse_mlflow",
        description="MLflow service account secret key for MinIO",
    )
    s3_region: str = Field(
        default="us-east-1",
        description="S3 region",
    )
    s3_warehouse_bucket: str = Field(
        default="lakehouse",
        description="Lakehouse data bucket name",
    )
    s3_mlflow_bucket: str = Field(
        default="mlflow",
        description="MLflow artifacts bucket name",
    )

    @field_validator("minio_endpoint")
    @classmethod
    def validate_endpoint_url(cls, v: str) -> str:
        """Ensure endpoint starts with http:// or https://."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("MinIO endpoint must start with http:// or https://")
        return v.rstrip("/")


class APISettings(BaseSettings):
    """External API configuration."""

    openelectricity_api_key: str = Field(
        ...,
        description="OpenElectricity API key (REQUIRED)",
    )
    openelectricity_api_url: str = Field(
        default="https://api.openelectricity.org.au/v4",
        description="OpenElectricity API base URL",
    )


class MLflowSettings(BaseSettings):
    """MLflow tracking server configuration."""

    mlflow_tracking_uri: str = Field(
        default="http://mlflow:5000",
        description="MLflow tracking server URI",
    )
    mlflow_dfs_tmp: Optional[str] = Field(
        default=None,
        description="Temporary DFS directory for MLflow",
    )


class PrefectSettings(BaseSettings):
    """Prefect orchestration configuration."""

    prefect_api_url: str = Field(
        default="http://prefect:4200/api",
        description="Prefect API server URL",
    )
    prefect_work_pool: str = Field(
        default="default-pool",
        description="Prefect work pool name",
    )
    prefect_work_queue: str = Field(
        default="default",
        description="Prefect work queue name",
    )


class SparkSettings(BaseSettings):
    """Apache Spark configuration."""

    spark_worker_cores: int = Field(
        default=4,
        ge=1,
        description="Number of cores per Spark worker",
    )
    spark_worker_memory: str = Field(
        default="3G",
        description="Memory allocation per Spark worker (e.g., '3G', '4096M')",
    )
    spark_shuffle_partitions: int = Field(
        default=200,
        ge=1,
        description="Number of partitions for shuffle operations",
    )
    spark_memory_fraction: str = Field(
        default="0.7",
        description="Fraction of heap space used for execution and storage",
    )
    spark_memory_storage_fraction: str = Field(
        default="0.4",
        description="Fraction of spark.memory.fraction used for caching",
    )


class Settings(BaseSettings):
    """Root settings container for PV Lakehouse configuration.

    All configuration is loaded from environment variables or .env file.
    Required fields will raise ValidationError if not provided.

    Example .env file:
        POSTGRES_PASSWORD=your_password
        SPARK_SVC_SECRET_KEY=your_secret
        OPENELECTRICITY_API_KEY=your_api_key
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    api: APISettings = Field(default_factory=APISettings)
    mlflow: MLflowSettings = Field(default_factory=MLflowSettings)
    prefect: PrefectSettings = Field(default_factory=PrefectSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)

    @property
    def jdbc_url(self) -> str:
        """Construct JDBC URL for Iceberg catalog from database settings."""
        return (
            f"jdbc:postgresql://{self.database.postgres_host}:"
            f"{self.database.postgres_port}/{self.database.iceberg_catalog_db}"
        )

    @property
    def s3_warehouse_path(self) -> str:
        """Construct S3 warehouse path for Iceberg."""
        return f"s3a://{self.s3.s3_warehouse_bucket}/iceberg/warehouse"


# Lazy initialization - only create settings when accessed
_settings: Optional[Settings] = None
_settings_lock = threading.Lock()


def get_settings() -> Settings:
    """Get or create the Settings singleton (thread-safe).

    This function uses double-checked locking to ensure thread-safe
    initialization of the Settings singleton.

    Returns:
        Settings instance loaded from environment variables/.env file.

    Raises:
        ValidationError: If required configuration is missing.
    """
    global _settings
    
    # First check without lock (fast path)
    if _settings is not None:
        return _settings
    
    # Acquire lock for initialization
    with _settings_lock:
        # Double-check after acquiring lock
        if _settings is None:
            LOGGER.info("Initializing Settings from environment variables and .env file")
            try:
                _settings = Settings()
                LOGGER.info("Settings initialized successfully")
            except ValidationError as e:
                LOGGER.error("Configuration validation failed: %s", e)
                raise
            except Exception as e:
                LOGGER.error("Unexpected error loading configuration: %s", e)
                raise
    
    return _settings


# For backward compatibility and convenience
# NOTE: In test environments, import will fail if env vars not set.
# Tests should use get_settings() function or proper fixtures.
try:
    settings = get_settings()
except (ValidationError, KeyError, ValueError) as e:
    # Only catch expected configuration errors in test/development
    # Production should fail fast if configuration is invalid
    LOGGER.warning(
        "Settings validation failed at module import (expected in test environments): %s",
        e
    )
    settings = None  # type: ignore


__all__ = ["settings", "Settings", "get_settings"]
