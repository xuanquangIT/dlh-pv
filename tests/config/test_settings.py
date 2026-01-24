"""Tests for Pydantic settings configuration.

This module tests the centralized configuration management system to ensure:
- Settings load correctly from environment variables and .env files
- Required fields raise clear ValidationError when missing
- Default values are applied correctly
- Computed properties work as expected
- Environment variables override .env file values
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from pv_lakehouse.config.settings import (
    APISettings,
    DatabaseSettings,
    MLflowSettings,
    PrefectSettings,
    S3Settings,
    Settings,
    SparkSettings,
)


class TestDatabaseSettings:
    """Tests for DatabaseSettings configuration."""

    def test_load_from_env_vars(self, monkeypatch, clean_env):
        """Database settings should load from environment variables."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass123")
        monkeypatch.setenv("POSTGRES_HOST", "db.example.com")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("ICEBERG_CATALOG_DB", "custom_catalog")

        db_settings = DatabaseSettings()

        assert db_settings.postgres_user == "testuser"
        assert db_settings.postgres_password == "testpass123"
        assert db_settings.postgres_host == "db.example.com"
        assert db_settings.postgres_port == 5433
        assert db_settings.iceberg_catalog_db == "custom_catalog"

    def test_default_values(self, monkeypatch, clean_env):
        """Database settings should have sensible defaults."""
        monkeypatch.setenv("POSTGRES_PASSWORD", "required_pass")

        db_settings = DatabaseSettings()

        assert db_settings.postgres_user == "pvlakehouse"
        assert db_settings.postgres_host == "postgres"
        assert db_settings.postgres_port == 5432
        assert db_settings.postgres_db == "postgres"
        assert db_settings.iceberg_catalog_db == "iceberg_catalog"

    def test_missing_password_raises_error(self, clean_env):
        """Missing POSTGRES_PASSWORD should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            DatabaseSettings()

        error = str(exc_info.value)
        assert "postgres_password" in error.lower()
        assert "field required" in error.lower()

    def test_invalid_port_raises_error(self, monkeypatch, clean_env):
        """Invalid port number should raise ValidationError."""
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("POSTGRES_PORT", "99999")  # Invalid port

        with pytest.raises(ValidationError) as exc_info:
            DatabaseSettings()

        error = str(exc_info.value)
        assert "postgres_port" in error.lower()


class TestS3Settings:
    """Tests for S3/MinIO storage configuration."""

    def test_load_from_env_vars(self, monkeypatch, clean_env):
        """S3 settings should load from environment variables."""
        monkeypatch.setenv("MINIO_ENDPOINT", "https://s3.example.com")
        monkeypatch.setenv("SPARK_SVC_ACCESS_KEY", "custom_access")
        monkeypatch.setenv("SPARK_SVC_SECRET_KEY", "custom_secret")
        monkeypatch.setenv("S3_REGION", "ap-southeast-2")
        monkeypatch.setenv("S3_WAREHOUSE_BUCKET", "custom-warehouse")

        s3_settings = S3Settings()

        assert s3_settings.minio_endpoint == "https://s3.example.com"
        assert s3_settings.spark_svc_access_key == "custom_access"
        assert s3_settings.spark_svc_secret_key == "custom_secret"
        assert s3_settings.s3_region == "ap-southeast-2"
        assert s3_settings.s3_warehouse_bucket == "custom-warehouse"

    def test_endpoint_validation(self, monkeypatch, clean_env):
        """MinIO endpoint must start with http:// or https://."""
        monkeypatch.setenv("MINIO_ENDPOINT", "minio:9000")  # Missing protocol
        monkeypatch.setenv("SPARK_SVC_SECRET_KEY", "test_secret")

        with pytest.raises(ValidationError) as exc_info:
            S3Settings()

        error = str(exc_info.value)
        assert "minio_endpoint" in error.lower() or "http://" in error.lower()

    def test_endpoint_trailing_slash_removed(self, monkeypatch, clean_env):
        """Trailing slash should be removed from endpoint."""
        monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000/")
        monkeypatch.setenv("SPARK_SVC_SECRET_KEY", "test_secret")

        s3_settings = S3Settings()

        assert s3_settings.minio_endpoint == "http://minio:9000"


class TestAPISettings:
    """Tests for external API configuration."""

    def test_load_api_key(self, monkeypatch, clean_env):
        """API key should load from environment variable."""
        monkeypatch.setenv("OPENELECTRICITY_API_KEY", "test_key_12345")

        api_settings = APISettings()

        assert api_settings.openelectricity_api_key == "test_key_12345"
        assert api_settings.openelectricity_api_url == "https://api.openelectricity.org.au/v4"

    def test_missing_api_key_raises_error(self, clean_env):
        """Missing API key should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            APISettings()

        error = str(exc_info.value)
        assert "openelectricity_api_key" in error.lower()


class TestMLflowSettings:
    """Tests for MLflow configuration."""

    def test_default_tracking_uri(self, clean_env):
        """MLflow should have default tracking URI."""
        mlflow_settings = MLflowSettings()

        assert mlflow_settings.mlflow_tracking_uri == "http://mlflow:5000"
        assert mlflow_settings.mlflow_dfs_tmp is None

    def test_custom_tracking_uri(self, monkeypatch, clean_env):
        """MLflow tracking URI should be customizable."""
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://custom-mlflow:6000")
        monkeypatch.setenv("MLFLOW_DFS_TMP", "/tmp/mlflow")

        mlflow_settings = MLflowSettings()

        assert mlflow_settings.mlflow_tracking_uri == "http://custom-mlflow:6000"
        assert mlflow_settings.mlflow_dfs_tmp == "/tmp/mlflow"


class TestPrefectSettings:
    """Tests for Prefect orchestration configuration."""

    def test_default_values(self, clean_env):
        """Prefect should have default configuration."""
        prefect_settings = PrefectSettings()

        assert prefect_settings.prefect_api_url == "http://prefect:4200/api"
        assert prefect_settings.prefect_work_pool == "default-pool"
        assert prefect_settings.prefect_work_queue == "default"


class TestSparkSettings:
    """Tests for Apache Spark configuration."""

    def test_default_values(self, clean_env):
        """Spark should have default configuration."""
        spark_settings = SparkSettings()

        assert spark_settings.spark_worker_cores == 4
        assert spark_settings.spark_worker_memory == "3G"
        assert spark_settings.spark_shuffle_partitions == 200
        assert spark_settings.spark_memory_fraction == "0.7"
        assert spark_settings.spark_memory_storage_fraction == "0.4"

    def test_custom_values(self, monkeypatch, clean_env):
        """Spark settings should be customizable."""
        monkeypatch.setenv("SPARK_WORKER_CORES", "8")
        monkeypatch.setenv("SPARK_WORKER_MEMORY", "16G")
        monkeypatch.setenv("SPARK_SHUFFLE_PARTITIONS", "400")

        spark_settings = SparkSettings()

        assert spark_settings.spark_worker_cores == 8
        assert spark_settings.spark_worker_memory == "16G"
        assert spark_settings.spark_shuffle_partitions == 400


class TestSettingsIntegration:
    """Integration tests for complete Settings class."""

    def test_load_from_env_file(self, clean_env, monkeypatch):
        """Settings should load from environment variables."""
        # Set all required variables via monkeypatch
        monkeypatch.setenv("POSTGRES_USER", "test_user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
        monkeypatch.setenv("SPARK_SVC_SECRET_KEY", "test_spark_secret")
        monkeypatch.setenv("SPARK_SVC_ACCESS_KEY", "test_spark_access")
        monkeypatch.setenv("OPENELECTRICITY_API_KEY", "test_api_key_12345")
        
        settings = Settings(_env_file=None)
        
        assert settings.database.postgres_user == "test_user"
        assert settings.database.postgres_password == "test_password"
        assert settings.s3.spark_svc_access_key == "test_spark_access"
        assert settings.api.openelectricity_api_key == "test_api_key_12345"

    def test_missing_required_vars_raises_clear_error(self, clean_env):
        """Missing required variables should raise clear ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(_env_file=None)

        error = str(exc_info.value)
        # Should mention at least one of the required fields
        assert any(
            field in error.lower()
            for field in ["postgres_password", "spark_svc_secret_key", "openelectricity_api_key"]
        )

    def test_jdbc_url_construction(self, mock_env_minimal):
        """Settings should construct JDBC URL correctly."""
        settings = Settings(_env_file=None)

        jdbc_url = settings.jdbc_url

        assert jdbc_url.startswith("jdbc:postgresql://")
        assert settings.database.postgres_host in jdbc_url
        assert str(settings.database.postgres_port) in jdbc_url
        assert settings.database.iceberg_catalog_db in jdbc_url

    def test_s3_warehouse_path_construction(self, mock_env_minimal):
        """Settings should construct S3 warehouse path correctly."""
        settings = Settings(_env_file=None)

        warehouse_path = settings.s3_warehouse_path

        assert warehouse_path.startswith("s3a://")
        assert settings.s3.s3_warehouse_bucket in warehouse_path
        assert "iceberg/warehouse" in warehouse_path

    def test_env_var_override_file(self, test_env_file, monkeypatch, clean_env):
        """Environment variables should override .env file values."""
        # Set explicit env var that should override file
        monkeypatch.setenv("POSTGRES_USER", "override_user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "override_pass")
        monkeypatch.setenv("SPARK_SVC_SECRET_KEY", "override_secret")
        monkeypatch.setenv("OPENELECTRICITY_API_KEY", "override_key")

        settings = Settings(_env_file=test_env_file)

        assert settings.database.postgres_user == "override_user"
        assert settings.database.postgres_password == "override_pass"

    def test_nested_settings_access(self, mock_env_minimal):
        """Nested settings should be accessible via dot notation."""
        settings = Settings(_env_file=None)

        # Test nested access
        assert hasattr(settings, "database")
        assert hasattr(settings, "s3")
        assert hasattr(settings, "api")
        assert hasattr(settings, "mlflow")
        assert hasattr(settings, "prefect")
        assert hasattr(settings, "spark")

        # Test nested field access
        assert isinstance(settings.database.postgres_port, int)
        assert isinstance(settings.spark.spark_worker_cores, int)
        assert settings.database.postgres_user == "testuser"
        assert settings.api.openelectricity_api_key == "test-api-key"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
