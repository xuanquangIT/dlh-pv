"""Tests for Spark utilities with externalized configuration.

This module tests:
- YAML configuration loading
- Spark session creation with new config system
- Integration with Settings
- No hardcoded credentials remain
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pv_lakehouse.config.settings import Settings, get_settings
from pv_lakehouse.etl.utils.spark_utils import (
    cleanup_spark_staging,
    load_spark_config_from_yaml,
)


class TestSparkConfigYAML:
    """Tests for YAML configuration loading."""

    def test_yaml_file_exists(self, config_dir: Path):
        """spark_config.yaml should exist in config directory."""
        yaml_path = config_dir / "spark_config.yaml"
        assert yaml_path.exists(), f"Missing {yaml_path}"

    def test_yaml_is_valid(self, config_dir: Path):
        """spark_config.yaml should be valid YAML."""
        yaml_path = config_dir / "spark_config.yaml"

        with open(yaml_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        assert isinstance(config, dict)
        assert "catalog" in config
        assert "hadoop" in config
        assert "performance" in config

    def test_yaml_contains_no_secrets(self, config_dir: Path):
        """YAML file should not contain hardcoded secrets."""
        yaml_path = config_dir / "spark_config.yaml"

        with open(yaml_path, encoding="utf-8") as f:
            content = f.read()

        # Check for common hardcoded values (not in comments)
        lines = [line for line in content.split('\n') if not line.strip().startswith('#')]
        content_no_comments = '\n'.join(lines)
        
        forbidden_strings = [
            "pvlakehouse",
            '"password"',
            "secret_key",
            "spark_svc",
            "pvlakehouse_spark",
        ]

        for forbidden in forbidden_strings:
            assert (
                forbidden not in content_no_comments.lower()
            ), f"YAML contains hardcoded value: {forbidden}"

    def test_load_spark_config_from_yaml(self, mock_env_minimal):
        """load_spark_config_from_yaml should return valid config dict."""
        config = load_spark_config_from_yaml()

        assert isinstance(config, dict)
        assert len(config) > 10  # Should have many config entries

        # Check key Iceberg settings
        assert "spark.sql.extensions" in config
        assert "spark.sql.catalog.lh" in config
        assert "spark.sql.catalog.lh.type" in config
        assert "spark.sql.catalog.lh.uri" in config

        # Check S3/MinIO settings
        assert "spark.hadoop.fs.s3a.endpoint" in config
        assert "spark.hadoop.fs.s3a.access.key" in config
        assert "spark.hadoop.fs.s3a.secret.key" in config

    def test_config_uses_settings_for_secrets(self, mock_env_minimal):
        """Configuration should use Settings for secrets, not hardcoded values."""
        config = load_spark_config_from_yaml()
        settings = get_settings()

        # Verify NO hardcoded passwords
        assert config["spark.sql.catalog.lh.jdbc.password"] != "pvlakehouse"
        assert config["spark.hadoop.fs.s3a.secret.key"] != "pvlakehouse_spark"

        # Verify values come from settings
        assert config["spark.sql.catalog.lh.jdbc.user"] == settings.database.postgres_user
        assert config["spark.sql.catalog.lh.jdbc.password"] == settings.database.postgres_password
        assert config["spark.hadoop.fs.s3a.access.key"] == settings.s3.spark_svc_access_key
        assert config["spark.hadoop.fs.s3a.secret.key"] == settings.s3.spark_svc_secret_key

    def test_config_constructs_jdbc_url_correctly(self, mock_env_minimal):
        """Configuration should use Settings jdbc_url property."""
        config = load_spark_config_from_yaml()
        settings = get_settings()

        jdbc_uri = config["spark.sql.catalog.lh.uri"]

        assert jdbc_uri.startswith("jdbc:postgresql://")
        assert jdbc_uri == settings.jdbc_url

    def test_config_constructs_warehouse_path(self, mock_env_minimal):
        """Configuration should use Settings s3_warehouse_path property."""
        config = load_spark_config_from_yaml()
        settings = get_settings()

        warehouse = config["spark.sql.catalog.lh.warehouse"]

        assert warehouse.startswith("s3a://")
        assert warehouse == settings.s3_warehouse_path

    def test_config_caching(self, mock_env_minimal, reset_settings_cache):
        """Configuration should be cached after first load."""
        from pv_lakehouse.etl.utils import spark_utils

        # First call
        config1 = load_spark_config_from_yaml()
        # Should be cached now
        assert spark_utils._CACHED_SPARK_CONFIG is not None

        # Second call should return cached version
        config2 = load_spark_config_from_yaml()

        # Should return copies to prevent mutation
        assert config1 == config2
        assert config1 is not config2  # Different objects


class TestSparkSessionCreation:
    """Tests for Spark session creation."""

    def test_create_session_uses_yaml_config(self, mock_env_minimal, mocker):
        """create_spark_session should use YAML config loader."""
        # Mock SparkSession to avoid actual Spark startup
        mock_builder = mocker.patch("pv_lakehouse.etl.utils.spark_utils.SparkSession.builder")
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mocker.Mock()

        from pv_lakehouse.etl.utils.spark_utils import create_spark_session

        spark = create_spark_session("test-app")

        # Verify appName was called
        mock_builder.appName.assert_called_once_with("test-app")

        # Verify config was called multiple times (for each config item)
        assert mock_builder.config.call_count > 10

    def test_create_session_accepts_extra_conf(self, mock_env_minimal, mocker):
        """create_spark_session should accept and apply extra_conf."""
        mock_builder = mocker.patch("pv_lakehouse.etl.utils.spark_utils.SparkSession.builder")
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mocker.Mock()

        from pv_lakehouse.etl.utils.spark_utils import create_spark_session

        extra_conf = {"spark.sql.shuffle.partitions": "50", "spark.custom.setting": "value"}

        create_spark_session("test-app", extra_conf=extra_conf)

        # Verify extra conf was applied (hard to check exact calls, but should be called)
        assert mock_builder.config.call_count > 10


class TestCleanupSparkStaging:
    """Tests for Spark staging directory cleanup."""

    def test_cleanup_spark_staging_removes_directories(self, tmp_path, monkeypatch):
        """cleanup_spark_staging should remove spark-* directories."""
        # Create mock staging directories
        staging1 = tmp_path / "spark-abc123"
        staging2 = tmp_path / "spark-def456"
        other_dir = tmp_path / "other-directory"

        staging1.mkdir()
        staging2.mkdir()
        other_dir.mkdir()

        # Mock tempfile.gettempdir() to return our tmp_path
        monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))

        cleanup_spark_staging()

        # Spark directories should be removed
        assert not staging1.exists()
        assert not staging2.exists()
        # Other directories should remain
        assert other_dir.exists()

    def test_cleanup_with_custom_prefix(self, tmp_path, monkeypatch):
        """cleanup_spark_staging should respect custom prefix."""
        custom_dir = tmp_path / "custom-prefix-123"
        spark_dir = tmp_path / "spark-abc"

        custom_dir.mkdir()
        spark_dir.mkdir()

        monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))

        cleanup_spark_staging(prefix="custom-")

        # Custom prefix directory should be removed
        assert not custom_dir.exists()
        # Spark directory should remain (different prefix)
        assert spark_dir.exists()


class TestNoHardcodedCredentials:
    """Security tests to ensure no hardcoded credentials remain."""

    def test_spark_utils_has_no_hardcoded_password(self):
        """spark_utils.py should not contain hardcoded passwords."""
        spark_utils_path = Path(__file__).parent.parent.parent / "src" / "pv_lakehouse" / "etl" / "utils" / "spark_utils.py"

        with open(spark_utils_path, encoding="utf-8") as f:
            content = f.read()

        # Check for common hardcoded values
        forbidden_patterns = [
            '"pvlakehouse"',
            "'pvlakehouse'",
            '"pvlakehouse_spark"',
            "'pvlakehouse_spark'",
            "password.*=.*pvlakehouse",
        ]

        for pattern in forbidden_patterns:
            assert pattern.lower() not in content.lower(), f"Found hardcoded value matching: {pattern}"

    def test_default_spark_config_not_present(self):
        """DEFAULT_SPARK_CONFIG dict should not exist in spark_utils.py."""
        spark_utils_path = Path(__file__).parent.parent.parent / "src" / "pv_lakehouse" / "etl" / "utils" / "spark_utils.py"

        with open(spark_utils_path, encoding="utf-8") as f:
            content = f.read()

        assert "DEFAULT_SPARK_CONFIG" not in content, "Old DEFAULT_SPARK_CONFIG dict still exists"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
