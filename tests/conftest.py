"""Shared pytest fixtures for PV Lakehouse tests."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Generator

import pytest


@pytest.fixture(scope="session")
def test_env_file(tmp_path_factory: Any) -> str:
    """Create test .env file with all required variables.

    NOTE: This fixture creates a temporary .env file with TEST-ONLY credentials.
    These credentials are NOT used in production and should NEVER be committed
    to version control or used in any production environment.

    Environment variables can be overridden by setting TEST_* prefixed variables
    for CI/CD flexibility. Default fallback values are provided for local
    development convenience and are clearly marked with _TESTONLY suffix.

    Returns:
        Path to temporary .env file for testing.

    Raises:
        OSError: If unable to create or write temporary .env file.
    """
    env_file = tmp_path_factory.mktemp("config") / ".env"

    # Use environment variable fallbacks for CI/CD flexibility
    test_config = f"""

# These are intentionally weak test credentials for local testing only

POSTGRES_USER={os.getenv('TEST_POSTGRES_USER', 'test_user')}
POSTGRES_PASSWORD={os.getenv('TEST_POSTGRES_PASSWORD', 'test_password_TESTONLY')}
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=postgres
ICEBERG_CATALOG_DB=test_iceberg

MINIO_ENDPOINT=http://localhost:9000
SPARK_SVC_ACCESS_KEY={os.getenv('TEST_SPARK_ACCESS_KEY', 'test_spark_access_TESTONLY')}
SPARK_SVC_SECRET_KEY={os.getenv('TEST_SPARK_SECRET_KEY', 'test_spark_secret_TESTONLY')}
TRINO_SVC_ACCESS_KEY={os.getenv('TEST_TRINO_ACCESS_KEY', 'test_trino_access_TESTONLY')}
TRINO_SVC_SECRET_KEY={os.getenv('TEST_TRINO_SECRET_KEY', 'test_trino_secret_TESTONLY')}
MLFLOW_SVC_ACCESS_KEY={os.getenv('TEST_MLFLOW_ACCESS_KEY', 'test_mlflow_access_TESTONLY')}
MLFLOW_SVC_SECRET_KEY={os.getenv('TEST_MLFLOW_SECRET_KEY', 'test_mlflow_secret_TESTONLY')}
S3_REGION=us-east-1
S3_WAREHOUSE_BUCKET=test-lakehouse
S3_MLFLOW_BUCKET=test-mlflow

OPENELECTRICITY_API_KEY={os.getenv('TEST_OPENELECTRICITY_API_KEY', 'test_api_key_12345_TESTONLY')}
OPENELECTRICITY_API_URL=https://api.openelectricity.org.au/v4

MLFLOW_TRACKING_URI=http://localhost:5000
PREFECT_API_URL=http://localhost:4200/api
PREFECT_WORK_POOL=test-pool
PREFECT_WORK_QUEUE=test-queue

SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=1G
SPARK_SHUFFLE_PARTITIONS=10
SPARK_MEMORY_FRACTION=0.6
SPARK_MEMORY_STORAGE_FRACTION=0.3
"""

    try:
        env_file.write_text(test_config, encoding="utf-8")
        return str(env_file)
    except OSError as e:
        pytest.fail(f"Failed to create test environment file: {e}")


@pytest.fixture
def clean_env(monkeypatch) -> None:
    """Remove all PV Lakehouse env vars for isolated testing."""
    env_vars = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "ICEBERG_CATALOG_DB",
        "MINIO_ENDPOINT",
        "SPARK_SVC_ACCESS_KEY",
        "SPARK_SVC_SECRET_KEY",
        "TRINO_SVC_ACCESS_KEY",
        "TRINO_SVC_SECRET_KEY",
        "MLFLOW_SVC_ACCESS_KEY",
        "MLFLOW_SVC_SECRET_KEY",
        "S3_REGION",
        "S3_WAREHOUSE_BUCKET",
        "S3_MLFLOW_BUCKET",
        "OPENELECTRICITY_API_KEY",
        "OPENELECTRICITY_API_URL",
        "MLFLOW_TRACKING_URI",
        "PREFECT_API_URL",
        "PREFECT_WORK_POOL",
        "PREFECT_WORK_QUEUE",
        "SPARK_WORKER_CORES",
        "SPARK_WORKER_MEMORY",
        "SPARK_SHUFFLE_PARTITIONS",
        "SPARK_MEMORY_FRACTION",
        "SPARK_MEMORY_STORAGE_FRACTION",
    ]
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def mock_env_minimal(monkeypatch, clean_env) -> None:
    """Set minimal required environment variables for testing."""
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_pass")
    monkeypatch.setenv("SPARK_SVC_SECRET_KEY", "test_secret")
    monkeypatch.setenv("OPENELECTRICITY_API_KEY", "test-api-key")


@pytest.fixture
def project_root() -> Path:
    """Get project root directory.

    Returns:
        Path to project root (where pyproject.toml exists).
    """
    return Path(__file__).parent.parent


@pytest.fixture
def config_dir(project_root: Path) -> Path:
    """Get config directory path.

    Returns:
        Path to src/pv_lakehouse/config directory.
    """
    return project_root / "src" / "pv_lakehouse" / "config"


@pytest.fixture(autouse=True)
def reset_settings_cache() -> Generator[None, None, None]:
    """Reset settings cache between tests to ensure isolation."""
    # Clear any cached Spark config
    from pv_lakehouse.etl.utils import spark_utils

    spark_utils._CACHED_SPARK_CONFIG = None

    yield

    # Cleanup after test
    spark_utils._CACHED_SPARK_CONFIG = None
