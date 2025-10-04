"""Smoke tests for PV Lakehouse package.

These tests verify basic package structure and imports work correctly.
"""

import pathlib
import sys

# Ensure `src/` is on sys.path so tests can import the package in-place.
root = pathlib.Path(__file__).resolve().parents[1]
src_path = str(root / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)


def test_package_imports():
    """Test that main package can be imported."""
    import pv_lakehouse

    assert pv_lakehouse is not None


def test_etl_module_imports():
    """Test that ETL modules can be imported."""
    from pv_lakehouse.etl import bronze_ingest

    assert bronze_ingest is not None


def test_project_structure():
    """Verify critical project files exist."""
    project_root = pathlib.Path(__file__).resolve().parents[1]

    critical_files = [
        "pyproject.toml",
        "README.md",
        "Makefile",
        "docker/docker-compose.yml",
        "docker/.env.sample",
    ]

    for file_path in critical_files:
        full_path = project_root / file_path
        assert full_path.exists(), f"Missing critical file: {file_path}"


def test_docker_env_sample_exists():
    """Verify docker/.env.sample contains required variables."""
    project_root = pathlib.Path(__file__).resolve().parents[1]
    env_sample = project_root / "docker" / ".env.sample"

    assert env_sample.exists()

    content = env_sample.read_text()

    required_vars = [
        "PV_USER",
        "PV_PASSWORD",
        "MINIO_ROOT_USER",
        "POSTGRES_USER",
        "ICEBERG_DB",
        "MLFLOW_DB",
    ]

    for var in required_vars:
        assert var in content, f"Missing required variable in .env.sample: {var}"
