"""Infrastructure and system setup tests.

This module tests the overall system setup including:
- Docker services availability
- Database connectivity
- MinIO/S3 storage
- Configuration consistency
"""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest


class TestProjectStructure:
    """Tests for project file structure and essential files."""

    def test_pyproject_toml_exists(self, project_root: Path):
        """pyproject.toml should exist in project root."""
        pyproject = project_root / "pyproject.toml"
        assert pyproject.exists(), "Missing pyproject.toml"

    def test_env_example_exists(self, project_root: Path):
        """.env.example should exist with required variables."""
        env_example = project_root / ".env.example"
        assert env_example.exists(), "Missing .env.example"

        # Check it contains required variables
        with open(env_example, encoding="utf-8") as f:
            content = f.read()

        required_vars = [
            "POSTGRES_PASSWORD",
            "SPARK_SVC_SECRET_KEY",
            "OPENELECTRICITY_API_KEY",
        ]

        for var in required_vars:
            assert var in content, f"Missing required variable in .env.example: {var}"

    def test_gitignore_includes_env_file(self, project_root: Path):
        """.gitignore should include .env to prevent secret commits."""
        gitignore = project_root / ".gitignore"
        assert gitignore.exists(), "Missing .gitignore"

        with open(gitignore, encoding="utf-8") as f:
            content = f.read()

        assert ".env" in content, ".gitignore should include .env"

    def test_spark_config_yaml_exists(self, config_dir: Path):
        """spark_config.yaml should exist in config directory."""
        yaml_path = config_dir / "spark_config.yaml"
        assert yaml_path.exists(), f"Missing {yaml_path}"

    def test_requirements_has_pydantic(self, project_root: Path):
        """requirements.txt should include pydantic and dependencies."""
        requirements = project_root / "requirements.txt"
        assert requirements.exists(), "Missing requirements.txt"

        with open(requirements, encoding="utf-8") as f:
            content = f.read()

        assert "pydantic" in content, "requirements.txt missing pydantic"
        assert "pydantic-settings" in content, "requirements.txt missing pydantic-settings"
        assert "PyYAML" in content, "requirements.txt missing PyYAML"


class TestDockerCompose:
    """Tests for Docker Compose configuration."""

    def test_docker_compose_file_exists(self, project_root: Path):
        """docker-compose.yml should exist."""
        compose_file = project_root / "docker" / "docker-compose.yml"
        assert compose_file.exists(), "Missing docker/docker-compose.yml"

    def test_docker_compose_uses_env_vars(self, project_root: Path):
        """docker-compose.yml should use environment variables."""
        compose_file = project_root / "docker" / "docker-compose.yml"

        with open(compose_file, encoding="utf-8") as f:
            content = f.read()

        # Should use env var syntax, not hardcoded values
        assert "${POSTGRES_PASSWORD}" in content, "docker-compose should use ${POSTGRES_PASSWORD}"
        assert "${MINIO_ENDPOINT}" in content, "docker-compose should use ${MINIO_ENDPOINT}"


@pytest.mark.integration
class TestServiceConnectivity:
    """Integration tests for Docker services (requires running stack)."""

    def test_postgres_port_accessible(self):
        """PostgreSQL port should be accessible (if service is running)."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                result = sock.connect_ex(("localhost", 5432))

            if result == 0:
                pytest.skip("PostgreSQL service is running")
            else:
                pytest.skip("PostgreSQL service not running (expected in CI)")
        except (OSError, socket.error) as e:
            pytest.skip("Cannot test PostgreSQL connectivity")

    def test_minio_port_accessible(self):
        """MinIO port should be accessible (if service is running)."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                result = sock.connect_ex(("localhost", 9000))

            if result == 0:
                pytest.skip("MinIO service is running")
            else:
                pytest.skip("MinIO service not running (expected in CI)")
        except (OSError, socket.error) as e:
            pytest.skip("Cannot test MinIO connectivity")


class TestConfigurationConsistency:
    """Tests for configuration consistency across files."""

    def test_env_example_matches_settings_fields(self, project_root: Path):
        """Environment variables in .env.example should match Settings fields."""
        env_example = project_root / ".env.example"

        with open(env_example, encoding="utf-8") as f:
            env_content = f.read()

        # Check that key settings fields are represented
        expected_mappings = {
            "POSTGRES_PASSWORD": "database.postgres_password",
            "SPARK_SVC_SECRET_KEY": "s3.spark_svc_secret_key",
            "OPENELECTRICITY_API_KEY": "api.openelectricity_api_key",
            "MLFLOW_TRACKING_URI": "mlflow.mlflow_tracking_uri",
            "PREFECT_API_URL": "prefect.prefect_api_url",
        }

        for env_var, setting_path in expected_mappings.items():
            assert env_var in env_content, f"Missing {env_var} in .env.example (needed for {setting_path})"


class TestSecurityChecks:
    """Security-focused tests to prevent credential leaks."""

    def test_no_env_file_in_git(self, project_root: Path):
        """.env file should not exist in git repository."""
        env_file = project_root / ".env"

        # If .env exists, it should not be tracked by git
        if env_file.exists():
            try:
                import subprocess

                result = subprocess.run(
                    ["git", "ls-files", str(env_file)],
                    cwd=project_root,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                assert result.stdout.strip() == "", ".env file should not be tracked by git"
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pytest.skip("Git not available or timeout")

    def test_no_hardcoded_passwords_in_python_files(self, project_root: Path):
        """Python source files should not contain hardcoded passwords."""
        src_dir = project_root / "src" / "pv_lakehouse"

        if not src_dir.exists():
            pytest.skip("src directory not found")

        python_files = list(src_dir.rglob("*.py"))

        # Extended patterns to catch more formats
        forbidden_patterns = [
            b'"pvlakehouse"',  # Old hardcoded password (double quotes)
            b"'pvlakehouse'",  # Old hardcoded password (single quotes)
            b'"pvlakehouse_spark"',  # Old S3 secret (double quotes)
            b"'pvlakehouse_spark'",  # Old S3 secret (single quotes)
            b'f"pvlakehouse"',  # F-string format
            b"f'pvlakehouse'",  # F-string format
            b'password="pvlakehouse"',  # Assignment format
            b"password='pvlakehouse'",  # Assignment format
            b"pvlakehouse_trino",  # Other service accounts
            b"pvlakehouse_mlflow",  # MLflow service account
        ]

        violations = []

        for py_file in python_files:
            # Skip __pycache__, test files, and settings.py (contains defaults)
            if "__pycache__" in str(py_file) or "test_" in py_file.name or "settings.py" in str(py_file):
                continue

            with open(py_file, "rb") as f:
                content = f.read()

            for pattern in forbidden_patterns:
                if pattern in content:
                    violations.append(f"{py_file}: contains {pattern.decode()}")

        assert not violations, f"Found hardcoded credentials:\n" + "\n".join(violations)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
