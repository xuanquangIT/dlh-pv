#!/usr/bin/env python3
"""Shared pytest fixtures for Bronze ETL tests."""

from __future__ import annotations

from typing import Generator
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def shared_spark() -> Generator[SparkSession, None, None]:
    """Create a session-scoped SparkSession shared across all bronze tests.

    This prevents temp directory conflicts when multiple tests create/close
    separate SparkSessions. Using session scope ensures only one SparkSession
    exists for all tests in the bronze test module.
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("bronze-loader-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()


@pytest.fixture(scope="session", autouse=True)
def mock_spark_session_creation(shared_spark: SparkSession) -> Generator[None, None, None]:
    """Patch create_spark_session to always return the shared session.

    This ensures ALL loaders created during tests use the same SparkSession,
    preventing temp directory conflicts and resource leaks.
    """
    def _return_shared(*args, **kwargs):
        return shared_spark

    with patch("pv_lakehouse.etl.utils.spark_utils.create_spark_session", side_effect=_return_shared):
        with patch("pv_lakehouse.etl.bronze.base.create_spark_session", side_effect=_return_shared):
            yield
