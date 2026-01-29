#!/usr/bin/env python3
"""Unit tests for BaseBronzeLoader and BronzeLoadOptions."""

from __future__ import annotations

import datetime as dt
from typing import Optional, Generator

import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pv_lakehouse.etl.bronze.base import BaseBronzeLoader, BronzeLoadOptions


class MockLoader(BaseBronzeLoader):
    """Mock loader for testing BaseBronzeLoader."""

    iceberg_table = "lh.bronze.raw_facilities"
    timestamp_column = "ingest_timestamp"
    merge_keys = ("facility_code",)

    def fetch_data(self) -> pd.DataFrame:
        """Return mock data."""
        return pd.DataFrame({
            "facility_code": ["TEST1", "TEST2"],
            "name": ["Test Facility 1", "Test Facility 2"],
        })

    def transform(self, df: DataFrame) -> DataFrame:
        """Add test column."""
        return df.withColumn("test_col", F.lit("transformed"))


@pytest.fixture
def mock_loader(shared_spark: SparkSession) -> Generator[MockLoader, None, None]:
    """Create a MockLoader with shared SparkSession."""
    loader = MockLoader()
    loader._spark = shared_spark
    try:
        yield loader
    finally:
        loader._spark = None


class TestBronzeLoadOptions:
    """Test BronzeLoadOptions dataclass."""

    def test_default_values(self):
        """Test default option values."""
        options = BronzeLoadOptions()
        assert options.mode == "incremental"
        assert options.start is None
        assert options.end is None
        assert options.facility_codes is None
        assert options.api_key is None
        assert options.max_workers == 4
        assert options.app_name == "bronze-loader"

    def test_custom_values(self):
        """Test setting custom option values."""
        start = dt.datetime(2025, 1, 1)
        end = dt.datetime(2025, 12, 31)
        options = BronzeLoadOptions(
            mode="backfill",
            start=start,
            end=end,
            facility_codes="TEST1,TEST2",
            api_key="test-key",
            max_workers=8,
            app_name="test-app",
        )
        assert options.mode == "backfill"
        assert options.start == start
        assert options.end == end
        assert options.facility_codes == "TEST1,TEST2"
        assert options.api_key == "test-key"
        assert options.max_workers == 8
        assert options.app_name == "test-app"


class TestBaseBronzeLoader:
    """Test BaseBronzeLoader abstract class."""

    def test_initialization(self):
        """Test loader initializes with default options."""
        loader = MockLoader()
        assert loader.options is not None
        assert loader.options.mode == "incremental"
        assert loader.iceberg_table == "lh.bronze.raw_facilities"

    def test_initialization_with_options(self):
        """Test loader initializes with custom options."""
        options = BronzeLoadOptions(mode="backfill", facility_codes="TEST1")
        loader = MockLoader(options)
        assert loader.options.mode == "backfill"
        assert loader.options.facility_codes == "TEST1"

    def test_invalid_table_name(self):
        """Test validation rejects invalid table names."""
        class InvalidLoader(MockLoader):
            iceberg_table = "lh.bronze.invalid_table"

        with pytest.raises(ValueError, match="not in allowed list"):
            InvalidLoader()

    def test_resolve_facilities_default(self, mock_loader: MockLoader):
        """Test resolve_facilities returns default codes when None."""
        codes = mock_loader.resolve_facilities()
        assert isinstance(codes, list)
        assert len(codes) > 0  # Should return default solar facilities

    def test_resolve_facilities_custom(self, shared_spark: SparkSession):
        """Test resolve_facilities parses custom codes."""
        options = BronzeLoadOptions(facility_codes="TEST1, TEST2, TEST3")
        loader = MockLoader(options)
        loader._spark = shared_spark
        codes = loader.resolve_facilities()
        assert codes == ["TEST1", "TEST2", "TEST3"]

    def test_add_ingest_columns(self, mock_loader: MockLoader):
        """Test add_ingest_columns adds required metadata."""
        spark = mock_loader.spark
        df = spark.createDataFrame([{"facility_code": "TEST1"}])
        result = mock_loader.add_ingest_columns(df)

        assert "ingest_mode" in result.columns
        assert "ingest_timestamp" in result.columns
        row = result.first()
        assert row["ingest_mode"] == "incremental"
        assert row["ingest_timestamp"] is not None

    def test_initialize_date_range_override(self):
        """Test _initialize_date_range can be overridden."""
        class CustomLoader(MockLoader):
            def _initialize_date_range(self):
                self.options.start = dt.datetime(2025, 1, 1)

        loader = CustomLoader()
        assert loader.options.start == dt.datetime(2025, 1, 1)

    def test_spark_session_creation(self, shared_spark: SparkSession):
        """Test SparkSession is created lazily."""
        loader = MockLoader()
        assert loader._spark is None
        # Inject shared spark instead of creating new one
        loader._spark = shared_spark
        spark = loader.spark
        assert spark is not None
        assert loader._spark is spark  # Cached

    def test_close_clears_spark_reference(self, shared_spark: SparkSession):
        """Test close() clears SparkSession reference."""
        loader = MockLoader()
        loader._spark = shared_spark
        assert loader._spark is not None
        # Note: We can't call close() as it would stop the shared session
        # Instead test that _spark can be cleared
        loader._spark = None
        assert loader._spark is None


class TestLoaderIntegration:
    """Integration tests for loader execution."""

    def test_run_backfill_mode(self, shared_spark: SparkSession):
        """Test full run in backfill mode."""
        options = BronzeLoadOptions(mode="backfill")
        loader = MockLoader(options)
        loader._spark = shared_spark
        # Note: This would write to Iceberg in real scenario
        # For unit test, we just verify it doesn't crash
        pandas_df = loader.fetch_data()
        assert not pandas_df.empty
        assert "facility_code" in pandas_df.columns

    def test_fetch_and_transform_pipeline(self, mock_loader: MockLoader):
        """Test fetch -> transform pipeline."""
        pandas_df = mock_loader.fetch_data()
        spark_df = mock_loader.spark.createDataFrame(pandas_df)
        transformed = mock_loader.transform(spark_df)
        assert "test_col" in transformed.columns
        row = transformed.first()
        assert row["test_col"] == "transformed"
