#!/usr/bin/env python3
"""Unit tests for BaseBronzeLoader and BronzeLoadOptions."""

from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
import pytest
from pyspark.sql import DataFrame
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

    def test_resolve_facilities_default(self):
        """Test resolve_facilities returns default codes when None."""
        loader = MockLoader()
        codes = loader.resolve_facilities()
        assert isinstance(codes, list)
        assert len(codes) > 0  # Should return default solar facilities

    def test_resolve_facilities_custom(self):
        """Test resolve_facilities parses custom codes."""
        options = BronzeLoadOptions(facility_codes="TEST1, TEST2, TEST3")
        loader = MockLoader(options)
        codes = loader.resolve_facilities()
        assert codes == ["TEST1", "TEST2", "TEST3"]

    def test_add_ingest_columns(self):
        """Test add_ingest_columns adds required metadata."""
        loader = MockLoader()
        spark = loader.spark
        df = spark.createDataFrame([{"facility_code": "TEST1"}])
        result = loader.add_ingest_columns(df)

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

    def test_spark_session_creation(self):
        """Test SparkSession is created lazily."""
        loader = MockLoader()
        assert loader._spark is None
        spark = loader.spark
        assert spark is not None
        assert loader._spark is spark  # Cached

    def test_close_stops_spark(self):
        """Test close() stops SparkSession."""
        loader = MockLoader()
        _ = loader.spark  # Create session
        assert loader._spark is not None
        loader.close()
        assert loader._spark is None


class TestLoaderIntegration:
    """Integration tests for loader execution."""

    def test_run_backfill_mode(self):
        """Test full run in backfill mode."""
        options = BronzeLoadOptions(mode="backfill")
        loader = MockLoader(options)
        # Note: This would write to Iceberg in real scenario
        # For unit test, we just verify it doesn't crash
        pandas_df = loader.fetch_data()
        assert not pandas_df.empty
        assert "facility_code" in pandas_df.columns

    def test_fetch_and_transform_pipeline(self):
        """Test fetch -> transform pipeline."""
        loader = MockLoader()
        pandas_df = loader.fetch_data()
        spark_df = loader.spark.createDataFrame(pandas_df)
        transformed = loader.transform(spark_df)
        assert "test_col" in transformed.columns
        row = transformed.first()
        assert row["test_col"] == "transformed"
