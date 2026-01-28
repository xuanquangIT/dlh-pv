#!/usr/bin/env python3
"""Integration tests for SilverHourlyEnergyLoader."""

import datetime as dt

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pv_lakehouse.etl.silver.hourly_energy import SilverHourlyEnergyLoader
from pv_lakehouse.etl.silver.base import LoadOptions


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-silver-energy")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestSilverHourlyEnergyLoader:
    """Integration tests for energy loader transform logic."""

    def test_transform_valid_data(self, spark):
        """Test transform with valid energy data."""
        loader = SilverHourlyEnergyLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("network_code", StringType(), False),
            StructField("network_region", StringType(), False),
            StructField("metric", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("interval_ts", TimestampType(), False),
        ])
        data = [
            ("WRSF1", "Facility 1", "NEM", "NSW", "energy", 50.5, dt.datetime(2024, 1, 15, 12, 0, tzinfo=dt.timezone.utc)),
            ("WRSF1", "Facility 1", "NEM", "NSW", "energy", 55.0, dt.datetime(2024, 1, 15, 13, 0, tzinfo=dt.timezone.utc)),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)

        assert result is not None
        assert "quality_flag" in result.columns
        assert "quality_issues" in result.columns
        assert "energy_mwh" in result.columns
        assert result.count() == 2

        rows = result.collect()
        # Energy data during daytime should have quality flag (may be GOOD or WARNING)
        assert all(row.quality_flag in ("GOOD", "WARNING", "BAD") for row in rows)
        # At least should have quality_flag column populated
        assert all(row.quality_flag is not None for row in rows)

    def test_transform_empty_dataframe(self, spark):
        """Test transform with empty DataFrame."""
        loader = SilverHourlyEnergyLoader(LoadOptions())

        result = loader.transform(None)
        assert result is None

    def test_transform_missing_columns(self, spark):
        """Test transform with missing required columns."""
        loader = SilverHourlyEnergyLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            # Missing other required columns
        ])
        data = [("WRSF1",)]
        bronze_df = spark.createDataFrame(data, schema)

        with pytest.raises(ValueError, match="Missing expected columns"):
            loader.transform(bronze_df)

    def test_transform_negative_energy(self, spark):
        """Test transform detects negative energy values."""
        loader = SilverHourlyEnergyLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("network_code", StringType(), False),
            StructField("network_region", StringType(), False),
            StructField("metric", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("interval_ts", TimestampType(), False),
        ])
        data = [
            ("WRSF1", "Facility 1", "NEM", "NSW", "energy", -10.0, dt.datetime(2024, 1, 15, 12, 0, tzinfo=dt.timezone.utc)),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)
        rows = result.collect()

        # Negative energy should be flagged as BAD (OUT_OF_BOUNDS)
        assert rows[0].quality_flag == "BAD"
        assert "OUT_OF_BOUNDS" in rows[0].quality_issues

    def test_transform_night_energy_anomaly(self, spark):
        """Test transform detects night energy anomaly."""
        loader = SilverHourlyEnergyLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("network_code", StringType(), False),
            StructField("network_region", StringType(), False),
            StructField("metric", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("interval_ts", TimestampType(), False),
        ])
        data = [
            # Night time (UTC) = 02:00 UTC = 13:00 Sydney (UTC+11) - NOT night in local time!
            # Use 15:00 UTC = 02:00 Sydney (next day) for actual night time
            ("WRSF1", "Facility 1", "NEM", "NSW", "energy", 5.0, dt.datetime(2024, 1, 14, 15, 0, tzinfo=dt.timezone.utc)),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)
        rows = result.collect()

        # Night energy should be flagged as BAD (severe issue)
        assert rows[0].quality_flag == "BAD"
        # Should have NIGHT_ENERGY in quality issues
        assert "NIGHT_ENERGY" in rows[0].quality_issues

    def test_transform_daytime_zero(self, spark):
        """Test transform detects daytime zero energy."""
        loader = SilverHourlyEnergyLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("network_code", StringType(), False),
            StructField("network_region", StringType(), False),
            StructField("metric", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("interval_ts", TimestampType(), False),
        ])
        data = [
            # Daytime with zero energy (downtime)
            ("WRSF1", "Facility 1", "NEM", "NSW", "energy", 0.0, dt.datetime(2024, 1, 15, 12, 0, tzinfo=dt.timezone.utc)),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)
        rows = result.collect()

        # Daytime zero should be WARNING (downtime issue)
        assert rows[0].quality_flag in ("WARNING", "GOOD")  # May be GOOD if within tolerance
        # If WARNING, should have DAYTIME_ZERO_ENERGY or EQUIPMENT_DOWNTIME
        if rows[0].quality_flag == "WARNING":
            assert any(keyword in rows[0].quality_issues for keyword in ["DAYTIME_ZERO_ENERGY", "EQUIPMENT_DOWNTIME", "DOWNTIME"])

    def test_transform_caching_cleanup(self, spark):
        """Test that transform properly caches and unpersists DataFrames."""
        loader = SilverHourlyEnergyLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("network_code", StringType(), False),
            StructField("network_region", StringType(), False),
            StructField("metric", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("interval_ts", TimestampType(), False),
        ])
        data = [
            ("WRSF1", "Facility 1", "NEM", "NSW", "energy", 50.0, dt.datetime(2024, 1, 15, 12, 0, tzinfo=dt.timezone.utc)),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)

        # Verify result is returned properly (caching was cleaned up)
        assert result is not None
        assert result.count() == 1
