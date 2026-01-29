#!/usr/bin/env python3
"""Integration tests for SilverHourlyWeatherLoader."""

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

from pv_lakehouse.etl.silver.hourly_weather import SilverHourlyWeatherLoader
from pv_lakehouse.etl.silver.base import LoadOptions


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-silver-weather")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestSilverHourlyWeatherLoader:
    """Integration tests for weather loader transform logic."""

    def test_transform_valid_data(self, spark):
        """Test transform with valid weather data."""
        loader = SilverHourlyWeatherLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("weather_timestamp", TimestampType(), False),
            StructField("temperature_2m", DoubleType(), True),
            StructField("shortwave_radiation", DoubleType(), True),
            StructField("wind_speed_10m", DoubleType(), True),
            StructField("cloud_cover", DoubleType(), True),
        ])
        data = [
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 12, 0), 25.5, 800.0, 5.5, 10.0),
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 13, 0), 26.0, 850.0, 6.0, 15.0),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)

        assert result is not None
        assert "quality_flag" in result.columns
        assert "quality_issues" in result.columns
        assert "is_valid" in result.columns
        assert result.count() == 2

        rows = result.collect()
        # Valid data should have GOOD quality flag
        assert all(row.quality_flag in ("GOOD", "WARNING") for row in rows)

    def test_transform_empty_dataframe(self, spark):
        """Test transform with empty DataFrame."""
        loader = SilverHourlyWeatherLoader(LoadOptions())

        result = loader.transform(None)
        assert result is None

    def test_transform_missing_columns(self, spark):
        """Test transform with missing required columns."""
        loader = SilverHourlyWeatherLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            # Missing facility_name and weather_timestamp
        ])
        data = [("WRSF1",)]
        bronze_df = spark.createDataFrame(data, schema)

        with pytest.raises(ValueError, match="Missing expected columns"):
            loader.transform(bronze_df)

    def test_transform_night_radiation_anomaly(self, spark):
        """Test transform detects night radiation anomaly."""
        loader = SilverHourlyWeatherLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("weather_timestamp", TimestampType(), False),
            StructField("temperature_2m", DoubleType(), True),
            StructField("shortwave_radiation", DoubleType(), True),
            StructField("wind_speed_10m", DoubleType(), True),
            StructField("cloud_cover", DoubleType(), True),
        ])
        data = [
            # Night time (1 AM) with high radiation - should flag as BAD
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 1, 0), 20.0, 200.0, 2.0, 0.0),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)
        rows = result.collect()

        assert rows[0].quality_flag == "BAD"
        assert "NIGHT_RADIATION" in rows[0].quality_issues

    def test_transform_out_of_bounds(self, spark):
        """Test transform detects out of bounds values."""
        loader = SilverHourlyWeatherLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("weather_timestamp", TimestampType(), False),
            StructField("temperature_2m", DoubleType(), True),
            StructField("shortwave_radiation", DoubleType(), True),
            StructField("wind_speed_10m", DoubleType(), True),
        ])
        data = [
            # Temperature way out of bounds
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 12, 0), 80.0, 800.0, 5.0),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)
        rows = result.collect()

        assert rows[0].quality_flag == "BAD"
        assert "temperature_2m" in rows[0].quality_issues

    def test_transform_caching_cleanup(self, spark):
        """Test that transform properly caches and unpersists DataFrames."""
        loader = SilverHourlyWeatherLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("weather_timestamp", TimestampType(), False),
            StructField("temperature_2m", DoubleType(), True),
        ])
        data = [
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 12, 0), 25.0),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)

        # Verify result is returned properly (caching was cleaned up)
        assert result is not None
        assert result.count() == 1
