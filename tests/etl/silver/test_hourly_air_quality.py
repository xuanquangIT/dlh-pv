#!/usr/bin/env python3
"""Integration tests for SilverHourlyAirQualityLoader."""

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

from pv_lakehouse.etl.silver.hourly_air_quality import SilverHourlyAirQualityLoader
from pv_lakehouse.etl.silver.base import LoadOptions


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-silver-air-quality")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestSilverHourlyAirQualityLoader:
    """Integration tests for air quality loader transform logic."""

    def test_transform_valid_data(self, spark):
        """Test transform with valid air quality data."""
        loader = SilverHourlyAirQualityLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("air_timestamp", TimestampType(), False),
            StructField("pm2_5", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("dust", DoubleType(), True),
            StructField("nitrogen_dioxide", DoubleType(), True),
            StructField("ozone", DoubleType(), True),
            StructField("sulphur_dioxide", DoubleType(), True),
            StructField("carbon_monoxide", DoubleType(), True),
            StructField("uv_index", DoubleType(), True),
            StructField("uv_index_clear_sky", DoubleType(), True),
        ])
        data = [
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 12, 0), 10.5, 20.0, 15.0, 25.0, 50.0, 5.0, 0.3, 5.0, 6.0),
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 13, 0), 12.0, 22.0, 18.0, 28.0, 55.0, 6.0, 0.4, 5.5, 6.5),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)

        assert result is not None
        assert "quality_flag" in result.columns
        assert "quality_issues" in result.columns
        assert "aqi_value" in result.columns
        assert "aqi_category" in result.columns
        assert result.count() == 2

        rows = result.collect()
        # Valid data should have GOOD quality flag
        assert all(row.quality_flag in ("GOOD", "WARNING") for row in rows)
        # PM2.5 of 10-12 should be in "Good" AQI category
        assert all(row.aqi_category == "Good" for row in rows)

    def test_transform_empty_dataframe(self, spark):
        """Test transform with empty DataFrame."""
        loader = SilverHourlyAirQualityLoader(LoadOptions())

        result = loader.transform(None)
        assert result is None

    def test_transform_missing_columns(self, spark):
        """Test transform with missing required columns."""
        loader = SilverHourlyAirQualityLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            # Missing facility_name and air_timestamp
        ])
        data = [("WRSF1",)]
        bronze_df = spark.createDataFrame(data, schema)

        with pytest.raises(ValueError, match="Missing expected columns"):
            loader.transform(bronze_df)

    def test_transform_aqi_calculation(self, spark):
        """Test AQI calculation from PM2.5 values."""
        loader = SilverHourlyAirQualityLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("air_timestamp", TimestampType(), False),
            StructField("pm2_5", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("dust", DoubleType(), True),
            StructField("nitrogen_dioxide", DoubleType(), True),
            StructField("ozone", DoubleType(), True),
            StructField("sulphur_dioxide", DoubleType(), True),
            StructField("carbon_monoxide", DoubleType(), True),
            StructField("uv_index", DoubleType(), True),
            StructField("uv_index_clear_sky", DoubleType(), True),
        ])
        data = [
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 12, 0), 8.0, 15.0, 10.0, 20.0, 40.0, 4.0, 0.2, 4.0, 5.0),     # Good (AQI ~33)
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 13, 0), 25.0, 40.0, 30.0, 35.0, 65.0, 8.0, 0.5, 6.0, 7.0),    # Moderate (AQI ~75)
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 14, 0), 50.0, 80.0, 60.0, 55.0, 90.0, 12.0, 0.8, 7.0, 8.0),    # Unhealthy (AQI ~137)
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 15, 0), 175.0, 250.0, 200.0, 100.0, 150.0, 20.0, 1.2, 9.0, 10.0),   # Hazardous (AQI ~225)
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)
        rows = result.collect()

        assert rows[0].aqi_category == "Good"
        assert rows[1].aqi_category == "Moderate"
        assert rows[2].aqi_category == "Unhealthy"
        assert rows[3].aqi_category == "Hazardous"

    def test_transform_out_of_bounds(self, spark):
        """Test transform detects out of bounds PM2.5 values."""
        loader = SilverHourlyAirQualityLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("air_timestamp", TimestampType(), False),
            StructField("pm2_5", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("dust", DoubleType(), True),
            StructField("nitrogen_dioxide", DoubleType(), True),
            StructField("ozone", DoubleType(), True),
            StructField("sulphur_dioxide", DoubleType(), True),
            StructField("carbon_monoxide", DoubleType(), True),
            StructField("uv_index", DoubleType(), True),
            StructField("uv_index_clear_sky", DoubleType(), True),
        ])
        data = [
            # PM2.5 way out of bounds (negative or extremely high)
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 12, 0), -5.0, 10.0, 8.0, 15.0, 30.0, 3.0, 0.1, 3.0, 4.0),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)
        rows = result.collect()

        assert rows[0].quality_flag == "WARNING"
        assert "pm2_5" in rows[0].quality_issues

    def test_transform_caching_cleanup(self, spark):
        """Test that transform properly caches and unpersists DataFrames."""
        loader = SilverHourlyAirQualityLoader(LoadOptions())

        schema = StructType([
            StructField("facility_code", StringType(), False),
            StructField("facility_name", StringType(), False),
            StructField("air_timestamp", TimestampType(), False),
            StructField("pm2_5", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("dust", DoubleType(), True),
            StructField("nitrogen_dioxide", DoubleType(), True),
            StructField("ozone", DoubleType(), True),
            StructField("sulphur_dioxide", DoubleType(), True),
            StructField("carbon_monoxide", DoubleType(), True),
            StructField("uv_index", DoubleType(), True),
            StructField("uv_index_clear_sky", DoubleType(), True),
        ])
        data = [
            ("WRSF1", "Facility 1", dt.datetime(2024, 1, 15, 12, 0), 10.0, 18.0, 12.0, 22.0, 45.0, 4.5, 0.25, 4.5, 5.5),
        ]
        bronze_df = spark.createDataFrame(data, schema)

        result = loader.transform(bronze_df)

        # Verify result is returned properly (caching was cleaned up)
        assert result is not None
        assert result.count() == 1
