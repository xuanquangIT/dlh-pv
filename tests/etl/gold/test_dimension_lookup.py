"""Tests for dimension lookup helper functions.

Tests cover:
- Generic dimension key lookup
- Facility key lookup with exact match
- Date key lookup with date expression
- Time key lookup with hour extraction
- AQI category key lookup with range matching
"""

from __future__ import annotations

import datetime as dt
from typing import Dict

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pv_lakehouse.etl.gold.dimension_lookup import (
    lookup_aqi_category_key,
    lookup_date_key,
    lookup_dimension_key,
    lookup_facility_key,
    lookup_time_key,
)


class TestLookupDimensionKey:
    """Tests for generic lookup_dimension_key function."""

    def test_raises_on_empty_fact(self, spark: SparkSession, dim_facility_df: DataFrame):
        """Should raise ValueError when fact DataFrame is empty."""
        schema = T.StructType([T.StructField("facility_code", T.StringType())])
        empty_fact = spark.createDataFrame([], schema)

        with pytest.raises(ValueError, match="fact_df cannot be None or empty"):
            lookup_dimension_key(
                empty_fact,
                dim_facility_df,
                F.col("fact.facility_code") == F.col("dim.facility_code"),
                "facility_key",
            )

    def test_raises_on_empty_dimension(self, spark: SparkSession):
        """Should raise ValueError when dimension DataFrame is empty."""
        fact = spark.createDataFrame([("FAC1",)], ["facility_code"])
        schema = T.StructType(
            [
                T.StructField("facility_code", T.StringType()),
                T.StructField("facility_key", T.IntegerType()),
            ]
        )
        empty_dim = spark.createDataFrame([], schema)

        with pytest.raises(ValueError, match="dim_df cannot be None or empty"):
            lookup_dimension_key(
                fact,
                empty_dim,
                F.col("fact.facility_code") == F.col("dim.facility_code"),
                "facility_key",
            )

    def test_joins_key_column(self, spark: SparkSession):
        """Should join dimension key column to fact table."""
        fact = spark.createDataFrame([("FAC1",), ("FAC2",)], ["facility_code"])
        dim = spark.createDataFrame(
            [(1, "FAC1"), (2, "FAC2")], ["facility_key", "facility_code"]
        )

        result = lookup_dimension_key(
            fact,
            dim,
            F.col("fact.facility_code") == F.col("dim.facility_code"),
            "facility_key",
        )

        assert "facility_key" in result.columns
        assert result.count() == 2

    def test_includes_additional_columns(self, spark: SparkSession):
        """Should include additional columns when specified."""
        fact = spark.createDataFrame([("FAC1",)], ["facility_code"])
        dim = spark.createDataFrame(
            [(1, "FAC1", "Facility One")], ["facility_key", "facility_code", "name"]
        )

        result = lookup_dimension_key(
            fact,
            dim,
            F.col("fact.facility_code") == F.col("dim.facility_code"),
            "facility_key",
            additional_columns=["name"],
        )

        assert "name" in result.columns
        row = result.collect()[0]
        assert row["name"] == "Facility One"


class TestLookupFacilityKey:
    """Tests for lookup_facility_key function."""

    def test_joins_facility_key(
        self, hourly_energy_df: DataFrame, dim_facility_df: DataFrame
    ):
        """Should join facility_key to fact table."""
        result = lookup_facility_key(hourly_energy_df, dim_facility_df)

        assert "facility_key" in result.columns

        # Verify SOLAR_FARM_A has facility_key = 1
        row = result.filter(F.col("facility_code") == "SOLAR_FARM_A").first()
        assert row["facility_key"] == 1

    def test_includes_capacity(
        self, hourly_energy_df: DataFrame, dim_facility_df: DataFrame
    ):
        """Should include total_capacity_mw by default."""
        result = lookup_facility_key(hourly_energy_df, dim_facility_df)

        assert "total_capacity_mw" in result.columns

    def test_handles_missing_facility(self, spark: SparkSession, dim_facility_df: DataFrame):
        """Should handle facilities not in dimension (left join)."""
        fact = spark.createDataFrame([("UNKNOWN_FAC",)], ["facility_code"])

        result = lookup_facility_key(fact, dim_facility_df)

        row = result.collect()[0]
        assert row["facility_key"] is None

    def test_returns_input_on_empty_dimension(
        self, hourly_energy_df: DataFrame, spark: SparkSession
    ):
        """Should return input when dimension is empty."""
        schema = T.StructType(
            [
                T.StructField("facility_key", T.IntegerType()),
                T.StructField("facility_code", T.StringType()),
                T.StructField("total_capacity_mw", T.DecimalType(10, 2)),
            ]
        )
        empty_dim = spark.createDataFrame([], schema)

        result = lookup_facility_key(hourly_energy_df, empty_dim)

        # Should return original DataFrame unchanged
        assert result.count() == hourly_energy_df.count()


class TestLookupDateKey:
    """Tests for lookup_date_key function."""

    def test_joins_date_key(
        self, hourly_energy_df: DataFrame, dim_date_df: DataFrame
    ):
        """Should join date_key based on date expression."""
        result = lookup_date_key(
            hourly_energy_df,
            dim_date_df,
            F.to_date(F.col("date_hour")),
        )

        assert "date_key" in result.columns

        # Verify 2024-01-01 has date_key = 20240101
        row = result.filter(F.to_date(F.col("date_hour")) == "2024-01-01").first()
        assert row["date_key"] == 20240101

    def test_includes_calendar_columns(
        self, hourly_energy_df: DataFrame, dim_date_df: DataFrame
    ):
        """Should include default calendar columns."""
        result = lookup_date_key(
            hourly_energy_df,
            dim_date_df,
            F.to_date(F.col("date_hour")),
        )

        # Default columns
        assert "year" in result.columns
        assert "quarter" in result.columns
        assert "month" in result.columns

    def test_custom_additional_columns(
        self, hourly_energy_df: DataFrame, dim_date_df: DataFrame
    ):
        """Should include specified additional columns."""
        result = lookup_date_key(
            hourly_energy_df,
            dim_date_df,
            F.to_date(F.col("date_hour")),
            additional_columns=["day_name"],
        )

        assert "day_name" in result.columns


class TestLookupTimeKey:
    """Tests for lookup_time_key function."""

    def test_joins_time_key(
        self, hourly_energy_df: DataFrame, dim_time_df: DataFrame
    ):
        """Should join time_key based on hour expression."""
        result = lookup_time_key(
            hourly_energy_df,
            dim_time_df,
            F.hour(F.col("date_hour")),
        )

        assert "time_key" in result.columns

        # Calculate expected time_key using Spark's hour function
        # Note: Spark converts timestamps to UTC, so hour values may shift
        result_with_hour = result.withColumn("_spark_hour", F.hour(F.col("date_hour")))
        row = result_with_hour.first()
        assert row is not None, "Result should have at least one row"

        # time_key should be hour * 100 format
        spark_hour = row["_spark_hour"]
        expected_time_key = spark_hour * 100
        assert row["time_key"] == expected_time_key, (
            f"time_key {row['time_key']} should equal {expected_time_key} "
            f"for Spark hour {spark_hour}"
        )

    def test_includes_time_of_day(
        self, hourly_energy_df: DataFrame, dim_time_df: DataFrame
    ):
        """Should include time_of_day by default."""
        result = lookup_time_key(
            hourly_energy_df,
            dim_time_df,
            F.hour(F.col("date_hour")),
        )

        assert "time_of_day" in result.columns


class TestLookupAqiCategoryKey:
    """Tests for lookup_aqi_category_key function."""

    def test_joins_aqi_key_by_range(
        self, hourly_air_quality_df: DataFrame, dim_aqi_category_df: DataFrame
    ):
        """Should join aqi_category_key based on AQI value range."""
        result = lookup_aqi_category_key(
            hourly_air_quality_df,
            dim_aqi_category_df,
            aqi_value_column="aqi_value",
        )

        assert "aqi_category_key" in result.columns

        # AQI 35 should be "Good" (category_key = 1)
        row = result.filter(F.col("aqi_value") == 35).first()
        assert row["aqi_category_key"] == 1

        # AQI 68 should be "Moderate" (category_key = 2)
        row = result.filter(F.col("aqi_value") == 68).first()
        assert row["aqi_category_key"] == 2

    def test_handles_edge_values(self, spark: SparkSession, dim_aqi_category_df: DataFrame):
        """Should correctly categorize boundary AQI values."""
        fact = spark.createDataFrame(
            [(50,), (51,), (100,), (101,)],  # Boundary values
            ["aqi_value"],
        )

        result = lookup_aqi_category_key(fact, dim_aqi_category_df)
        rows = result.collect()

        # Map results
        results = {row["aqi_value"]: row["aqi_category_key"] for row in rows}

        assert results[50] == 1  # Good (0-50)
        assert results[51] == 2  # Moderate (51-100)
        assert results[100] == 2  # Moderate (51-100)
        assert results[101] == 3  # Unhealthy for Sensitive (101-150)
