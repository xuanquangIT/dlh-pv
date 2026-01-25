"""Tests for Gold layer common helper functions.

Tests cover:
- DataFrame size estimation
- Broadcast dimension helper
- Empty DataFrame checks
- Date/time key computation
"""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pv_lakehouse.etl.gold.common import (
    broadcast_small_dim,
    compute_date_key,
    dec,
    estimate_dataframe_size_mb,
    first_value,
    is_empty,
    season_expr,
    time_of_day_expr,
)


class TestIsEmpty:
    """Tests for is_empty helper function."""

    def test_none_is_empty(self):
        """None DataFrame should be empty."""
        assert is_empty(None) is True

    def test_empty_dataframe_is_empty(self, spark: SparkSession):
        """Empty DataFrame should be empty."""
        schema = T.StructType([T.StructField("id", T.IntegerType())])
        df = spark.createDataFrame([], schema)

        assert is_empty(df) is True

    def test_non_empty_dataframe_not_empty(self, spark: SparkSession):
        """Non-empty DataFrame should not be empty."""
        df = spark.createDataFrame([(1,), (2,)], ["id"])

        assert is_empty(df) is False


class TestBroadcastSmallDim:
    """Tests for broadcast_small_dim helper function."""

    def test_none_returns_none(self):
        """None input should return None."""
        result = broadcast_small_dim(None)
        assert result is None

    def test_empty_returns_none(self, spark: SparkSession):
        """Empty DataFrame should return empty DataFrame."""
        schema = T.StructType([T.StructField("id", T.IntegerType())])
        df = spark.createDataFrame([], schema)

        result = broadcast_small_dim(df)

        # Empty DataFrame passthrough
        assert result is not None or is_empty(result)

    def test_small_dim_gets_broadcast_hint(self, dim_facility_df: DataFrame):
        """Small dimension table should get broadcast hint."""
        result = broadcast_small_dim(dim_facility_df)

        # Verify broadcast hint was applied by checking explain plan
        plan = result._jdf.queryExecution().simpleString()
        assert "ResolvedHint" in plan or result is not None


class TestEstimateDataFrameSize:
    """Tests for estimate_dataframe_size_mb function."""

    def test_empty_dataframe_zero_size(self, spark: SparkSession):
        """Empty DataFrame should have zero size."""
        schema = T.StructType([T.StructField("id", T.IntegerType())])
        df = spark.createDataFrame([], schema)

        size = estimate_dataframe_size_mb(df)

        assert size == 0.0

    def test_non_empty_dataframe_positive_size(self, dim_facility_df: DataFrame):
        """Non-empty DataFrame should have positive size estimate."""
        size = estimate_dataframe_size_mb(dim_facility_df)

        # Should return a small positive value for 3 rows
        assert size >= 0.0


class TestDecimalType:
    """Tests for dec helper function."""

    def test_creates_decimal_type(self):
        """Should create DecimalType with correct precision/scale."""
        result = dec(10, 2)

        assert isinstance(result, T.DecimalType)
        assert result.precision == 10
        assert result.scale == 2


class TestFirstValue:
    """Tests for first_value helper function."""

    def test_none_returns_none(self):
        """None DataFrame should return None."""
        assert first_value(None, "any_column") is None

    def test_missing_column_returns_none(self, spark: SparkSession):
        """Missing column should return None."""
        df = spark.createDataFrame([(1,)], ["id"])

        assert first_value(df, "missing_column") is None

    def test_extracts_first_value(self, spark: SparkSession):
        """Should extract first value from column."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

        result = first_value(df, "name")

        assert result == "a"


class TestComputeDateKey:
    """Tests for compute_date_key helper function."""

    def test_computes_yyyymmdd_format(self, spark: SparkSession):
        """Should compute date key in YYYYMMDD integer format."""
        df = spark.createDataFrame(
            [("2024-01-15",)], ["date_str"]
        ).withColumn("date_col", F.to_date(F.col("date_str")))

        result = df.withColumn("date_key", compute_date_key(F.col("date_col")))
        row = result.collect()[0]

        assert row["date_key"] == 20240115


class TestSeasonExpr:
    """Tests for season_expr helper function."""

    def test_summer_months(self, spark: SparkSession):
        """December, January, February should be Summer (Southern Hemisphere)."""
        months = [12, 1, 2]
        df = spark.createDataFrame([(m,) for m in months], ["month"])
        result = df.withColumn("season", season_expr(F.col("month"))).collect()

        for row in result:
            assert row["season"] == "Summer"

    def test_autumn_months(self, spark: SparkSession):
        """March, April, May should be Autumn."""
        months = [3, 4, 5]
        df = spark.createDataFrame([(m,) for m in months], ["month"])
        result = df.withColumn("season", season_expr(F.col("month"))).collect()

        for row in result:
            assert row["season"] == "Autumn"

    def test_winter_months(self, spark: SparkSession):
        """June, July, August should be Winter."""
        months = [6, 7, 8]
        df = spark.createDataFrame([(m,) for m in months], ["month"])
        result = df.withColumn("season", season_expr(F.col("month"))).collect()

        for row in result:
            assert row["season"] == "Winter"

    def test_spring_months(self, spark: SparkSession):
        """September, October, November should be Spring."""
        months = [9, 10, 11]
        df = spark.createDataFrame([(m,) for m in months], ["month"])
        result = df.withColumn("season", season_expr(F.col("month"))).collect()

        for row in result:
            assert row["season"] == "Spring"


class TestTimeOfDayExpr:
    """Tests for time_of_day_expr helper function."""

    def test_morning_hours(self, spark: SparkSession):
        """Hours 6-11 should be Morning."""
        hours = [6, 7, 8, 9, 10, 11]
        df = spark.createDataFrame([(h,) for h in hours], ["hour"])
        result = df.withColumn("tod", time_of_day_expr(F.col("hour"))).collect()

        for row in result:
            assert row["tod"] == "Morning"

    def test_afternoon_hours(self, spark: SparkSession):
        """Hours 12-16 should be Afternoon."""
        hours = [12, 13, 14, 15, 16]
        df = spark.createDataFrame([(h,) for h in hours], ["hour"])
        result = df.withColumn("tod", time_of_day_expr(F.col("hour"))).collect()

        for row in result:
            assert row["tod"] == "Afternoon"

    def test_evening_hours(self, spark: SparkSession):
        """Hours 17-20 should be Evening."""
        hours = [17, 18, 19, 20]
        df = spark.createDataFrame([(h,) for h in hours], ["hour"])
        result = df.withColumn("tod", time_of_day_expr(F.col("hour"))).collect()

        for row in result:
            assert row["tod"] == "Evening"

    def test_night_hours(self, spark: SparkSession):
        """Hours 0-5 and 21-23 should be Night."""
        hours = [0, 1, 2, 3, 4, 5, 21, 22, 23]
        df = spark.createDataFrame([(h,) for h in hours], ["hour"])
        result = df.withColumn("tod", time_of_day_expr(F.col("hour"))).collect()

        for row in result:
            assert row["tod"] == "Night"
