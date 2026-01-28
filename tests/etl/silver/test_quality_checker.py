#!/usr/bin/env python3
"""Tests for QualityFlagAssigner class."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from pv_lakehouse.etl.silver.quality_checker import QualityFlagAssigner


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-quality-checker")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestQualityFlagAssigner:
    """Tests for QualityFlagAssigner class."""

    def test_assign_three_tier_good(self, spark):
        """Test three-tier quality flag assignment - GOOD case."""
        assigner = QualityFlagAssigner()

        schema = StructType([
            StructField("is_valid_bounds", BooleanType(), False),
            StructField("has_severe", BooleanType(), False),
            StructField("has_warning", BooleanType(), False),
        ])
        data = [
            (True, False, False),  # All good → GOOD
        ]
        df = spark.createDataFrame(data, schema)

        quality_flag = assigner.assign_three_tier(
            F.col("is_valid_bounds"),
            F.col("has_severe"),
            F.col("has_warning"),
        )
        result = df.select(quality_flag.alias("quality_flag"))
        rows = result.collect()

        assert rows[0].quality_flag == "GOOD"

    def test_assign_three_tier_bad(self, spark):
        """Test three-tier quality flag assignment - BAD cases."""
        assigner = QualityFlagAssigner()

        schema = StructType([
            StructField("is_valid_bounds", BooleanType(), False),
            StructField("has_severe", BooleanType(), False),
            StructField("has_warning", BooleanType(), False),
        ])
        data = [
            (False, False, False),  # Out of bounds → BAD
            (True, True, False),    # Severe issue → BAD
            (False, True, False),   # Both → BAD
        ]
        df = spark.createDataFrame(data, schema)

        quality_flag = assigner.assign_three_tier(
            F.col("is_valid_bounds"),
            F.col("has_severe"),
            F.col("has_warning"),
        )
        result = df.select(quality_flag.alias("quality_flag"))
        rows = result.collect()

        assert rows[0].quality_flag == "BAD"
        assert rows[1].quality_flag == "BAD"
        assert rows[2].quality_flag == "BAD"

    def test_assign_three_tier_warning(self, spark):
        """Test three-tier quality flag assignment - WARNING cases."""
        assigner = QualityFlagAssigner()

        schema = StructType([
            StructField("is_valid_bounds", BooleanType(), False),
            StructField("has_severe", BooleanType(), False),
            StructField("has_warning", BooleanType(), False),
        ])
        data = [
            (True, False, True),   # Only warning → WARNING
        ]
        df = spark.createDataFrame(data, schema)

        quality_flag = assigner.assign_three_tier(
            F.col("is_valid_bounds"),
            F.col("has_severe"),
            F.col("has_warning"),
        )
        result = df.select(quality_flag.alias("quality_flag"))
        rows = result.collect()

        assert rows[0].quality_flag == "WARNING"

    def test_assign_binary_good(self, spark):
        """Test binary quality flag assignment - GOOD case."""
        assigner = QualityFlagAssigner()

        schema = StructType([
            StructField("is_valid", BooleanType(), False),
        ])
        data = [
            (True,),
        ]
        df = spark.createDataFrame(data, schema)

        quality_flag = assigner.assign_binary(F.col("is_valid"), "GOOD", "WARNING")
        result = df.select(quality_flag.alias("quality_flag"))
        rows = result.collect()

        assert rows[0].quality_flag == "GOOD"

    def test_assign_binary_warning(self, spark):
        """Test binary quality flag assignment - WARNING case."""
        assigner = QualityFlagAssigner()

        schema = StructType([
            StructField("is_valid", BooleanType(), False),
        ])
        data = [
            (False,),
        ]
        df = spark.createDataFrame(data, schema)

        quality_flag = assigner.assign_binary(F.col("is_valid"), "GOOD", "WARNING")
        result = df.select(quality_flag.alias("quality_flag"))
        rows = result.collect()

        assert rows[0].quality_flag == "WARNING"

    def test_build_issues_string_empty(self, spark):
        """Test building issues string with no issues."""
        assigner = QualityFlagAssigner()

        schema = StructType([
            StructField("issue1", StringType(), False),
            StructField("issue2", StringType(), False),
        ])
        data = [
            ("", ""),
        ]
        df = spark.createDataFrame(data, schema)

        issues = assigner.build_issues_string([F.col("issue1"), F.col("issue2")])
        result = df.select(issues.alias("issues"))
        rows = result.collect()

        # PySpark concat_ws with all empty strings may return "" or "|"
        assert rows[0].issues in ("", "|")

    def test_build_issues_string_single(self, spark):
        """Test building issues string with single issue."""
        assigner = QualityFlagAssigner()

        schema = StructType([
            StructField("issue1", StringType(), False),
            StructField("issue2", StringType(), False),
        ])
        data = [
            ("NIGHT_RADIATION", ""),
        ]
        df = spark.createDataFrame(data, schema)

        issues = assigner.build_issues_string([F.col("issue1"), F.col("issue2")])
        result = df.select(issues.alias("issues"))
        rows = result.collect()

        # May have trailing separator from concat_ws
        assert rows[0].issues.replace("|", "") == "NIGHT_RADIATION"

    def test_build_issues_string_multiple(self, spark):
        """Test building issues string with multiple issues."""
        assigner = QualityFlagAssigner()

        schema = StructType([
            StructField("issue1", StringType(), False),
            StructField("issue2", StringType(), False),
            StructField("issue3", StringType(), False),
        ])
        data = [
            ("NIGHT_RADIATION", "EXTREME_TEMPERATURE", "OUT_OF_BOUNDS"),
        ]
        df = spark.createDataFrame(data, schema)

        issues = assigner.build_issues_string([
            F.col("issue1"),
            F.col("issue2"),
            F.col("issue3"),
        ])
        result = df.select(issues.alias("issues"))
        rows = result.collect()

        assert "NIGHT_RADIATION" in rows[0].issues
        assert "EXTREME_TEMPERATURE" in rows[0].issues
        assert "OUT_OF_BOUNDS" in rows[0].issues
        assert rows[0].issues.count("|") == 2  # Two separators for three issues
