#!/usr/bin/env python3
"""Tests for NumericBoundsValidator and LogicValidator classes."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from pv_lakehouse.etl.silver.validators import LogicValidator, NumericBoundsValidator


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-validators")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestNumericBoundsValidator:
    """Tests for NumericBoundsValidator class."""

    def test_validate_all_within_bounds(self, spark):
        """Test validation when all values are within bounds."""
        bounds_config = {
            "temperature_2m": {"min": -10.0, "max": 50.0},
            "wind_speed_10m": {"min": 0.0, "max": 50.0},
        }
        validator = NumericBoundsValidator(bounds_config)

        schema = StructType([
            StructField("temperature_2m", DoubleType(), True),
            StructField("wind_speed_10m", DoubleType(), True),
        ])
        data = [
            (25.5, 10.2),
            (30.0, 15.5),
            (-5.0, 0.0),
        ]
        df = spark.createDataFrame(data, schema)

        is_valid, issues = validator.validate_all(df.columns)
        result = df.select(is_valid.alias("is_valid"), issues.alias("issues"))
        rows = result.collect()

        assert all(row.is_valid for row in rows)
        # PySpark concat_ws may return empty string or "|" when all parts are empty
        assert all(row.issues in ("", "|") for row in rows)

    def test_validate_all_out_of_bounds(self, spark):
        """Test validation when values are out of bounds."""
        bounds_config = {
            "temperature_2m": {"min": -10.0, "max": 50.0},
            "wind_speed_10m": {"min": 0.0, "max": 50.0},
        }
        validator = NumericBoundsValidator(bounds_config)

        schema = StructType([
            StructField("temperature_2m", DoubleType(), True),
            StructField("wind_speed_10m", DoubleType(), True),
        ])
        data = [
            (60.0, 10.2),  # temperature out of bounds
            (25.0, -5.0),  # wind_speed out of bounds
            (-15.0, 60.0),  # both out of bounds
        ]
        df = spark.createDataFrame(data, schema)

        is_valid, issues = validator.validate_all(df.columns)
        result = df.select(is_valid.alias("is_valid"), issues.alias("issues"))
        rows = result.collect()

        assert not rows[0].is_valid
        assert "temperature_2m" in rows[0].issues

        assert not rows[1].is_valid
        assert "wind_speed_10m" in rows[1].issues

        assert not rows[2].is_valid
        assert "temperature_2m" in rows[2].issues
        assert "wind_speed_10m" in rows[2].issues

    def test_validate_all_null_values(self, spark):
        """Test validation handles null values correctly."""
        bounds_config = {
            "temperature_2m": {"min": -10.0, "max": 50.0},
        }
        validator = NumericBoundsValidator(bounds_config)

        schema = StructType([
            StructField("temperature_2m", DoubleType(), True),
        ])
        data = [
            (None,),
            (25.0,),
        ]
        df = spark.createDataFrame(data, schema)

        is_valid, issues = validator.validate_all(df.columns)
        result = df.select(is_valid.alias("is_valid"), issues.alias("issues"))
        rows = result.collect()

        # Null values should pass validation (not considered out of bounds)
        assert rows[0].is_valid
        assert rows[0].issues == ""

        assert rows[1].is_valid
        assert rows[1].issues == ""


class TestLogicValidator:
    """Tests for LogicValidator class."""

    def test_check_night_radiation(self, spark):
        """Test night radiation anomaly detection."""
        import datetime as dt
        validator = LogicValidator()

        schema = StructType([
            StructField("timestamp_local", TimestampType(), True),
            StructField("shortwave_radiation", DoubleType(), True),
        ])
        data = [
            (dt.datetime(2024, 1, 15, 1, 0), 150.0),  # Night with high radiation (anomaly)
            (dt.datetime(2024, 1, 15, 12, 0), 800.0),  # Day with high radiation (normal)
            (dt.datetime(2024, 1, 15, 23, 0), 5.0),    # Night with low radiation (normal)
        ]
        df = spark.createDataFrame(data, schema)

        has_issue, issue_label = validator.check_night_radiation(
            F.col("timestamp_local"),
            F.col("shortwave_radiation"),
            threshold=100.0,
        )
        result = df.select(has_issue.alias("has_issue"), issue_label.alias("issue"))
        rows = result.collect()

        assert rows[0].has_issue  # Night anomaly
        assert rows[0].issue == "NIGHT_RADIATION"

        assert not rows[1].has_issue  # Day is OK
        assert rows[1].issue == ""

        assert not rows[2].has_issue  # Night low radiation is OK
        assert rows[2].issue == ""

    def test_check_radiation_consistency(self, spark):
        """Test radiation consistency validation."""
        validator = LogicValidator()

        schema = StructType([
            StructField("direct_radiation", DoubleType(), True),
            StructField("diffuse_radiation", DoubleType(), True),
            StructField("shortwave_radiation", DoubleType(), True),
        ])
        data = [
            (500.0, 300.0, 800.0),   # Consistent
            (500.0, 300.0, 600.0),   # Inconsistent (sum > total * 1.05)
            (100.0, 50.0, 160.0),    # Consistent (within tolerance)
        ]
        df = spark.createDataFrame(data, schema)

        has_issue, issue_label = validator.check_radiation_consistency(
            F.col("direct_radiation"),
            F.col("diffuse_radiation"),
            F.col("shortwave_radiation"),
            tolerance_factor=1.05,
        )
        result = df.select(has_issue.alias("has_issue"), issue_label.alias("issue"))
        rows = result.collect()

        assert not rows[0].has_issue
        assert rows[0].issue == ""

        assert rows[1].has_issue
        assert rows[1].issue == "RADIATION_INCONSISTENCY"

        assert not rows[2].has_issue
        assert rows[2].issue == ""

    def test_check_night_energy(self, spark):
        """Test night energy anomaly detection."""
        import datetime as dt
        validator = LogicValidator()

        schema = StructType([
            StructField("timestamp_local", TimestampType(), True),
            StructField("energy_mwh", DoubleType(), True),
        ])
        data = [
            (dt.datetime(2024, 1, 15, 2, 0), 5.0),   # Night with energy (anomaly)
            (dt.datetime(2024, 1, 15, 12, 0), 50.0),  # Day with energy (normal)
            (dt.datetime(2024, 1, 15, 23, 0), 0.5),   # Night low energy (normal)
        ]
        df = spark.createDataFrame(data, schema)

        has_issue, issue_label = validator.check_night_energy(
            F.col("timestamp_local"),
            F.col("energy_mwh"),
            threshold=1.0,
        )
        result = df.select(has_issue.alias("has_issue"), issue_label.alias("issue"))
        rows = result.collect()

        assert rows[0].has_issue
        assert rows[0].issue == "NIGHT_ENERGY"

        assert not rows[1].has_issue
        assert rows[1].issue == ""

        assert not rows[2].has_issue  # Below threshold
        assert rows[2].issue == ""

    def test_check_daytime_zero_energy(self, spark):
        """Test daytime zero energy detection."""
        import datetime as dt
        validator = LogicValidator()

        schema = StructType([
            StructField("timestamp_local", TimestampType(), True),
            StructField("energy_mwh", DoubleType(), True),
        ])
        data = [
            (dt.datetime(2024, 1, 15, 12, 0), 0.0),   # Day zero (anomaly)
            (dt.datetime(2024, 1, 15, 12, 0), 50.0),  # Day with energy (normal)
            (dt.datetime(2024, 1, 15, 23, 0), 0.0),   # Night zero (normal)
        ]
        df = spark.createDataFrame(data, schema)

        has_issue, issue_label = validator.check_daytime_zero_energy(
            F.col("timestamp_local"),
            F.col("energy_mwh"),
        )
        result = df.select(has_issue.alias("has_issue"), issue_label.alias("issue"))
        rows = result.collect()

        assert rows[0].has_issue
        assert rows[0].issue == "DAYTIME_ZERO_ENERGY"

        assert not rows[1].has_issue
        assert rows[1].issue == ""

        assert not rows[2].has_issue  # Night is OK
        assert rows[2].issue == ""

    def test_check_extreme_temperature(self, spark):
        """Test extreme temperature detection."""
        validator = LogicValidator()

        schema = StructType([
            StructField("temperature_2m", DoubleType(), True),
        ])
        data = [
            (-15.0,),  # Too cold (anomaly)
            (50.0,),   # Too hot (anomaly)
            (25.0,),   # Normal
        ]
        df = spark.createDataFrame(data, schema)

        has_issue, issue_label = validator.check_extreme_temperature(
            F.col("temperature_2m"),
            min_threshold=-10.0,
            max_threshold=45.0,
        )
        result = df.select(has_issue.alias("has_issue"), issue_label.alias("issue"))
        rows = result.collect()

        assert rows[0].has_issue
        assert rows[0].issue == "EXTREME_TEMPERATURE"

        assert rows[1].has_issue
        assert rows[1].issue == "EXTREME_TEMPERATURE"

        assert not rows[2].has_issue
        assert rows[2].issue == ""
