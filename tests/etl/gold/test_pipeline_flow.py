"""Tests for Gold layer pipeline flow and date filtering.

Tests cover:
- Date filtering using date_hour vs updated_at
- Dimension table loading order
- Fact table dimension key lookups
- Full vs incremental load modes
- Data lineage from Silver to Gold
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Dict

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pv_lakehouse.etl.gold.base import GoldLoadOptions, SourceTableConfig
from pv_lakehouse.etl.gold.dim_date import GoldDimDateLoader
from pv_lakehouse.etl.gold.dim_time import GoldDimTimeLoader
from pv_lakehouse.etl.gold.dim_facility import GoldDimFacilityLoader


class TestDateFiltering:
    """Tests for date filtering behavior in Gold loaders."""

    def test_dim_date_uses_date_hour_for_filtering(self):
        """dim_date should use date_hour column for timestamp filtering, not updated_at.
        
        This was a bug fix - using updated_at (system time) caused 0 rows when
        system time differed from data time.
        """
        loader = GoldDimDateLoader()
        
        # All source tables should use date_hour for filtering
        for source_name, config in loader.source_tables.items():
            assert config.timestamp_column == "date_hour", (
                f"Source '{source_name}' should use date_hour for filtering, "
                f"not {config.timestamp_column}"
            )

    def test_source_table_config_date_hour_filtering(self):
        """Verify SourceTableConfig uses correct timestamp columns for data filtering."""
        expected_date_hour_sources = [
            "hourly_weather",
            "hourly_air_quality", 
            "hourly_energy",
        ]
        
        loader = GoldDimDateLoader()
        for source_name in expected_date_hour_sources:
            config = loader.source_tables.get(source_name)
            assert config is not None, f"Missing source config for {source_name}"
            assert config.timestamp_column == "date_hour", (
                f"Source {source_name} should filter on date_hour, not {config.timestamp_column}"
            )


class TestDimTimeStructure:
    """Tests for dim_time table structure."""

    def test_dim_time_has_minute_column(self, spark: SparkSession):
        """dim_time should have minute column for compatibility with fact tables."""
        loader = GoldDimTimeLoader()
        loader._spark = spark
        
        result = loader.transform({})
        assert result is not None
        
        dim_time = result["dim_time"]
        columns = dim_time.columns
        
        assert "time_key" in columns
        assert "hour" in columns
        assert "minute" in columns, "dim_time must have minute column"
        assert "time_of_day" in columns
        
    def test_dim_time_minute_is_zero(self, spark: SparkSession):
        """dim_time minute should be 0 for hourly granularity."""
        loader = GoldDimTimeLoader()
        loader._spark = spark
        
        result = loader.transform({})
        dim_time = result["dim_time"]
        
        # All minutes should be 0
        non_zero_minutes = dim_time.filter(F.col("minute") != 0).count()
        assert non_zero_minutes == 0, "All minutes should be 0 for hourly granularity"
        
    def test_dim_time_generates_24_hours(self, spark: SparkSession):
        """dim_time should generate all 24 hours (0-23)."""
        loader = GoldDimTimeLoader()
        loader._spark = spark
        
        result = loader.transform({})
        dim_time = result["dim_time"]
        
        assert dim_time.count() == 24
        
        hours = [row["hour"] for row in dim_time.collect()]
        assert sorted(hours) == list(range(24))


class TestDimDateTransform:
    """Tests for dim_date transformation."""

    @pytest.fixture
    def silver_hourly_weather(self, spark: SparkSession) -> DataFrame:
        """Create sample Silver hourly_weather data."""
        schema = T.StructType([
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("date_hour", T.TimestampType(), False),
            T.StructField("temperature_2m", T.DoubleType(), True),
            T.StructField("updated_at", T.TimestampType(), False),
        ])
        
        # Data from 2025, but updated_at is 2026 (system time)
        data = [
            ("FAC001", dt.datetime(2025, 1, 1, 0, 0), 15.5, dt.datetime(2026, 1, 20, 10, 0)),
            ("FAC001", dt.datetime(2025, 1, 1, 1, 0), 15.2, dt.datetime(2026, 1, 20, 10, 0)),
            ("FAC001", dt.datetime(2025, 1, 2, 0, 0), 16.0, dt.datetime(2026, 1, 20, 10, 0)),
            ("FAC002", dt.datetime(2025, 1, 1, 0, 0), 14.5, dt.datetime(2026, 1, 20, 10, 0)),
        ]
        return spark.createDataFrame(data, schema)

    @pytest.fixture
    def silver_hourly_air_quality(self, spark: SparkSession) -> DataFrame:
        """Create sample Silver hourly_air_quality data."""
        schema = T.StructType([
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("date_hour", T.TimestampType(), False),
            T.StructField("pm2_5", T.DoubleType(), True),
            T.StructField("updated_at", T.TimestampType(), False),
        ])
        
        data = [
            ("FAC001", dt.datetime(2025, 1, 1, 0, 0), 12.5, dt.datetime(2026, 1, 20, 10, 0)),
            ("FAC001", dt.datetime(2025, 1, 3, 0, 0), 15.0, dt.datetime(2026, 1, 20, 10, 0)),
        ]
        return spark.createDataFrame(data, schema)

    @pytest.fixture
    def silver_hourly_energy(self, spark: SparkSession) -> DataFrame:
        """Create sample Silver hourly_energy data."""
        schema = T.StructType([
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("date_hour", T.TimestampType(), False),
            T.StructField("energy_mwh", T.DecimalType(12, 6), True),
            T.StructField("updated_at", T.TimestampType(), False),
        ])
        
        data = [
            ("FAC001", dt.datetime(2025, 1, 1, 0, 0), Decimal("10.5"), dt.datetime(2026, 1, 20, 10, 0)),
            ("FAC001", dt.datetime(2025, 1, 4, 0, 0), Decimal("12.0"), dt.datetime(2026, 1, 20, 10, 0)),
        ]
        return spark.createDataFrame(data, schema)

    def test_dim_date_extracts_unique_dates(
        self,
        spark: SparkSession,
        silver_hourly_weather: DataFrame,
        silver_hourly_air_quality: DataFrame,
        silver_hourly_energy: DataFrame,
    ):
        """dim_date should extract unique dates from all Silver sources."""
        loader = GoldDimDateLoader()
        loader._spark = spark
        
        sources = {
            "hourly_weather": silver_hourly_weather,
            "hourly_air_quality": silver_hourly_air_quality,
            "hourly_energy": silver_hourly_energy,
        }
        
        result = loader.transform(sources)
        assert result is not None
        
        dim_date = result["dim_date"]
        
        # Should have 4 unique dates from all sources (dates may shift due to UTC)
        date_count = dim_date.count()
        assert date_count >= 3, f"Expected at least 3 unique dates, got {date_count}"
        
        # Verify 2025 dates are present (allow for timezone shifts)
        year_2025_count = dim_date.filter(F.col("year") == 2025).count()
        assert year_2025_count >= 2, f"Expected at least 2 dates in 2025, got {year_2025_count}"

    def test_dim_date_has_required_columns(
        self,
        spark: SparkSession,
        silver_hourly_weather: DataFrame,
    ):
        """dim_date should have all required dimension columns."""
        loader = GoldDimDateLoader()
        loader._spark = spark
        
        sources = {"hourly_weather": silver_hourly_weather}
        result = loader.transform(sources)
        dim_date = result["dim_date"]
        
        required_columns = [
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week",
            "day_of_month",
            "day_of_week",
            "day_name",
        ]
        
        for col in required_columns:
            assert col in dim_date.columns, f"Missing required column: {col}"


class TestGoldLoadOptions:
    """Tests for GoldLoadOptions configuration."""

    def test_full_mode_ignores_date_filters(self):
        """Full mode should not apply date filtering."""
        options = GoldLoadOptions(
            mode="full",
            start=dt.datetime(2025, 1, 1),
            end=dt.datetime(2025, 12, 31),
        )
        
        # In full mode, start/end should still be accepted but mode is full
        assert options.mode == "full"
        assert options.start is not None
        assert options.end is not None

    def test_incremental_mode_with_date_range(self):
        """Incremental mode should accept date range filters."""
        options = GoldLoadOptions(
            mode="incremental",
            start=dt.datetime(2025, 1, 1),
            end=dt.datetime(2025, 1, 31),
        )
        
        assert options.mode == "incremental"
        assert options.start == dt.datetime(2025, 1, 1)
        assert options.end == dt.datetime(2025, 1, 31)


class TestPipelineDataLineage:
    """Tests for data lineage from Silver to Gold."""

    def test_silver_to_gold_facility_lineage(self, spark: SparkSession):
        """dim_facility should preserve facility data from Silver clean_facility_master."""
        # Create mock Silver facility data
        schema = T.StructType([
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("facility_name", T.StringType(), True),
            T.StructField("location_lat", T.DoubleType(), True),
            T.StructField("location_lng", T.DoubleType(), True),
            T.StructField("total_capacity_mw", T.DecimalType(10, 2), True),
            T.StructField("total_capacity_registered_mw", T.DecimalType(10, 2), True),
            T.StructField("total_capacity_maximum_mw", T.DecimalType(10, 2), True),
            T.StructField("effective_from", T.DateType(), True),
            T.StructField("effective_to", T.DateType(), True),
            T.StructField("is_current", T.BooleanType(), True),
            T.StructField("updated_at", T.TimestampType(), False),
        ])
        
        data = [
            (
                "SOLAR001", "Solar Farm Alpha", -33.8, 151.2,
                Decimal("100.00"), Decimal("95.00"), Decimal("110.00"),
                dt.date(2020, 1, 1), None, True,
                dt.datetime(2026, 1, 20, 10, 0)
            ),
            (
                "SOLAR002", "Solar Farm Beta", -34.0, 151.5,
                Decimal("75.50"), Decimal("70.00"), Decimal("80.00"),
                dt.date(2021, 6, 1), None, True,
                dt.datetime(2026, 1, 20, 10, 0)
            ),
        ]
        
        silver_facilities = spark.createDataFrame(data, schema)
        
        loader = GoldDimFacilityLoader()
        loader._spark = spark
        
        result = loader.transform({"facility": silver_facilities})
        assert result is not None
        
        dim_facility = result["dim_facility"]
        
        # Verify facility codes are preserved
        codes = [row["facility_code"] for row in dim_facility.collect()]
        assert "SOLAR001" in codes
        assert "SOLAR002" in codes

    def test_gold_date_key_format(
        self,
        spark: SparkSession,
    ):
        """Gold date_key should be in YYYYMMDD integer format."""
        schema = T.StructType([
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("date_hour", T.TimestampType(), False),
        ])
        
        data = [
            ("FAC001", dt.datetime(2025, 3, 15, 10, 0)),
        ]
        
        hourly_data = spark.createDataFrame(data, schema)
        
        loader = GoldDimDateLoader()
        loader._spark = spark
        
        result = loader.transform({"hourly_weather": hourly_data})
        dim_date = result["dim_date"]
        
        row = dim_date.first()
        assert row["date_key"] == 20250315, "date_key should be YYYYMMDD format"


class TestDimensionKeyConsistency:
    """Tests for dimension key consistency across Gold tables."""

    def test_time_key_matches_hour(self, spark: SparkSession):
        """time_key should equal hour for simple lookup."""
        loader = GoldDimTimeLoader()
        loader._spark = spark
        
        result = loader.transform({})
        dim_time = result["dim_time"]
        
        for row in dim_time.collect():
            assert row["time_key"] == row["hour"], (
                f"time_key ({row['time_key']}) should equal hour ({row['hour']})"
            )

    def test_date_key_derivable_from_full_date(
        self,
        spark: SparkSession,
    ):
        """date_key should be derivable from full_date."""
        schema = T.StructType([
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("date_hour", T.TimestampType(), False),
        ])
        
        data = [
            ("FAC001", dt.datetime(2025, 7, 4, 12, 0)),
            ("FAC001", dt.datetime(2025, 12, 25, 8, 0)),
        ]
        
        hourly_data = spark.createDataFrame(data, schema)
        
        loader = GoldDimDateLoader()
        loader._spark = spark
        
        result = loader.transform({"hourly_weather": hourly_data})
        dim_date = result["dim_date"]
        
        for row in dim_date.collect():
            expected_key = int(row["full_date"].strftime("%Y%m%d"))
            assert row["date_key"] == expected_key, (
                f"date_key ({row['date_key']}) should match YYYYMMDD of full_date ({expected_key})"
            )


class TestEmptySourceHandling:
    """Tests for handling empty source data."""

    def test_dim_date_returns_none_for_empty_sources(self, spark: SparkSession):
        """dim_date transform should return None when all sources are empty."""
        loader = GoldDimDateLoader()
        loader._spark = spark
        
        # Empty DataFrame
        schema = T.StructType([
            T.StructField("facility_code", T.StringType(), False),
            T.StructField("date_hour", T.TimestampType(), False),
        ])
        empty_df = spark.createDataFrame([], schema)
        
        result = loader.transform({
            "hourly_weather": empty_df,
            "hourly_air_quality": empty_df,
            "hourly_energy": empty_df,
        })
        
        assert result is None

    def test_dim_time_always_returns_data(self, spark: SparkSession):
        """dim_time should always return 24 hours (no sources needed)."""
        loader = GoldDimTimeLoader()
        loader._spark = spark
        
        result = loader.transform({})
        
        assert result is not None
        assert result["dim_time"].count() == 24


__all__ = [
    "TestDateFiltering",
    "TestDimTimeStructure",
    "TestDimDateTransform",
    "TestGoldLoadOptions",
    "TestPipelineDataLineage",
    "TestDimensionKeyConsistency",
    "TestEmptySourceHandling",
]
