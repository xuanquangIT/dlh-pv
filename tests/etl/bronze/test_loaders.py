#!/usr/bin/env python3
"""Unit tests for Bronze loader implementations."""

from __future__ import annotations

import datetime as dt

import pytest

from pv_lakehouse.etl.bronze.base import BronzeLoadOptions
from pv_lakehouse.etl.bronze.load_facilities import FacilitiesLoader
from pv_lakehouse.etl.bronze.load_facility_air_quality import AirQualityLoader
from pv_lakehouse.etl.bronze.load_facility_energy import EnergyLoader
from pv_lakehouse.etl.bronze.load_facility_weather import WeatherLoader


class TestFacilitiesLoader:
    """Test FacilitiesLoader implementation."""

    def test_class_attributes(self):
        """Test FacilitiesLoader has correct class attributes."""
        assert FacilitiesLoader.iceberg_table == "lh.bronze.raw_facilities"
        assert FacilitiesLoader.timestamp_column == "ingest_timestamp"
        assert FacilitiesLoader.merge_keys == ("facility_code",)

    def test_initialization(self):
        """Test FacilitiesLoader initializes correctly."""
        loader = FacilitiesLoader()
        assert loader.options is not None
        assert loader.iceberg_table == "lh.bronze.raw_facilities"

    def test_transform_adds_ingest_date(self):
        """Test transform adds ingest_date partition column."""
        loader = FacilitiesLoader()
        spark = loader.spark
        df = spark.createDataFrame([{"facility_code": "TEST1"}])
        result = loader.transform(df)
        assert "ingest_date" in result.columns


class TestEnergyLoader:
    """Test EnergyLoader implementation."""

    def test_class_attributes(self):
        """Test EnergyLoader has correct class attributes."""
        assert EnergyLoader.iceberg_table == "lh.bronze.raw_facility_energy"
        assert EnergyLoader.timestamp_column == "interval_ts"
        assert EnergyLoader.merge_keys == ("facility_code", "interval_ts", "metric")

    def test_initialization_backfill_mode(self):
        """Test initialization in backfill mode."""
        options = BronzeLoadOptions(mode="backfill")
        loader = EnergyLoader(options)
        assert loader.options.mode == "backfill"

    def test_date_range_initialization_incremental(self):
        """Test date range initialization for incremental mode."""
        options = BronzeLoadOptions(
            mode="incremental",
            start=dt.datetime(2025, 1, 1),
            end=dt.datetime(2025, 1, 31),
        )
        loader = EnergyLoader(options)
        assert loader.options.start == dt.datetime(2025, 1, 1)
        assert loader.options.end == dt.datetime(2025, 1, 31)

    def test_transform_adds_timestamp_columns(self):
        """Test transform adds interval_ts and interval_date columns."""
        loader = EnergyLoader()
        spark = loader.spark
        df = spark.createDataFrame([{
            "facility_code": "TEST1",
            "interval_start": "2025-01-01 00:00:00",
            "metric": "energy",
            "value": 100.0,
        }])
        result = loader.transform(df)
        assert "interval_ts" in result.columns
        assert "interval_date" in result.columns


class TestWeatherLoader:
    """Test WeatherLoader implementation."""

    def test_class_attributes(self):
        """Test WeatherLoader has correct class attributes."""
        assert WeatherLoader.iceberg_table == "lh.bronze.raw_facility_weather"
        assert WeatherLoader.timestamp_column == "weather_timestamp"
        assert WeatherLoader.merge_keys == ("facility_code", "weather_timestamp")

    def test_initialization_sets_date_range(self):
        """Test initialization sets default date range."""
        loader = WeatherLoader()
        assert loader.options.start is not None
        assert loader.options.end is not None

    def test_date_range_initialization_custom_dates(self):
        """Test date range initialization with custom dates."""
        options = BronzeLoadOptions(
            mode="backfill",
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 12, 31),
        )
        loader = WeatherLoader(options)
        assert loader.options.start == dt.date(2025, 1, 1)
        assert loader.options.end == dt.date(2025, 12, 31)

    def test_transform_adds_timestamp_columns(self):
        """Test transform adds weather_timestamp and weather_date columns."""
        loader = WeatherLoader()
        spark = loader.spark
        df = spark.createDataFrame([{
            "facility_code": "TEST1",
            "date": "2025-01-01 12:00:00",
            "temperature": 25.5,
        }])
        result = loader.transform(df)
        assert "weather_timestamp" in result.columns
        assert "weather_date" in result.columns

    def test_transform_filters_null_timestamps(self):
        """Test transform filters out null timestamps."""
        loader = WeatherLoader()
        spark = loader.spark
        df = spark.createDataFrame([
            {"facility_code": "TEST1", "date": "2025-01-01 12:00:00"},
            {"facility_code": "TEST2", "date": None},
        ])
        result = loader.transform(df)
        assert result.count() == 1  # Only non-null timestamp row


class TestAirQualityLoader:
    """Test AirQualityLoader implementation."""

    def test_class_attributes(self):
        """Test AirQualityLoader has correct class attributes."""
        assert AirQualityLoader.iceberg_table == "lh.bronze.raw_facility_air_quality"
        assert AirQualityLoader.timestamp_column == "air_timestamp"
        assert AirQualityLoader.merge_keys == ("facility_code", "air_timestamp")

    def test_initialization_sets_date_range(self):
        """Test initialization sets default date range."""
        loader = AirQualityLoader()
        assert loader.options.start is not None
        assert loader.options.end is not None

    def test_date_range_initialization_incremental(self):
        """Test date range initialization for incremental mode."""
        options = BronzeLoadOptions(mode="incremental")
        loader = AirQualityLoader(options)
        # Should set default dates for incremental mode
        assert loader.options.start is not None

    def test_transform_adds_timestamp_columns(self):
        """Test transform adds air_timestamp and air_date columns."""
        loader = AirQualityLoader()
        spark = loader.spark
        df = spark.createDataFrame([{
            "facility_code": "TEST1",
            "date": "2025-01-01 12:00:00",
            "pm25": 10.5,
        }])
        result = loader.transform(df)
        assert "air_timestamp" in result.columns
        assert "air_date" in result.columns

    def test_transform_filters_null_timestamps(self):
        """Test transform filters out null timestamps."""
        loader = AirQualityLoader()
        spark = loader.spark
        df = spark.createDataFrame([
            {"facility_code": "TEST1", "date": "2025-01-01 12:00:00"},
            {"facility_code": "TEST2", "date": None},
        ])
        result = loader.transform(df)
        assert result.count() == 1  # Only non-null timestamp row


class TestLoaderLineCount:
    """Test that all loaders meet the < 100 lines requirement."""

    def test_facilities_loader_line_count(self):
        """Test FacilitiesLoader is < 100 lines."""
        import inspect
        source = inspect.getsource(FacilitiesLoader)
        lines = source.split("\n")
        # Count only class definition lines (not imports)
        assert len(lines) < 100, f"FacilitiesLoader has {len(lines)} lines"

    def test_energy_loader_line_count(self):
        """Test EnergyLoader is < 100 lines."""
        import inspect
        source = inspect.getsource(EnergyLoader)
        lines = source.split("\n")
        assert len(lines) < 100, f"EnergyLoader has {len(lines)} lines"

    def test_weather_loader_line_count(self):
        """Test WeatherLoader is < 100 lines."""
        import inspect
        source = inspect.getsource(WeatherLoader)
        lines = source.split("\n")
        assert len(lines) < 100, f"WeatherLoader has {len(lines)} lines"

    def test_air_quality_loader_line_count(self):
        """Test AirQualityLoader is < 100 lines."""
        import inspect
        source = inspect.getsource(AirQualityLoader)
        lines = source.split("\n")
        assert len(lines) < 100, f"AirQualityLoader has {len(lines)} lines"
