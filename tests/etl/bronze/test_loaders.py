#!/usr/bin/env python3
"""Unit tests for Bronze loader implementations."""

from __future__ import annotations

import datetime as dt
from typing import Generator

import pytest
from pyspark.sql import SparkSession

from pv_lakehouse.etl.bronze.base import BaseBronzeLoader, BronzeLoadOptions
from pv_lakehouse.etl.bronze.load_facilities import FacilitiesLoader
from pv_lakehouse.etl.bronze.load_facility_air_quality import AirQualityLoader
from pv_lakehouse.etl.bronze.load_facility_energy import EnergyLoader
from pv_lakehouse.etl.bronze.load_facility_weather import WeatherLoader


@pytest.fixture
def facilities_loader(shared_spark: SparkSession) -> Generator[FacilitiesLoader, None, None]:
    """Create FacilitiesLoader with shared SparkSession."""
    loader = FacilitiesLoader()
    loader._spark = shared_spark  # Inject shared session
    try:
        yield loader
    finally:
        # Don't close - shared session managed by shared_spark fixture
        loader._spark = None


@pytest.fixture
def energy_loader(shared_spark: SparkSession) -> Generator[EnergyLoader, None, None]:
    """Create EnergyLoader with shared SparkSession."""
    loader = EnergyLoader()
    loader._spark = shared_spark
    try:
        yield loader
    finally:
        loader._spark = None


@pytest.fixture
def weather_loader(shared_spark: SparkSession) -> Generator[WeatherLoader, None, None]:
    """Create WeatherLoader with shared SparkSession."""
    loader = WeatherLoader()
    loader._spark = shared_spark
    try:
        yield loader
    finally:
        loader._spark = None


@pytest.fixture
def air_quality_loader(shared_spark: SparkSession) -> Generator[AirQualityLoader, None, None]:
    """Create AirQualityLoader with shared SparkSession."""
    loader = AirQualityLoader()
    loader._spark = shared_spark
    try:
        yield loader
    finally:
        loader._spark = None


class TestFacilitiesLoader:
    """Test FacilitiesLoader implementation."""

    def test_class_attributes(self):
        """Test FacilitiesLoader has correct class attributes."""
        assert FacilitiesLoader.iceberg_table == "lh.bronze.raw_facilities"
        assert FacilitiesLoader.timestamp_column == "ingest_timestamp"
        assert FacilitiesLoader.merge_keys == ("facility_code",)

    def test_initialization(self, shared_spark: SparkSession):
        """Test FacilitiesLoader initializes correctly."""
        loader = FacilitiesLoader()
        loader._spark = shared_spark
        assert loader.options is not None
        assert loader.iceberg_table == "lh.bronze.raw_facilities"

    def test_transform_adds_ingest_date(self, facilities_loader: FacilitiesLoader):
        """Test transform adds ingest_date partition column."""
        spark = facilities_loader.spark
        df = spark.createDataFrame([{"facility_code": "TEST1"}])
        result = facilities_loader.transform(df)
        assert "ingest_date" in result.columns


class TestEnergyLoader:
    """Test EnergyLoader implementation."""

    def test_class_attributes(self):
        """Test EnergyLoader has correct class attributes."""
        assert EnergyLoader.iceberg_table == "lh.bronze.raw_facility_energy"
        assert EnergyLoader.timestamp_column == "interval_ts"
        assert EnergyLoader.merge_keys == ("facility_code", "interval_ts", "metric")

    def test_initialization_backfill_mode(self, shared_spark: SparkSession):
        """Test initialization in backfill mode."""
        options = BronzeLoadOptions(mode="backfill")
        loader = EnergyLoader(options)
        loader._spark = shared_spark
        assert loader.options.mode == "backfill"

    def test_date_range_initialization_incremental(self, shared_spark: SparkSession):
        """Test date range initialization for incremental mode."""
        options = BronzeLoadOptions(
            mode="incremental",
            start=dt.datetime(2025, 1, 1),
            end=dt.datetime(2025, 1, 31),
        )
        loader = EnergyLoader(options)
        loader._spark = shared_spark
        assert loader.options.start == dt.datetime(2025, 1, 1)
        assert loader.options.end == dt.datetime(2025, 1, 31)

    def test_transform_adds_timestamp_columns(self, energy_loader: EnergyLoader):
        """Test transform adds interval_ts and interval_date columns."""
        spark = energy_loader.spark
        df = spark.createDataFrame([{
            "facility_code": "TEST1",
            "interval_start": "2025-01-01 00:00:00",
            "metric": "energy",
            "value": 100.0,
        }])
        result = energy_loader.transform(df)
        assert "interval_ts" in result.columns
        assert "interval_date" in result.columns


class TestWeatherLoader:
    """Test WeatherLoader implementation."""

    def test_class_attributes(self):
        """Test WeatherLoader has correct class attributes."""
        assert WeatherLoader.iceberg_table == "lh.bronze.raw_facility_weather"
        assert WeatherLoader.timestamp_column == "weather_timestamp"
        assert WeatherLoader.merge_keys == ("facility_code", "weather_timestamp")

    def test_initialization_sets_date_range(self, shared_spark: SparkSession):
        """Test initialization sets default date range."""
        loader = WeatherLoader()
        loader._spark = shared_spark
        assert loader.options.start is not None
        assert loader.options.end is not None

    def test_date_range_initialization_custom_dates(self, shared_spark: SparkSession):
        """Test date range initialization with custom dates."""
        options = BronzeLoadOptions(
            mode="backfill",
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 12, 31),
        )
        loader = WeatherLoader(options)
        loader._spark = shared_spark
        assert loader.options.start == dt.date(2025, 1, 1)
        assert loader.options.end == dt.date(2025, 12, 31)

    def test_transform_adds_timestamp_columns(self, weather_loader: WeatherLoader):
        """Test transform adds weather_timestamp and weather_date columns."""
        spark = weather_loader.spark
        df = spark.createDataFrame([{
            "facility_code": "TEST1",
            "date": "2025-01-01 12:00:00",
            "temperature": 25.5,
        }])
        result = weather_loader.transform(df)
        assert "weather_timestamp" in result.columns
        assert "weather_date" in result.columns

    def test_transform_filters_null_timestamps(self, weather_loader: WeatherLoader):
        """Test transform filters out null timestamps."""
        spark = weather_loader.spark
        df = spark.createDataFrame([
            {"facility_code": "TEST1", "date": "2025-01-01 12:00:00"},
            {"facility_code": "TEST2", "date": None},
        ])
        result = weather_loader.transform(df)
        assert result.count() == 1  # Only non-null timestamp row


class TestAirQualityLoader:
    """Test AirQualityLoader implementation."""

    def test_class_attributes(self):
        """Test AirQualityLoader has correct class attributes."""
        assert AirQualityLoader.iceberg_table == "lh.bronze.raw_facility_air_quality"
        assert AirQualityLoader.timestamp_column == "air_timestamp"
        assert AirQualityLoader.merge_keys == ("facility_code", "air_timestamp")

    def test_initialization_sets_date_range(self, shared_spark: SparkSession):
        """Test initialization sets default date range."""
        loader = AirQualityLoader()
        loader._spark = shared_spark
        assert loader.options.start is not None
        assert loader.options.end is not None

    def test_date_range_initialization_incremental(self, shared_spark: SparkSession):
        """Test date range initialization for incremental mode."""
        options = BronzeLoadOptions(mode="incremental")
        loader = AirQualityLoader(options)
        loader._spark = shared_spark
        # Should set default dates for incremental mode
        assert loader.options.start is not None

    def test_transform_adds_timestamp_columns(self, air_quality_loader: AirQualityLoader):
        """Test transform adds air_timestamp and air_date columns."""
        spark = air_quality_loader.spark
        df = spark.createDataFrame([{
            "facility_code": "TEST1",
            "date": "2025-01-01 12:00:00",
            "pm25": 10.5,
        }])
        result = air_quality_loader.transform(df)
        assert "air_timestamp" in result.columns
        assert "air_date" in result.columns

    def test_transform_filters_null_timestamps(self, air_quality_loader: AirQualityLoader):
        """Test transform filters out null timestamps."""
        spark = air_quality_loader.spark
        df = spark.createDataFrame([
            {"facility_code": "TEST1", "date": "2025-01-01 12:00:00"},
            {"facility_code": "TEST2", "date": None},
        ])
        result = air_quality_loader.transform(df)
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
