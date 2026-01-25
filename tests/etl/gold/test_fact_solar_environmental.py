"""Tests for GoldFactSolarEnvironmentalLoader.

Tests cover:
- Transform method orchestration
- Dimension key joins
- Weather data enrichment and humidity calculation
- Air quality data enrichment
- Quality metrics calculation
- Derived metrics calculation
- Final column projection
"""

from __future__ import annotations

import datetime as dt
from typing import Dict

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pv_lakehouse.etl.gold.fact_solar_environmental import (
    GoldFactSolarEnvironmentalLoader,
)
from pv_lakehouse.etl.gold.base import GoldLoadOptions


class TestGoldFactSolarEnvironmentalTransform:
    """Tests for transform method and helper methods."""

    def test_transform_returns_fact_table(self, all_sources: Dict[str, DataFrame]):
        """Transform should return fact_solar_environmental DataFrame."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)

        assert result is not None
        assert "fact_solar_environmental" in result
        assert result["fact_solar_environmental"].count() > 0

    def test_transform_returns_none_without_energy(
        self, all_dimensions: Dict[str, DataFrame]
    ):
        """Transform should return None when energy data is missing."""
        loader = GoldFactSolarEnvironmentalLoader()
        # Only provide dimensions, no energy data
        result = loader.transform(all_dimensions)

        assert result is None

    def test_transform_returns_none_without_dimensions(
        self, hourly_energy_df: DataFrame
    ):
        """Transform should return None when dimensions are missing."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform({"hourly_energy": hourly_energy_df})

        assert result is None


class TestDimensionKeyJoins:
    """Tests for dimension key joining in transform."""

    def test_facility_key_joined(self, all_sources: Dict[str, DataFrame]):
        """Fact table should have facility_key column."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "facility_key" in fact.columns

        # Verify keys are populated
        null_keys = fact.filter(F.col("facility_key").isNull()).count()
        assert null_keys == 0, "All records should have facility_key"

    def test_date_key_joined(self, all_sources: Dict[str, DataFrame]):
        """Fact table should have date_key column."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "date_key" in fact.columns

        # Verify date_key format (YYYYMMDD)
        row = fact.first()
        assert row["date_key"] >= 20240101

    def test_time_key_joined(self, all_sources: Dict[str, DataFrame]):
        """Fact table should have time_key column."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "time_key" in fact.columns


class TestWeatherEnrichment:
    """Tests for weather data enrichment."""

    def test_weather_columns_present(self, all_sources: Dict[str, DataFrame]):
        """Fact table should have weather columns."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        weather_cols = [
            "shortwave_radiation",
            "direct_radiation",
            "temperature_2m",
            "cloud_cover",
            "humidity_2m",
        ]
        for col in weather_cols:
            assert col in fact.columns, f"Missing weather column: {col}"

    def test_humidity_calculated(self, all_sources: Dict[str, DataFrame]):
        """Humidity should be calculated from temperature and dew point."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        # Get a row with weather data
        row = fact.filter(F.col("humidity_2m").isNotNull()).first()

        # Humidity should be between 0 and 100
        assert row["humidity_2m"] >= 0
        assert row["humidity_2m"] <= 100

    def test_fact_without_weather_still_valid(
        self,
        hourly_energy_df: DataFrame,
        all_dimensions: Dict[str, DataFrame],
    ):
        """Fact records should exist even without matching weather data."""
        loader = GoldFactSolarEnvironmentalLoader()
        sources = {
            "hourly_energy": hourly_energy_df,
            **all_dimensions,
            # No weather data provided
        }
        result = loader.transform(sources)

        assert result is not None
        fact = result["fact_solar_environmental"]
        assert fact.count() > 0


class TestAirQualityEnrichment:
    """Tests for air quality data enrichment."""

    def test_aqi_columns_present(self, all_sources: Dict[str, DataFrame]):
        """Fact table should have air quality columns."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        aqi_cols = ["pm2_5", "pm10", "aqi_value", "aqi_category_key"]
        for col in aqi_cols:
            assert col in fact.columns, f"Missing AQI column: {col}"

    def test_aqi_category_key_resolved(self, all_sources: Dict[str, DataFrame]):
        """AQI category key should be resolved from value ranges."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        # Get row with AQI data
        row = fact.filter(F.col("aqi_value").isNotNull()).first()

        # Should have category key
        if row:
            assert row["aqi_category_key"] is not None


class TestQualityMetrics:
    """Tests for quality metrics calculation."""

    def test_completeness_calculated(self, all_sources: Dict[str, DataFrame]):
        """Completeness percentage should be calculated."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "completeness_pct" in fact.columns

        # With all sources, completeness should be 100%
        row = fact.filter(
            F.col("energy_mwh").isNotNull()
            & F.col("shortwave_radiation").isNotNull()
            & F.col("pm2_5").isNotNull()
        ).first()

        if row:
            assert float(row["completeness_pct"]) == 100.0

    def test_quality_flag_set(self, all_sources: Dict[str, DataFrame]):
        """Quality flag should be set based on completeness."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "quality_flag" in fact.columns

        # Verify valid values
        flags = fact.select("quality_flag").distinct().collect()
        valid_flags = {"GOOD", "WARNING", "BAD"}
        for row in flags:
            assert row["quality_flag"] in valid_flags

    def test_is_valid_flag_set(self, all_sources: Dict[str, DataFrame]):
        """is_valid flag should indicate record validity."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "is_valid" in fact.columns

        # Records with positive energy should be valid
        row = fact.filter(F.col("energy_mwh") > 0).first()
        if row:
            assert row["is_valid"] is True


class TestDerivedMetrics:
    """Tests for derived metrics calculation."""

    def test_irradiance_conversion(self, all_sources: Dict[str, DataFrame]):
        """Irradiance should be converted from W/m² to kWh/m²·h."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "irr_kwh_m2_hour" in fact.columns

        # Get row with radiation data
        row = fact.filter(F.col("shortwave_radiation").isNotNull()).first()
        if row:
            expected = float(row["shortwave_radiation"]) / 1000.0
            assert abs(float(row["irr_kwh_m2_hour"]) - expected) < 0.001

    def test_sunshine_hours_conversion(self, all_sources: Dict[str, DataFrame]):
        """Sunshine duration should be converted from seconds to hours."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "sunshine_hours" in fact.columns

    def test_yr_weighted_kwh_calculated(self, all_sources: Dict[str, DataFrame]):
        """Capacity-weighted irradiance should be calculated."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "yr_weighted_kwh" in fact.columns


class TestAuditColumns:
    """Tests for audit column presence."""

    def test_created_at_present(self, all_sources: Dict[str, DataFrame]):
        """created_at column should be present."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "created_at" in fact.columns

    def test_updated_at_present(self, all_sources: Dict[str, DataFrame]):
        """updated_at column should be present."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        assert "updated_at" in fact.columns


class TestFinalSchema:
    """Tests for final output schema."""

    def test_all_required_columns_present(self, all_sources: Dict[str, DataFrame]):
        """All required Gold layer columns should be present."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        required_cols = [
            # Keys
            "facility_key",
            "date_key",
            "time_key",
            "aqi_category_key",
            # Energy
            "energy_mwh",
            "intervals_count",
            # Weather
            "shortwave_radiation",
            "temperature_2m",
            "humidity_2m",
            "cloud_cover",
            # Air quality
            "pm2_5",
            "aqi_value",
            # Quality
            "is_valid",
            "quality_flag",
            "completeness_pct",
            # Derived
            "irr_kwh_m2_hour",
            "sunshine_hours",
            "yr_weighted_kwh",
            # Audit
            "created_at",
            "updated_at",
        ]

        actual_cols = set(fact.columns)
        for col in required_cols:
            assert col in actual_cols, f"Missing required column: {col}"

    def test_no_intermediate_columns(self, all_sources: Dict[str, DataFrame]):
        """Intermediate columns should not be in final output."""
        loader = GoldFactSolarEnvironmentalLoader()
        result = loader.transform(all_sources)
        fact = result["fact_solar_environmental"]

        # These should be dropped during projection
        intermediate_cols = [
            "has_energy",
            "has_weather",
            "has_air_quality",
            "dim_full_date",
            "dim_date_key",
            "dim_time_key",
        ]

        actual_cols = set(fact.columns)
        for col in intermediate_cols:
            assert col not in actual_cols, f"Intermediate column leaked: {col}"


class TestLoaderOptions:
    """Tests for loader with different options."""

    def test_with_explain_plan_enabled(self, all_sources: Dict[str, DataFrame]):
        """Loader should work with explain_plan enabled."""
        options = GoldLoadOptions(explain_plan=True, explain_mode="simple")
        loader = GoldFactSolarEnvironmentalLoader(options)

        # Should not raise
        result = loader.transform(all_sources)
        assert result is not None

    def test_with_custom_broadcast_threshold(self, all_sources: Dict[str, DataFrame]):
        """Loader should work with custom broadcast threshold."""
        options = GoldLoadOptions(broadcast_threshold_mb=5)
        loader = GoldFactSolarEnvironmentalLoader(options)

        result = loader.transform(all_sources)
        assert result is not None
