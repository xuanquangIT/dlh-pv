"""Gold zone ETL helpers."""

from __future__ import annotations

from .base import BaseGoldLoader, GoldLoadOptions, GoldTableConfig, SourceTableConfig
from .dim_aqi_category import GoldDimAQICategoryLoader
from .dim_date import GoldDimDateLoader
from .dim_facility import GoldDimFacilityLoader
from .dim_time import GoldDimTimeLoader
from .dimension_lookup import (
    lookup_aqi_category_key,
    lookup_date_key,
    lookup_dimension_key,
    lookup_facility_key,
    lookup_time_key,
    lookup_weather_condition_key,
)
from .fact_solar_environmental import GoldFactSolarEnvironmentalLoader
from .fact_solar_forecast_regression import GoldFactSolarForecastRegressionLoader

__all__ = [
    "GoldLoadOptions",
    "GoldTableConfig",
    "SourceTableConfig",
    "BaseGoldLoader",
    "GoldDimAQICategoryLoader",
    "GoldDimDateLoader",
    "GoldDimTimeLoader",
    "GoldDimFacilityLoader",
    "GoldFactSolarEnvironmentalLoader",
    "GoldFactSolarForecastRegressionLoader",
    # Dimension lookup helpers
    "lookup_dimension_key",
    "lookup_facility_key",
    "lookup_date_key",
    "lookup_time_key",
    "lookup_aqi_category_key",
    "lookup_weather_condition_key",
]
