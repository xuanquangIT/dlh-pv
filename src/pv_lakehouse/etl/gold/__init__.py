"""Gold zone ETL helpers."""

from __future__ import annotations

from .base import BaseGoldLoader, GoldLoadOptions, GoldTableConfig, SourceTableConfig
from .dim_air_quality_category import GoldDimAirQualityCategoryLoader
from .dim_date import GoldDimDateLoader
from .dim_equipment_status import GoldDimEquipmentStatusLoader
from .dim_facility import GoldDimFacilityLoader
from .dim_model_version import GoldDimModelVersionLoader
from .dim_performance_issue import GoldDimPerformanceIssueLoader
from .dim_time import GoldDimTimeLoader
from .dim_weather_condition import GoldDimWeatherConditionLoader
from .fact_air_quality_impact import GoldFactAirQualityImpactLoader
from .fact_kpi_performance import GoldFactKpiPerformanceLoader
from .fact_root_cause_analysis import GoldFactRootCauseAnalysisLoader
from .fact_solar_forecast import GoldFactSolarForecastLoader
from .fact_weather_impact import GoldFactWeatherImpactLoader

__all__ = [
    "GoldLoadOptions",
    "GoldTableConfig",
    "SourceTableConfig",
    "BaseGoldLoader",
    "GoldDimDateLoader",
    "GoldDimTimeLoader",
    "GoldDimFacilityLoader",
    "GoldDimWeatherConditionLoader",
    "GoldDimModelVersionLoader",
    "GoldDimEquipmentStatusLoader",
    "GoldDimAirQualityCategoryLoader",
    "GoldDimPerformanceIssueLoader",
    "GoldFactKpiPerformanceLoader",
    "GoldFactRootCauseAnalysisLoader",
    "GoldFactWeatherImpactLoader",
    "GoldFactAirQualityImpactLoader",
    "GoldFactSolarForecastLoader",
]
