"""CLI entrypoint for running Gold loaders via spark-submit."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from typing import Callable, Dict, Optional, Type

from pv_lakehouse.etl.gold import (
    BaseGoldLoader,
    GoldDimAirQualityCategoryLoader,
    GoldDimDateLoader,
    GoldDimEquipmentStatusLoader,
    GoldDimFacilityLoader,
    GoldDimModelVersionLoader,
    GoldDimPerformanceIssueLoader,
    GoldDimTimeLoader,
    GoldDimWeatherConditionLoader,
    GoldFactAirQualityImpactLoader,
    GoldFactKpiPerformanceLoader,
    GoldFactRootCauseAnalysisLoader,
    GoldFactSolarForecastLoader,
    GoldFactWeatherImpactLoader,
    GoldLoadOptions,
)

LOGGER = logging.getLogger("pv_lakehouse.etl.gold.run_loader")


_LOADER_REGISTRY: Dict[str, Type[BaseGoldLoader]] = {
    "dim_air_quality_category": GoldDimAirQualityCategoryLoader,
    "dim_date": GoldDimDateLoader,
    "dim_equipment_status": GoldDimEquipmentStatusLoader,
    "dim_facility": GoldDimFacilityLoader,
    "dim_model_version": GoldDimModelVersionLoader,
    "dim_performance_issue": GoldDimPerformanceIssueLoader,
    "dim_time": GoldDimTimeLoader,
    "dim_weather_condition": GoldDimWeatherConditionLoader,
    "fact_air_quality_impact": GoldFactAirQualityImpactLoader,
    "fact_kpi_performance": GoldFactKpiPerformanceLoader,
    "fact_root_cause_analysis": GoldFactRootCauseAnalysisLoader,
    "fact_solar_forecast": GoldFactSolarForecastLoader,
    "fact_weather_impact": GoldFactWeatherImpactLoader,
}


def _parse_iso_datetime(value: Optional[str]) -> Optional[dt.datetime | dt.date | str]:
    if value is None:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        try:
            return dt.date.fromisoformat(value)
        except ValueError:
            return value


def _build_options(args: argparse.Namespace) -> GoldLoadOptions:
    return GoldLoadOptions(
        mode=args.mode,
        start=_parse_iso_datetime(args.start),
        end=_parse_iso_datetime(args.end),
        load_strategy=args.load_strategy,
        app_name=args.app_name,
        target_file_size_mb=args.target_file_size_mb,
        max_records_per_file=args.max_records_per_file,
    )


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Gold ETL loader")
    parser.add_argument("loader_name", choices=sorted(_LOADER_REGISTRY.keys()))
    parser.add_argument("--mode", default="incremental", choices=["incremental", "full"])
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--load-strategy", default="merge", choices=["merge", "overwrite"])
    parser.add_argument("--app-name", default="gold-loader")
    parser.add_argument("--target-file-size-mb", type=int, default=128)
    parser.add_argument("--max-records-per-file", type=int, default=250_000)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    _configure_logging(args.verbose)

    loader_cls = _LOADER_REGISTRY[args.loader_name]
    options = _build_options(args)
    loader = loader_cls(options=options)

    LOGGER.info("Starting loader %s with options %s", args.loader_name, options)
    row_count = loader.run()
    LOGGER.info("Loader %s completed. Rows written: %s", args.loader_name, row_count)


if __name__ == "__main__":
    main()
