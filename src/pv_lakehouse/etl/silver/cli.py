"""Command-line runner for Silver loaders (spark-submit friendly)."""

from __future__ import annotations

import argparse
import datetime as dt
from typing import Dict, Optional, Type

from pv_lakehouse.etl.silver.base import BaseSilverLoader, LoadOptions
from pv_lakehouse.etl.silver.daily_air_quality import SilverDailyAirQualityLoader
from pv_lakehouse.etl.silver.daily_weather import SilverDailyWeatherLoader
from pv_lakehouse.etl.silver.facility_master import SilverFacilityMasterLoader
from pv_lakehouse.etl.silver.hourly_air_quality import SilverHourlyAirQualityLoader
from pv_lakehouse.etl.silver.hourly_energy import SilverHourlyEnergyLoader
from pv_lakehouse.etl.silver.hourly_weather import SilverHourlyWeatherLoader


_LOADER_REGISTRY: Dict[str, Type[BaseSilverLoader]] = {
    "facility_master": SilverFacilityMasterLoader,
    "hourly_energy": SilverHourlyEnergyLoader,
    "hourly_weather": SilverHourlyWeatherLoader,
    "hourly_air_quality": SilverHourlyAirQualityLoader,
    # Legacy daily loaders (deprecated, use hourly versions)
    "daily_weather": SilverDailyWeatherLoader,
    "daily_air_quality": SilverDailyAirQualityLoader,
}


def _parse_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    if value is None:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - user input validation
        raise argparse.ArgumentTypeError(
            f"Invalid datetime '{value}'. Expected ISO 8601 format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)."
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Silver zone loaders against Bronze data.")
    parser.add_argument(
        "dataset",
        choices=sorted(_LOADER_REGISTRY),
        help="Which Silver dataset loader to run",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Load mode controlling the Bronze read window (default: incremental)",
    )
    parser.add_argument(
        "--load-strategy",
        choices=["merge", "overwrite"],
        default="merge",
        help="How to write into Silver: merge (partition overwrite) or full overwrite",
    )
    parser.add_argument(
        "--start",
        type=_parse_datetime,
        help="Optional ISO datetime lower bound (inclusive) for Bronze data",
    )
    parser.add_argument(
        "--end",
        type=_parse_datetime,
        help="Optional ISO datetime upper bound (inclusive) for Bronze data",
    )
    parser.add_argument(
        "--app-name",
        default="silver-loader",
        help="Custom Spark application name",
    )
    return parser


def run_cli(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    loader_cls = _LOADER_REGISTRY[args.dataset]
    options = LoadOptions(
        mode=args.mode,
        start=args.start,
        end=args.end,
        load_strategy=args.load_strategy,
        app_name=args.app_name or f"silver-{args.dataset}",
    )

    loader = loader_cls(options)
    row_count = loader.run()
    print(f"Loader '{args.dataset}' wrote {row_count} rows")
    return row_count


def main() -> None:  # pragma: no cover - CLI entrypoint
    run_cli()


if __name__ == "__main__":  # pragma: no cover - CLI execution path
    main()
