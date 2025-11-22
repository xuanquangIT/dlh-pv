#!/usr/bin/env python3
"""Command-line runner for Gold loaders (spark-submit friendly)."""

from __future__ import annotations

import argparse
import datetime as dt
from typing import Dict, Optional, Type

from pv_lakehouse.etl.gold.base import BaseGoldLoader, GoldLoadOptions
from pv_lakehouse.etl.gold.dim_aqi_category import GoldDimAQICategoryLoader
from pv_lakehouse.etl.gold.dim_date import GoldDimDateLoader
from pv_lakehouse.etl.gold.dim_facility import GoldDimFacilityLoader
from pv_lakehouse.etl.gold.dim_forecast_model_version import GoldDimForecastModelVersionLoader
from pv_lakehouse.etl.gold.dim_time import GoldDimTimeLoader
from pv_lakehouse.etl.gold.fact_solar_environmental import GoldFactSolarEnvironmentalLoader
from pv_lakehouse.etl.gold.fact_solar_forecast_regression import GoldFactSolarForecastRegressionLoader


_LOADER_REGISTRY: Dict[str, Type[BaseGoldLoader]] = {
    # Dimension tables (load these first)
    "dim_aqi_category": GoldDimAQICategoryLoader,
    "dim_date": GoldDimDateLoader,
    "dim_facility": GoldDimFacilityLoader,
    "dim_forecast_model_version": GoldDimForecastModelVersionLoader,
    "dim_time": GoldDimTimeLoader,
    # Fact tables (load these after dimensions)
    "fact_solar_environmental": GoldFactSolarEnvironmentalLoader,
    "fact_solar_forecast_regression": GoldFactSolarForecastRegressionLoader,
}


def _parse_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    """Parse ISO datetime string to datetime object."""
    if value is None:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime '{value}'. Expected ISO 8601 format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)."
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for Gold loader CLI."""
    parser = argparse.ArgumentParser(
        description="Run Gold zone loaders to build star schema from Silver data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load all dimension tables first (full refresh)
  python -m pv_lakehouse.etl.gold.cli dim_facility --mode full
  python -m pv_lakehouse.etl.gold.cli dim_aqi_category --mode full
  
  # Load fact table incrementally
  python -m pv_lakehouse.etl.gold.cli fact_solar_environmental --mode incremental
  
  # Load fact table for specific date range
  python -m pv_lakehouse.etl.gold.cli fact_solar_environmental \\
      --mode incremental --start 2024-01-01 --end 2024-01-31
        """,
    )
    parser.add_argument(
        "dataset",
        choices=sorted(_LOADER_REGISTRY),
        help="Which Gold dataset loader to run",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Load mode: full (all Silver data) or incremental (delta only, default)",
    )
    parser.add_argument(
        "--load-strategy",
        choices=["merge", "overwrite"],
        default="merge",
        help="How to write into Gold: merge (partition overwrite) or full overwrite (default: merge)",
    )
    parser.add_argument(
        "--start",
        type=_parse_datetime,
        help="Optional ISO datetime lower bound (inclusive) for Silver data reads",
    )
    parser.add_argument(
        "--end",
        type=_parse_datetime,
        help="Optional ISO datetime upper bound (inclusive) for Silver data reads",
    )
    parser.add_argument(
        "--app-name",
        default="gold-loader",
        help="Custom Spark application name (default: gold-loader)",
    )
    parser.add_argument(
        "--target-file-size-mb",
        type=int,
        default=128,
        help="Target file size in MB for Gold table files (default: 128)",
    )
    parser.add_argument(
        "--max-records-per-file",
        type=int,
        default=250_000,
        help="Maximum records per file for Gold tables (default: 250000)",
    )
    return parser


def run_cli(argv: Optional[list[str]] = None) -> int:
    """Run Gold loader based on CLI arguments."""
    parser = build_parser()
    args = parser.parse_args(argv)

    loader_cls = _LOADER_REGISTRY[args.dataset]
    options = GoldLoadOptions(
        mode=args.mode,
        start=args.start,
        end=args.end,
        load_strategy=args.load_strategy,
        app_name=args.app_name or f"gold-{args.dataset}",
        target_file_size_mb=args.target_file_size_mb,
        max_records_per_file=args.max_records_per_file,
    )

    print(f"[GOLD] Running loader: {args.dataset}", flush=True)
    print(f"[GOLD]   Mode: {args.mode}", flush=True)
    print(f"[GOLD]   Strategy: {args.load_strategy}", flush=True)
    if args.start:
        print(f"[GOLD]   Start: {args.start}", flush=True)
    if args.end:
        print(f"[GOLD]   End: {args.end}", flush=True)

    loader = loader_cls(options)
    row_count = loader.run()
    
    print(f"[GOLD] Loader '{args.dataset}' completed: {row_count} rows written", flush=True)
    return row_count


def main() -> None:
    """CLI entrypoint for Gold loaders."""
    run_cli()


if __name__ == "__main__":
    main()
