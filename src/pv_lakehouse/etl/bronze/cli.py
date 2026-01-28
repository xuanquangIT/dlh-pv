#!/usr/bin/env python3
from __future__ import annotations
import argparse
import datetime as dt
import logging
from typing import Dict, Optional, Type
from pv_lakehouse.etl.bronze.base import BaseBronzeLoader, BronzeLoadOptions
from pv_lakehouse.etl.utils.logging_config import configure_logging

LOGGER = logging.getLogger(__name__)

def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    """Parse date string in YYYY-MM-DD format."""
    if value is None:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected format: YYYY-MM-DD"
        ) from exc


def _parse_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    """Parse datetime string in ISO 8601 format."""
    if value is None:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime '{value}'. Expected ISO 8601 format."
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for Bronze CLI."""
    parser = argparse.ArgumentParser(
        description="Run Bronze zone loaders for facility data ingestion."
    )
    parser.add_argument(
        "dataset",
        choices=["facilities", "energy", "weather", "air_quality"],
        help="Which Bronze dataset loader to run",
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "incremental"],
        default="incremental",
        help="Load mode (default: incremental)",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        help="Start date YYYY-MM-DD (for weather/air_quality)",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        help="End date YYYY-MM-DD (for weather/air_quality)",
    )
    parser.add_argument(
        "--date-start",
        type=_parse_datetime,
        help="Start datetime ISO 8601 (for energy)",
    )
    parser.add_argument(
        "--date-end",
        type=_parse_datetime,
        help="End datetime ISO 8601 (for energy)",
    )
    parser.add_argument(
        "--facility-codes",
        help="Comma-separated facility codes (default: all solar facilities)",
    )
    parser.add_argument(
        "--api-key",
        help="Override API key",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Concurrent threads for API calls (default: 4)",
    )
    parser.add_argument(
        "--app-name",
        help="Custom Spark application name",
    )
    return parser


def _get_loader_registry() -> Dict[str, Type[BaseBronzeLoader]]:
    """Lazy import loader registry to avoid circular imports."""
    # Import here to avoid circular dependency issues
    from pv_lakehouse.etl.bronze.load_facilities import FacilitiesLoader
    from pv_lakehouse.etl.bronze.load_facility_air_quality import AirQualityLoader
    from pv_lakehouse.etl.bronze.load_facility_energy import EnergyLoader
    from pv_lakehouse.etl.bronze.load_facility_weather import WeatherLoader

    return {
        "facilities": FacilitiesLoader,
        "energy": EnergyLoader,
        "weather": WeatherLoader,
        "air_quality": AirQualityLoader,
    }


def run_cli(argv: Optional[list[str]] = None) -> int:
    """Execute CLI with given arguments.

    Args:
        argv: Command-line arguments (uses sys.argv if None).

    Returns:
        Number of rows written.
    """
    # Configure logging first
    configure_logging()
    
    parser = build_parser()
    args = parser.parse_args(argv)

    loader_registry = _get_loader_registry()
    loader_cls = loader_registry[args.dataset]

    # Normalize start/end based on dataset type
    start = args.date_start if args.dataset == "energy" else args.start
    end = args.date_end if args.dataset == "energy" else args.end

    options = BronzeLoadOptions(
        mode=args.mode,
        start=start,
        end=end,
        facility_codes=args.facility_codes,
        api_key=args.api_key,
        max_workers=args.max_workers,
        app_name=args.app_name or f"bronze-{args.dataset}",
    )

    loader = loader_cls(options)
    try:
        row_count = loader.run()
        LOGGER.info("Loader '%s' wrote %d rows", args.dataset, row_count)
        return row_count
    except ConnectionError as e:
        LOGGER.error("API connection failed for '%s': %s", args.dataset, e)
        raise
    except ValueError as e:
        LOGGER.error("Data validation failed for '%s': %s", args.dataset, e)
        raise
    except RuntimeError as e:
        LOGGER.error("Loader execution failed for '%s': %s", args.dataset, e)
        raise


def main() -> None:  # pragma: no cover - CLI entrypoint
    """CLI entry point."""
    run_cli()


if __name__ == "__main__":  # pragma: no cover - CLI execution path
    main()
