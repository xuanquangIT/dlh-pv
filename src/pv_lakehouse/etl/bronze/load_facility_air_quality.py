#!/usr/bin/env python3
"""Bronze ingestion job for facility-level Open-Meteo air quality data."""
from __future__ import annotations
import argparse, datetime as dt, logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import pandas as pd
from pyspark.sql import DataFrame, functions as F
from pv_lakehouse.etl.bronze.base import BaseBronzeLoader, BronzeLoadOptions
from pv_lakehouse.etl.bronze.facility_timezones import get_facility_timezone
from pv_lakehouse.etl.clients import openmeteo
from pv_lakehouse.etl.clients.openmeteo import RateLimiter
from pv_lakehouse.etl.utils import load_facility_locations, parse_date_argparse

LOGGER = logging.getLogger(__name__)


class AirQualityLoader(BaseBronzeLoader):
    """Bronze loader for facility air quality data."""
    iceberg_table = "lh.bronze.raw_facility_air_quality"
    timestamp_column = "air_timestamp"
    merge_keys = ("facility_code", "air_timestamp")

    def __init__(self, options: Optional[BronzeLoadOptions] = None) -> None:
        super().__init__(options)
        today = dt.date.today()
        if self.options.mode == "incremental" and self.options.start is None:
            max_ts = self.get_max_timestamp()
            self.options.start = max_ts.date() if max_ts else today - dt.timedelta(days=1)
        else:
            self.options.start = self.options.start or (today - dt.timedelta(days=1))
        self.options.end = self.options.end or today

    def fetch_data(self) -> pd.DataFrame:
        facilities = load_facility_locations(self.resolve_facilities(), self.options.api_key)
        limiter, frames = RateLimiter(30.0), []
        def fetch_one(f):
            return openmeteo.fetch_air_quality_dataframe(
                f, start=self.options.start, end=self.options.end,
                chunk_days=openmeteo.AIR_QUALITY_MAX_DAYS, hourly_variables=openmeteo.DEFAULT_AIR_VARS,
                timezone=get_facility_timezone(f.code), limiter=limiter,
                max_retries=openmeteo.DEFAULT_MAX_RETRIES,
                retry_backoff=openmeteo.DEFAULT_RETRY_BACKOFF, max_workers=self.options.max_workers,
            )
        with ThreadPoolExecutor(max_workers=self.options.max_workers) as ex:
            futures = {ex.submit(fetch_one, f): f for f in facilities}
            for fut in as_completed(futures):
                try:
                    df = fut.result()
                    if not df.empty:
                        frames.append(df)
                except (ConnectionError, TimeoutError, OSError) as e:
                    LOGGER.warning("Failed: %s - %s", futures[fut].code, e)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("air_timestamp", F.to_timestamp("date")).withColumn(
            "air_date", F.to_date("air_timestamp")
        ).filter(F.col("air_timestamp").isNotNull())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load facility air quality into Bronze")
    p.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    p.add_argument("--facility-codes", help="Comma-separated facility codes")
    p.add_argument("--start", type=parse_date_argparse, help="Start date YYYY-MM-DD")
    p.add_argument("--end", type=parse_date_argparse, help="End date YYYY-MM-DD")
    p.add_argument("--api-key", help="Override API key")
    p.add_argument("--max-workers", type=int, default=4, help="Concurrent threads")
    p.add_argument("--app-name", default="bronze-air-quality")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.end and args.start and args.end < args.start:
        raise SystemExit("End date must not be before start date")
    AirQualityLoader(BronzeLoadOptions(
        mode=args.mode, start=args.start, end=args.end, facility_codes=args.facility_codes,
        api_key=args.api_key, max_workers=args.max_workers, app_name=args.app_name,
    )).run()


if __name__ == "__main__":
    main()
