#!/usr/bin/env python3
"""Bronze ingestion job for facility-level Open-Meteo weather data."""

from __future__ import annotations

import argparse
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import pandas as pd
from pyspark.sql import functions as F

from pv_lakehouse.etl.bronze import openmeteo_common
from pv_lakehouse.etl.clients import openmeteo
from pv_lakehouse.etl.clients.openmeteo import FacilityLocation, RateLimiter
from pv_lakehouse.etl.utils.spark_utils import create_spark_session

S3_WEATHER_BASE = "s3a://lakehouse/bronze/raw_facility_weather"
ICEBERG_WEATHER_TABLE = "lh.bronze.raw_facility_weather"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Open-Meteo facility weather data into Bronze zone")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--facility-codes", help="Comma separated facility codes to include")
    parser.add_argument("--start", type=openmeteo_common.parse_date, help="Start date (YYYY-MM-DD). Defaults to yesterday.")
    parser.add_argument("--end", type=openmeteo_common.parse_date, help="End date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--weather-chunk-days", type=int, default=30)
    parser.add_argument("--weather-vars", default=openmeteo.DEFAULT_WEATHER_VARS)
    parser.add_argument(
        "--weather-endpoint",
        choices=sorted({"auto", *openmeteo.WEATHER_ENDPOINTS.keys()}),
        default="auto",
    )
    parser.add_argument("--timezone", default="UTC")
    parser.add_argument("--max-requests-per-minute", type=float, default=30.0)
    parser.add_argument("--max-workers", type=int, default=4, help="Concurrent facility fetch workers")
    parser.add_argument("--max-retries", type=int, default=openmeteo.DEFAULT_MAX_RETRIES)
    parser.add_argument("--retry-backoff", type=float, default=openmeteo.DEFAULT_RETRY_BACKOFF)
    parser.add_argument("--s3-weather-path", default=S3_WEATHER_BASE)
    parser.add_argument("--iceberg-weather-table", default=ICEBERG_WEATHER_TABLE)
    parser.add_argument(
        "--openelectricity-api-key",
        help="Override OpenElectricity API key used to resolve facility locations",
    )
    parser.add_argument("--app-name", default="bronze-facility-weather")
    return parser.parse_args()


def collect_weather_data(facilities: List[FacilityLocation], args: argparse.Namespace) -> pd.DataFrame:
    limiter = RateLimiter(args.max_requests_per_minute)
    frames: List[pd.DataFrame] = []

    def fetch_for_facility(facility: FacilityLocation) -> pd.DataFrame:
        print(f"Fetching weather for facility {facility.code} ({facility.name})")
        return openmeteo.fetch_weather_dataframe(
            facility,
            start=args.start,
            end=args.end,
            chunk_days=args.weather_chunk_days,
            hourly_variables=args.weather_vars,
            endpoint_preference=args.weather_endpoint,
            timezone=args.timezone,
            limiter=limiter,
            max_retries=args.max_retries,
            retry_backoff=args.retry_backoff,
            max_workers=args.max_workers,
        )

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(fetch_for_facility, facility): facility for facility in facilities}
        for future in as_completed(futures):
            facility = futures[future]
            try:
                frame = future.result()
            except Exception as exc:  # pragma: no cover - defensive
                print(f"Failed to fetch weather data for facility {facility.code}: {exc}")
                continue
            if not frame.empty:
                frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    args = parse_args()

    today = dt.date.today()
    default_start = today - dt.timedelta(days=1)
    args.start = args.start or default_start
    args.end = args.end or today

    if args.end < args.start:
        raise SystemExit("End date must not be before start date")

    facility_codes = openmeteo_common.resolve_facility_codes(args.facility_codes)
    facilities = openmeteo_common.load_facility_locations(facility_codes, args.openelectricity_api_key)

    weather_df = collect_weather_data(facilities, args)
    if weather_df.empty:
        print("No Open-Meteo weather data retrieved; nothing to write.")
        return

    spark = create_spark_session(args.app_name)
    ingest_ts = F.current_timestamp()
    ingest_date = today.isoformat()

    weather_spark_df = spark.createDataFrame(weather_df)
    weather_spark_df = (
        weather_spark_df.withColumn("ingest_mode", F.lit(args.mode))
        .withColumn("ingest_timestamp", ingest_ts)
        .withColumn("weather_timestamp", F.to_timestamp("date"))
        .withColumn("weather_date", F.to_date("weather_timestamp"))
    )
    weather_spark_df = weather_spark_df.filter(
        F.col("weather_timestamp").isNotNull() & (F.col("weather_timestamp") <= ingest_ts)
    )

    openmeteo_common.write_dataset(
        weather_spark_df,
        s3_base_path=args.s3_weather_path,
        iceberg_table=args.iceberg_weather_table,
        mode=args.mode,
        ingest_date=ingest_date,
        label="weather",
    )

    spark.stop()


if __name__ == "__main__":
    main()
