#!/usr/bin/env python3
"""Bronze ingestion job for facility-level Open-Meteo air quality data."""

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

S3_AIR_BASE = "s3a://lakehouse/bronze/raw_facility_air_quality"
ICEBERG_AIR_TABLE = "lh.bronze.raw_facility_air_quality"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load facility air quality data into Bronze zone")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--facility-codes", help="Comma-separated facility codes (default: all solar facilities)")
    parser.add_argument("--start", type=openmeteo_common.parse_date, help="Start date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--end", type=openmeteo_common.parse_date, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--api-key", help="Override OpenElectricity API key")
    parser.add_argument("--max-workers", type=int, default=4, help="Concurrent threads (default: 4)")
    parser.add_argument("--app-name", default="bronze-air-quality")
    return parser.parse_args()


def collect_air_quality_data(facilities: List[FacilityLocation], args: argparse.Namespace) -> pd.DataFrame:
    limiter = RateLimiter(30.0)  # 30 requests/minute for free API
    frames: List[pd.DataFrame] = []

    def fetch_for_facility(facility: FacilityLocation) -> pd.DataFrame:
        print(f"Fetching air quality: {facility.code} ({facility.name})")
        return openmeteo.fetch_air_quality_dataframe(
            facility,
            start=args.start,
            end=args.end,
            chunk_days=openmeteo.AIR_QUALITY_MAX_DAYS,  # 14 days max
            hourly_variables=openmeteo.DEFAULT_AIR_VARS,
            timezone="UTC",
            limiter=limiter,
            max_retries=openmeteo.DEFAULT_MAX_RETRIES,
            retry_backoff=openmeteo.DEFAULT_RETRY_BACKOFF,
            max_workers=args.max_workers,
        )

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(fetch_for_facility, facility): facility for facility in facilities}
        for future in as_completed(futures):
            facility = futures[future]
            try:
                frame = future.result()
            except Exception as exc:  # pragma: no cover - defensive
                print(f"Failed to fetch air quality data for facility {facility.code}: {exc}")
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
    facilities = openmeteo_common.load_facility_locations(facility_codes, args.api_key)

    air_df = collect_air_quality_data(facilities, args)
    if air_df.empty:
        print("No Open-Meteo air quality data retrieved; nothing to write.")
        return

    spark = create_spark_session(args.app_name)
    ingest_ts = F.current_timestamp()
    ingest_date = today.isoformat()

    air_spark_df = spark.createDataFrame(air_df)
    air_spark_df = (
        air_spark_df.withColumn("ingest_mode", F.lit(args.mode))
        .withColumn("ingest_timestamp", ingest_ts)
        .withColumn("air_timestamp", F.to_timestamp("date"))
        .withColumn("air_date", F.to_date("air_timestamp"))
    )
    air_spark_df = air_spark_df.filter(F.col("air_timestamp").isNotNull() & (F.col("air_timestamp") <= ingest_ts))

    openmeteo_common.write_dataset(
        air_spark_df,
        s3_base_path=S3_AIR_BASE,
        iceberg_table=ICEBERG_AIR_TABLE,
        mode=args.mode,
        ingest_date=ingest_date,
        label="air-quality",
    )

    spark.stop()


if __name__ == "__main__":
    main()
