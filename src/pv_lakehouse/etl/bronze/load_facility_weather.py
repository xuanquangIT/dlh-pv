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
from pv_lakehouse.etl.bronze.facility_timezones import get_facility_timezone
from pv_lakehouse.etl.clients import openmeteo
from pv_lakehouse.etl.clients.openmeteo import FacilityLocation, RateLimiter
from pv_lakehouse.etl.utils import (
    load_facility_locations,
    parse_date_argparse,
    resolve_facility_codes,
)
from pv_lakehouse.etl.utils.spark_utils import create_spark_session

ICEBERG_WEATHER_TABLE = "lh.bronze.raw_facility_weather"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for weather loader."""
    parser = argparse.ArgumentParser(description="Load facility weather data into Bronze zone")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument(
        "--facility-codes",
        help="Comma-separated facility codes (default: all solar facilities)",
    )
    parser.add_argument(
        "--start",
        type=parse_date_argparse,
        help="Start date YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--end",
        type=parse_date_argparse,
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument("--api-key", help="Override OpenElectricity API key")
    parser.add_argument("--max-workers", type=int, default=4, help="Concurrent threads (default: 4)")
    parser.add_argument("--app-name", default="bronze-weather")
    return parser.parse_args()


def collect_weather_data(
    facilities: List[FacilityLocation],
    args: argparse.Namespace,
) -> pd.DataFrame:
    """Collect weather data for all facilities using concurrent API calls."""
    limiter = RateLimiter(30.0)  # 30 requests/minute for free API
    frames: List[pd.DataFrame] = []

    def fetch_for_facility(facility: FacilityLocation) -> pd.DataFrame:
        print(f"Fetching weather: {facility.code} ({facility.name})")
        facility_tz = get_facility_timezone(facility.code)
        return openmeteo.fetch_weather_dataframe(
            facility,
            start=args.start,
            end=args.end,
            chunk_days=30,  # 30 days per chunk for archive API
            hourly_variables=openmeteo.DEFAULT_WEATHER_VARS,
            endpoint_preference="auto",
            timezone=facility_tz,  # Request in facility's local timezone
            limiter=limiter,
            max_retries=openmeteo.DEFAULT_MAX_RETRIES,
            retry_backoff=openmeteo.DEFAULT_RETRY_BACKOFF,
            max_workers=args.max_workers,
        )

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(fetch_for_facility, facility): facility
            for facility in facilities
        }
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
    """Main entry point for weather loader."""
    args = parse_args()

    today = dt.date.today()

    # Auto-detect start date for incremental mode (hour-level granularity)
    if args.mode == "incremental" and args.start is None:
        spark = create_spark_session(args.app_name)
        try:
            max_ts = spark.sql(
                f"SELECT MAX(weather_timestamp) FROM {ICEBERG_WEATHER_TABLE}"
            ).collect()[0][0]
            if max_ts:
                args.start = max_ts.date()
                print(
                    f"Incremental mode: Reloading from {args.start} "
                    f"to catch new hours (last loaded: {max_ts})"
                )
            else:
                args.start = today - dt.timedelta(days=1)
                print(f"Incremental mode: No existing data, loading from {args.start}")
        except Exception as e:
            print(f"Warning: Could not detect last loaded timestamp: {e}")
            args.start = today - dt.timedelta(days=1)
        finally:
            spark.stop()
            spark = None  # Will recreate later
    else:
        args.start = args.start or (today - dt.timedelta(days=1))

    args.end = args.end or today

    if args.end < args.start:
        raise SystemExit("End date must not be before start date")

    facility_codes = resolve_facility_codes(args.facility_codes)
    facilities = load_facility_locations(facility_codes, args.api_key)

    weather_df = collect_weather_data(facilities, args)
    if weather_df.empty:
        print("No Open-Meteo weather data retrieved; nothing to write.")
        return

    # Avoid an expensive full-DataFrame pandas NaN->NULL conversion on the driver.
    # Create the Spark DataFrame first and replace NaNs using Spark (distributed),
    spark = create_spark_session(args.app_name)
    ingest_ts = F.current_timestamp()

    weather_spark_df = spark.createDataFrame(weather_df)
    # Replace NaN in water vapour and boundary layer columns with NULL using Spark (distributed).
    # These columns commonly return NaN values from Open-Meteo API.
    nan_columns = ["total_column_integrated_water_vapour", "boundary_layer_height"]
    for col_name in nan_columns:
        if col_name in weather_spark_df.columns:
            weather_spark_df = weather_spark_df.withColumn(
                col_name,
                F.when(F.isnan(F.col(col_name)), F.lit(None))
                 .otherwise(F.col(col_name))
            )
    # Create facility_tz column for timezone conversion
    weather_spark_df = (
        weather_spark_df.withColumn("ingest_mode", F.lit(args.mode))
        .withColumn("ingest_timestamp", ingest_ts)
        # API returns date in facility LOCAL timezone - store as-is (no conversion needed)
        # This keeps data consistent with original API timestamps
        .withColumn(
            "weather_timestamp",
            F.to_timestamp(F.col("date"))
        )
        .withColumn("weather_date", F.to_date("weather_timestamp"))
        .filter(F.col("weather_timestamp").isNotNull() & (F.col("weather_timestamp") <= ingest_ts))
    )

    # Use Iceberg MERGE for both insert and deduplication (keep latest record only)
    weather_spark_df.createOrReplaceTempView("weather_source")
    
    merge_sql = f"""
    MERGE INTO {ICEBERG_WEATHER_TABLE} AS target
    USING (
        SELECT * FROM (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY facility_code, weather_timestamp ORDER BY ingest_timestamp DESC) as rn
            FROM weather_source
        ) WHERE rn = 1
    ) AS source
    ON target.facility_code = source.facility_code AND target.weather_timestamp = source.weather_timestamp
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    
    try:
        spark.sql(merge_sql)
        print(f"MERGE completed for {ICEBERG_WEATHER_TABLE}")
    except Exception as e:
        print(f"MERGE failed: {e}. Falling back to append...")
        openmeteo_common.write_dataset(
            weather_spark_df,
            iceberg_table=ICEBERG_WEATHER_TABLE,
            mode="append",
            label="weather",
        )

    spark.stop()


if __name__ == "__main__":
    main()
