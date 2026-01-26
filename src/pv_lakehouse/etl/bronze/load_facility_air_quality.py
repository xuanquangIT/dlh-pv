#!/usr/bin/env python3
"""Bronze ingestion job for facility-level Open-Meteo air quality data."""
from __future__ import annotations
import argparse
import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
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

LOGGER = logging.getLogger(__name__)
ICEBERG_AIR_TABLE = "lh.bronze.raw_facility_air_quality"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for air quality loader."""
    parser = argparse.ArgumentParser(
        description="Load facility air quality data into Bronze zone"
    )
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
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Concurrent threads (default: 4)",
    )
    parser.add_argument("--app-name", default="bronze-air-quality")
    return parser.parse_args()


def collect_air_quality_data(
    facilities: List[FacilityLocation],
    args: argparse.Namespace,
) -> pd.DataFrame:
    """Collect air quality data for all facilities using concurrent API calls."""
    limiter = RateLimiter(30.0)  # 30 requests/minute for free API
    frames: List[pd.DataFrame] = []

    def fetch_for_facility(facility: FacilityLocation) -> pd.DataFrame:
        LOGGER.info("Fetching air quality: %s (%s)", facility.code, facility.name)
        facility_tz = get_facility_timezone(facility.code)
        return openmeteo.fetch_air_quality_dataframe(
            facility,
            start=args.start,
            end=args.end,
            chunk_days=openmeteo.AIR_QUALITY_MAX_DAYS,  # 14 days max
            hourly_variables=openmeteo.DEFAULT_AIR_VARS,
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
            except (ValueError, RuntimeError, ConnectionError, TimeoutError, OSError) as exc:  # pragma: no cover - defensive
                LOGGER.warning("Failed to fetch air quality data for %s: %s", facility.code, exc)
                continue
            if not frame.empty:
                frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _get_max_timestamp_from_table(spark, table_name: str) -> Optional[dt.datetime]:
    """Safely get max timestamp from Iceberg table with bounds checking.
    
    Args:
        spark: SparkSession instance.
        table_name: Fully qualified table name (constant, not user input).
        
    Returns:
        Max timestamp or None if table is empty or doesn't exist.
    """
    try:
        # Validate table name format to prevent SQL injection
        if not table_name.startswith("lh."):
            raise ValueError(f"Invalid table name format: {table_name}")
        
        # Use string format() instead of f-string for clarity that this is a constant
        query = "SELECT MAX(air_timestamp) FROM {}".format(table_name)
        result = spark.sql(query).collect()
        if result and len(result) > 0 and result[0][0] is not None:
            return result[0][0]
        return None
    except (ValueError, RuntimeError) as e:
        LOGGER.warning("Could not query max timestamp from %s: %s", table_name, e)
        return None


def main() -> None:
    """Main entry point for air quality loader."""
    args = parse_args()

    today = dt.date.today()

    # Auto-detect start date for incremental mode (hour-level granularity)
    if args.mode == "incremental" and args.start is None:
        spark = create_spark_session(args.app_name)
        try:
            max_ts = _get_max_timestamp_from_table(spark, ICEBERG_AIR_TABLE)
            if max_ts:
                args.start = max_ts.date()
                LOGGER.info(
                    "Incremental mode: Reloading from %s to catch new hours (last loaded: %s)",
                    args.start, max_ts
                )
            else:
                args.start = today - dt.timedelta(days=1)
                LOGGER.info("Incremental mode: No existing data, loading from %s", args.start)
        except (ValueError, RuntimeError) as e:
            LOGGER.warning("Could not detect last loaded timestamp: %s", e)
            args.start = today - dt.timedelta(days=1)
        finally:
            spark.stop()
            spark = None  # Will recreate later
    else:
        args.start = args.start or (today - dt.timedelta(days=1))

    args.end = args.end or today

    if args.end < args.start:
        raise SystemExit("End date must not be before start date")

    # Load facility metadata with proper error handling
    try:
        facility_codes = resolve_facility_codes(args.facility_codes)
        facilities = load_facility_locations(facility_codes, args.api_key)
    except (ValueError, RuntimeError) as e:
        LOGGER.error("Failed to load facility metadata: %s", e)
        raise SystemExit(f"Failed to load facility metadata: {e}") from e

    air_df = collect_air_quality_data(facilities, args)
    if air_df.empty:
        LOGGER.info("No Open-Meteo air quality data retrieved; nothing to write.")
        return

    spark = create_spark_session(args.app_name)
    ingest_ts = F.current_timestamp()

    air_spark_df = spark.createDataFrame(air_df)
    air_spark_df = (
        air_spark_df.withColumn("ingest_mode", F.lit(args.mode))
        .withColumn("ingest_timestamp", ingest_ts)
        # API returns date in facility LOCAL timezone - store as-is (no conversion needed)
        # This keeps data consistent with original API timestamps
        .withColumn(
            "air_timestamp",
            F.to_timestamp(F.col("date"))
        )
        .withColumn("air_date", F.to_date("air_timestamp"))
        .filter(F.col("air_timestamp").isNotNull() & (F.col("air_timestamp") <= ingest_ts))
    )

    # Use Iceberg MERGE for both insert and deduplication (keep latest record only)
    air_spark_df.createOrReplaceTempView("air_source")
    
    # Validate table name format to prevent SQL injection
    if not ICEBERG_AIR_TABLE.startswith("lh."):
        raise ValueError(f"Invalid table name format: {ICEBERG_AIR_TABLE}")
    
    # Use string format() for SQL with validated table constant
    merge_sql = """
    MERGE INTO {} AS target
    USING (
        SELECT * FROM (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY facility_code, air_timestamp ORDER BY ingest_timestamp DESC) as rn
            FROM air_source
        ) WHERE rn = 1
    ) AS source
    ON target.facility_code = source.facility_code AND target.air_timestamp = source.air_timestamp
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """.format(ICEBERG_AIR_TABLE)
    
    try:
        spark.sql(merge_sql)
        LOGGER.info("MERGE completed for %s", ICEBERG_AIR_TABLE)
    except (ValueError, RuntimeError) as e:
        LOGGER.warning("MERGE failed: %s. Falling back to append...", e)
        openmeteo_common.write_dataset(
            air_spark_df,
            iceberg_table=ICEBERG_AIR_TABLE,
            mode="append",
            label="air-quality",
        )

    spark.stop()


if __name__ == "__main__":
    main()
