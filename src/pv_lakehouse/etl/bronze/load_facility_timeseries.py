#!/usr/bin/env python3
"""Bronze ingestion job for OpenElectricity facility timeseries."""
from __future__ import annotations
import argparse
import datetime as dt
import logging
from typing import List
from pyspark.sql import functions as F
from pv_lakehouse.etl.clients import openelectricity
from pv_lakehouse.etl.utils import resolve_facility_codes
from pv_lakehouse.etl.utils.spark_utils import (
    create_spark_session,
    write_iceberg_table,
)

LOGGER = logging.getLogger(__name__)
ICEBERG_TABLE = "lh.bronze.raw_facility_timeseries"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for timeseries loader."""
    parser = argparse.ArgumentParser(
        description="Load OpenElectricity facility timeseries into Bronze zone"
    )
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument(
        "--facility-codes",
        help="Comma-separated facility codes (default: all solar facilities)",
    )
    parser.add_argument("--date-start", help="Start timestamp (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--date-end", help="End timestamp (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--api-key", help="Override API key")
    parser.add_argument("--app-name", default="bronze-timeseries")
    return parser.parse_args()


def main() -> None:
    """Main entry point for timeseries loader."""
    args = parse_args()

    facility_codes = resolve_facility_codes(args.facility_codes)

    # Auto-detect start datetime for incremental mode
    if args.mode == "incremental" and args.date_start is None:
        spark = create_spark_session(args.app_name)
        try:
            max_ts = spark.sql(f"SELECT MAX(interval_ts) FROM {ICEBERG_TABLE}").collect()[0][0]
            if max_ts:
                # Start from 1 hour after last loaded data
                next_hour = max_ts + dt.timedelta(hours=1)
                args.date_start = next_hour.strftime("%Y-%m-%dT%H:%M:%S")
                args.date_end = args.date_end or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
                print(f"Incremental mode: Loading from {args.date_start} (last loaded: {max_ts})")
            else:
                print("Incremental mode: No existing data, using default lookback")
        except Exception as e:
            print(f"Warning: Could not detect last loaded timestamp: {e}")
        finally:
            spark.stop()

    # Note: OpenElectricity API handles timezone conversion internally
    # Just pass datetime strings as-is
    if args.date_start and args.date_end:
        print(f"Loading timeseries: {args.date_start} → {args.date_end}")

    # Try to fetch data from API, skip facility if data unavailable or no permissions
    dataframe = None
    last_error = None
    skipped_facilities = []
    
    for facility_code in facility_codes:
        try:
            print(f"Fetching data for facility: {facility_code}")
            facility_df = openelectricity.fetch_facility_timeseries_dataframe(
                facility_codes=[facility_code],
                metrics=["energy"],  
                interval="1h", 
                date_start=args.date_start,
                date_end=args.date_end,
                api_key=args.api_key,
                target_window_days=7,
                max_lookback_windows=52,
            )
            
            if not facility_df.empty:
                if dataframe is None:
                    dataframe = facility_df
                else:
                    dataframe = __import__('pandas').concat([dataframe, facility_df], ignore_index=True)
                LOGGER.info("Loaded %d records for %s", len(facility_df), facility_code)
            else:
                LOGGER.info("No data returned for %s", facility_code)
                
        except Exception as e:
            error_msg = str(e)
            # Skip facility if API returns 403 (Forbidden/No permissions) or 416 (no data available)
            if "403" in error_msg or "Forbidden" in error_msg:
                LOGGER.warning("No access to %s (403 Forbidden). Skipping...", facility_code)
                skipped_facilities.append((facility_code, "403 Forbidden"))
                last_error = e
            elif "416" in error_msg or "Range Not Satisfiable" in error_msg:
                LOGGER.warning("No data available for %s in date range (416). Skipping...", facility_code)
                skipped_facilities.append((facility_code, "416 No data"))
                last_error = e
            else:
                LOGGER.error("API call failed for facility %s: %s", facility_code, error_msg, exc_info=True)
                last_error = e
                # Re-raise original exception to preserve full stack trace
                raise
    
    # Log summary of skipped facilities
    if skipped_facilities:
        LOGGER.warning("Skipped %d facilities:", len(skipped_facilities))
        for code, reason in skipped_facilities:
            LOGGER.warning("  - %s: %s", code, reason)

    if dataframe is None or dataframe.empty:
        LOGGER.warning("No timeseries records returned for any facility; skipping writes.")
        if last_error:
            LOGGER.warning("Last error: %s", last_error)
        return

    spark = create_spark_session(args.app_name)

    spark_df = spark.createDataFrame(dataframe)
    spark_df = (
        spark_df.withColumn("ingest_mode", F.lit(args.mode))
        .withColumn("ingest_timestamp", F.current_timestamp())
    )
    
    spark_df = spark_df.withColumn(
        "interval_ts",
        F.to_timestamp(F.col("interval_start"))
    )
    spark_df = spark_df.withColumn("interval_date", F.to_date("interval_ts"))
    
    if args.mode == "backfill":
        # Backfill: overwrite entire table with deduplication (keep latest per key)
        spark_df.createOrReplaceTempView("timeseries_source")
        
        # Deduplicate: keep latest record per (facility_code, interval_ts, metric)
        dedup_sql = f"""
        INSERT OVERWRITE TABLE {ICEBERG_TABLE}
        SELECT * FROM (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY facility_code, interval_ts, metric ORDER BY ingest_timestamp DESC) as rn
            FROM timeseries_source
        ) WHERE rn = 1
        """
        
        try:
            spark.sql(dedup_sql)
            print(f"Wrote deduplicated records to {ICEBERG_TABLE} (mode=overwrite)")
        except Exception as e:
            print(f"INSERT OVERWRITE with dedup failed: {e}. Falling back to simple overwrite...")
            write_iceberg_table(spark_df, ICEBERG_TABLE, mode="overwrite")
        
    else:  # incremental - use MERGE INTO for upsert with deduplication
        spark_df.createOrReplaceTempView("timeseries_source")
        
        # MERGE with deduplication: keep latest record per (facility_code, interval_ts, metric)
        merge_sql = f"""
        MERGE INTO {ICEBERG_TABLE} AS target
        USING (
            SELECT * FROM (
                SELECT *,
                ROW_NUMBER() OVER (PARTITION BY facility_code, interval_ts, metric ORDER BY ingest_timestamp DESC) as rn
                FROM timeseries_source
            ) WHERE rn = 1
        ) AS source
        ON target.facility_code = source.facility_code 
            AND target.interval_ts = source.interval_ts 
            AND target.metric = source.metric
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        try:
            spark.sql(merge_sql)
            print(f"Merged deduplicated records into {ICEBERG_TABLE}")
        except Exception as e:
            print(f"MERGE failed: {e}. Falling back to append...")
            write_iceberg_table(spark_df, ICEBERG_TABLE, mode="append")
            print(f"Appended {spark_df.count()} rows to {ICEBERG_TABLE}")

    spark.stop()


if __name__ == "__main__":
    main()
