#!/usr/bin/env python3
"""Bronze ingestion job for OpenElectricity facility timeseries."""

from __future__ import annotations

import argparse
import datetime as dt
from typing import List, Optional

from pyspark.sql import functions as F

from pv_lakehouse.etl.clients import openelectricity
from pv_lakehouse.etl.utils.spark_utils import (
    create_spark_session,
    write_iceberg_table,
)

ICEBERG_TABLE = "lh.bronze.raw_facility_timeseries"


def parse_csv(value: Optional[str]) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load OpenElectricity facility timeseries into Bronze zone")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--facility-codes", help="Comma-separated facility codes (default: all solar facilities)")
    parser.add_argument("--date-start", help="Start timestamp (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--date-end", help="End timestamp (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--api-key", help="Override API key")
    parser.add_argument("--app-name", default="bronze-timeseries")
    return parser.parse_args()


def resolve_facility_codes(args: argparse.Namespace) -> List[str]:
    codes = parse_csv(args.facility_codes)
    if codes:
        return [code.upper() for code in codes]
    return openelectricity.load_default_facility_codes()


def adjust_utc_to_brisbane(utc_datetime_str: Optional[str]) -> Optional[str]:
    """
    Convert UTC datetime string to Brisbane local time (UTC+10) for OpenElectricity API.
    
    OpenElectricity API interprets datetime parameters in network local time.
    For NEM (Brisbane), we need to add 10 hours to UTC time.
    
    Example:
        Input:  "2025-10-01T00:00:00" (intended as UTC)
        Output: "2025-10-01T10:00:00" (Brisbane time, equals 2025-10-01 00:00 UTC)
    """
    if not utc_datetime_str:
        return None
    
    # Parse the UTC datetime
    utc_dt = dt.datetime.fromisoformat(utc_datetime_str)
    
    # Add 10 hours offset for Brisbane (UTC+10)
    brisbane_dt = utc_dt + dt.timedelta(hours=10)
    
    # Return in the same format
    return brisbane_dt.strftime("%Y-%m-%dT%H:%M:%S")


def main() -> None:
    args = parse_args()

    facility_codes = resolve_facility_codes(args)

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

    # Convert UTC datetime to Brisbane timezone for OpenElectricity API
    # API interprets datetime as network local time (Brisbane = UTC+10)
    adjusted_start = adjust_utc_to_brisbane(args.date_start)
    adjusted_end = adjust_utc_to_brisbane(args.date_end)
    
    if adjusted_start and adjusted_end:
        print(f"UTC request: {args.date_start} → {args.date_end}")
        print(f"Brisbane time (API): {adjusted_start} → {adjusted_end}")

    dataframe = openelectricity.fetch_facility_timeseries_dataframe(
        facility_codes=facility_codes,
        metrics=["energy"],  
        interval="1h", 
        date_start=adjusted_start,
        date_end=adjusted_end,
        api_key=args.api_key,
        target_window_days=7,
        max_lookback_windows=52,
    )

    if dataframe.empty:
        print("No timeseries records returned; skipping writes.")
        return

    spark = create_spark_session(args.app_name)

    spark_df = spark.createDataFrame(dataframe)
    spark_df = (
        spark_df.withColumn("ingest_mode", F.lit(args.mode))
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("interval_ts", F.to_timestamp("interval_start"))
        .withColumn("interval_date", F.to_date("interval_ts"))
    )

    if args.mode == "backfill":
        # Backfill: overwrite entire table
        write_iceberg_table(spark_df, ICEBERG_TABLE, mode="overwrite")
        row_count = spark_df.count()
        print(f"Wrote {row_count} rows to {ICEBERG_TABLE} (mode=overwrite)")
        
    else:  # incremental - use MERGE INTO for upsert
        # Create temp view for MERGE INTO
        temp_view = "temp_timeseries_data"
        spark_df.createOrReplaceTempView(temp_view)
        
        # Get all column names
        columns = spark_df.columns
        update_set = ", ".join([f"target.{col} = source.{col}" for col in columns])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"source.{col}" for col in columns])
        
        try:
            # Iceberg MERGE INTO: upsert based on (facility_code, interval_ts, metric)
            merge_sql = f"""
            MERGE INTO {ICEBERG_TABLE} AS target
            USING {temp_view} AS source
            ON target.facility_code = source.facility_code 
                AND target.interval_ts = source.interval_ts 
                AND target.metric = source.metric
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
            """
            
            print("Executing MERGE INTO for timeseries...")
            spark.sql(merge_sql)
            
            row_count = spark_df.count()
            print(f"Merged {row_count} rows into {ICEBERG_TABLE} (upsert mode)")
            
        except Exception as e:
            print(f"MERGE INTO failed (table may not exist): {e}")
            print("Falling back to append mode for first load...")
            write_iceberg_table(spark_df, ICEBERG_TABLE, mode="append")
            row_count = spark_df.count()
            print(f"Appended {row_count} rows to {ICEBERG_TABLE}")

    spark.stop()


if __name__ == "__main__":
    main()
