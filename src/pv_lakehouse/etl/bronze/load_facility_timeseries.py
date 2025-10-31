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


def _delete_s3_path_if_exists(spark, uri: str) -> None:
    """Delete an S3 path (recursively) if it already exists."""

    jvm = spark._jvm
    path_cls = jvm.org.apache.hadoop.fs.Path
    path = path_cls(uri)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(path.toUri(), spark._jsc.hadoopConfiguration())
    if fs.exists(path):
        fs.delete(path, True)

S3_OUTPUT_BASE = "s3a://lakehouse/bronze/raw_facility_timeseries"
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

    ingest_date = dt.date.today().isoformat()
    write_mode = "overwrite" if args.mode == "backfill" else "append"

    # For incremental mode, deduplicate with existing data
    if args.mode == "incremental":
        try:
            from pyspark.sql.window import Window
            
            # Read existing data from Iceberg table
            existing_df = spark.read.table(ICEBERG_TABLE)
            
            # Union new + existing data
            combined_df = spark_df.unionByName(existing_df, allowMissingColumns=True)
            
            # Deduplicate: keep latest record per (facility_code, interval_ts, metric)
            window_spec = Window.partitionBy("facility_code", "interval_ts", "metric").orderBy(
                F.col("ingest_timestamp").desc()
            )
            deduped_df = (
                combined_df
                .withColumn("_row_num", F.row_number().over(window_spec))
                .filter(F.col("_row_num") == 1)
                .drop("_row_num")
            )
            
            print(f"Deduplicated timeseries: {combined_df.count()} → {deduped_df.count()} rows")
            spark_df = deduped_df
            write_mode = "overwrite"  # Overwrite with deduped data
            
        except Exception as e:
            print(f"Could not read existing table (may not exist yet): {e}")
            # If table doesn't exist, just write new data

    s3_target = f"{S3_OUTPUT_BASE}/ingest_date={ingest_date}"

    # Remove any stale commit state left from interrupted runs
    _delete_s3_path_if_exists(spark, f"{s3_target}/_temporary")
    if write_mode == "overwrite":
        _delete_s3_path_if_exists(spark, s3_target)

    (
        spark_df.write.mode(write_mode)
        .format("parquet")
        .option("compression", "snappy")
        .save(s3_target)
    )
    print(f"Wrote timeseries to {s3_target}")

    write_iceberg_table(spark_df, ICEBERG_TABLE, mode=write_mode)
    print(f"Wrote to Iceberg table {ICEBERG_TABLE} (mode={write_mode})")

    spark.stop()


if __name__ == "__main__":
    main()
