#!/usr/bin/env python3
"""Bronze ingestion job for OpenElectricity facility timeseries."""

from __future__ import annotations

import argparse
import datetime as dt
from typing import List, Optional

import pandas as pd
from pyspark.sql import functions as F

from pv_lakehouse.etl.clients import openelectricity
from pv_lakehouse.etl.utils.spark_utils import (
    create_spark_session,
    register_iceberg_table_with_trino,
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
    parser.add_argument(
        "--facility-codes",
        help="Comma separated facility codes to fetch (defaults to core solar facilities if omitted)",
    )
    parser.add_argument("--metrics", default="power,energy,market_value,price", help="Comma separated metrics")
    parser.add_argument("--interval", default="1h", help="Data interval (5m,1h,1d,...) ")
    parser.add_argument("--date-start", help="Start timestamp (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--date-end", help="End timestamp (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--api-key", help="Override API key (otherwise env/.env)")
    parser.add_argument("--target-window-days", type=int, default=7)
    parser.add_argument("--max-lookback-windows", type=int, default=52)
    parser.add_argument("--s3-base-path", default=S3_OUTPUT_BASE)
    parser.add_argument("--iceberg-table", default=ICEBERG_TABLE)
    parser.add_argument("--app-name", default="bronze-openelectricity-timeseries")
    return parser.parse_args()


def resolve_facility_codes(args: argparse.Namespace) -> List[str]:
    codes = parse_csv(args.facility_codes)
    if codes:
        return [code.upper() for code in codes]
    return openelectricity.load_default_facility_codes()


def main() -> None:
    args = parse_args()

    facility_codes = resolve_facility_codes(args)
    metrics = parse_csv(args.metrics)

    dataframe = openelectricity.fetch_facility_timeseries_dataframe(
        facility_codes=facility_codes,
        metrics=metrics or None,
        interval=args.interval,
        date_start=args.date_start,
        date_end=args.date_end,
        api_key=args.api_key,
        target_window_days=args.target_window_days,
        max_lookback_windows=args.max_lookback_windows,
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

    s3_target = f"{args.s3_base_path}/ingest_date={ingest_date}"

    # Remove any stale commit state left from interrupted runs which can break S3 renames
    _delete_s3_path_if_exists(spark, f"{s3_target}/_temporary")
    if write_mode == "overwrite":
        _delete_s3_path_if_exists(spark, s3_target)

    (
        spark_df.write.mode(write_mode)
        .format("parquet")
        .option("compression", "snappy")
        .save(s3_target)
    )
    print(f"Wrote timeseries parquet to {s3_target}")

    write_iceberg_table(
        spark_df,
        args.iceberg_table,
        mode="overwrite" if args.mode == "backfill" else "append",
    )
    print(f"Upserted timeseries into Iceberg table {args.iceberg_table}")

    register_iceberg_table_with_trino(spark, args.iceberg_table)

    spark.stop()


if __name__ == "__main__":
    main()
