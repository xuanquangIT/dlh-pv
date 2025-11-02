#!/usr/bin/env python3
"""Bronze ingestion job for OpenElectricity facility metadata."""

from __future__ import annotations

import argparse
import datetime as dt
from typing import List, Optional

from pyspark.sql import functions as F

from pv_lakehouse.etl.clients import openelectricity
from pv_lakehouse.etl.utils.spark_utils import create_spark_session, write_iceberg_table

ICEBERG_TABLE = "lh.bronze.raw_facilities"


def parse_csv(value: Optional[str]) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load facility metadata into Bronze zone")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--facility-codes", help="Comma-separated facility codes (default: all solar facilities)")
    parser.add_argument("--api-key", help="Override API key")
    parser.add_argument("--app-name", default="bronze-facilities")
    return parser.parse_args()


def resolve_facility_codes(args: argparse.Namespace) -> List[str]:
    codes = parse_csv(args.facility_codes)
    if codes:
        return [code.upper() for code in codes]
    return openelectricity.load_default_facility_codes()


def main() -> None:
    args = parse_args()

    selected_codes = resolve_facility_codes(args)

    facilities_df = openelectricity.fetch_facilities_dataframe(
        api_key=args.api_key,
        selected_codes=selected_codes or None,
        networks=["NEM", "WEM"],  
        statuses=["operating"],  
        fueltechs=["solar_utility"],  
        region=None,
    )

    if facilities_df.empty:
        print("No facility metadata returned; skipping writes.")
        return

    spark = create_spark_session(args.app_name)

    spark_df = spark.createDataFrame(facilities_df, schema=openelectricity.FACILITY_SCHEMA)
    spark_df = (
        spark_df.withColumn("ingest_mode", F.lit(args.mode))
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_timestamp"))
    )

    # Facilities metadata: always overwrite (master data)
    write_iceberg_table(spark_df, ICEBERG_TABLE, mode="overwrite")
    row_count = spark_df.count()
    print(f"Wrote {row_count} rows to {ICEBERG_TABLE} (mode=overwrite)")

    spark.stop()


if __name__ == "__main__":
    main()
