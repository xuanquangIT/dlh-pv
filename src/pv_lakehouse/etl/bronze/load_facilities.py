#!/usr/bin/env python3
"""Bronze ingestion job for OpenElectricity facility metadata."""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from pyspark.sql import functions as F

from pv_lakehouse.etl.clients import openelectricity
from pv_lakehouse.etl.utils.spark_utils import create_spark_session, write_iceberg_table

S3_OUTPUT_BASE = "s3a://lakehouse/bronze/openelectricity/facilities"
ICEBERG_TABLE = "lh.bronze.openelectricity_facilities"

DEFAULT_FACILITY_CODES = [
    "NYNGAN",
    "COLEASF",
    "BNGSF1",
    "CLARESF",
    "GANNSF",
]


def parse_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    parts = [item.strip() for item in value.split(",")]
    return [item for item in parts if item]


def load_codes_from_csv(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Facilities CSV not found: {path}")
    frame = pd.read_csv(path, usecols=["facility_code"])
    codes = frame["facility_code"].dropna().astype(str).str.strip().unique().tolist()
    if not codes:
        raise ValueError(f"No facility codes found in {path}")
    return sorted(codes)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load OpenElectricity facility metadata into Bronze zone")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument(
        "--facility-codes",
        help="Comma separated facility codes to filter (defaults to core solar facilities if omitted)",
    )
    parser.add_argument(
        "--facilities-csv",
        type=Path,
        help="Optional CSV containing facility_code column used when --facility-codes is omitted",
    )
    parser.add_argument("--networks", default="NEM,WEM", help="Comma separated network_id filters")
    parser.add_argument("--statuses", default="operating", help="Comma separated status_id filters")
    parser.add_argument("--fueltechs", default="solar_utility", help="Comma separated fueltech_id filters")
    parser.add_argument("--region", help="Optional network_region filter")
    parser.add_argument("--api-key", help="Override API key (otherwise env/.env)")
    parser.add_argument(
        "--s3-base-path",
        default=S3_OUTPUT_BASE,
        help="Root path in MinIO/OBS for Bronze data",
    )
    parser.add_argument(
        "--iceberg-table",
        default=ICEBERG_TABLE,
        help="Fully qualified Iceberg table name",
    )
    parser.add_argument("--app-name", default="bronze-openelectricity-facilities")
    return parser.parse_args()


def resolve_facility_codes(args: argparse.Namespace) -> List[str]:
    codes = parse_csv(args.facility_codes)
    if codes:
        return [code.upper() for code in codes]
    if args.facilities_csv:
        return load_codes_from_csv(args.facilities_csv)
    return list(DEFAULT_FACILITY_CODES)


def main() -> None:
    args = parse_args()

    selected_codes = resolve_facility_codes(args)
    network_filters = parse_csv(args.networks) or ["NEM", "WEM"]
    status_filters = parse_csv(args.statuses) or ["operating"]
    fueltech_filters = parse_csv(args.fueltechs) or ["solar_utility"]

    facilities_df = openelectricity.fetch_facilities_dataframe(
        api_key=args.api_key,
        selected_codes=selected_codes or None,
        networks=network_filters,
        statuses=status_filters,
        fueltechs=fueltech_filters,
        region=args.region,
    )

    if facilities_df.empty:
        print("No facility metadata returned; skipping writes.")
        return

    spark = create_spark_session(args.app_name)

    spark_df = spark.createDataFrame(facilities_df)
    spark_df = (
        spark_df.withColumn("ingest_mode", F.lit(args.mode))
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_timestamp"))
    )

    ingest_date = dt.date.today().isoformat()
    write_mode = "overwrite" if args.mode == "backfill" else "append"

    s3_target = f"{args.s3_base_path}/ingest_date={ingest_date}"
    (
        spark_df.write.mode(write_mode)
        .format("parquet")
        .option("compression", "snappy")
        .save(s3_target)
    )
    print(f"Wrote facilities parquet to {s3_target}")

    write_iceberg_table(spark_df, args.iceberg_table, mode="overwrite" if args.mode == "backfill" else "append")
    print(f"Upserted facilities into Iceberg table {args.iceberg_table}")

    spark.stop()


if __name__ == "__main__":
    main()
