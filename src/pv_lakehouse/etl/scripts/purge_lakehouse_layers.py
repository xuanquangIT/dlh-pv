#!/usr/bin/env python3
"""Utility script to purge Bronze/Silver/Gold data from the Lakehouse."""

from __future__ import annotations

import argparse
import datetime as dt
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, LongType, StringType, TimestampType

from pv_lakehouse.etl.utils.spark_utils import create_spark_session

SUPPORTED_LAYERS: tuple[str, ...] = ("bronze", "silver", "gold")

S3_PREFIXES: Dict[str, List[str]] = {
    "bronze": [
        "s3a://lakehouse/bronze/",
        "s3a://lakehouse/iceberg/warehouse/bronze",
    ],
    "silver": [
        "s3a://lakehouse/silver/",
        "s3a://lakehouse/iceberg/warehouse/silver",
    ],
    "gold": [
        "s3a://lakehouse/gold/",
        "s3a://lakehouse/iceberg/warehouse/gold",
    ],
}


@dataclass(frozen=True)
class DateRange:
    start: dt.date
    end: dt.date


DATE_DIR_PATTERN = re.compile(r"(20\\d{2}-\\d{2}-\\d{2})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Permanently delete Lakehouse Bronze/Silver/Gold data")
    parser.add_argument(
        "--layers",
        default=",".join(SUPPORTED_LAYERS),
        help="Comma separated subset of layers to purge (default: bronze,silver,gold)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt. Required for destructive execution.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print actions without deleting tables or files.",
    )
    parser.add_argument(
        "--app-name",
        default="tools-purge-lakehouse",
        help="Spark application name to use for the purge job.",
    )
    parser.add_argument("--start-date", type=_parse_date, help="Start date (YYYY-MM-DD) for selective purge")
    parser.add_argument("--end-date", type=_parse_date, help="End date (YYYY-MM-DD) for selective purge")
    return parser.parse_args()


def _parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - invalid user input
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Expected YYYY-MM-DD") from exc


def normalise_layers(raw_layers: str) -> List[str]:
    layers = [token.strip().lower() for token in (raw_layers or "").split(",") if token.strip()]
    if not layers:
        return list(SUPPORTED_LAYERS)
    invalid = [layer for layer in layers if layer not in SUPPORTED_LAYERS]
    if invalid:
        raise SystemExit(f"Unsupported layer(s): {', '.join(invalid)}. Choose from {SUPPORTED_LAYERS}")
    # Preserve requested order but remove duplicates while keeping first occurrence
    seen = set()
    ordered: List[str] = []
    for layer in layers:
        if layer not in seen:
            ordered.append(layer)
            seen.add(layer)
    return ordered


def build_date_range(args: argparse.Namespace) -> Optional[DateRange]:
    if args.start_date and args.end_date:
        if args.end_date < args.start_date:
            raise SystemExit("End date must not be before start date")
        return DateRange(args.start_date, args.end_date)
    if args.start_date or args.end_date:
        raise SystemExit("Both --start-date and --end-date must be provided to use selective purge")
    return None


def confirm(force: bool, layers: Iterable[str], date_range: Optional[DateRange]) -> None:
    if force:
        return
    if date_range:
        prompt = (
            "This will delete data between "
            f"{date_range.start.isoformat()} and {date_range.end.isoformat()} for layers: "
            f"{', '.join(layers)}.\nType 'purge' to continue: "
        )
    else:
        prompt = (
            "This will permanently delete all Bronze/Silver/Gold data for the following layers: "
            f"{', '.join(layers)}.\nType 'purge' to continue: "
        )
    response = input(prompt)  # noqa: S322 - deliberate confirmation
    if response.strip().lower() != "purge":
        raise SystemExit("Aborted by user.")


def drop_namespace_tables(
    spark: SparkSession,
    namespace: str,
    *,
    dry_run: bool,
    date_range: Optional[DateRange],
) -> None:
    action = "Deleting rows" if date_range else "Dropping Iceberg tables"
    print(f"{action} in namespace {namespace} ...")
    tables = spark.sql(f"SHOW TABLES IN {namespace}").collect()
    if not tables:
        print(f"  No tables found in {namespace}.")
        return
    for row in tables:
        table_name = row.tableName
        full_name = f"{namespace}.{table_name}"
        purge_table(spark, full_name, dry_run=dry_run, date_range=date_range)


def delete_s3_prefix(
    spark: SparkSession,
    prefix: str,
    *,
    dry_run: bool,
    date_range: Optional[DateRange],
) -> None:
    print(f"Removing data at {prefix} ...")
    if date_range and "iceberg/warehouse" in prefix:
        print(
            "  Skipping direct S3 deletes for Iceberg warehouse when using a date range; "
            "Iceberg DELETE statements handle data removal."
        )
        return

    if dry_run:
        if date_range:
            print(
                f"  [dry-run] Would delete date-partitioned paths between "
                f"{date_range.start} and {date_range.end} under {prefix}"
            )
        else:
            print(f"  [dry-run] Would delete prefix {prefix}")
        return

    jvm = spark._jvm  # type: ignore[attr-defined]
    conf = spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    path = jvm.org.apache.hadoop.fs.Path(prefix)
    try:
        fs = path.getFileSystem(conf)
        if not fs.exists(path):
            print(f"  Path {prefix} does not exist")
            return

        if date_range:
            delete_partitioned_paths(fs, path, date_range)
            return

        if fs.delete(path, True):
            print(f"  Deleted {prefix}")
        else:
            print(f"  Failed to delete {prefix}")
    except Exception as error:  # pylint: disable=broad-except
        print(f"  Error deleting {prefix}: {error}")


def delete_partitioned_paths(fs, base_path, date_range: DateRange) -> None:
    statuses = fs.listStatus(base_path)
    if not statuses:
        print(f"  No subdirectories found under {base_path}")
        return

    for status in statuses:
        if not status.isDirectory():
            continue
        sub_path = status.getPath()
        name = sub_path.getName()
        match = DATE_DIR_PATTERN.search(name)
        if not match:
            continue
        part_date = dt.date.fromisoformat(match.group(1))
        if date_range.start <= part_date <= date_range.end:
            if fs.delete(sub_path, True):
                print(f"  Deleted {sub_path}" )
            else:
                print(f"  Failed to delete {sub_path}")


def purge_table(
    spark: SparkSession,
    full_name: str,
    *,
    dry_run: bool,
    date_range: Optional[DateRange],
) -> None:
    if date_range is None:
        if dry_run:
            print(f"  [dry-run] Would drop table {full_name}")
            return
        print(f"  Dropping table {full_name}")
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        except Exception as error:  # pylint: disable=broad-except
            message = str(error)
            if "Failed to open input stream" in message or "NotFoundException" in message:
                print(f"  Metadata missing for {full_name}; skipping explicit DROP ({message.splitlines()[0]})")
                return
            raise
        return

    try:
        schema = spark.table(full_name).schema
    except Exception as error:  # pylint: disable=broad-except
        print(f"  Skipping {full_name}: unable to inspect schema ({error})")
        return

    condition = select_date_condition(schema, date_range)
    if not condition:
        print(f"  Skipping {full_name}: no suitable date/timestamp column for selective purge")
        return

    delete_sql = (
        f"DELETE FROM {full_name} WHERE {condition}"
    )

    if dry_run:
        print(f"  [dry-run] Would execute: {delete_sql}")
        return

    print(f"  Executing selective delete on {full_name}")
    spark.sql(delete_sql)


def select_date_condition(schema, date_range: DateRange) -> Optional[str]:
    fields_by_lower = {field.name.lower(): field for field in schema}

    preferred_names = [
        "ingest_date",
        "ingest_timestamp",
        "weather_date",
        "air_date",
        "date",
        "full_date",
        "event_date",
        "event_timestamp",
        "created_at",
        "updated_at",
        "date_hour",
        "date_key",
    ]

    for name in preferred_names:
        field = fields_by_lower.get(name)
        if field:
            condition = build_condition_for_field(field, date_range)
            if condition:
                return condition

    for field in schema:
        condition = build_condition_for_field(field, date_range)
        if condition:
            return condition
    return None


def build_condition_for_field(field, date_range: DateRange) -> Optional[str]:
    column = f"`{field.name}`"
    start_literal = f"DATE '{date_range.start.isoformat()}'"
    end_literal = f"DATE '{date_range.end.isoformat()}'"

    data_type = field.dataType
    if isinstance(data_type, DateType):
        return f"{column} BETWEEN {start_literal} AND {end_literal}"
    if isinstance(data_type, TimestampType):
        return f"CAST({column} AS DATE) BETWEEN {start_literal} AND {end_literal}"
    if isinstance(data_type, StringType):
        return f"TO_DATE({column}) BETWEEN {start_literal} AND {end_literal}"
    if isinstance(data_type, (IntegerType, LongType)):
        start_key = int(date_range.start.strftime("%Y%m%d"))
        end_key = int(date_range.end.strftime("%Y%m%d"))
        return f"{column} BETWEEN {start_key} AND {end_key}"
    return None


def purge_layers(
    spark: SparkSession,
    layers: Iterable[str],
    *,
    dry_run: bool,
    date_range: Optional[DateRange],
) -> None:
    for layer in layers:
        namespace = f"lh.{layer}"
        drop_namespace_tables(spark, namespace, dry_run=dry_run, date_range=date_range)
        for prefix in S3_PREFIXES.get(layer, []):
            delete_s3_prefix(spark, prefix, dry_run=dry_run, date_range=date_range)


def main() -> None:
    args = parse_args()
    layers = normalise_layers(args.layers)
    date_range = build_date_range(args)
    confirm(args.force, layers, date_range)

    spark = create_spark_session(args.app_name)
    try:
        purge_layers(spark, layers, dry_run=args.dry_run, date_range=date_range)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
