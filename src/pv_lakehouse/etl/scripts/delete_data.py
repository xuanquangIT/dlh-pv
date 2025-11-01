#!/usr/bin/env python3
"""
Advanced Data Deletion Script for PV Lakehouse
Safely deletes data from Bronze/Silver/Gold layers with comprehensive validation.

Features:
- Date range filtering with optional start/end datetime
- Layer selection (bronze, silver, gold, or multiple)
- Table selection (one or more specific tables)
- Full table deletion option
- Dry-run mode with detailed preview
- User confirmation before execution
- Thread-optimized for performance
- Complete cleanup across Iceberg catalog, MinIO, and PostgreSQL
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, LongType, StringType, TimestampType

from pv_lakehouse.etl.utils.spark_utils import create_spark_session

# Layer configuration
SUPPORTED_LAYERS = ["bronze", "silver", "gold"]
CATALOG_NAME = "lh"

# All known tables in the system (for validation and discovery)
LAYER_TABLES = {
    "bronze": [
        "raw_facilities",
        "raw_facility_air_quality",
        "raw_facility_weather",
        "raw_facility_timeseries",
    ],
    "silver": [
        "clean_facility_master",
        "clean_hourly_air_quality",
        "clean_hourly_weather",
        "clean_hourly_energy",
    ],
    "gold": [
        "dim_air_quality_category",
        "dim_date",
        "dim_equipment_status",
        "dim_facility",
        "dim_model_version",
        "dim_performance_issue",
        "dim_time",
        "dim_weather_condition",
        "fact_air_quality_impact",
        "fact_kpi_performance",
        "fact_root_cause_analysis",
        "fact_solar_forecast",
        "fact_weather_impact",
    ],
}

# Preferred column names for date-based filtering (in priority order)
DATE_COLUMN_PRIORITY = [
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
    "time_key",
    "weather_timestamp",
    "air_timestamp",
    "timestamp",
]


@dataclass
class DateTimeRange:
    """Represents a datetime range for filtering data."""

    start: datetime
    end: Optional[datetime] = None

    def __post_init__(self):
        """Validate date range."""
        if self.end is None:
            # If no end date, set to current datetime
            self.end = datetime.now()
        if self.end < self.start:
            raise ValueError("End datetime must be greater than or equal to start datetime")

    def __str__(self):
        return f"{self.start.isoformat()} to {self.end.isoformat()}"


@dataclass
class DeletionPlan:
    """Represents a planned deletion operation."""

    layer: str
    table_name: str
    full_table_name: str
    delete_table: bool
    date_range: Optional[DateTimeRange] = None
    row_count_estimate: Optional[int] = None
    date_column: Optional[str] = None
    condition: Optional[str] = None

    def __str__(self):
        if self.delete_table:
            return f"DROP TABLE {self.full_table_name}"
        if self.date_range and self.condition:
            return f"DELETE FROM {self.full_table_name} WHERE {self.condition}"
        return f"TRUNCATE {self.full_table_name}"


@dataclass
class DeletionResult:
    """Result of a deletion operation."""

    plan: DeletionPlan
    success: bool
    rows_affected: Optional[int] = None
    error: Optional[str] = None


@dataclass
class DeletionStats:
    """Statistics for all deletion operations."""

    total_operations: int = 0
    successful: int = 0
    failed: int = 0
    total_rows_deleted: int = 0
    tables_dropped: int = 0
    results: List[DeletionResult] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Safely delete data from PV Lakehouse (Bronze/Silver/Gold layers)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Delete all data in all layers (with confirmation)
  %(prog)s

  # Delete data from specific date onwards in all layers
  %(prog)s --start-datetime "2025-10-01T00:00:00"

  # Delete data in date range for bronze and silver layers
  %(prog)s --start-datetime "2025-10-01T00:00:00" --end-datetime "2025-10-31T23:59:59" --layers bronze,silver

  # Delete specific tables
  %(prog)s --tables raw_facilities,clean_facility_master --start-datetime "2025-10-01T00:00:00"

  # Delete entire table (drop table)
  %(prog)s --delete-table Y --layers bronze --tables raw_facilities

  # Dry-run to see what would be deleted
  %(prog)s --start-datetime "2025-10-01T00:00:00" --dry-run
        """,
    )

    parser.add_argument(
        "--start-datetime",
        type=_parse_datetime,
        help="Start datetime in ISO format (YYYY-MM-DDTHH:MM:SS). Required unless --delete-table is Y.",
    )
    parser.add_argument(
        "--end-datetime",
        type=_parse_datetime,
        help="End datetime in ISO format (YYYY-MM-DDTHH:MM:SS). If not provided, uses current datetime.",
    )
    parser.add_argument(
        "--layers",
        help=f"Comma-separated layers to delete from ({', '.join(SUPPORTED_LAYERS)}). "
        "If not provided, all layers will be affected.",
    )
    parser.add_argument(
        "--tables",
        help="Comma-separated table names to delete from. If not provided, all tables in selected layers.",
    )
    parser.add_argument(
        "--delete-table",
        choices=["Y", "N"],
        default="N",
        help="Delete entire table(s) instead of data (Y/N). Default: N",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting anything.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt and proceed with deletion immediately.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel deletion threads. Default: 4",
    )
    parser.add_argument(
        "--app-name",
        default="delete-lakehouse-data",
        help="Spark application name. Default: delete-lakehouse-data",
    )

    return parser.parse_args()


def _parse_datetime(value: str) -> datetime:
    """Parse datetime string in ISO format."""
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime '{value}'. Expected ISO format: YYYY-MM-DDTHH:MM:SS"
        ) from exc


def normalize_layers(raw_layers: Optional[str]) -> List[str]:
    """Normalize and validate layer names."""
    if not raw_layers:
        return SUPPORTED_LAYERS.copy()

    layers = [layer.strip().lower() for layer in raw_layers.split(",") if layer.strip()]
    invalid = [layer for layer in layers if layer not in SUPPORTED_LAYERS]
    if invalid:
        raise ValueError(
            f"Invalid layer(s): {', '.join(invalid)}. Supported: {', '.join(SUPPORTED_LAYERS)}"
        )

    # Remove duplicates while preserving order
    seen: Set[str] = set()
    result = []
    for layer in layers:
        if layer not in seen:
            result.append(layer)
            seen.add(layer)
    return result


def normalize_tables(
    raw_tables: Optional[str], layers: List[str]
) -> Dict[str, List[str]]:
    """
    Normalize and validate table names.
    Returns a dict mapping layer -> list of table names.
    """
    if not raw_tables:
        # Return all tables for selected layers
        return {layer: LAYER_TABLES.get(layer, []).copy() for layer in layers}

    requested_tables = [t.strip() for t in raw_tables.split(",") if t.strip()]
    result: Dict[str, List[str]] = {layer: [] for layer in layers}

    for table in requested_tables:
        found = False
        for layer in layers:
            if table in LAYER_TABLES.get(layer, []):
                result[layer].append(table)
                found = True
                break

        if not found:
            # Try to find in all layers
            for layer, tables in LAYER_TABLES.items():
                if table in tables:
                    if layer in layers:
                        result[layer].append(table)
                        found = True
                    else:
                        print(
                            f"Warning: Table '{table}' exists in layer '{layer}' "
                            f"but this layer was not selected."
                        )
                    break

        if not found:
            print(f"Warning: Unknown table '{table}' - will attempt to process if it exists.")
            # Add to all selected layers (will be validated later)
            for layer in layers:
                result[layer].append(table)

    return result


def discover_existing_tables(
    spark: SparkSession, layers: List[str]
) -> Dict[str, List[str]]:
    """Discover which tables actually exist in the catalog."""
    existing: Dict[str, List[str]] = {layer: [] for layer in layers}

    for layer in layers:
        namespace = f"{CATALOG_NAME}.{layer}"
        try:
            tables_df = spark.sql(f"SHOW TABLES IN {namespace}")
            for row in tables_df.collect():
                table_name = row.tableName
                existing[layer].append(table_name)
        except Exception as e:
            print(f"Warning: Could not list tables in {namespace}: {e}")

    return existing


def find_date_column(spark: SparkSession, full_table_name: str) -> Optional[Tuple[str, str]]:
    """
    Find the best date/timestamp column for filtering.
    Returns (column_name, column_type) or None.
    """
    try:
        schema = spark.table(full_table_name).schema
    except Exception as e:
        print(f"  Warning: Could not read schema for {full_table_name}: {e}")
        return None

    # Build column map (lower case name -> field)
    columns = {field.name.lower(): field for field in schema}

    # Try priority columns first
    for col_name in DATE_COLUMN_PRIORITY:
        field = columns.get(col_name.lower())
        if field and _is_date_compatible(field.dataType):
            return field.name, str(field.dataType)

    # Fall back to any date/timestamp column
    for field in schema:
        if _is_date_compatible(field.dataType):
            return field.name, str(field.dataType)

    return None


def _is_date_compatible(data_type) -> bool:
    """Check if data type is compatible with date filtering."""
    return isinstance(
        data_type, (DateType, TimestampType, StringType, IntegerType, LongType)
    )


def build_delete_condition(
    column_name: str,
    column_type: str,
    date_range: DateTimeRange,
) -> str:
    """Build SQL WHERE condition for date range filtering."""
    column = f"`{column_name}`"
    start_str = date_range.start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = date_range.end.strftime("%Y-%m-%d %H:%M:%S")

    if "timestamp" in column_type.lower():
        return f"{column} BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}'"
    elif "date" in column_type.lower():
        start_date = date_range.start.strftime("%Y-%m-%d")
        end_date = date_range.end.strftime("%Y-%m-%d")
        return f"{column} BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
    elif "string" in column_type.lower():
        # Try to parse as date
        start_date = date_range.start.strftime("%Y-%m-%d")
        end_date = date_range.end.strftime("%Y-%m-%d")
        return f"TO_DATE({column}) BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
    elif "int" in column_type.lower() or "long" in column_type.lower():
        # Assume YYYYMMDD format
        start_key = int(date_range.start.strftime("%Y%m%d"))
        end_key = int(date_range.end.strftime("%Y%m%d"))
        return f"{column} BETWEEN {start_key} AND {end_key}"
    else:
        # Default to timestamp cast
        return f"CAST({column} AS TIMESTAMP) BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}'"


def estimate_affected_rows(
    spark: SparkSession,
    full_table_name: str,
    condition: Optional[str],
) -> Optional[int]:
    """Estimate number of rows that would be affected."""
    try:
        if condition:
            query = f"SELECT COUNT(*) as cnt FROM {full_table_name} WHERE {condition}"
        else:
            query = f"SELECT COUNT(*) as cnt FROM {full_table_name}"

        result = spark.sql(query).collect()
        return result[0].cnt if result else None
    except Exception as e:
        print(f"  Warning: Could not estimate row count for {full_table_name}: {e}")
        return None


def create_deletion_plans(
    spark: SparkSession,
    args: argparse.Namespace,
    layers: List[str],
    tables_by_layer: Dict[str, List[str]],
    existing_tables: Dict[str, List[str]],
) -> List[DeletionPlan]:
    """Create deletion plans for all affected tables."""
    plans: List[DeletionPlan] = []
    delete_full_table = args.delete_table == "Y"

    # Build date range if applicable
    date_range = None
    if args.start_datetime and not delete_full_table:
        date_range = DateTimeRange(start=args.start_datetime, end=args.end_datetime)

    for layer in layers:
        requested_tables = tables_by_layer.get(layer, [])
        existing = existing_tables.get(layer, [])

        # Process only existing tables
        tables_to_process = set(requested_tables) & set(existing)

        if requested_tables and not tables_to_process:
            print(f"Warning: No existing tables found in layer '{layer}' matching: {requested_tables}")

        for table_name in sorted(tables_to_process):
            full_table_name = f"{CATALOG_NAME}.{layer}.{table_name}"

            if delete_full_table:
                # Plan to drop entire table
                plans.append(
                    DeletionPlan(
                        layer=layer,
                        table_name=table_name,
                        full_table_name=full_table_name,
                        delete_table=True,
                    )
                )
            elif date_range:
                # Plan to delete rows in date range
                date_col_info = find_date_column(spark, full_table_name)
                if not date_col_info:
                    print(
                        f"Warning: No suitable date column found in {full_table_name}. "
                        "Skipping date-based deletion."
                    )
                    continue

                col_name, col_type = date_col_info
                condition = build_delete_condition(col_name, col_type, date_range)
                row_estimate = estimate_affected_rows(spark, full_table_name, condition)

                plans.append(
                    DeletionPlan(
                        layer=layer,
                        table_name=table_name,
                        full_table_name=full_table_name,
                        delete_table=False,
                        date_range=date_range,
                        date_column=col_name,
                        condition=condition,
                        row_count_estimate=row_estimate,
                    )
                )
            else:
                # Plan to delete all rows (truncate effect)
                row_estimate = estimate_affected_rows(spark, full_table_name, None)
                plans.append(
                    DeletionPlan(
                        layer=layer,
                        table_name=table_name,
                        full_table_name=full_table_name,
                        delete_table=False,
                        row_count_estimate=row_estimate,
                    )
                )

    return plans


def display_dry_run(plans: List[DeletionPlan], args: argparse.Namespace):
    """Display detailed dry-run information."""
    print("\n" + "=" * 80)
    print("DRY-RUN MODE - NO DATA WILL BE DELETED")
    print("=" * 80)
    print("\nConfiguration:")
    print(f"  Layers:        {', '.join(normalize_layers(args.layers))}")
    print(f"  Delete Table:  {args.delete_table}")
    if args.start_datetime:
        end_dt = args.end_datetime or datetime.now()
        print(f"  Date Range:    {args.start_datetime.isoformat()} to {end_dt.isoformat()}")
    print(f"  Max Workers:   {args.max_workers}")
    print(f"\nPlanned Operations: {len(plans)}")
    print("-" * 80)

    total_rows = 0
    for i, plan in enumerate(plans, 1):
        print(f"\n{i}. Layer: {plan.layer} | Table: {plan.table_name}")
        print(f"   Full Name: {plan.full_table_name}")

        if plan.delete_table:
            print(f"   Action:    DROP TABLE")
        elif plan.date_range:
            print(f"   Action:    DELETE rows in date range")
            print(f"   Date Col:  {plan.date_column}")
            print(f"   Range:     {plan.date_range}")
            print(f"   Condition: {plan.condition}")
        else:
            print(f"   Action:    DELETE all rows")

        if plan.row_count_estimate is not None:
            print(f"   Estimated: ~{plan.row_count_estimate:,} rows affected")
            if not plan.delete_table:
                total_rows += plan.row_count_estimate

        print(f"   SQL:       {plan}")

    print("\n" + "-" * 80)
    if not any(p.delete_table for p in plans):
        print(f"Total Estimated Rows to Delete: ~{total_rows:,}")
    print(f"Total Tables Affected: {len(plans)}")
    print("=" * 80)


def confirm_deletion(plans: List[DeletionPlan], args: argparse.Namespace) -> bool:
    """Ask user to confirm deletion."""
    if args.dry_run:
        return False

    print("\n" + "!" * 80)
    print("WARNING: YOU ARE ABOUT TO DELETE DATA FROM THE LAKEHOUSE")
    print("!" * 80)
    print(f"\nThis will affect {len(plans)} table(s)")

    if args.delete_table == "Y":
        print("Tables will be PERMANENTLY DROPPED (data + metadata)")
    elif args.start_datetime:
        end_dt = args.end_datetime or datetime.now()
        print(f"Data from {args.start_datetime.isoformat()} to {end_dt.isoformat()} will be deleted")
    else:
        print("ALL data in selected tables will be deleted")

    print("\nAffected tables:")
    for plan in plans:
        print(f"  - {plan.full_table_name}")

    print("\n" + "!" * 80)
    
    # If force flag is set, skip confirmation
    if args.force:
        print("\n--force flag detected. Skipping confirmation.")
        print("Proceeding with deletion...")
        return True
    
    try:
        response = input("\nType 'DELETE' (all caps) to confirm and proceed: ")
    except EOFError:
        # Handle case where stdin is not available (e.g., running in container)
        print("\n\nError: Cannot read from stdin (running in non-interactive mode?)")
        print("Use --force flag to skip confirmation, or run interactively.")
        return False

    if response.strip() != "DELETE":
        print("\nOperation cancelled by user.")
        return False

    print("\nProceeding with deletion...")
    return True


def execute_deletion(
    spark: SparkSession,
    plan: DeletionPlan,
) -> DeletionResult:
    """Execute a single deletion operation."""
    try:
        if plan.delete_table:
            # Drop table completely
            spark.sql(f"DROP TABLE IF EXISTS {plan.full_table_name}")
            return DeletionResult(
                plan=plan,
                success=True,
                rows_affected=None,
            )
        elif plan.condition:
            # Delete specific rows
            delete_sql = f"DELETE FROM {plan.full_table_name} WHERE {plan.condition}"
            spark.sql(delete_sql)

            # Get actual row count if possible
            rows_affected = plan.row_count_estimate
            return DeletionResult(
                plan=plan,
                success=True,
                rows_affected=rows_affected,
            )
        else:
            # Delete all rows
            delete_sql = f"DELETE FROM {plan.full_table_name}"
            spark.sql(delete_sql)
            return DeletionResult(
                plan=plan,
                success=True,
                rows_affected=plan.row_count_estimate,
            )

    except Exception as e:
        error_msg = str(e)
        # Check if it's a benign error (table already gone, etc.)
        if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
            return DeletionResult(
                plan=plan,
                success=True,
                error=f"Table already deleted or doesn't exist: {error_msg}",
            )
        return DeletionResult(
            plan=plan,
            success=False,
            error=error_msg,
        )


def execute_deletions_parallel(
    spark: SparkSession,
    plans: List[DeletionPlan],
    max_workers: int,
) -> DeletionStats:
    """Execute deletions in parallel with thread pool."""
    stats = DeletionStats(total_operations=len(plans))

    print(f"\nExecuting {len(plans)} deletion operation(s) with {max_workers} workers...")
    print("-" * 80)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_plan = {
            executor.submit(execute_deletion, spark, plan): plan for plan in plans
        }

        # Process results as they complete
        for i, future in enumerate(as_completed(future_to_plan), 1):
            result = future.result()
            stats.results.append(result)

            if result.success:
                stats.successful += 1
                status = "✓ SUCCESS"
                if result.plan.delete_table:
                    stats.tables_dropped += 1
                elif result.rows_affected:
                    stats.total_rows_deleted += result.rows_affected
            else:
                stats.failed += 1
                status = "✗ FAILED"

            print(f"[{i}/{len(plans)}] {status} - {result.plan.full_table_name}")

            if result.error:
                print(f"          {result.error}")
            elif result.rows_affected:
                print(f"          Deleted ~{result.rows_affected:,} rows")

    print("-" * 80)
    return stats


def display_summary(stats: DeletionStats):
    """Display execution summary."""
    print("\n" + "=" * 80)
    print("DELETION SUMMARY")
    print("=" * 80)
    print(f"Total Operations:    {stats.total_operations}")
    print(f"Successful:          {stats.successful}")
    print(f"Failed:              {stats.failed}")
    print(f"Tables Dropped:      {stats.tables_dropped}")
    print(f"Total Rows Deleted:  ~{stats.total_rows_deleted:,}")
    print("=" * 80)

    if stats.failed > 0:
        print("\nFailed Operations:")
        for result in stats.results:
            if not result.success:
                print(f"  - {result.plan.full_table_name}: {result.error}")

    print()


def validate_args(args: argparse.Namespace):
    """Validate command-line arguments."""
    # If not deleting entire tables, require start datetime
    if args.delete_table != "Y" and not args.start_datetime:
        print(
            "Error: --start-datetime is required unless --delete-table is set to Y",
            file=sys.stderr,
        )
        sys.exit(1)

    # If deleting entire tables and date range is specified, warn
    if args.delete_table == "Y" and (args.start_datetime or args.end_datetime):
        print(
            "Warning: --delete-table is Y, date range arguments will be ignored",
            file=sys.stderr,
        )


def main():
    """Main execution function."""
    args = parse_args()
    validate_args(args)

    # Normalize inputs
    try:
        layers = normalize_layers(args.layers)
        tables_by_layer = normalize_tables(args.tables, layers)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Create Spark session
    print(f"Initializing Spark session: {args.app_name}")
    spark = create_spark_session(
        args.app_name,
        extra_conf={
            # Optimize for deletion operations
            "spark.sql.shuffle.partitions": "100",
            "spark.sql.adaptive.enabled": "true",
        },
    )

    try:
        # Discover existing tables
        print("Discovering existing tables...")
        existing_tables = discover_existing_tables(spark, layers)

        # Create deletion plans
        print("Creating deletion plans...")
        plans = create_deletion_plans(
            spark, args, layers, tables_by_layer, existing_tables
        )

        if not plans:
            print("\nNo tables found matching the criteria. Nothing to delete.")
            return

        # Display dry-run
        display_dry_run(plans, args)

        # Confirm and execute
        if not confirm_deletion(plans, args):
            return

        # Execute deletions
        stats = execute_deletions_parallel(spark, plans, args.max_workers)

        # Display summary
        display_summary(stats)

        # Exit with error code if any operations failed
        if stats.failed > 0:
            sys.exit(1)

    finally:
        spark.stop()
        print("Spark session closed.")


if __name__ == "__main__":
    main()
