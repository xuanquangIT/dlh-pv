#!/usr/bin/env python3
"""Export all Bronze, Silver, Gold tables to CSV format."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

from pv_lakehouse.etl.utils.spark_utils import create_spark_session, cleanup_spark_staging

LOGGER = logging.getLogger(__name__)

# Define all tables to export
BRONZE_TABLES = [
    "lh.bronze.raw_facilities",
    "lh.bronze.raw_facility_timeseries",
    "lh.bronze.raw_facility_weather",
    "lh.bronze.raw_facility_air_quality",
]

SILVER_TABLES = [
    "lh.silver.clean_facility_master",
    "lh.silver.clean_hourly_energy",
    "lh.silver.clean_hourly_weather",
    "lh.silver.clean_hourly_air_quality",
]

GOLD_TABLES = [
    "lh.gold.dim_facility",
    "lh.gold.dim_date",
    "lh.gold.dim_time",
    "lh.gold.dim_weather_condition",
    "lh.gold.dim_air_quality_category",
    "lh.gold.dim_equipment_status",
    "lh.gold.dim_model_version",
    "lh.gold.dim_performance_issue",
    "lh.gold.fact_kpi_performance",
    "lh.gold.fact_weather_impact",
    "lh.gold.fact_air_quality_impact",
    "lh.gold.fact_solar_forecast",
    "lh.gold.fact_root_cause_analysis",
]


def setup_logging() -> None:
    """Configure logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def export_table_to_csv(spark_session, table_name: str, output_dir: Path) -> bool:
    """
    Export a single Iceberg table to CSV.
    
    Args:
        spark_session: Active Spark session
        table_name: Fully qualified table name (e.g., lh.bronze.raw_facilities)
        output_dir: Output directory path
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Read table
        df = spark_session.table(table_name)
        row_count = df.count()
        
        if row_count == 0:
            LOGGER.warning(f"Table {table_name} is empty, skipping")
            return False
        
        # Create output file path
        safe_table_name = table_name.replace(".", "_")
        csv_file = output_dir / f"{safe_table_name}.csv"
        # Use /tmp for Spark write operations (local filesystem compatible)
        import tempfile
        temp_dir = Path(tempfile.mkdtemp(prefix=f"{safe_table_name}_"))
        
        # Write to temp directory first
        LOGGER.info(f"Exporting {table_name} ({row_count} rows) to {csv_file}")
        df.write.mode("overwrite").option("header", "true").csv(str(temp_dir))
        
        # Merge partitions into single file
        import glob
        import shutil
        csv_parts = sorted(glob.glob(str(temp_dir / "part-*.csv")))
        
        if not csv_parts:
            LOGGER.error(f"No CSV parts found in {temp_dir}")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return False
        
        # Merge CSV files into one
        if csv_file.exists():
            csv_file.unlink()
        
        with open(csv_file, "w") as out_file:
            for idx, csv_part in enumerate(csv_parts):
                with open(csv_part, "r") as part_file:
                    if idx == 0:
                        # Write header from first file
                        out_file.write(part_file.read())
                    else:
                        # Skip header line from other files
                        next(part_file)
                        out_file.write(part_file.read())
        
        # Cleanup temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        
        LOGGER.info(f"✓ Exported {table_name} to {csv_file} ({row_count} rows)")
        return True
        
    except Exception as error:
        LOGGER.error(f"Failed to export {table_name}: {error}")
        return False


def main() -> None:
    """Main export function."""
    setup_logging()
    
    parser = argparse.ArgumentParser(description="Export Iceberg tables to CSV")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/home/pvlakehouse/dlh-pv/src/pv_lakehouse/exported_data"),
        help="Output directory for CSV files",
    )
    parser.add_argument(
        "--layers",
        nargs="+",
        choices=["bronze", "silver", "gold", "all"],
        default=["all"],
        help="Which layers to export",
    )
    parser.add_argument(
        "--app-name",
        default="export-to-csv",
        help="Spark application name",
    )
    
    args = parser.parse_args()
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine which tables to export
    tables_to_export = []
    layers = args.layers if args.layers != ["all"] else ["bronze", "silver", "gold"]
    
    if "bronze" in layers:
        tables_to_export.extend(BRONZE_TABLES)
    if "silver" in layers:
        tables_to_export.extend(SILVER_TABLES)
    if "gold" in layers:
        tables_to_export.extend(GOLD_TABLES)
    
    LOGGER.info(f"Exporting {len(tables_to_export)} tables to {args.output_dir}")
    LOGGER.info(f"Tables: {', '.join(tables_to_export)}")
    
    # Create Spark session
    spark = create_spark_session(args.app_name)
    
    try:
        # Export each table
        success_count = 0
        for table_name in tables_to_export:
            if export_table_to_csv(spark, table_name, args.output_dir):
                success_count += 1
        
        LOGGER.info(f"\n✓ Export complete: {success_count}/{len(tables_to_export)} tables exported")
        print(f"\nExported CSV files are in: {args.output_dir}")
        
    finally:
        spark.stop()
        cleanup_spark_staging()


if __name__ == "__main__":
    main()
