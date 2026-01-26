from __future__ import annotations
from typing import Iterable, List, Optional
from pv_lakehouse.etl.clients.openmeteo import FacilityLocation
from pv_lakehouse.etl.utils.date_utils import parse_date_argparse as parse_date
from pv_lakehouse.etl.utils.facility_utils import (
    load_facility_locations,
    resolve_facility_codes,
)
from pv_lakehouse.etl.utils.parsing import parse_csv_list
from pv_lakehouse.etl.utils.spark_utils import write_iceberg_table

# Re-export for backward compatibility
def parse_csv(value: Optional[str]) -> List[str]:
    """Split a comma separated string into cleaned tokens.

    Deprecated: Use parse_csv_list from pv_lakehouse.etl.utils.parsing instead.
    """
    return parse_csv_list(value)


def write_dataset(
    spark_df,  # type: ignore[valid-type]
    *,
    iceberg_table: str,
    mode: str,
    label: str,
) -> None:
    """
    Persist a Spark DataFrame to Iceberg Bronze table using MERGE INTO for deduplication.
    
    Modes:
    - backfill: Overwrite entire table
    - incremental: MERGE INTO (upsert) - updates existing records, inserts new ones
    
    Uses Iceberg MERGE INTO SQL for efficient deduplication without loading full table.
    """
    spark = spark_df.sparkSession
    
    if mode == "backfill":
        # Backfill: overwrite entire table
        write_iceberg_table(spark_df, iceberg_table, mode="overwrite")
        row_count = spark_df.count()
        print(f"Wrote {row_count} rows of {label} data to {iceberg_table} (mode=overwrite)")
        
    else:  # incremental
        # Create temp view for MERGE INTO
        temp_view = "temp_bronze_data"
        spark_df.createOrReplaceTempView(temp_view)
        
        # Determine merge keys based on table type
        if "weather_timestamp" in spark_df.columns:
            merge_keys = "target.facility_code = source.facility_code AND target.weather_timestamp = source.weather_timestamp"
        elif "air_timestamp" in spark_df.columns:
            merge_keys = "target.facility_code = source.facility_code AND target.air_timestamp = source.air_timestamp"
        else:
            merge_keys = "target.facility_code = source.facility_code AND target.date = source.date"
        
        # Get all column names for UPDATE SET and INSERT
        columns = spark_df.columns
        update_set = ", ".join([f"target.{col} = source.{col}" for col in columns])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"source.{col}" for col in columns])
        
        try:
            # Iceberg MERGE INTO: upsert (update if exists, insert if new)
            merge_sql = f"""
            MERGE INTO {iceberg_table} AS target
            USING {temp_view} AS source
            ON {merge_keys}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
            """
            
            print(f"Executing MERGE INTO for {label}...")
            spark.sql(merge_sql)
            
            row_count = spark_df.count()
            print(f"Merged {row_count} rows into {iceberg_table} (upsert mode)")
            
        except Exception as e:
            print(f"MERGE INTO failed (table may not exist): {e}")
            print(f"Falling back to append mode for first load...")
            write_iceberg_table(spark_df, iceberg_table, mode="append")
            row_count = spark_df.count()
            print(f"Appended {row_count} rows to {iceberg_table}")
