"""Shared helpers for Open-Meteo bronze ingestion jobs."""

from __future__ import annotations

import argparse
import datetime as dt
from typing import Iterable, List, Optional

from pv_lakehouse.etl.clients import openelectricity
from pv_lakehouse.etl.clients.openmeteo import FacilityLocation
from pv_lakehouse.etl.utils.spark_utils import write_iceberg_table


def parse_csv(value: Optional[str]) -> List[str]:
    """Split a comma separated string into cleaned tokens."""
    return [token.strip() for token in (value or "").split(",") if token.strip()]


def parse_date(value: str) -> dt.date:
    """Parse a YYYY-MM-DD string into a date, raising argparse errors on failure."""
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - invalid user input
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Expected YYYY-MM-DD") from exc


def resolve_facility_codes(facility_codes: Optional[str]) -> List[str]:
    """Return facility codes from CLI input or fall back to OpenElectricity defaults."""
    codes = parse_csv(facility_codes)
    if codes:
        return [code.upper() for code in codes]
    return openelectricity.load_default_facility_codes()


def load_facility_locations(
    facility_codes: Iterable[str],
    api_key: Optional[str],
) -> List[FacilityLocation]:
    """Resolve facility metadata (with coordinates) required for Open-Meteo calls."""
    selected_codes = [code.upper() for code in facility_codes if code]
    facilities_df = openelectricity.fetch_facilities_dataframe(
        api_key=api_key,
        selected_codes=selected_codes or None,
        networks=["NEM", "WEM"],
        statuses=["operating"],
        fueltechs=["solar_utility"],
    )

    if facilities_df.empty:
        raise ValueError("No facilities returned from OpenElectricity metadata API")

    facilities_df = facilities_df.dropna(subset=["location_lat", "location_lng"])
    if selected_codes:
        facilities_df = facilities_df[
            facilities_df["facility_code"].str.upper().isin(selected_codes)
        ]

    if facilities_df.empty:
        raise ValueError("Requested facilities missing latitude/longitude data")

    facilities: List[FacilityLocation] = []
    for row in facilities_df.itertuples(index=False):
        facilities.append(
            FacilityLocation(
                code=str(row.facility_code),
                name=str(row.facility_name or row.facility_code),
                latitude=float(row.location_lat),
                longitude=float(row.location_lng),
            )
        )
    return facilities


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
