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
    s3_base_path: str,
    iceberg_table: str,
    mode: str,
    ingest_date: str,
    label: str,
) -> None:
    """Persist a Spark DataFrame to S3 and Iceberg with Bronze conventions."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    write_mode = "overwrite" if mode == "backfill" else "append"
    
    # For incremental mode, deduplicate with existing data in Iceberg table
    if mode == "incremental":
        spark = spark_df.sparkSession
        try:
            # Read existing data from Iceberg table
            existing_df = spark.read.table(iceberg_table)
            
            # Determine dedup keys based on table (weather vs air_quality)
            if "weather_timestamp" in spark_df.columns:
                dedup_cols = ["facility_code", "weather_timestamp"]
            elif "air_timestamp" in spark_df.columns:
                dedup_cols = ["facility_code", "air_timestamp"]
            else:
                dedup_cols = ["facility_code", "date"]
            
            # Union new + existing data, then deduplicate keeping latest ingest_timestamp
            combined_df = spark_df.unionByName(existing_df, allowMissingColumns=True)
            
            # Use window function to keep only the latest record per key
            window_spec = Window.partitionBy(*dedup_cols).orderBy(F.col("ingest_timestamp").desc())
            deduped_df = (
                combined_df
                .withColumn("_row_num", F.row_number().over(window_spec))
                .filter(F.col("_row_num") == 1)
                .drop("_row_num")
            )
            
            print(f"Deduplicated: {combined_df.count()} → {deduped_df.count()} rows")
            spark_df = deduped_df
            write_mode = "overwrite"  # Overwrite with deduped data
            
        except Exception as e:
            print(f"Could not read existing table (may not exist yet): {e}")
            # If table doesn't exist, just write new data
            pass
    
    s3_target = f"{s3_base_path}/ingest_date={ingest_date}"
    (
        spark_df.write.mode(write_mode)
        .format("parquet")
        .option("compression", "snappy")
        .save(s3_target)
    )
    print(f"Wrote {label} parquet to {s3_target}")

    write_iceberg_table(
        spark_df,
        iceberg_table,
        mode=write_mode,
    )
    print(f"Wrote {label} data to Iceberg table {iceberg_table} (mode={write_mode})")
