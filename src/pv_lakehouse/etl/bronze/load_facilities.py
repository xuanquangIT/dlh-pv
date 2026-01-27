#!/usr/bin/env python3
"""Bronze ingestion job for OpenElectricity facility metadata."""

from __future__ import annotations

import argparse
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pv_lakehouse.etl.bronze.base import BaseBronzeLoader, BronzeLoadOptions
from pv_lakehouse.etl.clients import openelectricity


class FacilitiesLoader(BaseBronzeLoader):
    """Bronze loader for facility master data."""

    iceberg_table = "lh.bronze.raw_facilities"
    timestamp_column = "ingest_timestamp"
    merge_keys = ("facility_code",)

    def fetch_data(self) -> pd.DataFrame:
        """Fetch facility metadata from OpenElectricity API."""
        return openelectricity.fetch_facilities_dataframe(
            api_key=self.options.api_key,
            selected_codes=self.resolve_facilities() or None,
            networks=["NEM", "WEM"],
            statuses=["operating"],
            fueltechs=["solar_utility"],
            region=None,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Add ingest_date partition column."""
        return df.withColumn("ingest_date", F.to_date(F.current_timestamp()))

    def run(self) -> int:
        """Execute loader - facilities always use overwrite mode."""
        try:
            pandas_df = self.fetch_data()
            if pandas_df is None or pandas_df.empty:
                print("No facility metadata returned; skipping writes.")
                return 0

            spark_df = self.spark.createDataFrame(
                pandas_df, schema=openelectricity.FACILITY_SCHEMA
            )
            spark_df = self.transform(spark_df)
            spark_df = self.add_ingest_columns(spark_df)

            # Facilities: always overwrite (master data)
            self.write_overwrite(spark_df)
            row_count = spark_df.count()
            print(f"Wrote {row_count} rows to {self.iceberg_table} (mode=overwrite)")
            return row_count
        finally:
            self.close()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for facility metadata loader."""
    parser = argparse.ArgumentParser(
        description="Load facility metadata into Bronze zone"
    )
    parser.add_argument(
        "--mode", choices=["backfill", "incremental"], default="incremental"
    )
    parser.add_argument(
        "--facility-codes",
        help="Comma-separated facility codes (default: all solar facilities)",
    )
    parser.add_argument("--api-key", help="Override API key")
    parser.add_argument("--app-name", default="bronze-facilities")
    return parser.parse_args()


def main() -> None:
    """Main entry point for facility metadata loader."""
    args = parse_args()
    options = BronzeLoadOptions(
        mode=args.mode,
        facility_codes=args.facility_codes,
        api_key=args.api_key,
        app_name=args.app_name,
    )
    loader = FacilitiesLoader(options)
    loader.run()


if __name__ == "__main__":
    main()
