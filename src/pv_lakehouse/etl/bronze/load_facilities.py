#!/usr/bin/env python3
"""Bronze ingestion job for OpenElectricity facility metadata."""

from __future__ import annotations
import logging
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pv_lakehouse.etl.bronze.base import BaseBronzeLoader
from pv_lakehouse.etl.clients import openelectricity
from pv_lakehouse.etl.utils.etl_metrics import ETLTimer, log_etl_summary

LOGGER = logging.getLogger(__name__)


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
        timer = ETLTimer()
        
        try:
            LOGGER.info("Starting facilities metadata fetch")
            pandas_df = self.fetch_data()
            if pandas_df is None or pandas_df.empty:
                LOGGER.warning("No facility metadata returned; skipping writes.")
                return 0

            fetched_rows = len(pandas_df)
            LOGGER.info("Fetched %d facility records from API", fetched_rows)
            
            spark_df = self.spark.createDataFrame(
                pandas_df, schema=openelectricity.FACILITY_SCHEMA
            )
            spark_df = self.transform(spark_df)
            spark_df = self.add_ingest_columns(spark_df)

            # Facilities: always overwrite (master data)
            self.write_overwrite(spark_df)
            row_count = spark_df.count()
            
            log_etl_summary(
                LOGGER,
                self.iceberg_table,
                row_count,
                timer.elapsed(),
                operation="Facilities load",
            )
            return row_count
        finally:
            self.close()


if __name__ == "__main__":
    from pv_lakehouse.etl.bronze.cli import run_cli
    run_cli()

