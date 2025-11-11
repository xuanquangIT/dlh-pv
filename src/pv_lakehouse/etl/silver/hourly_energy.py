"""Silver loader producing clean_hourly_energy."""

from __future__ import annotations

import datetime as dt
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions


class SilverHourlyEnergyLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_timeseries"
    silver_table = "lh.silver.clean_hourly_energy"
    timestamp_column = "interval_ts"
    partition_cols = ("date_hour",)

    def __init__(self, options: Optional[LoadOptions] = None) -> None:
        if options is None:
            options = LoadOptions()
        else:
            # Cap at defaults to prevent memory issues
            options.target_file_size_mb = min(options.target_file_size_mb, self.DEFAULT_TARGET_FILE_SIZE_MB)
            options.max_records_per_file = min(options.max_records_per_file, self.DEFAULT_MAX_RECORDS_PER_FILE)
        super().__init__(options)

    def run(self) -> int:
        """Process bronze energy data in 3-day chunks to limit memory usage."""
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=3)

    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        if bronze_df is None or not bronze_df.columns:
            return None

        required_columns = {
            "facility_code",
            "facility_name",
            "network_code",
            "network_region",
            "metric",
            "value",
            "interval_ts",
        }
        missing = required_columns - set(bronze_df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in bronze timeseries source: {sorted(missing)}")

        filtered = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                "network_code",
                "network_region",
                "metric",
                F.col("value").cast("double").alias("metric_value"),
                F.col("interval_ts").cast("timestamp").alias("interval_ts"),
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("interval_ts").isNotNull())
            .where(F.col("metric").isin("energy", "power"))
        )
        
        # Aggregate energy by hour (local time)
        hourly = (
            filtered
            .withColumn("date_hour", F.date_trunc("hour", F.col("interval_ts")))
            .groupBy("facility_code", "facility_name", "network_code", "network_region", "date_hour")
            .agg(
                F.sum(F.when(F.col("metric") == "energy", F.col("metric_value"))).alias("energy_mwh"),
                F.count(F.when(F.col("metric") == "energy", F.lit(1))).alias("intervals_count")
            )
            .filter(F.col("intervals_count") > 0)
        )
        # Add quality flags and metadata
        result = (
            hourly
            .withColumn("completeness_pct", F.lit(100.0))
            .withColumn("is_valid", F.col("energy_mwh") >= 0)
            .withColumn("quality_flag", F.when(F.col("is_valid"), "GOOD").otherwise("NEGATIVE_ENERGY"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        return result.select(
            "facility_code", "facility_name", "network_code", "network_region",
            "date_hour", "energy_mwh", "intervals_count", "is_valid",
            "quality_flag", "completeness_pct", "created_at", "updated_at"
        )


__all__ = ["SilverHourlyEnergyLoader"]
