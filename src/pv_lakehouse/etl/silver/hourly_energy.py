"""Silver loader producing clean_hourly_energy."""

from __future__ import annotations

import datetime as dt
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
        """Process bronze energy data in 7-day chunks to limit memory usage."""
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=7)

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
        
        # EDA-based quality validation from large dataset analysis (679 days)
        # Facility-specific capacity thresholds (based on observed max + safety margin)
        TUKEY_LOWER = 0.0  # Physical bound: energy must be >= 0
        
        # Build intermediate columns in single pass to avoid multiple scans
        result = (
            hourly
            .withColumn("hour_of_day", F.hour(F.col("date_hour")))
            .withColumn("completeness_pct", F.lit(100.0))
            .withColumn(
                "facility_capacity_threshold",
                F.when(F.col("facility_code") == "COLEASF", 145.0)
                .when(F.col("facility_code") == "BNGSF1", 115.0)
                .when(F.col("facility_code") == "CLARESF", 115.0)
                .when(F.col("facility_code") == "GANNSF", 115.0)
                .when(F.col("facility_code") == "NYNGAN", 115.0)
                .otherwise(110.0)
            )
        )
        
        # Now reference columns directly (faster than nested expressions)
        hour_col = F.col("hour_of_day")
        energy_col = F.col("energy_mwh")
        capacity_col = F.col("facility_capacity_threshold")
        
        # Define validation checks using column references
        is_within_bounds = energy_col >= 0
        is_night = ((hour_col >= 22) | (hour_col < 6))
        is_night_anomaly = is_night & (energy_col > 1.0)
        is_statistical_outlier = (energy_col < TUKEY_LOWER) | (energy_col > capacity_col)
        is_equipment_issue = (hour_col >= 6) & (hour_col < 18) & (energy_col == 0.0)
        is_peak_anomaly = (hour_col >= 11) & (hour_col <= 15) & (energy_col < 5.0)
        
        # Add quality flags
        result = (
            result
            .withColumn(
                "quality_issues",
                F.concat_ws(
                    "|",
                    F.when(~is_within_bounds, F.lit("OUT_OF_BOUNDS")),
                    F.when(is_statistical_outlier, F.lit("STATISTICAL_OUTLIER")),
                    F.when(is_night_anomaly, F.lit("NIGHT_ENERGY_ANOMALY")),
                    F.when(is_equipment_issue, F.lit("ZERO_ENERGY_DAYTIME")),
                    F.when(is_peak_anomaly, F.lit("PEAK_HOUR_LOW_ENERGY"))
                )
            )
            .withColumn(
                "quality_flag",
                F.when(
                    ~is_within_bounds,
                    F.lit("REJECT")
                ).when(
                    is_statistical_outlier | is_night_anomaly | is_equipment_issue | is_peak_anomaly,
                    F.lit("CAUTION")
                ).otherwise(F.lit("GOOD"))
            )
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
            .drop("hour_of_day", "facility_capacity_threshold")
        )

        return result.select(
            "facility_code", "facility_name", "network_code", "network_region",
            "date_hour", "energy_mwh", "intervals_count",
            "quality_flag", "quality_issues", "completeness_pct",
            "created_at", "updated_at"
        )


__all__ = ["SilverHourlyEnergyLoader"]
