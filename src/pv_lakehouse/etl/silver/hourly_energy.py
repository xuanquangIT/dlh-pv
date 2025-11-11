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
            options = LoadOptions(
                target_file_size_mb=self.DEFAULT_TARGET_FILE_SIZE_MB,
                max_records_per_file=self.DEFAULT_MAX_RECORDS_PER_FILE,
            )
        else:
            options.target_file_size_mb = min(options.target_file_size_mb, self.DEFAULT_TARGET_FILE_SIZE_MB)
            options.max_records_per_file = min(options.max_records_per_file, self.DEFAULT_MAX_RECORDS_PER_FILE)
        super().__init__(options)

    def run(self) -> int:
        """
        Process bronze timeseries data in 3-day chunks.
        
        3-day chunks limit concurrent partition writers to 72 (3 days × 24 hours),
        preventing OOM from Iceberg FanoutDataWriter which keeps all partition writers open.
        """
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0

        # Process in 3-day chunks (narrow table: 3 columns)
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
        
        # Rename to mark as UTC
        filtered = filtered.withColumn("interval_ts_utc", F.col("interval_ts")).drop("interval_ts")
        
        # Convert UTC → local time based on network_code and region for analysis
        # NEM regions: QLD/NSW/VIC → Brisbane UTC+10; SA → Adelaide UTC+9:30
        # WEM → Perth UTC+8
        filtered = filtered.withColumn(
            "tz_string",
            F.when(F.col("network_code") == F.lit("NEM"), 
                F.when(F.col("network_region") == F.lit("SA1"), F.lit("Australia/Adelaide"))
                 .otherwise(F.lit("Australia/Brisbane")))
             .when(F.col("network_code") == F.lit("WEM"), F.lit("Australia/Perth"))
             .otherwise(F.lit("Australia/Brisbane"))
        )
        filtered = filtered.withColumn(
            "interval_ts_local",
            F.from_utc_timestamp(F.col("interval_ts_utc"), F.col("tz_string"))
        ).drop("tz_string")

        # Use UTC timestamps for hourly aggregation to avoid data loss
        hourly = filtered.withColumn("date_hour", F.date_trunc("hour", F.col("interval_ts_utc")))

        key_columns = [
            "facility_code",
            "facility_name",
            "network_code",
            "network_region",
            "date_hour",
        ]

        # Aggregate by hour with optimized partitioning
        aggregated = hourly.groupBy(*key_columns).agg(
            F.sum(F.when(F.col("metric") == F.lit("energy"), F.col("metric_value"))).alias("energy_mwh"),
            F.sum(F.when(F.col("metric") == F.lit("energy"), F.lit(1))).alias("energy_intervals"),
            F.first(F.col("interval_ts_utc")).alias("interval_ts_utc"),
            F.first(F.col("interval_ts_local")).alias("interval_ts_local"),
        )

        result = aggregated.filter(F.col("energy_intervals") > F.lit(0))

        result = result.withColumnRenamed("energy_intervals", "intervals_count")
        # All hourly records have 100% completeness since we filter energy_intervals > 0
        result = result.withColumn("completeness_pct", F.lit(100.0))
        result = result.withColumn("is_valid", F.col("energy_mwh") >= F.lit(0))
        result = result.withColumn(
            "quality_flag",
            F.when(F.col("is_valid"), F.lit("GOOD")).otherwise(F.lit("NEGATIVE_ENERGY")),
        )

        # Coalesce to 1 partition to minimize concurrent Iceberg partition writers
        # With hourly partitioning, FanoutDataWriter keeps ALL partition writers open
        result = result.coalesce(1)

        current_ts = F.current_timestamp()
        result = result.withColumn("created_at", current_ts).withColumn("updated_at", current_ts)

        return result.select(
            "facility_code",
            "facility_name",
            "network_code",
            "network_region",
            "date_hour",
            "energy_mwh",
            "intervals_count",
            "is_valid",
            "quality_flag",
            "completeness_pct",
            "created_at",
            "updated_at",
        )


__all__ = ["SilverHourlyEnergyLoader"]
