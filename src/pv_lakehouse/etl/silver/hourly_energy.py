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
    s3_base_path = "s3a://lakehouse/silver/clean_hourly_energy"
    timestamp_column = "interval_ts"
    partition_cols = ("date_hour",)

    DEFAULT_TARGET_FILE_SIZE_MB = 16
    DEFAULT_MAX_RECORDS_PER_FILE = 50_000

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
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0

        timestamp_col = self.timestamp_column
        if timestamp_col not in bronze_df.columns:
            raise ValueError(
                f"Timestamp column '{timestamp_col}' missing from bronze table {self.bronze_table}"
            )

        min_ts = bronze_df.select(F.min(F.col(timestamp_col))).collect()[0][0]
        max_ts = bronze_df.select(F.max(F.col(timestamp_col))).collect()[0][0]
        if min_ts is None or max_ts is None:
            return 0

        if isinstance(self.options.start, dt.datetime):
            min_ts = max(min_ts, self.options.start)
        elif isinstance(self.options.start, dt.date):
            min_ts = max(min_ts, dt.datetime.combine(self.options.start, dt.time.min))

        if isinstance(self.options.end, dt.datetime):
            max_ts = min(max_ts, self.options.end)
        elif isinstance(self.options.end, dt.date):
            max_ts = min(max_ts, dt.datetime.combine(self.options.end, dt.time.max))

        if min_ts > max_ts:
            return 0

        chunk_start = min_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        total_rows = 0

        original_start = self.options.start
        original_end = self.options.end

        try:
            while chunk_start <= max_ts:
                next_month = (chunk_start + dt.timedelta(days=32)).replace(day=1)
                chunk_end = min(max_ts, next_month - dt.timedelta(microseconds=1))

                print(
                    f"Processing monthly chunk {chunk_start:%Y-%m} ({chunk_start.isoformat()} -> {chunk_end.isoformat()})"
                )

                chunk_df = bronze_df.filter(
                    (F.col(timestamp_col) >= F.lit(chunk_start)) & (F.col(timestamp_col) <= F.lit(chunk_end))
                )

                transformed_df = self.transform(chunk_df)
                if transformed_df is None:
                    chunk_start = next_month
                    continue

                row_count = transformed_df.count()
                if row_count == 0:
                    chunk_start = next_month
                    continue

                self.options.start = chunk_start
                self.options.end = chunk_end
                self._write_outputs(transformed_df, row_count)
                total_rows += row_count

                print(
                    f"Finished chunk {chunk_start:%Y-%m}: wrote {row_count} rows to {self.silver_table}"
                )

                chunk_start = next_month
        finally:
            self.options.start = original_start
            self.options.end = original_end

        return total_rows

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

        hourly = filtered.withColumn("date_hour", F.date_trunc("hour", F.col("interval_ts")))

        key_columns = [
            "facility_code",
            "facility_name",
            "network_code",
            "network_region",
            "date_hour",
        ]

        aggregated = hourly.groupBy(*key_columns).agg(
            F.sum(F.when(F.col("metric") == F.lit("energy"), F.col("metric_value"))).alias("energy_mwh"),
            F.avg(F.when(F.col("metric") == F.lit("power"), F.col("metric_value"))).alias("power_avg_mw"),
            F.sum(F.when(F.col("metric") == F.lit("energy"), F.lit(1))).alias("energy_intervals"),
        )

        result = aggregated.filter(F.col("energy_intervals") > F.lit(0))
        if not result.columns:
            return None

        result = result.withColumnRenamed("energy_intervals", "intervals_count")
        result = result.withColumn(
            "completeness_pct",
            F.when(F.col("intervals_count") == F.lit(0), F.lit(0.0)).otherwise(F.lit(100.0)),
        )
        result = result.withColumn("is_valid", F.col("energy_mwh") >= F.lit(0))
        result = result.withColumn(
            "quality_flag",
            F.when(F.col("is_valid"), F.lit("GOOD")).otherwise(F.lit("NEGATIVE_ENERGY")),
        )

        result = result.repartition("facility_code", "date_hour")

        current_ts = F.current_timestamp()
        result = result.withColumn("created_at", current_ts).withColumn("updated_at", current_ts)

        return result.select(
            "facility_code",
            "facility_name",
            "network_code",
            "network_region",
            "date_hour",
            "energy_mwh",
            "power_avg_mw",
            "intervals_count",
            "is_valid",
            "quality_flag",
            "completeness_pct",
            "created_at",
            "updated_at",
        )


__all__ = ["SilverHourlyEnergyLoader"]
