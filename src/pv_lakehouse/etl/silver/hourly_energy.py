"""Silver loader producing clean_hourly_energy."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader


class SilverHourlyEnergyLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_timeseries"
    silver_table = "lh.silver.clean_hourly_energy"
    s3_base_path = "s3a://lakehouse/silver/clean_hourly_energy"
    timestamp_column = "interval_ts"
    partition_cols = ("date_hour",)

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
                "value",
                F.col("interval_ts").cast("timestamp").alias("interval_ts"),
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("interval_ts").isNotNull())
            .where(F.col("metric").isin("energy", "power"))
        )

        if filtered.rdd.isEmpty():
            return None

        hourly = filtered.withColumn("date_hour", F.date_trunc("hour", F.col("interval_ts")))

        key_columns = [
            "facility_code",
            "facility_name",
            "network_code",
            "network_region",
            "date_hour",
        ]

        aggregated = hourly.groupBy(*key_columns).agg(
            F.sum(F.when(F.col("metric") == F.lit("energy"), F.col("value")).otherwise(F.lit(0.0))).alias("energy_mwh"),
            F.avg(F.when(F.col("metric") == F.lit("power"), F.col("value"))).alias("power_avg_mw"),
            F.countDistinct("interval_ts").alias("intervals_count"),
        )

        result = aggregated.withColumn(
            "completeness_pct",
            F.when(F.col("intervals_count") == F.lit(0), F.lit(0.0)).otherwise(F.lit(100.0)),
        )
        result = result.withColumn("is_valid", F.col("energy_mwh") >= F.lit(0))
        result = result.withColumn(
            "quality_flag",
            F.when(F.col("is_valid"), F.lit("GOOD")).otherwise(F.lit("NEGATIVE_ENERGY")),
        )

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
