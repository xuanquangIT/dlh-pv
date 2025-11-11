"""Silver loader producing clean_hourly_energy."""

from __future__ import annotations

import datetime as dt
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions


class SilverHourlyEnergyLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_timeseries"
    silver_table = "lh.silver.clean_hourly_energy"
    timestamp_column = "interval_ts"
    partition_cols = ("date_hour",)

    # Outlier detection thresholds
    _outlier_zscore_threshold = 3.0  # Values > 3 sigma from mean
    _outlier_energy_deviation_pct = 95.0  # Flag if energy deviates >95% from typical day pattern
    _iqr_multiplier = 1.5  # Standard IQR multiplier for outliers

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

        # Outlier detection: Z-score method per facility and hour-of-day
        result = self._detect_outliers_zscore(result)
        
        # Outlier detection: Deviation from typical day pattern
        result = self._detect_outliers_deviation_pattern(result)
        
        # Outlier detection: IQR method per facility
        result = self._detect_outliers_iqr(result)

        return result.select(
            "facility_code", "facility_name", "network_code", "network_region",
            "date_hour", "energy_mwh", "intervals_count", "is_valid",
            "quality_flag", "completeness_pct", "created_at", "updated_at",
            "has_outliers_zscore", "outliers_zscore_reason",
            "has_outliers_deviation", "outliers_deviation_pct",
            "has_outliers_iqr", "outliers_iqr_reason"
        )

    def _detect_outliers_zscore(self, df: DataFrame) -> DataFrame:
        """
        Detect energy outliers using Z-score method per facility and hour-of-day.
        Marks energy values that deviate > threshold standard deviations from hourly mean.
        """
        window_spec = Window.partitionBy("facility_code", F.hour(F.col("date_hour")))
        
        df = (
            df
            .withColumn("energy_mwh_mean", F.avg(F.col("energy_mwh")).over(window_spec))
            .withColumn("energy_mwh_stddev", F.stddev(F.col("energy_mwh")).over(window_spec))
        )
        
        # Calculate Z-score and flag outliers
        df = (
            df
            .withColumn(
                "energy_zscore",
                F.when(
                    (F.col("energy_mwh_stddev").isNotNull()) & (F.col("energy_mwh_stddev") > 0),
                    F.abs((F.col("energy_mwh") - F.col("energy_mwh_mean")) / F.col("energy_mwh_stddev"))
                ).otherwise(F.lit(0))
            )
            .withColumn(
                "has_outliers_zscore",
                F.col("energy_zscore") > self._outlier_zscore_threshold
            )
            .withColumn(
                "outliers_zscore_reason",
                F.when(
                    F.col("has_outliers_zscore"),
                    F.concat(
                        F.lit("Z-score="),
                        F.round(F.col("energy_zscore"), 2),
                        F.lit("(threshold="),
                        F.lit(self._outlier_zscore_threshold),
                        F.lit(")")
                    )
                ).otherwise(F.lit(""))
            )
        )
        
        # Drop intermediate columns
        df = df.drop("energy_mwh_mean", "energy_mwh_stddev", "energy_zscore")
        
        return df

    def _detect_outliers_deviation_pattern(self, df: DataFrame) -> DataFrame:
        """
        Detect energy anomalies by comparing against typical day pattern.
        Flags records where energy deviates > deviation_pct from expected hourly generation.
        """
        # Calculate typical energy per facility-hour (median across all days)
        window_spec = Window.partitionBy("facility_code", F.hour(F.col("date_hour")))
        
        df = (
            df
            .withColumn("typical_energy_mwh", F.percentile_approx(F.col("energy_mwh"), 0.5).over(window_spec))
        )
        
        # Calculate deviation percentage
        df = (
            df
            .withColumn(
                "deviation_pct",
                F.when(
                    F.col("typical_energy_mwh") > 0,
                    (F.abs(F.col("energy_mwh") - F.col("typical_energy_mwh")) / F.col("typical_energy_mwh")) * 100
                ).otherwise(F.lit(0))
            )
            .withColumn(
                "has_outliers_deviation",
                F.col("deviation_pct") > self._outlier_energy_deviation_pct
            )
            .withColumn(
                "outliers_deviation_pct",
                F.round(F.col("deviation_pct"), 2)
            )
        )
        
        # Drop intermediate columns
        df = df.drop("typical_energy_mwh", "deviation_pct")
        
        return df

    def _detect_outliers_iqr(self, df: DataFrame) -> DataFrame:
        """
        Detect energy outliers using Interquartile Range (IQR) method per facility-hour.
        Flags values outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] range.
        """
        window_spec = Window.partitionBy("facility_code", F.hour(F.col("date_hour")))
        
        # Calculate Q1, Q3, IQR per facility-hour
        df = (
            df
            .withColumn("energy_q1", F.percentile_approx(F.col("energy_mwh"), 0.25).over(window_spec))
            .withColumn("energy_q3", F.percentile_approx(F.col("energy_mwh"), 0.75).over(window_spec))
        )
        
        df = (
            df
            .withColumn(
                "energy_iqr",
                F.col("energy_q3") - F.col("energy_q1")
            )
            .withColumn(
                "energy_lower_bound",
                F.col("energy_q1") - (self._iqr_multiplier * F.col("energy_iqr"))
            )
            .withColumn(
                "energy_upper_bound",
                F.col("energy_q3") + (self._iqr_multiplier * F.col("energy_iqr"))
            )
            .withColumn(
                "has_outliers_iqr",
                F.col("energy_mwh").isNotNull() & (
                    (F.col("energy_mwh") < F.col("energy_lower_bound")) |
                    (F.col("energy_mwh") > F.col("energy_upper_bound"))
                )
            )
            .withColumn(
                "outliers_iqr_reason",
                F.when(
                    F.col("has_outliers_iqr"),
                    F.concat(
                        F.lit("IQR:["),
                        F.round(F.col("energy_lower_bound"), 2),
                        F.lit(","),
                        F.round(F.col("energy_upper_bound"), 2),
                        F.lit("]")
                    )
                ).otherwise(F.lit(""))
            )
        )
        
        # Drop intermediate columns
        df = df.drop("energy_q1", "energy_q3", "energy_iqr", "energy_lower_bound", "energy_upper_bound")
        
        return df


__all__ = ["SilverHourlyEnergyLoader"]
