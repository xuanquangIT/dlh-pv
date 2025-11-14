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
        
        # NYNGAN max=102.0 MWh (P99.5=101.35, rounded to 105)
        # GANNSF max=49.99 MWh (P99.5=49.76, rounded to 50)
        TUKEY_LOWER = 0.0  # Physical bound: energy must be >= 0
        
        # Build intermediate columns in single pass to avoid multiple scans
        result = (
            hourly
            .withColumn("hour_of_day", F.hour(F.col("date_hour")))
            .withColumn("completeness_pct", F.lit(100.0))
            .withColumn(
                "facility_capacity_threshold",
                # Upper bound = rounded for clarity (data-driven from 679-day analysis)
                # NYNGAN: 105.0 MWh (P99.5=101.35 rounded)
                # GANNSF: 50.0 MWh (P99.5=49.76 rounded)
                F.when(F.col("facility_code") == "COLEASF", 170.0)      # 145 MW capacity + margin
                .when(F.col("facility_code") == "BNGSF1", 140.0)        # 115 MW capacity + margin (rounded)
                .when(F.col("facility_code") == "CLARESF", 140.0)       # 115 MW capacity + margin (rounded)
                .when(F.col("facility_code") == "GANNSF", 52.0)         # 50.0 MWh max (rounded)
                .when(F.col("facility_code") == "NYNGAN", 105.0)        # 105 MWh capacity (rounded)
                .otherwise(130.0)                                        # 110 MW capacity + margin (rounded)
            )
            .withColumn(
                "facility_expected_peak_threshold",
                # Peak threshold = rounded for clarity (95th percentile from 679-day analysis)
                # NYNGAN: 95.0 MWh (P95=95.40 rounded), GANNSF: 40.0 MWh (conservative threshold)
                F.when(F.col("facility_code") == "COLEASF", 115.0)      # 145 * 0.77 estimated (rounded)
                .when(F.col("facility_code") == "BNGSF1", 90.0)         # 115 * 0.77 estimated (rounded)
                .when(F.col("facility_code") == "CLARESF", 90.0)        # 115 * 0.77 estimated (rounded)
                .when(F.col("facility_code") == "GANNSF", 40.0)         # Conservative threshold for smaller facility
                .when(F.col("facility_code") == "NYNGAN", 95.0)         # P95 rounded
                .otherwise(85.0)                                         # 110 * 0.77 estimated (rounded)
            )
        )
        
        # Now reference columns directly (faster than nested expressions)
        hour_col = F.col("hour_of_day")
        energy_col = F.col("energy_mwh")
        capacity_col = F.col("facility_capacity_threshold")
        peak_threshold = F.col("facility_expected_peak_threshold")
        
        # Define validation checks using column references
        is_within_bounds = energy_col >= 0
        is_night = ((hour_col >= 22) | (hour_col < 6))
        is_peak = (hour_col >= 10) & (hour_col <= 14)
        
        is_night_anomaly = is_night & (energy_col > 1.0)
        is_statistical_outlier = (energy_col < TUKEY_LOWER) | (energy_col > capacity_col)
        
        is_daytime_zero = (hour_col >= 8) & (hour_col <= 17) & (energy_col == 0.0)
        
        is_equipment_downtime = is_peak & (energy_col == 0.0)
        
        # TRANSITION_HOUR_LOW_ENERGY detection (formerly named RAMP_ANOMALY)
        # Thresholds based on analysis:
        # - Sunrise (06:00-08:00): Only flag if <5% of expected peak
        # - Early Morning (08:00-10:00): Only flag if <8% of expected peak
        # - Sunset (17:00-19:00): Only flag if <10% of expected peak
        is_sunrise = (hour_col >= 6) & (hour_col < 8)
        is_early_morning = (hour_col >= 8) & (hour_col < 10)
        is_sunset = (hour_col >= 17) & (hour_col < 19)
        
        threshold_factor = (
            F.when(is_sunrise, 0.05)        # 5% of peak for sunrise
            .when(is_early_morning, 0.08)   # 8% of peak for early morning
            .when(is_sunset, 0.10)          # 10% of peak for sunset
            .otherwise(0.0)
        )
        
        is_transition_hour_low_energy = (
            ((is_sunrise | is_early_morning | is_sunset) & 
             (energy_col > 0.01) & 
             (energy_col < (peak_threshold * threshold_factor)))
        )
        
        is_efficiency_anomaly = is_peak & (energy_col > 0.5) & (energy_col < (peak_threshold * 0.50))
        
        result = (
            result
            .withColumn(
                "quality_issues",
                F.concat_ws(
                    "|",
                    F.when(~is_within_bounds, F.lit("OUT_OF_BOUNDS")),
                    F.when(is_statistical_outlier, F.lit("STATISTICAL_OUTLIER")),
                    F.when(is_night_anomaly, F.lit("NIGHT_ENERGY_ANOMALY")),
                    F.when(is_daytime_zero, F.lit("DAYTIME_ZERO_ENERGY")),
                    F.when(is_equipment_downtime, F.lit("EQUIPMENT_DOWNTIME")),
                    F.when(is_transition_hour_low_energy, F.lit("TRANSITION_HOUR_LOW_ENERGY")),
                    F.when(is_efficiency_anomaly, F.lit("PEAK_HOUR_LOW_ENERGY"))
                )
            )
            .withColumn(
                "quality_flag",
                F.when(
                    ~is_within_bounds,
                    F.lit("REJECT")
                ).when(
                    is_statistical_outlier | is_night_anomaly | is_daytime_zero | is_transition_hour_low_energy | is_efficiency_anomaly,
                    F.lit("CAUTION")
                ).otherwise(F.lit("GOOD"))
            )
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
            .drop("hour_of_day", "facility_capacity_threshold", "facility_expected_peak_threshold")
        )

        return result.select(
            "facility_code", "facility_name", "network_code", "network_region",
            "date_hour", "energy_mwh", "intervals_count",
            "quality_flag", "quality_issues", "completeness_pct",
            "created_at", "updated_at"
        )


__all__ = ["SilverHourlyEnergyLoader"]
