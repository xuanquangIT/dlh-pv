"""Silver loader producing clean_hourly_energy."""

from __future__ import annotations

import datetime as dt
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions
from pv_lakehouse.etl.bronze.facility_timezones import FACILITY_TIMEZONES, DEFAULT_TIMEZONE


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

    def _get_hour_offset(self) -> int:
        """Energy loader shifts hour by +1, need to account for this in incremental start calculation."""
        return 1

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
        # Convert `interval_ts` (assumed UTC) to facility local timestamp before truncating to hour.
        # Use the facility timezone map to perform a per-facility conversion. Default to DEFAULT_TIMEZONE.
        default_local = F.from_utc_timestamp(F.col("interval_ts"), DEFAULT_TIMEZONE)
        tz_expr = default_local
        # Wrap mapping entries as conditional expressions: when(facility_code==code, from_utc_timestamp(...)).otherwise(prev)
        for code, tz in FACILITY_TIMEZONES.items():
            tz_expr = F.when(F.col("facility_code") == code, F.from_utc_timestamp(F.col("interval_ts"), tz)).otherwise(tz_expr)

        hourly = (
            filtered
            .withColumn("timestamp_local", tz_expr)
            # Shift interval_start (which represents the start of the hour) by +1 hour
            # so the energy measured over [t, t+1) is labelled at the hour-end (t+1).
            .withColumn("date_hour", F.date_trunc("hour", F.expr("timestamp_local + INTERVAL 1 HOUR")))
            .groupBy("facility_code", "facility_name", "network_code", "network_region", "date_hour")
            .agg(
                F.sum(F.when(F.col("metric") == "energy", F.col("metric_value"))).alias("energy_mwh"),
                F.count(F.when(F.col("metric") == "energy", F.lit(1))).alias("intervals_count")
            )
            .filter(F.col("intervals_count") > 0)
        )
        
        # Physical bounds for energy
        ENERGY_LOWER = 0.0      
        PEAK_REFERENCE_MWH = 186.0  
        
        # Build intermediate columns in single pass to avoid multiple scans
        result = (
            hourly
            .withColumn("completeness_pct", F.lit(100.0))
        )
        
        # Extract hour from localized date_hour (already in facility local time)
        hour_col = F.hour(F.col("date_hour"))
        energy_col = F.col("energy_mwh")
        
        # Define validation checks using column references
        is_within_bounds = energy_col >= ENERGY_LOWER  # Only check lower bound (negative values)
        is_night = ((hour_col >= 22) | (hour_col < 6))
        is_peak = (hour_col >= 10) & (hour_col <= 14)
        
        is_night_anomaly = is_night & (energy_col > 1.0)
        
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
             (energy_col < (F.lit(PEAK_REFERENCE_MWH) * threshold_factor)))
        )
        
        is_efficiency_anomaly = is_peak & (energy_col > 0.5) & (energy_col < (F.lit(PEAK_REFERENCE_MWH) * 0.50))
        
        result = (
            result
            .withColumn(
                "quality_issues",
                F.concat_ws(
                    "|",
                    F.when(~is_within_bounds, F.lit("OUT_OF_BOUNDS")),
                    F.when(is_night_anomaly, F.lit("NIGHT_ENERGY_ANOMALY")),
                    F.when(is_daytime_zero, F.lit("DAYTIME_ZERO_ENERGY")),
                    F.when(is_equipment_downtime, F.lit("EQUIPMENT_DOWNTIME")),
                    F.when(is_transition_hour_low_energy, F.lit("TRANSITION_HOUR_LOW_ENERGY")),
                    F.when(is_efficiency_anomaly, F.lit("PEAK_HOUR_LOW_ENERGY"))
                )
            )
            # Align quality flag labels with Gold layer conventions:
            # - BAD for invalid/out-of-bounds energy
            # - WARNING for anomalous but not outright invalid records
            # - GOOD for everything else
            .withColumn(
                "quality_flag",
                F.when(
                    ~is_within_bounds,
                    F.lit("BAD")
                ).when(
                    is_night_anomaly | is_daytime_zero | is_equipment_downtime | is_transition_hour_low_energy | is_efficiency_anomaly,
                    F.lit("WARNING")
                ).otherwise(F.lit("GOOD"))
            )
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        return result.select(
            "facility_code", "facility_name", "network_code", "network_region",
            "date_hour", "energy_mwh", "intervals_count",
            "quality_flag", "quality_issues", "completeness_pct",
            "created_at", "updated_at"
        )


__all__ = ["SilverHourlyEnergyLoader"]
