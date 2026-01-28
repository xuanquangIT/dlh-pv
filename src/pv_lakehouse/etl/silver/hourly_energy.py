"""Silver loader producing clean_hourly_energy."""

from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions
from pv_lakehouse.etl.bronze.facility_timezones import FACILITY_TIMEZONES, DEFAULT_TIMEZONE

LOGGER = logging.getLogger(__name__)


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
        LOGGER.info("Starting energy Silver ETL for table %s", self.silver_table)
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            LOGGER.warning("No bronze data found for energy loader")
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=7)

    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        """Transform bronze energy data to Silver layer with validation and quality checks.
        
        Args:
            bronze_df: Bronze layer energy DataFrame.
            
        Returns:
            Transformed Silver DataFrame with quality flags, or None if input is invalid.
        """
        # Validation: Check input DataFrame
        if bronze_df is None or not bronze_df.columns:
            LOGGER.warning("Empty bronze DataFrame provided to energy transform()")
            return None

        required_columns = {
            "facility_code", "facility_name", "network_code",
            "network_region", "metric", "value", "interval_ts"
        }
        missing = required_columns - set(bronze_df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in bronze timeseries source: {sorted(missing)}")

        # Load configuration and validators
        quality_thresholds = self._get_quality_thresholds("energy")
        numeric_validator = self._get_numeric_validator("energy")
        logic_validator = self._get_logic_validator()
        quality_assigner = self._get_quality_assigner()
        
        LOGGER.debug("Loaded validation config for energy domain")

        # Step 1: Filter and cast bronze data
        filtered = bronze_df.select(
            "facility_code",
            "facility_name",
            "network_code",
            "network_region",
            "metric",
            F.col("value").cast("double").alias("metric_value"),
            F.col("interval_ts").cast("timestamp").alias("interval_ts"),
        ).where(
            F.col("facility_code").isNotNull()
            & F.col("interval_ts").isNotNull()
            & F.col("metric").isin("energy", "power")
        )

        # Step 2: Convert to local time and aggregate by hour
        # Convert UTC to facility-specific timezone
        default_local = F.from_utc_timestamp(F.col("interval_ts"), DEFAULT_TIMEZONE)
        tz_expr = default_local
        for code, tz in FACILITY_TIMEZONES.items():
            tz_expr = F.when(
                F.col("facility_code") == code,
                F.from_utc_timestamp(F.col("interval_ts"), tz)
            ).otherwise(tz_expr)

        # Shift by +1 hour (energy for [t, t+1) labeled at t+1)
        hourly = filtered.select(
            "*",
            tz_expr.alias("timestamp_local"),
            F.date_trunc("hour", F.expr("timestamp_local + INTERVAL 1 HOUR")).alias("date_hour"),
        ).groupBy(
            "facility_code", "facility_name", "network_code", "network_region", "date_hour"
        ).agg(
            F.sum(F.when(F.col("metric") == "energy", F.col("metric_value"))).alias("energy_mwh"),
            F.count(F.when(F.col("metric") == "energy", F.lit(1))).alias("intervals_count"),
        ).filter(F.col("intervals_count") > 0).persist()  # Cache: heavily reused in validation

        # Step 3: Numeric bounds validation
        energy_col = F.col("energy_mwh")
        energy_min = quality_thresholds.get("energy_min", 0.0)
        is_within_bounds = energy_col >= F.lit(energy_min)
        bound_issue = F.when(~is_within_bounds, F.lit("OUT_OF_BOUNDS")).otherwise(F.lit(""))

        # Step 4: Logical validation checks
        timestamp_col = F.col("date_hour")
        hour_col = F.hour(timestamp_col)
        
        # Night energy anomaly (uses default night_hours from validator)
        has_night_energy, night_energy_issue = logic_validator.check_night_energy(
            timestamp_col,
            energy_col,
            threshold=quality_thresholds.get("night_energy_threshold", 1.0),
        )
        
        # Daytime zero energy (uses default daytime_hours from validator)
        has_daytime_zero, daytime_zero_issue = logic_validator.check_daytime_zero_energy(
            timestamp_col,
            energy_col,
        )
        
        # Equipment downtime (uses default peak_hours from validator)
        has_downtime, downtime_issue = logic_validator.check_equipment_downtime(
            timestamp_col,
            energy_col,
        )
        
        # Transition hour low energy (sunrise/sunset ramps)
        sunrise_hours = quality_thresholds.get("sunrise_hours", [6, 7])
        early_morning_hours = quality_thresholds.get("early_morning_hours", [8, 9])
        sunset_hours = quality_thresholds.get("sunset_hours", [17, 18])
        peak_reference = quality_thresholds.get("peak_reference", 186.0)
        peak_hours = quality_thresholds.get("peak_hours", [10, 11, 12, 13, 14])
        
        is_sunrise = hour_col.isin(sunrise_hours)
        is_early_morning = hour_col.isin(early_morning_hours)
        is_sunset = hour_col.isin(sunset_hours)
        is_peak = hour_col.isin(peak_hours)
        
        threshold_factor = (
            F.when(is_sunrise, F.lit(quality_thresholds.get("sunrise_threshold_pct", 0.05)))
            .when(is_early_morning, F.lit(quality_thresholds.get("early_morning_threshold_pct", 0.08)))
            .when(is_sunset, F.lit(quality_thresholds.get("sunset_threshold_pct", 0.10)))
            .otherwise(F.lit(0.0))
        )
        
        has_transition_low = (
            (is_sunrise | is_early_morning | is_sunset)
            & (energy_col > F.lit(0.01))
            & (energy_col < (F.lit(peak_reference) * threshold_factor))
        )
        transition_issue = F.when(has_transition_low, F.lit("TRANSITION_HOUR_LOW_ENERGY")).otherwise(F.lit(""))
        
        # Peak hour low efficiency (reuses is_peak from above)
        peak_efficiency_threshold = quality_thresholds.get("peak_efficiency_threshold_pct", 0.50)
        has_low_efficiency = (
            is_peak
            & (energy_col > F.lit(0.5))
            & (energy_col < (F.lit(peak_reference) * F.lit(peak_efficiency_threshold)))
        )
        efficiency_issue = F.when(has_low_efficiency, F.lit("PEAK_HOUR_LOW_ENERGY")).otherwise(F.lit(""))

        # Step 5: Build quality columns using single select() operation
        has_severe_issue = ~is_within_bounds | has_night_energy
        has_warning_issue = has_daytime_zero | has_downtime | has_transition_low | has_low_efficiency
        
        quality_flag = quality_assigner.assign_three_tier(
            F.lit(True),  # Bounds checked separately via is_within_bounds
            has_severe_issue,
            has_warning_issue,
        )
        
        quality_issues = quality_assigner.build_issues_string([
            bound_issue,
            night_energy_issue,
            daytime_zero_issue,
            downtime_issue,
            transition_issue,
            efficiency_issue,
        ])

        # Step 6: Final result with all columns in single select()
        result = hourly.select(
            "facility_code",
            "facility_name",
            "network_code",
            "network_region",
            "date_hour",
            "energy_mwh",
            "intervals_count",
            quality_flag.alias("quality_flag"),
            quality_issues.alias("quality_issues"),
            F.lit(100.0).alias("completeness_pct"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )

        # Cleanup cached DataFrames
        hourly.unpersist()

        LOGGER.info("Energy transform completed successfully")
        return result


__all__ = ["SilverHourlyEnergyLoader"]
