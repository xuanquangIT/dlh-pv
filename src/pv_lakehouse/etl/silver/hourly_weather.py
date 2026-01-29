"""Silver loader producing clean_hourly_weather."""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions

LOGGER = logging.getLogger(__name__)


class SilverHourlyWeatherLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_weather"
    silver_table = "lh.silver.clean_hourly_weather"
    timestamp_column = "weather_timestamp"
    partition_cols = ("date_hour",)

    def __init__(self, options: Optional[LoadOptions] = None) -> None:
        if options is None:
            options = LoadOptions()
        else:
            options.target_file_size_mb = min(options.target_file_size_mb, self.DEFAULT_TARGET_FILE_SIZE_MB)
            options.max_records_per_file = min(options.max_records_per_file, self.DEFAULT_MAX_RECORDS_PER_FILE)
        super().__init__(options)

    def _get_timezone_lookback_hours(self) -> int:
        """Weather data is already in local time from API - no timezone conversion needed."""
        return 0

    def run(self) -> int:
        """Process bronze weather data in 7-day chunks to limit memory usage."""
        LOGGER.info("Starting weather Silver ETL for table %s", self.silver_table)
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            LOGGER.warning("No bronze data found for weather loader")
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=7)

    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        """Transform bronze weather data to Silver layer with validation and quality checks.
        
        Args:
            bronze_df: Bronze layer weather DataFrame.
            
        Returns:
            Transformed Silver DataFrame with quality flags, or None if input is invalid.
        """
        # Validation: Check input DataFrame
        if bronze_df is None or not bronze_df.columns:
            LOGGER.warning("Empty bronze DataFrame provided to weather transform()")
            return None

        required_columns = {"facility_code", "facility_name", "weather_timestamp"}
        missing = required_columns - set(bronze_df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in bronze weather source: {sorted(missing)}")

        # Load configuration and validators
        bounds_config = self._get_bounds_config("weather")
        quality_thresholds = self._get_quality_thresholds("weather")
        numeric_validator = self._get_numeric_validator("weather")
        logic_validator = self._get_logic_validator()
        quality_assigner = self._get_quality_assigner()
        
        LOGGER.debug("Loaded %d validation rules for weather domain", len(bounds_config))

        # Step 1: Prepare base data with timestamp and numeric columns
        numeric_columns = list(bounds_config.keys())
        prepared_base = bronze_df.select(
            "facility_code",
            "facility_name",
            F.col("weather_timestamp").cast("timestamp").alias("timestamp_local"),
            *[F.col(col) for col in numeric_columns if col in bronze_df.columns],
        ).where(
            F.col("facility_code").isNotNull() & F.col("weather_timestamp").isNotNull()
        ).persist()  # Cache: reused in Step 2

        try:
            # Step 2: Add date truncations
            prepared = prepared_base.select(
                "*",
                F.date_trunc("hour", F.col("timestamp_local")).alias("date_hour"),
                F.to_date(F.col("timestamp_local")).alias("date"),
            )

            # Step 3: Handle missing total_column_integrated_water_vapour with forward-fill
            if "total_column_integrated_water_vapour" in prepared.columns:
                window = Window.partitionBy("facility_code", "date").orderBy("timestamp_local").rowsBetween(-100, 0)
                prepared = prepared.select(
                    "*",
                    F.coalesce(
                        F.col("total_column_integrated_water_vapour"),
                        F.last(F.col("total_column_integrated_water_vapour"), ignorenulls=True).over(window),
                    ).alias("total_column_integrated_water_vapour_filled"),
                ).drop("total_column_integrated_water_vapour").withColumnRenamed(
                    "total_column_integrated_water_vapour_filled",
                    "total_column_integrated_water_vapour",
                )

            # Step 4: Round numeric columns in single pass
            rounded_exprs = [
                F.round(F.col(col), 4).alias(col) if col in prepared.columns else F.lit(None).alias(col)
                for col in numeric_columns
            ]
            prepared_rounded = prepared.select(
                "facility_code", "facility_name", "timestamp_local", "date_hour", "date",
                *rounded_exprs,
            )

            # Step 5: Numeric bounds validation
            is_valid_bounds, bound_issues = numeric_validator.validate_all(prepared_rounded.columns)

            # Step 6: Logical validation checks
            timestamp_col = F.col("timestamp_local")
            
            # Night radiation check (uses default night_hours from validator)
            has_night_rad, night_rad_issue = logic_validator.check_night_radiation(
                timestamp_col,
                F.col("shortwave_radiation"),
                threshold=quality_thresholds.get("night_radiation_threshold", 100.0),
            )
            
            # Radiation consistency check
            has_rad_inconsistency, rad_inconsistency_issue = logic_validator.check_radiation_consistency(
                F.col("direct_radiation"),
                F.col("diffuse_radiation"),
                F.col("shortwave_radiation"),
                tolerance_factor=quality_thresholds.get("radiation_consistency_factor", 1.05),
            )
            
            # Cloud-radiation mismatch (uses default peak_hours from validator)
            has_cloud_mismatch, cloud_mismatch_issue = logic_validator.check_cloud_radiation_mismatch(
                timestamp_col,
                F.col("cloud_cover"),
                F.col("shortwave_radiation"),
                cloud_threshold=quality_thresholds.get("high_cloud_threshold", 98.0),
                radiation_threshold=quality_thresholds.get("low_radiation_threshold", 600.0),
            )
            
            has_extreme_temp, extreme_temp_issue = logic_validator.check_extreme_temperature(
                F.col("temperature_2m"),
                min_threshold=quality_thresholds.get("extreme_temp_low", -10.0),
                max_threshold=quality_thresholds.get("extreme_temp_high", 45.0),
            )

            # Step 7: Build quality columns using single select() operation
            has_severe_issue = has_night_rad
            has_warning_issue = has_rad_inconsistency | has_cloud_mismatch | has_extreme_temp
            
            quality_flag = quality_assigner.assign_three_tier(
                is_valid_bounds, has_severe_issue, has_warning_issue
            )
            
            quality_issues = quality_assigner.build_issues_string([
                bound_issues,
                night_rad_issue,
                rad_inconsistency_issue,
                cloud_mismatch_issue,
                extreme_temp_issue,
            ])
            
            is_valid = is_valid_bounds & ~has_severe_issue & ~has_warning_issue

            # Step 8: Final result with all columns in single select()
            result = prepared_rounded.select(
                "facility_code",
                "facility_name",
                F.col("timestamp_local").alias("timestamp"),
                "date_hour",
                "date",
                *numeric_columns,
                is_valid.alias("is_valid"),
                quality_flag.alias("quality_flag"),
                quality_issues.alias("quality_issues"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )

            LOGGER.info("Weather transform completed successfully")
            return result
        finally:
            # Cleanup cached DataFrames - guaranteed even on exception
            prepared_base.unpersist()


__all__ = ["SilverHourlyWeatherLoader"]