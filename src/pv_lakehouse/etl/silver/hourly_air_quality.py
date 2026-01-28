"""Silver loader producing clean_hourly_air_quality."""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions

LOGGER = logging.getLogger(__name__)


class SilverHourlyAirQualityLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_air_quality"
    silver_table = "lh.silver.clean_hourly_air_quality"
    timestamp_column = "air_timestamp"
    partition_cols = ("date_hour",)

    def __init__(self, options: Optional[LoadOptions] = None) -> None:
        if options is None:
            options = LoadOptions()
        else:
            options.target_file_size_mb = min(options.target_file_size_mb, self.DEFAULT_TARGET_FILE_SIZE_MB)
            options.max_records_per_file = min(options.max_records_per_file, self.DEFAULT_MAX_RECORDS_PER_FILE)
        super().__init__(options)

    def _get_timezone_lookback_hours(self) -> int:
        """Air quality data is already in local time from API - no timezone conversion needed."""
        return 0

    def run(self) -> int:
        """Process bronze air quality data in 7-day chunks to limit memory usage."""
        LOGGER.info("Starting air quality Silver ETL for table %s", self.silver_table)
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            LOGGER.warning("No bronze data found for air quality loader")
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=7)

    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        """Transform bronze air quality data to Silver layer with validation and quality checks.
        
        Args:
            bronze_df: Bronze layer air quality DataFrame.
            
        Returns:
            Transformed Silver DataFrame with quality flags, or None if input is invalid.
        """
        # Validation: Check input DataFrame
        if bronze_df is None or not bronze_df.columns:
            LOGGER.warning("Empty bronze DataFrame provided to air quality transform()")
            return None

        required_columns = {"facility_code", "facility_name", "air_timestamp"}
        missing = required_columns - set(bronze_df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in bronze air-quality source: {sorted(missing)}")

        # Load configuration and validators
        bounds_config = self._get_bounds_config("air_quality")
        quality_thresholds = self._get_quality_thresholds("air_quality")
        numeric_validator = self._get_numeric_validator("air_quality")
        quality_assigner = self._get_quality_assigner()
        
        LOGGER.debug("Loaded %d validation rules for air quality domain", len(bounds_config))

        # Step 1: Prepare base data with timestamp and numeric columns
        numeric_columns = list(bounds_config.keys())
        prepared_base = bronze_df.select(
            "facility_code",
            "facility_name",
            F.col("air_timestamp").cast("timestamp").alias("timestamp_local"),
            *[F.col(col) for col in numeric_columns if col in bronze_df.columns],
        ).where(
            F.col("facility_code").isNotNull() & F.col("air_timestamp").isNotNull()
        ).persist()  # Cache: reused in Step 2

        # Step 2: Add date truncations
        prepared = prepared_base.select(
            "*",
            F.date_trunc("hour", F.col("timestamp_local")).alias("date_hour"),
            F.to_date(F.col("timestamp_local")).alias("date"),
        )

        # Step 3: Round numeric columns and calculate AQI in single pass
        rounded_exprs = [F.round(F.col(col), 4).alias(col) for col in numeric_columns]
        
        aqi_value = self._aqi_from_pm25(F.col("pm2_5"))
        aqi_category = (
            F.when(F.col("aqi_value").isNull(), F.lit(None))
            .when(aqi_value <= F.lit(50), F.lit("Good"))
            .when(aqi_value <= F.lit(100), F.lit("Moderate"))
            .when(aqi_value <= F.lit(200), F.lit("Unhealthy"))
            .otherwise(F.lit("Hazardous"))
        )
        
        prepared_with_aqi = prepared.select(
            "facility_code", "facility_name", "timestamp_local", "date_hour", "date",
            *rounded_exprs,
            F.round(aqi_value).cast("int").alias("aqi_value"),
            aqi_category.alias("aqi_category"),
        )

        # Step 4: Numeric bounds validation
        is_valid_bounds, bound_issues = numeric_validator.validate_all(prepared_with_aqi.columns)

        # Step 5: AQI validation
        aqi_min = quality_thresholds.get("aqi_min", 0)
        aqi_max = quality_thresholds.get("aqi_max", 500)
        aqi_valid = (
            F.col("aqi_value").isNull()
            | ((F.col("aqi_value") >= F.lit(aqi_min)) & (F.col("aqi_value") <= F.lit(aqi_max)))
        )
        aqi_issue = F.when(
            F.col("aqi_value").isNotNull() & ~aqi_valid,
            F.lit("AQI_OUT_OF_RANGE")
        ).otherwise(F.lit(""))

        # Step 6: Build quality columns (binary: GOOD/WARNING)
        is_valid_overall = is_valid_bounds & aqi_valid
        quality_issues = quality_assigner.build_issues_string([bound_issues, aqi_issue])
        quality_flag = quality_assigner.assign_binary(is_valid_overall, "GOOD", "WARNING")

        # Step 7: Final result with all columns in single select()
        result = prepared_with_aqi.select(
            "facility_code",
            "facility_name",
            F.col("timestamp_local").alias("timestamp"),
            "date_hour",
            "date",
            "pm2_5", "pm10", "dust", "nitrogen_dioxide", "ozone",
            "sulphur_dioxide", "carbon_monoxide", "uv_index", "uv_index_clear_sky",
            "aqi_category",
            "aqi_value",
            is_valid_overall.alias("is_valid"),
            quality_flag.alias("quality_flag"),
            quality_issues.alias("quality_issues"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )

        # Cleanup cached DataFrames
        prepared_base.unpersist()

        LOGGER.info("Air quality transform completed successfully")
        return result

    def _aqi_from_pm25(self, column: F.Column) -> F.Column:
        """Calculate AQI (Air Quality Index) from PM2.5 concentration using EPA breakpoints."""
        def scale(col: F.Column, c_low: float, c_high: float, aqi_low: int, aqi_high: int) -> F.Column:
            return ((col - F.lit(c_low)) / F.lit(c_high - c_low)) * F.lit(aqi_high - aqi_low) + F.lit(aqi_low)

        return (
            F.when(column.isNull(), None)
            .when(column <= F.lit(12.0), scale(column, 0.0, 12.0, 0, 50))
            .when(column <= F.lit(35.4), scale(column, 12.1, 35.4, 51, 100))
            .when(column <= F.lit(55.4), scale(column, 35.5, 55.4, 101, 150))
            .when(column <= F.lit(150.4), scale(column, 55.5, 150.4, 151, 200))
            .when(column <= F.lit(250.4), scale(column, 150.5, 250.4, 201, 300))
            .otherwise(scale(F.least(column, F.lit(500.0)), 250.5, 500.0, 301, 500))
        )


__all__ = ["SilverHourlyAirQualityLoader"]
