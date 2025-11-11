"""Silver loader producing clean_hourly_air_quality."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions


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

    def run(self) -> int:
        """Process bronze air quality data in 3-day chunks to limit memory usage."""
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=3)

    _numeric_columns = {
        "pm2_5": (0.0, 500.0),
        "pm10": (0.0, 500.0),
        "dust": (0.0, 500.0),
        "nitrogen_dioxide": (0.0, 500.0),
        "ozone": (0.0, 500.0),
        "sulphur_dioxide": (0.0, 500.0),
        "carbon_monoxide": (0.0, 500.0),
        "uv_index": (0.0, 15.0),
        "uv_index_clear_sky": (0.0, 15.0),
    }

    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        if bronze_df is None or not bronze_df.columns:
            return None

        required_columns = {
            "facility_code",
            "facility_name",
            "air_timestamp",
        }
        missing = required_columns - set(bronze_df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in bronze air-quality source: {sorted(missing)}")

        # Bronze timestamps are already in facility's local timezone
        prepared_base = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                F.col("air_timestamp").cast("timestamp").alias("timestamp_local"),
                *[F.col(column) for column in self._numeric_columns.keys() if column in bronze_df.columns],
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("air_timestamp").isNotNull())
        )
        
        # No conversion needed - aggregate by local time
        prepared = prepared_base.withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_local"))) \
                                 .withColumn("date", F.to_date(F.col("timestamp_local")))

        # Apply numeric column rounding and add missing columns
        result = prepared
        for column, (min_value, max_value) in self._numeric_columns.items():
            if column not in result.columns:
                result = result.withColumn(column, F.lit(None))
            else:
                result = result.withColumn(column, F.round(F.col(column), 4))

        # Calculate AQI from PM2.5 for each hourly record
        aqi_value = self._aqi_from_pm25(F.col("pm2_5"))
        result = result.withColumn("aqi_value", F.round(aqi_value).cast("int"))
        result = result.withColumn(
            "aqi_category",
            F.when(F.col("aqi_value").isNull(), F.lit(None))
            .when(F.col("aqi_value") <= 50, F.lit("Good"))
            .when(F.col("aqi_value") <= 100, F.lit("Moderate"))
            .when(F.col("aqi_value") <= 200, F.lit("Unhealthy"))
            .otherwise(F.lit("Hazardous")),
        )

        # Validation: each column within range + AQI valid (0-500)
        is_valid_expr = F.lit(True)
        for column, (min_val, max_val) in self._numeric_columns.items():
            is_valid_expr = is_valid_expr & (
                F.col(column).isNull() | ((F.col(column) >= min_val) & (F.col(column) <= max_val))
            )
        is_valid_expr = is_valid_expr & (
            F.col("aqi_value").isNull() | ((F.col("aqi_value") >= 0) & (F.col("aqi_value") <= 500))
        )

        result = (
            result
            .withColumn("is_valid", is_valid_expr)
            .withColumn("quality_flag", F.when(F.col("is_valid"), "GOOD").otherwise("OUT_OF_RANGE"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        return result.select(
            "facility_code", "facility_name", F.col("timestamp_local").alias("timestamp"),
            "date_hour", "date",
            "pm2_5", "pm10", "dust", "nitrogen_dioxide", "ozone", "sulphur_dioxide", "carbon_monoxide",
            "uv_index", "uv_index_clear_sky", "aqi_category", "aqi_value",
            "is_valid", "quality_flag", "created_at", "updated_at"
        )

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
