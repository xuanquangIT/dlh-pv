"""Silver loader producing clean_hourly_weather."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader, LoadOptions


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

    def run(self) -> int:
        """Process bronze weather data in 3-day chunks to limit memory usage."""
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=3)

    _numeric_columns = {
        "shortwave_radiation": (0.0, 1500.0),
        "direct_radiation": (0.0, 1500.0),
        "diffuse_radiation": (0.0, 1500.0),
        "direct_normal_irradiance": (0.0, 1500.0),
        "temperature_2m": (-50.0, 60.0),
        "dew_point_2m": (-50.0, 60.0),
        "wet_bulb_temperature_2m": (-50.0, 60.0),
        "cloud_cover": (0.0, 100.0),
        "cloud_cover_low": (0.0, 100.0),
        "cloud_cover_mid": (0.0, 100.0),
        "cloud_cover_high": (0.0, 100.0),
        "precipitation": (0.0, 1000.0),
        "sunshine_duration": (0.0, 3600.0),
        "total_column_integrated_water_vapour": (0.0, 100.0),
        "wind_speed_10m": (0.0, 60.0),
        "wind_direction_10m": (0.0, 360.0),
        "wind_gusts_10m": (0.0, 120.0),
        "pressure_msl": (800.0, 1100.0),
    }

    def transform(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        if bronze_df is None or not bronze_df.columns:
            return None

        required_columns = {
            "facility_code",
            "facility_name",
            "weather_timestamp",
        }
        missing = required_columns - set(bronze_df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in bronze weather source: {sorted(missing)}")

        # Bronze timestamps are already in facility's local timezone
        prepared_base = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                F.col("weather_timestamp").cast("timestamp").alias("timestamp_local"),
                *[F.col(column) for column in self._numeric_columns.keys() if column in bronze_df.columns],
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("weather_timestamp").isNotNull())
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

        # Validation: each column must be NULL or within expected range
        is_valid_expr = F.lit(True)
        for column, (min_val, max_val) in self._numeric_columns.items():
            is_valid_expr = is_valid_expr & (
                F.col(column).isNull() | ((F.col(column) >= min_val) & (F.col(column) <= max_val))
            )

        result = result.withColumn("is_valid", is_valid_expr)
        result = result.withColumn(
            "quality_flag",
            F.when(F.col("is_valid"), F.lit("GOOD")).otherwise(F.lit("OUT_OF_RANGE")),
        )

        # Add metadata and finalize
        result = (
            result
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        # Select output columns (rename timestamp_local to timestamp for schema)
        return result.select(
            "facility_code", "facility_name", F.col("timestamp_local").alias("timestamp"),
            "date_hour", "date", *self._numeric_columns.keys(),
            "is_valid", "quality_flag", "created_at", "updated_at"
        )


__all__ = ["SilverHourlyWeatherLoader"]
