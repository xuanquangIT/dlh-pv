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
        """Process bronze weather data in 7-day chunks to limit memory usage."""
        bronze_df = self._read_bronze()
        if bronze_df is None or not bronze_df.columns:
            return 0
        return self._process_in_chunks(bronze_df, chunk_days=7)

    _numeric_columns = {
        "shortwave_radiation": (0.0, 1000.0),  # Max ~1000 W/m² at Earth surface (1361 extraterrestrial)
        "direct_radiation": (0.0, 1000.0),
        "diffuse_radiation": (0.0, 800.0),    # Diffuse is typically lower
        "direct_normal_irradiance": (0.0, 900.0),
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
        prepared = (
            prepared_base
            .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_local")))
            .withColumn("date", F.to_date(F.col("timestamp_local")))
        )

        # Round numeric columns - use select to apply in one pass
        select_exprs = [
            "facility_code", "facility_name", "timestamp_local", "date_hour", "date"
        ]
        for column in self._numeric_columns.keys():
            if column in prepared.columns:
                select_exprs.append(F.round(F.col(column), 4).alias(column))
            else:
                select_exprs.append(F.lit(None).alias(column))
        
        result = prepared.select(select_exprs)

        # Validation: Check numeric columns within bounds and build quality_issues in single pass
        hour_of_day = F.hour(F.col("timestamp_local"))
        is_night = (hour_of_day < 6) | (hour_of_day >= 22)
        
        # Define radiation checks
        is_unrealistic_rad = F.col("shortwave_radiation") > 1000
        is_sunrise_spike = (hour_of_day == 6) & (F.col("shortwave_radiation") > 500)
        is_inconsistent_radiation = (
            (F.col("direct_normal_irradiance").isNotNull()) & 
            (F.col("shortwave_radiation").isNotNull()) &
            (F.col("direct_normal_irradiance") > 900) & 
            (F.col("shortwave_radiation") < 300)
        )
        is_night_rad_high = is_night & (F.col("shortwave_radiation") > 50)
        
        # Check numeric bounds for all columns
        is_valid_bounds = F.lit(True)
        bound_issues = F.lit("")
        
        for column, (min_val, max_val) in self._numeric_columns.items():
            col_expr = F.col(column)
            col_valid = col_expr.isNull() | ((col_expr >= min_val) & (col_expr <= max_val))
            is_valid_bounds = is_valid_bounds & col_valid
            
            bound_issues = F.concat_ws(
                "|",
                bound_issues,
                F.when((col_expr.isNotNull()) & ~col_valid, F.concat(F.lit(column), F.lit("_OUT_OF_BOUNDS")))
            )
        
        # Build quality_issues in single statement
        result = (
            result
            .withColumn(
                "quality_issues",
                F.concat_ws(
                    "|",
                    bound_issues,
                    F.when(is_night_rad_high, F.lit("NIGHT_RADIATION_SPIKE")),
                    F.when(is_unrealistic_rad, F.lit("UNREALISTIC_RADIATION")),
                    F.when(is_sunrise_spike, F.lit("SUNRISE_SPIKE_ANOMALY")),
                    F.when(is_inconsistent_radiation, F.lit("INCONSISTENT_RADIATION_COMPONENTS"))
                )
            )
            .withColumn(
                "quality_flag",
                F.when(
                    is_unrealistic_rad | is_sunrise_spike | is_inconsistent_radiation | ~is_valid_bounds,
                    F.lit("REJECT")
                ).when(
                    is_night_rad_high,
                    F.lit("CAUTION")
                ).otherwise(F.lit("GOOD"))
            )
            .withColumn("is_valid", is_valid_bounds & ~(is_unrealistic_rad | is_sunrise_spike | is_inconsistent_radiation))
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
            "is_valid", "quality_flag", "quality_issues", "created_at", "updated_at"
        )


__all__ = ["SilverHourlyWeatherLoader"]
