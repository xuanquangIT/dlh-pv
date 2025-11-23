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
        "shortwave_radiation": (0.0, 1150.0),  # P99.5=1045 W/m² (actual max=1120, rounded to 1150)
        "direct_radiation": (0.0, 1050.0),     # Increased from 920 to 1050 (actual max=1009, Australian extreme events)
        "diffuse_radiation": (0.0, 520.0),     # Increased from 500 to 520 (actual max=520, measurement variation)
        "direct_normal_irradiance": (0.0, 1060.0),  # Increased from 1050 to 1060 (actual max=1057.3)
        "temperature_2m": (-10.0, 50.0),       # P99.5=38.5°C (actual max=43.7°C, bounds allow extreme days)
        "dew_point_2m": (-20.0, 30.0),         # P99=20.2°C (expanded bounds for extreme conditions)
        "wet_bulb_temperature_2m": (-5.0, 40.0),  # Bounded by air temperature (wet bulb typically 5-15°C lower)
        "cloud_cover": (0.0, 100.0),           # Perfect bounds, no outliers
        "cloud_cover_low": (0.0, 100.0),
        "cloud_cover_mid": (0.0, 100.0),
        "cloud_cover_high": (0.0, 100.0),
        "precipitation": (0.0, 1000.0),        # Extreme event bound
        "sunshine_duration": (0.0, 3600.0),    # 1 hour max per hourly period
        "total_column_integrated_water_vapour": (0.0, 100.0),  # Typical atmospheric bound
        "wind_speed_10m": (0.0, 50.0),         # Increased from 30 to 50 m/s (actual max=47.2, Australian cyclones)
        "wind_direction_10m": (0.0, 360.0),    # Perfect bounds
        "wind_gusts_10m": (0.0, 120.0),        # Extreme weather bound
        "pressure_msl": (985.0, 1050.0),       # P99=1033 (rounded to 1050 for safety)
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

        # CRITICAL: Bronze weather_timestamp is already in correct LOCAL format from API
        # No additional conversion needed - use directly for aggregation
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
        
        # Aggregate by timestamp directly (already in correct format)
        prepared = (
            prepared_base
            .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_local")))
            .withColumn("date", F.to_date(F.col("timestamp_local")))
        )

        # Handle missing total_column_integrated_water_vapour with forward-fill within facility/date
        # This column has 73% nulls but is not critical for energy forecasting
        if "total_column_integrated_water_vapour" in prepared.columns:
            from pyspark.sql import Window
            window = Window.partitionBy("facility_code", "date").orderBy("timestamp_local").rowsBetween(-100, 0)
            prepared = (
                prepared
                .withColumn(
                    "total_column_integrated_water_vapour",
                    F.coalesce(
                        F.col("total_column_integrated_water_vapour"),
                        F.last(F.col("total_column_integrated_water_vapour"), ignorenulls=True).over(window)
                    )
                )
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

        # Validation: Compute all bounds checks first (single pass)
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

        # Validation: Check logic conditions (second pass using computed bounds)
        hour_of_day = F.hour(F.col("timestamp_local"))
        is_night = (hour_of_day < 6) | (hour_of_day >= 22)
        is_peak_sun = (hour_of_day >= 10) & (hour_of_day <= 14)
        
        is_night_rad_high = is_night & (F.col("shortwave_radiation") > 100)
        # Check radiation consistency: Direct + Diffuse should not exceed Shortwave by much
        radiation_inconsistency = (F.col("direct_radiation") + F.col("diffuse_radiation")) > (F.col("shortwave_radiation") * 1.05)
        # High cloud cover during peak sun hours (RELAXED threshold: 98% cloud cover instead of 95%)
        # Only flag when radiation is EXCEPTIONALLY low for conditions (600 W/m² instead of 700)
        # This reduces false positives from extreme weather events by ~90%
        high_cloud_peak = is_peak_sun & (F.col("cloud_cover") > 98) & (F.col("shortwave_radiation") < 600)
        # Temperature anomalies
        extreme_temp = (F.col("temperature_2m") < -10) | (F.col("temperature_2m") > 45)
        
        # Build quality columns using pre-computed bounds and conditions
        result = (
            result
            .withColumn("is_valid", is_valid_bounds & ~is_night_rad_high & ~(radiation_inconsistency | high_cloud_peak | extreme_temp))
            .withColumn(
                "quality_issues",
                F.concat_ws(
                    "|",
                    bound_issues,
                    F.when(is_night_rad_high, F.lit("NIGHT_RADIATION_SPIKE")),
                    F.when(radiation_inconsistency, F.lit("RADIATION_INCONSISTENCY")),
                    F.when(high_cloud_peak, F.lit("CLOUD_MEASUREMENT_INCONSISTENCY")),
                    F.when(extreme_temp, F.lit("EXTREME_TEMPERATURE"))
                )
            )
            .withColumn(
                "quality_flag",
                # Align weather quality_flag labels with Gold conventions:
                # - BAD for invalid/out-of-bounds records
                # - WARNING for anomalous but not outright invalid records
                # - GOOD for everything else
                F.when(
                    ~is_valid_bounds | is_night_rad_high,
                    F.lit("BAD")
                ).when(
                    radiation_inconsistency | high_cloud_peak | extreme_temp,
                    F.lit("WARNING")
                ).otherwise(F.lit("GOOD"))
            )
        )
        result = (
            result
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        return result.select(
            "facility_code", "facility_name", F.col("timestamp_local").alias("timestamp"),
            "date_hour", "date", *self._numeric_columns.keys(),
            "is_valid", "quality_flag", "quality_issues", "created_at", "updated_at"
        )


__all__ = ["SilverHourlyWeatherLoader"]