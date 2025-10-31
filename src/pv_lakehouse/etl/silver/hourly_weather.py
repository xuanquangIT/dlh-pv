"""Silver loader producing clean_hourly_weather."""

from __future__ import annotations

from typing import Optional
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader


class SilverHourlyWeatherLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_weather"
    silver_table = "lh.silver.clean_hourly_weather"
    s3_base_path = "s3a://lakehouse/silver/clean_hourly_weather"
    timestamp_column = "weather_timestamp"
    partition_cols = ("date_hour",)

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

        # Keep hourly granularity - no aggregation
        prepared = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                F.col("weather_timestamp").cast("timestamp").alias("timestamp"),
                F.date_trunc("hour", F.col("weather_timestamp")).alias("date_hour"),
                F.to_date(F.col("weather_timestamp")).alias("date"),
                *[F.col(column) for column in self._numeric_columns.keys() if column in bronze_df.columns],
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("weather_timestamp").isNotNull())
        )

        if prepared.rdd.isEmpty():
            return None

        # Apply numeric column rounding and add missing columns
        result = prepared
        for column, (min_value, max_value) in self._numeric_columns.items():
            if column not in result.columns:
                result = result.withColumn(column, F.lit(None))
            else:
                result = result.withColumn(column, F.round(F.col(column), 4))

        # Validation rules for each hourly record
        validity_conditions = [
            (F.col(column).isNull())
            | ((F.col(column) >= F.lit(min_value)) & (F.col(column) <= F.lit(max_value)))
            for column, (min_value, max_value) in self._numeric_columns.items()
        ]
        
        if validity_conditions:
            is_valid_expr = reduce(lambda acc, expr: acc & expr, validity_conditions)
        else:
            is_valid_expr = F.lit(True)

        result = result.withColumn("is_valid", is_valid_expr)
        result = result.withColumn(
            "quality_flag",
            F.when(F.col("is_valid"), F.lit("GOOD")).otherwise(F.lit("OUT_OF_RANGE")),
        )

        # Repartition for efficient writes
        result = result.repartition("facility_code", "date_hour")

        # Add metadata timestamps
        current_ts = F.current_timestamp()
        result = result.withColumn("created_at", current_ts).withColumn("updated_at", current_ts)

        # Select and order columns
        ordered_columns = [
            "facility_code",
            "facility_name",
            "timestamp",
            "date_hour",
            "date",
            *self._numeric_columns.keys(),
            "is_valid",
            "quality_flag",
            "created_at",
            "updated_at",
        ]
        return result.select(*ordered_columns)


__all__ = ["SilverHourlyWeatherLoader"]
