"""Silver loader producing clean_daily_weather."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader


class SilverDailyWeatherLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_weather"
    silver_table = "lh.silver.clean_daily_weather"
    s3_base_path = "s3a://lakehouse/silver/clean_daily_weather"
    timestamp_column = "weather_timestamp"
    partition_cols = ("date",)

    _numeric_columns = {
        "shortwave_radiation": (0.0, 1500.0, "avg"),
        "direct_radiation": (0.0, 1500.0, "avg"),
        "diffuse_radiation": (0.0, 1500.0, "avg"),
        "direct_normal_irradiance": (0.0, 1500.0, "avg"),
        "temperature_2m": (-50.0, 60.0, "avg"),
        "dew_point_2m": (-50.0, 60.0, "avg"),
        "wet_bulb_temperature_2m": (-50.0, 60.0, "avg"),
        "cloud_cover": (0.0, 100.0, "avg"),
        "cloud_cover_low": (0.0, 100.0, "avg"),
        "cloud_cover_mid": (0.0, 100.0, "avg"),
        "cloud_cover_high": (0.0, 100.0, "avg"),
        "precipitation": (0.0, 1000.0, "sum"),
        "sunshine_duration": (0.0, 86_400.0, "sum"),
        "total_column_integrated_water_vapour": (0.0, 100.0, "avg"),
        "wind_speed_10m": (0.0, 60.0, "avg"),
        "wind_direction_10m": (0.0, 360.0, "avg"),
        "wind_gusts_10m": (0.0, 120.0, "max"),
        "pressure_msl": (800.0, 1100.0, "avg"),
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

        prepared = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                F.coalesce(F.col("weather_date"), F.to_date("weather_timestamp")).alias("date"),
                *[F.col(column) for column in self._numeric_columns.keys() if column in bronze_df.columns],
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("date").isNotNull())
        )

        if prepared.rdd.isEmpty():
            return None

        aggregations = [
            (F.sum(column) if agg_type == "sum" else F.max(column) if agg_type == "max" else F.avg(column)).alias(column)
            for column, (_, _, agg_type) in self._numeric_columns.items()
            if column in prepared.columns
        ]

        grouped = prepared.groupBy("facility_code", "facility_name", "date").agg(*aggregations)

        for column, (min_value, max_value, _) in self._numeric_columns.items():
            if column not in grouped.columns:
                grouped = grouped.withColumn(column, F.lit(None))
            else:
                grouped = grouped.withColumn(column, F.round(F.col(column), 4))

        validity_conditions = [
            (F.col(column).isNull())
            | ((F.col(column) >= F.lit(min_value)) & (F.col(column) <= F.lit(max_value)))
            for column, (min_value, max_value, _) in self._numeric_columns.items()
        ]
        is_valid_expr = F.reduce(lambda acc, expr: acc & expr, validity_conditions[1:], validity_conditions[0])

        result = grouped.withColumn("is_valid", is_valid_expr)
        result = result.withColumn(
            "quality_flag",
            F.when(F.col("is_valid"), F.lit("GOOD")).otherwise(F.lit("OUT_OF_RANGE")),
        )

        # Avoid massive single-partition writes when materialising full history
        result = result.repartition("facility_code", "date")

        current_ts = F.current_timestamp()
        result = result.withColumn("created_at", current_ts).withColumn("updated_at", current_ts)

        ordered_columns = [
            "facility_code",
            "facility_name",
            "date",
            *self._numeric_columns.keys(),
            "is_valid",
            "quality_flag",
            "created_at",
            "updated_at",
        ]
        return result.select(*ordered_columns)


__all__ = ["SilverDailyWeatherLoader"]
