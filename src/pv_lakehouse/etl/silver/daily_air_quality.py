"""Silver loader producing clean_daily_air_quality."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseSilverLoader


class SilverDailyAirQualityLoader(BaseSilverLoader):
    bronze_table = "lh.bronze.raw_facility_air_quality"
    silver_table = "lh.silver.clean_daily_air_quality"
    s3_base_path = "s3a://lakehouse/silver/clean_daily_air_quality"
    timestamp_column = "air_timestamp"
    partition_cols = ("date",)

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

        prepared = (
            bronze_df.select(
                "facility_code",
                "facility_name",
                F.coalesce(F.col("air_date"), F.to_date("air_timestamp")).alias("date"),
                *[F.col(column) for column in self._numeric_columns.keys() if column in bronze_df.columns],
            )
            .where(F.col("facility_code").isNotNull())
            .where(F.col("date").isNotNull())
        )

        if prepared.rdd.isEmpty():
            return None

        aggregations = [
            F.avg(column).alias(column)
            for column in self._numeric_columns.keys()
            if column in prepared.columns
        ]

        grouped = prepared.groupBy("facility_code", "facility_name", "date").agg(*aggregations)

        for column in self._numeric_columns.keys():
            if column not in grouped.columns:
                grouped = grouped.withColumn(column, F.lit(None))
            else:
                grouped = grouped.withColumn(column, F.round(F.col(column), 4))

        aqi_value = self._aqi_from_pm25(F.col("pm2_5"))
        grouped = grouped.withColumn("aqi_value", F.round(aqi_value).cast("int"))
        grouped = grouped.withColumn(
            "aqi_category",
            F.when(F.col("aqi_value").isNull(), F.lit(None))
            .when(F.col("aqi_value") <= 50, F.lit("Good"))
            .when(F.col("aqi_value") <= 100, F.lit("Moderate"))
            .when(F.col("aqi_value") <= 200, F.lit("Unhealthy"))
            .otherwise(F.lit("Hazardous")),
        )

        validity_conditions = [
            (F.col(column).isNull())
            | ((F.col(column) >= F.lit(min_value)) & (F.col(column) <= F.lit(max_value)))
            for column, (min_value, max_value) in self._numeric_columns.items()
        ]
        validity_conditions.append(
            (F.col("aqi_value").isNull())
            | ((F.col("aqi_value") >= F.lit(0)) & (F.col("aqi_value") <= F.lit(500)))
        )
        is_valid_expr = F.reduce(lambda acc, expr: acc & expr, validity_conditions[1:], validity_conditions[0])

        result = grouped.withColumn("is_valid", is_valid_expr)
        result = result.withColumn(
            "quality_flag",
            F.when(F.col("is_valid"), F.lit("GOOD")).otherwise(F.lit("OUT_OF_RANGE")),
        )

        result = result.repartition("facility_code", "date")

        current_ts = F.current_timestamp()
        result = result.withColumn("created_at", current_ts).withColumn("updated_at", current_ts)

        ordered_columns = [
            "facility_code",
            "facility_name",
            "date",
            "pm2_5",
            "pm10",
            "dust",
            "nitrogen_dioxide",
            "ozone",
            "sulphur_dioxide",
            "carbon_monoxide",
            "uv_index",
            "uv_index_clear_sky",
            "aqi_category",
            "aqi_value",
            "is_valid",
            "quality_flag",
            "created_at",
            "updated_at",
        ]
        return result.select(*ordered_columns)

    def _aqi_from_pm25(self, column: F.Column) -> F.Column:
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


__all__ = ["SilverDailyAirQualityLoader"]
