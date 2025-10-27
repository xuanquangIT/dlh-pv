"""Shared helpers for Gold-zone loaders."""

from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def dec(precision: int, scale: int) -> T.DecimalType:
    """Convenience helper for DecimalType declarations."""

    return T.DecimalType(precision, scale)


def is_empty(dataframe: Optional[DataFrame]) -> bool:
    if dataframe is None:
        return True
    return dataframe.rdd.isEmpty()


def first_value(dataframe: Optional[DataFrame], column: str) -> Optional[Any]:
    if is_empty(dataframe) or column not in dataframe.columns:
        return None
    row = dataframe.select(column).limit(1).collect()
    if not row:
        return None
    return row[0][column]


def compute_date_key(date_col: Column) -> Column:
    return F.date_format(date_col, "yyyyMMdd").cast("int")


def compute_time_key(timestamp_col: Column) -> Column:
    return (F.hour(timestamp_col) * F.lit(100) + F.minute(timestamp_col)).cast("int")


def season_expr(month_col: Column) -> Column:
    return (
        F.when(month_col.isin(12, 1, 2), F.lit("Summer"))
        .when(month_col.isin(3, 4, 5), F.lit("Autumn"))
        .when(month_col.isin(6, 7, 8), F.lit("Winter"))
        .otherwise(F.lit("Spring"))
    )


def time_of_day_expr(hour_col: Column) -> Column:
    return (
        F.when((hour_col >= 6) & (hour_col < 12), F.lit("Morning"))
        .when((hour_col >= 12) & (hour_col < 17), F.lit("Afternoon"))
        .when((hour_col >= 17) & (hour_col < 21), F.lit("Evening"))
        .otherwise(F.lit("Night"))
    )


def classify_weather(dataframe: Optional[DataFrame]) -> Optional[DataFrame]:
    if is_empty(dataframe):
        return None
    enriched = dataframe.withColumn(
        "condition_name",
        F.when(F.col("shortwave_radiation") >= 800, F.lit("High Radiation"))
        .when(F.col("shortwave_radiation") >= 300, F.lit("Moderate Radiation"))
        .otherwise(F.lit("Low Radiation")),
    )
    enriched = enriched.withColumn(
        "radiation_level",
        F.when(F.col("shortwave_radiation") >= 800, F.lit("High"))
        .when(F.col("shortwave_radiation") >= 300, F.lit("Medium"))
        .otherwise(F.lit("Low")),
    )
    enriched = enriched.withColumn(
        "cloud_category",
        F.when(F.col("cloud_cover") <= 20, F.lit("Clear"))
        .when(F.col("cloud_cover") <= 70, F.lit("Partly Cloudy"))
        .otherwise(F.lit("Overcast")),
    )
    enriched = enriched.withColumn(
        "temperature_range",
        F.when(F.col("temperature_2m") <= 10, F.lit("Cold"))
        .when(F.col("temperature_2m") <= 25, F.lit("Mild"))
        .otherwise(F.lit("Hot")),
    )
    enriched = enriched.withColumn(
        "weather_severity",
        F.when(F.col("shortwave_radiation") < 100, F.lit("High"))
        .when(F.col("cloud_cover") > 80, F.lit("Medium"))
        .otherwise(F.lit("Low")),
    )
    return enriched


def classify_air_quality(dataframe: Optional[DataFrame]) -> Optional[DataFrame]:
    if is_empty(dataframe):
        return None
    return dataframe.withColumn(
        "aq_category",
        F.when(F.col("pm2_5") <= 12.0, F.lit("Good"))
        .when(F.col("pm2_5") <= 35.4, F.lit("Moderate"))
        .when(F.col("pm2_5") <= 55.4, F.lit("Unhealthy"))
        .otherwise(F.lit("Hazardous")),
    )


def build_weather_lookup(
    weather_records: Optional[DataFrame],
    dim_weather_condition: Optional[DataFrame],
) -> Optional[DataFrame]:
    if is_empty(weather_records) or is_empty(dim_weather_condition):
        return None
    aggregated = (
        weather_records.groupBy("facility_code", "date", "condition_name")
        .agg(
            F.first("shortwave_radiation").alias("shortwave_radiation"),
            F.first("direct_radiation").alias("direct_radiation"),
            F.first("diffuse_radiation").alias("diffuse_radiation"),
            F.first("temperature_2m").alias("temperature_2m"),
            F.first("cloud_cover").alias("cloud_cover"),
            F.first("wind_speed_10m").alias("wind_speed_10m"),
            F.first("precipitation").alias("precipitation"),
            F.first("sunshine_duration").alias("sunshine_duration"),
            F.first("weather_severity").alias("weather_severity"),
        )
        .join(dim_weather_condition, on="condition_name", how="left")
    )
    return aggregated.select(
        "facility_code",
        F.col("date").alias("full_date"),
        "weather_condition_key",
        "shortwave_radiation",
        "direct_radiation",
        "diffuse_radiation",
        "temperature_2m",
        "cloud_cover",
        "wind_speed_10m",
        "precipitation",
        "sunshine_duration",
        "weather_severity",
    )


def build_air_quality_lookup(
    air_quality_records: Optional[DataFrame],
    dim_air_quality_category: Optional[DataFrame],
) -> Optional[DataFrame]:
    if is_empty(air_quality_records) or is_empty(dim_air_quality_category):
        return None
    joined = air_quality_records.join(
        dim_air_quality_category.select("air_quality_category_key", "category_name"),
        air_quality_records["aq_category"] == F.col("category_name"),
        how="left",
    )
    return joined.select(
        "facility_code",
        F.col("date").alias("full_date"),
        "air_quality_category_key",
        "pm2_5",
        "pm10",
        "dust",
        "nitrogen_dioxide",
        "ozone",
        "uv_index",
    )
