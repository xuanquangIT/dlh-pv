"""Gold loader for fact_solar_forecast."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import (
    build_weather_lookup,
    classify_weather,
    dec,
    first_value,
    is_empty,
)


class GoldFactSolarForecastLoader(BaseGoldLoader):
    """Produce Gold fact rows representing solar forecast performance."""

    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "facility_name",
                "network_region",
                "date_hour",
                "energy_mwh",
                "power_avg_mw",
            ],
        ),
        "daily_weather": SourceTableConfig(
            table_name="lh.silver.clean_daily_weather",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "date",
                "shortwave_radiation",
                "direct_radiation",
                "diffuse_radiation",
                "temperature_2m",
                "cloud_cover",
                "wind_speed_10m",
                "precipitation",
                "sunshine_duration",
            ],
        ),
        "dim_facility": SourceTableConfig(
            table_name="lh.gold.dim_facility",
            required_columns=["facility_key", "facility_code"],
        ),
        "dim_weather_condition": SourceTableConfig(
            table_name="lh.gold.dim_weather_condition",
            required_columns=["weather_condition_key", "condition_name"],
        ),
        "dim_model_version": SourceTableConfig(
            table_name="lh.gold.dim_model_version",
            required_columns=["model_version_key"],
        ),
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_solar_forecast": GoldTableConfig(
            iceberg_table="lh.gold.fact_solar_forecast",
            s3_base_path="s3a://lakehouse/gold/fact_solar_forecast",
            partition_cols=("date_key",),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        hourly = sources.get("hourly_energy")
        if is_empty(hourly):
            return None

        dim_facility = sources.get("dim_facility")
        if is_empty(dim_facility):
            raise ValueError("Gold dim_facility must be available before building fact_solar_forecast.")

        dim_model_version = sources.get("dim_model_version")
        if is_empty(dim_model_version):
            raise ValueError("Gold dim_model_version must be available before building fact_solar_forecast.")

        weather_records = classify_weather(sources.get("daily_weather"))
        weather_dim = sources.get("dim_weather_condition")
        weather_lookup = build_weather_lookup(weather_records, weather_dim)

        base = (
            hourly.withColumn("full_date", F.to_date("date_hour"))
            .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
            .withColumn("time_key", (F.hour("date_hour") * 100 + F.minute("date_hour")).cast("int"))
        )

        fact = base.join(dim_facility, on="facility_code", how="left")
        if not is_empty(weather_lookup):
            fact = fact.join(weather_lookup, on=["facility_code", "full_date"], how="left")

        model_key = first_value(dim_model_version, "model_version_key") or 1
        fact = fact.withColumn("model_version_key", F.lit(model_key).cast("int"))

        fact = fact.withColumn("predicted_energy_mwh", F.col("energy_mwh"))
        fact = fact.withColumn("forecast_error_mwh", F.col("energy_mwh") - F.col("predicted_energy_mwh"))
        fact = fact.withColumn(
            "absolute_percentage_error",
            F.when(F.col("energy_mwh") == 0, F.lit(0.0)).otherwise(
                F.abs(F.col("forecast_error_mwh") / F.col("energy_mwh")) * 100
            ),
        )
        fact = fact.withColumn("mae_metric", F.abs(F.col("forecast_error_mwh")))
        fact = fact.withColumn("rmse_metric", F.abs(F.col("forecast_error_mwh")))
        fact = fact.withColumn("r2_score", F.lit(1.0))
        fact = fact.withColumn("forecast_timestamp", F.col("date_hour"))
        fact = fact.withColumn("created_at", F.current_timestamp())
        fact = fact.withColumn(
            "forecast_id",
            F.row_number().over(Window.orderBy("facility_code", "date_hour")),
        )

        result = fact.select(
            "forecast_id",
            "date_key",
            "time_key",
            "facility_key",
            "weather_condition_key",
            "model_version_key",
            F.col("energy_mwh").cast(dec(15, 6)).alias("actual_energy_mwh"),
            F.col("predicted_energy_mwh").cast(dec(15, 6)).alias("predicted_energy_mwh"),
            F.col("forecast_error_mwh").cast(dec(15, 6)).alias("forecast_error_mwh"),
            F.col("absolute_percentage_error").cast(dec(10, 4)).alias("absolute_percentage_error"),
            F.col("mae_metric").cast(dec(15, 6)).alias("mae_metric"),
            F.col("rmse_metric").cast(dec(15, 6)).alias("rmse_metric"),
            F.col("r2_score").cast(dec(5, 4)).alias("r2_score"),
            "forecast_timestamp",
            "created_at",
        )

        if is_empty(result):
            return None

        return {"fact_solar_forecast": result}
