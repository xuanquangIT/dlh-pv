"""Gold loader for fact_weather_impact."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import (
    build_hourly_fact_base,
    build_weather_lookup,
    classify_weather,
    dec,
    is_empty,
    require_sources,
)


class GoldFactWeatherImpactLoader(BaseGoldLoader):
    """Produce weather impact fact rows linking hourly energy with weather conditions."""

    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "date_hour",
                "energy_mwh",
            ],
        ),
        "hourly_weather": SourceTableConfig(
            table_name="lh.silver.clean_hourly_weather",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "date_hour",
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
        "dim_time": SourceTableConfig(
            table_name="lh.gold.dim_time",
            required_columns=["time_key"],
        ),
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_weather_impact": GoldTableConfig(
            iceberg_table="lh.gold.fact_weather_impact",
            s3_base_path="s3a://lakehouse/gold/fact_weather_impact",
            partition_cols=("date_key",),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        hourly = sources.get("hourly_energy")
        if is_empty(hourly):
            return None

        required = require_sources(
            {
                "dim_facility": sources.get("dim_facility"),
                "dim_weather_condition": sources.get("dim_weather_condition"),
                "dim_time": sources.get("dim_time"),
            },
            {
                "dim_facility": "fact_weather_impact",
                "dim_weather_condition": "fact_weather_impact",
                "dim_time": "fact_weather_impact",
            },
        )
        dim_facility = required["dim_facility"]
        dim_weather_condition = required["dim_weather_condition"]
        dim_time = required["dim_time"]

        weather_records = classify_weather(sources.get("hourly_weather"))
        weather_lookup = build_weather_lookup(weather_records, dim_weather_condition)

        base = build_hourly_fact_base(hourly)
        if is_empty(base):
            return None

        # Broadcast small dimension tables to avoid shuffle joins (5-10x faster)
        fact = base.join(F.broadcast(dim_facility), on="facility_code", how="left")
        if not is_empty(weather_lookup):
            # CRITICAL: Join on HOURLY timestamp (date_hour), not just date
            # This ensures 1:1 relationship - each fact row gets exactly one weather record
            # Previous join on (facility, date) caused 35x duplication (one per weather condition)
            fact = fact.withColumn("date_hour", F.col("date_hour").cast("timestamp"))
            fact = fact.join(weather_lookup, on=["facility_code", "date_hour"], how="left")

        fact = fact.join(F.broadcast(dim_time.select("time_key")), on="time_key", how="left")

        fact = fact.withColumn("created_at", F.current_timestamp())
        fact = fact.withColumn(
            "weather_impact_id",
            F.row_number().over(Window.orderBy("facility_code", "date_hour")),
        )

        result = fact.select(
            "weather_impact_id",
            "date_key",
            "time_key",
            "facility_key",
            "weather_condition_key",
            F.col("shortwave_radiation").cast(dec(10, 4)).alias("shortwave_radiation"),
            F.col("direct_radiation").cast(dec(10, 4)).alias("direct_radiation"),
            F.col("diffuse_radiation").cast(dec(10, 4)).alias("diffuse_radiation"),
            F.col("temperature_2m").cast(dec(10, 4)).alias("temperature_2m"),
            F.col("cloud_cover").cast(dec(5, 2)).alias("cloud_cover"),
            F.col("wind_speed_10m").cast(dec(10, 4)).alias("wind_speed_10m"),
            F.col("precipitation").cast(dec(10, 6)).alias("precipitation"),
            F.col("sunshine_duration").cast(dec(10, 4)).alias("sunshine_duration"),
            F.col("energy_mwh").cast(dec(15, 6)).alias("energy_output_mwh"),
            F.col("weather_severity").alias("weather_severity"),
            "created_at",
        )

        if is_empty(result):
            return None

        return {"fact_weather_impact": result}
