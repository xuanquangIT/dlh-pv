"""Gold loader for dim_weather_condition."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import classify_weather, is_empty


class GoldDimWeatherConditionLoader(BaseGoldLoader):
    """Builds the weather condition dimension from Silver hourly weather."""

    source_tables: Dict[str, SourceTableConfig] = {
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
            ],
        )
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_weather_condition": GoldTableConfig(
            iceberg_table="lh.gold.dim_weather_condition",
            s3_base_path="s3a://lakehouse/gold/dim_weather_condition",
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        weather = sources.get("hourly_weather")
        weather_records = classify_weather(weather)
        if is_empty(weather_records):
            return None

        base = weather_records.select(
            "condition_name",
            "radiation_level",
            "cloud_category",
            "temperature_range",
            "weather_severity",
        ).dropDuplicates()

        if is_empty(base):
            return None

        window_key = Window.orderBy("condition_name")
        result = base.withColumn("weather_condition_key", F.row_number().over(window_key)).select(
            "weather_condition_key",
            "condition_name",
            "radiation_level",
            "cloud_category",
            "temperature_range",
            "weather_severity",
        )

        return {"dim_weather_condition": result}
