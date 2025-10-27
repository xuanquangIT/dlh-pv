"""Gold loader for dim_air_quality_category."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig
from .common import dec


class GoldDimAirQualityCategoryLoader(BaseGoldLoader):
    """Materialise air quality categories from static ranges."""

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_air_quality_category": GoldTableConfig(
            iceberg_table="lh.gold.dim_air_quality_category",
            s3_base_path="s3a://lakehouse/gold/dim_air_quality_category",
        )
    }

    _ROWS = [
        {
            "category_name": "Good",
            "pm25_min": 0.0,
            "pm25_max": 12.0,
            "impact_level": "Low",
            "health_advisory": "Air quality is considered satisfactory with minimal impact.",
        },
        {
            "category_name": "Moderate",
            "pm25_min": 12.1,
            "pm25_max": 35.4,
            "impact_level": "Medium",
            "health_advisory": "Acceptable air quality; sensitive groups should limit exposure.",
        },
        {
            "category_name": "Unhealthy",
            "pm25_min": 35.5,
            "pm25_max": 55.4,
            "impact_level": "High",
            "health_advisory": "Members of sensitive groups may experience effects; reduce activity.",
        },
        {
            "category_name": "Hazardous",
            "pm25_min": 55.5,
            "pm25_max": 500.0,
            "impact_level": "High",
            "health_advisory": "Health alerts: everyone should avoid prolonged exposure.",
        },
    ]

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        schema = T.StructType(
            [
                T.StructField("category_name", T.StringType(), False),
                T.StructField("pm25_min", T.DoubleType(), False),
                T.StructField("pm25_max", T.DoubleType(), False),
                T.StructField("impact_level", T.StringType(), False),
                T.StructField("health_advisory", T.StringType(), False),
            ]
        )
        base = self.spark.createDataFrame(self._ROWS, schema=schema)
        window_key = Window.orderBy("category_name")
        result = base.withColumn("air_quality_category_key", F.row_number().over(window_key)).select(
            "air_quality_category_key",
            "category_name",
            F.col("pm25_min").cast(dec(10, 4)).alias("pm25_min"),
            F.col("pm25_max").cast(dec(10, 4)).alias("pm25_max"),
            "impact_level",
            "health_advisory",
        )

        return {"dim_air_quality_category": result}
