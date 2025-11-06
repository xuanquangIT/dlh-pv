"""Gold loader for dim_aqi_category."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import dec


class GoldDimAQICategoryLoader(BaseGoldLoader):
    """Builds the Gold dim_aqi_category table as a static dimension (EPA/WHO AQI standards)."""

    source_tables: Dict[str, SourceTableConfig] = {}

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_aqi_category": GoldTableConfig(
            iceberg_table="lh.gold.dim_aqi_category",
            s3_base_path="s3a://lakehouse/gold/dim_aqi_category",
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        """Generate AQI category dimension based on EPA/WHO standards.
        
        Reference: https://www.epa.gov/outdoor-air-quality-data/air-quality-index-daily-values-report
        """
        from decimal import Decimal
        
        # Define AQI categories following EPA/WHO standards
        aqi_data = [
            {
                "aqi_category_key": 1,
                "aqi_category": "Good",
                "aqi_range_min": 0,
                "aqi_range_max": 50,
                "health_advisory": "No risk",
                "color_code": "#00E400",
                "pm2_5_min_ug_m3": Decimal("0.000"),
                "pm2_5_max_ug_m3": Decimal("12.000"),
            },
            {
                "aqi_category_key": 2,
                "aqi_category": "Moderate",
                "aqi_range_min": 51,
                "aqi_range_max": 100,
                "health_advisory": "Acceptable risk",
                "color_code": "#FFFF00",
                "pm2_5_min_ug_m3": Decimal("12.100"),
                "pm2_5_max_ug_m3": Decimal("35.000"),
            },
            {
                "aqi_category_key": 3,
                "aqi_category": "Unhealthy for Sensitive",
                "aqi_range_min": 101,
                "aqi_range_max": 150,
                "health_advisory": "Sensitive groups affected",
                "color_code": "#FF7E00",
                "pm2_5_min_ug_m3": Decimal("35.100"),
                "pm2_5_max_ug_m3": Decimal("55.000"),
            },
            {
                "aqi_category_key": 4,
                "aqi_category": "Unhealthy",
                "aqi_range_min": 151,
                "aqi_range_max": 200,
                "health_advisory": "General public affected",
                "color_code": "#FF0000",
                "pm2_5_min_ug_m3": Decimal("55.100"),
                "pm2_5_max_ug_m3": Decimal("150.000"),
            },
            {
                "aqi_category_key": 5,
                "aqi_category": "Very Unhealthy",
                "aqi_range_min": 201,
                "aqi_range_max": 300,
                "health_advisory": "Emergency conditions",
                "color_code": "#8F3F97",
                "pm2_5_min_ug_m3": Decimal("150.100"),
                "pm2_5_max_ug_m3": Decimal("250.000"),
            },
            {
                "aqi_category_key": 6,
                "aqi_category": "Hazardous",
                "aqi_range_min": 301,
                "aqi_range_max": 500,
                "health_advisory": "Health emergency",
                "color_code": "#7E0023",
                "pm2_5_min_ug_m3": Decimal("250.100"),
                "pm2_5_max_ug_m3": Decimal("500.000"),
            },
        ]

        schema = T.StructType([
            T.StructField("aqi_category_key", T.LongType(), False),
            T.StructField("aqi_category", T.StringType(), False),
            T.StructField("aqi_range_min", T.IntegerType(), False),
            T.StructField("aqi_range_max", T.IntegerType(), False),
            T.StructField("health_advisory", T.StringType(), False),
            T.StructField("color_code", T.StringType(), False),
            T.StructField("pm2_5_min_ug_m3", T.DecimalType(8, 3), False),
            T.StructField("pm2_5_max_ug_m3", T.DecimalType(8, 3), False),
        ])

        result = self.spark.createDataFrame(aqi_data, schema=schema)

        # Add audit timestamp
        from pyspark.sql import functions as F
        result = result.withColumn("created_at", F.current_timestamp())

        return {"dim_aqi_category": result}


__all__ = ["GoldDimAQICategoryLoader"]
