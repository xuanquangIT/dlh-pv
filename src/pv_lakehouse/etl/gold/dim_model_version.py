"""Gold loader for dim_model_version."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig
from .common import dec


class GoldDimModelVersionLoader(BaseGoldLoader):
    """Materialise model version dimension rows from static metadata."""

    source_tables: Dict[str, None] = {}

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_model_version": GoldTableConfig(
            iceberg_table="lh.gold.dim_model_version",
            s3_base_path="s3a://lakehouse/gold/dim_model_version",
        )
    }

    _ROWS = [
        {
            "model_name": "solar_forecast_baseline",
            "version_number": "1.0.0",
            "algorithm_type": "XGBoost",
            "training_date": "2024-01-01",
            "features_used": "facility_code,weather,air_quality",
            "accuracy_baseline": 0.8500,
            "is_active": True,
        }
    ]

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        schema = T.StructType(
            [
                T.StructField("model_name", T.StringType(), False),
                T.StructField("version_number", T.StringType(), False),
                T.StructField("algorithm_type", T.StringType(), True),
                T.StructField("training_date", T.StringType(), True),
                T.StructField("features_used", T.StringType(), True),
                T.StructField("accuracy_baseline", T.DoubleType(), True),
                T.StructField("is_active", T.BooleanType(), False),
            ]
        )
        base = self.spark.createDataFrame(self._ROWS, schema=schema)
        window_key = Window.orderBy("model_name", "version_number")
        result = (
            base.withColumn("model_version_key", F.row_number().over(window_key))
            .withColumn("training_date", F.to_date("training_date"))
            .withColumn("created_at", F.current_timestamp())
            .select(
                "model_version_key",
                "model_name",
                "version_number",
                "algorithm_type",
                "training_date",
                "features_used",
                F.col("accuracy_baseline").cast(dec(5, 4)).alias("accuracy_baseline"),
                "is_active",
                "created_at",
            )
        )

        return {"dim_model_version": result}
