from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig


def get_dim_feature_importance_schema() -> T.StructType:
    return T.StructType([
        T.StructField("feature_importance_key", T.IntegerType(), nullable=False),
        T.StructField("feature_name", T.StringType(), nullable=False),
        T.StructField("feature_category", T.StringType(), nullable=False),
        T.StructField("importance_value", T.DoubleType(), nullable=False),
        T.StructField("importance_percentage", T.DoubleType(), nullable=False),
        T.StructField("cumulative_importance", T.DoubleType(), nullable=False),
        T.StructField("rank_overall", T.IntegerType(), nullable=False),
        T.StructField("rank_in_category", T.IntegerType(), nullable=False),
        T.StructField("is_top_15", T.BooleanType(), nullable=False),
        T.StructField("is_lag_feature", T.BooleanType(), nullable=False),
        T.StructField("model_version_key", T.IntegerType(), nullable=False),
        T.StructField("created_at", T.TimestampType(), nullable=False),
    ])


class GoldDimFeatureImportanceLoader(BaseGoldLoader):
    source_tables: Dict[str, SourceTableConfig] = {}

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_feature_importance": GoldTableConfig(
            iceberg_table="lh.gold.dim_feature_importance",
            s3_base_path="s3a://lakehouse/gold/dim_feature_importance",
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        schema = get_dim_feature_importance_schema()
        empty_df = self.spark.createDataFrame([], schema=schema)
        return {"dim_feature_importance": empty_df}
