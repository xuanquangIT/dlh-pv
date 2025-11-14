"""Gold loader for dim_forecast_model_version."""

from __future__ import annotations

from typing import Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig
from .common import is_empty


class GoldDimForecastModelVersionLoader(BaseGoldLoader):
    """Materialise forecast model version dimension records.
    
    This is a static/slowly-changing dimension that catalogs trained models.
    Can be populated manually or from MLflow experiment metadata.
    """

    source_tables: Dict[str, None] = {}

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_forecast_model_version": GoldTableConfig(
            iceberg_table="lh.gold.dim_forecast_model_version",
            s3_base_path="s3a://lakehouse/gold/dim_forecast_model_version",
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        """Create static model version dimension.
        
        This dimension should be updated whenever a new model is trained.
        For now, we create a baseline record. In production, this would be
        populated from MLflow tracking metadata or a model registry.
        """
        
        # Define baseline model version records
        # In production, query MLflow API or read from model registry
        data = [
            {
                "model_version_id": "lr_baseline_v1",
                "model_name": "logistic_regression",
                "model_type": "classification",
                "model_algorithm": "LogisticRegression",
                "train_start_date": "2024-10-01",
                "train_end_date": "2024-12-10",
                "hyperparameters": '{"maxIter":100,"regParam":0.0,"elasticNetParam":0.0}',
                "feature_columns": "intervals_count,completeness_pct,hour_of_day",
                "target_column": "energy_high_flag",
                "performance_metrics": '{"auc":0.94,"accuracy":0.885}',
                "is_active": True,
                "model_description": "Baseline logistic regression for solar energy forecast",
            }
        ]
        
        schema = T.StructType([
            T.StructField("model_version_id", T.StringType(), False),
            T.StructField("model_name", T.StringType(), False),
            T.StructField("model_type", T.StringType(), True),
            T.StructField("model_algorithm", T.StringType(), True),
            T.StructField("train_start_date", T.StringType(), True),
            T.StructField("train_end_date", T.StringType(), True),
            T.StructField("hyperparameters", T.StringType(), True),
            T.StructField("feature_columns", T.StringType(), True),
            T.StructField("target_column", T.StringType(), True),
            T.StructField("performance_metrics", T.StringType(), True),
            T.StructField("is_active", T.BooleanType(), True),
            T.StructField("model_description", T.StringType(), True),
        ])
        
        df = self.spark.createDataFrame(data, schema=schema)
        
        # Add audit timestamps
        df = df.withColumn("created_at", F.current_timestamp())
        df = df.withColumn("updated_at", F.current_timestamp())
        
        if is_empty(df):
            return None
            
        return {"dim_forecast_model_version": df}


__all__ = ["GoldDimForecastModelVersionLoader"]
