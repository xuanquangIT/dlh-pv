from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig


def get_dim_model_version_schema() -> T.StructType:
    return T.StructType([
        T.StructField("model_version_key", T.IntegerType(), nullable=False),
        T.StructField("model_name", T.StringType(), nullable=False),
        T.StructField("model_type", T.StringType(), nullable=False),
        T.StructField("version_number", T.StringType(), nullable=False),
        T.StructField("algorithm", T.StringType(), nullable=False),
        T.StructField("description", T.StringType(), nullable=True),
        T.StructField("hyperparameters", T.StringType(), nullable=True),
    ])


class GoldDimModelVersionLoader(BaseGoldLoader):
    source_tables: Dict[str, SourceTableConfig] = {}

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_model_version": GoldTableConfig(
            iceberg_table="lh.gold.dim_model_version",
            s3_base_path="s3a://lakehouse/gold/dim_model_version",
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        from pyspark.sql import functions as F
        
        model_data = [
            (1, "Solar Energy Regression", "Regression", "1.0", "GBTRegressor", "Gradient Boosted Trees regression model for solar energy forecasting", "maxIter=120, maxDepth=6, stepSize=0.1, subsamplingRate=0.8"),
            (2, "Solar Energy Regression V2", "Regression", "2.0", "GBTRegressor", "Enhanced GBT model with LAG features and interaction terms", "maxIter=120, maxDepth=6, stepSize=0.1, subsamplingRate=0.8, minInfoGain=0.01"),
            (3, "Solar Energy Regression V3", "Regression", "3.0", "GBTRegressor", "Production model with optimized hyperparameters and feature engineering", "maxIter=150, maxDepth=7, stepSize=0.08, subsamplingRate=0.85, minInfoGain=0.005"),
        ]
        
        schema = get_dim_model_version_schema()
        model_df = self.spark.createDataFrame(model_data, schema=schema)
        model_df = model_df.withColumn("created_at", F.current_timestamp())
        return {"dim_model_version": model_df}


def insert_model_version(
    spark: SparkSession,
    model_name: str,
    model_type: str,
    version_number: str,
    algorithm: str,
    description: str = None,
    hyperparameters: str = None,
) -> int:
    from pv_lakehouse.etl.utils.spark_utils import write_iceberg_table
    
    try:
        existing = spark.table("lh.gold.dim_model_version")
        max_key = existing.agg(F.max("model_version_key")).collect()[0][0]
        new_key = int(max_key) + 1 if max_key else 1
    except Exception:
        new_key = 1
    
    schema = get_dim_model_version_schema()
    new_row = [(
        new_key,
        model_name,
        model_type,
        version_number,
        algorithm,
        description,
        hyperparameters,
        F.current_timestamp(),
        F.current_timestamp(),
    )]
    
    new_df = spark.createDataFrame(new_row, schema)
    write_iceberg_table(new_df, "lh.gold.dim_model_version", mode="append")
    print(f" Inserted model version: {model_name} v{version_number} (key={new_key})")
    return new_key


def main():
    import sys
    import os
    
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))
    
    from pv_lakehouse.etl.utils.spark_utils import create_spark_session
    
    spark = create_spark_session("PopulateDimModelVersion")
    loader = GoldDimModelVersionLoader(spark)
    loader.load()
    spark.stop()
    print(" dim_model_version populated successfully")


if __name__ == "__main__":
    main()
