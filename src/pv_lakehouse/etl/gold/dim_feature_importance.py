"""Dimension table for ML model feature importance tracking.

This table stores feature importance metrics from trained ML models,
allowing analysis of which features contribute most to predictions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, BooleanType, TimestampType
)
from pv_lakehouse.etl.utils.spark_utils import create_spark_session, write_iceberg_table


# Table configuration
TABLE_NAME = "lh.gold.dim_feature_importance"
WRITE_MODE = "append"  # Append new feature importance records


def get_dim_feature_importance_schema() -> StructType:
    """Define schema for dim_feature_importance table.
    
    Returns:
        StructType: Schema definition matching model output
    """
    return StructType([
        # Surrogate key
        StructField("feature_importance_key", IntegerType(), nullable=False),
        
        # Feature identification
        StructField("feature_name", StringType(), nullable=False),
        StructField("feature_category", StringType(), nullable=False),
        
        # Importance metrics
        StructField("importance_value", DoubleType(), nullable=False),
        StructField("importance_percentage", DoubleType(), nullable=False),
        StructField("cumulative_importance", DoubleType(), nullable=False),
        
        # Rankings
        StructField("rank_overall", IntegerType(), nullable=False),
        StructField("rank_in_category", IntegerType(), nullable=False),
        
        # Flags
        StructField("is_top_15", BooleanType(), nullable=False),
        StructField("is_lag_feature", BooleanType(), nullable=False),
        
        # Model context (foreign key to dim_model_version)
        StructField("model_version_key", IntegerType(), nullable=False),
        
        # Audit columns
        StructField("created_at", TimestampType(), nullable=False),
    ])


def create_dim_feature_importance_table(spark: SparkSession) -> None:
    """Create dim_feature_importance table if not exists.
    
    Args:
        spark: Active SparkSession
    """
    schema = get_dim_feature_importance_schema()
    
    # Create empty DataFrame with schema
    empty_df = spark.createDataFrame([], schema)
    
    # Create table using Iceberg
    print(f"[INFO] Creating table: {TABLE_NAME}")
    write_iceberg_table(
        empty_df,
        TABLE_NAME,
        mode="append",
        partition_cols=["model_version_key"],  # Partition by model version
    )
    print(f"[SUCCESS] Table {TABLE_NAME} created successfully")


def load_dim_feature_importance():
    """Main function to create dim_feature_importance table.
    
    This is typically run once during initial setup, not during regular ETL.
    Feature importance data is populated by the training script.
    """
    print("=" * 60)
    print("Creating Gold Dimension: dim_feature_importance")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session("create_dim_feature_importance")
    
    try:
        # Check if table already exists
        try:
            existing = spark.sql(f"SELECT COUNT(*) as cnt FROM {TABLE_NAME}")
            count = existing.collect()[0]['cnt']
            print(f"[INFO] Table {TABLE_NAME} already exists with {count} records")
            print("[INFO] Skipping table creation")
            return
        except Exception:
            # Table doesn't exist, create it
            create_dim_feature_importance_table(spark)
            
    except Exception as e:
        print(f"[ERROR] Failed to create table: {str(e)}")
        raise
    finally:
        spark.stop()
    
    print("=" * 60)
    print("✓ dim_feature_importance table ready")
    print("=" * 60)


if __name__ == "__main__":
    load_dim_feature_importance()
