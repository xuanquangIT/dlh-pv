from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from decimal import Decimal


def get_dim_error_category_schema() -> T.StructType:
    return T.StructType([
        T.StructField("error_category_key", T.IntegerType(), nullable=False),
        T.StructField("error_category", T.StringType(), nullable=False),
        T.StructField("error_range_min", T.DecimalType(10, 2), nullable=False),
        T.StructField("error_range_max", T.DecimalType(10, 2), nullable=False),
        T.StructField("description", T.StringType(), nullable=True),
        T.StructField("color_code", T.StringType(), nullable=True),
    ])


class GoldDimErrorCategoryLoader(BaseGoldLoader):
    """Builds the Gold dim_error_category table as a static dimension."""

    source_tables: Dict[str, SourceTableConfig] = {}

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_error_category": GoldTableConfig(
            iceberg_table="lh.gold.dim_error_category",
            s3_base_path="s3a://lakehouse/gold/dim_error_category",
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        from pyspark.sql import functions as F
        
        error_data = [
            (1, "Excellent", Decimal("0.00"), Decimal("5.00"), "Forecast error less than 5% - excellent accuracy", "#4CAF50"),
            (2, "Good", Decimal("5.01"), Decimal("10.00"), "Forecast error 5-10% - good accuracy", "#8BC34A"),
            (3, "Acceptable", Decimal("10.01"), Decimal("20.00"), "Forecast error 10-20% - acceptable for operational use", "#FFC107"),
            (4, "Poor", Decimal("20.01"), Decimal("50.00"), "Forecast error 20-50% - poor accuracy, needs improvement", "#FF9800"),
            (5, "Very Poor", Decimal("50.01"), Decimal("100.00"), "Forecast error >50% - very poor, model needs retraining", "#F44336"),
            (6, "Extreme", Decimal("100.01"), Decimal("999999.99"), "Forecast error >100% - extreme outlier or data quality issue", "#9C27B0"),
        ]
        
        schema = get_dim_error_category_schema()
        error_df = self.spark.createDataFrame(error_data, schema=schema)
        error_df = error_df.withColumn("created_at", F.current_timestamp())
        
        return {"dim_error_category": error_df}


def main():
    import sys
    import os
    
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))
    
    from pv_lakehouse.etl.utils.spark_utils import create_spark_session
    
    spark = create_spark_session("PopulateDimErrorCategory")
    loader = GoldDimErrorCategoryLoader(spark)
    loader.load()
    spark.stop()
    print(" dim_error_category populated successfully")


if __name__ == "__main__":
    main()
