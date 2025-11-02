"""Gold loader for dim_time."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import time_of_day_expr


class GoldDimTimeLoader(BaseGoldLoader):
    """Builds the Gold dim_time table as a static dimension (all 24 hours)."""

    source_tables: Dict[str, SourceTableConfig] = {}

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_time": GoldTableConfig(
            iceberg_table="lh.gold.dim_time",
            s3_base_path="s3a://lakehouse/gold/dim_time",
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        # Generate all 24 hours (0-23) with minute=0
        hours_data = [{"hour": h, "minute": 0} for h in range(24)]
        
        schema = T.StructType([
            T.StructField("hour", T.IntegerType(), False),
            T.StructField("minute", T.IntegerType(), False),
        ])
        
        base = self.spark.createDataFrame(hours_data, schema=schema)

        result = base.select(
            (F.col("hour") * F.lit(100) + F.col("minute")).cast("int").alias("time_key"),
            F.col("hour"),
            F.col("minute"),
            time_of_day_expr(F.col("hour")).alias("time_of_day"),
            ((F.col("hour") >= F.lit(10)) & (F.col("hour") < F.lit(16))).alias("is_peak_hour"),
            F.when((F.col("hour") >= F.lit(6)) & (F.col("hour") < F.lit(18)), F.lit("Daylight"))
            .otherwise(F.lit("Night"))
            .alias("daylight_period"),
        )

        return {"dim_time": result}
