"""Gold loader for dim_time."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import is_empty, time_of_day_expr


class GoldDimTimeLoader(BaseGoldLoader):
    """Builds the Gold dim_time table from Silver hourly energy."""

    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="updated_at",
            required_columns=["date_hour"],
        )
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_time": GoldTableConfig(
            iceberg_table="lh.gold.dim_time",
            s3_base_path="s3a://lakehouse/gold/dim_time",
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        hourly = sources.get("hourly_energy")
        if is_empty(hourly) or "date_hour" not in hourly.columns:
            return None

        base = (
            hourly.select(
                F.hour("date_hour").alias("hour"),
                F.minute("date_hour").alias("minute"),
            )
            .dropna()
            .dropDuplicates(["hour", "minute"])
        )
        if is_empty(base):
            return None

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
