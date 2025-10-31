"""Gold loader for dim_date."""

from __future__ import annotations

from functools import reduce
from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import compute_date_key, season_expr


class GoldDimDateLoader(BaseGoldLoader):
    """Builds the Gold dim_date table from Silver hourly data."""

    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_weather": SourceTableConfig(
            table_name="lh.silver.clean_hourly_weather",
            timestamp_column="updated_at",
            required_columns=["date_hour"],
        ),
        "hourly_air_quality": SourceTableConfig(
            table_name="lh.silver.clean_hourly_air_quality",
            timestamp_column="updated_at",
            required_columns=["date_hour"],
        ),
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="updated_at",
            required_columns=["date_hour"],
        ),
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "dim_date": GoldTableConfig(
            iceberg_table="lh.gold.dim_date",
            s3_base_path="s3a://lakehouse/gold/dim_date",
            partition_cols=("year", "month"),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        frames = []
        for alias in ("hourly_weather", "hourly_air_quality", "hourly_energy"):
            dataframe = sources.get(alias)
            if dataframe is not None and "date_hour" in dataframe.columns and not self._is_empty(dataframe):
                frames.append(dataframe.select(F.to_date("date_hour").alias("full_date")))

        if not frames:
            return None

        combined = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)
        combined = combined.filter(F.col("full_date").isNotNull()).dropDuplicates(["full_date"])
        if self._is_empty(combined):
            return None

        spark_day = F.dayofweek("full_date")
        monday_based_day = F.pmod(spark_day + F.lit(5), F.lit(7)) + F.lit(1)

        result = combined.select(
            compute_date_key(F.col("full_date")).alias("date_key"),
            F.col("full_date"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.weekofyear("full_date").alias("week"),
            F.dayofmonth("full_date").alias("day_of_month"),
            monday_based_day.cast("int").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            (monday_based_day >= F.lit(6)).alias("is_weekend"),
            F.lit(False).alias("is_holiday"),
            season_expr(F.month("full_date")).alias("season"),
        )

        return {"dim_date": result}
