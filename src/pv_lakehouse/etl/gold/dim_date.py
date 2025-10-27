"""Gold loader for dim_date."""

from __future__ import annotations

from functools import reduce
from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import compute_date_key, season_expr


class GoldDimDateLoader(BaseGoldLoader):
    """Builds the Gold dim_date table from Silver weather and energy data."""

    source_tables: Dict[str, SourceTableConfig] = {
        "daily_weather": SourceTableConfig(
            table_name="lh.silver.clean_daily_weather",
            timestamp_column="updated_at",
            required_columns=["date"],
        ),
        "daily_air_quality": SourceTableConfig(
            table_name="lh.silver.clean_daily_air_quality",
            timestamp_column="updated_at",
            required_columns=["date"],
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
        for alias in ("daily_weather", "daily_air_quality"):
            dataframe = sources.get(alias)
            if dataframe is not None and "date" in dataframe.columns and not self._is_empty(dataframe):
                frames.append(dataframe.select(F.col("date").alias("full_date")))

        hourly = sources.get("hourly_energy")
        if hourly is not None and "date_hour" in hourly.columns and not self._is_empty(hourly):
            frames.append(hourly.select(F.to_date("date_hour").alias("full_date")))

        if not frames:
            return None

        combined = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)
        combined = combined.filter(F.col("full_date").isNotNull()).dropDuplicates(["full_date"])
        if self._is_empty(combined):
            return None

        result = combined.select(
            compute_date_key(F.col("full_date")).alias("date_key"),
            F.col("full_date"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.weekofyear("full_date").alias("week"),
            F.dayofmonth("full_date").alias("day_of_month"),
            F.date_format("full_date", "u").cast("int").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            (F.date_format("full_date", "u").cast("int") >= F.lit(6)).alias("is_weekend"),
            F.lit(False).alias("is_holiday"),
            season_expr(F.month("full_date")).alias("season"),
        )

        return {"dim_date": result}
