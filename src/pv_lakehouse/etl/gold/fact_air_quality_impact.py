"""Gold loader for fact_air_quality_impact."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import (
    build_air_quality_lookup,
    build_hourly_fact_base,
    classify_air_quality,
    dec,
    is_empty,
    require_sources,
)


class GoldFactAirQualityImpactLoader(BaseGoldLoader):
    """Produce fact rows quantifying air-quality impact on energy output."""

    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "date_hour",
                "energy_mwh",
            ],
        ),
        "daily_air_quality": SourceTableConfig(
            table_name="lh.silver.clean_daily_air_quality",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "date",
                "pm2_5",
                "pm10",
                "dust",
                "nitrogen_dioxide",
                "ozone",
                "uv_index",
            ],
        ),
        "dim_facility": SourceTableConfig(
            table_name="lh.gold.dim_facility",
            required_columns=["facility_key", "facility_code"],
        ),
        "dim_air_quality_category": SourceTableConfig(
            table_name="lh.gold.dim_air_quality_category",
            required_columns=["air_quality_category_key", "category_name"],
        ),
        "dim_time": SourceTableConfig(
            table_name="lh.gold.dim_time",
            required_columns=["time_key"],
        ),
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_air_quality_impact": GoldTableConfig(
            iceberg_table="lh.gold.fact_air_quality_impact",
            s3_base_path="s3a://lakehouse/gold/fact_air_quality_impact",
            partition_cols=("date_key",),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        hourly = sources.get("hourly_energy")
        if is_empty(hourly):
            return None

        required = require_sources(
            {
                "dim_facility": sources.get("dim_facility"),
                "dim_air_quality_category": sources.get("dim_air_quality_category"),
                "dim_time": sources.get("dim_time"),
            },
            {
                "dim_facility": "fact_air_quality_impact",
                "dim_air_quality_category": "fact_air_quality_impact",
                "dim_time": "fact_air_quality_impact",
            },
        )
        dim_facility = required["dim_facility"]
        dim_air_quality = required["dim_air_quality_category"]
        dim_time = required["dim_time"]

        air_quality_records = classify_air_quality(sources.get("daily_air_quality"))
        air_quality_lookup = build_air_quality_lookup(air_quality_records, dim_air_quality)

        base = build_hourly_fact_base(hourly)
        if is_empty(base):
            return None

        fact = base.join(dim_facility, on="facility_code", how="left")
        if not is_empty(air_quality_lookup):
            fact = fact.join(air_quality_lookup, on=["facility_code", "full_date"], how="left")

        fact = fact.join(dim_time.select("time_key"), on="time_key", how="left")

        fact = fact.withColumn("created_at", F.current_timestamp())
        fact = fact.withColumn(
            "air_quality_impact_id",
            F.row_number().over(Window.orderBy("facility_code", "date_hour")),
        )
        fact = fact.withColumn(
            "soiling_loss_estimate_pct",
            F.when(F.col("pm2_5") > 35.4, F.lit(25.0))
            .when(F.col("pm2_5") > 12.0, F.lit(10.0))
            .otherwise(F.lit(0.0)),
        )

        result = fact.select(
            "air_quality_impact_id",
            "date_key",
            "time_key",
            "facility_key",
            "air_quality_category_key",
            F.col("pm2_5").cast(dec(10, 4)).alias("pm2_5"),
            F.col("pm10").cast(dec(10, 4)).alias("pm10"),
            F.col("dust").cast(dec(10, 4)).alias("dust"),
            F.col("nitrogen_dioxide").cast(dec(10, 4)).alias("nitrogen_dioxide"),
            F.col("ozone").cast(dec(10, 4)).alias("ozone"),
            F.col("uv_index").cast(dec(5, 2)).alias("uv_index"),
            F.col("energy_mwh").cast(dec(15, 6)).alias("energy_output_mwh"),
            F.col("soiling_loss_estimate_pct").cast(dec(5, 2)).alias("soiling_loss_estimate_pct"),
            "created_at",
        )

        if is_empty(result):
            return None

        return {"fact_air_quality_impact": result}
