"""Gold loader for fact_kpi_performance."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import (
    build_air_quality_lookup,
    build_weather_lookup,
    classify_air_quality,
    classify_weather,
    dec,
    is_empty,
)


class GoldFactKpiPerformanceLoader(BaseGoldLoader):
    """Produce KPI fact rows summarising daily facility performance."""

    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "facility_name",
                "network_region",
                "date_hour",
                "energy_mwh",
                "power_avg_mw",
                "completeness_pct",
            ],
        ),
        "daily_weather": SourceTableConfig(
            table_name="lh.silver.clean_daily_weather",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "date",
                "shortwave_radiation",
                "direct_radiation",
                "diffuse_radiation",
                "temperature_2m",
                "cloud_cover",
                "wind_speed_10m",
                "precipitation",
                "sunshine_duration",
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
        "dim_weather_condition": SourceTableConfig(
            table_name="lh.gold.dim_weather_condition",
            required_columns=["weather_condition_key", "condition_name"],
        ),
        "dim_air_quality_category": SourceTableConfig(
            table_name="lh.gold.dim_air_quality_category",
            required_columns=["air_quality_category_key", "category_name"],
        ),
        "dim_equipment_status": SourceTableConfig(
            table_name="lh.gold.dim_equipment_status",
            required_columns=["equipment_status_key", "status_name"],
        ),
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_kpi_performance": GoldTableConfig(
            iceberg_table="lh.gold.fact_kpi_performance",
            s3_base_path="s3a://lakehouse/gold/fact_kpi_performance",
            partition_cols=("date_key",),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        hourly = sources.get("hourly_energy")
        if is_empty(hourly):
            return None

        dim_facility = sources.get("dim_facility")
        dim_weather_condition = sources.get("dim_weather_condition")
        dim_air_quality_category = sources.get("dim_air_quality_category")
        dim_equipment_status = sources.get("dim_equipment_status")

        for name, dataframe in {
            "dim_facility": dim_facility,
            "dim_weather_condition": dim_weather_condition,
            "dim_air_quality_category": dim_air_quality_category,
            "dim_equipment_status": dim_equipment_status,
        }.items():
            if is_empty(dataframe):
                raise ValueError(f"{name} must be populated before running fact_kpi_performance")

        weather_records = classify_weather(sources.get("daily_weather"))
        air_quality_records = classify_air_quality(sources.get("daily_air_quality"))

        weather_lookup = build_weather_lookup(weather_records, dim_weather_condition)
        air_quality_lookup = build_air_quality_lookup(air_quality_records, dim_air_quality_category)

        base = hourly.withColumn("full_date", F.to_date("date_hour"))
        daily = (
            base.groupBy("facility_code", "full_date")
            .agg(
                F.sum("energy_mwh").alias("actual_energy_mwh"),
                F.avg("energy_mwh").alias("expected_energy_mwh"),
                F.avg("completeness_pct").alias("completeness_pct"),
            )
            .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        )

        fact = daily.join(dim_facility, on="facility_code", how="left")

        if not is_empty(weather_lookup):
            fact = fact.join(weather_lookup, on=["facility_code", "full_date"], how="left")
        if not is_empty(air_quality_lookup):
            fact = fact.join(air_quality_lookup, on=["facility_code", "full_date"], how="left")

        status_mapping = (
            dim_equipment_status.select("equipment_status_key", "status_name")
            .withColumnRenamed("status_name", "dim_status_name")
        )
        fact = fact.withColumn(
            "status_name",
            F.when(F.col("completeness_pct") >= 95, F.lit("Available"))
            .when(F.col("completeness_pct") >= 70, F.lit("Degraded"))
            .otherwise(F.lit("Unavailable")),
        )
        fact = fact.join(status_mapping, F.col("status_name") == F.col("dim_status_name"), how="left")
        fact = fact.drop("dim_status_name")

        fact = fact.withColumn("energy_loss_mwh", F.col("expected_energy_mwh") - F.col("actual_energy_mwh"))
        fact = fact.withColumn(
            "energy_loss_pct",
            F.when(F.col("expected_energy_mwh") == 0, F.lit(0.0)).otherwise(
                (F.col("energy_loss_mwh") / F.col("expected_energy_mwh")) * 100
            ),
        )
        fact = fact.withColumn(
            "performance_ratio_pct",
            F.when(F.col("expected_energy_mwh") == 0, F.lit(0.0)).otherwise(
                (F.col("actual_energy_mwh") / F.col("expected_energy_mwh")) * 100
            ),
        )
        fact = fact.withColumn("capacity_utilization_factor_pct", F.col("performance_ratio_pct"))
        fact = fact.withColumn("specific_yield_kwh_per_kwp", F.col("actual_energy_mwh") * 1000)
        fact = fact.withColumn("system_availability_pct", F.col("completeness_pct"))
        fact = fact.withColumn("grid_availability_pct", F.col("completeness_pct"))
        fact = fact.withColumn("downtime_hours", (100 - F.col("completeness_pct")) / 100 * 24)
        fact = fact.withColumn("measurement_date", F.col("full_date"))
        fact = fact.withColumn("created_at", F.current_timestamp())
        fact = fact.withColumn(
            "performance_id",
            F.row_number().over(Window.orderBy("facility_code", "full_date")),
        )

        result = fact.select(
            "performance_id",
            "date_key",
            "facility_key",
            "weather_condition_key",
            "air_quality_category_key",
            F.col("equipment_status_key").alias("equipment_status_key"),
            F.col("actual_energy_mwh").cast(dec(15, 6)).alias("actual_energy_mwh"),
            F.col("expected_energy_mwh").cast(dec(15, 6)).alias("expected_energy_mwh"),
            F.col("capacity_utilization_factor_pct").cast(dec(5, 2)).alias("capacity_utilization_factor_pct"),
            F.col("performance_ratio_pct").cast(dec(5, 2)).alias("performance_ratio_pct"),
            F.col("specific_yield_kwh_per_kwp").cast(dec(10, 4)).alias("specific_yield_kwh_per_kwp"),
            F.col("system_availability_pct").cast(dec(5, 2)).alias("system_availability_pct"),
            F.col("grid_availability_pct").cast(dec(5, 2)).alias("grid_availability_pct"),
            F.col("downtime_hours").cast(dec(5, 2)).alias("downtime_hours"),
            F.col("energy_loss_mwh").cast(dec(15, 6)).alias("energy_loss_mwh"),
            F.col("energy_loss_pct").cast(dec(5, 2)).alias("energy_loss_pct"),
            F.col("measurement_date").alias("measurement_date"),
            "created_at",
        )

        if is_empty(result):
            return None

        return {"fact_kpi_performance": result}
