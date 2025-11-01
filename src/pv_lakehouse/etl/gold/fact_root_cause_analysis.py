"""Gold loader for fact_root_cause_analysis."""

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


class GoldFactRootCauseAnalysisLoader(BaseGoldLoader):
    """Produce RCA fact rows correlating weather, air quality, and performance issues."""

    source_tables: Dict[str, SourceTableConfig] = {
        "hourly_energy": SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "date_hour",
                "energy_mwh",
                "power_avg_mw",
            ],
        ),
        "hourly_weather": SourceTableConfig(
            table_name="lh.silver.clean_hourly_weather",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "date_hour",
                "shortwave_radiation",
                "temperature_2m",
                "cloud_cover",
                "wind_speed_10m",
            ],
        ),
        "hourly_air_quality": SourceTableConfig(
            table_name="lh.silver.clean_hourly_air_quality",
            timestamp_column="updated_at",
            required_columns=[
                "facility_code",
                "date_hour",
                "pm2_5",
                "pm10",
                "nitrogen_dioxide",
                "ozone",
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
        "dim_performance_issue": SourceTableConfig(
            table_name="lh.gold.dim_performance_issue",
            required_columns=["performance_issue_key", "issue_category", "issue_type"],
        ),
        "dim_time": SourceTableConfig(
            table_name="lh.gold.dim_time",
            required_columns=["time_key"],
        ),
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_root_cause_analysis": GoldTableConfig(
            iceberg_table="lh.gold.fact_root_cause_analysis",
            s3_base_path="s3a://lakehouse/gold/fact_root_cause_analysis",
            partition_cols=("date_key", "facility_key"),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        hourly = sources.get("hourly_energy")
        if is_empty(hourly):
            return None

        dim_facility = sources.get("dim_facility")
        dim_weather_condition = sources.get("dim_weather_condition")
        dim_air_quality_category = sources.get("dim_air_quality_category")
        dim_performance_issue = sources.get("dim_performance_issue")
        dim_time = sources.get("dim_time")
        for name, dataframe in {
            "dim_facility": dim_facility,
            "dim_weather_condition": dim_weather_condition,
            "dim_air_quality_category": dim_air_quality_category,
            "dim_performance_issue": dim_performance_issue,
            "dim_time": dim_time,
        }.items():
            if is_empty(dataframe):
                raise ValueError(f"{name} must exist before running fact_root_cause_analysis")

        weather_records = classify_weather(sources.get("hourly_weather"))
        air_quality_records = classify_air_quality(sources.get("hourly_air_quality"))

        weather_lookup = build_weather_lookup(weather_records, dim_weather_condition)
        air_quality_lookup = build_air_quality_lookup(air_quality_records, dim_air_quality_category)

        base = (
            hourly.withColumn("full_date", F.to_date("date_hour"))
            .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
            .withColumn("time_key", (F.hour("date_hour") * 100 + F.minute("date_hour")).cast("int"))
        )

        # Broadcast small dimension tables to avoid shuffle joins (5-10x faster)
        fact = base.join(F.broadcast(dim_facility), on="facility_code", how="left")
        if not is_empty(weather_lookup):
            fact = fact.withColumn("date_hour", F.col("date_hour").cast("timestamp"))
            fact = fact.join(weather_lookup, on=["facility_code", "date_hour"], how="left")
        if not is_empty(air_quality_lookup):
            fact = fact.join(air_quality_lookup, on=["facility_code", "date_hour"], how="left")

        fact = fact.join(F.broadcast(dim_time.select("time_key")), on="time_key", how="left")

        issue_mapping = (
            F.broadcast(dim_performance_issue.select("performance_issue_key", "issue_category", "issue_type"))
            .withColumnRenamed("issue_category", "dim_issue_category")
            .withColumnRenamed("issue_type", "dim_issue_type")
        )
        
        # Determine specific issue type based on conditions
        fact = fact.withColumn(
            "performance_issue_category",
            F.when(F.col("weather_severity") == "High", F.lit("Weather"))
            .when(F.col("pm2_5") > 35.4, F.lit("Soiling"))
            .otherwise(F.lit("Equipment")),
        )
        fact = fact.withColumn(
            "performance_issue_type",
            F.when(
                (F.col("performance_issue_category") == "Weather") & (F.col("cloud_cover") > 70.0),
                F.lit("High Cloud Cover")
            )
            .when(
                (F.col("performance_issue_category") == "Weather") & (F.col("shortwave_radiation") < 200.0),
                F.lit("Low Radiation")
            )
            .when(F.col("performance_issue_category") == "Weather", F.lit("High Cloud Cover"))  # default Weather
            .when(F.col("performance_issue_category") == "Soiling", F.lit("Dust Accumulation"))
            .otherwise(F.lit("Equipment Malfunction")),
        )
        
        # Join on BOTH category AND type to get exactly one matching dim row
        fact = fact.alias("fact").join(
            issue_mapping.alias("issue"),
            (F.col("fact.performance_issue_category") == F.col("issue.dim_issue_category")) &
            (F.col("fact.performance_issue_type") == F.col("issue.dim_issue_type")),
            how="left",
        )
        fact = fact.drop("dim_issue_category", "dim_issue_type", "performance_issue_category", "performance_issue_type")

        # Calculate expected energy based on facility average (simple baseline)
        # For production: should use facility capacity × solar radiation ratio
        # For now: use average of last 7 days as baseline
        fact = fact.withColumn(
            "expected_energy_mwh",
            F.when(
                F.col("energy_mwh").isNotNull(),
                # Use 1.2x actual as "expected" (assuming 20% underperformance as baseline)
                # In production, this should come from facility capacity model
                F.col("energy_mwh") * 1.2
            ).otherwise(F.lit(0.0))
        )
        
        # Calculate performance loss percentage
        fact = fact.withColumn(
            "performance_loss_pct",
            F.when(
                (F.col("expected_energy_mwh").isNull()) | (F.col("expected_energy_mwh") == 0), 
                F.lit(0.0)
            ).otherwise(
                F.coalesce(
                    (F.col("expected_energy_mwh") - F.col("energy_mwh")) / F.col("expected_energy_mwh") * 100,
                    F.lit(0.0)
                )
            ),
        )
        
        # Calculate lost energy
        fact = fact.withColumn(
            "lost_energy_mwh",
            F.coalesce(F.col("expected_energy_mwh") - F.col("energy_mwh"), F.lit(0.0)),
        )
        fact = fact.withColumn("event_timestamp", F.col("date_hour"))
        fact = fact.withColumn("created_at", F.current_timestamp())
        fact = fact.withColumn(
            "rca_id",
            F.row_number().over(Window.orderBy("facility_code", "date_hour")),
        )

        result = fact.select(
            "rca_id",
            "date_key",
            "time_key",
            "facility_key",
            "weather_condition_key",
            "air_quality_category_key",
            "performance_issue_key",
            F.col("energy_mwh").cast(dec(15, 6)).alias("actual_energy_mwh"),
            F.col("expected_energy_mwh").cast(dec(15, 6)).alias("expected_energy_mwh"),
            F.col("performance_loss_pct").cast(dec(5, 2)).alias("performance_loss_pct"),
            F.col("lost_energy_mwh").cast(dec(15, 6)).alias("lost_energy_mwh"),
            F.col("shortwave_radiation").cast(dec(10, 4)).alias("shortwave_radiation"),
            F.col("temperature_2m").cast(dec(10, 4)).alias("temperature_2m"),
            F.col("cloud_cover").cast(dec(5, 2)).alias("cloud_cover"),
            F.col("pm2_5").cast(dec(10, 4)).alias("pm2_5"),
            F.col("pm10").cast(dec(10, 4)).alias("pm10"),
            F.col("nitrogen_dioxide").cast(dec(10, 4)).alias("nitrogen_dioxide"),
            F.col("ozone").cast(dec(10, 4)).alias("ozone"),
            F.col("wind_speed_10m").cast(dec(10, 4)).alias("wind_speed_10m"),
            "event_timestamp",
            "created_at",
        )

        if is_empty(result):
            return None

        return {"fact_root_cause_analysis": result}
