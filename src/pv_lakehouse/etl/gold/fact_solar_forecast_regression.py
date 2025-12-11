from __future__ import annotations

from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import broadcast_small_dim, compute_date_key, dec, is_empty, require_sources


class GoldFactSolarForecastRegressionLoader(BaseGoldLoader):

    source_tables: Dict[str, SourceTableConfig] = {
        "fact_solar_forecast": SourceTableConfig(
            table_name="lh.gold.fact_solar_forecast_regression",
            required_columns=[
                "facility_key", "date_key", "time_key", "model_version_key", "error_category_key",
                "actual_energy_mwh", "predicted_energy_mwh", "forecast_error_mwh",
                "absolute_percentage_error", "mae_metric", "rmse_metric", "r2_score",
                "forecast_timestamp", "created_at"
            ],
        ),
        "dim_facility": SourceTableConfig(
            table_name="lh.gold.dim_facility",
            required_columns=["facility_key", "facility_code", "facility_name"],
        ),
        "dim_date": SourceTableConfig(
            table_name="lh.gold.dim_date",
            required_columns=["date_key", "full_date", "year", "month", "day_of_month"],
        ),
        "dim_time": SourceTableConfig(
            table_name="lh.gold.dim_time",
            required_columns=["time_key", "hour", "minute"],
        ),
        "dim_error_category": SourceTableConfig(
            table_name="lh.gold.dim_error_category",
            required_columns=["error_category_key", "error_category", "error_range_min", "error_range_max"],
        ),
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_solar_forecast_regression": GoldTableConfig(
            iceberg_table="lh.gold.fact_solar_forecast_regression",
            s3_base_path="s3a://lakehouse/gold/fact_solar_forecast_regression",
            partition_cols=("date_key",),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        required = require_sources(
            {
                "fact_solar_forecast": sources.get("fact_solar_forecast"),
                "dim_facility": sources.get("dim_facility"),
                "dim_date": sources.get("dim_date"),
                "dim_time": sources.get("dim_time"),
            },
            {
                "fact_solar_forecast": "fact_solar_forecast_regression",
                "dim_facility": "fact_solar_forecast_regression",
                "dim_date": "fact_solar_forecast_regression",
                "dim_time": "fact_solar_forecast_regression",
            },
        )
        
        dim_facility = broadcast_small_dim(required["dim_facility"])
        dim_date = broadcast_small_dim(required["dim_date"])
        dim_time = broadcast_small_dim(required["dim_time"])
        
        fact_source = required["fact_solar_forecast"]
        preds = fact_source
        
        pred_count = preds.count()
        print(f"[GOLD] Processing {pred_count} regression predictions")
        
        if pred_count == 0:
            print("[GOLD] No regression predictions found")
            return None
        
        fact = preds.alias("f")
        
        fact = fact.join(
            dim_facility.select(
                F.col("facility_key"),
                F.col("facility_code"),
                F.col("facility_name"),
            ).alias("fac"),
            on="facility_key",
            how="left"
        )
        
        fact = fact.join(
            dim_date.select(
                F.col("date_key"),
                F.col("year"),
                F.col("month"),
                F.col("day_of_month").alias("day"),
                F.col("full_date"),
            ).alias("d"),
            on="date_key",
            how="left"
        )
        
        fact = fact.join(
            dim_time.select(
                F.col("time_key"),
                F.col("hour"),
                F.col("minute"),
            ).alias("t"),
            on="time_key",
            how="left"
        )
        
        fact = (
            fact
            .withColumn("mape_pct", F.col("absolute_percentage_error"))
            .withColumn("prediction_accuracy_pct", 
                       F.lit(100.0) - F.col("absolute_percentage_error"))
            .withColumn("is_accurate_forecast",
                       F.when(F.col("absolute_percentage_error") <= 10.0, True)
                       .otherwise(False))
            .withColumn("residual_mwh", F.col("forecast_error_mwh"))
            .withColumn("updated_at", F.current_timestamp())
        )
        
        result = fact.select(
            F.monotonically_increasing_id().alias("forecast_id"),
            "facility_key",
            "date_key",
            "time_key",
            "model_version_key",
            "facility_code",
            "facility_name",
            "year",
            "month",
            "day",
            "hour",
            F.col("actual_energy_mwh").cast(dec(12, 6)).alias("actual_energy_mwh"),
            F.col("predicted_energy_mwh").cast(dec(12, 6)).alias("predicted_energy_mwh"),
            F.col("forecast_error_mwh").cast(dec(12, 6)).alias("forecast_error_mwh"),
            F.col("residual_mwh").cast(dec(12, 6)).alias("residual_mwh"),
            F.col("absolute_percentage_error").cast(dec(10, 4)).alias("absolute_percentage_error"),
            F.col("mape_pct").cast(dec(10, 4)).alias("mape_pct"),
            F.col("prediction_accuracy_pct").cast(dec(10, 4)).alias("prediction_accuracy_pct"),
            F.col("mae_metric").cast(dec(12, 6)).alias("mae_metric"),
            F.col("rmse_metric").cast(dec(12, 6)).alias("rmse_metric"),
            F.col("r2_score").cast(dec(10, 6)).alias("r2_score"),
            "is_accurate_forecast",
            "forecast_timestamp",
            "created_at",
            "updated_at",
        )
        
        if is_empty(result):
            return None
        
        return {"fact_solar_forecast_regression": result}


__all__ = ["GoldFactSolarForecastRegressionLoader"]
