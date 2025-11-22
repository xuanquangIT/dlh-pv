"""Gold loader for fact_solar_forecast_regression - RandomForest regression predictions."""

from __future__ import annotations

from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import broadcast_small_dim, compute_date_key, dec, is_empty, require_sources


class GoldFactSolarForecastRegressionLoader(BaseGoldLoader):
    """Materialise solar forecast fact records from RandomForest regression predictions.
    
    Reads predictions from the Iceberg table written by train_regression_model.py,
    joins with dimension tables to create a star schema fact table for regression forecasts.
    
    Grain: 1 row = 1 regression forecast for 1 hour at 1 facility
    
    Difference from fact_energy_forecast:
    - fact_energy_forecast: Classification predictions (HIGH/LOW) from LogisticRegression
    - fact_solar_forecast_regression: Continuous predictions (MWh) from RandomForestRegressor
    """

    source_tables: Dict[str, SourceTableConfig] = {
        # Predictions already in Gold layer (written by train_regression_model.py)
        "fact_solar_forecast": SourceTableConfig(
            table_name="lh.gold.fact_solar_forecast_regression",
            required_columns=[
                "facility_key", "date_key", "time_key", "model_version_key",
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
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_solar_forecast_regression": GoldTableConfig(
            iceberg_table="lh.gold.fact_solar_forecast_regression",
            s3_base_path="s3a://lakehouse/gold/fact_solar_forecast_regression",
            partition_cols=("date_key",),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        """Transform regression predictions from fact_solar_forecast into dedicated table."""
        
        # Validate required dimensions exist
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
        
        # Broadcast small dimensions for efficient joins
        dim_facility = broadcast_small_dim(required["dim_facility"])
        dim_date = broadcast_small_dim(required["dim_date"])
        dim_time = broadcast_small_dim(required["dim_time"])
        
        # Load predictions from fact_solar_forecast (already in Gold)
        fact_source = required["fact_solar_forecast"]
        
        # Filter for regression predictions only (model_version_key >= 2 or specific identifier)
        # For now, we'll keep all but you can add filter logic here
        preds = fact_source
        
        pred_count = preds.count()
        print(f"[GOLD] Processing {pred_count} regression predictions")
        
        if pred_count == 0:
            print("[GOLD] No regression predictions found")
            return None
        
        # Enrich with dimension attributes (for reporting convenience)
        fact = preds.alias("f")
        
        # Join with dim_facility
        fact = fact.join(
            dim_facility.select(
                F.col("facility_key"),
                F.col("facility_code"),
                F.col("facility_name"),
            ).alias("fac"),
            on="facility_key",
            how="left"
        )
        
        # Join with dim_date
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
        
        # Join with dim_time
        fact = fact.join(
            dim_time.select(
                F.col("time_key"),
                F.col("hour"),
                F.col("minute"),
            ).alias("t"),
            on="time_key",
            how="left"
        )
        
        # Add computed metrics
        fact = (
            fact
            # Accuracy metrics
            .withColumn("mape_pct", F.col("absolute_percentage_error"))
            .withColumn("prediction_accuracy_pct", 
                       F.lit(100.0) - F.col("absolute_percentage_error"))
            
            # Forecast quality flag
            .withColumn("is_accurate_forecast",
                       F.when(F.col("absolute_percentage_error") <= 10.0, True)
                       .otherwise(False))
            
            # Residual (signed error)
            .withColumn("residual_mwh", F.col("forecast_error_mwh"))
            
            # Timestamp columns
            .withColumn("updated_at", F.current_timestamp())
        )
        
        # Select final columns for regression fact table
        result = fact.select(
            # Primary keys
            F.monotonically_increasing_id().alias("forecast_id"),
            
            # Dimension keys
            "facility_key",
            "date_key",
            "time_key",
            "model_version_key",
            
            # Dimension attributes (denormalized for reporting)
            "facility_code",
            "facility_name",
            "year",
            "month",
            "day",
            "hour",
            
            # Measures - actual vs predicted
            F.col("actual_energy_mwh").cast(dec(12, 6)).alias("actual_energy_mwh"),
            F.col("predicted_energy_mwh").cast(dec(12, 6)).alias("predicted_energy_mwh"),
            
            # Error metrics
            F.col("forecast_error_mwh").cast(dec(12, 6)).alias("forecast_error_mwh"),
            F.col("residual_mwh").cast(dec(12, 6)).alias("residual_mwh"),
            F.col("absolute_percentage_error").cast(dec(10, 4)).alias("absolute_percentage_error"),
            F.col("mape_pct").cast(dec(10, 4)).alias("mape_pct"),
            F.col("prediction_accuracy_pct").cast(dec(10, 4)).alias("prediction_accuracy_pct"),
            
            # Model performance metrics
            F.col("mae_metric").cast(dec(12, 6)).alias("mae_metric"),
            F.col("rmse_metric").cast(dec(12, 6)).alias("rmse_metric"),
            F.col("r2_score").cast(dec(10, 6)).alias("r2_score"),
            
            # Quality flags
            "is_accurate_forecast",
            
            # Timestamps
            "forecast_timestamp",
            "created_at",
            "updated_at",
        )
        
        if is_empty(result):
            return None
        
        return {"fact_solar_forecast_regression": result}


__all__ = ["GoldFactSolarForecastRegressionLoader"]
