"""Gold loader for fact_energy_forecast."""

from __future__ import annotations

from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .base import BaseGoldLoader, GoldTableConfig, SourceTableConfig
from .common import broadcast_small_dim, compute_date_key, dec, is_empty, require_sources


class GoldFactEnergyForecastLoader(BaseGoldLoader):
    """Materialise energy forecast fact records from ML predictions.
    
    Reads predictions from the S3 Parquet path written by the training script,
    joins with dimension tables to create a star schema fact table.
    
    Grain: 1 row = 1 forecast for 1 hour at 1 facility
    """

    source_tables: Dict[str, SourceTableConfig] = {
        # Predictions loaded directly from S3 Parquet in transform() method
        "dim_facility": SourceTableConfig(
            table_name="lh.gold.dim_facility",
            required_columns=["facility_key", "facility_code"],
        ),
        "dim_date": SourceTableConfig(
            table_name="lh.gold.dim_date",
            required_columns=["date_key", "full_date"],
        ),
        "dim_time": SourceTableConfig(
            table_name="lh.gold.dim_time",
            required_columns=["time_key", "hour"],
        ),
        "dim_forecast_model_version": SourceTableConfig(
            table_name="lh.gold.dim_forecast_model_version",
            required_columns=["model_version_id"],
        ),
    }

    gold_tables: Dict[str, GoldTableConfig] = {
        "fact_energy_forecast": GoldTableConfig(
            iceberg_table="lh.gold.fact_energy_forecast",
            s3_base_path="s3a://lakehouse/gold/fact_energy_forecast",
            partition_cols=("date_key",),
        )
    }

    def transform(self, sources: Dict[str, DataFrame]) -> Optional[Dict[str, DataFrame]]:
        """Transform ML predictions into Gold fact table with dimension keys."""
        
        # Validate required dimensions exist
        required = require_sources(
            {
                "dim_facility": sources.get("dim_facility"),
                "dim_date": sources.get("dim_date"),
                "dim_time": sources.get("dim_time"),
                "dim_forecast_model_version": sources.get("dim_forecast_model_version"),
            },
            {
                "dim_facility": "fact_energy_forecast",
                "dim_date": "fact_energy_forecast",
                "dim_time": "fact_energy_forecast",
                "dim_forecast_model_version": "fact_energy_forecast",
            },
        )
        
        # Broadcast small dimensions for efficient joins
        dim_facility = broadcast_small_dim(required["dim_facility"])
        dim_date = broadcast_small_dim(required["dim_date"])
        dim_time = broadcast_small_dim(required["dim_time"])
        dim_model = broadcast_small_dim(required["dim_forecast_model_version"])
        
        # Load predictions from S3 Parquet (written by training script)
        spark = SparkSession.getActiveSession()
        preds_s3_path = "s3a://lakehouse/silver/energy_forecast_predictions"
        
        try:
            preds = spark.read.format("parquet").load(preds_s3_path)
            pred_count = preds.count()
            print(f"[GOLD] Loaded {pred_count} predictions from {preds_s3_path}")
            
            if pred_count == 0:
                print(f"[GOLD] No predictions found in {preds_s3_path}")
                return None
                
        except Exception as e:
            print(f"[GOLD] Failed to load predictions from {preds_s3_path}: {e}")
            return None
        
        # Ensure required columns exist
        required_cols = {"facility_code", "date_hour", "model_version_id", 
                        "forecast_energy_mwh", "forecast_score"}
        if not required_cols.issubset(set(preds.columns)):
            missing = required_cols - set(preds.columns)
            raise ValueError(f"Predictions missing required columns: {missing}")
        
        # Build fact table with timestamp as UTC
        fact = preds.withColumn("date_hour_ts_utc", F.col("date_hour").cast("timestamp"))
        
        # Join with dim_facility (broadcast join)
        fact = fact.join(dim_facility, on="facility_code", how="left")
        
        # Join with dim_date (broadcast join)
        dim_date_selected = dim_date.select(
            F.col("full_date").alias("dim_full_date"),
            F.col("date_key").alias("dim_date_key"),
        )
        fact = fact.join(
            dim_date_selected,
            F.to_date(F.col("date_hour_ts_utc")) == F.col("dim_full_date"),
            how="left"
        )
        fact = fact.withColumn("date_key", F.col("dim_date_key"))
        
        # Join with dim_time (broadcast join)
        dim_time_selected = dim_time.select(
            F.col("time_key").alias("dim_time_key"),
            F.col("hour"),
        )
        fact = fact.join(
            dim_time_selected,
            F.hour(F.col("date_hour_ts_utc")) == F.col("hour"),
            how="left"
        )
        fact = fact.withColumn("time_key", F.col("dim_time_key"))
        
        # Join with dim_forecast_model_version (broadcast join)
        fact = fact.join(dim_model.select("model_version_id"), on="model_version_id", how="left")
        
        # Add forecast timestamp and audit columns
        fact = fact.withColumn("forecast_timestamp", F.col("date_hour_ts_utc"))
        fact = fact.withColumn("created_at", F.current_timestamp())
        fact = fact.withColumn("updated_at", F.current_timestamp())
        
        # Select final columns matching Gold schema pattern
        result = fact.select(
            # Dimension keys
            "facility_key",
            "date_key",
            "time_key",
            "model_version_id",
            
            # Measures
            F.col("forecast_energy_mwh").cast(dec(12, 6)).alias("forecast_energy_mwh"),
            F.col("forecast_score").cast(dec(10, 6)).alias("forecast_score"),
            
            # Timestamps
            "forecast_timestamp",
            "created_at",
            "updated_at",
        )
        
        if is_empty(result):
            return None
        
        return {"fact_energy_forecast": result}


__all__ = ["GoldFactEnergyForecastLoader"]
