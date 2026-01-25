from __future__ import annotations

import logging
import math
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def add_temporal_features(df: DataFrame, time_col: str = "date_hour") -> DataFrame:
    try:
        # Validate column existence
        if time_col not in df.columns:
            raise ValueError(f"Column '{time_col}' not found in DataFrame")
        
        # Add temporal features with error handling
        df = df.withColumn("hour_of_day", F.hour(time_col).cast("double"))
        df = df.withColumn("day_of_week", F.dayofweek(time_col).cast("double"))
        df = df.withColumn("month", F.month(time_col).cast("double"))
        df = df.withColumn("is_weekend", 
                           F.when(F.dayofweek(time_col).isin([1, 7]), 1.0).otherwise(0.0))
        
        return df
    except (AnalysisException, ValueError) as e:
        logger.error(f"Failed to add temporal features: {str(e)}")
        raise


def add_cyclical_encoding(df: DataFrame) -> DataFrame:
    try:
        # Validate required columns exist
        required_cols = ["hour_of_day", "month"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Hour cyclical encoding (24 hours)
        df = df.withColumn("hour_sin", 
                           F.sin(2 * math.pi * F.col("hour_of_day") / 24))
        df = df.withColumn("hour_cos", 
                           F.cos(2 * math.pi * F.col("hour_of_day") / 24))
        
        # Month cyclical encoding (12 months)
        df = df.withColumn("month_sin", 
                           F.sin(2 * math.pi * F.col("month") / 12))
        df = df.withColumn("month_cos", 
                           F.cos(2 * math.pi * F.col("month") / 12))
        
        return df
    except (AnalysisException, ValueError) as e:
        logger.error(f"Failed to add cyclical encoding: {str(e)}")
        raise


def add_lag_features(df: DataFrame, 
                     value_col: str = "energy_mwh",
                     partition_col: str = "facility_code",
                     time_col: str = "date_hour",
                     lag_hours: List[int] = None) -> DataFrame:
    try:
        if lag_hours is None:
            lag_hours = [1, 24, 168]  # 1 hour, 1 day, 1 week
        
        # Validate required columns exist
        required_cols = [value_col, partition_col, time_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Validate lag_hours parameter
        if not isinstance(lag_hours, list) or not all(isinstance(h, int) and h > 0 for h in lag_hours):
            raise ValueError("lag_hours must be a list of positive integers")
        
        window_spec = Window.partitionBy(partition_col).orderBy(time_col)
        
        for lag_h in lag_hours:
            # Create lag column name: energy_mwh -> energy_lag_1h
            base_name = value_col.replace('_mwh', '')  # Remove _mwh suffix
            lag_col_name = f"{base_name}_lag_{lag_h}h"
            # Using lag with offset based on hourly data
            df = df.withColumn(lag_col_name, 
                              F.lag(F.col(value_col), lag_h).over(window_spec))
        
        return df
    except (AnalysisException, ValueError) as e:
        logger.error(f"Failed to add lag features: {str(e)}")
        raise
