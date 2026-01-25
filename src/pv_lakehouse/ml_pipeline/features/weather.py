from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


# Constants for better maintainability
EPSILON_TOLERANCE = 1e-6  # Division safety threshold for floating point comparisons
DEFAULT_MIN_RADIATION = 10.0  # Default minimum radiation threshold for production hours
DEFAULT_LOW_ENERGY_THRESHOLD = 0.1  # Default threshold for low energy period detection


def add_weather_interactions(df: DataFrame) -> DataFrame:
    # Validate input DataFrame
    if not isinstance(df, DataFrame):
        raise TypeError("Input must be a PySpark DataFrame")
    
    # Check required columns exist
    required_cols = ["shortwave_radiation", "temperature_2m", "cloud_cover", 
                    "hour_of_day", "completeness_pct"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    try:
        # Radiation-temperature interaction (solar efficiency)
        df = df.withColumn("radiation_temp_interaction",
                           F.col("shortwave_radiation") * F.col("temperature_2m"))
        
        # Cloud impact on radiation with epsilon tolerance
        df = df.withColumn("cloud_radiation_ratio",
                           F.when(F.col("shortwave_radiation") > EPSILON_TOLERANCE,
                                 F.col("cloud_cover") / F.col("shortwave_radiation"))
                           .otherwise(0.0))
        
        # Hour-radiation interaction (time of day effect on solar)
        df = df.withColumn("hour_radiation_interaction",
                           F.col("hour_of_day") * F.col("shortwave_radiation"))
        
        # Completeness-radiation interaction
        df = df.withColumn("completeness_radiation_interaction",
                           F.col("completeness_pct") * F.col("shortwave_radiation"))
        
        # Wind-temperature interaction (cooling effect)
        if "wind_speed_10m" in df.columns:
            df = df.withColumn("wind_temp_interaction",
                              F.col("wind_speed_10m") * F.col("temperature_2m"))
        
        # Direct vs diffuse radiation ratio (cloud condition indicator)
        if "direct_radiation" in df.columns and "diffuse_radiation" in df.columns:
            df = df.withColumn("direct_diffuse_ratio",
                              F.when(F.col("diffuse_radiation") > EPSILON_TOLERANCE,
                                    F.col("direct_radiation") / F.col("diffuse_radiation"))
                              .otherwise(0.0))
        
        # Non-linear effects
        df = df.withColumn("shortwave_radiation_sq",
                           F.col("shortwave_radiation") * F.col("shortwave_radiation"))
        
        df = df.withColumn("temperature_sq",
                           F.col("temperature_2m") * F.col("temperature_2m"))
        
        df = df.withColumn("cloud_temp_interaction",
                           F.col("cloud_cover") * F.col("temperature_2m"))
        
        logger.info("Successfully added weather interaction features")
        return df
        
    except (AnalysisException, ValueError) as e:
        logger.error(f"Error adding weather interactions: {str(e)}")
        raise


def add_production_indicators(df: DataFrame, 
                              min_radiation: float = DEFAULT_MIN_RADIATION,
                              low_energy_threshold: float = DEFAULT_LOW_ENERGY_THRESHOLD) -> DataFrame:
    # Validate input DataFrame
    if not isinstance(df, DataFrame):
        raise TypeError("Input must be a PySpark DataFrame")
    
    # Validate parameters
    if min_radiation < 0:
        raise ValueError("min_radiation must be non-negative")
    
    if low_energy_threshold < 0:
        raise ValueError("low_energy_threshold must be non-negative")
    
    # Check required column exists
    if "shortwave_radiation" not in df.columns:
        raise ValueError("Missing required column: shortwave_radiation")
    
    try:
        # Production hour: daytime with sufficient solar radiation
        df = df.withColumn("is_production_hour",
                           F.when(F.col("shortwave_radiation") >= min_radiation, 1.0)
                           .otherwise(0.0))
        
        # Low energy period detection (if energy_mwh exists)
        if "energy_mwh" in df.columns:
            df = df.withColumn("is_low_energy_period",
                              F.when(F.col("energy_mwh") <= low_energy_threshold, 1.0)
                              .otherwise(0.0))
        
        logger.info("Successfully added production indicators")
        return df
        
    except (AnalysisException, ValueError) as e:
        logger.error(f"Error adding production indicators: {str(e)}")
        raise
