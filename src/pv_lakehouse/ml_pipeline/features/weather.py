from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_weather_interactions(df: DataFrame) -> DataFrame:
    
    # Radiation-temperature interaction (solar efficiency)
    df = df.withColumn("radiation_temp_interaction",
                       F.col("shortwave_radiation") * F.col("temperature_2m"))
    
    # Cloud impact on radiation
    df = df.withColumn("cloud_radiation_ratio",
                       F.when(F.col("shortwave_radiation") > 0,
                             F.col("cloud_cover") / (F.col("shortwave_radiation") + 1))
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
                          F.when(F.col("diffuse_radiation") > 0,
                                F.col("direct_radiation") / (F.col("diffuse_radiation") + 1))
                          .otherwise(0.0))
    
    # Non-linear effects
    df = df.withColumn("shortwave_radiation_sq",
                       F.col("shortwave_radiation") * F.col("shortwave_radiation"))
    
    df = df.withColumn("temperature_sq",
                       F.col("temperature_2m") * F.col("temperature_2m"))
    
    df = df.withColumn("cloud_temp_interaction",
                       F.col("cloud_cover") * F.col("temperature_2m"))
    
    return df


def add_production_indicators(df: DataFrame, 
                              min_radiation: float = 10.0,
                              low_energy_threshold: float = 0.1) -> DataFrame:
    
    # Production hour: daytime with sufficient solar radiation
    df = df.withColumn("is_production_hour",
                       F.when(F.col("shortwave_radiation") >= min_radiation, 1.0)
                       .otherwise(0.0))
    
    # Low energy period detection (if energy_mwh exists)
    if "energy_mwh" in df.columns:
        df = df.withColumn("is_low_energy_period",
                          F.when(F.col("energy_mwh") <= low_energy_threshold, 1.0)
                          .otherwise(0.0))
    
    return df
