from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .temporal import add_temporal_features, add_cyclical_encoding, add_lag_features
from .weather import add_weather_interactions, add_production_indicators


def prepare_features(df: DataFrame, 
                     feature_config,
                     include_lag: bool = True,
                     include_air_quality: bool = False) -> DataFrame:
    
    print("Starting feature engineering pipeline...")
    
    # 1. Add temporal features
    df = add_temporal_features(df, time_col="date_hour")
    df = add_cyclical_encoding(df)
    
    # 2. Add lag features (if enabled and data supports it)
    if include_lag:
        df = add_lag_features(
            df,
            value_col=feature_config.target_column,
            partition_col="facility_code",
            time_col="date_hour",
            lag_hours=[1, 24, 168]
        )
    
    # 3. Add weather interactions
    df = add_weather_interactions(df)
    
    # 4. Add production indicators
    df = add_production_indicators(
        df,
        min_radiation=feature_config.min_radiation_threshold,
        low_energy_threshold=feature_config.low_energy_threshold
    )
    
    print("Feature engineering complete")
    return df


def select_training_features(df: DataFrame, 
                             feature_config,
                             include_air_quality: bool = False) -> DataFrame:
    
    # Get feature columns
    feature_cols = feature_config.get_all_features(include_air_quality)
    target_col = feature_config.target_column
    
    # Always keep facility_code and date_hour for tracking
    select_cols = ["facility_code", "date_hour", target_col] + feature_cols
    
    # Filter to existing columns only
    existing_cols = [c for c in select_cols if c in df.columns]
    
    return df.select(*existing_cols)


def validate_features(df: DataFrame, min_rows: int = 1000) -> DataFrame:
    
    # Debug: show row count before filtering
    initial_count = df.count()
    print(f"Initial row count: {initial_count}")
    
    # Fill nulls in lag features with 0 FIRST (before dropna)
    lag_cols = [c for c in df.columns if '_lag_' in c]
    if lag_cols:
        print(f"Filling {len(lag_cols)} lag columns with 0 for null values")
        for lag_col in lag_cols:
            df = df.fillna({lag_col: 0.0})
    
    # Then remove rows with null values only in non-lag columns
    # Keep facility_code and date_hour which may have nulls
    df = df.dropna(subset=[c for c in df.columns 
                           if c not in ['facility_code', 'date_hour'] 
                           and '_lag_' not in c])
    
    row_count = df.count()
    print(f"After null filtering: {row_count} rows (dropped {initial_count - row_count})")
    
    if row_count < min_rows:
        raise ValueError(f"Insufficient data: {row_count} rows (minimum {min_rows})")
    
    print(f"Validation passed: {row_count} rows")
    return df
