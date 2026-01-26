from __future__ import annotations

import logging
from typing import List, Protocol

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from .temporal import add_temporal_features, add_cyclical_encoding, add_lag_features
from .weather import add_weather_interactions, add_production_indicators

# Constants for better maintainability
DEFAULT_LAG_HOURS: List[int] = [1, 24, 168]  # 1h, 1d, 1w
DEFAULT_MIN_ROWS: int = 1000
DEFAULT_NULL_FILL_VALUE: float = 0.0

# Column name constants
TIME_COLUMN: str = "date_hour"
FACILITY_COLUMN: str = "facility_code"
LAG_PATTERN: str = "_lag_"

logger = logging.getLogger(__name__)


# Protocol for type safety
class FeatureConfig(Protocol):
    """Protocol defining the interface for feature configuration objects."""
    target_column: str
    min_radiation_threshold: float
    low_energy_threshold: float
    
    def get_all_features(self, include_air_quality: bool) -> List[str]:
        """Get all feature column names."""
        ...


def _validate_feature_config(feature_config: FeatureConfig, required_attrs: List[str]) -> None:
    """Validate feature config has all required attributes."""
    if feature_config is None:
        raise ValueError("feature_config cannot be None")
    
    missing_attrs = [attr for attr in required_attrs if not hasattr(feature_config, attr)]
    if missing_attrs:
        raise ValueError(f"feature_config missing required attributes: {missing_attrs}")


def prepare_features(df: DataFrame, 
                     feature_config: FeatureConfig,
                     include_lag: bool = True,
                     include_air_quality: bool = False) -> DataFrame:
    
    # Validate feature_config parameter using centralized function
    _validate_feature_config(feature_config, 
                            ['target_column', 'min_radiation_threshold', 'low_energy_threshold'])
    
    logger.info("Starting feature engineering pipeline...")
    
    # 1. Add temporal features
    df = add_temporal_features(df, time_col=TIME_COLUMN)
    df = add_cyclical_encoding(df)
    
    # 2. Add lag features (if enabled and data supports it)
    if include_lag:
        df = add_lag_features(
            df,
            value_col=feature_config.target_column,
            partition_col=FACILITY_COLUMN,
            time_col=TIME_COLUMN,
            lag_hours=DEFAULT_LAG_HOURS
        )
    
    # 3. Add weather interactions
    df = add_weather_interactions(df)
    
    # 4. Add production indicators
    df = add_production_indicators(
        df,
        min_radiation=feature_config.min_radiation_threshold,
        low_energy_threshold=feature_config.low_energy_threshold
    )
    
    logger.info("Feature engineering complete")
    return df


def select_training_features(df: DataFrame, 
                             feature_config: FeatureConfig,
                             include_air_quality: bool = False) -> DataFrame:
    
    # Validate feature_config parameter using centralized function
    _validate_feature_config(feature_config, ['get_all_features', 'target_column'])
    
    # Get feature columns
    feature_cols = feature_config.get_all_features(include_air_quality)
    target_col = feature_config.target_column
    
    # Always keep facility_code and date_hour for tracking
    select_cols = [FACILITY_COLUMN, TIME_COLUMN, target_col] + feature_cols
    
    # Filter to existing columns only
    existing_cols = [c for c in select_cols if c in df.columns]
    
    return df.select(*existing_cols)


def validate_features(df: DataFrame, min_rows: int = DEFAULT_MIN_ROWS) -> DataFrame:
    
    try:
        # Cache DataFrame to avoid multiple count operations
        df = df.cache()
        initial_count = df.count()
        logger.info(f"Initial row count: {initial_count}")
        
        # Batch fill nulls instead of iterative operations
        lag_cols = [c for c in df.columns if LAG_PATTERN in c]
        if lag_cols:
            logger.info(f"Filling {len(lag_cols)} lag columns with 0 for null values")
            # Create fill dict for all lag columns at once
            fill_dict = {col: DEFAULT_NULL_FILL_VALUE for col in lag_cols}
            df = df.fillna(fill_dict)  # Single fillna operation
        
        # Then remove rows with null values only in non-lag columns
        # Keep facility_code and date_hour which may have nulls
        excluded_cols = [FACILITY_COLUMN, TIME_COLUMN]
        df = df.dropna(subset=[c for c in df.columns 
                               if c not in excluded_cols
                               and LAG_PATTERN not in c])
        
        row_count = df.count()
        logger.info(f"After null filtering: {row_count} rows (dropped {initial_count - row_count})")
        
        if row_count < min_rows:
            raise ValueError(f"Insufficient data: {row_count} rows (minimum {min_rows})")
        
        logger.info(f"Validation passed: {row_count} rows")
        return df
        
    except (AnalysisException, ValueError) as e:
        logger.error(f"Feature validation failed: {str(e)}")
        raise
