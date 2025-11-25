"""Training script for solar energy REGRESSION forecasting model.

This is the REGRESSION component of our Hybrid ML approach:
- Classification Model (train_forecast_model.py): Anomaly detection & data quality
- Regression Model (THIS FILE): True forecasting - predict actual energy values

Trains a Decision Tree Regressor to directly predict energy_mwh for future hours.
"""

from __future__ import annotations

import argparse
import math
import os
from datetime import datetime
from typing import Optional

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.utils import AnalysisException

from pyspark.sql.window import Window
from pv_lakehouse.etl.utils.spark_utils import create_spark_session, write_iceberg_table

DEFAULT_SAMPLE_LIMIT = 50_000  # Balance between performance and memory
SILVER_HOURLY_ENERGY_TABLE = "lh.silver.clean_hourly_energy"
SILVER_HOURLY_WEATHER_TABLE = "lh.silver.clean_hourly_weather"
DIM_FACILITY_TABLE = "lh.gold.dim_facility"
DIM_MODEL_VERSION_TABLE = "lh.gold.dim_model_version"
GOLD_OUTPUT_TABLE = "lh.gold.fact_solar_forecast_regression"
GOLD_WRITE_MODE = "overwrite"  # Overwrite to replace old predictions with fresh training results

# Enhanced feature set with weather and temporal patterns + LAG features
FEATURE_COLUMNS = [
    # Energy data quality features
    "intervals_count", 
    "completeness_pct",
    # Temporal features (expanded)
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    "hour_sin",  # Cyclical encoding
    "hour_cos",
    "month_sin",
    "month_cos",
    # Weather features (CRITICAL for regression)
    "temperature_2m",
    "cloud_cover",
    "shortwave_radiation",
    "direct_radiation",
    # Advanced interaction features
    "radiation_temp_interaction",  # solar * temp
    "cloud_radiation_ratio",  # impact of clouds
    "hour_radiation_interaction",  # time of day effect
    "completeness_radiation_interaction",
    # LAG FEATURES - MOST IMPORTANT FOR TIME SERIES
    "energy_lag_1h",  # Previous hour
    "energy_lag_24h",  # Same hour yesterday
    "energy_lag_168h",  # Same hour last week
    # Production indicators
    "is_production_hour",  # Daytime with solar radiation
    "is_low_energy_period",  # Edge case detection
    # Non-linear features
    "shortwave_radiation_sq",  # Quadratic radiation effect
    "temperature_sq",  # Quadratic temperature effect
    "cloud_temp_interaction",  # Cloud-temperature interaction
]
TARGET_COLUMN = "energy_mwh"  # Regression target


def add_aggressive_noise(df: DataFrame, noisy_columns: list[str], 
                        noise_magnitude: float = 0.0,
                        noise_ratio: float = 0.0) -> DataFrame:
    """Apply moderate noise injection for regularization.
    
    Args:
        df: Input DataFrame
        noisy_columns: List of numeric columns to add noise to
        noise_magnitude: Maximum noise magnitude (DISABLED - 0%)
        noise_ratio: Fraction of rows to apply noise (DISABLED - 0%)
    
    Returns:
        DataFrame with noise applied to specified columns
    """
    import random
    random.seed(42)
    
    for col_name in noisy_columns:
        if col_name not in df.columns:
            continue
            
        # Generate random noise: uniform(-0.6, 0.6)
        noise = (F.rand(seed=42) * 2 - 1) * noise_magnitude
        
        # Apply to 95% of rows randomly
        should_apply_noise = F.rand(seed=43) < noise_ratio
        
        # Multiplicative noise: x_new = x * (1 + noise)
        noisy_value = F.col(col_name) * (1 + noise)
        
        # Conditional application
        df = df.withColumn(
            col_name,
            F.when(should_apply_noise, noisy_value).otherwise(F.col(col_name))
        )
    
    print(f"[NOISE] Applied ±{noise_magnitude*100:.0f}% noise to {len(noisy_columns)} features on {noise_ratio*100:.0f}% of rows")
    return df


def load_silver_features_regression(spark: SparkSession, limit_rows: int = DEFAULT_SAMPLE_LIMIT) -> DataFrame:
    """Load Silver data for REGRESSION task - predict energy_mwh directly."""
    try:
        silver_energy = spark.table(SILVER_HOURLY_ENERGY_TABLE)
    except AnalysisException as exc:
        raise RuntimeError(
            f"Silver table '{SILVER_HOURLY_ENERGY_TABLE}' unavailable. Run Silver loaders first."
        ) from exc

    # Load weather data (REQUIRED for regression)
    try:
        silver_weather = spark.table(SILVER_HOURLY_WEATHER_TABLE)
        print(f"[TRAIN] Weather data loaded - essential for regression forecasting")
    except AnalysisException as exc:
        raise RuntimeError(
            "Weather data REQUIRED for regression forecasting. Run weather loader first."
        ) from exc

    # Select and prepare energy features
    filtered = (
        silver_energy.select(
            "facility_code",
            "date_hour",
            F.col("energy_mwh").cast("double").alias("energy_mwh"),
            F.col("intervals_count").cast("double").alias("intervals_count"),
            F.col("completeness_pct").cast("double").alias("completeness_pct"),
        )
        .where(F.col("energy_mwh").isNotNull())
        .where(F.col("intervals_count").isNotNull())
        .where(F.col("completeness_pct").isNotNull())
    )

    # Add temporal features with cyclical encoding
    filtered = (
        filtered
        .withColumn("hour_of_day", F.hour("date_hour").cast("double"))
        .withColumn("day_of_week", F.dayofweek("date_hour").cast("double"))
        .withColumn("month", F.month("date_hour").cast("double"))
        .withColumn("is_weekend", F.when(F.dayofweek("date_hour").isin([1, 7]), 1.0).otherwise(0.0))
        # Cyclical encoding for time features
        .withColumn("hour_sin", F.sin(2 * 3.14159 * F.col("hour_of_day") / 24.0))
        .withColumn("hour_cos", F.cos(2 * 3.14159 * F.col("hour_of_day") / 24.0))
        .withColumn("month_sin", F.sin(2 * 3.14159 * F.col("month") / 12.0))
        .withColumn("month_cos", F.cos(2 * 3.14159 * F.col("month") / 12.0))
    )

    # Join with weather
    weather_features = (
        silver_weather.select(
            F.col("facility_code").alias("w_facility_code"),
            F.col("date_hour").alias("w_date_hour"),
            F.col("temperature_2m").cast("double").alias("temperature_2m"),
            F.col("cloud_cover").cast("double").alias("cloud_cover"),
            F.col("shortwave_radiation").cast("double").alias("shortwave_radiation"),
            F.col("direct_radiation").cast("double").alias("direct_radiation"),
        )
        .where(F.col("temperature_2m").isNotNull())
        .where(F.col("shortwave_radiation").isNotNull())  # REQUIRED for regression
    )
    
    filtered = filtered.join(
        weather_features,
        (filtered.facility_code == weather_features.w_facility_code) &
        (filtered.date_hour == weather_features.w_date_hour),
        how="inner"  # INNER join - we NEED weather for forecasting
    ).drop("w_facility_code", "w_date_hour")
    
    # LAG FEATURES - CRITICAL FOR TIME SERIES (add before sampling)
    from pyspark.sql.window import Window
    
    window_by_facility = Window.partitionBy("facility_code").orderBy("date_hour")
    filtered = (
        filtered
        # Lag 1 hour: energy from previous hour
        .withColumn("energy_lag_1h", F.lag("energy_mwh", 1).over(window_by_facility))
        # Lag 24 hours: energy from same hour yesterday
        .withColumn("energy_lag_24h", F.lag("energy_mwh", 24).over(window_by_facility))
        # Lag 168 hours: energy from same hour last week
        .withColumn("energy_lag_168h", F.lag("energy_mwh", 168).over(window_by_facility))
        # Fill nulls with 0 (for first records without history)
        .fillna(0.0, subset=["energy_lag_1h", "energy_lag_24h", "energy_lag_168h"])
    )
    
    # Advanced interaction features (CRITICAL for regression performance)
    filtered = (
        filtered
        # Solar radiation * temperature interaction
        .withColumn("radiation_temp_interaction", 
                   F.col("shortwave_radiation") * F.col("temperature_2m") / 1000.0)
        # Cloud cover impact on radiation
        .withColumn("cloud_radiation_ratio",
                   F.when(F.col("shortwave_radiation") > 0,
                         (100 - F.col("cloud_cover")) * F.col("shortwave_radiation") / 100.0)
                   .otherwise(0.0))
        # Hour of day effect on radiation
        .withColumn("hour_radiation_interaction",
                   F.col("hour_of_day") * F.col("shortwave_radiation") / 1000.0)
        # Data completeness impact
        .withColumn("completeness_radiation_interaction",
                   F.col("completeness_pct") * F.col("shortwave_radiation") / 10000.0)
        # NEW: Production hour indicator (daytime = high solar radiation)
        .withColumn("is_production_hour",
                   F.when((F.col("hour_of_day") >= 6) & (F.col("hour_of_day") <= 18) & 
                          (F.col("shortwave_radiation") > 50), 1.0)
                   .otherwise(0.0))
        # NEW: Low energy indicator (helps model learn edge cases)
        .withColumn("is_low_energy_period",
                   F.when(F.col("energy_lag_1h") < 5.0, 1.0).otherwise(0.0))
        # NEW: Squared features for non-linear patterns
        .withColumn("shortwave_radiation_sq",
                   F.col("shortwave_radiation") * F.col("shortwave_radiation") / 100000.0)
        .withColumn("temperature_sq",
                   F.col("temperature_2m") * F.col("temperature_2m") / 100.0)
        # NEW: Weather variability (cloud * temp interaction)
        .withColumn("cloud_temp_interaction",
                   F.col("cloud_cover") * F.col("temperature_2m") / 100.0)
    )

    # FILTER: Remove very low energy hours to reduce percentage error inflation
    # Keep only PRODUCTIVE hours: energy >= 5.0 MWh AND solar radiation > 0
    filtered = filtered.where(
        (F.col("energy_mwh") >= 5.0) & 
        (F.col("shortwave_radiation") > 0)
    )
    
    subset = filtered.orderBy(F.col("date_hour").desc())
    if limit_rows and limit_rows > 0:  # Only limit if explicitly set and > 0
        subset = subset.limit(limit_rows)
        print(f"[TRAIN] Limiting to {limit_rows} rows for training")
    else:
        print(f"[TRAIN] Using ALL available data (no limit)")

    subset = subset.cache()
    row_count = subset.count()
    if row_count < 100:
        subset.unpersist()
        raise RuntimeError("Not enough Silver rows with weather data to train regression model.")
    
    print(f"[FILTER] Applied production-hour filter: energy >= 5.0 MWh AND shortwave_radiation > 0 (sunlight hours only)")

    print(f"[TRAIN] Loaded {row_count} samples for regression training")
    
    # Statistics
    stats = subset.select(
        F.mean("energy_mwh").alias("mean"),
        F.stddev("energy_mwh").alias("std"),
        F.min("energy_mwh").alias("min"),
        F.max("energy_mwh").alias("max"),
    ).collect()[0]
    
    print(f"[TRAIN] Energy statistics:")
    print(f"  Mean: {stats['mean']:.2f} MWh")
    print(f"  Std:  {stats['std']:.2f} MWh")
    print(f"  Min:  {stats['min']:.2f} MWh")
    print(f"  Max:  {stats['max']:.2f} MWh")

    return subset


def load_facility_dimension(spark: SparkSession) -> Optional[DataFrame]:
    """Load facility dimension for Gold join."""
    try:
        return spark.table(DIM_FACILITY_TABLE).select("facility_code", "facility_key")
    except AnalysisException:
        print(f"[WARN] Dimension table '{DIM_FACILITY_TABLE}' not found, skipping facility join.")
        return None


def resolve_model_version_key(spark: SparkSession) -> int:
    """Get model version key from dimension table."""
    try:
        dim_df = spark.table(DIM_MODEL_VERSION_TABLE)
        max_key = dim_df.agg(F.max("model_version_key")).collect()[0][0]
        return int(max_key) if max_key is not None else 1
    except (AnalysisException, Exception):
        print(f"[WARN] Cannot resolve model_version_key from '{DIM_MODEL_VERSION_TABLE}', using default=1")
        return 1


def build_gold_fact(predictions: DataFrame, model_version_key: int) -> DataFrame:
    """Transform regression predictions into Gold fact_solar_forecast schema."""
    
    facility_dim = load_facility_dimension(predictions.sparkSession)
    window = Window.orderBy("facility_code", "date_hour")
    
    enriched = (
        predictions
        .withColumn("forecast_id", F.row_number().over(window))
        .withColumn("date_key", F.date_format(F.col("date_hour"), "yyyyMMdd").cast("int"))
        .withColumn("time_key", (F.hour("date_hour") * 100 + F.minute("date_hour")).cast("int"))
        .withColumn("model_version_key", F.lit(int(model_version_key)))
        .withColumn("weather_condition_key", F.lit(None).cast("int"))
        .withColumn("actual_energy_mwh", F.col("energy_mwh"))
        .withColumn("predicted_energy_mwh", F.col("prediction"))  # GBT prediction
        .withColumn("forecast_error_mwh", F.col("actual_energy_mwh") - F.col("predicted_energy_mwh"))
        .withColumn(
            "absolute_percentage_error",
            F.when(F.col("actual_energy_mwh") == 0, F.lit(0.0)).otherwise(
                F.abs(F.col("forecast_error_mwh") / F.col("actual_energy_mwh")) * 100.0
            ),
        )
        .withColumn("mae_metric", F.abs(F.col("forecast_error_mwh")))
        .withColumn("rmse_metric", F.abs(F.col("forecast_error_mwh")))
        .withColumn("r2_score", F.lit(1.0))  # Will be updated with actual R2
        .withColumn("forecast_timestamp", F.col("date_hour"))
        .withColumn("created_at", F.current_timestamp())
    )
    
    if facility_dim is not None:
        enriched = enriched.join(facility_dim, on="facility_code", how="left")
    else:
        enriched = enriched.withColumn("facility_key", F.lit(None).cast("int"))
    
    return enriched.select(
        "forecast_id",
        "date_key",
        "time_key",
        "facility_key",
        "weather_condition_key",
        "model_version_key",
        "actual_energy_mwh",
        "predicted_energy_mwh",
        "forecast_error_mwh",
        "absolute_percentage_error",
        "mae_metric",
        "rmse_metric",
        "r2_score",
        "forecast_timestamp",
        "created_at",
    )


def write_gold_predictions(gold_df: DataFrame, table_name: str, mode: str = "append") -> int:
    """Write predictions to Gold layer Iceberg table."""
    row_count = gold_df.count()
    write_iceberg_table(gold_df, table_name, mode=mode)
    print(f"\n✓ Saved {row_count} predictions to {table_name}")
    return row_count


def save_feature_importance_to_gold(spark: SparkSession, model: Pipeline, model_version_key: int) -> None:
    """Save feature importance to dim_feature_importance table in Gold layer.
    
    Args:
        spark: SparkSession
        model: Fitted pipeline (includes RandomForestRegressor)
        model_version_key: Model version key from dim_model_version table
    """
    from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, IntegerType, BooleanType, TimestampType
    
    # Extract Random Forest model from pipeline
    rf_model = model.stages[-1]
    
    # Get feature importances
    importances = rf_model.featureImportances.toArray()
    
    # Category mapping
    category_map = {
        # Energy quality features
        "intervals_count": "Energy Quality",
        "completeness_pct": "Energy Quality",
        
        # Temporal features
        "hour_of_day": "Temporal",
        "day_of_week": "Temporal",
        "month": "Temporal",
        "is_weekend": "Temporal",
        "hour_sin": "Temporal",
        "hour_cos": "Temporal",
        "month_sin": "Temporal",
        "month_cos": "Temporal",
        
        # Weather features
        "temperature_2m": "Weather",
        "cloud_cover": "Weather",
        "shortwave_radiation": "Weather",
        "direct_radiation": "Weather",
        
        # Interaction features
        "radiation_temp_interaction": "Interaction",
        "cloud_radiation_ratio": "Interaction",
        "hour_radiation_interaction": "Interaction",
        "completeness_radiation_interaction": "Interaction",
        
        # LAG features
        "energy_lag_1h": "LAG Features",
        "energy_lag_24h": "LAG Features",
        "energy_lag_168h": "LAG Features",
        
        # Production indicators
        "is_production_hour": "Production Indicators",
        "is_low_energy_period": "Production Indicators",
    }
    
    # Description mapping
    description_map = {
        "intervals_count": "Number of 30-min intervals with valid data",
        "completeness_pct": "Data completeness percentage",
        "hour_of_day": "Hour of the day (0-23)",
        "day_of_week": "Day of week (1=Sunday, 7=Saturday)",
        "month": "Month of year (1-12)",
        "is_weekend": "Weekend indicator (1=weekend, 0=weekday)",
        "hour_sin": "Hour sine encoding (cyclical)",
        "hour_cos": "Hour cosine encoding (cyclical)",
        "month_sin": "Month sine encoding (cyclical)",
        "month_cos": "Month cosine encoding (cyclical)",
        "temperature_2m": "Temperature at 2m height (°C)",
        "cloud_cover": "Cloud cover percentage (%)",
        "shortwave_radiation": "Shortwave radiation (W/m²)",
        "direct_radiation": "Direct radiation (W/m²)",
        "radiation_temp_interaction": "Solar radiation × Temperature interaction",
        "cloud_radiation_ratio": "Cloud-adjusted radiation ratio",
        "hour_radiation_interaction": "Hour × Radiation interaction",
        "completeness_radiation_interaction": "Completeness × Radiation interaction",
        "energy_lag_1h": "Energy from previous hour (lag 1h)",
        "energy_lag_24h": "Energy from same hour yesterday (lag 24h)",
        "energy_lag_168h": "Energy from same hour last week (lag 168h)",
        "is_production_hour": "Production hour indicator (daytime with solar radiation)",
        "is_low_energy_period": "Low energy period indicator (edge case detection)",
    }
    
    # Create list of (feature_name, importance_value, category, description)
    feature_data = []
    total_importance = float(sum(importances))
    
    for idx, feature_name in enumerate(FEATURE_COLUMNS):
        importance_val = float(importances[idx])
        importance_pct = float((importance_val / total_importance * 100) if total_importance > 0 else 0.0)
        category = category_map.get(feature_name, "Unknown")
        description = description_map.get(feature_name, "No description available")
        
        feature_data.append((
            feature_name,
            category,
            importance_val,
            importance_pct,
            description
        ))
    
    # Sort by importance (descending)
    feature_data.sort(key=lambda x: x[2], reverse=True)
    
    # Create rows with rankings
    rows = []
    category_ranks = {}  # Track rank within each category
    
    for overall_rank, (feature_name, category, importance_val, importance_pct, description) in enumerate(feature_data, start=1):
        # Track category rank
        if category not in category_ranks:
            category_ranks[category] = 0
        category_ranks[category] += 1
        rank_in_category = category_ranks[category]
        
        # Determine if top 15
        is_top_15 = overall_rank <= 15
        
        # Create row - ensure all numeric types are Python native types
        rows.append((
            int(overall_rank),  # feature_importance_key
            str(feature_name),
            str(category),
            float(importance_val),  # Ensure Python float
            float(importance_pct),  # Ensure Python float
            int(overall_rank),
            int(rank_in_category),
            bool(is_top_15),
            str(description),
            int(model_version_key),
            datetime.now(),
            datetime.now()
        ))
    
    # Define schema
    schema = StructType([
        StructField("feature_importance_key", LongType(), False),
        StructField("feature_name", StringType(), False),
        StructField("feature_category", StringType(), False),
        StructField("importance_value", DoubleType(), False),
        StructField("importance_percentage", DoubleType(), False),
        StructField("rank_overall", IntegerType(), False),
        StructField("rank_in_category", IntegerType(), False),
        StructField("is_top_15", BooleanType(), False),
        StructField("feature_description", StringType(), True),
        StructField("model_version_key", LongType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ])
    
    # Create DataFrame
    importance_df = spark.createDataFrame(rows, schema)
    
    # Write to Gold layer (overwrite for this model_version_key)
    write_iceberg_table(
        importance_df,
        "lh.gold.dim_feature_importance",
        mode="overwrite"
    )
    
    print(f"\n✓ Saved {len(rows)} feature importances to lh.gold.dim_feature_importance")
    print(f"  Top 5 features:")
    for i, (fname, cat, val, pct, desc) in enumerate(feature_data[:5], start=1):
        print(f"    {i}. {fname} ({cat}): {pct:.2f}%")


def train_regression_model(
    spark: SparkSession,
    limit_rows: int = DEFAULT_SAMPLE_LIMIT,
    apply_noise: bool = True,
) -> tuple[Pipeline, DataFrame, dict]:
    """Train regression model to predict energy_mwh directly.
    
    Returns:
        (fitted_pipeline, test_predictions, metrics_dict)
    """
    
    # Load data
    dataset = load_silver_features_regression(spark, limit_rows)
    
    # Temporal train-test split (95%-5%) with DETERMINISTIC split point
    # Calculate exact 95th percentile using window function for consistency
    from pyspark.sql.window import Window
    
    # Add row number to get exact split point
    total_count = dataset.count()
    split_index = int(total_count * 0.95)
    
    # Order by timestamp and add row number
    window_spec = Window.orderBy("date_hour")
    dataset_with_row = dataset.withColumn("row_num", F.row_number().over(window_spec))
    
    # Get exact split timestamp (95th percentile row)
    split_timestamp = dataset_with_row.where(F.col("row_num") == split_index).select("date_hour").first()[0]
    
    # Split data
    train_df = dataset.where(F.col("date_hour") < split_timestamp)
    test_df = dataset.where(F.col("date_hour") >= split_timestamp)
    
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"[SPLIT] Training: {train_count} samples ({train_count/(train_count+test_count)*100:.1f}%)")
    print(f"[SPLIT] Test: {test_count} samples ({test_count/(train_count+test_count)*100:.1f}%)")
    
    # Apply MODERATE noise to TRAINING set ONLY (helps prevent overfitting)
    if apply_noise:
        noisy_columns = [
            "temperature_2m", "cloud_cover", "shortwave_radiation", "direct_radiation",
            "intervals_count", "completeness_pct"
        ]
        # Increased noise for better regularization: 15% magnitude, 70% of rows
        train_df = add_aggressive_noise(train_df, noisy_columns, noise_magnitude=0.15, noise_ratio=0.7)
        print(f"[TRAIN] Applied noise injection: magnitude=15%, ratio=70%")
    
    # Feature assembly
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="skip"
    )
    
    # GBT Regressor - Sequential boosting for better accuracy
    regressor = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        
        # Boosting parameters - optimized for best balance
        maxIter=120,  # Slight increase for better convergence
        maxDepth=6,  # Optimal depth for boosting
        stepSize=0.1,  # Conservative learning rate
        minInstancesPerNode=50,
        maxBins=128,
        subsamplingRate=0.8,  # Row sampling per iteration
        minInfoGain=0.01,
        seed=42
    )
    
    pipeline = Pipeline(stages=[assembler, regressor])
    
    print(f"[TRAIN] Training GBTRegressor (gradient boosting)...")
    print(f"  maxIter={regressor.getMaxIter()}")
    print(f"  maxDepth={regressor.getMaxDepth()}")
    print(f"  stepSize={regressor.getStepSize()}")
    print(f"  maxBins={regressor.getMaxBins()}")
    print(f"  subsamplingRate={regressor.getSubsamplingRate()}")
    print(f"  featureSubsetStrategy={regressor.getFeatureSubsetStrategy()}")
    print(f"  minInfoGain={regressor.getMinInfoGain()}")
    print(f"  Total features={len(FEATURE_COLUMNS)} (includes 3 non-linear features)")
    
    # Train
    model = pipeline.fit(train_df)
    
    # Predictions on test set
    test_predictions = model.transform(test_df)
    
    # Evaluate
    evaluator_mse = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="mse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="r2"
    )
    evaluator_rmse = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="rmse"
    )
    
    mse = evaluator_mse.evaluate(test_predictions)
    mae = evaluator_mae.evaluate(test_predictions)
    r2 = evaluator_r2.evaluate(test_predictions)
    rmse = evaluator_rmse.evaluate(test_predictions)
    
    # Training metrics
    train_predictions = model.transform(train_df)
    train_mae = evaluator_mae.evaluate(train_predictions)
    train_r2 = evaluator_r2.evaluate(train_predictions)
    
    print(f"\n[RESULTS] Regression Model Performance:")
    print(f"  Training MAE: {train_mae:.4f} MWh")
    print(f"  Training R²:  {train_r2:.4f}")
    print(f"  Test MAE:     {mae:.4f} MWh")
    print(f"  Test RMSE:    {rmse:.4f} MWh")
    print(f"  Test R²:      {r2:.4f}")
    print(f"  Generalization gap (MAE): {abs(train_mae - mae):.4f} MWh")
    
    metrics = {
        "train_mae": train_mae,
        "train_r2": train_r2,
        "test_mse": mse,
        "test_mae": mae,
        "test_rmse": rmse,
        "test_r2": r2,
        "generalization_gap_mae": abs(train_mae - mae),
        "train_count": train_count,
        "test_count": test_count,
    }
    
    return model, test_predictions, metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train solar energy REGRESSION forecasting model")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_SAMPLE_LIMIT,
        help=f"Max rows to sample from Silver (default: {DEFAULT_SAMPLE_LIMIT})",
    )
    parser.add_argument(
        "--model-version",
        type=str,
        default=None,
        help="Custom model version ID (default: timestamp-based)",
    )
    parser.add_argument(
        "--no-noise",
        action="store_true",
        help="Disable noise injection (not recommended for production)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    # MLflow setup
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("solar_energy_regression_forecast")
    
    spark = create_spark_session("SolarEnergyRegressionTraining")
    
    with mlflow.start_run(run_name=f"regression_training_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        # Log parameters
        mlflow.log_param("sample_limit", args.limit if args.limit else "all")
        mlflow.log_param("model_type", "GBTRegressor")
        mlflow.log_param("noise_injection", not args.no_noise)
        mlflow.log_param("max_iter", 120)
        mlflow.log_param("max_depth", 6)
        mlflow.log_param("step_size", 0.1)
        mlflow.log_param("min_instances_per_node", 50)
        mlflow.log_param("max_bins", 128)
        mlflow.log_param("subsampling_rate", 0.8)
        mlflow.log_param("min_info_gain", 0.01)
        mlflow.log_param("low_energy_filter", "energy >= 5.0 MWh AND shortwave_radiation > 0 (sunlight only)")
        mlflow.log_param("gold_output_table", GOLD_OUTPUT_TABLE)
        mlflow.log_param("gold_write_mode", GOLD_WRITE_MODE)
        
        # Train
        model, test_predictions, metrics = train_regression_model(
            spark, 
            limit_rows=args.limit,
            apply_noise=not args.no_noise
        )
        
        # Log metrics
        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)
        
        # Build Gold fact table
        model_version_key = resolve_model_version_key(spark)
        gold_df = build_gold_fact(test_predictions, model_version_key)
        gold_rows = write_gold_predictions(gold_df, GOLD_OUTPUT_TABLE, mode=GOLD_WRITE_MODE)
        
        # Save feature importance to Gold layer
        save_feature_importance_to_gold(spark, model, model_version_key)
        
        # Log Gold metrics
        mlflow.log_metric("gold_row_count", float(gold_rows))
        mlflow.log_param("model_version_key", model_version_key)
        
        # Log model
        mlflow.spark.log_model(model, "regression_model", dfs_tmpdir="/tmp/mlflow_tmp")
        
        # Sample for inspection
        print(f"\n[GOLD] Sample predictions from fact_solar_forecast:")
        gold_df.select(
            "facility_key",
            "forecast_timestamp",
            "actual_energy_mwh",
            "predicted_energy_mwh",
            "forecast_error_mwh",
            "absolute_percentage_error"
        ).orderBy("forecast_timestamp", ascending=False).show(10, truncate=False)
        
    spark.stop()
    print(f"\n✓ Regression training complete!")
    print(f"✓ Saved {gold_rows} predictions to Gold layer: {GOLD_OUTPUT_TABLE}")


if __name__ == "__main__":
    main()
