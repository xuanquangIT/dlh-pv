"""Training script for solar energy REGRESSION forecasting model.

This is the REGRESSION component of our Hybrid ML approach:
- Classification Model (train_forecast_model.py): Anomaly detection & data quality
- Regression Model (THIS FILE): True forecasting - predict actual energy values

Trains a RandomForest Regressor with PARALLEL training to predict energy_mwh.
Optimized for high performance with distributed computing support.

Performance optimizations:
- Parallel tree training across all available cores
- Optimized data partitioning for distributed processing
- Efficient caching and memory management
- Configurable parallelism via environment variables
"""

from __future__ import annotations

import argparse
import multiprocessing
import os
from datetime import datetime
from typing import Optional

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

from pv_lakehouse.etl.utils.spark_utils import create_spark_session, write_iceberg_table

# ============================================================================
# CONFIGURATION - Can be overridden via environment variables
# ============================================================================
DEFAULT_SAMPLE_LIMIT = 0  # 0 = use all data
SILVER_HOURLY_ENERGY_TABLE = "lh.silver.clean_hourly_energy"
SILVER_HOURLY_WEATHER_TABLE = "lh.silver.clean_hourly_weather"
DIM_FACILITY_TABLE = "lh.gold.dim_facility"
DIM_MODEL_VERSION_TABLE = "lh.gold.dim_model_version"
GOLD_OUTPUT_TABLE = "lh.gold.fact_solar_forecast_regression"
GOLD_WRITE_MODE = "overwrite"

# Trade-off: fewer trees + shallower depth = fits in memory, still good accuracy
NUM_TREES = int(os.getenv("RF_NUM_TREES", "50"))       
MAX_DEPTH = int(os.getenv("RF_MAX_DEPTH", "10"))       
MIN_INSTANCES_PER_NODE = int(os.getenv("RF_MIN_INSTANCES", "50"))  
MAX_BINS = int(os.getenv("RF_MAX_BINS", "64"))         
SUBSAMPLING_RATE = float(os.getenv("RF_SUBSAMPLING_RATE", "0.7"))  

# Feature set
FEATURE_COLUMNS = [
    # Energy data quality features
    "intervals_count", "completeness_pct",
    # Temporal features (cyclical encoded)
    "hour_of_day", "day_of_week", "month", "is_weekend",
    "hour_sin", "hour_cos", "month_sin", "month_cos",
    # Weather features
    "temperature_2m", "cloud_cover", "shortwave_radiation", "direct_radiation",
    # Interaction features
    "radiation_temp_interaction", "cloud_radiation_ratio",
    "hour_radiation_interaction", "completeness_radiation_interaction",
    # LAG features (time series)
    "energy_lag_1h", "energy_lag_24h", "energy_lag_168h",
]
TARGET_COLUMN = "energy_mwh"


def get_optimal_parallelism() -> int:
    """Calculate optimal parallelism based on available resources."""
    cores = multiprocessing.cpu_count()
    return max(cores * 2, 16)


def add_noise_for_regularization(
    df: DataFrame, 
    columns: list[str], 
    magnitude: float = 0.15,
    ratio: float = 0.5
) -> DataFrame:
    """Apply noise injection for regularization (training data only)."""
    if magnitude <= 0 or ratio <= 0:
        return df
    
    for col_name in columns:
        if col_name not in df.columns:
            continue
        noise = (F.rand(seed=42) * 2 - 1) * magnitude
        apply_mask = F.rand(seed=43) < ratio
        df = df.withColumn(
            col_name,
            F.when(apply_mask, F.col(col_name) * (1 + noise)).otherwise(F.col(col_name))
        )
    
    print(f"[NOISE] Applied ±{magnitude*100:.0f}% noise to {len(columns)} features on {ratio*100:.0f}% of rows")
    return df


def load_and_prepare_data(spark: SparkSession, limit_rows: int = 0) -> DataFrame:
    """Load and prepare data with optimized partitioning for parallel processing."""
    
    parallelism = get_optimal_parallelism()
    print(f"[LOAD] Using parallelism={parallelism} based on {multiprocessing.cpu_count()} cores")
    
    # Load energy data
    try:
        silver_energy = spark.table(SILVER_HOURLY_ENERGY_TABLE)
    except AnalysisException as exc:
        raise RuntimeError(f"Silver table '{SILVER_HOURLY_ENERGY_TABLE}' unavailable.") from exc
    
    # Load weather data
    try:
        silver_weather = spark.table(SILVER_HOURLY_WEATHER_TABLE)
    except AnalysisException as exc:
        raise RuntimeError("Weather data REQUIRED for regression forecasting.") from exc
    
    # Prepare energy features - single chain for efficiency
    energy_df = (
        silver_energy
        .select(
            "facility_code", "date_hour",
            F.col("energy_mwh").cast("double").alias("energy_mwh"),
            F.col("intervals_count").cast("double").alias("intervals_count"),
            F.col("completeness_pct").cast("double").alias("completeness_pct"),
        )
        .where(F.col("energy_mwh").isNotNull())
        .where(F.col("intervals_count").isNotNull())
        .where(F.col("completeness_pct").isNotNull())
        # Add temporal features
        .withColumn("hour_of_day", F.hour("date_hour").cast("double"))
        .withColumn("day_of_week", F.dayofweek("date_hour").cast("double"))
        .withColumn("month", F.month("date_hour").cast("double"))
        .withColumn("is_weekend", F.when(F.dayofweek("date_hour").isin([1, 7]), 1.0).otherwise(0.0))
        .withColumn("hour_sin", F.sin(2 * 3.14159 * F.col("hour_of_day") / 24.0))
        .withColumn("hour_cos", F.cos(2 * 3.14159 * F.col("hour_of_day") / 24.0))
        .withColumn("month_sin", F.sin(2 * 3.14159 * F.col("month") / 12.0))
        .withColumn("month_cos", F.cos(2 * 3.14159 * F.col("month") / 12.0))
    )
    
    # Prepare weather features
    weather_df = (
        silver_weather
        .select(
            F.col("facility_code").alias("w_facility_code"),
            F.col("date_hour").alias("w_date_hour"),
            F.col("temperature_2m").cast("double").alias("temperature_2m"),
            F.col("cloud_cover").cast("double").alias("cloud_cover"),
            F.col("shortwave_radiation").cast("double").alias("shortwave_radiation"),
            F.col("direct_radiation").cast("double").alias("direct_radiation"),
        )
        .where(F.col("temperature_2m").isNotNull())
        .where(F.col("shortwave_radiation").isNotNull())
    )
    
    # Join energy and weather
    joined_df = (
        energy_df
        .join(
            weather_df,
            (energy_df.facility_code == weather_df.w_facility_code) &
            (energy_df.date_hour == weather_df.w_date_hour),
            how="inner"
        )
        .drop("w_facility_code", "w_date_hour")
    )
    
    # Add LAG features using window functions
    window_spec = Window.partitionBy("facility_code").orderBy("date_hour")
    joined_df = (
        joined_df
        .withColumn("energy_lag_1h", F.lag("energy_mwh", 1).over(window_spec))
        .withColumn("energy_lag_24h", F.lag("energy_mwh", 24).over(window_spec))
        .withColumn("energy_lag_168h", F.lag("energy_mwh", 168).over(window_spec))
        .fillna(0.0, subset=["energy_lag_1h", "energy_lag_24h", "energy_lag_168h"])
    )
    
    # Add interaction features
    result_df = (
        joined_df
        .withColumn("radiation_temp_interaction", 
                   F.col("shortwave_radiation") * F.col("temperature_2m") / 1000.0)
        .withColumn("cloud_radiation_ratio",
                   F.when(F.col("shortwave_radiation") > 0,
                         (100 - F.col("cloud_cover")) * F.col("shortwave_radiation") / 100.0)
                   .otherwise(0.0))
        .withColumn("hour_radiation_interaction",
                   F.col("hour_of_day") * F.col("shortwave_radiation") / 1000.0)
        .withColumn("completeness_radiation_interaction",
                   F.col("completeness_pct") * F.col("shortwave_radiation") / 10000.0)
    )
    
    # Apply limit if specified
    if limit_rows and limit_rows > 0:
        result_df = result_df.orderBy(F.col("date_hour").desc()).limit(limit_rows)
        print(f"[LOAD] Limiting to {limit_rows} rows")
    else:
        print(f"[LOAD] Using ALL available data")
    
    # Repartition for optimal parallel processing
    result_df = result_df.repartition(parallelism)
    
    # Cache and materialize
    result_df = result_df.cache()
    row_count = result_df.count()
    
    if row_count < 100:
        result_df.unpersist()
        raise RuntimeError(f"Not enough data: {row_count} rows (minimum 100 required)")
    
    # Print statistics
    stats = result_df.agg(
        F.mean("energy_mwh").alias("mean"),
        F.stddev("energy_mwh").alias("std"),
        F.min("energy_mwh").alias("min"),
        F.max("energy_mwh").alias("max"),
    ).collect()[0]
    
    print(f"[LOAD] Loaded {row_count:,} samples")
    print(f"[LOAD] Energy stats: mean={stats['mean']:.2f}, std={stats['std']:.2f}, min={stats['min']:.2f}, max={stats['max']:.2f} MWh")
    
    return result_df


def load_facility_dimension(spark: SparkSession) -> Optional[DataFrame]:
    """Load facility dimension for Gold join."""
    try:
        return spark.table(DIM_FACILITY_TABLE).select("facility_code", "facility_key")
    except AnalysisException:
        print(f"[WARN] Dimension table '{DIM_FACILITY_TABLE}' not found")
        return None


def resolve_model_version_key(spark: SparkSession) -> int:
    """Get model version key from dimension table."""
    try:
        dim_df = spark.table(DIM_MODEL_VERSION_TABLE)
        max_key = dim_df.agg(F.max("model_version_key")).collect()[0][0]
        return int(max_key) if max_key is not None else 1
    except Exception:
        return 1


def build_gold_fact(predictions: DataFrame, model_version_key: int, r2_score: float) -> DataFrame:
    """Transform regression predictions into Gold fact schema."""
    
    facility_dim = load_facility_dimension(predictions.sparkSession)
    window = Window.orderBy("facility_code", "date_hour")
    
    enriched = (
        predictions
        .withColumn("forecast_id", F.row_number().over(window))
        .withColumn("date_key", F.date_format("date_hour", "yyyyMMdd").cast("int"))
        .withColumn("time_key", (F.hour("date_hour") * 100 + F.minute("date_hour")).cast("int"))
        .withColumn("model_version_key", F.lit(int(model_version_key)))
        .withColumn("weather_condition_key", F.lit(None).cast("int"))
        .withColumn("actual_energy_mwh", F.col("energy_mwh"))
        .withColumn("predicted_energy_mwh", F.col("prediction"))
        .withColumn("forecast_error_mwh", F.col("actual_energy_mwh") - F.col("predicted_energy_mwh"))
        .withColumn("absolute_percentage_error",
            F.when(F.col("actual_energy_mwh") == 0, 0.0)
            .otherwise(F.abs(F.col("forecast_error_mwh") / F.col("actual_energy_mwh")) * 100.0))
        .withColumn("mae_metric", F.abs(F.col("forecast_error_mwh")))
        .withColumn("rmse_metric", F.abs(F.col("forecast_error_mwh")))
        .withColumn("r2_score", F.lit(r2_score))
        .withColumn("forecast_timestamp", F.col("date_hour"))
        .withColumn("created_at", F.current_timestamp())
    )
    
    if facility_dim is not None:
        enriched = enriched.join(facility_dim, on="facility_code", how="left")
    else:
        enriched = enriched.withColumn("facility_key", F.lit(None).cast("int"))
    
    return enriched.select(
        "forecast_id", "date_key", "time_key", "facility_key",
        "weather_condition_key", "model_version_key",
        "actual_energy_mwh", "predicted_energy_mwh", "forecast_error_mwh",
        "absolute_percentage_error", "mae_metric", "rmse_metric", "r2_score",
        "forecast_timestamp", "created_at",
    )


def train_model(
    spark: SparkSession,
    limit_rows: int = 0,
    apply_noise: bool = True,
    num_trees: int = NUM_TREES,
    max_depth: int = MAX_DEPTH,
) -> tuple[Pipeline, DataFrame, dict]:
    """Train RandomForest regression model with parallel optimization.
    
    Returns: (fitted_model, test_predictions, metrics_dict)
    """
    
    # Load data
    dataset = load_and_prepare_data(spark, limit_rows)
    
    # Temporal train-test split (95%-5%)
    total_count = dataset.count()
    split_index = int(total_count * 0.95)
    
    window_spec = Window.orderBy("date_hour")
    dataset_with_row = dataset.withColumn("row_num", F.row_number().over(window_spec))
    split_row = dataset_with_row.where(F.col("row_num") == split_index).select("date_hour").first()
    
    if split_row is None:
        raise RuntimeError("Could not determine split point")
    split_timestamp = split_row[0]
    
    train_df = dataset.where(F.col("date_hour") < split_timestamp).cache()
    test_df = dataset.where(F.col("date_hour") >= split_timestamp).cache()
    
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"[SPLIT] Training: {train_count:,} ({train_count/(train_count+test_count)*100:.1f}%)")
    print(f"[SPLIT] Test: {test_count:,} ({test_count/(train_count+test_count)*100:.1f}%)")
    
    # Apply noise for regularization
    if apply_noise:
        noisy_cols = ["temperature_2m", "cloud_cover", "shortwave_radiation", 
                      "direct_radiation", "intervals_count", "completeness_pct"]
        train_df = add_noise_for_regularization(train_df, noisy_cols, magnitude=0.15, ratio=0.5)
    
    # Build pipeline
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="skip"
    )
    
    # RandomForest with PARALLEL training
    regressor = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        numTrees=num_trees,
        maxDepth=max_depth,
        minInstancesPerNode=MIN_INSTANCES_PER_NODE,
        maxBins=MAX_BINS,
        subsamplingRate=SUBSAMPLING_RATE,
        featureSubsetStrategy="sqrt",
        minInfoGain=0.001,
        seed=42,
    )
    
    pipeline = Pipeline(stages=[assembler, regressor])
    
    print(f"\n[TRAIN] RandomForest Configuration:")
    print(f"  numTrees={num_trees} (parallel training)")
    print(f"  maxDepth={max_depth}")
    print(f"  minInstancesPerNode={MIN_INSTANCES_PER_NODE}")
    print(f"  maxBins={MAX_BINS}")
    print(f"  subsamplingRate={SUBSAMPLING_RATE}")
    
    # Train model
    print(f"\n[TRAIN] Training started...")
    import time
    start_time = time.time()
    
    model = pipeline.fit(train_df)
    
    train_time = time.time() - start_time
    print(f"[TRAIN] Training completed in {train_time:.1f} seconds")
    
    # Evaluate
    test_predictions = model.transform(test_df)
    train_predictions = model.transform(train_df)
    
    evaluators = {
        "mse": RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="mse"),
        "mae": RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="mae"),
        "rmse": RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="rmse"),
        "r2": RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="r2"),
    }
    
    test_mae = evaluators["mae"].evaluate(test_predictions)
    test_rmse = evaluators["rmse"].evaluate(test_predictions)
    test_r2 = evaluators["r2"].evaluate(test_predictions)
    test_mse = evaluators["mse"].evaluate(test_predictions)
    train_mae = evaluators["mae"].evaluate(train_predictions)
    train_r2 = evaluators["r2"].evaluate(train_predictions)
    
    print(f"\n[RESULTS] Model Performance:")
    print(f"  Train MAE: {train_mae:.4f} MWh | R²: {train_r2:.4f}")
    print(f"  Test  MAE: {test_mae:.4f} MWh | R²: {test_r2:.4f} | RMSE: {test_rmse:.4f}")
    print(f"  Generalization gap: {abs(train_mae - test_mae):.4f} MWh")
    
    metrics = {
        "train_mae": train_mae, "train_r2": train_r2,
        "test_mse": test_mse, "test_mae": test_mae, 
        "test_rmse": test_rmse, "test_r2": test_r2,
        "generalization_gap": abs(train_mae - test_mae),
        "train_count": train_count, "test_count": test_count,
        "training_time_seconds": train_time,
    }
    
    # Cleanup
    train_df.unpersist()
    
    return model, test_predictions, metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train solar energy regression model")
    parser.add_argument("--limit", type=int, default=DEFAULT_SAMPLE_LIMIT,
                        help="Max rows (0=all data)")
    parser.add_argument("--no-noise", action="store_true",
                        help="Disable noise injection")
    parser.add_argument("--num-trees", type=int, default=NUM_TREES,
                        help=f"Number of trees (default: {NUM_TREES})")
    parser.add_argument("--max-depth", type=int, default=MAX_DEPTH,
                        help=f"Max tree depth (default: {MAX_DEPTH})")
    return parser.parse_args()


def main():
    args = parse_args()
    
    # MLflow setup
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("solar_energy_regression_forecast")
    
    # Create Spark session
    spark = create_spark_session("SolarEnergyRegressionTraining")
    
    run_name = f"rf_regression_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    with mlflow.start_run(run_name=run_name):
        # Log parameters
        mlflow.log_param("sample_limit", args.limit if args.limit else "all")
        mlflow.log_param("model_type", "RandomForestRegressor")
        mlflow.log_param("noise_injection", not args.no_noise)
        mlflow.log_param("num_trees", args.num_trees)
        mlflow.log_param("max_depth", args.max_depth)
        mlflow.log_param("min_instances_per_node", MIN_INSTANCES_PER_NODE)
        mlflow.log_param("max_bins", MAX_BINS)
        mlflow.log_param("subsampling_rate", SUBSAMPLING_RATE)
        
        # Train
        model, test_predictions, metrics = train_model(
            spark, 
            limit_rows=args.limit,
            apply_noise=not args.no_noise,
            num_trees=args.num_trees,
            max_depth=args.max_depth,
        )
        
        # Log metrics
        for name, value in metrics.items():
            mlflow.log_metric(name, float(value))
        
        # Build and save Gold fact table
        model_version_key = resolve_model_version_key(spark)
        gold_df = build_gold_fact(test_predictions, model_version_key, metrics["test_r2"])
        
        gold_rows = gold_df.count()
        write_iceberg_table(gold_df, GOLD_OUTPUT_TABLE, mode=GOLD_WRITE_MODE)
        
        mlflow.log_metric("gold_row_count", float(gold_rows))
        mlflow.log_param("model_version_key", model_version_key)
        
        # Log model
        try:
            mlflow.spark.log_model(model, "model", dfs_tmpdir="/tmp/mlflow_tmp")
        except Exception as e:
            print(f"[WARN] Failed to log model: {e}")
        
        print(f"\n{'='*60}")
        print(f"✓ Training complete!")
        print(f"✓ Saved {gold_rows:,} predictions to {GOLD_OUTPUT_TABLE}")
        print(f"✓ Training time: {metrics['training_time_seconds']:.1f}s")
        print(f"✓ Test MAE: {metrics['test_mae']:.4f} MWh")
        print(f"✓ Test R²: {metrics['test_r2']:.4f}")
        print(f"{'='*60}")
    
    spark.stop()


if __name__ == "__main__":
    main()
