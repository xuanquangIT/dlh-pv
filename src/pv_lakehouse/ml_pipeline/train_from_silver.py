from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.utils import AnalysisException

from pv_lakehouse.etl.utils.spark_utils import create_spark_session
from pv_lakehouse.ml_pipeline.config import MLConfig
from pv_lakehouse.ml_pipeline.features.engineering import (
    prepare_features,
    select_training_features,
    validate_features
)
from pv_lakehouse.ml_pipeline.models.regressor import create_model
from pv_lakehouse.ml_pipeline.evaluation.metrics import evaluate_model, log_metrics
from pv_lakehouse.ml_pipeline.tracking.mlflow_logger import create_tracker

logger = logging.getLogger(__name__)


class CachedDataFrame:
    """Context manager for DataFrame caching with automatic cleanup"""
    def __init__(self, df: DataFrame):
        self.df = df.cache()
    
    def __enter__(self):
        return self.df
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.df.unpersist()


def get_mlflow_uri() -> str:
    """Get MLflow tracking URI with validation"""
    uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not uri:
        raise ValueError("MLFLOW_TRACKING_URI environment variable must be set")
    return uri


def get_spark_config() -> dict:
    """Get Spark configuration from environment or config file"""
    return {
        "spark.driver.memory": os.environ.get("SPARK_DRIVER_MEMORY", "4g"),
        "spark.executor.memory": os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"),
        "spark.sql.shuffle.partitions": os.environ.get("SPARK_SHUFFLE_PARTITIONS", "20"),
    }


class PipelineConfig:
    """Pipeline configuration constants"""
    def __init__(self, config_dict: dict = None):
        config = config_dict or {}
        self.OPTIMAL_PARTITIONS = config.get('optimal_partitions', 4)  # Balance between parallelism and overhead
        self.TOP_N_FEATURES = config.get('top_n_features', 10)  # Number of top features to display
        
        # Data source tables from config
        tables = config.get('tables', {})
        self.SILVER_ENERGY = tables.get('energy', "lh.silver.clean_hourly_energy")
        self.SILVER_WEATHER = tables.get('weather', "lh.silver.clean_hourly_weather")
        self.SILVER_AQ = tables.get('air_quality', "lh.silver.clean_hourly_air_quality")


def _split_data_temporal(df: DataFrame, train_ratio: float, val_ratio: float):
    from pyspark.sql.window import Window
    
    df_sorted = df.orderBy("date_hour")
    total_count = df_sorted.count()
    
    train_end = int(total_count * train_ratio)
    val_end = int(total_count * (train_ratio + val_ratio))
    
    window_spec = Window.orderBy("date_hour")
    df_indexed = df_sorted.withColumn("row_num", F.row_number().over(window_spec))
    
    train_df = df_indexed.filter(F.col("row_num") <= train_end).drop("row_num")
    val_df = df_indexed.filter(
        (F.col("row_num") > train_end) & (F.col("row_num") <= val_end)
    ).drop("row_num")
    test_df = df_indexed.filter(F.col("row_num") > val_end).drop("row_num")
    
    return train_df, val_df, test_df


def _split_data_random(df: DataFrame, train_ratio: float, val_ratio: float, 
                       test_ratio: float, seed: int):
    return df.randomSplit([train_ratio, val_ratio, test_ratio], seed=seed)


def _load_and_prepare_energy(spark: SparkSession, table_name: str = "lh.silver.clean_hourly_energy") -> DataFrame:
    try:
        return spark.table(table_name).select(
            "facility_code", "date_hour",
            F.col("energy_mwh").cast("double"),
            F.col("intervals_count").cast("double"),
            F.col("completeness_pct").cast("double"),
        )
    except AnalysisException as e:
        logger.error(f"Failed to load table {table_name}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading energy data: {e}", exc_info=True)
        raise RuntimeError(f"Failed to load energy data: {e}") from e


def _load_and_prepare_weather(spark: SparkSession, table_name: str = "lh.silver.clean_hourly_weather") -> DataFrame:
    
    try:
        return spark.table(table_name).select(
            "facility_code", "date_hour",
            F.col("temperature_2m").cast("double"),
            F.col("cloud_cover").cast("double"),
            F.col("shortwave_radiation").cast("double"),
            F.col("direct_radiation").cast("double"),
            F.col("diffuse_radiation").cast("double"),
            F.col("precipitation").cast("double"),
            F.col("wind_speed_10m").cast("double"),
        )
    except AnalysisException as e:
        logger.error(f"Failed to load table {table_name}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading weather data: {e}", exc_info=True)
        raise RuntimeError(f"Failed to load weather data: {e}") from e


def _load_and_prepare_air_quality(spark: SparkSession, table_name: str = "lh.silver.clean_hourly_air_quality") -> DataFrame:
    
    try:
        return spark.table(table_name).select(
            "facility_code", "date_hour",
            F.col("pm2_5").cast("double"),
            F.col("pm10").cast("double"),
            F.col("ozone").cast("double"),
            F.col("nitrogen_dioxide").cast("double"),
        )
    except AnalysisException as e:
        logger.error(f"Failed to load table {table_name}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading air quality data: {e}", exc_info=True)
        raise RuntimeError(f"Failed to load air quality data: {e}") from e


def _join_silver_tables(energy_df: DataFrame, weather_df: DataFrame, air_df: DataFrame) -> DataFrame:
    
    df = energy_df.join(weather_df, on=["facility_code", "date_hour"], how="left")
    return df.join(air_df, on=["facility_code", "date_hour"], how="left")


def load_silver_data(spark: SparkSession, sample_limit: int = None, pipeline_config: PipelineConfig = None) -> DataFrame:
    
    if pipeline_config is None:
        pipeline_config = PipelineConfig()
        
    logger.info("Loading data from Silver layer...")
    
    # Load tables
    energy_df = _load_and_prepare_energy(spark, pipeline_config.SILVER_ENERGY)
    weather_df = _load_and_prepare_weather(spark, pipeline_config.SILVER_WEATHER)
    air_df = _load_and_prepare_air_quality(spark, pipeline_config.SILVER_AQ)
    
    # Join and filter
    df = _join_silver_tables(energy_df, weather_df, air_df)
    df = df.where(F.col("energy_mwh").isNotNull())
    df = df.orderBy(F.col("date_hour").desc())
    
    # Apply sampling if requested
    if sample_limit and sample_limit > 0:
        df = df.limit(sample_limit)
        logger.info(f"Limited to {sample_limit} rows")
    
    # Optimize: reduce partitions and cache for better performance
    df = df.coalesce(pipeline_config.OPTIMAL_PARTITIONS).cache()
    
    total = df.count()
    logger.info(f"Loaded {total} rows from Silver layer")
    
    # Note: Cached DataFrame should be unpersisted by caller to prevent memory leaks
    return df


def train_pipeline(config: MLConfig, spark: SparkSession, sample_limit: int = None):
    
    # Initialize pipeline configuration
    pipeline_config = PipelineConfig()
    
    # 1. Load data
    df = load_silver_data(spark, sample_limit, pipeline_config)
    
    try:
        # 2. Feature engineering
        logger.info("\n=== Feature Engineering ===")
        df = prepare_features(df, config.features, include_lag=True, include_air_quality=False)
        df = select_training_features(df, config.features, include_air_quality=False)
        df = validate_features(df, min_rows=config.training.min_rows_required)
        
        # 3. Split data
        logger.info("\n=== Data Split ===")
        train_ratio = config.training.train_ratio
        val_ratio = config.training.validation_ratio
        test_ratio = config.training.test_ratio
        
        if config.training.data_split_method == "temporal":
            logger.info("Using temporal split (train on older data, test on recent)")
            train_df, val_df, test_df = _split_data_temporal(df, train_ratio, val_ratio)
        else:
            logger.info("Using random split")
            train_df, val_df, test_df = _split_data_random(
                df, train_ratio, val_ratio, test_ratio, config.training.random_seed
            )
        
        train_count = train_df.count()
        val_count = val_df.count()
        test_count = test_df.count()
        logger.info(f"Train: {train_count}, Val: {val_count}, Test: {test_count}")
        
        # 4. Train model
        logger.info("\n=== Model Training ===")
        try:
            if not config.model:
                raise ValueError("Model configuration is missing")
            
            model = create_model(config.model)
            
            feature_cols = config.features.get_all_features(include_air_quality=False)
            if not feature_cols or len(feature_cols) == 0:
                raise ValueError("No feature columns found")
            
            target_col = config.features.target_column
            if not target_col:
                raise ValueError("Target column is not specified")
            
            # Validate that features exist in DataFrame
            missing_features = set(feature_cols) - set(train_df.columns)
            if missing_features:
                raise ValueError(f"Missing features in training data: {missing_features}")
            
            if target_col not in train_df.columns:
                raise ValueError(f"Target column '{target_col}' not found in training data")
            
            logger.info(f"Training model with {len(feature_cols)} features")
            trained_model = model.train(train_df, feature_cols, target_col)
            
            if not trained_model:
                raise ValueError("Model training failed - no model returned")
            
        except Exception as e:
            logger.error(f"Model training failed: {e}", exc_info=True)
            raise
        
        # 5. Evaluate on test set
        logger.info("\n=== Model Evaluation ===")
        predictions = model.predict(test_df)
        metrics = evaluate_model(predictions, target_col, "prediction")
        log_metrics(metrics)
        
        # 6. Feature importance
        feature_importance = model.get_feature_importance()
        top_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:pipeline_config.TOP_N_FEATURES]
        logger.info(f"\nTop {pipeline_config.TOP_N_FEATURES} Features:")
        for feat, imp in top_features:
            logger.info(f"  {feat:40s}: {imp:.4f}")
        
        # 7. Track experiment
        logger.info("\n=== Experiment Tracking ===")
        tracker = create_tracker(
            tracking_uri=get_mlflow_uri(),
            experiment_name=config.mlflow.experiment_name,
            use_mlflow=True
        )
        
        try:
            with tracker.run_context(run_name=f"{config.mlflow.run_name_prefix}_{config.model.model_type}"):
                try:
                    # Log configuration
                    tracker.log_params({
                        "model_type": config.model.model_type,
                        "max_depth": config.model.max_depth,
                        "sample_limit": sample_limit or "all",
                        "train_ratio": train_ratio,
                        "num_features": len(feature_cols),
                    })
                    logger.info("Parameters logged successfully")
                except Exception as e:
                    logger.warning(f"Failed to log parameters: {e}")
                
                try:
                    # Log metrics
                    tracker.log_metrics(metrics)
                    logger.info("Metrics logged successfully")
                except Exception as e:
                    logger.warning(f"Failed to log metrics: {e}")
                
                # Log model (if enabled)
                if config.mlflow.log_model:
                    try:
                        tracker.log_model(trained_model, config.mlflow.artifact_path)
                        logger.info("Model logged successfully")
                    except Exception as e:
                        logger.warning(f"Failed to log model: {e}")
                
                try:
                    # Log tags
                    tracker.set_tags({
                        "pipeline_version": "refactored_v1",
                        "feature_engineering": "modular",
                    })
                    logger.info("Tags set successfully")
                except Exception as e:
                    logger.warning(f"Failed to set tags: {e}")
                    
        except Exception as e:
                logger.error(f"MLflow tracking failed: {e}")
                # Continue execution even if tracking fails
        
        logger.info("\n=== Training Complete ===")
        return model, metrics
    finally:
        # Cleanup cached DataFrame to prevent memory leaks
        if df.is_cached:
            df.unpersist()
            logger.debug("DataFrame cache cleared")


def parse_args():
    parser = argparse.ArgumentParser(description="Train ML model from Silver layer")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of rows for testing (default: use all data)"
    )
    parser.add_argument(
        "--features-config",
        default="config/ml_features.yaml",
        help="Path to features YAML config"
    )
    parser.add_argument(
        "--hyperparams-config",
        default="config/ml_hyperparams.yaml",
        help="Path to hyperparameters YAML config"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Resolve config paths
    project_root = Path(__file__).parent.parent.parent.parent
    features_path = project_root / args.features_config
    hyperparams_path = project_root / args.hyperparams_config
    
    logger.info("Loading configuration from:")
    logger.info(f"  Features: {features_path}")
    logger.info(f"  Hyperparams: {hyperparams_path}")
    
    # Load configuration
    config = MLConfig.from_yaml(str(features_path), str(hyperparams_path))
    
    # Create Spark session
    spark = create_spark_session(
        "pv_ml_training",
        extra_conf=get_spark_config()
    )
    
    try:
        # Run training pipeline
        model, metrics = train_pipeline(config, spark, sample_limit=args.limit)
        
        if model is None or metrics is None:
            raise ValueError("Training pipeline returned invalid results")
        
        logger.info("\nTraining pipeline completed successfully")
        primary_metric_value = metrics.get(config.training.primary_metric, 'N/A')
        logger.info(f"Primary metric ({config.training.primary_metric}): {primary_metric_value}")
        
    except Exception as e:
        logger.error(f"Training pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

