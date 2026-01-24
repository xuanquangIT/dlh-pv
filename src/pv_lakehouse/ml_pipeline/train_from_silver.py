from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, functions as F

from pv_lakehouse.etl.utils.spark_utils import create_spark_session
from pv_lakehouse.ml_pipeline.config import MLConfig
from pv_lakehouse.ml_pipeline.features.engineering import (
    prepare_features,
    select_training_features,
    validate_features
)
from pv_lakehouse.ml_pipeline.models.regressor import create_model
from pv_lakehouse.ml_pipeline.evaluation.metrics import evaluate_model, print_metrics
from pv_lakehouse.ml_pipeline.tracking.mlflow_logger import create_tracker

logger = logging.getLogger(__name__)


# Constants
SILVER_ENERGY = "lh.silver.clean_hourly_energy"
SILVER_WEATHER = "lh.silver.clean_hourly_weather"
SILVER_AQ = "lh.silver.clean_hourly_air_quality"

OPTIMAL_PARTITIONS = 4  # Balance between parallelism and overhead
TOP_N_FEATURES = 10  # Number of top features to display
DEFAULT_MLFLOW_URI = "http://mlflow:5000"


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


def _load_and_prepare_energy(spark: SparkSession) -> DataFrame:
	try:
		return spark.table(SILVER_ENERGY).select(
			"facility_code", "date_hour",
			F.col("energy_mwh").cast("double"),
			F.col("intervals_count").cast("double"),
			F.col("completeness_pct").cast("double"),
		)
	except Exception as e:
		logger.error(f"Failed to load table {SILVER_ENERGY}: {e}")
		raise


def _load_and_prepare_weather(spark: SparkSession) -> DataFrame:
	
	try:
		return spark.table(SILVER_WEATHER).select(
			"facility_code", "date_hour",
			F.col("temperature_2m").cast("double"),
			F.col("cloud_cover").cast("double"),
			F.col("shortwave_radiation").cast("double"),
			F.col("direct_radiation").cast("double"),
			F.col("diffuse_radiation").cast("double"),
			F.col("precipitation").cast("double"),
			F.col("wind_speed_10m").cast("double"),
		)
	except Exception as e:
		logger.error(f"Failed to load table {SILVER_WEATHER}: {e}")
		raise


def _load_and_prepare_air_quality(spark: SparkSession) -> DataFrame:
	
	try:
		return spark.table(SILVER_AQ).select(
			"facility_code", "date_hour",
			F.col("pm2_5").cast("double"),
			F.col("pm10").cast("double"),
			F.col("ozone").cast("double"),
			F.col("nitrogen_dioxide").cast("double"),
		)
	except Exception as e:
		logger.error(f"Failed to load table {SILVER_AQ}: {e}")
		raise


def _join_silver_tables(energy_df: DataFrame, weather_df: DataFrame, air_df: DataFrame) -> DataFrame:
	
	df = energy_df.join(weather_df, on=["facility_code", "date_hour"], how="left")
	return df.join(air_df, on=["facility_code", "date_hour"], how="left")


def load_silver_data(spark: SparkSession, sample_limit: int = None) -> DataFrame:
	
	logger.info("Loading data from Silver layer...")
	
	# Load tables
	energy_df = _load_and_prepare_energy(spark)
	weather_df = _load_and_prepare_weather(spark)
	air_df = _load_and_prepare_air_quality(spark)
	
	# Join and filter
	df = _join_silver_tables(energy_df, weather_df, air_df)
	df = df.where(F.col("energy_mwh").isNotNull())
	df = df.orderBy(F.col("date_hour").desc())
	
	# Apply sampling if requested
	if sample_limit and sample_limit > 0:
		df = df.limit(sample_limit)
		logger.info(f"Limited to {sample_limit} rows")
	
	# Optimize: reduce partitions and cache for better performance
	df = df.coalesce(OPTIMAL_PARTITIONS).cache()
	
	total = df.count()
	logger.info(f"Loaded {total} rows from Silver layer")
	
	return df


def train_pipeline(config: MLConfig, spark: SparkSession, sample_limit: int = None):
	
	# 1. Load data
	df = load_silver_data(spark, sample_limit)
	
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
	model = create_model(config.model)
	feature_cols = config.features.get_all_features(include_air_quality=False)
	target_col = config.features.target_column
	
	trained_model = model.train(train_df, feature_cols, target_col)
	
	# 5. Evaluate on test set
	logger.info("\n=== Model Evaluation ===")
	predictions = model.predict(test_df)
	metrics = evaluate_model(predictions, target_col, "prediction")
	print_metrics(metrics)
	
	# 6. Feature importance
	feature_importance = model.get_feature_importance()
	top_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:TOP_N_FEATURES]
	logger.info(f"\nTop {TOP_N_FEATURES} Features:")
	for feat, imp in top_features:
		logger.info(f"  {feat:40s}: {imp:.4f}")
	
	# 7. Track experiment
	logger.info("\n=== Experiment Tracking ===")
	tracker = create_tracker(
		tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_MLFLOW_URI),
		experiment_name=config.mlflow.experiment_name,
		use_mlflow=True
	)
	
	with tracker.run_context(run_name=f"{config.mlflow.run_name_prefix}_{config.model.model_type}"):
		# Log configuration
		tracker.log_params({
			"model_type": config.model.model_type,
			"max_depth": config.model.max_depth,
			"sample_limit": sample_limit or "all",
			"train_ratio": train_ratio,
			"num_features": len(feature_cols),
		})
		
		# Log metrics
		tracker.log_metrics(metrics)
		
		# Log model (if enabled)
		if config.mlflow.log_model:
			tracker.log_model(trained_model, config.mlflow.artifact_path)
		
		# Log tags
		tracker.set_tags({
			"pipeline_version": "refactored_v1",
			"feature_engineering": "modular",
		})
	
	logger.info("\n=== Training Complete ===")
	return model, metrics


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
		extra_conf={
			"spark.driver.memory": "4g",
			"spark.executor.memory": "4g",
			"spark.sql.shuffle.partitions": "20",
		}
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

