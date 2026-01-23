"""Train a regression model from Silver tables using modular ML pipeline.

Refactored to use modular architecture:
- Configuration loaded from YAML files
- Feature engineering in separate modules
- Model abstraction for flexibility
- Experiment tracking abstracted from training logic
"""
from __future__ import annotations

import os
import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

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


# Silver table names
SILVER_ENERGY = "lh.silver.clean_hourly_energy"
SILVER_WEATHER = "lh.silver.clean_hourly_weather"
SILVER_AQ = "lh.silver.clean_hourly_air_quality"


def load_silver_data(spark: SparkSession, sample_limit: int = None) -> object:
	"""Load and join Silver layer tables.
	
	Args:
		spark: SparkSession
		sample_limit: Optional limit on number of rows
		
	Returns:
		DataFrame with joined Silver data
	"""
	print("Loading data from Silver layer...")
	
	# Load energy data
	energy_df = spark.table(SILVER_ENERGY).select(
		"facility_code", "date_hour",
		F.col("energy_mwh").cast("double"),
		F.col("intervals_count").cast("double"),
		F.col("completeness_pct").cast("double"),
	)
	
	# Load weather data
	weather_df = spark.table(SILVER_WEATHER).select(
		"facility_code", "date_hour",
		F.col("temperature_2m").cast("double"),
		F.col("cloud_cover").cast("double"),
		F.col("shortwave_radiation").cast("double"),
		F.col("direct_radiation").cast("double"),
		F.col("diffuse_radiation").cast("double"),
		F.col("precipitation").cast("double"),
		F.col("wind_speed_10m").cast("double"),
	)
	
	# Load air quality data (optional)
	air_df = spark.table(SILVER_AQ).select(
		"facility_code", "date_hour",
		F.col("pm2_5").cast("double"),
		F.col("pm10").cast("double"),
		F.col("ozone").cast("double"),
		F.col("nitrogen_dioxide").cast("double"),
	)
	
	# Join all tables
	df = energy_df.join(weather_df, on=["facility_code", "date_hour"], how="left")
	df = df.join(air_df, on=["facility_code", "date_hour"], how="left")
	
	# Filter nulls and order
	df = df.where(F.col("energy_mwh").isNotNull())
	df = df.orderBy(F.col("date_hour").desc())
	
	# Apply sampling if requested
	if sample_limit and sample_limit > 0:
		df = df.limit(sample_limit)
		print(f"Limited to {sample_limit} rows")
	
	total = df.count()
	print(f"Loaded {total} rows from Silver layer")
	
	return df


def train_pipeline(config: MLConfig, spark: SparkSession, sample_limit: int = None):
	"""Main training pipeline orchestration.
	
	Args:
		config: ML configuration
		spark: SparkSession
		sample_limit: Optional sample limit for testing
	"""
	# 1. Load data
	df = load_silver_data(spark, sample_limit)
	
	# 2. Feature engineering
	print("\n=== Feature Engineering ===")
	df = prepare_features(df, config.features, include_lag=True, include_air_quality=False)
	df = select_training_features(df, config.features, include_air_quality=False)
	df = validate_features(df, min_rows=config.training.min_rows_required)
	
	# 3. Split data
	print("\n=== Data Split ===")
	train_ratio = config.training.train_ratio
	val_ratio = config.training.validation_ratio
	test_ratio = config.training.test_ratio
	
	train_df, val_df, test_df = df.randomSplit(
		[train_ratio, val_ratio, test_ratio],
		seed=config.training.random_seed
	)
	
	print(f"Train: {train_df.count()}, Val: {val_df.count()}, Test: {test_df.count()}")
	
	# 4. Train model
	print("\n=== Model Training ===")
	model = create_model(config.model)
	feature_cols = config.features.get_all_features(include_air_quality=False)
	target_col = config.features.target_column
	
	trained_model = model.train(train_df, feature_cols, target_col)
	
	# 5. Evaluate on test set
	print("\n=== Model Evaluation ===")
	predictions = model.predict(test_df)
	metrics = evaluate_model(predictions, target_col, "prediction")
	print_metrics(metrics)
	
	# 6. Feature importance
	feature_importance = model.get_feature_importance()
	top_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:10]
	print("\nTop 10 Features:")
	for feat, imp in top_features:
		print(f"  {feat:40s}: {imp:.4f}")
	
	# 7. Track experiment
	print("\n=== Experiment Tracking ===")
	tracker = create_tracker(
		tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
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
	
	print("\n=== Training Complete ===")
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
	"""Main entry point."""
	args = parse_args()
	
	# Resolve config paths
	project_root = Path(__file__).parent.parent.parent.parent
	features_path = project_root / args.features_config
	hyperparams_path = project_root / args.hyperparams_config
	
	print(f"Loading configuration from:")
	print(f"  Features: {features_path}")
	print(f"  Hyperparams: {hyperparams_path}")
	
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
		
		print("\n✅ Training pipeline completed successfully")
		print(f"Primary metric ({config.training.primary_metric}): {metrics.get(config.training.primary_metric, 'N/A')}")
		
	finally:
		spark.stop()


if __name__ == "__main__":
	main()

