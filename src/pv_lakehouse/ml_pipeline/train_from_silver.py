"""Train a small PySpark ML pipeline from Silver tables and log to MLflow.

This script is a smoke/experiment job: it reads Silver tables, assembles
features, trains a LogisticRegression model, evaluates it and logs metrics
and artifacts to MLflow. Intended for experiments and quick validation.
"""
from __future__ import annotations

import os
import argparse
import json
import tempfile
from typing import Optional, Tuple

import mlflow
import mlflow.spark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

import pandas as pd
import numpy as np
from sklearn.metrics import roc_curve, precision_recall_curve

from pv_lakehouse.etl.utils.spark_utils import create_spark_session, write_iceberg_table

# Default: use all rows when limit==0, otherwise sample N rows for quick tests
DEFAULT_SAMPLE = 1000

# Silver table names
SILVER_ENERGY = "lh.silver.clean_hourly_energy"
SILVER_WEATHER = "lh.silver.clean_hourly_weather"
SILVER_AQ = "lh.silver.clean_hourly_air_quality"

FEATURE_COLS = [
	"intervals_count", "completeness_pct", "hour_of_day",
	"temperature_2m", "cloud_cover", "shortwave_radiation", "direct_radiation",
	"diffuse_radiation", "precipitation", "wind_speed_10m",
	"pm2_5", "pm10", "ozone", "nitrogen_dioxide",
]
LABEL_COL = "energy_high_flag"


def load_and_prepare(spark: SparkSession, limit: Optional[int] = DEFAULT_SAMPLE) -> Tuple[object, float]:
	"""Load Silver tables, join features, compute label threshold and return dataframe + threshold.

	limit: if None use full dataset; if 0 also treated as full dataset; if >0 limit rows.
	"""
	print("Loading data from Silver layer...")
	lim_display = 'No limit (using all data)' if (limit is None or limit == 0) else limit
	print(f"Row limit: {lim_display}")

	energy_df = spark.table(SILVER_ENERGY).select(
		"facility_code", "date_hour",
		F.col("energy_mwh").cast("double").alias("energy_mwh"),
		F.col("intervals_count").cast("double"),
		F.col("completeness_pct").cast("double"),
	)

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

	air_df = spark.table(SILVER_AQ).select(
		"facility_code", "date_hour",
		F.col("pm2_5").cast("double"), F.col("pm10").cast("double"),
		F.col("ozone").cast("double"), F.col("nitrogen_dioxide").cast("double"),
	)

	df = energy_df.join(weather_df, on=["facility_code", "date_hour"], how="left")\
				  .join(air_df, on=["facility_code", "date_hour"], how="left")

	df = df.where(F.col("energy_mwh").isNotNull())
	df = df.withColumn("hour_of_day", F.hour("date_hour").cast("double"))
	df = df.orderBy(F.col("date_hour").desc())

	df = df.cache()
	total = df.count()
	print(f"Total rows in Silver: {total}")

	# Apply limit: treat None or 0 as full dataset
	if limit is not None and limit > 0:
		df = df.limit(limit)
		print(f"Limited to {limit} rows for smoke run")
	else:
		print("Using all rows")

	cnt = df.count()
	print(f"Final row count: {cnt}")
	if cnt < 20:
		raise RuntimeError("Not enough Silver rows to train model (minimum 20 required)")

	quant = df.approxQuantile("energy_mwh", [0.6], 0.05)
	threshold = float(quant[0]) if quant else 0.0
	print(f"Energy threshold (60th percentile): {threshold:.3f} MWh")

	labelled = df.withColumn(LABEL_COL, F.when(F.col("energy_mwh") >= F.lit(threshold), F.lit(1.0)).otherwise(F.lit(0.0)))

	return labelled.select("facility_code", "date_hour", "energy_mwh", *FEATURE_COLS, LABEL_COL), threshold


def calculate_class_weights(df) -> dict:
	counts = df.groupBy(LABEL_COL).count().collect()
	total = sum(r["count"] for r in counts)
	return {r[LABEL_COL]: total / (2.0 * r["count"]) for r in counts}


def save_curve_points_from_sample(preds, label_col: str, score_col: str, out_dir: str, sample_limit: int = 20000) -> dict:
	"""Sample predictions to Pandas and compute ROC/PR curve points using sklearn. Returns file paths."""
	pdf = preds.select(score_col, label_col).limit(sample_limit).toPandas()
	if pdf.empty:
		return {}

	scores = pdf[score_col].fillna(0.0).astype(float).values
	labels = pdf[label_col].astype(int).values

	try:
		fpr, tpr, _ = roc_curve(labels, scores)
		recall, precision, _ = precision_recall_curve(labels, scores)

		roc_path = os.path.join(out_dir, "roc_points.csv")
		pr_path = os.path.join(out_dir, "pr_points.csv")

		pd.DataFrame({"fpr": fpr, "tpr": tpr}).to_csv(roc_path, index=False)
		pd.DataFrame({"recall": recall, "precision": precision}).to_csv(pr_path, index=False)

		return {"roc": roc_path, "pr": pr_path}
	except Exception as e:
		print(f"Warning: failed to compute curves from sample: {e}")
		return {}


def train_and_log(df, threshold: float, limit: Optional[int] = None) -> None:
	print("Starting training...")

	train_df, val_df, test_df = df.randomSplit([0.6, 0.2, 0.2], seed=42)

	class_weights = calculate_class_weights(train_df)
	train_df = train_df.withColumn("weight", F.when(F.col(LABEL_COL) == 1.0, F.lit(class_weights.get(1.0, 1.0))).otherwise(F.lit(class_weights.get(0.0, 1.0))))

	assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
	lr = LogisticRegression(labelCol=LABEL_COL, featuresCol="features", maxIter=100, regParam=0.01, elasticNetParam=0.1, weightCol="weight")
	pipeline = Pipeline(stages=[assembler, lr])

	with mlflow.start_run(run_name="pv_silver_full_training"):
		model = pipeline.fit(train_df)
		preds = model.transform(test_df)

		# Extract probability score safely
		try:
			preds = preds.withColumn("score", vector_to_array(F.col("probability")).getItem(1).cast(DoubleType()))
		except Exception:
			from pyspark.sql.functions import udf
			def _get_score(v):
				try:
					return float(v[1])
				except Exception:
					return 0.0
			_get_score_udf = udf(_get_score, DoubleType())
			preds = preds.withColumn("score", _get_score_udf(F.col("probability")))

		# Basic metrics via Spark evaluators
		auc = BinaryClassificationEvaluator(labelCol=LABEL_COL, metricName="areaUnderROC").evaluate(preds)
		aupr = BinaryClassificationEvaluator(labelCol=LABEL_COL, metricName="areaUnderPR").evaluate(preds)
		acc = MulticlassClassificationEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="accuracy").evaluate(preds)
		weighted_precision = MulticlassClassificationEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="weightedPrecision").evaluate(preds)
		weighted_recall = MulticlassClassificationEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="weightedRecall").evaluate(preds)
		weighted_f1 = MulticlassClassificationEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="f1").evaluate(preds)

		# Confusion counts
		tp = preds.filter((F.col(LABEL_COL) == 1.0) & (F.col("prediction") == 1.0)).count()
		tn = preds.filter((F.col(LABEL_COL) == 0.0) & (F.col("prediction") == 0.0)).count()
		fp = preds.filter((F.col(LABEL_COL) == 0.0) & (F.col("prediction") == 1.0)).count()
		fn = preds.filter((F.col(LABEL_COL) == 1.0) & (F.col("prediction") == 0.0)).count()
		precision_manual = tp / (tp + fp) if (tp + fp) > 0 else 0.0
		recall_manual = tp / (tp + fn) if (tp + fn) > 0 else 0.0
		f1_manual = 2 * precision_manual * recall_manual / (precision_manual + recall_manual) if (precision_manual + recall_manual) > 0 else 0.0

		# Log params
		mlflow.log_param("sample_limit", limit)
		mlflow.log_param("feature_cols", ",".join(FEATURE_COLS))
		mlflow.log_param("threshold", float(threshold))
		mlflow.log_param("class_weights", str(class_weights))
		mlflow.log_param("train_size", train_df.count())
		mlflow.log_param("val_size", val_df.count())
		mlflow.log_param("test_size", test_df.count())

		# Log metrics
		mlflow.log_metric("test_auc", float(auc))
		mlflow.log_metric("test_aupr", float(aupr))
		mlflow.log_metric("test_accuracy", float(acc))
		mlflow.log_metric("test_weighted_precision", float(weighted_precision))
		mlflow.log_metric("test_weighted_recall", float(weighted_recall))
		mlflow.log_metric("test_weighted_f1", float(weighted_f1))
		mlflow.log_metric("test_precision", float(precision_manual))
		mlflow.log_metric("test_recall", float(recall_manual))
		mlflow.log_metric("test_f1", float(f1_manual))

		# Save artifacts: curves and sample predictions
		tmpdir = tempfile.mkdtemp()
		curves = {}
		try:
			curves = save_curve_points_from_sample(preds.select("score", LABEL_COL), LABEL_COL, "score", tmpdir)
		except Exception as e:
			print(f"Warning: curve computation failed: {e}")

		# sample predictions -> pandas -> CSV
		try:
			sample_limit = 10000
			sample_pd = preds.select("facility_code", "date_hour", "energy_mwh", LABEL_COL, "prediction", "score").limit(sample_limit).toPandas()
			preds_path = os.path.join(tmpdir, "predictions_sample.csv")
			sample_pd.to_csv(preds_path, index=False)
			mlflow.log_artifact(preds_path, artifact_path="predictions")
		except Exception as e:
			print(f"Warning: failed to save sample predictions: {e}")

		for name, path in curves.items():
			if os.path.exists(path):
				mlflow.log_artifact(path, artifact_path="curves")

		# Save metrics summary
		metrics_summary = {
			"auc": float(auc),
			"aupr": float(aupr),
			"accuracy": float(acc),
			"weighted_precision": float(weighted_precision),
			"weighted_recall": float(weighted_recall),
			"weighted_f1": float(weighted_f1),
			"precision": float(precision_manual),
			"recall": float(recall_manual),
			"f1": float(f1_manual),
			"tp": int(tp), "tn": int(tn), "fp": int(fp), "fn": int(fn),
		}
		metrics_json = os.path.join(tmpdir, "metrics_summary.json")
		with open(metrics_json, "w") as fh:
			json.dump(metrics_summary, fh, indent=2)
		mlflow.log_artifact(metrics_json, artifact_path="metrics")

		# Log model (best-effort). use dfs_tmpdir to stage locally if needed.
		try:
			mlflow.spark.log_model(model, "model", dfs_tmpdir=os.environ.get("MLFLOW_DFS_TMP", None))
		except Exception as e:
			print(f"Warning: failed to log model to MLflow artifact store: {e}")

		print(f"MLflow run logged. sample_limit={limit} threshold={threshold:.3f} test_auc={auc:.3f} test_acc={acc:.3f}")


def parse_args():
	p = argparse.ArgumentParser()
	p.add_argument("--limit", type=int, default=DEFAULT_SAMPLE, help="0 or None = use all rows; >0 = sample")
	p.add_argument("--mlflow", default=os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
	return p.parse_args()


def main():
	args = parse_args()

	spark = create_spark_session(
		"pv_silver_full_training",
		extra_conf={
			"spark.driver.memory": "4g",
			"spark.executor.memory": "4g",
			"spark.sql.shuffle.partitions": "20",
			"spark.default.parallelism": "20",
			"spark.sql.autoBroadcastJoinThreshold": "100m",
		},
	)

	mlflow.set_tracking_uri(args.mlflow)
	mlflow.set_experiment("pv_silver_full_training")

	lim = args.limit
	# treat 0 as full
	if lim == 0:
		lim = None

	df, threshold = load_and_prepare(spark, limit=lim)
	train_and_log(df, threshold, args.limit)
	df.unpersist()
	spark.stop()


if __name__ == "__main__":
	main()

