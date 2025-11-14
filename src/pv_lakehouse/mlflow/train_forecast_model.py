"""Training script for solar energy forecasting model.

Trains a logistic regression model on Silver energy data and writes
predictions to S3 for downstream Gold layer consumption.
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
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, TimestampType
from pyspark.sql.utils import AnalysisException

from pv_lakehouse.etl.utils.spark_utils import create_spark_session

DEFAULT_SAMPLE_LIMIT = 10_000
SILVER_HOURLY_ENERGY_TABLE = "lh.silver.clean_hourly_energy"
PREDICTIONS_S3_PATH = "s3a://lakehouse/silver/energy_forecast_predictions"
FEATURE_COLUMNS = ["intervals_count", "completeness_pct", "hour_of_day"]
LABEL_COLUMN = "energy_high_flag"


def load_silver_features(spark: SparkSession, limit_rows: int = DEFAULT_SAMPLE_LIMIT) -> tuple[DataFrame, float]:
    """Load Silver hourly-energy data and derive binary label for high output."""
    try:
        silver_df = spark.table(SILVER_HOURLY_ENERGY_TABLE)
    except AnalysisException as exc:
        raise RuntimeError(
            f"Silver table '{SILVER_HOURLY_ENERGY_TABLE}' unavailable. Run Silver loaders first."
        ) from exc

    filtered = (
        silver_df.select(
            "facility_code",
            "date_hour",
            F.col("energy_mwh").cast("double").alias("energy_mwh"),
            F.col("intervals_count").cast("double").alias("intervals_count"),
            F.col("completeness_pct").cast("double").alias("completeness_pct"),
        )
        .where(F.col("energy_mwh").isNotNull())
        .where(F.col("intervals_count").isNotNull())
        .where(F.col("completeness_pct").isNotNull())
        .withColumn("hour_of_day", F.hour("date_hour").cast("double"))
    )

    subset = filtered.orderBy(F.col("date_hour").desc())
    if limit_rows:
        subset = subset.limit(limit_rows)

    subset = subset.cache()
    row_count = subset.count()
    if row_count < 20:
        subset.unpersist()
        raise RuntimeError("Not enough Silver rows to train and evaluate model.")

    quantiles = subset.approxQuantile("energy_mwh", [0.6], 0.05)
    threshold = float(quantiles[0]) if quantiles else 0.0
    if math.isnan(threshold):
        threshold = 0.0

    labelled = subset.withColumn(
        LABEL_COLUMN,
        F.when(F.col("energy_mwh") >= F.lit(threshold), F.lit(1.0)).otherwise(F.lit(0.0)),
    )

    dataset = labelled.select(
        "facility_code",
        "date_hour",
        "energy_mwh",
        *FEATURE_COLUMNS,
        LABEL_COLUMN,
    ).cache()
    dataset.count()
    subset.unpersist()

    return dataset, threshold


def build_predictions_dataframe(predictions: DataFrame, model_version_id: str, threshold: float) -> DataFrame:
    """Transform ML predictions into Gold-compatible format."""
    
    # Extract probability of positive class
    prob_udf = F.udf(lambda v: float(v[1]) if v is not None and len(v) > 1 else None, DoubleType())
    
    result = (
        predictions
        .withColumn("forecast_score", prob_udf(F.col("probability")))
        .withColumn("forecast_energy_mwh", 
                   F.when(F.col("prediction") == 1.0, F.col("energy_mwh"))
                   .otherwise(F.col("energy_mwh") * F.col("forecast_score")))
        .withColumn("model_version_id", F.lit(model_version_id))
        .select(
            "facility_code",
            "date_hour",
            "model_version_id",
            F.col("forecast_energy_mwh").cast("double"),
            F.col("forecast_score").cast("double"),
        )
    )
    
    return result


def write_predictions_to_s3(predictions_df: DataFrame, s3_path: str) -> int:
    """Write predictions to S3 as Parquet for Gold layer consumption."""
    row_count = predictions_df.count()
    
    # Write as Parquet to S3
    (
        predictions_df
        .write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(s3_path)
    )
    
    print(f"✓ Wrote {row_count} prediction rows to {s3_path}")
    
    # Also write sample CSV for quick inspection
    sample_path = s3_path.replace("energy_forecast_predictions", "predictions_sample_100rows.csv")
    (
        predictions_df
        .limit(100)
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(sample_path)
    )
    print(f"✓ Wrote sample CSV to {sample_path}")
    
    return row_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train solar energy forecasting model")
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
        help="Model version ID (default: auto-generated from timestamp)",
    )
    return parser.parse_args()


def main(limit_rows: int = DEFAULT_SAMPLE_LIMIT, model_version_id: Optional[str] = None) -> None:
    spark = create_spark_session("SolarForecastTraining")
    dataset: Optional[DataFrame] = None

    try:
        # Set MLflow tracking
        mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
        mlflow.set_experiment("solar_energy_forecast")

        # Auto-generate model version if not provided
        if model_version_id is None:
            model_version_id = f"lr_v{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        print(f"[TRAIN] Model version: {model_version_id}")
        print(f"[TRAIN] Loading Silver data (limit={limit_rows})...")
        
        dataset, threshold = load_silver_features(spark, limit_rows=limit_rows)
        train_df, test_df = dataset.randomSplit([0.7, 0.3], seed=42)

        print(f"[TRAIN] Training logistic regression model...")
        assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")
        lr = LogisticRegression(labelCol=LABEL_COLUMN, featuresCol="features", maxIter=100)
        pipeline = Pipeline(stages=[assembler, lr])

        with mlflow.start_run(run_name=f"forecast_{model_version_id}"):
            model = pipeline.fit(train_df)
            predictions = model.transform(test_df)

            # Evaluate model
            auc = BinaryClassificationEvaluator(labelCol=LABEL_COLUMN).evaluate(predictions)
            accuracy = MulticlassClassificationEvaluator(
                labelCol=LABEL_COLUMN,
                predictionCol="prediction",
                metricName="accuracy",
            ).evaluate(predictions)

            print(f"[TRAIN] Model metrics:")
            print(f"  - AUC: {auc:.4f}")
            print(f"  - Accuracy: {accuracy:.4f}")
            print(f"  - Energy threshold (60th percentile): {threshold:.3f} MWh")

            # Transform all data for predictions
            all_predictions = model.transform(dataset)
            predictions_df = build_predictions_dataframe(all_predictions, model_version_id, threshold)
            
            # Write to S3
            pred_count = write_predictions_to_s3(predictions_df, PREDICTIONS_S3_PATH)

            # Log to MLflow
            mlflow.log_param("model_version_id", model_version_id)
            mlflow.log_param("silver_table", SILVER_HOURLY_ENERGY_TABLE)
            mlflow.log_param("energy_threshold_mwh", threshold)
            mlflow.log_param("max_iter", lr.getMaxIter())
            mlflow.log_param("predictions_s3_path", PREDICTIONS_S3_PATH)
            mlflow.log_metric("test_auc", auc)
            mlflow.log_metric("test_accuracy", accuracy)
            mlflow.log_metric("prediction_count", float(pred_count))
            
            try:
                mlflow.spark.log_model(model, "model", dfs_tmpdir="/tmp/mlflow_tmp")
            except Exception as e:
                print(f"[WARN] Could not log model to MLflow: {e}")

            print(f"[TRAIN] ✓ Training complete: {pred_count} predictions saved")
            
    finally:
        if dataset is not None:
            dataset.unpersist()
        spark.stop()


if __name__ == "__main__":
    parsed_args = parse_args()
    main(
        limit_rows=max(parsed_args.limit, 1),
        model_version_id=parsed_args.model_version,
    )
