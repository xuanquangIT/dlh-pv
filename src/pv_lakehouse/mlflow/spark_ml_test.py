"""PySpark ML demo that reads Silver inputs and materialises a Gold-style forecast fact.

The script trains a simple logistic-regression classifier using PySpark MLlib,
logs metrics/models to MLflow, and writes a Gold-shaped dataset derived from
Silver-layer features. It is intended as a smoke test / reference job rather
than production-quality forecasting code.
"""

from __future__ import annotations

import argparse
import math
import os
from typing import Optional

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from pv_lakehouse.etl.utils.spark_utils import create_spark_session, write_iceberg_table

DEFAULT_SAMPLE_LIMIT = 10_000

SILVER_HOURLY_ENERGY_TABLE = "lh.silver.clean_hourly_energy"
DIM_FACILITY_TABLE = "lh.gold.dim_facility"
DIM_MODEL_VERSION_TABLE = "lh.gold.dim_model_version"
GOLD_OUTPUT_TABLE = "lh.gold.fact_solar_forecast"
GOLD_WRITE_MODE = "overwrite"

FEATURE_COLUMNS = ["power_avg_mw", "intervals_count", "completeness_pct"]
LABEL_COLUMN = "energy_high_flag"


def load_silver_features(spark: SparkSession, limit_rows: int = DEFAULT_SAMPLE_LIMIT) -> tuple[DataFrame, float]:
    """Load Silver hourly-energy data and derive a binary label for high output."""

    try:
        silver_df = spark.table(SILVER_HOURLY_ENERGY_TABLE)
    except AnalysisException as exc:  # pragma: no cover - depends on runtime setup
        raise RuntimeError(
            f"Silver table '{SILVER_HOURLY_ENERGY_TABLE}' is unavailable. Run Silver loaders first."
        ) from exc

    filtered = (
        silver_df.select(
            "facility_code",
            "date_hour",
            F.col("energy_mwh").cast("double").alias("energy_mwh"),
            F.col("power_avg_mw").cast("double").alias("power_avg_mw"),
            F.col("intervals_count").cast("double").alias("intervals_count"),
            F.col("completeness_pct").cast("double").alias("completeness_pct"),
        )
        .where(F.col("energy_mwh").isNotNull())
        .where(F.col("power_avg_mw").isNotNull())
        .where(F.col("intervals_count").isNotNull())
        .where(F.col("completeness_pct").isNotNull())
    )

    subset = filtered.orderBy(F.col("date_hour").desc())
    if limit_rows:
        subset = subset.limit(limit_rows)

    subset = subset.cache()
    row_count = subset.count()
    if row_count < 20:  # pragma: no cover - defensive guard when data missing
        subset.unpersist()
        raise RuntimeError("Not enough Silver rows to train and evaluate the model.")

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
    dataset.count()  # materialise cache
    subset.unpersist()

    return dataset, threshold


def resolve_model_version_key(spark: SparkSession) -> int:
    """Fetch the active model_version_key from the Gold dimension, defaulting to 1."""

    try:
        table = spark.table(DIM_MODEL_VERSION_TABLE)
    except AnalysisException:
        return 1

    if "model_version_key" not in table.columns:
        return 1

    ordering = []
    if "is_active" in table.columns:
        ordering.append(F.col("is_active").desc_nulls_last())
    ordering.append(F.col("model_version_key").desc())

    row = table.select("model_version_key").orderBy(*ordering).limit(1).collect()
    if row:
        return int(row[0]["model_version_key"])
    return 1


def load_facility_dimension(spark: SparkSession) -> Optional[DataFrame]:
    """Load facility dimension mapping if available."""

    try:
        dim_facility = spark.table(DIM_FACILITY_TABLE).select("facility_code", "facility_key")
    except AnalysisException:
        return None

    required_cols = {"facility_code", "facility_key"}
    if not required_cols.issubset(dim_facility.columns):
        return None
    return dim_facility


def build_gold_fact(predictions: DataFrame, model_version_key: int) -> DataFrame:
    """Shape predictions into a Gold-style fact dataset."""

    facility_dim = load_facility_dimension(predictions.sparkSession)
    window = Window.orderBy("facility_code", "date_hour")

    prob_udf = F.udf(lambda v: float(v[1]) if v is not None and len(v) > 1 else None, DoubleType())

    enriched = (
        predictions.withColumn("prob_positive", prob_udf(F.col("probability")))
        .withColumn("forecast_id", F.row_number().over(window))
        .withColumn("date_key", F.date_format(F.col("date_hour"), "yyyyMMdd").cast("int"))
        .withColumn("time_key", (F.hour("date_hour") * 100 + F.minute("date_hour")).cast("int"))
        .withColumn("model_version_key", F.lit(int(model_version_key)))
        .withColumn("weather_condition_key", F.lit(None).cast("int"))
        .withColumn("actual_energy_mwh", F.col("energy_mwh"))
        .withColumn(
            "predicted_energy_mwh",
            (F.col("prob_positive") * F.col("energy_mwh")).cast("double"),
        )
        .withColumn("forecast_error_mwh", F.col("actual_energy_mwh") - F.col("predicted_energy_mwh"))
        .withColumn(
            "absolute_percentage_error",
            F.when(F.col("actual_energy_mwh") == 0, F.lit(0.0)).otherwise(
                F.abs(F.col("forecast_error_mwh") / F.col("actual_energy_mwh")) * 100.0
            ),
        )
        .withColumn("mae_metric", F.abs(F.col("forecast_error_mwh")))
        .withColumn("rmse_metric", F.abs(F.col("forecast_error_mwh")))
        .withColumn("r2_score", F.lit(1.0))
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


def write_gold_demo(gold_df: DataFrame, table_name: str, mode: str = "overwrite") -> int:
    """Write the Gold-style DataFrame into an Iceberg table and return the row count."""

    row_count = gold_df.count()
    write_iceberg_table(gold_df, table_name, mode=mode)
    return row_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PySpark MLflow smoke test on Silver data.")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_SAMPLE_LIMIT,
        help="Maximum number of recent rows to sample from the Silver table (default: %(default)s).",
    )
    return parser.parse_args()


def main(limit_rows: int = DEFAULT_SAMPLE_LIMIT) -> None:
    spark = create_spark_session("SparkMLGoldDemo")
    dataset: Optional[DataFrame] = None

    try:
        mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
        mlflow.set_experiment("spark_silver_demo_v2")

        dataset, threshold = load_silver_features(spark, limit_rows=limit_rows)
        train_df, test_df = dataset.randomSplit([0.7, 0.3], seed=42)

        assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")
        lr = LogisticRegression(labelCol=LABEL_COLUMN, featuresCol="features", maxIter=100)
        pipeline = Pipeline(stages=[assembler, lr])

        with mlflow.start_run(run_name="gold_forecast_lr_silver_input"):
            model = pipeline.fit(train_df)
            predictions = model.transform(test_df)

            auc = BinaryClassificationEvaluator(labelCol=LABEL_COLUMN).evaluate(predictions)
            accuracy = MulticlassClassificationEvaluator(
                labelCol=LABEL_COLUMN,
                predictionCol="prediction",
                metricName="accuracy",
            ).evaluate(predictions)

            model_version_key = resolve_model_version_key(spark)
            gold_df = build_gold_fact(predictions, model_version_key)
            gold_rows = write_gold_demo(gold_df, GOLD_OUTPUT_TABLE, mode=GOLD_WRITE_MODE)

            mlflow.log_param("silver_table", SILVER_HOURLY_ENERGY_TABLE)
            mlflow.log_param("dim_model_version_table", DIM_MODEL_VERSION_TABLE)
            mlflow.log_param("energy_threshold_mwh", threshold)
            mlflow.log_param("max_iter", lr.getMaxIter())
            mlflow.log_param("gold_output_table", GOLD_OUTPUT_TABLE)
            mlflow.log_param("gold_write_mode", GOLD_WRITE_MODE)
            mlflow.log_metric("test_auc", auc)
            mlflow.log_metric("test_accuracy", accuracy)
            mlflow.log_metric("gold_row_count", float(gold_rows))
            mlflow.spark.log_model(model, "model", dfs_tmpdir="/tmp/mlflow_tmp")

            print(f"Energy threshold (60th percentile): {threshold:.3f} MWh")
            print(f"Test AUC: {auc:.3f}")
            print(f"Test accuracy: {accuracy:.3f}")
            print(f"Gold-style rows materialised: {gold_rows}")
    finally:
        if dataset is not None:
            dataset.unpersist()
        spark.stop()


if __name__ == "__main__":
    parsed_args = parse_args()
    main(limit_rows=max(parsed_args.limit, 1))
