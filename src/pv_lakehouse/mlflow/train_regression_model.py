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
from pv_lakehouse.etl.gold.dim_feature_importance import get_dim_feature_importance_schema
from pv_lakehouse.etl.gold.dim_error_category import get_dim_error_category_schema

DEFAULT_SAMPLE_LIMIT = 50_000
SILVER_HOURLY_ENERGY_TABLE = "lh.silver.clean_hourly_energy"
SILVER_HOURLY_WEATHER_TABLE = "lh.silver.clean_hourly_weather"
DIM_FACILITY_TABLE = "lh.gold.dim_facility"
DIM_MODEL_VERSION_TABLE = "lh.gold.dim_model_version"
GOLD_OUTPUT_TABLE = "lh.gold.fact_solar_forecast_regression"
GOLD_WRITE_MODE = "overwrite"
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
    import random
    random.seed(42)
    
    for col_name in noisy_columns:
        if col_name not in df.columns:
            continue
        
        noise = (F.rand(seed=42) * 2 - 1) * noise_magnitude
        should_apply_noise = F.rand(seed=43) < noise_ratio
        noisy_value = F.col(col_name) * (1 + noise)
        
        df = df.withColumn(
            col_name,
            F.when(should_apply_noise, noisy_value).otherwise(F.col(col_name))
        )
    
    return df


def load_silver_features_regression(spark: SparkSession, limit_rows: int = DEFAULT_SAMPLE_LIMIT) -> DataFrame:
    try:
        silver_energy = spark.table(SILVER_HOURLY_ENERGY_TABLE)
    except AnalysisException as exc:
        raise RuntimeError(
            f"Silver table '{SILVER_HOURLY_ENERGY_TABLE}' unavailable. Run Silver loaders first."
        ) from exc

    try:
        silver_weather = spark.table(SILVER_HOURLY_WEATHER_TABLE)
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

    filtered = (
        filtered
        .withColumn("hour_of_day", F.hour("date_hour").cast("double"))
        .withColumn("day_of_week", F.dayofweek("date_hour").cast("double"))
        .withColumn("month", F.month("date_hour").cast("double"))
        .withColumn("is_weekend", F.when(F.dayofweek("date_hour").isin([1, 7]), 1.0).otherwise(0.0))
        .withColumn("hour_sin", F.sin(2 * 3.14159 * F.col("hour_of_day") / 24.0))
        .withColumn("hour_cos", F.cos(2 * 3.14159 * F.col("hour_of_day") / 24.0))
        .withColumn("month_sin", F.sin(2 * 3.14159 * F.col("month") / 12.0))
        .withColumn("month_cos", F.cos(2 * 3.14159 * F.col("month") / 12.0))
    )

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
        .where(F.col("shortwave_radiation").isNotNull())
    )
    
    filtered = filtered.join(
        weather_features,
        (filtered.facility_code == weather_features.w_facility_code) &
        (filtered.date_hour == weather_features.w_date_hour),
        how="inner"
    ).drop("w_facility_code", "w_date_hour")
    
    from pyspark.sql.window import Window
    
    window_by_facility = Window.partitionBy("facility_code").orderBy("date_hour")
    filtered = (
        filtered
        .withColumn("energy_lag_1h", F.lag("energy_mwh", 1).over(window_by_facility))
        .withColumn("energy_lag_24h", F.lag("energy_mwh", 24).over(window_by_facility))
        .withColumn("energy_lag_168h", F.lag("energy_mwh", 168).over(window_by_facility))
        .fillna(0.0, subset=["energy_lag_1h", "energy_lag_24h", "energy_lag_168h"])
    )
    
    filtered = (
        filtered
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
        .withColumn("is_production_hour",
                   F.when((F.col("hour_of_day") >= 6) & (F.col("hour_of_day") <= 18) & 
                          (F.col("shortwave_radiation") > 50), 1.0)
                   .otherwise(0.0))
        .withColumn("is_low_energy_period",
                   F.when(F.col("energy_lag_1h") < 5.0, 1.0).otherwise(0.0))
        .withColumn("shortwave_radiation_sq",
                   F.col("shortwave_radiation") * F.col("shortwave_radiation") / 100000.0)
        .withColumn("temperature_sq",
                   F.col("temperature_2m") * F.col("temperature_2m") / 100.0)
        .withColumn("cloud_temp_interaction",
                   F.col("cloud_cover") * F.col("temperature_2m") / 100.0)
    )

    filtered = filtered.where(
        (F.col("energy_mwh") >= 5.0) & 
        (F.col("shortwave_radiation") > 0)
    )
    
    subset = filtered.orderBy(F.col("date_hour").desc())
    if limit_rows and limit_rows > 0:
        subset = subset.limit(limit_rows)

    subset = subset.cache()
    row_count = subset.count()
    if row_count < 100:
        subset.unpersist()
        raise RuntimeError("Not enough Silver rows with weather data to train regression model.")
    
    return subset


def load_facility_dimension(spark: SparkSession) -> Optional[DataFrame]:
    try:
        return spark.table(DIM_FACILITY_TABLE).select("facility_code", "facility_key")
    except AnalysisException:
        return None


def resolve_model_version_key(spark: SparkSession) -> int:
    try:
        dim_df = spark.table(DIM_MODEL_VERSION_TABLE)
        max_key = dim_df.agg(F.max("model_version_key")).collect()[0][0]
        return int(max_key) if max_key is not None else 1
    except (AnalysisException, Exception):
        return 1


def build_gold_fact(predictions: DataFrame, model_version_key: int, metrics: dict = None) -> DataFrame:
    agg_mae = float(metrics.get("test_mae", 0.0)) if metrics else 0.0
    agg_rmse = float(metrics.get("test_rmse", 0.0)) if metrics else 0.0
    agg_r2 = float(metrics.get("test_r2", 0.0)) if metrics else 0.0
    
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
        .withColumn("predicted_energy_mwh", F.when(F.col("prediction") < 0, F.lit(0.0)).otherwise(F.col("prediction")))
        .withColumn("forecast_error_mwh", F.col("actual_energy_mwh") - F.col("predicted_energy_mwh"))
        .withColumn(
            "absolute_percentage_error",
            F.when(F.col("actual_energy_mwh") == 0, F.lit(0.0)).otherwise(
                F.abs(F.col("forecast_error_mwh") / F.col("actual_energy_mwh")) * 100.0
            ),
        )
        .withColumn(
            "error_category_key",
            F.when(F.col("absolute_percentage_error") <= 5.0, 1)
             .when(F.col("absolute_percentage_error") <= 10.0, 2)
             .when(F.col("absolute_percentage_error") <= 20.0, 3)
             .when(F.col("absolute_percentage_error") <= 50.0, 4)
             .when(F.col("absolute_percentage_error") <= 100.0, 5)
             .otherwise(6)
        )
        .withColumn("mae_metric", F.lit(agg_mae))
        .withColumn("rmse_metric", F.lit(agg_rmse))
        .withColumn("r2_score", F.lit(agg_r2))
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
        "error_category_key",
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
    row_count = gold_df.count()
    write_iceberg_table(gold_df, table_name, mode=mode)
    return row_count


def save_feature_importance_to_gold(spark: SparkSession, model: Pipeline, model_version_key: int) -> None:
    rf_model = model.stages[-1]
    importances = rf_model.featureImportances.toArray()
    
    category_map = {
        "intervals_count": "Energy Quality",
        "completeness_pct": "Energy Quality",
        "hour_of_day": "Temporal",
        "day_of_week": "Temporal",
        "month": "Temporal",
        "is_weekend": "Temporal",
        "hour_sin": "Temporal",
        "hour_cos": "Temporal",
        "month_sin": "Temporal",
        "month_cos": "Temporal",
        "temperature_2m": "Weather",
        "cloud_cover": "Weather",
        "shortwave_radiation": "Weather",
        "direct_radiation": "Weather",
        "radiation_temp_interaction": "Interaction",
        "cloud_radiation_ratio": "Interaction",
        "hour_radiation_interaction": "Interaction",
        "completeness_radiation_interaction": "Interaction",
        "energy_lag_1h": "LAG Features",
        "energy_lag_24h": "LAG Features",
        "energy_lag_168h": "LAG Features",
        "is_production_hour": "Production Indicators",
        "is_low_energy_period": "Production Indicators",
    }
    
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
    
    feature_data.sort(key=lambda x: x[2], reverse=True)
    
    rows = []
    category_ranks = {}
    cumulative = 0.0
    
    for overall_rank, (feature_name, category, importance_val, importance_pct, description) in enumerate(feature_data, start=1):
        if category not in category_ranks:
            category_ranks[category] = 0
        category_ranks[category] += 1
        rank_in_category = category_ranks[category]
        cumulative += importance_pct
        is_top_15 = overall_rank <= 15
        is_lag_feature = category == "LAG Features"
        
        rows.append((
            int(overall_rank),
            str(feature_name),
            str(category),
            float(importance_val),
            float(importance_pct),
            float(cumulative),
            int(overall_rank),
            int(rank_in_category),
            bool(is_top_15),
            bool(is_lag_feature),
            int(model_version_key),
            datetime.now()
        ))
    
    schema = get_dim_feature_importance_schema()
    importance_df = spark.createDataFrame(rows, schema)
    
    try:
        spark.sql(f"""
            DELETE FROM lh.gold.dim_feature_importance
            WHERE model_version_key = {model_version_key}
        """)
    except Exception:
        pass
    
    write_iceberg_table(
        importance_df,
        "lh.gold.dim_feature_importance",
        mode="append",
        partition_cols=["model_version_key"]
    )


def train_regression_model(
    spark: SparkSession,
    limit_rows: int = DEFAULT_SAMPLE_LIMIT,
    apply_noise: bool = True,
) -> tuple[Pipeline, DataFrame, dict]:
    dataset = load_silver_features_regression(spark, limit_rows)
    
    from pyspark.sql.window import Window
    
    total_count = dataset.count()
    split_index = int(total_count * 0.95)
    window_spec = Window.orderBy("date_hour")
    dataset_with_row = dataset.withColumn("row_num", F.row_number().over(window_spec))
    split_timestamp = dataset_with_row.where(F.col("row_num") == split_index).select("date_hour").first()[0]
    
    train_df = dataset.where(F.col("date_hour") < split_timestamp)
    test_df = dataset.where(F.col("date_hour") >= split_timestamp)
    
    train_count = train_df.count()
    test_count = test_df.count()
    
    if apply_noise:
        noisy_columns = [
            "temperature_2m", "cloud_cover", "shortwave_radiation", "direct_radiation",
            "intervals_count", "completeness_pct"
        ]
        train_df = add_aggressive_noise(train_df, noisy_columns, noise_magnitude=0.15, noise_ratio=0.7)
    
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="skip"
    )
    
    regressor = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        maxIter=120,
        maxDepth=6,
        stepSize=0.1,
        minInstancesPerNode=50,
        maxBins=128,
        subsamplingRate=0.8,
        minInfoGain=0.01,
        seed=42
    )
    
    pipeline = Pipeline(stages=[assembler, regressor])
    
    model = pipeline.fit(train_df)
    test_predictions = model.transform(test_df)
    
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
    
    train_predictions = model.transform(train_df)
    train_mae = evaluator_mae.evaluate(train_predictions)
    train_r2 = evaluator_r2.evaluate(train_predictions)
    
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
    parser = argparse.ArgumentParser(description="Train solar energy regression model")
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
    
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("solar_energy_regression_forecast")
    
    spark = create_spark_session("SolarEnergyRegressionTraining")
    
    with mlflow.start_run(run_name=f"regression_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
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
        
        model, test_predictions, metrics = train_regression_model(
            spark, 
            limit_rows=args.limit,
            apply_noise=not args.no_noise
        )
        
        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)
        
        model_version_key = resolve_model_version_key(spark)
        gold_df = build_gold_fact(test_predictions, model_version_key, metrics=metrics)
        gold_rows = write_gold_predictions(gold_df, GOLD_OUTPUT_TABLE, mode=GOLD_WRITE_MODE)
        save_feature_importance_to_gold(spark, model, model_version_key)
        
        mlflow.log_metric("gold_row_count", float(gold_rows))
        mlflow.log_param("model_version_key", model_version_key)
        mlflow.spark.log_model(model, "regression_model", dfs_tmpdir="/tmp/mlflow_tmp")
        
    spark.stop()


if __name__ == "__main__":
    main()
