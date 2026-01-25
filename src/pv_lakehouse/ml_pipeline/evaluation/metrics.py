from __future__ import annotations

import math
from typing import Dict
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame


def calculate_regression_metrics(predictions_df: DataFrame, 
                                 label_col: str = "energy_mwh",
                                 prediction_col: str = "prediction") -> Dict[str, float]:
    
    evaluator = RegressionEvaluator(
        labelCol=label_col,
        predictionCol=prediction_col
    )
    
    # Calculate standard metrics
    rmse = evaluator.setMetricName("rmse").evaluate(predictions_df)
    mae = evaluator.setMetricName("mae").evaluate(predictions_df)
    r2 = evaluator.setMetricName("r2").evaluate(predictions_df)
    mse = evaluator.setMetricName("mse").evaluate(predictions_df)
    
    return {
        "rmse": float(rmse),
        "mae": float(mae),
        "r2": float(r2),
        "mse": float(mse),
    }


def calculate_mape(predictions_df: DataFrame,
                  label_col: str = "energy_mwh",
                  prediction_col: str = "prediction") -> float:
    
    from pyspark.sql import functions as F
    
    mape_df = predictions_df.select(
        F.abs((F.col(label_col) - F.col(prediction_col)) / F.col(label_col)).alias("ape")
    ).filter(F.col(label_col) != 0)
    
    mape = mape_df.agg(F.mean("ape")).collect()[0][0]
    return float(mape) if mape is not None else float('inf')


def calculate_custom_metrics(predictions_df: DataFrame,
                            label_col: str = "energy_mwh",
                            prediction_col: str = "prediction") -> Dict[str, float]:
    
    from pyspark.sql import functions as F
    
    # Calculate residuals
    with_residuals = predictions_df.withColumn(
        "residual",
        F.abs(F.col(label_col) - F.col(prediction_col))
    )
    
    # Mean residual
    mean_residual = with_residuals.agg(F.mean("residual")).collect()[0][0]
    
    # Max residual (worst prediction)
    max_residual = with_residuals.agg(F.max("residual")).collect()[0][0]
    min_residual = with_residuals.agg(F.min("residual")).collect()[0][0]
    
    # Residual standard deviation (error variance)
    residual_std = with_residuals.agg(F.stddev("residual")).collect()[0][0]
    
    # Median residual (robust bias measure)
    median_residual = with_residuals.approxQuantile("residual", [0.5], 0.01)[0]
    
    # Prediction count
    total_predictions = predictions_df.count()
    
    return {
        "mean_residual": float(mean_residual) if mean_residual else 0.0,
        "median_residual": float(median_residual) if median_residual else 0.0,
        "residual_std": float(residual_std) if residual_std else 0.0,
        "max_residual": float(max_residual) if max_residual else 0.0,
        "min_residual": float(min_residual) if min_residual else 0.0,
        "prediction_count": int(total_predictions),
    }


def evaluate_model(predictions_df: DataFrame,
                  label_col: str = "energy_mwh",
                  prediction_col: str = "prediction",
                  include_custom: bool = True) -> Dict[str, float]:
    
    metrics = calculate_regression_metrics(predictions_df, label_col, prediction_col)
    
    # Add MAPE
    try:
        metrics["mape"] = calculate_mape(predictions_df, label_col, prediction_col)
    except Exception as e:
        print(f"Warning: Could not calculate MAPE: {e}")
        metrics["mape"] = float('inf')
    
    # Add custom metrics
    if include_custom:
        custom = calculate_custom_metrics(predictions_df, label_col, prediction_col)
        metrics.update(custom)
    
    return metrics


def print_metrics(metrics: Dict[str, float]) -> None:
    
    print("\n" + "="*50)
    print("MODEL EVALUATION METRICS")
    print("="*50)
    
    for name, value in sorted(metrics.items()):
        if isinstance(value, float):
            print(f"{name:30s}: {value:.4f}")
        else:
            print(f"{name:30s}: {value}")
    
    print("="*50 + "\n")
