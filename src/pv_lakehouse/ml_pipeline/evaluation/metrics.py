from __future__ import annotations

import logging
from typing import Dict
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


# Constants for better maintainability
DEFAULT_LABEL_COL = "energy_mwh"
DEFAULT_PREDICTION_COL = "prediction"
LOG_SEPARATOR_LENGTH = 50
METRIC_NAME_WIDTH = 30


def calculate_regression_metrics(predictions_df: DataFrame, 
                                 label_col: str = DEFAULT_LABEL_COL,
                                 prediction_col: str = DEFAULT_PREDICTION_COL
                                 ) -> Dict[str, float]:
    
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
                  label_col: str = DEFAULT_LABEL_COL,
                  prediction_col: str = DEFAULT_PREDICTION_COL
                  ) -> float:
    
    from pyspark.sql import functions as F
    
    try:
        mape_df = predictions_df.select(
            F.abs((F.col(label_col) - F.col(prediction_col)) / F.col(label_col)).alias("ape")
        ).filter(F.col(label_col) != 0)
        
        result = mape_df.agg(F.mean("ape")).collect()
        if not result or result[0][0] is None:
            return float('inf')
        mape = result[0][0]
        return float(mape) if mape is not None else float('inf')
    except (AnalysisException, IndexError, TypeError) as e:
        logger.warning(f"Error calculating MAPE: {e}")
        return float('inf')


def calculate_custom_metrics(predictions_df: DataFrame,
                            label_col: str = DEFAULT_LABEL_COL,
                            prediction_col: str = DEFAULT_PREDICTION_COL
                            ) -> Dict[str, float]:
    
    from pyspark.sql import functions as F
    
    try:
        # Calculate residuals
        with_residuals = predictions_df.withColumn(
            "residual",
            F.abs(F.col(label_col) - F.col(prediction_col))
        )
        
        # Calculate all metrics with batched aggregations for efficiency
        stats_result = with_residuals.agg(
            F.mean("residual").alias("mean_residual"),
            F.max("residual").alias("max_residual"),
            F.min("residual").alias("min_residual"),
            F.stddev("residual").alias("residual_std")
        ).collect()
        
        if not stats_result:
            raise ValueError("Unable to calculate residual statistics")
            
        stats = stats_result[0]
        mean_residual = stats["mean_residual"]
        max_residual = stats["max_residual"]
        min_residual = stats["min_residual"]
        residual_std = stats["residual_std"]
        
        # Median residual (robust bias measure)
        median_result = with_residuals.approxQuantile("residual", [0.5], 0.01)
        median_residual = median_result[0] if median_result else 0.0
        
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
    except (AnalysisException, IndexError, TypeError, ValueError) as e:
        logger.error(f"Error calculating custom metrics: {e}")
        return {
            "mean_residual": 0.0,
            "median_residual": 0.0,
            "residual_std": 0.0,
            "max_residual": 0.0,
            "min_residual": 0.0,
            "prediction_count": 0,
        }


def evaluate_model(predictions_df: DataFrame,
                  label_col: str = DEFAULT_LABEL_COL,
                  prediction_col: str = DEFAULT_PREDICTION_COL,
                  include_custom: bool = True) -> Dict[str, float]:
    
    metrics = calculate_regression_metrics(predictions_df, label_col, prediction_col)
    
    # Add MAPE (error handling is now in calculate_mape function)
    metrics["mape"] = calculate_mape(predictions_df, label_col, prediction_col)
    
    # Add custom metrics
    if include_custom:
        custom = calculate_custom_metrics(predictions_df, label_col, prediction_col)
        metrics.update(custom)
    
    return metrics


def log_metrics(metrics: Dict[str, float]) -> None:
    """Log model evaluation metrics using proper logging instead of print statements."""
    
    logger.info("MODEL EVALUATION METRICS")
    logger.info("=" * LOG_SEPARATOR_LENGTH)
    
    for name, value in sorted(metrics.items()):
        if isinstance(value, float):
            logger.info(f"{name:{METRIC_NAME_WIDTH}s}: {value:.4f}")
        else:
            logger.info(f"{name:{METRIC_NAME_WIDTH}s}: {value}")
    
    logger.info("=" * LOG_SEPARATOR_LENGTH)
