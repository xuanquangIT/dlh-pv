from __future__ import annotations

import logging
from typing import Any, Protocol

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor
from pyspark.sql import DataFrame

from .base import BaseModel

logger = logging.getLogger(__name__)

# Column name constants
FEATURES_COL = "features"
PREDICTION_COL = "prediction"
HANDLE_INVALID_SKIP = "skip"


class ModelConfig(Protocol):
    """Protocol defining the interface for model configuration."""
    model_type: str


class DecisionTreeRegressorModel(BaseModel):
    """
    Decision Tree Regressor model implementation using PySpark ML.
    
    Provides functionality to build and train decision tree regression models
    with configurable hyperparameters.
    """
    
    def build_pipeline(self, feature_cols: list[str], target_col: str) -> Pipeline:
        """
        Build a PySpark ML pipeline with feature assembly and decision tree regression.
        
        Args:
            feature_cols: List of column names to use as features
            target_col: Name of the target column for regression
            
        Returns:
            Pipeline: Configured PySpark ML pipeline
        """
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol=FEATURES_COL,
            handleInvalid=HANDLE_INVALID_SKIP
        )
        
        regressor = DecisionTreeRegressor(
            featuresCol=FEATURES_COL,
            labelCol=target_col,
            predictionCol=PREDICTION_COL,
            maxDepth=self.config.get('max_depth', 20),
            minInstancesPerNode=self.config.get('min_instances_per_node', 20),
            maxBins=self.config.get('max_bins', 64),
            minInfoGain=self.config.get('min_info_gain', 0.0),
            seed=self.config.get('seed', 42)
        )
        
        return Pipeline(stages=[assembler, regressor])
    
    def train(self, train_df: DataFrame, feature_cols: list[str], target_col: str) -> PipelineModel:
        # Input validation
        if not feature_cols:
            raise ValueError("feature_cols cannot be empty")
        if not target_col:
            raise ValueError("target_col cannot be empty or None")
        if train_df is None:
            raise ValueError("train_df cannot be None")
        
        try:
            logger.info("Training Decision Tree Regressor")
            logger.info(f"Features: {len(feature_cols)}, Target: {target_col}")
            
            pipeline = self.build_pipeline(feature_cols, target_col)
            self.pipeline_model = pipeline.fit(train_df)
            
            logger.info("Training complete")
            return self.pipeline_model
        except Exception as e:
            logger.error(f"Decision Tree training failed: {str(e)}")
            raise RuntimeError(f"Model training failed: {str(e)}") from e


class GBTRegressorModel(BaseModel):
    """
    Gradient Boosted Trees Regressor model implementation using PySpark ML.
    
    Provides functionality to build and train GBT regression models
    with configurable hyperparameters.
    """
    
    def build_pipeline(self, feature_cols: list[str], target_col: str) -> Pipeline:
        """
        Build a PySpark ML pipeline with feature assembly and GBT regression.
        
        Args:
            feature_cols: List of column names to use as features
            target_col: Name of the target column for regression
            
        Returns:
            Pipeline: Configured PySpark ML pipeline
        """
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol=FEATURES_COL,
            handleInvalid=HANDLE_INVALID_SKIP
        )
        
        regressor = GBTRegressor(
            featuresCol=FEATURES_COL,
            labelCol=target_col,
            predictionCol=PREDICTION_COL,
            maxDepth=self.config.get('max_depth', 20),
            maxIter=self.config.get('gbt_max_iter', 120),
            stepSize=self.config.get('gbt_step_size', 0.1),
            minInstancesPerNode=self.config.get('min_instances_per_node', 20),
            maxBins=self.config.get('max_bins', 64),
            subsamplingRate=self.config.get('gbt_subsample_rate', 0.8),
            featureSubsetStrategy=self.config.get('gbt_feature_subset_strategy', 'auto'),
            minInfoGain=self.config.get('min_info_gain', 0.0),
            seed=self.config.get('seed', 42)
        )
        
        return Pipeline(stages=[assembler, regressor])
    
    def train(self, train_df: DataFrame, feature_cols: list[str], target_col: str) -> PipelineModel:
        # Input validation
        if not feature_cols:
            raise ValueError("feature_cols cannot be empty")
        if not target_col:
            raise ValueError("target_col cannot be empty or None")
        if train_df is None:
            raise ValueError("train_df cannot be None")
        
        try:
            logger.info("Training Gradient Boosted Trees Regressor")
            logger.info(f"Features: {len(feature_cols)}, Target: {target_col}")
            
            pipeline = self.build_pipeline(feature_cols, target_col)
            self.pipeline_model = pipeline.fit(train_df)
            
            logger.info("Training complete")
            return self.pipeline_model
        except Exception as e:
            logger.error(f"GBT training failed: {str(e)}")
            raise RuntimeError(f"Model training failed: {str(e)}") from e


def create_model(config: ModelConfig) -> BaseModel:
    """
    Factory function to create model instances based on configuration.
    
    Args:
        config: Configuration object containing model_type and other parameters
        
    Returns:
        BaseModel: Instance of the appropriate model class
        
    Raises:
        ValueError: If config is None, missing model_type, or unknown model_type
    """
    # Input validation
    if config is None:
        raise ValueError("config cannot be None")
    
    # Handle both dict and dataclass configs
    if hasattr(config, 'model_type'):
        model_type = config.model_type
    elif isinstance(config, dict):
        model_type = config.get('model_type')
        if model_type is None:
            raise ValueError("config must have 'model_type' key")
    else:
        raise ValueError("config must have 'model_type' attribute or be a dictionary with 'model_type' key")
    
    if model_type == "decision_tree":
        return DecisionTreeRegressorModel(config)
    elif model_type == "gbt":
        return GBTRegressorModel(config)
    else:
        raise ValueError(f"Unknown model type: {model_type}")
