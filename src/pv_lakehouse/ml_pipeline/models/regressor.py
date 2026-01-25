from __future__ import annotations

import logging

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor
from pyspark.sql import DataFrame

from .base import BaseModel

logger = logging.getLogger(__name__)


class DecisionTreeRegressorModel(BaseModel):
    
    def build_pipeline(self, feature_cols: list[str], target_col: str):
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features",
            handleInvalid="skip"
        )
        
        regressor = DecisionTreeRegressor(
            featuresCol="features",
            labelCol=target_col,
            predictionCol="prediction",
            maxDepth=self.config.max_depth,
            minInstancesPerNode=self.config.min_instances_per_node,
            maxBins=self.config.max_bins,
            minInfoGain=self.config.min_info_gain,
            seed=self.config.seed
        )
        
        return Pipeline(stages=[assembler, regressor])
    
    def train(self, train_df: DataFrame, feature_cols: list[str], target_col: str):
        logger.info("Training Decision Tree Regressor")
        logger.info(f"Features: {len(feature_cols)}, Target: {target_col}")
        
        pipeline = self.build_pipeline(feature_cols, target_col)
        self.pipeline_model = pipeline.fit(train_df)
        
        logger.info("Training complete")
        return self.pipeline_model


class GBTRegressorModel(BaseModel):
    
    def build_pipeline(self, feature_cols: list[str], target_col: str):
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features",
            handleInvalid="skip"
        )
        
        regressor = GBTRegressor(
            featuresCol="features",
            labelCol=target_col,
            predictionCol="prediction",
            maxDepth=self.config.max_depth,
            maxIter=self.config.gbt_max_iter,
            stepSize=self.config.gbt_step_size,
            minInstancesPerNode=self.config.min_instances_per_node,
            maxBins=self.config.max_bins,
            subsamplingRate=self.config.gbt_subsample_rate,
            featureSubsetStrategy=self.config.gbt_feature_subset_strategy,
            minInfoGain=self.config.min_info_gain,
            seed=self.config.seed
        )
        
        return Pipeline(stages=[assembler, regressor])
    
    def train(self, train_df: DataFrame, feature_cols: list[str], target_col: str):
        logger.info("Training Gradient Boosted Trees Regressor")
        logger.info(f"Features: {len(feature_cols)}, Target: {target_col}")
        
        pipeline = self.build_pipeline(feature_cols, target_col)
        self.pipeline_model = pipeline.fit(train_df)
        
        logger.info("Training complete")
        return self.pipeline_model


def create_model(config):
    if config.model_type == "decision_tree":
        return DecisionTreeRegressorModel(config)
    elif config.model_type == "gbt":
        return GBTRegressorModel(config)
    else:
        raise ValueError(f"Unknown model type: {config.model_type}")
