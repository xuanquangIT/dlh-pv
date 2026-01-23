"""Regression model implementations for solar energy forecasting."""
from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor
from pyspark.sql import DataFrame

from .base import BaseModel


class DecisionTreeRegressorModel(BaseModel):
    """Decision Tree Regressor for energy forecasting."""
    
    def build_pipeline(self, feature_cols: list[str], target_col: str):
        """Build Decision Tree regression pipeline."""
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
        """Train the Decision Tree model."""
        print(f"Training Decision Tree Regressor...")
        print(f"Features: {len(feature_cols)}, Target: {target_col}")
        
        pipeline = self.build_pipeline(feature_cols, target_col)
        self.pipeline_model = pipeline.fit(train_df)
        
        print("Training complete")
        return self.pipeline_model
    
    def get_feature_importance(self) -> dict[str, float]:
        """Extract feature importance from trained Decision Tree."""
        if self.pipeline_model is None:
            return {}
        
        # Get the trained model from pipeline
        stages = self.pipeline_model.stages
        if len(stages) < 2:
            return {}
        
        assembler = stages[0]
        tree_model = stages[1]
        
        if hasattr(tree_model, 'featureImportances'):
            importances = tree_model.featureImportances.toArray()
            feature_names = assembler.getInputCols()
            
            return {
                name: float(importance)
                for name, importance in zip(feature_names, importances)
            }
        
        return {}


class GBTRegressorModel(BaseModel):
    """Gradient Boosted Trees Regressor for energy forecasting."""
    
    def build_pipeline(self, feature_cols: list[str], target_col: str):
        """Build GBT regression pipeline."""
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
            subsamplingRate=self.config.gbt_subsample_rate,
            featureSubsetStrategy=self.config.gbt_feature_subset_strategy,
            seed=self.config.seed
        )
        
        return Pipeline(stages=[assembler, regressor])
    
    def train(self, train_df: DataFrame, feature_cols: list[str], target_col: str):
        """Train the GBT model."""
        print(f"Training Gradient Boosted Trees Regressor...")
        print(f"Features: {len(feature_cols)}, Target: {target_col}")
        
        pipeline = self.build_pipeline(feature_cols, target_col)
        self.pipeline_model = pipeline.fit(train_df)
        
        print("Training complete")
        return self.pipeline_model
    
    def get_feature_importance(self) -> dict[str, float]:
        """Extract feature importance from trained GBT."""
        if self.pipeline_model is None:
            return {}
        
        stages = self.pipeline_model.stages
        if len(stages) < 2:
            return {}
        
        assembler = stages[0]
        gbt_model = stages[1]
        
        if hasattr(gbt_model, 'featureImportances'):
            importances = gbt_model.featureImportances.toArray()
            feature_names = assembler.getInputCols()
            
            return {
                name: float(importance)
                for name, importance in zip(feature_names, importances)
            }
        
        return {}


def create_model(config):
    """Factory function to create model based on config.
    
    Args:
        config: ModelConfig instance
        
    Returns:
        BaseModel instance
    """
    if config.model_type == "decision_tree":
        return DecisionTreeRegressorModel(config)
    elif config.model_type == "gbt":
        return GBTRegressorModel(config)
    else:
        raise ValueError(f"Unknown model type: {config.model_type}")
