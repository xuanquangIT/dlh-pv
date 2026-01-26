from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Union
from dataclasses import asdict, is_dataclass
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__) 


class BaseModel(ABC):
    
    def __init__(self, config: Union[dict[str, Any], Any]) -> None:  
        # Handle both dict and dataclass configs
        if is_dataclass(config):
            self.config = asdict(config)
        elif isinstance(config, dict):
            self.config = config
        else:
            raise TypeError("Config must be a dictionary or dataclass")
        
        self.pipeline_model: PipelineModel | None = None
    
    @abstractmethod
    def build_pipeline(self, feature_cols: list[str], target_col: str):
        pass
    
    @abstractmethod
    def train(self, train_df: DataFrame, feature_cols: list[str], target_col: str): 
        pass
    
    def predict(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise TypeError("Input must be a PySpark DataFrame")
        
        if self.pipeline_model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        try:
            return self.pipeline_model.transform(df)  
        except Exception as e:
            logger.error(f"Prediction failed: {str(e)}")
            raise RuntimeError(f"Failed to make predictions: {str(e)}") from e
    
    def get_feature_importance(self) -> dict[str, float]: 
        
        if self.pipeline_model is None:
            return {}
        
        stages = self.pipeline_model.stages
        if len(stages) < 2:
            return {}
        
        # Validate assembler (first stage should have getInputCols method)
        assembler = stages[0]
        if not hasattr(assembler, 'getInputCols'):
            return {}
        
        # Validate model (second stage should have featureImportances)
        model = stages[1]
        if not hasattr(model, 'featureImportances'):
            return {}
        
        try:
            importances = model.featureImportances.toArray()
            feature_names = assembler.getInputCols()
            
            if len(feature_names) != len(importances):
                logger.warning("Feature names and importance arrays have different lengths")
                return {}
            
            if len(importances) > 10000:
                logger.warning(f"Large number of features ({len(importances)}), consider optimization")
            
            return {
                name: float(importance)
                for name, importance in zip(feature_names, importances)
            }
        except (AttributeError, IndexError) as e:  
            logger.warning(f"Could not extract feature importance: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error in feature importance calculation: {str(e)}")
            return {}
    
    def save(self, path: str) -> None:
        
        if not isinstance(path, str):
            raise TypeError("Path must be a string")
        if not path.strip():
            raise ValueError("Path cannot be empty")
        
        if self.pipeline_model is None:
            raise ValueError("No trained model to save")
        
        try:
            self.pipeline_model.save(path)  
            logger.info(f"Model saved successfully to {path}")
        except Exception as e:
            logger.error(f"Failed to save model to {path}: {str(e)}")
            raise RuntimeError(f"Failed to save model: {str(e)}") from e
    
    @classmethod
    def load(cls, path: str) -> PipelineModel:
        if not isinstance(path, str):
            raise TypeError("Path must be a string")
        if not path.strip():
            raise ValueError("Path cannot be empty")
        
        try:
            model = PipelineModel.load(path)  
            logger.info(f"Model loaded successfully from {path}")
            return model
        except Exception as e:
            logger.error(f"Failed to load model from {path}: {str(e)}")
            raise RuntimeError(f"Failed to load model: {str(e)}") from e
