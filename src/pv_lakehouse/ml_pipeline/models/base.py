from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame


class BaseModel(ABC):
    
    def __init__(self, config):
        
        self.config = config
        self.pipeline_model = None
    
    @abstractmethod
    def build_pipeline(self, feature_cols: list[str], target_col: str):
        
        pass
    
    @abstractmethod
    def train(self, train_df: DataFrame, feature_cols: list[str], target_col: str):
        
        pass
    
    def predict(self, df: DataFrame) -> DataFrame:
        
        if self.pipeline_model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        return self.pipeline_model.transform(df)
    
    def get_feature_importance(self) -> dict[str, float]:
        
        if self.pipeline_model is None:
            return {}
        
        stages = self.pipeline_model.stages
        if len(stages) < 2:
            return {}
        
        assembler = stages[0]
        model = stages[1]
        
        if hasattr(model, 'featureImportances'):
            importances = model.featureImportances.toArray()
            feature_names = assembler.getInputCols()
            
            return {
                name: float(importance)
                for name, importance in zip(feature_names, importances)
            }
        
        return {}
    
    def save(self, path: str) -> None:
        
        if self.pipeline_model is None:
            raise ValueError("No trained model to save")
        
        self.pipeline_model.save(path)
    
    @classmethod
    def load(cls, path: str):
        
        return PipelineModel.load(path)
