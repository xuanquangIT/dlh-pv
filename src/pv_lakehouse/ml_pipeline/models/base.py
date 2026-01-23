"""Abstract base class for ML models.

Defines the interface that all models must implement.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame


class BaseModel(ABC):
    """Abstract base class for ML models."""
    
    def __init__(self, config):
        """Initialize model with configuration.
        
        Args:
            config: ModelConfig instance
        """
        self.config = config
        self.pipeline_model = None
    
    @abstractmethod
    def build_pipeline(self, feature_cols: list[str], target_col: str):
        """Build the ML pipeline with preprocessing and model.
        
        Args:
            feature_cols: List of feature column names
            target_col: Target column name
            
        Returns:
            PySpark Pipeline object
        """
        pass
    
    @abstractmethod
    def train(self, train_df: DataFrame, feature_cols: list[str], target_col: str):
        """Train the model on training data.
        
        Args:
            train_df: Training DataFrame
            feature_cols: List of feature column names
            target_col: Target column name
            
        Returns:
            Trained pipeline model
        """
        pass
    
    def predict(self, df: DataFrame) -> DataFrame:
        """Make predictions on new data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with predictions
        """
        if self.pipeline_model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        return self.pipeline_model.transform(df)
    
    def get_feature_importance(self) -> dict[str, float]:
        """Get feature importance from trained model.
        
        Returns:
            Dictionary mapping feature names to importance scores
        """
        return {}
    
    def save(self, path: str) -> None:
        """Save model to disk.
        
        Args:
            path: Path to save model
        """
        if self.pipeline_model is None:
            raise ValueError("No trained model to save")
        
        self.pipeline_model.save(path)
    
    @classmethod
    def load(cls, path: str):
        """Load model from disk.
        
        Args:
            path: Path to load model from
            
        Returns:
            Loaded model instance
        """
        return PipelineModel.load(path)
