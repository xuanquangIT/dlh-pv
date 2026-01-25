"""Experiment tracking abstraction layer.

Isolates all MLflow-specific code from the rest of the pipeline.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pathlib import Path


class ExperimentTracker(ABC):
    """Abstract interface for experiment tracking."""
    
    @abstractmethod
    def start_run(self, run_name: Optional[str] = None):
        """Start a new experiment run."""
        pass
    
    @abstractmethod
    def end_run(self):
        """End the current experiment run."""
        pass
    
    @abstractmethod
    def log_params(self, params: Dict[str, Any]):
        """Log parameters."""
        pass
    
    @abstractmethod
    def log_metrics(self, metrics: Dict[str, float]):
        """Log metrics."""
        pass
    
    @abstractmethod
    def log_metric(self, key: str, value: float):
        """Log a single metric."""
        pass
    
    @abstractmethod
    def log_artifact(self, file_path: str, artifact_path: Optional[str] = None):
        """Log an artifact file."""
        pass
    
    @abstractmethod
    def log_model(self, model: Any, artifact_path: str):
        """Log a trained model."""
        pass
    
    @abstractmethod
    def set_tags(self, tags: Dict[str, str]):
        """Set tags for the run."""
        pass


class NoOpTracker(ExperimentTracker):
    """No-op tracker for testing without MLflow."""
    
    def start_run(self, run_name: Optional[str] = None):
        print(f"[NoOp] Starting run: {run_name}")
    
    def end_run(self):
        print("[NoOp] Ending run")
    
    def log_params(self, params: Dict[str, Any]):
        print(f"[NoOp] Logging params: {list(params.keys())}")
    
    def log_metrics(self, metrics: Dict[str, float]):
        print(f"[NoOp] Logging metrics: {list(metrics.keys())}")
    
    def log_metric(self, key: str, value: float):
        print(f"[NoOp] Logging metric: {key}={value}")
    
    def log_artifact(self, file_path: str, artifact_path: Optional[str] = None):
        print(f"[NoOp] Logging artifact: {file_path}")
    
    def log_model(self, model: Any, artifact_path: str):
        print(f"[NoOp] Logging model to: {artifact_path}")
    
    def set_tags(self, tags: Dict[str, str]):
        print(f"[NoOp] Setting tags: {list(tags.keys())}")
