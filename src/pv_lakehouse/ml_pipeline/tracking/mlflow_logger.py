"""MLflow implementation of experiment tracker.

All MLflow imports and calls are isolated in this module.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional
from contextlib import contextmanager

import mlflow
import mlflow.spark

from . import ExperimentTracker


class MLflowTracker(ExperimentTracker):
    """MLflow-based experiment tracker."""
    
    def __init__(self, tracking_uri: str, experiment_name: str):
        """Initialize MLflow tracker.
        
        Args:
            tracking_uri: MLflow tracking server URI
            experiment_name: Name of the experiment
        """
        self.tracking_uri = tracking_uri
        self.experiment_name = experiment_name
        
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)
        
        self._active_run = None
    
    def start_run(self, run_name: Optional[str] = None):
        """Start a new MLflow run."""
        self._active_run = mlflow.start_run(run_name=run_name)
        return self._active_run
    
    def end_run(self):
        """End the current MLflow run."""
        if self._active_run:
            mlflow.end_run()
            self._active_run = None
    
    def log_params(self, params: Dict[str, Any]):
        """Log parameters to MLflow."""
        for key, value in params.items():
            try:
                mlflow.log_param(key, value)
            except Exception as e:
                print(f"Warning: Failed to log param {key}: {e}")
    
    def log_metrics(self, metrics: Dict[str, float]):
        """Log metrics to MLflow."""
        for key, value in metrics.items():
            try:
                mlflow.log_metric(key, float(value))
            except Exception as e:
                print(f"Warning: Failed to log metric {key}: {e}")
    
    def log_metric(self, key: str, value: float):
        """Log a single metric to MLflow."""
        try:
            mlflow.log_metric(key, float(value))
        except Exception as e:
            print(f"Warning: Failed to log metric {key}: {e}")
    
    def log_artifact(self, file_path: str, artifact_path: Optional[str] = None):
        """Log an artifact file to MLflow."""
        try:
            mlflow.log_artifact(file_path, artifact_path)
        except Exception as e:
            print(f"Warning: Failed to log artifact {file_path}: {e}")
    
    def log_model(self, model: Any, artifact_path: str):
        """Log a PySpark model to MLflow.
        
        Args:
            model: PySpark Pipeline or Model
            artifact_path: Path within MLflow artifacts
        """
        try:
            dfs_tmpdir = os.environ.get("MLFLOW_DFS_TMP", None)
            mlflow.spark.log_model(
                model,
                artifact_path,
                dfs_tmpdir=dfs_tmpdir
            )
        except Exception as e:
            print(f"Warning: Failed to log model: {e}")
    
    def set_tags(self, tags: Dict[str, str]):
        """Set tags for the current run."""
        for key, value in tags.items():
            try:
                mlflow.set_tag(key, value)
            except Exception as e:
                print(f"Warning: Failed to set tag {key}: {e}")
    
    @contextmanager
    def run_context(self, run_name: Optional[str] = None):
        """Context manager for MLflow runs.
        
        Usage:
            with tracker.run_context("my_run"):
                # training code
                pass
        """
        try:
            self.start_run(run_name)
            yield self
        finally:
            self.end_run()


def create_tracker(tracking_uri: str, 
                  experiment_name: str,
                  use_mlflow: bool = True) -> ExperimentTracker:
    """Factory function to create an experiment tracker.
    
    Args:
        tracking_uri: MLflow tracking server URI
        experiment_name: Experiment name
        use_mlflow: Whether to use MLflow (False for testing)
        
    Returns:
        ExperimentTracker instance
    """
    if use_mlflow:
        return MLflowTracker(tracking_uri, experiment_name)
    else:
        from . import NoOpTracker
        return NoOpTracker()
