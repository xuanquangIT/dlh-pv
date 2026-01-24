from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional
from contextlib import contextmanager

import mlflow
import mlflow.spark

from . import ExperimentTracker

logger = logging.getLogger(__name__)


class MLflowTracker(ExperimentTracker):
    
    def __init__(self, tracking_uri: str, experiment_name: str):
        self.tracking_uri = tracking_uri
        self.experiment_name = experiment_name
        
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)
        
        self._active_run = None
    
    def start_run(self, run_name: Optional[str] = None):
        self._active_run = mlflow.start_run(run_name=run_name)
        return self._active_run
    
    def end_run(self, status: str = "FINISHED"):
        if self._active_run:
            mlflow.end_run(status=status)
            self._active_run = None
    
    def log_params(self, params: Dict[str, Any]):
        for key, value in params.items():
            try:
                mlflow.log_param(key, value)
            except Exception as e:
                logger.warning(f"Failed to log param {key}: {e}", exc_info=True)
    
    def log_metrics(self, metrics: Dict[str, float]):
        for key, value in metrics.items():
            try:
                mlflow.log_metric(key, float(value))
            except Exception as e:
                logger.warning(f"Failed to log metric {key}: {e}", exc_info=True)
    
    def log_metric(self, key: str, value: float):
        try:
            mlflow.log_metric(key, float(value))
        except Exception as e:
            logger.warning(f"Failed to log metric {key}: {e}", exc_info=True)
    
    def log_artifact(self, file_path: str, artifact_path: Optional[str] = None):
        try:
            mlflow.log_artifact(file_path, artifact_path)
        except Exception as e:
            logger.warning(f"Failed to log artifact {file_path}: {e}", exc_info=True)
    
    def log_model(self, model: Any, artifact_path: str):
        try:
            dfs_tmpdir = os.environ.get("MLFLOW_DFS_TMP", None)
            mlflow.spark.log_model(
                model,
                artifact_path,
                dfs_tmpdir=dfs_tmpdir
            )
        except Exception as e:
            logger.warning(f"Failed to log model: {e}", exc_info=True)
    
    def set_tags(self, tags: Dict[str, str]):
        for key, value in tags.items():
            try:
                mlflow.set_tag(key, value)
            except Exception as e:
                logger.warning(f"Failed to set tag {key}: {e}", exc_info=True)
    
    @contextmanager
    def run_context(self, run_name: Optional[str] = None):
        run_status = "FINISHED"
        try:
            self.start_run(run_name)
            yield self
        except Exception as e:
            run_status = "FAILED"
            # Log the exception
            try:
                mlflow.set_tag("error", str(e)[:250])
            except Exception as tag_error:
                logger.warning(f"Failed to set error tag: {tag_error}", exc_info=True)
            raise
        finally:
            self.end_run(status=run_status)


def create_tracker(tracking_uri: str, 
                  experiment_name: str,
                  use_mlflow: bool = True) -> ExperimentTracker:
    if use_mlflow:
        return MLflowTracker(tracking_uri, experiment_name)
    else:
        from . import NoOpTracker
        return NoOpTracker()
