from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Union, Type, TYPE_CHECKING
from contextlib import contextmanager

if TYPE_CHECKING:
    import mlflow
    import mlflow.spark
    import mlflow.exceptions

class MLflowDependencyManager:
    def __init__(self):
        self._mlflow_module = None
        self._spark_module = None
        self._exceptions_module = None
        self._available = self._check_availability()
    
    def _check_availability(self) -> bool:
        try:
            import mlflow
            import mlflow.spark
            import mlflow.exceptions
            self._mlflow_module = mlflow
            self._spark_module = mlflow.spark
            self._exceptions_module = mlflow.exceptions
            return True
        except ImportError:
            return False
    
    @property
    def available(self) -> bool:
        return self._available
    
    @property
    def mlflow(self) -> 'mlflow':  
        if not self._available:
            raise RuntimeError("MLflow not available")
        return self._mlflow_module
    
    @property
    def spark(self) -> 'mlflow.spark':  
        if not self._available:
            raise RuntimeError("MLflow not available")
        return self._spark_module
    
    @property
    def exceptions(self) -> 'mlflow.exceptions':  
        if not self._available:
            raise RuntimeError("MLflow not available")
        return self._exceptions_module

from . import ExperimentTracker  

logger = logging.getLogger(__name__)

# Constants
ERROR_TAG_MAX_LENGTH = 250

def _validate_dfs_tmpdir(dfs_tmpdir: Optional[str]) -> Optional[str]:
    """Safely validate MLFLOW_DFS_TMP directory."""
    if not dfs_tmpdir:
        return None
    
    try:
        if os.path.isdir(dfs_tmpdir):
            return dfs_tmpdir
    except (OSError, TypeError) as e:  
        logger.warning(f"Error checking MLFLOW_DFS_TMP directory: {e}")
    
    logger.warning(f"Invalid MLFLOW_DFS_TMP directory: {dfs_tmpdir}")
    return None


class MLflowTracker(ExperimentTracker):
    
    @classmethod
    def is_available(cls) -> bool:
        """Check if MLflow is available before instantiation."""
        try:
            import mlflow
            return True
        except ImportError:
            return False
    
    def __init__(self, tracking_uri: str, experiment_name: str):
        self._mlflow_deps = MLflowDependencyManager()
        
        if not self._mlflow_deps.available:
            raise RuntimeError(
                "MLflow is not available. Install MLflow to use MLflowTracker: "
                "pip install mlflow"
            )
       
        if not tracking_uri or not isinstance(tracking_uri, str):
            raise ValueError("tracking_uri must be a non-empty string")
        if not experiment_name or not isinstance(experiment_name, str):
            raise ValueError("experiment_name must be a non-empty string")
            
        self.tracking_uri = tracking_uri
        self.experiment_name = experiment_name
        
        try:
            self._mlflow_deps.mlflow.set_tracking_uri(tracking_uri)  
            self._mlflow_deps.mlflow.set_experiment(experiment_name) 
        except (self._mlflow_deps.exceptions.MlflowException, ConnectionError, ValueError) as e:  
            logger.error("Failed to initialize MLflow tracking")
            raise RuntimeError(f"MLflow initialization failed: {e}") from e
        
        self._active_run = None
    
    def start_run(self, run_name: Optional[str] = None):
        try:
            self._active_run = self._mlflow_deps.mlflow.start_run(run_name=run_name)
            return self._active_run
        except (self._mlflow_deps.exceptions.MlflowException, RuntimeError) as e:  
            logger.error(f"Failed to start MLflow run: {e}")
            raise RuntimeError(f"Failed to start run: {e}") from e
    
    def end_run(self, status: str = "FINISHED"):
        if self._active_run:
            try:
                self._mlflow_deps.mlflow.end_run(status=status)
                self._active_run = None
            except self._mlflow_deps.exceptions.MlflowException as e:  
                logger.error(f"Failed to end MLflow run: {e}")
                # Don't raise here as this is cleanup
    
    def log_params(self, params: Dict[str, Any]):
        for key, value in params.items():
            try:
                self._mlflow_deps.mlflow.log_param(key, value)
            except (self._mlflow_deps.exceptions.MlflowException, ValueError, TypeError) as e:  
                logger.warning(f"Failed to log param {key}: {e}", exc_info=True)
    
    def log_metrics(self, metrics: Dict[str, float]):
        for key, value in metrics.items():
            try:
                self._mlflow_deps.mlflow.log_metric(key, float(value))
            except (self._mlflow_deps.exceptions.MlflowException, ValueError, TypeError) as e:  
                logger.warning(f"Failed to log metric {key}: {e}", exc_info=True)
    
    def log_metric(self, key: str, value: float):
        try:
            self._mlflow_deps.mlflow.log_metric(key, float(value))
        except (self._mlflow_deps.exceptions.MlflowException, ValueError, TypeError) as e:  
            logger.warning(f"Failed to log metric {key}: {e}", exc_info=True)
    
    def log_artifact(self, file_path: str, artifact_path: Optional[str] = None):
        if not os.path.exists(file_path):
            logger.error(f"Artifact file does not exist: {file_path}")
            return
            
        try:
            self._mlflow_deps.mlflow.log_artifact(file_path, artifact_path)
        except (self._mlflow_deps.exceptions.MlflowException, OSError, IOError) as e:  
            logger.warning(f"Failed to log artifact {file_path}: {e}", exc_info=True)
    
    def log_model(self, model: Any, artifact_path: str):
        try:
            dfs_tmpdir = _validate_dfs_tmpdir(os.environ.get("MLFLOW_DFS_TMP"))
            
            self._mlflow_deps.spark.log_model(  
                model,
                artifact_path,
                dfs_tmpdir=dfs_tmpdir
            )
        except (self._mlflow_deps.exceptions.MlflowException, AttributeError, TypeError) as e:  
            logger.warning(f"Failed to log model {artifact_path}: {e}", exc_info=True)
    
    def set_tags(self, tags: Dict[str, Union[str, int, float]]):
        for key, value in tags.items():
            try:
                self._mlflow_deps.mlflow.set_tag(key, str(value))
            except (self._mlflow_deps.exceptions.MlflowException, ValueError, TypeError) as e:  
                logger.warning(f"Failed to set tag {key}: {e}", exc_info=True)
    
    @contextmanager
    def run_context(self, run_name: Optional[str] = None):
        run_status = "FINISHED"
        try:
            self.start_run(run_name)
            yield self
        except (RuntimeError, self._mlflow_deps.exceptions.MlflowException) as e:  
            run_status = "FAILED"
            # Log the exception
            try:
                self._mlflow_deps.mlflow.set_tag("error", str(e)[:ERROR_TAG_MAX_LENGTH])
            except self._mlflow_deps.exceptions.MlflowException as tag_error:  
                logger.warning(f"Failed to set error tag: {tag_error}", exc_info=True)
            raise
        except Exception as e:
            run_status = "FAILED"
            logger.error(f"Unexpected error in run context: {e}", exc_info=True)
            try:
                self._mlflow_deps.mlflow.set_tag("error", f"Unexpected error: {str(e)[:ERROR_TAG_MAX_LENGTH]}")
            except self._mlflow_deps.exceptions.MlflowException as tag_error:  
                logger.warning(f"Failed to set error tag: {tag_error}", exc_info=True)
            raise
        finally:
            self.end_run(status=run_status)


def create_tracker(tracking_uri: str, 
                  experiment_name: str,
                  use_mlflow: bool = True) -> ExperimentTracker:
    if use_mlflow:
        # Check availability before attempting to create tracker
        if not MLflowTracker.is_available():
            raise ImportError(
                "MLflow is required but not available. "
                "Install with: pip install mlflow"
            )
        return MLflowTracker(tracking_uri, experiment_name)
    else:
        try:
            from .noop_tracker import NoOpTracker  
            return NoOpTracker()
        except ImportError as e:
            logger.error("Failed to import NoOpTracker")
            raise ImportError("NoOpTracker not available") from e
