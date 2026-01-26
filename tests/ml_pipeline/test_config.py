"""Tests for ML Pipeline configuration."""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

from pv_lakehouse.ml_pipeline.config import (
    FeatureConfig, ModelConfig, TrainingConfig, MLflowConfig, 
    OutputConfig, MLConfig
)


class TestMLConfigYAMLParsing:
    """Test MLConfig YAML parsing functionality."""
    
    @pytest.fixture
    def sample_features_yaml(self):
        """Sample features YAML content."""
        return """
target:
  column: "energy_kwh"

energy_features:
  - "energy_kwh"

temporal_features:
  basic:
    - "hour"
    - "day_of_week"
    - "month"
  cyclical:
    - "hour_sin"
    - "hour_cos"

weather_features:
  primary:
    - "temperature_2m"
    - "relative_humidity_2m"
  derived:
    - "temperature_humidity_interaction"

lag_features:
  - "energy_lag_1h"
  - "energy_lag_24h"

production_features:
  - "production_indicator"

air_quality_features:
  - "aqi"
  - "pm2_5"

feature_engineering:
  min_radiation_threshold: 10.0
  low_energy_threshold: 0.1
  noise_magnitude: 0.0
  noise_ratio: 0.0
"""
    
    @pytest.fixture
    def sample_hyperparams_yaml(self):
        """Sample hyperparameters YAML content."""
        return """
model:
  type: "gbt"

decision_tree:
  max_depth: 20
  min_instances_per_node: 20
  max_bins: 64
  min_info_gain: 0.0
  seed: 42

gbt:
  max_depth: 15
  min_instances_per_node: 15
  max_bins: 32
  min_info_gain: 0.1
  max_iter: 100
  step_size: 0.1
  subsample_rate: 0.8
  feature_subset_strategy: "auto"
  seed: 42

training:
  sample_limit: 50000
  train_ratio: 0.7
  validation_ratio: 0.15
  test_ratio: 0.15
  random_seed: 42
  min_rows_required: 1000

data_split:
  method: "random"

metrics:
  primary: "rmse"
  track:
    - "rmse"
    - "mae"
    - "r2"
    - "mse"

mlflow:
  experiment_name: "pv_solar_regression_test"
  run_name_prefix: "test_run"
  artifact_path: "model"
  log_model: true
  log_artifacts: true

output:
  gold_table: "lh.gold.test_table"
  write_mode: "overwrite"
  enable_predictions_write: true
"""
    
    def test_mlconfig_from_yaml_gbt(self, sample_features_yaml, sample_hyperparams_yaml):
        """Test MLConfig creation from YAML files with GBT model."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write YAML files
            features_path = os.path.join(temp_dir, "features.yaml")
            hyperparams_path = os.path.join(temp_dir, "hyperparams.yaml")
            
            with open(features_path, 'w') as f:
                f.write(sample_features_yaml)
            
            with open(hyperparams_path, 'w') as f:
                f.write(sample_hyperparams_yaml)
            
            # Test parsing
            config = MLConfig.from_yaml(features_path, hyperparams_path)
            
            # Verify features config
            assert config.features.target_column == "energy_kwh"
            assert config.features.energy_features == ["energy_kwh"]
            assert config.features.temporal_basic == ["hour", "day_of_week", "month"]
            assert config.features.temporal_cyclical == ["hour_sin", "hour_cos"]
            assert config.features.weather_primary == ["temperature_2m", "relative_humidity_2m"]
            assert config.features.weather_derived == ["temperature_humidity_interaction"]
            assert config.features.lag_features == ["energy_lag_1h", "energy_lag_24h"]
            assert config.features.production_features == ["production_indicator"]
            assert config.features.air_quality_features == ["aqi", "pm2_5"]
            assert config.features.min_radiation_threshold == 10.0
            assert config.features.low_energy_threshold == 0.1
            
            # Verify model config (GBT)
            assert config.model.model_type == "gbt"
            assert config.model.max_depth == 15
            assert config.model.min_instances_per_node == 15
            assert config.model.max_bins == 32
            assert config.model.min_info_gain == 0.1
            assert config.model.gbt_max_iter == 100
            assert config.model.gbt_step_size == 0.1
            assert config.model.gbt_subsample_rate == 0.8
            assert config.model.gbt_feature_subset_strategy == "auto"
            assert config.model.seed == 42
            
            # Verify training config
            assert config.training.sample_limit == 50000
            assert config.training.train_ratio == 0.7
            assert config.training.validation_ratio == 0.15
            assert config.training.test_ratio == 0.15
            assert config.training.random_seed == 42
            assert config.training.min_rows_required == 1000
            assert config.training.data_split_method == "random"
            assert config.training.primary_metric == "rmse"
            assert config.training.metrics_to_track == ["rmse", "mae", "r2", "mse"]
            
            # Verify MLflow config
            assert config.mlflow.experiment_name == "pv_solar_regression_test"
            assert config.mlflow.run_name_prefix == "test_run"
            assert config.mlflow.artifact_path == "model"
            assert config.mlflow.log_model is True
            assert config.mlflow.log_artifacts is True
            
            # Verify output config
            assert config.output.gold_table == "lh.gold.test_table"
            assert config.output.write_mode == "overwrite"
            assert config.output.enable_predictions_write is True
    
    def test_mlconfig_from_yaml_decision_tree(self, sample_features_yaml):
        """Test MLConfig creation with Decision Tree model."""
        dt_hyperparams_yaml = """
model:
  type: "decision_tree"

decision_tree:
  max_depth: 25
  min_instances_per_node: 25
  max_bins: 48
  min_info_gain: 0.05
  seed: 123

gbt:
  max_depth: 15
  min_instances_per_node: 15
  max_bins: 32
  min_info_gain: 0.1
  max_iter: 100
  step_size: 0.1
  subsample_rate: 0.8
  feature_subset_strategy: "auto"
  seed: 42

training:
  sample_limit: 25000
  train_ratio: 0.8
  validation_ratio: 0.1
  test_ratio: 0.1
  random_seed: 123
  min_rows_required: 500

data_split:
  method: "temporal"

metrics:
  primary: "mae"
  track:
    - "mae"
    - "rmse"

mlflow:
  experiment_name: "dt_test"
  run_name_prefix: "dt_run"
  artifact_path: "dt_model"
  log_model: true
  log_artifacts: false

output:
  gold_table: "lh.gold.dt_test"
  write_mode: "append"
  enable_predictions_write: false
"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write YAML files
            features_path = os.path.join(temp_dir, "features.yaml")
            hyperparams_path = os.path.join(temp_dir, "hyperparams.yaml")
            
            with open(features_path, 'w') as f:
                f.write(sample_features_yaml)
            
            with open(hyperparams_path, 'w') as f:
                f.write(dt_hyperparams_yaml)
            
            # Test parsing
            config = MLConfig.from_yaml(features_path, hyperparams_path)
            
            # Verify model config (Decision Tree)
            assert config.model.model_type == "decision_tree"
            assert config.model.max_depth == 25
            assert config.model.min_instances_per_node == 25
            assert config.model.max_bins == 48
            assert config.model.min_info_gain == 0.05
            assert config.model.seed == 123
            
            # Verify training config
            assert config.training.sample_limit == 25000
            assert config.training.train_ratio == 0.8
            assert config.training.validation_ratio == 0.1
            assert config.training.test_ratio == 0.1
            assert config.training.random_seed == 123
            assert config.training.min_rows_required == 500
            assert config.training.primary_metric == "mae"
            assert config.training.metrics_to_track == ["mae", "rmse"]


class TestTrainingConfig:
    """Test TrainingConfig functionality."""
    
    def test_training_config_defaults(self):
        """Test TrainingConfig default values."""
        config = TrainingConfig()
        
        assert config.sample_limit == 50000
        assert config.train_ratio == 0.7
        assert config.validation_ratio == 0.15
        assert config.test_ratio == 0.15
        assert config.random_seed == 42
        assert config.min_rows_required == 1000
        assert config.data_split_method == "random"
        assert config.primary_metric == "rmse"
        assert config.metrics_to_track == ["rmse", "mae", "r2", "mse"]
    
    def test_training_config_custom_values(self):
        """Test TrainingConfig with custom values."""
        config = TrainingConfig(
            sample_limit=100000,
            train_ratio=0.8,
            validation_ratio=0.1,
            test_ratio=0.1,
            random_seed=123,
            min_rows_required=2000,
            data_split_method="temporal",
            primary_metric="mae",
            metrics_to_track=["mae", "rmse"]
        )
        
        assert config.sample_limit == 100000
        assert config.train_ratio == 0.8
        assert config.validation_ratio == 0.1
        assert config.test_ratio == 0.1
        assert config.random_seed == 123
        assert config.min_rows_required == 2000
        assert config.data_split_method == "temporal"
        assert config.primary_metric == "mae"
        assert config.metrics_to_track == ["mae", "rmse"]


class TestMLflowConfig:
    """Test MLflowConfig functionality."""
    
    def test_mlflow_config_defaults(self):
        """Test MLflowConfig default values."""
        config = MLflowConfig()
        
        assert config.experiment_name == "pv_solar_regression"
        assert config.run_name_prefix == "regression_dt"
        assert config.artifact_path == "model"
        assert config.log_model is True
        assert config.log_artifacts is True
    
    def test_mlflow_config_custom_values(self):
        """Test MLflowConfig with custom values."""
        config = MLflowConfig(
            experiment_name="custom_experiment",
            run_name_prefix="custom_run",
            artifact_path="custom_artifacts",
            log_model=False,
            log_artifacts=False
        )
        
        assert config.experiment_name == "custom_experiment"
        assert config.run_name_prefix == "custom_run"
        assert config.artifact_path == "custom_artifacts"
        assert config.log_model is False
        assert config.log_artifacts is False


class TestOutputConfig:
    """Test OutputConfig functionality."""
    
    def test_output_config_defaults(self):
        """Test OutputConfig default values."""
        config = OutputConfig()
        
        assert config.gold_table == "lh.gold.fact_solar_forecast_regression"
        assert config.write_mode == "overwrite"
        assert config.enable_predictions_write is True
    
    def test_output_config_custom_values(self):
        """Test OutputConfig with custom values."""
        config = OutputConfig(
            gold_table="lh.gold.custom_table",
            write_mode="append",
            enable_predictions_write=False
        )
        
        assert config.gold_table == "lh.gold.custom_table"
        assert config.write_mode == "append"
        assert config.enable_predictions_write is False