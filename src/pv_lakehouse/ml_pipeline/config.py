from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

# Named constants for magic numbers
DEFAULT_MIN_RADIATION_THRESHOLD = 10.0
DEFAULT_LOW_ENERGY_THRESHOLD = 0.1
DEFAULT_NOISE_MAGNITUDE = 0.0
DEFAULT_NOISE_RATIO = 0.0


@dataclass
class FeatureConfig:
    
    target_column: str
    energy_features: list[str] = field(default_factory=list)
    temporal_basic: list[str] = field(default_factory=list)
    temporal_cyclical: list[str] = field(default_factory=list)
    weather_primary: list[str] = field(default_factory=list)
    weather_derived: list[str] = field(default_factory=list)
    lag_features: list[str] = field(default_factory=list)
    production_features: list[str] = field(default_factory=list)
    air_quality_features: list[str] = field(default_factory=list)
    
    min_radiation_threshold: float = DEFAULT_MIN_RADIATION_THRESHOLD
    low_energy_threshold: float = DEFAULT_LOW_ENERGY_THRESHOLD
    noise_magnitude: float = DEFAULT_NOISE_MAGNITUDE
    noise_ratio: float = DEFAULT_NOISE_RATIO
    
    def get_all_features(self, include_air_quality: bool = False) -> list[str]:
        features = (
            self.energy_features +
            self.temporal_basic +
            self.temporal_cyclical +
            self.weather_primary +
            self.weather_derived +
            self.lag_features +
            self.production_features
        )
        if include_air_quality:
            features += self.air_quality_features
        return features


@dataclass
class ModelConfig:
    
    model_type: str = "gbt"  # Default to GBT for production
    
    # Decision Tree params
    max_depth: int = 20
    min_instances_per_node: int = 20
    max_bins: int = 64
    min_info_gain: float = 0.0
    seed: int = 42
    
    # GBT params (if using GBT)
    gbt_max_iter: int = 120
    gbt_step_size: float = 0.1
    gbt_subsample_rate: float = 0.8
    gbt_feature_subset_strategy: str = "auto"


@dataclass
class TrainingConfig:
    
    sample_limit: int = 50000
    train_ratio: float = 0.7
    validation_ratio: float = 0.15
    test_ratio: float = 0.15
    random_seed: int = 42
    min_rows_required: int = 1000
    data_split_method: str = "random"  # "random" or "temporal"
    
    primary_metric: str = "rmse"
    metrics_to_track: list[str] = field(default_factory=lambda: ["rmse", "mae", "r2", "mse"])


@dataclass
class MLflowConfig:
    
    experiment_name: str = "pv_solar_regression"
    run_name_prefix: str = "regression_dt"
    artifact_path: str = "model"
    log_model: bool = True
    log_artifacts: bool = True


@dataclass
class OutputConfig:
    
    gold_table: str = "lh.gold.fact_solar_forecast_regression"
    write_mode: str = "overwrite"
    enable_predictions_write: bool = True


@dataclass
class MLConfig:
    
    features: FeatureConfig
    model: ModelConfig
    training: TrainingConfig
    mlflow: MLflowConfig
    output: OutputConfig
    
    @classmethod
    def from_yaml(cls, features_path: str, hyperparams_path: str) -> MLConfig:
        """Load ML configuration from YAML files.
        
        Args:
            features_path: Path to features configuration YAML file
            hyperparams_path: Path to hyperparameters configuration YAML file
            
        Returns:
            MLConfig instance with loaded configuration
            
        Raises:
            FileNotFoundError: If configuration files don't exist
            yaml.YAMLError: If YAML parsing fails
            KeyError: If required configuration keys are missing
        """
        # Validate file existence first
        features_path_obj = Path(features_path)
        hyperparams_path_obj = Path(hyperparams_path)
        
        if not features_path_obj.exists():
            raise FileNotFoundError(f"Features configuration file not found: {features_path}")
        if not hyperparams_path_obj.exists():
            raise FileNotFoundError(f"Hyperparameters configuration file not found: {hyperparams_path}")
        
        # Load features configuration
        try:
            with features_path_obj.open('r', encoding='utf-8') as f:
                features_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Failed to parse features YAML file {features_path}: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error loading features config {features_path}: {e}") from e
        
        # Load hyperparameters configuration
        try:
            with hyperparams_path_obj.open('r', encoding='utf-8') as f:
                hyperparams_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Failed to parse hyperparameters YAML file {hyperparams_path}: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error loading hyperparams config {hyperparams_path}: {e}") from e
        
        # Parse features config
        features = FeatureConfig(
            target_column=features_data["target"]["column"],
            energy_features=features_data["energy_features"],
            temporal_basic=features_data["temporal_features"]["basic"],
            temporal_cyclical=features_data["temporal_features"]["cyclical"],
            weather_primary=features_data["weather_features"]["primary"],
            weather_derived=features_data["weather_features"]["derived"],
            lag_features=features_data["lag_features"],
            production_features=features_data["production_features"],
            air_quality_features=features_data["air_quality_features"],
            min_radiation_threshold=features_data["feature_engineering"]["min_radiation_threshold"],
            low_energy_threshold=features_data["feature_engineering"]["low_energy_threshold"],
            noise_magnitude=features_data["feature_engineering"]["noise_magnitude"],
            noise_ratio=features_data["feature_engineering"]["noise_ratio"],
        )
        
        # Parse model config
        model_type = hyperparams_data["model"]["type"]
        if model_type == "decision_tree":
            dt_params = hyperparams_data["decision_tree"]
            model = ModelConfig(
                model_type=model_type,
                max_depth=dt_params["max_depth"],
                min_instances_per_node=dt_params["min_instances_per_node"],
                max_bins=dt_params["max_bins"],
                min_info_gain=dt_params["min_info_gain"],
                seed=dt_params["seed"],
            )
        else:  # GBT
            gbt_params = hyperparams_data["gbt"]
            model = ModelConfig(
                model_type=model_type,
                max_depth=gbt_params["max_depth"],
                min_instances_per_node=gbt_params["min_instances_per_node"],
                max_bins=gbt_params["max_bins"],
                min_info_gain=gbt_params["min_info_gain"],
                gbt_max_iter=gbt_params["max_iter"],
                gbt_step_size=gbt_params["step_size"],
                gbt_subsample_rate=gbt_params["subsample_rate"],
                gbt_feature_subset_strategy=gbt_params["feature_subset_strategy"],
                seed=gbt_params["seed"],
            )
        
        # Parse training config
        training_params = hyperparams_data["training"]
        training = TrainingConfig(
            sample_limit=training_params["sample_limit"],
            train_ratio=training_params["train_ratio"],
            validation_ratio=training_params["validation_ratio"],
            test_ratio=training_params["test_ratio"],
            random_seed=training_params["random_seed"],
            min_rows_required=training_params["min_rows_required"],
            data_split_method=hyperparams_data["data_split"].get("method", "random"),
            primary_metric=hyperparams_data["metrics"]["primary"],
            metrics_to_track=hyperparams_data["metrics"]["track"],
        )
        
        # Parse MLflow config
        mlflow_params = hyperparams_data["mlflow"]
        mlflow_config = MLflowConfig(
            experiment_name=mlflow_params["experiment_name"],
            run_name_prefix=mlflow_params["run_name_prefix"],
            artifact_path=mlflow_params["artifact_path"],
            log_model=mlflow_params["log_model"],
            log_artifacts=mlflow_params["log_artifacts"],
        )
        
        # Parse output config
        output_params = hyperparams_data["output"]
        output = OutputConfig(
            gold_table=output_params["gold_table"],
            write_mode=output_params["write_mode"],
            enable_predictions_write=output_params["enable_predictions_write"],
        )
        
        return cls(
            features=features,
            model=model,
            training=training,
            mlflow=mlflow_config,
            output=output,
        )
