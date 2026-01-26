"""Tests for ML Pipeline model modules."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

from pv_lakehouse.ml_pipeline.config import ModelConfig
from pv_lakehouse.ml_pipeline.models.base import BaseModel
from pv_lakehouse.ml_pipeline.models.regressor import DecisionTreeRegressorModel, GBTRegressorModel


class TestBaseModel:
    """Test BaseModel abstract class functionality."""
    
    @pytest.fixture
    def model_config(self):
        """Create test model configuration."""
        return ModelConfig(
            model_type="decision_tree",
            max_depth=20,
            min_instances_per_node=20,
            max_bins=64,
            seed=42
        )
    
    def test_base_model_initialization(self, model_config):
        """Test BaseModel initialization."""
        # Create a concrete implementation for testing
        class ConcreteModel(BaseModel):
            def build_pipeline(self, feature_cols, target_col):
                pass
            
            def train(self, train_df, feature_cols, target_col):
                pass
        
        model = ConcreteModel(model_config)
        assert model.config == model_config
        assert model.pipeline_model is None
    
    def test_predict_without_training(self, model_config):
        """Test predict raises error when model not trained."""
        class ConcreteModel(BaseModel):
            def build_pipeline(self, feature_cols, target_col):
                pass
            
            def train(self, train_df, feature_cols, target_col):
                pass
        
        model = ConcreteModel(model_config)
        mock_df = Mock(spec=DataFrame)
        
        with pytest.raises(ValueError, match="Model not trained. Call train\\(\\) first."):
            model.predict(mock_df)
    
    def test_predict_with_trained_model(self, model_config):
        """Test predict works when model is trained."""
        class ConcreteModel(BaseModel):
            def build_pipeline(self, feature_cols, target_col):
                pass
            
            def train(self, train_df, feature_cols, target_col):
                pass
        
        model = ConcreteModel(model_config)
        
        # Mock a trained pipeline model
        mock_pipeline = Mock()
        mock_result_df = Mock(spec=DataFrame)
        mock_pipeline.transform.return_value = mock_result_df
        
        model.pipeline_model = mock_pipeline
        mock_df = Mock(spec=DataFrame)
        
        result = model.predict(mock_df)
        
        mock_pipeline.transform.assert_called_once_with(mock_df)
        assert result == mock_result_df
    
    def test_get_feature_importance_no_model(self, model_config):
        """Test feature importance returns empty dict when no model."""
        class ConcreteModel(BaseModel):
            def build_pipeline(self, feature_cols, target_col):
                pass
            
            def train(self, train_df, feature_cols, target_col):
                pass
        
        model = ConcreteModel(model_config)
        importance = model.get_feature_importance()
        assert importance == {}
    
    def test_get_feature_importance_invalid_stages(self, model_config):
        """Test feature importance with invalid pipeline stages."""
        class ConcreteModel(BaseModel):
            def build_pipeline(self, feature_cols, target_col):
                pass
            
            def train(self, train_df, feature_cols, target_col):
                pass
        
        model = ConcreteModel(model_config)
        
        # Mock pipeline with insufficient stages
        mock_pipeline = Mock()
        mock_pipeline.stages = [Mock()]  # Only one stage
        model.pipeline_model = mock_pipeline
        
        importance = model.get_feature_importance()
        assert importance == {}


class TestDecisionTreeRegressorModel:
    """Test DecisionTreeRegressorModel implementation."""
    
    @pytest.fixture
    def spark_session(self):
        """Create a test Spark session."""
        spark = SparkSession.builder \
            .appName("test_models") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def dt_config(self):
        """Create decision tree configuration."""
        return ModelConfig(
            model_type="decision_tree",
            max_depth=10,
            min_instances_per_node=10,
            max_bins=32,
            min_info_gain=0.0,
            seed=42
        )
    
    @pytest.fixture
    def sample_df(self, spark_session):
        """Create sample DataFrame for testing."""
        schema = StructType([
            StructField("feature1", FloatType(), True),
            StructField("feature2", FloatType(), True),
            StructField("energy_kwh", FloatType(), True)
        ])
        
        data = [
            (1.0, 2.0, 150.5),
            (2.0, 3.0, 200.0),
            (3.0, 4.0, 175.0),
            (4.0, 5.0, 220.0),
            (5.0, 6.0, 180.0)
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    def test_dt_model_initialization(self, dt_config):
        """Test DecisionTreeRegressorModel initialization."""
        model = DecisionTreeRegressorModel(dt_config)
        assert model.config == dt_config
        assert model.pipeline_model is None
    
    @patch('pv_lakehouse.ml_pipeline.models.regressor.Pipeline')
    @patch('pv_lakehouse.ml_pipeline.models.regressor.VectorAssembler')
    @patch('pv_lakehouse.ml_pipeline.models.regressor.DecisionTreeRegressor')
    def test_dt_build_pipeline(self, mock_dt, mock_assembler, mock_pipeline, dt_config):
        """Test DecisionTreeRegressorModel pipeline building."""
        model = DecisionTreeRegressorModel(dt_config)
        feature_cols = ["feature1", "feature2"]
        target_col = "energy_kwh"
        
        # Setup mocks
        mock_assembler_instance = Mock()
        mock_assembler.return_value = mock_assembler_instance
        
        mock_dt_instance = Mock()
        mock_dt.return_value = mock_dt_instance
        
        mock_pipeline_instance = Mock()
        mock_pipeline.return_value = mock_pipeline_instance
        
        pipeline = model.build_pipeline(feature_cols, target_col)
        
        # Verify VectorAssembler was created correctly
        mock_assembler.assert_called_once_with(
            inputCols=feature_cols,
            outputCol="features",
            handleInvalid="skip"
        )
        
        # Verify DecisionTreeRegressor was created with correct parameters
        mock_dt.assert_called_once_with(
            featuresCol="features",
            labelCol=target_col,
            predictionCol="prediction",
            maxDepth=dt_config.max_depth,
            minInstancesPerNode=dt_config.min_instances_per_node,
            maxBins=dt_config.max_bins,
            minInfoGain=dt_config.min_info_gain,
            seed=dt_config.seed
        )
        
        # Verify Pipeline was created
        mock_pipeline.assert_called_once_with(stages=[mock_assembler_instance, mock_dt_instance])
        assert pipeline == mock_pipeline_instance
    
    def test_dt_train_integration(self, dt_config, sample_df):
        """Test DecisionTreeRegressorModel training integration."""
        model = DecisionTreeRegressorModel(dt_config)
        feature_cols = ["feature1", "feature2"]
        target_col = "energy_kwh"
        
        # Mock the pipeline and its fit method
        with patch.object(model, 'build_pipeline') as mock_build:
            mock_pipeline = Mock()
            mock_fitted_pipeline = Mock()
            mock_pipeline.fit.return_value = mock_fitted_pipeline
            mock_build.return_value = mock_pipeline
            
            model.train(sample_df, feature_cols, target_col)
            
            # Verify pipeline was built and fitted
            mock_build.assert_called_once_with(feature_cols, target_col)
            mock_pipeline.fit.assert_called_once_with(sample_df)
            assert model.pipeline_model == mock_fitted_pipeline


class TestGBTRegressorModel:
    """Test GBTRegressorModel implementation."""
    
    @pytest.fixture
    def gbt_config(self):
        """Create GBT configuration."""
        return ModelConfig(
            model_type="gbt",
            max_depth=10,
            min_instances_per_node=10,
            max_bins=32,
            min_info_gain=0.0,
            gbt_max_iter=100,
            gbt_step_size=0.1,
            gbt_subsample_rate=0.8,
            gbt_feature_subset_strategy="auto",
            seed=42
        )
    
    def test_gbt_model_initialization(self, gbt_config):
        """Test GBTRegressorModel initialization."""
        model = GBTRegressorModel(gbt_config)
        assert model.config == gbt_config
        assert model.pipeline_model is None
    
    @patch('pv_lakehouse.ml_pipeline.models.regressor.Pipeline')
    @patch('pv_lakehouse.ml_pipeline.models.regressor.VectorAssembler')
    @patch('pv_lakehouse.ml_pipeline.models.regressor.GBTRegressor')
    def test_gbt_build_pipeline(self, mock_gbt, mock_assembler, mock_pipeline, gbt_config):
        """Test GBTRegressorModel pipeline building."""
        model = GBTRegressorModel(gbt_config)
        feature_cols = ["feature1", "feature2"]
        target_col = "energy_kwh"
        
        # Setup mocks
        mock_assembler_instance = Mock()
        mock_assembler.return_value = mock_assembler_instance
        
        mock_gbt_instance = Mock()
        mock_gbt.return_value = mock_gbt_instance
        
        mock_pipeline_instance = Mock()
        mock_pipeline.return_value = mock_pipeline_instance
        
        pipeline = model.build_pipeline(feature_cols, target_col)
        
        # Verify VectorAssembler was created correctly
        mock_assembler.assert_called_once_with(
            inputCols=feature_cols,
            outputCol="features",
            handleInvalid="skip"
        )
        
        # Verify GBTRegressor was created with correct parameters
        mock_gbt.assert_called_once_with(
            featuresCol="features",
            labelCol=target_col,
            predictionCol="prediction",
            maxDepth=gbt_config.max_depth,
            minInstancesPerNode=gbt_config.min_instances_per_node,
            maxBins=gbt_config.max_bins,
            minInfoGain=gbt_config.min_info_gain,
            maxIter=gbt_config.gbt_max_iter,
            stepSize=gbt_config.gbt_step_size,
            subsamplingRate=gbt_config.gbt_subsample_rate,
            featureSubsetStrategy=gbt_config.gbt_feature_subset_strategy,
            seed=gbt_config.seed
        )
        
        # Verify Pipeline was created
        mock_pipeline.assert_called_once_with(stages=[mock_assembler_instance, mock_gbt_instance])
        assert pipeline == mock_pipeline_instance


class TestModelConfig:
    """Test ModelConfig functionality."""
    
    def test_model_config_defaults(self):
        """Test ModelConfig default values."""
        config = ModelConfig()
        
        assert config.model_type == "gbt"
        assert config.max_depth == 20
        assert config.min_instances_per_node == 20
        assert config.max_bins == 64
        assert config.min_info_gain == 0.0
        assert config.seed == 42
        assert config.gbt_max_iter == 120
        assert config.gbt_step_size == 0.1
        assert config.gbt_subsample_rate == 0.8
        assert config.gbt_feature_subset_strategy == "auto"
    
    def test_model_config_custom_values(self):
        """Test ModelConfig with custom values."""
        config = ModelConfig(
            model_type="decision_tree",
            max_depth=15,
            min_instances_per_node=10,
            max_bins=32,
            seed=123
        )
        
        assert config.model_type == "decision_tree"
        assert config.max_depth == 15
        assert config.min_instances_per_node == 10
        assert config.max_bins == 32
        assert config.seed == 123