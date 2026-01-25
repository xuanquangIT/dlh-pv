"""Tests for ML Pipeline feature engineering modules."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

from pv_lakehouse.ml_pipeline.config import FeatureConfig
from pv_lakehouse.ml_pipeline.features.engineering import (
    prepare_features,
    select_training_features
)


class TestFeatureEngineering:
    """Test feature engineering functions."""
    
    @pytest.fixture
    def spark_session(self):
        """Create a test Spark session."""
        spark = SparkSession.builder \
            .appName("test_features") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_df(self, spark_session):
        """Create sample DataFrame for testing."""
        schema = StructType([
            StructField("facility_code", StringType(), True),
            StructField("date_hour", TimestampType(), True),
            StructField("energy_kwh", FloatType(), True),
            StructField("temperature_2m", FloatType(), True),
            StructField("relative_humidity_2m", FloatType(), True),
            StructField("global_tilted_irradiance_wm2", FloatType(), True)
        ])
        
        data = [
            ("FACILITY_001", datetime(2024, 1, 1, 10, 0, 0), 150.5, 25.0, 60.0, 800.0),
            ("FACILITY_001", datetime(2024, 1, 1, 11, 0, 0), 200.0, 26.0, 58.0, 850.0),
            ("FACILITY_002", datetime(2024, 1, 1, 10, 0, 0), 175.0, 24.0, 62.0, 820.0),
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def feature_config(self):
        """Create test feature configuration."""
        return FeatureConfig(
            target_column="energy_kwh",
            energy_features=["energy_kwh"],
            temporal_basic=["hour", "day_of_week", "month"],
            temporal_cyclical=["hour_sin", "hour_cos"],
            weather_primary=["temperature_2m", "relative_humidity_2m"],
            weather_derived=["temperature_humidity_interaction"],
            lag_features=["energy_lag_1h", "energy_lag_24h"],
            production_features=["production_indicator"],
            min_radiation_threshold=10.0,
            low_energy_threshold=0.1
        )
    
    def test_prepare_features_basic(self, sample_df, feature_config):
        """Test basic feature preparation."""
        # Mock the individual feature functions
        with patch('pv_lakehouse.ml_pipeline.features.engineering.add_temporal_features') as mock_temporal, \
             patch('pv_lakehouse.ml_pipeline.features.engineering.add_cyclical_encoding') as mock_cyclical, \
             patch('pv_lakehouse.ml_pipeline.features.engineering.add_lag_features') as mock_lag, \
             patch('pv_lakehouse.ml_pipeline.features.engineering.add_weather_interactions') as mock_weather, \
             patch('pv_lakehouse.ml_pipeline.features.engineering.add_production_indicators') as mock_production:
            
            # Setup mock return values (each function returns the DataFrame)
            mock_temporal.return_value = sample_df
            mock_cyclical.return_value = sample_df
            mock_lag.return_value = sample_df
            mock_weather.return_value = sample_df
            mock_production.return_value = sample_df
            
            result = prepare_features(sample_df, feature_config, include_lag=True)
            
            # Verify all functions were called
            mock_temporal.assert_called_once_with(sample_df, time_col="date_hour")
            mock_cyclical.assert_called_once_with(sample_df)
            mock_lag.assert_called_once()
            mock_weather.assert_called_once_with(sample_df)
            mock_production.assert_called_once()
            
            assert result is not None
    
    def test_prepare_features_no_lag(self, sample_df, feature_config):
        """Test feature preparation without lag features."""
        with patch('pv_lakehouse.ml_pipeline.features.engineering.add_temporal_features') as mock_temporal, \
             patch('pv_lakehouse.ml_pipeline.features.engineering.add_cyclical_encoding') as mock_cyclical, \
             patch('pv_lakehouse.ml_pipeline.features.engineering.add_lag_features') as mock_lag, \
             patch('pv_lakehouse.ml_pipeline.features.engineering.add_weather_interactions') as mock_weather, \
             patch('pv_lakehouse.ml_pipeline.features.engineering.add_production_indicators') as mock_production:
            
            # Setup mock return values
            mock_temporal.return_value = sample_df
            mock_cyclical.return_value = sample_df
            mock_weather.return_value = sample_df
            mock_production.return_value = sample_df
            
            result = prepare_features(sample_df, feature_config, include_lag=False)
            
            # Verify lag features were not added
            mock_lag.assert_not_called()
            mock_temporal.assert_called_once()
            mock_cyclical.assert_called_once()
            mock_weather.assert_called_once()
            mock_production.assert_called_once()
    
    def test_select_training_features(self, sample_df, feature_config):
        """Test feature selection for training."""
        # Create a better mock DataFrame with columns attribute
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = [
            "facility_code", "date_hour", "energy_kwh", 
            "temperature_2m", "relative_humidity_2m",
            "hour", "day_of_week", "month"  # Some features that should exist
        ]
        mock_df.select.return_value = mock_df
        
        result = select_training_features(mock_df, feature_config, include_air_quality=False)
        
        # Verify select was called
        mock_df.select.assert_called_once()
        
    def test_feature_config_get_all_features(self, feature_config):
        """Test feature configuration get_all_features method."""
        # Test without air quality
        features = feature_config.get_all_features(include_air_quality=False)
        
        expected_features = (
            feature_config.energy_features +
            feature_config.temporal_basic +
            feature_config.temporal_cyclical +
            feature_config.weather_primary +
            feature_config.weather_derived +
            feature_config.lag_features +
            feature_config.production_features
        )
        
        assert features == expected_features
        
        # Test with air quality
        features_with_aq = feature_config.get_all_features(include_air_quality=True)
        expected_with_aq = expected_features + feature_config.air_quality_features
        
        assert features_with_aq == expected_with_aq


class TestFeatureConfigValidation:
    """Test feature configuration validation."""
    
    def test_feature_config_creation(self):
        """Test FeatureConfig creation with valid parameters."""
        config = FeatureConfig(
            target_column="energy_kwh",
            energy_features=["energy_kwh"],
            min_radiation_threshold=10.0,
            low_energy_threshold=0.1
        )
        
        assert config.target_column == "energy_kwh"
        assert config.energy_features == ["energy_kwh"]
        assert config.min_radiation_threshold == 10.0
        assert config.low_energy_threshold == 0.1
    
    def test_feature_config_defaults(self):
        """Test FeatureConfig default values."""
        config = FeatureConfig(target_column="energy_kwh")
        
        assert config.energy_features == []
        assert config.temporal_basic == []
        assert config.temporal_cyclical == []
        assert config.weather_primary == []
        assert config.weather_derived == []
        assert config.lag_features == []
        assert config.production_features == []
        assert config.air_quality_features == []
        assert config.min_radiation_threshold == 10.0
        assert config.low_energy_threshold == 0.1
        assert config.noise_magnitude == 0.0
        assert config.noise_ratio == 0.0