#!/usr/bin/env python3
"""
Test ML training pipeline using CSV data to validate end-to-end process
"""

import pandas as pd
import numpy as np
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging
import sys
import os

# Add src to path
sys.path.append('/home/pvlakehouse/work/dlh-pv/src')

from pv_lakehouse.config.settings import get_settings
from pv_lakehouse.ml_pipeline.config import MLConfig
from pv_lakehouse.ml_pipeline.models.regressor import GBTRegressorModel, DecisionTreeRegressorModel

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session for local testing"""
    return SparkSession.builder \
        .appName("CSV_Training_Test") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_csv_to_dataframe(spark, csv_path):
    """Load CSV file to Spark DataFrame with proper schema inference"""
    logger = logging.getLogger(__name__)
    logger.info(f"Loading CSV: {csv_path}")
    
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") \
        .csv(str(csv_path))

def prepare_training_data(spark, data_dir, limit=1000):
    """
    Load and join CSV data to create training dataset
    Simulates the Silver layer joins
    """
    logger = logging.getLogger(__name__)
    
    # Load Silver layer data
    energy_df = load_csv_to_dataframe(spark, data_dir / "lh_silver_clean_hourly_energy.csv")
    weather_df = load_csv_to_dataframe(spark, data_dir / "lh_silver_clean_hourly_weather.csv")
    air_quality_df = load_csv_to_dataframe(spark, data_dir / "lh_silver_clean_hourly_air_quality.csv")
    
    logger.info(f"Energy records: {energy_df.count()}")
    logger.info(f"Weather records: {weather_df.count()}")
    logger.info(f"Air quality records: {air_quality_df.count()}")
    
    # Join data (similar to Silver layer logic)
    training_data = energy_df.alias("e") \
        .join(weather_df.alias("w"), 
              (F.col("e.facility_code") == F.col("w.facility_code")) & 
              (F.col("e.date_hour") == F.col("w.date_hour")), "inner") \
        .join(air_quality_df.alias("aq"),
              (F.col("e.facility_code") == F.col("aq.facility_code")) & 
              (F.col("e.date_hour") == F.col("aq.date_hour")), "left")
    
    # Select features for training
    feature_cols = [
        # Target
        F.col("e.energy_mwh").alias("target"),
        
        # Weather features (using actual column names from CSV)
        F.col("w.shortwave_radiation"),
        F.col("w.direct_radiation"), 
        F.col("w.diffuse_radiation"),
        F.col("w.direct_normal_irradiance"),
        F.col("w.temperature_2m"),
        F.col("w.dew_point_2m").alias("humidity_2m"),  # Use dew_point as proxy for humidity
        F.col("w.cloud_cover"),
        F.col("w.wind_speed_10m"),
        F.col("w.sunshine_duration").alias("sunshine_hours"),  # sunshine_duration not sunshine_hours
        
        # Air quality features (optional)
        F.coalesce(F.col("aq.pm2_5"), F.lit(0.0)).alias("pm2_5"),
        F.coalesce(F.col("aq.ozone"), F.lit(0.0)).alias("ozone"),
        
        # Time features
        F.hour(F.col("e.date_hour")).alias("hour_of_day"),
        F.dayofweek(F.col("e.date_hour")).alias("day_of_week"),
        F.dayofyear(F.col("e.date_hour")).alias("day_of_year")
    ]
    
    final_data = training_data.select(*feature_cols) \
        .filter(F.col("target").isNotNull() & (F.col("target") >= 0)) \
        .filter(F.col("shortwave_radiation").isNotNull()) \
        .limit(limit)
    
    logger.info(f"Final training data records: {final_data.count()}")
    return final_data

def test_model_training(training_data, config):
    """Test both GBT and DecisionTree models"""
    logger = logging.getLogger(__name__)
    
    # Test the model configured in config (either GBT or DecisionTree)
    logger.info(f"Testing {config.model.model_type} Regressor...")
    
    if config.model.model_type == "gbt":
        model = GBTRegressorModel(config.model)
    else:
        model = DecisionTreeRegressorModel(config.model)
    
    feature_cols = [col for col in training_data.columns if col != 'target']
    logger.info(f"Feature columns: {feature_cols}")
    
    try:
        # Build training pipeline
        pipeline = model.build_pipeline(feature_cols, "target")
        logger.info(f"✅ {config.model.model_type} Pipeline created successfully")
        
        # Fit model
        logger.info(f"Training {config.model.model_type} model...")
        fitted_model = pipeline.fit(training_data)
        logger.info(f"✅ {config.model.model_type} Model trained successfully")
        
        # Make predictions
        logger.info("Making predictions...")
        predictions = fitted_model.transform(training_data)
        
        # Show sample predictions
        logger.info("Sample predictions:")
        predictions.select("target", "prediction").show(10)
        
        # Calculate basic metrics
        from pyspark.ml.evaluation import RegressionEvaluator
        evaluator = RegressionEvaluator(
            labelCol="target",
            predictionCol="prediction",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        logger.info(f"✅ RMSE: {rmse:.4f}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ {config.model.model_type} training failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    logger = setup_logging()
    logger.info("🚀 Starting CSV training test...")
    
    try:
        # Load configuration
        logger.info("Loading ML configuration...")
        config = MLConfig.from_yaml(
            features_path="/home/pvlakehouse/work/dlh-pv/config/ml_features.yaml",
            hyperparams_path="/home/pvlakehouse/work/dlh-pv/config/ml_hyperparams.yaml"
        )
        logger.info("✅ Configuration loaded")
        
        # Create Spark session
        logger.info("Creating Spark session...")
        spark = create_spark_session()
        logger.info("✅ Spark session created")
        
        # Load and prepare data
        logger.info("Loading training data from CSV...")
        data_dir = Path("/home/pvlakehouse/work/dlh-pv/src/pv_lakehouse/exported_data")
        training_data = prepare_training_data(spark, data_dir, limit=1000)
        logger.info("✅ Training data prepared")
        
        # Test training
        logger.info("Testing model training...")
        success = test_model_training(training_data, config)
        
        if success:
            logger.info("🎉 Training test completed successfully!")
            logger.info("✅ ML pipeline is working correctly with CSV data")
        else:
            logger.error("❌ Training test failed")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if 'spark' in locals():
            spark.stop()
    
    return 0

if __name__ == "__main__":
    exit(main())