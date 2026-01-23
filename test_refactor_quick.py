#!/usr/bin/env python3
"""Quick test of refactored ML pipeline components."""

import sys
sys.path.insert(0, '/home/pvlakehouse/work/dlh-pv/src')

from pv_lakehouse.ml_pipeline.config import MLConfig

# Test 1: Config loading
print("Test 1: Loading configuration...")
config = MLConfig.from_yaml(
    'config/ml_features.yaml',
    'config/ml_hyperparams.yaml'
)
print(f"✓ Loaded config: {len(config.features.get_all_features())} features")
print(f"✓ Model type: {config.model.model_type}")
print(f"✓ Min rows: {config.training.min_rows_required}")

# Test 2: Model creation
print("\nTest 2: Creating model...")
from pv_lakehouse.ml_pipeline.models.regressor import create_model
model_instance = create_model(config.model)
print(f"✓ Created model: {model_instance.__class__.__name__}")

print("\n✅ All tests passed!")
