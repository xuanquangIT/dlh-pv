#!/usr/bin/env python3
"""Quick syntax check for refactored ML pipeline."""

import sys
sys.path.insert(0, '/opt/workdir/src')

print("=== Syntax Check ===\n")

try:
    print("1. Importing config...")
    from pv_lakehouse.ml_pipeline.config import MLConfig
    print("   ✅ config.py OK")
    
    print("\n2. Importing features...")
    from pv_lakehouse.ml_pipeline.features import engineering, temporal, weather
    print("   ✅ features OK")
    
    print("\n3. Importing models...")
    from pv_lakehouse.ml_pipeline.models import base, regressor
    print("   ✅ models OK")
    
    print("\n4. Importing evaluation...")
    from pv_lakehouse.ml_pipeline.evaluation import metrics
    print("   ✅ evaluation OK")
    
    print("\n5. Importing tracking...")
    from pv_lakehouse.ml_pipeline.tracking import mlflow_logger
    print("   ✅ tracking OK")
    
    print("\n6. Loading config from YAML...")
    config = MLConfig.from_yaml(
        '/opt/workdir/config/ml_features.yaml',
        '/opt/workdir/config/ml_hyperparams.yaml'
    )
    print(f"   ✅ Config loaded: model_type={config.model.model_type}")
    print(f"   ✅ Features: {len(config.features.get_all_features())} total")
    
    print("\n7. Creating model...")
    from pv_lakehouse.ml_pipeline.models.regressor import create_model
    model = create_model(config.model)
    print(f"   ✅ Model created: {type(model).__name__}")
    
    print("\n" + "="*50)
    print("✅ ALL SYNTAX CHECKS PASSED!")
    print("="*50)
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
