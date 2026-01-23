#!/bin/bash
# Quick test script for refactored ML pipeline
# Run inside Spark container

set -e

echo "=== ML Pipeline Refactor Test ==="
echo ""

# Set PYTHONPATH
export PYTHONPATH=/opt/workdir/src:$PYTHONPATH

# Test with small sample
echo "Testing with 1000 rows sample..."
python3 -m pv_lakehouse.ml_pipeline.train_from_silver --limit 1000

echo ""
echo "✅ Test complete!"
