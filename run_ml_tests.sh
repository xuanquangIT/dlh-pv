#!/bin/bash

# Script to run ML Pipeline tests with correct PYTHONPATH
# Usage: ./run_ml_tests.sh [optional pytest arguments]

set -e

# Set PYTHONPATH to include src directory
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Change to project directory
cd "$(dirname "$0")"

echo "Running ML Pipeline tests..."
echo "PYTHONPATH: $PYTHONPATH"
echo "Current directory: $(pwd)"
echo

# Run pytest with provided arguments or default to ML pipeline tests
if [ $# -eq 0 ]; then
    # No arguments provided, run all ML pipeline tests
    pytest tests/ml_pipeline/ -v
else
    # Run pytest with provided arguments
    pytest "$@"
fi