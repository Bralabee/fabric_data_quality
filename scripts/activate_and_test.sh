#!/bin/bash
# Quick activation and test script

# Ensure we are in the project root
if [ ! -d "dq_framework" ]; then
    echo "❌ Error: Please run this script from the project root directory."
    echo "Usage: ./scripts/activate_and_test.sh"
    exit 1
fi

echo "========================================="
echo "Activating fabric-dq environment..."
echo "========================================="

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "❌ Conda not found. Please install Miniconda/Anaconda first."
    exit 1
fi

# Activate environment
eval "$(conda shell.bash hook)"
conda activate fabric-dq

# Verify activation
if [[ "$CONDA_DEFAULT_ENV" != "fabric-dq" ]]; then
    echo "❌ Failed to activate environment"
    exit 1
fi

echo "✅ Environment activated: $CONDA_DEFAULT_ENV"
echo ""

# Test imports
echo "Testing framework imports..."
python -c "
from dq_framework import DataQualityValidator, FabricDataQualityRunner, ConfigLoader
import great_expectations
print('✅ All core modules imported successfully')
print(f'✅ Great Expectations version: {great_expectations.__version__}')
"

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    echo "✅ Environment is ready to use!"
    echo "========================================="
    echo ""
    echo "Quick commands:"
    echo "  python scripts/profile_data.py --help  - Run profiler"
    echo "  make test                              - Run tests"
    echo "  make help                              - Show all commands"
    echo ""
    echo "To use in other terminals, run:"
    echo "  conda activate fabric-dq"
    echo ""
else
    echo ""
    echo "❌ Import test failed. Try reinstalling:"
    echo "  pip install -e ."
fi
