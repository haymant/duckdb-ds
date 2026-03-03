#!/bin/bash

# DuckDB Query API - Setup Script
# Automatic setup for local development

set -e

echo "========================================="
echo "DuckDB Query API - Setup Script"
echo "========================================="

# Check Python version
echo -e "\n[1/4] Checking Python installation..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.9 or higher."
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
echo "✓ Found Python $PYTHON_VERSION"

# Check for uv or create venv
echo -e "\n[2/4] Setting up virtual environment..."
if command -v uv &> /dev/null; then
    echo "✓ uv found - using uv venv"
    if [ ! -d ".venv" ]; then
        uv venv
        echo "✓ Virtual environment created"
    else
        echo "✓ Virtual environment already exists"
    fi
    source .venv/bin/activate
    echo "✓ Virtual environment activated"
else
    echo "⚠ uv not found - using python venv"
    if [ ! -d ".venv" ]; then
        python3 -m venv .venv
        echo "✓ Virtual environment created"
    else
        echo "✓ Virtual environment already exists"
    fi
    source .venv/bin/activate
    echo "✓ Virtual environment activated"
fi

# Install dependencies
echo -e "\n[3/4] Installing dependencies..."
if command -v uv &> /dev/null; then
    uv pip install -e .
else
    pip install -r requirements.txt
fi
echo "✓ Dependencies installed"

# Show next steps
echo -e "\n[4/4] Setup complete!"
echo -e "\n========================================="
echo "Next Steps:"
echo "========================================="
echo ""
echo "1. Activate virtual environment (if not already):"
echo "   source .venv/bin/activate"
echo ""
echo "2. Start the development server:"
echo "   uvicorn main:app --reload"
echo ""
echo "3. Open in browser:"
echo "   http://localhost:8000/docs"
echo ""
echo "4. Test the API:"
echo "   curl http://localhost:8000/health"
echo ""
echo "========================================="
echo "Documentation:"
echo "========================================="
echo "  • Quick Start: QUICKSTART.md"
echo "  • Full Docs: README.md"
echo "  • Security: SECURITY.md"
echo "  • Examples: examples/CURL_EXAMPLES.sh"
echo "========================================="
echo ""
