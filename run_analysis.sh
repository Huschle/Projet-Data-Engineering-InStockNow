#!/bin/bash

echo "=== Running Spark Analysis ==="
cd composant_5_analysis

echo "Starting analysis..."
echo "======================================"

sbt -warn run 2>/dev/null

echo "======================================"
echo "Analysis complete!"