#!/bin/bash

# Script to run analysis and extract clean results
cd /home/whimsical-wizard/ing/ing2/data_eng/project/Data-engineering

echo "Running IoT Data Analysis..."
echo "=============================="
echo "This script will run the analysis component and display the results. It will take a few minutes to complete."
# Run analysis and capture output
docker compose run --rm analysis 2>/dev/null | grep -E "(ANALYSE|Week-end|Semaine|Top|^[0-9]|===|produits|alertes|stock|heure)" | head -50

echo ""
echo "Analysis complete! Full logs available with: ./docker-run.sh logs analysis"
