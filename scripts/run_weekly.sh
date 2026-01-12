#!/bin/bash

# Ensure we are in the project root (assuming script is in scripts/)
cd "$(dirname "$0")/.." || exit

# Load environment variables
if [ -f .env ]; then
  echo "Loading environment variables from .env..."
  set -a
  source .env
  set +a
else
  echo "Warning: .env file not found in $(pwd). Ensure environment variables are set."
fi

# Generate timestamped report filename
mkdir -p reports/weekly
REPORT_FILE="reports/weekly/digest-$(date +%Y%m%d).md"

echo "=================================================="
echo "Starting weekly pipeline run at $(date)"
echo "Report will be saved to: $REPORT_FILE"

# Run the pipeline in weekly mode
# Using python3 from environment
python3 apify_pipeline/pipeline.py \
  --mode weekly \
  --weekly-model "deepseek-reasoner" \
  --weekly-max-posts 150 \
  --report "$REPORT_FILE"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Success! Weekly report generated at $REPORT_FILE"
else
    echo "❌ Weekly pipeline failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE
