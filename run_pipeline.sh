#!/bin/bash

# Navigate to script directory (works from anywhere)
cd "$(dirname "$0")" || exit

# Load environment variables
if [ -f .env ]; then
  echo "Loading environment variables from .env..."
  export $(cat .env | grep -v '^#' | xargs)
else
  echo "Warning: .env file not found. Ensure environment variables are set."
fi

# Generate timestamped report filename
mkdir -p reports
REPORT_FILE="reports/digest-$(date +%Y%m%d-%H%M%S).md"
LOG_FILE="cron.log"

echo "==================================================" | tee -a "$LOG_FILE"
echo "Starting pipeline run at $(date)" | tee -a "$LOG_FILE"
echo "Report will be saved to: $REPORT_FILE" | tee -a "$LOG_FILE"

# Run the pipeline
# Note: Using /usr/local/bin/python3 (Python 3.13) where packages are installed
/usr/local/bin/python3 apify_pipeline/pipeline.py \
  --mode apify \
  --limit 20 \
  --summary-model "deepseek-reasoner" \
  --report "$REPORT_FILE" 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Success! Report generated at $REPORT_FILE" | tee -a "$LOG_FILE"
else
    echo "❌ Pipeline failed with exit code $EXIT_CODE" | tee -a "$LOG_FILE"
fi

exit $EXIT_CODE
