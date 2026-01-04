#!/bin/bash

# Navigate to project directory
cd "/Users/renwenjie/Nutstore Files/我的坚果云/Claude/TweetDigest"

# Load environment variables
if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
fi

# Generate timestamped report filename
REPORT_FILE="reports/digest-$(date +%Y%m%d-%H%M%S).md"

# Run the pipeline
# Note: Using full path to python to ensure correct environment. 
# Assuming system python3 or venv python. Adjust if using specific venv.
/usr/bin/python3 apify_pipeline/pipeline.py \
  --mode apify \
  --limit 20 \
  --summary-model "deepseek-chat" \
  --report "$REPORT_FILE" >> cron.log 2>&1

echo "Report generated at $REPORT_FILE" >> cron.log
