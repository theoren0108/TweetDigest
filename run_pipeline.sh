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
# Note: Using /usr/local/bin/python3 (Python 3.13) where packages are installed
/usr/local/bin/python3 apify_pipeline/pipeline.py \
  --mode apify \
  --limit 20 \
  --summary-model "deepseek-reasoner" \
  --report "$REPORT_FILE" >> cron.log 2>&1

echo "Report generated at $REPORT_FILE" >> cron.log
