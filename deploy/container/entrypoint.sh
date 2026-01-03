#!/usr/bin/env bash
set -euo pipefail

CRON_SCHEDULE="${CRON_SCHEDULE:-0 0,12 * * *}"
WORKDIR="${WORKDIR:-/app}"
LOGFILE="${LOGFILE:-/var/log/cron.log}"
PIPELINE_CMD="${PIPELINE_CMD:-python -m apify_pipeline.pipeline --mode apify --config apify_pipeline/accounts.yml --db apify_pipeline/data/digests.db --report reports/apify-daily.md}"

mkdir -p "$(dirname "$LOGFILE")"
touch "$LOGFILE"

cat > /etc/cron.d/apify-pipeline <<EOF2
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
CRON_TZ=UTC
WORKDIR=${WORKDIR}
APIFY_TOKEN=${APIFY_TOKEN:-}

${CRON_SCHEDULE} root cd ${WORKDIR} && ${PIPELINE_CMD} >> ${LOGFILE} 2>&1
EOF2
chmod 0644 /etc/cron.d/apify-pipeline
crontab /etc/cron.d/apify-pipeline

# Start cron in the background (supports Debian/Ubuntu cron or BusyBox crond)
if command -v cron >/dev/null 2>&1; then
  cron
elif command -v crond >/dev/null 2>&1; then
  crond
else
  echo "Neither cron nor crond is installed; cannot schedule jobs" >&2
  exit 1
fi

# Stream logs to stdout for observability
exec tail -F "$LOGFILE"
