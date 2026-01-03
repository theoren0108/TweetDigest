# Apify-based X digest (apidojo/twitter-scraper-lite)

Minimal scaffold that runs the Apify actor [`apidojo/twitter-scraper-lite`](https://apify.com/apidojo/twitter-scraper-lite) to pull timelines for a list of X accounts, store them incrementally, and emit a lightweight keyword-oriented report.

## Quick start (sample/offline mode)
```bash
python apify_pipeline/pipeline.py --mode sample \
  --report reports/apify-sample.md
```
Uses `sample_data/sample_tweets.jsonl` and writes a Markdown digest to `reports/`.

## Run against Apify

### 1. Configure Environment
Copy the example environment file and add your Apify Token:
```bash
cp .env.example .env
# Edit .env and set APIFY_TOKEN="your_token_here"
```

### 2. Run the Pipeline
```bash
# Load env vars (if not using a tool that does it auto) and run
export $(cat .env | xargs)
python apify_pipeline/pipeline.py --mode apify --limit 10
```

Key flags:
- `--actor-id`: defaults to `apidojo~twitter-scraper-lite`.
- `--input-template`: defaults to `apify_pipeline/input.template.json` (edit if the actor schema changes).
- `--config`: account list (YAML/JSON), defaults to `apify_pipeline/accounts.yml`.
- `--limit`: max tweets per account per run (sets `maxItems`).
- `--summary-model`: optional OpenAI-compatible model id (for example, `gpt-4o-mini` or DeepSeek's `deepseek-chat`) to append an LLM-written summary to the report. Set `OPENAI_API_KEY` or `DEEPSEEK_API_KEY` (or pass `--summary-api-key`), and install the `openai` Python package. For non-OpenAI hosts, pass `--summary-base-url` (e.g., `https://api.deepseek.com`).

## Scheduled runs (cron/systemd/Kubernetes)
- Cron: copy `deploy/cron/apify-pipeline.cron` to `/etc/cron.d/`, set `APIFY_TOKEN` in `/etc/default/apify-pipeline`, and (optionally) set `WORKDIR`/`LOGFILE`. The job runs at `0 0,12 * * *` UTC and executes `python -m apify_pipeline.pipeline --mode apify --config apify_pipeline/accounts.yml --db apify_pipeline/data/digests.db --report reports/apify-daily.md`.
- systemd: place `deploy/systemd/apify-pipeline.service` and `deploy/systemd/apify-pipeline.timer` in `/etc/systemd/system/`, adjust `WorkingDirectory` if needed, and set `APIFY_TOKEN` in `/etc/default/apify-pipeline`. Enable with `systemctl enable --now apify-pipeline.timer`.
- Kubernetes: apply `deploy/kubernetes/apify-pipeline-cronjob.yaml`, replace the `image:` with your build, and create a secret named `apify-token` with key `token` holding `APIFY_TOKEN`. The manifest mounts a PVC (`apify-pipeline-pvc`) to persist `apify_pipeline/data/` and `reports/`.
- Containerized cron: `deploy/container/entrypoint.sh` writes the cron entry, starts cron, and tails logs to stdout. Build an image that installs cron and uses this script as the entrypoint; set `APIFY_TOKEN` (env or secret), and optionally override `CRON_SCHEDULE`, `WORKDIR`, `LOGFILE`, or `PIPELINE_CMD`.

### Notes
- The client keeps a per-account `since_id` in SQLite at `apify_pipeline/data/digests.db` (auto-created) to avoid re-fetching old posts.
- Reports are keyword-frequency oriented. To append an LLM summary, pass `--summary-model` (and optionally `--summary-max-posts`) along with `OPENAI_API_KEY` or `DEEPSEEK_API_KEY`. Use `--summary-base-url` if your provider requires it.
- For large account sets, run multiple batches or lower `--limit` to manage cost.

## Database schema and migrations
- SQLite migrations live in `apify_pipeline/sql/` and are applied automatically on startup. The initial migration introduces:
  - `accounts`: normalized handles with crawl state (`since_id`, `latest_timestamp`) and optional metadata.
  - `media`: attachments linked to posts (type, URL, preview, dimensions, description).
