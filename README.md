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

### Notes
- The client keeps a per-account `since_id` in SQLite at `apify_pipeline/data/digests.db` (auto-created) to avoid re-fetching old posts.
- Reports are keyword-frequency only; you can add LLM summarization downstream if desired.
- For large account sets, run multiple batches or lower `--limit` to manage cost.
