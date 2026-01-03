# Apify-based X digest (apidojo/tweet-scraper)

Minimal scaffold that runs the Apify actor [`apidojo/tweet-scraper`](https://apify.com/apidojo/tweet-scraper) to pull timelines for a list of X accounts, store them incrementally, and emit a lightweight keyword-oriented report.

## Quick start (sample/offline mode)
```bash
python apify_pipeline/pipeline.py --mode sample \
  --report reports/apify-sample.md
```
Uses `sample_data/sample_tweets.jsonl` and writes a Markdown digest to `reports/`.

## Run against Apify
```bash
export APIFY_TOKEN="<your_apify_token>"

python apify_pipeline/pipeline.py --mode apify \
  --token "$APIFY_TOKEN" \
  --report reports/apify-daily.md \
  --limit 10
```
Key flags:
- `--actor-id`: defaults to `apidojo~tweet-scraper`.
- `--input-template`: defaults to `apify_pipeline/input.template.json` (edit if the actor schema changes).
- `--config`: account list (YAML/JSON), defaults to `apify_pipeline/accounts.yml`.
- `--limit`: max tweets per account per run (sets `maxItems`/`tweetsDesired`).

### Notes
- The client keeps a per-account `since_id` in SQLite at `apify_pipeline/data/digests.db` (auto-created) to avoid re-fetching old posts.
- Reports are keyword-frequency only; you can add LLM summarization downstream if desired.
- For large account sets, run multiple batches or lower `--limit` to manage cost.
