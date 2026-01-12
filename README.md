# Apify-based X digest (apidojo/twitter-scraper-lite) v0.3.0

Minimal scaffold that runs the Apify actor [`apidojo/twitter-scraper-lite`](https://apify.com/apidojo/twitter-scraper-lite) to pull timelines for a list of X accounts, store them incrementally, and emit a lightweight keyword-oriented report.

## New in v0.3.0
- **Feishu/Lark Integration**: Automatically pushes reports to a Feishu group chat as cloud documents (Docx).
  - Includes smart permission management: automatically adds the group as a collaborator to ensure seamless access.
  - Logic decoupled into a standalone `feishu_connector` library for easy reuse.
- **Weekly Report Mode**: New `--mode weekly` aggregates posts from the past 7 days (ignoring "summarized" status) for a deeper retrospective, optimized for reasoning models like `deepseek-reasoner`.
- **Reliability Fixes**: Improved Feishu document link generation (using standard `feishu.cn` URLs) and permissions handling.

## New in v0.2.2
- **Incremental Summarization**: The LLM summary now only processes new posts that haven't been summarized in previous runs.
- **Cost Optimization**: Default fetch limit reduced to 20 posts per account.
- **Enhanced Reliability**: Switched network client to `requests`.

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
python apify_pipeline/pipeline.py --mode apify --limit 20
```

Key flags:
- `--actor-id`: defaults to `apidojo~twitter-scraper-lite`.
- `--input-template`: defaults to `apify_pipeline/input.template.json` (edit if the actor schema changes).
- `--config`: account list (YAML/JSON), defaults to `apify_pipeline/accounts.yml`.
- `--limit`: max tweets per account per run (sets `maxItems`). Defaults to 20.
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
  - `posts`: stores post content and summarization status (`is_summarized`).

---

# 中文说明 (Chinese Translation)

# 基于 Apify 的 X (Twitter) 摘要生成器 (使用 apidojo/twitter-scraper-lite) v0.3.0

这是一个极简的脚手架工具，用于运行 Apify actor [`apidojo/twitter-scraper-lite`](https://apify.com/apidojo/twitter-scraper-lite)，抓取指定 X (Twitter) 账号的时间线，增量存储数据，并生成基于关键词的轻量级报告。

## v0.3.0 更新内容
- **飞书/Lark 集成**: 自动将报告以云文档 (Docx) 形式推送到飞书群聊。
  - **智能权限**: 自动将目标群组添加为文档协作者，确保群内成员无需申请权限即可直接打开。
  - **模块化**: 飞书核心逻辑已拆分为独立的 `feishu_connector` 库。
- **周报模式**: 新增 `--mode weekly`，汇总过去 7 天的所有推文（不限是否已摘要），适合配合 `deepseek-reasoner` 等推理模型生成深度周报。
- **体验优化**: 修复了文档链接无法打开的问题，并优化了标题生成逻辑。

## v0.2.2 更新内容
- **增量总结**：LLM 摘要现在仅处理之前运行中未被总结的新推文。
- **成本优化**：默认抓取限制降至每账号 20 条。
- **可靠性增强**：网络客户端切换至 `requests` 库。

## 快速开始 (样本/离线模式)
```bash
python apify_pipeline/pipeline.py --mode sample \
  --report reports/apify-sample.md
```
使用 `sample_data/sample_tweets.jsonl` 中的数据，并在 `reports/` 目录下生成 Markdown 摘要。

## 运行 Apify 抓取

### 1. 配置环境
复制环境变量示例文件并添加您的 Apify Token：
```bash
cp .env.example .env
# 编辑 .env 文件并设置 APIFY_TOKEN="您的_token"
```

### 2. 运行流水线
```bash
# 加载环境变量 (如果未通过其他工具自动加载) 并运行
export $(cat .env | xargs)
python apify_pipeline/pipeline.py --mode apify --limit 20
```

关键参数：
- `--actor-id`: 默认为 `apidojo~twitter-scraper-lite`。
- `--input-template`: 默认为 `apify_pipeline/input.template.json` (如果 actor schema 变更，请修改此文件)。
- `--config`: 账号列表 (YAML/JSON)，默认为 `apify_pipeline/accounts.yml`。
- `--limit`: 每次运行每个账号抓取的最大推文数 (设置 `maxItems`)。默认为 20。
- `--summary-model`: 可选的 OpenAI 兼容模型 ID (例如 `gpt-4o-mini` 或 DeepSeek 的 `deepseek-reasoner`)，用于在报告末尾附加 LLM 生成的摘要。需设置 `OPENAI_API_KEY` 或 `DEEPSEEK_API_KEY` (或通过 `--summary-api-key` 传递)，并安装 `openai` Python 包。对于非 OpenAI 服务商，请传递 `--summary-base-url` (例如 `https://api.deepseek.com`)。

## 定时任务 (Cron/Systemd/Kubernetes)
- **Cron**: 将 `deploy/cron/apify-pipeline.cron` 复制到 `/etc/cron.d/`，在 `/etc/default/apify-pipeline` 中设置 `APIFY_TOKEN`，并 (可选) 设置 `WORKDIR`/`LOGFILE`。任务默认在 UTC 时间 `0 0,12 * * *` 运行。
- **Systemd**: 将 `deploy/systemd/apify-pipeline.service` 和 `deploy/systemd/apify-pipeline.timer` 放置在 `/etc/systemd/system/`，根据需要调整 `WorkingDirectory`，并在 `/etc/default/apify-pipeline` 中设置 `APIFY_TOKEN`。使用 `systemctl enable --now apify-pipeline.timer` 启用。
- **Kubernetes**: 应用 `deploy/kubernetes/apify-pipeline-cronjob.yaml`，替换 `image:` 为您的构建镜像，并创建一个名为 `apify-token` 的 secret，其中 key `token` 包含 `APIFY_TOKEN`。该清单挂载了一个 PVC (`apify-pipeline-pvc`) 用于持久化 `apify_pipeline/data/` 和 `reports/`。
- **容器化 Cron**: `deploy/container/entrypoint.sh` 负责写入 cron 条目，启动 cron 并将日志输出到 stdout。构建镜像时安装 cron 并将此脚本作为入口点；设置 `APIFY_TOKEN` (环境变量或 secret)，并可选地覆盖 `CRON_SCHEDULE`, `WORKDIR`, `LOGFILE` 或 `PIPELINE_CMD`。

### 注意事项
- 客户端会在 `apify_pipeline/data/digests.db` (自动创建) 的 SQLite 数据库中保存每个账号的 `since_id`，以避免重复抓取旧推文。
- 报告主要基于关键词频率。如需附加 LLM 摘要，请传递 `--summary-model` (可选 `--summary-max-posts`) 以及 `OPENAI_API_KEY` 或 `DEEPSEEK_API_KEY`。如果服务商需要，请使用 `--summary-base-url`。
- 对于大量账号集合，建议分批运行或降低 `--limit` 以控制成本。

## 数据库 Schema 与迁移
- SQLite 迁移文件位于 `apify_pipeline/sql/`，并在启动时自动应用。初始迁移包含：
  - `accounts`: 标准化的账号句柄及抓取状态 (`since_id`, `latest_timestamp`) 和可选元数据。
  - `media`: 关联到推文的附件 (类型, URL, 预览图, 尺寸, 描述)。
  - `posts`: 存储推文内容及摘要状态 (`is_summarized`)。
