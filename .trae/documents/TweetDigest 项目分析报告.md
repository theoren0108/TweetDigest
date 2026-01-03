这是一个基于 **Python** 和 **Apify** 的推特（X）内容抓取与摘要生成工具，似乎被集成在一个 **Logseq** 知识库目录中。

### 核心功能
该项目的主要目的是**自动化获取指定 Twitter 账号的最新推文**，并生成一份 Markdown 格式的日报（Digest）。

### 工作流程
1.  **抓取 (Fetch)**: 使用 `apify_pipeline/apify_client.py` 调用 Apify 的 `apidojo/tweet-scraper` Actor 来抓取推文。支持 `sample`（本地测试）和 `apify`（在线 API）两种模式。
2.  **存储 (Store)**: 使用 `sqlite3` 数据库 (`apify_pipeline/data/digests.db`) 本地存储推文，并记录每个账号的 `since_id` 以实现**增量更新**（避免重复抓取）。
3.  **分析 (Analyze)**: 使用 `apify_pipeline/analyzer.py` 对最近 48 小时内的推文进行分析，提取高频关键词（排除停用词）。
4.  **报告 (Report)**: 生成 Markdown 格式的报告，包含关键词统计和按账号分组的推文列表。

### 关键文件结构
- **`apify_pipeline/pipeline.py`**: 主程序入口。负责协调数据库初始化、配置读取、API 调用、数据存储和报告生成。
- **`apify_pipeline/apify_client.py`**: Apify 客户端封装。处理 API 请求、轮询任务状态、数据标准化。
- **`apify_pipeline/analyzer.py`**: 分析逻辑。包含关键词提取 (`extract_keywords`) 和 Markdown 报告构建 (`build_report`)。
- **`apify_pipeline/accounts.yml`**: 配置文件。列出了需要监控的 Twitter 账号列表（如 `sama`, `FundaBottom` 等）。
- **`apify_pipeline/input.template.json`**: Apify Actor 的输入参数模板。
- **`logseq/`**: 项目所在的父目录结构表明这是一个 Logseq 图谱，生成的报告（Markdown）可以直接作为 Logseq 的页面或日志内容被浏览。

### 下一步建议
目前我已完成了对项目的分析。如果您需要运行该项目、修改配置、或添加新功能（例如集成 LLM 摘要），请告诉我。