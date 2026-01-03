这是一个将当前项目发布到 GitHub 的计划。在推送之前，我会先更新文档和配置文件以匹配最新的代码变更。

### 1. 更新文档与配置
*   **更新 README.md**:
    *   将 Actor 名称从 `apidojo/tweet-scraper` 更新为 `apidojo/twitter-scraper-lite`。
    *   更新“快速开始”部分，说明如何配置 `.env` 文件和环境变量 `APIFY_TOKEN`。
*   **完善 .gitignore**:
    *   添加 Python 常见忽略文件 (`__pycache__`, `*.pyc`)。
    *   添加 macOS 系统文件 (`.DS_Store`)。
    *   忽略本地数据库 (`apify_pipeline/data/digests.db`) 以避免冲突。

### 2. 初始化 Git 仓库
*   在项目根目录运行 `git init`。
*   添加所有文件并进行首次提交 (`Initial commit`)。

### 3. 发布到 GitHub
*   使用 GitHub API 创建一个新的私有仓库，名称暂定为 `TweetDigest`（如果已存在则尝试其他名称）。
*   将本地仓库关联到远程 GitHub 仓库。
*   推送代码到 `main` 分支。

**注意**: 我将使用您的 GitHub 账号（通过 MCP 工具）来创建仓库。如果您希望使用公开仓库，请在确认时告知，默认为**私有**。