# Feishu Connector (飞书/Lark 集成库)

这是一个独立的 Python 库，用于简化与飞书（Feishu/Lark）开放平台的交互。主要功能集中在**云文档自动化创建**和**消息通知**，特别优化了权限管理和文档链接生成的体验。

## ✨ 核心功能

- **自动化文档创建**: 将 Markdown 内容直接转换为飞书云文档（Docx）。
- **智能权限管理**: 自动将指定群组（Chat ID）添加为文档协作者，确保群成员可直接访问，无需手动申请权限。
- **消息推送**: 向指定群组发送包含文档链接的通知消息。
- **链接优化**: 优先生成标准文档链接 (`feishu.cn/docx/...`)，避免使用不稳定的分享链接。

## 📂 目录结构

```text
feishu_connector/
├── pyproject.toml          # 依赖配置
├── README.md               # 本说明文件
└── src/
    └── feishu_connector/
        ├── __init__.py     # 导出 FeishuClient 和 FeishuConfig
        ├── client.py       # 核心客户端逻辑 (API 调用、Token 管理)
        └── config.py       # 配置数据类
```

## 🚀 快速开始

### 1. 安装

您可以将此目录作为一个本地包安装，或者直接将 `src/feishu_connector` 复制到您的项目中。

**本地安装**:
```bash
pip install -e .
```

### 2. 使用示例

```python
from feishu_connector import FeishuClient, FeishuConfig

# 1. 初始化配置
config = FeishuConfig(
    app_id="cli_a...",          # 飞书应用 App ID
    app_secret="...",           # 飞书应用 App Secret
    chat_id="oc_...",           # 目标群组 ID (用于接收消息和授予权限)
    # folder_token="...",       # (可选) 将文档归档到指定文件夹 Token
)

# 2. 创建客户端
client = FeishuClient(config)

# 3. 创建文档并发送通知
markdown_content = "# 今日报告\n\n这是自动生成的内容..."
doc_url = client.create_doc_from_markdown("报告标题", markdown_content)

print(f"文档已创建: {doc_url}")
client.send_text_message(doc_url)
```

## ⚙️ 配置说明

| 参数 | 说明 | 必填 |
| :--- | :--- | :--- |
| `app_id` | 飞书自建应用的 App ID | ✅ |
| `app_secret` | 飞书自建应用的 App Secret | ✅ |
| `chat_id` | 接收通知的群组 Open Chat ID | ✅ |
| `folder_token` | 父文件夹 Token (从文件夹 URL 获取) | ❌ |
| `base_url` | API 域名 (默认 `https://open.feishu.cn`) | ❌ |

## 🛠️ 常见问题

**Q: 为什么生成的链接是 `www.feishu.cn` 而不是我的租户域名？**
A: 库默认构造通用链接，飞书会自动将其重定向到您企业专属的租户域名，这是最兼容的做法。

**Q: 为什么文档创建后要“添加群组协作者”？**
A: 为了解决“点击链接提示无权限”的问题。相比于开放“链接分享”设置，直接将群组加入协作者白名单更加安全且稳定。
