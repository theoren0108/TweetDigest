import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# 动态添加 feishu_connector 到路径中，以便直接导入
# 假设 feishu_connector 在项目根目录下，与 apify_pipeline 同级
ROOT = Path(__file__).resolve().parent.parent
FEISHU_LIB_PATH = ROOT / "feishu_connector" / "src"
if str(FEISHU_LIB_PATH) not in sys.path:
    sys.path.insert(0, str(FEISHU_LIB_PATH))

from feishu_connector import FeishuClient, FeishuConfig


def _load_feishu_config() -> Optional[FeishuConfig]:
    if load_dotenv is not None:
        load_dotenv()

    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    chat_id = os.getenv("FEISHU_TARGET_CHAT_ID")
    if not app_id or not app_secret or not chat_id:
        return None

    folder_token = os.getenv("FEISHU_FOLDER_TOKEN") or None
    base_url = os.getenv("FEISHU_BASE_URL", "https://open.feishu.cn")
    return FeishuConfig(
        app_id=app_id,
        app_secret=app_secret,
        chat_id=chat_id,
        folder_token=folder_token,
        base_url=base_url,
    )


def _default_title(report_mode: str) -> str:
    date_str = datetime.now().strftime("%Y%m%d")
    if report_mode == "weekly":
        return f"行业专家周报_{date_str}"
    elif report_mode in ("apify", "sample", "daily"):
        return f"行业专家日报_{date_str}"
    # Fallback for unknown modes
    return f"行业专家报告_{date_str}"


def send_report_to_feishu(markdown_content: str, report_mode: str) -> Optional[str]:
    config = _load_feishu_config()
    if not config:
        print("Feishu config missing; skipping doc creation.")
        return None

    client = FeishuClient(config)
    doc_title = _default_title(report_mode)
    
    # 调用新库的方法创建文档
    doc_url = client.create_doc_from_markdown(doc_title, markdown_content)
    
    # 发送通知
    client.send_text_message(doc_url)
    return doc_url
