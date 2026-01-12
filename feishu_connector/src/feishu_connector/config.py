from dataclasses import dataclass
from typing import Optional

@dataclass
class FeishuConfig:
    app_id: str
    app_secret: str
    chat_id: str
    folder_token: Optional[str] = None
    base_url: str = "https://open.feishu.cn"
