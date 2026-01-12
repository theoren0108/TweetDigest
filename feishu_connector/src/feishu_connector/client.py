import json
import time
from typing import Optional, Tuple

import requests
from .config import FeishuConfig


class FeishuClient:
    def __init__(self, config: FeishuConfig) -> None:
        self.config = config
        self._token_cache = {"access_token": "", "expire_at": 0}

    def _request(
        self, method: str, url: str, token: Optional[str], payload: Optional[dict], timeout: int
    ) -> dict:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = requests.request(method, url, headers=headers, json=payload, timeout=timeout)
        try:
            result = response.json()
        except ValueError:
            raise RuntimeError(
                f"Feishu API invalid response ({response.status_code}): {response.text}"
            )

        if response.status_code >= 400:
            message = result.get("msg") if isinstance(result, dict) else response.text
            raise RuntimeError(f"Feishu API HTTP {response.status_code}: {message}")

        if result.get("code") not in (0, None):
            raise RuntimeError(f"Feishu API error: {result.get('msg')}")

        return result

    def _get_tenant_token(self, force_refresh: bool = False) -> str:
        now = time.time()
        cached_token = self._token_cache["access_token"]
        if not force_refresh and cached_token and now < self._token_cache["expire_at"]:
            return cached_token

        url = f"{self.config.base_url}/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": self.config.app_id, "app_secret": self.config.app_secret}
        result = self._request("POST", url, None, payload, timeout=10)

        token = result["tenant_access_token"]
        expire = int(result.get("expire", 7200))
        self._token_cache["access_token"] = token
        self._token_cache["expire_at"] = now + expire - 60
        return token

    def create_doc_from_markdown(self, title: str, markdown_content: str) -> str:
        token = self._get_tenant_token()
        document_id, doc_url = self._create_doc(title, token)

        # 尝试将群组添加为协作者，确保群成员可访问
        if self.config.chat_id:
            self._add_perm_member(token, document_id, "openchat", self.config.chat_id, "view")

        blocks = self._convert_markdown(markdown_content, token)
        if blocks:
            self._append_blocks(document_id, document_id, blocks, token)

        # 尝试获取分享链接
        share_url = self._share_doc_to_tenant(document_id, token)

        # 优先返回原始文档链接，因为添加协作者后群成员应可直接访问
        if doc_url:
            return doc_url

        # 如果 API 未返回 URL，尝试获取或使用构造链接
        if not doc_url:
            try:
                doc_url = self._get_doc_url(document_id, token)
                if doc_url:
                    return doc_url
            except Exception:
                pass

        # 如果 API 未返回 URL，构造标准文档访问链接
        # 注意：使用 www.feishu.cn 通用域名，它会自动重定向到租户域名
        return f"https://www.feishu.cn/docx/{document_id}"

    def _add_perm_member(
        self, token: str, document_id: str, member_type: str, member_id: str, perm: str
    ) -> None:
        """
        添加文档协作者
        API文档: https://open.feishu.cn/document/server-docs/docs/permission/permission-member/create
        """
        # 注意: type=docx 必须作为 Query Parameter
        url = (
            f"{self.config.base_url}/open-apis/drive/v1/permissions/{document_id}/members?type=docx"
        )

        payload = {"member_type": member_type, "member_id": member_id, "perm": perm}
        try:
            self._request("POST", url, token, payload, timeout=10)
        except RuntimeError as exc:
            print(
                f"Warning: Failed to add permission member (type={member_type}, id={member_id}): {exc}"
            )

    def _create_doc(self, title: str, token: str) -> Tuple[str, Optional[str]]:
        url = f"{self.config.base_url}/open-apis/docx/v1/documents"
        payload = {"title": title}
        if self.config.folder_token:
            payload["folder_token"] = self.config.folder_token

        try:
            result = self._request("POST", url, token, payload, timeout=15)
        except RuntimeError as exc:
            if self.config.folder_token and "folder not found" in str(exc).lower():
                payload.pop("folder_token", None)
                result = self._request("POST", url, token, payload, timeout=15)
            else:
                raise

        data = result.get("data", {})
        document = data.get("document", {})
        document_id = document.get("document_id") or data.get("document_id")
        if not document_id:
            raise RuntimeError("Feishu docx create response missing document_id")
        return document_id, document.get("url")

    def _convert_markdown(self, markdown_content: str, token: str) -> list[dict]:
        if not markdown_content.strip():
            return []

        url = f"{self.config.base_url}/open-apis/docx/v1/document/convert"
        payload = {"source_content": markdown_content, "content_type": "markdown"}
        try:
            result = self._request("POST", url, token, payload, timeout=15)
        except RuntimeError as exc:
            if "404" in str(exc).lower() and "page not found" in str(exc).lower():
                return self._markdown_to_plain_blocks(markdown_content)
            raise

        data = result.get("data", {})
        blocks = data.get("blocks") or data.get("children")
        if blocks is None:
            raise RuntimeError("Feishu markdown convert response missing blocks")
        return blocks

    def _markdown_to_plain_blocks(self, markdown_content: str) -> list[dict]:
        blocks: list[dict] = []
        for line in markdown_content.splitlines():
            content = line.rstrip()
            if not content:
                content = " "
            blocks.append(
                {
                    "block_type": 2,
                    "text": {"elements": [{"text_run": {"content": content}}]},
                }
            )
        return blocks

    def _append_blocks(
        self,
        document_id: str,
        parent_block_id: str,
        blocks: list[dict],
        token: str,
        batch_size: int = 50,
    ) -> None:
        url = f"{self.config.base_url}/open-apis/docx/v1/documents/{document_id}/blocks/{parent_block_id}/children"
        for start in range(0, len(blocks), batch_size):
            batch = blocks[start : start + batch_size]
            payload = {"children": batch, "index": -1}
            self._request("POST", url, token, payload, timeout=15)

    def _share_doc_to_tenant(self, document_id: str, token: str) -> Optional[str]:
        patch_url = (
            f"{self.config.base_url}/open-apis/drive/v2/permissions/{document_id}/public"
            "?type=docx"
        )
        payload = {
            "external_access_entity": "closed",
            "security_entity": "anyone_can_view",
            "comment_entity": "anyone_can_view",
            "share_entity": "same_tenant",
            "link_share_entity": "tenant_readable",
        }
        result = self._request("PATCH", patch_url, token, payload, timeout=10)
        share_url = result.get("data", {}).get("share_url")
        if share_url:
            return share_url

        get_url = (
            f"{self.config.base_url}/open-apis/drive/v2/permissions/{document_id}/public"
            "?type=docx"
        )
        result = self._request("GET", get_url, token, None, timeout=10)
        return result.get("data", {}).get("share_url")

    def _get_doc_url(self, document_id: str, token: str) -> Optional[str]:
        url = f"{self.config.base_url}/open-apis/docx/v1/documents/{document_id}"
        result = self._request("GET", url, token, None, timeout=10)
        data = result.get("data", {})
        document = data.get("document", {})
        return document.get("url")

    def send_text_message(self, text: str) -> None:
        token = self._get_tenant_token()
        url = f"{self.config.base_url}/open-apis/im/v1/messages?receive_id_type=chat_id"
        payload = {
            "receive_id": self.config.chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        }
        self._request("POST", url, token, payload, timeout=10)
