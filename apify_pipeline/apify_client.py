import json
import time
import urllib.parse
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests


class ApifyTweetScraperClient:
    """
    Thin wrapper to run apidojo/tweet-scraper on Apify and normalize output items.

    Modes:
    - "sample": load posts from a JSONL file for offline/local testing.
    - "apify": run the actor through the Apify REST API and read dataset items.
    """

    def __init__(
        self,
        token: Optional[str],
        actor_id: str = "apidojo~tweet-scraper",
        base_url: str = "https://api.apify.com/v2",
        mode: str = "sample",
        sample_file: Optional[Path] = None,
        input_template: Optional[Dict] = None,
        poll_interval: int = 5,
        timeout_seconds: int = 120,
    ):
        self.token = token
        self.actor_id = actor_id
        self.base_url = base_url.rstrip("/")
        self.mode = mode
        self.sample_file = sample_file or Path(__file__).parent / "sample_data" / "sample_tweets.jsonl"
        self.base_input = deepcopy(input_template) if input_template else {}
        self.poll_interval = poll_interval
        self.timeout_seconds = timeout_seconds

    def fetch_accounts(
        self,
        handles: List[str],
        since_map: Dict[str, Optional[str]],
        since_ts_map: Dict[str, Optional[str]],
        limit: int = 50,
        max_total_limit: int = 500,
    ) -> List[Dict]:
        normalized_handles = [self._normalize_handle(h) for h in handles if h]
        since_map = {self._normalize_handle(k): v for k, v in since_map.items() if k}
        since_ts_map = {self._normalize_handle(k): v for k, v in since_ts_map.items() if k}

        if not normalized_handles:
            return []

        if self.mode == "sample":
            return self._load_sample(normalized_handles, since_map, since_ts_map, limit)
        if self.mode != "apify":
            raise ValueError(f"Unsupported mode: {self.mode}")
        return self._run_actor(normalized_handles, since_map, since_ts_map, limit, max_total_limit)

    def _load_sample(
        self,
        handles: List[str],
        since_map: Dict[str, Optional[str]],
        since_ts_map: Dict[str, Optional[str]],
        limit: int,
    ) -> List[Dict]:
        if not self.sample_file.exists():
            return []
        buckets: Dict[str, List[Dict]] = {h: [] for h in handles}
        with self.sample_file.open("r", encoding="utf-8") as fh:
            for line in fh:
                payload = json.loads(line)
                author = self._normalize_handle(payload.get("author", ""))
                if author not in buckets:
                    continue
                tweet_id = str(payload.get("id"))
                buckets[author].append(
                    {
                        "id": tweet_id,
                        "author": author,
                        "created_at": self._coerce_timestamp(payload.get("created_at")),
                        "text": payload.get("text", ""),
                        "url": payload.get("url", ""),
                        "media": payload.get("media") or [],
                    }
                )

        return self._collect_sorted_posts(handles, buckets, since_map, since_ts_map, limit)

    def _run_actor(
        self,
        handles: List[str],
        since_map: Dict[str, Optional[str]],
        since_ts_map: Dict[str, Optional[str]],
        limit: int,
        max_total_limit: int,
    ) -> List[Dict]:
        if not self.token:
            raise RuntimeError("Apify token is required in apify mode")

        input_payload = self._build_input(handles, since_map, since_ts_map, limit, max_total_limit)
        
        run_data = self._start_run(input_payload)
        if run_data.get("status") != "SUCCEEDED":
            run_id = run_data.get("id")
            print(f"Actor run started: {run_id}. Polling for completion...")
            run_data = self._poll_run(run_id)
            
        if run_data.get("status") != "SUCCEEDED":
            print(f"Run failed or timed out: {run_data.get('status')}")
            return []

        dataset_id = run_data.get("defaultDatasetId")
        if not dataset_id:
            return []
        items = self._fetch_dataset_items(dataset_id)
        return self._normalize_items(items, handles, since_map, since_ts_map, limit)

    def _build_input(
        self,
        handles: List[str],
        since_map: Dict[str, Optional[str]],
        since_ts_map: Dict[str, Optional[str]],
        per_account_limit: int,
        max_total_limit: int,
    ) -> Dict:
        payload = deepcopy(self.base_input)
        per_account_limit = max(per_account_limit, 1)
        total_limit = min(max(per_account_limit * max(len(handles), 1), 1), max_total_limit)
        
        # apidojo/twitter-scraper-lite uses 'searchTerms'
        search_terms = []
        now_date = datetime.now(timezone.utc).date()
        for handle in handles:
            query_parts = [f"from:{handle}"]
            since_ts = since_ts_map.get(handle)
            since_date = self._coerce_to_date(since_ts, fallback_days=2)
            if since_date:
                query_parts.append(f"since:{since_date.isoformat()}")
                until_date = max(since_date, now_date)
                query_parts.append(f"until:{(until_date + timedelta(days=3)).isoformat()}")
            search_terms.append(" ".join(query_parts))
            
        payload["searchTerms"] = search_terms
        payload["maxItems"] = total_limit
        
        # Clean up any potential legacy fields
        for key in ["handles", "usernames", "startUrls", "tweetsDesired", "author"]:
            payload.pop(key, None)
            
        return payload

    def _start_run(self, input_payload: Dict) -> Dict:
        url = (
            f"{self.base_url}/acts/{urllib.parse.quote(self.actor_id)}/runs"
            f"?token={urllib.parse.quote(self.token or '')}&waitForFinish={self.timeout_seconds}"
        )
        response = requests.post(url, json=input_payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        body = response.json()
        return body.get("data") or {}

    def _poll_run(self, run_id: Optional[str]) -> Dict:
        if not run_id:
            raise RuntimeError("Missing run id when polling Apify")
        deadline = time.time() + self.timeout_seconds
        status_url = f"{self.base_url}/runs/{urllib.parse.quote(run_id)}?token={urllib.parse.quote(self.token or '')}"
        terminal_states = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED", "CANCELLED"}

        while time.time() < deadline:
            response = requests.get(status_url)
            response.raise_for_status()
            body = response.json()
            data = body.get("data") or {}
            if data.get("status") in terminal_states:
                return data
            time.sleep(self.poll_interval)

        raise TimeoutError(f"Apify run {run_id} did not finish in {self.timeout_seconds} seconds")

    def _fetch_dataset_items(self, dataset_id: str) -> List[Dict]:
        url = (
            f"{self.base_url}/datasets/{urllib.parse.quote(dataset_id)}/items"
            f"?token={urllib.parse.quote(self.token or '')}&format=json"
        )
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def _normalize_items(
        self,
        items: Iterable[Dict],
        handles: List[str],
        since_map: Dict[str, Optional[str]],
        since_ts_map: Dict[str, Optional[str]],
        limit: int,
    ) -> List[Dict]:
        buckets: Dict[str, List[Dict]] = {h: [] for h in handles}
        handle_set = set(handles)

        for raw in items:
            post = self._normalize_item(raw)
            if not post:
                continue
            author = self._normalize_handle(post["author"])
            if author not in handle_set:
                continue
            buckets[author].append(
                {
                    "id": str(post["id"]),
                    "author": author,
                    "created_at": post["created_at"],
                    "text": post["text"],
                    "url": post["url"],
                    "media": post.get("media") or [],
                }
            )

        return self._collect_sorted_posts(handles, buckets, since_map, since_ts_map, limit)

    def _normalize_item(self, raw: Dict) -> Optional[Dict]:
        tweet_id = raw.get("id_str") or raw.get("id") or raw.get("tweetId")
        if tweet_id is None:
            return None

        author = (
            raw.get("author")
            or raw.get("username")
            or raw.get("userName")
            or raw.get("user", {}).get("username")
            or raw.get("user", {}).get("screen_name")
        )
        if isinstance(author, dict):
            author = author.get("userName") or author.get("username") or author.get("screen_name") or author.get("name")
            
        if not author:
            return None

        created_at = (
            raw.get("created_at")
            or raw.get("createdAt")
            or raw.get("timestamp")
            or raw.get("date")
        )
        created_iso = self._coerce_timestamp(created_at)

        text = raw.get("full_text") or raw.get("text") or raw.get("tweet") or ""
        url = raw.get("url")
        if not url and author and tweet_id:
            url = f"https://x.com/{self._normalize_handle(author)}/status/{tweet_id}"

        media = self._extract_media(raw, str(tweet_id))

        return {
            "id": str(tweet_id),
            "author": self._normalize_handle(author),
            "created_at": created_iso or "",
            "text": text,
            "url": url or "",
            "media": media,
        }

    @staticmethod
    def _coerce_timestamp(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(float(raw), tz=timezone.utc).isoformat()
        if isinstance(raw, datetime):
            return raw.astimezone(timezone.utc).isoformat()
        text = str(raw)
        for candidate in (text, text.replace("Z", "+00:00")):
            try:
                dt = datetime.fromisoformat(candidate)
                return dt.astimezone(timezone.utc).isoformat()
            except Exception:
                continue
        try:
            dt = parsedate_to_datetime(text)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return None

    @staticmethod
    def _normalize_handle(handle: str) -> str:
        return handle.lower().lstrip("@").strip()

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None

    def _collect_sorted_posts(
        self,
        handles: List[str],
        buckets: Dict[str, List[Dict]],
        since_map: Dict[str, Optional[str]],
        since_ts_map: Dict[str, Optional[str]],
        limit: int,
    ) -> List[Dict]:
        posts: List[Dict] = []
        for handle in handles:
            author_posts = buckets.get(handle, [])
            sorted_posts = sorted(
                author_posts,
                key=lambda p: (
                    self._parse_timestamp(p.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc),
                    str(p.get("id", "")),
                ),
                reverse=True,
            )
            since_id = since_map.get(handle)
            since_ts = self._parse_timestamp(since_ts_map.get(handle))
            appended = 0

            for post in sorted_posts:
                tweet_id = str(post.get("id"))
                created_ts = self._parse_timestamp(post.get("created_at"))

                if since_id and tweet_id <= str(since_id):
                    break
                if since_ts and created_ts and created_ts <= since_ts:
                    break

                posts.append(post)
                appended += 1
                if appended >= limit:
                    break
        return posts

    def _extract_media(self, raw: Dict, tweet_id: str) -> List[Dict]:
        media_sources: List[Dict] = []
        direct_media = raw.get("media")
        if isinstance(direct_media, list):
            media_sources.extend(direct_media)
        elif isinstance(direct_media, dict):
            media_sources.append(direct_media)

        attachments = raw.get("attachments") or {}
        attachments_media = attachments.get("media")
        if isinstance(attachments_media, list):
            media_sources.extend(attachments_media)
        elif isinstance(attachments_media, dict):
            media_sources.append(attachments_media)

        extended_entities = raw.get("extended_entities") or {}
        extended_media = extended_entities.get("media")
        if isinstance(extended_media, list):
            media_sources.extend(extended_media)

        media_items: List[Dict] = []
        for idx, media in enumerate(media_sources):
            if not isinstance(media, dict):
                continue
            media_id = media.get("id_str") or media.get("id") or media.get("media_key") or f"{tweet_id}-media-{idx}"
            media_url = (
                media.get("media_url_https")
                or media.get("media_url")
                or media.get("url")
                or media.get("expanded_url")
                or media.get("preview_image_url")
            )
            preview_url = media.get("preview_image_url") or media.get("thumbnail") or media.get("thumbnail_url")
            media_items.append(
                {
                    "id": str(media_id),
                    "type": media.get("type"),
                    "url": media_url,
                    "preview_url": preview_url,
                    "width": media.get("width") or media.get("original_width"),
                    "height": media.get("height") or media.get("original_height"),
                    "description": media.get("alt_text") or media.get("description"),
                }
            )
        return media_items

    def _coerce_to_date(self, timestamp: Optional[str], fallback_days: int = 1) -> Optional[date]:
        dt = self._parse_timestamp(timestamp)
        if not dt:
            dt = datetime.now(timezone.utc)
        return (dt - timedelta(days=fallback_days)).date()
