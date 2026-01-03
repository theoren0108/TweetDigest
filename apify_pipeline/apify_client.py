import json
import ssl
import time
import urllib.parse
import urllib.request
from copy import deepcopy
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional


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
        self, handles: List[str], since_map: Dict[str, Optional[str]], limit: int = 50
    ) -> List[Dict]:
        normalized_handles = [self._normalize_handle(h) for h in handles if h]
        since_map = {self._normalize_handle(k): v for k, v in since_map.items() if k}

        if not normalized_handles:
            return []

        if self.mode == "sample":
            return self._load_sample(normalized_handles, since_map, limit)
        if self.mode != "apify":
            raise ValueError(f"Unsupported mode: {self.mode}")
        return self._run_actor(normalized_handles, since_map, limit)

    def _load_sample(
        self, handles: List[str], since_map: Dict[str, Optional[str]], limit: int
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
                if since_map.get(author) and tweet_id <= str(since_map[author]):
                    continue
                buckets[author].append(
                    {
                        "id": tweet_id,
                        "author": author,
                        "created_at": self._coerce_timestamp(payload.get("created_at")),
                        "text": payload.get("text", ""),
                        "url": payload.get("url", ""),
                    }
                )

        posts: List[Dict] = []
        for handle in handles:
            posts.extend(sorted(buckets.get(handle, []), key=lambda p: str(p["id"]))[:limit])
        return posts

    def _run_actor(
        self, handles: List[str], since_map: Dict[str, Optional[str]], limit: int
    ) -> List[Dict]:
        if not self.token:
            raise RuntimeError("Apify token is required in apify mode")

        input_payload = self._build_input(handles, since_map, limit)
        run_data = self._start_run(input_payload)
        if run_data.get("status") != "SUCCEEDED":
            run_id = run_data.get("id")
            run_data = self._poll_run(run_id)
        dataset_id = run_data.get("defaultDatasetId")
        if not dataset_id:
            return []
        items = self._fetch_dataset_items(dataset_id)
        return self._normalize_items(items, handles, since_map, limit)

    def _build_input(
        self, handles: List[str], since_map: Dict[str, Optional[str]], limit: int
    ) -> Dict:
        payload = deepcopy(self.base_input)
        total_limit = max(limit * max(len(handles), 1), 1)
        
        # apidojo/twitter-scraper-lite uses 'searchTerms'
        # To get profile tweets, we use 'from:username'
        search_terms = []
        for handle in handles:
            query = f"from:{handle}"
            # Note: The Lite actor might not support since_id in search terms directly like API v1.1
            # But standard search operators include since_id or since:YYYY-MM-DD
            # However, mapping since_id to date might be complex without looking up the snowflake ID.
            # For simplicity, we just search 'from:handle'. 
            # If we wanted to be more precise, we could add 'since:YYYY-MM-DD' if we computed it.
            search_terms.append(query)
            
        payload["searchTerms"] = search_terms
        payload["maxItems"] = total_limit
        
        # Clean up any potential legacy fields from template if they exist
        for key in ["handles", "usernames", "startUrls", "tweetsDesired", "author"]:
            payload.pop(key, None)
            
        return payload

    def _start_run(self, input_payload: Dict) -> Dict:
        url = (
            f"{self.base_url}/acts/{urllib.parse.quote(self.actor_id)}/runs"
            f"?token={urllib.parse.quote(self.token or '')}&waitForFinish={self.timeout_seconds}"
        )
        request = urllib.request.Request(
            url,
            data=json.dumps(input_payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds, context=ssl._create_unverified_context()) as response:
            body = json.loads(response.read().decode("utf-8"))
        return body.get("data") or {}

    def _poll_run(self, run_id: Optional[str]) -> Dict:
        if not run_id:
            raise RuntimeError("Missing run id when polling Apify")
        deadline = time.time() + self.timeout_seconds
        status_url = f"{self.base_url}/runs/{urllib.parse.quote(run_id)}?token={urllib.parse.quote(self.token)}"
        terminal_states = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED", "CANCELLED"}

        while time.time() < deadline:
            request = urllib.request.Request(status_url, method="GET")
            with urllib.request.urlopen(request, timeout=self.poll_interval + 5, context=ssl._create_unverified_context()) as response:
                body = json.loads(response.read().decode("utf-8"))
            data = body.get("data") or {}
            if data.get("status") in terminal_states:
                if data.get("status") != "SUCCEEDED":
                    raise RuntimeError(f"Apify run {run_id} ended with status {data.get('status')}")
                return data
            time.sleep(self.poll_interval)

        raise TimeoutError(f"Apify run {run_id} did not finish in {self.timeout_seconds} seconds")

    def _fetch_dataset_items(self, dataset_id: str) -> List[Dict]:
        url = (
            f"{self.base_url}/datasets/{urllib.parse.quote(dataset_id)}/items"
            f"?token={urllib.parse.quote(self.token or '')}&format=json"
        )
        request = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(request, timeout=self.timeout_seconds, context=ssl._create_unverified_context()) as response:
            return json.loads(response.read().decode("utf-8"))

    def _normalize_items(
        self,
        items: Iterable[Dict],
        handles: List[str],
        since_map: Dict[str, Optional[str]],
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
            tweet_id = str(post["id"])
            if since_map.get(author) and tweet_id <= str(since_map[author]):
                continue
            buckets[author].append(
                {
                    "id": tweet_id,
                    "author": author,
                    "created_at": post["created_at"],
                    "text": post["text"],
                    "url": post["url"],
                }
            )

        posts: List[Dict] = []
        for handle in handles:
            posts.extend(sorted(buckets.get(handle, []), key=lambda p: str(p["id"]))[:limit])
        return posts

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
        # Handle case where author is a dictionary (e.g. from some Apify actors)
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

        return {
            "id": str(tweet_id),
            "author": self._normalize_handle(author),
            "created_at": created_iso or "",
            "text": text,
            "url": url or "",
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
