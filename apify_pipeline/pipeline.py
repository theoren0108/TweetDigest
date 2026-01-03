import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apify_pipeline.analyzer import build_report, parse_timestamp, summarize_posts
from apify_pipeline.apify_client import ApifyTweetScraperClient


def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            author TEXT,
            created_at TEXT,
            text TEXT,
            url TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS since_ids (
            account TEXT PRIMARY KEY,
            since_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS latest_timestamps (
            account TEXT PRIMARY KEY,
            latest_timestamp TEXT
        )
        """
    )
    apply_sql_migrations(conn, Path(__file__).parent / "sql")
    conn.commit()
    return conn


def apply_sql_migrations(conn: sqlite3.Connection, migrations_dir: Path) -> None:
    migrations_dir.mkdir(parents=True, exist_ok=True)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()

    applied = {row[0] for row in conn.execute("SELECT name FROM schema_migrations")}
    for path in sorted(migrations_dir.glob("*.sql")):
        name = path.name
        if name in applied:
            continue
        sql = path.read_text(encoding="utf-8")
        conn.executescript(sql)
        conn.execute("INSERT INTO schema_migrations(name) VALUES (?)", (name,))
    conn.commit()


def read_accounts(config_path: Path) -> List[str]:
    text = config_path.read_text(encoding="utf-8").strip()

    if text.startswith("{") or text.startswith("["):
        data = json.loads(text)
        if isinstance(data, dict):
            accounts = data.get("accounts", [])
        else:
            accounts = data
        return [str(acc).strip() for acc in accounts if acc]

    accounts: List[str] = []
    in_accounts = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("accounts"):
            in_accounts = True
            continue
        if in_accounts and stripped.startswith("-"):
            accounts.append(stripped.lstrip("-").strip())
    return [acc for acc in accounts if acc]


def get_since_id(conn: sqlite3.Connection, account: str) -> Optional[str]:
    since_id, _ = get_since_state(conn, account)
    return since_id


def set_since_id(conn: sqlite3.Connection, account: str, since_id: str, latest_timestamp: Optional[str] = None) -> None:
    set_since_state(conn, account, since_id=since_id, latest_timestamp=latest_timestamp)


def get_latest_timestamp(conn: sqlite3.Connection, account: str) -> Optional[str]:
    _, latest_ts = get_since_state(conn, account)
    return latest_ts


def set_latest_timestamp(conn: sqlite3.Connection, account: str, latest_timestamp: str) -> None:
    set_since_state(conn, account, latest_timestamp=latest_timestamp)


def get_since_state(conn: sqlite3.Connection, account: str) -> Tuple[Optional[str], Optional[str]]:
    normalized = account.lower()
    cur = conn.execute("SELECT since_id, latest_timestamp FROM accounts WHERE handle = ?", (normalized,))
    account_row = cur.fetchone()
    if account_row:
        acc_since_id, acc_latest_ts = account_row
    else:
        acc_since_id, acc_latest_ts = None, None

    cur = conn.execute("SELECT since_id FROM since_ids WHERE account = ?", (account,))
    since_row = cur.fetchone()
    cur = conn.execute("SELECT latest_timestamp FROM latest_timestamps WHERE account = ?", (account,))
    ts_row = cur.fetchone()
    since_id = since_row[0] if since_row else acc_since_id
    latest_ts = ts_row[0] if ts_row else acc_latest_ts
    return since_id, latest_ts


def set_since_state(
    conn: sqlite3.Connection,
    account: str,
    since_id: Optional[str] = None,
    latest_timestamp: Optional[str] = None,
) -> None:
    normalized = account.lower()
    if since_id is not None or latest_timestamp is not None:
        conn.execute(
            """
            INSERT INTO accounts(handle, platform, since_id, latest_timestamp, updated_at)
            VALUES(?, 'x', ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(handle) DO UPDATE SET
                since_id = COALESCE(excluded.since_id, accounts.since_id),
                latest_timestamp = COALESCE(excluded.latest_timestamp, accounts.latest_timestamp),
                updated_at = CURRENT_TIMESTAMP
            """,
            (normalized, since_id, latest_timestamp),
        )
    if since_id is not None:
        conn.execute(
            """
            INSERT INTO since_ids(account, since_id)
            VALUES(?, ?)
            ON CONFLICT(account) DO UPDATE SET since_id = excluded.since_id
            """,
            (account, since_id),
        )
    if latest_timestamp is not None:
        conn.execute(
            """
            INSERT INTO latest_timestamps(account, latest_timestamp)
            VALUES(?, ?)
            ON CONFLICT(account) DO UPDATE SET latest_timestamp = excluded.latest_timestamp
            """,
            (account, latest_timestamp),
        )
    conn.commit()


def store_posts(conn: sqlite3.Connection, posts: Iterable[Dict]) -> None:
    accounts: Set[str] = set()
    media_rows = []
    normalized_posts = []

    for post in posts:
        post_id = str(post.get("id"))
        author = str(post.get("author", "")).lower()
        normalized_posts.append(
            {
                "id": post_id,
                "author": author,
                "created_at": post.get("created_at"),
                "text": post.get("text"),
                "url": post.get("url"),
            }
        )
        if author:
            accounts.add(author)

        media_items = post.get("media") or []
        for idx, media in enumerate(media_items):
            media_id = media.get("id") or media.get("media_key") or f"{post_id}-media-{idx}"
            media_rows.append(
                (
                    str(media_id),
                    post_id,
                    media.get("type"),
                    media.get("url"),
                    media.get("preview_url"),
                    media.get("width"),
                    media.get("height"),
                    media.get("description"),
                )
            )

    if accounts:
        conn.executemany(
            """
            INSERT INTO accounts(handle, platform)
            VALUES(?, 'x')
            ON CONFLICT(handle) DO NOTHING
            """,
            [(account,) for account in accounts],
        )

    conn.executemany(
        "INSERT OR IGNORE INTO posts(id, author, created_at, text, url) VALUES(:id, :author, :created_at, :text, :url)",
        normalized_posts,
    )

    if media_rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO media(id, post_id, type, url, preview_url, width, height, description)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            media_rows,
        )
    conn.commit()


def load_posts_in_window(conn: sqlite3.Connection, window_hours: int = 48) -> List[Dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    cur = conn.execute("SELECT post_id, id, type, url, preview_url, width, height, description FROM media")
    media_rows = cur.fetchall()
    media_map: Dict[str, List[Dict]] = {}
    for row in media_rows:
        media_map.setdefault(row[0], []).append(
            {
                "id": row[1],
                "type": row[2],
                "url": row[3],
                "preview_url": row[4],
                "width": row[5],
                "height": row[6],
                "description": row[7],
            }
        )

    cur = conn.execute("SELECT id, author, created_at, text, url FROM posts")
    rows = cur.fetchall()
    posts: List[Dict] = []
    for row in rows:
        post = {
            "id": row[0],
            "author": row[1],
            "created_at": row[2],
            "text": row[3],
            "url": row[4],
            "media": media_map.get(row[0], []),
        }
        try:
            ts = parse_timestamp(post["created_at"])
        except Exception:
            continue
        if ts >= cutoff:
            posts.append(post)
    return posts


def ensure_accounts(conn: sqlite3.Connection, accounts: Iterable[str]) -> None:
    normalized = [acc.lower().lstrip("@") for acc in accounts if acc]
    if not normalized:
        return
    conn.executemany(
        """
        INSERT INTO accounts(handle, platform)
        VALUES(?, 'x')
        ON CONFLICT(handle) DO NOTHING
        """,
        [(account,) for account in normalized],
    )
    conn.commit()


def run_pipeline(
    mode: str,
    token: Optional[str],
    actor_id: str,
    input_template: Optional[Path],
    config_path: Path,
    db_path: Path,
    sample_file: Optional[Path],
    report_path: Path,
    window_hours: int,
    limit: int,
    max_total_limit: int,
    base_url: str,
    summary_model: Optional[str] = None,
    summary_api_key: Optional[str] = None,
    summary_base_url: Optional[str] = None,
    summary_max_posts: int = 30,
) -> str:
    accounts = [acc.lower().lstrip("@") for acc in read_accounts(config_path)]
    conn = init_db(db_path)
    ensure_accounts(conn, accounts)

    template_data: Optional[Dict] = None
    if input_template and input_template.exists():
        template_data = json.loads(input_template.read_text(encoding="utf-8"))

    client = ApifyTweetScraperClient(
        token=token,
        actor_id=actor_id,
        base_url=base_url,
        mode=mode,
        sample_file=sample_file,
        input_template=template_data,
    )

    since_map = {account: get_since_id(conn, account) for account in accounts}
    since_ts_map = {account: get_latest_timestamp(conn, account) for account in accounts}
    posts = client.fetch_accounts(accounts, since_map, since_ts_map, limit=limit, max_total_limit=max_total_limit)
    if posts:
        store_posts(conn, posts)
        latest_per_author: Dict[str, Dict[str, str]] = {}
        for post in posts:
            author = str(post.get("author", "")).lower()
            tweet_id = str(post.get("id"))
            created_at = post.get("created_at")
            try:
                created_ts = parse_timestamp(created_at) if created_at else None
            except Exception:
                created_ts = None
            if not tweet_id:
                continue
            existing = latest_per_author.get(author)
            try:
                existing_ts = parse_timestamp(existing.get("created_at", "")) if existing and existing.get("created_at") else None
            except Exception:
                existing_ts = None
            if not existing or (created_ts and (not existing_ts or existing_ts < created_ts)):
                latest_per_author[author] = {"id": tweet_id, "created_at": created_at or ""}
            elif not existing_ts and existing and existing.get("id", "") < tweet_id:
                # Fallback to id ordering if timestamp is missing
                latest_per_author[author] = {"id": tweet_id, "created_at": created_at or ""}
        for author, data in latest_per_author.items():
            set_since_id(conn, author, data["id"], latest_timestamp=data.get("created_at"))

    posts = load_posts_in_window(conn, window_hours=window_hours)
    window_label = f"past {window_hours}h ending {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    summary_text = None
    if summary_model:
        try:
            summary_text = summarize_posts(
                posts,
                model=summary_model,
                api_key=summary_api_key,
                base_url=summary_base_url,
                max_posts=summary_max_posts,
            )
        except Exception as exc:
            print(f"LLM summarization failed: {exc}", file=sys.stderr)
    report_body = build_report(posts, window_label=window_label, summary=summary_text)

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report_body, encoding="utf-8")
    return report_body


def main():
    parser = argparse.ArgumentParser(description="Apify-based X digest (tweet-scraper actor)")
    parser.add_argument("--mode", choices=["sample", "apify"], default="sample")
    parser.add_argument("--token", type=str, default=os.environ.get("APIFY_TOKEN"), help="Apify API token (required in apify mode)")
    parser.add_argument("--actor-id", type=str, default="apidojo~twitter-scraper-lite")
    parser.add_argument(
        "--input-template",
        type=Path,
        default=Path(__file__).parent / "input.template.json",
        help="Path to JSON template merged into actor input",
    )
    parser.add_argument("--config", type=Path, default=Path(__file__).parent / "accounts.yml")
    parser.add_argument("--db", type=Path, default=Path(__file__).parent / "data" / "digests.db")
    parser.add_argument("--sample-file", type=Path, default=None)
    parser.add_argument("--report", type=Path, default=Path(__file__).parent.parent / "reports" / "apify-daily.md")
    parser.add_argument("--window-hours", type=int, default=48)
    parser.add_argument("--limit", type=int, default=40, help="Max posts per account per run")
    parser.add_argument("--max-total-limit", type=int, default=400, help="Global cap across all accounts to avoid over-fetch")
    parser.add_argument("--base-url", type=str, default="https://api.apify.com/v2")
    parser.add_argument("--summary-model", type=str, default="deepseek-chat", help="Optional OpenAI model id to summarize posts")
    parser.add_argument("--summary-api-key", type=str, default=os.environ.get("OPENAI_API_KEY"))
    parser.add_argument(
        "--summary-base-url",
        type=str,
        default=os.environ.get("OPENAI_BASE_URL") or os.environ.get("DEEPSEEK_API_BASE"),
        help="Override the OpenAI-compatible base URL (e.g., https://api.deepseek.com for DeepSeek)",
    )
    parser.add_argument("--summary-max-posts", type=int, default=30, help="Max posts to pass to the LLM summarizer")
    args = parser.parse_args()

    report = run_pipeline(
        mode=args.mode,
        token=args.token,
        actor_id=args.actor_id,
        input_template=args.input_template,
        config_path=args.config,
        db_path=args.db,
        sample_file=args.sample_file,
        report_path=args.report,
        window_hours=args.window_hours,
        limit=args.limit,
        max_total_limit=args.max_total_limit,
        base_url=args.base_url,
        summary_model=args.summary_model,
        summary_api_key=args.summary_api_key,
        summary_base_url=args.summary_base_url,
        summary_max_posts=args.summary_max_posts,
    )
    print(report)


if __name__ == "__main__":
    main()
