import argparse
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apify_pipeline.analyzer import build_report, parse_timestamp
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
    conn.commit()
    return conn


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
    cur = conn.execute("SELECT since_id FROM since_ids WHERE account = ?", (account,))
    row = cur.fetchone()
    return row[0] if row else None


def set_since_id(conn: sqlite3.Connection, account: str, since_id: str) -> None:
    conn.execute(
        "INSERT INTO since_ids(account, since_id) VALUES(?, ?) ON CONFLICT(account) DO UPDATE SET since_id = excluded.since_id",
        (account, since_id),
    )
    conn.commit()


def store_posts(conn: sqlite3.Connection, posts: Iterable[Dict]) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO posts(id, author, created_at, text, url) VALUES(:id, :author, :created_at, :text, :url)",
        list(posts),
    )
    conn.commit()


def load_posts_in_window(conn: sqlite3.Connection, window_hours: int = 48) -> List[Dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    cur = conn.execute("SELECT id, author, created_at, text, url FROM posts")
    rows = cur.fetchall()
    posts: List[Dict] = []
    for row in rows:
        post = {"id": row[0], "author": row[1], "created_at": row[2], "text": row[3], "url": row[4]}
        try:
            ts = parse_timestamp(post["created_at"])
        except Exception:
            continue
        if ts >= cutoff:
            posts.append(post)
    return posts


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
    base_url: str,
) -> str:
    accounts = [acc.lower().lstrip("@") for acc in read_accounts(config_path)]
    conn = init_db(db_path)

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
    posts = client.fetch_accounts(accounts, since_map, limit=limit)
    if posts:
        store_posts(conn, posts)
        latest_per_author: Dict[str, str] = {}
        for post in posts:
            author = str(post.get("author", "")).lower()
            tweet_id = str(post.get("id"))
            if not tweet_id:
                continue
            if author not in latest_per_author or tweet_id > latest_per_author[author]:
                latest_per_author[author] = tweet_id
        for author, since_id in latest_per_author.items():
            set_since_id(conn, author, since_id)

    posts = load_posts_in_window(conn, window_hours=window_hours)
    window_label = f"past {window_hours}h ending {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    report_body = build_report(posts, window_label=window_label)

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report_body, encoding="utf-8")
    return report_body


def main():
    parser = argparse.ArgumentParser(description="Apify-based X digest (tweet-scraper actor)")
    parser.add_argument("--mode", choices=["sample", "apify"], default="sample")
    parser.add_argument("--token", type=str, default=None, help="Apify API token (required in apify mode)")
    parser.add_argument("--actor-id", type=str, default="apidojo~tweet-scraper")
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
    parser.add_argument("--base-url", type=str, default="https://api.apify.com/v2")
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
        base_url=args.base_url,
    )
    print(report)


if __name__ == "__main__":
    main()
