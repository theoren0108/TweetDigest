import argparse
import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apify_pipeline.analyzer import build_report, parse_timestamp
from apify_pipeline.apify_client import ApifyTweetScraperClient
from apify_pipeline.migrations import apply_sqlite_migrations


def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    apply_sqlite_migrations(conn)
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


def ensure_account_row(conn: sqlite3.Connection, account: str) -> None:
    conn.execute(
        """
        INSERT INTO accounts(handle, since_id, last_synced_at)
        VALUES(:handle, COALESCE((SELECT since_id FROM since_ids WHERE account = :handle), NULL), NULL)
        ON CONFLICT(handle) DO NOTHING
        """,
        {"handle": account},
    )
    conn.commit()


def get_since_id(conn: sqlite3.Connection, account: str) -> Optional[str]:
    cur = conn.execute("SELECT since_id FROM accounts WHERE handle = ?", (account,))
    row = cur.fetchone()
    if row and row[0] is not None:
        return row[0]

    # Fallback to legacy table
    cur = conn.execute("SELECT since_id FROM since_ids WHERE account = ?", (account,))
    row = cur.fetchone()
    return row[0] if row else None


def set_since_id(conn: sqlite3.Connection, account: str, since_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    ensure_account_row(conn, account)
    conn.execute(
        """
        INSERT INTO accounts(handle, since_id, last_synced_at)
        VALUES(:handle, :since_id, :last_synced_at)
        ON CONFLICT(handle) DO UPDATE SET since_id = excluded.since_id, last_synced_at = excluded.last_synced_at
        """,
        {"handle": account, "since_id": since_id, "last_synced_at": now},
    )
    # Keep legacy table updated for compatibility
    conn.execute(
        """
        INSERT INTO since_ids(account, since_id)
        VALUES(:account, :since_id)
        ON CONFLICT(account) DO UPDATE SET since_id = excluded.since_id
        """,
        {"account": account, "since_id": since_id},
    )
    conn.commit()


def store_posts(conn: sqlite3.Connection, posts: Iterable[Dict]) -> None:
    rows = []
    seen_accounts = set()
    for post in posts:
        account = post.get("author")
        if account and account not in seen_accounts:
            ensure_account_row(conn, account)
            seen_accounts.add(account)
        rows.append(
            {
                "id": post.get("id"),
                "author": post.get("author"),
                "account": account,
                "created_at": post.get("created_at"),
                "text": post.get("text"),
                "url": post.get("url"),
                "raw": json.dumps(post.get("raw", {}), ensure_ascii=False),
                "media_manifest": json.dumps(post.get("media_manifest", []), ensure_ascii=False),
            }
        )
    conn.executemany(
        """
        INSERT OR IGNORE INTO posts(id, author, account, created_at, text, url, raw, media_manifest)
        VALUES(:id, :author, :account, :created_at, :text, :url, :raw, :media_manifest)
        """,
        rows,
    )
    conn.commit()


def load_posts_in_window(conn: sqlite3.Connection, window_hours: int = 48) -> List[Dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    cur = conn.execute("SELECT id, author, created_at, text, url, media_manifest FROM posts")
    rows = cur.fetchall()
    posts: List[Dict] = []
    for row in rows:
        post = {
            "id": row[0],
            "author": row[1],
            "created_at": row[2],
            "text": row[3],
            "url": row[4],
            "media_manifest": json.loads(row[5] or "[]"),
        }
        try:
            ts = parse_timestamp(post["created_at"])
        except Exception:
            continue
        if ts >= cutoff:
            posts.append(post)
    return posts

def extract_media_urls(raw: Dict) -> List[Tuple[str, str]]:
    urls: List[Tuple[str, str]] = []
    if not isinstance(raw, dict):
        return urls

    for key, media_type in (("photos", "photo"), ("images", "photo"), ("videos", "video"), ("media", "media")):
        value = raw.get(key, [])
        if isinstance(value, dict):
            value = value.values()
        for item in value:
            if isinstance(item, str):
                urls.append((item, media_type))
            elif isinstance(item, dict):
                src = item.get("url") or item.get("src") or item.get("downloadUrl")
                if src:
                    urls.append((src, media_type))
    return urls


def download_media(url: str, media_root: Path) -> Tuple[Optional[Path], Optional[str]]:
    media_root.mkdir(parents=True, exist_ok=True)
    parsed = urllib.parse.urlparse(url)
    suffix = Path(parsed.path).suffix or ".bin"
    target = media_root / f"{sha256(url.encode('utf-8')).hexdigest()}{suffix}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = resp.read()
        file_hash = sha256(data).hexdigest()
        with target.open("wb") as fh:
            fh.write(data)
        return target, file_hash
    except Exception:
        return None, None


def upsert_media_records(
    conn: sqlite3.Connection, post_id: str, media_entries: List[Dict]
) -> List[Dict]:
    rows: List[Dict] = []
    for entry in media_entries:
        conn.execute(
            """
            INSERT OR IGNORE INTO media(post_id, media_type, source_url, local_path, remote_url, hash)
            VALUES(:post_id, :media_type, :source_url, :local_path, :remote_url, :hash)
            """,
            {
                "post_id": post_id,
                "media_type": entry.get("media_type"),
                "source_url": entry.get("source_url"),
                "local_path": entry.get("local_path"),
                "remote_url": entry.get("remote_url"),
                "hash": entry.get("hash"),
            },
        )
        conn.execute(
            """
            UPDATE media
            SET local_path = COALESCE(:local_path, local_path),
                remote_url = COALESCE(:remote_url, remote_url),
                hash = COALESCE(:hash, hash)
            WHERE post_id = :post_id AND source_url = :source_url
            """,
            {
                "post_id": post_id,
                "source_url": entry.get("source_url"),
                "local_path": entry.get("local_path"),
                "remote_url": entry.get("remote_url"),
                "hash": entry.get("hash"),
            },
        )
        rows.append(entry)
    conn.commit()
    return rows


def process_media(
    conn: sqlite3.Connection, posts: List[Dict], media_root: Path, upload: bool = False
) -> List[Dict]:
    processed: List[Dict] = []
    for post in posts:
        raw = post.get("raw", {})
        media_urls = extract_media_urls(raw)
        manifest: List[Dict] = []
        for source_url, media_type in media_urls:
            local_path: Optional[Path] = None
            file_hash: Optional[str] = None
            if not upload:
                local_path, file_hash = download_media(source_url, media_root)
            entry = {
                "media_type": media_type,
                "source_url": source_url,
                "local_path": str(local_path) if local_path else None,
                "remote_url": None,
                "hash": file_hash,
            }
            manifest.append(entry)

        if manifest:
            upsert_media_records(conn, str(post.get("id")), manifest)
        post["media_manifest"] = manifest
        processed.append(post)
    return processed


def cleanup_media(
    conn: sqlite3.Connection, media_root: Path, archive_reports_days: Optional[int] = None
) -> None:
    # Remove DB rows that reference missing files
    cur = conn.execute("SELECT id, local_path, hash FROM media")
    rows = cur.fetchall()
    for media_id, local_path, file_hash in rows:
        if local_path:
            path = Path(local_path)
            if not path.exists():
                conn.execute("DELETE FROM media WHERE id = ?", (media_id,))
                continue
    conn.commit()

    # Deduplicate by hash (keep first entry)
    cur = conn.execute(
        "SELECT hash, GROUP_CONCAT(id) FROM media WHERE hash IS NOT NULL GROUP BY hash HAVING COUNT(*) > 1"
    )
    for file_hash, id_list in cur.fetchall():
        ids = [int(part) for part in str(id_list).split(",") if part]
        if not ids:
            continue
        keeper = min(ids)
        for dup_id in ids:
            if dup_id == keeper:
                continue
            row = conn.execute("SELECT local_path FROM media WHERE id = ?", (dup_id,)).fetchone()
            if row and row[0]:
                path = Path(row[0])
                if path.exists():
                    path.unlink()
            conn.execute("DELETE FROM media WHERE id = ?", (dup_id,))
    conn.commit()

    # Optional report archiving
    if archive_reports_days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=archive_reports_days)
        reports_dir = ROOT / "reports"
        if reports_dir.exists():
            for md_file in reports_dir.glob("*.md"):
                mtime = datetime.fromtimestamp(md_file.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff and md_file.with_suffix(md_file.suffix + ".gz").exists() is False:
                    import gzip

                    with md_file.open("rb") as src, gzip.open(
                        md_file.with_suffix(md_file.suffix + ".gz"), "wb"
                    ) as dst:
                        dst.writelines(src)
                    md_file.unlink()


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
    media_root: Path,
    cleanup_after: bool,
    archive_reports_days: Optional[int],
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

    for account in accounts:
        ensure_account_row(conn, account)

    since_map = {account: get_since_id(conn, account) for account in accounts}
    posts = client.fetch_accounts(accounts, since_map, limit=limit)
    if posts:
        posts = process_media(conn, posts, media_root=media_root)
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

    if cleanup_after:
        cleanup_media(conn, media_root=media_root, archive_reports_days=archive_reports_days)

    posts = load_posts_in_window(conn, window_hours=window_hours)
    window_label = f"past {window_hours}h ending {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    report_body = build_report(posts, window_label=window_label)

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
    parser.add_argument("--base-url", type=str, default="https://api.apify.com/v2")
    parser.add_argument("--media-root", type=Path, default=Path(__file__).parent / "media")
    parser.add_argument("--cleanup-media", action="store_true", help="Validate media files and deduplicate by hash after run")
    parser.add_argument(
        "--archive-reports-days",
        type=int,
        default=None,
        help="If set, compress .md reports older than N days during cleanup",
    )
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
        media_root=args.media_root,
        cleanup_after=args.cleanup_media,
        archive_reports_days=args.archive_reports_days,
    )
    print(report)


if __name__ == "__main__":
    main()
