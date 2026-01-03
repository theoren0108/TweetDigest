import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute("PRAGMA table_info(%s)" % table)
    return any(row[1] == column for row in cur.fetchall())


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return cur.fetchone() is not None


def _create_base_tables(conn: sqlite3.Connection) -> None:
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


def _migrate_to_v1(conn: sqlite3.Connection) -> None:
    # Historical baseline
    _create_base_tables(conn)
    conn.execute("PRAGMA user_version = 1")


def _migrate_to_v2(conn: sqlite3.Connection) -> None:
    # Add accounts table, extend posts, and introduce media table/indexes
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            handle TEXT PRIMARY KEY,
            display_name TEXT,
            since_id TEXT,
            last_synced_at TEXT
        )
        """
    )

    if not _column_exists(conn, "posts", "account"):
        conn.execute("ALTER TABLE posts ADD COLUMN account TEXT")
    if not _column_exists(conn, "posts", "raw"):
        conn.execute("ALTER TABLE posts ADD COLUMN raw TEXT DEFAULT '{}' NOT NULL")
    if not _column_exists(conn, "posts", "media_manifest"):
        conn.execute("ALTER TABLE posts ADD COLUMN media_manifest TEXT DEFAULT '[]' NOT NULL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id TEXT NOT NULL,
            media_type TEXT NOT NULL,
            source_url TEXT NOT NULL,
            local_path TEXT,
            remote_url TEXT,
            hash TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(post_id, source_url)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_media_post_id ON media(post_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_media_hash ON media(hash)")

    # Backfill accounts from existing data
    handles: List[str] = []
    if _table_exists(conn, "since_ids"):
        cur = conn.execute("SELECT account FROM since_ids")
        handles.extend([row[0] for row in cur.fetchall() if row[0]])
    cur = conn.execute("SELECT DISTINCT author FROM posts WHERE author IS NOT NULL")
    handles.extend([row[0] for row in cur.fetchall() if row[0]])
    for handle in sorted(set(handles)):
        conn.execute(
            "INSERT OR IGNORE INTO accounts(handle, since_id) VALUES(?, (SELECT since_id FROM since_ids WHERE account = ?))",
            (handle, handle),
        )

    # Ensure account column is populated for existing posts
    conn.execute("UPDATE posts SET account = author WHERE account IS NULL")

    conn.execute("PRAGMA user_version = 2")


def apply_sqlite_migrations(conn: sqlite3.Connection) -> None:
    """
    Apply schema migrations for the local SQLite database. This keeps PRAGMA
    user_version in sync and is idempotent for already-applied migrations.
    """

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version < 1:
        _migrate_to_v1(conn)
        version = 1
    if version < 2:
        _migrate_to_v2(conn)
        version = 2
    conn.commit()


def dump_schema_sql(target: Path) -> None:
    """Write a portable SQL migration for reference (SQLite/Postgres friendly)."""

    statements: List[str] = [
        "-- accounts table (SQLite/Postgres)",
        "CREATE TABLE IF NOT EXISTS accounts (",
        "    handle TEXT PRIMARY KEY,",
        "    display_name TEXT,",
        "    since_id TEXT,",
        "    last_synced_at TEXT",
        ");",
        "",
        "-- posts extensions",
        "ALTER TABLE posts ADD COLUMN account TEXT; -- add if missing",
        "ALTER TABLE posts ADD COLUMN raw TEXT; -- JSON payload",
        "ALTER TABLE posts ADD COLUMN media_manifest TEXT; -- serialized media info",
        "",
        "-- media table",
        "CREATE TABLE IF NOT EXISTS media (",
        "    id SERIAL PRIMARY KEY,",
        "    post_id TEXT NOT NULL,",
        "    media_type TEXT NOT NULL,",
        "    source_url TEXT NOT NULL,",
        "    local_path TEXT,",
        "    remote_url TEXT,",
        "    hash TEXT,",
        "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "    UNIQUE(post_id, source_url)",
        ");",
        "CREATE INDEX IF NOT EXISTS idx_media_post_id ON media(post_id);",
        "CREATE INDEX IF NOT EXISTS idx_media_hash ON media(hash);",
    ]

    target.write_text("\n".join(statements), encoding="utf-8")


def generate_migration_snapshot(output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / "001_accounts_media.sql"
    dump_schema_sql(target)
    return target


def rehydrate_raw(row: Iterable) -> Dict:
    """Helper to safely load JSON columns from SQLite rows."""

    if row is None:
        return {}
    try:
        return json.loads(row)
    except Exception:
        return {}
