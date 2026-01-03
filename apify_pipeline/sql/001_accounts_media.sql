-- Migration: add normalized accounts table and media attachments
-- Creates reusable tables for account metadata, crawl state, and media linked to posts.

CREATE TABLE IF NOT EXISTS accounts (
    handle TEXT PRIMARY KEY,
    platform TEXT NOT NULL DEFAULT 'x',
    display_name TEXT,
    profile_url TEXT,
    since_id TEXT,
    latest_timestamp TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL,
    type TEXT,
    url TEXT NOT NULL,
    preview_url TEXT,
    width INTEGER,
    height INTEGER,
    description TEXT,
    FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_media_post_id ON media(post_id);

-- Backfill account handles from existing posts if they are not present yet.
INSERT OR IGNORE INTO accounts(handle, platform)
SELECT DISTINCT LOWER(author), 'x'
FROM posts
WHERE author IS NOT NULL AND author <> '';
