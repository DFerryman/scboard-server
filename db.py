"""SQLite connection management and schema initialization.

Plan §SQLite Conventions:

- Open short-lived connections per repository operation or transaction.
- ``connection.row_factory = sqlite3.Row``.
- Run ``PRAGMA foreign_keys=ON`` on every connection.
- Run ``PRAGMA synchronous=NORMAL`` on writable connections.
- Run ``PRAGMA journal_mode=WAL`` once during initialization.
- Wrap writes in explicit transactions via :func:`transaction`.
- Do not share one global connection across FastAPI request threads.
"""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Iterator, Optional

from . import settings


_WRITE_TRANSACTION_LOCK = RLock()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stories (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL DEFAULT 'story'
    CHECK (kind IN ('story', 'ask', 'show', 'job')),
  title_en TEXT NOT NULL DEFAULT '',
  title_zh TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL DEFAULT '',
  domain TEXT NOT NULL DEFAULT 'news.ycombinator.com',
  by TEXT NOT NULL DEFAULT '',
  score INTEGER NOT NULL DEFAULT 0,
  descendants INTEGER NOT NULL DEFAULT 0,
  hn_time INTEGER NOT NULL DEFAULT 0,
  raw_text TEXT NOT NULL DEFAULT '',
  raw_json TEXT NOT NULL DEFAULT '{}',
  topic TEXT NOT NULL DEFAULT 'web',
  ai_summary TEXT NOT NULL DEFAULT '',
  discussion_themes TEXT NOT NULL DEFAULT '[]',
  insights TEXT NOT NULL DEFAULT '[]',
  terms TEXT NOT NULL DEFAULT '[]',
  enrich_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (enrich_status IN ('pending', 'enriching', 'done', 'failed')),
  enrich_attempts INTEGER NOT NULL DEFAULT 0,
  enrich_error TEXT,
  enrich_started_at INTEGER,
  enriched_at INTEGER,
  enriched_title_en TEXT NOT NULL DEFAULT '',
  enriched_url TEXT NOT NULL DEFAULT '',
  enriched_domain TEXT NOT NULL DEFAULT '',
  enriched_by TEXT NOT NULL DEFAULT '',
  enriched_hn_time INTEGER NOT NULL DEFAULT 0,
  enriched_score INTEGER NOT NULL DEFAULT 0,
  enriched_descendants INTEGER NOT NULL DEFAULT 0,
  image_url TEXT NOT NULL DEFAULT '',
  image_file_id TEXT NOT NULL DEFAULT '',
  image_source_url TEXT NOT NULL DEFAULT '',
  image_status TEXT NOT NULL DEFAULT '',
  image_checked_at INTEGER NOT NULL DEFAULT 0,
  image_error TEXT NOT NULL DEFAULT '',
  comments_fetched_descendants INTEGER NOT NULL DEFAULT 0,
  needs_reenrich INTEGER NOT NULL DEFAULT 0,
  reenrich_started_at INTEGER,
  reenrich_attempts INTEGER NOT NULL DEFAULT 0,
  enrich_retry_after INTEGER NOT NULL DEFAULT 0,
  reenrich_retry_after INTEGER NOT NULL DEFAULT 0,
  fetched_at INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stories_topic_score
ON stories(topic, score DESC, hn_time DESC);

CREATE INDEX IF NOT EXISTS idx_stories_enrich_work
ON stories(enrich_status, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS topics (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_topics_last_seen
ON topics(last_seen_at DESC);

CREATE TABLE IF NOT EXISTS rankings (
  feed TEXT NOT NULL
    CHECK (feed IN ('top', 'new', 'best', 'ask', 'show', 'job')),
  rank INTEGER NOT NULL,
  story_id INTEGER NOT NULL,
  refreshed_at INTEGER NOT NULL,
  PRIMARY KEY (feed, rank),
  UNIQUE (feed, story_id),
  FOREIGN KEY (story_id) REFERENCES stories(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rankings_type_rank
ON rankings(feed, rank);

CREATE TABLE IF NOT EXISTS ranking_candidates (
  run_id TEXT NOT NULL,
  feed TEXT NOT NULL
    CHECK (feed IN ('top', 'new', 'best', 'ask', 'show', 'job')),
  rank INTEGER NOT NULL,
  story_id INTEGER NOT NULL,
  fetched_at INTEGER NOT NULL,
  PRIMARY KEY (run_id, feed, rank),
  UNIQUE (run_id, feed, story_id),
  FOREIGN KEY (story_id) REFERENCES stories(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ranking_candidates_run_feed
ON ranking_candidates(run_id, feed, rank);

CREATE TABLE IF NOT EXISTS comments (
  id INTEGER PRIMARY KEY,
  story_id INTEGER NOT NULL,
  parent_id INTEGER,
  by TEXT NOT NULL DEFAULT '',
  text TEXT NOT NULL DEFAULT '',
  hn_time INTEGER NOT NULL DEFAULT 0,
  depth INTEGER NOT NULL DEFAULT 0,
  rank INTEGER NOT NULL DEFAULT 0,
  raw_json TEXT NOT NULL DEFAULT '{}',
  fetched_at INTEGER NOT NULL,
  FOREIGN KEY (story_id) REFERENCES stories(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_comments_story_rank
ON comments(story_id, rank);

CREATE TABLE IF NOT EXISTS digests (
  date TEXT PRIMARY KEY,
  intro TEXT NOT NULL DEFAULT '',
  story_ids TEXT NOT NULL DEFAULT '[]',
  generated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS insights (
  date TEXT PRIMARY KEY,
  payload TEXT NOT NULL DEFAULT '{}',
  source_story_ids TEXT NOT NULL DEFAULT '[]',
  generated_at INTEGER NOT NULL,
  window_days INTEGER NOT NULL DEFAULT 7,
  material_fingerprint TEXT NOT NULL DEFAULT '',
  model_usage TEXT
);

CREATE TABLE IF NOT EXISTS insights_runs (
  run_id TEXT PRIMARY KEY,
  date TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  finished_at INTEGER,
  status TEXT NOT NULL,
  model_usage TEXT,
  summary TEXT,
  error TEXT
);

CREATE INDEX IF NOT EXISTS idx_insights_runs_started
ON insights_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS insight_evidence_cache (
  cache_key TEXT PRIMARY KEY,
  payload TEXT NOT NULL DEFAULT '{}',
  story_count INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_insight_evidence_cache_updated
ON insight_evidence_cache(updated_at DESC);

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_runs (
  run_id TEXT PRIMARY KEY,
  started_at INTEGER NOT NULL,
  deadline_at INTEGER,
  finished_at INTEGER,
  status TEXT NOT NULL,
  phase TEXT NOT NULL DEFAULT '',
  raw_count INTEGER NOT NULL DEFAULT 0,
  candidate_count INTEGER NOT NULL DEFAULT 0,
  claimed INTEGER NOT NULL DEFAULT 0,
  done INTEGER NOT NULL DEFAULT 0,
  failed INTEGER NOT NULL DEFAULT 0,
  retried INTEGER NOT NULL DEFAULT 0,
  ai_usage TEXT,
  error TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_started
ON ingest_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS cloud_sync_runs (
  run_id TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  finished_at INTEGER,
  status TEXT NOT NULL,
  sync_version INTEGER,
  stories INTEGER,
  topics INTEGER,
  digests INTEGER,
  insights INTEGER,
  elapsed_seconds REAL,
  error TEXT,
  -- cleanupOld is the best-effort stage at the end of push_read_model; when it
  -- fails the main flow is still ok, but old versions accumulate in the cloud.
  -- Recording its status separately lets operations see the "cleanup failing
  -- repeatedly" silent-degradation situation directly from the dashboard.
  -- Value convention: 'ok' / 'failed:<reason>' / 'skipped:deadline' / 'skipped:initial' / NULL
  cleanup_status TEXT,
  PRIMARY KEY (run_id, started_at)
);

CREATE INDEX IF NOT EXISTS idx_cloud_sync_runs_started
ON cloud_sync_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS story_image_assets (
  image_file_id TEXT NOT NULL PRIMARY KEY,
  story_id INTEGER NOT NULL,
  image_url TEXT NOT NULL DEFAULT '',
  image_source_url TEXT NOT NULL DEFAULT '',
  cloud_path TEXT NOT NULL DEFAULT '',
  sha256 TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT ''
    CHECK (status IN ('ready', 'pending_delete', 'deleted', 'delete_failed', '')),
  uploaded_at INTEGER NOT NULL DEFAULT 0,
  last_referenced_at INTEGER NOT NULL DEFAULT 0,
  delete_after INTEGER NOT NULL DEFAULT 0,
  deleted_at INTEGER NOT NULL DEFAULT 0,
  error TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_story_image_assets_status_delete
ON story_image_assets(status, delete_after);

CREATE INDEX IF NOT EXISTS idx_story_image_assets_story_status
ON story_image_assets(story_id, status);
"""


STORY_COLUMN_MIGRATIONS = {
    "enriched_title_en": (
        "ALTER TABLE stories ADD COLUMN "
        "enriched_title_en TEXT NOT NULL DEFAULT ''"
    ),
    "enriched_url": (
        "ALTER TABLE stories ADD COLUMN "
        "enriched_url TEXT NOT NULL DEFAULT ''"
    ),
    "enriched_domain": (
        "ALTER TABLE stories ADD COLUMN "
        "enriched_domain TEXT NOT NULL DEFAULT ''"
    ),
    "enriched_by": (
        "ALTER TABLE stories ADD COLUMN "
        "enriched_by TEXT NOT NULL DEFAULT ''"
    ),
    "enriched_hn_time": (
        "ALTER TABLE stories ADD COLUMN "
        "enriched_hn_time INTEGER NOT NULL DEFAULT 0"
    ),
    "enriched_score": (
        "ALTER TABLE stories ADD COLUMN "
        "enriched_score INTEGER NOT NULL DEFAULT 0"
    ),
    "enriched_descendants": (
        "ALTER TABLE stories ADD COLUMN "
        "enriched_descendants INTEGER NOT NULL DEFAULT 0"
    ),
    "comments_fetched_descendants": (
        "ALTER TABLE stories ADD COLUMN "
        "comments_fetched_descendants INTEGER NOT NULL DEFAULT 0"
    ),
    "discussion_themes": (
        "ALTER TABLE stories ADD COLUMN "
        "discussion_themes TEXT NOT NULL DEFAULT '[]'"
    ),
    "needs_reenrich": (
        "ALTER TABLE stories ADD COLUMN "
        "needs_reenrich INTEGER NOT NULL DEFAULT 0"
    ),
    "reenrich_started_at": (
        "ALTER TABLE stories ADD COLUMN reenrich_started_at INTEGER"
    ),
    "reenrich_attempts": (
        "ALTER TABLE stories ADD COLUMN "
        "reenrich_attempts INTEGER NOT NULL DEFAULT 0"
    ),
    # AI capacity-deferred park: a 429 / all-providers-busy result parks
    # the story until ``now >= enrich_retry_after`` instead of bumping
    # enrich_attempts. Default 0 means "no defer in effect".
    "enrich_retry_after": (
        "ALTER TABLE stories ADD COLUMN "
        "enrich_retry_after INTEGER NOT NULL DEFAULT 0"
    ),
    "reenrich_retry_after": (
        "ALTER TABLE stories ADD COLUMN "
        "reenrich_retry_after INTEGER NOT NULL DEFAULT 0"
    ),
    "image_url": (
        "ALTER TABLE stories ADD COLUMN "
        "image_url TEXT NOT NULL DEFAULT ''"
    ),
    "image_file_id": (
        "ALTER TABLE stories ADD COLUMN "
        "image_file_id TEXT NOT NULL DEFAULT ''"
    ),
    "image_source_url": (
        "ALTER TABLE stories ADD COLUMN "
        "image_source_url TEXT NOT NULL DEFAULT ''"
    ),
    "image_status": (
        "ALTER TABLE stories ADD COLUMN "
        "image_status TEXT NOT NULL DEFAULT ''"
    ),
    "image_checked_at": (
        "ALTER TABLE stories ADD COLUMN "
        "image_checked_at INTEGER NOT NULL DEFAULT 0"
    ),
    "image_error": (
        "ALTER TABLE stories ADD COLUMN "
        "image_error TEXT NOT NULL DEFAULT ''"
    ),
}

INGEST_RUN_COLUMN_MIGRATIONS = {
    "ai_usage": "ALTER TABLE ingest_runs ADD COLUMN ai_usage TEXT",
}

CLOUD_SYNC_RUN_COLUMN_MIGRATIONS = {
    "insights": "ALTER TABLE cloud_sync_runs ADD COLUMN insights INTEGER",
    "cleanup_status": (
        "ALTER TABLE cloud_sync_runs ADD COLUMN cleanup_status TEXT"
    ),
}

INSIGHT_COLUMN_MIGRATIONS = {
    "material_fingerprint": (
        "ALTER TABLE insights ADD COLUMN "
        "material_fingerprint TEXT NOT NULL DEFAULT ''"
    ),
}

INSIGHT_RUN_COLUMN_MIGRATIONS = {
    "summary": "ALTER TABLE insights_runs ADD COLUMN summary TEXT",
}


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def connect(path: Optional[Path] = None) -> sqlite3.Connection:
    """Open a short-lived connection in autocommit mode.

    With ``isolation_level=None`` the caller controls transactions explicitly
    via :func:`transaction`. ``PRAGMA foreign_keys=ON`` is set immediately.
    """
    target = path or settings.get_db_path()
    _ensure_parent(target)
    conn = sqlite3.connect(
        target,
        timeout=30.0,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def connect_readonly(path: Optional[Path] = None) -> sqlite3.Connection:
    """Open an existing SQLite database without creating or migrating it."""
    target = path or settings.get_db_path()
    uri = f"{target.resolve().as_uri()}?mode=ro"
    conn = sqlite3.connect(
        uri,
        uri=True,
        timeout=30.0,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def assert_initialized_readonly(path: Optional[Path] = None) -> None:
    """Verify that the API process can read an already-initialized DB.

    This intentionally does not call :func:`init_db`; process B must not
    create schema, migrate, or seed metadata on startup.
    """
    target = path or settings.get_db_path()
    if not target.exists():
        raise RuntimeError(
            f"SQLite DB is not initialized at {target}; run process A first"
        )

    required = {"stories", "rankings", "digests", "meta"}
    conn = connect_readonly(target)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    finally:
        conn.close()
    existing = {str(r["name"]) for r in rows}
    missing = sorted(required - existing)
    if missing:
        raise RuntimeError(
            "SQLite DB is not initialized; missing tables: " + ", ".join(missing)
        )


def init_db(path: Optional[Path] = None) -> None:
    """Create schema (idempotent), enable WAL once, and seed catalog_version."""
    conn = connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA_SQL)
        _migrate_story_columns(conn)
        _migrate_story_image_assets_table(conn)
        _migrate_ingest_run_columns(conn)
        _migrate_insight_columns(conn)
        _migrate_insight_run_columns(conn)
        _migrate_cloud_sync_run_columns(conn)
        now = int(time.time())
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value, updated_at) "
            "VALUES('catalog_version', '0', ?)",
            (now,),
        )
    finally:
        conn.close()


def _migrate_story_columns(conn: sqlite3.Connection) -> None:
    """Add columns introduced after the initial SQLite schema."""
    rows = conn.execute("PRAGMA table_info(stories)").fetchall()
    existing = {str(r["name"]) for r in rows}
    for name, sql in STORY_COLUMN_MIGRATIONS.items():
        if name not in existing:
            conn.execute(sql)
    conn.execute(
        """
        UPDATE stories
        SET enriched_title_en=CASE
                WHEN enriched_title_en='' THEN title_en ELSE enriched_title_en
            END,
            enriched_url=CASE
                WHEN enriched_url='' THEN url ELSE enriched_url
            END,
            enriched_domain=CASE
                WHEN enriched_domain='' THEN domain ELSE enriched_domain
            END,
            enriched_by=CASE
                WHEN enriched_by='' THEN by ELSE enriched_by
            END,
            enriched_hn_time=CASE
                WHEN enriched_hn_time=0 THEN hn_time ELSE enriched_hn_time
            END,
            enriched_score=CASE
                WHEN enriched_score=0 THEN score ELSE enriched_score
            END,
            enriched_descendants=CASE
                WHEN enriched_descendants=0 THEN descendants ELSE enriched_descendants
            END,
            comments_fetched_descendants=CASE
                WHEN comments_fetched_descendants=0 THEN descendants
                ELSE comments_fetched_descendants
            END
        WHERE enrich_status='done'
          AND enriched_at IS NOT NULL
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stories_reenrich_work "
        "ON stories(needs_reenrich, last_seen_at DESC)"
    )


def _create_story_image_assets_indexes(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_story_image_assets_status_delete "
        "ON story_image_assets(status, delete_after)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_story_image_assets_story_status "
        "ON story_image_assets(story_id, status)"
    )


def _migrate_story_image_assets_table(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(story_image_assets)").fetchall()
    if not rows:
        return
    pk_columns = [
        str(r["name"])
        for r in sorted(rows, key=lambda r: int(r["pk"] or 0))
        if int(r["pk"] or 0) > 0
    ]
    if pk_columns == ["image_file_id"]:
        _create_story_image_assets_indexes(conn)
        return
    if pk_columns != ["story_id"]:
        return

    conn.execute("ALTER TABLE story_image_assets RENAME TO story_image_assets_old")
    conn.execute(
        """
        CREATE TABLE story_image_assets (
          image_file_id TEXT NOT NULL PRIMARY KEY,
          story_id INTEGER NOT NULL,
          image_url TEXT NOT NULL DEFAULT '',
          image_source_url TEXT NOT NULL DEFAULT '',
          cloud_path TEXT NOT NULL DEFAULT '',
          sha256 TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT ''
            CHECK (status IN ('ready', 'pending_delete', 'deleted', 'delete_failed', '')),
          uploaded_at INTEGER NOT NULL DEFAULT 0,
          last_referenced_at INTEGER NOT NULL DEFAULT 0,
          delete_after INTEGER NOT NULL DEFAULT 0,
          deleted_at INTEGER NOT NULL DEFAULT 0,
          error TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO story_image_assets(
            image_file_id, story_id, image_url, image_source_url, cloud_path,
            sha256, status, uploaded_at, last_referenced_at, delete_after,
            deleted_at, error
        )
        SELECT
            image_file_id, story_id, image_url, image_source_url, cloud_path,
            sha256, status, uploaded_at, last_referenced_at, delete_after,
            deleted_at, error
        FROM story_image_assets_old
        WHERE COALESCE(image_file_id, '') != ''
        ORDER BY uploaded_at ASC, story_id ASC
        """
    )
    conn.execute("DROP TABLE story_image_assets_old")
    _create_story_image_assets_indexes(conn)


def _migrate_ingest_run_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(ingest_runs)").fetchall()
    existing = {str(r["name"]) for r in rows}
    for name, sql in INGEST_RUN_COLUMN_MIGRATIONS.items():
        if name not in existing:
            conn.execute(sql)


def _migrate_insight_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(insights)").fetchall()
    existing = {str(r["name"]) for r in rows}
    for name, sql in INSIGHT_COLUMN_MIGRATIONS.items():
        if name not in existing:
            conn.execute(sql)


def _migrate_insight_run_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(insights_runs)").fetchall()
    existing = {str(r["name"]) for r in rows}
    for name, sql in INSIGHT_RUN_COLUMN_MIGRATIONS.items():
        if name not in existing:
            conn.execute(sql)


def _migrate_cloud_sync_run_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(cloud_sync_runs)").fetchall()
    existing = {str(r["name"]) for r in rows}
    for name, sql in CLOUD_SYNC_RUN_COLUMN_MIGRATIONS.items():
        if name not in existing:
            conn.execute(sql)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Wrap a write block in BEGIN IMMEDIATE / COMMIT (ROLLBACK on error)."""
    with _WRITE_TRANSACTION_LOCK:
        _execute_write_control(conn, "BEGIN IMMEDIATE")
        try:
            yield conn
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        else:
            try:
                _execute_write_control(conn, "COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise


def _is_sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message


def _execute_write_control(conn: sqlite3.Connection, sql: str) -> None:
    retries = max(0, int(settings.DB_WRITE_LOCK_RETRY_ATTEMPTS))
    base_delay = max(0.0, float(settings.DB_WRITE_LOCK_RETRY_BASE_SECONDS))
    max_delay = max(0.0, float(settings.DB_WRITE_LOCK_RETRY_MAX_SECONDS))
    for attempt in range(retries + 1):
        try:
            conn.execute(sql)
            return
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_lock_error(exc) or attempt >= retries:
                raise
            delay = min(max_delay, base_delay * (2**attempt))
            if delay > 0:
                time.sleep(delay)
