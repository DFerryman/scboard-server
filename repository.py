"""Row-to-Story conversion and data access helpers.

Reads return validated Pydantic models; writes operate on SQLite rows. JSON
fields are decoded/encoded centrally so routes never call ``json.loads``.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Iterable, Iterator, List, Optional, Sequence, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.9+ has zoneinfo built in
    ZoneInfo = None  # type: ignore[assignment]

from . import settings
from .schemas import (
    DiscussionTheme,
    Insight,
    Story,
    StoryType,
    Term,
    TopicEntry,
)
from .topics import (
    DEFAULT_TOPIC_ID,
    DEFAULT_TOPIC_NAME,
    normalize_topic,
    topic_name_from_id,
)


# ---------- time helpers ----------

def _digest_timezone():
    if ZoneInfo is None:
        return None
    return ZoneInfo(settings.DIGEST_TIMEZONE)


def now_seconds() -> int:
    return int(time.time())


def today_in_digest_tz() -> str:
    """Return today's date in ``settings.DIGEST_TIMEZONE`` as ``YYYY-MM-DD``."""
    tz = _digest_timezone()
    if tz is not None:
        return datetime.now(tz).strftime("%Y-%m-%d")
    return time.strftime("%Y-%m-%d", time.localtime())


def date_in_digest_tz(epoch_seconds: int) -> str:
    tz = _digest_timezone()
    if tz is not None:
        return datetime.fromtimestamp(epoch_seconds, tz).strftime("%Y-%m-%d")
    return time.strftime("%Y-%m-%d", time.localtime(epoch_seconds))


def digest_date_minus_days(days: int) -> str:
    tz = _digest_timezone()
    if tz is not None:
        return (datetime.now(tz) - timedelta(days=days)).strftime("%Y-%m-%d")
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")


def digest_date_epoch_bounds(date: str) -> Tuple[int, int]:
    """Return ``[start, end)`` epoch seconds covering ``date`` in DIGEST_TIMEZONE."""
    start_dt = datetime.strptime(date, "%Y-%m-%d")
    end_dt = start_dt + timedelta(days=1)
    tz = _digest_timezone()
    if tz is not None:
        return (
            int(start_dt.replace(tzinfo=tz).timestamp()),
            int(end_dt.replace(tzinfo=tz).timestamp()),
        )
    return (
        int(time.mktime(start_dt.timetuple())),
        int(time.mktime(end_dt.timetuple())),
    )


# ---------- json helpers ----------

# ---------- dynamic IN-list safety ----------
#
# SQLite's compile-time SQLITE_MAX_VARIABLE_NUMBER bounds inline ``IN (?, ?, …)``
# parameter counts. The library default has been 999 historically and 32766
# more recently — but production binaries vary, and configs like
# DIGEST_RETENTION_DAYS * DIGEST_MAX_STORIES can push protected-id sets into
# the thousands. Inline IN is kept for the common small case (fast, no temp
# table overhead); above the threshold we materialize the ids into a TEMP
# table and use ``IN (SELECT id FROM <table>)`` instead. The temp table is
# auto-dropped on context exit so we never leave per-connection state behind.

_INLINE_IN_THRESHOLD = 900


@contextmanager
def id_in_clause(
    conn: sqlite3.Connection, ids: Sequence[int]
) -> Iterator[Tuple[str, List[int]]]:
    """Yield (clause, params) so callers can spell ``id <clause>`` safely.

    ``clause`` is either ``IN (?, ?, …)`` (with matching params) or
    ``IN (SELECT id FROM <temp>)`` (with empty params) depending on size.

    The ids must be non-empty; an empty list is a caller bug because
    ``IN ()`` is not valid SQL.
    """
    ids_list = [int(i) for i in ids]
    if not ids_list:
        raise ValueError("id_in_clause requires at least one id")
    if len(ids_list) <= _INLINE_IN_THRESHOLD:
        placeholders = ",".join("?" * len(ids_list))
        yield (f"IN ({placeholders})", ids_list)
        return
    name = f"_id_set_{uuid.uuid4().hex[:12]}"
    conn.execute(f"CREATE TEMP TABLE {name}(id INTEGER PRIMARY KEY)")
    try:
        conn.executemany(
            f"INSERT OR IGNORE INTO {name}(id) VALUES (?)",
            [(i,) for i in ids_list],
        )
        yield (f"IN (SELECT id FROM {name})", [])
    finally:
        conn.execute(f"DROP TABLE IF EXISTS {name}")


@contextmanager
def _optional_in(
    conn: sqlite3.Connection, ids: Sequence[int]
) -> Iterator[Optional[Tuple[str, List[int]]]]:
    """Variant of :func:`id_in_clause` that yields ``None`` for empty ids.

    Useful for callers that build a SQL clause conditionally: an empty
    list means "skip this clause" instead of raising. The body runs even
    when ids is empty so the caller can still take the no-clause branch.
    """
    ids_list = [int(i) for i in ids]
    if not ids_list:
        yield None
        return
    with id_in_clause(conn, ids_list) as (clause, params):
        yield (clause, params)


def _json_loads(text: Optional[str], default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return default


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def hash_int_sequence(ids: Sequence[int]) -> str:
    """Stable short hash of an ordered ID sequence; used for ranking deltas."""
    h = hashlib.sha1()
    for i in ids:
        h.update(str(i).encode("utf-8"))
        h.update(b",")
    return h.hexdigest()


# ---------- catalog version ----------

def get_catalog_version(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT value FROM meta WHERE key='catalog_version'"
    ).fetchone()
    return row["value"] if row else "0"


def bump_catalog_version(conn: sqlite3.Connection) -> str:
    """Increment ``meta.catalog_version`` atomically.

    Caller must already be inside a write transaction so the bump commits in
    the same atomic block as the content change that caused it.
    """
    now = now_seconds()
    conn.execute(
        "INSERT INTO meta(key, value, updated_at) VALUES('catalog_version', '0', ?) "
        "ON CONFLICT(key) DO NOTHING",
        (now,),
    )
    conn.execute(
        "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT), "
        "updated_at = ? WHERE key='catalog_version'",
        (now,),
    )
    return get_catalog_version(conn)


# ---------- meta ----------

def get_meta(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value, updated_at) VALUES(?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (key, value, now_seconds()),
    )


def get_meta_int(conn: sqlite3.Connection, key: str) -> Optional[int]:
    raw = get_meta(conn, key)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


# ---------- row -> Story ----------

def _story_type_from(row: sqlite3.Row, feed: Optional[str]) -> StoryType:
    """Plan §5: feed wins if given; otherwise map kind, defaulting story->top."""
    if feed:
        try:
            return StoryType(feed)
        except ValueError:
            pass
    kind = row["kind"]
    if kind in ("ask", "show", "job"):
        return StoryType(kind)
    return StoryType.TOP


def _coerce_discussion_themes(data: Any) -> List[DiscussionTheme]:
    if not isinstance(data, list):
        return []
    out: List[DiscussionTheme] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        try:
            out.append(DiscussionTheme(**item))
        except Exception:
            continue
    return out


def _coerce_insights(data: Any) -> List[Insight]:
    if not isinstance(data, list):
        return []
    out: List[Insight] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        try:
            out.append(Insight(**item))
        except Exception:
            continue
    return out


def _coerce_terms(data: Any) -> List[Term]:
    if not isinstance(data, list):
        return []
    out: List[Term] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        payload = {"term": item.get("term", "")}
        if "def" in item:
            payload["def"] = item["def"]
        elif "def_" in item:
            payload["def"] = item["def_"]
        else:
            payload["def"] = ""
        try:
            out.append(Term.model_validate(payload))
        except Exception:
            continue
    return out


def row_to_story(row: sqlite3.Row, feed: Optional[str] = None) -> Story:
    enriched_at = row["enriched_at"]
    use_enriched_snapshot = (
        row["enrich_status"] == "done" and enriched_at is not None
    )
    if use_enriched_snapshot:
        title_en = row["enriched_title_en"] or row["title_en"] or ""
        url = row["enriched_url"] if row["enriched_url"] is not None else row["url"]
        domain = row["enriched_domain"] or row["domain"] or "news.ycombinator.com"
        by = row["enriched_by"] or row["by"] or ""
        score = row["enriched_score"]
        descendants = row["enriched_descendants"]
        hn_time = row["enriched_hn_time"] or row["hn_time"]
    else:
        title_en = row["title_en"] or ""
        url = row["url"] or ""
        domain = row["domain"] or "news.ycombinator.com"
        by = row["by"] or ""
        score = row["score"]
        descendants = row["descendants"]
        hn_time = row["hn_time"]
    title_zh = row["title_zh"] or title_en
    topic = row["topic"] or DEFAULT_TOPIC_ID

    return Story(
        id=row["id"],
        type=_story_type_from(row, feed),
        titleZh=title_zh,
        titleEn=title_en,
        url=url,
        domain=domain,
        by=by,
        score=max(int(score or 0), 0),
        descendants=max(int(descendants or 0), 0),
        time=int(hn_time or 0),
        updatedAt=int(enriched_at) if enriched_at else None,
        topic=topic,
        aiSummary=row["ai_summary"] or "",
        discussionThemes=_coerce_discussion_themes(
            _json_loads(row["discussion_themes"], [])
        ),
        insights=_coerce_insights(_json_loads(row["insights"], [])),
        terms=_coerce_terms(_json_loads(row["terms"], [])),
    )


# ---------- story reads ----------

def list_known_topics(
    conn: sqlite3.Connection, *, limit: Optional[int] = None
) -> List[TopicEntry]:
    """Return the current dynamic taxonomy, including temporarily empty topics."""
    limit_clause = ""
    params: Tuple[Any, ...] = ()
    if limit is not None:
        limit_clause = "LIMIT ?"
        params = (max(0, int(limit)),)
    rows = conn.execute(
        f"""
        SELECT id, name
        FROM topics
        ORDER BY last_seen_at DESC, updated_at DESC, id ASC
        {limit_clause}
        """,
        params,
    ).fetchall()
    return [
        TopicEntry(id=str(r["id"] or DEFAULT_TOPIC_ID), name=str(r["name"] or ""), count=0)
        for r in rows
    ]

def ensure_topic(
    conn: sqlite3.Connection,
    topic_id: str,
    name: str,
    *,
    seen_at: Optional[int] = None,
) -> TopicEntry:
    max_topics = max(1, int(settings.TOPIC_MAX_ACTIVE_TOPICS))
    known_topics = list_known_topics(conn, limit=max_topics)
    normalized_id, normalized_name = normalize_topic(
        topic=topic_id,
        topic_id=topic_id,
        topic_name=name,
        existing_topics=known_topics,
    )
    known_by_id = {t.id: t for t in known_topics}
    if normalized_id not in known_by_id and len(known_topics) >= max_topics:
        fallback = known_topics[0]
        normalized_id, normalized_name = fallback.id, fallback.name
    now = now_seconds()
    last_seen = int(seen_at if seen_at is not None else now)
    conn.execute(
        """
        INSERT INTO topics(id, name, created_at, updated_at, last_seen_at)
        VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            updated_at=excluded.updated_at,
            last_seen_at=max(topics.last_seen_at, excluded.last_seen_at)
        """,
        (normalized_id, normalized_name, now, now, last_seen),
    )
    return TopicEntry(id=normalized_id, name=normalized_name, count=0)


def list_topics(conn: sqlite3.Connection, *, limit: Optional[int] = None) -> List[TopicEntry]:
    effective_limit = (
        settings.TOPIC_MAX_ACTIVE_TOPICS if limit is None else max(0, int(limit))
    )
    limit_clause = "LIMIT ?"
    params: Tuple[Any, ...] = (max(0, int(effective_limit)),)
    rows = conn.execute(
        f"""
        SELECT
            s.topic AS id,
            COALESCE(t.name, '') AS name,
            COUNT(DISTINCT s.id) AS c,
            MAX(COALESCE(s.enriched_at, s.last_seen_at, 0)) AS last_seen
        FROM stories s
        JOIN rankings r ON r.story_id=s.id
        LEFT JOIN topics t ON t.id=s.topic
        WHERE s.enrich_status='done'
          AND COALESCE(s.topic, '') != ''
        GROUP BY s.topic, t.name
        HAVING c > 0
        ORDER BY c DESC, last_seen DESC, id ASC
        {limit_clause}
        """,
        params,
    ).fetchall()
    out: List[TopicEntry] = []
    for row in rows:
        topic_id = str(row["id"] or DEFAULT_TOPIC_ID)
        name = str(row["name"] or "")
        if not name:
            name = DEFAULT_TOPIC_NAME if topic_id == DEFAULT_TOPIC_ID else topic_name_from_id(topic_id)
        out.append(TopicEntry(id=topic_id, name=name, count=int(row["c"] or 0)))
    return out


def list_active_topics(
    conn: sqlite3.Connection,
    *,
    limit: int = 30,
) -> List[TopicEntry]:
    return list_topics(conn, limit=min(max(1, int(limit)), settings.TOPIC_MAX_ACTIVE_TOPICS))


def get_story(conn: sqlite3.Connection, story_id: int) -> Optional[Story]:
    row = conn.execute(
        "SELECT * FROM stories WHERE id=? AND enrich_status='done'",
        (story_id,),
    ).fetchone()
    if not row:
        return None
    return row_to_story(row)


def list_feed_stories(
    conn: sqlite3.Connection, feed: str, page: int, page_size: int
) -> Tuple[List[Story], int, bool]:
    offset = (page - 1) * page_size
    total_row = conn.execute(
        "SELECT COUNT(*) AS c FROM rankings r "
        "JOIN stories s ON s.id=r.story_id "
        "WHERE r.feed=? AND s.enrich_status='done'",
        (feed,),
    ).fetchone()
    total = int(total_row["c"]) if total_row else 0
    rows = conn.execute(
        "SELECT s.* FROM rankings r "
        "JOIN stories s ON s.id=r.story_id "
        "WHERE r.feed=? AND s.enrich_status='done' "
        "ORDER BY r.rank ASC "
        "LIMIT ? OFFSET ?",
        (feed, page_size, offset),
    ).fetchall()
    stories = [row_to_story(r, feed=feed) for r in rows]
    has_more = offset + len(rows) < total
    return stories, total, has_more


def list_topic_stories(
    conn: sqlite3.Connection, topic_id: str, page: int, page_size: int
) -> Tuple[List[Story], int, bool]:
    offset = (page - 1) * page_size
    total_row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM stories s
        WHERE s.topic=?
          AND s.enrich_status='done'
          AND EXISTS (SELECT 1 FROM rankings r WHERE r.story_id=s.id)
        """,
        (topic_id,),
    ).fetchone()
    total = int(total_row["c"]) if total_row else 0
    rows = conn.execute(
        """
        SELECT s.*
        FROM stories s
        WHERE s.topic=?
          AND s.enrich_status='done'
          AND EXISTS (SELECT 1 FROM rankings r WHERE r.story_id=s.id)
        ORDER BY
          CASE
            WHEN s.enriched_at IS NOT NULL THEN COALESCE(s.enriched_score, s.score, 0)
            ELSE COALESCE(s.score, 0)
          END DESC,
          CASE
            WHEN s.enriched_at IS NOT NULL THEN COALESCE(NULLIF(s.enriched_hn_time, 0), s.hn_time, 0)
            ELSE COALESCE(s.hn_time, 0)
          END DESC,
          s.id ASC
        LIMIT ? OFFSET ?
        """,
        (topic_id, page_size, offset),
    ).fetchall()
    stories = [row_to_story(r) for r in rows]
    has_more = offset + len(rows) < total
    return stories, total, has_more


def topic_count(conn: sqlite3.Connection, topic_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM stories s
        WHERE s.topic=?
          AND s.enrich_status='done'
          AND EXISTS (SELECT 1 FROM rankings r WHERE r.story_id=s.id)
        """,
        (topic_id,),
    ).fetchone()
    return int(row["c"]) if row else 0


# ---------- digest reads ----------

def latest_digest_row(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """Plan §Web Layer Read Logic: ``/api/digest`` without date returns the
    newest row by ``generated_at``. A back-filled older date can win if its
    generation timestamp is newer than a previously-generated newer date.
    """
    return conn.execute(
        "SELECT * FROM digests ORDER BY generated_at DESC, date DESC LIMIT 1"
    ).fetchone()


def get_digest_row(conn: sqlite3.Connection, date: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM digests WHERE date=?", (date,)).fetchone()


def digest_story_ids(conn: sqlite3.Connection, date: str) -> List[int]:
    row = get_digest_row(conn, date)
    if row is None:
        return []
    raw_ids = _json_loads(row["story_ids"], [])
    if not isinstance(raw_ids, list):
        return []
    out: List[int] = []
    seen: set[int] = set()
    for raw_id in raw_ids:
        try:
            sid = int(raw_id)
        except (TypeError, ValueError):
            continue
        if sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
    return out


def stories_by_ids(
    conn: sqlite3.Connection, story_ids: Sequence[int]
) -> List[sqlite3.Row]:
    if not story_ids:
        return []
    with id_in_clause(conn, story_ids) as (clause, params):
        return conn.execute(
            f"SELECT * FROM stories WHERE id {clause} "
            "AND enrich_status='done'",
            tuple(params),
        ).fetchall()


def _story_hn_time_for_digest(row: sqlite3.Row) -> int:
    if row["enrich_status"] == "done" and row["enriched_at"] is not None:
        return int(row["enriched_hn_time"] or row["hn_time"] or 0)
    return int(row["hn_time"] or 0)


def _story_matches_digest_date(row: sqlite3.Row, date: str) -> bool:
    hn_time = _story_hn_time_for_digest(row)
    return hn_time > 0 and date_in_digest_tz(hn_time) == date


def digest_story_ids_matching_date(
    conn: sqlite3.Connection,
    date: str,
    story_ids: Sequence[int],
) -> List[int]:
    int_ids: List[int] = []
    for sid in story_ids:
        try:
            int_ids.append(int(sid))
        except (TypeError, ValueError):
            continue
    if not int_ids:
        return []

    rows = stories_by_ids(conn, int_ids)
    rows_by_id = {int(r["id"]): r for r in rows}
    return [
        sid
        for sid in int_ids
        if sid in rows_by_id and _story_matches_digest_date(rows_by_id[sid], date)
    ]


def get_digest(
    conn: sqlite3.Connection, date: Optional[str]
) -> Tuple[str, str, List[Story]]:
    """Read a digest by date, defaulting to today's digest.

    - ``date is None``: today (``DIGEST_TIMEZONE``), never an older fallback.
    - ``date`` given but missing: that date, empty payload.
    - Stored story IDs whose HN publish date does not match the digest date
      are filtered out defensively, including legacy rows written before this
      invariant existed.
    """
    if date is None:
        date = today_in_digest_tz()

    row = get_digest_row(conn, date)
    if row is None:
        return date, "", []
    intro = row["intro"] or ""
    story_ids = _json_loads(row["story_ids"], [])

    if not isinstance(story_ids, list) or not story_ids:
        return date, intro, []

    int_ids = digest_story_ids_matching_date(conn, date, story_ids)
    if not int_ids:
        return date, intro, []

    rows = stories_by_ids(conn, int_ids)
    rows_by_id = {int(r["id"]): r for r in rows}
    stories = [row_to_story(rows_by_id[sid]) for sid in int_ids if sid in rows_by_id]
    return date, intro, stories


# ---------- story writes ----------

def insert_story_pending(
    conn: sqlite3.Connection, story_row: dict
) -> bool:
    """Insert a fresh HN story with ``enrich_status='pending'``.

    Returns True if a new row was inserted, False if the id already exists.
    """
    now = now_seconds()
    fetched_at = story_row.get("fetched_at", now)
    last_seen_at = story_row.get("last_seen_at", now)
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO stories (
            id, kind, title_en, title_zh, url, domain, by,
            score, descendants, hn_time,
            raw_text, raw_json,
            topic, ai_summary,
            discussion_themes, insights, terms,
            enrich_status, enrich_attempts, enrich_error,
            enrich_started_at, enriched_at,
            fetched_at, last_seen_at
        ) VALUES (
            :id, :kind, :title_en, :title_zh, :url, :domain, :by,
            :score, :descendants, :hn_time,
            :raw_text, :raw_json,
            :topic, '',
            '[]', '[]', '[]',
            'pending', 0, NULL,
            NULL, NULL,
            :fetched_at, :last_seen_at
        )
        """,
        {
            "id": story_row["id"],
            "kind": story_row.get("kind", "story"),
            "title_en": story_row.get("title_en", ""),
            "title_zh": story_row.get("title_zh", "") or story_row.get("title_en", ""),
            "url": story_row.get("url", ""),
            "domain": story_row.get("domain", "news.ycombinator.com"),
            "by": story_row.get("by", ""),
            "score": int(story_row.get("score", 0) or 0),
            "descendants": int(story_row.get("descendants", 0) or 0),
            "hn_time": int(story_row.get("hn_time", 0) or 0),
            "raw_text": story_row.get("raw_text", "") or "",
            "raw_json": story_row.get("raw_json", "{}") or "{}",
            "topic": story_row.get("topic", DEFAULT_TOPIC_ID) or DEFAULT_TOPIC_ID,
            "fetched_at": fetched_at,
            "last_seen_at": last_seen_at,
        },
    )
    return cursor.rowcount > 0


def update_story_metrics(
    conn: sqlite3.Connection,
    story_id: int,
    *,
    score: int,
    descendants: int,
    last_seen_at: int,
    title_en: Optional[str] = None,
    url: Optional[str] = None,
    domain: Optional[str] = None,
    by: Optional[str] = None,
    hn_time: Optional[int] = None,
    raw_text: Optional[str] = None,
    raw_json: Optional[str] = None,
    fetched_at: Optional[int] = None,
) -> None:
    """Plan §A2: only score/descendants/last_seen_at on existing rows."""
    current = conn.execute(
        """
        SELECT
            title_en, url, domain, by, hn_time, raw_text, raw_json, fetched_at,
            descendants, enrich_status, needs_reenrich,
            comments_fetched_descendants, enriched_descendants
        FROM stories WHERE id=?
        """,
        (int(story_id),),
    ).fetchone()
    if current is None:
        return

    next_title = title_en if title_en is not None else current["title_en"]
    next_url = url if url is not None else current["url"]
    next_domain = domain if domain is not None else current["domain"]
    next_by = by if by is not None else current["by"]
    next_hn_time = hn_time if hn_time is not None else current["hn_time"]
    next_raw_text = raw_text if raw_text is not None else current["raw_text"]
    next_raw_json = raw_json if raw_json is not None else current["raw_json"]
    next_fetched_at = fetched_at if fetched_at is not None else current["fetched_at"]
    next_descendants = int(descendants)
    should_reenrich = False
    if current["enrich_status"] == "done":
        text_changed = (
            (next_title or "") != (current["title_en"] or "")
            or (next_url or "") != (current["url"] or "")
            or (next_domain or "") != (current["domain"] or "")
            or (next_by or "") != (current["by"] or "")
            or int(next_hn_time or 0) != int(current["hn_time"] or 0)
            or (next_raw_text or "") != (current["raw_text"] or "")
        )
        previous_descendants = max(
            int(current["descendants"] or 0),
            int(current["enriched_descendants"] or 0),
            int(current["comments_fetched_descendants"] or 0),
        )
        descendant_delta = max(0, next_descendants - previous_descendants)
        crossed_comment_gate = (
            previous_descendants < settings.COMMENT_MIN_DESCENDANTS
            <= next_descendants
        )
        enough_absolute_growth = (
            descendant_delta >= settings.REENRICH_DESCENDANTS_MIN_DELTA
        )
        enough_relative_growth = (
            previous_descendants > 0
            and descendant_delta * 100
            >= previous_descendants
            * max(0, int(settings.REENRICH_DESCENDANTS_MIN_GROWTH_PERCENT))
        )
        should_reenrich = bool(
            text_changed
            or crossed_comment_gate
            or enough_absolute_growth
            or enough_relative_growth
        )
    reenrich_flag = 1 if should_reenrich else 0
    conn.execute(
        """
        UPDATE stories
        SET title_en=?,
            title_zh=CASE
                WHEN enrich_status IN ('pending', 'failed') THEN ?
                ELSE title_zh
            END,
            url=?,
            domain=?,
            by=?,
            score=?,
            descendants=?,
            hn_time=?,
            raw_text=?,
            raw_json=?,
            fetched_at=?,
            last_seen_at=?,
            enrich_status=CASE
                WHEN enrich_status='failed' THEN 'pending'
                ELSE enrich_status
            END,
            enrich_attempts=CASE
                WHEN enrich_status='failed' THEN 0
                ELSE enrich_attempts
            END,
            enrich_error=CASE
                WHEN enrich_status='failed' THEN NULL
                ELSE enrich_error
            END,
            enrich_started_at=CASE
                WHEN enrich_status='failed' THEN NULL
                ELSE enrich_started_at
            END,
            needs_reenrich=CASE
                WHEN enrich_status='done' AND ?=1 THEN 1
                ELSE needs_reenrich
            END,
            comments_fetched_descendants=CASE
                WHEN enrich_status='done' AND ?=1 THEN 0
                ELSE comments_fetched_descendants
            END,
            reenrich_started_at=CASE
                WHEN enrich_status='done' AND ?=1 THEN NULL
                ELSE reenrich_started_at
            END,
            reenrich_attempts=CASE
                WHEN enrich_status='done' AND ?=1 THEN 0
                ELSE reenrich_attempts
            END
        WHERE id=?
        """,
        (
            next_title or "",
            next_title or "",
            next_url or "",
            next_domain or "news.ycombinator.com",
            next_by or "",
            int(score),
            int(descendants),
            int(next_hn_time or 0),
            next_raw_text or "",
            next_raw_json or "{}",
            int(next_fetched_at or now_seconds()),
            int(last_seen_at),
            reenrich_flag,
            reenrich_flag,
            reenrich_flag,
            reenrich_flag,
            int(story_id),
        ),
    )


def update_story_kind(conn: sqlite3.Connection, story_id: int, kind: str) -> None:
    """Plan §5: kind may upgrade story->ask/show when feed/title proves it."""
    if kind not in ("story", "ask", "show", "job"):
        return
    conn.execute(
        """
        UPDATE stories
        SET kind=?,
            needs_reenrich=CASE
                WHEN enrich_status='done' THEN 1
                ELSE needs_reenrich
            END,
            comments_fetched_descendants=CASE
                WHEN enrich_status='done' THEN 0
                ELSE comments_fetched_descendants
            END,
            reenrich_started_at=CASE
                WHEN enrich_status='done' THEN NULL
                ELSE reenrich_started_at
            END,
            reenrich_attempts=CASE
                WHEN enrich_status='done' THEN 0
                ELSE reenrich_attempts
            END
        WHERE id=?
        """,
        (kind, int(story_id)),
    )


def story_exists(conn: sqlite3.Connection, story_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM stories WHERE id=?", (int(story_id),)).fetchone()
    return row is not None


def get_story_basic(
    conn: sqlite3.Connection, story_id: int
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT id, kind, score, descendants, enrich_status FROM stories WHERE id=?",
        (int(story_id),),
    ).fetchone()


# ---------- ranking writes ----------

def replace_feed_ranking(
    conn: sqlite3.Connection, feed: str, story_ids: Sequence[int]
) -> None:
    """Atomically replace the ranking window for a feed.

    Plan §A3: delete + insert preserves HN order while evicting stale tail.
    Caller must already be inside a transaction.
    """
    now = now_seconds()
    conn.execute("DELETE FROM rankings WHERE feed=?", (feed,))
    rows = [
        (feed, rank + 1, int(sid), now)
        for rank, sid in enumerate(story_ids[: settings.FEED_WINDOW_SIZE])
    ]
    if rows:
        conn.executemany(
            "INSERT INTO rankings(feed, rank, story_id, refreshed_at) VALUES(?, ?, ?, ?)",
            rows,
        )


def replace_ranking_candidates(
    conn: sqlite3.Connection,
    run_id: str,
    feed: str,
    story_ids: Sequence[int],
) -> None:
    """Stage one feed's ranking for an ingest run without publishing it."""
    now = now_seconds()
    conn.execute(
        "DELETE FROM ranking_candidates WHERE run_id=? AND feed=?",
        (run_id, feed),
    )
    rows = [
        (run_id, feed, rank + 1, int(sid), now)
        for rank, sid in enumerate(story_ids[: settings.FEED_WINDOW_SIZE])
    ]
    if rows:
        conn.executemany(
            """
            INSERT INTO ranking_candidates(run_id, feed, rank, story_id, fetched_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            rows,
        )


def candidate_story_ids(conn: sqlite3.Connection, run_id: str) -> List[int]:
    rows = conn.execute(
        "SELECT DISTINCT story_id FROM ranking_candidates WHERE run_id=?",
        (run_id,),
    ).fetchall()
    return [int(r["story_id"]) for r in rows]


def candidate_count(conn: sqlite3.Connection, run_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(DISTINCT story_id) AS c FROM ranking_candidates WHERE run_id=?",
        (run_id,),
    ).fetchone()
    return int(row["c"]) if row else 0


def count_incomplete_candidates(conn: sqlite3.Connection, run_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT rc.story_id) AS c
        FROM ranking_candidates rc
        JOIN stories s ON s.id=rc.story_id
        WHERE rc.run_id=?
          AND NOT (s.enrich_status='done' AND s.needs_reenrich=0)
        """,
        (run_id,),
    ).fetchone()
    return int(row["c"]) if row else 0


def delete_ranking_candidates(conn: sqlite3.Connection, run_id: str) -> int:
    cursor = conn.execute("DELETE FROM ranking_candidates WHERE run_id=?", (run_id,))
    return cursor.rowcount or 0


def _visible_done_story_ids(conn: sqlite3.Connection, feed: str) -> List[int]:
    rows = conn.execute(
        """
        SELECT r.story_id
        FROM rankings r
        JOIN stories s ON s.id=r.story_id
        WHERE r.feed=? AND s.enrich_status='done'
        ORDER BY r.rank ASC
        """,
        (feed,),
    ).fetchall()
    return [int(r["story_id"]) for r in rows]


def publish_ranking_candidates(
    conn: sqlite3.Connection,
    run_id: str,
    feeds: Sequence[str],
    *,
    preserve_existing: bool = False,
) -> dict:
    """Publish currently displayable staged candidates into visible rankings.

    ``preserve_existing`` is used by incremental checkpoints. It keeps the
    previous visible tail behind newly-ready candidates so clients can see
    successful work stream in without the feed collapsing while slower items
    are still pending.

    Feeds with no staged rows are left untouched, which preserves the last
    known-good feed when the HN ranking request failed in the current run.
    """
    summary = {
        "changed": False,
        "feeds": {},
        "published_count": 0,
        "ready_count": 0,
        "preserve_existing": bool(preserve_existing),
        "skipped_stale_run": False,
    }
    current_run = conn.execute(
        "SELECT started_at FROM ingest_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if current_run is not None:
        newer = conn.execute(
            """
            SELECT run_id
            FROM ingest_runs
            WHERE run_id <> ?
              AND started_at > ?
              AND status IN ('running', 'completed', 'partial')
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (run_id, int(current_run["started_at"])),
        ).fetchone()
        if newer is not None:
            summary["skipped_stale_run"] = True
            summary["stale_reason"] = (
                "newer ingest run already started: " + str(newer["run_id"])
            )
            for feed in feeds:
                summary["feeds"][feed] = {
                    "size": 0,
                    "ready": 0,
                    "changed": False,
                    "skipped": True,
                }
            return summary

    any_feed_written = False
    changed = False
    for feed in feeds:
        staged_rows = conn.execute(
            """
            SELECT
                rc.story_id,
                s.enrich_status,
                s.needs_reenrich
            FROM ranking_candidates rc
            JOIN stories s ON s.id=rc.story_id
            WHERE rc.run_id=?
              AND rc.feed=?
            ORDER BY rc.rank ASC
            """,
            (run_id, feed),
        ).fetchall()
        total_staged = len(staged_rows)
        if total_staged <= 0:
            summary["feeds"][feed] = {
                "size": 0,
                "ready": 0,
                "changed": False,
                "skipped": True,
            }
            continue

        story_ids: List[int] = []
        seen_story_ids: set[int] = set()
        ready_count = 0
        for row in staged_rows:
            sid = int(row["story_id"])
            if row["enrich_status"] != "done":
                continue
            # A done row with needs_reenrich=1 still has a complete previous
            # display snapshot, so it is safe to keep visible while refresh
            # catches up. First-time pending/failed rows are not publishable.
            story_ids.append(sid)
            seen_story_ids.add(sid)
            ready_count += 1

        if preserve_existing:
            for sid in _visible_done_story_ids(conn, feed):
                if sid in seen_story_ids:
                    continue
                story_ids.append(sid)
                seen_story_ids.add(sid)
                if len(story_ids) >= settings.FEED_WINDOW_SIZE:
                    break

        visible_hash = hash_int_sequence(story_ids)
        prev_hash = get_meta(conn, f"visible_ranking_hash:{feed}") or ""
        replace_feed_ranking(conn, feed, story_ids)
        set_meta(conn, f"last_refresh_at:{feed}", str(now_seconds()))
        feed_changed = visible_hash != prev_hash
        if feed_changed:
            set_meta(conn, f"visible_ranking_hash:{feed}", visible_hash)
            changed = True
        summary["feeds"][feed] = {
            "size": len(story_ids),
            "ready": ready_count,
            "changed": feed_changed,
            "skipped": False,
        }
        summary["published_count"] += len(story_ids)
        summary["ready_count"] += ready_count
        any_feed_written = True

    if any_feed_written:
        set_meta(conn, "last_full_fetch_at", str(now_seconds()))
    if changed:
        bump_catalog_version(conn)
        summary["changed"] = True
    return summary


def feed_story_ids(conn: sqlite3.Connection, feed: str) -> List[int]:
    rows = conn.execute(
        "SELECT story_id FROM rankings WHERE feed=? ORDER BY rank ASC",
        (feed,),
    ).fetchall()
    return [int(r["story_id"]) for r in rows]


def all_ranked_story_ids(conn: sqlite3.Connection) -> List[int]:
    rows = conn.execute("SELECT DISTINCT story_id FROM rankings").fetchall()
    return [int(r["story_id"]) for r in rows]


# ---------- enrich queue ----------

def claim_pending_stories(
    conn: sqlite3.Connection,
    batch_size: int,
    stale_before: int,
    exclude_ids: Sequence[int] = (),
    target_ids: Optional[Sequence[int]] = None,
) -> List[sqlite3.Row]:
    """Atomically claim initial enrich jobs and done-story refresh jobs."""
    now = now_seconds()
    excluded = [int(sid) for sid in exclude_ids]
    targets = [int(sid) for sid in target_ids] if target_ids is not None else []
    if target_ids is not None and not targets:
        return []

    with _optional_in(conn, excluded) as exclude_part, \
            _optional_in(conn, targets) as target_part:
        exclude_clause_sql = (
            f"AND id NOT {exclude_part[0]}" if exclude_part is not None else ""
        )
        target_clause_sql = (
            f"AND id {target_part[0]}" if target_part is not None else ""
        )
        exclude_params = exclude_part[1] if exclude_part is not None else []
        target_params = target_part[1] if target_part is not None else []
        candidates = conn.execute(
            f"""
            SELECT id, enrich_status, needs_reenrich FROM stories
            WHERE (
                (enrich_status='pending'
                   AND (enrich_started_at IS NULL
                        OR COALESCE(enrich_started_at, 0) < ?)
                   AND COALESCE(enrich_retry_after, 0) <= ?)
               OR (enrich_status='enriching' AND COALESCE(enrich_started_at, 0) < ?)
               OR (enrich_status='done'
                   AND needs_reenrich=1
                   AND COALESCE(reenrich_attempts, 0) < ?
                   AND (reenrich_started_at IS NULL
                        OR COALESCE(reenrich_started_at, 0) < ?)
                   AND COALESCE(reenrich_retry_after, 0) <= ?)
            )
            {exclude_clause_sql}
            {target_clause_sql}
            ORDER BY
                CASE
                    WHEN enrich_status='pending' THEN 0
                    WHEN enrich_status='enriching' THEN 1
                    ELSE 2
                END,
                last_seen_at DESC
            LIMIT ?
            """,
            (
                stale_before,
                now,
                stale_before,
                settings.ENRICH_MAX_ATTEMPTS,
                stale_before,
                now,
                *exclude_params,
                *target_params,
                batch_size,
            ),
        ).fetchall()
    if not candidates:
        return []

    initial_ids = [
        int(r["id"])
        for r in candidates
        if r["enrich_status"] in ("pending", "enriching")
    ]
    refresh_ids = [
        int(r["id"])
        for r in candidates
        if r["enrich_status"] == "done" and int(r["needs_reenrich"] or 0) == 1
    ]

    if initial_ids:
        with id_in_clause(conn, initial_ids) as (clause, params):
            conn.execute(
                f"UPDATE stories SET enrich_status='enriching', enrich_started_at=? "
                f"WHERE id {clause} AND ("
                f"(enrich_status='pending' AND (enrich_started_at IS NULL "
                f"OR COALESCE(enrich_started_at, 0) < ?)) "
                f"OR (enrich_status='enriching' AND COALESCE(enrich_started_at, 0) < ?))",
                (now, *params, stale_before, stale_before),
            )

    if refresh_ids:
        with id_in_clause(conn, refresh_ids) as (clause, params):
            conn.execute(
                f"UPDATE stories SET needs_reenrich=1, reenrich_started_at=? "
                f"WHERE id {clause} "
                f"AND enrich_status='done' "
                f"AND needs_reenrich=1 "
                f"AND COALESCE(reenrich_attempts, 0) < ? "
                f"AND (reenrich_started_at IS NULL "
                f"OR COALESCE(reenrich_started_at, 0) < ?)",
                (now, *params, settings.ENRICH_MAX_ATTEMPTS, stale_before),
            )

    ids = [int(r["id"]) for r in candidates]
    with id_in_clause(conn, ids) as (clause, params):
        rows = conn.execute(
            f"SELECT * FROM stories WHERE id {clause} AND ("
            f"(enrich_status='enriching' AND enrich_started_at=?) "
            f"OR (enrich_status='done' AND needs_reenrich=1 AND reenrich_started_at=?))",
            (*params, now, now),
        ).fetchall()
    return rows


def reset_stale_enriching(conn: sqlite3.Connection, stale_before: int) -> int:
    """Plan §B Stale recovery: revert stuck rows to ``pending``.

    Preserves ``enrich_attempts`` so retries do not become infinite.
    """
    cursor = conn.execute(
        "UPDATE stories SET enrich_status='pending' "
        "WHERE enrich_status='enriching' AND COALESCE(enrich_started_at, 0) < ?",
        (stale_before,),
    )
    return cursor.rowcount or 0


def release_inflight_claims_for_ids(
    conn: sqlite3.Connection, story_ids: Sequence[int]
) -> dict:
    """C.#4: release a specific subset of in-flight claims (initial enrich
    or refresh) so a deadline-bound enrich loop can hand them back instead
    of leaving them stuck in ``enriching`` until stale-recovery."""
    ids = [int(sid) for sid in story_ids]
    if not ids:
        return {"pending_released": 0, "refresh_released": 0}
    with id_in_clause(conn, ids) as (clause, params):
        pending = conn.execute(
            f"""
            UPDATE stories
            SET enrich_status='pending',
                enrich_started_at=NULL
            WHERE enrich_status='enriching'
              AND id {clause}
            """,
            params,
        ).rowcount or 0
        refresh = conn.execute(
            f"""
            UPDATE stories
            SET reenrich_started_at=NULL
            WHERE enrich_status='done'
              AND needs_reenrich=1
              AND reenrich_started_at IS NOT NULL
              AND id {clause}
            """,
            params,
        ).rowcount or 0
    return {"pending_released": int(pending), "refresh_released": int(refresh)}


def reset_inflight_enrichment(
    conn: sqlite3.Connection, run_id: str
) -> dict:
    """A.#10: release in-flight enrich claims for ONE run, identified by
    its ranking_candidates rows.

    Previously this released every ``enriching`` row globally, which would
    silently steal another active ingest's claims if two processes ran
    concurrently. Now the release is scoped: only stories that ``run_id``
    staged into ``ranking_candidates`` are affected.
    """
    candidate_ids = candidate_story_ids(conn, run_id)
    if not candidate_ids:
        return {"pending_released": 0, "refresh_released": 0}

    with id_in_clause(conn, candidate_ids) as (clause, params):
        pending = conn.execute(
            f"""
            UPDATE stories
            SET enrich_status='pending',
                enrich_started_at=NULL
            WHERE enrich_status='enriching'
              AND id {clause}
            """,
            params,
        ).rowcount or 0
        refresh = conn.execute(
            f"""
            UPDATE stories
            SET reenrich_started_at=NULL
            WHERE enrich_status='done'
              AND needs_reenrich=1
              AND reenrich_started_at IS NOT NULL
              AND id {clause}
            """,
            params,
        ).rowcount or 0
    return {"pending_released": int(pending), "refresh_released": int(refresh)}


def write_enriched_story(
    conn: sqlite3.Connection,
    story_id: int,
    *,
    title_zh: str,
    topic: str,
    topic_name: Optional[str] = None,
    ai_summary: str,
    insights: List[dict],
    terms: List[dict],
    discussion_themes: Optional[List[dict]] = None,
    comments_fetched_descendants: Optional[int] = None,
) -> None:
    """Plan §B step 5/6: write all enrichment outputs and mark ``done``."""
    now = now_seconds()
    topic_entry = ensure_topic(
        conn,
        topic or DEFAULT_TOPIC_ID,
        topic_name or topic or DEFAULT_TOPIC_NAME,
        seen_at=now,
    )
    conn.execute(
        """
        UPDATE stories
        SET title_zh=?,
            topic=?,
            ai_summary=?,
            discussion_themes=?,
            insights=?,
            terms=?,
            enrich_status='done',
            enriched_at=?,
            enriched_title_en=title_en,
            enriched_url=url,
            enriched_domain=domain,
            enriched_by=by,
            enriched_hn_time=hn_time,
            enriched_score=score,
            enriched_descendants=descendants,
            comments_fetched_descendants=COALESCE(?, descendants),
            needs_reenrich=0,
            enrich_started_at=NULL,
            reenrich_started_at=NULL,
            reenrich_attempts=0,
            enrich_retry_after=0,
            reenrich_retry_after=0,
            enrich_error=NULL
        WHERE id=?
        """,
        (
            title_zh,
            topic_entry.id,
            ai_summary,
            _json_dumps(discussion_themes or []),
            _json_dumps(insights or []),
            _json_dumps(terms or []),
            now,
            (
                int(comments_fetched_descendants)
                if comments_fetched_descendants is not None
                else None
            ),
            int(story_id),
        ),
    )


def mark_enrich_failed(
    conn: sqlite3.Connection,
    story_id: int,
    *,
    title_en: str,
    error: str,
) -> bool:
    """Plan §B: write fallback display fields and mark ``failed``.

    Returns True iff any client-visible field actually changed. Pending rows
    already carry the same fallback values (see ``insert_story_pending``), so
    most pending->failed transitions are visible no-ops and the caller must
    skip the ``catalog_version`` bump.
    """
    target_title_zh = title_en or ""
    target_ai_summary = ""
    target_discussion_themes = "[]"
    target_insights = "[]"
    target_terms = "[]"

    existing = conn.execute(
        """
        SELECT title_zh, ai_summary,
               discussion_themes, insights, terms
        FROM stories WHERE id=?
        """,
        (int(story_id),),
    ).fetchone()

    changed_visible = True
    if existing is not None:
        changed_visible = not (
            (existing["title_zh"] or "") == target_title_zh
            and (existing["ai_summary"] or "") == target_ai_summary
            and (existing["discussion_themes"] or "[]") == target_discussion_themes
            and (existing["insights"] or "[]") == target_insights
            and (existing["terms"] or "[]") == target_terms
        )

    conn.execute(
        """
        UPDATE stories
        SET title_zh=?,
            ai_summary=?,
            discussion_themes=?,
            insights=?,
            terms=?,
            enrich_status='failed',
            enrich_error=?,
            enrich_started_at=NULL
        WHERE id=?
        """,
        (
            target_title_zh,
            target_ai_summary,
            target_discussion_themes,
            target_insights,
            target_terms,
            error[:500] if error else "",
            int(story_id),
        ),
    )
    return changed_visible


def mark_enrich_pending_retry(
    conn: sqlite3.Connection, story_id: int, *, error: str
) -> None:
    """Re-queue a failed enrichment without touching ``enrich_attempts``.

    The caller (``ingest.run_enricher_once``) already bumps the counter via
    :func:`increment_enrich_attempts`; doing it twice would consume retries
    twice as fast and make ``enrich_attempts`` lie about reality.
    """
    conn.execute(
        """
        UPDATE stories
        SET enrich_status='pending',
            enrich_error=?,
            enrich_started_at=NULL
        WHERE id=?
        """,
        (error[:500] if error else "", int(story_id)),
    )


def increment_enrich_attempts(conn: sqlite3.Connection, story_id: int) -> int:
    conn.execute(
        "UPDATE stories SET enrich_attempts = enrich_attempts + 1 WHERE id=?",
        (int(story_id),),
    )
    row = conn.execute(
        "SELECT enrich_attempts FROM stories WHERE id=?", (int(story_id),)
    ).fetchone()
    return int(row["enrich_attempts"]) if row else 0


def increment_reenrich_attempts(conn: sqlite3.Connection, story_id: int) -> int:
    conn.execute(
        "UPDATE stories SET reenrich_attempts = reenrich_attempts + 1 WHERE id=?",
        (int(story_id),),
    )
    row = conn.execute(
        "SELECT reenrich_attempts FROM stories WHERE id=?", (int(story_id),)
    ).fetchone()
    return int(row["reenrich_attempts"]) if row else 0


def mark_reenrich_retry(conn: sqlite3.Connection, story_id: int, *, error: str) -> None:
    """Release a failed done-story refresh while preserving old AI output."""
    conn.execute(
        """
        UPDATE stories
        SET needs_reenrich=1,
            reenrich_started_at=NULL,
            enrich_error=?
        WHERE id=?
        """,
        (error[:500] if error else "", int(story_id)),
    )


def mark_reenrich_failed(conn: sqlite3.Connection, story_id: int, *, error: str) -> None:
    """Stop retrying this refresh cycle but keep the previous done payload."""
    conn.execute(
        """
        UPDATE stories
        SET needs_reenrich=0,
            reenrich_started_at=NULL,
            enrich_error=?
        WHERE id=?
        """,
        (error[:500] if error else "", int(story_id)),
    )


def defer_enrich_retry(
    conn: sqlite3.Connection,
    story_id: int,
    *,
    retry_after: int,
    error: str,
) -> None:
    """Park a pending/enriching row until ``retry_after`` without bumping attempts.

    Used when the AI stack reports a capacity event (429, all providers
    cooling down, request not even attempted because of provider concurrency
    caps). The story stays in the queue but won't be picked up by
    :func:`claim_pending_stories` until ``now >= enrich_retry_after``, so
    quota incidents don't promote stories to ``failed`` after a few retries.
    """
    conn.execute(
        """
        UPDATE stories
        SET enrich_status='pending',
            enrich_started_at=NULL,
            enrich_retry_after=?,
            enrich_error=?
        WHERE id=?
        """,
        (
            int(max(0, retry_after)),
            error[:500] if error else "",
            int(story_id),
        ),
    )


def defer_reenrich_retry(
    conn: sqlite3.Connection,
    story_id: int,
    *,
    retry_after: int,
    error: str,
) -> None:
    """Park a refresh job until ``retry_after`` without bumping attempts."""
    conn.execute(
        """
        UPDATE stories
        SET reenrich_started_at=NULL,
            reenrich_retry_after=?,
            enrich_error=?
        WHERE id=?
        """,
        (
            int(max(0, retry_after)),
            error[:500] if error else "",
            int(story_id),
        ),
    )


def reset_failed_to_pending(conn: sqlite3.Connection) -> int:
    """CLI ``--reset-failed`` support: requeue failed rows for retry."""
    cursor = conn.execute(
        "UPDATE stories SET enrich_status='pending', enrich_attempts=0, enrich_error=NULL "
        "WHERE enrich_status='failed'"
    )
    return cursor.rowcount or 0


def count_enrich_status(conn: sqlite3.Connection, status: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM stories WHERE enrich_status=?", (status,)
    ).fetchone()
    return int(row["c"]) if row else 0


def count_pending_imminent_failures(
    conn: sqlite3.Connection, max_attempts: int
) -> int:
    """Pending rows that have already burned ``max_attempts - 1`` retries.

    Such rows are one failed attempt away from being promoted to ``failed``;
    surfacing them in pipeline metrics prevents ``failure_rate`` from reading
    0% during the death-loop window where a small set of stories fails every
    round but hasn't yet exhausted attempts (e.g. ``max_tokens`` truncation
    on a story whose schema doesn't fit the configured output budget).
    """
    threshold = max(1, int(max_attempts) - 1)
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM stories "
        "WHERE enrich_status='pending' AND enrich_attempts >= ?",
        (threshold,),
    ).fetchone()
    return int(row["c"]) if row else 0


def _ingest_run_row_to_dict(row: sqlite3.Row) -> dict:
    out = dict(row)
    if "ai_usage" in out and out["ai_usage"]:
        usage = _json_loads(out["ai_usage"], None)
        out["ai_usage"] = usage if isinstance(usage, dict) else None
    return out


def get_pipeline_metrics(conn: sqlite3.Connection) -> dict:
    """Plan P2: read-only operational metrics for ``--metrics`` CLI output.

    Empty DB returns zeros / nulls, never raises.

    ``failure_rate`` counts both fully-failed rows and pending rows that have
    already burned through ``ENRICH_MAX_ATTEMPTS - 1`` retries. Without the
    second bucket a death-loop (e.g. recurring ``max_tokens`` truncation on
    a stubborn story) reads as 0% until the row finally exhausts attempts —
    which can be hours later if every round is timed out before the last try
    lands. ``pending_imminent_failures`` is reported separately so dashboards
    can distinguish "already given up" from "next failure tips it over".
    """
    pending = count_enrich_status(conn, "pending")
    enriching = count_enrich_status(conn, "enriching")
    done = count_enrich_status(conn, "done")
    failed = count_enrich_status(conn, "failed")
    pending_imminent = count_pending_imminent_failures(
        conn, settings.ENRICH_MAX_ATTEMPTS
    )
    total = pending + enriching + done + failed
    failure_rate = ((failed + pending_imminent) / total) if total else 0.0

    feeds = ("top", "new", "best", "ask", "show", "job")
    last_refresh_at = {
        feed: get_meta_int(conn, f"last_refresh_at:{feed}") for feed in feeds
    }

    latest = get_digest_row(conn, today_in_digest_tz())
    latest_digest = (
        {"date": latest["date"], "generated_at": int(latest["generated_at"])}
        if latest is not None
        else None
    )
    latest_run = latest_ingest_run(conn)

    return {
        "catalog_version": get_catalog_version(conn),
        "enrich_status_counts": {
            "pending": pending,
            "enriching": enriching,
            "done": done,
            "failed": failed,
            "pending_imminent_failures": pending_imminent,
        },
        "total_stories": total,
        "failure_rate": round(failure_rate, 4),
        "last_full_fetch_at": get_meta_int(conn, "last_full_fetch_at"),
        "last_refresh_at": last_refresh_at,
        "latest_digest": latest_digest,
        "latest_run": _ingest_run_row_to_dict(latest_run)
        if latest_run is not None
        else None,
    }


def recent_enrich_errors(conn: sqlite3.Connection, limit: int = 5) -> List[dict]:
    rows = conn.execute(
        """
        SELECT id, enrich_status, enrich_error
        FROM stories
        WHERE enrich_error IS NOT NULL AND enrich_error <> ''
        ORDER BY COALESCE(enrich_started_at, enriched_at, last_seen_at) DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------- ingest run tracking ----------

def start_ingest_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    started_at: int,
    deadline_at: Optional[int],
) -> None:
    conn.execute(
        """
        INSERT INTO ingest_runs(
            run_id, started_at, deadline_at, status, phase
        ) VALUES(?, ?, ?, 'running', '')
        ON CONFLICT(run_id) DO UPDATE SET
            started_at=excluded.started_at,
            deadline_at=excluded.deadline_at,
            finished_at=NULL,
            status='running',
            phase='',
            raw_count=0,
            candidate_count=0,
            claimed=0,
            done=0,
            failed=0,
            retried=0,
            ai_usage=NULL,
            error=NULL
        """,
        (run_id, int(started_at), deadline_at),
    )


def update_ingest_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    phase: Optional[str] = None,
    raw_count: Optional[int] = None,
    candidate_count: Optional[int] = None,
    claimed: Optional[int] = None,
    done: Optional[int] = None,
    failed: Optional[int] = None,
    retried: Optional[int] = None,
    ai_usage: Optional[dict] = None,
) -> None:
    fields = []
    params: List[Any] = []
    for key, value in (
        ("phase", phase),
        ("raw_count", raw_count),
        ("candidate_count", candidate_count),
        ("claimed", claimed),
        ("done", done),
        ("failed", failed),
        ("retried", retried),
    ):
        if value is None:
            continue
        fields.append(f"{key}=?")
        params.append(value)
    if ai_usage is not None:
        fields.append("ai_usage=?")
        params.append(_json_dumps(ai_usage))
    if not fields:
        return
    params.append(run_id)
    conn.execute(
        f"UPDATE ingest_runs SET {', '.join(fields)} WHERE run_id=?",
        tuple(params),
    )


def finish_ingest_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    status: str,
    error: str = "",
    finished_at: Optional[int] = None,
) -> None:
    conn.execute(
        """
        UPDATE ingest_runs
        SET status=?,
            error=?,
            finished_at=?
        WHERE run_id=?
        """,
        (
            status,
            error[:1000] if error else None,
            int(finished_at or now_seconds()),
            run_id,
        ),
    )


def latest_ingest_run(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM ingest_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()


def purge_old_ingest_runs(conn: sqlite3.Connection, retention_days: int) -> int:
    cutoff = now_seconds() - max(0, int(retention_days)) * 24 * 60 * 60
    cursor = conn.execute(
        """
        DELETE FROM ingest_runs
        WHERE started_at < ?
          AND status <> 'running'
        """,
        (cutoff,),
    )
    return cursor.rowcount or 0


def purge_old_cloud_sync_runs(conn: sqlite3.Connection, retention_days: int) -> int:
    cutoff = now_seconds() - max(0, int(retention_days)) * 24 * 60 * 60
    cursor = conn.execute(
        """
        DELETE FROM cloud_sync_runs
        WHERE started_at < ?
          AND status <> 'running'
        """,
        (cutoff,),
    )
    return cursor.rowcount or 0


def purge_old_ranking_candidates(
    conn: sqlite3.Connection, retention_days: int
) -> int:
    cutoff = now_seconds() - max(0, int(retention_days)) * 24 * 60 * 60
    cursor = conn.execute(
        """
        DELETE FROM ranking_candidates
        WHERE fetched_at < ?
          AND run_id NOT IN (
              SELECT run_id FROM ingest_runs WHERE status='running'
          )
        """,
        (cutoff,),
    )
    return cursor.rowcount or 0


# ---------- comments ----------

def replace_story_comments(
    conn: sqlite3.Connection,
    story_id: int,
    comment_rows: Iterable[dict],
    *,
    fetched_descendants: Optional[int] = None,
) -> None:
    """Replace all comments for one story atomically. Caller owns the txn.

    When ``fetched_descendants`` is given, also stamp
    ``stories.comments_fetched_descendants`` so the AI-retry cache gate in
    ``ingest._maybe_fetch_comments`` recognises this snapshot as fresh on
    the next attempt without waiting for ``write_enriched_story``.
    """
    conn.execute("DELETE FROM comments WHERE story_id=?", (int(story_id),))
    payload = []
    for c in comment_rows:
        payload.append(
            (
                int(c["id"]),
                int(story_id),
                c.get("parent_id"),
                c.get("by", "") or "",
                c.get("text", "") or "",
                int(c.get("hn_time", 0) or 0),
                int(c.get("depth", 0) or 0),
                int(c.get("rank", 0) or 0),
                c.get("raw_json", "{}") or "{}",
                int(c.get("fetched_at", now_seconds())),
            )
        )
    if payload:
        conn.executemany(
            """
            INSERT INTO comments(
                id, story_id, parent_id, by, text, hn_time, depth, rank, raw_json, fetched_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                story_id=excluded.story_id,
                parent_id=excluded.parent_id,
                by=excluded.by,
                text=excluded.text,
                hn_time=excluded.hn_time,
                depth=excluded.depth,
                rank=excluded.rank,
                raw_json=excluded.raw_json,
                fetched_at=excluded.fetched_at
            """,
            payload,
        )
    if fetched_descendants is not None:
        conn.execute(
            "UPDATE stories SET comments_fetched_descendants=? WHERE id=?",
            (int(fetched_descendants), int(story_id)),
        )


def list_story_comments(
    conn: sqlite3.Connection, story_id: int, limit: int = 60
) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM comments WHERE story_id=? ORDER BY rank ASC LIMIT ?",
        (int(story_id), int(limit)),
    ).fetchall()


def purge_old_comments(
    conn: sqlite3.Connection, retention_days: int
) -> int:
    """Delete comments older than retention for completed stories."""
    cutoff = now_seconds() - max(int(retention_days), 0) * 24 * 60 * 60
    cursor = conn.execute(
        """
        DELETE FROM comments
        WHERE fetched_at < ?
          AND story_id IN (
              SELECT id FROM stories WHERE enrich_status IN ('done', 'failed')
          )
        """,
        (cutoff,),
    )
    return cursor.rowcount or 0


# ---------- digest writes ----------

def upsert_digest(
    conn: sqlite3.Connection,
    date: str,
    intro: str,
    story_ids: Sequence[int],
) -> bool:
    """Insert/update a digest row. Returns True iff content actually changed.

    Caller owns the transaction and is responsible for bumping
    ``catalog_version`` when this function returns True.
    """
    valid_story_ids = digest_story_ids_matching_date(conn, date, story_ids)
    encoded_ids = _json_dumps(valid_story_ids)
    existing = conn.execute(
        "SELECT intro, story_ids FROM digests WHERE date=?", (date,)
    ).fetchone()
    if existing and existing["intro"] == intro and existing["story_ids"] == encoded_ids:
        return False
    conn.execute(
        """
        INSERT INTO digests(date, intro, story_ids, generated_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            intro=excluded.intro,
            story_ids=excluded.story_ids,
            generated_at=excluded.generated_at
        """,
        (date, intro, encoded_ids, now_seconds()),
    )
    return True


# ---------- insights reads/writes ----------

def get_insight_row(conn: sqlite3.Connection, date: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM insights WHERE date=?", (date,)).fetchone()


def list_insight_rows(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM insights ORDER BY date").fetchall()


def _stable_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _stable_int_ids(ids: Sequence[int]) -> List[int]:
    out: List[int] = []
    seen: set[int] = set()
    for raw in ids:
        try:
            sid = int(raw)
        except (TypeError, ValueError):
            continue
        if sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
    return out


def upsert_insight(
    conn: sqlite3.Connection,
    date: str,
    payload: dict,
    source_story_ids: Sequence[int],
    generated_at: int,
    window_days: int,
    model_usage: Optional[dict] = None,
) -> bool:
    """Insert/update one insights payload.

    Returns True only when publishable content changed. Caller owns the
    transaction and bumps ``catalog_version`` on True.
    """
    encoded_payload = _stable_json_dumps(payload if isinstance(payload, dict) else {})
    encoded_source_ids = _stable_json_dumps(_stable_int_ids(source_story_ids))
    encoded_usage = (
        _stable_json_dumps(model_usage)
        if isinstance(model_usage, dict)
        else None
    )
    existing = conn.execute(
        """
        SELECT payload, source_story_ids, window_days
        FROM insights
        WHERE date=?
        """,
        (date,),
    ).fetchone()
    changed = not (
        existing
        and (existing["payload"] or "{}") == encoded_payload
        and (existing["source_story_ids"] or "[]") == encoded_source_ids
        and int(existing["window_days"] or 0) == int(window_days)
    )
    conn.execute(
        """
        INSERT INTO insights(
            date, payload, source_story_ids, generated_at, window_days, model_usage
        ) VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            payload=excluded.payload,
            source_story_ids=excluded.source_story_ids,
            generated_at=excluded.generated_at,
            window_days=excluded.window_days,
            model_usage=excluded.model_usage
        """,
        (
            date,
            encoded_payload,
            encoded_source_ids,
            int(generated_at),
            int(window_days),
            encoded_usage,
        ),
    )
    return changed


def insight_needs_update(
    conn: sqlite3.Connection,
    date: str,
    min_interval_seconds: int,
    current_candidate_story_ids: Sequence[int],
) -> bool:
    row = get_insight_row(conn, date)
    if row is None:
        return True
    interval = int(min_interval_seconds)
    if interval <= 0:
        return True
    generated_at = int(row["generated_at"] or 0)
    return now_seconds() - generated_at >= interval


def candidate_rows_for_insights(
    conn: sqlite3.Connection,
    *,
    start_ts: int,
    end_ts: int,
    limit: Optional[int] = None,
) -> List[sqlite3.Row]:
    limit_clause = ""
    params: List[Any] = [int(start_ts), int(end_ts)]
    if limit is not None:
        limit_clause = "LIMIT ?"
        params.append(max(1, int(limit)))
    rows = conn.execute(
        f"""
        SELECT s.*, COALESCE(t.name, '') AS topic_name
        FROM stories s
        LEFT JOIN topics t ON t.id=s.topic
        WHERE s.enrich_status='done'
          AND s.enriched_at IS NOT NULL
          AND s.needs_reenrich=0
          AND s.hn_time >= ? AND s.hn_time < ?
        ORDER BY s.hn_time DESC, s.score DESC, s.descendants DESC, s.id ASC
        {limit_clause}
        """,
        tuple(params),
    ).fetchall()
    return list(rows)


def insight_feed_ranks_for_story_ids(
    conn: sqlite3.Connection,
    story_ids: Sequence[int],
) -> dict[int, dict[str, int]]:
    ids = _stable_int_ids(story_ids)
    if not ids:
        return {}
    out: dict[int, dict[str, int]] = {sid: {} for sid in ids}
    with id_in_clause(conn, ids) as (clause, params):
        rows = conn.execute(
            f"""
            SELECT story_id, feed, rank
            FROM rankings
            WHERE story_id {clause}
            ORDER BY story_id ASC, rank ASC
            """,
            tuple(params),
        ).fetchall()
    for row in rows:
        out.setdefault(int(row["story_id"]), {})[str(row["feed"])] = int(row["rank"])
    return out


def insight_comment_rows_for_story_ids(
    conn: sqlite3.Connection,
    story_ids: Sequence[int],
    *,
    limit_per_story: Optional[int],
) -> dict[int, List[sqlite3.Row]]:
    ids = _stable_int_ids(story_ids)
    if not ids or (limit_per_story is not None and int(limit_per_story) <= 0):
        return {}
    grouped: dict[int, List[sqlite3.Row]] = {sid: [] for sid in ids}
    with id_in_clause(conn, ids) as (clause, params):
        rows = conn.execute(
            f"""
            SELECT *
            FROM comments
            WHERE story_id {clause}
            ORDER BY story_id ASC, rank ASC
            """,
            tuple(params),
        ).fetchall()
    limit = None if limit_per_story is None else max(0, int(limit_per_story))
    for row in rows:
        sid = int(row["story_id"])
        bucket = grouped.setdefault(sid, [])
        if limit is None or len(bucket) < limit:
            bucket.append(row)
    return grouped


def record_insight_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    date: str,
    started_at: int,
    finished_at: Optional[int],
    status: str,
    model_usage: Optional[dict] = None,
    error: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO insights_runs(
            run_id, date, started_at, finished_at, status, model_usage, error
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            date=excluded.date,
            started_at=excluded.started_at,
            finished_at=excluded.finished_at,
            status=excluded.status,
            model_usage=excluded.model_usage,
            error=excluded.error
        """,
        (
            run_id,
            date,
            int(started_at),
            finished_at,
            status,
            _stable_json_dumps(model_usage) if isinstance(model_usage, dict) else None,
            error[:1000] if error else None,
        ),
    )


def candidate_done_stories_for_digest(
    conn: sqlite3.Connection,
    date: str,
    limit: int,
    *,
    only_ids: Optional[Sequence[int]] = None,
) -> List[sqlite3.Row]:
    """Done stories whose HN publish time falls on ``date``, ordered by score.

    Filtering by ``hn_time`` (not ``enriched_at``) ensures the digest reflects
    what was actually posted that day, even when older stories happen to be
    enriched in the same window (e.g. backlog catch-up).
    """
    start, end = digest_date_epoch_bounds(date)
    scoped_ids = [int(sid) for sid in only_ids] if only_ids is not None else []
    if only_ids is not None and not scoped_ids:
        return []
    with _optional_in(conn, scoped_ids) as only_part:
        only_clause = ""
        params: Tuple[Any, ...] = (start, end)
        if only_part is not None:
            only_clause = f"AND id {only_part[0]}"
            params = (start, end, *only_part[1])
        rows = conn.execute(
            f"""
            SELECT * FROM stories
            WHERE enrich_status='done'
              AND enriched_at IS NOT NULL
              AND needs_reenrich=0
              AND hn_time >= ? AND hn_time < ?
              {only_clause}
            ORDER BY score DESC, hn_time DESC
            LIMIT ?
            """,
            (*params, max(int(limit), 1)),
        ).fetchall()
    return list(rows)


def count_today_done_stories(conn: sqlite3.Connection, date: str) -> int:
    start, end = digest_date_epoch_bounds(date)
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM stories "
        "WHERE enrich_status='done' AND enriched_at IS NOT NULL "
        "  AND needs_reenrich=0 "
        "  AND hn_time >= ? AND hn_time < ?",
        (start, end),
    ).fetchone()
    return int(row["n"]) if row else 0


def done_story_ids_for_digest_date(
    conn: sqlite3.Connection,
    date: str,
    *,
    max_enriched_at: Optional[int] = None,
    only_ids: Optional[Sequence[int]] = None,
) -> List[int]:
    """Done story IDs whose HN publish time falls on ``date``.

    When ``max_enriched_at`` is provided, also require the story to have been
    enriched by that timestamp — used to reconstruct the candidate baseline at
    the moment a digest was generated.
    """
    start, end = digest_date_epoch_bounds(date)
    params: List[Any] = [start, end]
    scoped_ids = [int(sid) for sid in only_ids] if only_ids is not None else []
    if only_ids is not None and not scoped_ids:
        return []
    enriched_clause = ""
    if max_enriched_at is not None:
        enriched_clause = "AND enriched_at <= ?"
        params.append(int(max_enriched_at))
    with _optional_in(conn, scoped_ids) as only_part:
        only_clause = ""
        query_params = list(params)
        if only_part is not None:
            only_clause = f"AND id {only_part[0]}"
            query_params = [start, end, *only_part[1]]
            if max_enriched_at is not None:
                query_params.append(int(max_enriched_at))
        rows = conn.execute(
            f"""
            SELECT id FROM stories
            WHERE enrich_status='done'
              AND enriched_at IS NOT NULL
              AND needs_reenrich=0
              AND hn_time >= ? AND hn_time < ?
              {only_clause}
              {enriched_clause}
            """,
            tuple(query_params),
        ).fetchall()
    return [int(r["id"]) for r in rows]


def get_digest_seen_done_ids(
    conn: sqlite3.Connection,
    date: str,
    *,
    fallback_generated_at: Optional[int] = None,
) -> List[int]:
    """Return the done-story baseline seen by the latest digest generation.

    ``generated_at`` has only second precision, so using ``enriched_at >
    generated_at`` can miss stories completed later in the same second. Store
    an explicit ID baseline in ``meta`` instead. For digest rows created before
    this key existed, fall back to the old timestamp boundary.
    """
    raw = get_meta(conn, f"digest_seen_done_ids:{date}")
    if raw is not None:
        parsed = _json_loads(raw, [])
        if isinstance(parsed, list):
            out: List[int] = []
            for sid in parsed:
                try:
                    out.append(int(sid))
                except (TypeError, ValueError):
                    continue
            return out
        return []
    if fallback_generated_at is None:
        return []
    return done_story_ids_for_digest_date(
        conn, date, max_enriched_at=int(fallback_generated_at)
    )


def set_digest_seen_done_ids(
    conn: sqlite3.Connection, date: str, story_ids: Sequence[int]
) -> None:
    stable_ids = sorted({int(sid) for sid in story_ids})
    set_meta(conn, f"digest_seen_done_ids:{date}", _json_dumps(stable_ids))


def purge_old_digests(conn: sqlite3.Connection, retention_days: int) -> int:
    cutoff = digest_date_minus_days(int(retention_days))
    cursor = conn.execute("DELETE FROM digests WHERE date < ?", (cutoff,))
    return cursor.rowcount or 0


def purge_old_digest_meta(conn: sqlite3.Connection, retention_days: int) -> int:
    cutoff = digest_date_minus_days(int(retention_days))
    prefixes = ("digest_seen_done_ids:", "last_digest_check_at:")
    deleted = 0
    for prefix in prefixes:
        cursor = conn.execute(
            """
            DELETE FROM meta
            WHERE key GLOB ?
              AND substr(key, ?) < ?
            """,
            (prefix + "*", len(prefix) + 1, cutoff),
        )
        deleted += cursor.rowcount or 0
    return deleted


def purge_orphan_topics(conn: sqlite3.Connection, retention_days: int) -> int:
    cutoff = now_seconds() - max(0, int(retention_days)) * 24 * 60 * 60
    cursor = conn.execute(
        """
        DELETE FROM topics
        WHERE last_seen_at < ?
          AND id NOT IN (
            SELECT DISTINCT topic
            FROM stories
            WHERE COALESCE(topic, '') <> ''
          )
        """,
        (cutoff,),
    )
    return cursor.rowcount or 0


# ---------- cleanup ----------

def digest_referenced_story_ids(
    conn: sqlite3.Connection, archive_cutoff_date: str
) -> List[int]:
    """Plan §D: story IDs referenced by digests in the retention window.

    Decoded in Python to stay portable against SQLite builds without JSON1.
    """
    rows = conn.execute(
        "SELECT story_ids FROM digests WHERE date >= ?",
        (archive_cutoff_date,),
    ).fetchall()
    out: List[int] = []
    for r in rows:
        ids = _json_loads(r["story_ids"], [])
        if not isinstance(ids, list):
            continue
        for sid in ids:
            try:
                out.append(int(sid))
            except (TypeError, ValueError):
                continue
    return out


def delete_orphan_stories(
    conn: sqlite3.Connection,
    *,
    grace_seconds: int,
    archive_cutoff_date: str,
) -> int:
    """Plan §D unified deletion rule.

    Caller owns the transaction. Cleanup does not bump ``catalog_version``.

    A row is deleted when ALL of the following hold:
    - not in any active rankings,
    - not referenced by a digest in the retention window,
    - not currently in flight (``enrich_status != 'enriching'``),
    - past the grace window (``last_seen_at < now - grace_seconds``).

    Stale ``pending`` rows ARE eligible. Per A.#2 they correspond to ingest
    rounds that were discarded after fetcher inserted the row but before
    enrichment completed; without this clause they accumulated forever.
    """
    grace_cutoff = now_seconds() - max(int(grace_seconds), 0)
    protected_ids = digest_referenced_story_ids(conn, archive_cutoff_date)
    with _optional_in(conn, protected_ids) as protected_part:
        if protected_part is None:
            protected_clause = ""
            params: Tuple[Any, ...] = (grace_cutoff,)
        else:
            protected_clause = f"AND id NOT {protected_part[0]} "
            params = (*protected_part[1], grace_cutoff)
        sql = f"""
            DELETE FROM stories
            WHERE id NOT IN (SELECT story_id FROM rankings)
              {protected_clause}AND enrich_status != 'enriching'
              AND last_seen_at < ?
            """
        cursor = conn.execute(sql, params)
    return cursor.rowcount or 0


def evict_story_overflow(
    conn: sqlite3.Connection,
    *,
    max_rows: int,
    archive_cutoff_date: str,
) -> int:
    """Enforce a hard cap on the backing ``stories`` table.

    Protected rows are never evicted: active rankings, current staging rows,
    retained digest references, and in-flight enrich jobs. Among unprotected
    rows, lower HN ranking signals are deleted first.
    """
    cap = int(max_rows)
    if cap <= 0:
        return 0
    total_row = conn.execute("SELECT COUNT(*) AS c FROM stories").fetchone()
    total = int(total_row["c"] if total_row else 0)
    overflow = total - cap
    if overflow <= 0:
        return 0

    protected_ids = digest_referenced_story_ids(conn, archive_cutoff_date)
    with _optional_in(conn, protected_ids) as protected_part:
        if protected_part is None:
            protected_clause = ""
            prot_params: Tuple[Any, ...] = ()
        else:
            protected_clause = f"AND id NOT {protected_part[0]} "
            prot_params = tuple(protected_part[1])

        sql = f"""
            DELETE FROM stories
            WHERE id IN (
                SELECT id
                FROM stories
                WHERE id NOT IN (SELECT story_id FROM rankings)
                  AND id NOT IN (SELECT story_id FROM ranking_candidates)
                  {protected_clause}
                  AND enrich_status != 'enriching'
                ORDER BY
                  CASE enrich_status
                    WHEN 'failed' THEN 0
                    WHEN 'pending' THEN 1
                    ELSE 2
                  END ASC,
                  score ASC,
                  descendants ASC,
                  last_seen_at ASC,
                  hn_time ASC,
                  id ASC
                LIMIT ?
            )
        """
        cursor = conn.execute(sql, (*prot_params, overflow))
    return cursor.rowcount or 0


def delete_run_pending_orphans(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    archive_cutoff_date: str,
) -> int:
    """A.#2 surgical cleanup tied to a discarded ingest run.

    Delete unpublished ``pending`` / ``failed`` stories that this ``run_id`` staged into
    ``ranking_candidates`` but nobody else owns: not visible in
    ``rankings``, not referenced by any digest in retention, not staged
    by any other run. Caller owns the transaction.
    """
    candidate_ids = candidate_story_ids(conn, run_id)
    if not candidate_ids:
        return 0

    protected_ids = digest_referenced_story_ids(conn, archive_cutoff_date)
    with id_in_clause(conn, candidate_ids) as (cand_clause, cand_params), \
            _optional_in(conn, protected_ids) as protected_part:
        if protected_part is None:
            protected_clause = ""
            prot_params: Tuple[Any, ...] = ()
        else:
            protected_clause = f"AND id NOT {protected_part[0]} "
            prot_params = tuple(protected_part[1])

        sql = f"""
            DELETE FROM stories
            WHERE id {cand_clause}
              AND enrich_status IN ('pending', 'failed')
              AND id NOT IN (SELECT story_id FROM rankings)
              AND id NOT IN (
                SELECT story_id FROM ranking_candidates WHERE run_id != ?
              )
              {protected_clause}
        """
        params: Tuple[Any, ...] = (*cand_params, run_id, *prot_params)
        cursor = conn.execute(sql, params)
    return cursor.rowcount or 0


__all__ = [
    "id_in_clause",
    "now_seconds",
    "today_in_digest_tz",
    "date_in_digest_tz",
    "digest_date_minus_days",
    "digest_date_epoch_bounds",
    "hash_int_sequence",
    "get_catalog_version",
    "bump_catalog_version",
    "get_meta",
    "set_meta",
    "get_meta_int",
    "row_to_story",
    "ensure_topic",
    "list_known_topics",
    "list_topics",
    "list_active_topics",
    "get_story",
    "list_feed_stories",
    "list_topic_stories",
    "topic_count",
    "latest_digest_row",
    "get_digest_row",
    "digest_story_ids",
    "stories_by_ids",
    "digest_story_ids_matching_date",
    "get_digest",
    "insert_story_pending",
    "update_story_metrics",
    "update_story_kind",
    "story_exists",
    "get_story_basic",
    "replace_feed_ranking",
    "replace_ranking_candidates",
    "candidate_story_ids",
    "candidate_count",
    "count_incomplete_candidates",
    "delete_ranking_candidates",
    "publish_ranking_candidates",
    "feed_story_ids",
    "all_ranked_story_ids",
    "claim_pending_stories",
    "reset_stale_enriching",
    "reset_inflight_enrichment",
    "release_inflight_claims_for_ids",
    "write_enriched_story",
    "mark_enrich_failed",
    "mark_enrich_pending_retry",
    "increment_enrich_attempts",
    "increment_reenrich_attempts",
    "mark_reenrich_retry",
    "mark_reenrich_failed",
    "defer_enrich_retry",
    "defer_reenrich_retry",
    "reset_failed_to_pending",
    "count_enrich_status",
    "count_pending_imminent_failures",
    "get_pipeline_metrics",
    "recent_enrich_errors",
    "start_ingest_run",
    "update_ingest_run",
    "finish_ingest_run",
    "latest_ingest_run",
    "purge_old_ingest_runs",
    "purge_old_cloud_sync_runs",
    "purge_old_ranking_candidates",
    "replace_story_comments",
    "list_story_comments",
    "purge_old_comments",
    "upsert_digest",
    "get_insight_row",
    "list_insight_rows",
    "upsert_insight",
    "insight_needs_update",
    "candidate_rows_for_insights",
    "insight_feed_ranks_for_story_ids",
    "insight_comment_rows_for_story_ids",
    "record_insight_run",
    "candidate_done_stories_for_digest",
    "count_today_done_stories",
    "done_story_ids_for_digest_date",
    "get_digest_seen_done_ids",
    "set_digest_seen_done_ids",
    "purge_old_digests",
    "purge_old_digest_meta",
    "purge_orphan_topics",
    "digest_referenced_story_ids",
    "delete_orphan_stories",
    "evict_story_overflow",
    "delete_run_pending_orphans",
]
