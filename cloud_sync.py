"""Generate cloud read-model + dashboard projection JSON from SQLite.

Reads SQLite via ``repository`` and emits read-model files plus the
dashboard projection into the directory returned by
:func:`settings.get_cloud_sync_output_dir` (defaults to a
``.cloud-sync-output`` sibling of the SQLite DB so writes stay inside the
systemd ``ReadWritePaths``):

    stories.jsonl                    one doc per line, versioned _id = "${v}:${id}"
    topics.jsonl                     versioned _id = "${v}:${topicId}"
    digests.jsonl                    self-contained; versioned _id = "${v}:${date}"
    insights.jsonl                   versioned _id = "${v}:${date}"
    meta.json                        single doc, _id = "catalog"
    dashboard_summary.json           dashboard pipeline snapshot; _id = "summary"
    dashboard_ingest_runs.jsonl      most recent N ingest runs
    dashboard_cloud_sync_runs.jsonl  most recent N cloud sync pushes

Doc shapes match the cloud collection design verified in Step 1 PoC.
This module does NOT transport — JSON files are picked up by
``cloud_push.push_read_model``.

Run::
    python -m server.cloud_sync
"""
from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from . import ai_config_status, db, dashboard_projection, repository, settings
from .schemas import Story, TopicEntry
from .topics import DEFAULT_TOPIC_ID, topic_name_from_id


log = logging.getLogger(__name__)


FEED_TYPES = ("top", "new", "best", "ask", "show", "job")
_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
AI_READY_STORY_SQL = """
    s.enrich_status='done'
    AND s.enriched_at IS NOT NULL
    AND TRIM(COALESCE(s.title_zh, '')) <> ''
    AND TRIM(COALESCE(s.ai_summary, '')) <> ''
    AND TRIM(COALESCE(s.title_zh, '')) <> TRIM(COALESCE(s.title_en, ''))
"""


def _atomic_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.{time.time_ns()}.tmp")


def _write_text_atomic(path: Path, text: str) -> None:
    tmp = _atomic_path(path)
    try:
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _write_jsonl_atomic(path: Path, docs: Iterable[dict]) -> int:
    tmp = _atomic_path(path)
    count = 0
    try:
        with tmp.open("w", encoding="utf-8") as f:
            for doc in docs:
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                count += 1
        tmp.replace(path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
    return count


def default_output_dir() -> Path:
    """Resolve the read-model output directory at call time.

    Resolving at call time (rather than at import) keeps the path in sync
    with ``settings.set_db_path`` overrides used in tests and admin tooling.
    """
    return settings.get_cloud_sync_output_dir()


def _story_default_type(kind: str) -> str:
    """Mirror repository._story_type_from when no feed context is given."""
    if kind in ("ask", "show", "job"):
        return kind
    return "top"


def _story_to_doc(
    story: Story,
    *,
    sync_version: int,
    feed_ranks: Dict[str, int],
    default_type: str,
) -> dict:
    return {
        "_id": f"{sync_version}:{story.id}",
        "id": story.id,
        "syncVersion": sync_version,
        "defaultType": default_type,
        "titleZh": story.titleZh,
        "titleEn": story.titleEn,
        "url": story.url,
        "domain": story.domain,
        "by": story.by,
        "score": story.score,
        "descendants": story.descendants,
        "time": story.time,
        "updatedAt": story.updatedAt,
        "topic": story.topic,
        "aiSummary": story.aiSummary,
        "discussionThemes": [t.model_dump() for t in story.discussionThemes],
        "insights": [i.model_dump() for i in story.insights],
        "terms": [{"term": t.term, "def": t.def_} for t in story.terms],
        "feedRanks": feed_ranks,
        # Used by topicStories to distinguish an in-ranking story from an
        # orphan referenced only by a digest. Mirrors the EXISTS rankings
        # condition in repository.list_topic_stories.
        "inAnyRanking": bool(feed_ranks),
    }


def _client_story_dict_from_story(story: Story) -> dict:
    """Shape the read cloud function will return -- used inside digest docs."""
    return {
        "id": story.id,
        "type": story.type.value,
        "titleZh": story.titleZh,
        "titleEn": story.titleEn,
        "url": story.url,
        "domain": story.domain,
        "by": story.by,
        "score": story.score,
        "descendants": story.descendants,
        "time": story.time,
        "updatedAt": story.updatedAt,
        "topic": story.topic,
        "aiSummary": story.aiSummary,
        "discussionThemes": [t.model_dump() for t in story.discussionThemes],
        "insights": [i.model_dump() for i in story.insights],
        "terms": [{"term": t.term, "def": t.def_} for t in story.terms],
    }


def _story_is_ai_ready(story: Story) -> bool:
    title_zh = (story.titleZh or "").strip()
    title_en = (story.titleEn or "").strip()
    summary = (story.aiSummary or "").strip()
    return bool(
        story.updatedAt
        and title_zh
        and summary
        and title_zh != title_en
        and _CJK_RE.search(title_zh)
        and _CJK_RE.search(summary)
    )


def _collect_feed_ranks(conn) -> Dict[int, Dict[str, int]]:
    rows = conn.execute(
        "SELECT feed, rank, story_id FROM rankings"
    ).fetchall()
    out: Dict[int, Dict[str, int]] = {}
    for r in rows:
        out.setdefault(int(r["story_id"]), {})[r["feed"]] = int(r["rank"])
    return out


def _all_visible_story_ids(conn) -> List[int]:
    """Union of ranked/digest stories that have real AI enrichment output."""
    in_rankings = {
        int(r["id"]) for r in conn.execute(
            "SELECT DISTINCT s.id FROM rankings r JOIN stories s ON s.id=r.story_id "
            f"WHERE {AI_READY_STORY_SQL}"
        ).fetchall()
    }
    in_digests: set[int] = set()
    for r in conn.execute("SELECT story_ids FROM digests").fetchall():
        try:
            ids = json.loads(r["story_ids"])
        except (TypeError, ValueError):
            continue
        if isinstance(ids, list):
            for sid in ids:
                try:
                    in_digests.add(int(sid))
                except (TypeError, ValueError):
                    pass
    candidate = in_rankings | in_digests
    if not candidate:
        return []
    candidate_ids = sorted(int(sid) for sid in candidate)
    with repository.id_in_clause(conn, candidate_ids) as (clause, params):
        rows = conn.execute(
            f"""
            SELECT *
            FROM stories s
            WHERE s.id {clause}
              AND {AI_READY_STORY_SQL}
            """,
            params,
        ).fetchall()
    return sorted(
        int(r["id"])
        for r in rows
        if _story_is_ai_ready(repository.row_to_story(r))
    )


def _build_feed_counts(conn, visible_ids: Iterable[int]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    visible = {int(sid) for sid in visible_ids}
    for feed in FEED_TYPES:
        rows = conn.execute(
            "SELECT story_id FROM rankings WHERE feed=?",
            (feed,),
        ).fetchall()
        out[feed] = sum(1 for row in rows if int(row["story_id"]) in visible)
    return out


def _list_ai_ready_topics(conn, visible_ids: Iterable[int]) -> List[TopicEntry]:
    ids = [int(sid) for sid in visible_ids]
    if not ids:
        return []
    with repository.id_in_clause(conn, ids) as (clause, params):
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
            WHERE s.id {clause}
              AND COALESCE(s.topic, '') != ''
            GROUP BY s.topic, t.name
            HAVING c > 0
            ORDER BY c DESC, last_seen DESC, id ASC
            """,
            params,
        ).fetchall()
    return [
        TopicEntry(
            id=str(row["id"] or DEFAULT_TOPIC_ID),
            name=str(row["name"] or topic_name_from_id(row["id"])),
            count=int(row["c"] or 0),
        )
        for row in rows
    ]


def build_read_model(
    out_dir: Optional[Path] = None,
    *,
    ai_status: Optional[Dict[str, Any]] = None,
    include_dashboard: bool = True,
) -> dict:
    """Build read-model + dashboard projection into ``out_dir``.

    ``ai_status`` is the dashboard-bound AI status that lands in the cloud
    ``hn_dashboard_summary`` document. When ``None`` and ``include_dashboard``
    is true, this calls :func:`ai_config_status.refresh_ai_config_status`
    synchronously so the dashboard snapshot always reflects a probe taken at
    sync cadence rather than whatever happened to be in the disk/in-memory
    cache from a previous round (or worse, ``"unknown"`` after a process
    restart). The probe is bounded by ``_PROBE_TIMEOUT_CAP_SECONDS`` (10s)
    per config; if the refresh fails for any reason we fall back to the pure
    in-memory cached value, so a flaky AI provider can never block the cloud
    push.
    """
    out_dir = out_dir or default_output_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    conn = db.connect_readonly()
    try:
        current_version_str = repository.get_catalog_version(conn) or "0"
        try:
            current_version = int(current_version_str)
        except (TypeError, ValueError):
            current_version = 0
        if current_version <= 0:
            raise RuntimeError(
                "catalog_version is 0; mock DB not initialized? "
                "Run `python -m server.scripts.build_mock_db` first."
            )

        feed_ranks_index = _collect_feed_ranks(conn)
        visible_ids = _all_visible_story_ids(conn)

        # ---------- stories.jsonl ----------
        stories_path = out_dir / "stories.jsonl"
        story_docs = []
        for sid in visible_ids:
            row = conn.execute(
                "SELECT * FROM stories WHERE id=?", (sid,)
            ).fetchone()
            if row is None:
                continue
            story = repository.row_to_story(row, feed=None)
            if not _story_is_ai_ready(story):
                continue
            story_docs.append(_story_to_doc(
                story,
                sync_version=current_version,
                feed_ranks=feed_ranks_index.get(sid, {}),
                default_type=_story_default_type(row["kind"]),
            ))
        story_count = _write_jsonl_atomic(stories_path, story_docs)

        # ---------- topics.jsonl (preserves repository.list_topics order) ----------
        topics = _list_ai_ready_topics(conn, visible_ids)
        topics_path = out_dir / "topics.jsonl"
        topic_docs = [
            {
                "_id": f"{current_version}:{t.id}",
                "id": t.id,
                "syncVersion": current_version,
                "name": t.name,
                "count": t.count,
            }
            for t in topics
        ]
        _write_jsonl_atomic(topics_path, topic_docs)

        # ---------- digests.jsonl (self-contained, versioned) ----------
        # Digest docs are versioned for the same reason stories/topics are:
        # writeBatch can succeed while switchMeta fails. The digest cloud
        # function only reads the doc for meta.currentVersion, so a half-pushed
        # digest never becomes visible early.
        digest_rows = conn.execute(
            "SELECT date FROM digests ORDER BY date"
        ).fetchall()
        digests_path = out_dir / "digests.jsonl"
        digest_docs = []
        for r in digest_rows:
            date, intro, stories = repository.get_digest(conn, r["date"])
            stories = [s for s in stories if _story_is_ai_ready(s)]
            digest_docs.append({
                "_id": f"{current_version}:{date}",
                "syncVersion": current_version,
                "date": date,
                "intro": intro,
                "stories": [_client_story_dict_from_story(s) for s in stories],
            })
        _write_jsonl_atomic(digests_path, digest_docs)

        # ---------- insights.jsonl (self-contained, versioned) ----------
        insight_rows = repository.list_insight_rows(conn)
        insights_path = out_dir / "insights.jsonl"
        insight_docs = []
        for r in insight_rows:
            try:
                payload = json.loads(r["payload"] or "{}")
            except (TypeError, ValueError):
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            date = str(r["date"] or payload.get("date") or "")
            if not date:
                continue
            doc = dict(payload)
            doc["_id"] = f"{current_version}:{date}"
            doc["syncVersion"] = current_version
            doc["date"] = payload.get("date") or date
            doc["version"] = int(payload.get("version") or 1)
            doc.setdefault("access", {"unlocked": True, "tier": "pro"})
            insight_docs.append(doc)
        insight_count = _write_jsonl_atomic(insights_path, insight_docs)

        # ---------- meta.json ----------
        #
        # previousVersion must point at a version number that actually still
        # exists in the cloud -- cleanup uses it as the cutoff and switchMeta
        # uses it as the rollback anchor. Blindly using ``currentVersion - 1``
        # would write a version number the cloud does not have into meta
        # (which can happen after any failed/skipped push), so take the last
        # successfully pushed version from cloud_sync_runs as the
        # authoritative value, falling back to None only on the first push or
        # when the cloud_sync_runs table is empty.
        last_pushed = dashboard_projection.last_successful_cloud_sync_version(conn)
        if last_pushed is not None and last_pushed >= current_version:
            # Current version already pushed (re-running the same catalog) ->
            # there is no "previous version" to clean up.
            previous_version: Optional[int] = None
        elif last_pushed is not None and last_pushed >= 1:
            previous_version = int(last_pushed)
        else:
            previous_version = None

        published_at = int(time.time())
        meta = {
            "_id": "catalog",
            "currentVersion": current_version,
            "previousVersion": previous_version,
            "feedCounts": _build_feed_counts(conn, visible_ids),
            "publishedAt": published_at,
        }
        _write_text_atomic(
            out_dir / "meta.json",
            json.dumps(meta, ensure_ascii=False, indent=2),
        )

        stats = {
            "currentVersion": current_version,
            "previousVersion": previous_version,
            "stories": story_count,
            "topics": len(topics),
            "digests": len(digest_rows),
            "insights": insight_count,
            "feedCounts": meta["feedCounts"],
            "outputDir": str(out_dir),
        }
    finally:
        conn.close()

    if include_dashboard:
        dashboard_status = ai_status
        if dashboard_status is None:
            # Fresh probe at sync cadence — see docstring. We accept ~10s/config
            # on this critical path because dashboard projection is the only
            # window operators have into AI provider health under sync-only.
            try:
                dashboard_status = ai_config_status.refresh_ai_config_status()
            except Exception:  # noqa: BLE001 -- dashboard must not fail just because AI status is unavailable
                log.exception("[cloud-sync] dashboard ai_status refresh failed; falling back to cached")
                # Fallback uses ``cached_ai_config_status`` (pure in-memory read,
                # no schedule_refresh side effect). Otherwise a refresh failure
                # would queue yet another background probe that is just as
                # likely to fail, contradicting the sync-only "no request-path
                # network work" boundary.
                dashboard_status = ai_config_status.cached_ai_config_status()
        dashboard_stats = dashboard_projection.build_dashboard_projection(
            out_dir,
            sync_version=current_version,
            published_at=published_at,
            ai_status=dashboard_status,
            server_time=published_at,
        )
        stats["dashboard"] = dashboard_stats

    return stats


def main() -> None:
    result = build_read_model()
    print("[cloud-sync] wrote read model:")
    print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
