"""Retention and eviction job.

Plan §D unified deletion rule for stories (revised by A.#2):

- not in any rankings
- not referenced by digests in the last DIGEST_RETENTION_DAYS days
- enrich_status != 'enriching'   (in-flight rows stay)
- last_seen_at < now - RANKING_GRACE_SECONDS

Stale ``pending`` rows ARE eligible. Discarded ingest rounds leave behind
orphan pending rows that fetcher inserted before the round failed; this
clause ages them out. Active pending rows are protected by the grace
window because fetcher refreshes ``last_seen_at`` every round.

Plan §D startup guard: read ``meta.last_full_fetch_at``; if missing or
older than ``CLEANUP_STALE_GUARD_SECONDS``, skip the round so a long
Fetcher outage doesn't get interpreted as "everything is stale".

Cleanup does NOT bump ``catalog_version`` because it touches only
client-invisible old data.

The backing ``stories`` table also has a hard row cap. When the cap is
exceeded, cleanup evicts unprotected rows with lower HN ranking signals first.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from . import db, repository, settings


log = logging.getLogger("server.cleanup")


def run_cleanup_once() -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "skipped": False,
        "reason": "",
        "stories_deleted": 0,
        "overflow_stories_deleted": 0,
        "comments_deleted": 0,
        "digests_deleted": 0,
        "digest_meta_deleted": 0,
        "ingest_runs_deleted": 0,
        "cloud_sync_runs_deleted": 0,
        "ranking_candidates_deleted": 0,
        "topics_deleted": 0,
    }

    conn = db.connect()
    try:
        with db.transaction(conn):
            summary["ranking_candidates_deleted"] = (
                repository.purge_old_ranking_candidates(
                    conn, settings.RANKING_CANDIDATE_RETENTION_DAYS
                )
            )
            summary["digest_meta_deleted"] = repository.purge_old_digest_meta(
                conn, settings.DIGEST_RETENTION_DAYS
            )
            summary["ingest_runs_deleted"] = repository.purge_old_ingest_runs(
                conn, settings.INGEST_RUN_RETENTION_DAYS
            )
            summary["cloud_sync_runs_deleted"] = (
                repository.purge_old_cloud_sync_runs(
                    conn, settings.CLOUD_SYNC_RUN_RETENTION_DAYS
                )
            )

        last_full = repository.get_meta_int(conn, "last_full_fetch_at")
        now = repository.now_seconds()
        if last_full is None:
            summary["skipped"] = True
            summary["reason"] = "no_last_full_fetch_at"
            return summary
        if now - last_full > settings.CLEANUP_STALE_GUARD_SECONDS:
            summary["skipped"] = True
            summary["reason"] = "fetcher_stale"
            return summary

        archive_cutoff_date = repository.digest_date_minus_days(
            settings.DIGEST_RETENTION_DAYS
        )

        with db.transaction(conn):
            summary["stories_deleted"] = repository.delete_orphan_stories(
                conn,
                grace_seconds=settings.RANKING_GRACE_SECONDS,
                archive_cutoff_date=archive_cutoff_date,
            )
            summary["overflow_stories_deleted"] = repository.evict_story_overflow(
                conn,
                max_rows=settings.STORY_STORE_MAX_ROWS,
                archive_cutoff_date=archive_cutoff_date,
            )
            summary["stories_deleted"] += summary["overflow_stories_deleted"]
            summary["comments_deleted"] = repository.purge_old_comments(
                conn, settings.COMMENT_RETENTION_DAYS
            )
            summary["digests_deleted"] = repository.purge_old_digests(
                conn, settings.DIGEST_RETENTION_DAYS
            )
            summary["topics_deleted"] = repository.purge_orphan_topics(
                conn, settings.TOPIC_RETENTION_DAYS
            )
    finally:
        conn.close()

    return summary


__all__ = ["run_cleanup_once"]
