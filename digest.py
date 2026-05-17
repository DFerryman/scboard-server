"""Daily digest generator.

Plan §C: rolling updates within a single day, ``Asia/Shanghai`` calendar
date, AI Agent selected story ids, AI-written intro, UPSERT only when
content actually changes.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from . import db, repository, settings


log = logging.getLogger("server.digest")


_DIGEST_MODES = ("auto", "force")


def _stable_unique_ints(values: Sequence[int]) -> List[int]:
    out: List[int] = []
    seen: set[int] = set()
    for value in values:
        sid = int(value)
        if sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
    return out


def prepare_digest_payload(
    ai_agent=None,
    *,
    mode: str = "auto",
    target_ids: Optional[Sequence[int]] = None,
    include_existing_digest: bool = False,
) -> Dict[str, Any]:
    """Prepare today's digest payload without writing it.

    ``mode`` is internal:

    - ``auto`` (default): decide whether to run based on first-time creation,
      incremental new-done eligibility, or timer eligibility.
    - ``force``: skip all gates and recompute candidates; ``upsert_digest``
      still decides whether content actually changed.

    The caller must pass the returned payload to
    :func:`commit_digest_payload` to make it visible.
    """
    if mode not in _DIGEST_MODES:
        raise ValueError(
            f"unknown digest mode {mode!r}; expected one of {_DIGEST_MODES}"
        )

    from .ai_agent import build_ai_agent

    if ai_agent is None:
        ai_agent = build_ai_agent()

    today = repository.today_in_digest_tz()
    summary: Dict[str, Any] = {
        "date": today,
        "candidates": 0,
        "selected": 0,
        "changed": False,
        "skipped": False,
        "reason": "",
        "trigger": None,
        "mode": mode,
        "intro": "",
        "story_ids": [],
        "current_done_ids": [],
    }

    conn = db.connect()
    try:
        scoped_ids: Optional[List[int]]
        if target_ids is None:
            scoped_ids = None
        else:
            scoped_ids = _stable_unique_ints([int(sid) for sid in target_ids])
        if include_existing_digest:
            existing_digest_ids = repository.digest_story_ids(conn, today)
            scoped_ids = _stable_unique_ints(
                [*existing_digest_ids, *(scoped_ids or [])]
            )

        current_done_ids = repository.done_story_ids_for_digest_date(
            conn, today, only_ids=scoped_ids
        )
        done_today = len(current_done_ids)
        summary["candidates"] = done_today
        summary["current_done_ids"] = current_done_ids

        # Skip-early data conditions never advance ``last_digest_check_at`` so
        # the next round can fire immediately once content arrives.
        if done_today <= 0:
            summary["skipped"] = True
            summary["reason"] = "no_done_today"
            return summary

        existing = repository.get_digest_row(conn, today)

        # Plan P1 trigger semantics:
        #
        # - ``force``: caller demanded a recompute; skip gates entirely.
        # - ``auto`` first-time: no row for today => any done story runs the
        #   first generation, regardless of timer / min-new thresholds.
        # - ``auto`` incremental: |current_done - seen_done| crossed
        #   DIGEST_MIN_NEW_DONE_STORIES.
        # - ``auto`` timer: enough wall-time elapsed since the last successful
        #   check that we should at least re-evaluate, even when no new done
        #   stories arrived. ``upsert_digest`` is still the gate for content
        #   changes, so a no-op timer pass leaves catalog_version untouched.
        if mode == "force":
            trigger = "force"
        elif existing is None:
            trigger = "first"
        else:
            seen_done_ids = repository.get_digest_seen_done_ids(
                conn,
                today,
                fallback_generated_at=int(existing["generated_at"]),
            )
            new_since = len(set(current_done_ids) - set(seen_done_ids))
            if new_since >= settings.DIGEST_MIN_NEW_DONE_STORIES:
                trigger = "incremental"
            else:
                last_check = repository.get_meta_int(
                    conn, f"last_digest_check_at:{today}"
                )
                now_s = repository.now_seconds()
                interval = settings.DIGEST_UPDATE_INTERVAL_SECONDS
                if last_check is None or (now_s - int(last_check)) >= interval:
                    trigger = "timer"
                else:
                    summary["skipped"] = True
                    summary["reason"] = "below_min_new_done"
                    return summary

        summary["trigger"] = trigger

        # Cap candidate_limit at DIGEST_MAX_STORIES * 4 even when scoped_ids
        # is larger: ``only_ids`` already restricts the pool, and the
        # underlying query orders by ``score DESC`` then LIMITs, so the AI
        # selector still receives the top-N by score within scope. Expanding
        # the limit to len(scoped) forced the selector to ingest 100+ rows
        # per round (~20k tokens of selection prompt) for no added quality.
        candidate_limit = settings.DIGEST_MAX_STORIES * 4
        candidates = repository.candidate_done_stories_for_digest(
            conn,
            today,
            candidate_limit,
            only_ids=scoped_ids,
        )
        if not candidates:
            summary["skipped"] = True
            summary["reason"] = "no_candidates"
            return summary

        try:
            selected_ids = ai_agent.select_digest_story_ids(
                today,
                candidates,
                settings.DIGEST_MAX_STORIES,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"AI digest story selection failed: {exc}") from exc

        candidate_by_id = {int(r["id"]): r for r in candidates}
        if not selected_ids:
            raise RuntimeError("AI digest story selection returned no story ids")
        if len(selected_ids) > settings.DIGEST_MAX_STORIES:
            raise RuntimeError("AI digest story selection returned too many story ids")
        selected: List[Any] = []
        seen: set[int] = set()
        for raw_id in selected_ids:
            sid = int(raw_id)
            if sid in seen:
                raise RuntimeError(
                    f"AI digest story selection returned duplicate id {sid}"
                )
            row = candidate_by_id.get(sid)
            if row is None:
                raise RuntimeError(
                    f"AI digest story selection returned non-candidate id {sid}"
                )
            seen.add(sid)
            selected.append(row)
        summary["selected"] = len(selected)

        story_ids = [int(r["id"]) for r in selected]
        intro = ai_agent.write_digest_intro(today, selected) or ""
        summary["intro"] = intro
        summary["story_ids"] = story_ids
    finally:
        conn.close()

    return summary


def commit_digest_payload(conn, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Write a prepared digest payload. Caller owns the transaction."""
    summary = dict(payload)
    if summary.get("skipped"):
        return summary

    date = str(summary["date"])
    intro = str(summary.get("intro") or "")
    story_ids = [int(sid) for sid in summary.get("story_ids") or []]
    current_done_ids = [int(sid) for sid in summary.get("current_done_ids") or []]
    changed = repository.upsert_digest(conn, date, intro, story_ids)
    repository.set_digest_seen_done_ids(conn, date, current_done_ids)
    repository.set_meta(conn, f"last_digest_check_at:{date}", str(repository.now_seconds()))
    if changed:
        repository.bump_catalog_version(conn)
        summary["changed"] = True
    else:
        summary["changed"] = False
    return summary


def run_digester_once(ai_agent=None, *, mode: str = "auto") -> Dict[str, Any]:
    """Generate or refresh today's digest."""
    payload = prepare_digest_payload(ai_agent=ai_agent, mode=mode)
    if payload.get("skipped"):
        return payload

    conn = db.connect()
    try:
        with db.transaction(conn):
            return commit_digest_payload(conn, payload)
    finally:
        conn.close()


__all__ = ["prepare_digest_payload", "commit_digest_payload", "run_digester_once"]
