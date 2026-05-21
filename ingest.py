"""Fetcher + Enricher + Digester + Cleanup orchestration and CLI.

Run a single round::

    python -m server.ingest --once

Run only a phase::

    python -m server.ingest --fetch
    python -m server.ingest --enrich
    python -m server.ingest --digest
    python -m server.ingest --cleanup

Maintenance::

    python -m server.ingest --reset-failed
"""

from __future__ import annotations

import argparse
import inspect
import json
import logging
import os
import random
import re
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping as AbcMapping, Sequence as AbcSequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from . import db, normalizer, repository, settings
from .ai_agent import (
    AiCapacityDeferred,
    AiProviderHttpError,
    AiProviderResponseError,
    is_ai_capacity_error,
)
from .hn_client import HnClient, HnFetchError
from .logging_config import configure_logging


log = logging.getLogger("server.ingest")


FEEDS: Sequence[str] = ("top", "new", "best", "ask", "show", "job")
# Higher-priority feed wins when one item appears in several rankings.
_FEED_PRIORITY: Sequence[str] = ("job", "ask", "show", "top", "best", "new")
_ENRICH_AI_USAGE_PURPOSES: Sequence[str] = ("story", "story-batch")
_CJK_CHARS = "\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff"
_LATIN_SOURCE_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+-]{2,}")
_CJK_WITH_LOWERCASE_SUFFIX_RE = re.compile(
    rf"(?P<cjk>[{_CJK_CHARS}])(?P<tail>[a-z]{{2,8}})(?=$|[^A-Za-z])"
)
_LATIN_CJK_GLUE_RE = re.compile(
    rf"(?P<cjk>[{_CJK_CHARS}])(?P<latin>[A-Za-z][A-Za-z0-9_+.-]{{1,}})"
    rf"|(?P<latin2>[A-Za-z][A-Za-z0-9_+.-]{{1,}})(?P<cjk2>[{_CJK_CHARS}])"
)
_ENGLISH_PHRASE_RE = re.compile(r"\b[A-Za-z]+(?:\s+[A-Za-z]+){4,}\b")
_OUTPUT_META_PHRASE_RE = re.compile(
    r"(作为(?:一个|一名)?AI|我(?:无法|不能)|"
    r"由于(?:输入|原文|材料|正文|评论).{0,16}(?:未|没有|无法|不足)|"
    r"仅凭(?:标题|链接|现有信息)|"
    r"(?:原始输入|输入材料|题目|标题)(?:未|没有).{0,12}(?:提供|包含)|"
    r"(?:无法|不能)(?:确认|判断|确定))"
)
_JSON_OR_MARKDOWN_LEAK_RE = re.compile(
    r"```|</?(?:story_title|story_body|comment)\b|"
    r'"(?:titleZh|aiSummary|discussionThemes|insights|terms)"\s*:'
)
_BRACKET_PAIRS = (("（", "）"), ("(", ")"), ("《", "》"), ("“", "”"), ("「", "」"))
_MAX_AI_QUALITY_ISSUES = 8

# Plan P3: kind upgrade priority. ``job`` is the strongest signal (the HN
# item type itself), then explicit ``ask``/``show`` feeds, then plain
# ``story``. We never downgrade an existing more-specific kind.
_KIND_PRIORITY = {"story": 0, "ask": 1, "show": 1, "job": 2}


def _kind_can_supersede(current: str, new: str) -> bool:
    return _KIND_PRIORITY.get(new, 0) > _KIND_PRIORITY.get(current, 0)


def _row_value(row: Mapping[str, Any], key: str, default: Any = "") -> Any:
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key, default)
    try:
        return row[key]
    except (IndexError, KeyError, TypeError):
        return default


def _source_latin_tokens(story_row: Mapping[str, Any]) -> Dict[str, str]:
    """Map lower-case source tokens to their original spelling for QA checks."""
    source = "\n".join(
        str(_row_value(story_row, key, "") or "")
        for key in ("title_en", "raw_text")
    )
    out: Dict[str, str] = {}
    for match in _LATIN_SOURCE_TOKEN_RE.finditer(source):
        token = match.group(0)
        out.setdefault(token.lower(), token)
    return out


def _iter_ai_reader_texts(processed: Mapping[str, Any]) -> Iterable[tuple[str, str]]:
    for key in ("titleZh", "aiSummary"):
        value = processed.get(key)
        if isinstance(value, str) and value:
            yield key, value

    themes = processed.get("discussionThemes")
    if isinstance(themes, AbcSequence) and not isinstance(themes, (str, bytes)):
        for idx, item in enumerate(themes):
            if not isinstance(item, AbcMapping):
                continue
            for key in ("title", "summary"):
                value = item.get(key)
                if isinstance(value, str) and value:
                    yield f"discussionThemes[{idx}].{key}", value

    insights = processed.get("insights")
    if isinstance(insights, AbcSequence) and not isinstance(insights, (str, bytes)):
        for idx, item in enumerate(insights):
            if not isinstance(item, AbcMapping):
                continue
            value = item.get("text")
            if isinstance(value, str) and value:
                yield f"insights[{idx}].text", value

    terms = processed.get("terms")
    if isinstance(terms, AbcSequence) and not isinstance(terms, (str, bytes)):
        for idx, item in enumerate(terms):
            if not isinstance(item, AbcMapping):
                continue
            for key in ("term", "def"):
                value = item.get(key)
                if isinstance(value, str) and value:
                    yield f"terms[{idx}].{key}", value


def _append_quality_issue(issues: List[str], seen: set[str], issue: str) -> bool:
    if issue in seen:
        return len(issues) >= _MAX_AI_QUALITY_ISSUES
    seen.add(issue)
    issues.append(issue)
    return len(issues) >= _MAX_AI_QUALITY_ISSUES


def _text_has_unbalanced_brackets(text: str) -> bool:
    for left, right in _BRACKET_PAIRS:
        if text.count(left) != text.count(right):
            return True
    return False


def _text_has_cjk(text: str) -> bool:
    return re.search(rf"[{_CJK_CHARS}]", text) is not None


def _ai_output_quality_issues(
    story_row: Mapping[str, Any],
    processed: Mapping[str, Any],
) -> List[str]:
    """Detect suspicious reader-facing AI output for reviewer escalation.

    The deterministic layer is intentionally conservative about acting on its
    own: findings trigger a Codex-first reviewer, which can approve false
    positives. The server never edits, shortens, or rewrites generated text.
    """
    source_tokens = _source_latin_tokens(story_row)
    issues: List[str] = []
    seen: set[str] = set()
    for path, text in _iter_ai_reader_texts(processed):
        if "\ufffd" in text or "\x00" in text:
            if _append_quality_issue(
                issues,
                seen,
                f"{path} contains replacement/control characters",
            ):
                return issues

        if _JSON_OR_MARKDOWN_LEAK_RE.search(text):
            if _append_quality_issue(
                issues,
                seen,
                f"{path} appears to leak JSON, markdown fences, or prompt delimiters",
            ):
                return issues

        if _OUTPUT_META_PHRASE_RE.search(text):
            if _append_quality_issue(
                issues,
                seen,
                f"{path} contains reader-facing meta/disclaimer phrasing instead of natural editorial Chinese",
            ):
                return issues

        if _text_has_unbalanced_brackets(text):
            if _append_quality_issue(
                issues,
                seen,
                f"{path} has unbalanced Chinese/English brackets or quotes",
            ):
                return issues

        if _text_has_cjk(text) and _ENGLISH_PHRASE_RE.search(text):
            if _append_quality_issue(
                issues,
                seen,
                f"{path} contains a long untranslated English phrase",
            ):
                return issues

        for match in _LATIN_CJK_GLUE_RE.finditer(text):
            latin = (match.group("latin") or match.group("latin2") or "").strip()
            if not latin or latin.isupper():
                continue
            if _append_quality_issue(
                issues,
                seen,
                f"{path} contains Latin token {latin!r} glued directly to Chinese; review spacing, translation, or proper noun handling",
            ):
                return issues

        if source_tokens:
            for match in _CJK_WITH_LOWERCASE_SUFFIX_RE.finditer(text):
                tail = match.group("tail").lower()
                source_token = ""
                for token_lower, token_original in source_tokens.items():
                    if token_lower != tail and token_lower.endswith(tail):
                        source_token = token_original
                        break
                if not source_token:
                    continue
                if _append_quality_issue(
                    issues,
                    seen,
                    f"{path} contains partial English suffix {tail!r} glued to Chinese, likely a malformed rendering of source token {source_token!r}",
                ):
                    return issues
    return issues


class _LazyAiQualityReviewer:
    def __init__(self) -> None:
        self._lock = Lock()
        self._reviewer: Optional[Any] = None

    def _get(self):
        if self._reviewer is None:
            with self._lock:
                if self._reviewer is None:
                    from .ai_agent import build_ai_quality_reviewer

                    self._reviewer = build_ai_quality_reviewer()
        return self._reviewer

    def review_story_output(
        self,
        story_row: Mapping[str, Any],
        processed: Mapping[str, Any],
        issues: Sequence[str],
    ) -> Mapping[str, Any]:
        return self._get().review_story_output(story_row, processed, issues)


def _repair_sequence_shrunk(
    original: Mapping[str, Any],
    repaired: Mapping[str, Any],
    key: str,
) -> bool:
    original_value = original.get(key)
    repaired_value = repaired.get(key)
    if not isinstance(original_value, AbcSequence) or isinstance(
        original_value, (str, bytes)
    ):
        return False
    if not isinstance(repaired_value, AbcSequence) or isinstance(
        repaired_value, (str, bytes)
    ):
        return False
    return len(repaired_value) < len(original_value)


def _quality_repair_fragment(
    original: Mapping[str, Any],
    repaired: Any,
) -> tuple[Optional[Dict[str, Any]], str]:
    if not isinstance(repaired, AbcMapping):
        return None, f"repair payload must be object, got {type(repaired).__name__}"

    title = repaired.get("titleZh")
    summary = repaired.get("aiSummary")
    if not isinstance(title, str) or not title.strip():
        return None, "repair payload requires non-empty titleZh"
    if not isinstance(summary, str):
        return None, "repair payload requires aiSummary string"
    if isinstance(original.get("aiSummary"), str) and original.get("aiSummary"):
        if not summary.strip():
            return None, "repair payload must not drop a non-empty aiSummary"

    fragment: Dict[str, Any] = {
        "titleZh": title.strip(),
        "aiSummary": summary.strip(),
    }
    for key in ("discussionThemes", "insights", "terms"):
        if _repair_sequence_shrunk(original, repaired, key):
            return None, f"repair payload must not reduce {key} count"
        value = repaired.get(key)
        if not isinstance(value, AbcSequence) or isinstance(value, (str, bytes)):
            return None, f"repair payload requires {key} array"
        for item in value:
            if not isinstance(item, AbcMapping):
                return None, f"repair payload {key} item must be object"
        fragment[key] = list(value)
    return fragment, ""


def _apply_ai_output_quality_review(
    story_row: Mapping[str, Any],
    processed: Mapping[str, Any],
    *,
    quality_reviewer,
) -> tuple[Optional[Dict[str, Any]], str]:
    issues = _ai_output_quality_issues(story_row, processed)
    if not issues:
        return dict(processed), ""
    try:
        decision = quality_reviewer.review_story_output(
            story_row,
            processed,
            issues,
        )
    except Exception as exc:  # noqa: BLE001
        return None, (
            "AI output quality review failed; suspicious result was not "
            f"approved: {type(exc).__name__}: {exc}"
        )
    if not isinstance(decision, AbcMapping):
        return None, (
            "AI output quality review failed; suspicious result was not "
            f"approved: reviewer returned {type(decision).__name__}"
        )
    action = str(decision.get("action") or "").strip().lower()
    approved = bool(decision.get("approved"))
    if approved and action == "approve":
        log.info(
            "AI output quality review approved story %s despite heuristic findings: %s",
            _row_value(story_row, "id", ""),
            decision.get("reason") or "",
        )
        return dict(processed), ""
    if approved and action == "repair":
        fragment, repair_error = _quality_repair_fragment(
            processed,
            decision.get("repaired"),
        )
        if repair_error:
            return None, (
                "AI output quality repair returned invalid payload: "
                + repair_error
                + "; heuristic findings: "
                + "; ".join(issues)
            )
        repaired_processed = dict(processed)
        repaired_processed.update(fragment or {})
        log.info(
            "AI output quality review repaired story %s: %s",
            _row_value(story_row, "id", ""),
            decision.get("reason") or "",
        )
        return repaired_processed, ""
    reason = str(decision.get("reason") or "review rejected suspicious output")
    return None, (
        "AI output quality review rejected result: "
        + reason
        + "; heuristic findings: "
        + "; ".join(issues)
    )


# ---------- Fetcher ----------

def _deadline_reached(deadline_at: Optional[float]) -> bool:
    return deadline_at is not None and time.time() >= float(deadline_at)


def _collect_feed_rankings(
    client,
    *,
    deadline_at: Optional[float] = None,
) -> tuple[Dict[str, List[int]], bool]:
    """A1: pull each feed's top-N IDs.

    A failure on one feed does not skip the others. Plan section A3 line 399 will
    leave that feed's existing rankings untouched.
    """
    feed_ids: Dict[str, List[int]] = {}
    for feed in FEEDS:
        if _deadline_reached(deadline_at):
            for rest in FEEDS:
                feed_ids.setdefault(rest, [])
            return feed_ids, True
        try:
            ids = client.get_ranking(feed)
        except HnFetchError as exc:
            log.warning("ranking %s fetch failed: %s", feed, exc)
            feed_ids[feed] = []
            continue
        except Exception as exc:  # noqa: BLE001
            log.warning("ranking %s fetch raised: %s", feed, exc)
            feed_ids[feed] = []
            continue
        feed_ids[feed] = ids[: settings.FEED_WINDOW_SIZE]
    return feed_ids, False


def _fetch_items(
    client,
    ids: Iterable[int],
    *,
    deadline_at: Optional[float] = None,
) -> tuple[Dict[int, Dict[str, Any]], bool]:
    """A2: fetch each item with per-item retry; one failure must not poison the round."""
    out: Dict[int, Dict[str, Any]] = {}
    for sid in ids:
        if _deadline_reached(deadline_at):
            return out, True
        try:
            item = client.get_item(sid)
        except HnFetchError as exc:
            log.warning("item %s fetch failed: %s", sid, exc)
            continue
        except Exception as exc:  # noqa: BLE001
            log.warning("item %s fetch raised: %s", sid, exc)
            continue
        if item is None:
            continue
        out[sid] = item
    return out, False


def _resolve_source_feed(
    feed_ids: Dict[str, List[int]], story_id: int
) -> Optional[str]:
    """Return the highest-priority feed this ID currently belongs to."""
    for feed in _FEED_PRIORITY:
        if story_id in feed_ids.get(feed, ()):  # type: ignore[operator]
            return feed
    return None


def run_fetcher_once(
    client=None,
    *,
    run_id: Optional[str] = None,
    deadline_at: Optional[float] = None,
) -> Dict[str, Any]:
    """Fetch HN data and stage visible rankings for a run.

    Raw story rows are updated immediately, but client-visible rankings are
    written to ``ranking_candidates``. They become visible only after the
    whole run is enriched and published.
    """
    if client is None:
        client = HnClient()
    if run_id is None:
        run_id = f"manual-{uuid.uuid4().hex}"

    summary: Dict[str, Any] = {
        "run_id": run_id,
        "feeds": {},
        "stories_inserted": 0,
        "stories_updated": 0,
        "stories_skipped": 0,
        "items_fetched": 0,
        "candidate_count": 0,
        "successful_round": False,
        "timed_out": False,
    }

    feed_ids, ranking_timed_out = _collect_feed_rankings(
        client,
        deadline_at=deadline_at,
    )
    summary["timed_out"] = bool(ranking_timed_out)

    conn = db.connect()
    try:
        with db.transaction(conn):
            for feed, ids in feed_ids.items():
                # Plan P3: store ``source_ranking_hash:<feed>`` only when A1
                # returned a non-empty ID list. Empty lists mean the feed
                # request failed; intentionally preserve the previous hash
                # so a transient network blip does not invalidate the
                # ranking-comparison signal.
                if ids:
                    repository.set_meta(
                        conn,
                        f"source_ranking_hash:{feed}",
                        repository.hash_int_sequence(ids),
                    )
    finally:
        conn.close()

    unique_ids: List[int] = []
    seen: set = set()
    for feed in _FEED_PRIORITY:
        for sid in feed_ids.get(feed, []):
            if sid in seen:
                continue
            seen.add(sid)
            unique_ids.append(sid)

    items, item_timed_out = _fetch_items(client, unique_ids, deadline_at=deadline_at)
    summary["timed_out"] = bool(summary["timed_out"] or item_timed_out)
    summary["items_fetched"] = len(items)

    # Plan P0 (A3 exclusion): classify every successfully fetched item once,
    # in A2, so A3 can distinguish "already-present row that is now non-display"
    # (must be removed from rankings) from "temporary item fetch failure"
    # (keep the existing row eligible until cleanup ages it out).
    displayable_ids: set = set()
    fetched_non_display_ids: set = set()

    conn = db.connect()
    try:
        for sid, raw in items.items():
            source_feed = _resolve_source_feed(feed_ids, sid)
            normalized = normalizer.normalize_item(raw, source_feed=source_feed)
            if not normalized:
                fetched_non_display_ids.add(sid)
                summary["stories_skipped"] += 1
                continue

            with db.transaction(conn):
                if not repository.story_exists(conn, sid):
                    inserted = repository.insert_story_pending(conn, normalized)
                    if inserted:
                        summary["stories_inserted"] += 1
                else:
                    repository.update_story_metrics(
                        conn,
                        sid,
                        score=normalized["score"],
                        descendants=normalized["descendants"],
                        last_seen_at=normalized["last_seen_at"],
                        title_en=normalized["title_en"],
                        url=normalized["url"],
                        domain=normalized["domain"],
                        by=normalized["by"],
                        hn_time=normalized["hn_time"],
                        raw_text=normalized["raw_text"],
                        raw_json=normalized["raw_json"],
                        fetched_at=normalized["fetched_at"],
                    )
                    existing = repository.get_story_basic(conn, sid)
                    new_kind = normalized["kind"]
                    if existing is not None and _kind_can_supersede(
                        existing["kind"] or "story", new_kind
                    ):
                        repository.update_story_kind(conn, sid, new_kind)
                    summary["stories_updated"] += 1
            displayable_ids.add(sid)
    finally:
        conn.close()

    any_feed_staged = False
    conn = db.connect()
    try:
        for feed, ids in feed_ids.items():
            feed_summary = {"size": 0, "skipped": False}
            if not ids:
                feed_summary["skipped"] = True
                summary["feeds"][feed] = feed_summary
                continue

            with db.transaction(conn):
                stored_ids: List[int] = []
                for sid in ids:
                    if sid in displayable_ids:
                        stored_ids.append(sid)
                    elif sid in fetched_non_display_ids:
                        # Plan P0: HN now reports this id as deleted/dead or a
                        # non-display type (poll/pollopt/comment). Drop it from
                        # the active ranking even when an old `stories` row
                        # still exists; cleanup.py owns the row deletion once
                        # last_seen_at ages past RANKING_GRACE_SECONDS.
                        continue
                    elif repository.story_exists(conn, sid):
                        # Temporary item fetch failure for an existing story:
                        # keep it staged this round so an HTTP blip does not
                        # blank out the feed after publish.
                        stored_ids.append(sid)
                repository.replace_ranking_candidates(conn, run_id, feed, stored_ids)
                feed_summary["size"] = len(stored_ids)
                any_feed_staged = True
            summary["feeds"][feed] = feed_summary

        if any_feed_staged:
            with db.transaction(conn):
                summary["candidate_count"] = repository.candidate_count(conn, run_id)
            summary["successful_round"] = True
    finally:
        conn.close()

    return summary


# ---------- Enricher ----------

def _row_to_comment_dict(row) -> dict:
    """Cached comment rows must be served to the AI agent in the same dict
    shape as freshly normalized comments."""
    return {
        "id": int(row["id"]),
        "story_id": int(row["story_id"]),
        "parent_id": row["parent_id"],
        "by": row["by"] or "",
        "text": row["text"] or "",
        "hn_time": int(row["hn_time"] or 0),
        "depth": int(row["depth"] or 0),
        "rank": int(row["rank"] or 0),
    }


def _crawl_comments_from_hn(client, story_row) -> List[dict]:
    """Walk the HN kids tree, bounded by ``COMMENT_FETCH_LIMIT`` and
    ``COMMENT_MAX_DEPTH``. The threshold check itself lives in
    :func:`_maybe_fetch_comments`."""
    raw_json = story_row["raw_json"] or "{}"
    try:
        import json as _json

        meta = _json.loads(raw_json)
    except ValueError:
        return []
    kids = meta.get("kids") if isinstance(meta, dict) else None
    if not isinstance(kids, list):
        return []

    out: List[dict] = []
    rank = 0
    fetched_at = repository.now_seconds()
    story_id = int(story_row["id"])

    def _walk(child_ids: List[int], parent_id: Optional[int], depth: int) -> None:
        nonlocal rank
        if depth >= settings.COMMENT_MAX_DEPTH:
            return
        for cid in child_ids:
            if len(out) >= settings.COMMENT_FETCH_LIMIT:
                return
            try:
                child = client.get_item(int(cid))
            except HnFetchError as exc:
                log.debug("comment %s fetch failed: %s", cid, exc)
                continue
            except Exception as exc:  # noqa: BLE001
                log.debug("comment %s fetch raised: %s", cid, exc)
                continue
            normalized = normalizer.normalize_comment(
                child,
                story_id=story_id,
                parent_id=parent_id,
                depth=depth,
                rank=rank,
                fetched_at=fetched_at,
            )
            if normalized:
                out.append(normalized)
                rank += 1
                next_parent = normalized["id"]
            else:
                next_parent = parent_id
            child_kids = child.get("kids") if isinstance(child, dict) else None
            if isinstance(child_kids, list):
                _walk(child_kids, parent_id=next_parent, depth=depth + 1)

    _walk(kids, parent_id=story_id, depth=0)
    return out


def _maybe_fetch_comments(client, conn, story_row):
    """Resolve the comment payload for AI enrichment.

    Plan section: Comment fetch policy + Plan P1 cache reuse:

    - Stories with ``descendants < COMMENT_MIN_DESCENDANTS`` return ``[]``
      without consulting the cache. The threshold owns the early exit.
    - When cached comments exist, reuse them so failed AI retries do not
      re-crawl HN. Returns ``freshly_fetched=False`` and the caller skips
      ``replace_story_comments``.
    - Otherwise crawl HN and return ``freshly_fetched=True``; the caller
      persists the result.

    Note on the plan wording: "pending" in the plan means "selected from the
    enrichment queue before AI processing." By the time this function runs
    the row is technically ``enrich_status='enriching'``.

    Returns ``(comments, freshly_fetched)``.
    """
    descendants = int(story_row["descendants"] or 0)
    if descendants < settings.COMMENT_MIN_DESCENDANTS:
        return [], False
    sid = int(story_row["id"])
    cached_rows = repository.list_story_comments(
        conn, sid, limit=settings.COMMENT_FETCH_LIMIT
    )
    is_done_refresh = (
        story_row["enrich_status"] == "done"
        and int(story_row["needs_reenrich"] or 0) == 1
    )
    cache_matches_source = (
        int(story_row["comments_fetched_descendants"] or 0) >= descendants
    )
    if cached_rows and (not is_done_refresh or cache_matches_source):
        return [_row_to_comment_dict(r) for r in cached_rows], False
    return _crawl_comments_from_hn(client, story_row), True


_CAPACITY_DEFER_BASE_SECONDS = 30
_CAPACITY_DEFER_JITTER_SECONDS = 15


def _is_capacity_deferred_exception(exc: Exception) -> bool:
    """Whether ``exc`` should park a row instead of bumping enrich_attempts."""
    return is_ai_capacity_error(exc)


def _iter_exception_chain(exc: BaseException) -> Iterable[BaseException]:
    seen: set[int] = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        cur = cur.__cause__ or cur.__context__


def _provider_http_error_from_exception(exc: Exception) -> Optional[AiProviderHttpError]:
    """Find the provider HTTP error hidden behind wrapper RuntimeErrors."""
    for cur in _iter_exception_chain(exc):
        if isinstance(cur, AiProviderHttpError):
            return cur
    return None


def _capacity_error_from_exception(exc: Exception) -> Optional[Exception]:
    """Return the capacity-class exception in ``exc``'s chain, if any."""
    for cur in _iter_exception_chain(exc):
        if isinstance(cur, Exception) and _is_capacity_deferred_exception(cur):
            return cur
    return None


def _is_nonrecoverable_provider_batch_error(exc: Exception) -> bool:
    """Whether retrying this failed batch as N singles is just request spam."""
    http_error = _provider_http_error_from_exception(exc)
    if http_error is None:
        return False
    if _is_capacity_deferred_exception(http_error):
        return False
    status_code = int(http_error.status_code)
    if status_code in (401, 403, 404):
        return True
    detail = " ".join(
        part
        for part in (
            getattr(http_error, "provider_error_code", ""),
            getattr(http_error, "provider_error_type", ""),
            str(http_error),
        )
        if part
    ).lower()
    if status_code == 400 and any(
        marker in detail
        for marker in (
            "model_not_found",
            "model not found",
            "modelnotfound",
            "model_not_supported",
            "unsupported model",
            "does not support access",
        )
    ):
        return True
    return False


def _capacity_retry_after_seconds(exc: Exception) -> int:
    """Pick a wait-before-reclaim window for a capacity-deferred row.

    ``Retry-After`` from the provider (when present) lifts the floor; we
    add a random jitter so a wave of deferred rows doesn't all wake up on
    the same tick and stampede the provider that's just out of cooldown.
    """
    base = _CAPACITY_DEFER_BASE_SECONDS
    hint: Optional[float] = None
    if isinstance(exc, AiCapacityDeferred):
        hint = exc.retry_after_seconds
    elif isinstance(exc, AiProviderHttpError):
        hint = exc.retry_after_seconds
    if hint is not None and hint > base:
        base = int(hint)
    return base + random.randint(0, _CAPACITY_DEFER_JITTER_SECONDS)


def _defer_work_item_in_tx(
    conn,
    summary: Dict[str, Any],
    item: Mapping[str, Any],
    *,
    retry_after_seconds: int,
    error_msg: str,
) -> None:
    """Park ONE work item; caller already holds an active write transaction."""
    sid = int(item["story"]["id"])
    deadline = repository.now_seconds() + max(0, int(retry_after_seconds))
    if item["is_refresh"]:
        repository.defer_reenrich_retry(
            conn, sid, retry_after=deadline, error=error_msg
        )
    else:
        repository.defer_enrich_retry(
            conn, sid, retry_after=deadline, error=error_msg
        )
    summary["deferred"] = int(summary.get("deferred", 0) or 0) + 1


def _record_enrich_failure(
    conn,
    summary: Dict[str, Any],
    story_row,
    *,
    is_refresh: bool,
    error_msg: str,
    bump_visible_version: bool,
    final: bool = False,
) -> None:
    sid = int(story_row["id"])
    if is_refresh:
        attempts = repository.increment_reenrich_attempts(conn, sid)
        if final or attempts >= settings.ENRICH_MAX_ATTEMPTS:
            repository.mark_reenrich_failed(conn, sid, error=error_msg)
            summary["failed"] += 1
        else:
            repository.mark_reenrich_retry(conn, sid, error=error_msg)
            summary["retried"] += 1
        return

    attempts = repository.increment_enrich_attempts(conn, sid)
    if final or attempts >= settings.ENRICH_MAX_ATTEMPTS:
        visible_changed = repository.mark_enrich_failed(
            conn,
            sid,
            title_en=story_row["title_en"] or "",
            error=error_msg,
        )
        if visible_changed and bump_visible_version:
            repository.bump_catalog_version(conn)
        summary["failed"] += 1
    else:
        repository.mark_enrich_pending_retry(conn, sid, error=error_msg)
        summary["retried"] += 1


def _record_batch_provider_failure(
    conn,
    summary: Dict[str, Any],
    items: Sequence[Mapping[str, Any]],
    *,
    error_msg: str,
    bump_visible_version: bool,
) -> None:
    """Record one failed attempt per item without issuing duplicate AI calls."""
    for item in items:
        _record_enrich_failure(
            conn,
            summary,
            item["story"],
            is_refresh=bool(item.get("is_refresh")),
            error_msg=error_msg,
            bump_visible_version=bump_visible_version,
        )


def _write_enriched_result(
    conn,
    summary: Dict[str, Any],
    story_row,
    processed: Dict[str, Any],
    *,
    comments_snapshot_descendants: int,
    bump_visible_version: bool,
) -> None:
    sid = int(story_row["id"])
    repository.write_enriched_story(
        conn,
        sid,
        title_zh=processed.get("titleZh") or story_row["title_en"] or "",
        topic=processed.get("topic") or "general",
        topic_name=processed.get("topicName"),
        ai_summary=processed.get("aiSummary") or "",
        discussion_themes=processed.get("discussionThemes") or [],
        insights=processed.get("insights") or [],
        terms=processed.get("terms") or [],
        comments_fetched_descendants=comments_snapshot_descendants,
    )
    if bump_visible_version:
        repository.bump_catalog_version(conn)
    summary["done"] += 1


def _normalize_batch_results(raw: Any) -> Dict[int, Optional[Dict[str, Any]]]:
    if not isinstance(raw, dict):
        raise ValueError("batch agent must return a dict keyed by story id")
    out: Dict[int, Optional[Dict[str, Any]]] = {}
    for key, value in raw.items():
        out[int(key)] = value
    return out


def _accepts_positional_arg(fn: Any, count: int) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    positional = 0
    for param in sig.parameters.values():
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            return True
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
    return positional >= count


def _call_process_story(ai_agent, story_row, comments, topic_catalog):
    fn = ai_agent.process_story
    if _accepts_positional_arg(fn, 3):
        return fn(story_row, comments, topic_catalog)
    return fn(story_row, comments)


def _call_process_stories_batch(ai_agent, work_items, topic_catalog):
    fn = ai_agent.process_stories_batch
    if _accepts_positional_arg(fn, 2):
        return fn(work_items, topic_catalog)
    return fn(work_items)


def _resolve_enrich_batch_size(ai_agent) -> int:
    configured = max(1, int(settings.ENRICH_BATCH_SIZE))
    fn = getattr(ai_agent, "recommended_enrich_batch_size", None)
    if not callable(fn):
        return configured
    try:
        recommended = int(fn(configured))
    except Exception as exc:  # noqa: BLE001
        log.warning("AI batch size recommendation failed: %s", exc)
        return configured
    return max(1, min(configured, recommended))


def _enrich_work_item_single(
    conn,
    summary: Dict[str, Any],
    ai_agent,
    item: Mapping[str, Any],
    topic_catalog,
    quality_reviewer,
    *,
    bump_visible_version: bool,
) -> None:
    result = _process_work_item_single_unlocked(
        ai_agent,
        item,
        topic_catalog,
        quality_reviewer,
    )
    _apply_single_work_item_result(
        conn,
        summary,
        item,
        result,
        bump_visible_version=bump_visible_version,
    )


def _process_work_item_single_unlocked(
    ai_agent,
    item: Mapping[str, Any],
    topic_catalog,
    quality_reviewer,
) -> Dict[str, Any]:
    """Call AI for one item without holding a SQLite write transaction."""
    story_row = item["story"]
    sid = int(story_row["id"])
    try:
        processed = _call_process_story(
            ai_agent,
            story_row,
            item["comments"],
            topic_catalog,
        )
    except Exception as exc:  # noqa: BLE001
        capacity_exc = _capacity_error_from_exception(exc)
        if capacity_exc is not None:
            error_msg = f"{type(capacity_exc).__name__}: {capacity_exc}"
            log.info(
                "ai process_story(%d) deferred: %s",
                sid,
                error_msg,
            )
            return {
                "status": "deferred",
                "error_msg": error_msg,
                "retry_after_seconds": _capacity_retry_after_seconds(capacity_exc),
            }
        log.warning("ai process_story(%d) failed: %s", sid, exc)
        return {
            "status": "failed",
            "error_msg": f"{type(exc).__name__}: {exc}",
        }

    if processed is None:
        return {"status": "failed", "error_msg": "ai agent returned None"}
    processed, quality_error = _apply_ai_output_quality_review(
        story_row,
        processed,
        quality_reviewer=quality_reviewer,
    )
    if quality_error:
        log.warning(
            "ai process_story(%d) quality review failed closed: %s",
            sid,
            quality_error,
        )
        return {
            "status": "failed",
            "error_msg": quality_error,
            "final_failure": True,
        }
    return {"status": "done", "processed": processed}


def _apply_single_work_item_result(
    conn,
    summary: Dict[str, Any],
    item: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    bump_visible_version: bool,
) -> None:
    """Persist one AI result while the caller holds a write transaction."""
    story_row = item["story"]
    status = str(result.get("status") or "")
    if status == "deferred":
        _defer_work_item_in_tx(
            conn,
            summary,
            item,
            retry_after_seconds=int(result.get("retry_after_seconds") or 0),
            error_msg=str(result.get("error_msg") or ""),
        )
        return

    if status == "failed":
        _record_enrich_failure(
            conn,
            summary,
            story_row,
            is_refresh=bool(item["is_refresh"]),
            error_msg=str(result.get("error_msg") or "ai agent returned None"),
            bump_visible_version=bump_visible_version,
            final=bool(result.get("final_failure")),
        )
        return

    _write_enriched_result(
        conn,
        summary,
        story_row,
        result["processed"],
        comments_snapshot_descendants=int(item["comments_fetched_descendants"]),
        bump_visible_version=bump_visible_version,
    )


def _release_remaining_work_items(
    conn,
    summary: Dict[str, Any],
    items: Sequence[Mapping[str, Any]],
) -> None:
    """Hand back ``enriching`` claims for items we won't get to this round."""
    if not items:
        return
    remaining_ids = [int(item["story"]["id"]) for item in items]
    with db.transaction(conn):
        released = repository.release_inflight_claims_for_ids(conn, remaining_ids)
    summary["timed_out"] = True
    summary["released_on_timeout"] = int(
        summary.get("released_on_timeout", 0) or 0
    ) + int(released.get("pending_released", 0)) + int(
        released.get("refresh_released", 0)
    )


def _fallback_to_singles(
    conn,
    summary: Dict[str, Any],
    ai_agent,
    items: Sequence[Mapping[str, Any]],
    topic_catalog,
    quality_reviewer,
    *,
    bump_visible_version: bool,
    deadline_at: Optional[float],
) -> bool:
    """Run each item via :func:`_enrich_work_item_single`.

    Each iteration uses its own write transaction so a single bad row can't
    roll back successful neighbors. Returns False when the deadline aborts
    before the loop finishes.
    """
    for index, item in enumerate(items):
        if deadline_at is not None and time.time() >= deadline_at:
            _release_remaining_work_items(conn, summary, items[index:])
            return False
        result = _process_work_item_single_unlocked(
            ai_agent,
            item,
            topic_catalog,
            quality_reviewer,
        )
        with db.transaction(conn):
            _apply_single_work_item_result(
                conn,
                summary,
                item,
                result,
                bump_visible_version=bump_visible_version,
            )
    return True


def _process_work_items(
    conn,
    summary: Dict[str, Any],
    ai_agent,
    work_items: List[Dict[str, Any]],
    topic_catalog,
    quality_reviewer,
    *,
    bump_visible_version: bool,
    deadline_at: Optional[float],
) -> bool:
    """Run AI enrichment for ``work_items``.

    - Empty input or deadline-already-passed: release remaining claims, return.
    - Single work item: ``_enrich_work_item_single`` (records usage as the
      "story" step; capacity-deferred handling lives there).
    - Multi-item batch: dispatch to ``process_stories_batch``. Main outcomes:
      - Capacity-deferred (rate limited, cooled, or quota/balance exhausted):
        park *every* item with ``enrich_retry_after`` set, **without**
        bumping ``enrich_attempts``, so quota incidents don't promote stories
        to ``failed`` after a handful of retries.
      - Provider response JSON errors: bisect the batch, then bottom out at
        the single-story path so only the actual bad rows burn attempts.
      - Nonrecoverable provider HTTP errors (auth/access/model-not-found):
        record one failed attempt per item without re-sending duplicate
        single-story calls.
      - Any other failure: fall back to single-story for each item so one
        bad batch does not poison successful neighbors.
    - On batch success: write resolved items, retry any missing ids as
      singles (covers the "model silently dropped a result" partial case).

    Returns False when the round deadline aborted processing.
    """
    if not work_items:
        return True

    if deadline_at is not None and time.time() >= deadline_at:
        _release_remaining_work_items(conn, summary, work_items)
        return False

    if len(work_items) == 1:
        result = _process_work_item_single_unlocked(
            ai_agent,
            work_items[0],
            topic_catalog,
            quality_reviewer,
        )
        with db.transaction(conn):
            _apply_single_work_item_result(
                conn,
                summary,
                work_items[0],
                result,
                bump_visible_version=bump_visible_version,
            )
        return True

    try:
        processed_by_id = _normalize_batch_results(
            _call_process_stories_batch(ai_agent, work_items, topic_catalog)
        )
    except AiCapacityDeferred as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        log.info(
            "ai batch (%d items) deferred: %s",
            len(work_items),
            error_msg,
        )
        with db.transaction(conn):
            retry_after = _capacity_retry_after_seconds(exc)
            for item in work_items:
                _defer_work_item_in_tx(
                    conn,
                    summary,
                    item,
                    retry_after_seconds=retry_after,
                    error_msg=error_msg,
                )
        return True
    except AiProviderHttpError as exc:
        capacity_exc = _capacity_error_from_exception(exc)
        if capacity_exc is not None:
            error_msg = f"{type(capacity_exc).__name__}: {capacity_exc}"
            log.info(
                "ai batch (%d items) deferred (HTTP %d): %s",
                len(work_items),
                exc.status_code,
                error_msg,
            )
            with db.transaction(conn):
                retry_after = _capacity_retry_after_seconds(capacity_exc)
                for item in work_items:
                    _defer_work_item_in_tx(
                        conn,
                        summary,
                        item,
                        retry_after_seconds=retry_after,
                        error_msg=error_msg,
                    )
            return True
        if _is_nonrecoverable_provider_batch_error(exc):
            error_msg = f"{type(exc).__name__}: {exc}"
            log.warning(
                "ai batch failed for %d stories with nonrecoverable provider HTTP %d; "
                "recording batch failure without single-story retry: %s",
                len(work_items),
                exc.status_code,
                error_msg,
            )
            with db.transaction(conn):
                _record_batch_provider_failure(
                    conn,
                    summary,
                    work_items,
                    error_msg=error_msg,
                    bump_visible_version=bump_visible_version,
                )
            return True
        log.warning(
            "ai batch failed for %d stories; falling back to single-story enrich: %s: %s",
            len(work_items),
            type(exc).__name__,
            exc,
        )
        return _fallback_to_singles(
            conn,
            summary,
            ai_agent,
            work_items,
            topic_catalog,
            quality_reviewer,
            bump_visible_version=bump_visible_version,
            deadline_at=deadline_at,
        )
    except AiProviderResponseError as exc:
        # Every provider returned malformed JSON for this batch. Bisect:
        # smaller inputs often parse cleanly because batch responses can be
        # truncated by ``max_tokens`` ceilings or cause models to drop into
        # degraded JSON paths under load. Falling out to N singles would
        # work too but burns N x cost on a content shape that may already be
        # near a token limit.
        if len(work_items) <= 1:
            log.warning(
                "ai batch (size 1) malformed JSON; falling back to single-story enrich: %s",
                exc,
            )
            return _fallback_to_singles(
                conn,
                summary,
                ai_agent,
                work_items,
                topic_catalog,
                quality_reviewer,
                bump_visible_version=bump_visible_version,
                deadline_at=deadline_at,
            )
        mid = len(work_items) // 2
        log.info(
            "ai batch (%d items) malformed JSON across all providers; bisecting to %d+%d: %s",
            len(work_items),
            mid,
            len(work_items) - mid,
            exc,
        )
        if not _process_work_items(
            conn,
            summary,
            ai_agent,
            work_items[:mid],
            topic_catalog,
            quality_reviewer,
            bump_visible_version=bump_visible_version,
            deadline_at=deadline_at,
        ):
            _release_remaining_work_items(conn, summary, work_items[mid:])
            return False
        return _process_work_items(
            conn,
            summary,
            ai_agent,
            work_items[mid:],
            topic_catalog,
            quality_reviewer,
            bump_visible_version=bump_visible_version,
            deadline_at=deadline_at,
        )
    except Exception as exc:  # noqa: BLE001
        capacity_exc = _capacity_error_from_exception(exc)
        if capacity_exc is not None:
            http_error = _provider_http_error_from_exception(capacity_exc)
            error_msg = f"{type(capacity_exc).__name__}: {capacity_exc}"
            if http_error is not None:
                log.info(
                    "ai batch (%d items) deferred (HTTP %d): %s",
                    len(work_items),
                    http_error.status_code,
                    error_msg,
                )
            else:
                log.info(
                    "ai batch (%d items) deferred: %s",
                    len(work_items),
                    error_msg,
                )
            with db.transaction(conn):
                retry_after = _capacity_retry_after_seconds(capacity_exc)
                for item in work_items:
                    _defer_work_item_in_tx(
                        conn,
                        summary,
                        item,
                        retry_after_seconds=retry_after,
                        error_msg=error_msg,
                    )
            return True
        if _is_nonrecoverable_provider_batch_error(exc):
            http_error = _provider_http_error_from_exception(exc)
            status_code = http_error.status_code if http_error is not None else 0
            error_msg = f"{type(exc).__name__}: {exc}"
            log.warning(
                "ai batch failed for %d stories with nonrecoverable provider HTTP %d; "
                "recording batch failure without single-story retry: %s",
                len(work_items),
                status_code,
                error_msg,
            )
            with db.transaction(conn):
                _record_batch_provider_failure(
                    conn,
                    summary,
                    work_items,
                    error_msg=error_msg,
                    bump_visible_version=bump_visible_version,
                )
            return True
        log.warning(
            "ai batch failed for %d stories; falling back to single-story enrich: %s: %s",
            len(work_items),
            type(exc).__name__,
            exc,
        )
        return _fallback_to_singles(
            conn,
            summary,
            ai_agent,
            work_items,
            topic_catalog,
            quality_reviewer,
            bump_visible_version=bump_visible_version,
            deadline_at=deadline_at,
        )

    resolved_items: List[Dict[str, Any]] = []
    resolved_results: Dict[int, Dict[str, Any]] = {}
    missing_items: List[Dict[str, Any]] = []
    quality_failed_results: List[tuple[Dict[str, Any], str]] = []
    for item in work_items:
        sid = int(item["story"]["id"])
        processed = processed_by_id.get(sid)
        if processed is None:
            missing_items.append(item)
            continue

        reviewed, quality_error = _apply_ai_output_quality_review(
            item["story"],
            processed,
            quality_reviewer=quality_reviewer,
        )
        if quality_error:
            log.warning(
                "ai batch item %d quality review failed closed without single retry: %s",
                sid,
                quality_error,
            )
            quality_failed_results.append((item, quality_error))
        else:
            resolved_items.append(item)
            resolved_results[sid] = reviewed or processed

    if resolved_items:
        with db.transaction(conn):
            for item in resolved_items:
                story_row = item["story"]
                sid = int(story_row["id"])
                _write_enriched_result(
                    conn,
                    summary,
                    story_row,
                    resolved_results[sid],
                    comments_snapshot_descendants=int(
                        item["comments_fetched_descendants"]
                    ),
                    bump_visible_version=bump_visible_version,
                )

    if quality_failed_results:
        with db.transaction(conn):
            for item, quality_error in quality_failed_results:
                _record_enrich_failure(
                    conn,
                    summary,
                    item["story"],
                    is_refresh=bool(item.get("is_refresh")),
                    error_msg=quality_error,
                    bump_visible_version=bump_visible_version,
                    final=True,
                )

    if missing_items:
        log.info(
            "ai batch returned %d/%d accepted stories; retrying %d missing "
            "as singles and recording %d quality failures without retry",
            len(resolved_items),
            len(work_items),
            len(missing_items),
            len(quality_failed_results),
        )
        return _fallback_to_singles(
            conn,
            summary,
            ai_agent,
            missing_items,
            topic_catalog,
            quality_reviewer,
            bump_visible_version=bump_visible_version,
            deadline_at=deadline_at,
        )
    return True


def _enrich_claimed_rows_batch(
    client,
    ai_agent,
    claimed_rows,
    *,
    quality_reviewer,
    bump_visible_version: bool = True,
    deadline_at: Optional[float] = None,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "claimed": len(claimed_rows),
        "done": 0,
        "failed": 0,
        "retried": 0,
        "deferred": 0,
        "timed_out": False,
        "released_on_timeout": 0,
    }
    if not claimed_rows:
        return summary

    work_items: List[Dict[str, Any]] = []
    conn = db.connect()
    try:
        topic_catalog = repository.list_active_topics(conn)
        for index, story_row in enumerate(claimed_rows):
            if deadline_at is not None and time.time() >= deadline_at:
                remaining_ids = [int(r["id"]) for r in claimed_rows[index:]]
                with db.transaction(conn):
                    released = repository.release_inflight_claims_for_ids(
                        conn, remaining_ids
                    )
                summary["timed_out"] = True
                summary["released_on_timeout"] = int(
                    released.get("pending_released", 0)
                ) + int(released.get("refresh_released", 0))
                return summary

            sid = int(story_row["id"])
            is_refresh = (
                story_row["enrich_status"] == "done"
                and int(story_row["needs_reenrich"] or 0) == 1
            )
            try:
                comments, freshly_fetched = _maybe_fetch_comments(
                    client, conn, story_row
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("comment fetch for %d raised: %s", sid, exc)
                comments, freshly_fetched = [], False

            comments_snapshot_descendants = int(story_row["descendants"] or 0)
            with db.transaction(conn):
                if freshly_fetched:
                    repository.replace_story_comments(
                        conn,
                        sid,
                        comments,
                        fetched_descendants=comments_snapshot_descendants,
                    )
            work_items.append(
                {
                    "story": story_row,
                    "comments": comments,
                    "is_refresh": is_refresh,
                    "comments_fetched_descendants": comments_snapshot_descendants,
                }
            )

        _process_work_items(
            conn,
            summary,
            ai_agent,
            work_items,
            topic_catalog,
            quality_reviewer,
            bump_visible_version=bump_visible_version,
            deadline_at=deadline_at,
        )
    finally:
        conn.close()

    return summary


def _enrich_claimed_rows(
    client,
    ai_agent,
    claimed_rows,
    *,
    quality_reviewer=None,
    bump_visible_version: bool = True,
    deadline_at: Optional[float] = None,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "claimed": len(claimed_rows),
        "done": 0,
        "failed": 0,
        "retried": 0,
        "deferred": 0,
        "timed_out": False,
        "released_on_timeout": 0,
    }
    if not claimed_rows:
        return summary
    if quality_reviewer is None:
        quality_reviewer = _LazyAiQualityReviewer()

    if bool(getattr(ai_agent, "supports_batch_enrich", False)):
        batch_size = _resolve_enrich_batch_size(ai_agent)
        batch_summary: Dict[str, Any] = {
            "claimed": 0,
            "done": 0,
            "failed": 0,
            "retried": 0,
            "deferred": 0,
            "timed_out": False,
            "released_on_timeout": 0,
        }
        for start in range(0, len(claimed_rows), batch_size):
            if deadline_at is not None and time.time() >= deadline_at:
                remaining_ids = [int(r["id"]) for r in claimed_rows[start:]]
                conn = db.connect()
                try:
                    with db.transaction(conn):
                        released = repository.release_inflight_claims_for_ids(
                            conn, remaining_ids
                        )
                finally:
                    conn.close()
                batch_summary["timed_out"] = True
                batch_summary["released_on_timeout"] = int(
                    batch_summary.get("released_on_timeout", 0) or 0
                ) + int(released.get("pending_released", 0)) + int(
                    released.get("refresh_released", 0)
                )
                return batch_summary

            end = min(len(claimed_rows), start + batch_size)
            part = _enrich_claimed_rows_batch(
                client,
                ai_agent,
                claimed_rows[start:end],
                quality_reviewer=quality_reviewer,
                bump_visible_version=bump_visible_version,
                deadline_at=deadline_at,
            )
            _merge_enrich_summary(batch_summary, part)
            if part.get("timed_out"):
                remaining_ids = [int(r["id"]) for r in claimed_rows[end:]]
                if remaining_ids:
                    conn = db.connect()
                    try:
                        with db.transaction(conn):
                            released = repository.release_inflight_claims_for_ids(
                                conn, remaining_ids
                            )
                    finally:
                        conn.close()
                    batch_summary["released_on_timeout"] = int(
                        batch_summary.get("released_on_timeout", 0) or 0
                    ) + int(released.get("pending_released", 0)) + int(
                        released.get("refresh_released", 0)
                    )
                return batch_summary
        return batch_summary

    conn = db.connect()
    try:
        topic_catalog = repository.list_active_topics(conn)
        for index, story_row in enumerate(claimed_rows):
            # C.#4: per-story deadline check. If we've already burned the
            # round budget before reaching this row, hand any unprocessed
            # claims back to ``pending`` instead of letting them sit in
            # ``enriching`` until ENRICH_STALE_SECONDS elapses.
            if deadline_at is not None and time.time() >= deadline_at:
                remaining_ids = [int(r["id"]) for r in claimed_rows[index:]]
                with db.transaction(conn):
                    released = repository.release_inflight_claims_for_ids(
                        conn, remaining_ids
                    )
                summary["timed_out"] = True
                summary["released_on_timeout"] = int(
                    released.get("pending_released", 0)
                ) + int(released.get("refresh_released", 0))
                return summary
            sid = int(story_row["id"])
            is_refresh = (
                story_row["enrich_status"] == "done"
                and int(story_row["needs_reenrich"] or 0) == 1
            )

            comments: List[dict]
            freshly_fetched = False
            try:
                comments, freshly_fetched = _maybe_fetch_comments(
                    client, conn, story_row
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("comment fetch for %d raised: %s", sid, exc)
                comments, freshly_fetched = [], False

            comments_snapshot_descendants = int(
                story_row["descendants"] or 0
            )
            with db.transaction(conn):
                if freshly_fetched:
                    # Stamp the descendants snapshot inline so a subsequent
                    # AI retry sees the cache as fresh. Without this stamp,
                    # ``comments_fetched_descendants`` only advances on AI
                    # success, and any retry would re-crawl HN for the same
                    # comments -- defeating the cache reuse contract.
                    repository.replace_story_comments(
                        conn,
                        sid,
                        comments,
                        fetched_descendants=comments_snapshot_descendants,
                    )

            last_error: Optional[str] = None
            try:
                processed = _call_process_story(
                    ai_agent,
                    story_row,
                    comments,
                    topic_catalog,
                )
            except Exception as exc:  # noqa: BLE001
                if _is_capacity_deferred_exception(exc):
                    error_msg = f"{type(exc).__name__}: {exc}"
                    log.info(
                        "ai process_story(%d) deferred: %s",
                        sid,
                        error_msg,
                    )
                    item_for_defer = {
                        "story": story_row,
                        "is_refresh": is_refresh,
                    }
                    with db.transaction(conn):
                        _defer_work_item_in_tx(
                            conn,
                            summary,
                            item_for_defer,
                            retry_after_seconds=_capacity_retry_after_seconds(exc),
                            error_msg=error_msg,
                        )
                    continue
                log.warning("ai process_story(%d) failed: %s", sid, exc)
                processed = None
                last_error = f"{type(exc).__name__}: {exc}"

            if processed is None:
                error_msg = last_error or "ai agent returned None"
                with db.transaction(conn):
                    if is_refresh:
                        attempts = repository.increment_reenrich_attempts(
                            conn, sid
                        )
                        if attempts >= settings.ENRICH_MAX_ATTEMPTS:
                            repository.mark_reenrich_failed(
                                conn, sid, error=error_msg
                            )
                            summary["failed"] += 1
                        else:
                            repository.mark_reenrich_retry(
                                conn, sid, error=error_msg
                            )
                            summary["retried"] += 1
                    else:
                        attempts = repository.increment_enrich_attempts(conn, sid)
                        if attempts >= settings.ENRICH_MAX_ATTEMPTS:
                            visible_changed = repository.mark_enrich_failed(
                                conn,
                                sid,
                                title_en=story_row["title_en"] or "",
                                error=error_msg,
                            )
                            if visible_changed and bump_visible_version:
                                repository.bump_catalog_version(conn)
                            summary["failed"] += 1
                        else:
                            repository.mark_enrich_pending_retry(
                                conn, sid, error=error_msg
                            )
                            summary["retried"] += 1
                continue

            processed, quality_error = _apply_ai_output_quality_review(
                story_row,
                processed,
                quality_reviewer=quality_reviewer,
            )
            if quality_error:
                log.warning(
                    "ai process_story(%d) quality review failed closed: %s",
                    sid,
                    quality_error,
                )
                with db.transaction(conn):
                    _record_enrich_failure(
                        conn,
                        summary,
                        story_row,
                        is_refresh=is_refresh,
                        error_msg=quality_error,
                        bump_visible_version=bump_visible_version,
                        final=True,
                    )
                continue

            with db.transaction(conn):
                repository.write_enriched_story(
                    conn,
                    sid,
                    title_zh=processed.get("titleZh") or story_row["title_en"] or "",
                    topic=processed.get("topic") or "general",
                    topic_name=processed.get("topicName"),
                    ai_summary=processed.get("aiSummary") or "",
                    discussion_themes=processed.get("discussionThemes") or [],
                    insights=processed.get("insights") or [],
                    terms=processed.get("terms") or [],
                    comments_fetched_descendants=comments_snapshot_descendants,
                )
                if bump_visible_version:
                    repository.bump_catalog_version(conn)
                summary["done"] += 1
    finally:
        conn.close()

    return summary


def _merge_enrich_summary(target: Dict[str, Any], part: Dict[str, Any]) -> None:
    for key in (
        "claimed",
        "done",
        "failed",
        "retried",
        "deferred",
        "released_on_timeout",
    ):
        target[key] = int(target.get(key, 0) or 0) + int(part.get(key, 0) or 0)
    if part.get("timed_out"):
        target["timed_out"] = True


def _ai_usage_checkpoint(ai_agent) -> Optional[Any]:
    fn = getattr(ai_agent, "usage_checkpoint", None)
    if not callable(fn):
        return None
    try:
        checkpoint = fn()
        return checkpoint if checkpoint is not None else None
    except Exception as exc:  # noqa: BLE001
        log.warning("ai usage checkpoint failed: %s", exc)
        return None


def _finalize_enrich_summary(
    summary: Dict[str, Any],
    ai_agent,
    usage_checkpoint: Optional[Any],
) -> Dict[str, Any]:
    if usage_checkpoint is None:
        return summary
    fn = getattr(ai_agent, "usage_summary_since", None)
    if not callable(fn):
        return summary
    try:
        _, usage = fn(usage_checkpoint, purposes=_ENRICH_AI_USAGE_PURPOSES)
    except Exception as exc:  # noqa: BLE001
        log.warning("ai usage summary failed: %s", exc)
        return summary
    if usage:
        summary["ai_usage"] = usage
    return summary


def _publish_ranking_checkpoint(
    run_id: str,
    *,
    preserve_existing: bool,
) -> Dict[str, Any]:
    conn = db.connect()
    try:
        with db.transaction(conn):
            return repository.publish_ranking_candidates(
                conn,
                run_id,
                FEEDS,
                preserve_existing=preserve_existing,
            )
    finally:
        conn.close()


def _commit_digest_checkpoint(
    *,
    ai_agent,
    target_ids: Sequence[int],
    mode: str = "force",
) -> Dict[str, Any]:
    from .digest import commit_digest_payload, prepare_digest_payload

    try:
        payload = prepare_digest_payload(
            ai_agent=ai_agent,
            mode=mode,
            target_ids=target_ids,
            include_existing_digest=True,
        )
        if payload.get("skipped"):
            return payload
        conn = db.connect()
        try:
            with db.transaction(conn):
                return commit_digest_payload(conn, payload)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        log.warning("digest checkpoint failed: %s", exc)
        return {
            "skipped": True,
            "reason": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "mode": mode,
        }


def run_enricher_once(
    client=None,
    ai_agent=None,
    *,
    quality_reviewer=None,
    deadline_at: Optional[float] = None,
    max_waves: Optional[int] = None,
    target_ids: Optional[Sequence[int]] = None,
    bump_visible_version: bool = True,
    publish_run_id: Optional[str] = None,
    publish_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """Drain current AI work in bounded concurrent chunks."""
    from .ai_agent import build_ai_agent

    if ai_agent is None:
        ai_agent = build_ai_agent()
    if quality_reviewer is None:
        quality_reviewer = _LazyAiQualityReviewer()

    summary: Dict[str, Any] = {
        "claimed": 0,
        "done": 0,
        "failed": 0,
        "retried": 0,
        "deferred": 0,
        "waves": 0,
        "timed_out": False,
        "publish_checkpoints": [],
    }
    usage_checkpoint = _ai_usage_checkpoint(ai_agent)

    worker_count = max(1, int(settings.ENRICH_WORKER_COUNT))
    session_limit = max(1, int(settings.ENRICH_SESSION_STORY_LIMIT))

    attempted_ids: set[int] = set()
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        while True:
            if deadline_at is not None and time.time() >= deadline_at:
                summary["timed_out"] = True
                return _finalize_enrich_summary(summary, ai_agent, usage_checkpoint)
            if max_waves is not None and summary["waves"] >= max(0, int(max_waves)):
                return _finalize_enrich_summary(summary, ai_agent, usage_checkpoint)

            stale_before = repository.now_seconds() - settings.ENRICH_STALE_SECONDS
            chunks: List[List[Any]] = []
            conn = db.connect()
            try:
                with db.transaction(conn):
                    repository.reset_stale_enriching(conn, stale_before)

                for _ in range(worker_count):
                    with db.transaction(conn):
                        claimed_rows = repository.claim_pending_stories(
                            conn,
                            session_limit,
                            stale_before,
                            exclude_ids=attempted_ids,
                            target_ids=target_ids,
                        )
                    if claimed_rows:
                        chunks.append(claimed_rows)
                        attempted_ids.update(int(r["id"]) for r in claimed_rows)
            finally:
                conn.close()

            if not chunks:
                return _finalize_enrich_summary(summary, ai_agent, usage_checkpoint)

            summary["waves"] += 1
            futures = [
                executor.submit(
                    _enrich_claimed_rows,
                    client if client is not None else HnClient(),
                    ai_agent,
                    chunk,
                    quality_reviewer=quality_reviewer,
                    bump_visible_version=bump_visible_version,
                    deadline_at=deadline_at,
                )
                for chunk in chunks
            ]
            for future in as_completed(futures):
                _merge_enrich_summary(summary, future.result())
                _finalize_enrich_summary(summary, ai_agent, usage_checkpoint)
                if progress_callback is not None:
                    try:
                        progress_callback(dict(summary))
                    except Exception as exc:  # noqa: BLE001
                        log.warning("enrich progress callback failed: %s", exc)
                if publish_run_id is not None:
                    publish_summary = _publish_ranking_checkpoint(
                        publish_run_id,
                        preserve_existing=True,
                    )
                    summary["publish_checkpoints"].append(publish_summary)
                    if publish_callback is not None:
                        publish_callback(publish_summary)
            if deadline_at is not None and time.time() >= deadline_at:
                summary["timed_out"] = True
                return _finalize_enrich_summary(summary, ai_agent, usage_checkpoint)
            # If any chunk hit the deadline mid-flight, stop launching new waves.
            if summary.get("timed_out"):
                return _finalize_enrich_summary(summary, ai_agent, usage_checkpoint)

# ---------- CLI ----------

def _setup_logging(verbose: bool) -> None:
    configure_logging(verbose=verbose)


def _maintenance_reset_failed() -> int:
    conn = db.connect()
    try:
        with db.transaction(conn):
            n = repository.reset_failed_to_pending(conn)
        return n
    finally:
        conn.close()


def _new_run_id() -> str:
    return time.strftime("%Y%m%d%H%M%S", time.localtime()) + "-" + uuid.uuid4().hex[:8]


def _alert(
    event_type: str,
    subject: str,
    message: str,
    *,
    run_id: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        from .notifications import send_admin_alert

        settings.refresh_ai_settings_from_env_files()
        ai_model_label = (
            "configured-list"
            if (settings.AI_CONFIGS_JSON or "").strip()
            else settings.AI_MODEL or ""
        )
        fields: Dict[str, Any] = {
            "ai_model": ai_model_label,
            "ai_provider": settings.AI_PROVIDER or "",
        }
        if run_id:
            fields["run_id"] = run_id
        if extra:
            fields.update(extra)

        conn = db.connect()
        try:
            metrics = repository.get_pipeline_metrics(conn)
            fields["enrich_status_counts"] = json.dumps(
                metrics.get("enrich_status_counts", {}), ensure_ascii=False
            )
            fields["recent_enrich_errors"] = json.dumps(
                repository.recent_enrich_errors(conn, limit=5),
                ensure_ascii=False,
            )
        finally:
            conn.close()

        send_admin_alert(event_type, subject, message, fields=fields)
    except Exception as exc:  # noqa: BLE001
        log.exception("admin alert %s failed: %s", event_type, exc)


def _round_alert_extra(
    *,
    started_at: float,
    candidate_count: int = 0,
    target_ids: Optional[Sequence[int]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "started_at": int(started_at),
        "elapsed_seconds": round(time.time() - started_at, 1),
        "candidate_count": int(candidate_count),
    }
    if target_ids is not None:
        out["target_count"] = len(target_ids)
    if extra:
        out.update(extra)
    return out


def _alert_cloud_sync_result(run_id: str, result: Dict[str, Any]) -> None:
    status = str(result.get("status") or "")
    if status in ("", "ok", "skipped"):
        return
    if status == "deferred":
        event_type = "cloud_sync_deferred"
        subject = "HN cloud sync deferred"
    elif status == "warning":
        event_type = "cloud_sync_warning"
        subject = "HN cloud sync degraded"
    else:
        event_type = "cloud_sync_failed"
        subject = "HN cloud sync failed"
    message = str(result.get("error") or status)
    _alert(
        event_type,
        subject,
        message,
        run_id=run_id,
        extra={
            "cloud_sync_status": status,
            "cloud_sync_version": result.get("sync_version"),
            "cloud_sync_elapsed_seconds": result.get("elapsed_seconds"),
            "cloud_sync_timeout_seconds": settings.CLOUD_SYNC_TIMEOUT_SECONDS,
            "ingest_interval_seconds": settings.INGEST_INTERVAL_SECONDS,
            "ingest_round_timeout_seconds": settings.INGEST_ROUND_TIMEOUT_SECONDS,
            "cloud_sync_error": message,
        },
    )


def _finish_run(run_id: str, status: str, *, error: str = "") -> None:
    conn = db.connect()
    try:
        with db.transaction(conn):
            repository.finish_ingest_run(conn, run_id, status=status, error=error)
    finally:
        conn.close()


# Dashboard publish is the second phase after the business publish: if the
# remaining budget is below this value, skip it and let the business publish
# stand on its own as ok (the local cloud_sync_runs row is downgraded to
# warning). 10 seconds is aligned with cloud_push._MIN_PER_CALL_SECONDS to
# guarantee the dashboard HTTP call gets at least one full attempt.
_MIN_DASHBOARD_BUDGET_SECONDS = 10


def _trigger_and_record_cloud_sync(
    run_id: str,
    *,
    deadline_at: Optional[float] = None,
) -> Dict[str, Any]:
    """Run one cloud sync (build read model -> push to the cloud database) and
    write the cloud_sync_runs table.

    No failure is ever re-raised (double-protected by the try-except inside the
    runner plus the catch-all here), so the main ingest flow is never blocked by
    cloud sync problems. The returned fields are referenced by the ingest summary.

    ``deadline_at`` is the supervisor-imposed wall-time deadline for the whole
    round. Cloud sync only runs if enough budget remains for at least one full
    ``CLOUD_SYNC_TIMEOUT_SECONDS`` window plus a safety margin; otherwise the
    run is recorded with ``status='deferred'`` so we don't burn the round's
    remaining time and get killed mid-push.
    """
    if not settings.CLOUD_SYNC_ENABLED:
        return {
            "status": "skipped",
            "sync_version": None,
            "elapsed_seconds": 0.0,
            "error": None,
        }

    safety_margin = max(
        10, int(settings.INGEST_CHILD_KILL_GRACE_SECONDS)
    )
    per_call_timeout = int(settings.CLOUD_SYNC_TIMEOUT_SECONDS)
    if deadline_at is not None:
        remaining = float(deadline_at) - time.time()
        if remaining - safety_margin < per_call_timeout:
            # Skip rather than start a push that the supervisor would kill
            # mid-flight, marking the whole round as 'timeout' and losing
            # the publish that already succeeded.
            log.warning(
                "[cloud_sync] deferring run_id=%s: only %.1fs remain (need >=%ds + %ds safety)",
                run_id, remaining, per_call_timeout, safety_margin,
            )
            result = {
                "status": "deferred",
                "sync_version": None,
                "elapsed_seconds": 0.0,
                "error": (
                    f"insufficient round budget: {remaining:.1f}s remain, "
                    f"need >={per_call_timeout + safety_margin}s"
                ),
            }
            now = repository.now_seconds()
            _write_cloud_sync_run(
                run_id,
                started_at=now,
                finished_at=now,
                status="deferred",
                sync_version=None,
                push_stats={},
                elapsed_seconds=0.0,
                error=result["error"],
            )
            _alert_cloud_sync_result(run_id, result)
            return result

    from . import cloud_sync_runner  # lazy import to avoid a startup-time circular dependency

    # Reserve the safety margin so cloud_push aborts at a phase boundary
    # rather than racing the supervisor's kill grace period.
    cloud_deadline_at: Optional[float] = None
    if deadline_at is not None:
        cloud_deadline_at = float(deadline_at) - float(safety_margin)

    started_at = int(time.time())

    # Write a ``running`` row before pushing so the dashboard projection can
    # also see the in-flight push; otherwise, if the runner crashes and we miss
    # the update below, this push would not exist in the table at all and ops
    # would assume it never happened.
    _write_cloud_sync_run(
        run_id,
        started_at=started_at,
        finished_at=None,
        status="running",
        sync_version=None,
        push_stats={},
        elapsed_seconds=0.0,
        error=None,
    )

    # ---------- Phase A: business publish (stories/topics/digests + switchMeta) ----------
    try:
        business = cloud_sync_runner.run_business_once(
            run_id=run_id,
            timeout_seconds=per_call_timeout,
            deadline_at=cloud_deadline_at,
        )
    except Exception as exc:  # noqa: BLE001
        # the runner is designed to swallow all exceptions; this is belt-and-suspenders
        log.exception("[cloud_sync] runner.run_business_once raised unexpectedly")
        finished_at = int(time.time())
        elapsed = round(finished_at - started_at, 2)
        crash_error = f"runner crashed: {type(exc).__name__}: {exc}"
        _write_cloud_sync_run(
            run_id,
            started_at=started_at,
            finished_at=finished_at,
            status="failed",
            sync_version=None,
            push_stats={},
            elapsed_seconds=elapsed,
            error=crash_error,
            update_running=True,
        )
        result = {
            "status": "failed",
            "sync_version": None,
            "elapsed_seconds": elapsed,
            "error": crash_error,
        }
        _alert_cloud_sync_result(run_id, result)
        return result

    business_finished_at = int(time.time())
    business_elapsed = round(business.elapsed_seconds, 2)

    # Persist the business terminal state to the table first -- critical: this
    # must complete before build_dashboard_projection, so the dashboard
    # projection reads this round's cloud_sync_runs as a terminal state rather
    # than running, and the ops dashboard no longer lags a full round behind.
    record_ok = _write_cloud_sync_run(
        run_id,
        started_at=started_at,
        finished_at=business_finished_at,
        status=business.status,
        sync_version=business.sync_version,
        push_stats=business.push_stats,
        elapsed_seconds=business_elapsed,
        error=business.error,
        update_running=True,
    )

    if not record_ok and business.status == "ok":
        # Business push succeeded but the local table write failed -- we must
        # not proceed to the dashboard phase. Without a record, the next
        # build_read_model would read the wrong previousVersion and treat this
        # successful push as if it never happened. Downgrade to warning so ops
        # takes notice.
        warning_error = (
            (business.error + " | " if business.error else "")
            + "cloud_sync_runs record write failed"
        )
        result = {
            "status": "warning",
            "sync_version": business.sync_version,
            "elapsed_seconds": business_elapsed,
            "error": warning_error,
        }
        _alert_cloud_sync_result(run_id, result)
        return result

    if business.status != "ok":
        result = {
            "status": business.status,
            "sync_version": business.sync_version,
            "elapsed_seconds": business_elapsed,
            "error": business.error,
        }
        _alert_cloud_sync_result(run_id, result)
        return result

    if business.push_stats.get("businessSkipped"):
        log.info(
            "[cloud_sync] dashboard skipped run_id=%s: business version unchanged",
            run_id,
        )
        return {
            "status": "ok",
            "sync_version": business.sync_version,
            "elapsed_seconds": business_elapsed,
            "error": None,
        }

    # ---------- Phase B: dashboard publish (only when business is ok) ----------
    # The business push has already succeeded and been recorded; publishing the
    # dashboard next is best-effort: a failure does not roll back the business
    # publish, it only downgrades the local cloud_sync_runs from ok to warning,
    # and the next round's dashboard projection backfills it automatically.

    def _downgrade_to_warning(error_text: str) -> Dict[str, Any]:
        # The warning path is called when the dashboard phase fails or is skipped.
        # finished_at and elapsed_seconds both use the dashboard wrap-up time so
        # the "start / end / elapsed" triplet in the local cloud_sync_runs table
        # is self-consistent and reflects the total business + dashboard cost
        # rather than only the business segment.
        now = int(time.time())
        elapsed_total = round(now - started_at, 2)
        _write_cloud_sync_run(
            run_id,
            started_at=started_at,
            finished_at=now,
            status="warning",
            sync_version=business.sync_version,
            push_stats=business.push_stats,
            elapsed_seconds=elapsed_total,
            error=error_text,
            update_running=True,
        )
        result = {
            "status": "warning",
            "sync_version": business.sync_version,
            "elapsed_seconds": elapsed_total,
            "error": error_text,
        }
        _alert_cloud_sync_result(run_id, result)
        return result

    if business.sync_version is None or business.published_at is None:
        # Defensive: a business ok must carry sync_version + published_at, otherwise the runner has a bug
        warning_error = "business ok but sync_version/published_at missing"
        log.error("[cloud_sync] %s", warning_error)
        return _downgrade_to_warning(warning_error)

    if cloud_deadline_at is not None:
        dashboard_remaining = float(cloud_deadline_at) - time.time()
        if dashboard_remaining < _MIN_DASHBOARD_BUDGET_SECONDS:
            log.warning(
                "[cloud_sync] dashboard publish skipped run_id=%s: only %.1fs remain",
                run_id, dashboard_remaining,
            )
            return _downgrade_to_warning(
                f"business ok; dashboard skipped (only {dashboard_remaining:.1f}s remain)"
            )

    try:
        dashboard = cloud_sync_runner.run_dashboard_once(
            run_id=run_id,
            sync_version=business.sync_version,
            published_at=business.published_at,
            timeout_seconds=per_call_timeout,
            deadline_at=cloud_deadline_at,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("[cloud_sync] runner.run_dashboard_once raised unexpectedly")
        return _downgrade_to_warning(
            f"business ok; dashboard crashed: {type(exc).__name__}: {exc}"
        )

    if not dashboard.ok:
        return _downgrade_to_warning(
            f"business ok; dashboard publish failed: {dashboard.error or 'unknown'}"
        )

    # Everything succeeded -- the business UPDATE wrote elapsed_seconds for the
    # business segment only; UPDATE once more so elapsed reflects the total
    # business + dashboard cost, advancing finished_at to the moment the
    # dashboard completed and keeping the time fields self-consistent.
    now_ok = int(time.time())
    elapsed_total = round(now_ok - started_at, 2)
    _write_cloud_sync_run(
        run_id,
        started_at=started_at,
        finished_at=now_ok,
        status="ok",
        sync_version=business.sync_version,
        push_stats=business.push_stats,
        elapsed_seconds=elapsed_total,
        error=None,
        update_running=True,
    )
    return {
        "status": "ok",
        "sync_version": business.sync_version,
        "elapsed_seconds": elapsed_total,
        "error": None,
    }


def _derive_cleanup_status(push_stats: Optional[Mapping[str, Any]]) -> Optional[str]:
    """Map cloud_push.push_read_model's ``cleanup`` sub-result into a short tag.

    push_stats["cleanup"] takes one of these shapes (from cloud_push.py):

      - ``{"skipped": True}``         initial value, cleanup never reached
      - ``{"skipped": "deadline"}``   deadline gate skipped the call
      - ``{"ok": True, ...}``         cloud function reported success
      - ``{"ok": False, "error":x}``  cloud function reported failure

    Anything else (push_stats empty, business push didn't run, malformed
    cleanup dict) maps to ``None`` so the table column stays NULL and
    operators can distinguish "wasn't tried" from "tried and skipped".
    The failure tag is truncated to keep a single column readable in the
    dashboard.
    """
    if not isinstance(push_stats, Mapping):
        return None
    cleanup = push_stats.get("cleanup")
    if not isinstance(cleanup, Mapping):
        return None
    skipped = cleanup.get("skipped")
    if skipped == "deadline":
        return "skipped:deadline"
    if skipped is True:
        return "skipped:initial"
    ok = cleanup.get("ok")
    if ok is True:
        return "ok"
    if ok is False:
        reason = cleanup.get("error") or cleanup.get("message") or "unknown"
        tag = f"failed:{reason}"
        return tag[:500]
    return None


def _write_cloud_sync_run(
    run_id: str,
    *,
    started_at: int,
    finished_at: Optional[int],
    status: str,
    sync_version: Optional[int],
    push_stats: Dict[str, Any],
    elapsed_seconds: float,
    error: Optional[str],
    update_running: bool = False,
) -> bool:
    """Write/update a row in ``cloud_sync_runs``.

    ``update_running`` makes the post-push UPDATE first try to update the
    ``running`` row written during the INSERT phase; if that row does not exist
    (older DB / table upgrade not yet applied), it falls back to an INSERT. When
    the table itself does not exist (old DB that never ran the init_db upgrade),
    it logs a warning and swallows the error -- the main flow should not be
    blocked by a monitoring write. Returns True when this push was actually
    persisted to the table.
    """
    cleanup_status = _derive_cleanup_status(push_stats)
    try:
        conn = db.connect()
        try:
            with db.transaction(conn):
                if update_running:
                    cur = conn.execute(
                        """
                        UPDATE cloud_sync_runs
                        SET finished_at=?, status=?, sync_version=?,
                            stories=?, topics=?, digests=?, insights=?,
                            elapsed_seconds=?, error=?, cleanup_status=?
                        WHERE run_id=? AND started_at=?
                        """,
                        (
                            finished_at,
                            status,
                            sync_version,
                            push_stats.get("stories"),
                            push_stats.get("topics"),
                            push_stats.get("digests"),
                            push_stats.get("insights"),
                            elapsed_seconds,
                            (error or None),
                            cleanup_status,
                            run_id,
                            started_at,
                        ),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        return True
                conn.execute(
                    """
                    INSERT INTO cloud_sync_runs
                        (run_id, started_at, finished_at, status, sync_version,
                         stories, topics, digests, insights, elapsed_seconds, error,
                         cleanup_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        started_at,
                        finished_at,
                        status,
                        sync_version,
                        push_stats.get("stories"),
                        push_stats.get("topics"),
                        push_stats.get("digests"),
                        push_stats.get("insights"),
                        elapsed_seconds,
                        (error or None),
                        cleanup_status,
                    ),
                )
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        log.exception("[cloud_sync] failed to record run into cloud_sync_runs")
        return False
    return True


def _discard_run(
    run_id: str,
    *,
    release_inflight: bool = False,
    delete_pending_orphans: bool = True,
) -> dict:
    archive_cutoff_date = repository.digest_date_minus_days(
        settings.DIGEST_RETENTION_DAYS
    )
    conn = db.connect()
    try:
        with db.transaction(conn):
            released = (
                repository.reset_inflight_enrichment(conn, run_id)
                if release_inflight
                else {"pending_released": 0, "refresh_released": 0}
            )
            # Capture row count up-front because deleting the orphan stories
            # below will cascade through the FK on ranking_candidates.story_id
            # and zero out delete_ranking_candidates' rowcount.
            initial_row = conn.execute(
                "SELECT COUNT(*) AS c FROM ranking_candidates WHERE run_id=?",
                (run_id,),
            ).fetchone()
            initial_candidate_rows = int(initial_row["c"] if initial_row else 0)
            # Identify orphan pending rows via this run's ranking_candidates
            # BEFORE the explicit candidate delete drops them.
            orphan_deleted = (
                repository.delete_run_pending_orphans(
                    conn, run_id, archive_cutoff_date=archive_cutoff_date
                )
                if delete_pending_orphans
                else 0
            )
            repository.delete_ranking_candidates(conn, run_id)
        released["candidates_deleted"] = initial_candidate_rows
        released["orphan_stories_deleted"] = int(orphan_deleted)
        return released
    finally:
        conn.close()


def _update_run_enrich_progress(run_id: str, enrich_summary: Dict[str, Any]) -> None:
    conn = db.connect()
    try:
        with db.transaction(conn):
            repository.update_ingest_run(
                conn,
                run_id,
                claimed=int(enrich_summary.get("claimed", 0) or 0),
                done=int(enrich_summary.get("done", 0) or 0),
                failed=int(enrich_summary.get("failed", 0) or 0),
                retried=int(enrich_summary.get("retried", 0) or 0),
            )
    finally:
        conn.close()


def _record_run_ai_usage_snapshot(
    run_id: str,
    ai_agent,
    checkpoint: Optional[Any],
) -> None:
    """Refresh ingest_runs.ai_usage with cumulative usage since ``checkpoint``.

    No purpose filter -- every AI call made during the round (enrich, digest,
    digest-selection) is counted, so the dashboard's tokens/cost columns
    reflect the whole round in real time.
    """
    if checkpoint is None or ai_agent is None:
        return
    fn = getattr(ai_agent, "usage_summary_since", None)
    if not callable(fn):
        return
    try:
        _, usage = fn(checkpoint)
    except Exception as exc:  # noqa: BLE001
        log.warning("ai usage snapshot failed: %s", exc)
        return
    if not usage:
        return
    conn = db.connect()
    try:
        with db.transaction(conn):
            repository.update_ingest_run(conn, run_id, ai_usage=usage)
    finally:
        conn.close()


def _compact_digest_for_log(
    digest_summary: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not isinstance(digest_summary, Mapping):
        return None
    out: Dict[str, Any] = {}
    for key in (
        "date",
        "candidates",
        "selected",
        "changed",
        "skipped",
        "reason",
        "trigger",
        "mode",
        "error",
    ):
        if key in digest_summary:
            out[key] = digest_summary.get(key)
    story_ids = digest_summary.get("story_ids")
    if isinstance(story_ids, Sequence) and not isinstance(story_ids, (str, bytes)):
        out["story_ids"] = list(story_ids)[:10]
        out["story_ids_count"] = len(story_ids)
    current_done_ids = digest_summary.get("current_done_ids")
    if isinstance(current_done_ids, Sequence) and not isinstance(
        current_done_ids, (str, bytes)
    ):
        out["current_done_ids_count"] = len(current_done_ids)
    return out


def _compact_enrich_for_log(
    enrich_summary: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not isinstance(enrich_summary, Mapping):
        return None
    out = {
        key: enrich_summary.get(key)
        for key in (
            "claimed",
            "done",
            "failed",
            "retried",
            "deferred",
            "waves",
            "timed_out",
            "released_on_timeout",
        )
        if key in enrich_summary
    }
    checkpoints = enrich_summary.get("publish_checkpoints")
    if isinstance(checkpoints, Sequence) and not isinstance(
        checkpoints, (str, bytes)
    ):
        out["publish_checkpoints_count"] = len(checkpoints)
        if checkpoints:
            out["last_publish_checkpoint"] = checkpoints[-1]
    ai_usage = enrich_summary.get("ai_usage")
    if isinstance(ai_usage, Mapping):
        out["ai_usage"] = {
            key: ai_usage.get(key)
            for key in (
                "requests",
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "unpriced_tokens",
            )
            if key in ai_usage
        }
    return out


def _compact_publish_for_log(
    publish_summary: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not isinstance(publish_summary, Mapping):
        return None
    out = {
        key: publish_summary.get(key)
        for key in (
            "changed",
            "published_count",
            "ready_count",
            "preserve_existing",
            "skipped_stale_run",
        )
        if key in publish_summary
    }
    feeds = publish_summary.get("feeds")
    if isinstance(feeds, Mapping):
        out["feeds"] = {
            str(feed): {
                key: summary.get(key)
                for key in ("size", "ready", "changed", "skipped")
                if isinstance(summary, Mapping) and key in summary
            }
            for feed, summary in feeds.items()
        }
    return out


def _compact_round_summary_for_log(summary: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "run_id": summary.get("run_id"),
        "status": summary.get("status"),
        "error": summary.get("error"),
        "fetch": summary.get("fetch"),
        "enrich": _compact_enrich_for_log(summary.get("enrich")),
        "digest": _compact_digest_for_log(summary.get("digest")),
        "publish": _compact_publish_for_log(summary.get("publish")),
        "cleanup": summary.get("cleanup"),
        "discard": summary.get("discard"),
    }
    checkpoints = summary.get("digest_checkpoints")
    if isinstance(checkpoints, Sequence) and not isinstance(
        checkpoints, (str, bytes)
    ):
        out["digest_checkpoints_count"] = len(checkpoints)
    cloud = summary.get("cloud_sync")
    if isinstance(cloud, Mapping):
        out["cloud_sync"] = {
            key: cloud.get(key)
            for key in ("status", "sync_version", "elapsed_seconds", "error")
            if key in cloud
        }
    else:
        out["cloud_sync"] = cloud
    return out


def run_ingest_round(
    *,
    run_id: Optional[str] = None,
    round_timeout_seconds: Optional[int] = None,
    digest_reserved_seconds: Optional[int] = None,
    client=None,
    ai_agent=None,
    run_cleanup: bool = True,
) -> Dict[str, Any]:
    """Run one production round with incremental publish checkpoints.

    Successful candidates become visible as soon as their enrich chunk commits.
    Candidates that are still pending or failed at the end of the round are
    discarded from this run; the next HN fetch will add them back if they still
    belong in a source ranking.
    """

    run_id = run_id or _new_run_id()
    timeout = (
        int(round_timeout_seconds)
        if round_timeout_seconds is not None
        else int(settings.INGEST_ROUND_TIMEOUT_SECONDS)
    )
    reserved = (
        int(digest_reserved_seconds)
        if digest_reserved_seconds is not None
        else int(settings.INGEST_DIGEST_RESERVED_SECONDS)
    )
    started = time.time()
    deadline_at = started + timeout if timeout > 0 else None
    deadline_epoch = int(deadline_at) if deadline_at is not None else None

    summary: Dict[str, Any] = {
        "run_id": run_id,
        "status": "running",
        "fetch": None,
        "enrich": None,
        "digest": None,
        "digest_checkpoints": [],
        "insights": None,
        "publish": None,
        "cleanup": None,
    }
    candidate_count = 0
    target_ids: List[int] = []
    digest_error_alerted = False

    conn = db.connect()
    try:
        with db.transaction(conn):
            repository.start_ingest_run(
                conn,
                run_id,
                started_at=int(started),
                deadline_at=deadline_epoch,
            )
    finally:
        conn.close()

    try:
        # Build the AI agent after the run row exists so startup-time
        # configuration errors are visible in ingest_runs/dashboard history.
        if ai_agent is None:
            from .ai_agent import build_ai_agent

            ai_agent = build_ai_agent()
        round_ai_usage_checkpoint = _ai_usage_checkpoint(ai_agent)

        conn = db.connect()
        try:
            with db.transaction(conn):
                repository.update_ingest_run(conn, run_id, phase="fetch")
        finally:
            conn.close()

        fetch_deadline = None
        if deadline_at is not None:
            fetch_deadline = deadline_at - max(0, reserved)
            if time.time() >= fetch_deadline:
                error = "round deadline reached before fetch could start"
                _finish_run(run_id, "timeout", error=error)
                _alert(
                    "ingest_timeout",
                    "HN ingest timed out before fetch",
                    error,
                    run_id=run_id,
                    extra=_round_alert_extra(
                        started_at=started,
                        candidate_count=0,
                        target_ids=[],
                    ),
                )
                summary["status"] = "timeout"
                summary["error"] = error
                return summary

        fetch_summary = run_fetcher_once(
            client=client,
            run_id=run_id,
            deadline_at=fetch_deadline,
        )
        summary["fetch"] = fetch_summary
        log.info("fetcher: %s", fetch_summary)

        conn = db.connect()
        try:
            with db.transaction(conn):
                repository.update_ingest_run(
                    conn,
                    run_id,
                    raw_count=int(fetch_summary.get("items_fetched", 0) or 0),
                    candidate_count=int(fetch_summary.get("candidate_count", 0) or 0),
                )
                target_ids = repository.candidate_story_ids(conn, run_id)
                candidate_count = len(target_ids)
        finally:
            conn.close()

        if fetch_summary.get("timed_out"):
            error = "round deadline reached during fetch"
            cleanup = _discard_run(run_id, release_inflight=True)
            _finish_run(run_id, "timeout", error=error)
            _alert(
                "ingest_timeout",
                "HN ingest timed out during fetch",
                error,
                run_id=run_id,
                extra=_round_alert_extra(
                    started_at=started,
                    candidate_count=candidate_count,
                    target_ids=target_ids,
                    extra={
                        "fetch": json.dumps(fetch_summary, ensure_ascii=False),
                        **cleanup,
                    },
                ),
            )
            summary["status"] = "timeout"
            summary["error"] = error
            return summary

        if not fetch_summary.get("successful_round") or not target_ids:
            error = "fetch produced no publishable candidates"
            _discard_run(run_id)
            _finish_run(run_id, "failed", error=error)
            _alert(
                "fetch_failed",
                "HN ingest fetch produced no candidates",
                error,
                run_id=run_id,
                extra=_round_alert_extra(
                    started_at=started,
                    candidate_count=candidate_count,
                    target_ids=target_ids,
                    extra={"fetch": json.dumps(fetch_summary, ensure_ascii=False)},
                ),
            )
            summary["status"] = "failed"
            summary["error"] = error
            return summary

        enrich_deadline = None
        if deadline_at is not None:
            enrich_deadline = fetch_deadline
            if time.time() >= enrich_deadline:
                error = "round deadline reached before enrich could start"
                cleanup = _discard_run(run_id, release_inflight=True)
                _finish_run(run_id, "timeout", error=error)
                _alert(
                    "enrich_timeout",
                    "HN ingest timed out before enrich",
                    error,
                    run_id=run_id,
                    extra=_round_alert_extra(
                        started_at=started,
                        candidate_count=candidate_count,
                        target_ids=target_ids,
                        extra=cleanup,
                    ),
                )
                summary["status"] = "timeout"
                summary["error"] = error
                return summary

        conn = db.connect()
        try:
            with db.transaction(conn):
                repository.update_ingest_run(conn, run_id, phase="enrich")
        finally:
            conn.close()

        def record_digest_summary(
            digest_summary: Dict[str, Any],
            *,
            checkpoint: str,
        ) -> None:
            nonlocal digest_error_alerted
            summary["digest"] = digest_summary
            summary["digest_checkpoints"].append(digest_summary)
            if digest_summary.get("reason") != "error" or digest_error_alerted:
                return
            digest_error_alerted = True
            error = str(digest_summary.get("error") or "digest checkpoint failed")
            _alert(
                "digest_failed",
                "HN ingest digest failed",
                error,
                run_id=run_id,
                extra=_round_alert_extra(
                    started_at=started,
                    candidate_count=candidate_count,
                    target_ids=target_ids,
                    extra={
                        "checkpoint": checkpoint,
                        "digest": json.dumps(digest_summary, ensure_ascii=False),
                    },
                ),
            )

        def _on_enrich_progress(progress: Dict[str, Any]) -> None:
            _update_run_enrich_progress(run_id, progress)
            _record_run_ai_usage_snapshot(
                run_id, ai_agent, round_ai_usage_checkpoint
            )

        enrich_summary = run_enricher_once(
            client=client,
            ai_agent=ai_agent,
            deadline_at=enrich_deadline,
            target_ids=target_ids,
            bump_visible_version=False,
            publish_run_id=run_id,
            progress_callback=_on_enrich_progress,
        )
        summary["enrich"] = enrich_summary
        log.info("enricher: %s", _compact_enrich_for_log(enrich_summary))

        conn = db.connect()
        try:
            with db.transaction(conn):
                repository.update_ingest_run(
                    conn,
                    run_id,
                    claimed=int(enrich_summary.get("claimed", 0) or 0),
                    done=int(enrich_summary.get("done", 0) or 0),
                    failed=int(enrich_summary.get("failed", 0) or 0),
                    retried=int(enrich_summary.get("retried", 0) or 0),
                )
                incomplete = repository.count_incomplete_candidates(conn, run_id)
        finally:
            conn.close()
        _record_run_ai_usage_snapshot(
            run_id, ai_agent, round_ai_usage_checkpoint
        )

        timed_out = bool(enrich_summary.get("timed_out"))
        partial_error = ""
        if timed_out:
            partial_error = "enrich timed out before all candidates completed"
        elif incomplete > 0:
            partial_error = f"{incomplete} staged candidates did not finish enrichment"

        conn = db.connect()
        try:
            with db.transaction(conn):
                repository.update_ingest_run(conn, run_id, phase="publish")
                publish_summary = repository.publish_ranking_candidates(
                    conn,
                    run_id,
                    FEEDS,
                    preserve_existing=bool(partial_error),
                )
            summary["publish"] = publish_summary
            log.info(
                "publisher final: %s",
                _compact_publish_for_log(publish_summary),
            )
        finally:
            conn.close()

        if int(publish_summary.get("ready_count", 0) or 0) > 0:
            # Use ``auto`` so the digest's first / incremental / timer gates
            # apply (digest.py:118-142). ``force`` previously made every round
            # burn ~20k tokens on selection even when nothing meaningful had
            # landed; the gates already cover the cases that need a recompute.
            digest_summary = _commit_digest_checkpoint(
                ai_agent=ai_agent,
                target_ids=target_ids,
                mode="auto",
            )
            record_digest_summary(digest_summary, checkpoint="final")
            log.info("digester final: %s", _compact_digest_for_log(digest_summary))
            _record_run_ai_usage_snapshot(
                run_id, ai_agent, round_ai_usage_checkpoint
            )

        ready_count = int(publish_summary.get("ready_count", 0) or 0)
        cleanup = _discard_run(
            run_id,
            release_inflight=timed_out,
            delete_pending_orphans=not bool(partial_error),
        )
        summary["discard"] = cleanup

        if partial_error:
            final_status = (
                "partial"
                if ready_count > 0
                else ("timeout" if timed_out else "failed")
            )
            _finish_run(run_id, final_status, error=partial_error)
            _alert(
                "enrich_timeout" if timed_out else "enrich_incomplete",
                "HN ingest enrich timed out" if timed_out else "HN ingest enrich incomplete",
                partial_error,
                run_id=run_id,
                extra={
                    **_round_alert_extra(
                        started_at=started,
                        candidate_count=candidate_count,
                        target_ids=target_ids,
                        extra=cleanup,
                    ),
                    "incomplete_candidates": incomplete,
                    "enrich": json.dumps(
                        _compact_enrich_for_log(enrich_summary),
                        ensure_ascii=False,
                    ),
                    "publish": json.dumps(
                        _compact_publish_for_log(publish_summary),
                        ensure_ascii=False,
                    ),
                },
            )
            summary["status"] = final_status
            summary["error"] = partial_error
            if final_status != "partial":
                return summary
        elif ready_count <= 0:
            error = "publish produced no visible stories"
            _finish_run(run_id, "failed", error=error)
            _alert(
                "publish_failed",
                "HN ingest publish produced no visible stories",
                error,
                run_id=run_id,
                extra={
                    **_round_alert_extra(
                        started_at=started,
                        candidate_count=candidate_count,
                        target_ids=target_ids,
                        extra=cleanup,
                    ),
                    "publish": json.dumps(
                        _compact_publish_for_log(publish_summary),
                        ensure_ascii=False,
                    ),
                },
            )
            summary["status"] = "failed"
            summary["error"] = error
            return summary
        else:
            _finish_run(run_id, "completed")
            summary["status"] = "completed"

        try:
            conn = db.connect()
            try:
                with db.transaction(conn):
                    repository.update_ingest_run(conn, run_id, phase="insights")
            finally:
                conn.close()
            from .insights import run_insights_once

            insights_summary = run_insights_once()
            summary["insights"] = insights_summary
            if insights_summary.get("status") == "failed":
                log.warning("insights failed: %s", insights_summary)
            else:
                log.info("insights: %s", insights_summary)
        except Exception as exc:  # noqa: BLE001
            # Insights is an additive read model. It must never roll back or
            # block stories/topics/digests publication.
            log.exception("insights failed unexpectedly: %s", exc)
            summary["insights"] = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }

        if run_cleanup:
            try:
                conn = db.connect()
                try:
                    with db.transaction(conn):
                        repository.update_ingest_run(conn, run_id, phase="cleanup")
                finally:
                    conn.close()
                from .cleanup import run_cleanup_once

                cleanup_summary = run_cleanup_once()
                summary["cleanup"] = cleanup_summary
                log.info("cleanup: %s", cleanup_summary)
            except Exception as exc:  # noqa: BLE001
                log.exception("cleanup failed: %s", exc)
                _alert(
                    "cleanup_failed",
                    "HN ingest cleanup failed",
                    f"{type(exc).__name__}: {exc}",
                    run_id=run_id,
                    extra=_round_alert_extra(
                        started_at=started,
                        candidate_count=candidate_count,
                        target_ids=target_ids,
                    ),
                )

        # Half-migration: push the newly published read model to the cloud
        # database. This only makes sense when the publish actually has visible
        # stories (completed / partial). Failures are already swallowed and
        # recorded in the helper, so they do not affect the main ingest flow's status.
        if settings.CLOUD_SYNC_ENABLED and summary.get("status") in ("completed", "partial"):
            summary["cloud_sync"] = _trigger_and_record_cloud_sync(
                run_id, deadline_at=deadline_at
            )

        return summary

    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
        _discard_run(
            run_id,
            release_inflight=True,
            delete_pending_orphans=not bool(target_ids),
        )
        _finish_run(run_id, "failed", error=error)
        _alert(
            "ingest_failed",
            "HN ingest round failed",
            error,
            run_id=run_id,
            extra=_round_alert_extra(
                started_at=started,
                candidate_count=candidate_count,
                target_ids=target_ids,
            ),
        )
        summary["status"] = "failed"
        summary["error"] = error
        log.exception("ingest round %s failed: %s", run_id, exc)
        return summary


def _reset_after_killed_child(run_id: str) -> dict:
    conn = db.connect()
    try:
        row = conn.execute(
            "SELECT status FROM ingest_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        status = str(row["status"]) if row is not None else ""
    finally:
        conn.close()

    cleanup = _discard_run(
        run_id, release_inflight=True, delete_pending_orphans=False
    )
    cleanup["previous_status"] = status
    if not status:
        conn = db.connect()
        try:
            with db.transaction(conn):
                repository.start_ingest_run(
                    conn,
                    run_id,
                    started_at=repository.now_seconds(),
                    deadline_at=None,
                )
        finally:
            conn.close()
    _finish_run(run_id, "timeout", error="supervisor killed timed-out child")
    return cleanup


def _reset_after_failed_child(run_id: str, return_code: int) -> dict:
    conn = db.connect()
    try:
        row = conn.execute(
            "SELECT status FROM ingest_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        status = str(row["status"]) if row is not None else ""
    finally:
        conn.close()

    cleanup = _discard_run(
        run_id, release_inflight=True, delete_pending_orphans=False
    )
    cleanup["previous_status"] = status
    if status in ("", "running"):
        if not status:
            conn = db.connect()
            try:
                with db.transaction(conn):
                    repository.start_ingest_run(
                        conn,
                        run_id,
                        started_at=repository.now_seconds(),
                        deadline_at=None,
                    )
            finally:
                conn.close()
        _finish_run(
            run_id,
            "failed",
            error=f"supervisor observed child exit code {int(return_code)}",
        )
    return cleanup


def _reset_after_child_start_failed(run_id: str, error: str) -> dict:
    conn = db.connect()
    try:
        row = conn.execute(
            "SELECT status FROM ingest_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        status = str(row["status"]) if row is not None else ""
    finally:
        conn.close()

    cleanup = _discard_run(
        run_id, release_inflight=True, delete_pending_orphans=False
    )
    cleanup["previous_status"] = status
    if status in ("", "running"):
        if not status:
            conn = db.connect()
            try:
                with db.transaction(conn):
                    repository.start_ingest_run(
                        conn,
                        run_id,
                        started_at=repository.now_seconds(),
                        deadline_at=None,
                    )
            finally:
                conn.close()
        _finish_run(run_id, "failed", error=error)
    return cleanup


def _reset_after_stopped_child(run_id: str, reason: str) -> dict:
    conn = db.connect()
    try:
        row = conn.execute(
            "SELECT status FROM ingest_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        status = str(row["status"]) if row is not None else ""
    finally:
        conn.close()

    cleanup = _discard_run(
        run_id, release_inflight=True, delete_pending_orphans=False
    )
    cleanup["previous_status"] = status
    if status in ("", "running"):
        if not status:
            conn = db.connect()
            try:
                with db.transaction(conn):
                    repository.start_ingest_run(
                        conn,
                        run_id,
                        started_at=repository.now_seconds(),
                        deadline_at=None,
                    )
            finally:
                conn.close()
        _finish_run(run_id, "discarded", error=reason)
    return cleanup


def _recover_abandoned_running_runs(*, now: Optional[int] = None) -> List[dict]:
    """Close runs left running by a previous supervisor/process crash."""
    now_s = int(now if now is not None else repository.now_seconds())
    conn = db.connect()
    try:
        rows = conn.execute(
            """
            SELECT run_id, phase, deadline_at
            FROM ingest_runs
            WHERE status='running'
            ORDER BY started_at
            """
        ).fetchall()
    finally:
        conn.close()

    recovered: List[dict] = []
    for row in rows:
        run_id = str(row["run_id"])
        deadline_at = row["deadline_at"]
        deadline_s = int(deadline_at) if deadline_at is not None else None
        status = "timeout" if deadline_s is not None and deadline_s <= now_s else "discarded"
        error = (
            "supervisor recovered abandoned timed-out run"
            if status == "timeout"
            else "supervisor recovered abandoned running run"
        )
        cleanup = _discard_run(
            run_id, release_inflight=True, delete_pending_orphans=False
        )
        _finish_run(run_id, status, error=error)
        cleanup.update(
            {
                "run_id": run_id,
                "previous_phase": row["phase"] or "",
                "previous_deadline_at": deadline_s,
                "status": status,
            }
        )
        recovered.append(cleanup)
    return recovered


class _SupervisorShutdown(Exception):
    def __init__(self, signum: int):
        super().__init__(signum)
        self.signum = signum


# Sentinel signum reported when the supervisor is asked to stop via the stop
# flag file (Windows graceful stop path). Negative so it never collides with a
# real OS signal number.
_STOP_FLAG_SIGNUM = -1


def _signal_name(signum: int) -> str:
    if signum == _STOP_FLAG_SIGNUM:
        return "STOP_FLAG"
    try:
        return signal.Signals(signum).name
    except ValueError:
        return str(signum)


def _stop_flag_path():
    return settings.get_db_path().parent / "supervisor.stop"


def _stop_flag_set() -> bool:
    try:
        return _stop_flag_path().exists()
    except OSError:
        return False


def _clear_stop_flag() -> None:
    try:
        _stop_flag_path().unlink()
    except FileNotFoundError:
        pass
    except OSError as exc:
        log.warning("failed to remove stop flag: %s", exc)


def _check_stop_flag() -> None:
    if _stop_flag_set():
        raise _SupervisorShutdown(_STOP_FLAG_SIGNUM)


def _wait_child_or_stop(
    proc: subprocess.Popen, *, deadline: float, poll_seconds: float = 1.0
) -> int:
    """Wait for child to exit, polling the stop flag every poll_seconds.

    Mirrors proc.wait(timeout=...) semantics: returns the exit code when the
    child finishes, raises subprocess.TimeoutExpired when the deadline is
    crossed, and raises _SupervisorShutdown if the stop flag appears mid-wait.
    """
    while True:
        return_code = proc.poll()
        if return_code is not None:
            return int(return_code)
        _check_stop_flag()
        remaining = deadline - time.time()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(proc.args, deadline)
        try:
            return proc.wait(timeout=min(poll_seconds, remaining))
        except subprocess.TimeoutExpired:
            continue


def _sleep_or_stop(duration: float, *, poll_seconds: float = 1.0) -> None:
    """time.sleep(duration), but raise _SupervisorShutdown if the stop flag
    appears while sleeping."""
    if duration <= 0:
        _check_stop_flag()
        return
    deadline = time.time() + duration
    while True:
        _check_stop_flag()
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        time.sleep(min(poll_seconds, remaining))


def _terminate_child_process(
    proc: subprocess.Popen,
    *,
    run_id: str,
    kill_grace: int,
    reason: str,
) -> None:
    if proc.poll() is not None:
        return
    log.warning("terminating ingest child %s: %s", run_id, reason)
    try:
        proc.terminate()
    except OSError as exc:
        log.warning("failed to terminate ingest child %s: %s", run_id, exc)
    try:
        proc.wait(timeout=kill_grace)
    except subprocess.TimeoutExpired:
        log.warning("ingest child %s did not terminate; killing", run_id)
        proc.kill()
        proc.wait()


def _install_supervisor_shutdown_handlers():
    previous = {}

    def _handle(signum, _frame):
        raise _SupervisorShutdown(signum)

    for signum in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None)):
        if signum is None:
            continue
        try:
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, _handle)
        except (OSError, RuntimeError, ValueError):
            continue
    return previous


def _restore_supervisor_shutdown_handlers(previous) -> None:
    for signum, handler in previous.items():
        try:
            signal.signal(signum, handler)
        except (OSError, RuntimeError, ValueError):
            continue


class _SupervisorLockBusy(RuntimeError):
    pass


# Windows _locking() forbids setting the end-of-file inside a locked byte
# range. Locking byte 0 (the original implementation) was therefore
# invalidated by __enter__'s subsequent truncate()+write(pid_line), which
# moved EOF through that region and made every LK_UNLCK on shutdown raise
# PermissionError. Park the 1-byte lock far past any plausible file size so
# truncate/write of the pid line never crosses it. msvcrt.locking() takes
# the offset from the current file pointer and is documented to permit
# locking byte ranges beyond EOF.
_SUPERVISOR_LOCK_OFFSET = 0x7FFFFFFE


class _SupervisorInstanceLock:
    def __init__(self) -> None:
        self.path = settings.get_db_path().with_suffix(".lock")
        self._fh = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a+b")
        try:
            self._lock()
        except OSError as exc:
            self._fh.close()
            self._fh = None
            raise _SupervisorLockBusy(
                f"another ingest supervisor is already running: {self.path}"
            ) from exc

        self._fh.seek(0)
        self._fh.truncate()
        self._fh.write(str(os.getpid()).encode("ascii") + b"\n")
        self._fh.flush()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        if self._fh is None:
            return
        try:
            self._unlock()
        finally:
            self._fh.close()
            self._fh = None

    def _lock(self) -> None:
        assert self._fh is not None
        if os.name == "nt":
            import msvcrt

            self._fh.seek(_SUPERVISOR_LOCK_OFFSET)
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            return

        import fcntl

        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(self) -> None:
        assert self._fh is not None
        if os.name == "nt":
            import msvcrt

            self._fh.seek(_SUPERVISOR_LOCK_OFFSET)
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            return

        import fcntl

        fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)


def _supervisor_failure_sleep_seconds(consecutive_failures: int) -> float:
    base = max(1, int(settings.INGEST_FAILURE_BACKOFF_SECONDS))
    exponent = max(0, min(8, int(consecutive_failures) - 1))
    return float(min(300, base * (2 ** exponent)))


def _current_ingest_module_name() -> str:
    package = (__package__ or "").strip()
    if package:
        return f"{package}.ingest"
    return "server.ingest"


def run_supervisor_loop(
    *,
    interval_seconds: int,
    round_timeout_seconds: int,
    digest_reserved_seconds: int,
    verbose: bool,
) -> int:
    """Run ingest rounds in child processes and hard-kill overrun rounds."""
    interval = max(1, int(interval_seconds))
    timeout = max(1, int(round_timeout_seconds))
    kill_grace = max(1, int(settings.INGEST_CHILD_KILL_GRACE_SECONDS))

    supervisor_lock = _SupervisorInstanceLock()
    try:
        supervisor_lock.__enter__()
    except _SupervisorLockBusy as exc:
        log.error("%s", exc)
        return 1

    previous_handlers = _install_supervisor_shutdown_handlers()
    # A leftover stop flag from a previous shutdown would terminate this
    # supervisor on the first poll. Clear it before installing the handlers.
    _clear_stop_flag()
    try:
        recovered = _recover_abandoned_running_runs()
        if recovered:
            log.warning("recovered abandoned ingest runs on startup: %s", recovered)
        consecutive_failures = 0
        while True:
            _check_stop_flag()
            run_id = _new_run_id()
            cmd = [
                sys.executable,
                "-m",
                _current_ingest_module_name(),
                "--once",
                "--child",
                "--run-id",
                run_id,
                "--round-timeout-seconds",
                str(timeout),
                "--digest-reserved-seconds",
                str(digest_reserved_seconds),
            ]
            if verbose:
                cmd.append("--verbose")

            started = time.time()
            failed_round = False
            log.info("starting ingest child run_id=%s timeout=%ss", run_id, timeout)
            try:
                proc = subprocess.Popen(cmd, env=os.environ.copy())
            except OSError as exc:
                failed_round = True
                error = f"failed to start ingest child: {type(exc).__name__}: {exc}"
                cleanup = _reset_after_child_start_failed(run_id, error)
                log.exception("failed to start ingest child run_id=%s", run_id)
                _alert(
                    "ingest_child_start_failed",
                    "HN ingest child failed to start",
                    error,
                    run_id=run_id,
                    extra={
                        **cleanup,
                        "started_at": int(started),
                        "elapsed_seconds": round(time.time() - started, 1),
                    },
                )
            else:
                try:
                    return_code = _wait_child_or_stop(proc, deadline=started + timeout)
                except _SupervisorShutdown as exc:
                    signal_name = _signal_name(exc.signum)
                    reason = f"supervisor received {signal_name}"
                    _terminate_child_process(
                        proc,
                        run_id=run_id,
                        kill_grace=kill_grace,
                        reason=reason,
                    )
                    cleanup = _reset_after_stopped_child(run_id, reason)
                    log.info("supervisor stop reset run_id=%s cleanup=%s", run_id, cleanup)
                    return 0
                except subprocess.TimeoutExpired:
                    failed_round = True
                    _terminate_child_process(
                        proc,
                        run_id=run_id,
                        kill_grace=kill_grace,
                        reason=f"exceeded {timeout}s",
                    )
                    cleanup = _reset_after_killed_child(run_id)
                    _alert(
                        "ingest_timeout",
                        "HN ingest child timed out",
                        "supervisor terminated a timed-out ingest child",
                        run_id=run_id,
                        extra={
                            **cleanup,
                            "timeout_seconds": timeout,
                            "elapsed_seconds": round(time.time() - started, 1),
                        },
                    )
                else:
                    if return_code != 0:
                        failed_round = True
                        cleanup = _reset_after_failed_child(run_id, return_code)
                        _alert(
                            "ingest_child_failed",
                            "HN ingest child exited non-zero",
                            f"child exit code: {return_code}",
                            run_id=run_id,
                            extra={
                                **cleanup,
                                "exit_code": return_code,
                                "started_at": int(started),
                                "elapsed_seconds": round(time.time() - started, 1),
                            },
                        )

            elapsed = time.time() - started
            if failed_round:
                consecutive_failures += 1
                sleep_for = max(
                    max(0.0, float(interval) - elapsed),
                    _supervisor_failure_sleep_seconds(consecutive_failures),
                )
            else:
                consecutive_failures = 0
                sleep_for = max(0.0, float(interval) - elapsed)
            log.info("loop sleeping %.1fs", sleep_for)
            try:
                _sleep_or_stop(sleep_for)
            except _SupervisorShutdown as exc:
                log.info(
                    "supervisor received %s while idle; exiting loop",
                    _signal_name(exc.signum),
                )
                return 0
            except KeyboardInterrupt:
                log.info("interrupted; exiting loop")
                return 0
    except _SupervisorShutdown as exc:
        log.info(
            "supervisor received %s before child start; exiting loop",
            _signal_name(exc.signum),
        )
        return 0
    finally:
        _restore_supervisor_shutdown_handlers(previous_handlers)
        # Clean up our own marker so the next supervisor start isn't tripped
        # by a stop flag we already acted on.
        _clear_stop_flag()
        supervisor_lock.__exit__(None, None, None)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="hnreader ingestion CLI")
    parser.add_argument(
        "--once", action="store_true", help="Run fetch + enrich + digest + cleanup once"
    )
    parser.add_argument("--fetch", action="store_true", help="Run Fetcher only")
    parser.add_argument("--enrich", action="store_true", help="Run Enricher only")
    parser.add_argument("--digest", action="store_true", help="Run Digester only")
    parser.add_argument("--insights", action="store_true", help="Run Insights only")
    parser.add_argument(
        "--force-insights",
        action="store_true",
        help="Regenerate insights even when the update interval gate says not due",
    )
    parser.add_argument("--date", default="", help="Target date for --insights (YYYY-MM-DD)")
    parser.add_argument("--cleanup", action="store_true", help="Run Cleanup only")
    parser.add_argument(
        "--reset-failed",
        action="store_true",
        help="Reset all failed stories back to pending so they re-enter the AI queue",
    )
    parser.add_argument(
        "--metrics",
        action="store_true",
        help="Print pipeline metrics as JSON and exit (read-only)",
    )
    parser.add_argument("--loop", action="store_true", help="Repeat --once forever")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=settings.INGEST_INTERVAL_SECONDS,
        help="Sleep between iterations when --loop is set (default 1800)",
    )
    parser.add_argument(
        "--round-timeout-seconds",
        type=int,
        default=settings.INGEST_ROUND_TIMEOUT_SECONDS,
        help="Maximum wall time for one full ingest round",
    )
    parser.add_argument(
        "--digest-reserved-seconds",
        type=int,
        default=settings.INGEST_DIGEST_RESERVED_SECONDS,
        help="Time reserved after enrich for digest/publish/cleanup",
    )
    parser.add_argument("--run-id", default="", help=argparse.SUPPRESS)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    _setup_logging(args.verbose)
    db.init_db()
    if (args.once or args.loop) and settings.ADMIN_EMAIL_ENABLED:
        from .notifications import validate_admin_alert_config

        validate_admin_alert_config()

    if args.reset_failed:
        n = _maintenance_reset_failed()
        log.info("reset %d failed -> pending", n)
        return 0

    if args.metrics:
        conn = db.connect()
        try:
            metrics = repository.get_pipeline_metrics(conn)
        finally:
            conn.close()
        print(json.dumps(metrics, ensure_ascii=True, indent=2))
        return 0

    requested = {
        "fetch": args.fetch,
        "enrich": args.enrich,
        "digest": args.digest,
        "insights": args.insights,
        "cleanup": args.cleanup,
    }
    full_round_requested = args.once or args.loop
    if not any(requested.values()):
        if not full_round_requested:
            parser.print_help()
            return 1

    def _one_round() -> None:
        if requested["fetch"]:
            try:
                summary = run_fetcher_once(run_id=args.run_id or None)
                log.info("fetcher: %s", summary)
            except Exception as exc:  # noqa: BLE001
                log.exception("fetcher failed: %s", exc)
        if requested["enrich"]:
            try:
                summary = run_enricher_once()
                log.info("enricher: %s", summary)
            except Exception as exc:  # noqa: BLE001
                log.exception("enricher failed: %s", exc)
        if requested["digest"]:
            try:
                from .digest import run_digester_once

                summary = run_digester_once()
                log.info("digester: %s", summary)
            except Exception as exc:  # noqa: BLE001
                log.exception("digester failed: %s", exc)
        if requested["insights"]:
            try:
                from .insights import run_insights_once

                summary = run_insights_once(
                    date=args.date or None,
                    force=bool(args.force_insights),
                )
                log.info("insights: %s", summary)
            except Exception as exc:  # noqa: BLE001
                log.exception("insights failed: %s", exc)
        if requested["cleanup"]:
            try:
                from .cleanup import run_cleanup_once

                summary = run_cleanup_once()
                log.info("cleanup: %s", summary)
            except Exception as exc:  # noqa: BLE001
                log.exception("cleanup failed: %s", exc)

    if args.loop:
        return run_supervisor_loop(
            interval_seconds=args.interval_seconds,
            round_timeout_seconds=args.round_timeout_seconds,
            digest_reserved_seconds=args.digest_reserved_seconds,
            verbose=args.verbose,
        )

    def _run_full_round_once() -> int:
        summary = run_ingest_round(
            run_id=args.run_id or None,
            round_timeout_seconds=args.round_timeout_seconds,
            digest_reserved_seconds=args.digest_reserved_seconds,
        )
        if args.child:
            log.info(
                "ingest_round_summary: %s",
                json.dumps(_compact_round_summary_for_log(summary), ensure_ascii=True),
            )
        else:
            print(json.dumps(summary, ensure_ascii=True, indent=2))
        return 0 if summary.get("status") in ("completed", "partial") else 2

    if args.once:
        if args.child:
            return _run_full_round_once()
        try:
            with _SupervisorInstanceLock():
                return _run_full_round_once()
        except _SupervisorLockBusy as exc:
            log.error("%s", exc)
            return 1

    if any(requested.values()):
        _one_round()
    return 0


if __name__ == "__main__":
    sys.exit(main())
