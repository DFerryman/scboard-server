"""Server-side insights generation pipeline."""

from __future__ import annotations

import html
import hashlib
import json
import logging
import re
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from . import db, repository, settings
from .insights_agents import (
    InsightsAgentRunner,
    InsightsValidationError,
    contains_forbidden_words,
    sanitize_forbidden_words,
)
from .topics import (
    DEFAULT_TOPIC_ID,
    DEFAULT_TOPIC_NAME,
    clean_topic_name,
    legacy_topic_id,
    resolve_fixed_source_topic,
)


log = logging.getLogger(__name__)

INSIGHTS_VERSION = 1
INSIGHTS_WINDOW_LABEL = "24h"
SIGNALS_MIN_STORIES = 3
OPPORTUNITY_MIN_CANDIDATES = 3
DEBATE_MIN_CANDIDATES = 2
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _elapsed_seconds(started: float) -> float:
    return round(max(0.0, time.monotonic() - started), 3)


def _json_loads(text: Any, default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


def _digest_tz():
    if ZoneInfo is None:
        return None
    return ZoneInfo(settings.DIGEST_TIMEZONE)


def _target_date(value: Optional[str]) -> str:
    if value is None or str(value).strip() == "":
        return repository.today_in_digest_tz()
    clean = str(value).strip()
    if not _DATE_RE.fullmatch(clean):
        raise ValueError(f"invalid insights date: {clean}")
    datetime.strptime(clean, "%Y-%m-%d")
    return clean


def _window_bounds(date: str, window_days: int) -> Tuple[int, int, str]:
    target_start, target_end = repository.digest_date_epoch_bounds(date)
    start_date = (
        datetime.strptime(date, "%Y-%m-%d") - timedelta(days=max(1, window_days) - 1)
    ).strftime("%Y-%m-%d")
    start_ts, _ = repository.digest_date_epoch_bounds(start_date)
    return start_ts, target_end, start_date


def _now_iso_in_digest_tz() -> str:
    tz = _digest_tz()
    if tz is not None:
        return datetime.now(tz).isoformat(timespec="seconds")
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _as_of_label(date: str) -> str:
    tz = _digest_tz()
    label = date.replace("-", ".")
    if tz is None:
        return f"{label} · UTC"
    dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=tz)
    offset = dt.utcoffset() or timedelta(0)
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    suffix = f"UTC{sign}{hours}" if minutes == 0 else f"UTC{sign}{hours}:{minutes:02d}"
    return f"{label} · {suffix}"


def _clean_comment_text(value: Any, max_chars: Optional[int]) -> str:
    _ = max_chars
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _coerce_list_json(value: Any) -> List[Any]:
    data = _json_loads(value, [])
    return data if isinstance(data, list) else []


def _row_value(row: Any, key: str, default: Any = "") -> Any:
    try:
        if key in row.keys():
            return row[key]
    except AttributeError:
        if isinstance(row, Mapping):
            return row.get(key, default)
    return default


def _topic_label(topic: str, topic_name: Any = "") -> str:
    fixed = resolve_fixed_source_topic(topic=topic, topic_name=topic_name)
    if fixed:
        return fixed[1]
    name = clean_topic_name(topic_name)
    if name:
        return name
    legacy_id = legacy_topic_id(topic)
    return legacy_id or DEFAULT_TOPIC_NAME


def _row_topic_label(row: Any) -> str:
    return _topic_label(
        str(row["topic"] or ""),
        _row_value(row, "topic_name", ""),
    )


def _row_topic_id(row: Any) -> str:
    fixed = resolve_fixed_source_topic(
        topic=str(row["topic"] or ""),
        topic_name=_row_value(row, "topic_name", ""),
    )
    if fixed:
        return fixed[0]
    return legacy_topic_id(str(row["topic"] or "")) or DEFAULT_TOPIC_ID


def _story_payload(
    row,
    *,
    feed_ranks: Mapping[int, Mapping[str, int]],
    include_raw_text: bool = False,
    raw_text_max_chars: Optional[int] = None,
    include_domain: bool = False,
    include_insights: bool = False,
    comments: Optional[Sequence[Any]] = None,
    comment_max_chars: Optional[int] = None,
) -> Dict[str, Any]:
    story_id = int(row["id"])
    out: Dict[str, Any] = {
        "id": story_id,
        "kind": row["kind"] or "story",
        "topic": _row_topic_id(row),
        "topicName": _row_topic_label(row),
        "titleZh": row["title_zh"] or row["title_en"] or "",
        "titleEn": row["title_en"] or "",
        "score": int(row["score"] or 0),
        "descendants": int(row["descendants"] or 0),
        "time": int(row["hn_time"] or 0),
        "feedRanks": dict(feed_ranks.get(story_id, {})),
        "aiSummary": row["ai_summary"] or "",
        "discussionThemes": _coerce_list_json(row["discussion_themes"]),
    }
    if include_domain and "domain" in row.keys():
        out["domain"] = row["domain"] or ""
    insights = _coerce_list_json(row["insights"])
    if include_insights and insights:
        out["insights"] = insights
    if include_raw_text:
        raw_text = row["raw_text"] or ""
        _ = raw_text_max_chars
        out["rawTextSnippet"] = raw_text
    if comments is not None:
        out["comments"] = [
            {
                "by": c["by"] or "",
                "text": _clean_comment_text(c["text"] or "", comment_max_chars),
                "score": 0,
            }
            for c in comments
            if _clean_comment_text(c["text"] or "", comment_max_chars)
        ]
    return out


def _build_today_topic_summary(rows: Sequence[Any]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    score_sum: Dict[str, int] = {}
    for row in rows:
        topic = _row_topic_label(row)
        counts[topic] = counts.get(topic, 0) + 1
        score_sum[topic] = score_sum.get(topic, 0) + int(row["score"] or 0)
    return [
        {"topic": topic, "count": counts[topic], "scoreSum": score_sum[topic]}
        for topic in sorted(counts, key=lambda t: (-counts[t], -score_sum[t], t))
    ]


def build_today_signals_input(
    today_rows: Sequence[Any],
    *,
    target_date: str,
    feed_ranks: Mapping[int, Mapping[str, int]],
    comments_by_story: Optional[Mapping[int, Sequence[Any]]] = None,
) -> Dict[str, Any]:
    rows = sorted(
        today_rows,
        key=lambda r: (int(r["score"] or 0), int(r["descendants"] or 0), int(r["hn_time"] or 0)),
        reverse=True,
    )
    stories = []
    for row in rows:
        sid = int(row["id"])
        story_kwargs: Dict[str, Any] = {}
        if comments_by_story is not None and sid in comments_by_story:
            story_kwargs["comments"] = comments_by_story.get(sid, [])
            story_kwargs["comment_max_chars"] = None
        stories.append(
            _story_payload(
                row,
                feed_ranks=feed_ranks,
                **story_kwargs,
            )
        )
    return {
        "date": target_date,
        "topicSummary": _build_today_topic_summary(rows),
        "stories": stories,
    }


def _feed_rank_score(feed_ranks: Mapping[str, int]) -> int:
    best = 999
    for feed in ("top", "best", "ask", "show"):
        if feed in feed_ranks:
            best = min(best, int(feed_ranks[feed]))
    return best


def build_opportunity_input(
    window_rows: Sequence[Any],
    *,
    target_end_ts: int,
    feed_ranks: Mapping[int, Mapping[str, int]],
    comments_by_story: Mapping[int, Sequence[Any]],
) -> Dict[str, Any]:
    topic_counts: Dict[str, int] = {}
    for row in window_rows:
        topic = _row_topic_id(row)
        topic_counts[topic] = topic_counts.get(topic, 0) + 1

    candidates = []
    for row in window_rows:
        sid = int(row["id"])
        ranks = feed_ranks.get(sid, {})
        repeated = topic_counts.get(_row_topic_id(row), 0) >= 2
        high_rank = _feed_rank_score(ranks) <= 30
        if not (
            int(row["score"] or 0) >= 80
            or int(row["descendants"] or 0) >= 30
            or high_rank
            or repeated
        ):
            continue
        recent_bonus = 1 if int(row["hn_time"] or 0) >= target_end_ts - 2 * 86400 else 0
        candidates.append((recent_bonus, high_rank, repeated, row))

    candidates.sort(
        key=lambda item: (
            item[0],
            item[1],
            int(item[3]["score"] or 0),
            int(item[3]["descendants"] or 0),
            int(item[3]["hn_time"] or 0),
        ),
        reverse=True,
    )
    rows = [item[3] for item in candidates]
    return {
        "candidates": [
            _story_payload(
                row,
                feed_ranks=feed_ranks,
                include_raw_text=True,
                raw_text_max_chars=None,
                include_domain=True,
                include_insights=True,
                comments=comments_by_story.get(int(row["id"]), []),
                comment_max_chars=None,
            )
            for row in rows
        ]
    }


def build_debate_input(
    window_rows: Sequence[Any],
    *,
    feed_ranks: Mapping[int, Mapping[str, int]],
    comments_by_story: Mapping[int, Sequence[Any]],
) -> Dict[str, Any]:
    candidates = []
    for row in window_rows:
        themes = _coerce_list_json(row["discussion_themes"])
        insights = _coerce_list_json(row["insights"])
        score = max(1, int(row["score"] or 0))
        descendants = int(row["descendants"] or 0)
        if not (
            descendants >= 40
            or descendants / score >= 0.6
            or len(themes) >= 2
            or len(insights) >= 2
        ):
            continue
        candidates.append(row)
    candidates.sort(
        key=lambda row: (
            int(row["descendants"] or 0),
            len(_coerce_list_json(row["discussion_themes"])),
            int(row["score"] or 0),
        ),
        reverse=True,
    )
    return {
        "candidates": [
            _story_payload(
                row,
                feed_ranks=feed_ranks,
                include_insights=True,
                comments=comments_by_story.get(int(row["id"]), []),
                comment_max_chars=None,
            )
            for row in candidates
        ]
    }


def _story_ref(row: Any, feed_ranks: Mapping[int, Mapping[str, int]]) -> Dict[str, Any]:
    sid = int(row["id"])
    return {
        "id": sid,
        "topic": _row_topic_id(row),
        "topicName": _row_topic_label(row),
        "titleZh": row["title_zh"] or row["title_en"] or "",
        "titleEn": row["title_en"] or "",
        "score": int(row["score"] or 0),
        "descendants": int(row["descendants"] or 0),
        "time": int(row["hn_time"] or 0),
        "feedRanks": dict(feed_ranks.get(sid, {})),
    }


def _story_refs_by_id(
    rows: Sequence[Any],
    feed_ranks: Mapping[int, Mapping[str, int]],
) -> Dict[int, Dict[str, Any]]:
    return {int(row["id"]): _story_ref(row, feed_ranks) for row in rows}


def _card_story_ids(item: Mapping[str, Any]) -> List[int]:
    out: List[int] = []
    seen: set[int] = set()
    for raw_id in item.get("storyIds") or []:
        try:
            sid = int(raw_id)
        except (TypeError, ValueError):
            continue
        if sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
    return out


def _evidence_card_metrics(
    item: Mapping[str, Any],
    *,
    story_refs: Mapping[int, Mapping[str, Any]],
    target_end_ts: int,
) -> Dict[str, Any]:
    refs = [
        story_refs[sid]
        for sid in _card_story_ids(item)
        if sid in story_refs
    ]
    scores = [int(ref.get("score") or 0) for ref in refs]
    descendants = [int(ref.get("descendants") or 0) for ref in refs]
    times = [int(ref.get("time") or 0) for ref in refs if int(ref.get("time") or 0) > 0]
    feed_ranks = [
        _feed_rank_score(ref.get("feedRanks") or {})
        for ref in refs
        if isinstance(ref.get("feedRanks"), Mapping)
    ]
    best_feed_rank = min(feed_ranks) if feed_ranks else 999
    newest_time = max(times) if times else 0
    return {
        "storyCount": len(refs),
        "totalScore": sum(scores),
        "maxScore": max(scores) if scores else 0,
        "totalDescendants": sum(descendants),
        "maxDescendants": max(descendants) if descendants else 0,
        "oldestTime": min(times) if times else 0,
        "newestTime": newest_time,
        "recencyHours": (
            max(0, int(target_end_ts) - newest_time) // 3600
            if newest_time
            else None
        ),
        "bestFeedRank": best_feed_rank if best_feed_rank < 999 else None,
        "rankedFeedCount": sum(1 for rank in feed_ranks if rank < 999),
    }


def _insight_row_signal_key(
    row: Any,
    feed_ranks: Mapping[int, Mapping[str, int]],
) -> Tuple[int, int, int, int, int]:
    sid = int(row["id"])
    best_feed_rank = _feed_rank_score(feed_ranks.get(sid, {}))
    feed_signal = 1000 - min(999, best_feed_rank)
    return (
        feed_signal,
        int(row["score"] or 0),
        int(row["descendants"] or 0),
        int(row["hn_time"] or 0),
        -sid,
    )


def _limit_insight_rows(
    rows: Sequence[Any],
    *,
    max_rows: int,
    feed_ranks: Mapping[int, Mapping[str, int]],
) -> List[Any]:
    cap = max(0, int(max_rows))
    if cap <= 0:
        return []
    return sorted(
        rows,
        key=lambda row: _insight_row_signal_key(row, feed_ranks),
        reverse=True,
    )[:cap]


def _select_evidence_rows(
    window_rows: Sequence[Any],
    *,
    today_rows: Sequence[Any],
    feed_ranks: Mapping[int, Mapping[str, int]],
    max_rows: int,
) -> List[Any]:
    cap = max(0, int(max_rows))
    if cap <= 0:
        return []
    selected: List[Any] = []
    seen: set[int] = set()

    def add_rows(rows: Sequence[Any]) -> None:
        for row in rows:
            if len(selected) >= cap:
                break
            sid = int(row["id"])
            if sid in seen:
                continue
            selected.append(row)
            seen.add(sid)

    add_rows(today_rows)
    if len(selected) < cap:
        add_rows(
            _limit_insight_rows(
                window_rows,
                max_rows=cap,
                feed_ranks=feed_ranks,
            )
        )
    return selected


def build_evidence_input(
    window_rows: Sequence[Any],
    *,
    target_date: str,
    start_date: str,
    feed_ranks: Mapping[int, Mapping[str, int]],
    comments_by_story: Mapping[int, Sequence[Any]],
) -> Dict[str, Any]:
    rows = sorted(
        window_rows,
        key=lambda r: (
            int(r["hn_time"] or 0),
            int(r["score"] or 0),
            int(r["descendants"] or 0),
        ),
        reverse=True,
    )
    return {
        "date": target_date,
        "window": {"startDate": start_date, "endDate": target_date},
        "storyCount": len(rows),
        "stories": [
            _story_payload(
                row,
                feed_ranks=feed_ranks,
                include_raw_text=True,
                raw_text_max_chars=None,
                include_domain=True,
                include_insights=True,
                comments=comments_by_story.get(int(row["id"]), []),
                comment_max_chars=None,
            )
            for row in rows
        ],
    }


def _hash_text(value: Any) -> Dict[str, Any]:
    text = str(value or "")
    return {
        "len": len(text),
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def _hash_update_json(hasher: "hashlib._Hash", value: Any) -> None:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    hasher.update(encoded)
    hasher.update(b"\n")


def _insights_material_fingerprint(
    *,
    target_date: str,
    start_date: str,
    window_rows: Sequence[Any],
    feed_ranks: Mapping[int, Mapping[str, int]],
    comments_by_story: Mapping[int, Sequence[Any]],
) -> str:
    hasher = hashlib.sha256()
    _hash_update_json(
        hasher,
        {
            "stage": "insights-evidence",
            "schema": 4,
            "date": target_date,
            "startDate": start_date,
            "insightsVersion": INSIGHTS_VERSION,
            "inputCaps": {
                "maxStories": int(settings.INSIGHTS_EVIDENCE_MAX_STORIES),
                "commentLimitPerStory": int(
                    settings.INSIGHTS_EVIDENCE_COMMENT_LIMIT_PER_STORY
                ),
                "batchStories": int(settings.INSIGHTS_EVIDENCE_BATCH_STORIES),
            },
        },
    )
    for row in sorted(window_rows, key=lambda r: int(r["id"])):
        sid = int(row["id"])
        _hash_update_json(
            hasher,
            {
                "id": sid,
                "kind": row["kind"] or "story",
                "topic": _row_topic_id(row),
                "topicName": _row_topic_label(row),
                "titleZh": row["title_zh"] or "",
                "titleEn": row["title_en"] or "",
                "url": row["url"] or "",
                "domain": row["domain"] or "",
                "by": row["by"] or "",
                "score": int(row["score"] or 0),
                "descendants": int(row["descendants"] or 0),
                "hnTime": int(row["hn_time"] or 0),
                "enrichedAt": int(row["enriched_at"] or 0),
                "feedRanks": dict(feed_ranks.get(sid, {})),
                "aiSummary": row["ai_summary"] or "",
                "discussionThemes": row["discussion_themes"] or "[]",
                "insights": row["insights"] or "[]",
                "terms": row["terms"] or "[]",
                "rawText": _hash_text(row["raw_text"] or ""),
            },
        )
        for comment in comments_by_story.get(sid, []):
            _hash_update_json(
                hasher,
                {
                    "storyId": sid,
                    "id": int(_row_value(comment, "id", 0) or 0),
                    "parentId": int(_row_value(comment, "parent_id", 0) or 0),
                    "by": _row_value(comment, "by", "") or "",
                    "hnTime": int(_row_value(comment, "hn_time", 0) or 0),
                    "depth": int(_row_value(comment, "depth", 0) or 0),
                    "rank": int(_row_value(comment, "rank", 0) or 0),
                    "fetchedAt": int(_row_value(comment, "fetched_at", 0) or 0),
                    "text": _hash_text(_row_value(comment, "text", "") or ""),
                },
            )
    return hasher.hexdigest()


def _insights_evidence_cache_key(
    *,
    target_date: str,
    start_date: str,
    window_rows: Sequence[Any],
    feed_ranks: Mapping[int, Mapping[str, int]],
    comments_by_story: Mapping[int, Sequence[Any]],
) -> str:
    fingerprint = _insights_material_fingerprint(
        target_date=target_date,
        start_date=start_date,
        window_rows=window_rows,
        feed_ranks=feed_ranks,
        comments_by_story=comments_by_story,
    )
    return f"insights:evidence:v4:{target_date}:{fingerprint}"


def _load_cached_evidence(cache_key: str) -> Optional[Dict[str, Any]]:
    conn = db.connect()
    try:
        row = repository.get_insight_evidence_cache(conn, cache_key)
        if row is not None:
            repository.touch_insight_evidence_cache(
                conn,
                cache_key,
                repository.now_seconds(),
            )
    finally:
        conn.close()
    if row is None:
        return None
    payload = _json_loads(row["payload"], None)
    return payload if isinstance(payload, dict) else None


def _store_cached_evidence(
    cache_key: str,
    payload: Mapping[str, Any],
    story_count: int,
) -> None:
    conn = db.connect()
    try:
        with db.transaction(conn):
            repository.upsert_insight_evidence_cache(
                conn,
                cache_key,
                dict(payload),
                story_count,
                repository.now_seconds(),
            )
            repository.purge_insight_evidence_cache_over_limit(
                conn,
                settings.INSIGHTS_EVIDENCE_CACHE_MAX_ENTRIES,
            )
    finally:
        conn.close()


def _row_batches(rows: Sequence[Any], batch_size: int) -> List[Sequence[Any]]:
    size = max(1, int(batch_size))
    return [rows[index:index + size] for index in range(0, len(rows), size)]


def _evidence_cache_bucket_count(row_count: int, batch_size: int) -> int:
    count = max(0, int(row_count))
    if count <= 0:
        return 0
    size = max(1, int(batch_size))
    natural = max(1, (count + size - 1) // size)
    configured = max(
        1,
        (int(settings.INSIGHTS_EVIDENCE_MAX_STORIES) + size - 1) // size,
    )
    # Small backfills should not fan out into many tiny AI calls, but once the
    # evidence set is near production size keep bucket count stable across runs.
    if count < configured * size // 2:
        return natural
    return min(count, configured)


def _stable_story_bucket(story_id: int, bucket_count: int) -> int:
    if bucket_count <= 1:
        return 0
    digest = hashlib.sha256(f"insights-evidence:{int(story_id)}".encode("utf-8"))
    return int(digest.hexdigest()[:12], 16) % bucket_count


def _stable_evidence_batches(
    rows: Sequence[Any],
    batch_size: int,
) -> List[Sequence[Any]]:
    bucket_count = _evidence_cache_bucket_count(len(rows), batch_size)
    if bucket_count <= 0:
        return []
    buckets: List[List[Any]] = [[] for _ in range(bucket_count)]
    for row in rows:
        sid = int(row["id"])
        buckets[_stable_story_bucket(sid, bucket_count)].append(row)

    batches: List[Sequence[Any]] = []
    for bucket in buckets:
        if not bucket:
            continue
        ordered = sorted(bucket, key=lambda row: int(row["id"]))
        batches.extend(_row_batches(ordered, batch_size))
    return batches


def _evidence_batch_cache_key(
    *,
    target_date: str,
    start_date: str,
    batch_rows: Sequence[Any],
    feed_ranks: Mapping[int, Mapping[str, int]],
    comments_by_story: Mapping[int, Sequence[Any]],
) -> str:
    fingerprint = _insights_material_fingerprint(
        target_date=target_date,
        start_date=start_date,
        window_rows=batch_rows,
        feed_ranks=feed_ranks,
        comments_by_story=comments_by_story,
    )
    return f"insights:evidence-batch:v1:{target_date}:{fingerprint}"


def _unique_topic_key(raw_key: Any, seen: set[str], fallback: str) -> str:
    base = str(raw_key or "").strip() or fallback
    key = base
    suffix = 2
    while key in seen:
        key = f"{base}-{suffix}"
        suffix += 1
    seen.add(key)
    return key


def _merge_evidence_outputs(outputs: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    cards: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    assigned_ids: set[int] = set()
    excluded_ids: List[int] = []
    excluded_seen: set[int] = set()
    exclusion_reasons: Dict[str, Any] = {}
    input_count = 0

    for output in outputs:
        coverage = output.get("coverage") or {}
        if isinstance(coverage, Mapping):
            input_count += int(coverage.get("inputStoryCount") or 0)
        for item in output.get("evidenceCards") or []:
            if not isinstance(item, Mapping):
                continue
            card = dict(item)
            card["topicKey"] = _unique_topic_key(
                card.get("topicKey"),
                seen_keys,
                f"topic-{len(cards) + 1}",
            )
            for sid in card.get("storyIds") or []:
                try:
                    assigned_ids.add(int(sid))
                except (TypeError, ValueError):
                    continue
            cards.append(card)
        for sid in output.get("excludedStoryIds") or []:
            try:
                clean_sid = int(sid)
            except (TypeError, ValueError):
                continue
            if clean_sid not in excluded_seen:
                excluded_ids.append(clean_sid)
                excluded_seen.add(clean_sid)
        raw_reasons = output.get("exclusionReasons") or {}
        if isinstance(raw_reasons, Mapping):
            for key, value in raw_reasons.items():
                exclusion_reasons[str(key)] = value

    if input_count <= 0:
        input_count = len(assigned_ids) + len(excluded_ids)
    return {
        "evidenceCards": cards,
        "excludedStoryIds": excluded_ids,
        "exclusionReasons": exclusion_reasons,
        "coverage": {
            "inputStoryCount": input_count,
            "assignedStoryCount": len(assigned_ids),
            "excludedStoryCount": len(excluded_ids),
        },
    }


def _run_evidence_batches(
    agent: Any,
    evidence_rows: Sequence[Any],
    *,
    target_date: str,
    start_date: str,
    feed_ranks: Mapping[int, Mapping[str, int]],
    comments_by_story: Mapping[int, Sequence[Any]],
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    batches = _stable_evidence_batches(
        list(evidence_rows),
        settings.INSIGHTS_EVIDENCE_BATCH_STORIES,
    )
    stats = {
        "batches": len(batches),
        "hits": 0,
        "misses": 0,
    }
    if not batches:
        return (
            {
                "evidenceCards": [],
                "excludedStoryIds": [],
                "exclusionReasons": {},
                "coverage": {
                    "inputStoryCount": 0,
                    "assignedStoryCount": 0,
                    "excludedStoryCount": 0,
                },
            },
            stats,
        )

    def _run_one(index: int, batch_rows: Sequence[Any]) -> Tuple[int, Mapping[str, Any], bool]:
        cache_key = _evidence_batch_cache_key(
            target_date=target_date,
            start_date=start_date,
            batch_rows=batch_rows,
            feed_ranks=feed_ranks,
            comments_by_story=comments_by_story,
        )
        cached = _load_cached_evidence(cache_key)
        if cached is not None:
            return index, cached, True

        payload = build_evidence_input(
            batch_rows,
            target_date=target_date,
            start_date=start_date,
            feed_ranks=feed_ranks,
            comments_by_story=comments_by_story,
        )
        output = agent.run_evidence(payload)
        _store_cached_evidence(cache_key, output, len(batch_rows))
        return index, output, False

    outputs_by_index: List[Optional[Mapping[str, Any]]] = [None] * len(batches)
    worker_count = max(
        1,
        min(int(settings.INSIGHTS_EVIDENCE_WORKERS), len(batches)),
    )
    if worker_count == 1:
        for index, batch_rows in enumerate(batches):
            result_index, output, cache_hit = _run_one(index, batch_rows)
            outputs_by_index[result_index] = output
            stats["hits" if cache_hit else "misses"] += 1
    else:
        executor = ThreadPoolExecutor(max_workers=worker_count)
        futures: Dict[Any, int] = {}
        next_index = 0

        def _submit_next() -> None:
            nonlocal next_index
            if next_index >= len(batches):
                return
            futures[executor.submit(_run_one, next_index, batches[next_index])] = next_index
            next_index += 1

        try:
            for _ in range(worker_count):
                _submit_next()
            while futures:
                done, _pending = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    futures.pop(future, None)
                    result_index, output, cache_hit = future.result()
                    outputs_by_index[result_index] = output
                    stats["hits" if cache_hit else "misses"] += 1
                    _submit_next()
        except BaseException:
            for future in futures:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True)

    outputs: List[Mapping[str, Any]] = [
        output for output in outputs_by_index if output is not None
    ]
    if len(outputs) == 1:
        return dict(outputs[0]), stats
    return _merge_evidence_outputs(outputs), stats


def _evidence_cache_status(stats: Mapping[str, int]) -> str:
    batches = int(stats.get("batches") or 0)
    hits = int(stats.get("hits") or 0)
    misses = int(stats.get("misses") or 0)
    if batches <= 0:
        return "empty"
    if hits >= batches and misses == 0:
        return "hit"
    if hits == 0 and misses >= batches:
        return "miss"
    return "partial"


def build_topic_scout_input(
    evidence: Mapping[str, Any],
    *,
    target_date: str,
    story_refs: Optional[Mapping[int, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    _target_start, target_end_ts = repository.digest_date_epoch_bounds(target_date)
    refs = story_refs or {}
    cards = []
    for item in evidence.get("evidenceCards") or []:
        if not isinstance(item, Mapping):
            continue
        card = dict(item)
        card["metrics"] = _evidence_card_metrics(
            card,
            story_refs=refs,
            target_end_ts=target_end_ts,
        )
        cards.append(card)
    return {
        "date": target_date,
        "evidenceCoverage": evidence.get("coverage") or {},
        "evidenceCards": cards,
        "excludedStoryIds": evidence.get("excludedStoryIds") or [],
    }


def _selected_routes(scout: Mapping[str, Any]) -> Dict[str, List[str]]:
    routes: Dict[str, List[str]] = {}
    for item in scout.get("selectedTopics") or []:
        if not isinstance(item, Mapping):
            continue
        key = str(item.get("topicKey") or "")
        if not key:
            continue
        raw_routes = item.get("routes") or []
        routes[key] = [str(route) for route in raw_routes if str(route)]
    return routes


def _excluded_topic_keys(scout: Mapping[str, Any]) -> set[str]:
    keys: set[str] = set()
    for item in scout.get("excludedTopics") or []:
        if not isinstance(item, Mapping):
            continue
        key = str(item.get("topicKey") or "")
        if key:
            keys.add(key)
    return keys


def _scout_summary(scout: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "selectedTopics": scout.get("selectedTopics") or [],
        "excludedTopics": scout.get("excludedTopics") or [],
    }


def _enrich_evidence_cards(
    evidence: Mapping[str, Any],
    *,
    story_refs: Mapping[int, Mapping[str, Any]],
    target_date: str,
) -> List[Dict[str, Any]]:
    cards = []
    _target_start, target_end_ts = repository.digest_date_epoch_bounds(target_date)
    for item in evidence.get("evidenceCards") or []:
        if not isinstance(item, Mapping):
            continue
        story_ids = _card_story_ids(item)
        topic = str(item.get("topic") or "")
        cards.append(
            {
                "topicKey": str(item.get("topicKey") or ""),
                "topic": topic,
                "storyIds": story_ids,
                "storyRefs": [
                    dict(story_refs[sid])
                    for sid in story_ids
                    if sid in story_refs
                ],
                "synthesis": item.get("synthesis") or "",
                "painPoints": item.get("painPoints") or [],
                "opportunityAngles": item.get("opportunityAngles") or [],
                "debatePoints": item.get("debatePoints") or [],
                "commentSignals": item.get("commentSignals") or [],
                "storySignals": item.get("storySignals") or [],
                "metrics": _evidence_card_metrics(
                    item,
                    story_refs=story_refs,
                    target_end_ts=target_end_ts,
                ),
            }
        )
    return cards


def _cards_for_route(
    cards: Sequence[Mapping[str, Any]],
    scout: Mapping[str, Any],
    route: str,
    *,
    min_cards: int,
) -> List[Dict[str, Any]]:
    routes = _selected_routes(scout)
    excluded_keys = _excluded_topic_keys(scout)
    selected_keys = [
        key
        for key, route_names in routes.items()
        if key not in excluded_keys and route in route_names
    ]
    selected = [
        dict(card)
        for card in cards
        if str(card.get("topicKey") or "") in selected_keys
    ]
    if len(selected) >= min_cards:
        return selected
    selected_seen = {str(card.get("topicKey") or "") for card in selected}
    for card in cards:
        key = str(card.get("topicKey") or "")
        if key in selected_seen or key in excluded_keys:
            continue
        selected.append(dict(card))
        selected_seen.add(key)
        if len(selected) >= min_cards:
            break
    return selected


def build_routed_insights_inputs(
    *,
    target_date: str,
    today_rows: Sequence[Any],
    evidence: Mapping[str, Any],
    scout: Mapping[str, Any],
    story_refs: Mapping[int, Mapping[str, Any]],
    previous_insight: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    cards = _enrich_evidence_cards(
        evidence,
        story_refs=story_refs,
        target_date=target_date,
    )
    compact_scout = _scout_summary(scout)
    signals_input = {
        "date": target_date,
        "topicSummary": _build_today_topic_summary(today_rows),
        "topicScout": compact_scout,
        "evidenceCards": _cards_for_route(
            cards,
            scout,
            "signals",
            min_cards=SIGNALS_MIN_STORIES,
        ),
    }
    if previous_insight:
        signals_input["previousInsight"] = previous_insight.get("signals") or {}
    opportunities_input = {
        "topicScout": compact_scout,
        "candidates": _cards_for_route(
            cards,
            scout,
            "opportunities",
            min_cards=OPPORTUNITY_MIN_CANDIDATES,
        ),
    }
    if previous_insight:
        opportunities_input["previousInsight"] = (
            previous_insight.get("opportunities") or {}
        )
    debates_input = {
        "topicScout": compact_scout,
        "candidates": _cards_for_route(
            cards,
            scout,
            "debates",
            min_cards=DEBATE_MIN_CANDIDATES,
        ),
    }
    if previous_insight:
        debates_input["previousInsight"] = previous_insight.get("debates") or {}
    return signals_input, opportunities_input, debates_input


def _collect_story_reference_ids(*payloads: Mapping[str, Any]) -> List[int]:
    ids: List[int] = []

    def add_id(value: Any) -> None:
        try:
            ids.append(int(value))
        except (TypeError, ValueError):
            pass

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key in ("storyId", "linkedStoryId", "todayStoryId"):
                if key in value:
                    add_id(value[key])
            for key in ("linkedStoryIds", "todayStoryIds", "storyIds"):
                if key not in value or not isinstance(value[key], list):
                    continue
                for sid in value[key]:
                    add_id(sid)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    for payload in payloads:
        visit(payload)
    out = []
    seen = set()
    for sid in ids:
        if sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out


def _input_count(value: Mapping[str, Any], key: str) -> int:
    items = value.get(key)
    return len(items) if isinstance(items, list) else 0


def _first_input_count(value: Mapping[str, Any], keys: Sequence[str]) -> int:
    for key in keys:
        count = _input_count(value, key)
        if count > 0:
            return count
    return 0


def _insights_input_counts(
    *,
    signals_input: Mapping[str, Any],
    opportunities_input: Mapping[str, Any],
    debates_input: Mapping[str, Any],
) -> Dict[str, int]:
    return {
        "signals_stories": _first_input_count(signals_input, ("stories", "evidenceCards")),
        "opportunity_candidates": _input_count(opportunities_input, "candidates"),
        "debate_candidates": _input_count(debates_input, "candidates"),
    }


def _insights_input_gaps(counts: Mapping[str, int]) -> List[str]:
    checks = (
        ("signals_stories", SIGNALS_MIN_STORIES, "signals stories"),
        ("opportunity_candidates", OPPORTUNITY_MIN_CANDIDATES, "opportunity candidates"),
        ("debate_candidates", DEBATE_MIN_CANDIDATES, "debate candidates"),
    )
    gaps = []
    for key, minimum, label in checks:
        actual = int(counts.get(key) or 0)
        if actual < minimum:
            gaps.append(f"{label} {actual}/{minimum}")
    return gaps


def _run_final_agents(
    agent: Any,
    *,
    signals_input: Mapping[str, Any],
    opportunities_input: Mapping[str, Any],
    debates_input: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, float]]:
    tasks = (
        ("signals", agent.run_signals, signals_input),
        ("opportunities", agent.run_opportunities, opportunities_input),
        ("debates", agent.run_debates, debates_input),
    )
    worker_count = max(
        1,
        min(int(settings.INSIGHTS_FINAL_WORKERS), len(tasks)),
    )
    timings: Dict[str, float] = {}

    def _run_one(name: str, fn: Any, payload: Mapping[str, Any]) -> Tuple[str, Dict[str, Any], float]:
        started = time.monotonic()
        result = fn(payload)
        return name, result, _elapsed_seconds(started)

    results: Dict[str, Dict[str, Any]] = {}
    if worker_count == 1:
        for name, fn, payload in tasks:
            result_name, result, elapsed = _run_one(name, fn, payload)
            results[result_name] = result
            timings[f"{result_name}_seconds"] = elapsed
    else:
        executor = ThreadPoolExecutor(max_workers=worker_count)
        futures = {
            executor.submit(_run_one, name, fn, payload): name
            for name, fn, payload in tasks
        }
        try:
            for future in as_completed(futures):
                result_name, result, elapsed = future.result()
                results[result_name] = result
                timings[f"{result_name}_seconds"] = elapsed
        except BaseException:
            for future in futures:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True)

    return (
        results["signals"],
        results["opportunities"],
        results["debates"],
        timings,
    )


def _usage_checkpoint(agent: Any) -> Optional[Any]:
    fn = getattr(agent, "usage_checkpoint", None)
    if not callable(fn):
        return None
    try:
        checkpoint = fn()
        return checkpoint if checkpoint is not None else None
    except Exception:  # noqa: BLE001
        return None


def _usage_since(agent: Any, checkpoint: Optional[Any]) -> Optional[dict]:
    if checkpoint is None:
        return None
    fn = getattr(agent, "usage_summary_since", None)
    if not callable(fn):
        return None
    try:
        _next, usage = fn(checkpoint)
        return usage if isinstance(usage, dict) else None
    except Exception:  # noqa: BLE001
        return None


def _validate_final_payload(payload: Mapping[str, Any], allowed_story_ids: Sequence[int]) -> None:
    required = (
        "version",
        "date",
        "asOf",
        "asOfLabel",
        "generatedAt",
        "window",
        "access",
        "headline",
        "summary",
        "stats",
        "signals",
        "opportunities",
        "debates",
    )
    for key in required:
        if key not in payload:
            raise InsightsValidationError(f"insights payload missing {key}")
    if len(payload.get("signals") or []) != 3:
        raise InsightsValidationError("signals must contain exactly 3 items")
    if not (OPPORTUNITY_MIN_CANDIDATES <= len(payload.get("opportunities") or []) <= 5):
        raise InsightsValidationError("opportunities must contain 3-5 items")
    if not (DEBATE_MIN_CANDIDATES <= len(payload.get("debates") or []) <= 4):
        raise InsightsValidationError("debates must contain 2-4 items")
    allowed = {int(sid) for sid in allowed_story_ids}
    for sid in _collect_story_reference_ids(payload):
        if int(sid) not in allowed:
            raise InsightsValidationError(f"linkedStoryId {sid} outside 7-day window")
    if contains_forbidden_words(payload):
        raise InsightsValidationError("insights payload contains forbidden words")


def _build_stats(
    *,
    window_rows: Sequence[Any],
    source_story_ids: Sequence[int],
    debates: Sequence[Mapping[str, Any]],
) -> List[Dict[str, str]]:
    topics = {_row_topic_id(row) for row in window_rows if _row_topic_id(row)}
    strong_debates = sum(1 for item in debates if int(item.get("intensity") or 0) >= 80)
    return [
        {"label": "追踪主题", "value": str(len(topics))},
        {"label": "引用帖子", "value": str(len(set(int(sid) for sid in source_story_ids)))},
        {"label": "强分歧", "value": str(strong_debates)},
    ]


def _previous_insight_context(row: Any) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    payload = _json_loads(row["payload"], {})
    if not isinstance(payload, Mapping):
        return None
    return {
        "headline": payload.get("headline") or "",
        "summary": payload.get("summary") or "",
        "signals": {
            "headline": payload.get("headline") or "",
            "summary": payload.get("summary") or "",
            "items": [
                {
                    "title": item.get("title") or "",
                    "brief": item.get("brief") or "",
                    "label": item.get("label") or "",
                }
                for item in payload.get("signals") or []
                if isinstance(item, Mapping)
            ],
        },
        "opportunities": {
            "items": [
                {
                    "title": item.get("title") or "",
                    "thesis": item.get("thesis") or "",
                    "whyNow": item.get("whyNow") or "",
                    "risk": item.get("risk") or "",
                    "linkedStoryIds": item.get("linkedStoryIds") or [],
                }
                for item in payload.get("opportunities") or []
                if isinstance(item, Mapping)
            ],
        },
        "debates": {
            "items": [
                {
                    "topic": item.get("topic") or "",
                    "verdict": item.get("verdict") or "",
                    "support": item.get("support") or "",
                    "oppose": item.get("oppose") or "",
                    "watch": item.get("watch") or "",
                }
                for item in payload.get("debates") or []
                if isinstance(item, Mapping)
            ],
        },
    }


def _comment_count(comments_by_story: Mapping[int, Sequence[Any]]) -> int:
    return sum(len(items) for items in comments_by_story.values())


def _analysis_meta_key(target_date: str) -> str:
    return f"insights:analysis_fingerprint:{target_date}"


def _analysis_fingerprint_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _analysis_fingerprint_value(child)
            for key, child in sorted(value.items(), key=lambda item: str(item[0]))
            if str(key) not in ("previousInsight", "reason")
        }
    if isinstance(value, list):
        return [_analysis_fingerprint_value(item) for item in value]
    return value


def _insights_analysis_fingerprint(
    *,
    target_date: str,
    signals_input: Mapping[str, Any],
    opportunities_input: Mapping[str, Any],
    debates_input: Mapping[str, Any],
) -> str:
    hasher = hashlib.sha256()
    _hash_update_json(
        hasher,
        {
            "stage": "insights-analysis",
            "schema": 1,
            "date": target_date,
            "signals": _analysis_fingerprint_value(signals_input),
            "opportunities": _analysis_fingerprint_value(opportunities_input),
            "debates": _analysis_fingerprint_value(debates_input),
        },
    )
    return hasher.hexdigest()


def _attach_stage_timings(
    summary: Dict[str, Any],
    stage_timings: Optional[Mapping[str, float]],
) -> None:
    if not stage_timings:
        return
    summary["stage_seconds"] = {
        str(key): round(max(0.0, float(value)), 3)
        for key, value in sorted(stage_timings.items())
    }
    summary["concurrency"] = {
        "evidence_workers": int(settings.INSIGHTS_EVIDENCE_WORKERS),
        "final_workers": int(settings.INSIGHTS_FINAL_WORKERS),
    }


def _insights_run_summary(
    *,
    today_rows: Sequence[Any],
    evidence_rows: Sequence[Any],
    comments_by_story: Mapping[int, Sequence[Any]],
    material_fingerprint: str = "",
    analysis_fingerprint: str = "",
    evidence_cache: str = "",
    evidence_cache_stats: Optional[Mapping[str, int]] = None,
    topic_scout: Optional[Mapping[str, Any]] = None,
    stage_timings: Optional[Mapping[str, float]] = None,
    skip_reason: str = "",
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "today_story_count": len(today_rows),
        "evidence_story_count": len(evidence_rows),
        "comment_count": _comment_count(comments_by_story),
    }
    if material_fingerprint:
        out["material_fingerprint"] = material_fingerprint
    if analysis_fingerprint:
        out["analysis_fingerprint"] = analysis_fingerprint
    if evidence_cache:
        out["evidence_cache"] = evidence_cache
    if evidence_cache_stats:
        out["evidence_cache_batches"] = int(evidence_cache_stats.get("batches") or 0)
        out["evidence_cache_hits"] = int(evidence_cache_stats.get("hits") or 0)
        out["evidence_cache_misses"] = int(evidence_cache_stats.get("misses") or 0)
    if topic_scout is not None:
        out["topic_scout_selected_count"] = len(topic_scout.get("selectedTopics") or [])
        out["topic_scout_excluded_count"] = len(topic_scout.get("excludedTopics") or [])
    _attach_stage_timings(out, stage_timings)
    if skip_reason:
        out["skip_reason"] = skip_reason
    return out


def _finish_run_record(
    *,
    run_id: str,
    date: str,
    started_at: int,
    status: str,
    model_usage: Optional[dict] = None,
    summary: Optional[dict] = None,
    error: str = "",
) -> None:
    conn = db.connect()
    try:
        with db.transaction(conn):
            repository.record_insight_run(
                conn,
                run_id=run_id,
                date=date,
                started_at=started_at,
                finished_at=repository.now_seconds(),
                status=status,
                model_usage=model_usage,
                summary=summary,
                error=error,
            )
    finally:
        conn.close()


def run_insights_once(
    date: Optional[str] = None,
    force: bool = False,
    ai_agent: Optional[Any] = None,
) -> dict:
    target_date = _target_date(date)
    run_id = f"insights-{target_date}-{uuid.uuid4().hex[:8]}"
    started_at = repository.now_seconds()
    total_started = time.monotonic()
    stage_timings: Dict[str, float] = {}

    if not settings.INSIGHTS_ENABLED:
        return {"status": "skipped", "reason": "disabled", "date": target_date}

    window_days = int(settings.INSIGHTS_WINDOW_DAYS)
    start_ts, end_ts, start_date = _window_bounds(target_date, window_days)
    today_start, today_end = repository.digest_date_epoch_bounds(target_date)

    input_started = time.monotonic()
    conn = db.connect()
    try:
        window_rows = repository.candidate_rows_for_insights(
            conn, start_ts=start_ts, end_ts=end_ts
        )
        today_rows = [
            row for row in window_rows
            if today_start <= int(row["hn_time"] or 0) < today_end
        ]
        candidate_story_ids = [int(row["id"]) for row in window_rows]
        existing_insight_row = repository.get_insight_row(conn, target_date)
        if len(today_rows) < int(settings.INSIGHTS_MIN_TODAY_STORIES):
            reason = "insufficient_today_stories"
            summary = {
                "today_story_count": len(today_rows),
                "skip_reason": reason,
            }
            stage_timings["input_seconds"] = _elapsed_seconds(input_started)
            stage_timings["total_seconds"] = _elapsed_seconds(total_started)
            _attach_stage_timings(summary, stage_timings)
            _finish_run_record(
                run_id=run_id,
                date=target_date,
                started_at=started_at,
                status="skipped",
                summary=summary,
                error=reason,
            )
            return {
                "status": "skipped",
                "reason": reason,
                "date": target_date,
                "today_story_count": len(today_rows),
                "run_summary": summary,
            }
        if not force and not repository.insight_needs_update(
            conn,
            target_date,
            settings.INSIGHTS_UPDATE_INTERVAL_SECONDS,
            candidate_story_ids,
        ):
            reason = "not_due"
            summary = {"skip_reason": reason}
            stage_timings["input_seconds"] = _elapsed_seconds(input_started)
            stage_timings["total_seconds"] = _elapsed_seconds(total_started)
            _attach_stage_timings(summary, stage_timings)
            _finish_run_record(
                run_id=run_id,
                date=target_date,
                started_at=started_at,
                status="skipped",
                summary=summary,
                error=reason,
            )
            return {
                "status": "skipped",
                "reason": reason,
                "date": target_date,
                "run_summary": summary,
            }

        feed_ranks = repository.insight_feed_ranks_for_story_ids(
            conn, candidate_story_ids
        )
        today_rows = _limit_insight_rows(
            today_rows,
            max_rows=settings.INSIGHTS_MAX_TODAY_STORIES,
            feed_ranks=feed_ranks,
        )
        evidence_rows = _select_evidence_rows(
            window_rows,
            today_rows=today_rows,
            feed_ranks=feed_ranks,
            max_rows=settings.INSIGHTS_EVIDENCE_MAX_STORIES,
        )
        comment_ids = [int(row["id"]) for row in evidence_rows]
        comments_by_story = repository.insight_comment_rows_for_story_ids(
            conn,
            comment_ids,
            limit_per_story=settings.INSIGHTS_EVIDENCE_COMMENT_LIMIT_PER_STORY,
        )
        material_fingerprint = _insights_material_fingerprint(
            target_date=target_date,
            start_date=start_date,
            window_rows=evidence_rows,
            feed_ranks=feed_ranks,
            comments_by_story=comments_by_story,
        )
        previous_insight = _previous_insight_context(existing_insight_row)
    finally:
        conn.close()
    stage_timings["input_seconds"] = _elapsed_seconds(input_started)

    agent = None
    usage_checkpoint = None
    run_summary: Optional[dict] = None

    try:
        preflight_started = time.monotonic()
        preflight_counts = {
            "signals_stories": len(today_rows),
            "evidence_stories": len(evidence_rows),
        }
        preflight_gaps: List[str] = []
        if preflight_counts["signals_stories"] < SIGNALS_MIN_STORIES:
            preflight_gaps.append(
                f"signals stories {preflight_counts['signals_stories']}/{SIGNALS_MIN_STORIES}"
            )
        required_evidence_stories = max(
            SIGNALS_MIN_STORIES,
            OPPORTUNITY_MIN_CANDIDATES,
            DEBATE_MIN_CANDIDATES,
        )
        if preflight_counts["evidence_stories"] < required_evidence_stories:
            preflight_gaps.append(
                "evidence stories "
                f"{preflight_counts['evidence_stories']}/{required_evidence_stories}"
            )
        if preflight_gaps:
            reason = "insufficient_insights_inputs"
            stage_timings["preflight_seconds"] = _elapsed_seconds(preflight_started)
            stage_timings["total_seconds"] = _elapsed_seconds(total_started)
            summary = _insights_run_summary(
                today_rows=today_rows,
                evidence_rows=evidence_rows,
                comments_by_story=comments_by_story,
                material_fingerprint=material_fingerprint,
                stage_timings=stage_timings,
                skip_reason=reason,
            )
            _finish_run_record(
                run_id=run_id,
                date=target_date,
                started_at=started_at,
                status="skipped",
                summary=summary,
                error=f"{reason}: {'; '.join(preflight_gaps)}",
            )
            return {
                "status": "skipped",
                "reason": reason,
                "date": target_date,
                "input_counts": preflight_counts,
                "input_gaps": preflight_gaps,
                "run_summary": summary,
            }
        stage_timings["preflight_seconds"] = _elapsed_seconds(preflight_started)

        if (
            not force
            and material_fingerprint
            and str(_row_value(existing_insight_row, "material_fingerprint", "") or "")
            == material_fingerprint
        ):
            reason = "material_unchanged"
            stage_timings["total_seconds"] = _elapsed_seconds(total_started)
            summary = _insights_run_summary(
                today_rows=today_rows,
                evidence_rows=evidence_rows,
                comments_by_story=comments_by_story,
                material_fingerprint=material_fingerprint,
                stage_timings=stage_timings,
                skip_reason=reason,
            )
            _finish_run_record(
                run_id=run_id,
                date=target_date,
                started_at=started_at,
                status="skipped",
                summary=summary,
                error=reason,
            )
            return {
                "status": "skipped",
                "reason": reason,
                "date": target_date,
                "material_fingerprint": material_fingerprint,
                "run_summary": summary,
            }

        agent_started = time.monotonic()
        try:
            agent = ai_agent or InsightsAgentRunner()
            usage_checkpoint = _usage_checkpoint(agent)
        finally:
            stage_timings["agent_init_seconds"] = _elapsed_seconds(agent_started)

        evidence_started = time.monotonic()
        try:
            evidence_out, evidence_cache_stats = _run_evidence_batches(
                agent,
                evidence_rows,
                target_date=target_date,
                start_date=start_date,
                feed_ranks=feed_ranks,
                comments_by_story=comments_by_story,
            )
        finally:
            stage_timings["evidence_seconds"] = _elapsed_seconds(evidence_started)
        evidence_cache_status = _evidence_cache_status(evidence_cache_stats)
        routing_started = time.monotonic()
        story_refs = _story_refs_by_id(window_rows, feed_ranks)
        topic_scout_input = build_topic_scout_input(
            evidence_out,
            target_date=target_date,
            story_refs=story_refs,
        )
        stage_timings["topic_scout_input_seconds"] = _elapsed_seconds(routing_started)
        topic_scout_started = time.monotonic()
        try:
            topic_scout_out = agent.run_topic_scout(topic_scout_input)
        finally:
            stage_timings["topic_scout_seconds"] = _elapsed_seconds(topic_scout_started)
        routing_started = time.monotonic()
        signals_input, opportunities_input, debates_input = (
            build_routed_insights_inputs(
                target_date=target_date,
                today_rows=today_rows,
                evidence=evidence_out,
                scout=topic_scout_out,
                story_refs=story_refs,
                previous_insight=previous_insight,
            )
        )
        stage_timings["routing_seconds"] = _elapsed_seconds(routing_started)
        run_summary = _insights_run_summary(
            today_rows=today_rows,
            evidence_rows=evidence_rows,
            comments_by_story=comments_by_story,
            material_fingerprint=material_fingerprint,
            evidence_cache=evidence_cache_status,
            evidence_cache_stats=evidence_cache_stats,
            topic_scout=topic_scout_out,
            stage_timings=stage_timings,
        )

        input_counts = _insights_input_counts(
            signals_input=signals_input,
            opportunities_input=opportunities_input,
            debates_input=debates_input,
        )
        input_gaps = _insights_input_gaps(input_counts)
        if input_gaps:
            reason = "insufficient_insights_inputs"
            stage_timings["total_seconds"] = _elapsed_seconds(total_started)
            skip_summary = dict(run_summary)
            skip_summary["skip_reason"] = reason
            _attach_stage_timings(skip_summary, stage_timings)
            _finish_run_record(
                run_id=run_id,
                date=target_date,
                started_at=started_at,
                status="skipped",
                model_usage=_usage_since(agent, usage_checkpoint),
                summary=skip_summary,
                error=f"{reason}: {'; '.join(input_gaps)}",
            )
            return {
                "status": "skipped",
                "reason": reason,
                "date": target_date,
                "input_counts": input_counts,
                "input_gaps": input_gaps,
                "run_summary": skip_summary,
            }

        analysis_started = time.monotonic()
        try:
            analysis_fingerprint = _insights_analysis_fingerprint(
                target_date=target_date,
                signals_input=signals_input,
                opportunities_input=opportunities_input,
                debates_input=debates_input,
            )
        finally:
            stage_timings["analysis_fingerprint_seconds"] = _elapsed_seconds(
                analysis_started
            )
        run_summary["analysis_fingerprint"] = analysis_fingerprint
        if not force:
            analysis_gate_started = time.monotonic()
            try:
                conn = db.connect()
                try:
                    previous_analysis_fingerprint = repository.get_meta(
                        conn,
                        _analysis_meta_key(target_date),
                    )
                finally:
                    conn.close()
            finally:
                stage_timings["analysis_gate_seconds"] = _elapsed_seconds(
                    analysis_gate_started
                )
            if previous_analysis_fingerprint == analysis_fingerprint:
                reason = "analysis_unchanged"
                stage_timings["total_seconds"] = _elapsed_seconds(total_started)
                skip_summary = dict(run_summary)
                skip_summary["skip_reason"] = reason
                _attach_stage_timings(skip_summary, stage_timings)
                _finish_run_record(
                    run_id=run_id,
                    date=target_date,
                    started_at=started_at,
                    status="skipped",
                    model_usage=_usage_since(agent, usage_checkpoint),
                    summary=skip_summary,
                    error=reason,
                )
                return {
                    "status": "skipped",
                    "reason": reason,
                    "date": target_date,
                    "material_fingerprint": material_fingerprint,
                    "analysis_fingerprint": analysis_fingerprint,
                    "run_summary": skip_summary,
                }

        final_started = time.monotonic()
        try:
            signals_out, opportunities_out, debates_out, final_timings = _run_final_agents(
                agent,
                signals_input=signals_input,
                opportunities_input=opportunities_input,
                debates_input=debates_input,
            )
        finally:
            stage_timings["final_agents_wall_seconds"] = _elapsed_seconds(
                final_started
            )
        stage_timings.update(final_timings)

        payload_started = time.monotonic()
        try:
            source_story_ids = _collect_story_reference_ids(
                signals_out,
                opportunities_out,
                debates_out,
            )
            debates = debates_out["debates"]
            payload = {
                "version": INSIGHTS_VERSION,
                "date": target_date,
                "asOf": target_date,
                "asOfLabel": _as_of_label(target_date),
                "generatedAt": _now_iso_in_digest_tz(),
                "window": INSIGHTS_WINDOW_LABEL,
                "access": {"unlocked": True, "tier": "pro"},
                "headline": signals_out["headline"],
                "summary": signals_out["summary"],
                "stats": _build_stats(
                    window_rows=window_rows,
                    source_story_ids=source_story_ids,
                    debates=debates,
                ),
                "signals": signals_out["signals"],
                "opportunities": opportunities_out["opportunities"],
                "debates": debates,
            }
            payload = sanitize_forbidden_words(payload)
            _validate_final_payload(payload, candidate_story_ids)
        finally:
            stage_timings["payload_seconds"] = _elapsed_seconds(payload_started)
        stage_timings["total_seconds"] = _elapsed_seconds(total_started)
        _attach_stage_timings(run_summary, stage_timings)

        model_usage = _usage_since(agent, usage_checkpoint)
        conn = db.connect()
        try:
            with db.transaction(conn):
                changed = repository.upsert_insight(
                    conn,
                    target_date,
                    payload,
                    source_story_ids,
                    repository.now_seconds(),
                    window_days,
                    model_usage=model_usage,
                    material_fingerprint=material_fingerprint,
                )
                repository.set_meta(
                    conn,
                    _analysis_meta_key(target_date),
                    analysis_fingerprint,
                )
                if changed:
                    repository.bump_catalog_version(conn)
                repository.record_insight_run(
                    conn,
                    run_id=run_id,
                    date=target_date,
                    started_at=started_at,
                    finished_at=repository.now_seconds(),
                    status="ok",
                    model_usage=model_usage,
                    summary=run_summary,
                    error="",
                )
        finally:
            conn.close()
        return {
            "status": "ok",
            "changed": bool(changed),
            "date": target_date,
            "source_story_ids_count": len(set(source_story_ids)),
            "evidence_cache": evidence_cache_status,
            "material_fingerprint": material_fingerprint,
            "analysis_fingerprint": analysis_fingerprint,
            "run_summary": run_summary or {},
            "agent_usage": model_usage or {},
        }
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
        log.exception("insights generation failed for %s: %s", target_date, exc)
        model_usage = _usage_since(agent, usage_checkpoint)
        stage_timings["total_seconds"] = _elapsed_seconds(total_started)
        failed_summary = dict(run_summary or {})
        failed_summary["failed"] = True
        _attach_stage_timings(failed_summary, stage_timings)
        _finish_run_record(
            run_id=run_id,
            date=target_date,
            started_at=started_at,
            status="failed",
            model_usage=model_usage,
            summary=failed_summary,
            error=error,
        )
        return {
            "status": "failed",
            "date": target_date,
            "error": error,
            "run_summary": failed_summary,
            "agent_usage": model_usage or {},
        }


__all__ = [
    "build_evidence_input",
    "build_debate_input",
    "build_opportunity_input",
    "build_routed_insights_inputs",
    "build_today_signals_input",
    "build_topic_scout_input",
    "run_insights_once",
]
