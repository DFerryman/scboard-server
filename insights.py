"""Server-side insights generation pipeline."""

from __future__ import annotations

import html
import hashlib
import json
import logging
import math
import re
import uuid
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
TREND_HEAT_MIN_TOPICS = 5
OPPORTUNITY_MIN_CANDIDATES = 3
DEBATE_MIN_CANDIDATES = 2
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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


def _date_list(start_date: str, days: int) -> List[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]


def _daily_topic_activity_score(item: Mapping[str, Any]) -> float:
    count = max(0, int(item.get("count") or 0))
    score_sum = max(0, int(item.get("scoreSum") or 0))
    descendants_sum = max(0, int(item.get("descendantsSum") or 0))
    if count <= 0:
        return 0.0
    # Raw activity is intentionally unbounded. Display heat is normalized
    # against today's peer topics later so multiple active topics do not all
    # flatten into 100.
    return (
        count * 14.0
        + math.log1p(score_sum) * 8.0
        + math.log1p(descendants_sum) * 10.0
    )


def _relative_topic_heat(score: float, max_today_score: float) -> int:
    if score <= 0 or max_today_score <= 0:
        return 0
    return max(0, min(100, int(round(100.0 * math.sqrt(score / max_today_score)))))


def _trend_key(
    today_heat: float,
    previous_avg_heat: float,
    *,
    today_count: int,
    previous_avg_count: float,
    total_count: int,
) -> str:
    heat_delta = today_heat - previous_avg_heat
    if (
        (heat_delta >= 18 and today_heat >= 55)
        or (today_count >= previous_avg_count * 2 + 1 and today_count >= 2)
    ):
        return "burst"
    if heat_delta >= 6:
        return "rising"
    if heat_delta <= -6 and total_count > 1:
        return "cooling"
    return "stable"


def build_trend_heat_input(
    window_rows: Sequence[Any],
    *,
    target_date: str,
    start_date: str,
) -> Dict[str, Any]:
    dates = _date_list(start_date, settings.INSIGHTS_WINDOW_DAYS)
    by_topic: Dict[str, Dict[str, Any]] = {}
    for row in window_rows:
        topic = _row_topic_label(row)
        day = repository.date_in_digest_tz(int(row["hn_time"] or 0))
        if day not in dates:
            continue
        bucket = by_topic.setdefault(
            topic,
            {
                "topic": topic,
                "daily": {
                    d: {"date": d, "count": 0, "scoreSum": 0, "descendantsSum": 0}
                    for d in dates
                },
                "todayStoryIds": [],
                "sampleTitles": [],
            },
        )
        stats = bucket["daily"][day]
        stats["count"] += 1
        stats["scoreSum"] += int(row["score"] or 0)
        stats["descendantsSum"] += int(row["descendants"] or 0)
        title = row["title_zh"] or row["title_en"] or ""
        if day == target_date:
            bucket["todayStoryIds"].append(int(row["id"]))
        if title:
            bucket["sampleTitles"].append(title)

    scored = []
    max_today_score = 0.0
    for topic, bucket in by_topic.items():
        daily = [bucket["daily"][d] for d in dates]
        today = daily[-1]
        today_score = _daily_topic_activity_score(today)
        bucket["_todayActivityScore"] = today_score
        max_today_score = max(max_today_score, today_score)

    for topic, bucket in by_topic.items():
        daily = [bucket["daily"][d] for d in dates]
        today = daily[-1]
        previous = daily[:-1] or daily[-1:]
        prev_count = sum(item["count"] for item in previous) / max(1, len(previous))
        prev_score = sum(item["scoreSum"] for item in previous) / max(1, len(previous))
        prev_desc = sum(item["descendantsSum"] for item in previous) / max(1, len(previous))
        today_activity_score = float(bucket.get("_todayActivityScore") or 0.0)
        previous_avg_activity_score = sum(
            _daily_topic_activity_score(item) for item in previous
        ) / max(1, len(previous))
        today_heat = _relative_topic_heat(today_activity_score, max_today_score)
        previous_avg_heat = _relative_topic_heat(
            previous_avg_activity_score,
            max_today_score,
        )
        count_delta = today["count"] - prev_count
        score_delta = today["scoreSum"] - prev_score
        descendants_delta = today["descendantsSum"] - prev_desc
        heat_delta = today_heat - previous_avg_heat
        rank_score = today_heat + max(0.0, heat_delta) * 0.75 + min(
            10.0,
            sum(item["count"] for item in daily),
        )
        scored.append(
            {
                "topic": topic,
                "daily": daily,
                "todayStoryIds": bucket["todayStoryIds"],
                "sampleTitles": bucket["sampleTitles"],
                "countDelta": round(count_delta, 2),
                "scoreDelta": round(score_delta, 2),
                "descendantsDelta": round(descendants_delta, 2),
                "heatDelta": round(heat_delta, 2),
                "activityScore": round(today_activity_score, 2),
                "previousAvgActivityScore": round(previous_avg_activity_score, 2),
                "previousHeat": int(previous_avg_heat),
                "_todayHeat": today_heat,
                "_rankScore": rank_score,
                "trendKey": _trend_key(
                    today_heat,
                    previous_avg_heat,
                    today_count=int(today["count"]),
                    previous_avg_count=prev_count,
                    total_count=sum(item["count"] for item in daily),
                ),
            }
        )
    scored.sort(key=lambda item: (item["_rankScore"], len(item["todayStoryIds"])), reverse=True)
    out_items = []
    for item in scored:
        heat = int(round(float(item["_todayHeat"])))
        delta = int(round(float(item["heatDelta"])))
        sign = "+" if delta >= 0 else ""
        out_items.append(
            {
                **{
                    k: v
                    for k, v in item.items()
                    if k not in ("_todayHeat", "_rankScore")
                },
                "heat": max(0, min(100, heat)),
                "deltaText": f"{sign}{int(round(delta))} / 24h",
            }
        )
    return {"date": target_date, "topicDailyStats": out_items}


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


def _insights_evidence_cache_key(
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
            "schema": 3,
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
    return f"insights:evidence:v3:{target_date}:{hasher.hexdigest()}"


def _load_cached_evidence(cache_key: str) -> Optional[Dict[str, Any]]:
    conn = db.connect()
    try:
        row = repository.get_insight_evidence_cache(conn, cache_key)
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
    finally:
        conn.close()


def _row_batches(rows: Sequence[Any], batch_size: int) -> List[Sequence[Any]]:
    size = max(1, int(batch_size))
    return [rows[index:index + size] for index in range(0, len(rows), size)]


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
) -> Dict[str, Any]:
    batches = _row_batches(
        list(evidence_rows),
        settings.INSIGHTS_EVIDENCE_BATCH_STORIES,
    )
    if not batches:
        return {
            "evidenceCards": [],
            "excludedStoryIds": [],
            "exclusionReasons": {},
            "coverage": {
                "inputStoryCount": 0,
                "assignedStoryCount": 0,
                "excludedStoryCount": 0,
            },
        }
    outputs: List[Mapping[str, Any]] = []
    for batch_rows in batches:
        payload = build_evidence_input(
            batch_rows,
            target_date=target_date,
            start_date=start_date,
            feed_ranks=feed_ranks,
            comments_by_story=comments_by_story,
        )
        outputs.append(agent.run_evidence(payload))
    if len(outputs) == 1:
        return dict(outputs[0])
    return _merge_evidence_outputs(outputs)


def build_topic_scout_input(
    evidence: Mapping[str, Any],
    trends_input: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "date": trends_input.get("date") or "",
        "evidenceCoverage": evidence.get("coverage") or {},
        "evidenceCards": evidence.get("evidenceCards") or [],
        "excludedStoryIds": evidence.get("excludedStoryIds") or [],
        "topicDailyStats": trends_input.get("topicDailyStats") or [],
    }


def _topic_stats_by_topic(trends_input: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    out: Dict[str, Mapping[str, Any]] = {}
    for item in trends_input.get("topicDailyStats") or []:
        if isinstance(item, Mapping):
            topic = str(item.get("topic") or "")
            if topic:
                out[topic] = item
    return out


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
    trends_input: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    metrics_by_topic = _topic_stats_by_topic(trends_input)
    cards = []
    for item in evidence.get("evidenceCards") or []:
        if not isinstance(item, Mapping):
            continue
        story_ids = []
        for raw_id in item.get("storyIds") or []:
            try:
                sid = int(raw_id)
            except (TypeError, ValueError):
                continue
            if sid not in story_ids:
                story_ids.append(sid)
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
                "metrics": dict(metrics_by_topic.get(topic, {})),
                "synthesis": item.get("synthesis") or "",
                "painPoints": item.get("painPoints") or [],
                "opportunityAngles": item.get("opportunityAngles") or [],
                "debatePoints": item.get("debatePoints") or [],
                "commentSignals": item.get("commentSignals") or [],
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


def _trend_stats_for_scout(
    trends_input: Mapping[str, Any],
    scout: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    stats = [
        dict(item)
        for item in trends_input.get("topicDailyStats") or []
        if isinstance(item, Mapping)
    ]
    routes = _selected_routes(scout)
    excluded_keys = _excluded_topic_keys(scout)
    trend_keys = {
        key
        for key, route_names in routes.items()
        if key not in excluded_keys and "trends" in route_names
    }
    topic_keys_by_topic = {
        str(item.get("topic") or ""): str(item.get("topicKey") or "")
        for item in scout.get("evidenceCards") or []
        if isinstance(item, Mapping)
    }
    routed = [
        item
        for item in stats
        if topic_keys_by_topic.get(str(item.get("topic") or "")) in trend_keys
    ]
    if len(routed) >= TREND_HEAT_MIN_TOPICS:
        return routed
    seen_topics = {str(item.get("topic") or "") for item in routed}
    for item in stats:
        topic = str(item.get("topic") or "")
        if topic in seen_topics or topic_keys_by_topic.get(topic) in excluded_keys:
            continue
        routed.append(item)
        seen_topics.add(topic)
        if len(routed) >= TREND_HEAT_MIN_TOPICS:
            break
    return routed


def build_routed_insights_inputs(
    *,
    target_date: str,
    today_rows: Sequence[Any],
    trends_input: Mapping[str, Any],
    evidence: Mapping[str, Any],
    scout: Mapping[str, Any],
    story_refs: Mapping[int, Mapping[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    cards = _enrich_evidence_cards(
        evidence,
        story_refs=story_refs,
        trends_input=trends_input,
    )
    scout_with_cards = {**scout, "evidenceCards": cards}
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
    routed_trend_stats = _trend_stats_for_scout(trends_input, scout_with_cards)
    trends_routed_input = {
        **dict(trends_input),
        "topicDailyStats": routed_trend_stats,
        "topicScout": compact_scout,
    }
    opportunities_input = {
        "topicScout": compact_scout,
        "candidates": _cards_for_route(
            cards,
            scout,
            "opportunities",
            min_cards=OPPORTUNITY_MIN_CANDIDATES,
        ),
    }
    debates_input = {
        "topicScout": compact_scout,
        "candidates": _cards_for_route(
            cards,
            scout,
            "debates",
            min_cards=DEBATE_MIN_CANDIDATES,
        ),
    }
    return signals_input, trends_routed_input, opportunities_input, debates_input


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
    trends_input: Mapping[str, Any],
    opportunities_input: Mapping[str, Any],
    debates_input: Mapping[str, Any],
) -> Dict[str, int]:
    return {
        "signals_stories": _first_input_count(signals_input, ("stories", "evidenceCards")),
        "trend_topics": _input_count(trends_input, "topicDailyStats"),
        "opportunity_candidates": _input_count(opportunities_input, "candidates"),
        "debate_candidates": _input_count(debates_input, "candidates"),
    }


def _insights_input_gaps(counts: Mapping[str, int]) -> List[str]:
    checks = (
        ("signals_stories", SIGNALS_MIN_STORIES, "signals stories"),
        ("trend_topics", TREND_HEAT_MIN_TOPICS, "trend topics"),
        ("opportunity_candidates", OPPORTUNITY_MIN_CANDIDATES, "opportunity candidates"),
        ("debate_candidates", DEBATE_MIN_CANDIDATES, "debate candidates"),
    )
    gaps = []
    for key, minimum, label in checks:
        actual = int(counts.get(key) or 0)
        if actual < minimum:
            gaps.append(f"{label} {actual}/{minimum}")
    return gaps


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
        "trendHeatmap",
        "opportunities",
        "debates",
    )
    for key in required:
        if key not in payload:
            raise InsightsValidationError(f"insights payload missing {key}")
    if len(payload.get("signals") or []) != 3:
        raise InsightsValidationError("signals must contain exactly 3 items")
    trend_items = ((payload.get("trendHeatmap") or {}).get("items") or [])
    if not (TREND_HEAT_MIN_TOPICS <= len(trend_items) <= 8):
        raise InsightsValidationError("trendHeatmap.items must contain 5-8 items")
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


def _finish_run_record(
    *,
    run_id: str,
    date: str,
    started_at: int,
    status: str,
    model_usage: Optional[dict] = None,
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

    if not settings.INSIGHTS_ENABLED:
        return {"status": "skipped", "reason": "disabled", "date": target_date}

    window_days = int(settings.INSIGHTS_WINDOW_DAYS)
    start_ts, end_ts, start_date = _window_bounds(target_date, window_days)
    today_start, today_end = repository.digest_date_epoch_bounds(target_date)

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
        if len(today_rows) < int(settings.INSIGHTS_MIN_TODAY_STORIES):
            reason = "insufficient_today_stories"
            _finish_run_record(
                run_id=run_id,
                date=target_date,
                started_at=started_at,
                status="skipped",
                error=reason,
            )
            return {
                "status": "skipped",
                "reason": reason,
                "date": target_date,
                "today_story_count": len(today_rows),
            }
        if not force and not repository.insight_needs_update(
            conn,
            target_date,
            settings.INSIGHTS_UPDATE_INTERVAL_SECONDS,
            candidate_story_ids,
        ):
            reason = "not_due"
            _finish_run_record(
                run_id=run_id,
                date=target_date,
                started_at=started_at,
                status="skipped",
                error=reason,
            )
            return {"status": "skipped", "reason": reason, "date": target_date}

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
    finally:
        conn.close()

    agent = None
    usage_checkpoint = None

    try:
        trends_seed_input = build_trend_heat_input(
            window_rows,
            target_date=target_date,
            start_date=start_date,
        )

        preflight_counts = {
            "signals_stories": len(today_rows),
            "evidence_stories": len(evidence_rows),
            "trend_topics": _input_count(trends_seed_input, "topicDailyStats"),
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
        if preflight_counts["trend_topics"] < TREND_HEAT_MIN_TOPICS:
            preflight_gaps.append(
                f"trend topics {preflight_counts['trend_topics']}/{TREND_HEAT_MIN_TOPICS}"
            )
        if preflight_gaps:
            reason = "insufficient_insights_inputs"
            _finish_run_record(
                run_id=run_id,
                date=target_date,
                started_at=started_at,
                status="skipped",
                error=f"{reason}: {'; '.join(preflight_gaps)}",
            )
            return {
                "status": "skipped",
                "reason": reason,
                "date": target_date,
                "input_counts": preflight_counts,
                "input_gaps": preflight_gaps,
            }

        evidence_cache_key = _insights_evidence_cache_key(
            target_date=target_date,
            start_date=start_date,
            window_rows=evidence_rows,
            feed_ranks=feed_ranks,
            comments_by_story=comments_by_story,
        )

        agent = ai_agent or InsightsAgentRunner()
        usage_checkpoint = _usage_checkpoint(agent)

        evidence_cache_status = "hit"
        evidence_out = _load_cached_evidence(evidence_cache_key)
        if evidence_out is None:
            evidence_cache_status = "miss"
            evidence_out = _run_evidence_batches(
                agent,
                evidence_rows,
                target_date=target_date,
                start_date=start_date,
                feed_ranks=feed_ranks,
                comments_by_story=comments_by_story,
            )
            _store_cached_evidence(
                evidence_cache_key,
                evidence_out,
                len(evidence_rows),
            )
        topic_scout_input = build_topic_scout_input(evidence_out, trends_seed_input)
        topic_scout_out = agent.run_topic_scout(topic_scout_input)
        story_refs = _story_refs_by_id(window_rows, feed_ranks)
        signals_input, trends_input, opportunities_input, debates_input = (
            build_routed_insights_inputs(
                target_date=target_date,
                today_rows=today_rows,
                trends_input=trends_seed_input,
                evidence=evidence_out,
                scout=topic_scout_out,
                story_refs=story_refs,
            )
        )

        input_counts = _insights_input_counts(
            signals_input=signals_input,
            trends_input=trends_input,
            opportunities_input=opportunities_input,
            debates_input=debates_input,
        )
        input_gaps = _insights_input_gaps(input_counts)
        if input_gaps:
            reason = "insufficient_insights_inputs"
            _finish_run_record(
                run_id=run_id,
                date=target_date,
                started_at=started_at,
                status="skipped",
                model_usage=_usage_since(agent, usage_checkpoint),
                error=f"{reason}: {'; '.join(input_gaps)}",
            )
            return {
                "status": "skipped",
                "reason": reason,
                "date": target_date,
                "input_counts": input_counts,
                "input_gaps": input_gaps,
            }

        signals_out = agent.run_signals(signals_input)
        trends_out = agent.run_trends(trends_input)
        opportunities_out = agent.run_opportunities(opportunities_input)
        debates_out = agent.run_debates(debates_input)

        source_story_ids = _collect_story_reference_ids(
            signals_out,
            trends_out,
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
            "trendHeatmap": trends_out["trendHeatmap"],
            "opportunities": opportunities_out["opportunities"],
            "debates": debates,
        }
        payload = sanitize_forbidden_words(payload)
        _validate_final_payload(payload, candidate_story_ids)

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
            "agent_usage": model_usage or {},
        }
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
        log.exception("insights generation failed for %s: %s", target_date, exc)
        model_usage = _usage_since(agent, usage_checkpoint)
        _finish_run_record(
            run_id=run_id,
            date=target_date,
            started_at=started_at,
            status="failed",
            model_usage=model_usage,
            error=error,
        )
        return {
            "status": "failed",
            "date": target_date,
            "error": error,
            "agent_usage": model_usage or {},
        }


__all__ = [
    "build_evidence_input",
    "build_debate_input",
    "build_opportunity_input",
    "build_routed_insights_inputs",
    "build_today_signals_input",
    "build_topic_scout_input",
    "build_trend_heat_input",
    "run_insights_once",
]
