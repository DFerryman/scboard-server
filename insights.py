"""Server-side insights generation pipeline."""

from __future__ import annotations

import html
import json
import logging
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
from .topics import clean_topic_name, topic_name_from_id


log = logging.getLogger(__name__)

INSIGHTS_VERSION = 1
INSIGHTS_WINDOW_LABEL = "24h"
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


def _clean_comment_text(value: Any, max_chars: int) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_chars:
        text = text[:max_chars].rstrip()
    return text


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
    name = clean_topic_name(topic_name)
    if name:
        return name
    clean = str(topic or "").strip()
    return topic_name_from_id(clean)


def _row_topic_label(row: Any) -> str:
    return _topic_label(
        str(row["topic"] or ""),
        _row_value(row, "topic_name", ""),
    )


def _story_payload(
    row,
    *,
    feed_ranks: Mapping[int, Mapping[str, int]],
    include_raw_text: bool = False,
    raw_text_max_chars: int = 0,
    include_domain: bool = False,
    include_insights: bool = False,
    comments: Optional[Sequence[Any]] = None,
    comment_max_chars: int = 180,
) -> Dict[str, Any]:
    story_id = int(row["id"])
    out: Dict[str, Any] = {
        "id": story_id,
        "kind": row["kind"] or "story",
        "topic": row["topic"] or "",
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
        out["rawTextSnippet"] = (row["raw_text"] or "")[: max(0, raw_text_max_chars)]
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
            story_kwargs["comment_max_chars"] = settings.INSIGHTS_COMMENT_MAX_CHARS
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


def _daily_topic_activity_heat(item: Mapping[str, Any]) -> float:
    count = max(0, int(item.get("count") or 0))
    score_sum = max(0, int(item.get("scoreSum") or 0))
    descendants_sum = max(0, int(item.get("descendantsSum") or 0))
    if count <= 0:
        return 0.0
    raw = count * 18 + score_sum / 18 + descendants_sum / 9
    return max(0.0, min(100.0, raw))


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
    for topic, bucket in by_topic.items():
        daily = [bucket["daily"][d] for d in dates]
        today = daily[-1]
        previous = daily[:-1] or daily[-1:]
        prev_count = sum(item["count"] for item in previous) / max(1, len(previous))
        prev_score = sum(item["scoreSum"] for item in previous) / max(1, len(previous))
        prev_desc = sum(item["descendantsSum"] for item in previous) / max(1, len(previous))
        today_heat = _daily_topic_activity_heat(today)
        previous_avg_heat = sum(_daily_topic_activity_heat(item) for item in previous) / max(
            1, len(previous)
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
        topic = row["topic"] or ""
        topic_counts[topic] = topic_counts.get(topic, 0) + 1

    candidates = []
    for row in window_rows:
        sid = int(row["id"])
        ranks = feed_ranks.get(sid, {})
        repeated = topic_counts.get(row["topic"] or "", 0) >= 2
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
                raw_text_max_chars=settings.INSIGHTS_RAW_TEXT_MAX_CHARS,
                include_domain=True,
                include_insights=True,
                comments=comments_by_story.get(int(row["id"]), []),
                comment_max_chars=settings.INSIGHTS_COMMENT_MAX_CHARS,
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
                comment_max_chars=settings.INSIGHTS_COMMENT_MAX_CHARS,
            )
            for row in candidates
        ]
    }


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


def _usage_checkpoint(agent: Any) -> Optional[int]:
    fn = getattr(agent, "usage_checkpoint", None)
    if not callable(fn):
        return None
    try:
        return int(fn())
    except Exception:  # noqa: BLE001
        return None


def _usage_since(agent: Any, checkpoint: Optional[int]) -> Optional[dict]:
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
    if not (5 <= len(trend_items) <= 8):
        raise InsightsValidationError("trendHeatmap.items must contain 5-8 items")
    if not (3 <= len(payload.get("opportunities") or []) <= 5):
        raise InsightsValidationError("opportunities must contain 3-5 items")
    if not (2 <= len(payload.get("debates") or []) <= 4):
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
    topics = {str(row["topic"] or "") for row in window_rows if str(row["topic"] or "").strip()}
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
        comment_ids = candidate_story_ids
        comments_by_story = repository.insight_comment_rows_for_story_ids(
            conn,
            comment_ids,
            limit_per_story=None,
        )
    finally:
        conn.close()

    agent = None
    usage_checkpoint = None

    try:
        agent = ai_agent or InsightsAgentRunner()
        usage_checkpoint = _usage_checkpoint(agent)

        signals_input = build_today_signals_input(
            today_rows,
            target_date=target_date,
            feed_ranks=feed_ranks,
            comments_by_story=comments_by_story,
        )
        trends_input = build_trend_heat_input(
            window_rows,
            target_date=target_date,
            start_date=start_date,
        )
        opportunities_input = build_opportunity_input(
            window_rows,
            target_end_ts=end_ts,
            feed_ranks=feed_ranks,
            comments_by_story=comments_by_story,
        )
        debates_input = build_debate_input(
            window_rows,
            feed_ranks=feed_ranks,
            comments_by_story=comments_by_story,
        )

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
    "build_debate_input",
    "build_opportunity_input",
    "build_today_signals_input",
    "build_trend_heat_input",
    "run_insights_once",
]
