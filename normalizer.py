"""HN item -> repository row dict normalization.

Plan §A2 / §5: derive ``stories.kind`` with priority job > ask/show feed >
title prefix > default ``story``. Skip ``deleted``/``dead`` items and any
HN ``type`` outside ``{story, job}`` so the ``stories.kind`` CHECK
constraint cannot be violated.
"""

from __future__ import annotations

import json
import time
import urllib.parse
from typing import Any, Dict, Optional

from .topics import DEFAULT_TOPIC_ID


_DEFAULT_DOMAIN = "news.ycombinator.com"


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _safe_int(value: Any) -> int:
    if value is None or isinstance(value, bool):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def extract_domain(url: str) -> str:
    if not url:
        return _DEFAULT_DOMAIN
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError:
        return _DEFAULT_DOMAIN
    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if not host:
        return _DEFAULT_DOMAIN
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError:
        pass
    if host.startswith("www."):
        host = host[4:]
    return host or _DEFAULT_DOMAIN


def derive_kind(
    *,
    hn_type: str,
    source_feed: Optional[str],
    title: str,
) -> str:
    """Plan §5 priority: job > explicit ask/show feed > title prefix > story."""
    if hn_type == "job" or source_feed == "job":
        return "job"
    if source_feed == "ask":
        return "ask"
    if source_feed == "show":
        return "show"
    title = title or ""
    if title.startswith("Ask HN:"):
        return "ask"
    if title.startswith("Show HN:"):
        return "show"
    return "story"


def normalize_item(
    item: Optional[Dict[str, Any]],
    *,
    source_feed: Optional[str] = None,
    fetched_at: Optional[int] = None,
    last_seen_at: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Convert an HN item dict into a stories row dict.

    Returns ``None`` for items that must be skipped (deleted/dead, or types
    HN exposes that we do not display: ``comment``, ``poll``, ``pollopt``).
    """
    if not isinstance(item, dict):
        return None
    if item.get("deleted") or item.get("dead"):
        return None

    raw_id = item.get("id")
    if raw_id is None:
        return None
    try:
        item_id = int(raw_id)
    except (TypeError, ValueError):
        return None

    hn_type = _safe_str(item.get("type", "story")).lower() or "story"
    if hn_type not in ("story", "job"):
        return None

    title = _safe_str(item.get("title"))
    url = _safe_str(item.get("url"))
    text = _safe_str(item.get("text"))

    kind = derive_kind(hn_type=hn_type, source_feed=source_feed, title=title)

    domain = extract_domain(url) if url else _DEFAULT_DOMAIN

    score = max(_safe_int(item.get("score")), 0)
    descendants = max(_safe_int(item.get("descendants")), 0)
    hn_time = _safe_int(item.get("time"))
    by = _safe_str(item.get("by"))

    now = int(time.time())
    return {
        "id": item_id,
        "kind": kind,
        "title_en": title,
        "title_zh": title,
        "url": url,
        "domain": domain,
        "by": by,
        "score": score,
        "descendants": descendants,
        "hn_time": hn_time,
        "raw_text": text,
        "raw_json": json.dumps(item, ensure_ascii=False, separators=(",", ":")),
        "topic": DEFAULT_TOPIC_ID,
        "fetched_at": fetched_at if fetched_at is not None else now,
        "last_seen_at": last_seen_at if last_seen_at is not None else now,
    }


def normalize_comment(
    item: Optional[Dict[str, Any]],
    *,
    story_id: int,
    parent_id: Optional[int],
    depth: int,
    rank: int,
    fetched_at: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    if item.get("deleted") or item.get("dead"):
        return None
    if _safe_str(item.get("type")).lower() != "comment":
        return None

    raw_id = item.get("id")
    if raw_id is None:
        return None
    try:
        cid = int(raw_id)
    except (TypeError, ValueError):
        return None

    text = _safe_str(item.get("text"))
    if not text.strip():
        return None

    by = _safe_str(item.get("by"))
    hn_time = _safe_int(item.get("time"))

    return {
        "id": cid,
        "story_id": int(story_id),
        "parent_id": int(parent_id) if parent_id is not None else None,
        "by": by,
        "text": text,
        "hn_time": hn_time,
        "depth": int(depth),
        "rank": int(rank),
        "raw_json": json.dumps(item, ensure_ascii=False, separators=(",", ":")),
        "fetched_at": fetched_at if fetched_at is not None else int(time.time()),
    }


__all__ = ["derive_kind", "extract_domain", "normalize_item", "normalize_comment"]
