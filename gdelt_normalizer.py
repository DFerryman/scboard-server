"""GDELT DOC article -> repository row dict normalization."""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .normalizer import extract_domain
from .topics import DEFAULT_TOPIC_ID


_GDELT_ID_BASE = 2_000_000_000
_GDELT_ID_SPACE = 1_000_000_000
_SEENDATE_DIGITS_RE = re.compile(r"(\d{14})")


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def stable_gdelt_story_id(url: str) -> int:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return _GDELT_ID_BASE + (int(digest[:12], 16) % _GDELT_ID_SPACE)


def parse_gdelt_seendate(value: Any, *, fallback: Optional[int] = None) -> int:
    text = _safe_str(value)
    if text:
        match = _SEENDATE_DIGITS_RE.search(text)
        if match:
            try:
                dt = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
                return int(dt.replace(tzinfo=timezone.utc).timestamp())
            except ValueError:
                pass
    return int(fallback if fallback is not None else time.time())


def _article_excerpt(article: Dict[str, Any]) -> str:
    parts = []
    for label, key in (
        ("title", "title"),
        ("url", "url"),
        ("domain", "domain"),
        ("sourceCountry", "sourcecountry"),
        ("sourceLanguage", "sourcelanguage"),
        ("seendate", "seendate"),
    ):
        value = _safe_str(article.get(key))
        if value:
            parts.append(f"{label}: {value}")
    return "\n".join(parts)


def normalize_article(
    article: Optional[Dict[str, Any]],
    *,
    rank: int,
    total: int,
    fetched_at: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(article, dict):
        return None
    title = _safe_str(article.get("title"))
    url = _safe_str(article.get("url"))
    if not title or not url:
        return None
    if not (url.startswith("http://") or url.startswith("https://")):
        return None

    now = int(fetched_at if fetched_at is not None else time.time())
    hn_time = parse_gdelt_seendate(article.get("seendate"), fallback=now)
    domain = _safe_str(article.get("domain")) or extract_domain(url)
    by = _safe_str(article.get("sourcecountry")) or "GDELT"
    # DOC ArticleList has an official result order, but no per-article score.
    # Keep score at 0 so clients do not display a fabricated HN-style metric.
    score = 0

    return {
        "id": stable_gdelt_story_id(url),
        "source": "gdelt",
        "kind": "story",
        "title_en": title,
        "title_zh": title,
        "url": url,
        "domain": domain,
        "by": by,
        "score": score,
        "descendants": 0,
        "hn_time": hn_time,
        "raw_text": _article_excerpt(article),
        "raw_json": json.dumps(article, ensure_ascii=False, separators=(",", ":")),
        "topic": DEFAULT_TOPIC_ID,
        "fetched_at": now,
        "last_seen_at": now,
    }


__all__ = [
    "normalize_article",
    "parse_gdelt_seendate",
    "stable_gdelt_story_id",
]
