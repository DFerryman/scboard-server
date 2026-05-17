"""Dynamic topic helpers.

Hacker News exposes feed/type buckets such as top, ask, show, and job, but it
does not expose subject-matter categories. The classification tab therefore
uses AI-maintained topics stored in SQLite. This module only normalizes the
topic id/name pair and provides the fallback topic.
"""

from __future__ import annotations

import hashlib
import re
from typing import Iterable, Tuple

from .schemas import TopicEntry


DEFAULT_TOPIC_ID = "general"
DEFAULT_TOPIC_NAME = "综合技术"
MAX_TOPIC_NAME_CHARS = 24
MAX_TOPIC_ID_CHARS = 64

_TOPIC_ID_RE = re.compile(r"[^a-z0-9-]+")
_DASH_RE = re.compile(r"-+")


def clean_topic_id(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip().lower().replace("_", "-")
    text = _TOPIC_ID_RE.sub("-", text)
    text = _DASH_RE.sub("-", text).strip("-")
    return text[:MAX_TOPIC_ID_CHARS]


def clean_topic_name(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = " ".join(value.strip().split())
    text = text.strip(" -_/，,。.:：;；|")
    if not text or len(text) > MAX_TOPIC_NAME_CHARS:
        return ""
    return text


def topic_id_from_name(name: str) -> str:
    clean_name = clean_topic_name(name)
    if not clean_name:
        return DEFAULT_TOPIC_ID
    if all(ord(ch) < 128 for ch in clean_name):
        topic_id = clean_topic_id(clean_name)
        if topic_id:
            return topic_id
    digest = hashlib.sha1(clean_name.encode("utf-8")).hexdigest()[:10]
    return f"topic-{digest}"


def topic_name_from_id(topic_id: str) -> str:
    clean_id = clean_topic_id(topic_id)
    if not clean_id or clean_id == DEFAULT_TOPIC_ID:
        return DEFAULT_TOPIC_NAME
    return " ".join(part.capitalize() for part in clean_id.split("-") if part)


def _topic_name_key(name: str) -> str:
    return clean_topic_name(name).casefold()


def normalize_topic(
    *,
    topic: object = None,
    topic_id: object = None,
    topic_name: object = None,
    existing_topics: Iterable[TopicEntry] | None = None,
) -> Tuple[str, str]:
    """Return a safe ``(topic_id, topic_name)`` pair.

    Existing ids and names win so the AI can reuse active topics. When the AI
    proposes a genuinely new topic, the server derives a stable id from the
    display name unless the model supplied a clean slug alongside the name.
    """

    existing = list(existing_topics or [])
    by_id = {clean_topic_id(t.id): t for t in existing if clean_topic_id(t.id)}
    by_name = {
        _topic_name_key(t.name): t
        for t in existing
        if _topic_name_key(t.name)
    }

    raw_ids = [clean_topic_id(topic_id), clean_topic_id(topic)]
    for raw_id in raw_ids:
        if raw_id in by_id:
            entry = by_id[raw_id]
            return entry.id, entry.name

    name = clean_topic_name(topic_name)
    if name:
        existing_by_name = by_name.get(_topic_name_key(name))
        if existing_by_name is not None:
            return existing_by_name.id, existing_by_name.name

    if not name:
        legacy_name = clean_topic_name(topic)
        if legacy_name:
            legacy_id = clean_topic_id(legacy_name)
            name = topic_name_from_id(legacy_id) if legacy_id == legacy_name else legacy_name

    if not name:
        return DEFAULT_TOPIC_ID, DEFAULT_TOPIC_NAME

    preferred_id = clean_topic_id(topic_id) or clean_topic_id(topic)
    if preferred_id and clean_topic_name(topic_name):
        return preferred_id, name
    return topic_id_from_name(name), name


def topic_id_set() -> set[str]:
    """Compatibility shim for older imports.

    Dynamic topics are no longer represented by a fixed in-code set.
    """

    return {DEFAULT_TOPIC_ID}


def is_valid_topic(topic_id: str) -> bool:
    return bool(clean_topic_id(topic_id))
