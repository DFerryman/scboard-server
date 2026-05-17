"""Field-level diff:cloud_sync read-model JSON vs SQLite source-of-truth.

For every cloud read shape, compare the SQLite source data after the
same AI-ready gate used by ``cloud_sync`` against what a cloud read function
would produce on top of the read-model JSON files.

Exit status:
    0 — 0 mismatches (shape fully aligned)
    1 — at least one mismatch (details printed to stdout)
    2 — read-model output missing (run ``python -m server.cloud_sync`` first)

Run::
    python -m server.cloud_sync_diff
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from . import db, repository
from .cloud_sync import (
    FEED_TYPES,
    _all_visible_story_ids,
    _list_ai_ready_topics,
    _story_is_ai_ready,
    default_output_dir,
)


# ---------- io ----------

def _console_text(text: str, stream) -> str:
    encoding = getattr(stream, "encoding", None) or "utf-8"
    try:
        text.encode(encoding)
        return text
    except LookupError:
        return text
    except UnicodeEncodeError:
        return text.encode(encoding, errors="backslashreplace").decode(encoding)


def _console_print(text: str, *, file=None) -> None:
    stream = file or sys.stdout
    print(_console_text(text, stream), file=stream)


def _load_jsonl(path: Path) -> List[dict]:
    out: List[dict] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


# ---------- shape conversion ----------

def _story_doc_to_client(doc: dict, type_: str) -> dict:
    """Project a cloud_sync story doc to the schemas.py Story shape."""
    return {
        "id": doc["id"],
        "type": type_,
        "titleZh": doc["titleZh"],
        "titleEn": doc["titleEn"],
        "url": doc["url"],
        "domain": doc["domain"],
        "by": doc["by"],
        "score": doc["score"],
        "descendants": doc["descendants"],
        "time": doc["time"],
        "updatedAt": doc["updatedAt"],
        "topic": doc["topic"],
        "aiSummary": doc["aiSummary"],
        "discussionThemes": doc["discussionThemes"],
        "insights": doc["insights"],
        "terms": doc["terms"],
    }


def _story_to_dict(story) -> dict:
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


def _expected_feed_stories(
    conn, feed: str, page: int, page_size: int
) -> Tuple[List[dict], int]:
    """Source-side feed projection using the cloud_sync AI-ready gate."""
    rows = conn.execute(
        """
        SELECT s.*
        FROM rankings r
        JOIN stories s ON s.id=r.story_id
        WHERE r.feed=? AND s.enrich_status='done'
        ORDER BY r.rank ASC
        """,
        (feed,),
    ).fetchall()
    stories = [repository.row_to_story(r, feed=feed) for r in rows]
    ready = [s for s in stories if _story_is_ai_ready(s)]
    total = len(ready)
    offset = (page - 1) * page_size
    return [_story_to_dict(s) for s in ready[offset:offset + page_size]], total


def _expected_topic_stories(
    conn, topic_id: str, page: int, page_size: int
) -> Tuple[List[dict], int]:
    """Source-side topic projection using the cloud_sync AI-ready gate."""
    rows = conn.execute(
        """
        SELECT s.*
        FROM stories s
        WHERE s.topic=?
          AND s.enrich_status='done'
          AND EXISTS (SELECT 1 FROM rankings r WHERE r.story_id=s.id)
        """,
        (topic_id,),
    ).fetchall()
    stories = [repository.row_to_story(r) for r in rows]
    ready = [s for s in stories if _story_is_ai_ready(s)]
    ready.sort(key=lambda s: (-int(s.score), -int(s.time), int(s.id)))
    total = len(ready)
    offset = (page - 1) * page_size
    return [_story_to_dict(s) for s in ready[offset:offset + page_size]], total


# ---------- cloud-side simulators ----------

def _index_stories(stories_jsonl: List[dict], current_version: int) -> Dict[int, dict]:
    out: Dict[int, dict] = {}
    for doc in stories_jsonl:
        if doc.get("syncVersion") == current_version:
            out[int(doc["id"])] = doc
    return out


def _simulate_feed_stories(
    stories_idx: Dict[int, dict], feed: str, page: int, page_size: int
) -> Tuple[List[dict], int]:
    """Mirror what the cloud `stories` function will do: filter feedRanks.<feed> > 0,
    order asc by that rank, paginate."""
    matching = [
        (d["feedRanks"][feed], d)
        for d in stories_idx.values()
        if d.get("feedRanks", {}).get(feed, 0) > 0
    ]
    matching.sort(key=lambda x: x[0])
    total = len(matching)
    offset = (page - 1) * page_size
    page_docs = [m[1] for m in matching[offset:offset + page_size]]
    return [_story_doc_to_client(d, feed) for d in page_docs], total


def _simulate_topic_stories(
    stories_idx: Dict[int, dict], topic_id: str, page: int, page_size: int
) -> Tuple[List[dict], int]:
    """Mirror cloud `topicStories`: stories with topic==topic_id and at least
    one feedRank, ordered score DESC, time DESC."""
    matching = [
        d for d in stories_idx.values()
        if d.get("topic") == topic_id
        and any(v > 0 for v in d.get("feedRanks", {}).values())
    ]
    matching.sort(key=lambda d: (-int(d["score"]), -int(d["time"]), int(d["id"])))
    total = len(matching)
    offset = (page - 1) * page_size
    page_docs = matching[offset:offset + page_size]
    # In single-story context (no feed), use defaultType as the type
    return [_story_doc_to_client(d, d["defaultType"]) for d in page_docs], total


# ---------- diff harness ----------

class Differ:
    def __init__(self) -> None:
        self.errors: List[str] = []
        self.checks = 0

    def expect(self, label: str, expected, actual) -> None:
        self.checks += 1
        if expected != actual:
            self.errors.append(
                f"[MISMATCH] {label}\n"
                f"  expected: {expected!r}\n"
                f"  actual:   {actual!r}"
            )

    def report(self) -> int:
        if not self.errors:
            _console_print(f"[diff] {self.checks} checks, 0 mismatch - shape aligned")
            return 0
        _console_print(f"[diff] {self.checks} checks, {len(self.errors)} mismatch")
        for e in self.errors:
            _console_print(e)
        return 1


# ---------- main ----------

def main() -> None:
    out_dir = default_output_dir()
    if not (out_dir / "meta.json").exists():
        print(
            f"[diff] cloud-sync output does not exist ({out_dir}); "
            f"run `python -m server.cloud_sync` first",
            file=sys.stderr,
        )
        sys.exit(2)

    meta = json.loads((out_dir / "meta.json").read_text(encoding="utf-8"))
    stories_jsonl = _load_jsonl(out_dir / "stories.jsonl")
    topics_jsonl = _load_jsonl(out_dir / "topics.jsonl")
    digests_jsonl = _load_jsonl(out_dir / "digests.jsonl")

    current_version = int(meta["currentVersion"])
    stories_idx = _index_stories(stories_jsonl, current_version)
    differ = Differ()

    conn = db.connect_readonly()
    try:
        sqlite_version = int(repository.get_catalog_version(conn) or "0")
        differ.expect("catalog_version", sqlite_version, current_version)
        source_visible_ids = _all_visible_story_ids(conn)

        # ----- feed stories -----
        for feed in FEED_TYPES:
            for page in (1, 2):
                expected, exp_total = _expected_feed_stories(
                    conn, feed, page, 5
                )
                actual, actual_total = _simulate_feed_stories(
                    stories_idx, feed, page, 5
                )
                differ.expect(f"feed={feed} page={page} total", exp_total, actual_total)
                differ.expect(f"feed={feed} page={page} list", expected, actual)

        # ----- topics list (ordered as returned by repository.list_topics) -----
        expected_topics = _list_ai_ready_topics(conn, source_visible_ids)
        expected_topics_dicts = [
            {"id": t.id, "name": t.name, "count": t.count} for t in expected_topics
        ]
        actual_topics = [
            {"id": d["id"], "name": d["name"], "count": d["count"]}
            for d in topics_jsonl
            if d["syncVersion"] == current_version
        ]
        differ.expect("topics list", expected_topics_dicts, actual_topics)

        # ----- topic stories (each topic verified separately) -----
        for t in expected_topics:
            expected, exp_total = _expected_topic_stories(
                conn, t.id, 1, 50
            )
            actual, actual_total = _simulate_topic_stories(
                stories_idx, t.id, 1, 50
            )
            differ.expect(f"topic={t.id} total", exp_total, actual_total)
            differ.expect(f"topic={t.id} list", expected, actual)

        # ----- digests -----
        digest_idx = {d["_id"]: d for d in digests_jsonl}
        digest_rows = conn.execute("SELECT date FROM digests").fetchall()
        for r in digest_rows:
            date = r["date"]
            exp_date, exp_intro, exp_stories = repository.get_digest(conn, date)
            digest_doc_id = f"{current_version}:{date}"
            actual = digest_idx.get(digest_doc_id)
            differ.expect(f"digest {date} exists", True, actual is not None)
            if actual is None:
                continue
            differ.expect(
                f"digest {date} syncVersion",
                current_version,
                actual["syncVersion"],
            )
            differ.expect(f"digest {date} date", exp_date, actual["date"])
            differ.expect(f"digest {date} intro", exp_intro, actual["intro"])
            exp_stories_dicts = [
                _story_to_dict(s)
                for s in exp_stories
                if _story_is_ai_ready(s)
            ]
            differ.expect(f"digest {date} stories", exp_stories_dicts, actual["stories"])

        # ----- feedCounts -----
        source_visible = {int(sid) for sid in source_visible_ids}
        for feed in FEED_TYPES:
            rows = conn.execute(
                "SELECT story_id FROM rankings WHERE feed=?",
                (feed,),
            ).fetchall()
            expected_count = sum(
                1 for row in rows if int(row["story_id"]) in source_visible
            )
            differ.expect(
                f"feedCounts.{feed}",
                expected_count,
                int(meta["feedCounts"].get(feed, 0)),
            )
    finally:
        conn.close()

    sys.exit(differ.report())


if __name__ == "__main__":
    main()
