"""Build a mock SQLite for cloud_sync PoC.

Idempotent: drops the target DB file and rebuilds schema + seed rows.
Goal: produce a small but complete dataset that exercises every read
endpoint (list_feed_stories / list_topic_stories / list_topics /
get_digest) so cloud_sync_diff can verify byte-level shape alignment.

Run::
    python -m server.scripts.build_mock_db
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

from server import db, settings
from server.repository import (
    digest_date_epoch_bounds,
    digest_date_minus_days,
    today_in_digest_tz,
)


# ---------- helpers ----------

def _today_anchors() -> dict:
    today = today_in_digest_tz()
    yesterday = digest_date_minus_days(1)
    today_start, _ = digest_date_epoch_bounds(today)
    yesterday_start, _ = digest_date_epoch_bounds(yesterday)
    return {
        "today_str": today,
        "yesterday_str": yesterday,
        # noon 12:00 of the day (DIGEST_TIMEZONE), so date_in_digest_tz maps back to the correct date
        "today_hn_time": today_start + 12 * 3600,
        "yesterday_hn_time": yesterday_start + 12 * 3600,
        "two_days_ago_hn_time": today_start - 36 * 3600,
    }


# ---------- mock data ----------

# (id, kind, topic, title_zh, title_en, url, domain, by, score, descendants, time_key, ai_summary)
STORIES = [
    (11001, "story", "ai",       "AI 推理成本曲线再次下探",      "AI Inference Cost Drops Again",   "https://example.com/ai-cost",   "example.com",            "alice",   420, 215, "today",        "推理边际成本进入新一轮快速下降周期。"),
    (11002, "story", "ai",       "Transformer 之外的新架构",     "Beyond Transformers",              "https://example.com/beyond-tx", "example.com",            "bob",     350, 180, "today",        "讨论 Mamba 等替代架构在长序列任务上的表现。"),
    (11003, "story", "database", "PostgreSQL 18 正式发布",       "PostgreSQL 18 Released",           "https://example.com/pg-18",     "example.com",            "carol",   310,  98, "today",        "PG 18 主推 logical replication 改进与新 backup 工具。"),
    (11004, "story", "rust",     "Rust 异步运行时格局变化",      "Rust Async Runtimes",              "https://example.com/rust-async","example.com",            "dan",     280, 165, "today",        "tokio 之外社区出现多个轻量替代品。"),
    (11005, "story", "devops",   "Kubernetes 1.32 路线图",       "Kubernetes 1.32 Roadmap",          "https://example.com/k8s-132",   "example.com",            "eve",     270,  60, "yesterday",    "1.32 关注调度器扩展与节点级 OOM 改进。"),
    (11006, "story", "cs",       "一篇被遗忘的 1972 年论文",     "A Forgotten 1972 Paper",           "https://example.com/paper-72",  "example.com",            "frank",   240,  22, "yesterday",    ""),  # empty aiSummary
    (11007, "story", "security", "Linux supply chain 攻击复盘",  "Linux Supply Chain Attack",        "https://example.com/scattack",  "example.com",            "grace",   220, 130, "yesterday",    "被入侵的小型依赖如何被快速发现。"),
    (11008, "story", "ai",       "LLM 评测的新方法",             "New LLM Benchmark",                "https://example.com/new-bench", "example.com",            "henry",   200,  70, "two_days_ago", "针对长上下文的新评测集发布。"),
    (11009, "story", "web",      "CSS 新特性整理",               "CSS New Features",                 "https://example.com/css-new",   "example.com",            "ivy",     180,  45, "today",        "container query 等新特性的浏览器支持现状。"),
    (11010, "story", "database", "SQLite WAL 调优实战",          "Tuning SQLite WAL",                "https://example.com/sqlite-wal","example.com",            "jack",    160,  30, "today",        "WAL 模式下的 checkpoint 频率与 fsync 行为。"),
    (11011, "show",  "ai",       "Show HN:个人 LLM 助手",        "Show HN: Personal LLM Tool",       "https://example.com/show1",     "example.com",            "kelly",   120,  20, "today",        "本地部署的轻量助手,只用于个人笔记场景。"),
    (11012, "show",  "web",      "Show HN:简洁 markdown 编辑器", "Show HN: Markdown Editor",         "https://example.com/show2",     "example.com",            "leo",     100,  12, "yesterday",    "无插件、纯键盘操作的 markdown 编辑器。"),
    (11013, "ask",   "cs",       "Ask HN:系统设计书推荐",        "Ask HN: System Design Books",      "",                              "news.ycombinator.com",   "mia",      80,  55, "today",        "讨论 DDIA 以外还有哪些经典系统设计读物。"),
    (11014, "ask",   "rust",     "Ask HN:Rust 学习路径",         "Ask HN: Learning Rust",            "",                              "news.ycombinator.com",   "noah",     70,  35, "today",        "从已经熟悉 Go 的角度进入 Rust 的建议路径。"),
    (11015, "ask",   "security", "Ask HN:密码管理器选择",        "Ask HN: Password Manager",         "",                              "news.ycombinator.com",   "olivia",   60,  25, "yesterday",    "个人和团队场景下的密码管理器选型讨论。"),
]

DISCUSSION_THEMES = {
    11001: [
        {"title": "成本曲线", "summary": "推理边际成本下降速度超出半年前的预期。"},
        {"title": "商业模式", "summary": "低成本是否会引发新的应用形态变化。"},
    ],
    11003: [{"title": "迁移成本", "summary": "生产 PG 集群从 16 升级到 18 的实际工作量。"}],
    11007: [{"title": "依赖治理", "summary": "小型依赖被入侵后如何在 CI 阶段拦截。"}],
}

INSIGHTS = {
    11001: [
        {"author": "commenter1", "score": 80, "text": "这个数据说明 unit economics 已经反转。"},
        {"author": "commenter2", "score": 45, "text": "但要注意 batched inference 的隐藏成本。"},
    ],
    11013: [{"author": "cs_phd", "score": 35, "text": "DDIA 之外可以补一本《Systems Performance》。"}],
}

TERMS = {
    11001: [{"term": "TPS", "def": "Tokens per second,推理速度常用衡量单位。"}],
    11003: [{"term": "WAL", "def": "Write-Ahead Log,先写日志再写主数据的崩溃恢复机制。"}],
}

# rankings: feed -> list of story_id (order is the rank, starting at 1)
RANKINGS = {
    "top":  [11001, 11002, 11003, 11004, 11005, 11006, 11007, 11008],
    "new":  [11009, 11010, 11011, 11012, 11013, 11014],
    "best": [11001, 11003, 11005, 11007, 11010],
    "ask":  [11013, 11014, 11015],
    "show": [11011, 11012],
    "job":  [],
}

TOPIC_NAMES = {
    "ai":       "AI 与机器学习",
    "database": "数据库",
    "rust":     "Rust",
    "devops":   "DevOps",
    "cs":       "计算机基础",
    "security": "安全",
    "web":      "Web 前端",
}

# story_id referenced by the digest (order preserved)
DIGEST_TODAY_IDS = [11003, 11001, 11011, 11013]
DIGEST_YESTERDAY_IDS = [11005, 11007, 11015]


# ---------- builder ----------

def _resolve_hn_time(time_key: str, anchors: dict) -> int:
    if time_key == "today":
        return anchors["today_hn_time"]
    if time_key == "yesterday":
        return anchors["yesterday_hn_time"]
    if time_key == "two_days_ago":
        return anchors["two_days_ago_hn_time"]
    raise ValueError(f"unknown time_key: {time_key}")


def _insert_story(conn, sid, kind, topic, title_zh, title_en, url, domain, by,
                  score, descendants, hn_time, ai_summary, now):
    themes = json.dumps(DISCUSSION_THEMES.get(sid, []), ensure_ascii=False)
    insights = json.dumps(INSIGHTS.get(sid, []), ensure_ascii=False)
    terms = json.dumps(TERMS.get(sid, []), ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO stories (
            id, kind, title_en, title_zh, url, domain, by,
            score, descendants, hn_time, raw_text, raw_json,
            topic, ai_summary, discussion_themes, insights, terms,
            enrich_status, enriched_at,
            enriched_title_en, enriched_url, enriched_domain, enriched_by,
            enriched_hn_time, enriched_score, enriched_descendants,
            comments_fetched_descendants,
            fetched_at, last_seen_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            'done', ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?,
            ?, ?
        )
        """,
        (
            sid, kind, title_en, title_zh, url, domain, by,
            score, descendants, hn_time, "", "{}",
            topic, ai_summary, themes, insights, terms,
            now,
            title_en, url, domain, by,
            hn_time, score, descendants,
            descendants,
            now, now,
        ),
    )


def build(db_path: Optional[Path] = None) -> dict:
    target = db_path or settings.DEFAULT_DB_PATH
    print(f"[mock-db] target: {target}")
    if target.exists():
        target.unlink()
        print("[mock-db] removed existing file")

    db.init_db(target)
    conn = db.connect(target)
    try:
        anchors = _today_anchors()
        now = int(time.time())
        with db.transaction(conn):
            for s in STORIES:
                (sid, kind, topic, t_zh, t_en, url, dom, by, score, desc, time_key, ai_summary) = s
                _insert_story(
                    conn, sid, kind, topic, t_zh, t_en, url, dom, by,
                    score, desc,
                    _resolve_hn_time(time_key, anchors),
                    ai_summary, now,
                )
            for feed, story_ids in RANKINGS.items():
                for rank, sid in enumerate(story_ids, start=1):
                    conn.execute(
                        "INSERT INTO rankings (feed, rank, story_id, refreshed_at) "
                        "VALUES (?, ?, ?, ?)",
                        (feed, rank, sid, now),
                    )
            for topic_id, name in TOPIC_NAMES.items():
                conn.execute(
                    "INSERT INTO topics (id, name, created_at, updated_at, last_seen_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (topic_id, name, now, now, now),
                )
            conn.execute(
                "INSERT INTO digests (date, intro, story_ids, generated_at) "
                "VALUES (?, ?, ?, ?)",
                (anchors["today_str"],
                 "今日精选导语:推理成本曲线与新数据库特性。",
                 json.dumps(DIGEST_TODAY_IDS), now),
            )
            conn.execute(
                "INSERT INTO digests (date, intro, story_ids, generated_at) "
                "VALUES (?, ?, ?, ?)",
                (anchors["yesterday_str"],
                 "昨日精选:k8s 路线图与一次 supply chain 安全事件。",
                 json.dumps(DIGEST_YESTERDAY_IDS), now),
            )
            # after the initial publish, catalog_version = 1
            conn.execute(
                "UPDATE meta SET value='1', updated_at=? WHERE key='catalog_version'",
                (now,),
            )

        summary = {
            "target": str(target),
            "stories": len(STORIES),
            "rankings": sum(len(v) for v in RANKINGS.values()),
            "topics": len(TOPIC_NAMES),
            "digests": 2,
            "catalog_version": 1,
            "today": anchors["today_str"],
            "yesterday": anchors["yesterday_str"],
        }
        print("[mock-db] done: " + json.dumps(summary, ensure_ascii=False))
        return summary
    finally:
        conn.close()


if __name__ == "__main__":
    build()
