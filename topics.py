"""Fixed topic taxonomy helpers.

Hacker News exposes feed/type buckets such as top, ask, show, and job, but it
does not expose subject-matter categories. The classification tab uses this
server-owned taxonomy: AI may choose a topic id from the catalog, but it may
not create categories.
"""

from __future__ import annotations

import re
from typing import Iterable, Mapping, Optional, Tuple

from .schemas import TopicEntry


DEFAULT_TOPIC_ID = "general"
DEFAULT_TOPIC_NAME = "综合 / 其他"
MAX_TOPIC_NAME_CHARS = 24
MAX_TOPIC_ID_CHARS = 64

_TOPIC_ID_RE = re.compile(r"[^a-z0-9-]+")
_DASH_RE = re.compile(r"-+")


FIXED_TOPIC_CATALOG: Tuple[Mapping[str, object], ...] = (
    {
        "id": "ai",
        "name": "AI / 大模型",
        "description": "AI models, AI products, AI research, model behavior, and AI industry news.",
        "include": "LLMs, model releases, AI safety, AI economics, robotics driven by AI.",
        "exclude": "Coding agents and AI developer tools belong to ai-devtools.",
        "aliases": ("人工智能", "AI模型", "大模型", "机器学习", "topic-63fe855a00", "topic-70de7f4c32", "topic-3401ed05a1", "topic-3848429900"),
    },
    {
        "id": "ai-devtools",
        "name": "AI 编程工具",
        "description": "AI coding assistants, agents, MCP, prompt tooling, and developer workflows involving AI.",
        "include": "Codex, Claude Code, Copilot, coding agents, MCP servers, AI IDEs, agent harnesses.",
        "exclude": "General LLM business or model news belongs to ai.",
        "aliases": ("AI 工具", "AI编程工具", "AI 编程工具", "AI应用", "ai-tools", "topic-89a52576b7", "topic-51b81d6de9", "topic-2fc8e829ec", "topic-30fa466d91"),
    },
    {
        "id": "devtools",
        "name": "开发工具",
        "description": "Developer tools, editors, CLIs, productivity utilities, and software products for builders.",
        "include": "IDEs, shells, Git tools, terminals, linters, project tools, Show HN utilities.",
        "exclude": "Infrastructure operations belongs to infra; AI coding tools belong to ai-devtools.",
        "aliases": ("工具", "开发者工具", "效率工具", "终端工具", "系统工具", "topic-a72ef18d9a", "topic-ac1367cb8f", "topic-6d5d7228c9", "topic-4f61e988bd", "topic-aa3a4e95d5", "topic-9ec026018c"),
    },
    {
        "id": "programming",
        "name": "编程语言",
        "description": "Programming languages, compilers, runtimes, type systems, and implementation techniques.",
        "include": "Rust, Zig, Lisp, C/C++, compilers, interpreters, language design.",
        "exclude": "End-user developer tools belong to devtools.",
        "aliases": ("编程语言", "编译技术", "计算机科学", "软件工程", "rust", "cs", "topic-6136ce4d7b", "topic-1f1be36a43", "topic-1809216cdc", "topic-5b600583be"),
    },
    {
        "id": "infra",
        "name": "系统 / 云 / DevOps",
        "description": "Operating systems, distributed systems, networking, cloud, DevOps, observability, and reliability.",
        "include": "Linux, Kubernetes, cloud infra, queues, filesystems, networking, CI/CD reliability.",
        "exclude": "Databases and storage engines belong to database.",
        "aliases": ("DevOps", "云原生", "操作系统", "数据中心", "网络工具", "平台服务", "虚拟化", "软件迁移", "devops", "topic-02b32d5713", "topic-7c30099b89", "topic-189979277d", "topic-5b04bca2ea", "topic-15c603ef75", "topic-39b2963010", "topic-8f76c9cc5d"),
    },
    {
        "id": "database",
        "name": "数据库 / 存储",
        "description": "Databases, storage systems, query engines, search, and data infrastructure.",
        "include": "Postgres, SQLite, DuckDB, search, WAL, queues in databases, storage engines.",
        "exclude": "Generic cloud operations belongs to infra.",
        "aliases": ("数据库", "数据工具", "topic-f4dbbc63a5", "topic-b813ef0aed"),
    },
    {
        "id": "security",
        "name": "安全 / 隐私",
        "description": "Security vulnerabilities, malware, privacy risks, cryptography, abuse, and incident response.",
        "include": "CVE, RCE, supply-chain attacks, surveillance, data leaks, auth bugs, VPN/privacy.",
        "exclude": "Policy-only regulation belongs to policy unless the technical security issue is central.",
        "aliases": ("安全", "网络安全", "隐私", "密码管理", "AI安全", "topic-8e662a5618", "topic-38c84c8015", "topic-4334e71fe8", "topic-3d9235402e", "topic-d1cf51e38"),
    },
    {
        "id": "web",
        "name": "Web / 互联网",
        "description": "Browsers, frontend, web platforms, protocols, social internet, and online communities.",
        "include": "Chrome, Firefox, web APIs, CSS, browser apps, federated social web, web UX.",
        "exclude": "Policy debates about platforms belong to policy when regulation is central.",
        "aliases": ("Web", "web"),
    },
    {
        "id": "opensource",
        "name": "开源生态",
        "description": "Open-source projects, governance, maintainers, licenses, package ecosystems, and code forges.",
        "include": "GitHub/GitLab/Codeberg as ecosystems, maintainership, open-source funding, package registries.",
        "exclude": "A specific developer utility belongs to devtools when the tool itself is central.",
        "aliases": ("开源", "开源生态", "开源项目", "topic-17015572d8", "topic-4385ce1e34", "topic-bbc0713c58"),
    },
    {
        "id": "hardware",
        "name": "硬件 / 芯片",
        "description": "Hardware, chips, devices, electronics, robotics hardware, and physical computing.",
        "include": "GPUs, CPUs, chips, Apple hardware, drones, robots, devices, repairability.",
        "exclude": "AI model software belongs to ai; cloud operations belongs to infra.",
        "aliases": ("硬件", "开源硬件", "无人机", "机器人", "游戏机", "键盘", "航空", "iOS", "topic-b4cd99b8d4", "topic-2a0d6b99e6", "topic-1674b06f55", "topic-df3f9d38d7", "topic-1fd8a0dedc", "topic-e2766fb28a", "topic-3b20efdc66", "ios"),
    },
    {
        "id": "policy",
        "name": "科技政策 / 法律",
        "description": "Technology policy, law, regulation, geopolitics, digital rights, and public institutions.",
        "include": "Antitrust, courts, government tech, surveillance pricing, national policy, elections, telecom regulation.",
        "exclude": "Technical vulnerability writeups belong to security.",
        "aliases": ("科技政策", "法律政策", "法律", "政治", "AI政策", "AI监管", "维修权", "科技经济", "经济", "topic-5426735a3c", "topic-bd3476976d", "topic-a08bc1dd72", "topic-745c0fb47b", "topic-61a578c991", "topic-a5311752db", "topic-951ead85a5", "topic-479e31148e", "topic-cd0bf6887d"),
    },
    {
        "id": "business",
        "name": "商业 / 创业 / 职业",
        "description": "Startups, companies, markets, jobs, careers, management, and business models.",
        "include": "Hiring posts, funding, acquisitions, company strategy, career advice, productivity at work.",
        "exclude": "Pure technology releases belong to their technical topic.",
        "aliases": ("招聘", "职业发展", "企业并购", "金融工具", "合作经济", "经济学", "topic-2e0c7289c4", "topic-68fb7d6bae", "topic-31c0f9f7f9", "topic-68ead68033", "topic-7663e76163", "topic-d4b2e0190a"),
    },
    {
        "id": "science-culture",
        "name": "科学 / 文化",
        "description": "Science, education, health, culture, history, art, society, and non-software knowledge.",
        "include": "Physics, biology, medicine, education, books, art, travel, social science, history.",
        "exclude": "Technology policy belongs to policy; hardware engineering belongs to hardware.",
        "aliases": ("科学", "文化", "教育", "物理", "太空", "太空探索", "医药健康", "趣味百科", "物理学与哲学", "数字存档", "3D设计", "趣闻", "能源", "topic-6747c881f5", "topic-0c7c55d841", "topic-492b7ebef5", "topic-00f6b25e7f", "topic-40db796201", "topic-5c47f7b4c4", "topic-67031a564f", "topic-14a7382f59", "topic-6a5652712d", "topic-5f7c9cd766", "topic-96b7b6bdf2"),
    },
    {
        "id": DEFAULT_TOPIC_ID,
        "name": DEFAULT_TOPIC_NAME,
        "description": "Only for stories that genuinely do not fit any listed topic.",
        "include": "Ambiguous or cross-domain stories after checking all other topics.",
        "exclude": "Do not use as a shortcut for unfamiliar technical stories.",
        "aliases": ("综合", "综合技术", "其他", "General", "general", "topic-ab238e3409", "topic-c00da5c4ce"),
    },
)

_CATALOG_BY_ID = {
    str(item["id"]): item for item in FIXED_TOPIC_CATALOG
}
_ALIAS_TO_ID = {}
for item in FIXED_TOPIC_CATALOG:
    topic_id = str(item["id"])
    _ALIAS_TO_ID[topic_id.casefold()] = topic_id
    _ALIAS_TO_ID[str(item["name"]).casefold()] = topic_id
    for alias in item.get("aliases", ()):
        _ALIAS_TO_ID[str(alias).casefold()] = topic_id


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
    text = text.strip(" -_/，。:：；")
    if not text or len(text) > MAX_TOPIC_NAME_CHARS:
        return ""
    return text


def legacy_topic_id(value: object) -> str:
    clean = clean_topic_id(value)
    if clean:
        return clean
    if not isinstance(value, str):
        return ""
    text = " ".join(value.strip().split())
    return text[:MAX_TOPIC_ID_CHARS]


def _alias_key(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().split()).casefold()


def _topic_entry(topic_id: str, *, count: int = 0) -> TopicEntry:
    item = _CATALOG_BY_ID.get(topic_id) or _CATALOG_BY_ID[DEFAULT_TOPIC_ID]
    return TopicEntry(id=str(item["id"]), name=str(item["name"]), count=int(count))


def fixed_topic_entries() -> Tuple[TopicEntry, ...]:
    return tuple(_topic_entry(str(item["id"])) for item in FIXED_TOPIC_CATALOG)


def topic_prompt_catalog(counts: Mapping[str, int] | None = None) -> Tuple[dict, ...]:
    counts = counts or {}
    return tuple(
        {
            "id": str(item["id"]),
            "name": str(item["name"]),
            "description": str(item["description"]),
            "include": str(item["include"]),
            "exclude": str(item["exclude"]),
            "count": int(counts.get(str(item["id"]), 0) or 0),
        }
        for item in FIXED_TOPIC_CATALOG
    )


def topic_id_from_name(name: str) -> str:
    return normalize_topic(topic_name=name)[0]


def topic_name_from_id(topic_id: str) -> str:
    topic_id = clean_topic_id(topic_id)
    return _topic_entry(topic_id).name


def topic_aliases(topic_id: str) -> set[str]:
    resolved = _resolve_fixed_topic_id(topic_id) or DEFAULT_TOPIC_ID
    item = _CATALOG_BY_ID.get(resolved) or _CATALOG_BY_ID[DEFAULT_TOPIC_ID]
    aliases = {resolved}
    for alias in item.get("aliases", ()):
        text = str(alias)
        if all(ord(ch) < 128 for ch in text):
            clean = clean_topic_id(text)
            if clean:
                aliases.add(clean)
    return aliases


def _resolve_fixed_topic_id(*values: object) -> str:
    for value in values:
        if isinstance(value, str) and all(ord(ch) < 128 for ch in value):
            clean_id = clean_topic_id(value)
            if clean_id in _CATALOG_BY_ID:
                return clean_id
        key = _alias_key(value)
        if key in _ALIAS_TO_ID:
            return _ALIAS_TO_ID[key]
    return ""


def resolve_fixed_topic(
    *,
    topic: object = None,
    topic_id: object = None,
    topic_name: object = None,
) -> Optional[Tuple[str, str]]:
    resolved = _resolve_fixed_topic_id(topic_id, topic, topic_name)
    if not resolved:
        return None
    entry = _topic_entry(resolved)
    return entry.id, entry.name


def normalize_topic(
    *,
    topic: object = None,
    topic_id: object = None,
    topic_name: object = None,
    existing_topics: Iterable[TopicEntry] | None = None,
    strict: bool = False,
) -> Tuple[str, str]:
    """Return a server-owned ``(topic_id, topic_name)`` pair.

    ``existing_topics`` is accepted for compatibility with older callers, but
    it no longer authorizes dynamic topic creation. In strict mode, generated
    or unknown topics fail loudly so the caller can retry or mark enrichment
    failed instead of publishing an inaccurate category.
    """

    if strict:
        resolved = _resolve_fixed_topic_id(topic_id, topic)
        if not resolved:
            raise ValueError("topicId must be one of the fixed topic ids")
        entry = _topic_entry(resolved)
        return entry.id, entry.name

    fixed = resolve_fixed_topic(
        topic=topic,
        topic_id=topic_id,
        topic_name=topic_name,
    )
    if fixed:
        return fixed
    return DEFAULT_TOPIC_ID, DEFAULT_TOPIC_NAME


def topic_id_set() -> set[str]:
    return {str(item["id"]) for item in FIXED_TOPIC_CATALOG}


def is_valid_topic(topic_id: str) -> bool:
    return clean_topic_id(topic_id) in _CATALOG_BY_ID
