"""AI agents for server-side insights generation."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence

from . import settings
from .ai_agent import (
    AiProviderConfig,
    RealAiAgent,
    _resolve_max_tokens,
    build_insights_ai_provider_configs,
)


FORBIDDEN_WORD_RE = re.compile(
    r"\bShow\s+HN\b|\bHacker\s*News\b|\bHackerNews\b|\bHN\b",
    re.IGNORECASE,
)


class InsightsValidationError(ValueError):
    pass


def _clean_text(value: Any, *, max_chars: int = 240) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    text = FORBIDDEN_WORD_RE.sub(
        lambda m: "产品展示" if m.group(0).lower().startswith("show") else "社区",
        text,
    )
    return text


def sanitize_forbidden_words(value: Any) -> Any:
    if isinstance(value, str):
        return _clean_text(value, max_chars=len(value) + 20)
    if isinstance(value, list):
        return [sanitize_forbidden_words(item) for item in value]
    if isinstance(value, dict):
        return {str(k): sanitize_forbidden_words(v) for k, v in value.items()}
    return value


def contains_forbidden_words(value: Any) -> bool:
    if isinstance(value, str):
        return FORBIDDEN_WORD_RE.search(value) is not None
    if isinstance(value, list):
        return any(contains_forbidden_words(item) for item in value)
    if isinstance(value, dict):
        return any(contains_forbidden_words(v) for v in value.values())
    return False


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _clamp_int(value: Any, *, min_value: int = 0, max_value: int = 100) -> int:
    try:
        n = int(round(float(value)))
    except (TypeError, ValueError):
        n = min_value
    return max(min_value, min(max_value, n))


def _require_dict(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise InsightsValidationError(f"{label} must be a JSON object")
    return value


def _require_count(items: Sequence[Any], label: str, min_count: int, max_count: int) -> None:
    if not (min_count <= len(items) <= max_count):
        if min_count == max_count:
            raise InsightsValidationError(f"{label} must contain exactly {min_count} items")
        raise InsightsValidationError(
            f"{label} must contain {min_count}-{max_count} items"
        )


def _slug(value: str, fallback: str) -> str:
    raw = re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-")
    return (raw or fallback)[:64]


TODAY_SIGNALS_SYSTEM_PROMPT = (
    "You write the opening signal panel for a paid Chinese product-research "
    "brief. This is not a news summary. Use only the provided stories, topic "
    "summary, discussion themes, and comments. Output Chinese reader-facing "
    "text, but keep JSON keys exactly as shown. The reader should understand "
    "the day's market direction in ten seconds. Produce exactly three dense "
    "judgment calls, preferably covering an opportunity, a pattern, and a "
    "risk. Each title should be a judgment about what is changing, not a topic "
    "name. Each brief should explain why the signal matters for builders, "
    "operators, or buyers. Avoid generic hype, do not list stories, and do not "
    "invent evidence beyond the input. Avoid the words HN, HackerNews, "
    "Hacker News, and Show HN. Never end a field with ellipses or an "
    "unfinished sentence; if evidence is thin, write concise complete fields. "
    "Return strict JSON only: "
    "{\"headline\":\"\",\"summary\":\"\",\"signals\":[{\"id\":\"\","
    "\"label\":\"opportunity|pattern|risk|debate\",\"title\":\"\",\"brief\":\"\","
    "\"trend\":\"+18\",\"tone\":\"up|down|flat\"}]}. "
    "signals must contain exactly 3 items."
)


TREND_HEAT_SYSTEM_PROMPT = (
    "You polish a mobile-friendly trend bar ranking for a Chinese "
    "product-research brief. Use only the provided topicDailyStats. This is a "
    "simple ranked bar list, not a heatmap matrix. Do not invent heat, "
    "deltaText, trendKey, or topic names; keep those values aligned to input. "
    "The module answers which themes are warming up and which are merely "
    "ordinary heat. Use a short note only if it helps interpret the ranking. "
    "Output Chinese reader-facing text, but keep JSON keys exactly as shown. "
    "Never end a field with ellipses or an unfinished sentence; if evidence "
    "is thin, write concise complete fields. "
    "Avoid the words HN, HackerNews, Hacker News, and Show HN. Return strict "
    "JSON only: {\"trendHeatmap\":{\"title\":\"\",\"note\":\"\","
    "\"items\":[{\"topic\":\"\",\"heat\":96,"
    "\"deltaText\":\"+18 / 24h\",\"trendKey\":\"burst\"}]}}. "
    "items must contain 5-8 items."
)


OPPORTUNITY_SYSTEM_PROMPT = (
    "You are a Chinese startup opportunity analyst writing the main paid "
    "module of a product-research brief. Use only the provided candidates, "
    "their summaries, discussion themes, comments, domains, and story ids. "
    "Score each opportunity by pain intensity, discussion heat, 7-day "
    "recurrence, small-team entry, clear paying audience, and "
    "incumbent/open-source risk. Each opportunity must read like a concrete "
    "product thesis, not a topic recap. The title should describe a specific "
    "opportunity surface. thesis is the core judgment. whyNow must explain "
    "the timing using evidence from recent discussion. risk must name the "
    "main adoption, competition, trust, or distribution risk. audience must "
    "name specific buyer/user groups. linkedStoryIds must cite the input "
    "stories that support the thesis. Output Chinese reader-facing text, but "
    "keep JSON keys exactly as shown. Avoid the words HN, HackerNews, "
    "Hacker News, and Show HN. Never end a field with ellipses or an "
    "unfinished sentence; if evidence is thin, write concise complete fields. "
    "Return strict JSON only: "
    "{\"opportunities\":[{\"rank\":1,\"rankText\":\"01\","
    "\"title\":\"\",\"score\":92,\"category\":\"\","
    "\"audience\":[\"specific buyer group\"],\"thesis\":\"\",\"whyNow\":\"\","
    "\"risk\":\"\",\"linkedStoryIds\":[123]}]}. Return 3-5 items; "
    "linkedStoryIds must come from input."
)


DEBATE_SYSTEM_PROMPT = (
    "You are a Chinese research editor writing a disagreement index for a "
    "paid product-research brief. The value is not just what is hot; the "
    "value is where smart readers disagree and what that disagreement means. "
    "Use only the provided candidates, discussion themes, insights, and "
    "comments. Do not force extreme conflict; describe tradeoffs when the "
    "evidence is mixed. topic should be a debatable claim, support should "
    "summarize the strongest pro argument, oppose should summarize the "
    "strongest counterargument, and watch should be an actionable observation "
    "for builders or buyers. Output Chinese reader-facing text, but keep JSON "
    "keys exactly as shown. Avoid the words HN, HackerNews, Hacker News, and "
    "Show HN. Never end a field with ellipses or an unfinished sentence; if "
    "evidence is thin, write concise complete fields. Return strict JSON only: "
    "{\"debates\":[{\"topic\":\"\",\"verdict\":\"\","
    "\"intensity\":91,\"supportWidth\":57,\"opposeWidth\":43,"
    "\"support\":\"\",\"oppose\":\"\",\"watch\":\"\"}]}. "
    "Return 2-4 items."
)


INSIGHTS_SIGNALS_MAX_TOKENS = 4096
INSIGHTS_TRENDS_MAX_TOKENS = 3072
INSIGHTS_OPPORTUNITIES_MAX_TOKENS = 8192
INSIGHTS_DEBATES_MAX_TOKENS = 6144


def _normalize_signal_label(value: Any) -> str:
    label = _clean_text(value, max_chars=16)
    mapped = {
        "opportunity": "机会",
        "chance": "机会",
        "pattern": "模式",
        "mode": "模式",
        "risk": "风险",
        "debate": "分歧",
        "disagreement": "分歧",
    }.get(label.strip().lower())
    return mapped or label


class InsightsAiClient(RealAiAgent):
    """Small JSON-completion surface backed by the existing provider pool."""

    def complete_json(
        self,
        *,
        purpose: str,
        system_prompt: str,
        user_payload: Mapping[str, Any],
        max_tokens: int = 2400,
    ) -> Dict[str, Any]:
        base_payload: Dict[str, Any] = {
            "temperature": 0.25,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": json.dumps(
                        user_payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                },
            ],
        }

        def _run(config: AiProviderConfig) -> Dict[str, Any]:
            response = self._post_chat_for_purpose(
                purpose,
                config,
                {
                    **base_payload,
                    "model": config.model,
                    "max_tokens": _resolve_max_tokens(config, max_tokens),
                },
            )
            raw = self._extract_json(response)
            return _require_dict(raw, purpose)

        return self._with_failover(purpose, _run)


def build_insights_ai_client() -> InsightsAiClient:
    provider = (settings.INSIGHTS_AI_PROVIDER or "").strip().lower()
    if provider in ("", "none", "fallback", "off", "disabled"):
        raise RuntimeError("insights AI provider is disabled")
    try:
        configs = build_insights_ai_provider_configs()
    except ValueError as exc:
        raise RuntimeError(f"insights AI config is invalid: {exc}") from exc
    if not configs:
        raise RuntimeError(
            "insights AI is enabled but no usable HNREADER_INSIGHTS_AI_* "
            "api_key/model config was found"
        )
    return InsightsAiClient(configs=configs)


class TodaySignalsAgent:
    purpose = "insights-signals"

    def __init__(self, client: InsightsAiClient) -> None:
        self.client = client

    def run(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        raw = self.client.complete_json(
            purpose=self.purpose,
            system_prompt=TODAY_SIGNALS_SYSTEM_PROMPT,
            user_payload=payload,
            max_tokens=INSIGHTS_SIGNALS_MAX_TOKENS,
        )
        return self.validate(raw)

    def validate(self, raw: Mapping[str, Any]) -> Dict[str, Any]:
        data = _require_dict(raw, "signals output")
        signals = _as_list(data.get("signals"))
        _require_count(signals, "signals", 3, 3)
        out = {
            "headline": _clean_text(data.get("headline"), max_chars=80),
            "summary": _clean_text(data.get("summary"), max_chars=180),
            "signals": [],
        }
        for index, item in enumerate(signals, start=1):
            obj = _require_dict(item, "signal")
            title = _clean_text(obj.get("title"), max_chars=80)
            tone = _clean_text(obj.get("tone"), max_chars=16).lower()
            if tone not in ("up", "down", "flat"):
                tone = "flat"
            out["signals"].append(
                {
                    "id": _slug(_clean_text(obj.get("id"), max_chars=80) or title, f"signal-{index}"),
                    "label": _normalize_signal_label(obj.get("label")) or "模式",
                    "title": title,
                    "brief": _clean_text(obj.get("brief"), max_chars=140),
                    "trend": _clean_text(obj.get("trend"), max_chars=16) or "+0",
                    "tone": tone,
                }
            )
        if not out["headline"] or not out["summary"]:
            raise InsightsValidationError("signals output missing headline or summary")
        if contains_forbidden_words(out):
            raise InsightsValidationError("signals output contains forbidden words")
        return out


class TrendHeatAgent:
    purpose = "insights-trends"

    def __init__(self, client: InsightsAiClient) -> None:
        self.client = client

    def run(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        raw = self.client.complete_json(
            purpose=self.purpose,
            system_prompt=TREND_HEAT_SYSTEM_PROMPT,
            user_payload=payload,
            max_tokens=INSIGHTS_TRENDS_MAX_TOKENS,
        )
        return self.validate(raw, payload)

    def validate(self, raw: Mapping[str, Any], payload: Mapping[str, Any]) -> Dict[str, Any]:
        data = _require_dict(raw, "trend output")
        heatmap = _require_dict(data.get("trendHeatmap"), "trendHeatmap")
        stats = {
            str(item.get("topic") or ""): item
            for item in _as_list(payload.get("topicDailyStats"))
            if isinstance(item, dict) and item.get("topic")
        }
        items = _as_list(heatmap.get("items"))
        _require_count(items, "trendHeatmap.items", 5, 8)
        out_items = []
        seen = set()
        for item in items:
            obj = _require_dict(item, "trend item")
            topic = _clean_text(obj.get("topic"), max_chars=60)
            if topic not in stats or topic in seen:
                raise InsightsValidationError(f"unknown or duplicate trend topic: {topic}")
            seen.add(topic)
            source = stats[topic]
            trend_key = str(source.get("trendKey") or "stable")
            if trend_key not in ("burst", "rising", "stable", "cooling"):
                trend_key = "stable"
            out_items.append(
                {
                    "topic": topic,
                    "heat": _clamp_int(source.get("heat")),
                    "deltaText": _clean_text(source.get("deltaText"), max_chars=24),
                    "trendKey": trend_key,
                }
            )
        out = {
            "trendHeatmap": {
                "title": _clean_text(heatmap.get("title"), max_chars=32) or "趋势温度",
                "note": _clean_text(heatmap.get("note"), max_chars=120)
                or "热度综合帖子数量、分数、评论量和近 24 小时变化。",
                "items": out_items,
            }
        }
        if contains_forbidden_words(out):
            raise InsightsValidationError("trend output contains forbidden words")
        return out


class OpportunityAgent:
    purpose = "insights-opportunities"

    def __init__(self, client: InsightsAiClient) -> None:
        self.client = client

    def run(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        raw = self.client.complete_json(
            purpose=self.purpose,
            system_prompt=OPPORTUNITY_SYSTEM_PROMPT,
            user_payload=payload,
            max_tokens=INSIGHTS_OPPORTUNITIES_MAX_TOKENS,
        )
        return self.validate(raw, payload)

    def validate(self, raw: Mapping[str, Any], payload: Mapping[str, Any]) -> Dict[str, Any]:
        data = _require_dict(raw, "opportunities output")
        candidates = _as_list(payload.get("candidates"))
        allowed_ids = {
            int(item.get("id"))
            for item in candidates
            if isinstance(item, dict) and item.get("id") is not None
        }
        items = _as_list(data.get("opportunities"))
        _require_count(items, "opportunities", 3, 5)
        out_items = []
        for item in items:
            obj = _require_dict(item, "opportunity")
            linked = []
            for raw_id in _as_list(obj.get("linkedStoryIds")):
                try:
                    sid = int(raw_id)
                except (TypeError, ValueError):
                    continue
                if sid not in allowed_ids:
                    raise InsightsValidationError(f"linkedStoryId {sid} is not a candidate")
                if sid not in linked:
                    linked.append(sid)
            if not linked:
                raise InsightsValidationError("opportunity missing linkedStoryIds")
            audience = [
                _clean_text(v, max_chars=20)
                for v in _as_list(obj.get("audience"))
                if _clean_text(v, max_chars=20)
            ]
            out_items.append(
                {
                    "rank": 0,
                    "rankText": "",
                    "title": _clean_text(obj.get("title"), max_chars=80),
                    "score": _clamp_int(obj.get("score")),
                    "category": _clean_text(obj.get("category"), max_chars=40),
                    "audience": audience or ["开发者"],
                    "thesis": _clean_text(obj.get("thesis"), max_chars=180),
                    "whyNow": _clean_text(obj.get("whyNow"), max_chars=160),
                    "risk": _clean_text(obj.get("risk"), max_chars=140),
                    "linkedStoryIds": linked,
                }
            )
        out_items.sort(key=lambda x: int(x["score"]), reverse=True)
        for index, item in enumerate(out_items, start=1):
            item["rank"] = index
            item["rankText"] = f"{index:02d}"
        out = {"opportunities": out_items}
        if contains_forbidden_words(out):
            raise InsightsValidationError("opportunities output contains forbidden words")
        return out


class DebateAgent:
    purpose = "insights-debates"

    def __init__(self, client: InsightsAiClient) -> None:
        self.client = client

    def run(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        raw = self.client.complete_json(
            purpose=self.purpose,
            system_prompt=DEBATE_SYSTEM_PROMPT,
            user_payload=payload,
            max_tokens=INSIGHTS_DEBATES_MAX_TOKENS,
        )
        return self.validate(raw)

    def validate(self, raw: Mapping[str, Any]) -> Dict[str, Any]:
        data = _require_dict(raw, "debates output")
        items = _as_list(data.get("debates"))
        _require_count(items, "debates", 2, 4)
        out_items = []
        for item in items:
            obj = _require_dict(item, "debate")
            support = _clamp_int(obj.get("supportWidth"))
            oppose = _clamp_int(obj.get("opposeWidth"))
            total = support + oppose
            if total <= 0:
                support, oppose = 50, 50
            else:
                support = int(round(support * 100 / total))
                oppose = 100 - support
            out_items.append(
                {
                    "topic": _clean_text(obj.get("topic"), max_chars=80),
                    "verdict": _clean_text(obj.get("verdict"), max_chars=32)
                    or "分歧仍在观察",
                    "intensity": _clamp_int(obj.get("intensity")),
                    "supportWidth": support,
                    "opposeWidth": oppose,
                    "support": _clean_text(obj.get("support"), max_chars=180),
                    "oppose": _clean_text(obj.get("oppose"), max_chars=180),
                    "watch": _clean_text(obj.get("watch"), max_chars=160),
                }
            )
        out = {"debates": out_items}
        if contains_forbidden_words(out):
            raise InsightsValidationError("debates output contains forbidden words")
        return out


class InsightsAgentRunner:
    def __init__(self, client: Optional[InsightsAiClient] = None) -> None:
        self.client = client or build_insights_ai_client()
        self.signals_agent = TodaySignalsAgent(self.client)
        self.trends_agent = TrendHeatAgent(self.client)
        self.opportunities_agent = OpportunityAgent(self.client)
        self.debates_agent = DebateAgent(self.client)

    def usage_checkpoint(self) -> int:
        return self.client.usage_checkpoint()

    def usage_summary_since(self, checkpoint: int):
        return self.client.usage_summary_since(
            checkpoint,
            purposes=(
                TodaySignalsAgent.purpose,
                TrendHeatAgent.purpose,
                OpportunityAgent.purpose,
                DebateAgent.purpose,
            ),
        )

    def run_signals(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        return self.signals_agent.run(payload)

    def run_trends(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        return self.trends_agent.run(payload)

    def run_opportunities(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        return self.opportunities_agent.run(payload)

    def run_debates(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        return self.debates_agent.run(payload)


__all__ = [
    "DebateAgent",
    "FORBIDDEN_WORD_RE",
    "InsightsAgentRunner",
    "InsightsAiClient",
    "InsightsValidationError",
    "DEBATE_SYSTEM_PROMPT",
    "INSIGHTS_DEBATES_MAX_TOKENS",
    "INSIGHTS_OPPORTUNITIES_MAX_TOKENS",
    "INSIGHTS_SIGNALS_MAX_TOKENS",
    "INSIGHTS_TRENDS_MAX_TOKENS",
    "OPPORTUNITY_SYSTEM_PROMPT",
    "OpportunityAgent",
    "TODAY_SIGNALS_SYSTEM_PROMPT",
    "TREND_HEAT_SYSTEM_PROMPT",
    "TodaySignalsAgent",
    "TrendHeatAgent",
    "build_insights_ai_client",
    "contains_forbidden_words",
    "sanitize_forbidden_words",
]
