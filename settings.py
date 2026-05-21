"""Runtime configuration knobs.

All values may be overridden via ``HNREADER_*`` environment variables.
``HACKERMINI_*`` is honored as a compatibility fallback for deployments
that still set the pre-rebrand names; the canonical ``HNREADER_*`` value
always wins when both are set.

The DB path additionally supports a programmatic override via
``set_db_path(...)`` so unit tests can point at a temp file.
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.9+ has zoneinfo built in
    ZoneInfo = None  # type: ignore[assignment]


_PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = _PACKAGE_DIR / "data" / "hnreader.db"
LEGACY_DB_PATH = _PACKAGE_DIR / "data" / "hackermini.db"

_db_path_override: Optional[Path] = None


def _env_first(*names: str) -> Optional[str]:
    """Return the first non-empty value among ``names`` (canonical first)."""
    for n in names:
        v = os.environ.get(n)
        if v is not None and v != "":
            return v
    return None


def _env_if_present(*names: str) -> Optional[str]:
    """Return the first explicitly-set env value, including an empty string."""
    for n in names:
        if n in os.environ:
            return os.environ.get(n) or ""
    return None


def _path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return False


def get_db_path() -> Path:
    if _db_path_override is not None:
        return _db_path_override
    env = _env_first("HNREADER_DB_PATH", "HACKERMINI_DB_PATH")
    if env:
        return Path(env)
    # Migration grace: a pre-rebrand deployment has data only in
    # ``hackermini.db``. Pointing fresh at the new default would silently
    # start with an empty DB. Prefer the legacy file when the new default
    # has not been created yet, so existing data keeps serving.
    if _path_exists(LEGACY_DB_PATH) and not _path_exists(DEFAULT_DB_PATH):
        return LEGACY_DB_PATH
    return DEFAULT_DB_PATH


def set_db_path(path: Optional[Path]) -> None:
    global _db_path_override
    _db_path_override = path


def get_alert_outbox_path() -> Path:
    env = _env_first("HNREADER_ALERT_OUTBOX_PATH", "HACKERMINI_ALERT_OUTBOX_PATH")
    if env:
        return Path(env)
    return get_db_path().with_name("alerts.jsonl")


def get_ai_config_status_cache_path() -> Path:
    env = _env_first(
        "HNREADER_AI_CONFIG_STATUS_CACHE_PATH",
        "HACKERMINI_AI_CONFIG_STATUS_CACHE_PATH",
    )
    if env:
        return Path(env)
    return get_db_path().with_name("ai-config-status-cache.json")


def get_cloud_sync_output_dir() -> Path:
    """Where cloud_sync.build_read_model writes the JSON read model.

    Defaults to a sibling of the SQLite DB so it stays inside the systemd
    unit's ``ReadWritePaths`` (set to the DB directory). Putting it under
    the package directory used to fail under ``ProtectSystem=strict``.
    """
    env = _env_first(
        "HNREADER_CLOUD_SYNC_OUTPUT_DIR",
        "HACKERMINI_CLOUD_SYNC_OUTPUT_DIR",
    )
    if env:
        return Path(env)
    return get_db_path().with_name(".cloud-sync-output")


def _env_int(name: str, default: int, *, fallback: Optional[str] = None) -> int:
    raw = os.environ.get(name)
    source = name
    if (raw is None or raw == "") and fallback:
        raw = os.environ.get(fallback)
        source = fallback
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{source} must be an integer") from exc


def _env_optional_int(name: str, *, fallback: Optional[str] = None) -> Optional[int]:
    raw = os.environ.get(name)
    source = name
    if (raw is None or raw == "") and fallback:
        raw = os.environ.get(fallback)
        source = fallback
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{source} must be an integer") from exc


def _env_float(name: str, default: float, *, fallback: Optional[str] = None) -> float:
    raw = os.environ.get(name)
    source = name
    if (raw is None or raw == "") and fallback:
        raw = os.environ.get(fallback)
        source = fallback
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{source} must be a number") from exc


def _env_bool(name: str, default: bool, *, fallback: Optional[str] = None) -> bool:
    """Read a boolean env var. ``fallback`` provides a compat name read only
    when the canonical name is unset."""
    raw = os.environ.get(name)
    source = name
    if (raw is None or raw == "") and fallback:
        raw = os.environ.get(fallback)
        source = fallback
    if raw is None or raw == "":
        return default
    normalized = raw.strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    raise RuntimeError(f"{source} must be a boolean")


def _env_str(
    name: str, default: str, *, fallback: Optional[str] = None
) -> str:
    raw = os.environ.get(name)
    if (raw is None or raw == "") and fallback:
        raw = os.environ.get(fallback)
    if raw is None:
        return default
    return raw


def _env_csv(name: str, default: str = "", *, fallback: Optional[str] = None) -> Tuple[str, ...]:
    raw = os.environ.get(name)
    if (raw is None or raw == "") and fallback:
        raw = os.environ.get(fallback)
    if raw is None or raw == "":
        raw = default
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def _require_int_range(
    name: str,
    value: int,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    if min_value is not None and value < min_value:
        raise RuntimeError(f"{name} must be >= {min_value}")
    if max_value is not None and value > max_value:
        raise RuntimeError(f"{name} must be <= {max_value}")
    return value


def _require_float_range(
    name: str,
    value: float,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if min_value is not None and value < min_value:
        raise RuntimeError(f"{name} must be >= {min_value}")
    if max_value is not None and value > max_value:
        raise RuntimeError(f"{name} must be <= {max_value}")
    return value


def _require_less(
    left_name: str,
    left_value: int,
    right_name: str,
    right_value: int,
) -> None:
    if left_value >= right_value:
        raise RuntimeError(f"{left_name} must be smaller than {right_name}")


def _require_choice(name: str, value: str, choices: Tuple[str, ...]) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in choices:
        raise RuntimeError(f"{name} must be one of: {', '.join(choices)}")
    return normalized


def _require_timezone(name: str, value: str) -> str:
    clean = str(value or "").strip()
    if not clean:
        raise RuntimeError(f"{name} must not be empty")
    if ZoneInfo is not None:
        try:
            ZoneInfo(clean)
        except Exception as exc:
            raise RuntimeError(f"{name} must be a valid IANA timezone") from exc
    return clean


# ---------- Feed / window sizing ----------

FEED_WINDOW_SIZE = _require_int_range(
    "HNREADER_FEED_WINDOW_SIZE",
    _env_int(
        "HNREADER_FEED_WINDOW_SIZE", 100, fallback="HACKERMINI_FEED_WINDOW_SIZE"
    ),
    min_value=1,
    max_value=500,
)

# Hard cap for the deduplicated story backing store. Active rankings,
# current staging rows, in-flight enrich jobs, and retained digest references
# are protected; overflow cleanup evicts lower-value unprotected rows first.
STORY_STORE_MAX_ROWS = _require_int_range(
    "HNREADER_STORY_STORE_MAX_ROWS",
    _env_int(
        "HNREADER_STORY_STORE_MAX_ROWS",
        1000,
        fallback="HACKERMINI_STORY_STORE_MAX_ROWS",
    ),
    min_value=1,
    max_value=100000,
)

# ---------- Comment fetching ----------

COMMENT_FETCH_LIMIT = _require_int_range(
    "HNREADER_COMMENT_FETCH_LIMIT",
    _env_int(
        "HNREADER_COMMENT_FETCH_LIMIT", 60, fallback="HACKERMINI_COMMENT_FETCH_LIMIT"
    ),
    min_value=0,
    max_value=500,
)
COMMENT_MAX_DEPTH = _require_int_range(
    "HNREADER_COMMENT_MAX_DEPTH",
    _env_int("HNREADER_COMMENT_MAX_DEPTH", 2, fallback="HACKERMINI_COMMENT_MAX_DEPTH"),
    min_value=0,
    max_value=10,
)
COMMENT_MIN_DESCENDANTS = _require_int_range(
    "HNREADER_COMMENT_MIN_DESCENDANTS",
    _env_int(
        "HNREADER_COMMENT_MIN_DESCENDANTS",
        5,
        fallback="HACKERMINI_COMMENT_MIN_DESCENDANTS",
    ),
    min_value=0,
    max_value=10000,
)

# ---------- Enricher recovery / retention ----------

ENRICH_STALE_SECONDS = _require_int_range(
    "HNREADER_ENRICH_STALE_SECONDS",
    _env_int(
        "HNREADER_ENRICH_STALE_SECONDS",
        10 * 60,
        fallback="HACKERMINI_ENRICH_STALE_SECONDS",
    ),
    min_value=1,
    max_value=24 * 60 * 60,
)
ENRICH_MAX_ATTEMPTS = _require_int_range(
    "HNREADER_ENRICH_MAX_ATTEMPTS",
    _env_int(
        "HNREADER_ENRICH_MAX_ATTEMPTS", 3, fallback="HACKERMINI_ENRICH_MAX_ATTEMPTS"
    ),
    min_value=1,
    max_value=20,
)
# Default kept in lockstep with ``server/launcher.sh`` and ``server/launcher.bat``.
# Real AI agents further cap this from provider max_output_tokens. With the
# current 3200-token per-story output budget, an 8000-token provider runs two
# stories per batch; if no provider cap is available, the configured default
# still prevents the old naked ``python -m server.ingest`` batch=20 truncation.
ENRICH_BATCH_SIZE = _require_int_range(
    "HNREADER_ENRICH_BATCH_SIZE",
    _env_int(
        "HNREADER_ENRICH_BATCH_SIZE", 3, fallback="HACKERMINI_ENRICH_BATCH_SIZE"
    ),
    min_value=1,
    max_value=100,
)
ENRICH_WORKER_COUNT = _require_int_range(
    "HNREADER_ENRICH_WORKER_COUNT",
    _env_int(
        "HNREADER_ENRICH_WORKER_COUNT", 8, fallback="HACKERMINI_ENRICH_WORKER_COUNT"
    ),
    min_value=1,
    max_value=32,
)
ENRICH_SESSION_STORY_LIMIT = _require_int_range(
    "HNREADER_ENRICH_SESSION_STORY_LIMIT",
    _env_int(
        "HNREADER_ENRICH_SESSION_STORY_LIMIT",
        16,
        fallback="HACKERMINI_ENRICH_SESSION_STORY_LIMIT",
    ),
    min_value=1,
    max_value=200,
)
REENRICH_DESCENDANTS_MIN_DELTA = _require_int_range(
    "HNREADER_REENRICH_DESCENDANTS_MIN_DELTA",
    _env_int(
        "HNREADER_REENRICH_DESCENDANTS_MIN_DELTA",
        20,
        fallback="HACKERMINI_REENRICH_DESCENDANTS_MIN_DELTA",
    ),
    min_value=0,
    max_value=10000,
)
REENRICH_DESCENDANTS_MIN_GROWTH_PERCENT = _require_int_range(
    "HNREADER_REENRICH_DESCENDANTS_MIN_GROWTH_PERCENT",
    _env_int(
        "HNREADER_REENRICH_DESCENDANTS_MIN_GROWTH_PERCENT",
        30,
        fallback="HACKERMINI_REENRICH_DESCENDANTS_MIN_GROWTH_PERCENT",
    ),
    min_value=0,
    max_value=1000,
)
COMMENT_RETENTION_DAYS = _require_int_range(
    "HNREADER_COMMENT_RETENTION_DAYS",
    _env_int(
        "HNREADER_COMMENT_RETENTION_DAYS",
        7,
        fallback="HACKERMINI_COMMENT_RETENTION_DAYS",
    ),
    min_value=0,
    max_value=3650,
)
DIGEST_RETENTION_DAYS = _require_int_range(
    "HNREADER_DIGEST_RETENTION_DAYS",
    _env_int(
        "HNREADER_DIGEST_RETENTION_DAYS",
        30,
        fallback="HACKERMINI_DIGEST_RETENTION_DAYS",
    ),
    min_value=0,
    max_value=3650,
)
INGEST_RUN_RETENTION_DAYS = _require_int_range(
    "HNREADER_INGEST_RUN_RETENTION_DAYS",
    _env_int(
        "HNREADER_INGEST_RUN_RETENTION_DAYS",
        30,
        fallback="HACKERMINI_INGEST_RUN_RETENTION_DAYS",
    ),
    min_value=0,
    max_value=3650,
)
CLOUD_SYNC_RUN_RETENTION_DAYS = _require_int_range(
    "HNREADER_CLOUD_SYNC_RUN_RETENTION_DAYS",
    _env_int(
        "HNREADER_CLOUD_SYNC_RUN_RETENTION_DAYS",
        30,
        fallback="HACKERMINI_CLOUD_SYNC_RUN_RETENTION_DAYS",
    ),
    min_value=0,
    max_value=3650,
)
RANKING_CANDIDATE_RETENTION_DAYS = _require_int_range(
    "HNREADER_RANKING_CANDIDATE_RETENTION_DAYS",
    _env_int(
        "HNREADER_RANKING_CANDIDATE_RETENTION_DAYS",
        3,
        fallback="HACKERMINI_RANKING_CANDIDATE_RETENTION_DAYS",
    ),
    min_value=0,
    max_value=3650,
)

# ---------- Digester cadence and gates ----------

DIGEST_UPDATE_INTERVAL_SECONDS = _require_int_range(
    "HNREADER_DIGEST_UPDATE_INTERVAL_SECONDS",
    _env_int(
        "HNREADER_DIGEST_UPDATE_INTERVAL_SECONDS",
        30 * 60,
        fallback="HACKERMINI_DIGEST_UPDATE_INTERVAL_SECONDS",
    ),
    min_value=0,
    max_value=24 * 60 * 60,
)
DIGEST_MIN_NEW_DONE_STORIES = _require_int_range(
    "HNREADER_DIGEST_MIN_NEW_DONE_STORIES",
    _env_int(
        "HNREADER_DIGEST_MIN_NEW_DONE_STORIES",
        3,
        fallback="HACKERMINI_DIGEST_MIN_NEW_DONE_STORIES",
    ),
    min_value=0,
    max_value=100,
)
DIGEST_MAX_STORIES = _require_int_range(
    "HNREADER_DIGEST_MAX_STORIES",
    _env_int(
        "HNREADER_DIGEST_MAX_STORIES", 7, fallback="HACKERMINI_DIGEST_MAX_STORIES"
    ),
    min_value=1,
    max_value=50,
)
DIGEST_TIMEZONE = _require_timezone(
    "HNREADER_DIGEST_TIMEZONE",
    _env_str(
        "HNREADER_DIGEST_TIMEZONE",
        "Asia/Shanghai",
        fallback="HACKERMINI_DIGEST_TIMEZONE",
    ),
)

# ---------- Insights cadence and gates ----------

def _default_insights_update_interval_seconds() -> int:
    return 60 * 60


INSIGHTS_ENABLED = _env_bool("HNREADER_INSIGHTS_ENABLED", True)
INSIGHTS_WINDOW_DAYS = _require_int_range(
    "HNREADER_INSIGHTS_WINDOW_DAYS",
    _env_int("HNREADER_INSIGHTS_WINDOW_DAYS", 7),
    min_value=1,
    max_value=14,
)
INSIGHTS_UPDATE_INTERVAL_SECONDS = _require_int_range(
    "HNREADER_INSIGHTS_UPDATE_INTERVAL_SECONDS",
    _env_int(
        "HNREADER_INSIGHTS_UPDATE_INTERVAL_SECONDS",
        _default_insights_update_interval_seconds(),
    ),
    min_value=0,
    max_value=4 * 24 * 60 * 60,
)
INSIGHTS_MIN_TODAY_STORIES = _require_int_range(
    "HNREADER_INSIGHTS_MIN_TODAY_STORIES",
    _env_int("HNREADER_INSIGHTS_MIN_TODAY_STORIES", 10),
    min_value=0,
    max_value=500,
)
INSIGHTS_MAX_TODAY_STORIES = _require_int_range(
    "HNREADER_INSIGHTS_MAX_TODAY_STORIES",
    _env_int("HNREADER_INSIGHTS_MAX_TODAY_STORIES", 120),
    min_value=1,
    max_value=1000,
)
if INSIGHTS_MAX_TODAY_STORIES < INSIGHTS_MIN_TODAY_STORIES:
    raise RuntimeError(
        "HNREADER_INSIGHTS_MAX_TODAY_STORIES must be >= "
        "HNREADER_INSIGHTS_MIN_TODAY_STORIES"
    )
INSIGHTS_EVIDENCE_MAX_STORIES = _require_int_range(
    "HNREADER_INSIGHTS_EVIDENCE_MAX_STORIES",
    _env_int("HNREADER_INSIGHTS_EVIDENCE_MAX_STORIES", 200),
    min_value=1,
    max_value=1000,
)
INSIGHTS_EVIDENCE_COMMENT_LIMIT_PER_STORY = _require_int_range(
    "HNREADER_INSIGHTS_EVIDENCE_COMMENT_LIMIT_PER_STORY",
    _env_int("HNREADER_INSIGHTS_EVIDENCE_COMMENT_LIMIT_PER_STORY", 12),
    min_value=0,
    max_value=200,
)
INSIGHTS_EVIDENCE_BATCH_STORIES = _require_int_range(
    "HNREADER_INSIGHTS_EVIDENCE_BATCH_STORIES",
    _env_int("HNREADER_INSIGHTS_EVIDENCE_BATCH_STORIES", 20),
    min_value=1,
    max_value=200,
)
INSIGHTS_EVIDENCE_CACHE_RETENTION_DAYS = _require_int_range(
    "HNREADER_INSIGHTS_EVIDENCE_CACHE_RETENTION_DAYS",
    _env_int("HNREADER_INSIGHTS_EVIDENCE_CACHE_RETENTION_DAYS", 14),
    min_value=0,
    max_value=3650,
)

# Keep the dynamic classification taxonomy broad enough to cover the product
# surface without letting one-off story topics proliferate.
TOPIC_MAX_ACTIVE_TOPICS = _require_int_range(
    "HNREADER_TOPIC_MAX_ACTIVE_TOPICS",
    _env_int(
        "HNREADER_TOPIC_MAX_ACTIVE_TOPICS",
        16,
        fallback="HACKERMINI_TOPIC_MAX_ACTIVE_TOPICS",
    ),
    min_value=1,
    max_value=100,
)
TOPIC_RETENTION_DAYS = _require_int_range(
    "HNREADER_TOPIC_RETENTION_DAYS",
    _env_int(
        "HNREADER_TOPIC_RETENTION_DAYS",
        30,
        fallback="HACKERMINI_TOPIC_RETENTION_DAYS",
    ),
    min_value=0,
    max_value=3650,
)

# ---------- Cleanup safety ----------

RANKING_GRACE_SECONDS = _require_int_range(
    "HNREADER_RANKING_GRACE_SECONDS",
    _env_int(
        "HNREADER_RANKING_GRACE_SECONDS",
        24 * 60 * 60,
        fallback="HACKERMINI_RANKING_GRACE_SECONDS",
    ),
    min_value=1,
    max_value=30 * 24 * 60 * 60,
)
CLEANUP_STALE_GUARD_SECONDS = _require_int_range(
    "HNREADER_CLEANUP_STALE_GUARD_SECONDS",
    _env_int(
        "HNREADER_CLEANUP_STALE_GUARD_SECONDS",
        12 * 60 * 60,
        fallback="HACKERMINI_CLEANUP_STALE_GUARD_SECONDS",
    ),
    min_value=1,
    max_value=30 * 24 * 60 * 60,
)

# Per plan §Configuration: CLEANUP_STALE_GUARD_SECONDS must be < RANKING_GRACE_SECONDS,
# otherwise the guard would only trigger after the deletion window and be ineffective.
_require_less(
    "HNREADER_CLEANUP_STALE_GUARD_SECONDS",
    CLEANUP_STALE_GUARD_SECONDS,
    "HNREADER_RANKING_GRACE_SECONDS",
    RANKING_GRACE_SECONDS,
)

# ---------- AI provider ----------

AI_CONFIG_FILE = _env_str(
    "HNREADER_AI_CONFIG_FILE", "", fallback="HACKERMINI_AI_CONFIG_FILE"
)
AI_PROVIDER = _env_str(
    "HNREADER_AI_PROVIDER", "none", fallback="HACKERMINI_AI_PROVIDER"
).strip().lower()
AI_CONFIGS_JSON = _env_str(
    "HNREADER_AI_CONFIGS", "", fallback="HACKERMINI_AI_CONFIGS"
)
AI_API_KEY = _env_str("HNREADER_AI_API_KEY", "", fallback="HACKERMINI_AI_API_KEY")
AI_MODEL = _env_str("HNREADER_AI_MODEL", "", fallback="HACKERMINI_AI_MODEL")
AI_BASE_URL = _env_str("HNREADER_AI_BASE_URL", "", fallback="HACKERMINI_AI_BASE_URL")
# Operator escape hatch for the AI base_url / balance_url denylist that
# blocks private / link-local / cloud-metadata hosts. Comma-separated
# list of exact hostnames; matched hosts skip the SSRF check (use only
# when the provider sits behind a legitimate internal proxy).
AI_INTERNAL_HOST_ALLOWLIST = _env_csv(
    "HNREADER_AI_INTERNAL_HOST_ALLOWLIST",
    fallback="HACKERMINI_AI_INTERNAL_HOST_ALLOWLIST",
)
AI_REQUEST_TIMEOUT_SECONDS = _require_float_range(
    "HNREADER_AI_REQUEST_TIMEOUT_SECONDS",
    _env_float(
        "HNREADER_AI_REQUEST_TIMEOUT_SECONDS",
        60.0,
        fallback="HACKERMINI_AI_REQUEST_TIMEOUT_SECONDS",
    ),
    min_value=0.1,
    max_value=600.0,
)
AI_CONFIG_STATUS_CACHE_TTL_SECONDS = _require_int_range(
    "HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS",
    _env_int(
        "HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS",
        60,
        fallback="HACKERMINI_AI_CONFIG_STATUS_CACHE_TTL_SECONDS",
    ),
    min_value=1,
    max_value=3600,
)

# AI prompt body/comment trimming. COMMENT_FETCH_LIMIT controls how many
# comments are stored for the API surface; the AI prompt does not need that
# many. Bigger prompts mean longer requests, more IncompleteRead, and lower
# batch parallelism — keep the slice tight.
AI_ENRICH_BODY_MAX_CHARS = _require_int_range(
    "HNREADER_AI_ENRICH_BODY_MAX_CHARS",
    _env_int(
        "HNREADER_AI_ENRICH_BODY_MAX_CHARS",
        1200,
        fallback="HACKERMINI_AI_ENRICH_BODY_MAX_CHARS",
    ),
    min_value=0,
    max_value=8000,
)
AI_ENRICH_COMMENT_LIMIT = _require_int_range(
    "HNREADER_AI_ENRICH_COMMENT_LIMIT",
    _env_int(
        "HNREADER_AI_ENRICH_COMMENT_LIMIT",
        18,
        fallback="HACKERMINI_AI_ENRICH_COMMENT_LIMIT",
    ),
    min_value=0,
    max_value=200,
)
AI_ENRICH_COMMENT_MAX_CHARS = _require_int_range(
    "HNREADER_AI_ENRICH_COMMENT_MAX_CHARS",
    _env_int(
        "HNREADER_AI_ENRICH_COMMENT_MAX_CHARS",
        240,
        fallback="HACKERMINI_AI_ENRICH_COMMENT_MAX_CHARS",
    ),
    min_value=50,
    max_value=2000,
)

# ---------- Codex CLI primary AI path ----------

CODEX_ENABLED = _env_bool(
    "HNREADER_CODEX_ENABLED", True, fallback="HACKERMINI_CODEX_ENABLED"
)
CODEX_CLI_PATH = _env_str(
    "HNREADER_CODEX_CLI_PATH", "codex", fallback="HACKERMINI_CODEX_CLI_PATH"
)
CODEX_HOME = _env_str(
    "HNREADER_CODEX_HOME", "", fallback="HACKERMINI_CODEX_HOME"
).strip()
CODEX_EXTRA_PATH = _env_str(
    "HNREADER_CODEX_EXTRA_PATH", "", fallback="HACKERMINI_CODEX_EXTRA_PATH"
).strip()
CODEX_MODEL = _env_str(
    "HNREADER_CODEX_MODEL", "", fallback="HACKERMINI_CODEX_MODEL"
).strip()
CODEX_REQUEST_TIMEOUT_SECONDS = _require_float_range(
    "HNREADER_CODEX_REQUEST_TIMEOUT_SECONDS",
    _env_float(
        "HNREADER_CODEX_REQUEST_TIMEOUT_SECONDS",
        900.0,
        fallback="HACKERMINI_CODEX_REQUEST_TIMEOUT_SECONDS",
    ),
    min_value=1.0,
    max_value=1800.0,
)
CODEX_IGNORE_USER_CONFIG = _env_bool(
    "HNREADER_CODEX_IGNORE_USER_CONFIG",
    False,
    fallback="HACKERMINI_CODEX_IGNORE_USER_CONFIG",
)

# ---------- Insights AI provider ----------

INSIGHTS_AI_CONFIG_FILE = _env_str("HNREADER_INSIGHTS_AI_CONFIG_FILE", "")
INSIGHTS_AI_PROVIDER = _env_str(
    "HNREADER_INSIGHTS_AI_PROVIDER", "enabled"
).strip().lower()
INSIGHTS_AI_CONFIGS_JSON = _env_str("HNREADER_INSIGHTS_AI_CONFIGS", "")
INSIGHTS_AI_API_KEY = _env_str("HNREADER_INSIGHTS_AI_API_KEY", "")
INSIGHTS_AI_MODEL = _env_str("HNREADER_INSIGHTS_AI_MODEL", "")
INSIGHTS_AI_BASE_URL = _env_str("HNREADER_INSIGHTS_AI_BASE_URL", "")
INSIGHTS_AI_INTERNAL_HOST_ALLOWLIST = _env_csv(
    "HNREADER_INSIGHTS_AI_INTERNAL_HOST_ALLOWLIST"
)
INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS = _require_float_range(
    "HNREADER_INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS",
    _env_float("HNREADER_INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS", 120.0),
    min_value=0.1,
    max_value=600.0,
)
INSIGHTS_AI_MAX_OUTPUT_TOKENS = _require_int_range(
    "HNREADER_INSIGHTS_AI_MAX_OUTPUT_TOKENS",
    _env_optional_int("HNREADER_INSIGHTS_AI_MAX_OUTPUT_TOKENS") or 0,
    min_value=0,
    max_value=1_000_000,
) or None

INSIGHTS_COMPRESSION_AI_CONFIG_FILE = _env_str(
    "HNREADER_INSIGHTS_COMPRESSION_AI_CONFIG_FILE",
    INSIGHTS_AI_CONFIG_FILE,
)
INSIGHTS_COMPRESSION_AI_PROVIDER = (
    _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_PROVIDER", "")
    or INSIGHTS_AI_PROVIDER
).strip().lower()
INSIGHTS_COMPRESSION_AI_CONFIGS_JSON = (
    _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_CONFIGS", "")
    or INSIGHTS_AI_CONFIGS_JSON
)
INSIGHTS_COMPRESSION_AI_API_KEY = (
    _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_API_KEY", "")
    or INSIGHTS_AI_API_KEY
)
INSIGHTS_COMPRESSION_AI_MODEL = (
    _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_MODEL", "")
    or INSIGHTS_AI_MODEL
)
INSIGHTS_COMPRESSION_AI_BASE_URL = (
    _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_BASE_URL", "")
    or INSIGHTS_AI_BASE_URL
)
INSIGHTS_COMPRESSION_AI_INTERNAL_HOST_ALLOWLIST = (
    _env_csv("HNREADER_INSIGHTS_COMPRESSION_AI_INTERNAL_HOST_ALLOWLIST")
    or INSIGHTS_AI_INTERNAL_HOST_ALLOWLIST
)
INSIGHTS_COMPRESSION_AI_REQUEST_TIMEOUT_SECONDS = _require_float_range(
    "HNREADER_INSIGHTS_COMPRESSION_AI_REQUEST_TIMEOUT_SECONDS",
    _env_float(
        "HNREADER_INSIGHTS_COMPRESSION_AI_REQUEST_TIMEOUT_SECONDS",
        INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS,
    ),
    min_value=0.1,
    max_value=600.0,
)
_INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS_RAW = _env_optional_int(
    "HNREADER_INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS"
)
INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS = _require_int_range(
    "HNREADER_INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS",
    (
        _INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS_RAW
        if _INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS_RAW is not None
        else (INSIGHTS_AI_MAX_OUTPUT_TOKENS or 0)
    ),
    min_value=0,
    max_value=1_000_000,
) or None


_AI_ENV_NAMES = {
    "HNREADER_CODEX_ENABLED",
    "HNREADER_CODEX_CLI_PATH",
    "HNREADER_CODEX_HOME",
    "HNREADER_CODEX_EXTRA_PATH",
    "HNREADER_CODEX_MODEL",
    "HNREADER_CODEX_REQUEST_TIMEOUT_SECONDS",
    "HNREADER_CODEX_IGNORE_USER_CONFIG",
    "HNREADER_AI_CONFIG_FILE",
    "HNREADER_AI_PROVIDER",
    "HNREADER_AI_CONFIGS",
    "HNREADER_AI_API_KEY",
    "HNREADER_AI_MODEL",
    "HNREADER_AI_BASE_URL",
    "HNREADER_AI_INTERNAL_HOST_ALLOWLIST",
    "HNREADER_AI_REQUEST_TIMEOUT_SECONDS",
    "HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS",
    "HNREADER_AI_CONFIG_STATUS_CACHE_PATH",
    "HNREADER_AI_ENRICH_BODY_MAX_CHARS",
    "HNREADER_AI_ENRICH_COMMENT_LIMIT",
    "HNREADER_AI_ENRICH_COMMENT_MAX_CHARS",
    "HACKERMINI_AI_CONFIG_FILE",
    "HACKERMINI_AI_PROVIDER",
    "HACKERMINI_AI_CONFIGS",
    "HACKERMINI_AI_API_KEY",
    "HACKERMINI_AI_MODEL",
    "HACKERMINI_AI_BASE_URL",
    "HACKERMINI_AI_INTERNAL_HOST_ALLOWLIST",
    "HACKERMINI_AI_REQUEST_TIMEOUT_SECONDS",
    "HACKERMINI_AI_CONFIG_STATUS_CACHE_TTL_SECONDS",
    "HACKERMINI_AI_CONFIG_STATUS_CACHE_PATH",
    "HACKERMINI_AI_ENRICH_BODY_MAX_CHARS",
    "HACKERMINI_AI_ENRICH_COMMENT_LIMIT",
    "HACKERMINI_AI_ENRICH_COMMENT_MAX_CHARS",
    "HACKERMINI_CODEX_ENABLED",
    "HACKERMINI_CODEX_CLI_PATH",
    "HACKERMINI_CODEX_HOME",
    "HACKERMINI_CODEX_EXTRA_PATH",
    "HACKERMINI_CODEX_MODEL",
    "HACKERMINI_CODEX_REQUEST_TIMEOUT_SECONDS",
    "HACKERMINI_CODEX_IGNORE_USER_CONFIG",
}

_AI_JSON_FIELD_ENV_NAMES = {
    "provider": "HNREADER_AI_PROVIDER",
    "configs": "HNREADER_AI_CONFIGS",
    "api_key": "HNREADER_AI_API_KEY",
    "model": "HNREADER_AI_MODEL",
    "base_url": "HNREADER_AI_BASE_URL",
    "internal_host_allowlist": "HNREADER_AI_INTERNAL_HOST_ALLOWLIST",
    "request_timeout_seconds": "HNREADER_AI_REQUEST_TIMEOUT_SECONDS",
    "config_status_cache_ttl_seconds": "HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS",
    "config_status_cache_path": "HNREADER_AI_CONFIG_STATUS_CACHE_PATH",
    "enrich_body_max_chars": "HNREADER_AI_ENRICH_BODY_MAX_CHARS",
    "enrich_comment_limit": "HNREADER_AI_ENRICH_COMMENT_LIMIT",
    "enrich_comment_max_chars": "HNREADER_AI_ENRICH_COMMENT_MAX_CHARS",
}

_INSIGHTS_AI_ENV_NAMES = {
    "HNREADER_INSIGHTS_AI_CONFIG_FILE",
    "HNREADER_INSIGHTS_AI_PROVIDER",
    "HNREADER_INSIGHTS_AI_CONFIGS",
    "HNREADER_INSIGHTS_AI_API_KEY",
    "HNREADER_INSIGHTS_AI_MODEL",
    "HNREADER_INSIGHTS_AI_BASE_URL",
    "HNREADER_INSIGHTS_AI_INTERNAL_HOST_ALLOWLIST",
    "HNREADER_INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS",
    "HNREADER_INSIGHTS_AI_MAX_OUTPUT_TOKENS",
}

_INSIGHTS_COMPRESSION_AI_ENV_NAMES = {
    "HNREADER_INSIGHTS_COMPRESSION_AI_CONFIG_FILE",
    "HNREADER_INSIGHTS_COMPRESSION_AI_PROVIDER",
    "HNREADER_INSIGHTS_COMPRESSION_AI_CONFIGS",
    "HNREADER_INSIGHTS_COMPRESSION_AI_API_KEY",
    "HNREADER_INSIGHTS_COMPRESSION_AI_MODEL",
    "HNREADER_INSIGHTS_COMPRESSION_AI_BASE_URL",
    "HNREADER_INSIGHTS_COMPRESSION_AI_INTERNAL_HOST_ALLOWLIST",
    "HNREADER_INSIGHTS_COMPRESSION_AI_REQUEST_TIMEOUT_SECONDS",
    "HNREADER_INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS",
}

_INSIGHTS_AI_JSON_FIELD_ENV_NAMES = {
    "provider": "HNREADER_INSIGHTS_AI_PROVIDER",
    "configs": "HNREADER_INSIGHTS_AI_CONFIGS",
    "api_key": "HNREADER_INSIGHTS_AI_API_KEY",
    "model": "HNREADER_INSIGHTS_AI_MODEL",
    "base_url": "HNREADER_INSIGHTS_AI_BASE_URL",
    "internal_host_allowlist": "HNREADER_INSIGHTS_AI_INTERNAL_HOST_ALLOWLIST",
    "request_timeout_seconds": "HNREADER_INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS",
    "max_output_tokens": "HNREADER_INSIGHTS_AI_MAX_OUTPUT_TOKENS",
}

_INSIGHTS_COMPRESSION_AI_JSON_FIELD_ENV_NAMES = {
    "provider": "HNREADER_INSIGHTS_COMPRESSION_AI_PROVIDER",
    "configs": "HNREADER_INSIGHTS_COMPRESSION_AI_CONFIGS",
    "api_key": "HNREADER_INSIGHTS_COMPRESSION_AI_API_KEY",
    "model": "HNREADER_INSIGHTS_COMPRESSION_AI_MODEL",
    "base_url": "HNREADER_INSIGHTS_COMPRESSION_AI_BASE_URL",
    "internal_host_allowlist": (
        "HNREADER_INSIGHTS_COMPRESSION_AI_INTERNAL_HOST_ALLOWLIST"
    ),
    "request_timeout_seconds": (
        "HNREADER_INSIGHTS_COMPRESSION_AI_REQUEST_TIMEOUT_SECONDS"
    ),
    "max_output_tokens": "HNREADER_INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS",
}


def _split_config_files(value: str) -> Tuple[Path, ...]:
    raw = str(value or "").strip()
    if not raw:
        return ()
    return tuple(Path(part) for part in raw.split(os.pathsep) if part.strip())


def _ai_env_file_candidates() -> Tuple[Path, ...]:
    explicit = _env_if_present("HNREADER_AI_CONFIG_FILE", "HACKERMINI_AI_CONFIG_FILE")
    if explicit is not None:
        return _split_config_files(explicit)

    candidates = []
    env_file = os.environ.get("ENV_FILE")
    if env_file:
        candidates.append(Path(env_file))
    candidates.append(Path("/etc/hnreader/server.env"))
    deduped = []
    seen = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return tuple(deduped)


def _unquote_env_value(value: str) -> str:
    raw = value.strip()
    if len(raw) >= 2 and raw[0] == raw[-1] == "'":
        return raw[1:-1]
    if len(raw) >= 2 and raw[0] == raw[-1] == '"':
        inner = raw[1:-1]
        out = []
        escaped = False
        for ch in inner:
            if escaped:
                out.append(ch)
                escaped = False
            elif ch == "\\":
                escaped = True
            else:
                out.append(ch)
        if escaped:
            out.append("\\")
        return "".join(out)
    return value.rstrip()


def _read_ai_env_file(path: Path) -> Dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {}
    except OSError:
        return {}

    values: Dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in _AI_ENV_NAMES:
            continue
        values[key] = _unquote_env_value(value)
    return values


def _json_value_to_env(value: Any, *, key: str) -> str:
    if value is None:
        return ""
    if (
        key == "internal_host_allowlist"
        or str(key).endswith("_INTERNAL_HOST_ALLOWLIST")
    ) and isinstance(value, list):
        return ",".join(str(item) for item in value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    raise ValueError(f"AI JSON config field {key!r} has unsupported type")


def _read_ai_json_file(path: Path) -> Dict[str, str]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
    except FileNotFoundError:
        return {}
    except OSError:
        return {}

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{path} is not valid AI JSON config: {exc.msg}"
        ) from exc

    values: Dict[str, str] = {}
    if isinstance(parsed, list):
        values["HNREADER_AI_PROVIDER"] = "enabled"
        values["HNREADER_AI_CONFIGS"] = json.dumps(
            parsed, ensure_ascii=False, separators=(",", ":")
        )
        return values

    if not isinstance(parsed, dict):
        raise ValueError(f"{path} AI JSON config must be an object or array")

    for key, value in parsed.items():
        if key in _AI_ENV_NAMES:
            values[key] = _json_value_to_env(value, key=key)

    for field, env_name in _AI_JSON_FIELD_ENV_NAMES.items():
        if field not in parsed:
            continue
        values[env_name] = _json_value_to_env(parsed[field], key=field)

    if "HNREADER_AI_CONFIGS" in values and "HNREADER_AI_PROVIDER" not in values:
        values["HNREADER_AI_PROVIDER"] = "enabled"

    if not values:
        raise ValueError(
            f"{path} AI JSON config must contain a provider/configs object "
            "or HNREADER_AI_* keys"
        )
    return values


def _read_ai_config_file(path: Path) -> Tuple[Dict[str, str], str]:
    if path.suffix.lower() == ".json":
        return _read_ai_json_file(path), "json"
    return _read_ai_env_file(path), "env"


def _insights_ai_env_file_candidates() -> Tuple[Tuple[Path, str], ...]:
    raw_values = [
        (_env_first("HNREADER_INSIGHTS_AI_CONFIG_FILE"), "insights"),
        (_env_first("HNREADER_INSIGHTS_COMPRESSION_AI_CONFIG_FILE"), "compression"),
    ]
    deduped: list[Tuple[Path, str]] = []
    by_path: Dict[str, int] = {}
    for raw, scope in raw_values:
        if not raw:
            continue
        for path in _split_config_files(raw):
            key = str(path)
            if key in by_path:
                existing_index = by_path[key]
                existing_path, existing_scope = deduped[existing_index]
                if existing_scope != scope:
                    deduped[existing_index] = (existing_path, "combined")
                continue
            by_path[key] = len(deduped)
            deduped.append((path, scope))
    return tuple(deduped)


def _read_insights_ai_env_file(path: Path) -> Dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {}
    except OSError:
        return {}

    values: Dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if (
            key not in _INSIGHTS_AI_ENV_NAMES
            and key not in _INSIGHTS_COMPRESSION_AI_ENV_NAMES
        ):
            continue
        values[key] = _unquote_env_value(value)
    return values


def _apply_insights_ai_json_section(
    values: Dict[str, str],
    parsed: Any,
    *,
    label: str,
    env_names: set,
    field_env_names: Dict[str, str],
    provider_env_name: str,
    configs_env_name: str,
) -> None:
    if isinstance(parsed, list):
        values[provider_env_name] = "enabled"
        values[configs_env_name] = json.dumps(
            parsed, ensure_ascii=False, separators=(",", ":")
        )
        return

    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must be an object or array")

    before = set(values.keys())
    for key, value in parsed.items():
        if key in env_names:
            values[key] = _json_value_to_env(value, key=key)

    for field, env_name in field_env_names.items():
        if field not in parsed:
            continue
        values[env_name] = _json_value_to_env(parsed[field], key=field)

    changed = set(values.keys()) - before
    if configs_env_name in changed and provider_env_name not in values:
        values[provider_env_name] = "enabled"


def _read_insights_ai_json_file(path: Path, *, scope: str = "combined") -> Dict[str, str]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
    except FileNotFoundError:
        return {}
    except OSError:
        return {}

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{path} is not valid insights AI JSON config: {exc.msg}"
        ) from exc

    values: Dict[str, str] = {}
    if isinstance(parsed, list):
        if scope == "compression":
            provider_name = "HNREADER_INSIGHTS_COMPRESSION_AI_PROVIDER"
            configs_name = "HNREADER_INSIGHTS_COMPRESSION_AI_CONFIGS"
        else:
            provider_name = "HNREADER_INSIGHTS_AI_PROVIDER"
            configs_name = "HNREADER_INSIGHTS_AI_CONFIGS"
        values[provider_name] = "enabled"
        values[configs_name] = json.dumps(
            parsed, ensure_ascii=False, separators=(",", ":")
        )
        return values

    if not isinstance(parsed, dict):
        raise ValueError(f"{path} insights AI JSON config must be an object or array")

    if scope == "compression":
        top_field_env_names = _INSIGHTS_COMPRESSION_AI_JSON_FIELD_ENV_NAMES
        top_provider_env_name = "HNREADER_INSIGHTS_COMPRESSION_AI_PROVIDER"
        top_configs_env_name = "HNREADER_INSIGHTS_COMPRESSION_AI_CONFIGS"
    else:
        top_field_env_names = _INSIGHTS_AI_JSON_FIELD_ENV_NAMES
        top_provider_env_name = "HNREADER_INSIGHTS_AI_PROVIDER"
        top_configs_env_name = "HNREADER_INSIGHTS_AI_CONFIGS"

    _apply_insights_ai_json_section(
        values,
        parsed,
        label=f"{path} insights AI JSON config",
        env_names=_INSIGHTS_AI_ENV_NAMES | _INSIGHTS_COMPRESSION_AI_ENV_NAMES,
        field_env_names=top_field_env_names,
        provider_env_name=top_provider_env_name,
        configs_env_name=top_configs_env_name,
    )

    for nested_key, env_names, field_env_names, provider_name, configs_name in (
        (
            "insights",
            _INSIGHTS_AI_ENV_NAMES,
            _INSIGHTS_AI_JSON_FIELD_ENV_NAMES,
            "HNREADER_INSIGHTS_AI_PROVIDER",
            "HNREADER_INSIGHTS_AI_CONFIGS",
        ),
        (
            "compression",
            _INSIGHTS_COMPRESSION_AI_ENV_NAMES,
            _INSIGHTS_COMPRESSION_AI_JSON_FIELD_ENV_NAMES,
            "HNREADER_INSIGHTS_COMPRESSION_AI_PROVIDER",
            "HNREADER_INSIGHTS_COMPRESSION_AI_CONFIGS",
        ),
    ):
        if nested_key not in parsed:
            continue
        _apply_insights_ai_json_section(
            values,
            parsed[nested_key],
            label=f"{path} insights AI JSON {nested_key!r} section",
            env_names=env_names,
            field_env_names=field_env_names,
            provider_env_name=provider_name,
            configs_env_name=configs_name,
        )

    if not values:
        raise ValueError(
            f"{path} insights AI JSON config must contain provider/configs, "
            "an insights/compression section, or HNREADER_INSIGHTS_*AI_* keys"
        )
    return values


def _read_insights_ai_config_file(
    path: Path,
    *,
    scope: str = "combined",
) -> Tuple[Dict[str, str], str]:
    if path.suffix.lower() == ".json":
        return _read_insights_ai_json_file(path, scope=scope), "json"
    return _read_insights_ai_env_file(path), "env"


def _reload_codex_settings_from_process_env() -> None:
    global CODEX_ENABLED, CODEX_CLI_PATH, CODEX_HOME, CODEX_EXTRA_PATH, CODEX_MODEL
    global CODEX_REQUEST_TIMEOUT_SECONDS, CODEX_IGNORE_USER_CONFIG

    CODEX_ENABLED = _env_bool(
        "HNREADER_CODEX_ENABLED", True, fallback="HACKERMINI_CODEX_ENABLED"
    )
    CODEX_CLI_PATH = _env_str(
        "HNREADER_CODEX_CLI_PATH", "codex", fallback="HACKERMINI_CODEX_CLI_PATH"
    )
    CODEX_HOME = _env_str(
        "HNREADER_CODEX_HOME", "", fallback="HACKERMINI_CODEX_HOME"
    ).strip()
    CODEX_EXTRA_PATH = _env_str(
        "HNREADER_CODEX_EXTRA_PATH", "", fallback="HACKERMINI_CODEX_EXTRA_PATH"
    ).strip()
    CODEX_MODEL = _env_str(
        "HNREADER_CODEX_MODEL", "", fallback="HACKERMINI_CODEX_MODEL"
    ).strip()
    CODEX_REQUEST_TIMEOUT_SECONDS = _require_float_range(
        "HNREADER_CODEX_REQUEST_TIMEOUT_SECONDS",
        _env_float(
            "HNREADER_CODEX_REQUEST_TIMEOUT_SECONDS",
            900.0,
            fallback="HACKERMINI_CODEX_REQUEST_TIMEOUT_SECONDS",
        ),
        min_value=1.0,
        max_value=1800.0,
    )
    CODEX_IGNORE_USER_CONFIG = _env_bool(
        "HNREADER_CODEX_IGNORE_USER_CONFIG",
        False,
        fallback="HACKERMINI_CODEX_IGNORE_USER_CONFIG",
    )


def _reload_ai_settings_from_process_env() -> None:
    global AI_BASE_URL, AI_CONFIG_FILE, AI_CONFIGS_JSON, AI_PROVIDER
    global AI_API_KEY, AI_MODEL, AI_INTERNAL_HOST_ALLOWLIST
    global AI_REQUEST_TIMEOUT_SECONDS, AI_CONFIG_STATUS_CACHE_TTL_SECONDS
    global AI_ENRICH_BODY_MAX_CHARS, AI_ENRICH_COMMENT_LIMIT
    global AI_ENRICH_COMMENT_MAX_CHARS

    AI_CONFIG_FILE = _env_str(
        "HNREADER_AI_CONFIG_FILE", "", fallback="HACKERMINI_AI_CONFIG_FILE"
    )
    AI_PROVIDER = _env_str(
        "HNREADER_AI_PROVIDER", "none", fallback="HACKERMINI_AI_PROVIDER"
    ).strip().lower()
    AI_CONFIGS_JSON = _env_str(
        "HNREADER_AI_CONFIGS", "", fallback="HACKERMINI_AI_CONFIGS"
    )
    AI_API_KEY = _env_str(
        "HNREADER_AI_API_KEY", "", fallback="HACKERMINI_AI_API_KEY"
    )
    AI_MODEL = _env_str("HNREADER_AI_MODEL", "", fallback="HACKERMINI_AI_MODEL")
    AI_BASE_URL = _env_str(
        "HNREADER_AI_BASE_URL", "", fallback="HACKERMINI_AI_BASE_URL"
    )
    AI_INTERNAL_HOST_ALLOWLIST = _env_csv(
        "HNREADER_AI_INTERNAL_HOST_ALLOWLIST",
        fallback="HACKERMINI_AI_INTERNAL_HOST_ALLOWLIST",
    )
    AI_REQUEST_TIMEOUT_SECONDS = _require_float_range(
        "HNREADER_AI_REQUEST_TIMEOUT_SECONDS",
        _env_float(
            "HNREADER_AI_REQUEST_TIMEOUT_SECONDS",
            60.0,
            fallback="HACKERMINI_AI_REQUEST_TIMEOUT_SECONDS",
        ),
        min_value=0.1,
        max_value=600.0,
    )
    AI_CONFIG_STATUS_CACHE_TTL_SECONDS = _require_int_range(
        "HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS",
        _env_int(
            "HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS",
            60,
            fallback="HACKERMINI_AI_CONFIG_STATUS_CACHE_TTL_SECONDS",
        ),
        min_value=1,
        max_value=3600,
    )
    AI_ENRICH_BODY_MAX_CHARS = _require_int_range(
        "HNREADER_AI_ENRICH_BODY_MAX_CHARS",
        _env_int(
            "HNREADER_AI_ENRICH_BODY_MAX_CHARS",
            1200,
            fallback="HACKERMINI_AI_ENRICH_BODY_MAX_CHARS",
        ),
        min_value=0,
        max_value=8000,
    )
    AI_ENRICH_COMMENT_LIMIT = _require_int_range(
        "HNREADER_AI_ENRICH_COMMENT_LIMIT",
        _env_int(
            "HNREADER_AI_ENRICH_COMMENT_LIMIT",
            18,
            fallback="HACKERMINI_AI_ENRICH_COMMENT_LIMIT",
        ),
        min_value=0,
        max_value=200,
    )
    AI_ENRICH_COMMENT_MAX_CHARS = _require_int_range(
        "HNREADER_AI_ENRICH_COMMENT_MAX_CHARS",
        _env_int(
            "HNREADER_AI_ENRICH_COMMENT_MAX_CHARS",
            240,
            fallback="HACKERMINI_AI_ENRICH_COMMENT_MAX_CHARS",
        ),
        min_value=50,
        max_value=2000,
    )
    _reload_codex_settings_from_process_env()


def _reload_insights_ai_settings_from_process_env() -> None:
    global INSIGHTS_AI_BASE_URL, INSIGHTS_AI_CONFIG_FILE, INSIGHTS_AI_CONFIGS_JSON
    global INSIGHTS_AI_PROVIDER, INSIGHTS_AI_API_KEY, INSIGHTS_AI_MODEL
    global INSIGHTS_AI_INTERNAL_HOST_ALLOWLIST, INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS
    global INSIGHTS_AI_MAX_OUTPUT_TOKENS
    global INSIGHTS_COMPRESSION_AI_BASE_URL, INSIGHTS_COMPRESSION_AI_CONFIG_FILE
    global INSIGHTS_COMPRESSION_AI_CONFIGS_JSON, INSIGHTS_COMPRESSION_AI_PROVIDER
    global INSIGHTS_COMPRESSION_AI_API_KEY, INSIGHTS_COMPRESSION_AI_MODEL
    global INSIGHTS_COMPRESSION_AI_INTERNAL_HOST_ALLOWLIST
    global INSIGHTS_COMPRESSION_AI_REQUEST_TIMEOUT_SECONDS
    global INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS

    INSIGHTS_AI_CONFIG_FILE = _env_str("HNREADER_INSIGHTS_AI_CONFIG_FILE", "")
    INSIGHTS_AI_PROVIDER = _env_str(
        "HNREADER_INSIGHTS_AI_PROVIDER", "enabled"
    ).strip().lower()
    INSIGHTS_AI_CONFIGS_JSON = _env_str("HNREADER_INSIGHTS_AI_CONFIGS", "")
    INSIGHTS_AI_API_KEY = _env_str("HNREADER_INSIGHTS_AI_API_KEY", "")
    INSIGHTS_AI_MODEL = _env_str("HNREADER_INSIGHTS_AI_MODEL", "")
    INSIGHTS_AI_BASE_URL = _env_str("HNREADER_INSIGHTS_AI_BASE_URL", "")
    INSIGHTS_AI_INTERNAL_HOST_ALLOWLIST = _env_csv(
        "HNREADER_INSIGHTS_AI_INTERNAL_HOST_ALLOWLIST"
    )
    INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS = _require_float_range(
        "HNREADER_INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS",
        _env_float("HNREADER_INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS", 120.0),
        min_value=0.1,
        max_value=600.0,
    )
    INSIGHTS_AI_MAX_OUTPUT_TOKENS = _require_int_range(
        "HNREADER_INSIGHTS_AI_MAX_OUTPUT_TOKENS",
        _env_optional_int("HNREADER_INSIGHTS_AI_MAX_OUTPUT_TOKENS") or 0,
        min_value=0,
        max_value=1_000_000,
    ) or None
    INSIGHTS_COMPRESSION_AI_CONFIG_FILE = _env_str(
        "HNREADER_INSIGHTS_COMPRESSION_AI_CONFIG_FILE",
        INSIGHTS_AI_CONFIG_FILE,
    )
    INSIGHTS_COMPRESSION_AI_PROVIDER = (
        _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_PROVIDER", "")
        or INSIGHTS_AI_PROVIDER
    ).strip().lower()
    INSIGHTS_COMPRESSION_AI_CONFIGS_JSON = (
        _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_CONFIGS", "")
        or INSIGHTS_AI_CONFIGS_JSON
    )
    INSIGHTS_COMPRESSION_AI_API_KEY = (
        _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_API_KEY", "")
        or INSIGHTS_AI_API_KEY
    )
    INSIGHTS_COMPRESSION_AI_MODEL = (
        _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_MODEL", "")
        or INSIGHTS_AI_MODEL
    )
    INSIGHTS_COMPRESSION_AI_BASE_URL = (
        _env_str("HNREADER_INSIGHTS_COMPRESSION_AI_BASE_URL", "")
        or INSIGHTS_AI_BASE_URL
    )
    INSIGHTS_COMPRESSION_AI_INTERNAL_HOST_ALLOWLIST = (
        _env_csv("HNREADER_INSIGHTS_COMPRESSION_AI_INTERNAL_HOST_ALLOWLIST")
        or INSIGHTS_AI_INTERNAL_HOST_ALLOWLIST
    )
    INSIGHTS_COMPRESSION_AI_REQUEST_TIMEOUT_SECONDS = _require_float_range(
        "HNREADER_INSIGHTS_COMPRESSION_AI_REQUEST_TIMEOUT_SECONDS",
        _env_float(
            "HNREADER_INSIGHTS_COMPRESSION_AI_REQUEST_TIMEOUT_SECONDS",
            INSIGHTS_AI_REQUEST_TIMEOUT_SECONDS,
        ),
        min_value=0.1,
        max_value=600.0,
    )
    compression_max_tokens = _env_optional_int(
        "HNREADER_INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS"
    )
    INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS = _require_int_range(
        "HNREADER_INSIGHTS_COMPRESSION_AI_MAX_OUTPUT_TOKENS",
        (
            compression_max_tokens
            if compression_max_tokens is not None
            else (INSIGHTS_AI_MAX_OUTPUT_TOKENS or 0)
        ),
        min_value=0,
        max_value=1_000_000,
    ) or None
    _reload_codex_settings_from_process_env()


_last_applied_ai_env_keys: set = set()
_last_ai_config_sources: Tuple[Tuple[str, str], ...] = ()
_last_applied_insights_ai_env_keys: set = set()
_last_insights_ai_config_sources: Tuple[Tuple[str, str], ...] = ()


def get_ai_config_sources() -> Tuple[Dict[str, str], ...]:
    return tuple(
        {"path": path, "format": fmt}
        for path, fmt in _last_ai_config_sources
    )


def get_insights_ai_config_sources() -> Tuple[Dict[str, str], ...]:
    return tuple(
        {"path": path, "format": fmt}
        for path, fmt in _last_insights_ai_config_sources
    )


def refresh_ai_settings_from_env_files() -> bool:
    """Reload only AI-related settings from operator-editable env files.

    A running process cannot observe external environment changes by itself.
    This function gives long-lived ingest loops a narrow hot-reload path for
    ``HNREADER_AI_*`` values: edit the AI env file and the next AI agent build
    will use the new provider/model settings without redeploying code.

    Deletions are honored too: any AI key that was applied from the file on
    a previous call but is absent in the current read is popped from
    ``os.environ`` so the long-lived process doesn't keep using the stale
    value. The env file is treated as the authoritative source for AI keys
    listed in ``_AI_ENV_NAMES``.
    """
    global _last_ai_config_sources, _last_applied_ai_env_keys

    new_values: Dict[str, str] = {}
    sources = []
    saw_existing_file = False
    for path in _ai_env_file_candidates():
        try:
            file_exists = path.is_file()
        except OSError:
            file_exists = False
        if not file_exists:
            continue
        saw_existing_file = True
        values, source_format = _read_ai_config_file(path)
        new_values.update(values)
        sources.append((str(path), source_format))

    if not saw_existing_file:
        _last_ai_config_sources = ()
        return False

    new_keys = set(new_values.keys())
    for key in _last_applied_ai_env_keys - new_keys:
        os.environ.pop(key, None)

    if new_values:
        os.environ.update(new_values)

    _last_applied_ai_env_keys = new_keys
    _last_ai_config_sources = tuple(sources)
    _reload_ai_settings_from_process_env()
    return True


def refresh_insights_ai_settings_from_env_files() -> bool:
    """Reload only ``HNREADER_INSIGHTS_AI_*`` settings from configured files."""
    global _last_insights_ai_config_sources, _last_applied_insights_ai_env_keys

    new_values: Dict[str, str] = {}
    sources = []
    saw_existing_file = False
    for path, scope in _insights_ai_env_file_candidates():
        try:
            file_exists = path.is_file()
        except OSError:
            file_exists = False
        if not file_exists:
            continue
        saw_existing_file = True
        values, source_format = _read_insights_ai_config_file(path, scope=scope)
        new_values.update(values)
        sources.append((str(path), source_format))

    if not saw_existing_file:
        _last_insights_ai_config_sources = ()
        return False

    new_keys = set(new_values.keys())
    for key in _last_applied_insights_ai_env_keys - new_keys:
        os.environ.pop(key, None)

    if new_values:
        os.environ.update(new_values)

    _last_applied_insights_ai_env_keys = new_keys
    _last_insights_ai_config_sources = tuple(sources)
    _reload_insights_ai_settings_from_process_env()
    return True


# ---------- Hacker News HTTP ----------

HN_API_BASE = _env_str(
    "HNREADER_HN_API_BASE",
    "https://hacker-news.firebaseio.com/v0",
    fallback="HACKERMINI_HN_API_BASE",
)
HN_REQUEST_TIMEOUT_SECONDS = _require_float_range(
    "HNREADER_HN_REQUEST_TIMEOUT_SECONDS",
    _env_float(
        "HNREADER_HN_REQUEST_TIMEOUT_SECONDS",
        10.0,
        fallback="HACKERMINI_HN_REQUEST_TIMEOUT_SECONDS",
    ),
    min_value=0.1,
    max_value=120.0,
)
HN_RETRY_ATTEMPTS = _require_int_range(
    "HNREADER_HN_RETRY_ATTEMPTS",
    _env_int(
        "HNREADER_HN_RETRY_ATTEMPTS", 3, fallback="HACKERMINI_HN_RETRY_ATTEMPTS"
    ),
    min_value=1,
    max_value=20,
)

# ---------- App version ----------

APP_VERSION = _env_str(
    "HNREADER_APP_VERSION", "1.0.0", fallback="HACKERMINI_APP_VERSION"
)

# ---------- DB write lock ----------

DB_WRITE_LOCK_RETRY_ATTEMPTS = _require_int_range(
    "HNREADER_DB_WRITE_LOCK_RETRY_ATTEMPTS",
    _env_int(
        "HNREADER_DB_WRITE_LOCK_RETRY_ATTEMPTS",
        2,
        fallback="HACKERMINI_DB_WRITE_LOCK_RETRY_ATTEMPTS",
    ),
    min_value=0,
    max_value=10,
)
DB_WRITE_LOCK_RETRY_BASE_SECONDS = _require_float_range(
    "HNREADER_DB_WRITE_LOCK_RETRY_BASE_SECONDS",
    _env_float(
        "HNREADER_DB_WRITE_LOCK_RETRY_BASE_SECONDS",
        0.1,
        fallback="HACKERMINI_DB_WRITE_LOCK_RETRY_BASE_SECONDS",
    ),
    min_value=0.0,
    max_value=10.0,
)
DB_WRITE_LOCK_RETRY_MAX_SECONDS = _require_float_range(
    "HNREADER_DB_WRITE_LOCK_RETRY_MAX_SECONDS",
    _env_float(
        "HNREADER_DB_WRITE_LOCK_RETRY_MAX_SECONDS",
        1.0,
        fallback="HACKERMINI_DB_WRITE_LOCK_RETRY_MAX_SECONDS",
    ),
    min_value=0.0,
    max_value=60.0,
)


# ---------- Standalone ingest supervisor ----------

INGEST_INTERVAL_SECONDS = _require_int_range(
    "HNREADER_INGEST_INTERVAL_SECONDS",
    _env_int(
        "HNREADER_INGEST_INTERVAL_SECONDS",
        60 * 60,
        fallback="HACKERMINI_INGEST_INTERVAL_SECONDS",
    ),
    min_value=1,
    max_value=24 * 60 * 60,
)
INGEST_ROUND_TIMEOUT_SECONDS = _require_int_range(
    "HNREADER_INGEST_ROUND_TIMEOUT_SECONDS",
    _env_int(
        "HNREADER_INGEST_ROUND_TIMEOUT_SECONDS",
        28 * 60,
        fallback="HACKERMINI_INGEST_ROUND_TIMEOUT_SECONDS",
    ),
    min_value=30,
    max_value=24 * 60 * 60,
)
INGEST_DIGEST_RESERVED_SECONDS = _require_int_range(
    "HNREADER_INGEST_DIGEST_RESERVED_SECONDS",
    _env_int(
        "HNREADER_INGEST_DIGEST_RESERVED_SECONDS",
        90,
        fallback="HACKERMINI_INGEST_DIGEST_RESERVED_SECONDS",
    ),
    min_value=0,
    max_value=60 * 60,
)
INGEST_CHILD_KILL_GRACE_SECONDS = _require_int_range(
    "HNREADER_INGEST_CHILD_KILL_GRACE_SECONDS",
    _env_int(
        "HNREADER_INGEST_CHILD_KILL_GRACE_SECONDS",
        10,
        fallback="HACKERMINI_INGEST_CHILD_KILL_GRACE_SECONDS",
    ),
    min_value=1,
    max_value=300,
)
INGEST_FAILURE_BACKOFF_SECONDS = _require_int_range(
    "HNREADER_INGEST_FAILURE_BACKOFF_SECONDS",
    _env_int(
        "HNREADER_INGEST_FAILURE_BACKOFF_SECONDS",
        60,
        fallback="HACKERMINI_INGEST_FAILURE_BACKOFF_SECONDS",
    ),
    min_value=1,
    max_value=300,
)
_require_less(
    "HNREADER_INGEST_DIGEST_RESERVED_SECONDS",
    INGEST_DIGEST_RESERVED_SECONDS,
    "HNREADER_INGEST_ROUND_TIMEOUT_SECONDS",
    INGEST_ROUND_TIMEOUT_SECONDS,
)

# ---------- Cloud sync (partial migration: push the read model to the cloud database) ----------
#
# CLOUD_SYNC_ENABLED defaults to False: even if the cloud_sync_runner module is
# imported, as long as the switch is not turned on it will not trigger
# build + push, and the main ingest flow is unaffected.
#
# When URL / SECRET are empty, even with ENABLED=true it only logs a warning
# and skips, without raising an error.

CLOUD_SYNC_ENABLED = _env_bool(
    "HNREADER_CLOUD_SYNC_ENABLED",
    False,
    fallback="HACKERMINI_CLOUD_SYNC_ENABLED",
)
CLOUD_PUSH_URL = _env_str(
    "HNREADER_CLOUD_PUSH_URL", "", fallback="HACKERMINI_CLOUD_PUSH_URL"
)
CLOUD_PUSH_SECRET = _env_str(
    "HNREADER_CLOUD_PUSH_SECRET", "", fallback="HACKERMINI_CLOUD_PUSH_SECRET"
)
CLOUD_PUSH_BATCH_SIZE = _require_int_range(
    "HNREADER_CLOUD_PUSH_BATCH_SIZE",
    _env_int(
        "HNREADER_CLOUD_PUSH_BATCH_SIZE",
        50,
        fallback="HACKERMINI_CLOUD_PUSH_BATCH_SIZE",
    ),
    min_value=1,
    max_value=1000,
)
CLOUD_PUSH_MAX_BODY_BYTES = _require_int_range(
    "HNREADER_CLOUD_PUSH_MAX_BODY_BYTES",
    _env_int(
        "HNREADER_CLOUD_PUSH_MAX_BODY_BYTES",
        80000,
        fallback="HACKERMINI_CLOUD_PUSH_MAX_BODY_BYTES",
    ),
    min_value=1024,
    max_value=6 * 1024 * 1024,
)
CLOUD_SYNC_TIMEOUT_SECONDS = _require_int_range(
    "HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS",
    _env_int(
        "HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS",
        120,
        fallback="HACKERMINI_CLOUD_SYNC_TIMEOUT_SECONDS",
    ),
    min_value=10,
    max_value=600,
)
DASHBOARD_INGEST_RUN_LIMIT = _require_int_range(
    "HNREADER_DASHBOARD_INGEST_RUN_LIMIT",
    _env_int(
        "HNREADER_DASHBOARD_INGEST_RUN_LIMIT",
        20,
        fallback="HACKERMINI_DASHBOARD_INGEST_RUN_LIMIT",
    ),
    min_value=1,
    max_value=100,
)
DASHBOARD_CLOUD_SYNC_RUN_LIMIT = _require_int_range(
    "HNREADER_DASHBOARD_CLOUD_SYNC_RUN_LIMIT",
    _env_int(
        "HNREADER_DASHBOARD_CLOUD_SYNC_RUN_LIMIT",
        20,
        fallback="HACKERMINI_DASHBOARD_CLOUD_SYNC_RUN_LIMIT",
    ),
    min_value=1,
    max_value=100,
)

# ---------- Admin alert email ----------

ADMIN_EMAIL_ENABLED = _env_bool(
    "HNREADER_ADMIN_EMAIL_ENABLED",
    False,
    fallback="HACKERMINI_ADMIN_EMAIL_ENABLED",
)
ADMIN_EMAIL_TO = _env_str(
    "HNREADER_ADMIN_EMAIL_TO", "", fallback="HACKERMINI_ADMIN_EMAIL_TO"
)
SMTP_HOST = _env_str("HNREADER_SMTP_HOST", "", fallback="HACKERMINI_SMTP_HOST")
SMTP_PORT = _require_int_range(
    "HNREADER_SMTP_PORT",
    _env_int("HNREADER_SMTP_PORT", 587, fallback="HACKERMINI_SMTP_PORT"),
    min_value=1,
    max_value=65535,
)
SMTP_USERNAME = _env_str(
    "HNREADER_SMTP_USERNAME", "", fallback="HACKERMINI_SMTP_USERNAME"
)
SMTP_PASSWORD = _env_str(
    "HNREADER_SMTP_PASSWORD", "", fallback="HACKERMINI_SMTP_PASSWORD"
)
SMTP_FROM = _env_str("HNREADER_SMTP_FROM", "", fallback="HACKERMINI_SMTP_FROM")
SMTP_STARTTLS = _env_bool(
    "HNREADER_SMTP_STARTTLS", True, fallback="HACKERMINI_SMTP_STARTTLS"
)
SMTP_SSL = _env_bool("HNREADER_SMTP_SSL", False, fallback="HACKERMINI_SMTP_SSL")
ALERT_COOLDOWN_SECONDS = _require_int_range(
    "HNREADER_ALERT_COOLDOWN_SECONDS",
    _env_int(
        "HNREADER_ALERT_COOLDOWN_SECONDS",
        30 * 60,
        fallback="HACKERMINI_ALERT_COOLDOWN_SECONDS",
    ),
    min_value=0,
    max_value=24 * 60 * 60,
)
ALERT_OUTBOX_MAX_RECORDS = _require_int_range(
    "HNREADER_ALERT_OUTBOX_MAX_RECORDS",
    _env_int(
        "HNREADER_ALERT_OUTBOX_MAX_RECORDS",
        1000,
        fallback="HACKERMINI_ALERT_OUTBOX_MAX_RECORDS",
    ),
    min_value=1,
    max_value=100000,
)
