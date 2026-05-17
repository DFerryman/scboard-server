"""AI provider configuration and connectivity probes.

Sync-only mode: the only consumer is :mod:`cloud_sync` building the
dashboard projection. It calls :func:`refresh_ai_config_status` once per
cloud sync round so the ``hn_dashboard_summary`` document always carries
a fresh probe result. :func:`get_ai_config_status` remains as a cached
fallback path; there is no longer a background FastAPI lifespan that
auto-refreshes — sync cadence is the cadence.
"""

from __future__ import annotations

import copy
import hashlib
import http.client
import json
import logging
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Tuple

from . import http_client, settings
from .ai_agent import AiProviderConfig, build_ai_provider_configs, sanitize_error_text
from .http_client import (
    ERROR_BODY_MAX_BYTES,
    PROBE_RESPONSE_MAX_BYTES,
    ResponseTooLargeError,
    read_limited,
)


_PROBE_TIMEOUT_CAP_SECONDS = 10.0
_DISABLED_PROVIDERS = {"", "none", "fallback", "off", "disabled"}
_CACHE_LOCK = Lock()
_CACHE_KEY: Optional[Tuple[Any, ...]] = None
_CACHE_EXPIRES_AT = 0.0
_CACHE_VALUE: Optional[Dict[str, Any]] = None
_REFRESHING_KEY: Optional[Tuple[Any, ...]] = None
_REFRESHING_GENERATION: Optional[int] = None
_CACHE_GENERATION = 0
log = logging.getLogger("server.ai_config_status")


def _provider_enabled_value(provider: str) -> bool:
    return (provider or "none").strip().lower() not in _DISABLED_PROVIDERS


def _refresh_settings_error() -> Optional[str]:
    try:
        settings.refresh_ai_settings_from_env_files()
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"
    return None


def _provider_enabled() -> bool:
    refresh_error = _refresh_settings_error()
    if refresh_error:
        return True
    return _provider_enabled_value(settings.AI_PROVIDER)


def _host_label(url: str) -> str:
    host = urllib.parse.urlsplit(url).hostname or ""
    if host.startswith("api."):
        host = host[4:]
    return host.split(".")[0] if host else "provider"


def _config_name(config: AiProviderConfig, index: int) -> str:
    return config.name or _host_label(config.base_url) or f"config #{index}"


def _redact(text: str, configs: Sequence[AiProviderConfig]) -> str:
    return sanitize_error_text(text, configs)


def _probe_timeout(config: AiProviderConfig) -> float:
    return max(0.1, min(float(config.timeout), _PROBE_TIMEOUT_CAP_SECONDS))


def _models_probe_url(config: AiProviderConfig) -> str:
    return f"{config.base_url}/models"


def _read_probe_json(
    config: AiProviderConfig,
    url: str,
) -> Tuple[int, Optional[Mapping[str, Any]], Optional[str]]:
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {config.api_key}",
            "User-Agent": "hnreader/1.0",
        },
    )
    try:
        with http_client.urlopen_no_redirect(
            req, timeout=_probe_timeout(config)
        ) as resp:
            status_code = int(resp.getcode())
            raw = read_limited(resp, PROBE_RESPONSE_MAX_BYTES).decode(
                "utf-8", "ignore"
            )
    except ResponseTooLargeError as exc:
        return 0, None, _redact(f"probe response too large: {exc}", [config])
    except urllib.error.HTTPError as exc:
        try:
            body = read_limited(exc, ERROR_BODY_MAX_BYTES).decode("utf-8", "ignore")
        except ResponseTooLargeError:
            body = "<error body too large>"
        except Exception:  # noqa: BLE001
            body = ""
        detail = f"HTTP {exc.code}: {exc.reason}"
        if body:
            detail = f"{detail}: {body[:300]}"
        return int(exc.code), None, _redact(detail, [config])
    except (TimeoutError, socket.timeout) as exc:
        return 0, None, f"timeout: {exc}"
    except urllib.error.URLError as exc:
        return 0, None, f"network error: {exc.reason}"
    except http.client.HTTPException as exc:
        return 0, None, f"network error: {exc}"
    except OSError as exc:
        return 0, None, f"network error: {exc}"

    if not raw.strip():
        return status_code, {}, None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return status_code, None, "provider returned invalid JSON"
    if not isinstance(parsed, Mapping):
        return status_code, None, "provider returned non-object JSON"
    return status_code, parsed, None


def _normalize_balance(data: Mapping[str, Any]) -> Dict[str, Any]:
    raw_infos = data.get("balance_infos")
    infos: List[Dict[str, str]] = []
    if isinstance(raw_infos, list):
        for item in raw_infos:
            if not isinstance(item, Mapping):
                continue
            infos.append(
                {
                    "currency": str(item.get("currency") or ""),
                    "total_balance": str(item.get("total_balance") or ""),
                    "granted_balance": str(item.get("granted_balance") or ""),
                    "topped_up_balance": str(item.get("topped_up_balance") or ""),
                }
            )
    out: Dict[str, Any] = {
        "is_available": bool(data.get("is_available")),
        "balance_infos": infos,
    }
    return out


def _public_config_base(index: int, config: AiProviderConfig, checked_at: int) -> Dict[str, Any]:
    pricing: Dict[str, float] = {}
    if config.input_token_price_per_million is not None:
        pricing["input_token_price_per_million"] = config.input_token_price_per_million
    if config.output_token_price_per_million is not None:
        pricing["output_token_price_per_million"] = config.output_token_price_per_million
    out: Dict[str, Any] = {
        "index": index,
        "name": _config_name(config, index),
        "model": config.model,
        "base_url": config.base_url,
        "chat_url": f"{config.base_url}/chat/completions",
        "balance_url": config.balance_url,
        "probe_url": config.balance_url or _models_probe_url(config),
        "probe_kind": "balance" if config.balance_url else "models",
        "timeout_seconds": config.timeout,
        "probe_timeout_seconds": _probe_timeout(config),
        "max_concurrent_requests": config.max_concurrent_requests,
        "api_key_configured": bool(config.api_key),
        "checked_at": checked_at,
    }
    if pricing:
        out["pricing"] = pricing
    return out


def _probe_config(index: int, config: AiProviderConfig, checked_at: int) -> Dict[str, Any]:
    out = _public_config_base(index, config, checked_at)
    status_code, data, error = _read_probe_json(config, out["probe_url"])
    out["http_status"] = status_code or None
    if error:
        out.update(
            {
                "status": "err",
                "connected": False,
                "message": error,
                "balance": None,
            }
        )
        return out

    balance = _normalize_balance(data or {}) if config.balance_url else None
    if balance and not balance.get("is_available"):
        out.update(
            {
                "status": "warn",
                "connected": True,
                "message": "balance endpoint reachable but account is unavailable",
                "balance": balance,
            }
        )
        return out

    out.update(
        {
            "status": "ok",
            "connected": True,
            "message": (
                "balance endpoint reachable"
                if config.balance_url
                else "models endpoint reachable"
            ),
            "balance": balance,
        }
    )
    return out


def _overall_status(configs: Sequence[Mapping[str, Any]]) -> str:
    if not configs:
        return "missing_config"
    statuses = {str(config.get("status") or "") for config in configs}
    if statuses == {"ok"}:
        return "ok"
    if "err" in statuses and len(statuses) == 1:
        return "err"
    return "warn"


def _settings_cache_key() -> Tuple[Any, ...]:
    refresh_error = _refresh_settings_error()
    return (
        settings.AI_PROVIDER,
        settings.AI_CONFIG_FILE,
        settings.AI_CONFIGS_JSON,
        settings.AI_API_KEY,
        settings.AI_MODEL,
        settings.AI_BASE_URL,
        settings.AI_INTERNAL_HOST_ALLOWLIST,
        settings.AI_REQUEST_TIMEOUT_SECONDS,
        refresh_error,
    )


def _cache_key_digest(key: Tuple[Any, ...]) -> str:
    raw = json.dumps(key, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _write_json_atomic(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(path)


def _persist_cache(key: Tuple[Any, ...], status: Dict[str, Any], expires_at: float) -> None:
    payload = {
        "key": _cache_key_digest(key),
        "expires_at": float(expires_at),
        "stored_at": time.time(),
        "status": status,
    }
    try:
        _write_json_atomic(settings.get_ai_config_status_cache_path(), payload)
    except OSError as exc:
        log.warning("AI config status cache write failed: %s", exc)


def _load_persisted_cache(
    key: Tuple[Any, ...],
) -> Optional[Tuple[Dict[str, Any], float]]:
    path = settings.get_ai_config_status_cache_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        log.warning("AI config status cache read failed: %s", exc)
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, Mapping):
        return None
    if payload.get("key") != _cache_key_digest(key):
        return None
    status = payload.get("status")
    if not isinstance(status, Mapping):
        return None
    try:
        expires_at = float(payload.get("expires_at"))
    except (TypeError, ValueError):
        return None
    return copy.deepcopy(dict(status)), expires_at


def _delete_persisted_cache() -> None:
    try:
        settings.get_ai_config_status_cache_path().unlink()
    except FileNotFoundError:
        return
    except OSError as exc:
        log.warning("AI config status cache delete failed: %s", exc)


def clear_ai_config_status_cache(*, delete_disk: bool = True) -> None:
    global _CACHE_EXPIRES_AT, _CACHE_GENERATION, _CACHE_KEY, _CACHE_VALUE
    global _REFRESHING_GENERATION, _REFRESHING_KEY
    with _CACHE_LOCK:
        _CACHE_GENERATION += 1
        _CACHE_KEY = None
        _CACHE_EXPIRES_AT = 0.0
        _CACHE_VALUE = None
        _REFRESHING_KEY = None
        _REFRESHING_GENERATION = None
        if delete_disk:
            _delete_persisted_cache()


def _probe_ai_config_status(now: int) -> Dict[str, Any]:
    refresh_error = _refresh_settings_error()
    provider = (settings.AI_PROVIDER or "none").strip().lower()
    enabled = _provider_enabled_value(provider) or bool(refresh_error)
    out: Dict[str, Any] = {
        "enabled": enabled,
        "provider": provider,
        "checked_at": now,
        "status": "disabled" if not enabled else "unknown",
        "configs": [],
    }
    if refresh_error:
        out.update({"status": "err", "config_error": refresh_error})
        return out
    if not enabled:
        return out

    try:
        configs = build_ai_provider_configs()
    except ValueError as exc:
        out.update({"status": "err", "config_error": str(exc)})
        return out

    out["configs"] = [
        _probe_config(index, config, now)
        for index, config in enumerate(configs, start=1)
    ]
    out["status"] = _overall_status(out["configs"])
    return out


def _pending_ai_config_status(now: int, *, refreshing: bool) -> Dict[str, Any]:
    refresh_error = _refresh_settings_error()
    provider = (settings.AI_PROVIDER or "none").strip().lower()
    enabled = _provider_enabled_value(provider) or bool(refresh_error)
    out: Dict[str, Any] = {
        "enabled": enabled,
        "provider": provider,
        "checked_at": now,
        "status": "disabled" if not enabled else "unknown",
        "configs": [],
        "cached": False,
        "stale": False,
        "refreshing": bool(refreshing),
    }
    if refresh_error:
        out.update({"status": "err", "config_error": refresh_error, "refreshing": False})
        return out
    if not enabled:
        return out
    try:
        configs = build_ai_provider_configs()
    except ValueError as exc:
        out.update({"status": "err", "config_error": str(exc), "refreshing": False})
        return out
    out["configs"] = [
        {
            **_public_config_base(index, config, now),
            "status": "unknown",
            "connected": False,
            "http_status": None,
            "message": "probe pending",
            "balance": None,
        }
        for index, config in enumerate(configs, start=1)
    ]
    return out


def _with_cache_flags(
    status: Dict[str, Any],
    *,
    cached: bool,
    stale: bool,
    refreshing: bool,
) -> Dict[str, Any]:
    out = copy.deepcopy(status)
    out["cached"] = bool(cached)
    out["stale"] = bool(stale)
    out["refreshing"] = bool(refreshing)
    return out


def _store_cache(key: Tuple[Any, ...], status: Dict[str, Any]) -> None:
    global _CACHE_EXPIRES_AT, _CACHE_KEY, _CACHE_VALUE
    ttl = int(settings.AI_CONFIG_STATUS_CACHE_TTL_SECONDS)
    expires_at = time.time() + float(ttl)
    with _CACHE_LOCK:
        _CACHE_KEY = key
        _CACHE_EXPIRES_AT = expires_at
        _CACHE_VALUE = copy.deepcopy(status)
        _persist_cache(key, status, expires_at)


def _store_refresh_cache(
    key: Tuple[Any, ...],
    status: Dict[str, Any],
    generation: int,
) -> None:
    global _CACHE_EXPIRES_AT, _CACHE_KEY, _CACHE_VALUE
    ttl = int(settings.AI_CONFIG_STATUS_CACHE_TTL_SECONDS)
    expires_at = time.time() + float(ttl)
    with _CACHE_LOCK:
        if _REFRESHING_KEY != key or _REFRESHING_GENERATION != generation:
            return
        _CACHE_KEY = key
        _CACHE_EXPIRES_AT = expires_at
        _CACHE_VALUE = copy.deepcopy(status)
        _persist_cache(key, status, expires_at)


def refresh_ai_config_status(*, checked_at: Optional[int] = None) -> Dict[str, Any]:
    now = int(checked_at if checked_at is not None else time.time())
    key = _settings_cache_key()
    status = _probe_ai_config_status(now)
    _store_cache(key, status)
    return _with_cache_flags(status, cached=False, stale=False, refreshing=False)


def estimated_refresh_timeout_seconds() -> float:
    """Return the worst-case blocking probe budget for current AI configs.

    Dashboard sync can use this before calling ``refresh_ai_config_status`` so
    a large provider list does not consume the remaining ingest deadline and
    get the child killed mid-push. Config errors return 0 because refresh will
    fail fast without network probes.
    """
    if not _provider_enabled():
        return 0.0
    try:
        configs = build_ai_provider_configs()
    except Exception:  # noqa: BLE001
        return 0.0
    return sum(_probe_timeout(config) for config in configs)


def _refresh_worker(key: Tuple[Any, ...], generation: int) -> None:
    global _REFRESHING_GENERATION, _REFRESHING_KEY
    try:
        status = _probe_ai_config_status(int(time.time()))
        _store_refresh_cache(key, status, generation)
    finally:
        with _CACHE_LOCK:
            if _REFRESHING_KEY == key and _REFRESHING_GENERATION == generation:
                _REFRESHING_KEY = None
                _REFRESHING_GENERATION = None


def _schedule_refresh(key: Tuple[Any, ...]) -> bool:
    global _REFRESHING_GENERATION, _REFRESHING_KEY
    with _CACHE_LOCK:
        if _REFRESHING_KEY == key:
            return True
        generation = _CACHE_GENERATION
        _REFRESHING_KEY = key
        _REFRESHING_GENERATION = generation
    Thread(
        target=_refresh_worker,
        args=(key, generation),
        name="hnreader-ai-config-probe",
        daemon=True,
    ).start()
    return True


def cached_ai_config_status() -> Optional[Dict[str, Any]]:
    """Pure in-memory cache read — no scheduling, no probing, no disk I/O.

    Returns the cached status if it matches the current settings cache key
    (regardless of TTL), or ``None`` if no in-memory cache exists. Designed
    for fallback paths after :func:`refresh_ai_config_status` already failed:
    we want "show last known" without burning another network call against
    a provider that just timed out.
    """
    key = _settings_cache_key()
    with _CACHE_LOCK:
        if _CACHE_KEY != key or _CACHE_VALUE is None:
            return None
        cached = copy.deepcopy(_CACHE_VALUE)
    return _with_cache_flags(cached, cached=True, stale=True, refreshing=False)


def get_ai_config_status(*, checked_at: Optional[int] = None) -> Dict[str, Any]:
    """Return dashboard-safe AI config status without blocking on the network.

    On cache miss / TTL expiry this still **schedules a background probe** via
    :func:`_schedule_refresh` so a subsequent call can see fresh data — that
    behavior is what original FastAPI request-path callers needed. In sync-only
    mode there's no longer a request path; production callers should prefer
    :func:`refresh_ai_config_status` for an explicit blocking probe and
    :func:`cached_ai_config_status` for a strictly side-effect-free fallback.
    """

    global _CACHE_EXPIRES_AT, _CACHE_KEY, _CACHE_VALUE
    now = int(checked_at if checked_at is not None else time.time())
    key = _settings_cache_key()
    with _CACHE_LOCK:
        cached = copy.deepcopy(_CACHE_VALUE) if _CACHE_KEY == key else None
        stale = cached is not None and time.time() >= _CACHE_EXPIRES_AT
        refreshing = _REFRESHING_KEY == key
    if cached is not None and not stale:
        return _with_cache_flags(cached, cached=True, stale=False, refreshing=False)

    if cached is None or stale:
        persisted = _load_persisted_cache(key)
        if persisted is not None:
            persisted_cached, expires_at = persisted
            if cached is None or expires_at > _CACHE_EXPIRES_AT:
                cached = persisted_cached
                with _CACHE_LOCK:
                    _CACHE_KEY = key
                    _CACHE_EXPIRES_AT = expires_at
                    _CACHE_VALUE = copy.deepcopy(cached)
                    refreshing = _REFRESHING_KEY == key
            with _CACHE_LOCK:
                expires_at = _CACHE_EXPIRES_AT
                refreshing = _REFRESHING_KEY == key
            stale = time.time() >= expires_at
            if not stale:
                return _with_cache_flags(
                    cached,
                    cached=True,
                    stale=False,
                    refreshing=False,
                )

    scheduled = _schedule_refresh(key)
    if cached is not None:
        return _with_cache_flags(
            cached,
            cached=True,
            stale=True,
            refreshing=scheduled or refreshing,
        )
    return _pending_ai_config_status(now, refreshing=scheduled or refreshing)
