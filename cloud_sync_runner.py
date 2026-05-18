"""End-to-end cloud sync orchestration: cloud_sync.build → cloud_push.push.

The business publish and the dashboard publish are split, with
:mod:`ingest` controlling the orchestration order:

    1. :func:`run_business_once` -- build_read_model(include_dashboard=False)
       + push_read_model (switchMeta version switch). After it returns, ingest
       persists this round's final state to SQLite ``cloud_sync_runs``.
    2. :func:`run_dashboard_once` -- build_dashboard_projection (reads this
       round's final state) + push_dashboard (writeDashboard). On failure,
       ingest downgrades the local ``cloud_sync_runs`` from ok to warning.

Enabling conditions:
    - ``settings.CLOUD_SYNC_ENABLED`` is true
    - ``settings.CLOUD_PUSH_URL`` and ``CLOUD_PUSH_SECRET`` are both non-empty

If any is not met:
    - ENABLED=false        -> status='skipped', ok=True, no warning logged
    - URL/SECRET missing   -> status='failed', ok=False, warning logged
                              (misconfiguration)
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from . import (
    ai_config_status,
    cloud_push,
    cloud_sync,
    dashboard_projection,
    db,
    repository,
    settings,
)


log = logging.getLogger(__name__)
_MIN_DASHBOARD_PUSH_SECONDS = 10.0


@dataclass
class CloudBusinessResult:
    """Result of :func:`run_business_once`.

    Three status states:
        - ``'ok'``       build + push all succeeded
        - ``'failed'``   any step failed (including missing configuration)
        - ``'skipped'``  CLOUD_SYNC_ENABLED=false
    """
    ok: bool
    status: str
    sync_version: Optional[int] = None
    published_at: Optional[int] = None
    elapsed_seconds: float = 0.0
    push_stats: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class CloudDashboardResult:
    """Result of :func:`run_dashboard_once`.

    Three status states:
        - ``'ok'``       projection + push all succeeded
        - ``'failed'``   projection or push failed
        - ``'skipped'``  CLOUD_SYNC_ENABLED=false (in theory ingest would not
                         call dashboard after a skipped business run, but keep
                         a symmetric safeguard)
    """
    ok: bool
    status: str
    elapsed_seconds: float = 0.0
    push_stats: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


def _check_config() -> Optional[str]:
    """Return None when enabled+configured, otherwise an error string."""
    if not settings.CLOUD_SYNC_ENABLED:
        return None  # caller treats this as skipped
    if not settings.CLOUD_PUSH_URL or not settings.CLOUD_PUSH_SECRET:
        return "missing CLOUD_PUSH_URL or CLOUD_PUSH_SECRET"
    try:
        cloud_push.validate_cloud_push_url(settings.CLOUD_PUSH_URL)
        cloud_push.validate_cloud_push_secret(settings.CLOUD_PUSH_SECRET)
    except cloud_push.CloudPushError as exc:
        return str(exc)
    return None


def _local_cloud_versions() -> tuple[int, Optional[int]]:
    """Return local catalog_version and the last syncVersion recorded as ok."""
    conn = db.connect_readonly()
    try:
        raw_current = repository.get_catalog_version(conn) or "0"
        try:
            current = int(raw_current)
        except (TypeError, ValueError):
            current = 0
        last_pushed = dashboard_projection.last_successful_cloud_sync_version(conn)
    finally:
        conn.close()
    return current, last_pushed


def _remaining_seconds(deadline_at: Optional[float]) -> Optional[float]:
    if deadline_at is None:
        return None
    return float(deadline_at) - time.time()


def _dashboard_ai_status(deadline_at: Optional[float]) -> Optional[Dict[str, Any]]:
    """Get a fresh AI status when budget allows, otherwise use cached status."""
    remaining = _remaining_seconds(deadline_at)
    if remaining is not None:
        probe_budget = float(ai_config_status.estimated_refresh_timeout_seconds())
        required = probe_budget + _MIN_DASHBOARD_PUSH_SECONDS
        if probe_budget > 0 and remaining < required:
            log.warning(
                "[cloud_sync] dashboard ai_status refresh skipped: "
                "%.1fs remain, need >=%.1fs (probe %.1fs + push %.1fs)",
                remaining, required, probe_budget, _MIN_DASHBOARD_PUSH_SECONDS,
            )
            return ai_config_status.cached_ai_config_status()
    try:
        return ai_config_status.refresh_ai_config_status()
    except Exception:  # noqa: BLE001
        log.exception(
            "[cloud_sync] dashboard ai_status refresh failed; falling back to cached"
        )
        return ai_config_status.cached_ai_config_status()


def run_business_once(
    *,
    run_id: Optional[str] = None,
    timeout_seconds: Optional[int] = None,
    deadline_at: Optional[float] = None,
) -> CloudBusinessResult:
    """Business publish: build read model (include_dashboard=False) + push_read_model.

    On success the returned ``sync_version`` / ``published_at`` are used by
    :func:`run_dashboard_once` to chain the two stages -- they come from this
    round's ``meta.json`` and represent the version number and timestamp at
    the moment the cloud switched versions.

    ``timeout_seconds`` overrides the per-HTTP-call push timeout. ``deadline_at``
    is a wall-clock deadline cloud_push will respect at phase boundaries.
    """
    if not settings.CLOUD_SYNC_ENABLED:
        return CloudBusinessResult(ok=True, status="skipped")

    cfg_error = _check_config()
    if cfg_error:
        log.warning("[cloud_sync] business skipped run_id=%s: %s", run_id, cfg_error)
        return CloudBusinessResult(ok=False, status="failed", error=cfg_error)

    try:
        current_version, last_pushed = _local_cloud_versions()
    except Exception as exc:  # noqa: BLE001
        log.exception("[cloud_sync] failed to read local catalog version")
        return CloudBusinessResult(
            ok=False,
            status="failed",
            error=f"{type(exc).__name__}: {exc}",
        )
    if current_version <= 0:
        return CloudBusinessResult(
            ok=False,
            status="failed",
            error="catalog_version is 0; nothing publishable for cloud sync",
        )
    if last_pushed is not None and last_pushed > current_version:
        return CloudBusinessResult(
            ok=False,
            status="failed",
            sync_version=current_version,
            error=(
                f"local catalog_version {current_version} is behind last "
                f"successful cloud sync {last_pushed}; refusing rollback"
            ),
        )
    if last_pushed == current_version:
        published_at = (
            _read_published_at(
                cloud_sync.default_output_dir(), expected_version=current_version
            )
            or int(time.time())
        )
        log.info(
            "[cloud_sync] business already published run_id=%s v=%s; "
            "skipping writeBatch/switchMeta",
            run_id,
            current_version,
        )
        return CloudBusinessResult(
            ok=True,
            status="ok",
            sync_version=current_version,
            published_at=published_at,
            elapsed_seconds=0.0,
            push_stats={
                "stories": 0,
                "topics": 0,
                "digests": 0,
                "businessSkipped": True,
            },
        )

    effective_timeout = int(
        timeout_seconds
        if timeout_seconds is not None
        else settings.CLOUD_SYNC_TIMEOUT_SECONDS
    )

    started = time.time()
    try:
        build_stats = cloud_sync.build_read_model(include_dashboard=False)
        push_stats = cloud_push.push_read_model(
            url=settings.CLOUD_PUSH_URL,
            secret=settings.CLOUD_PUSH_SECRET,
            batch_size=settings.CLOUD_PUSH_BATCH_SIZE,
            max_body_bytes=settings.CLOUD_PUSH_MAX_BODY_BYTES,
            timeout_seconds=effective_timeout,
            deadline_at=deadline_at,
        )
    except Exception as exc:  # noqa: BLE001
        elapsed = time.time() - started
        log.exception(
            "[cloud_sync] business FAILED run_id=%s elapsed=%.1fs", run_id, elapsed
        )
        return CloudBusinessResult(
            ok=False,
            status="failed",
            elapsed_seconds=elapsed,
            error=f"{type(exc).__name__}: {exc}",
        )

    elapsed = time.time() - started
    sync_version = int(build_stats["currentVersion"])
    published_at = _read_published_at(
        cloud_sync.default_output_dir(), expected_version=sync_version
    )
    log.info(
        "[cloud_sync] business OK run_id=%s v=%s took=%.1fs stories=%s topics=%s digests=%s",
        run_id, sync_version, elapsed,
        push_stats.get("stories"), push_stats.get("topics"), push_stats.get("digests"),
    )
    return CloudBusinessResult(
        ok=True,
        status="ok",
        sync_version=sync_version,
        published_at=published_at,
        elapsed_seconds=elapsed,
        push_stats=push_stats,
    )


def run_dashboard_once(
    *,
    run_id: Optional[str] = None,
    sync_version: int,
    published_at: int,
    timeout_seconds: Optional[int] = None,
    deadline_at: Optional[float] = None,
) -> CloudDashboardResult:
    """Dashboard publish: build projection (reads this round's final
    cloud_sync_runs state) + push_dashboard (writeDashboard).

    Must only be called **after** the caller has written this round's
    business-publish final state (ok/failed) into SQLite ``cloud_sync_runs``,
    otherwise the projection will again read this round's ``running`` state.
    """
    if not settings.CLOUD_SYNC_ENABLED:
        return CloudDashboardResult(ok=True, status="skipped")

    cfg_error = _check_config()
    if cfg_error:
        log.warning("[cloud_sync] dashboard skipped run_id=%s: %s", run_id, cfg_error)
        return CloudDashboardResult(ok=False, status="failed", error=cfg_error)

    effective_timeout = int(
        timeout_seconds
        if timeout_seconds is not None
        else settings.CLOUD_SYNC_TIMEOUT_SECONDS
    )

    started = time.time()
    try:
        # The AI status refresh is done only once, in the dashboard stage; the
        # business publish does not depend on it, which also avoids dragging a
        # request-path AI probe into the business publish's critical path.
        # When too little deadline remains, use the cache directly so the
        # dashboard writeDashboard has wrap-up budget left.
        ai_status = _dashboard_ai_status(deadline_at)

        out_dir = cloud_sync.default_output_dir()
        dashboard_projection.build_dashboard_projection(
            out_dir,
            sync_version=int(sync_version),
            published_at=int(published_at),
            ai_status=ai_status,
        )
        push_stats = cloud_push.push_dashboard(
            url=settings.CLOUD_PUSH_URL,
            secret=settings.CLOUD_PUSH_SECRET,
            sync_version=int(sync_version),
            timeout_seconds=effective_timeout,
            max_body_bytes=settings.CLOUD_PUSH_MAX_BODY_BYTES,
            deadline_at=deadline_at,
        )
    except Exception as exc:  # noqa: BLE001
        elapsed = time.time() - started
        log.exception(
            "[cloud_sync] dashboard FAILED run_id=%s elapsed=%.1fs", run_id, elapsed
        )
        return CloudDashboardResult(
            ok=False,
            status="failed",
            elapsed_seconds=elapsed,
            error=f"{type(exc).__name__}: {exc}",
        )

    elapsed = time.time() - started
    log.info(
        "[cloud_sync] dashboard OK run_id=%s v=%s took=%.1fs", run_id, sync_version, elapsed
    )
    return CloudDashboardResult(
        ok=True,
        status="ok",
        elapsed_seconds=elapsed,
        push_stats=push_stats,
    )


def _read_published_at(
    out_dir: Path, *, expected_version: Optional[int] = None
) -> Optional[int]:
    """Read publishedAt from this round's ``meta.json`` (the timestamp at the moment the business switched versions)."""
    path = out_dir / "meta.json"
    try:
        meta = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if expected_version is not None:
        try:
            if int(meta.get("currentVersion")) != int(expected_version):
                return None
        except (TypeError, ValueError):
            return None
    val = meta.get("publishedAt")
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None
