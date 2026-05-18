"""Dashboard projection: build SQLite -> JSON snapshot for the cloud dashboard.

In the production architecture the server no longer exposes the admin/dashboard
HTTP surface; both the mini-program backend and operations read the dashboard
state from the cloud-development database. ``build_dashboard_projection``
gathers the SQLite data relevant to the monitoring page into three files,
written under ``.cloud-sync-output/``, which ``cloud_push.push_read_model``
pushes together to the ``pushSync`` cloud function.

Output files:

``dashboard_summary.json``
    Single document, ``_id="summary"``. pipeline metrics + latest run + AI status.
``dashboard_ingest_runs.jsonl``
    The most recent N ingest run records, ``_id="{syncVersion}:{run_id}"``.
``dashboard_cloud_sync_runs.jsonl``
    The most recent N cloud sync push records, ``_id="{syncVersion}:{run_id}:{started_at}"``.

Sensitive information such as error stack traces, API keys, and article body
text is never written -- the dashboard collection holds display-only summaries.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from . import db, repository, settings


log = logging.getLogger(__name__)


DASHBOARD_SUMMARY_FILE = "dashboard_summary.json"
DASHBOARD_INGEST_RUNS_FILE = "dashboard_ingest_runs.jsonl"
DASHBOARD_CLOUD_SYNC_RUNS_FILE = "dashboard_cloud_sync_runs.jsonl"

DEFAULT_RECENT_INGEST_RUNS = 100
DEFAULT_RECENT_CLOUD_SYNC_RUNS = 100


def _decode_run_ai_usage(row: sqlite3.Row) -> Optional[Dict[str, Any]]:
    if row is None or "ai_usage" not in row.keys():
        return None
    raw = row["ai_usage"]
    if not raw:
        return None
    try:
        usage = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return usage if isinstance(usage, dict) else None


def _compact_ai_usage_for_dashboard(
    usage: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Keep dashboard run history small while preserving token/cost columns."""
    if not isinstance(usage, dict):
        return None
    keep = (
        "requests",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_input_tokens",
        "cost",
        "unpriced_tokens",
    )
    out = {key: usage.get(key) for key in keep if key in usage}
    return out or None


def row_to_run_summary(
    row: sqlite3.Row,
    *,
    now: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Compact, dashboard-safe view of an ``ingest_runs`` row.

    Marks stale ``running`` rows when their deadline has passed so the
    cloud dashboard can highlight stuck rounds even if no terminal status
    was ever written.
    """
    if row is None:
        return None
    now_epoch = int(now if now is not None else time.time())
    deadline_at = (
        int(row["deadline_at"]) if row["deadline_at"] is not None else None
    )
    raw_status = row["status"] or ""
    stale = (
        raw_status == "running"
        and deadline_at is not None
        and deadline_at < now_epoch
    )
    status = "stale" if stale else raw_status
    out: Dict[str, Any] = {
        "run_id": row["run_id"],
        "started_at": int(row["started_at"]) if row["started_at"] is not None else None,
        "deadline_at": deadline_at,
        "finished_at": int(row["finished_at"]) if row["finished_at"] is not None else None,
        "status": status,
        "raw_status": raw_status,
        "stale": stale,
        "overdue_seconds": (
            max(0, now_epoch - deadline_at) if stale and deadline_at else 0
        ),
        "phase": row["phase"] or "",
        "raw_count": int(row["raw_count"] or 0),
        "candidate_count": int(row["candidate_count"] or 0),
        "claimed": int(row["claimed"] or 0),
        "done": int(row["done"] or 0),
        "failed": int(row["failed"] or 0),
        "retried": int(row["retried"] or 0),
        "has_error": bool(row["error"]),
    }
    ai_usage = _decode_run_ai_usage(row)
    compact_ai_usage = _compact_ai_usage_for_dashboard(ai_usage)
    if compact_ai_usage is not None:
        out["ai_usage"] = compact_ai_usage
    return out


def recent_ingest_runs(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_RECENT_INGEST_RUNS,
    now: Optional[int] = None,
) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM ingest_runs ORDER BY started_at DESC LIMIT ?",
        (max(1, int(limit)),),
    ).fetchall()
    return [row_to_run_summary(r, now=now) for r in rows if r is not None]


def _cloud_sync_row_summary(row: sqlite3.Row) -> Dict[str, Any]:
    error_raw = row["error"]
    # Old DBs may not have cleanup_status yet; ``sqlite3.Row`` raises
    # IndexError on missing keys, so be defensive.
    try:
        cleanup_status = row["cleanup_status"]
    except (IndexError, KeyError):
        cleanup_status = None
    return {
        "run_id": row["run_id"],
        "started_at": int(row["started_at"]) if row["started_at"] is not None else None,
        "finished_at": (
            int(row["finished_at"]) if row["finished_at"] is not None else None
        ),
        "status": row["status"] or "",
        "sync_version": (
            int(row["sync_version"]) if row["sync_version"] is not None else None
        ),
        "stories": int(row["stories"]) if row["stories"] is not None else None,
        "topics": int(row["topics"]) if row["topics"] is not None else None,
        "digests": int(row["digests"]) if row["digests"] is not None else None,
        "elapsed_seconds": (
            float(row["elapsed_seconds"])
            if row["elapsed_seconds"] is not None
            else None
        ),
        # Only expose the has_error boolean -- the error text may contain
        # operations-only details such as the push URL / upstream response
        # fragments, which must not reach the cloud dashboard. The full error
        # is still readable from the server logs or the local SQLite
        # ``cloud_sync_runs`` table.
        "has_error": bool(error_raw),
        # cleanup_status is the final state of the cleanupOld cloud function
        # (ok / failed:* / skipped:*). It lets the dashboard directly see the
        # "business ok but cleanup failing repeatedly" silent-degradation
        # scenario. NULL means this round never reached cleanup at all (the
        # business push did not succeed or the business was skipped).
        "cleanup_status": cleanup_status,
    }


def recent_cloud_sync_runs(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_RECENT_CLOUD_SYNC_RUNS,
) -> List[Dict[str, Any]]:
    """Read the most recent N ``cloud_sync_runs``. Returns an empty list if the table does not exist."""
    try:
        rows = conn.execute(
            "SELECT * FROM cloud_sync_runs ORDER BY started_at DESC LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [_cloud_sync_row_summary(r) for r in rows if r is not None]


def last_successful_cloud_sync_version(
    conn: sqlite3.Connection,
) -> Optional[int]:
    """The last syncVersion that was successfully pushed (used for meta.previousVersion).

    Returning ``None`` means nothing has ever been pushed successfully -- in
    that case both cleanup and previousVersion must be treated as "there is no
    previous version" rather than blindly using ``currentVersion - 1``, to
    avoid referencing a version number that does not actually exist in the cloud.
    """
    try:
        row = conn.execute(
            """
            SELECT sync_version
            FROM cloud_sync_runs
            WHERE status = 'ok'
              AND sync_version IS NOT NULL
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.Error:
        return None
    if row is None or row["sync_version"] is None:
        return None
    try:
        return int(row["sync_version"])
    except (TypeError, ValueError):
        return None


def _safe_ai_status_for_dashboard(ai_status: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Strip dashboard AI status of fields we don't want in cloud DB.

    The dashboard only needs the overall verdict, per-config status, and
    balance summary. Provider URLs / models are kept because they are
    operator-facing identifiers (not secrets) and already exist in
    ``ai_config_status``. ``api_key_configured`` boolean is kept; the key
    itself is never produced by that module.
    """
    if not isinstance(ai_status, dict):
        return None
    keep_top = {
        "enabled",
        "provider",
        "status",
        "checked_at",
        "cached",
        "stale",
        "refreshing",
        "config_error",
    }
    out: Dict[str, Any] = {k: ai_status.get(k) for k in keep_top if k in ai_status}
    configs_raw = ai_status.get("configs")
    if isinstance(configs_raw, list):
        keep_cfg = {
            "index",
            "name",
            "model",
            "base_url",
            "probe_url",
            "probe_kind",
            "status",
            "connected",
            "http_status",
            "message",
            "balance",
            "checked_at",
            "api_key_configured",
            "max_concurrent_requests",
            "pricing",
            "timeout_seconds",
            "probe_timeout_seconds",
        }
        out["configs"] = [
            {k: cfg.get(k) for k in keep_cfg if k in cfg}
            for cfg in configs_raw
            if isinstance(cfg, dict)
        ]
    return out


def build_dashboard_summary(
    conn: sqlite3.Connection,
    *,
    sync_version: int,
    server_time: int,
    published_at: int,
    ai_status: Optional[Dict[str, Any]] = None,
    recent_runs: Optional[Iterable[Dict[str, Any]]] = None,
    recent_cloud_syncs: Optional[Iterable[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build the ``hn_dashboard_summary`` single document.

    Caller passes ``recent_runs`` / ``recent_cloud_syncs`` so the per-run
    JSONL files stay in lockstep with whatever this summary reports for
    ``latest_run`` and ``latest_cloud_sync``.
    """
    metrics = repository.get_pipeline_metrics(conn)
    latest = repository.latest_ingest_run(conn)
    latest_run = row_to_run_summary(latest, now=server_time)
    # ``repository.get_pipeline_metrics`` embeds ``latest_run`` as a raw row dict
    # — that includes the full ``error`` text, which we MUST NOT publish to a
    # cloud collection (could carry provider keys, stack traces, PII). Overwrite
    # with the sanitized projection ``row_to_run_summary`` produced (only
    # ``has_error`` boolean leaks). Same trick the old in-process admin_metrics
    # used to apply — keep it here so the cloud copy never reintroduces the leak.
    metrics["latest_run"] = latest_run
    # ``recent_runs`` may already include latest_run as its first element; we
    # still serialize it under ``latestRun`` so dashboards can highlight the
    # most recent round without scanning the list.

    runs_list = list(recent_runs) if recent_runs is not None else recent_ingest_runs(
        conn, now=server_time
    )
    cloud_syncs_list = (
        list(recent_cloud_syncs)
        if recent_cloud_syncs is not None
        else recent_cloud_sync_runs(conn)
    )
    latest_cloud_sync = cloud_syncs_list[0] if cloud_syncs_list else None

    return {
        "_id": "summary",
        "syncVersion": int(sync_version),
        "publishedAt": int(published_at),
        "serverTime": int(server_time),
        "appVersion": settings.APP_VERSION,
        "metrics": metrics,
        "latestRun": latest_run,
        "latestCloudSync": latest_cloud_sync,
        "ai": _safe_ai_status_for_dashboard(ai_status),
        "recentIngestRunsCount": len(runs_list),
        "recentCloudSyncRunsCount": len(cloud_syncs_list),
    }


def _write_jsonl(path: Path, docs: Iterable[Dict[str, Any]]) -> int:
    tmp = path.with_name(f".{path.name}.{time.time_ns()}.tmp")
    count = 0
    try:
        with tmp.open("w", encoding="utf-8") as f:
            for doc in docs:
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                count += 1
        tmp.replace(path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
    return count


def _write_text_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.{time.time_ns()}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _ingest_run_doc(
    summary: Dict[str, Any], *, sync_version: int
) -> Dict[str, Any]:
    return {
        "_id": f"{sync_version}:{summary['run_id']}",
        "syncVersion": int(sync_version),
        **summary,
    }


def _cloud_sync_run_doc(
    summary: Dict[str, Any], *, sync_version: int
) -> Dict[str, Any]:
    started = summary.get("started_at") or 0
    run_id = summary.get("run_id") or "unknown"
    return {
        "_id": f"{sync_version}:{run_id}:{int(started)}",
        "syncVersion": int(sync_version),
        **summary,
    }


def build_dashboard_projection(
    out_dir: Path,
    *,
    sync_version: int,
    published_at: int,
    ai_status: Optional[Dict[str, Any]] = None,
    server_time: Optional[int] = None,
    ingest_run_limit: int = DEFAULT_RECENT_INGEST_RUNS,
    cloud_sync_run_limit: int = DEFAULT_RECENT_CLOUD_SYNC_RUNS,
) -> Dict[str, Any]:
    """Write dashboard summary + recent runs to ``out_dir`` and return stats.

    Caller is responsible for ``out_dir.mkdir(parents=True, exist_ok=True)``
    (typically the cloud sync output directory which already exists).

    ``ai_status`` should be the result of a deliberate
    ``ai_config_status.refresh_ai_config_status()`` or ``get_ai_config_status``
    call taken at the ingest cadence — dashboard projection itself never
    schedules a probe so a cloud sync round can't trigger network calls to
    the AI provider on a request path.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    server_time_int = int(server_time if server_time is not None else time.time())
    sync_version = int(sync_version)
    published_at_int = int(published_at)

    conn = db.connect_readonly()
    try:
        runs = recent_ingest_runs(
            conn, limit=ingest_run_limit, now=server_time_int
        )
        cloud_syncs = recent_cloud_sync_runs(conn, limit=cloud_sync_run_limit)
        summary = build_dashboard_summary(
            conn,
            sync_version=sync_version,
            server_time=server_time_int,
            published_at=published_at_int,
            ai_status=ai_status,
            recent_runs=runs,
            recent_cloud_syncs=cloud_syncs,
        )
    finally:
        conn.close()

    summary_path = out_dir / DASHBOARD_SUMMARY_FILE
    _write_text_atomic(
        summary_path,
        json.dumps(summary, ensure_ascii=False, indent=2),
    )

    ingest_runs_path = out_dir / DASHBOARD_INGEST_RUNS_FILE
    ingest_docs = [
        _ingest_run_doc(r, sync_version=sync_version) for r in runs
    ]
    _write_jsonl(ingest_runs_path, ingest_docs)

    cloud_runs_path = out_dir / DASHBOARD_CLOUD_SYNC_RUNS_FILE
    cloud_docs = [
        _cloud_sync_run_doc(s, sync_version=sync_version) for s in cloud_syncs
    ]
    _write_jsonl(cloud_runs_path, cloud_docs)

    log.info(
        "[dashboard] projection v=%d ingest_runs=%d cloud_sync_runs=%d ai_status=%s",
        sync_version,
        len(ingest_docs),
        len(cloud_docs),
        summary.get("ai", {}).get("status") if summary.get("ai") else "n/a",
    )
    return {
        "syncVersion": sync_version,
        "ingestRuns": len(ingest_docs),
        "cloudSyncRuns": len(cloud_docs),
        "outputDir": str(out_dir),
    }
