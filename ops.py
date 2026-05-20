"""Operator diagnostics for the sync-only server runtime."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from . import ai_config_status, db, repository, settings
from .ai_agent import AiProviderConfig, build_ai_agent, build_ai_provider_configs
from .codex_cli import inspect_codex_runtime


def _provider_enabled(provider: str) -> bool:
    return provider.strip().lower() not in ("", "none", "fallback", "off", "disabled")


def _json_default(value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _print_json(data: Mapping[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=True, indent=2, default=_json_default))


def _console_text(text: str, stream) -> str:
    encoding = getattr(stream, "encoding", None) or "utf-8"
    try:
        text.encode(encoding)
        return text
    except LookupError:
        return text
    except UnicodeEncodeError:
        return text.encode(encoding, errors="backslashreplace").decode(encoding)


def _print_line(text: str = "") -> None:
    print(_console_text(str(text), sys.stdout))


def _config_summary(config: AiProviderConfig, index: int) -> Dict[str, Any]:
    return {
        "index": index,
        "name": config.name or f"provider-{index}",
        "model": config.model,
        "base_url": config.base_url,
        "balance_url": config.balance_url,
        "timeout_seconds": config.timeout,
        "max_concurrent_requests": config.max_concurrent_requests,
        "max_output_tokens": config.max_output_tokens,
        "api_key_configured": bool(config.api_key),
    }


def collect_ai_check(*, probe: bool) -> Dict[str, Any]:
    """Validate the AI provider config and optionally run connectivity probes."""

    try:
        settings.refresh_ai_settings_from_env_files()
    except Exception as exc:  # noqa: BLE001
        provider = (settings.AI_PROVIDER or "none").strip().lower()
        return {
            "enabled": _provider_enabled(provider),
            "provider": provider,
            "status": "err",
            "configs": [],
            "sources": _ai_config_sources(),
            "config_error": f"{type(exc).__name__}: {exc}",
        }
    provider = (settings.AI_PROVIDER or "none").strip().lower()
    enabled = _provider_enabled(provider)
    codex = inspect_codex_runtime()
    out: Dict[str, Any] = {
        "enabled": enabled,
        "provider": provider,
        "status": "disabled" if not enabled else "unknown",
        "configs": [],
        "sources": _ai_config_sources(),
        "codex": codex,
    }
    if not enabled:
        if not _codex_check_exit_ok(codex):
            out.update(
                {
                    "status": "err",
                    "config_error": f"Codex CLI unavailable: {codex.get('error')}",
                }
            )
        return out
    if "REPLACE_WITH" in (settings.AI_CONFIGS_JSON or "") or "REPLACE_WITH" in (
        settings.AI_API_KEY or ""
    ):
        out.update(
            {
                "status": "err",
                "config_error": "AI config still contains a REPLACE_WITH placeholder",
            }
        )
        return out

    try:
        configs = build_ai_provider_configs()
        build_ai_agent()
    except Exception as exc:  # noqa: BLE001
        out.update(
            {
                "status": "err",
                "config_error": f"{type(exc).__name__}: {exc}",
            }
        )
        return out

    out["configs"] = [
        _config_summary(config, index)
        for index, config in enumerate(configs, start=1)
    ]
    if not configs:
        out.update({"status": "missing_config", "config_error": "no usable AI configs"})
        return out

    if not probe:
        out["status"] = "parse_ok"
        return out

    probe_status = ai_config_status.refresh_ai_config_status()
    out.update(probe_status)
    return out


def _ai_check_exit_ok(status: Mapping[str, Any]) -> bool:
    return str(status.get("status") or "") in (
        "disabled",
        "parse_ok",
        "ok",
    ) and _codex_check_exit_ok(status.get("codex") or {})


def _codex_check_exit_ok(status: Mapping[str, Any]) -> bool:
    if not bool(status.get("enabled")):
        return True
    return str(status.get("status") or "") == "ok"


def collect_runtime_config_check() -> Dict[str, Any]:
    """Validate non-AI runtime settings before launcher writes systemd env.

    Importing this module has already loaded ``settings``, so malformed numeric,
    boolean, range, and timezone values fail before this function is reached.
    The explicit checks below cover config whose validation lives closer to the
    runtime boundary, such as cloud push URL/secret safety.
    """

    try:
        if settings.CLOUD_SYNC_ENABLED:
            from .cloud_push import validate_cloud_push_secret, validate_cloud_push_url

            validate_cloud_push_url(settings.CLOUD_PUSH_URL)
            validate_cloud_push_secret(settings.CLOUD_PUSH_SECRET)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "err",
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {
        "status": "ok",
        "cloud_sync_enabled": bool(settings.CLOUD_SYNC_ENABLED),
    }


def _runtime_config_check_exit_ok(status: Mapping[str, Any]) -> bool:
    return str(status.get("status") or "") == "ok"


def _ai_config_sources() -> Sequence[Mapping[str, str]]:
    sources = settings.get_ai_config_sources()
    if sources:
        return sources
    raw = (
        os.environ.get("HNREADER_AI_CONFIG_FILE")
        or os.environ.get("HACKERMINI_AI_CONFIG_FILE")
        or settings.AI_CONFIG_FILE
    )
    if raw:
        out = []
        for part in raw.split(os.pathsep):
            if not part:
                continue
            path = Path(part)
            if path.is_file():
                fmt = "json" if path.suffix.lower() == ".json" else "env"
            else:
                fmt = "missing"
            out.append({"path": str(path), "format": fmt})
        if out:
            return tuple(out)
    return ({"path": "process environment", "format": "env"},)


def _print_ai_check(status: Mapping[str, Any]) -> None:
    _print_line(f"AI provider: {status.get('provider')} (enabled={status.get('enabled')})")
    _print_line(f"AI status: {status.get('status')}")
    sources = status.get("sources") or []
    if sources:
        rendered = []
        for source in sources:
            if isinstance(source, Mapping):
                rendered.append(f"{source.get('format')}:{source.get('path')}")
        if rendered:
            _print_line(f"AI source: {', '.join(rendered)}")
    if status.get("config_error"):
        _print_line(f"Config error: {status.get('config_error')}")
    codex = status.get("codex")
    if isinstance(codex, Mapping):
        _print_line(
            "Codex CLI: "
            f"{codex.get('status')} enabled={codex.get('enabled')} "
            f"executable={codex.get('resolved_executable') or codex.get('executable')}"
        )
        if codex.get("codex_home"):
            _print_line(f"Codex home: {codex.get('codex_home')}")
        if codex.get("version"):
            _print_line(f"Codex version: {codex.get('version')}")
        if codex.get("error"):
            _print_line(f"Codex error: {codex.get('error')}")
    configs = status.get("configs") or []
    if configs:
        _print_line("Configs:")
        for item in configs:
            if not isinstance(item, Mapping):
                continue
            parts = [
                f"#{item.get('index')}",
                str(item.get("name") or ""),
                f"model={item.get('model')}",
                f"base_url={item.get('base_url')}",
            ]
            if "status" in item:
                parts.append(f"status={item.get('status')}")
            if item.get("message"):
                parts.append(f"message={item.get('message')}")
            _print_line("  " + " ".join(part for part in parts if part))


def _print_runtime_config_check(status: Mapping[str, Any]) -> None:
    _print_line(f"Runtime config status: {status.get('status')}")
    if status.get("error"):
        _print_line(f"Runtime config error: {status.get('error')}")


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    return dict(row) if row is not None else None


def _integrity_check(db_path: Path) -> Dict[str, Any]:
    if not db_path.is_file():
        return {"status": "missing", "path": str(db_path)}
    try:
        conn = db.connect_readonly(db_path)
        try:
            row = conn.execute("PRAGMA integrity_check").fetchone()
            result = row[0] if row is not None else ""
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "err",
            "path": str(db_path),
            "error": f"{type(exc).__name__}: {exc}",
        }
    if result == "ok":
        return {"status": "ok", "path": str(db_path)}
    return {"status": "err", "path": str(db_path), "error": str(result)}


def _latest_ingest(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    return _row_to_dict(repository.latest_ingest_run(conn))


def _latest_cloud_sync(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    try:
        row = conn.execute(
            "SELECT * FROM cloud_sync_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        return None
    return _row_to_dict(row)


def _ingest_attention_reason(
    latest: Optional[Mapping[str, Any]], *, now: Optional[int] = None
) -> str:
    if not isinstance(latest, Mapping):
        return ""
    status = str(latest.get("status") or "")
    if status in ("failed", "timeout"):
        return f"latest ingest status={status}"
    now_s = int(now if now is not None else time.time())
    if status != "running":
        reference_name = "finished_at"
        reference_at = latest.get("finished_at")
        if reference_at in (None, ""):
            reference_name = "started_at"
            reference_at = latest.get("started_at")
        try:
            reference_s = int(reference_at) if reference_at is not None else None
        except (TypeError, ValueError):
            reference_s = None
        if reference_s is not None:
            max_age = (
                int(settings.INGEST_INTERVAL_SECONDS)
                + int(settings.INGEST_ROUND_TIMEOUT_SECONDS)
                + int(settings.INGEST_CHILD_KILL_GRACE_SECONDS)
            )
            age = now_s - reference_s
            if age > max_age:
                return (
                    f"latest ingest stale: no newer run for {age}s "
                    f"after {reference_name}={reference_s}; expected within {max_age}s"
                )
        return ""
    deadline_at = latest.get("deadline_at")
    try:
        deadline_s = int(deadline_at) if deadline_at is not None else None
    except (TypeError, ValueError):
        deadline_s = None
    if deadline_s is not None and deadline_s <= now_s:
        return f"running ingest exceeded deadline_at={deadline_s}"

    started_at = latest.get("started_at")
    try:
        started_s = int(started_at) if started_at is not None else None
    except (TypeError, ValueError):
        started_s = None
    if started_s is not None:
        max_age = (
            int(settings.INGEST_ROUND_TIMEOUT_SECONDS)
            + int(settings.INGEST_CHILD_KILL_GRACE_SECONDS)
        )
        if now_s - started_s > max_age:
            return f"running ingest older than {max_age}s without terminal status"
    return ""


def _ingest_recommended_action(reason: str) -> str:
    if not reason:
        return ""
    if "status=timeout" in reason:
        return (
            "inspect logs for this run_id, identify whether HN fetch, AI, publish, "
            "cleanup, or cloud sync consumed the timeout, then reduce workload or "
            "increase HNREADER_INGEST_ROUND_TIMEOUT_SECONDS before restarting the supervisor"
        )
    if "status=failed" in reason:
        return (
            "inspect logs for this run_id and fix the first fetch/AI/publish error; "
            "use reset-failed only for story enrichment retries after the root cause is fixed"
        )
    if reason.startswith("running ingest"):
        return (
            "confirm whether the child process is still alive; if it is abandoned, stop the "
            "supervisor and run python -m server.ops repair before restarting"
        )
    if "latest ingest stale" in reason:
        return (
            "check launcher/systemd status and restart the ingest supervisor if no newer run "
            "has started within the configured interval"
        )
    return "inspect ingest logs and run python -m server.ops repair only for abandoned running rows"


def _cloud_sync_attention_reason(latest: Optional[Mapping[str, Any]]) -> str:
    if not isinstance(latest, Mapping):
        return ""
    status = str(latest.get("status") or "")
    if status in ("failed", "warning"):
        return f"latest cloud sync status={status}"
    cleanup_status = latest.get("cleanup_status")
    if not isinstance(cleanup_status, str):
        return ""
    if cleanup_status.startswith("failed:") or cleanup_status == "skipped:deadline":
        return f"latest cloud sync cleanup={cleanup_status}"
    return ""


def _cloud_sync_recommended_action(reason: str) -> str:
    if not reason:
        return ""
    if "status=failed" in reason:
        return (
            "inspect pushSync logs, HNREADER_CLOUD_SYNC_URL, and HNREADER_CLOUD_SYNC_SECRET; "
            "confirm the cloud database still serves the previous successful version before retrying"
        )
    if "status=warning" in reason:
        return (
            "inspect pushSync logs and dashboard projection output; business data may be current "
            "while dashboard or local bookkeeping needs the next run to catch up"
        )
    if "cleanup=" in reason:
        return (
            "inspect pushSync logs for cleanupOld, verify keepVersions handling and cloud DB permissions, "
            "then let the next cloud sync retry cleanup after the business version is stable"
        )
    return "inspect cloud_sync_runs error fields and pushSync logs before forcing another push"


def _collect_db_status(db_path: Path) -> Dict[str, Any]:
    out = {"integrity": _integrity_check(db_path), "schema_warnings": []}
    if out["integrity"].get("status") != "ok":
        return out
    try:
        conn = db.connect_readonly(db_path)
        try:
            out["latest_ingest"] = _latest_ingest(conn)
            reason = _ingest_attention_reason(out["latest_ingest"])
            out["latest_ingest_needs_attention"] = bool(reason)
            if reason:
                out["latest_ingest_attention_reason"] = reason
                out["latest_ingest_recommended_action"] = _ingest_recommended_action(
                    reason
                )
            out["latest_cloud_sync"] = _latest_cloud_sync(conn)
            cloud_reason = _cloud_sync_attention_reason(out["latest_cloud_sync"])
            out["latest_cloud_sync_needs_attention"] = bool(cloud_reason)
            if cloud_reason:
                out["latest_cloud_sync_attention_reason"] = cloud_reason
                out["latest_cloud_sync_recommended_action"] = (
                    _cloud_sync_recommended_action(cloud_reason)
                )
            if out["latest_cloud_sync"] is None:
                try:
                    conn.execute("SELECT 1 FROM cloud_sync_runs LIMIT 1").fetchone()
                except sqlite3.Error as exc:
                    out["schema_warnings"].append(
                        f"cloud_sync_runs unavailable: {type(exc).__name__}: {exc}"
                    )
            out["metrics"] = repository.get_pipeline_metrics(conn)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        out["query_error"] = f"{type(exc).__name__}: {exc}"
    return out


def _disk_status(path: Path) -> Dict[str, Any]:
    target = path if path.is_dir() else path.parent
    try:
        usage = shutil.disk_usage(target)
    except OSError as exc:
        return {"status": "err", "path": str(target), "error": str(exc)}
    return {
        "status": "ok",
        "path": str(target),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "free_gb": round(usage.free / (1024**3), 2),
    }


def _config_status(db_path: Path) -> Dict[str, Any]:
    return {
        "db_path": str(db_path),
        "cloud_sync_enabled": bool(settings.CLOUD_SYNC_ENABLED),
        "cloud_push_url_configured": bool(settings.CLOUD_PUSH_URL),
        "cloud_push_secret_configured": bool(settings.CLOUD_PUSH_SECRET),
        "ingest_run_retention_days": settings.INGEST_RUN_RETENTION_DAYS,
        "cloud_sync_run_retention_days": settings.CLOUD_SYNC_RUN_RETENTION_DAYS,
        "digest_retention_days": settings.DIGEST_RETENTION_DAYS,
        "comment_retention_days": settings.COMMENT_RETENTION_DAYS,
        "ai_config_file": settings.AI_CONFIG_FILE,
    }


def collect_doctor(*, probe_ai: bool) -> Dict[str, Any]:
    db_path = settings.get_db_path()
    ai_status = collect_ai_check(probe=probe_ai)
    out = {
        "checked_at": int(time.time()),
        "db": _collect_db_status(db_path),
        "ai": ai_status,
        "disk": _disk_status(db_path),
        "config": _config_status(db_path),
    }
    ok = (
        out["db"].get("integrity", {}).get("status") == "ok"
        and not out["db"].get("query_error")
        and not out["db"].get("schema_warnings")
        and not out["db"].get("latest_ingest_needs_attention")
        and not out["db"].get("latest_cloud_sync_needs_attention")
        and out["disk"].get("status") == "ok"
        and _ai_check_exit_ok(ai_status)
    )
    out["status"] = "ok" if ok else "err"
    return out


def collect_repair(*, now: Optional[int] = None) -> Dict[str, Any]:
    """Run bounded local repairs that must not overlap an active supervisor.

    Repairs are intentionally conservative:
    - run the idempotent SQLite schema initializer/migrator;
    - close ``ingest_runs`` rows left ``running`` by a crashed process.
    """

    from . import ingest

    lock = ingest._SupervisorInstanceLock()
    acquired = False
    try:
        lock.__enter__()
        acquired = True
    except ingest._SupervisorLockBusy as exc:
        return {
            "status": "err",
            "error": str(exc),
            "schema_initialized": False,
            "recovered_runs": [],
        }

    schema_initialized = False
    recovered = []
    try:
        db.init_db()
        schema_initialized = True
        recovered = ingest._recover_abandoned_running_runs(now=now)
        doctor = collect_doctor(probe_ai=False)
        return {
            "status": "ok" if doctor.get("status") == "ok" else "err",
            "schema_initialized": True,
            "recovered_runs": recovered,
            "doctor": doctor,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "err",
            "error": f"{type(exc).__name__}: {exc}",
            "schema_initialized": schema_initialized,
            "recovered_runs": recovered,
        }
    finally:
        if acquired:
            lock.__exit__(None, None, None)


def _fmt_time(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(value)))
    except (TypeError, ValueError, OSError):
        return str(value)


def _print_doctor(status: Mapping[str, Any]) -> None:
    _print_line(f"Doctor status: {status.get('status')}")
    db_status = status.get("db") or {}
    integrity = db_status.get("integrity") or {}
    _print_line(f"DB integrity: {integrity.get('status')} {integrity.get('path')}")
    if integrity.get("error"):
        _print_line(f"DB error: {integrity.get('error')}")
    for warning in db_status.get("schema_warnings") or []:
        _print_line(f"DB warning: {warning}")

    latest = db_status.get("latest_ingest")
    if isinstance(latest, Mapping):
        _print_line(
            "Latest ingest: "
            f"{latest.get('run_id')} status={latest.get('status')} "
            f"phase={latest.get('phase') or '-'} started={_fmt_time(latest.get('started_at'))}"
        )
        if db_status.get("latest_ingest_attention_reason"):
            _print_line(
                "Latest ingest warning: "
                f"{db_status.get('latest_ingest_attention_reason')}"
            )
        if db_status.get("latest_ingest_recommended_action"):
            _print_line(
                "Latest ingest action: "
                f"{db_status.get('latest_ingest_recommended_action')}"
            )
    else:
        _print_line("Latest ingest: -")

    latest_cloud = db_status.get("latest_cloud_sync")
    if isinstance(latest_cloud, Mapping):
        _print_line(
            "Latest cloud sync: "
            f"{latest_cloud.get('run_id')} status={latest_cloud.get('status')} "
            f"version={latest_cloud.get('sync_version')} "
            f"cleanup={latest_cloud.get('cleanup_status') or '-'} "
            f"started={_fmt_time(latest_cloud.get('started_at'))}"
        )
        if db_status.get("latest_cloud_sync_attention_reason"):
            _print_line(
                "Latest cloud sync warning: "
                f"{db_status.get('latest_cloud_sync_attention_reason')}"
            )
        if db_status.get("latest_cloud_sync_recommended_action"):
            _print_line(
                "Latest cloud sync action: "
                f"{db_status.get('latest_cloud_sync_recommended_action')}"
            )
    else:
        _print_line("Latest cloud sync: -")

    ai_status = status.get("ai") or {}
    _print_line(f"AI status: {ai_status.get('status')} provider={ai_status.get('provider')}")
    if ai_status.get("config_error"):
        _print_line(f"AI config error: {ai_status.get('config_error')}")
    codex_status = ai_status.get("codex")
    if isinstance(codex_status, Mapping):
        _print_line(
            "Codex CLI: "
            f"{codex_status.get('status')} enabled={codex_status.get('enabled')} "
            f"executable={codex_status.get('resolved_executable') or codex_status.get('executable')}"
        )
        if codex_status.get("error"):
            _print_line(f"Codex error: {codex_status.get('error')}")

    disk = status.get("disk") or {}
    _print_line(f"Disk: {disk.get('status')} free={disk.get('free_gb')}GB path={disk.get('path')}")

    config = status.get("config") or {}
    _print_line(
        "Config: "
        f"cloud_sync_enabled={config.get('cloud_sync_enabled')} "
        f"ingest_run_retention_days={config.get('ingest_run_retention_days')} "
        f"cloud_sync_run_retention_days={config.get('cloud_sync_run_retention_days')}"
    )


def _print_repair(status: Mapping[str, Any]) -> None:
    _print_line(f"Repair status: {status.get('status')}")
    if status.get("error"):
        _print_line(f"Repair error: {status.get('error')}")
    _print_line(f"Schema initialized: {status.get('schema_initialized')}")
    recovered = status.get("recovered_runs") or []
    _print_line(f"Recovered running ingest runs: {len(recovered)}")
    for item in recovered:
        if not isinstance(item, Mapping):
            continue
        _print_line(
            "  "
            f"{item.get('run_id')} status={item.get('status')} "
            f"previous_phase={item.get('previous_phase') or '-'}"
        )
    doctor = status.get("doctor")
    if isinstance(doctor, Mapping):
        _print_doctor(doctor)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m server.ops",
        description="HN Reader operator diagnostics.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    ai_parser = sub.add_parser("ai-check", help="validate AI config and probe providers")
    ai_parser.add_argument("--no-probe", action="store_true", help="only parse local config")
    ai_parser.add_argument("--json", action="store_true", help="print JSON")
    ai_parser.add_argument("--quiet", action="store_true", help="suppress success output")

    config_parser = sub.add_parser(
        "config-check",
        help="validate non-AI runtime config without touching the DB",
    )
    config_parser.add_argument("--json", action="store_true", help="print JSON")
    config_parser.add_argument("--quiet", action="store_true", help="suppress success output")

    doctor_parser = sub.add_parser("doctor", help="run local production diagnostics")
    doctor_parser.add_argument("--no-probe-ai", action="store_true", help="skip AI network probes")
    doctor_parser.add_argument("--json", action="store_true", help="print JSON")

    repair_parser = sub.add_parser(
        "repair",
        help="initialize schema and recover abandoned running ingest rows",
    )
    repair_parser.add_argument("--json", action="store_true", help="print JSON")

    args = parser.parse_args(argv)
    if args.cmd == "ai-check":
        status = collect_ai_check(probe=not args.no_probe)
        if args.json:
            _print_json(status)
        elif not args.quiet or not _ai_check_exit_ok(status):
            _print_ai_check(status)
        return 0 if _ai_check_exit_ok(status) else 1

    if args.cmd == "config-check":
        status = collect_runtime_config_check()
        if args.json:
            _print_json(status)
        elif not args.quiet or not _runtime_config_check_exit_ok(status):
            _print_runtime_config_check(status)
        return 0 if _runtime_config_check_exit_ok(status) else 1

    if args.cmd == "doctor":
        status = collect_doctor(probe_ai=not args.no_probe_ai)
        if args.json:
            _print_json(status)
        else:
            _print_doctor(status)
        return 0 if status.get("status") == "ok" else 1

    if args.cmd == "repair":
        status = collect_repair()
        if args.json:
            _print_json(status)
        else:
            _print_repair(status)
        return 0 if status.get("status") == "ok" else 1

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
