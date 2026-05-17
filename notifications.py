"""Admin alert delivery for ingest failures."""

from __future__ import annotations

import json
import logging
import os
import re
import smtplib
from collections import deque
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from . import db, repository, settings


log = logging.getLogger("server.notifications")

_ALERT_SENDING_ORPHAN_SECONDS = 60
_ALERT_SENDING_STALE_SECONDS = 15 * 60

_ALERT_TITLES = {
    "fetch_failed": "Fetch produced no publishable candidates",
    "enrich_timeout": "AI enrichment timed out",
    "enrich_incomplete": "AI enrichment incomplete",
    "publish_failed": "Publish produced no visible content",
    "digest_failed": "Daily digest generation failed",
    "cleanup_failed": "Cleanup task failed",
    "ingest_failed": "Ingest pipeline error",
    "ingest_timeout": "Ingest child process timed out",
    "ingest_child_start_failed": "Ingest child process failed to start",
    "ingest_child_failed": "Ingest child process exited abnormally",
    "cloud_sync_deferred": "Cloud sync deferred",
    "cloud_sync_warning": "Cloud sync degraded",
    "cloud_sync_failed": "Cloud sync failed",
    "alert_outbox_parse_error": "Local alert queue has unparseable records",
}

_FIELD_LABELS = {
    "ai_model": "AI model",
    "ai_provider": "AI provider",
    "candidate_count": "Candidate count",
    "checkpoint": "Checkpoint",
    "cloud_sync_elapsed_seconds": "Cloud sync elapsed",
    "cloud_sync_error": "Cloud sync error",
    "cloud_sync_status": "Cloud sync status",
    "cloud_sync_timeout_seconds": "Cloud sync per-call timeout",
    "cloud_sync_version": "Cloud sync version",
    "digest": "Digest info",
    "elapsed_seconds": "Elapsed",
    "enrich": "AI enrichment info",
    "enrich_status_counts": "AI enrichment status",
    "exit_code": "Exit code",
    "fetch": "Fetch info",
    "incomplete_candidates": "Incomplete candidates",
    "ingest_interval_seconds": "Ingest poll interval",
    "ingest_round_timeout_seconds": "Per-round total timeout",
    "publish": "Publish info",
    "recent_enrich_errors": "Recent AI errors",
    "run_id": "Run ID",
    "started_at": "Started at",
    "target_count": "Target count",
    "timeout_seconds": "Timeout",
}


def _cooldown_key(event_type: str) -> str:
    return f"alert_last_sent:{event_type}"


def _claim_cooldown_slot(event_type: str, now: int) -> bool:
    """Atomic CAS: try to claim the cooldown slot for ``event_type``.

    Returns ``True`` iff this caller successfully claimed the slot and may
    proceed with sending / outbox-writing the alert. ``False`` means another
    caller (or the same one within the cooldown window) already claimed it
    and the alert must be suppressed.

    The read of the previous timestamp and the write of the new one happen
    inside a single ``BEGIN IMMEDIATE`` transaction (see ``db.transaction``)
    so two concurrent failures cannot both observe an empty / expired
    cooldown and end up duplicating SMTP sends or outbox rows. The previous
    "check then write" implementation was a TOCTOU race even on a single
    process when two threads or two SQLite connections raced.
    """
    conn = db.connect()
    try:
        with db.transaction(conn):
            last = repository.get_meta_int(conn, _cooldown_key(event_type))
            if last is not None and now - last < settings.ALERT_COOLDOWN_SECONDS:
                return False
            # Write the claim inside the same transaction so the next caller
            # observes it the instant our BEGIN IMMEDIATE commits.
            repository.set_meta(conn, _cooldown_key(event_type), str(now))
            return True
    finally:
        conn.close()


def _alert_title(event_type: str, subject: str) -> str:
    return _ALERT_TITLES.get(event_type) or subject.strip() or event_type


def _format_subject(event_type: str, subject: str) -> str:
    return f"HNReader alert: {_alert_title(event_type, subject)}"


def _text_blob(message: str, fields: Optional[Mapping[str, Any]]) -> str:
    parts = [message]
    if fields:
        for key, value in fields.items():
            parts.append(str(key))
            parts.append(str(value))
    return "\n".join(parts).lower()


_HTTP_ERROR_CODE_RE = re.compile(r"\bhttp\s+(\d{3})\b", re.IGNORECASE)


def _ai_http_error_codes(text: str) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for match in _HTTP_ERROR_CODE_RE.finditer(text):
        try:
            code = int(match.group(1))
        except (TypeError, ValueError):
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _has_ai_http_code(text: str, code: int) -> bool:
    return code in _ai_http_error_codes(text)


def _has_ai_provider_unavailable_code(text: str) -> bool:
    return any(code == 429 or 500 <= code <= 599 for code in _ai_http_error_codes(text))


def _format_seconds(value: Any) -> str:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)
    if n.is_integer():
        return str(int(n))
    return str(round(n, 1))


def _status_label(event_type: str, message: str, fields: Optional[Mapping[str, Any]]) -> str:
    if event_type == "cloud_sync_deferred":
        return "Deferred (cloud sync did not start this round; will retry in a later round)"
    if event_type == "cloud_sync_warning":
        return "Degraded (core pipeline partially succeeded, but follow-up state needs handling)"
    if event_type == "enrich_incomplete":
        return "Partial success (usable content published; some candidates still pending)"
    if event_type in {"digest_failed", "cleanup_failed"}:
        return "Error (main content may be usable; auxiliary task failed)"
    return "Error"


def _severity_label(event_type: str, message: str, fields: Optional[Mapping[str, Any]]) -> str:
    text = _text_blob(message, fields)
    if event_type in {"enrich_timeout", "enrich_incomplete"}:
        if _has_ai_http_code(text, 402):
            return "P1: AI balance/quota problem, address ASAP"
        if _has_ai_provider_unavailable_code(text):
            return "P1: AI service unavailable, address ASAP"
    if event_type in {
        "ingest_failed",
        "ingest_timeout",
        "ingest_child_start_failed",
        "ingest_child_failed",
        "publish_failed",
    }:
        return "P1: address ASAP"
    if event_type == "cloud_sync_failed":
        if "refusing rollback" in text:
            return "P1: automatic overwrite blocked, manual version check required"
        return "P1: cloud publish failed, address ASAP"
    if event_type in {
        "enrich_timeout",
        "enrich_incomplete",
        "digest_failed",
        "cloud_sync_warning",
    }:
        return "P2: needs attention, usually compensated by the next round"
    if event_type in {"fetch_failed", "cleanup_failed", "cloud_sync_deferred"}:
        return "P3: observe first, act only if it recurs"
    return "P2: needs review"


def _impact_summary(
    event_type: str,
    message: str,
    fields: Optional[Mapping[str, Any]],
) -> str:
    text = _text_blob(message, fields)
    if event_type == "cloud_sync_deferred":
        return (
            "The local ingest pipeline was not blocked by cloud sync; the cloud database "
            "temporarily stays on the last successful version. It will retry next round "
            "when the budget is sufficient, and a deferral never rewrites an already "
            "successful cloud version."
        )
    if event_type == "cloud_sync_warning":
        if "record write failed" in text:
            return (
                "The cloud business data may have been pushed successfully, but the local "
                "cloud_sync_runs table has no reliable record. The next incremental baseline "
                "may be untrustworthy; check SQLite writes and disk state first."
            )
        if "dashboard" in text:
            return (
                "The cloud business collections completed or skipped a duplicate version; "
                "the ops dashboard projection did not finish syncing. User-facing main "
                "content usually keeps using the last available state, but the ops view "
                "may lag."
            )
        return (
            "The cloud sync main path degraded; some state may already have succeeded, "
            "but cloud and local records still need to be reconciled."
        )
    if event_type == "cloud_sync_failed":
        if "refusing rollback" in text:
            return (
                "The local catalog_version is behind the last successful cloud version, so "
                "the system refused to roll back and overwrite the cloud database. The cloud "
                "keeps the newer version; the local database needs manual review or restore."
            )
        if "catalog_version is 0" in text:
            return "There is no publishable version locally yet, so the cloud database will not be overwritten with empty data."
        return (
            "Cloud business publish did not complete this round; the cloud database should "
            "keep the last successful version. Local data is retained and later rounds can "
            "retry."
        )
    if event_type == "ingest_timeout":
        return "The ingest child process was terminated by the supervisor this round; the last successfully published version should remain available."
    if event_type == "ingest_child_start_failed":
        return "The child process did not start this round; the last successfully published version should remain available, but the runtime environment needs checking."
    if event_type == "ingest_child_failed":
        return "The child process exited with a non-zero code this round; the last successfully published version should remain available, but confirm it is not failing repeatedly."
    if event_type == "ingest_failed":
        return "The ingest pipeline did not complete normally this round; the last successfully published version should remain available."
    if event_type == "publish_failed":
        return "No new visible content was produced this round; the client keeps relying on the last successful publish."
    if event_type == "fetch_failed":
        return "No publishable candidates were fetched this round; this usually does not damage existing data, but repeated occurrences leave content stale for a long time."
    if event_type == "enrich_timeout":
        if _has_ai_http_code(text, 402):
            return "The AI provider returned HTTP 402; the current balance, quota, or billing status is unavailable. Candidates stay in the queue and the last successful publish remains available."
        if _has_ai_provider_unavailable_code(text):
            return "The AI provider returned a rate-limit or 5xx error; enrichment is currently unavailable. Candidates stay in the queue and the last successful publish remains available."
        return "Some AI enrichment did not finish this round; candidates were released and later rounds can continue processing them."
    if event_type == "enrich_incomplete":
        if _has_ai_http_code(text, 402):
            return "The AI provider returned HTTP 402; the current balance, quota, or billing status is unavailable. Candidates stay in the queue and the last successful publish remains available."
        if _has_ai_provider_unavailable_code(text):
            return "The AI provider returned a rate-limit or 5xx error; enrichment is currently unavailable. Candidates stay in the queue and the last successful publish remains available."
        return "Only content that was already prepared was published this round; unfinished candidates will be completed in later rounds."
    if event_type == "digest_failed":
        return "The main news list may have been published, but daily digest generation or validation failed; the digest page may stay on its previous state."
    if event_type == "cleanup_failed":
        return "The main pipeline is usually unaffected, but cleanup of historical data, temporary state, or old records may lag."
    return "This alert means a pipeline step did not meet expectations; review it together with the run ID and context."


def _recovery_summary(
    event_type: str,
    message: str,
    fields: Optional[Mapping[str, Any]],
) -> str:
    if event_type.startswith("cloud_sync_"):
        return (
            "The system will retry cloud sync in later ingest rounds; an already "
            "successfully pushed catalog_version is detected and skipped, avoiding "
            "repeated rewrites of the cloud database."
        )
    if event_type in {"enrich_timeout", "enrich_incomplete"}:
        text = _text_blob(message, fields)
        if _has_ai_http_code(text, 402):
            return "Candidates stay in the queue and retry automatically per retry_after/cooldown; processing resumes after the balance is topped up, billing is fixed, or an available provider/model is switched to."
        if _has_ai_provider_unavailable_code(text):
            return "Candidates stay in the queue and retry automatically per retry_after/cooldown; processing resumes once the rate limit lifts or the provider recovers."
        return "Unfinished candidates return to a retryable state and the next round can continue processing them."
    if event_type in {
        "ingest_timeout",
        "ingest_child_start_failed",
        "ingest_child_failed",
        "ingest_failed",
    }:
        return "The next round continues after the supervisor/launcher restarts; versions already successfully published locally are not cleared by a restart."
    if event_type in {"digest_failed", "cleanup_failed"}:
        return "Later rounds will run the corresponding stage again; main data completed before the failure is not rolled back."
    return "Alerts of the same type are rate-limited by a cooldown; if the problem persists, you will be reminded again later."


def _recommendations(
    event_type: str,
    message: str,
    fields: Optional[Mapping[str, Any]],
) -> list[str]:
    text = _text_blob(message, fields)
    actions: list[str] = []

    def add(action: str) -> None:
        if action not in actions:
            actions.append(action)

    run_id = (fields or {}).get("run_id")
    if run_id:
        add(f"First filter the ingest log by run ID {run_id} to confirm whether the failure happened in the fetch, AI, publish, or cloud sync stage.")
    else:
        add("First inspect the ingest log around the email timestamp to confirm the failing stage and the first exception.")

    if event_type == "cloud_sync_deferred":
        add("If this is just an occasional deferral, observe whether the next round catches up automatically.")
        add(
            "If deferrals are continuous, increase HNREADER_INGEST_ROUND_TIMEOUT_SECONDS, or reduce per-round AI/fetch work, "
            "so the finalize stage keeps at least HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS plus a safety margin."
        )
        add("Check whether cloud_sync_runs keeps showing deferred, to avoid the cloud version falling behind for a long time.")
    elif event_type == "cloud_sync_warning":
        if "record write failed" in text:
            add("First check the SQLite database directory permissions, disk space, and lock waits; do not manually delete cloud_sync_runs.")
            add("Confirm whether the cloud currentVersion matches the local catalog_version before deciding whether to restore the local record from backup.")
        if "dashboard" in text:
            add("Check the pushSync cloud function logs, dashboard projection write permissions, and the HNREADER_CLOUD_SYNC_URL/SECRET configuration.")
            add("The business collections are usually already available; confirm whether the next round's dashboard catches up automatically.")
        add("Inspect the cloud sync error field to determine whether it is a network, auth, cloud function error, or a local record-write failure.")
    elif event_type == "cloud_sync_failed":
        if "refusing rollback" in text:
            add("Do not lower or rewrite the cloud version; first reconcile the cloud currentVersion, cloud_sync_runs, and the local catalog_version.")
            add("Prefer restoring the local database from a backup or the last successful run, then re-run the launcher.sh-managed services.")
        elif "catalog_version is 0" in text:
            add("Determine why the local fetch/publish stage produced no catalog_version; do not push an empty collection to the cloud.")
        else:
            add("Check the pushSync cloud function logs, network connectivity, HNREADER_CLOUD_SYNC_URL, and HNREADER_CLOUD_SYNC_SECRET.")
            add("Confirm the cloud still keeps the last successful version; after fixing, just let the next round retry automatically.")
        add("Do not manually clear the cloud collections; the system uses version protection to avoid a rollback overwrite after a restart.")
    elif event_type == "ingest_timeout":
        add("Check whether the child process is stuck on an AI request, HN request, or cloud sync request; locate the longest-running stage first.")
        add("If timeouts are frequent, increase HNREADER_INGEST_ROUND_TIMEOUT_SECONDS, or reduce per-round candidate volume / AI work.")
        add("Confirm the launcher.sh-managed services are still running; if the process has exited, restart the services to continue the next round.")
    elif event_type == "ingest_child_start_failed":
        add("Check the Python executable path, working-directory permissions, and environment variables; confirm launcher.sh/systemd uses the same deployment directory.")
        add("Inspect the original Popen/OSError exception in the supervisor log; after fixing, restart the launcher.sh-managed services.")
    elif event_type == "ingest_child_failed":
        add("Inspect the exit code and the child's last log section; if it is a configuration error, fix the environment variables before restarting.")
        add("Confirm the failure is not caused by a partial round; partial published content should not be treated as a child crash.")
    elif event_type == "ingest_failed":
        add("Inspect the first exception in the stack trace; prioritize fixing configuration, database permission, or external API errors.")
        add("After fixing, restart the launcher.sh-managed services and the system will continue the next round from local state.")
    elif event_type == "fetch_failed":
        add("Check Hacker News API connectivity, the local network/DNS, and HNREADER_HN_REQUEST_TIMEOUT_SECONDS.")
        add("If there are no candidates for several consecutive rounds, check the fetch window, filter conditions, and database write state.")
    elif event_type in {"enrich_timeout", "enrich_incomplete"}:
        if _has_ai_http_code(text, 402):
            add("Per HTTP 402, log in to the AI provider console and confirm balance, billing, and quota; switch to a backup provider/model with balance if needed.")
        if _has_ai_http_code(text, 429):
            add("Per HTTP 429, check the provider's rate-limit policy, concurrency cap, and Retry-After; reduce per-round AI concurrency or switch to a backup model if needed.")
        if any(500 <= code <= 599 for code in _ai_http_error_codes(text)):
            add("Per HTTP 5xx, check the provider status page and the network path; if a backup provider/model is available, switch to it to restore enrichment.")
        add("Check the AI provider's latency, rate limiting, balance, and recent AI errors.")
        add("If truncation or timeouts are frequent, reduce input length / candidate count, or adjust the AI output cap and request timeout.")
    elif event_type == "publish_failed":
        add("Check candidate status, the publish summary, and digest inputs to confirm whether all candidates lack a publishable summary.")
        add("Do not clear the database; after fixing the candidate/AI issue, let the next round publish again.")
    elif event_type == "digest_failed":
        add("Check the digest generation log and AI configuration; the main list may already be published, so confirm first whether the digest page needs a manual re-run.")
    elif event_type == "cleanup_failed":
        add("Check the database directory permissions, disk space, and the cleanup stack trace; when the main pipeline succeeds, the cleanup can be fixed separately later.")
    else:
        add("Inspect the key context fields to confirm whether the failure affects the published version or only auxiliary tasks.")

    if "truncated" in text or "max_tokens" in text:
        add("When AI output is truncated, shorten the input or raise the output cap first, to avoid repeatedly generating invalid JSON.")
    if "invalid batch enrich json" in text or "malformed json" in text:
        add("When AI JSON is malformed, check the model output constraints and the recent provider response; reduce the batch size if needed.")
    if (
        "ssl" in text
        or "eof" in text
        or "connectionreseterror" in text
        or "remote end closed" in text
        or "connection reset" in text
    ):
        add("For a network/TLS interruption, check the local network, DNS, proxy, cloud function access logs, and upstream API availability.")
    if "timed out" in text:
        add("For timeout-class issues, first confirm the external API latency, then decide whether to raise the timeout or reduce per-round work.")

    return actions


def _format_reason(
    event_type: str,
    message: str,
    fields: Optional[Mapping[str, Any]],
) -> str:
    text = _text_blob(message, fields)
    reasons: list[str] = []

    timeout_seconds = (fields or {}).get("timeout_seconds")
    if event_type == "ingest_timeout":
        if timeout_seconds is not None:
            reasons.append(
                f"The ingest child process ran longer than {_format_seconds(timeout_seconds)} s "
                "without finishing and timed out; the system terminated this round to avoid a long-running hang."
            )
        else:
            reasons.append("The ingest child process timed out without finishing; the system terminated this round.")
    elif event_type == "ingest_child_start_failed":
        reasons.append("The supervisor could not start the ingest child process, usually a Python executable, permission, working-directory, or environment-variable issue.")
    elif event_type == "enrich_timeout":
        reasons.append("AI enrichment timed out: it exceeded the time available this round; remaining candidates were released to await a later retry.")
    elif event_type == "enrich_incomplete":
        reasons.append("Some candidates did not finish AI enrichment, so only already-prepared content was published this round.")
    elif event_type == "fetch_failed":
        reasons.append("The HN fetch stage produced no publishable candidates.")
    elif event_type == "publish_failed":
        reasons.append("The publish stage produced no visible content; check candidate status and the publish summary.")
    elif event_type == "digest_failed":
        reasons.append("Daily digest generation or validation failed; news visibility this round may be unaffected, but the digest needs checking.")
    elif event_type == "cleanup_failed":
        reasons.append("The background cleanup task failed; the core fetch may have completed, but historical-data cleanup needs checking.")
    elif event_type == "ingest_child_failed":
        reasons.append("The ingest child process exited abnormally; inspect the exit code and the most recent error.")
    elif event_type == "ingest_failed":
        reasons.append("The ingest pipeline raised an unhandled exception and did not complete normally this round.")
    elif event_type == "cloud_sync_deferred":
        reasons.append("Insufficient time remained this round, so the system proactively skipped cloud sync to avoid being killed by the supervisor mid-push.")
    elif event_type == "cloud_sync_warning":
        if "dashboard" in text:
            reasons.append("After cloud business publish completed, the dashboard projection write failed or was skipped.")
        elif "record write failed" in text:
            reasons.append("Cloud business publish may have succeeded, but the local cloud_sync_runs record write failed.")
        else:
            reasons.append("Cloud sync partially completed but its status was degraded; reconcile the local record and the cloud result.")
    elif event_type == "cloud_sync_failed":
        if "refusing rollback" in text:
            reasons.append("The local version is behind the last successful cloud version, so the system refused to roll back and overwrite the cloud database.")
        elif "catalog_version is 0" in text:
            reasons.append("There is no publishable local catalog_version, so the system refused to push an empty version to the cloud.")
        else:
            reasons.append("Cloud business publish did not complete, so the cloud database will not switch to a new version this round.")

    codes = _ai_http_error_codes(text)
    if 402 in codes:
        reasons.append("The most recent AI error code is HTTP 402, which usually means the corresponding provider/model balance, quota, or billing status is unavailable.")
    if 429 in codes:
        reasons.append("The most recent AI error code is HTTP 429, which means the corresponding provider/model is rate-limiting.")
    if any(500 <= code <= 599 for code in codes):
        reasons.append("The most recent AI error code is HTTP 5xx, which means the corresponding provider/model or an upstream service is temporarily unavailable.")
    if "truncated" in text or "max_tokens" in text:
        reasons.append("The most recent AI error shows the AI output was truncated; the output cap may need raising or the input shortening.")
    if "invalid batch enrich json" in text or "malformed json" in text:
        reasons.append("The JSON returned by the AI service did not match expectations; the system already tried splitting or retrying.")
    if (
        "ssl" in text
        or "eof" in text
        or "connectionreseterror" in text
        or "remote end closed" in text
        or "connection reset" in text
    ):
        reasons.append("Recent requests hit a network/TLS connection interruption, possibly an unstable upstream API or local network.")
    if "timed out" in text and not any("timed out" in reason.lower() for reason in reasons):
        reasons.append("A request or processing step timed out.")

    if not reasons:
        fallback = message.strip() or "The alert event has no specific error detail; review the key context."
        reasons.append(fallback)

    deduped = list(dict.fromkeys(reasons))
    return "; ".join(deduped)


def _field_label(key: str) -> str:
    return _FIELD_LABELS.get(key, key)


def _format_field_value(key: str, value: Any) -> str:
    if value is None:
        return ""
    if key in {
        "elapsed_seconds",
        "timeout_seconds",
        "cloud_sync_elapsed_seconds",
        "cloud_sync_timeout_seconds",
        "ingest_interval_seconds",
        "ingest_round_timeout_seconds",
    }:
        return f"{_format_seconds(value)} s"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ""
        if stripped[:1] in "[{":
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return stripped
            return json.dumps(parsed, ensure_ascii=False, sort_keys=True, default=str)
        return stripped
    return str(value)


def _format_field_lines(fields: Optional[Mapping[str, Any]]) -> list[str]:
    if not fields:
        return []
    priority = {
        "run_id": 0,
        "cloud_sync_status": 1,
        "cloud_sync_version": 2,
        "cloud_sync_error": 3,
        "timeout_seconds": 4,
        "elapsed_seconds": 5,
        "cloud_sync_elapsed_seconds": 6,
        "cloud_sync_timeout_seconds": 7,
        "ingest_round_timeout_seconds": 8,
        "ingest_interval_seconds": 9,
        "candidate_count": 10,
        "target_count": 11,
        "ai_provider": 12,
        "ai_model": 13,
        "recent_enrich_errors": 14,
    }
    lines: list[str] = []
    for key in sorted(fields, key=lambda item: (priority.get(item, 100), item)):
        value = _format_field_value(key, fields[key])
        if value == "":
            continue
        lines.append(f"- {_field_label(key)}: {value}")
    return lines


def _format_body(
    event_type: str,
    subject: str,
    message: str,
    fields: Optional[Mapping[str, Any]],
) -> str:
    title = _alert_title(event_type, subject)
    actions = _recommendations(event_type, message, fields)
    error_codes = _ai_http_error_codes(_text_blob(message, fields))
    lines = [
        f"Conclusion: bad ({title})",
        f"Status: {_status_label(event_type, message, fields)}",
        f"Severity: {_severity_label(event_type, message, fields)}",
    ]
    if error_codes:
        lines.append(
            "AI error codes: " + ", ".join(f"HTTP {code}" for code in error_codes)
        )
    lines.extend(
        [
            f"Impact: {_impact_summary(event_type, message, fields)}",
            "Intent: notify the admin that the HN fetch / AI enrichment pipeline did not complete normally and needs review.",
            f"Cause: {_format_reason(event_type, message, fields)}",
            f"Recovery expectation: {_recovery_summary(event_type, message, fields)}",
        ]
    )
    if actions:
        lines.extend(["", "Recommended actions:"])
        lines.extend(f"{idx}. {action}" for idx, action in enumerate(actions, start=1))
    if message.strip():
        lines.extend(["", f"Raw error: {message.strip()}"])

    field_lines = _format_field_lines(fields)
    if field_lines:
        lines.extend(["", "Key context:", *field_lines])

    lines.extend(
        [
            "",
            "Note: this email is sent only on error, degradation, or deferral; rounds that complete successfully do not send an alert of this type.",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _alert_record(
    now: int,
    event_type: str,
    subject: str,
    message: str,
    fields: Optional[Mapping[str, Any]],
) -> dict:
    return {
        "created_at": int(now),
        "event_type": str(event_type),
        "subject": str(subject),
        "message": str(message),
        "fields": dict(fields or {}),
    }


def _alert_outbox_limit() -> int:
    return max(1, int(settings.ALERT_OUTBOX_MAX_RECORDS))


def _alert_sending_path(path: Path) -> Path:
    return path.with_name(path.name + ".sending")


def _alert_sending_lease_path(sending_path: Path) -> Path:
    return sending_path.with_name(sending_path.name + ".lease")


def _path_age_seconds(path: Path, *, now: Optional[int] = None) -> Optional[float]:
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return None
    except OSError as exc:
        log.warning("failed to stat alert sending outbox %s: %s", path, exc)
        return None
    checked_at = int(now if now is not None else repository.now_seconds())
    return checked_at - mtime


def _process_is_alive(pid: int) -> Optional[bool]:
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, False, int(pid))
            if handle:
                kernel32.CloseHandle(handle)
                return True
            # ERROR_INVALID_PARAMETER means the PID no longer exists. Access
            # denied means a protected process exists but cannot be queried.
            err = kernel32.GetLastError()
            if err == 87:
                return False
            if err == 5:
                return True
            return None
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to inspect process %s: %s", pid, exc)
            return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        log.warning("failed to inspect process %s: %s", pid, exc)
        return None
    return True


def _read_alert_sending_lease(sending_path: Path) -> Optional[dict]:
    lease_path = _alert_sending_lease_path(sending_path)
    try:
        raw = lease_path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("failed to read alert sending lease %s: %s", lease_path, exc)
        return None
    return data if isinstance(data, dict) else None


def _write_alert_sending_lease(sending_path: Path) -> None:
    lease_path = _alert_sending_lease_path(sending_path)
    payload = {
        "pid": os.getpid(),
        "claimed_at": repository.now_seconds(),
    }
    try:
        tmp = lease_path.with_name(lease_path.name + ".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        tmp.replace(lease_path)
    except OSError as exc:
        log.warning("failed to write alert sending lease %s: %s", lease_path, exc)


def _clear_alert_sending_lease(sending_path: Path) -> None:
    lease_path = _alert_sending_lease_path(sending_path)
    try:
        if lease_path.exists():
            lease_path.unlink()
    except OSError as exc:
        log.warning("failed to clear alert sending lease %s: %s", lease_path, exc)


def _sending_outbox_is_recoverable(
    sending_path: Path,
    *,
    now: Optional[int] = None,
) -> bool:
    lease = _read_alert_sending_lease(sending_path)
    age = _path_age_seconds(sending_path, now=now)
    if lease is None:
        return age is not None and age >= _ALERT_SENDING_ORPHAN_SECONDS

    try:
        pid = int(lease.get("pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    alive = _process_is_alive(pid)
    if alive is False:
        return True
    if alive is True:
        return age is not None and age >= _ALERT_SENDING_STALE_SECONDS
    return age is not None and age >= _ALERT_SENDING_STALE_SECONDS


def _write_alert_record(fh, record: Mapping[str, Any]) -> None:
    json.dump(record, fh, ensure_ascii=False, sort_keys=True, default=str)
    fh.write("\n")


def _trim_alert_outbox(path: Path) -> None:
    if not path.exists():
        return
    max_records = _alert_outbox_limit()
    kept = deque(maxlen=max_records)
    total = 0
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                total += 1
                kept.append(line if line.endswith("\n") else line + "\n")
        if total <= max_records:
            return
        tmp = path.with_name(path.name + ".trim")
        with tmp.open("w", encoding="utf-8") as fh:
            fh.writelines(kept)
        tmp.replace(path)
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to trim alert outbox %s: %s", path, exc)


def _append_alert_outbox_records(records: Iterable[Mapping[str, Any]]) -> bool:
    path = settings.get_alert_outbox_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            for record in records:
                _write_alert_record(fh, record)
        _trim_alert_outbox(path)
        return True
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to write alert outbox %s: %s", path, exc)
        return False


def _append_alert_outbox(record: Mapping[str, Any]) -> bool:
    return _append_alert_outbox_records([record])


def _record_created_at(record: Mapping[str, Any]) -> int:
    try:
        return int(record.get("created_at") or 0)
    except (TypeError, ValueError):
        return 0


def _replace_alert_outbox_records(
    path: Path,
    records: Sequence[Mapping[str, Any]],
) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        kept = sorted(records, key=_record_created_at)[-_alert_outbox_limit() :]
        tmp = path.with_name(path.name + ".recover")
        with tmp.open("w", encoding="utf-8") as fh:
            for record in kept:
                _write_alert_record(fh, record)
        tmp.replace(path)
        return True
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to recover alert outbox %s: %s", path, exc)
        return False


def _recover_stale_sending_outbox(path: Path) -> None:
    sending = _alert_sending_path(path)
    if not sending.exists() or not _sending_outbox_is_recoverable(sending):
        return

    if path.exists():
        records = [*_read_alert_outbox(sending), *_read_alert_outbox(path)]
        if records and not _replace_alert_outbox_records(path, records):
            return
        _clear_alert_outbox(sending)
        _clear_alert_sending_lease(sending)
        log.warning("recovered stale alert sending outbox into %s", path)
        return

    try:
        sending.rename(path)
        _clear_alert_sending_lease(sending)
        log.warning("recovered stale alert sending outbox %s", sending)
    except FileNotFoundError:
        return
    except FileExistsError:
        return
    except Exception as exc:  # noqa: BLE001
        log.exception(
            "failed to recover stale alert sending outbox %s: %s",
            sending,
            exc,
        )


def _claim_alert_outbox() -> Optional[Path]:
    path = settings.get_alert_outbox_path()
    _recover_stale_sending_outbox(path)
    if not path.exists():
        return None
    sending = _alert_sending_path(path)
    if sending.exists():
        log.warning("alert outbox is already being sent: %s", sending)
        return None
    try:
        path.rename(sending)
        try:
            os.utime(sending, None)
        except OSError:
            pass
        _write_alert_sending_lease(sending)
        return sending
    except FileNotFoundError:
        return None
    except FileExistsError:
        log.warning("alert outbox is already being sent: %s", sending)
        return None
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to claim alert outbox %s: %s", path, exc)
        return None


def _read_alert_outbox(path: Optional[Path] = None) -> list[dict]:
    if path is None:
        path = settings.get_alert_outbox_path()
    if not path.exists():
        return []
    records = deque(maxlen=_alert_outbox_limit())
    try:
        with path.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.rstrip("\n")
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    data = {
                        "created_at": 0,
                        "event_type": "alert_outbox_parse_error",
                        "subject": "Unreadable alert outbox line",
                        "message": line,
                        "fields": {},
                    }
                if isinstance(data, dict):
                    records.append(data)
        return list(records)
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to read alert outbox %s: %s", path, exc)
        return []


def _clear_alert_outbox(path: Optional[Path] = None) -> None:
    if path is None:
        path = settings.get_alert_outbox_path()
    try:
        if path.exists():
            path.unlink()
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to clear alert outbox %s: %s", path, exc)


def _format_outbox(records: Sequence[Mapping[str, Any]]) -> str:
    if not records:
        return ""
    lines = ["", "Local alerts pending re-send:"]
    for record in records:
        fields = record.get("fields") or {}
        event_type = str(record.get("event_type", "unknown"))
        title = _alert_title(event_type, str(record.get("subject", "")))
        lines.append(
            "- {title} ({event_type}, created_at={created_at}): {message}".format(
                title=title,
                event_type=event_type,
                created_at=record.get("created_at", ""),
                message=record.get("message", ""),
            )
        )
        lines.extend("  " + line for line in _format_field_lines(fields))
    return "\n".join(lines)


def validate_admin_alert_config() -> None:
    if not settings.ADMIN_EMAIL_ENABLED:
        raise RuntimeError(
            "HNREADER_ADMIN_EMAIL_ENABLED must be true for the ingest process"
        )

    missing = []
    if not settings.ADMIN_EMAIL_TO:
        missing.append("HNREADER_ADMIN_EMAIL_TO")
    if not settings.SMTP_HOST:
        missing.append("HNREADER_SMTP_HOST")
    if settings.SMTP_USERNAME and not settings.SMTP_PASSWORD:
        missing.append("HNREADER_SMTP_PASSWORD")
    if missing:
        raise RuntimeError(
            "admin alert email is not fully configured: " + ", ".join(missing)
        )


def send_admin_alert(
    event_type: str,
    subject: str,
    message: str,
    *,
    fields: Optional[Mapping[str, Any]] = None,
) -> bool:
    """Send a rate-limited admin email.

    Returns True only when an email was actually sent. Disabled or
    misconfigured alerts are logged and treated as non-fatal.

    The cooldown is consumed atomically up-front via
    :func:`_claim_cooldown_slot`. Once a caller wins the claim, the slot is
    burned even if the subsequent send / outbox write fails — that matches
    the original "mark sent on failure" behavior and prevents repeated
    attempts to spam SMTP / disk after a known-broken state. There is no
    longer a separate post-send mark step.
    """
    now = repository.now_seconds()
    if not _claim_cooldown_slot(event_type, now):
        log.info("alert %s suppressed by cooldown", event_type)
        return False

    record = _alert_record(now, event_type, subject, message, fields)
    if not settings.ADMIN_EMAIL_ENABLED:
        log.warning("alert %s: %s; fields=%s", event_type, message, fields or {})
        _append_alert_outbox(record)
        return False

    if not settings.ADMIN_EMAIL_TO or not settings.SMTP_HOST:
        log.warning(
            "alert %s not sent because admin email is not fully configured",
            event_type,
        )
        _append_alert_outbox(record)
        return False

    claimed_outbox = _claim_alert_outbox()
    pending = _read_alert_outbox(claimed_outbox) if claimed_outbox is not None else []
    sender = settings.SMTP_FROM or settings.SMTP_USERNAME or settings.ADMIN_EMAIL_TO
    msg = EmailMessage()
    msg["Subject"] = _format_subject(event_type, subject)
    msg["From"] = sender
    msg["To"] = settings.ADMIN_EMAIL_TO
    msg.set_content(
        _format_body(event_type, subject, message, fields) + _format_outbox(pending)
    )

    try:
        smtp_cls = smtplib.SMTP_SSL if settings.SMTP_SSL else smtplib.SMTP
        with smtp_cls(settings.SMTP_HOST, settings.SMTP_PORT, timeout=15) as smtp:
            if settings.SMTP_STARTTLS and not settings.SMTP_SSL:
                smtp.starttls()
            if settings.SMTP_USERNAME:
                smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
            smtp.send_message(msg)
    except Exception as exc:  # noqa: BLE001
        log.exception("alert %s delivery failed: %s", event_type, exc)
        if pending:
            _append_alert_outbox_records(pending)
        if claimed_outbox is not None:
            _clear_alert_outbox(claimed_outbox)
            _clear_alert_sending_lease(claimed_outbox)
        _append_alert_outbox(record)
        return False

    if claimed_outbox is not None:
        _clear_alert_outbox(claimed_outbox)
        _clear_alert_sending_lease(claimed_outbox)
    log.info("alert %s sent to %s", event_type, settings.ADMIN_EMAIL_TO)
    return True


__all__ = ["send_admin_alert", "validate_admin_alert_config"]
