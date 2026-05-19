"""Push the read model to cloud DB via pushSync.

Business publish (:func:`push_read_model`):
    1. ping          - connectivity + signature liveness check
    2. writeBatch    - write topics / digests / stories in byte-limited
                       batches
    3. switchMeta    - atomically switch meta.currentVersion (the mini program
                       can read the new version from this point on)

Dashboard publish (:func:`push_dashboard`, called independently):
    1. writeDashboard - write summary + recent ingest runs + recent cloud sync
                        runs in byte-limited batches

Cleanup (:func:`cleanup_old`, after the business publish):
    cleanupOld - delete old data where syncVersion NOT IN [current, previous],
                 covering the stories / topics / hn_dashboard_* run collections.

Why they are split: the business publish and the dashboard publish are
decoupled so that:
    - the dashboard summary never overwrites the cloud copy before the
      business switchMeta (when a business push half-fails, the cloud summary
      does not "get ahead of" the business collections)
    - the dashboard projection can read this round's final cloud_sync_runs
      state instead of always lagging one round behind
    - when the dashboard publish fails, the local cloud_sync_runs is marked
      warning while business reads are unaffected

Module entry point::

    from server.cloud_push import push_read_model, push_dashboard, CloudPushError
    business_stats = push_read_model(url=..., secret=..., batch_size=20)
    dashboard_stats = push_dashboard(url=..., secret=..., sync_version=v)

CLI entry point (reads credentials from environment variables)::

    python -m server.cloud_push

Env vars (required, for CLI):
    HNREADER_CLOUD_PUSH_URL     HTTP trigger URL for pushSync
    HNREADER_CLOUD_PUSH_SECRET  shared HMAC secret (must match the cloud
                                function's PUSH_SECRET)

Env vars (optional, for CLI):
    HNREADER_CLOUD_PUSH_BATCH_SIZE / HNREADER_CLOUD_BATCH_SIZE  stories per
                                batch, default 20
    HNREADER_CLOUD_PUSH_MAX_BODY_BYTES  max writeBatch request body size,
                                default 80000
"""
from __future__ import annotations

import hashlib
import hmac
import http.client
import ipaddress
import json
import logging
import os
import re
import secrets
import socket
import ssl
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import dashboard_projection
from .cloud_sync import default_output_dir
from .http_client import PROBE_RESPONSE_MAX_BYTES, ResponseTooLargeError, read_limited
from .logging_config import configure_logging


log = logging.getLogger(__name__)

TS_TOLERANCE_MS = 60 * 1000
DEFAULT_TIMEOUT_SECONDS = 120
# CloudBase documents a 100KB limit for text cloud-function request bodies.
# Keep headroom for JSON formatting and gateway accounting instead of riding
# the exact edge.
DEFAULT_WRITE_BATCH_MAX_BODY_BYTES = 80_000
DEFAULT_WRITE_BATCH_MAX_ATTEMPTS = 3
_WRITE_BATCH_RETRY_DELAYS_SECONDS = (1.0, 2.0)
# Minimum wall-time budget required before starting a single HTTP call.
# A push with a deadline_at must abort cleanly at a phase boundary rather
# than start a call that has nowhere near enough time to complete.
_MIN_PER_CALL_SECONDS = 10


def _console_text(text: str, stream) -> str:
    encoding = getattr(stream, "encoding", None) or "utf-8"
    try:
        text.encode(encoding)
        return text
    except LookupError:
        return text
    except UnicodeEncodeError:
        return text.encode(encoding, errors="backslashreplace").decode(encoding)


def _print_console(text: str, *, file=None) -> None:
    stream = file or sys.stdout
    print(_console_text(str(text), stream), file=stream)


class CloudPushError(RuntimeError):
    """Raised when push_read_model fails at any of ping / writeBatch / switchMeta.

    A cleanupOld failure is not fatal; it only logs a warning."""


class CloudPushUrlError(CloudPushError):
    """Raised when ``HNREADER_CLOUD_PUSH_URL`` fails validation.

    In the sync-only architecture the push URL is the critical outbound path
    the server initiates, so any unsafe target (loopback / private /
    link-local / cloud metadata / non-https / carrying userinfo / carrying a
    query) must be rejected immediately. This prevents the read model and the
    HMAC signature from being sent to an unintended address, or a temporary
    token leaking into the journal via the query string.
    """


class CloudPushSecretError(CloudPushError):
    """``HNREADER_CLOUD_PUSH_SECRET`` is missing or too weak."""


def _redacted_url_for_log(url: str) -> str:
    """Return ``scheme://host[:port]/path`` for logging; drop userinfo / query / fragment.

    Logs go to journald in production and end up indexed downstream. Even
    after ``validate_cloud_push_url`` rejects userinfo/query/fragment, some
    callers log URLs before validation (or with intermediate variants). Be
    defensive: strip anything that isn't structural so a journal scrape never
    bleeds credentials a future regression might let through.

    Malformed ports (``https://host:bad/x``) make ``parsed.port`` raise
    ``ValueError``; treat that as "unsafe to log" rather than crashing the
    caller — a logging helper must never become the source of an outage.
    """
    try:
        parsed = urllib.parse.urlsplit(url)
    except ValueError:
        return "<malformed-url>"
    host = (parsed.hostname or "").strip()
    if not host:
        return "<malformed-url>"
    try:
        port = parsed.port
    except ValueError:
        return "<malformed-url>"
    netloc = f"{host}:{port}" if port else host
    return urllib.parse.urlunsplit(
        (parsed.scheme, netloc, parsed.path, "", "")
    )


# Outbound targets blocked by default: outbound is only allowed to public
# HTTPS, never to the internal network or a cloud metadata service.
_BLOCKED_HOSTNAMES = frozenset({
    "localhost",
    "ip6-localhost",
    "ip6-loopback",
    "metadata",  # short name of the GCP / OCI metadata service
    "metadata.google.internal",
    "metadata.goog",
})

_CGNAT_IPV4_NETWORK = ipaddress.ip_network("100.64.0.0/10")
_CLOUD_PUSH_SECRET_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def _resolve_cloud_push_host(host: str) -> str:
    """Resolve ``host`` and return one allowed address, failing closed on any deny."""
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except (socket.gaierror, socket.herror, OSError) as exc:
        raise CloudPushUrlError(
            f"CLOUD_PUSH_URL host {host!r} could not be resolved: {exc}"
        ) from exc
    pinned_ip: Optional[str] = None
    for info in infos:
        sockaddr = info[4]
        if not sockaddr:
            continue
        addr = str(sockaddr[0])
        try:
            ip = ipaddress.ip_address(addr.split("%", 1)[0])
        except ValueError:
            continue
        if _is_blocked_ip(ip):
            raise CloudPushUrlError(
                f"CLOUD_PUSH_URL host {host!r} resolves to disallowed address {addr}"
            )
        if pinned_ip is None:
            pinned_ip = addr.split("%", 1)[0]
    if pinned_ip is None:
        raise CloudPushUrlError(
            f"CLOUD_PUSH_URL host {host!r} did not resolve to an IP address"
        )
    return pinned_ip


def _is_blocked_ip(ip: ipaddress._BaseAddress) -> bool:
    if isinstance(ip, ipaddress.IPv4Address) and ip in _CGNAT_IPV4_NETWORK:
        return True
    if not ip.is_global:
        return True
    if isinstance(ip, ipaddress.IPv4Address):
        # AWS / Azure / OCI / DigitalOcean / Alicloud and others all expose
        # instance metadata on 169.254.169.254. ipaddress.is_link_local
        # already covers it, but keep the explicit note.
        if ip == ipaddress.IPv4Address("169.254.169.254"):
            return True
    if isinstance(ip, ipaddress.IPv6Address):
        if ip == ipaddress.IPv6Address("fd00:ec2::254"):  # AWS IMDS over IPv6
            return True
    return False


def _validate_cloud_push_url_with_pinned_ip(url: str) -> Tuple[str, str]:
    """Reject any push URL that could become an SSRF or credential-bleed risk.

    Returns the URL on success. Raises :class:`CloudPushUrlError` otherwise so
    callers fail fast rather than silently fall through to ``urllib`` (which
    happily resolves and posts to ``http://169.254.169.254`` etc.).
    """
    if not isinstance(url, str) or not url.strip():
        raise CloudPushUrlError("CLOUD_PUSH_URL is empty")
    clean = url.strip()
    parsed = urllib.parse.urlsplit(clean)
    if parsed.scheme.lower() != "https":
        raise CloudPushUrlError(
            f"CLOUD_PUSH_URL must use https (got scheme={parsed.scheme!r})"
        )
    if parsed.username or parsed.password:
        # userinfo in the URL would be signed and sent along with the
        # request; no credentials should ever be put here.
        raise CloudPushUrlError(
            "CLOUD_PUSH_URL must not include userinfo (user:pass@host)"
        )
    if parsed.fragment:
        raise CloudPushUrlError("CLOUD_PUSH_URL must not contain a fragment")
    if parsed.query:
        # A cloud function trigger URL should not carry a query; operators
        # sometimes stuff a temporary token into the query (?token=...),
        # which would be written verbatim into the journal/log and cause an
        # unexpected credential leak. If authentication is genuinely needed,
        # put it in the signed headers, not the URL.
        raise CloudPushUrlError(
            "CLOUD_PUSH_URL must not contain a query string"
        )
    try:
        # ``urllib.parse`` lazily validates the port: ``urlsplit`` accepts
        # ``https://host:bad/path``, but the first ``parsed.port`` access
        # raises ValueError. Trigger that here so callers fail with a
        # CloudPushUrlError before ever reaching the HTTP request.
        parsed.port  # type: ignore[union-attr]
    except ValueError as exc:
        raise CloudPushUrlError(
            f"CLOUD_PUSH_URL has invalid port: {exc}"
        ) from exc
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise CloudPushUrlError("CLOUD_PUSH_URL is missing a host")
    if host in _BLOCKED_HOSTNAMES:
        raise CloudPushUrlError(
            f"CLOUD_PUSH_URL host {host!r} is on the blocked-host list"
        )
    try:
        literal_ip = ipaddress.ip_address(host)
    except ValueError:
        literal_ip = None
    if literal_ip is not None:
        if _is_blocked_ip(literal_ip):
            raise CloudPushUrlError(
                f"CLOUD_PUSH_URL points to disallowed address {host}"
            )
        pinned_ip = str(literal_ip)
    else:
        pinned_ip = _resolve_cloud_push_host(host)
    return clean, pinned_ip


def validate_cloud_push_url(url: str) -> str:
    """Reject any push URL that could become an SSRF or credential-bleed risk."""
    clean, _pinned_ip = _validate_cloud_push_url_with_pinned_ip(url)
    return clean


def validate_cloud_push_secret(secret: str) -> str:
    """Require the documented 32-byte random HMAC key encoded as 64 hex chars."""
    if not isinstance(secret, str) or not secret.strip():
        raise CloudPushSecretError("CLOUD_PUSH_SECRET is empty")
    clean = secret.strip()
    if not _CLOUD_PUSH_SECRET_RE.fullmatch(clean):
        raise CloudPushSecretError(
            "CLOUD_PUSH_SECRET must be 64 hexadecimal characters "
            "(generate with: python -c \"import secrets; print(secrets.token_hex(32))\")"
        )
    return clean


def _budget_remaining(deadline_at: Optional[float]) -> Optional[float]:
    if deadline_at is None:
        return None
    return float(deadline_at) - time.time()


def _next_call_timeout(
    deadline_at: Optional[float], configured: int
) -> int:
    """Per-call HTTP timeout clamped to the remaining wall-time budget.

    Floored at ``_MIN_PER_CALL_SECONDS`` so we never pass a useless
    1-second timeout into urllib; the caller must check budget via
    :func:`_abort_if_insufficient` before invoking the call.
    """
    remaining = _budget_remaining(deadline_at)
    if remaining is None:
        return max(_MIN_PER_CALL_SECONDS, configured)
    return max(_MIN_PER_CALL_SECONDS, min(configured, int(remaining)))


def _abort_if_insufficient(
    deadline_at: Optional[float],
    *,
    phase: str,
    min_required: int = _MIN_PER_CALL_SECONDS,
) -> None:
    """Raise :class:`CloudPushError` if too little wall time remains.

    Called at every phase boundary so the supervisor never kills us
    mid-HTTP-call: we surrender cleanly before starting a call we
    cannot finish.
    """
    remaining = _budget_remaining(deadline_at)
    if remaining is None:
        return
    if remaining < min_required:
        raise CloudPushError(
            f"deadline reached before {phase}: "
            f"{remaining:.1f}s remain, need >={min_required}s"
        )


def _require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        _print_console(f"[push] env var {name} is required", file=sys.stderr)
        sys.exit(2)
    return v


def _sign(secret: str, body_bytes: bytes) -> tuple[str, str, str]:
    ts = str(int(time.time() * 1000))
    nonce = secrets.token_hex(16)
    body_hash = hashlib.sha256(body_bytes).hexdigest()
    sig = hmac.new(
        secret.encode("utf-8"),
        f"{ts}.{nonce}.{body_hash}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return ts, nonce, sig


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """Connect to a validated IP while verifying the original hostname."""

    def __init__(
        self,
        host: str,
        *,
        pinned_ip: str,
        port: int,
        timeout: int,
    ) -> None:
        super().__init__(
            host,
            port=port,
            timeout=timeout,
            context=ssl.create_default_context(),
        )
        self._pinned_ip = pinned_ip

    def connect(self) -> None:
        sock = socket.create_connection(
            (self._pinned_ip, self.port),
            self.timeout,
            self.source_address,
        )
        try:
            self.sock = self._context.wrap_socket(sock, server_hostname=self.host)
        except Exception:
            sock.close()
            raise


def _host_header_value(parsed: urllib.parse.SplitResult) -> str:
    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = parsed.port
    if port is not None and port != 443:
        return f"{host}:{port}"
    return host


def _request_target(parsed: urllib.parse.SplitResult) -> str:
    path = parsed.path or "/"
    if parsed.query:
        return f"{path}?{parsed.query}"
    return path


def _payload_json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _payload_size_bytes(payload: dict) -> int:
    return len(_payload_json_bytes(payload))


def _is_retryable_write_batch_response(response: dict) -> bool:
    if response.get("ok"):
        return False
    error = str(response.get("error") or response.get("raw") or "").lower()
    status = int(response.get("statusCode") or 0)
    if status in {502, 503, 504}:
        return True
    if "network error" in error:
        return True
    transient_markers = (
        "timeout",
        "timed out",
        "etimedout",
        "econnreset",
        "econnaborted",
        "temporarily unavailable",
        "resource system error",
    )
    return any(marker in error for marker in transient_markers)


def _write_batch_retry_delay(attempt_index: int) -> float:
    if attempt_index < len(_WRITE_BATCH_RETRY_DELAYS_SECONDS):
        return _WRITE_BATCH_RETRY_DELAYS_SECONDS[attempt_index]
    return _WRITE_BATCH_RETRY_DELAYS_SECONDS[-1]


def _post(url: str, secret: str, payload: dict, *, timeout: int) -> dict:
    try:
        url, pinned_ip = _validate_cloud_push_url_with_pinned_ip(url)
    except CloudPushUrlError as exc:
        return {"ok": False, "error": str(exc)}

    body_bytes = _payload_json_bytes(payload)
    ts, nonce, sig = _sign(secret, body_bytes)
    parsed = urllib.parse.urlsplit(url)
    conn: Optional[_PinnedHTTPSConnection] = None
    try:
        conn = _PinnedHTTPSConnection(
            parsed.hostname or "",
            pinned_ip=pinned_ip,
            port=parsed.port or 443,
            timeout=timeout,
        )
        conn.request(
            "POST",
            _request_target(parsed),
            body=body_bytes,
            headers={
                "Host": _host_header_value(parsed),
                "Content-Type": "application/json; charset=utf-8",
                "x-push-ts": ts,
                "x-push-nonce": nonce,
                "x-push-signature": sig,
            },
        )
        resp = conn.getresponse()
        raw = read_limited(resp, PROBE_RESPONSE_MAX_BYTES).decode(
            "utf-8", errors="replace"
        )
        status = resp.status
    except ResponseTooLargeError as e:
        return {"ok": False, "error": f"response too large: {e}"}
    except (
        TimeoutError,
        socket.timeout,
        http.client.HTTPException,
        OSError,
    ) as e:
        return {"ok": False, "error": f"network error: {e}"}
    finally:
        if conn is not None:
            conn.close()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"ok": False, "statusCode": status, "raw": raw[:500]}
    if status != 200 and "statusCode" not in data:
        data["statusCode"] = status
    return data


def _load_jsonl(path: Path) -> List[dict]:
    out: List[dict] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _load_required_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        raise CloudPushError(f"required JSONL file missing: {path}")
    try:
        return _load_jsonl(path)
    except (OSError, ValueError) as exc:
        raise CloudPushError(f"required JSONL file unreadable: {path}: {exc}")


def _assert_versioned_doc(doc: dict, version: int, collection: str) -> None:
    doc_id = doc.get("_id") if isinstance(doc, dict) else None
    if not doc_id or not str(doc_id).startswith(f"{version}:"):
        raise CloudPushError(
            f"{collection} doc _id={doc_id!r} not in syncVersion={version}"
        )
    if doc.get("syncVersion") != version:
        raise CloudPushError(
            f"{collection} doc _id={doc_id!r} has syncVersion="
            f"{doc.get('syncVersion')!r}, expected {version}"
        )


def _validate_versioned_docs(
    collection: str, docs: Iterable[dict], version: int
) -> None:
    for doc in docs:
        _assert_versioned_doc(doc, version, collection)


def _load_dashboard_summary(out_dir: Path) -> Dict[str, Any]:
    """Read the dashboard summary doc;raise if missing/corrupt.

    Dashboard publish is its own action — by the time we call it the
    projection must have produced a valid summary. A missing or unreadable
    file means the build step is broken,not "OK, just skip", so we fail
    fast and let the caller record a warning.
    """
    path = out_dir / dashboard_projection.DASHBOARD_SUMMARY_FILE
    if not path.exists():
        raise CloudPushError(f"dashboard summary missing: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CloudPushError(f"dashboard summary unreadable: {path}: {exc}")
    if not isinstance(data, dict):
        raise CloudPushError(f"dashboard summary is not a JSON object: {path}")
    return data


def _chunks(items: List[dict], size: int) -> Iterable[List[dict]]:
    if size <= 0:
        size = len(items)
    for i in range(0, len(items), max(1, size)):
        yield items[i:i + size]


def _write_batch_payload(
    *,
    current_version: int,
    stories: Optional[List[dict]] = None,
    topics: Optional[List[dict]] = None,
    digests: Optional[List[dict]] = None,
    insights: Optional[List[dict]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "action": "writeBatch",
        "syncVersion": current_version,
        "stories": stories or [],
    }
    if topics is not None:
        payload["topics"] = topics
    if digests is not None:
        payload["digests"] = digests
    if insights is not None:
        payload["insights"] = insights
    return payload


def _doc_id_for_error(doc: dict) -> str:
    doc_id = doc.get("_id") if isinstance(doc, dict) else None
    return str(doc_id or "<missing _id>")


def _chunk_write_batch_docs(
    *,
    current_version: int,
    field: str,
    docs: List[dict],
    max_docs: int,
    max_body_bytes: int,
) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []
    chunk: List[dict] = []

    for doc in docs:
        candidate = chunk + [doc]
        candidate_payload = _write_batch_payload(
            current_version=current_version,
            **{field: candidate},
        )
        candidate_size = _payload_size_bytes(candidate_payload)

        if candidate_size <= max_body_bytes and len(candidate) <= max_docs:
            chunk = candidate
            continue

        if chunk:
            payloads.append(
                _write_batch_payload(
                    current_version=current_version,
                    **{field: chunk},
                )
            )
            chunk = []

        single_payload = _write_batch_payload(
            current_version=current_version,
            **{field: [doc]},
        )
        single_size = _payload_size_bytes(single_payload)
        if single_size > max_body_bytes:
            raise CloudPushError(
                f"single {field} doc {_doc_id_for_error(doc)} makes a "
                f"writeBatch payload of {single_size} bytes, exceeding "
                f"HNREADER_CLOUD_PUSH_MAX_BODY_BYTES={max_body_bytes}; "
                "reduce the cloud document size or move this payload through "
                "cloud storage / a finer-grained cloud protocol"
            )
        chunk = [doc]

    if chunk:
        payloads.append(
            _write_batch_payload(
                current_version=current_version,
                **{field: chunk},
            )
        )
    return payloads


def _write_batch_payloads(
    *,
    current_version: int,
    stories: List[dict],
    topics: List[dict],
    digests: List[dict],
    insights: List[dict],
    batch_size: int,
    max_body_bytes: int,
) -> List[Dict[str, Any]]:
    if batch_size <= 0:
        raise CloudPushError(f"batch_size must be >= 1 (got {batch_size})")
    if max_body_bytes <= 0:
        raise CloudPushError(
            f"HNREADER_CLOUD_PUSH_MAX_BODY_BYTES must be >= 1 "
            f"(got {max_body_bytes})"
        )

    payloads: List[Dict[str, Any]] = []
    remaining_stories = list(stories)

    # Preserve the original cloud protocol shape when it fits: the first
    # writeBatch carries topics + digests + insights and as many complete story docs as
    # fit. This avoids truncation while staying compatible with the deployed
    # pushSync implementation that already accepts the old first-batch layout.
    first_story_chunk: List[dict] = []
    first_payload = _write_batch_payload(
        current_version=current_version,
        stories=first_story_chunk,
        topics=topics,
        digests=digests,
        insights=insights,
    )
    if _payload_size_bytes(first_payload) <= max_body_bytes:
        while remaining_stories and len(first_story_chunk) < batch_size:
            candidate = first_story_chunk + [remaining_stories[0]]
            candidate_payload = _write_batch_payload(
                current_version=current_version,
                stories=candidate,
                topics=topics,
                digests=digests,
                insights=insights,
            )
            if _payload_size_bytes(candidate_payload) > max_body_bytes:
                break
            first_story_chunk = candidate
            remaining_stories.pop(0)

        payloads.append(
            _write_batch_payload(
                current_version=current_version,
                stories=first_story_chunk,
                topics=topics,
                digests=digests,
                insights=insights,
            )
        )
    else:
        # Rare fallback: if topics+digests+insights alone exceed the request limit,
        # split each collection without dropping fields. Single oversized docs
        # fail locally with a clear error instead of being truncated.
        for field, docs in (("topics", topics), ("digests", digests), ("insights", insights)):
            payloads.extend(
                _chunk_write_batch_docs(
                    current_version=current_version,
                    field=field,
                    docs=docs,
                    max_docs=batch_size,
                    max_body_bytes=max_body_bytes,
                )
            )

    if remaining_stories:
        payloads.extend(
            _chunk_write_batch_docs(
                current_version=current_version,
                field="stories",
                docs=remaining_stories,
                max_docs=batch_size,
                max_body_bytes=max_body_bytes,
            )
        )

    if not payloads:
        empty_payload = {
            "action": "writeBatch",
            "syncVersion": current_version,
            "stories": [],
        }
        empty_size = _payload_size_bytes(empty_payload)
        if empty_size > max_body_bytes:
            raise CloudPushError(
                f"empty writeBatch payload is {empty_size} bytes, exceeding "
                f"HNREADER_CLOUD_PUSH_MAX_BODY_BYTES={max_body_bytes}"
            )
        payloads.append(empty_payload)

    return payloads


def _dashboard_payload(
    *,
    sync_version: int,
    summary: Dict[str, Any],
    ingest_runs: Optional[List[dict]] = None,
    cloud_sync_runs: Optional[List[dict]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "action": "writeDashboard",
        "syncVersion": int(sync_version),
        "dashboardSummary": summary,
    }
    if ingest_runs is not None:
        payload["dashboardIngestRuns"] = ingest_runs
    if cloud_sync_runs is not None:
        payload["dashboardCloudSyncRuns"] = cloud_sync_runs
    return payload


def _chunk_dashboard_docs(
    *,
    sync_version: int,
    summary: Dict[str, Any],
    field: str,
    docs: List[dict],
    max_body_bytes: int,
) -> List[Dict[str, Any]]:
    if field not in ("dashboardIngestRuns", "dashboardCloudSyncRuns"):
        raise CloudPushError(f"unsupported dashboard field: {field}")
    if not docs:
        return []

    payloads: List[Dict[str, Any]] = []
    chunk: List[dict] = []
    for doc in docs:
        candidate = chunk + [doc]
        candidate_payload = _dashboard_payload(
            sync_version=sync_version,
            summary=summary,
            ingest_runs=candidate if field == "dashboardIngestRuns" else None,
            cloud_sync_runs=(
                candidate if field == "dashboardCloudSyncRuns" else None
            ),
        )
        if _payload_size_bytes(candidate_payload) <= max_body_bytes:
            chunk = candidate
            continue

        if chunk:
            payloads.append(
                _dashboard_payload(
                    sync_version=sync_version,
                    summary=summary,
                    ingest_runs=chunk if field == "dashboardIngestRuns" else None,
                    cloud_sync_runs=(
                        chunk if field == "dashboardCloudSyncRuns" else None
                    ),
                )
            )
            chunk = [doc]
            single_payload = _dashboard_payload(
                sync_version=sync_version,
                summary=summary,
                ingest_runs=chunk if field == "dashboardIngestRuns" else None,
                cloud_sync_runs=(
                    chunk if field == "dashboardCloudSyncRuns" else None
                ),
            )
            if _payload_size_bytes(single_payload) <= max_body_bytes:
                continue
        else:
            single_payload = candidate_payload

        single_size = _payload_size_bytes(single_payload)
        raise CloudPushError(
            f"single {field} doc {_doc_id_for_error(doc)} makes a "
            f"writeDashboard payload of {single_size} bytes, exceeding "
            f"HNREADER_CLOUD_PUSH_MAX_BODY_BYTES={max_body_bytes}; "
            "reduce the cloud document size or move this payload through "
            "cloud storage / a finer-grained cloud protocol"
        )

    if chunk:
        payloads.append(
            _dashboard_payload(
                sync_version=sync_version,
                summary=summary,
                ingest_runs=chunk if field == "dashboardIngestRuns" else None,
                cloud_sync_runs=(
                    chunk if field == "dashboardCloudSyncRuns" else None
                ),
            )
        )
    return payloads


def _dashboard_payloads(
    *,
    sync_version: int,
    summary: Dict[str, Any],
    ingest_runs: List[dict],
    cloud_sync_runs: List[dict],
    max_body_bytes: int,
) -> List[Dict[str, Any]]:
    if max_body_bytes <= 0:
        raise CloudPushError(
            f"HNREADER_CLOUD_PUSH_MAX_BODY_BYTES must be >= 1 "
            f"(got {max_body_bytes})"
        )
    summary_payload = _dashboard_payload(
        sync_version=sync_version,
        summary=summary,
    )
    summary_size = _payload_size_bytes(summary_payload)
    if summary_size > max_body_bytes:
        raise CloudPushError(
            f"dashboard summary payload is {summary_size} bytes, exceeding "
            f"HNREADER_CLOUD_PUSH_MAX_BODY_BYTES={max_body_bytes}"
        )

    full_payload = _dashboard_payload(
        sync_version=sync_version,
        summary=summary,
        ingest_runs=ingest_runs if ingest_runs else None,
        cloud_sync_runs=cloud_sync_runs if cloud_sync_runs else None,
    )
    if _payload_size_bytes(full_payload) <= max_body_bytes:
        return [full_payload]

    payloads: List[Dict[str, Any]] = []
    payloads.extend(
        _chunk_dashboard_docs(
            sync_version=sync_version,
            summary=summary,
            field="dashboardIngestRuns",
            docs=ingest_runs,
            max_body_bytes=max_body_bytes,
        )
    )
    payloads.extend(
        _chunk_dashboard_docs(
            sync_version=sync_version,
            summary=summary,
            field="dashboardCloudSyncRuns",
            docs=cloud_sync_runs,
            max_body_bytes=max_body_bytes,
        )
    )
    return payloads or [summary_payload]


def push_read_model(
    *,
    url: str,
    secret: str,
    batch_size: int = 20,
    max_body_bytes: int = DEFAULT_WRITE_BATCH_MAX_BODY_BYTES,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    write_batch_max_attempts: int = DEFAULT_WRITE_BATCH_MAX_ATTEMPTS,
    source_dir: Optional[Path] = None,
    deadline_at: Optional[float] = None,
) -> dict:
    """Business publish: push stories / topics / digests / meta to the cloud DB.

    ``deadline_at`` is a wall-clock deadline that the entire push must
    finish by. Each phase boundary checks the remaining budget and aborts
    with :class:`CloudPushError` before starting an HTTP call that would
    not have time to complete; individual call timeouts are clamped to
    the remaining budget so a single phase never overruns it. The trailing
    ``cleanupOld`` is non-fatal and skipped silently when budget runs out
    (its work will be picked up on the next round).

    Dashboard collections are out of scope for this function -- the caller
    invokes :func:`push_dashboard` separately, only after this function
    returns ok and the local cloud_sync_runs has reached its final state.

    Returns:
        A stats dict containing ``syncVersion`` / ``previousVersion`` /
        ``stories`` / ``topics`` / ``digests`` / ``cleanup``. stories/topics/
        digests are the counts the cloud confirmed it wrote.

    Raises:
        CloudPushError: any of ping / writeBatch / switchMeta failed, the
            deadline was exhausted before switchMeta, or meta.json is missing.
        A cleanupOld failure only logs a warning and does not raise; cleanup
        is skipped when the deadline is exhausted.
    """
    out_dir = source_dir or default_output_dir()

    if not (out_dir / "meta.json").exists():
        raise CloudPushError(
            f"cloud-sync output does not exist ({out_dir}/meta.json); "
            "call cloud_sync.build_read_model() or `python -m server.cloud_sync` first"
        )

    # SSRF / misconfiguration safeguard: gate the URL before any HMAC signing.
    url = validate_cloud_push_url(url)
    secret = validate_cloud_push_secret(secret)

    meta = json.loads((out_dir / "meta.json").read_text(encoding="utf-8"))
    stories = _load_required_jsonl(out_dir / "stories.jsonl")
    topics = _load_required_jsonl(out_dir / "topics.jsonl")
    digests = _load_required_jsonl(out_dir / "digests.jsonl")
    insights = _load_required_jsonl(out_dir / "insights.jsonl")
    current_version = int(meta["currentVersion"])
    previous_version = meta.get("previousVersion")
    _validate_versioned_docs("stories", stories, current_version)
    _validate_versioned_docs("topics", topics, current_version)
    _validate_versioned_docs("digests", digests, current_version)
    _validate_versioned_docs("insights", insights, current_version)
    write_batch_payloads = _write_batch_payloads(
        current_version=current_version,
        stories=stories,
        topics=topics,
        digests=digests,
        insights=insights,
        batch_size=batch_size,
        max_body_bytes=max_body_bytes,
    )

    # The manifest covers business collections only. Stale cleanup of the
    # dashboard collections is delegated to cleanupOld (keepVersions); it no
    # longer goes through the cleanupByManifest path inside switchMeta.
    manifest: Dict[str, Any] = {
        "stories": [str(doc["_id"]) for doc in stories if doc.get("_id")],
        "topics": [str(doc["_id"]) for doc in topics if doc.get("_id")],
        "digests": [str(doc["_id"]) for doc in digests if doc.get("_id")],
        "insights": [str(doc["_id"]) for doc in insights if doc.get("_id")],
    }

    log.info(
        "[push] target version=%d previous=%s stories=%d topics=%d digests=%d insights=%d url=%s",
        current_version, previous_version,
        len(stories), len(topics), len(digests), len(insights), _redacted_url_for_log(url),
    )

    # ---------- 1. ping ----------
    _abort_if_insufficient(deadline_at, phase="ping")
    r = _post(
        url, secret, {"action": "ping"},
        timeout=_next_call_timeout(deadline_at, timeout_seconds),
    )
    if not r.get("ok"):
        raise CloudPushError(f"ping failed: {r}")
    log.info("[push] ping ok: %s", r)

    if write_batch_max_attempts < 1:
        raise CloudPushError(
            f"write_batch_max_attempts must be >= 1 (got {write_batch_max_attempts})"
        )

    # ---------- 2. writeBatch ----------
    sent = {"stories": 0, "topics": 0, "digests": 0, "insights": 0}
    for payload in write_batch_payloads:
        payload_bytes = _payload_size_bytes(payload)
        r: dict = {}
        for attempt in range(write_batch_max_attempts):
            _abort_if_insufficient(deadline_at, phase="writeBatch")
            r = _post(
                url, secret, payload,
                timeout=_next_call_timeout(deadline_at, timeout_seconds),
            )
            if r.get("ok"):
                break
            if (
                attempt + 1 >= write_batch_max_attempts
                or not _is_retryable_write_batch_response(r)
            ):
                break
            delay = _write_batch_retry_delay(attempt)
            remaining = _budget_remaining(deadline_at)
            if remaining is not None and remaining < delay + _MIN_PER_CALL_SECONDS:
                log.warning(
                    "[push] writeBatch transient failure not retried: "
                    "only %.1fs remain after attempt %s/%s: %s",
                    remaining, attempt + 1, write_batch_max_attempts, r,
                )
                break
            log.warning(
                "[push] writeBatch transient failure attempt %s/%s; "
                "retrying in %.1fs: %s",
                attempt + 1, write_batch_max_attempts, delay, r,
            )
            time.sleep(delay)
        if not r.get("ok"):
            raise CloudPushError(f"writeBatch failed: {r}")
        for k in sent:
            sent[k] += int(r.get(k, 0) or 0)
        log.info(
            "[push] writeBatch chunk: stories+%s topics+%s digests+%s insights+%s bytes=%s/%s",
            r.get("stories", 0), r.get("topics", 0), r.get("digests", 0),
            r.get("insights", 0), payload_bytes, max_body_bytes,
        )

    # ---------- 3. switchMeta ----------
    # Aborting here is preferable to starting a switchMeta that gets killed
    # mid-flight — without switchMeta the partial batches stay invisible.
    _abort_if_insufficient(deadline_at, phase="switchMeta")
    switch_meta_payload: Dict[str, Any] = {
        "currentVersion": current_version,
        "previousVersion": previous_version,
        "feedCounts": meta["feedCounts"],
        "publishedAt": meta.get("publishedAt"),
        "manifest": manifest,
    }
    r = _post(
        url, secret,
        {"action": "switchMeta", "meta": switch_meta_payload},
        timeout=_next_call_timeout(deadline_at, timeout_seconds),
    )
    if not r.get("ok"):
        raise CloudPushError(f"switchMeta failed: {r}")
    log.info("[push] switchMeta ok: %s", r)

    # ---------- 4. cleanupOld (non-fatal) ----------
    # keepVersions=[current, previous] -- keep only two generations in the
    # cloud, which both allows rollback and sweeps away the partial residue
    # of a few half-failed rounds in between (v17/18/19 partials are cleaned
    # up together once v20 is ok).
    cleanup_result: dict = {"skipped": True}
    keep_versions: List[int] = [current_version]
    if previous_version and int(previous_version) >= 1 and int(previous_version) != current_version:
        keep_versions.append(int(previous_version))
    remaining = _budget_remaining(deadline_at)
    if remaining is not None and remaining < _MIN_PER_CALL_SECONDS:
        log.info(
            "[push] cleanupOld skipped: only %.1fs remain (need >=%ds); "
            "next round will retry", remaining, _MIN_PER_CALL_SECONDS,
        )
        cleanup_result = {"skipped": "deadline"}
    else:
        r = _post(
            url, secret,
            {"action": "cleanupOld", "keepVersions": keep_versions},
            timeout=_next_call_timeout(deadline_at, timeout_seconds),
        )
        cleanup_result = r
        if not r.get("ok"):
            log.warning("[push] cleanupOld failed (non-fatal): %s", r)
        else:
            log.info("[push] cleanupOld ok: %s", r)

    return {
        "syncVersion": current_version,
        "previousVersion": previous_version,
        "stories": sent["stories"],
        "topics": sent["topics"],
        "digests": sent["digests"],
        "insights": sent["insights"],
        "cleanup": cleanup_result,
    }


def push_dashboard(
    *,
    url: str,
    secret: str,
    sync_version: int,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_body_bytes: int = DEFAULT_WRITE_BATCH_MAX_BODY_BYTES,
    source_dir: Optional[Path] = None,
    deadline_at: Optional[float] = None,
) -> dict:
    """Dashboard publish: push hn_dashboard_summary / ingest_runs / cloud_sync_runs.

    Must only be called after :func:`push_read_model` returns ok and the local
    ``cloud_sync_runs`` has written this round's final state, so the dashboard
    summary reflects this round's real result (rather than a ``running`` state
    that is always one round behind).

    A missing or corrupt file raises :class:`CloudPushError` directly --
    the dashboard publish is a deterministic artifact, so a missing file
    means the build stage is broken; do not silently skip.

    Returns:
        A stats dict containing ``syncVersion`` / ``dashboardSummary`` /
        ``dashboardIngestRuns`` / ``dashboardCloudSyncRuns``; the last three
        are the counts the cloud confirmed it wrote.

    Raises:
        CloudPushError: a dashboard file is missing/corrupt, or
            writeDashboard failed.
    """
    out_dir = source_dir or default_output_dir()
    url = validate_cloud_push_url(url)
    secret = validate_cloud_push_secret(secret)

    summary = _load_dashboard_summary(out_dir)
    if summary.get("_id") not in (None, "summary"):
        raise CloudPushError(
            f"dashboard summary _id must be 'summary': {summary.get('_id')!r}"
        )
    if summary.get("syncVersion") is not None:
        try:
            summary_version = int(summary.get("syncVersion"))
        except (TypeError, ValueError) as exc:
            raise CloudPushError(
                f"dashboard summary syncVersion is invalid: "
                f"{summary.get('syncVersion')!r}"
            ) from exc
        if summary_version != int(sync_version):
            raise CloudPushError(
                f"dashboard summary syncVersion={summary.get('syncVersion')!r}, "
                f"expected {int(sync_version)}"
            )

    ingest_runs = _load_required_jsonl(
        out_dir / dashboard_projection.DASHBOARD_INGEST_RUNS_FILE
    )
    cloud_sync_runs = _load_required_jsonl(
        out_dir / dashboard_projection.DASHBOARD_CLOUD_SYNC_RUNS_FILE
    )
    _validate_versioned_docs(
        "hn_dashboard_ingest_runs", ingest_runs, int(sync_version)
    )
    _validate_versioned_docs(
        "hn_dashboard_cloud_sync_runs", cloud_sync_runs, int(sync_version)
    )

    log.info(
        "[push-dashboard] target version=%d summary=1 ingest=%d cloud_sync=%d url=%s",
        int(sync_version), len(ingest_runs), len(cloud_sync_runs),
        _redacted_url_for_log(url),
    )

    payloads = _dashboard_payloads(
        sync_version=int(sync_version),
        summary=summary,
        ingest_runs=ingest_runs,
        cloud_sync_runs=cloud_sync_runs,
        max_body_bytes=int(max_body_bytes),
    )
    sent_summary = 0
    sent_ingest = 0
    sent_cloud_sync = 0
    for payload in payloads:
        _abort_if_insufficient(deadline_at, phase="writeDashboard")
        payload_bytes = _payload_size_bytes(payload)
        r = _post(
            url, secret, payload,
            timeout=_next_call_timeout(deadline_at, timeout_seconds),
        )
        if not r.get("ok"):
            raise CloudPushError(f"writeDashboard failed: {r}")
        sent_summary = max(sent_summary, int(r.get("dashboardSummary", 0) or 0))
        sent_ingest += int(r.get("dashboardIngestRuns", 0) or 0)
        sent_cloud_sync += int(r.get("dashboardCloudSyncRuns", 0) or 0)
        log.info(
            "[push-dashboard] writeDashboard chunk: summary=%s ingest+%s "
            "cloud_sync+%s bytes=%s/%s",
            1 if payload.get("dashboardSummary") else 0,
            len(payload.get("dashboardIngestRuns") or []),
            len(payload.get("dashboardCloudSyncRuns") or []),
            payload_bytes,
            int(max_body_bytes),
        )
    log.info(
        "[push-dashboard] writeDashboard ok: %s",
        {
            "ok": True,
            "action": "writeDashboard",
            "syncVersion": int(sync_version),
            "dashboardSummary": sent_summary,
            "dashboardIngestRuns": sent_ingest,
            "dashboardCloudSyncRuns": sent_cloud_sync,
        },
    )

    return {
        "syncVersion": int(sync_version),
        "dashboardSummary": sent_summary,
        "dashboardIngestRuns": sent_ingest,
        "dashboardCloudSyncRuns": sent_cloud_sync,
    }


def main() -> None:
    """CLI: read credentials from environment variables, call push_read_model, print the result."""
    configure_logging(verbose=False)
    url = _require_env("HNREADER_CLOUD_PUSH_URL")
    secret = _require_env("HNREADER_CLOUD_PUSH_SECRET")
    # Prefer HNREADER_CLOUD_PUSH_BATCH_SIZE (new name), fall back to HNREADER_CLOUD_BATCH_SIZE (old name)
    batch_raw = os.environ.get("HNREADER_CLOUD_PUSH_BATCH_SIZE") \
        or os.environ.get("HNREADER_CLOUD_BATCH_SIZE") \
        or "20"
    try:
        batch_size = int(batch_raw)
    except ValueError:
        _print_console(f"[push] invalid batch size: {batch_raw}", file=sys.stderr)
        sys.exit(2)
    max_body_raw = os.environ.get("HNREADER_CLOUD_PUSH_MAX_BODY_BYTES") \
        or str(DEFAULT_WRITE_BATCH_MAX_BODY_BYTES)
    try:
        max_body_bytes = int(max_body_raw)
    except ValueError:
        _print_console(
            f"[push] invalid max body bytes: {max_body_raw}", file=sys.stderr
        )
        sys.exit(2)

    try:
        stats = push_read_model(
            url=url,
            secret=secret,
            batch_size=batch_size,
            max_body_bytes=max_body_bytes,
        )
    except CloudPushError as exc:
        _print_console(f"[push] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)

    _print_console("[push] all done")
    _print_console(json.dumps(stats, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
