"""Small urllib helpers shared by server-side provider clients."""

from __future__ import annotations

import urllib.error
import urllib.request
from typing import Any


# Hard upper bounds applied at every external HTTP boundary so a single
# pathological upstream response cannot OOM the ingest worker. The numbers
# are picked to accommodate the largest legitimate payloads observed in
# normal operation with headroom, and to fail loudly otherwise.
HN_RESPONSE_MAX_BYTES = 1 * 1024 * 1024
"""1 MiB — HN list endpoints (~500 ints) and item payloads are tiny."""

GDELT_RESPONSE_MAX_BYTES = 5 * 1024 * 1024
"""5 MiB — GDELT DOC ArtList JSON for bounded maxrecords windows."""

AI_RESPONSE_MAX_BYTES = 4 * 1024 * 1024
"""4 MiB — OpenAI-compatible chat completion JSON. 8k output tokens fit easily."""

PROBE_RESPONSE_MAX_BYTES = 256 * 1024
"""256 KiB — dashboard probes (/models, balance) are small JSON docs."""

ERROR_BODY_MAX_BYTES = 256 * 1024
"""256 KiB — provider error bodies; we only keep the head for the log line."""


class ResponseTooLargeError(RuntimeError):
    """Raised by :func:`read_limited` when a response exceeds its limit."""


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler)


def urlopen_no_redirect(req: urllib.request.Request, *, timeout: float) -> Any:
    return _NO_REDIRECT_OPENER.open(req, timeout=timeout)


def read_limited(resp: Any, max_bytes: int) -> bytes:
    """Read at most ``max_bytes`` from a stdlib HTTP response.

    Works on both successful responses (``http.client.HTTPResponse``) and
    error bodies (``urllib.error.HTTPError`` — also file-like). Raises
    :class:`ResponseTooLargeError` if the advertised ``Content-Length`` or
    actual bytes exceed the cap so callers can fail fast instead of loading
    a hostile payload into memory.
    """
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")

    # Short-circuit on advertised length so we never even start reading a
    # body that the server itself claims is oversized.
    try:
        headers = getattr(resp, "headers", None)
        length_header = headers.get("content-length") if headers else None
    except Exception:  # noqa: BLE001
        length_header = None
    if length_header is not None:
        try:
            advertised = int(length_header)
        except (TypeError, ValueError):
            advertised = None
        if advertised is not None and advertised > max_bytes:
            raise ResponseTooLargeError(
                f"response advertised {advertised} bytes > limit {max_bytes}"
            )

    try:
        data = resp.read(max_bytes + 1)
    except TypeError:
        # Some adapters (and a few test fakes) implement read() without the
        # size argument. Fall back to a full read; the size guard below still
        # rejects oversized responses, so we lose only the early-out.
        data = resp.read()
    if data is None:
        return b""
    if not isinstance(data, (bytes, bytearray)):
        # Some adapters may return str on text endpoints; coerce to bytes.
        data = str(data).encode("utf-8", "replace")
    if len(data) > max_bytes:
        raise ResponseTooLargeError(f"response exceeded {max_bytes} bytes")
    return bytes(data)
