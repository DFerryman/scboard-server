"""GDELT DOC 2.0 ArtList client.

The ingest pipeline only needs a bounded JSON article list. This client keeps
the request shape explicit and treats rate limits as non-fatal upstream state
for the orchestrator to handle.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from . import settings
from .http_client import (
    GDELT_RESPONSE_MAX_BYTES,
    ResponseTooLargeError,
    read_limited,
    urlopen_no_redirect,
)


class GdeltFetchError(RuntimeError):
    """Raised for recoverable GDELT transport or response failures."""


class GdeltRateLimitError(GdeltFetchError):
    """Raised on HTTP 429 so callers can throttle without failing HN ingest."""

    def __init__(self, detail: str, *, retry_after_seconds: Optional[float] = None):
        super().__init__(detail)
        self.retry_after_seconds = retry_after_seconds


def validate_gdelt_api_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(str(url or "").strip())
    if parsed.scheme != "https":
        raise GdeltFetchError("GDELT API URL must use https")
    if parsed.username or parsed.password:
        raise GdeltFetchError("GDELT API URL userinfo is not allowed")
    host = (parsed.hostname or "").lower().rstrip(".")
    if host != "api.gdeltproject.org":
        raise GdeltFetchError("GDELT API URL host must be api.gdeltproject.org")
    if parsed.query or parsed.fragment:
        raise GdeltFetchError("GDELT API URL must not include query or fragment")
    if not parsed.path:
        parsed = parsed._replace(path="/api/v2/doc/doc")
    return urllib.parse.urlunsplit(parsed)


def _retry_after_seconds(exc: urllib.error.HTTPError) -> Optional[float]:
    raw = exc.headers.get("Retry-After") if exc.headers else None
    if not raw:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None


class GdeltClient:
    def __init__(
        self,
        *,
        api_url: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.api_url = validate_gdelt_api_url(api_url or settings.GDELT_DOC_API_URL)
        self.timeout = float(
            timeout
            if timeout is not None
            else settings.GDELT_REQUEST_TIMEOUT_SECONDS
        )

    def fetch_articles(
        self,
        *,
        query: str,
        timespan: str,
        maxrecords: int,
        sort: str = "DateDesc",
        mode: str = "ArtList",
        format_: str = "json",
    ) -> List[Dict[str, Any]]:
        params = {
            "query": str(query or "").strip(),
            "mode": str(mode or "ArtList").strip(),
            "format": str(format_ or "json").strip(),
            "timespan": str(timespan or "24h").strip(),
            "sort": str(sort or "DateDesc").strip(),
            "maxrecords": str(max(1, min(250, int(maxrecords)))),
        }
        if not params["query"]:
            raise GdeltFetchError("GDELT query must not be empty")
        url = self.api_url + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "HackerMiniGDELT/1.0",
            },
        )
        try:
            with urlopen_no_redirect(req, timeout=self.timeout) as resp:
                raw = read_limited(resp, GDELT_RESPONSE_MAX_BYTES)
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                raise GdeltRateLimitError(
                    "GDELT API returned HTTP 429",
                    retry_after_seconds=_retry_after_seconds(exc),
                ) from exc
            raise GdeltFetchError(f"GDELT API returned HTTP {exc.code}") from exc
        except ResponseTooLargeError as exc:
            raise GdeltFetchError(f"GDELT response too large: {exc}") from exc
        except (TimeoutError, socket.timeout, urllib.error.URLError) as exc:
            raise GdeltFetchError(f"GDELT fetch failed: {exc}") from exc

        try:
            payload = json.loads(raw.decode("utf-8", "replace"))
        except json.JSONDecodeError as exc:
            raise GdeltFetchError("GDELT returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise GdeltFetchError("GDELT JSON root must be an object")
        articles = payload.get("articles") or []
        if not isinstance(articles, list):
            raise GdeltFetchError("GDELT articles field must be an array")
        return [item for item in articles if isinstance(item, dict)]


__all__ = [
    "GdeltClient",
    "GdeltFetchError",
    "GdeltRateLimitError",
    "validate_gdelt_api_url",
]
