"""Minimal Hacker News API client.

Stdlib-only (``urllib``) so the first version stays dependency-light. Plan
§Implementation notes line 427: "Minimal version: standard-library
``urllib.request``". Each call retries up to ``settings.HN_RETRY_ATTEMPTS``
times with exponential backoff so transient blips do not fail a Fetcher
round.

For tests, subclass ``HnClient`` or pass any object exposing the same
``get_ranking`` / ``get_item`` methods to the Fetcher.
"""

from __future__ import annotations

import http.client
import json
import logging
import ipaddress
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from . import settings
from .http_client import (
    HN_RESPONSE_MAX_BYTES,
    ResponseTooLargeError,
    read_limited,
    urlopen_no_redirect,
)


log = logging.getLogger(__name__)


_FEED_PATH = {
    "top": "topstories.json",
    "new": "newstories.json",
    "best": "beststories.json",
    "ask": "askstories.json",
    "show": "showstories.json",
    "job": "jobstories.json",
}


class HnFetchError(RuntimeError):
    pass


class HnApiBaseUrlError(HnFetchError):
    pass


_BLOCKED_HOSTNAMES = frozenset({
    "localhost",
    "ip6-localhost",
    "ip6-loopback",
    "metadata",
    "metadata.google.internal",
    "metadata.goog",
})
_CGNAT_IPV4_NETWORK = ipaddress.ip_network("100.64.0.0/10")


def _is_blocked_hn_ip(ip: ipaddress._BaseAddress) -> bool:
    if isinstance(ip, ipaddress.IPv4Address) and ip in _CGNAT_IPV4_NETWORK:
        return True
    return not ip.is_global


def _hn_host_resolves_to_blocked_ip(host: str) -> Optional[str]:
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except (socket.gaierror, socket.herror, OSError):
        return None
    for info in infos:
        sockaddr = info[4]
        if not sockaddr:
            continue
        addr = str(sockaddr[0])
        try:
            ip = ipaddress.ip_address(addr.split("%", 1)[0])
        except ValueError:
            continue
        if _is_blocked_hn_ip(ip):
            return addr
    return None


def validate_hn_api_base(url: str) -> str:
    """Validate configurable HN API roots before any outbound fetch."""
    if not isinstance(url, str) or not url.strip():
        raise HnApiBaseUrlError("HN_API_BASE is empty")
    clean = url.strip().rstrip("/")
    parsed = urllib.parse.urlsplit(clean)
    if parsed.scheme.lower() != "https":
        raise HnApiBaseUrlError(
            f"HN_API_BASE must use https (got scheme={parsed.scheme!r})"
        )
    if parsed.username or parsed.password:
        raise HnApiBaseUrlError("HN_API_BASE must not include userinfo")
    if parsed.query:
        raise HnApiBaseUrlError("HN_API_BASE must not contain a query string")
    if parsed.fragment:
        raise HnApiBaseUrlError("HN_API_BASE must not contain a fragment")
    try:
        parsed.port
    except ValueError as exc:
        raise HnApiBaseUrlError(f"HN_API_BASE has invalid port: {exc}") from exc
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise HnApiBaseUrlError("HN_API_BASE is missing a host")
    if host in _BLOCKED_HOSTNAMES:
        raise HnApiBaseUrlError(
            f"HN_API_BASE host {host!r} is on the blocked-host list"
        )
    try:
        literal_ip = ipaddress.ip_address(host)
    except ValueError:
        literal_ip = None
    if literal_ip is not None:
        if _is_blocked_hn_ip(literal_ip):
            raise HnApiBaseUrlError(
                f"HN_API_BASE points to disallowed address {host}"
            )
    else:
        blocked = _hn_host_resolves_to_blocked_ip(host)
        if blocked is not None:
            raise HnApiBaseUrlError(
                f"HN_API_BASE host {host!r} resolves to disallowed address {blocked}"
            )
    return clean


class HnClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[float] = None,
        retry_attempts: Optional[int] = None,
    ) -> None:
        self.base_url = validate_hn_api_base(base_url or settings.HN_API_BASE)
        self.timeout = (
            timeout if timeout is not None else settings.HN_REQUEST_TIMEOUT_SECONDS
        )
        self.retry_attempts = max(
            1,
            int(
                retry_attempts
                if retry_attempts is not None
                else settings.HN_RETRY_ATTEMPTS
            ),
        )

    def _get_json(self, path: str) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_err: Optional[Exception] = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                req = urllib.request.Request(
                    url,
                    headers={"User-Agent": "hnreader/1.0"},
                )
                with urlopen_no_redirect(req, timeout=self.timeout) as resp:
                    if getattr(resp, "status", 200) >= 400:
                        raise HnFetchError(f"HTTP {resp.status} for {url}")
                    body = read_limited(resp, HN_RESPONSE_MAX_BYTES)
                if not body:
                    return None
                return json.loads(body.decode("utf-8"))
            except (
                urllib.error.URLError,
                urllib.error.HTTPError,
                http.client.HTTPException,
                TimeoutError,
                socket.timeout,
                OSError,
                ValueError,
                ResponseTooLargeError,
            ) as exc:
                last_err = exc
                if attempt < self.retry_attempts:
                    backoff = 0.5 * (2 ** (attempt - 1))
                    log.warning(
                        "HN GET %s failed (attempt %d/%d): %s — retrying in %.1fs",
                        url,
                        attempt,
                        self.retry_attempts,
                        exc,
                        backoff,
                    )
                    time.sleep(backoff)
        raise HnFetchError(
            f"GET {url} failed after {self.retry_attempts} attempts: {last_err}"
        )

    def get_ranking(self, feed: str) -> List[int]:
        if feed not in _FEED_PATH:
            raise ValueError(f"unknown feed: {feed}")
        data = self._get_json(_FEED_PATH[feed])
        if not isinstance(data, list):
            return []
        out: List[int] = []
        for raw in data:
            try:
                out.append(int(raw))
            except (TypeError, ValueError):
                continue
        return out

    def get_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        data = self._get_json(f"item/{int(item_id)}.json")
        if data is None:
            return None
        if not isinstance(data, dict):
            return None
        return data


__all__ = ["HnClient", "HnFetchError", "HnApiBaseUrlError", "validate_hn_api_base"]
