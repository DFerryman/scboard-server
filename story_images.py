"""Story thumbnail extraction, upload, and storage lifecycle management."""

from __future__ import annotations

import base64
import hashlib
import html
import ipaddress
import logging
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html.parser import HTMLParser
from io import BytesIO
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from PIL import Image, ImageOps

from . import cloud_image_upload, db, repository, settings
from .http_client import ResponseTooLargeError, read_limited, urlopen_no_redirect


log = logging.getLogger("server.story_images")

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36 "
    "HackerMiniStoryImage/1.0"
)
_MAX_REDIRECTS = 5
_PNG_MIME = "image/png"
_THUMBNAIL_SIZE = 128


class StoryImageError(RuntimeError):
    """Base error for recoverable image pipeline failures."""


class UnsafeUrlError(StoryImageError):
    """Raised when a page or image URL points at a disallowed network target."""


class NoImageCandidate(StoryImageError):
    """Raised when no usable image candidate is present for a story."""


@dataclass(frozen=True)
class ImageCandidate:
    kind: str
    url: str


@dataclass(frozen=True)
class ProcessedImage:
    story_id: int
    source_url: str
    cloud_path: str
    sha256: str
    png_bytes: bytes


class _ImageCandidateParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.candidates: List[ImageCandidate] = []
        self._first_img_seen = False

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        data = {str(k).lower(): html.unescape(v or "") for k, v in attrs}
        if tag.lower() == "meta":
            prop = data.get("property", "").lower()
            name = data.get("name", "").lower()
            content = data.get("content", "")
            if content and prop in {"og:image", "og:image:url", "og:image:secure_url"}:
                self._add("meta", content)
            elif content and name in {"twitter:image", "twitter:image:src"}:
                self._add("meta", content)
        elif tag.lower() == "link":
            rel = data.get("rel", "").lower()
            href = data.get("href", "")
            if href and ("apple-touch-icon" in rel or "icon" in rel):
                self._add("icon", href)
        elif tag.lower() == "img" and not self._first_img_seen:
            src = data.get("src") or data.get("data-src") or data.get("data-original")
            if src:
                self._first_img_seen = True
                self._add("first-img", src)

    def _add(self, kind: str, url: str) -> None:
        resolved = _resolve_url(self.base_url, url)
        if not resolved:
            return
        if not any(c.url == resolved for c in self.candidates):
            self.candidates.append(ImageCandidate(kind=kind, url=resolved))


def _resolve_url(base_url: str, candidate: str) -> str:
    raw = str(candidate or "").strip()
    if not raw or raw.startswith("data:"):
        return ""
    try:
        return urllib.parse.urljoin(base_url, raw)
    except Exception:
        return ""


def _is_public_ip(address: str) -> bool:
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def _validate_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(str(url or "").strip())
    if parsed.scheme not in {"http", "https"}:
        raise UnsafeUrlError(f"unsupported URL scheme: {parsed.scheme or '<empty>'}")
    if parsed.username or parsed.password:
        raise UnsafeUrlError("URL userinfo is not allowed")
    host = parsed.hostname
    if not host:
        raise UnsafeUrlError("URL host is required")
    if host.lower() in {"localhost", "localhost.localdomain"}:
        raise UnsafeUrlError("localhost is not allowed")
    try:
        infos = socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise UnsafeUrlError(f"DNS resolution failed for {host}") from exc
    addresses = {info[4][0] for info in infos if info and info[4]}
    if not addresses or any(not _is_public_ip(addr) for addr in addresses):
        raise UnsafeUrlError(f"URL host resolves to a non-public address: {host}")
    return urllib.parse.urlunsplit(parsed)


def _fetch_limited(
    url: str,
    *,
    accept: str,
    max_bytes: int,
    timeout: float,
) -> tuple[str, bytes, Mapping[str, str]]:
    current = _validate_url(url)
    for _ in range(_MAX_REDIRECTS + 1):
        req = urllib.request.Request(
            current,
            headers={
                "User-Agent": _UA,
                "Accept": accept,
            },
        )
        try:
            with urlopen_no_redirect(req, timeout=timeout) as resp:
                final_url = getattr(resp, "url", current)
                headers = {str(k).lower(): str(v) for k, v in resp.headers.items()}
                return final_url, read_limited(resp, max_bytes), headers
        except urllib.error.HTTPError as exc:
            if exc.code in {301, 302, 303, 307, 308}:
                location = exc.headers.get("Location")
                if not location:
                    raise StoryImageError(f"redirect without Location from {current}") from exc
                current = _validate_url(urllib.parse.urljoin(current, location))
                continue
            raise StoryImageError(f"HTTP {exc.code} for {current}") from exc
        except ResponseTooLargeError as exc:
            raise StoryImageError(str(exc)) from exc
        except (TimeoutError, socket.timeout, urllib.error.URLError) as exc:
            raise StoryImageError(str(exc)) from exc
    raise StoryImageError(f"too many redirects for {url}")


def extract_image_candidates(html_text: str, base_url: str) -> List[ImageCandidate]:
    parser = _ImageCandidateParser(base_url)
    parser.feed(html_text or "")
    origin = urllib.parse.urlsplit(base_url)
    if origin.scheme in {"http", "https"} and origin.netloc:
        root = urllib.parse.urlunsplit((origin.scheme, origin.netloc, "", "", ""))
        for path in ("/apple-touch-icon.png", "/favicon.ico"):
            resolved = root + path
            if not any(c.url == resolved for c in parser.candidates):
                parser.candidates.append(ImageCandidate(kind="fallback-icon", url=resolved))
    return parser.candidates


def _normalize_image(raw: bytes, *, max_pixels: int) -> bytes:
    old_limit = Image.MAX_IMAGE_PIXELS
    Image.MAX_IMAGE_PIXELS = max_pixels
    try:
        with Image.open(BytesIO(raw)) as im:
            im.seek(0)
            if int(im.width) * int(im.height) > int(max_pixels):
                raise StoryImageError(
                    f"image has {int(im.width) * int(im.height)} pixels > limit {max_pixels}"
                )
            im = im.convert("RGBA")
            thumb = ImageOps.contain(
                im, (_THUMBNAIL_SIZE, _THUMBNAIL_SIZE),
                method=Image.Resampling.LANCZOS,
            )
            out = Image.new(
                "RGBA", (_THUMBNAIL_SIZE, _THUMBNAIL_SIZE), (0, 0, 0, 255)
            )
            out.alpha_composite(
                thumb,
                (
                    (_THUMBNAIL_SIZE - thumb.width) // 2,
                    (_THUMBNAIL_SIZE - thumb.height) // 2,
                ),
            )
            buf = BytesIO()
            out.save(buf, format="PNG", optimize=True)
            return buf.getvalue()
    finally:
        Image.MAX_IMAGE_PIXELS = old_limit


def _cloud_path(story_id: int, sha256_hex: str) -> str:
    prefix = (settings.STORY_IMAGE_CLOUD_PATH_PREFIX or "hn/story-thumbs/v1").strip("/")
    return f"{prefix}/{int(story_id)}-{sha256_hex[:16]}.png"


def fetch_and_normalize_story_image(row: Mapping[str, Any]) -> ProcessedImage:
    story_id = int(row["id"])
    story_url = str(row["url"] or "").strip()
    if not story_url:
        raise NoImageCandidate("story has no URL")
    final_url, page, headers = _fetch_limited(
        story_url,
        accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        max_bytes=int(settings.STORY_IMAGE_MAX_HTML_BYTES),
        timeout=float(settings.STORY_IMAGE_DOWNLOAD_TIMEOUT_SECONDS),
    )
    content_type = headers.get("content-type", "")
    if "text/html" not in content_type and "application/xhtml" not in content_type:
        raise NoImageCandidate(f"story URL is not HTML ({content_type or 'unknown'})")
    candidates = extract_image_candidates(
        page.decode("utf-8", "replace"),
        final_url,
    )
    if not candidates:
        raise NoImageCandidate("no image candidates")
    last_error = ""
    for candidate in candidates:
        try:
            _img_url, raw, image_headers = _fetch_limited(
                candidate.url,
                accept="image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                max_bytes=int(settings.STORY_IMAGE_MAX_BYTES),
                timeout=float(settings.STORY_IMAGE_DOWNLOAD_TIMEOUT_SECONDS),
            )
            image_type = image_headers.get("content-type", "")
            if image_type and not image_type.lower().startswith("image/"):
                last_error = f"candidate was not image content: {image_type}"
                continue
            if "svg" in image_type.lower() or candidate.url.lower().endswith(".svg"):
                last_error = "svg candidates are skipped"
                continue
            png = _normalize_image(
                raw,
                max_pixels=int(settings.STORY_IMAGE_MAX_PIXELS),
            )
            digest = hashlib.sha256(png).hexdigest()
            return ProcessedImage(
                story_id=story_id,
                source_url=candidate.url,
                cloud_path=_cloud_path(story_id, digest),
                sha256=digest,
                png_bytes=png,
            )
        except Exception as exc:  # noqa: BLE001 - try the next candidate.
            last_error = f"{type(exc).__name__}: {exc}"
            continue
    raise NoImageCandidate(last_error or "no usable image candidate")


def _rows_to_dicts(rows: Iterable[Any]) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows]


def _image_payload(item: ProcessedImage) -> Dict[str, Any]:
    return {
        "storyId": item.story_id,
        "sourceUrl": item.source_url,
        "cloudPath": item.cloud_path,
        "sha256": item.sha256,
        "pngBase64": base64.b64encode(item.png_bytes).decode("ascii"),
    }


def process_story_images_for_ids(story_ids: Sequence[int]) -> Dict[str, Any]:
    """Fetch, normalize, upload, and persist thumbnails for candidate stories."""

    if not settings.STORY_IMAGES_ENABLED:
        return {"skipped": True, "reason": "disabled"}
    if not settings.STORY_IMAGE_UPLOAD_URL or not settings.STORY_IMAGE_UPLOAD_SECRET:
        return {"skipped": True, "reason": "missing_upload_config"}
    started = time.time()
    conn = db.connect()
    try:
        rows = _rows_to_dicts(repository.story_rows_for_image_processing(conn, story_ids))
    finally:
        conn.close()
    todo = [
        row for row in rows
        if not (str(row.get("image_status") or "") == "ready" and row.get("image_url"))
    ]
    summary: Dict[str, Any] = {
        "skipped": False,
        "candidates": len(rows),
        "already_ready": len(rows) - len(todo),
        "processed": 0,
        "uploaded": 0,
        "missing": 0,
        "failed": 0,
        "elapsed_seconds": 0.0,
    }
    if not todo:
        summary["elapsed_seconds"] = time.time() - started
        return summary

    processed: List[ProcessedImage] = []
    worker_count = max(1, min(int(settings.STORY_IMAGE_WORKERS), len(todo)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(fetch_and_normalize_story_image, row): row for row in todo}
        for future in as_completed(futures):
            row = futures[future]
            story_id = int(row["id"])
            try:
                item = future.result()
                processed.append(item)
                summary["processed"] += 1
            except NoImageCandidate as exc:
                conn = db.connect()
                try:
                    with db.transaction(conn):
                        repository.record_story_image_missing(
                            conn, story_id, status="missing", error=str(exc)
                        )
                finally:
                    conn.close()
                summary["missing"] += 1
            except Exception as exc:  # noqa: BLE001
                conn = db.connect()
                try:
                    with db.transaction(conn):
                        repository.record_story_image_missing(
                            conn,
                            story_id,
                            status="failed",
                            error=f"{type(exc).__name__}: {exc}",
                        )
                finally:
                    conn.close()
                summary["failed"] += 1

    if processed:
        payloads = [_image_payload(item) for item in processed]
        try:
            uploaded = cloud_image_upload.upload_story_images(
                url=settings.STORY_IMAGE_UPLOAD_URL,
                secret=settings.STORY_IMAGE_UPLOAD_SECRET,
                images=payloads,
                batch_size=int(settings.STORY_IMAGE_UPLOAD_BATCH_SIZE),
                timeout_seconds=max(1, int(settings.STORY_IMAGE_DOWNLOAD_TIMEOUT_SECONDS)),
            )
        except Exception as exc:  # noqa: BLE001
            uploaded = {}
            upload_error = f"{type(exc).__name__}: {exc}"
            conn = db.connect()
            try:
                with db.transaction(conn):
                    for item in processed:
                        repository.record_story_image_missing(
                            conn,
                            item.story_id,
                            status="failed",
                            error=upload_error,
                        )
            finally:
                conn.close()
            summary["failed"] += len(processed)
        else:
            by_story = {item.story_id: item for item in processed}
            conn = db.connect()
            try:
                with db.transaction(conn):
                    for story_id, item in by_story.items():
                        result = uploaded.get(story_id) or {}
                        file_id = str(result.get("fileID") or "")
                        image_url = str(result.get("tempFileURL") or result.get("url") or "")
                        if file_id and image_url:
                            repository.record_story_image_upload(
                                conn,
                                story_id=story_id,
                                image_url=image_url,
                                image_file_id=file_id,
                                image_source_url=item.source_url,
                                cloud_path=item.cloud_path,
                                sha256=item.sha256,
                            )
                            summary["uploaded"] += 1
                        else:
                            repository.record_story_image_missing(
                                conn,
                                story_id,
                                status="failed",
                                error="upload result missing fileID/tempFileURL",
                            )
                            summary["failed"] += 1
            finally:
                conn.close()

    summary["elapsed_seconds"] = time.time() - started
    return summary


def active_image_file_ids_from_manifest(source_dir: Optional[Any] = None) -> List[str]:
    from pathlib import Path
    import json

    base = Path(source_dir) if source_dir is not None else settings.get_cloud_sync_output_dir()
    path = base / "story_images.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    values = payload.get("activeFileIDs") if isinstance(payload, dict) else []
    if not isinstance(values, list):
        return []
    return [str(v) for v in values if str(v).strip()]


def cleanup_cloud_images_after_publish(*, active_file_ids: Sequence[str]) -> Dict[str, Any]:
    """Delete cloud storage files no longer referenced by the published model."""

    if not settings.STORY_IMAGES_ENABLED:
        return {"skipped": True, "reason": "disabled"}
    if not settings.STORY_IMAGE_UPLOAD_URL or not settings.STORY_IMAGE_UPLOAD_SECRET:
        return {"skipped": True, "reason": "missing_upload_config"}

    now = repository.now_seconds()
    delete_after = now + int(settings.STORY_IMAGE_DELETE_GRACE_SECONDS)
    conn = db.connect()
    try:
        with db.transaction(conn):
            referenced = repository.mark_story_images_referenced(
                conn, active_file_ids, referenced_at=now
            )
            marked = repository.mark_unreferenced_story_images_pending_delete(
                conn, active_file_ids, delete_after=delete_after
            )
            pending = repository.pending_story_image_deletes(
                conn,
                limit=int(settings.STORY_IMAGE_DELETE_BATCH_SIZE),
                now=now,
            )
    finally:
        conn.close()

    file_ids = [str(r["image_file_id"]) for r in pending]
    if not file_ids:
        return {
            "skipped": False,
            "referenced": referenced,
            "markedPending": marked,
            "deleted": 0,
            "failed": 0,
        }

    result = cloud_image_upload.delete_story_images(
        url=settings.STORY_IMAGE_UPLOAD_URL,
        secret=settings.STORY_IMAGE_UPLOAD_SECRET,
        file_ids=file_ids,
        batch_size=int(settings.STORY_IMAGE_DELETE_BATCH_SIZE),
        timeout_seconds=max(1, int(settings.STORY_IMAGE_DOWNLOAD_TIMEOUT_SECONDS)),
    )
    deleted = [str(v) for v in result.get("deleted") or []]
    failed = result.get("failed") or []
    failed_ids = [str(item.get("fileID") or "") for item in failed if item.get("fileID")]

    conn = db.connect()
    try:
        with db.transaction(conn):
            repository.mark_story_images_deleted(conn, deleted, deleted_at=now)
            if failed_ids:
                repository.mark_story_image_delete_failed(
                    conn,
                    failed_ids,
                    error="deleteStoryImages failed",
                    retry_after=now + int(settings.STORY_IMAGE_DELETE_GRACE_SECONDS),
                )
    finally:
        conn.close()

    return {
        "skipped": False,
        "referenced": referenced,
        "markedPending": marked,
        "deleted": len(deleted),
        "failed": len(failed),
    }


__all__ = [
    "ImageCandidate",
    "ProcessedImage",
    "StoryImageError",
    "UnsafeUrlError",
    "NoImageCandidate",
    "extract_image_candidates",
    "fetch_and_normalize_story_image",
    "process_story_images_for_ids",
    "active_image_file_ids_from_manifest",
    "cleanup_cloud_images_after_publish",
]
