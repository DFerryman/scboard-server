"""Signed client for the story image CloudBase storage function."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence

from . import cloud_push


class StoryImageUploadError(RuntimeError):
    """Raised when the image storage cloud function rejects a request."""


_PAYLOAD_TOO_LARGE_CODES = {
    "EXCEED_MAX_PAYLOAD_SIZE",
    "PAYLOAD_TOO_LARGE",
    "REQUEST_ENTITY_TOO_LARGE",
}


def _require_config(url: str, secret: str) -> tuple[str, str]:
    clean_url = str(url or "").strip()
    clean_secret = str(secret or "").strip()
    if not clean_url:
        raise StoryImageUploadError("story image upload URL is not configured")
    if not clean_secret:
        raise StoryImageUploadError("story image upload secret is not configured")
    return clean_url, clean_secret


def _chunks(values: Sequence[Mapping[str, Any]], size: int) -> Iterable[List[Mapping[str, Any]]]:
    step = max(1, int(size))
    for i in range(0, len(values), step):
        yield list(values[i:i + step])


def _upload_payload(images: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    return {"action": "uploadStoryImages", "images": list(images)}


def _upload_payload_size(images: Sequence[Mapping[str, Any]]) -> int:
    return cloud_push._payload_size_bytes(_upload_payload(images))


def _planned_upload_batches(
    images: Sequence[Mapping[str, Any]],
    *,
    batch_size: int,
    max_body_bytes: int | None,
) -> Iterable[List[Mapping[str, Any]]]:
    """Pack images into request bodies before upload.

    ``batch_size`` mirrors the cloud function's MAX_IMAGES_PER_CALL. The byte
    check uses the exact JSON serialization that ``cloud_push._post`` signs and
    sends, so normal operation should never rely on reactive payload splitting.
    """

    values = list(images)
    max_count = max(1, int(batch_size))
    max_bytes = int(max_body_bytes or 0)
    if max_bytes <= 0:
        yield from _chunks(values, max_count)
        return

    current: List[Mapping[str, Any]] = []
    for image in values:
        candidate = [*current, image]
        if len(candidate) <= max_count and _upload_payload_size(candidate) <= max_bytes:
            current = candidate
            continue
        if current:
            yield current
            current = []
        yield [image]
    if current:
        yield current


def _is_payload_too_large(response: Mapping[str, Any]) -> bool:
    code = str(response.get("code") or "").strip().upper()
    if code in _PAYLOAD_TOO_LARGE_CODES:
        return True
    message = str(response.get("message") or response.get("error") or "").lower()
    return (
        "max payload" in message
        or "payload too large" in message
        or "request entity too large" in message
    )


def _story_id_from_payload(image: Mapping[str, Any]) -> int | None:
    try:
        return int(image.get("storyId"))
    except (TypeError, ValueError):
        return None


def _error_result_for_image(
    image: Mapping[str, Any],
    response: Mapping[str, Any] | str,
) -> Dict[int, Dict[str, Any]]:
    story_id = _story_id_from_payload(image)
    if story_id is None:
        return {}
    if isinstance(response, str):
        error = response
    else:
        error = f"uploadStoryImages failed: {dict(response)}"
    return {
        story_id: {
            "storyId": story_id,
            "error": error,
        }
    }


def _results_by_story_id(response: Mapping[str, Any]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for item in response.get("results") or []:
        try:
            story_id = int(item.get("storyId"))
        except (TypeError, ValueError):
            continue
        out[story_id] = dict(item)
    for item in response.get("failed") or []:
        try:
            story_id = int(item.get("storyId"))
        except (TypeError, ValueError):
            continue
        out[story_id] = {
            "storyId": story_id,
            "error": str(item.get("error") or "uploadStoryImages failed"),
        }
    return out


def _upload_batch(
    *,
    url: str,
    secret: str,
    batch: Sequence[Mapping[str, Any]],
    timeout_seconds: int,
    max_body_bytes: int | None,
) -> Dict[int, Dict[str, Any]]:
    payload = _upload_payload(batch)
    if max_body_bytes is not None and int(max_body_bytes) > 0:
        body_size = cloud_push._payload_size_bytes(payload)
        if body_size > int(max_body_bytes):
            return (
                _error_result_for_image(
                    batch[0],
                    (
                        "uploadStoryImages payload "
                        f"{body_size} bytes exceeds configured max "
                        f"{int(max_body_bytes)}"
                    ),
                )
                if len(batch) == 1
                else {
                    story_id: result
                    for image in batch
                    for story_id, result in _error_result_for_image(
                        image,
                        (
                            "uploadStoryImages planned batch exceeded "
                            f"configured max {int(max_body_bytes)} bytes"
                        ),
                    ).items()
                }
            )
    response = cloud_push._post(  # Reuse the existing pinned HTTPS + HMAC path.
        url,
        secret,
        payload,
        timeout=max(1, int(timeout_seconds)),
    )
    if response.get("ok"):
        return _results_by_story_id(response)
    if _is_payload_too_large(response):
        if len(batch) <= 1:
            return _error_result_for_image(batch[0], response) if batch else {}
        mid = max(1, len(batch) // 2)
        out = _upload_batch(
            url=url,
            secret=secret,
            batch=batch[:mid],
            timeout_seconds=timeout_seconds,
            max_body_bytes=max_body_bytes,
        )
        out.update(
            _upload_batch(
                url=url,
                secret=secret,
                batch=batch[mid:],
                timeout_seconds=timeout_seconds,
                max_body_bytes=max_body_bytes,
            )
        )
        return out
    raise StoryImageUploadError(f"uploadStoryImages failed: {response}")


def upload_story_images(
    *,
    url: str,
    secret: str,
    images: Sequence[Mapping[str, Any]],
    batch_size: int,
    timeout_seconds: int,
    max_body_bytes: int | None = None,
) -> Dict[int, Dict[str, Any]]:
    """Upload pre-normalized PNG images and return results by story id."""

    if not images:
        return {}
    clean_url, clean_secret = _require_config(url, secret)
    out: Dict[int, Dict[str, Any]] = {}
    for batch in _planned_upload_batches(
        images,
        batch_size=batch_size,
        max_body_bytes=max_body_bytes,
    ):
        out.update(
            _upload_batch(
                url=clean_url,
                secret=clean_secret,
                batch=batch,
                timeout_seconds=timeout_seconds,
                max_body_bytes=max_body_bytes,
            )
        )
    return out


def delete_story_images(
    *,
    url: str,
    secret: str,
    file_ids: Sequence[str],
    batch_size: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    """Delete CloudBase storage files. Missing files are treated by the cloud
    function as deleted so GC can converge.
    """

    ids = [str(v).strip() for v in file_ids if str(v).strip()]
    if not ids:
        return {"deleted": [], "failed": []}
    clean_url, clean_secret = _require_config(url, secret)
    deleted: List[str] = []
    failed: List[Dict[str, Any]] = []
    for i in range(0, len(ids), max(1, int(batch_size))):
        batch = ids[i:i + max(1, int(batch_size))]
        response = cloud_push._post(
            clean_url,
            clean_secret,
            {"action": "deleteStoryImages", "fileIDs": batch},
            timeout=max(1, int(timeout_seconds)),
        )
        if not response.get("ok"):
            failed.extend({"fileID": file_id, "error": str(response)} for file_id in batch)
            continue
        deleted.extend(str(v) for v in response.get("deleted") or [])
        failed.extend(dict(v) for v in response.get("failed") or [])
    return {"deleted": deleted, "failed": failed}


__all__ = [
    "StoryImageUploadError",
    "upload_story_images",
    "delete_story_images",
]
