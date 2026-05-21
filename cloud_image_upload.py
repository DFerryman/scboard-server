"""Signed client for the story image CloudBase storage function."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence

from . import cloud_push


class StoryImageUploadError(RuntimeError):
    """Raised when the image storage cloud function rejects a request."""


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


def upload_story_images(
    *,
    url: str,
    secret: str,
    images: Sequence[Mapping[str, Any]],
    batch_size: int,
    timeout_seconds: int,
) -> Dict[int, Dict[str, Any]]:
    """Upload pre-normalized PNG images and return results by story id."""

    if not images:
        return {}
    clean_url, clean_secret = _require_config(url, secret)
    out: Dict[int, Dict[str, Any]] = {}
    for batch in _chunks(list(images), batch_size):
        response = cloud_push._post(  # Reuse the existing pinned HTTPS + HMAC path.
            clean_url,
            clean_secret,
            {"action": "uploadStoryImages", "images": batch},
            timeout=max(1, int(timeout_seconds)),
        )
        if not response.get("ok"):
            raise StoryImageUploadError(f"uploadStoryImages failed: {response}")
        for item in response.get("results") or []:
            try:
                story_id = int(item.get("storyId"))
            except (TypeError, ValueError):
                continue
            out[story_id] = dict(item)
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
