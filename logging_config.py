"""Central logging setup for server CLIs."""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import date, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


DEFAULT_LOG_DIR = Path(__file__).resolve().parent / "logs"
DEFAULT_LOG_NAME = "server.log"
DEFAULT_RETENTION_DAYS = 30
LOG_DIR_ENV = "HNREADER_LOG_DIR"

_FILE_HANDLER_MARKER = "_hnreader_daily_file_handler"
_STREAM_HANDLER_MARKER = "_hnreader_stream_handler"
_ROTATED_LOG_RE = re.compile(r"^server\.log\.(\d{4}-\d{2}-\d{2})$")


def _cleanup_old_rotated_logs(
    log_dir: Path,
    *,
    retention_days: int,
    today: date | None = None,
) -> None:
    cutoff = (today or date.today()) - timedelta(days=retention_days)
    for path in log_dir.glob(f"{DEFAULT_LOG_NAME}.*"):
        match = _ROTATED_LOG_RE.match(path.name)
        if match is None:
            continue
        try:
            log_day = date.fromisoformat(match.group(1))
        except ValueError:
            continue
        if log_day < cutoff:
            try:
                path.unlink()
            except OSError:
                logging.getLogger(__name__).warning(
                    "failed to remove old log file %s", path
                )


def _remove_marked_file_handlers(root: logging.Logger) -> None:
    for handler in list(root.handlers):
        if getattr(handler, _FILE_HANDLER_MARKER, False):
            root.removeHandler(handler)
            handler.close()


def _ensure_stream_handler(root: logging.Logger, formatter: logging.Formatter) -> None:
    for handler in root.handlers:
        if getattr(handler, _STREAM_HANDLER_MARKER, False):
            handler.setFormatter(formatter)
            return

    stream_handler = logging.StreamHandler(sys.stderr)
    setattr(stream_handler, _STREAM_HANDLER_MARKER, True)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)


def configure_logging(
    *,
    verbose: bool = False,
    log_dir: str | Path | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
) -> Path:
    """Configure console logging plus a daily rotating server log file.

    The active file is ``server.log``. At local midnight the standard library
    rotating handler rolls it to ``server.log.YYYY-MM-DD`` and opens a fresh
    active file. Rotated files older than ``retention_days`` are removed.
    """
    if retention_days < 1:
        raise ValueError("retention_days must be >= 1")

    resolved_log_dir = (
        Path(log_dir)
        if log_dir is not None
        else Path(os.environ.get(LOG_DIR_ENV) or DEFAULT_LOG_DIR)
    )
    resolved_log_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_old_rotated_logs(
        resolved_log_dir,
        retention_days=retention_days,
    )

    level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    root = logging.getLogger()
    root.setLevel(level)

    _ensure_stream_handler(root, formatter)
    _remove_marked_file_handlers(root)

    log_path = resolved_log_dir / DEFAULT_LOG_NAME
    file_handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
        delay=True,
        utc=False,
    )
    file_handler.suffix = "%Y-%m-%d"
    setattr(file_handler, _FILE_HANDLER_MARKER, True)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    return log_path


__all__ = [
    "DEFAULT_LOG_DIR",
    "DEFAULT_LOG_NAME",
    "DEFAULT_RETENTION_DAYS",
    "LOG_DIR_ENV",
    "configure_logging",
]
