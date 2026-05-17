"""SQLite online backup + restore for hnreader.

Wired up to ``server/launcher.sh backup`` and ``... restore``. Online backup
runs via ``sqlite3.Connection.backup`` so it is safe under WAL: it never
blocks ingest, and the snapshot is a self-consistent copy at the moment the
backup completes (writes during the copy land in the next backup, not a
half-state).

Restore expects ingest to be stopped by the caller (launcher does this); it
verifies the snapshot's ``PRAGMA integrity_check`` before swapping the live
file, and moves the previous DB aside as ``hnreader.db.pre-restore-<ts>.bak``
so the operator can roll back if the chosen snapshot turns out to be wrong.
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional, Sequence

from . import settings


def _integrity_check(db_path: Path) -> str:
    """Return empty string when the DB is healthy, else a human-readable error.

    A garbage file (one that isn't even a SQLite database) raises
    ``sqlite3.DatabaseError`` from ``PRAGMA integrity_check`` instead of
    returning rows; we collapse both failure modes into a single string so
    callers only need one branch.
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        return f"cannot open: {exc}"
    try:
        try:
            rows = list(conn.execute("PRAGMA integrity_check"))
        except sqlite3.DatabaseError as exc:
            return f"not a SQLite database: {exc}"
    finally:
        conn.close()
    if len(rows) == 1 and rows[0] and rows[0][0] == "ok":
        return ""
    return "; ".join(str(r[0]) for r in rows if r)


def backup(dst_path: Path, *, src_path: Path) -> None:
    if not src_path.is_file():
        raise SystemExit(f"[backup] source DB not found: {src_path}")
    if dst_path.exists():
        raise SystemExit(
            f"[backup] refuse to overwrite existing file: {dst_path}"
        )
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    src: Optional[sqlite3.Connection] = None
    dst: Optional[sqlite3.Connection] = None
    try:
        src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
        dst = sqlite3.connect(str(dst_path))
        src.backup(dst)
        # Backup inherits the source's WAL journal mode, which leaves
        # ``<dst>-wal`` / ``<dst>-shm`` sidecars on disk after close. Switch
        # the snapshot to DELETE so the operator carries one self-contained
        # ``.db`` file around (and integrity_check below opens a clean file).
        dst.execute("PRAGMA journal_mode = DELETE").fetchall()
    except sqlite3.Error as exc:
        # Source file existed but couldn't be read (corrupt / wrong format /
        # truncated mid-write). Translate to SystemExit so callers (incl.
        # ``restore``'s rollback step) can handle "snapshot impossible"
        # without sprinkling sqlite3.* catches everywhere. Tear down the
        # half-written destination so we don't leave a fake snapshot.
        try:
            if dst is not None:
                dst.close()
        finally:
            dst = None
        for stale in (dst_path,
                      dst_path.with_name(dst_path.name + "-wal"),
                      dst_path.with_name(dst_path.name + "-shm")):
            try:
                stale.unlink()
            except OSError:
                pass
        raise SystemExit(
            f"[backup] failed to read {src_path}: {exc}"
        ) from exc
    finally:
        if src is not None:
            src.close()
        if dst is not None:
            dst.close()

    error = _integrity_check(dst_path)
    if error:
        try:
            dst_path.unlink()
        except OSError:
            pass
        raise SystemExit(
            f"[backup] integrity_check failed on snapshot; removed file: {error}"
        )

    size = dst_path.stat().st_size
    print(f"[backup] OK src={src_path} dst={dst_path} size={size}B")


def restore(src_path: Path, *, dst_path: Path) -> None:
    """Replace ``dst_path`` with the verified contents of ``src_path``.

    Ordering matters for crash safety:

      1. Verify the snapshot **before** touching the live DB so a corrupt
         file can never replace a healthy one.
      2. Take a self-contained online-backup snapshot of the current live DB
         as the rollback target. ``Path.rename`` of the live ``.db`` is not
         enough — it leaves the previous WAL sidecar behind and the renamed
         file may not be checkpoint-coherent, so a rollback could read stale
         state. ``backup()`` produces a DELETE-mode standalone file that is
         always consistent.
      3. Stage the new DB next to the live path as ``<name>.restoring-*.tmp``,
         re-verify it (catches mid-flight disk corruption), then call
         ``os.replace`` for an atomic swap. ``os.replace`` is atomic on POSIX
         and Windows, so the live path either points to the OLD file or the
         NEW file at every observable instant — never to nothing, even if the
         process is killed between steps.
      4. Remove the previous DB's ``-wal`` / ``-shm`` sidecars; the new file
         is DELETE-mode and the sidecars are now orphans referencing a file
         identity SQLite no longer recognizes.
    """
    if not src_path.is_file():
        raise SystemExit(f"[restore] backup file not found: {src_path}")

    error = _integrity_check(src_path)
    if error:
        raise SystemExit(
            f"[restore] refuse to restore: integrity_check failed on "
            f"{src_path}: {error}"
        )

    # ``time.strftime`` is second-grained; back-to-back restores in the same
    # second would otherwise collide on the rollback filename and silently
    # trip backup()'s "refuse to overwrite" guard. Mix the pid in so two
    # invocations cannot clash even when launched concurrently or in a tight
    # retry loop.
    ts = f"{time.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}"
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    rollback_path: Optional[Path] = None
    if dst_path.exists():
        # Probe the live DB BEFORE deciding rollback policy:
        #   - healthy live  → rollback is mandatory. If we can't snapshot
        #     it (perm/disk/whatever), refuse to proceed; otherwise the
        #     operator loses the only good copy without warning.
        #   - corrupt live  → there is nothing meaningful to "roll back to"
        #     anyway. Save a best-effort byte copy as forensic evidence,
        #     but don't block the restore on it — the operator is
        #     presumably running restore *because* the live file is bad.
        live_error = _integrity_check(dst_path)
        if not live_error:
            candidate = dst_path.with_name(
                dst_path.name + f".pre-restore-{ts}.bak"
            )
            try:
                backup(candidate, src_path=dst_path)
            except SystemExit as exc:
                raise SystemExit(
                    f"[restore] refusing to proceed: live DB is healthy but "
                    f"rollback snapshot failed ({exc}); resolve the snapshot "
                    f"error and retry"
                ) from exc
            rollback_path = candidate
            print(f"[restore] rollback snapshot: {rollback_path}")
        else:
            evidence = dst_path.with_name(
                dst_path.name + f".pre-restore-{ts}.corrupt.bak"
            )
            try:
                shutil.copyfile(dst_path, evidence)
            except OSError as exc:
                print(
                    f"[restore] WARNING live DB is corrupt ({live_error}); "
                    f"could not save evidence copy: {exc}"
                )
            else:
                print(
                    f"[restore] live DB is corrupt ({live_error}); saved "
                    f"forensic copy: {evidence}"
                )

    tmp_path = dst_path.with_name(dst_path.name + f".restoring-{ts}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    try:
        # ``shutil.copyfile`` (not ``copy``/``copy2``) intentionally drops the
        # source's permission bits. A 0444 readonly backup file would
        # otherwise carry over and leave ingest unable to write to live.db
        # — the failure only surfaces on next service restart.
        shutil.copyfile(src_path, tmp_path)
        staged_error = _integrity_check(tmp_path)
        if staged_error:
            raise SystemExit(
                f"[restore] staged copy failed integrity_check: {staged_error}"
            )
        # ``os.replace`` is documented atomic on POSIX and Windows; live path
        # either points to the OLD file or the NEW file at every instant.
        os.replace(tmp_path, dst_path)
    except SystemExit:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise
    except OSError as exc:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise SystemExit(f"[restore] atomic replace failed: {exc}") from exc

    # Reassert the production mode (rw for owner, r for group/other) even if
    # the staged file inherited a tighter umask. systemd's ingest service
    # owner needs write access; we don't want the next ingest restart to be
    # the moment we learn the mode is wrong.
    try:
        os.chmod(dst_path, 0o644)
    except OSError as exc:
        print(f"[restore] WARNING could not set mode 0o644 on {dst_path}: {exc}")

    print(f"[restore] OK src={src_path} dst={dst_path}")

    for suffix in ("-wal", "-shm"):
        sidecar = dst_path.with_name(dst_path.name + suffix)
        if sidecar.exists():
            sidecar.unlink()
            print(f"[restore] removed stale {sidecar.name}")

    if rollback_path is not None:
        print(f"[restore] rollback available at: {rollback_path}")
    print("[restore] start the service with: bash server/launcher.sh start")


def verify(db_path: Path) -> None:
    if not db_path.is_file():
        raise SystemExit(f"[verify] DB file not found: {db_path}")
    error = _integrity_check(db_path)
    if error:
        raise SystemExit(f"[verify] integrity_check FAILED: {error}")
    print(f"[verify] OK {db_path}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m server.db_backup",
        description="SQLite online backup / restore / verify for hnreader.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_backup = sub.add_parser("backup", help="online backup of HNREADER_DB_PATH")
    p_backup.add_argument(
        "dst",
        help="output backup file path; must not exist",
    )

    p_restore = sub.add_parser(
        "restore", help="replace HNREADER_DB_PATH from a backup file"
    )
    p_restore.add_argument(
        "src",
        help="input backup file path (must pass PRAGMA integrity_check)",
    )

    sub.add_parser(
        "verify", help="run PRAGMA integrity_check on HNREADER_DB_PATH"
    )

    args = parser.parse_args(argv)
    db_path = settings.get_db_path()

    if args.cmd == "backup":
        backup(Path(args.dst), src_path=db_path)
    elif args.cmd == "restore":
        restore(Path(args.src), dst_path=db_path)
    elif args.cmd == "verify":
        verify(db_path)
    else:  # argparse already enforces this
        parser.print_help()
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
