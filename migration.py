#!/usr/bin/env python3
"""Export and import hnreader runtime state.

Supported target: Ubuntu 22.04 deployments managed by ``launcher.sh``.

The archive format intentionally stores runtime state, not source code:

* local env files: .env, .env.local, .env.*.local
* DB runtime directory: the directory containing HNREADER_DB_PATH
* log directory: HNREADER_LOG_DIR
* optional generated service env: /etc/hnreader/server.env

The active SQLite DB is copied with sqlite3.Connection.backup() so the archive
contains a self-contained DB file and does not need WAL/SHM sidecars.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


ARCHIVE_FORMAT = "hnreader-migration-v1"
MANIFEST_NAME = "hnreader-migration-manifest.json"

ENV_PATTERNS = (".env", ".env.local", ".env.*.local")
SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
TRANSIENT_NAMES = {"supervisor.stop"}
TRANSIENT_SUFFIXES = (".pid", ".lock")

DANGEROUS_REMOVE_DIRS = {
    Path("/"),
    Path("/bin"),
    Path("/boot"),
    Path("/dev"),
    Path("/etc"),
    Path("/home"),
    Path("/lib"),
    Path("/lib64"),
    Path("/opt"),
    Path("/proc"),
    Path("/root"),
    Path("/run"),
    Path("/sbin"),
    Path("/srv"),
    Path("/sys"),
    Path("/tmp"),
    Path("/usr"),
    Path("/var"),
    Path("/var/lib"),
    Path("/var/log"),
    Path("/var/tmp"),
}


def _now_utc() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).isoformat(timespec="seconds")


def _detect_server_dir(project_dir: Path) -> Path:
    if (project_dir / "ingest.py").is_file() and (project_dir / "__init__.py").is_file():
        return project_dir
    candidate = project_dir / "server"
    if (candidate / "ingest.py").is_file() and (candidate / "__init__.py").is_file():
        return candidate
    raise SystemExit(
        f"cannot locate hnreader server package under {project_dir}; "
        "run from the directory containing launcher.sh"
    )


def _unquote_env_value(value: str) -> str:
    raw = value.strip()
    if len(raw) >= 2 and raw[0] == raw[-1] == "'":
        return raw[1:-1]
    if len(raw) >= 2 and raw[0] == raw[-1] == '"':
        inner = raw[1:-1]
        out: List[str] = []
        escaped = False
        for ch in inner:
            if escaped:
                out.append(ch)
                escaped = False
            elif ch == "\\":
                escaped = True
            else:
                out.append(ch)
        if escaped:
            out.append("\\")
        return "".join(out)
    return value.rstrip()


def _load_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return values
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        values[key] = _unquote_env_value(value)
    return values


def _load_launcher_env(project_dir: Path, server_dir: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for path in (project_dir / ".env.local", server_dir / ".env.local"):
        values.update(_load_env_file(path))
    return values


def _env_first(values: Mapping[str, str], *names: str) -> str:
    for name in names:
        value = values.get(name)
        if value:
            return value
    return ""


def _abs_from_project(project_dir: Path, value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return project_dir / path


def _runtime_paths(project_dir: Path, server_dir: Path) -> Dict[str, Path]:
    env = _load_launcher_env(project_dir, server_dir)
    db_path = _abs_from_project(
        project_dir,
        _env_first(env, "HNREADER_DB_PATH", "HACKERMINI_DB_PATH")
        or server_dir / "data" / "hnreader.db",
    )
    log_dir = _abs_from_project(
        project_dir,
        env.get("HNREADER_LOG_DIR") or server_dir / "logs",
    )
    cloud_sync_output_dir = _abs_from_project(
        project_dir,
        _env_first(
            env,
            "HNREADER_CLOUD_SYNC_OUTPUT_DIR",
            "HACKERMINI_CLOUD_SYNC_OUTPUT_DIR",
        )
        or db_path.with_name(".cloud-sync-output"),
    )
    alert_outbox_path = _abs_from_project(
        project_dir,
        _env_first(
            env,
            "HNREADER_ALERT_OUTBOX_PATH",
            "HACKERMINI_ALERT_OUTBOX_PATH",
        )
        or db_path.with_name("alerts.jsonl"),
    )
    ai_status_cache_path = _abs_from_project(
        project_dir,
        _env_first(
            env,
            "HNREADER_AI_CONFIG_STATUS_CACHE_PATH",
            "HACKERMINI_AI_CONFIG_STATUS_CACHE_PATH",
        )
        or db_path.with_name("ai-config-status-cache.json"),
    )
    return {
        "db_path": db_path,
        "db_dir": db_path.parent,
        "log_dir": log_dir,
        "cloud_sync_output_dir": cloud_sync_output_dir,
        "alert_outbox_path": alert_outbox_path,
        "ai_status_cache_path": ai_status_cache_path,
    }


def _runtime_ai_config_files(project_dir: Path, server_dir: Path) -> List[Path]:
    env = _load_launcher_env(project_dir, server_dir)
    raw = _env_first(env, "HNREADER_AI_CONFIG_FILE", "HACKERMINI_AI_CONFIG_FILE")
    if not raw:
        return []
    files: List[Path] = []
    for part in raw.split(os.pathsep):
        clean = part.strip()
        if clean:
            files.append(_abs_from_project(project_dir, clean))
    return files


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _target_for_path(path: Path, project_dir: Path) -> Tuple[str, str]:
    resolved = path.resolve()
    project = project_dir.resolve()
    try:
        rel = resolved.relative_to(project)
    except ValueError:
        return "absolute", str(resolved)
    return "project_relative", rel.as_posix()


def _payload_path_for(path: Path, project_dir: Path) -> str:
    target_type, target = _target_for_path(path, project_dir)
    if target_type == "project_relative":
        return f"payload/{target}"
    digest = hashlib.sha256(str(path.resolve()).encode("utf-8")).hexdigest()[:16]
    return f"payload/external/{digest}/{path.name}"


def _env_files_for_project(project_dir: Path, server_dir: Path) -> List[Path]:
    roots = [project_dir]
    if server_dir != project_dir:
        roots.append(server_dir)
    found: Dict[Path, Path] = {}
    for root in roots:
        for pattern in ENV_PATTERNS:
            for path in root.glob(pattern):
                if path.is_file():
                    found[path.resolve()] = path
    return [found[key] for key in sorted(found)]


def _is_sqlite_sidecar(path: Path) -> bool:
    return any(path.name.endswith(suffix) for suffix in SQLITE_SIDECAR_SUFFIXES)


def _is_transient_runtime_file(path: Path) -> bool:
    return path.name in TRANSIENT_NAMES or any(
        path.name.endswith(suffix) for suffix in TRANSIENT_SUFFIXES
    )


def _same_path(left: Path, right: Path) -> bool:
    try:
        return left.resolve() == right.resolve()
    except OSError:
        return left.absolute() == right.absolute()


def _is_covered_by_dirs(path: Path, dirs: Iterable[Path]) -> bool:
    for directory in dirs:
        if not directory.exists():
            continue
        if _same_path(path, directory) or _is_relative_to(path, directory):
            return True
    return False


def _sqlite_backup(src_path: Path, dst_path: Path) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    src: Optional[sqlite3.Connection] = None
    dst: Optional[sqlite3.Connection] = None
    try:
        src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
        dst = sqlite3.connect(str(dst_path))
        src.backup(dst)
        dst.execute("PRAGMA journal_mode = DELETE").fetchall()
    except sqlite3.Error as exc:
        raise SystemExit(f"failed to snapshot SQLite DB {src_path}: {exc}") from exc
    finally:
        if src is not None:
            src.close()
        if dst is not None:
            dst.close()

    for suffix in ("-wal", "-shm"):
        sidecar = dst_path.with_name(dst_path.name + suffix)
        try:
            sidecar.unlink()
        except FileNotFoundError:
            pass

    error = _sqlite_integrity_error(dst_path)
    if error:
        try:
            dst_path.unlink()
        except FileNotFoundError:
            pass
        raise SystemExit(f"snapshot integrity_check failed for {src_path}: {error}")


def _sqlite_integrity_error(path: Path) -> str:
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
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
    return "; ".join(str(row[0]) for row in rows if row)


def _copy_tree_filtered(
    src_dir: Path,
    dst_dir: Path,
    *,
    active_db: Optional[Path],
    exclude_paths: Iterable[Path],
) -> List[Dict[str, str]]:
    copied: List[Dict[str, str]] = []
    excluded = [path.resolve() for path in exclude_paths]
    if not src_dir.exists():
        return copied

    dst_dir.mkdir(parents=True, exist_ok=True)
    for src in sorted(src_dir.rglob("*")):
        try:
            resolved = src.resolve()
        except OSError:
            continue
        if any(resolved == item or _is_relative_to(resolved, item) for item in excluded):
            copied.append({"path": str(src), "reason": "archive-output"})
            continue
        rel = src.relative_to(src_dir)
        dst = dst_dir / rel
        if src.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
            continue
        if src.is_symlink():
            copied.append({"path": str(src), "reason": "symlink"})
            continue
        if active_db is not None and _same_path(src, active_db):
            copied.append({"path": str(src), "reason": "active-db-snapshot"})
            continue
        if _is_sqlite_sidecar(src):
            copied.append({"path": str(src), "reason": "sqlite-sidecar"})
            continue
        if _is_transient_runtime_file(src):
            copied.append({"path": str(src), "reason": "transient-runtime"})
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    return copied


def _write_json(path: Path, data: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def export_archive(
    archive_path: str | Path,
    *,
    project_dir: str | Path = ".",
    service_env_file: str | Path = "/etc/hnreader/server.env",
    include_service_env: bool = False,
) -> Dict[str, Any]:
    project = Path(project_dir).resolve()
    server = _detect_server_dir(project)
    archive = Path(archive_path).resolve()
    runtime = _runtime_paths(project, server)
    db_path = runtime["db_path"]
    db_dir = runtime["db_dir"]
    log_dir = runtime["log_dir"]
    service_env = Path(service_env_file)

    if archive.exists():
        raise SystemExit(f"refuse to overwrite existing archive: {archive}")
    archive.parent.mkdir(parents=True, exist_ok=True)

    entries: List[Dict[str, Any]] = []
    excludes: List[Dict[str, str]] = []

    with tempfile.TemporaryDirectory(prefix="hnreader-migration-export-") as tmp:
        staging = Path(tmp)
        payload = staging / "payload"
        payload.mkdir()

        for env_file in _env_files_for_project(project, server):
            target_type, target = _target_for_path(env_file, project)
            archive_member = _payload_path_for(env_file, project)
            staged = staging / archive_member
            staged.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(env_file, staged)
            entries.append(
                {
                    "kind": "env_file",
                    "archive_path": archive_member,
                    "target_type": target_type,
                    "target": target,
                }
            )

        if db_dir.exists():
            data_member = _payload_path_for(db_dir, project)
            data_staged = staging / data_member
            excludes.extend(
                _copy_tree_filtered(
                    db_dir,
                    data_staged,
                    active_db=db_path if db_path.exists() else None,
                    exclude_paths=[archive],
                )
            )
            if db_path.exists():
                if _is_relative_to(db_path, db_dir):
                    db_rel = db_path.resolve().relative_to(db_dir.resolve())
                    staged_db = data_staged / db_rel
                else:
                    db_rel = Path(db_path.name)
                    staged_db = staging / _payload_path_for(db_path, project)
                _sqlite_backup(db_path, staged_db)
            target_type, target = _target_for_path(db_dir, project)
            entries.append(
                {
                    "kind": "data_dir",
                    "archive_path": data_member,
                    "target_type": target_type,
                    "target": target,
                    "active_db_name": db_path.name,
                    "active_db_relative_path": db_rel.as_posix()
                    if db_path.exists()
                    else "",
                }
            )

        if log_dir.exists():
            log_member = _payload_path_for(log_dir, project)
            log_staged = staging / log_member
            if not _same_path(log_dir, db_dir):
                shutil.copytree(log_dir, log_staged, dirs_exist_ok=True)
                target_type, target = _target_for_path(log_dir, project)
                entries.append(
                    {
                        "kind": "log_dir",
                        "archive_path": log_member,
                        "target_type": target_type,
                        "target": target,
                    }
                )

        covered_dirs = [path for path in (db_dir, log_dir) if path.exists()]
        cloud_sync_output_dir = runtime["cloud_sync_output_dir"]
        if cloud_sync_output_dir.exists() and not _is_covered_by_dirs(
            cloud_sync_output_dir, covered_dirs
        ):
            cloud_member = _payload_path_for(cloud_sync_output_dir, project)
            shutil.copytree(
                cloud_sync_output_dir,
                staging / cloud_member,
                dirs_exist_ok=True,
            )
            target_type, target = _target_for_path(cloud_sync_output_dir, project)
            entries.append(
                {
                    "kind": "cloud_sync_output_dir",
                    "archive_path": cloud_member,
                    "target_type": target_type,
                    "target": target,
                }
            )
            covered_dirs.append(cloud_sync_output_dir)

        extra_file_paths = [
            ("alert_outbox_file", runtime["alert_outbox_path"]),
            ("ai_status_cache_file", runtime["ai_status_cache_path"]),
        ]
        for ai_config_file in _runtime_ai_config_files(project, server):
            if include_service_env or not _same_path(ai_config_file, service_env):
                extra_file_paths.append(("ai_config_file", ai_config_file))

        seen_extra_files: set[str] = set()
        for kind, file_path in extra_file_paths:
            if not file_path.is_file():
                continue
            resolved_key = str(file_path.resolve())
            if resolved_key in seen_extra_files:
                continue
            seen_extra_files.add(resolved_key)
            if _is_covered_by_dirs(file_path, covered_dirs):
                continue
            file_member = _payload_path_for(file_path, project)
            staged = staging / file_member
            staged.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file_path, staged)
            target_type, target = _target_for_path(file_path, project)
            entries.append(
                {
                    "kind": kind,
                    "archive_path": file_member,
                    "target_type": target_type,
                    "target": target,
                }
            )

        if include_service_env and service_env.is_file():
            service_member = "payload/service-env/server.env"
            staged = staging / service_member
            staged.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(service_env, staged)
            entries.append(
                {
                    "kind": "service_env_file",
                    "archive_path": service_member,
                    "target_type": "absolute",
                    "target": str(service_env.resolve()),
                }
            )

        manifest: Dict[str, Any] = {
            "format": ARCHIVE_FORMAT,
            "created_at": _now_utc(),
            "source": {
                "project_dir": str(project),
                "server_dir": str(server),
                "db_path": str(db_path),
                "db_dir": str(db_dir),
                "log_dir": str(log_dir),
                "service_env_file": str(service_env),
            },
            "entries": entries,
            "excluded_runtime_files": excludes,
            "notes": [
                "SQLite WAL/SHM sidecars are excluded because the active DB is backed up as a self-contained file.",
                "PID, lock, and supervisor stop files are excluded because they are process-local transient state.",
            ],
        }
        _write_json(staging / MANIFEST_NAME, manifest)

        with tarfile.open(archive, "w:gz") as tf:
            tf.add(staging / MANIFEST_NAME, arcname=MANIFEST_NAME)
            for child in sorted(payload.iterdir()):
                tf.add(child, arcname=f"payload/{child.name}")
        try:
            os.chmod(archive, 0o600)
        except OSError:
            pass

    print(f"Exported migration archive: {archive}")
    return manifest


def _read_manifest_from_archive(archive_path: Path) -> Dict[str, Any]:
    with tarfile.open(archive_path, "r:gz") as tf:
        try:
            member = tf.getmember(MANIFEST_NAME)
        except KeyError as exc:
            raise SystemExit(f"archive is missing {MANIFEST_NAME}") from exc
        extracted = tf.extractfile(member)
        if extracted is None:
            raise SystemExit(f"archive manifest is unreadable: {MANIFEST_NAME}")
        manifest = json.loads(extracted.read().decode("utf-8"))
    if manifest.get("format") != ARCHIVE_FORMAT:
        raise SystemExit(
            f"unsupported archive format: {manifest.get('format')!r}; "
            f"expected {ARCHIVE_FORMAT}"
        )
    return manifest


def _safe_extract(tf: tarfile.TarFile, target_dir: Path) -> None:
    root = target_dir.resolve()
    for member in tf.getmembers():
        member_path = root / member.name
        try:
            member_path.resolve().relative_to(root)
        except ValueError as exc:
            raise SystemExit(f"unsafe archive member path: {member.name}") from exc
        if member.name.startswith("/") or ".." in Path(member.name).parts:
            raise SystemExit(f"unsafe archive member path: {member.name}")
        if member.isdir():
            member_path.mkdir(parents=True, exist_ok=True)
            continue
        if not member.isfile():
            raise SystemExit(f"unsupported archive member type: {member.name}")
        member_path.parent.mkdir(parents=True, exist_ok=True)
        src = tf.extractfile(member)
        if src is None:
            raise SystemExit(f"archive member is unreadable: {member.name}")
        with src, member_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)


def _verify_extracted_payload(manifest: Mapping[str, Any], extracted_root: Path) -> None:
    for entry in manifest.get("entries", []):
        if entry.get("kind") != "data_dir":
            continue
        active_db_relative = (
            entry.get("active_db_relative_path")
            or entry.get("active_db_name")
            or ""
        )
        if not active_db_relative:
            continue
        db_path = extracted_root / str(entry["archive_path"]) / str(active_db_relative)
        if not db_path.exists():
            raise SystemExit(
                f"archive active DB is missing: {entry['archive_path']}/{active_db_relative}"
            )
        error = _sqlite_integrity_error(db_path)
        if error:
            raise SystemExit(
                f"archive active DB integrity_check failed for "
                f"{entry['archive_path']}/{active_db_relative}: {error}"
            )


def _archived_env_values(
    manifest: Mapping[str, Any],
    extracted_root: Path,
) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for entry in manifest.get("entries", []):
        if entry.get("kind") != "env_file":
            continue
        env_path = extracted_root / str(entry["archive_path"])
        values.update(_load_env_file(env_path))
    return values


def _allowed_absolute_targets_from_archived_env(
    manifest: Mapping[str, Any],
    extracted_root: Path,
    *,
    project_dir: Path,
    service_env_file: Path,
    include_service_env: bool,
) -> set[Path]:
    env = _archived_env_values(manifest, extracted_root)
    allowed: set[Path] = set()

    db_value = _env_first(env, "HNREADER_DB_PATH", "HACKERMINI_DB_PATH")
    if db_value:
        allowed.add(_abs_from_project(project_dir, db_value).parent.resolve())

    log_value = env.get("HNREADER_LOG_DIR") or ""
    if log_value:
        allowed.add(_abs_from_project(project_dir, log_value).resolve())

    cloud_value = _env_first(
        env,
        "HNREADER_CLOUD_SYNC_OUTPUT_DIR",
        "HACKERMINI_CLOUD_SYNC_OUTPUT_DIR",
    )
    if cloud_value:
        allowed.add(_abs_from_project(project_dir, cloud_value).resolve())

    alert_value = _env_first(
        env,
        "HNREADER_ALERT_OUTBOX_PATH",
        "HACKERMINI_ALERT_OUTBOX_PATH",
    )
    if alert_value:
        allowed.add(_abs_from_project(project_dir, alert_value).resolve())

    cache_value = _env_first(
        env,
        "HNREADER_AI_CONFIG_STATUS_CACHE_PATH",
        "HACKERMINI_AI_CONFIG_STATUS_CACHE_PATH",
    )
    if cache_value:
        allowed.add(_abs_from_project(project_dir, cache_value).resolve())

    ai_config_value = _env_first(
        env,
        "HNREADER_AI_CONFIG_FILE",
        "HACKERMINI_AI_CONFIG_FILE",
    )
    if ai_config_value:
        for part in ai_config_value.split(os.pathsep):
            clean = part.strip()
            if not clean:
                continue
            path = _abs_from_project(project_dir, clean).resolve()
            if include_service_env or path != service_env_file.resolve():
                allowed.add(path)

    if include_service_env:
        allowed.add(service_env_file.resolve())
    return allowed


def _confirm_destructive(archive: Path, project: Path) -> None:
    print("Import is destructive.")
    print(f"Archive: {archive}")
    print(f"Project: {project}")
    print("Managed env/data/log targets will be removed before import.")
    answer = input("Type IMPORT to continue: ").strip()
    if answer != "IMPORT":
        raise SystemExit("aborted")


def _run_root(cmd: Sequence[str]) -> int:
    if os.name != "posix":
        return 0
    if os.geteuid() == 0:
        full_cmd = list(cmd)
    else:
        full_cmd = ["sudo", *cmd]
    try:
        completed = subprocess.run(full_cmd, check=False)
    except FileNotFoundError:
        return 127
    return int(completed.returncode)


def _systemctl_unit_exists(service: str) -> bool:
    try:
        completed = subprocess.run(
            ["systemctl", "cat", service],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        return False
    return completed.returncode == 0


def _stop_services(service_prefix: str) -> None:
    if os.name != "posix":
        return
    if shutil.which("systemctl") is None:
        return
    for service in (
        f"{service_prefix}-ingest.service",
        f"{service_prefix}-db-init.service",
    ):
        if not _systemctl_unit_exists(service):
            continue
        rc = _run_root(["systemctl", "stop", service])
        if rc != 0:
            raise SystemExit(f"failed to stop {service}; aborting import")


def _assert_safe_remove_dir(path: Path, *, project_dir: Path) -> None:
    resolved = path.resolve()
    if resolved == project_dir.resolve():
        raise SystemExit(f"refuse to remove project directory itself: {resolved}")
    for dangerous in DANGEROUS_REMOVE_DIRS:
        if resolved == dangerous:
            raise SystemExit(f"refuse to remove protected directory: {resolved}")


def _remove_file(path: Path) -> None:
    try:
        if path.is_file() or path.is_symlink():
            path.unlink()
    except FileNotFoundError:
        return


def _remove_dir(path: Path, *, project_dir: Path) -> None:
    if not path.exists():
        return
    if not path.is_dir() or path.is_symlink():
        _remove_file(path)
        return
    _assert_safe_remove_dir(path, project_dir=project_dir)
    shutil.rmtree(path)


def _target_path(
    entry: Mapping[str, Any],
    *,
    project_dir: Path,
    service_env_file: Path,
) -> Path:
    if entry.get("kind") == "service_env_file":
        return service_env_file
    target_type = entry.get("target_type")
    target = str(entry.get("target") or "")
    if target_type == "project_relative":
        return project_dir / target
    if target_type == "absolute":
        return Path(target)
    raise SystemExit(f"invalid manifest target entry: {entry}")


def _copy_payload(src: Path, dst: Path) -> None:
    if src.is_dir():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dst, dirs_exist_ok=True)
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _apply_import_mode(kind: str, target: Path) -> None:
    if os.name != "posix" or not target.is_file():
        return
    mode: Optional[int] = None
    if kind in {"env_file", "ai_config_file"}:
        mode = 0o600
    elif kind == "service_env_file":
        mode = 0o640
    if mode is None:
        return
    try:
        os.chmod(target, mode)
    except OSError:
        pass


def _entry_is_dir(entry: Mapping[str, Any], extracted_root: Path) -> bool:
    src = extracted_root / str(entry["archive_path"])
    return src.is_dir()


def import_archive(
    archive_path: str | Path,
    *,
    project_dir: str | Path = ".",
    service_env_file: str | Path = "/etc/hnreader/server.env",
    include_service_env: bool = False,
    assume_yes: bool = False,
    stop_services: bool = True,
    service_prefix: str = "hnreader",
) -> Dict[str, Any]:
    project = Path(project_dir).resolve()
    server = _detect_server_dir(project)
    archive = Path(archive_path).resolve()
    service_env = Path(service_env_file)
    if not archive.is_file():
        raise SystemExit(f"archive not found: {archive}")

    manifest = _read_manifest_from_archive(archive)
    if not assume_yes:
        _confirm_destructive(archive, project)
    if stop_services:
        _stop_services(service_prefix)

    with tempfile.TemporaryDirectory(prefix="hnreader-migration-import-") as tmp:
        extracted_root = Path(tmp)
        with tarfile.open(archive, "r:gz") as tf:
            _safe_extract(tf, extracted_root)
        _verify_extracted_payload(manifest, extracted_root)
        allowed_absolute_targets = _allowed_absolute_targets_from_archived_env(
            manifest,
            extracted_root,
            project_dir=project,
            service_env_file=service_env,
            include_service_env=include_service_env,
        )

        current_runtime = _runtime_paths(project, server)
        env_targets = _env_files_for_project(project, server)
        current_ai_config_files = [
            path
            for path in _runtime_ai_config_files(project, server)
            if include_service_env or not _same_path(path, service_env)
        ]
        dir_targets = {
            current_runtime["db_dir"].resolve(),
            current_runtime["log_dir"].resolve(),
            current_runtime["cloud_sync_output_dir"].resolve(),
        }
        file_targets = {
            path.resolve()
            for path in [
                *env_targets,
                current_runtime["alert_outbox_path"],
                current_runtime["ai_status_cache_path"],
                *current_ai_config_files,
            ]
        }
        if include_service_env:
            file_targets.add(service_env.resolve())

        for entry in manifest.get("entries", []):
            if entry.get("kind") == "service_env_file" and not include_service_env:
                continue
            target = _target_path(
                entry, project_dir=project, service_env_file=service_env
            )
            if (
                entry.get("target_type") == "absolute"
                and entry.get("kind") != "service_env_file"
                and target.resolve() not in allowed_absolute_targets
            ):
                raise SystemExit(
                    f"manifest absolute target is not declared by archived env: {target}"
                )
            if str(entry.get("kind") or "").endswith("_dir"):
                dir_targets.add(target.resolve())
            else:
                file_targets.add(target.resolve())

        for file_target in sorted(file_targets):
            _remove_file(file_target)
        for dir_target in sorted(
            dir_targets, key=lambda path: len(str(path)), reverse=True
        ):
            _remove_dir(dir_target, project_dir=project)

        imported: List[str] = []
        for entry in manifest.get("entries", []):
            if entry.get("kind") == "service_env_file" and not include_service_env:
                continue
            src = extracted_root / str(entry["archive_path"])
            if not src.exists():
                raise SystemExit(f"archive payload missing: {entry['archive_path']}")
            dst = _target_path(entry, project_dir=project, service_env_file=service_env)
            if _entry_is_dir(entry, extracted_root):
                _remove_dir(dst, project_dir=project)
            else:
                _remove_file(dst)
            _copy_payload(src, dst)
            _apply_import_mode(str(entry.get("kind") or ""), dst)
            imported.append(str(dst))

    print("Imported migration archive.")
    print("Next step: run bash launcher.sh restart")
    result = dict(manifest)
    result["imported_targets"] = imported
    return result


def inspect_archive(archive_path: str | Path) -> Dict[str, Any]:
    archive = Path(archive_path).resolve()
    manifest = _read_manifest_from_archive(archive)
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return manifest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export/import hnreader runtime migration archives."
    )
    parser.add_argument(
        "--project-dir",
        default=".",
        help="project directory containing launcher.sh (default: current directory)",
    )
    parser.add_argument(
        "--service-env-file",
        default="/etc/hnreader/server.env",
        help="generated systemd env file path (default: /etc/hnreader/server.env)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_export = sub.add_parser("export", help="create a migration archive")
    p_export.add_argument("archive", help="output .tar.gz path; must not exist")
    p_export.add_argument(
        "--include-service-env",
        action="store_true",
        help="also include the generated service env file",
    )

    p_import = sub.add_parser("import", help="destructively import an archive")
    p_import.add_argument("archive", help="input archive created by export")
    p_import.add_argument(
        "--include-service-env",
        action="store_true",
        help="also replace the generated service env file from the archive",
    )
    p_import.add_argument(
        "--yes",
        action="store_true",
        help="skip the IMPORT confirmation prompt",
    )
    p_import.add_argument(
        "--no-stop-services",
        action="store_true",
        help="do not try to stop hnreader systemd services before import",
    )
    p_import.add_argument(
        "--service-prefix",
        default="hnreader",
        help="systemd service prefix to stop during import (default: hnreader)",
    )

    p_inspect = sub.add_parser("inspect", help="print archive manifest")
    p_inspect.add_argument("archive", help="input archive to inspect")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "export":
        export_archive(
            args.archive,
            project_dir=args.project_dir,
            service_env_file=args.service_env_file,
            include_service_env=args.include_service_env,
        )
    elif args.cmd == "import":
        import_archive(
            args.archive,
            project_dir=args.project_dir,
            service_env_file=args.service_env_file,
            include_service_env=args.include_service_env,
            assume_yes=args.yes,
            stop_services=not args.no_stop_services,
            service_prefix=args.service_prefix,
        )
    elif args.cmd == "inspect":
        inspect_archive(args.archive)
    else:
        parser.print_help()
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
