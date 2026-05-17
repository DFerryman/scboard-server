"""Clean dev caches and build artifacts from the repo.

Removes Python bytecode caches, tool caches, and packaging output.
Does NOT touch runtime state (data/, logs/, *.db) — those are not
build artifacts. Pass --deep to also wipe local temp test dirs.

Run::
    python -m server.scripts.clean              # clean
    python -m server.scripts.clean --dry-run    # preview only
    python -m server.scripts.clean --deep       # also temp test dirs
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

# Repo root = parent of the scripts/ dir holding this file.
REPO_ROOT = Path(__file__).resolve().parent.parent

# Directory names removed wherever they appear in the tree.
CACHE_DIR_NAMES = {
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "build",
    "dist",
}

# Glob patterns for directories matched by suffix.
CACHE_DIR_GLOBS = ("*.egg-info",)

# Glob patterns for files removed wherever they appear.
ARTIFACT_FILE_GLOBS = (
    "*.pyc",
    "*.pyo",
    "*.pyd",
    ".coverage",
    ".coverage.*",
    "coverage.xml",
)

# Extra dirs cleaned only with --deep (local debug scratch, not artifacts
# git tracks anyway, but useful to reset between runs).
DEEP_DIR_NAMES = {".tmp-tests", "tmp-tests2", ".cloud-sync-output"}

# Never descend into these (would be slow and/or destructive).
SKIP_TRAVERSAL = {".git", ".venv", "venv", "node_modules", "data", "logs"}


def _iter_targets(deep: bool):
    """Yield (path, kind) pairs to delete, bottom-up where it matters."""
    dir_names = set(CACHE_DIR_NAMES)
    if deep:
        dir_names |= DEEP_DIR_NAMES

    for path in sorted(REPO_ROOT.rglob("*"), reverse=True):
        rel_parts = path.relative_to(REPO_ROOT).parts
        if any(part in SKIP_TRAVERSAL for part in rel_parts):
            continue
        if path.is_dir():
            if path.name in dir_names:
                yield path, "dir"
            elif any(path.match(g) for g in CACHE_DIR_GLOBS):
                yield path, "dir"
        elif path.is_file():
            if any(path.match(g) for g in ARTIFACT_FILE_GLOBS):
                yield path, "file"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Clean dev caches and build artifacts."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be removed without deleting.",
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Also remove local temp test dirs (.tmp-tests, tmp-tests2, "
        ".cloud-sync-output).",
    )
    args = parser.parse_args(argv)

    removed = 0
    for path, kind in _iter_targets(args.deep):
        rel = path.relative_to(REPO_ROOT)
        if args.dry_run:
            print(f"would remove [{kind}] {rel}")
            removed += 1
            continue
        try:
            if kind == "dir":
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)
        except OSError as exc:  # pragma: no cover - defensive
            print(f"skip {rel}: {exc}", file=sys.stderr)
            continue
        print(f"removed [{kind}] {rel}")
        removed += 1

    verb = "would remove" if args.dry_run else "removed"
    print(f"\n{verb} {removed} item(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
