"""Read-only Codex CLI JSON completion adapter."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from . import settings


class CodexCliError(RuntimeError):
    """Raised when the local Codex CLI path cannot produce valid JSON."""


_CODEX_REASONING_EFFORTS = frozenset(("minimal", "low", "medium", "high", "xhigh"))


def _normalize_reasoning_effort(value: Optional[str]) -> Optional[str]:
    effort = str(value or "").strip().lower()
    if not effort:
        return None
    if effort not in _CODEX_REASONING_EFFORTS:
        allowed = ", ".join(sorted(_CODEX_REASONING_EFFORTS))
        raise CodexCliError(
            f"invalid Codex reasoning effort {value!r}; expected one of: {allowed}"
        )
    return effort


def _is_executable_file(path: Path) -> bool:
    try:
        return path.is_file() and os.access(path, os.X_OK)
    except OSError:
        return False


def _path_with_extra(extra_path: str = "") -> str:
    base = os.environ.get("PATH", "")
    extra = str(extra_path or "").strip()
    if not extra:
        return base
    if not base:
        return extra
    return extra + os.pathsep + base


def _native_codex_candidates(wrapper: Path) -> Sequence[Path]:
    try:
        real = wrapper.resolve()
    except OSError:
        real = wrapper
    package_root = real.parent.parent if real.parent.name == "bin" else real.parent
    native_root = package_root / "node_modules" / "@openai"
    if not native_root.is_dir():
        return ()
    return tuple(sorted(native_root.glob("codex-*/vendor/*/codex/codex")))


def _looks_like_node_wrapper(path: Path) -> bool:
    try:
        with path.open("rb") as fh:
            head = fh.read(256)
    except OSError:
        return False
    return b"/usr/bin/env node" in head or b"node" in head.splitlines()[:1]


def _prefer_native_codex_binary(path: str) -> str:
    candidate = Path(path)
    if not _looks_like_node_wrapper(candidate):
        return path
    for native in _native_codex_candidates(candidate):
        if _is_executable_file(native):
            return str(native)
    return path


def _which_all(executable: str, *, path: str) -> Sequence[str]:
    matches = []
    seen = set()
    for directory in path.split(os.pathsep):
        if not directory:
            continue
        candidate = Path(directory) / executable
        raw = str(candidate)
        if raw in seen:
            continue
        seen.add(raw)
        if _is_executable_file(candidate):
            matches.append(raw)
    return tuple(matches)


def resolve_codex_executable(executable: str = "codex", *, extra_path: str = "") -> str:
    configured = str(executable or "codex").strip() or "codex"
    if configured != "codex":
        resolved = shutil.which(configured, path=_path_with_extra(extra_path))
        if resolved:
            return _prefer_native_codex_binary(resolved)
        return _prefer_native_codex_binary(configured)

    search_path = _path_with_extra(extra_path)
    resolved_candidates = list(_which_all(configured, path=search_path))
    resolved = shutil.which(configured, path=search_path)
    if resolved and resolved not in resolved_candidates:
        resolved_candidates.insert(0, resolved)
    for candidate in resolved_candidates:
        preferred = _prefer_native_codex_binary(candidate)
        if preferred != candidate:
            return preferred
    if resolved_candidates:
        return resolved_candidates[0]

    home = Path.home()
    candidates = [
        home / ".local" / "bin" / "codex",
        home / ".npm-global" / "bin" / "codex",
        home / ".bun" / "bin" / "codex",
    ]
    if os.name == "nt":
        local_appdata = os.environ.get("LOCALAPPDATA")
        appdata = os.environ.get("APPDATA")
        windows_candidates = []
        if local_appdata:
            windows_candidates.extend(
                [
                    Path(local_appdata) / "Programs" / "codex" / "codex.exe",
                    Path(local_appdata) / "npm" / "codex.cmd",
                ]
            )
        if appdata:
            windows_candidates.append(Path(appdata) / "npm" / "codex.cmd")
        candidates = windows_candidates + candidates

    for path in candidates:
        if _is_executable_file(path):
            return _prefer_native_codex_binary(str(path))
    raise CodexCliError("Codex CLI executable not found for the current user")


def _codex_env(*, codex_home: str = "", extra_path: str = "") -> Dict[str, str]:
    env = os.environ.copy()
    if codex_home:
        env["CODEX_HOME"] = codex_home
    if extra_path:
        env["PATH"] = _path_with_extra(extra_path)
    return env


def _check_writable_dir(path: Path) -> Optional[str]:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            prefix=".hnreader-codex-check-",
            dir=path,
            delete=True,
        ):
            pass
    except OSError as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


def inspect_codex_runtime(
    *,
    executable: Optional[str] = None,
    codex_home: Optional[str] = None,
    extra_path: Optional[str] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Return a local Codex CLI readiness report without making an AI request."""

    enabled = bool(settings.CODEX_ENABLED)
    configured = executable if executable is not None else settings.CODEX_CLI_PATH
    home = (codex_home if codex_home is not None else settings.CODEX_HOME).strip()
    extra = (
        extra_path if extra_path is not None else settings.CODEX_EXTRA_PATH
    ).strip()
    out: Dict[str, Any] = {
        "enabled": enabled,
        "status": "disabled" if not enabled else "unknown",
        "executable": configured or "codex",
        "codex_home": home,
        "extra_path": extra,
    }
    if not enabled:
        return out

    if home:
        home_path = Path(home).expanduser()
        out["codex_home"] = str(home_path)
        home_error = _check_writable_dir(home_path)
        if home_error:
            out.update(
                {
                    "status": "err",
                    "error": f"CODEX_HOME is not writable: {home_error}",
                }
            )
            return out

    try:
        resolved = resolve_codex_executable(configured or "codex", extra_path=extra)
    except CodexCliError as exc:
        out.update({"status": "missing", "error": str(exc)})
        return out
    out["resolved_executable"] = resolved

    try:
        completed = subprocess.run(
            [resolved, "--version"],
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=float(timeout),
            check=False,
            env=_codex_env(codex_home=home, extra_path=extra),
        )
    except subprocess.TimeoutExpired:
        out.update(
            {
                "status": "err",
                "error": f"Codex CLI version timed out after {timeout:.1f}s",
            }
        )
        return out
    except OSError as exc:
        out.update({"status": "err", "error": f"{type(exc).__name__}: {exc}"})
        return out

    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    if stdout:
        out["version"] = stdout.splitlines()[-1]
    if stderr:
        out["stderr"] = stderr[-1000:]
    if completed.returncode != 0:
        detail = (stderr or stdout or f"exit {completed.returncode}").strip()
        out.update({"status": "err", "error": detail[-1000:]})
        return out
    out["status"] = "ok"
    return out


def _loads_json_from_text(content: str) -> Any:
    text = str(content or "").strip()
    if not text:
        raise CodexCliError("Codex CLI returned an empty final message")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].lstrip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        fenced = "\n".join(lines).strip()
        if fenced:
            return json.loads(fenced)

    start = text.find("{")
    end = text.rfind("}")
    if 0 <= start < end:
        return json.loads(text[start : end + 1])
    raise CodexCliError("Codex CLI final message is not valid JSON")


def _usage_from_jsonl(stdout: str) -> Optional[Dict[str, int]]:
    usage: Optional[Mapping[str, Any]] = None
    for line in str(stdout or "").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and event.get("type") == "turn.completed":
            raw_usage = event.get("usage")
            if isinstance(raw_usage, Mapping):
                usage = raw_usage
    if usage is None:
        return None

    out: Dict[str, int] = {}
    key_map = {
        "input_tokens": ("input_tokens",),
        "cached_input_tokens": ("cached_input_tokens",),
        "output_tokens": ("output_tokens",),
        "reasoning_output_tokens": ("reasoning_output_tokens",),
        "total_tokens": ("total_tokens",),
    }
    for out_key, raw_keys in key_map.items():
        for raw_key in raw_keys:
            value = usage.get(raw_key)
            if value is None:
                continue
            try:
                out[out_key] = int(value)
            except (TypeError, ValueError):
                pass
            break
    if "total_tokens" not in out:
        total = int(out.get("input_tokens", 0)) + int(out.get("output_tokens", 0))
        reasoning = int(out.get("reasoning_output_tokens", 0))
        if total or reasoning:
            out["total_tokens"] = total + reasoning
    return out or None


def _final_message_from_jsonl(stdout: str) -> str:
    final = ""
    for line in str(stdout or "").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        item = event.get("item")
        if not isinstance(item, dict):
            continue
        if item.get("type") == "agent_message" and isinstance(item.get("text"), str):
            final = str(item.get("text") or "")
    if not final.strip():
        raise CodexCliError("Codex CLI JSON stream did not include a final message")
    return final


def _new_usage_bucket() -> Dict[str, int]:
    return {
        "requests": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cached_input_tokens": 0,
        "reasoning_output_tokens": 0,
    }


def _add_usage(target: Dict[str, int], source: Mapping[str, Any]) -> None:
    for key in (
        "requests",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_input_tokens",
        "reasoning_output_tokens",
        "unpriced_tokens",
    ):
        value = source.get(key)
        if value is None:
            continue
        target[key] = int(target.get(key, 0)) + int(value or 0)


def _final_usage(bucket: Mapping[str, Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for key in (
        "requests",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_input_tokens",
        "reasoning_output_tokens",
        "unpriced_tokens",
    ):
        value = int(bucket.get(key) or 0)
        if value > 0 or key in ("requests", "total_tokens"):
            out[key] = value
    return out


def summarize_usage_records(
    records: Sequence[Mapping[str, Any]],
    *,
    purposes: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    allowed = set(purposes) if purposes is not None else None
    total = _new_usage_bucket()
    by_step: Dict[str, Dict[str, int]] = {}
    by_model: Dict[Tuple[str, str], Dict[str, int]] = {}

    for record in records:
        step = str(record.get("step") or "unknown")
        if allowed is not None and step not in allowed:
            continue
        _add_usage(total, record)
        step_bucket = by_step.setdefault(step, _new_usage_bucket())
        _add_usage(step_bucket, record)
        model_key = (
            str(record.get("model") or "codex-cli"),
            str(record.get("base_url") or "local"),
        )
        model_bucket = by_model.setdefault(model_key, _new_usage_bucket())
        _add_usage(model_bucket, record)

    if int(total.get("requests") or 0) <= 0:
        return {}
    out: Dict[str, Any] = _final_usage(total)
    out["by_step"] = {
        step: _final_usage(bucket) for step, bucket in sorted(by_step.items())
    }
    out["by_model"] = sorted(
        (
            {
                "model": model,
                "base_url": base_url,
                **_final_usage(bucket),
            }
            for (model, base_url), bucket in by_model.items()
        ),
        key=lambda entry: (
            -int(entry.get("total_tokens") or 0),
            str(entry.get("model") or ""),
            str(entry.get("base_url") or ""),
        ),
    )
    return out


def merge_usage_summaries(*summaries: Mapping[str, Any]) -> Dict[str, Any]:
    total: Dict[str, int] = _new_usage_bucket()
    by_step: Dict[str, Dict[str, int]] = {}
    by_model: Dict[Tuple[str, str], Dict[str, int]] = {}

    for summary in summaries:
        if not isinstance(summary, Mapping) or not summary:
            continue
        _add_usage(total, summary)
        raw_by_step = summary.get("by_step") or {}
        if isinstance(raw_by_step, Mapping):
            for step, bucket in raw_by_step.items():
                if not isinstance(bucket, Mapping):
                    continue
                target = by_step.setdefault(str(step), _new_usage_bucket())
                _add_usage(target, bucket)
        raw_by_model = summary.get("by_model") or []
        if isinstance(raw_by_model, list):
            for entry in raw_by_model:
                if not isinstance(entry, Mapping):
                    continue
                key = (
                    str(entry.get("model") or "unknown"),
                    str(entry.get("base_url") or ""),
                )
                target = by_model.setdefault(key, _new_usage_bucket())
                _add_usage(target, entry)

    if int(total.get("requests") or 0) <= 0:
        return {}
    out: Dict[str, Any] = _final_usage(total)
    out["by_step"] = {
        step: _final_usage(bucket) for step, bucket in sorted(by_step.items())
    }
    out["by_model"] = sorted(
        (
            {
                "model": model,
                "base_url": base_url,
                **_final_usage(bucket),
            }
            for (model, base_url), bucket in by_model.items()
        ),
        key=lambda entry: (
            -int(entry.get("total_tokens") or 0),
            str(entry.get("model") or ""),
            str(entry.get("base_url") or ""),
        ),
    )
    return out


class CodexCliJsonClient:
    """Run ``codex exec`` as a local, read-only structured-output engine."""

    def __init__(
        self,
        *,
        executable: Optional[str] = None,
        model: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.executable = executable or settings.CODEX_CLI_PATH or "codex"
        self.model = (model if model is not None else settings.CODEX_MODEL).strip()
        self.codex_home = settings.CODEX_HOME
        self.extra_path = settings.CODEX_EXTRA_PATH
        self.timeout = (
            float(timeout)
            if timeout is not None
            else float(settings.CODEX_REQUEST_TIMEOUT_SECONDS)
        )
        self._usage_lock = Lock()
        self._usage_records: list[Dict[str, Any]] = []

    def usage_checkpoint(self) -> int:
        with self._usage_lock:
            return len(self._usage_records)

    def usage_summary_since(
        self,
        checkpoint: int,
        *,
        purposes: Optional[Sequence[str]] = None,
    ) -> Tuple[int, Dict[str, Any]]:
        with self._usage_lock:
            start = max(0, min(int(checkpoint), len(self._usage_records)))
            records = list(self._usage_records[start:])
            next_checkpoint = len(self._usage_records)
        return next_checkpoint, summarize_usage_records(records, purposes=purposes)

    def _record_usage(self, purpose: str, stdout: str) -> None:
        usage = _usage_from_jsonl(stdout)
        record: Dict[str, Any] = {
            "step": purpose,
            "model": self.model or "codex-cli",
            "base_url": "codex-cli://local",
            "requests": 1,
        }
        if usage:
            record.update(usage)
        with self._usage_lock:
            self._usage_records.append(record)

    def complete_json(
        self,
        *,
        purpose: str,
        system_prompt: str,
        user_content: str,
        output_schema: Mapping[str, Any],
        reasoning_effort: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not settings.CODEX_ENABLED:
            raise CodexCliError("Codex CLI is disabled")
        effort = _normalize_reasoning_effort(reasoning_effort)
        executable = resolve_codex_executable(
            self.executable,
            extra_path=self.extra_path,
        )
        with tempfile.TemporaryDirectory(prefix="hmini-codex-") as tmp:
            tmp_path = Path(tmp)
            system_path = tmp_path / "system-prompt.md"
            schema_path = tmp_path / "output-schema.json"
            system_path.write_text(system_prompt, encoding="utf-8")
            schema_path.write_text(
                json.dumps(output_schema, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            args = [
                executable,
                "--ask-for-approval",
                "never",
                "exec",
                "--sandbox",
                "read-only",
                "--skip-git-repo-check",
                "--ephemeral",
                "--ignore-rules",
                "--color",
                "never",
                "--json",
                "--output-schema",
                str(schema_path),
                "-c",
                f"model_instructions_file={json.dumps(str(system_path))}",
                "-c",
                "features.shell_tool=false",
                "-c",
                "features.hooks=false",
            ]
            if effort:
                args.extend(["-c", f"model_reasoning_effort={json.dumps(effort)}"])
            if settings.CODEX_IGNORE_USER_CONFIG:
                args.append("--ignore-user-config")
            if self.model:
                args.extend(["--model", self.model])
            args.extend(["--cd", str(tmp_path), "-"])

            try:
                completed = subprocess.run(
                    args,
                    input=user_content,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    capture_output=True,
                    timeout=self.timeout,
                    check=False,
                    env=_codex_env(
                        codex_home=self.codex_home,
                        extra_path=self.extra_path,
                    ),
                )
            except FileNotFoundError as exc:
                raise CodexCliError(
                    f"Codex CLI executable not found: {self.executable}"
                ) from exc
            except subprocess.TimeoutExpired as exc:
                raise CodexCliError(
                    f"Codex CLI timed out after {self.timeout:.1f}s"
                ) from exc
            except OSError as exc:
                raise CodexCliError(f"Codex CLI failed to start: {exc}") from exc

            if completed.returncode != 0:
                detail = (completed.stderr or completed.stdout or "").strip()
                if len(detail) > 2000:
                    detail = detail[-2000:]
                raise CodexCliError(
                    f"Codex CLI exited with {completed.returncode}: {detail}"
                )

            final_text = _final_message_from_jsonl(completed.stdout)
            raw = _loads_json_from_text(final_text)
            if not isinstance(raw, dict):
                raise CodexCliError("Codex CLI final JSON must be an object")
            self._record_usage(purpose, completed.stdout)
            return raw


__all__ = [
    "CodexCliError",
    "CodexCliJsonClient",
    "inspect_codex_runtime",
    "merge_usage_summaries",
    "resolve_codex_executable",
    "summarize_usage_records",
]
