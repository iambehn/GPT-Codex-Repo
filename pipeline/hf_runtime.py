from __future__ import annotations

import importlib
import shutil
from pathlib import Path
from typing import Any


def check_hf_runtime(
    *,
    stage_name: str,
    execution_mode: str,
    packages: tuple[str, ...],
    needs_ffmpeg: bool = False,
) -> dict[str, Any]:
    checks: dict[str, dict[str, Any]] = {}
    overall_ok = True
    status = "ok"

    if execution_mode != "local":
        return {
            "ok": False,
            "status": "unsupported_runtime",
            "checks": {},
            "error": f"unsupported execution_mode '{execution_mode}' for stage '{stage_name}'",
        }

    for package_name in packages:
        try:
            module = importlib.import_module(package_name)
            checks[package_name] = {
                "ok": True,
                "version": getattr(module, "__version__", "unknown"),
            }
        except Exception as exc:
            overall_ok = False
            status = "runtime_missing"
            checks[package_name] = {
                "ok": False,
                "error": str(exc),
            }

    if needs_ffmpeg:
        ffmpeg = resolve_ffmpeg_path()
        checks["ffmpeg"] = {
            "ok": ffmpeg is not None,
            "path": str(ffmpeg) if ffmpeg is not None else None,
        }
        if ffmpeg is None:
            overall_ok = False
            status = "runtime_missing"
            checks["ffmpeg"]["error"] = "ffmpeg binary was not found"

    return {
        "ok": overall_ok,
        "status": status,
        "checks": checks,
    }


def resolve_ffmpeg_path() -> Path | None:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    ffmpeg_path = Path(ffmpeg)
    if ffmpeg_path.exists():
        return ffmpeg_path
    return None


def format_runtime_failure(runtime_report: dict[str, Any]) -> str:
    if runtime_report.get("status") == "unsupported_runtime":
        return str(runtime_report.get("error", "unsupported local runtime"))

    missing = [
        name
        for name, row in dict(runtime_report.get("checks", {})).items()
        if not bool(row.get("ok"))
    ]
    if missing:
        return f"missing dependency or runtime capability: {', '.join(sorted(missing))}"
    return str(runtime_report.get("error", "runtime unavailable"))


def resolve_torch_device(torch_module: Any, requested: str | None) -> str:
    requested_device = str(requested or "auto").strip().lower() or "auto"
    if requested_device == "auto":
        if bool(getattr(getattr(torch_module, "cuda", None), "is_available", lambda: False)()):
            return "cuda"
        mps_backend = getattr(getattr(torch_module, "backends", None), "mps", None)
        if bool(getattr(mps_backend, "is_available", lambda: False)()):
            return "mps"
        return "cpu"

    if requested_device == "cuda" and not bool(getattr(getattr(torch_module, "cuda", None), "is_available", lambda: False)()):
        raise ValueError("unsupported local environment: CUDA was requested but is not available")
    if requested_device == "mps":
        mps_backend = getattr(getattr(torch_module, "backends", None), "mps", None)
        if not bool(getattr(mps_backend, "is_available", lambda: False)()):
            raise ValueError("unsupported local environment: MPS was requested but is not available")
    if requested_device not in {"cpu", "cuda", "mps"}:
        raise ValueError(f"unsupported local environment: unknown device '{requested_device}'")
    return requested_device

