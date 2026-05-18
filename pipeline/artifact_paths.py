from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def utc_timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def resolve_timestamped_output_path(
    *,
    output_path: str | Path | None,
    default_dir: str | Path,
    filename_suffix: str,
    timestamp_slug: str | None = None,
) -> Path:
    if output_path is not None:
        return resolve_path(output_path)
    slug = timestamp_slug or utc_timestamp_slug()
    return resolve_path(Path(default_dir) / f"{slug}.{filename_suffix}")


def write_json(
    path: str | Path,
    payload: Any,
    *,
    trailing_newline: bool = False,
) -> Path:
    target = resolve_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, indent=2)
    if trailing_newline:
        rendered += "\n"
    target.write_text(rendered, encoding="utf-8")
    return target
