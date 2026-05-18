from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


_EXPORT_PATH_FIELDS = (
    "segment_path",
    "runtime_sidecar_path",
    "fused_sidecar_path",
    "selection_manifest_path",
)


def infer_origin_source(
    *,
    source: Any,
    fused_sidecar_path: Any = None,
    highlight_selection_manifest_path: Any = None,
) -> str:
    source_text = _clean_path(source)
    artifact_paths = [
        source_text,
        _clean_path(fused_sidecar_path),
        _clean_path(highlight_selection_manifest_path),
    ]
    for artifact_path in artifact_paths:
        if not artifact_path:
            continue
        origin = _origin_source_from_export_summary(artifact_path)
        if origin:
            return origin
    return source_text


def build_split_lineage_key(
    *,
    game: Any,
    source: Any,
    origin_source: Any = None,
    fixture_id: Any = None,
) -> str:
    base_source = _clean_path(origin_source) or _clean_path(source)
    parts = [
        str(game or ""),
        base_source,
    ]
    fixture_text = str(fixture_id or "")
    if fixture_text:
        parts.append(fixture_text)
    return "::".join(parts)


def _origin_source_from_export_summary(path_text: str) -> str:
    path = Path(path_text).expanduser()
    for parent in [path.parent, *path.parents]:
        summary_path = parent / "summary.json"
        payload = _load_json(summary_path)
        if not isinstance(payload, dict):
            continue
        exports = payload.get("exports")
        if not isinstance(exports, list):
            continue
        for row in exports:
            if not isinstance(row, dict):
                continue
            for field in _EXPORT_PATH_FIELDS:
                if _clean_path(row.get(field)) == path_text:
                    return _clean_path(row.get("clip_path"))
    return ""


@lru_cache(maxsize=256)
def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _clean_path(value: Any) -> str:
    return str(value or "").strip()
