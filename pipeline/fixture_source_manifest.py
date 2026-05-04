from __future__ import annotations

import json
from pathlib import Path
from typing import Any


FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION = "fixture_source_manifest_v1"


def load_fixture_source_manifest(path: str | Path) -> dict[str, Any]:
    manifest_path = _resolve_path(path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return _validate_manifest(payload, manifest_path)


def _validate_manifest(payload: Any, manifest_path: Path) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("fixture source manifest must be a mapping")
    if payload.get("schema_version") != FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            f"fixture source manifest must use {FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION}"
        )
    fixtures = payload.get("fixtures")
    if not isinstance(fixtures, list) or not fixtures:
        raise ValueError("fixture source manifest must include a non-empty fixtures list")

    fixture_ids: set[str] = set()
    validated: list[dict[str, Any]] = []
    for index, row in enumerate(fixtures):
        if not isinstance(row, dict):
            raise ValueError(f"fixture source row at index {index} must be a mapping")
        fixture_id = str(row.get("fixture_id", "")).strip()
        if not fixture_id:
            raise ValueError(f"fixture source row at index {index} is missing fixture_id")
        if fixture_id in fixture_ids:
            raise ValueError(f"fixture source manifest duplicates fixture_id '{fixture_id}'")
        fixture_ids.add(fixture_id)
        game = str(row.get("game", "")).strip()
        if not game:
            raise ValueError(f"fixture source '{fixture_id}' is missing game")
        source_path_text = str(row.get("source_path", "")).strip()
        if not source_path_text:
            raise ValueError(f"fixture source '{fixture_id}' is missing source_path")

        produce_layers = row.get("produce_layers", {})
        if produce_layers is None:
            produce_layers = {}
        if not isinstance(produce_layers, dict):
            raise ValueError(f"fixture source '{fixture_id}' produce_layers must be a mapping")
        normalized_layers: dict[str, bool] = {}
        for key, value in produce_layers.items():
            normalized_key = str(key).strip().lower()
            if normalized_key not in {"proxy", "runtime", "fused"}:
                raise ValueError(
                    f"fixture source '{fixture_id}' produce_layers keys must be proxy, runtime, or fused"
                )
            normalized_layers[normalized_key] = bool(value)

        source_path = _resolve_manifest_relative_path(manifest_path, source_path_text)
        chat_log_text = str(row.get("chat_log_path", "")).strip()
        chat_log_path = _resolve_manifest_relative_path(manifest_path, chat_log_text) if chat_log_text else None
        validated.append(
            {
                "fixture_id": fixture_id,
                "game": game,
                "source_path": str(source_path),
                "chat_log_path": str(chat_log_path) if chat_log_path is not None else None,
                "produce_layers": normalized_layers,
                "notes": str(row.get("notes", "")).strip(),
            }
        )

    return {
        "schema_version": FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION,
        "manifest_path": str(manifest_path),
        "fixtures": validated,
        "fixture_count": len(validated),
    }


def _resolve_manifest_relative_path(manifest_path: Path, path_like: str) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (manifest_path.parent / path).resolve()
    else:
        path = path.resolve()
    return path


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path
