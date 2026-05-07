from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.fixture_source_manifest import FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION


REPO_ROOT = Path(__file__).resolve().parent.parent
ACCEPTED_CLIP_INTAKE_MANIFEST_SCHEMA_VERSION = "accepted_clip_intake_manifest_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "accepted_clip_source_manifests"
_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


def adapt_accepted_clip_intake_to_source_manifest(
    accepted_clip_intake_manifest: str | Path,
    *,
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(accepted_clip_intake_manifest)
    if not manifest_path.exists() or not manifest_path.is_file():
        return {
            "ok": False,
            "status": "invalid_accepted_intake_manifest",
            "accepted_clip_intake_manifest": str(manifest_path),
            "error": "accepted clip intake manifest does not exist or is not a file",
        }

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "ok": False,
            "status": "invalid_accepted_intake_manifest",
            "accepted_clip_intake_manifest": str(manifest_path),
            "error": "accepted clip intake manifest is not readable JSON",
        }

    validation_error = _validate_intake_manifest(payload)
    if validation_error is not None:
        return {
            "ok": False,
            "status": validation_error["status"],
            "accepted_clip_intake_manifest": str(manifest_path),
            "error": validation_error["error"],
        }

    fixtures = [_adapt_row(row) for row in payload["rows"]]
    if not fixtures:
        return {
            "ok": True,
            "status": "no_rows",
            "schema_version": FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION,
            "adapted_manifest_id": _adapted_manifest_id(
                source_manifest_path=str(manifest_path),
                source_manifest_id=str(payload["intake_manifest_id"]),
                game=str(payload["game"]),
                fixtures=[],
            ),
            "created_at": datetime.now(UTC).isoformat(),
            "source_accepted_clip_intake_manifest_path": str(manifest_path),
            "source_accepted_clip_intake_manifest_id": payload["intake_manifest_id"],
            "game": payload["game"],
            "row_count": 0,
            "fixtures": [],
            "manifest_path": None,
        }

    adapted_manifest_id = _adapted_manifest_id(
        source_manifest_path=str(manifest_path),
        source_manifest_id=str(payload["intake_manifest_id"]),
        game=str(payload["game"]),
        fixtures=fixtures,
    )
    final_output_path = _default_output_path(
        output_root=output_root,
        output_path=output_path,
        game=str(payload["game"]),
        adapted_manifest_id=adapted_manifest_id,
    )
    final_output_path.parent.mkdir(parents=True, exist_ok=True)

    result = {
        "ok": True,
        "status": "ok",
        "schema_version": FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION,
        "adapted_manifest_id": adapted_manifest_id,
        "created_at": datetime.now(UTC).isoformat(),
        "source_accepted_clip_intake_manifest_path": str(manifest_path),
        "source_accepted_clip_intake_manifest_id": payload["intake_manifest_id"],
        "game": payload["game"],
        "row_count": len(fixtures),
        "fixtures": fixtures,
        "manifest_path": str(final_output_path),
    }
    final_output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def _validate_intake_manifest(payload: Any) -> dict[str, str] | None:
    if not isinstance(payload, dict):
        return {
            "status": "invalid_accepted_intake_manifest",
            "error": "accepted clip intake manifest must be a mapping",
        }
    if payload.get("schema_version") != ACCEPTED_CLIP_INTAKE_MANIFEST_SCHEMA_VERSION:
        return {
            "status": "invalid_accepted_intake_schema",
            "error": f"accepted clip intake manifest must use {ACCEPTED_CLIP_INTAKE_MANIFEST_SCHEMA_VERSION}",
        }
    intake_manifest_id = str(payload.get("intake_manifest_id", "")).strip()
    if not intake_manifest_id:
        return {
            "status": "invalid_accepted_intake_manifest",
            "error": "accepted clip intake manifest is missing intake_manifest_id",
        }
    game = str(payload.get("game", "")).strip()
    if not game:
        return {
            "status": "invalid_accepted_intake_manifest",
            "error": "accepted clip intake manifest is missing game",
        }
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return {
            "status": "invalid_accepted_intake_manifest",
            "error": "accepted clip intake manifest rows must be a list",
        }
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            return {
                "status": "invalid_accepted_intake_manifest",
                "error": f"accepted clip intake row at index {index} must be a mapping",
            }
        clip_id = str(row.get("clip_id", "")).strip()
        game_value = str(row.get("game", "")).strip()
        source_path = str(row.get("canonical_clip_path", "")).strip()
        if not clip_id:
            return {
                "status": "invalid_accepted_intake_manifest",
                "error": f"accepted clip intake row at index {index} is missing clip_id",
            }
        if not game_value:
            return {
                "status": "invalid_accepted_intake_manifest",
                "error": f"accepted clip intake row at index {index} is missing game",
            }
        if not source_path:
            return {
                "status": "invalid_accepted_intake_manifest",
                "error": f"accepted clip intake row at index {index} is missing canonical_clip_path",
            }
    return None


def _adapt_row(row: dict[str, Any]) -> dict[str, Any]:
    clip_id = str(row["clip_id"]).strip()
    fixture_id = _fixture_id_from_clip_id(clip_id)
    meta_path = row.get("meta_path")
    quality_tag = _optional_text(row.get("quality_tag"))
    notes_parts = [
        f"accepted_clip_id={clip_id}",
        f"ingestion_ready={bool(row.get('ingestion_ready'))}",
    ]
    if quality_tag:
        notes_parts.append(f"quality_tag={quality_tag}")
    if meta_path:
        notes_parts.append(f"meta_path={_resolve_path(meta_path)}")
    return {
        "fixture_id": fixture_id,
        "game": str(row["game"]).strip(),
        "source_path": str(_resolve_path(str(row["canonical_clip_path"]))),
        "chat_log_path": None,
        "produce_layers": {},
        "notes": "; ".join(notes_parts),
        "accepted_clip_id": clip_id,
    }


def _fixture_id_from_clip_id(clip_id: str) -> str:
    normalized = _SLUG_PATTERN.sub("_", clip_id.lower()).strip("_")
    normalized = re.sub(r"_+", "_", normalized)
    if not normalized:
        normalized = "accepted_clip"
    digest = hashlib.sha1(clip_id.encode("utf-8")).hexdigest()[:8]
    return f"accepted_{normalized}_{digest}"


def _adapted_manifest_id(
    *,
    source_manifest_path: str,
    source_manifest_id: str,
    game: str,
    fixtures: list[dict[str, Any]],
) -> str:
    raw = json.dumps(
        {
            "source_manifest_path": source_manifest_path,
            "source_manifest_id": source_manifest_id,
            "game": game,
            "fixture_ids": [row.get("fixture_id") for row in fixtures],
        },
        sort_keys=True,
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _default_output_path(
    *,
    output_root: str | Path | None,
    output_path: str | Path | None,
    game: str,
    adapted_manifest_id: str,
) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    root = DEFAULT_OUTPUT_ROOT if output_root is None else _resolve_path(output_root)
    return root / game / f"accepted-source-manifest-{adapted_manifest_id}.json"


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _optional_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
