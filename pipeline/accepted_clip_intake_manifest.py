from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
ACCEPTED_CLIP_INTAKE_MANIFEST_SCHEMA_VERSION = "accepted_clip_intake_manifest_v1"
ACCEPTED_CLIP_INVENTORY_SCHEMA_VERSION = "accepted_clip_inventory_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "accepted_clip_intake"
_ROW_FIELDS = [
    "clip_id",
    "game",
    "canonical_clip_path",
    "meta_path",
    "quality_tag",
    "downloaded_at",
    "duration_seconds",
    "has_audio",
    "resolution_width",
    "resolution_height",
    "fps",
    "has_meta",
    "ingestion_ready",
]


def build_accepted_clip_intake_manifest(
    accepted_inventory_manifest: str | Path,
    *,
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(accepted_inventory_manifest)
    if not manifest_path.exists() or not manifest_path.is_file():
        return {
            "ok": False,
            "status": "invalid_inventory_manifest",
            "accepted_inventory_manifest": str(manifest_path),
            "error": "accepted inventory manifest does not exist or is not a file",
        }

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "ok": False,
            "status": "invalid_inventory_manifest",
            "accepted_inventory_manifest": str(manifest_path),
            "error": "accepted inventory manifest is not readable JSON",
        }

    validation_error = _validate_inventory_manifest(payload)
    if validation_error is not None:
        return {
            "ok": False,
            "status": validation_error["status"],
            "accepted_inventory_manifest": str(manifest_path),
            "error": validation_error["error"],
        }

    rows = [_copy_row(row) for row in payload["rows"]]
    if not rows:
        return {
            "ok": True,
            "status": "no_rows",
            "schema_version": ACCEPTED_CLIP_INTAKE_MANIFEST_SCHEMA_VERSION,
            "intake_manifest_id": _intake_manifest_id(
                source_inventory_manifest_path=str(manifest_path),
                source_inventory_id=str(payload["inventory_id"]),
                game=str(payload["game"]),
                rows=[],
            ),
            "created_at": datetime.now(UTC).isoformat(),
            "source_inventory_manifest_path": str(manifest_path),
            "source_inventory_id": payload["inventory_id"],
            "game": payload["game"],
            "row_count": 0,
            "ingestion_ready_count": 0,
            "rows": [],
            "manifest_path": None,
            "csv_path": None,
        }

    intake_manifest_id = _intake_manifest_id(
        source_inventory_manifest_path=str(manifest_path),
        source_inventory_id=str(payload["inventory_id"]),
        game=str(payload["game"]),
        rows=rows,
    )
    final_output_path = _default_output_path(
        output_root=output_root,
        output_path=output_path,
        game=str(payload["game"]),
        intake_manifest_id=intake_manifest_id,
    )
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = final_output_path.with_suffix(".csv")

    result = {
        "ok": True,
        "status": "ok",
        "schema_version": ACCEPTED_CLIP_INTAKE_MANIFEST_SCHEMA_VERSION,
        "intake_manifest_id": intake_manifest_id,
        "created_at": datetime.now(UTC).isoformat(),
        "source_inventory_manifest_path": str(manifest_path),
        "source_inventory_id": payload["inventory_id"],
        "game": payload["game"],
        "row_count": len(rows),
        "ingestion_ready_count": sum(1 for row in rows if bool(row.get("ingestion_ready"))),
        "rows": rows,
        "manifest_path": str(final_output_path),
        "csv_path": str(csv_path),
    }
    final_output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    _write_csv(csv_path, rows)
    return result


def _validate_inventory_manifest(payload: Any) -> dict[str, str] | None:
    if not isinstance(payload, dict):
        return {
            "status": "invalid_inventory_manifest",
            "error": "accepted inventory manifest must be a mapping",
        }
    if payload.get("schema_version") != ACCEPTED_CLIP_INVENTORY_SCHEMA_VERSION:
        return {
            "status": "invalid_inventory_schema",
            "error": f"accepted inventory manifest must use {ACCEPTED_CLIP_INVENTORY_SCHEMA_VERSION}",
        }
    inventory_id = str(payload.get("inventory_id", "")).strip()
    if not inventory_id:
        return {
            "status": "invalid_inventory_manifest",
            "error": "accepted inventory manifest is missing inventory_id",
        }
    game = str(payload.get("game", "")).strip()
    if not game:
        return {
            "status": "invalid_inventory_manifest",
            "error": "accepted inventory manifest is missing game",
        }
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return {
            "status": "invalid_inventory_manifest",
            "error": "accepted inventory manifest rows must be a list",
        }
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            return {
                "status": "invalid_inventory_manifest",
                "error": f"accepted inventory row at index {index} must be a mapping",
            }
        canonical_clip_path = str(row.get("canonical_clip_path", "")).strip()
        row_game = str(row.get("game", "")).strip()
        if not canonical_clip_path:
            return {
                "status": "invalid_inventory_manifest",
                "error": f"accepted inventory row at index {index} is missing canonical_clip_path",
            }
        if not row_game:
            return {
                "status": "invalid_inventory_manifest",
                "error": f"accepted inventory row at index {index} is missing game",
            }
    return None


def _copy_row(row: dict[str, Any]) -> dict[str, Any]:
    copied = {field: row.get(field) for field in _ROW_FIELDS}
    canonical_clip_path = copied.get("canonical_clip_path")
    meta_path = copied.get("meta_path")
    copied["canonical_clip_path"] = str(_resolve_path(canonical_clip_path)) if canonical_clip_path else None
    copied["meta_path"] = str(_resolve_path(meta_path)) if meta_path else None
    return copied


def _intake_manifest_id(
    *,
    source_inventory_manifest_path: str,
    source_inventory_id: str,
    game: str,
    rows: list[dict[str, Any]],
) -> str:
    raw = json.dumps(
        {
            "source_inventory_manifest_path": source_inventory_manifest_path,
            "source_inventory_id": source_inventory_id,
            "game": game,
            "clip_ids": [row.get("clip_id") or row.get("canonical_clip_path") for row in rows],
        },
        sort_keys=True,
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _default_output_path(
    *,
    output_root: str | Path | None,
    output_path: str | Path | None,
    game: str,
    intake_manifest_id: str,
) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    root = DEFAULT_OUTPUT_ROOT if output_root is None else _resolve_path(output_root)
    return root / game / f"accepted-clip-intake-{intake_manifest_id}.manifest.json"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_ROW_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in _ROW_FIELDS})


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path
