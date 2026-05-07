from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
ACCEPTED_CLIP_INVENTORY_SCHEMA_VERSION = "accepted_clip_inventory_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "accepted_clip_inventory"
_CLIP_ID_PATTERN = re.compile(r"^(?P<title>.+?)_(?P<numeric_id>\d{6,})$")
_DATED_PREFIX_PATTERN = re.compile(r"^(?P<game>[a-z0-9_]+)_(?P<date>\d{8})_(?P<rest>.+)$", re.IGNORECASE)


def build_accepted_clip_inventory(
    *,
    source_root: str | Path,
    game: str,
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_root = _resolve_source_root(source_root)
    if not resolved_root.exists() or not resolved_root.is_dir():
        return {
            "ok": False,
            "status": "invalid_source_root",
            "source_root": str(resolved_root),
            "error": "source root does not exist or is not a directory",
        }

    rows = _inventory_rows(resolved_root, game=game)
    if not rows:
        return {
            "ok": True,
            "status": "no_clips_found",
            "schema_version": ACCEPTED_CLIP_INVENTORY_SCHEMA_VERSION,
            "inventory_id": _inventory_id(str(resolved_root), game, []),
            "created_at": datetime.now(UTC).isoformat(),
            "source_root": str(resolved_root),
            "game": game,
            "row_count": 0,
            "canonical_clip_count": 0,
            "meta_linked_count": 0,
            "duplicate_group_count": 0,
            "rows": [],
            "manifest_path": None,
            "csv_path": None,
        }

    inventory_id = _inventory_id(str(resolved_root), game, rows)
    manifest_path = _default_output_path(
        output_root=output_root,
        output_path=output_path,
        game=game,
        inventory_id=inventory_id,
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = manifest_path.with_suffix(".csv")

    payload = {
        "ok": True,
        "status": "ok",
        "schema_version": ACCEPTED_CLIP_INVENTORY_SCHEMA_VERSION,
        "inventory_id": inventory_id,
        "created_at": datetime.now(UTC).isoformat(),
        "source_root": str(resolved_root),
        "game": game,
        "row_count": len(rows),
        "canonical_clip_count": len(rows),
        "meta_linked_count": sum(1 for row in rows if row.get("has_meta")),
        "duplicate_group_count": sum(1 for row in rows if int(row.get("variant_count") or 0) > 1),
        "rows": rows,
        "manifest_path": str(manifest_path),
        "csv_path": str(csv_path),
    }
    manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    _write_csv(csv_path, rows)
    return payload


def _inventory_rows(source_root: Path, *, game: str) -> list[dict[str, Any]]:
    files = [
        path for path in sorted(source_root.iterdir())
        if path.is_file() and path.name != ".gitkeep"
    ]
    clip_paths = [path for path in files if path.suffix.lower() == ".mp4"]
    meta_by_base = {
        _base_name(path.name): path
        for path in files
        if path.name.endswith(".meta.json")
    }

    grouped: dict[str, list[Path]] = {}
    for clip_path in clip_paths:
        clip_id = _resolve_clip_id(clip_path.name)
        group_key = clip_id or f"unresolved::{clip_path.name}"
        grouped.setdefault(group_key, []).append(clip_path)

    rows = []
    for group_key, variants in sorted(grouped.items()):
        canonical = _choose_canonical_primary(variants=variants, meta_by_base=meta_by_base)
        meta_path = _link_meta(canonical, meta_by_base=meta_by_base)
        rows.append(
            _inventory_row(
                game=game,
                group_key=group_key,
                canonical=canonical,
                variants=variants,
                meta_path=meta_path,
            )
        )
    return rows


def _resolve_clip_id(filename: str) -> str | None:
    stem = Path(filename).stem
    rest = _strip_dated_prefix(stem)
    if rest is not None:
        return rest if _CLIP_ID_PATTERN.match(rest) else None
    match = _CLIP_ID_PATTERN.match(stem)
    if match is not None:
        return stem
    return None


def _group_variants(paths: list[Path]) -> list[Path]:
    return sorted(paths, key=lambda path: path.name)


def _choose_canonical_primary(*, meta_by_base: dict[str, Path], variants: list[Path]) -> Path:
    ranked = sorted(
        _group_variants(variants),
        key=lambda path: (
            0 if _is_unprefixed(path.name) and _base_name(path.name) in meta_by_base else 1,
            0 if _is_unprefixed(path.name) else 1,
            len(path.name),
            path.name,
        ),
    )
    return ranked[0]


def _link_meta(canonical: Path, *, meta_by_base: dict[str, Path]) -> Path | None:
    return meta_by_base.get(_base_name(canonical.name))


def _inventory_row(
    *,
    game: str,
    group_key: str,
    canonical: Path,
    variants: list[Path],
    meta_path: Path | None,
) -> dict[str, Any]:
    meta_payload = _load_meta(meta_path) if meta_path is not None else {}
    clip_id = None if group_key.startswith("unresolved::") else group_key
    variant_paths = [str(path.resolve()) for path in _group_variants(variants)]
    variant_filenames = [path.name for path in _group_variants(variants)]
    has_meta = meta_path is not None
    return {
        "clip_id": clip_id,
        "game": game,
        "canonical_clip_path": str(canonical.resolve()),
        "canonical_filename": canonical.name,
        "meta_path": str(meta_path.resolve()) if meta_path is not None else None,
        "quality_tag": meta_payload.get("quality_tag"),
        "downloaded_at": meta_payload.get("downloaded_at"),
        "duration_seconds": meta_payload.get("duration_seconds"),
        "has_audio": meta_payload.get("has_audio"),
        "resolution_width": meta_payload.get("resolution_width"),
        "resolution_height": meta_payload.get("resolution_height"),
        "fps": meta_payload.get("fps"),
        "variant_paths": variant_paths,
        "variant_filenames": variant_filenames,
        "variant_count": len(variant_paths),
        "has_meta": has_meta,
        "naming_pattern": _naming_pattern(variants),
        "preferred_source_reason": _preferred_source_reason(canonical, meta_path=meta_path),
        "ingestion_ready": bool(clip_id and canonical.exists()),
    }


def _inventory_id(source_root: str, game: str, rows: list[dict[str, Any]]) -> str:
    raw = json.dumps(
        {
            "source_root": source_root,
            "game": game,
            "clip_ids": [row.get("clip_id") or row.get("canonical_filename") for row in rows],
        },
        sort_keys=True,
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _default_output_path(
    *,
    output_root: str | Path | None,
    output_path: str | Path | None,
    game: str,
    inventory_id: str,
) -> Path:
    if output_path is not None:
        return Path(output_path).expanduser().resolve()
    root = DEFAULT_OUTPUT_ROOT if output_root is None else Path(output_root).expanduser().resolve()
    return root / game / f"accepted-clip-inventory-{inventory_id}.manifest.json"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "clip_id",
        "game",
        "canonical_clip_path",
        "canonical_filename",
        "meta_path",
        "quality_tag",
        "downloaded_at",
        "duration_seconds",
        "has_audio",
        "resolution_width",
        "resolution_height",
        "fps",
        "variant_count",
        "has_meta",
        "naming_pattern",
        "preferred_source_reason",
        "ingestion_ready",
        "variant_paths",
        "variant_filenames",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                **{key: row.get(key) for key in fieldnames if key not in {"variant_paths", "variant_filenames"}},
                "variant_paths": json.dumps(row.get("variant_paths", []), ensure_ascii=False),
                "variant_filenames": json.dumps(row.get("variant_filenames", []), ensure_ascii=False),
            })


def _resolve_source_root(source_root: str | Path) -> Path:
    return Path(source_root).expanduser().resolve()


def _load_meta(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _base_name(filename: str) -> str:
    stem = Path(filename).stem
    if stem.endswith(".meta"):
        stem = Path(stem).stem
    return stem


def _is_unprefixed(filename: str) -> bool:
    return _strip_dated_prefix(Path(filename).stem) is None


def _strip_dated_prefix(stem: str) -> str | None:
    parts = stem.split("_")
    for index, part in enumerate(parts[:-1]):
        if index == 0:
            continue
        if len(part) == 8 and part.isdigit():
            remainder = "_".join(parts[index + 1 :])
            return remainder or None
    return None


def _naming_pattern(variants: list[Path]) -> str:
    has_prefixed = any(not _is_unprefixed(path.name) for path in variants)
    has_unprefixed = any(_is_unprefixed(path.name) for path in variants)
    if has_prefixed and has_unprefixed:
        return "mixed_variants"
    if has_prefixed:
        return "dated_prefixed"
    if has_unprefixed:
        return "unprefixed"
    return "unresolved"


def _preferred_source_reason(canonical: Path, *, meta_path: Path | None) -> str:
    if _is_unprefixed(canonical.name) and meta_path is not None:
        return "unprefixed_with_meta"
    if _is_unprefixed(canonical.name):
        return "unprefixed_without_meta"
    return "fallback_variant"
