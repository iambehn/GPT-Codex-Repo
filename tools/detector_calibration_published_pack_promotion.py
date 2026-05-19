from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.artifact_paths import (
    resolve_path as shared_resolve_path,
    utc_timestamp_slug,
    write_json as shared_write_json,
)
from pipeline.roi_matcher import RoiMatcherError, validate_published_pack
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from tools.detector_calibration_crop_promotion import (
    validate_revised_crop_promotion_draft,
)

PUBLISHED_PROMOTION_RESULT_SCHEMA_VERSION = "detector_calibration_published_pack_promotion_v1"
ASSET_PROMOTION_FIELDS = (
    "calibration_promotion_source",
    "calibration_promotion_record_path",
    "calibration_promotion_session_root",
    "calibration_promotion_candidate_id",
    "calibration_promotion_run_id",
    "calibration_promotion_revised_crop_path",
    "calibration_promotion_approved_by",
    "calibration_promotion_promoted_at",
    "calibration_promotion_promoted_by",
)
TEMPLATE_PROMOTION_FIELDS = (
    "calibration_promotion_record_path",
    "calibration_promotion_run_id",
    "calibration_promotion_promoted_at",
    "calibration_promotion_promoted_by",
)


def promote_revised_crop_to_published_pack(
    promotion_record_path: str | Path,
    *,
    promoted_by: str,
) -> dict[str, Any]:
    normalized_promoted_by = str(promoted_by or "").strip()
    if not normalized_promoted_by:
        return {"ok": False, "status": "missing_promoted_by", "error": "promoted_by is required"}

    record_path = _resolve_path(promotion_record_path)
    draft_validation = validate_revised_crop_promotion_draft(record_path)
    if not draft_validation.get("ok"):
        return {
            "ok": False,
            "status": "invalid_draft_promotion_record",
            "error": "draft promotion record failed validation",
            "draft_validation": draft_validation,
        }

    record = _load_json(record_path)
    game = str(record.get("game") or "").strip()
    asset_id = str(_nested_value(record, "published_asset", "asset_id") or "").strip()
    if not game or not asset_id:
        return {"ok": False, "status": "invalid_promotion_record", "error": "promotion record is missing game or published asset_id"}

    assets_manifest_path = REPO_ROOT / "assets" / "games" / game / "manifests" / "assets_manifest.json"
    cv_templates_path = REPO_ROOT / "assets" / "games" / game / "manifests" / "cv_templates.yaml"
    assets_manifest = _load_json(assets_manifest_path)
    cv_templates = load_yaml_file(cv_templates_path)

    published_assets = assets_manifest.get("published_assets")
    if not isinstance(published_assets, list):
        return {"ok": False, "status": "invalid_assets_manifest", "error": "assets_manifest.json must define published_assets as a list"}
    template_rows = cv_templates.get("templates") if isinstance(cv_templates, dict) else None
    if not isinstance(template_rows, list):
        return {"ok": False, "status": "invalid_cv_templates", "error": "cv_templates.yaml must define templates as a list"}

    asset_index = _find_row_index(published_assets, "asset_id", asset_id)
    if asset_index is None:
        return {"ok": False, "status": "missing_published_asset_row", "error": f"published asset row not found for {asset_id}"}
    template_index = _find_row_index(template_rows, "asset_id", asset_id)
    if template_index is None:
        return {"ok": False, "status": "missing_template_row", "error": f"template row not found for {asset_id}"}

    asset_row = published_assets[asset_index]
    template_row = template_rows[template_index]
    expected_template_path = str(_nested_value(record, "published_asset", "template_path") or "").strip()
    actual_asset_template_path = str(_resolve_published_relative_or_absolute(game, str(asset_row.get("template_path") or "")))
    actual_template_row_path = str(_resolve_published_relative_or_absolute(game, str(template_row.get("template_path") or "")))
    if expected_template_path != actual_asset_template_path or expected_template_path != actual_template_row_path:
        return {
            "ok": False,
            "status": "published_linkage_drift",
            "error": "current published manifest linkage no longer matches the promotion record",
            "expected_template_path": expected_template_path,
            "asset_template_path": actual_asset_template_path,
            "cv_template_path": actual_template_row_path,
        }

    published_template_path = _resolve_path(expected_template_path)
    if not published_template_path.is_file():
        return {"ok": False, "status": "missing_published_template", "error": f"published template is missing: {published_template_path}"}

    draft_template_path = _resolve_path(_nested_value(record, "draft_update", "template_path"))
    if not draft_template_path.is_file():
        return {"ok": False, "status": "missing_draft_template", "error": f"draft template is missing: {draft_template_path}"}

    backup_path = _backup_path_for(published_template_path, asset_id=asset_id)
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(published_template_path, backup_path)
    shutil.copy2(draft_template_path, published_template_path)

    file_hash = _sha256_file(published_template_path)
    promoted_at = _utc_now()
    calibration_fields = {
        "calibration_promotion_source": "detector_calibration_revised_crop_promotion_v1",
        "calibration_promotion_record_path": str(record_path),
        "calibration_promotion_session_root": str(_nested_value(record, "source_evidence", "session_root") or "").strip(),
        "calibration_promotion_candidate_id": str(_nested_value(record, "source_evidence", "candidate_id") or "").strip(),
        "calibration_promotion_run_id": str(_nested_value(record, "source_evidence", "run_id") or "").strip(),
        "calibration_promotion_revised_crop_path": str(_nested_value(record, "revised_crop", "crop_png_path") or "").strip(),
        "calibration_promotion_approved_by": str(_nested_value(record, "operator_approval", "approved_by") or "").strip(),
        "calibration_promotion_promoted_at": promoted_at,
        "calibration_promotion_promoted_by": normalized_promoted_by,
    }
    asset_row["file_hash"] = file_hash
    asset_row.update(calibration_fields)
    template_row["file_hash"] = file_hash
    template_row["calibration_promotion_record_path"] = str(record_path)
    template_row["calibration_promotion_run_id"] = str(_nested_value(record, "source_evidence", "run_id") or "").strip()
    template_row["calibration_promotion_promoted_at"] = promoted_at
    template_row["calibration_promotion_promoted_by"] = normalized_promoted_by

    shared_write_json(assets_manifest_path, assets_manifest, trailing_newline=True)
    dump_yaml_file(cv_templates_path, cv_templates)

    post_validation = validate_published_calibration_promotion(
        promotion_record_path=record_path,
        backup_path=backup_path,
    )
    if not post_validation.get("ok"):
        return {
            "ok": False,
            "status": "post_validation_failed",
            "error": "published-pack promotion failed post-mutation validation",
            "backup_path": str(backup_path),
            "post_validation": post_validation,
        }
    return {
        "ok": True,
        "status": "ok",
        "schema_version": PUBLISHED_PROMOTION_RESULT_SCHEMA_VERSION,
        "promotion_record_path": str(record_path),
        "backup_path": str(backup_path),
        "published_template_path": str(published_template_path),
        "assets_manifest_path": str(assets_manifest_path),
        "cv_templates_path": str(cv_templates_path),
        "file_hash": file_hash,
        "post_validation": post_validation,
    }


def validate_published_calibration_promotion(
    *,
    promotion_record_path: str | Path,
    backup_path: str | Path | None = None,
) -> dict[str, Any]:
    record_path = _resolve_path(promotion_record_path)
    draft_validation = validate_revised_crop_promotion_draft(record_path)
    if not draft_validation.get("ok"):
        return {
            "ok": False,
            "status": "invalid_draft_promotion_record",
            "error": "draft promotion record failed validation",
            "draft_validation": draft_validation,
        }
    record = _load_json(record_path)
    game = str(record.get("game") or "").strip()
    asset_id = str(_nested_value(record, "published_asset", "asset_id") or "").strip()
    assets_manifest_path = REPO_ROOT / "assets" / "games" / game / "manifests" / "assets_manifest.json"
    cv_templates_path = REPO_ROOT / "assets" / "games" / game / "manifests" / "cv_templates.yaml"
    assets_manifest = _load_json(assets_manifest_path)
    cv_templates = load_yaml_file(cv_templates_path)
    published_assets = assets_manifest.get("published_assets")
    template_rows = cv_templates.get("templates") if isinstance(cv_templates, dict) else None
    if not isinstance(published_assets, list) or not isinstance(template_rows, list):
        return {"ok": False, "status": "invalid_published_manifests", "error": "published manifests are structurally invalid"}

    asset_index = _find_row_index(published_assets, "asset_id", asset_id)
    template_index = _find_row_index(template_rows, "asset_id", asset_id)
    if asset_index is None or template_index is None:
        return {"ok": False, "status": "missing_published_rows", "error": f"published rows missing for {asset_id}"}

    asset_row = published_assets[asset_index]
    template_row = template_rows[template_index]
    published_template_path = _resolve_published_relative_or_absolute(game, str(asset_row.get("template_path") or ""))
    if not published_template_path.is_file():
        return {"ok": False, "status": "missing_published_template", "error": f"published template is missing: {published_template_path}"}

    actual_hash = _sha256_file(published_template_path)
    if str(asset_row.get("file_hash") or "").strip() != actual_hash:
        return {"ok": False, "status": "asset_hash_mismatch", "error": "assets_manifest file_hash does not match published template bytes"}
    if str(template_row.get("file_hash") or "").strip() != actual_hash:
        return {"ok": False, "status": "template_hash_mismatch", "error": "cv_templates file_hash does not match published template bytes"}

    for key in ASSET_PROMOTION_FIELDS:
        if not str(asset_row.get(key) or "").strip():
            return {"ok": False, "status": "missing_asset_provenance", "error": f"published asset row is missing {key}"}

    for key in TEMPLATE_PROMOTION_FIELDS:
        if not str(template_row.get(key) or "").strip():
            return {"ok": False, "status": "missing_template_provenance", "error": f"template row is missing {key}"}

    expected_record_path = str(record_path)
    if str(asset_row.get("calibration_promotion_record_path") or "").strip() != expected_record_path:
        return {"ok": False, "status": "asset_record_path_mismatch", "error": "published asset row points at a different promotion record"}
    if str(template_row.get("calibration_promotion_record_path") or "").strip() != expected_record_path:
        return {"ok": False, "status": "template_record_path_mismatch", "error": "template row points at a different promotion record"}

    expected_template_path = str(_nested_value(record, "published_asset", "template_path") or "").strip()
    if str(published_template_path) != expected_template_path:
        return {"ok": False, "status": "published_template_path_mismatch", "error": "published template path no longer matches the promotion record"}
    if str(_resolve_published_relative_or_absolute(game, str(template_row.get("template_path") or ""))) != expected_template_path:
        return {"ok": False, "status": "cv_template_path_mismatch", "error": "cv_templates template_path no longer matches the promotion record"}

    if backup_path is not None and not _resolve_path(backup_path).is_file():
        return {"ok": False, "status": "missing_backup", "error": f"backup file is missing: {backup_path}"}

    try:
        pack_validation = validate_published_pack(game)
    except RoiMatcherError as exc:
        return {"ok": False, "status": getattr(exc, "status", "invalid_game_pack"), "error": str(exc)}
    if not pack_validation.get("ok"):
        return {"ok": False, "status": "published_pack_validation_failed", "error": "validate_published_pack reported failures", "pack_validation": pack_validation}

    return {
        "ok": True,
        "status": "ok",
        "published_template_path": str(published_template_path),
        "file_hash": actual_hash,
        "pack_validation_status": pack_validation.get("status"),
    }


def rollback_revised_crop_published_pack_promotion(
    promotion_record_path: str | Path,
    *,
    backup_path: str | Path,
    rolled_back_by: str,
) -> dict[str, Any]:
    normalized_rolled_back_by = str(rolled_back_by or "").strip()
    if not normalized_rolled_back_by:
        return {"ok": False, "status": "missing_rolled_back_by", "error": "rolled_back_by is required"}

    record_path = _resolve_path(promotion_record_path)
    backup_resolved = _resolve_path(backup_path)
    draft_validation = validate_revised_crop_promotion_draft(record_path)
    if not draft_validation.get("ok"):
        return {
            "ok": False,
            "status": "invalid_draft_promotion_record",
            "error": "draft promotion record failed validation",
            "draft_validation": draft_validation,
        }

    record = _load_json(record_path)
    game = str(record.get("game") or "").strip()
    asset_id = str(_nested_value(record, "published_asset", "asset_id") or "").strip()
    if not game or not asset_id:
        return {"ok": False, "status": "invalid_promotion_record", "error": "promotion record is missing game or published asset_id"}
    if not backup_resolved.is_file():
        return {"ok": False, "status": "missing_backup", "error": f"backup file is missing: {backup_resolved}"}

    assets_manifest_path = REPO_ROOT / "assets" / "games" / game / "manifests" / "assets_manifest.json"
    cv_templates_path = REPO_ROOT / "assets" / "games" / game / "manifests" / "cv_templates.yaml"
    assets_manifest = _load_json(assets_manifest_path)
    cv_templates = load_yaml_file(cv_templates_path)
    published_assets = assets_manifest.get("published_assets")
    template_rows = cv_templates.get("templates") if isinstance(cv_templates, dict) else None
    if not isinstance(published_assets, list) or not isinstance(template_rows, list):
        return {"ok": False, "status": "invalid_published_manifests", "error": "published manifests are structurally invalid"}

    asset_index = _find_row_index(published_assets, "asset_id", asset_id)
    template_index = _find_row_index(template_rows, "asset_id", asset_id)
    if asset_index is None or template_index is None:
        return {"ok": False, "status": "missing_published_rows", "error": f"published rows missing for {asset_id}"}

    asset_row = published_assets[asset_index]
    template_row = template_rows[template_index]
    expected_record_path = str(record_path)
    if str(asset_row.get("calibration_promotion_record_path") or "").strip() != expected_record_path:
        return {"ok": False, "status": "asset_record_path_mismatch", "error": "published asset row does not point at the specified promotion record"}
    if str(template_row.get("calibration_promotion_record_path") or "").strip() != expected_record_path:
        return {"ok": False, "status": "template_record_path_mismatch", "error": "template row does not point at the specified promotion record"}

    expected_template_path = str(_nested_value(record, "published_asset", "template_path") or "").strip()
    published_template_path = _resolve_published_relative_or_absolute(game, str(asset_row.get("template_path") or ""))
    if not published_template_path.is_file():
        return {"ok": False, "status": "missing_published_template", "error": f"published template is missing: {published_template_path}"}
    if str(published_template_path) != expected_template_path:
        return {"ok": False, "status": "published_template_path_mismatch", "error": "published template path no longer matches the promotion record"}
    if str(_resolve_published_relative_or_absolute(game, str(template_row.get("template_path") or ""))) != expected_template_path:
        return {"ok": False, "status": "cv_template_path_mismatch", "error": "cv_templates template_path no longer matches the promotion record"}

    shutil.copy2(backup_resolved, published_template_path)
    restored_hash = _sha256_file(published_template_path)
    asset_row["file_hash"] = restored_hash
    template_row["file_hash"] = restored_hash
    for key in ASSET_PROMOTION_FIELDS:
        asset_row.pop(key, None)
    for key in TEMPLATE_PROMOTION_FIELDS:
        template_row.pop(key, None)

    shared_write_json(assets_manifest_path, assets_manifest, trailing_newline=True)
    dump_yaml_file(cv_templates_path, cv_templates)

    post_validation = validate_rolled_back_calibration_promotion(
        promotion_record_path=record_path,
        backup_path=backup_resolved,
    )
    if not post_validation.get("ok"):
        return {
            "ok": False,
            "status": "post_rollback_validation_failed",
            "error": "published-pack rollback failed post-rollback validation",
            "post_validation": post_validation,
        }
    return {
        "ok": True,
        "status": "ok",
        "schema_version": PUBLISHED_PROMOTION_RESULT_SCHEMA_VERSION,
        "promotion_record_path": str(record_path),
        "backup_path": str(backup_resolved),
        "published_template_path": str(published_template_path),
        "file_hash": restored_hash,
        "rolled_back_by": normalized_rolled_back_by,
        "post_validation": post_validation,
    }


def validate_rolled_back_calibration_promotion(
    *,
    promotion_record_path: str | Path,
    backup_path: str | Path,
) -> dict[str, Any]:
    record_path = _resolve_path(promotion_record_path)
    backup_resolved = _resolve_path(backup_path)
    draft_validation = validate_revised_crop_promotion_draft(record_path)
    if not draft_validation.get("ok"):
        return {
            "ok": False,
            "status": "invalid_draft_promotion_record",
            "error": "draft promotion record failed validation",
            "draft_validation": draft_validation,
        }
    if not backup_resolved.is_file():
        return {"ok": False, "status": "missing_backup", "error": f"backup file is missing: {backup_resolved}"}

    record = _load_json(record_path)
    game = str(record.get("game") or "").strip()
    asset_id = str(_nested_value(record, "published_asset", "asset_id") or "").strip()
    assets_manifest_path = REPO_ROOT / "assets" / "games" / game / "manifests" / "assets_manifest.json"
    cv_templates_path = REPO_ROOT / "assets" / "games" / game / "manifests" / "cv_templates.yaml"
    assets_manifest = _load_json(assets_manifest_path)
    cv_templates = load_yaml_file(cv_templates_path)
    published_assets = assets_manifest.get("published_assets")
    template_rows = cv_templates.get("templates") if isinstance(cv_templates, dict) else None
    if not isinstance(published_assets, list) or not isinstance(template_rows, list):
        return {"ok": False, "status": "invalid_published_manifests", "error": "published manifests are structurally invalid"}

    asset_index = _find_row_index(published_assets, "asset_id", asset_id)
    template_index = _find_row_index(template_rows, "asset_id", asset_id)
    if asset_index is None or template_index is None:
        return {"ok": False, "status": "missing_published_rows", "error": f"published rows missing for {asset_id}"}

    asset_row = published_assets[asset_index]
    template_row = template_rows[template_index]
    published_template_path = _resolve_published_relative_or_absolute(game, str(asset_row.get("template_path") or ""))
    if not published_template_path.is_file():
        return {"ok": False, "status": "missing_published_template", "error": f"published template is missing: {published_template_path}"}

    actual_hash = _sha256_file(published_template_path)
    backup_hash = _sha256_file(backup_resolved)
    if actual_hash != backup_hash:
        return {"ok": False, "status": "backup_restore_mismatch", "error": "published template bytes do not match the provided backup"}
    if str(asset_row.get("file_hash") or "").strip() != actual_hash:
        return {"ok": False, "status": "asset_hash_mismatch", "error": "assets_manifest file_hash does not match restored template bytes"}
    if str(template_row.get("file_hash") or "").strip() != actual_hash:
        return {"ok": False, "status": "template_hash_mismatch", "error": "cv_templates file_hash does not match restored template bytes"}

    for key in ASSET_PROMOTION_FIELDS:
        if key in asset_row:
            return {"ok": False, "status": "stale_asset_promotion_field", "error": f"published asset row still contains {key}"}
    for key in TEMPLATE_PROMOTION_FIELDS:
        if key in template_row:
            return {"ok": False, "status": "stale_template_promotion_field", "error": f"template row still contains {key}"}

    try:
        pack_validation = validate_published_pack(game)
    except RoiMatcherError as exc:
        return {"ok": False, "status": getattr(exc, "status", "invalid_game_pack"), "error": str(exc)}
    if not pack_validation.get("ok"):
        return {"ok": False, "status": "published_pack_validation_failed", "error": "validate_published_pack reported failures", "pack_validation": pack_validation}

    return {
        "ok": True,
        "status": "ok",
        "published_template_path": str(published_template_path),
        "file_hash": actual_hash,
        "pack_validation_status": pack_validation.get("status"),
    }


def _backup_path_for(path: Path, *, asset_id: str) -> Path:
    stem = path.stem
    suffix = path.suffix
    slug = asset_id.split(".")[-1]
    return path.with_name(f"{stem}.calibration-backup-{_utc_stamp()}-{slug}{suffix}")


def _resolve_published_relative_or_absolute(game: str, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path.expanduser().resolve()
    return (REPO_ROOT / "assets" / "games" / game / path).resolve()


def _find_row_index(rows: list[Any], key: str, value: str) -> int | None:
    target = str(value or "").strip()
    for index, row in enumerate(rows):
        if isinstance(row, dict) and str(row.get(key) or "").strip() == target:
            return index
    return None


def _nested_value(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _resolve_path(path: str | Path | None) -> Path:
    return shared_resolve_path(path or "")


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_stamp() -> str:
    return utc_timestamp_slug()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Promote a validated detector-calibration revised crop into the published pack.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    promote_parser = subparsers.add_parser("promote", help="Promote a validated draft promotion record into the published pack.")
    promote_parser.add_argument("--promotion-record-path", required=True)
    promote_parser.add_argument("--promoted-by", required=True)

    validate_parser = subparsers.add_parser("validate", help="Validate a published-pack promotion result.")
    validate_parser.add_argument("--promotion-record-path", required=True)
    validate_parser.add_argument("--backup-path")

    validate_rollback_parser = subparsers.add_parser("validate-rollback", help="Validate a rolled-back published-pack promotion state.")
    validate_rollback_parser.add_argument("--promotion-record-path", required=True)
    validate_rollback_parser.add_argument("--backup-path", required=True)

    rollback_parser = subparsers.add_parser("rollback", help="Roll back a published-pack promotion using its deterministic backup.")
    rollback_parser.add_argument("--promotion-record-path", required=True)
    rollback_parser.add_argument("--backup-path", required=True)
    rollback_parser.add_argument("--rolled-back-by", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "promote":
        result = promote_revised_crop_to_published_pack(args.promotion_record_path, promoted_by=args.promoted_by)
    elif args.command == "validate-rollback":
        result = validate_rolled_back_calibration_promotion(
            promotion_record_path=args.promotion_record_path,
            backup_path=args.backup_path,
        )
    elif args.command == "rollback":
        result = rollback_revised_crop_published_pack_promotion(
            args.promotion_record_path,
            backup_path=args.backup_path,
            rolled_back_by=args.rolled_back_by,
        )
    else:
        result = validate_published_calibration_promotion(
            promotion_record_path=args.promotion_record_path,
            backup_path=args.backup_path,
        )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
