from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.detector_calibration_comparison import build_dual_layer_comparison_selection
from pipeline.roi_matcher import RoiMatcherError, load_published_runtime_pack

DEFAULT_DRAFT_ROOT_NAME = "detector_calibration_promotions"
PROMOTION_RECORD_SCHEMA_VERSION = "detector_calibration_revised_crop_promotion_v1"


def prepare_revised_crop_promotion(
    session_root: str | Path,
    *,
    candidate_id: str,
    run_id: str,
    approved_by: str,
    notes: str | None = None,
    promotion_root: str | Path | None = None,
) -> dict[str, Any]:
    normalized_approved_by = str(approved_by or "").strip()
    if not normalized_approved_by:
        return {"ok": False, "status": "missing_approved_by", "error": "approved_by is required"}

    review_record_path = _review_record_path(session_root)
    review_record = _load_json(review_record_path)
    game = str(review_record.get("game") or "").strip()
    target = review_record.get("target") if isinstance(review_record.get("target"), dict) else {}
    asset_id = str(target.get("asset_id") or "").strip()
    if not game or not asset_id:
        return {"ok": False, "status": "invalid_review_record", "error": "review record is missing game or target asset_id"}

    candidate = _find_by_id(review_record.get("crop_candidates"), "candidate_id", candidate_id)
    if candidate is None:
        return {"ok": False, "status": "unknown_candidate_id", "error": f"candidate_id '{candidate_id}' was not found"}
    replay_run = _find_by_id(review_record.get("replay_runs"), "run_id", run_id)
    if replay_run is None:
        return {"ok": False, "status": "unknown_run_id", "error": f"run_id '{run_id}' was not found"}
    if str(replay_run.get("candidate_id") or "").strip() != candidate_id:
        return {
            "ok": False,
            "status": "candidate_run_mismatch",
            "error": f"run_id '{run_id}' is not associated with candidate_id '{candidate_id}'",
        }

    replay_result_path = _resolve_path(replay_run.get("replay_result_path"))
    if not replay_result_path.is_file():
        return {
            "ok": False,
            "status": "missing_replay_result",
            "error": f"replay_result_path does not exist: {replay_result_path}",
        }
    replay_result = _load_json(replay_result_path)

    try:
        published_pack = load_published_runtime_pack(game)
    except (FileNotFoundError, RoiMatcherError) as exc:
        return {"ok": False, "status": getattr(exc, "status", "invalid_game_pack"), "error": str(exc)}

    published_asset = _load_published_asset_row(game, asset_id)
    if published_asset is None:
        return {"ok": False, "status": "unknown_published_asset_id", "error": f"asset_id '{asset_id}' is not in published_assets"}
    template_spec = next((row for row in published_pack.templates if row.asset_id == asset_id), None)
    if template_spec is None:
        return {"ok": False, "status": "unknown_template_asset_id", "error": f"asset_id '{asset_id}' is not in cv_templates.yaml"}

    comparison_selection = build_dual_layer_comparison_selection(
        replay_result.get("derived_template_comparison"),
        candidate.get("template_comparison"),
    )
    eligibility = _build_promotion_eligibility(
        comparison_selection=comparison_selection,
        candidate=candidate,
        replay_run=replay_run,
        replay_result_path=replay_result_path,
    )
    if not eligibility.get("ok"):
        return {
            "ok": False,
            "status": str(eligibility.get("status") or "ineligible"),
            "error": str(eligibility.get("error") or "promotion candidate is not eligible"),
            "eligibility": eligibility,
        }

    resolved_promotion_root = _promotion_root(game=game, asset_id=asset_id, promotion_root=promotion_root)
    draft_template_relpath = _draft_template_relpath(published_asset, template_spec)
    draft_template_path = resolved_promotion_root / draft_template_relpath
    draft_template_path.parent.mkdir(parents=True, exist_ok=True)
    revised_crop_path = _resolve_path(candidate.get("crop_png_path"))
    shutil.copy2(revised_crop_path, draft_template_path)

    promotion_record_path = resolved_promotion_root / "manifests" / "revised_crop_promotion.json"
    promotion_record_path.parent.mkdir(parents=True, exist_ok=True)
    promotion_record = _build_promotion_record(
        review_record=review_record,
        review_record_path=review_record_path,
        candidate=candidate,
        replay_run=replay_run,
        replay_result_path=replay_result_path,
        published_asset=published_asset,
        template_spec=template_spec,
        promotion_root=resolved_promotion_root,
        draft_template_path=draft_template_path,
        draft_template_relpath=draft_template_relpath,
        approved_by=normalized_approved_by,
        notes=notes,
        eligibility=eligibility,
    )
    _write_json(promotion_record_path, promotion_record)
    validation = validate_revised_crop_promotion_draft(promotion_record_path)
    if not validation.get("ok"):
        return {
            "ok": False,
            "status": str(validation.get("status") or "validation_failed"),
            "error": str(validation.get("error") or "promotion draft validation failed"),
            "promotion_root": str(resolved_promotion_root),
            "promotion_record_path": str(promotion_record_path),
            "eligibility": eligibility,
            "validation": validation,
        }
    return {
        "ok": True,
        "status": "ok",
        "promotion_root": str(resolved_promotion_root),
        "promotion_record_path": str(promotion_record_path),
        "draft_template_path": str(draft_template_path),
        "eligibility": eligibility,
        "validation": validation,
    }


def validate_revised_crop_promotion_draft(promotion_record_path: str | Path) -> dict[str, Any]:
    record_path = _resolve_path(promotion_record_path)
    if not record_path.is_file():
        return {"ok": False, "status": "missing_promotion_record", "error": f"promotion record does not exist: {record_path}"}
    record = _load_json(record_path)
    if str(record.get("schema_version") or "").strip() != PROMOTION_RECORD_SCHEMA_VERSION:
        return {"ok": False, "status": "invalid_schema_version", "error": "promotion record schema_version is invalid"}

    missing = [
        field
        for field in (
            ("source_evidence.review_record_path", _nested_value(record, "source_evidence", "review_record_path")),
            ("source_evidence.session_root", _nested_value(record, "source_evidence", "session_root")),
            ("source_evidence.candidate_id", _nested_value(record, "source_evidence", "candidate_id")),
            ("source_evidence.run_id", _nested_value(record, "source_evidence", "run_id")),
            ("source_evidence.replay_result_path", _nested_value(record, "source_evidence", "replay_result_path")),
            ("published_asset.asset_id", _nested_value(record, "published_asset", "asset_id")),
            ("published_asset.template_path", _nested_value(record, "published_asset", "template_path")),
            ("draft_update.template_path", _nested_value(record, "draft_update", "template_path")),
            ("operator_approval.approved_by", _nested_value(record, "operator_approval", "approved_by")),
        )
        if not str(field[1] or "").strip()
    ]
    if missing:
        return {"ok": False, "status": "incomplete_provenance", "error": f"promotion record is missing required fields: {', '.join(name for name, _ in missing)}"}

    game = str(record.get("game") or "").strip()
    asset_id = str(_nested_value(record, "published_asset", "asset_id") or "").strip()
    published_asset = _load_published_asset_row(game, asset_id)
    if published_asset is None:
        return {"ok": False, "status": "unknown_published_asset_id", "error": f"published asset '{asset_id}' was not found"}

    published_template_path = str(_nested_value(record, "published_asset", "template_path") or "").strip()
    expected_published_template_path = str(_expected_published_template_path(game, published_asset))
    if published_template_path != expected_published_template_path:
        return {
            "ok": False,
            "status": "inconsistent_asset_linkage",
            "error": f"promotion record template_path does not match published asset template: {published_template_path}",
        }

    draft_template_path = _resolve_path(_nested_value(record, "draft_update", "template_path"))
    if not draft_template_path.is_file():
        return {"ok": False, "status": "missing_draft_template", "error": f"draft template does not exist: {draft_template_path}"}

    promotion_root = _resolve_path(record.get("promotion_root"))
    if not str(draft_template_path).startswith(str(promotion_root)):
        return {
            "ok": False,
            "status": "inconsistent_asset_linkage",
            "error": "draft template path is not inside promotion_root",
        }

    copied_from = str(_nested_value(record, "draft_update", "copied_from_revised_crop_path") or "").strip()
    if copied_from and not _resolve_path(copied_from).is_file():
        return {
            "ok": False,
            "status": "missing_revised_crop",
            "error": f"copied_from_revised_crop_path does not exist: {copied_from}",
        }

    return {
        "ok": True,
        "status": "ok",
        "promotion_record_path": str(record_path),
        "promotion_root": str(promotion_root),
        "draft_template_path": str(draft_template_path),
    }


def _build_promotion_eligibility(
    *,
    comparison_selection: dict[str, Any] | None,
    candidate: dict[str, Any],
    replay_run: dict[str, Any],
    replay_result_path: Path,
) -> dict[str, Any]:
    if not isinstance(comparison_selection, dict):
        return {
            "ok": False,
            "status": "missing_replay_derived_comparison",
            "error": "replay-derived comparison is required for promotion",
        }
    if str(comparison_selection.get("source") or "").strip() != "replay_run":
        return {
            "ok": False,
            "status": "ineligible_primary_source",
            "error": "promotion requires primary_source = replay_run",
        }
    comparison = comparison_selection.get("comparison") if isinstance(comparison_selection.get("comparison"), dict) else {}
    overlap = comparison.get("spatial_overlap") if isinstance(comparison.get("spatial_overlap"), dict) else {}
    primary_reference_source = str(overlap.get("reference_source") or "").strip()
    if primary_reference_source != "localized_match":
        return {
            "ok": False,
            "status": "ineligible_reference_source",
            "error": "promotion requires replay-derived primary_reference_source = localized_match",
        }
    primary_iou = _safe_float(overlap.get("iou"))
    secondary_comparison = comparison_selection.get("secondary_comparison") if isinstance(comparison_selection.get("secondary_comparison"), dict) else None
    secondary_overlap = secondary_comparison.get("spatial_overlap") if isinstance(secondary_comparison, dict) and isinstance(secondary_comparison.get("spatial_overlap"), dict) else {}
    secondary_iou = _safe_float(secondary_overlap.get("iou"))
    delta_iou = None if primary_iou is None or secondary_iou is None else round(primary_iou - secondary_iou, 6)
    if delta_iou is None or delta_iou <= 0:
        return {
            "ok": False,
            "status": "ineligible_delta_iou",
            "error": "promotion requires delta_iou > 0 relative to the candidate-time baseline",
        }
    return {
        "ok": True,
        "status": "ok",
        "candidate_id": str(candidate.get("candidate_id") or "").strip(),
        "run_id": str(replay_run.get("run_id") or "").strip(),
        "replay_result_path": str(replay_result_path),
        "primary_source": "replay_run",
        "secondary_source": str(comparison_selection.get("secondary_source") or "crop_candidate"),
        "primary_reference_source": primary_reference_source,
        "primary_reference_crop": str(overlap.get("reference_crop") or "").strip() or None,
        "secondary_reference_source": str(secondary_overlap.get("reference_source") or "").strip() or None,
        "secondary_reference_crop": str(secondary_overlap.get("reference_crop") or "").strip() or None,
        "primary_iou": primary_iou,
        "secondary_iou": secondary_iou,
        "delta_iou": delta_iou,
        "difference_summary": str(comparison_selection.get("difference_summary") or "").strip() or None,
    }


def _build_promotion_record(
    *,
    review_record: dict[str, Any],
    review_record_path: Path,
    candidate: dict[str, Any],
    replay_run: dict[str, Any],
    replay_result_path: Path,
    published_asset: dict[str, Any],
    template_spec: Any,
    promotion_root: Path,
    draft_template_path: Path,
    draft_template_relpath: Path,
    approved_by: str,
    notes: str | None,
    eligibility: dict[str, Any],
) -> dict[str, Any]:
    target = review_record.get("target") if isinstance(review_record.get("target"), dict) else {}
    return {
        "schema_version": PROMOTION_RECORD_SCHEMA_VERSION,
        "promotion_id": promotion_root.name,
        "promotion_root": str(promotion_root),
        "created_at": _utc_now(),
        "game": str(review_record.get("game") or "").strip(),
        "operator_approval": {
            "approved_by": approved_by,
            "notes": notes,
        },
        "source_evidence": {
            "session_root": str(review_record_path.parent),
            "review_record_path": str(review_record_path),
            "candidate_id": str(candidate.get("candidate_id") or "").strip(),
            "run_id": str(replay_run.get("run_id") or "").strip(),
            "replay_result_path": str(replay_result_path),
            "source": str(target.get("source") or "").strip() or None,
            "runtime_sidecar_path": str(target.get("runtime_sidecar_path") or "").strip() or None,
            "fused_sidecar_path": str(target.get("fused_sidecar_path") or "").strip() or None,
            "proxy_sidecar_path": str(target.get("proxy_sidecar_path") or "").strip() or None,
            "event_type": str(target.get("event_type") or "").strip() or None,
            "event_row_id": str(target.get("event_row_id") or "").strip() or None,
        },
        "published_asset": {
            "asset_id": str(published_asset.get("asset_id") or "").strip(),
            "candidate_id": str(published_asset.get("candidate_id") or "").strip() or None,
            "detection_id": str(published_asset.get("detection_id") or "").strip() or None,
            "asset_family": str(published_asset.get("asset_family") or "").strip() or None,
            "roi_ref": str(getattr(template_spec, "roi_ref", "") or "").strip() or None,
            "template_path": str(_expected_published_template_path(str(review_record.get("game") or ""), published_asset)),
            "master_path": str(_expected_published_master_path(str(review_record.get("game") or ""), published_asset)),
        },
        "revised_crop": {
            "candidate_id": str(candidate.get("candidate_id") or "").strip(),
            "crop_png_path": str(_resolve_path(candidate.get("crop_png_path"))),
            "crop": dict(candidate.get("crop", {})) if isinstance(candidate.get("crop"), dict) else None,
            "template_comparison": candidate.get("template_comparison"),
        },
        "replay_evidence": {
            "trial_runtime_sidecar_path": str(replay_run.get("trial_runtime_sidecar_path") or "").strip() or None,
            "trial_fused_sidecar_path": str(replay_run.get("trial_fused_sidecar_path") or "").strip() or None,
            "current_runtime_sidecar_path": str(replay_run.get("current_runtime_sidecar_path") or "").strip() or None,
            "current_fused_sidecar_path": str(replay_run.get("current_fused_sidecar_path") or "").strip() or None,
            "eligibility": eligibility,
        },
        "draft_update": {
            "template_path": str(draft_template_path),
            "template_relpath": draft_template_relpath.as_posix(),
            "copied_from_revised_crop_path": str(_resolve_path(candidate.get("crop_png_path"))),
        },
    }


def _load_published_asset_row(game: str, asset_id: str) -> dict[str, Any] | None:
    manifest_path = REPO_ROOT / "assets" / "games" / game / "manifests" / "assets_manifest.json"
    payload = _load_json(manifest_path)
    rows = payload.get("published_assets") if isinstance(payload.get("published_assets"), list) else []
    return next((row for row in rows if str(row.get("asset_id") or "").strip() == asset_id), None)


def _draft_template_relpath(published_asset: dict[str, Any], template_spec: Any) -> Path:
    raw = str(published_asset.get("template_path") or "").strip()
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            return path
    return template_spec.template_path.relative_to(template_spec.template_path.parents[2])


def _expected_published_template_path(game: str, published_asset: dict[str, Any]) -> Path:
    raw = str(published_asset.get("template_path") or "").strip()
    path = Path(raw)
    if path.is_absolute():
        return path.expanduser().resolve()
    return (REPO_ROOT / "assets" / "games" / game / path).resolve()


def _expected_published_master_path(game: str, published_asset: dict[str, Any]) -> Path:
    raw = str(published_asset.get("master_path") or "").strip()
    path = Path(raw)
    if path.is_absolute():
        return path.expanduser().resolve()
    return (REPO_ROOT / "assets" / "games" / game / path).resolve()


def _promotion_root(*, game: str, asset_id: str, promotion_root: str | Path | None) -> Path:
    if promotion_root is not None:
        return _resolve_path(promotion_root)
    slug = _slugify(asset_id.split(".")[-1] or asset_id)
    return REPO_ROOT / "assets" / "games" / game / "drafts" / DEFAULT_DRAFT_ROOT_NAME / f"{_utc_stamp()}-{slug}"


def _review_record_path(session_root: str | Path) -> Path:
    root = _resolve_path(session_root)
    return root / "review_record.json"


def _find_by_id(rows: Any, key: str, value: str) -> dict[str, Any] | None:
    if not isinstance(rows, list):
        return None
    target = str(value or "").strip()
    for row in rows:
        if isinstance(row, dict) and str(row.get(key) or "").strip() == target:
            return row
    return None


def _nested_value(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "promotion"


def _resolve_path(path: str | Path | None) -> Path:
    return Path(path or "").expanduser().resolve()


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare or validate draft-first revised crop promotions from detector calibration sessions.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="Prepare a draft-local revised crop promotion candidate.")
    prepare_parser.add_argument("--session-root", required=True)
    prepare_parser.add_argument("--candidate-id", required=True)
    prepare_parser.add_argument("--run-id", required=True)
    prepare_parser.add_argument("--approved-by", required=True)
    prepare_parser.add_argument("--notes")
    prepare_parser.add_argument("--promotion-root")

    validate_parser = subparsers.add_parser("validate", help="Validate an existing draft-local revised crop promotion.")
    validate_parser.add_argument("--promotion-record-path", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "prepare":
        result = prepare_revised_crop_promotion(
            args.session_root,
            candidate_id=args.candidate_id,
            run_id=args.run_id,
            approved_by=args.approved_by,
            notes=args.notes,
            promotion_root=args.promotion_root,
        )
    else:
        result = validate_revised_crop_promotion_draft(args.promotion_record_path)
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
