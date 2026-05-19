from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.artifact_paths import (
    resolve_path,
    resolve_timestamped_output_path,
    utc_now_iso,
    write_json,
)
from tools.detector_calibration_crop_promotion import PROMOTION_RECORD_SCHEMA_VERSION, validate_revised_crop_promotion_draft


SCHEMA_VERSION = "detector_calibration_promotion_triage_manifest_v1"
DEFAULT_THRESHOLD = 0.15
DEFAULT_DRAFT_ROOT_NAME = "detector_calibration_promotions"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"
STATUS_ORDER = {
    "publish_ready": 0,
    "needs_more_replay": 1,
    "hold": 2,
}


def generate_detector_calibration_promotion_triage_manifest(
    *,
    game: str,
    draft_promotions_root: str | Path | None = None,
    output_path: str | Path | None = None,
    publish_ready_threshold_delta_iou: float = DEFAULT_THRESHOLD,
) -> dict[str, Any]:
    resolved_root = (
        _resolve_path(draft_promotions_root)
        if draft_promotions_root is not None
        else REPO_ROOT / "assets" / "games" / game / "drafts" / DEFAULT_DRAFT_ROOT_NAME
    )
    promotion_record_paths = _discover_promotion_records(resolved_root)
    rows = [
        _derive_triage_row(
            promotion_record_path,
            publish_ready_threshold_delta_iou=publish_ready_threshold_delta_iou,
        )
        for promotion_record_path in promotion_record_paths
    ]
    ordered_rows = sorted(rows, key=_triage_sort_key)
    status_counts = {
        "publish_ready": sum(1 for row in ordered_rows if row.get("triage_status") == "publish_ready"),
        "needs_more_replay": sum(1 for row in ordered_rows if row.get("triage_status") == "needs_more_replay"),
        "hold": sum(1 for row in ordered_rows if row.get("triage_status") == "hold"),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "generated_at": _utc_now(),
        "publish_ready_threshold_delta_iou": publish_ready_threshold_delta_iou,
        "source_promotion_count": len(promotion_record_paths),
        "row_count": len(ordered_rows),
        "status_counts": status_counts,
        "rows": ordered_rows,
    }
    target_output_path = _resolve_output_path(game=game, output_path=output_path)
    _write_json(target_output_path, manifest)
    return {
        "ok": True,
        "status": "ok",
        "game": game,
        "draft_promotions_root": str(resolved_root),
        "output_path": str(target_output_path),
        "source_promotion_count": len(promotion_record_paths),
        "row_count": len(ordered_rows),
        "status_counts": status_counts,
        "rows": ordered_rows,
        "emitted_manifest": {
            "path": str(target_output_path),
            "row_count": len(ordered_rows),
            "source_promotion_count": len(promotion_record_paths),
            "status_counts": status_counts,
            "top_row": ordered_rows[0] if ordered_rows else None,
        },
    }


def _derive_triage_row(
    promotion_record_path: Path,
    *,
    publish_ready_threshold_delta_iou: float,
) -> dict[str, Any]:
    validation = validate_revised_crop_promotion_draft(promotion_record_path)
    payload = _load_json_safely(promotion_record_path)
    if payload is None:
        return {
            "triage_status": "hold",
            "triage_reason": "invalid_promotion_record_json",
            "promotion_record_path": str(promotion_record_path.resolve()),
            "promotion_root": str(promotion_record_path.parent.parent.resolve()),
            "promotion_id": promotion_record_path.parent.parent.name,
            "created_at": None,
            "asset_id": None,
            "candidate_id": None,
            "run_id": None,
            "delta_iou": None,
            "absolute_delta_iou": None,
            "primary_iou": None,
            "secondary_iou": None,
            "primary_source": None,
            "secondary_source": None,
            "primary_reference_source": None,
            "secondary_reference_source": None,
            "difference_summary": None,
            "draft_validation_status": "invalid_json",
        }

    eligibility = _nested_value(payload, "replay_evidence", "eligibility")
    eligibility = eligibility if isinstance(eligibility, dict) else {}
    delta_iou = _safe_float(eligibility.get("delta_iou"))
    primary_iou = _safe_float(eligibility.get("primary_iou"))
    secondary_iou = _safe_float(eligibility.get("secondary_iou"))
    primary_source = str(eligibility.get("primary_source") or "").strip() or None
    secondary_source = str(eligibility.get("secondary_source") or "").strip() or None
    primary_reference_source = str(eligibility.get("primary_reference_source") or "").strip() or None
    secondary_reference_source = str(eligibility.get("secondary_reference_source") or "").strip() or None
    triage_status, triage_reason = _classify_promotion(
        validation=validation,
        eligibility=eligibility,
        delta_iou=delta_iou,
        primary_source=primary_source,
        primary_reference_source=primary_reference_source,
        publish_ready_threshold_delta_iou=publish_ready_threshold_delta_iou,
    )
    return {
        "triage_status": triage_status,
        "triage_reason": triage_reason,
        "promotion_record_path": str(promotion_record_path.resolve()),
        "promotion_root": str(_resolve_path(payload.get("promotion_root"))) if payload.get("promotion_root") else str(promotion_record_path.parent.parent.resolve()),
        "promotion_id": str(payload.get("promotion_id") or "").strip() or promotion_record_path.parent.parent.name,
        "created_at": str(payload.get("created_at") or "").strip() or None,
        "asset_id": str(_nested_value(payload, "published_asset", "asset_id") or "").strip() or None,
        "candidate_id": str(_nested_value(payload, "source_evidence", "candidate_id") or "").strip() or None,
        "run_id": str(_nested_value(payload, "source_evidence", "run_id") or "").strip() or None,
        "delta_iou": delta_iou,
        "absolute_delta_iou": round(abs(delta_iou), 6) if delta_iou is not None else None,
        "primary_iou": primary_iou,
        "secondary_iou": secondary_iou,
        "primary_source": primary_source,
        "secondary_source": secondary_source,
        "primary_reference_source": primary_reference_source,
        "secondary_reference_source": secondary_reference_source,
        "difference_summary": str(eligibility.get("difference_summary") or "").strip() or None,
        "draft_validation_status": str(validation.get("status") or "").strip() or None,
    }


def _classify_promotion(
    *,
    validation: dict[str, Any],
    eligibility: dict[str, Any],
    delta_iou: float | None,
    primary_source: str | None,
    primary_reference_source: str | None,
    publish_ready_threshold_delta_iou: float,
) -> tuple[str, str]:
    if not validation.get("ok"):
        return "hold", "draft_promotion_validation_failed"
    if str(eligibility.get("status") or "").strip() == "":
        return "hold", "missing_replay_eligibility"
    if primary_source != "replay_run":
        return "hold", "missing_replay_derived_primary_source"
    if primary_reference_source != "localized_match":
        if delta_iou is not None and delta_iou > 0:
            return "needs_more_replay", "missing_localized_match_reference"
        return "hold", "missing_localized_match_reference"
    if delta_iou is None:
        return "hold", "missing_delta_iou"
    if delta_iou <= 0:
        return "hold", "non_positive_replay_improvement"
    if delta_iou >= publish_ready_threshold_delta_iou:
        return "publish_ready", "validated_replay_improvement_meets_publish_threshold"
    return "needs_more_replay", "positive_replay_improvement_below_publish_threshold"


def _discover_promotion_records(root: Path) -> list[Path]:
    if not root.exists():
        return []
    discovered = [path.resolve() for path in root.rglob("revised_crop_promotion.json")]
    return sorted(discovered, key=lambda item: str(item))


def _resolve_output_path(*, game: str, output_path: str | Path | None) -> Path:
    return resolve_timestamped_output_path(
        output_path=output_path,
        default_dir=DEFAULT_OUTPUT_ROOT / game / "promotion_triage",
        filename_suffix="detector_calibration_promotion_triage_manifest.json",
    )


def _triage_sort_key(row: dict[str, Any]) -> tuple[int, float, float, str]:
    status_rank = STATUS_ORDER.get(str(row.get("triage_status") or ""), len(STATUS_ORDER))
    absolute_delta = _safe_float(row.get("absolute_delta_iou"))
    created_at = _parse_iso_datetime(str(row.get("created_at") or "").strip())
    created_epoch = created_at.timestamp() if created_at is not None else 0.0
    return (
        status_rank,
        -(absolute_delta if absolute_delta is not None else -1.0),
        -created_epoch,
        f"{row.get('promotion_record_path') or ''}",
    )


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _nested_value(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _resolve_path(path: str | Path) -> Path:
    return resolve_path(path)


def _load_json_safely(path: str | Path) -> dict[str, Any] | None:
    try:
        return json.loads(_resolve_path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    write_json(path, payload)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return utc_now_iso()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate detector calibration draft promotion triage manifest")
    parser.add_argument("--game", required=True)
    parser.add_argument("--draft-promotions-root")
    parser.add_argument("--output-path")
    parser.add_argument("--publish-ready-threshold-delta-iou", type=float, default=DEFAULT_THRESHOLD)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_promotion_triage_manifest(
        game=args.game,
        draft_promotions_root=args.draft_promotions_root,
        output_path=args.output_path,
        publish_ready_threshold_delta_iou=args.publish_ready_threshold_delta_iou,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
