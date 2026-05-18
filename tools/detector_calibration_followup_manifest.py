from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.detector_calibration_comparison import build_dual_layer_comparison_selection


SCHEMA_VERSION = "detector_calibration_followup_manifest_v1"
DEFAULT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"


def generate_detector_calibration_followup_manifest(
    *,
    game: str,
    calibration_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_root = _resolve_path(calibration_root) if calibration_root is not None else DEFAULT_ROOT / game
    review_record_paths = _discover_review_records(resolved_root)
    rows: list[dict[str, Any]] = []
    for review_record_path in review_record_paths:
        rows.extend(_derive_followup_rows_from_review_record(review_record_path, game=game))
    ordered_rows = sorted(rows, key=_followup_sort_key)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "generated_at": _utc_now(),
        "source_session_count": len(review_record_paths),
        "row_count": len(ordered_rows),
        "rows": ordered_rows,
    }
    target_output_path = _resolve_output_path(game=game, output_path=output_path)
    _write_json(target_output_path, manifest)
    return {
        "ok": True,
        "status": "ok",
        "game": game,
        "calibration_root": str(resolved_root),
        "output_path": str(target_output_path),
        "source_session_count": len(review_record_paths),
        "row_count": len(ordered_rows),
        "rows": ordered_rows,
        "emitted_manifest": {
            "path": str(target_output_path),
            "row_count": len(ordered_rows),
            "source_session_count": len(review_record_paths),
            "top_row": ordered_rows[0] if ordered_rows else None,
        },
    }


def _derive_followup_rows_from_review_record(review_record_path: Path, *, game: str) -> list[dict[str, Any]]:
    payload = _load_json(review_record_path)
    if str(payload.get("schema_version") or "").strip() != "detector_calibration_review_v1":
        return []
    if str(payload.get("game") or "").strip() != game:
        return []
    target = payload.get("target", {})
    if not isinstance(target, dict):
        return []
    session_root = review_record_path.parent.resolve()
    candidates = payload.get("crop_candidates", [])
    if not isinstance(candidates, list):
        return []
    candidate_by_id = {
        str(row.get("candidate_id") or "").strip(): row
        for row in candidates
        if isinstance(row, dict) and str(row.get("candidate_id") or "").strip()
    }
    rows: list[dict[str, Any]] = []
    replay_runs = payload.get("replay_runs", [])
    if not isinstance(replay_runs, list):
        return []
    for run in replay_runs:
        if not isinstance(run, dict):
            continue
        candidate = candidate_by_id.get(str(run.get("candidate_id") or "").strip())
        if not isinstance(candidate, dict):
            continue
        selection = build_dual_layer_comparison_selection(
            run.get("derived_template_comparison"),
            candidate.get("template_comparison"),
        )
        if not isinstance(selection, dict):
            continue
        secondary = selection.get("secondary_comparison")
        if not isinstance(secondary, dict):
            continue
        primary = selection.get("comparison")
        if not isinstance(primary, dict):
            continue
        primary_overlap = primary.get("spatial_overlap", {}) if isinstance(primary.get("spatial_overlap"), dict) else {}
        secondary_overlap = secondary.get("spatial_overlap", {}) if isinstance(secondary.get("spatial_overlap"), dict) else {}
        primary_iou = _safe_float(primary_overlap.get("iou"))
        secondary_iou = _safe_float(secondary_overlap.get("iou"))
        delta_iou = round(primary_iou - secondary_iou, 6) if primary_iou is not None and secondary_iou is not None else None
        rows.append(
            {
                "session_root": str(session_root),
                "review_record_path": str(review_record_path.resolve()),
                "run_id": str(run.get("run_id") or "").strip() or None,
                "candidate_id": str(run.get("candidate_id") or "").strip() or None,
                "runtime_sidecar_path": str(run.get("trial_runtime_sidecar_path") or "").strip() or None,
                "asset_id": str(target.get("asset_id") or "").strip() or None,
                "event_type": str(target.get("event_type") or "").strip() or None,
                "event_row_id": str(target.get("event_row_id") or "").strip() or None,
                "difference_summary": str(selection.get("difference_summary") or "").strip() or None,
                "primary_source": str(selection.get("source") or "").strip() or None,
                "secondary_source": str(selection.get("secondary_source") or "").strip() or None,
                "primary_reference_crop": str(primary_overlap.get("reference_crop") or "").strip() or None,
                "secondary_reference_crop": str(secondary_overlap.get("reference_crop") or "").strip() or None,
                "primary_iou": primary_iou,
                "secondary_iou": secondary_iou,
                "delta_iou": delta_iou,
                "absolute_delta_iou": round(abs(delta_iou), 6) if delta_iou is not None else None,
                "replay_created_at": str(run.get("created_at") or "").strip() or None,
            }
        )
    return rows


def _discover_review_records(root: Path) -> list[Path]:
    if not root.exists():
        return []
    discovered = []
    for path in root.rglob("review_record.json"):
        if "followup" in path.parts:
            continue
        discovered.append(path.resolve())
    return sorted(discovered, key=lambda item: str(item))


def _resolve_output_path(*, game: str, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_ROOT / game / "followup" / f"{timestamp}.detector_calibration_followup_manifest.json"


def _followup_sort_key(row: dict[str, Any]) -> tuple[float, float, str]:
    absolute_delta = _safe_float(row.get("absolute_delta_iou"))
    created_at = _parse_iso_datetime(str(row.get("replay_created_at") or "").strip())
    created_epoch = created_at.timestamp() if created_at is not None else 0.0
    return (
        -(absolute_delta if absolute_delta is not None else -1.0),
        -created_epoch,
        f"{row.get('session_root') or ''}::{row.get('run_id') or ''}",
    )


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = _resolve_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate detector calibration follow-up manifest")
    parser.add_argument("--game", required=True)
    parser.add_argument("--calibration-root")
    parser.add_argument("--output-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_followup_manifest(
        game=args.game,
        calibration_root=args.calibration_root,
        output_path=args.output_path,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
