from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.artifact_paths import (
    resolve_path as _artifact_resolve_path,
    resolve_timestamped_output_path as _artifact_resolve_timestamped_output_path,
    utc_now_iso as _artifact_utc_now_iso,
    utc_timestamp_slug as _artifact_utc_timestamp_slug,
    write_json as _artifact_write_json,
)

SCHEMA_VERSION = "detector_calibration_followup_report_v1"
DEFAULT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"


def generate_detector_calibration_followup_report(
    *,
    followup_manifest: str | Path,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    followup_manifest_path = _resolve_path(followup_manifest)
    payload = _load_json(followup_manifest_path)
    game = str(payload.get("game") or "").strip()
    if not game:
        raise ValueError("follow-up manifest is missing required game")
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise ValueError("follow-up manifest rows must be a list")
    compact_rows = [_compact_report_row(row) for row in rows if isinstance(row, dict)]
    top_followup = compact_rows[0] if compact_rows else None
    top_followups_by_asset = _top_rows_by_asset(compact_rows)
    report = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "generated_at": _utc_now(),
        "source_followup_manifest_path": str(followup_manifest_path),
        "source_row_count": len(compact_rows),
        "asset_count": len(top_followups_by_asset),
        "top_followup": top_followup,
        "top_followups_by_asset": top_followups_by_asset,
    }
    target_output_path = _resolve_output_path(game=game, output_path=output_path)
    _write_json(target_output_path, report)
    return {
        "ok": True,
        "status": "ok",
        "game": game,
        "followup_manifest_path": str(followup_manifest_path),
        "output_path": str(target_output_path),
        "source_row_count": len(compact_rows),
        "asset_count": len(top_followups_by_asset),
        "top_followup": top_followup,
        "top_followups_by_asset": top_followups_by_asset,
        "emitted_report": {
            "path": str(target_output_path),
            "source_row_count": len(compact_rows),
            "asset_count": len(top_followups_by_asset),
            "top_followup": top_followup,
        },
    }


def _compact_report_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "review_record_path": _optional_string(row.get("review_record_path")),
        "run_id": _optional_string(row.get("run_id")),
        "runtime_sidecar_path": _optional_string(row.get("runtime_sidecar_path")),
        "asset_id": _optional_string(row.get("asset_id")),
        "event_type": _optional_string(row.get("event_type")),
        "event_row_id": _optional_string(row.get("event_row_id")),
        "difference_summary": _optional_string(row.get("difference_summary")),
        "primary_source": _optional_string(row.get("primary_source")),
        "secondary_source": _optional_string(row.get("secondary_source")),
        "primary_reference_crop": _optional_string(row.get("primary_reference_crop")),
        "secondary_reference_crop": _optional_string(row.get("secondary_reference_crop")),
        "primary_iou": _safe_float(row.get("primary_iou")),
        "secondary_iou": _safe_float(row.get("secondary_iou")),
        "delta_iou": _safe_float(row.get("delta_iou")),
        "absolute_delta_iou": _safe_float(row.get("absolute_delta_iou")),
    }


def _top_rows_by_asset(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen_asset_ids: set[str] = set()
    selected: list[dict[str, Any]] = []
    for row in rows:
        asset_id = str(row.get("asset_id") or "").strip()
        if not asset_id or asset_id in seen_asset_ids:
            continue
        seen_asset_ids.add(asset_id)
        selected.append(row)
    return selected


def _resolve_output_path(*, game: str, output_path: str | Path | None) -> Path:
    return _artifact_resolve_timestamped_output_path(
        output_path=output_path,
        default_dir=DEFAULT_ROOT / game / "reports",
        filename_suffix="detector_calibration_followup_report.json",
        timestamp_slug=_utc_timestamp_slug(),
    )


def _resolve_path(path: str | Path) -> Path:
    return _artifact_resolve_path(path)


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    _artifact_write_json(path, payload)


def _optional_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return _artifact_utc_now_iso()


def _utc_timestamp_slug() -> str:
    return _artifact_utc_timestamp_slug()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate detector calibration follow-up report")
    parser.add_argument("--followup-manifest", required=True)
    parser.add_argument("--output-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_followup_report(
        followup_manifest=args.followup_manifest,
        output_path=args.output_path,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
