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

SCHEMA_VERSION = "detector_calibration_next_actions_manifest_v1"
PROGRESS_SCHEMA_VERSION = "detector_calibration_evidence_expansion_progress_manifest_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"
STATUS_ORDER = {
    "review_for_publish": 0,
    "collect_more_evidence": 1,
    "investigate_state_gap": 2,
}


def generate_detector_calibration_next_actions_manifest(
    *,
    progress_manifest: str | Path,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    progress_path = _resolve_path(progress_manifest)
    progress_payload = _load_json(progress_path)
    _validate_progress_manifest(progress_payload)
    progress_rows = progress_payload.get("rows") if isinstance(progress_payload.get("rows"), list) else []
    next_action_rows = [_derive_next_action_row(row) for row in progress_rows if isinstance(row, dict)]
    ordered_rows = sorted(next_action_rows, key=_next_action_sort_key)
    status_counts = {
        "review_for_publish": sum(1 for row in ordered_rows if row.get("action_status") == "review_for_publish"),
        "collect_more_evidence": sum(1 for row in ordered_rows if row.get("action_status") == "collect_more_evidence"),
        "investigate_state_gap": sum(1 for row in ordered_rows if row.get("action_status") == "investigate_state_gap"),
    }
    game = str(progress_payload.get("game") or "").strip()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "generated_at": _utc_now(),
        "source_progress_manifest_path": str(progress_path),
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
        "output_path": str(target_output_path),
        "row_count": len(ordered_rows),
        "status_counts": status_counts,
        "rows": ordered_rows,
        "emitted_manifest": {
            "path": str(target_output_path),
            "row_count": len(ordered_rows),
            "status_counts": status_counts,
            "top_row": ordered_rows[0] if ordered_rows else None,
        },
    }


def _derive_next_action_row(progress_row: dict[str, Any]) -> dict[str, Any]:
    progress_status = str(progress_row.get("progress_status") or "").strip()
    action_status, action_reason = _classify_next_action(progress_status)
    remaining_replay_gap = _safe_int(progress_row.get("remaining_replay_gap"))
    remaining_distinct_source_gap = _safe_int(progress_row.get("remaining_distinct_source_gap"))
    linked_session_count = _safe_int(progress_row.get("linked_session_count")) or 0
    linked_promotion_count = _safe_int(progress_row.get("linked_promotion_count")) or 0
    return {
        "asset_id": str(progress_row.get("asset_id") or "").strip() or None,
        "action_status": action_status,
        "action_reason": action_reason,
        "progress_status": progress_status or None,
        "queue_status": str(progress_row.get("queue_status") or "").strip() or None,
        "decision_status": str(progress_row.get("decision_status") or "").strip() or None,
        "triage_status": str(progress_row.get("triage_status") or "").strip() or None,
        "remaining_replay_gap": remaining_replay_gap,
        "remaining_distinct_source_gap": remaining_distinct_source_gap,
        "linked_session_count": linked_session_count,
        "linked_promotion_count": linked_promotion_count,
        "recommended_action_note": _recommended_action_note(
            action_status=action_status,
            remaining_replay_gap=remaining_replay_gap,
            remaining_distinct_source_gap=remaining_distinct_source_gap,
        ),
    }


def _classify_next_action(progress_status: str) -> tuple[str, str]:
    if progress_status == "ready_to_publish":
        return "review_for_publish", "progress_row_ready_to_publish"
    if progress_status in {"actively_collecting", "waiting_for_work"}:
        return "collect_more_evidence", "progress_row_requires_broader_evidence"
    return "investigate_state_gap", "progress_row_missing_publish_decision_state"


def _recommended_action_note(
    *,
    action_status: str,
    remaining_replay_gap: int | None,
    remaining_distinct_source_gap: int | None,
) -> str:
    if action_status == "review_for_publish":
        return "Asset is ready_to_publish; review for live pack mutation."
    if action_status == "collect_more_evidence":
        replay_gap = remaining_replay_gap or 0
        distinct_gap = remaining_distinct_source_gap or 0
        return (
            f"Need {replay_gap} more replay-backed source"
            f"{'' if replay_gap == 1 else 's'} and {distinct_gap} more distinct source"
            f"{'' if distinct_gap == 1 else 's'} for publish readiness."
        )
    return "Asset is missing a current publish-decision row."


def _combined_gap(row: dict[str, Any]) -> int:
    return (_safe_int(row.get("remaining_replay_gap")) or 0) + (_safe_int(row.get("remaining_distinct_source_gap")) or 0)


def _next_action_sort_key(row: dict[str, Any]) -> tuple[int, int, int, str]:
    status_rank = STATUS_ORDER.get(str(row.get("action_status") or ""), len(STATUS_ORDER))
    linked_session_count = _safe_int(row.get("linked_session_count")) or 0
    return (
        status_rank,
        -_combined_gap(row),
        -linked_session_count,
        str(row.get("asset_id") or ""),
    )


def _validate_progress_manifest(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != PROGRESS_SCHEMA_VERSION:
        raise ValueError("progress manifest schema_version is invalid")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("progress manifest rows must be a list")


def _resolve_output_path(*, game: str, output_path: str | Path | None) -> Path:
    return _artifact_resolve_timestamped_output_path(
        output_path=output_path,
        default_dir=DEFAULT_OUTPUT_ROOT / game / "next_actions",
        filename_suffix="detector_calibration_next_actions_manifest.json",
        timestamp_slug=_utc_timestamp_slug(),
    )


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    _artifact_write_json(path, payload)


def _resolve_path(path: str | Path) -> Path:
    return _artifact_resolve_path(path)


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return _artifact_utc_now_iso()


def _utc_timestamp_slug() -> str:
    return _artifact_utc_timestamp_slug()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate detector calibration next actions manifest")
    parser.add_argument("--progress-manifest", required=True)
    parser.add_argument("--output-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_next_actions_manifest(
        progress_manifest=args.progress_manifest,
        output_path=args.output_path,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
