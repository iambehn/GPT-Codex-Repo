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

SCHEMA_VERSION = "detector_calibration_evidence_expansion_progress_manifest_v1"
QUEUE_SCHEMA_VERSION = "detector_calibration_evidence_expansion_queue_manifest_v1"
PUBLISH_DECISION_SCHEMA_VERSION = "detector_calibration_publish_decision_manifest_v1"
PROMOTION_TRIAGE_SCHEMA_VERSION = "detector_calibration_promotion_triage_manifest_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"
STATUS_ORDER = {
    "actively_collecting": 0,
    "waiting_for_work": 1,
    "ready_to_publish": 2,
    "not_triaged": 3,
}
QUEUE_STATUS_ORDER = ("in_progress", "planned", "satisfied", "abandoned")


def generate_detector_calibration_evidence_expansion_progress_manifest(
    *,
    queue_manifest: str | Path,
    publish_decision_manifest: str | Path,
    promotion_triage_manifest: str | Path,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    queue_path = _resolve_path(queue_manifest)
    queue_payload = _load_json(queue_path)
    _validate_queue_manifest(queue_payload)

    decision_path = _resolve_path(publish_decision_manifest)
    decision_payload = _load_json(decision_path)
    _validate_publish_decision_manifest(decision_payload)

    triage_path = _resolve_path(promotion_triage_manifest)
    triage_payload = _load_json(triage_path)
    _validate_promotion_triage_manifest(triage_payload)

    grouped_queue_rows = _group_queue_rows(queue_payload.get("rows") if isinstance(queue_payload.get("rows"), list) else [])
    decision_rows = decision_payload.get("rows") if isinstance(decision_payload.get("rows"), list) else []
    triage_rows = triage_payload.get("rows") if isinstance(triage_payload.get("rows"), list) else []

    progress_rows = [
        _derive_progress_row(
            asset_id=asset_id,
            queue_rows=queue_rows,
            decision_row=_first_asset_row(decision_rows, asset_id),
            triage_row=_first_asset_row(triage_rows, asset_id),
            thresholds=decision_payload.get("decision_thresholds") if isinstance(decision_payload.get("decision_thresholds"), dict) else {},
        )
        for asset_id, queue_rows in grouped_queue_rows.items()
    ]
    ordered_rows = sorted(progress_rows, key=_progress_sort_key)
    status_counts = {
        "actively_collecting": sum(1 for row in ordered_rows if row.get("progress_status") == "actively_collecting"),
        "waiting_for_work": sum(1 for row in ordered_rows if row.get("progress_status") == "waiting_for_work"),
        "ready_to_publish": sum(1 for row in ordered_rows if row.get("progress_status") == "ready_to_publish"),
        "not_triaged": sum(1 for row in ordered_rows if row.get("progress_status") == "not_triaged"),
    }
    game = str(queue_payload.get("game") or "").strip()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "generated_at": _utc_now(),
        "source_evidence_expansion_queue_manifest_path": str(queue_path),
        "source_publish_decision_manifest_path": str(decision_path),
        "source_promotion_triage_manifest_path": str(triage_path),
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


def _derive_progress_row(
    *,
    asset_id: str,
    queue_rows: list[dict[str, Any]],
    decision_row: dict[str, Any] | None,
    triage_row: dict[str, Any] | None,
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    expansion_manifest_count = len(queue_rows)
    in_progress_manifest_count = sum(1 for row in queue_rows if str(row.get("status") or "").strip() == "in_progress")
    linked_session_count = sum(_safe_int(row.get("linked_session_count")) or 0 for row in queue_rows)
    linked_promotion_count = sum(_safe_int(row.get("linked_promotion_count")) or 0 for row in queue_rows)
    queue_status = _aggregate_queue_status(queue_rows)

    decision_status = str(decision_row.get("decision_status") or "").strip() if isinstance(decision_row, dict) else None
    triage_status = str(triage_row.get("triage_status") or "").strip() if isinstance(triage_row, dict) else None
    target_replay_count = _safe_int(thresholds.get("minimum_publish_ready_replays_per_asset"))
    target_distinct_source_count = _safe_int(thresholds.get("minimum_distinct_sources_per_asset"))
    replay_count_for_asset = _safe_int(decision_row.get("replay_count_for_asset")) if isinstance(decision_row, dict) else None
    distinct_source_count_for_asset = _safe_int(decision_row.get("distinct_source_count_for_asset")) if isinstance(decision_row, dict) else None
    remaining_replay_gap = _remaining_gap(target_replay_count, replay_count_for_asset)
    remaining_distinct_source_gap = _remaining_gap(target_distinct_source_count, distinct_source_count_for_asset)
    progress_status, progress_reason = _classify_progress_status(
        decision_status=decision_status,
        in_progress_manifest_count=in_progress_manifest_count,
    )
    return {
        "asset_id": asset_id,
        "progress_status": progress_status,
        "progress_reason": progress_reason,
        "queue_status": queue_status,
        "decision_status": decision_status,
        "triage_status": triage_status,
        "expansion_manifest_count": expansion_manifest_count,
        "in_progress_manifest_count": in_progress_manifest_count,
        "linked_session_count": linked_session_count,
        "linked_promotion_count": linked_promotion_count,
        "replay_count_for_asset": replay_count_for_asset,
        "distinct_source_count_for_asset": distinct_source_count_for_asset,
        "target_replay_count": target_replay_count,
        "target_distinct_source_count": target_distinct_source_count,
        "remaining_replay_gap": remaining_replay_gap,
        "remaining_distinct_source_gap": remaining_distinct_source_gap,
    }


def _classify_progress_status(
    *,
    decision_status: str | None,
    in_progress_manifest_count: int,
) -> tuple[str, str]:
    if decision_status == "ready_to_publish":
        return "ready_to_publish", "publish_decision_ready_to_publish"
    if decision_status == "needs_broader_evidence":
        if in_progress_manifest_count > 0:
            return "actively_collecting", "needs_broader_evidence_with_active_expansion_work"
        return "waiting_for_work", "needs_broader_evidence_without_active_expansion_work"
    return "not_triaged", "missing_publish_decision_row"


def _aggregate_queue_status(rows: list[dict[str, Any]]) -> str | None:
    for status in QUEUE_STATUS_ORDER:
        if any(str(row.get("status") or "").strip() == status for row in rows):
            return status
    return None


def _group_queue_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset_id = str(row.get("asset_id") or "").strip()
        if not asset_id:
            continue
        grouped.setdefault(asset_id, []).append(row)
    return grouped


def _first_asset_row(rows: list[dict[str, Any]], asset_id: str) -> dict[str, Any] | None:
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("asset_id") or "").strip() == asset_id:
            return row
    return None


def _remaining_gap(target: int | None, current: int | None) -> int | None:
    if target is None or current is None:
        return None
    return max(target - current, 0)


def _combined_gap(row: dict[str, Any]) -> int:
    return (_safe_int(row.get("remaining_replay_gap")) or 0) + (_safe_int(row.get("remaining_distinct_source_gap")) or 0)


def _progress_sort_key(row: dict[str, Any]) -> tuple[int, int, int, str]:
    status_rank = STATUS_ORDER.get(str(row.get("progress_status") or ""), len(STATUS_ORDER))
    in_progress_manifest_count = _safe_int(row.get("in_progress_manifest_count")) or 0
    return (
        status_rank,
        -_combined_gap(row),
        -in_progress_manifest_count,
        str(row.get("asset_id") or ""),
    )


def _validate_queue_manifest(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != QUEUE_SCHEMA_VERSION:
        raise ValueError("queue manifest schema_version is invalid")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("queue manifest rows must be a list")


def _validate_publish_decision_manifest(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != PUBLISH_DECISION_SCHEMA_VERSION:
        raise ValueError("publish decision manifest schema_version is invalid")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("publish decision manifest rows must be a list")


def _validate_promotion_triage_manifest(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != PROMOTION_TRIAGE_SCHEMA_VERSION:
        raise ValueError("promotion triage manifest schema_version is invalid")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("promotion triage manifest rows must be a list")


def _resolve_output_path(*, game: str, output_path: str | Path | None) -> Path:
    return _artifact_resolve_timestamped_output_path(
        output_path=output_path,
        default_dir=DEFAULT_OUTPUT_ROOT / game / "evidence_expansion_progress",
        filename_suffix="detector_calibration_evidence_expansion_progress_manifest.json",
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
    parser = argparse.ArgumentParser(description="Generate detector calibration evidence expansion progress manifest")
    parser.add_argument("--queue-manifest", required=True)
    parser.add_argument("--publish-decision-manifest", required=True)
    parser.add_argument("--promotion-triage-manifest", required=True)
    parser.add_argument("--output-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_evidence_expansion_progress_manifest(
        queue_manifest=args.queue_manifest,
        publish_decision_manifest=args.publish_decision_manifest,
        promotion_triage_manifest=args.promotion_triage_manifest,
        output_path=args.output_path,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
