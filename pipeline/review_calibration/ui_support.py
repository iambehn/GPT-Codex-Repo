from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline.artifact_paths import resolve_path as _artifact_resolve_path


def resolve_review_path(path_like: str | Path) -> Path:
    return _artifact_resolve_path(path_like)


def load_json_payload(path: str | Path) -> dict[str, Any]:
    return json.loads(resolve_review_path(path).read_text(encoding="utf-8"))


def load_json_payload_or_empty(path: str | Path) -> dict[str, Any]:
    try:
        return load_json_payload(path)
    except (OSError, json.JSONDecodeError):
        return {}


def load_optional_sidecar_payload(
    path: str | Path | None,
    *,
    schema_version: str,
) -> tuple[Path | None, dict[str, Any] | bool | None]:
    if path is None:
        return None, None
    resolved = resolve_review_path(path)
    payload = load_json_payload(resolved)
    if payload.get("schema_version") != schema_version:
        return resolved, False
    return resolved, payload


def load_optional_report(path: str | Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return load_json_payload(path)


def load_report_bundle(
    *,
    fixture_comparison_report: str | Path | None,
    fixture_trial_batch_manifest: str | Path | None,
    proxy_calibration_report: str | Path | None,
    proxy_replay_report: str | Path | None,
    runtime_calibration_report: str | Path | None,
    runtime_replay_report: str | Path | None,
) -> dict[str, Any]:
    return {
        "fixture_comparison": load_optional_report(fixture_comparison_report),
        "fixture_trial_batch": load_optional_report(fixture_trial_batch_manifest),
        "proxy_calibration": load_optional_report(proxy_calibration_report),
        "proxy_replay": load_optional_report(proxy_replay_report),
        "runtime_calibration": load_optional_report(runtime_calibration_report),
        "runtime_replay": load_optional_report(runtime_replay_report),
    }


def payload_text(payload: dict[str, Any] | None, key: str) -> str:
    return str(payload.get(key, "")).strip() if isinstance(payload, dict) else ""


def first_nonempty(*values: str) -> str:
    for value in values:
        if str(value).strip():
            return str(value).strip()
    return ""


def resolve_media_path(value: Any) -> Path | None:
    source_text = str(value or "").strip()
    if not source_text:
        return None
    return resolve_review_path(source_text)


def sidecar_mismatch(
    proxy_payload: dict[str, Any] | None,
    runtime_payload: dict[str, Any] | None,
    fused_payload: dict[str, Any] | None,
) -> str | None:
    games = {
        payload_text(payload, "game")
        for payload in (proxy_payload, runtime_payload, fused_payload)
        if payload_text(payload, "game")
    }
    if len(games) > 1:
        return "provided sidecars refer to different games"
    sources = {
        payload_text(payload, "source")
        for payload in (proxy_payload, runtime_payload, fused_payload)
        if payload_text(payload, "source")
    }
    if len(sources) > 1:
        return "provided sidecars refer to different sources"
    return None


def load_detector_calibration_followup_manifest(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {
            "ok": True,
            "status": "no_followup_manifest",
            "manifest_loaded": False,
            "rows": [],
        }
    resolved = resolve_review_path(path)
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "status": "invalid_detector_calibration_followup_manifest",
            "followup_manifest_path": str(resolved),
            "error": f"failed to load detector calibration follow-up manifest: {exc}",
        }
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        rows = []
    normalized_rows = [row for row in rows if isinstance(row, dict)]
    return {
        "ok": True,
        "status": "ok",
        "manifest_loaded": True,
        "manifest_path": str(resolved),
        "rows": normalized_rows,
    }


def active_review_records(
    records: list[dict[str, Any]],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
    show_only_followup_records: bool,
) -> list[dict[str, Any]]:
    if not show_only_followup_records:
        return list(records)
    return [
        row
        for row in records
        if match_calibration_followup_rows(
            row,
            manifest_loaded=manifest_loaded,
            followup_rows=followup_rows,
        )
    ]


def resolve_active_record_id(current_record_id: str | None, active_record_ids: list[str]) -> str | None:
    current = str(current_record_id or "").strip()
    if current and current in active_record_ids:
        return current
    if active_record_ids:
        return active_record_ids[0]
    return None


def calibration_followup_counts(
    records: list[dict[str, Any]],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
) -> tuple[int, int]:
    total_count = len(records)
    if not manifest_loaded:
        return 0, total_count
    matched_count = len(
        active_review_records(
            records,
            manifest_loaded=manifest_loaded,
            followup_rows=followup_rows,
            show_only_followup_records=True,
        )
    )
    return matched_count, total_count


def followup_toggle_label(
    records: list[dict[str, Any]],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
) -> str:
    matched_count, total_count = calibration_followup_counts(
        records,
        manifest_loaded=manifest_loaded,
        followup_rows=followup_rows,
    )
    return f"Show only calibration follow-up records ({matched_count}/{total_count})"


def render_calibration_followup_status(
    records: list[dict[str, Any]],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
    show_only_followup_records: bool,
) -> str:
    if not manifest_loaded:
        return "Calibration follow-up manifest not loaded."
    matched_count, total_count = calibration_followup_counts(
        records,
        manifest_loaded=manifest_loaded,
        followup_rows=followup_rows,
    )
    if show_only_followup_records:
        return f"Showing {matched_count} of {total_count} records with calibration follow-up."
    return f"{matched_count} of {total_count} records have calibration follow-up."


def match_calibration_followup_rows(
    row: dict[str, Any],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not manifest_loaded:
        return []
    if str(row.get("kind") or "").strip() != "sidecar":
        return []
    runtime_sidecar_path = str(row.get("runtime_sidecar_path") or "").strip()
    if runtime_sidecar_path:
        return [
            candidate
            for candidate in followup_rows
            if str(candidate.get("runtime_sidecar_path") or "").strip() == runtime_sidecar_path
        ]
    source = str(row.get("source") or "").strip()
    if not source:
        return []
    return [
        candidate
        for candidate in followup_rows
        if str(candidate.get("source") or "").strip() == source
        and str(candidate.get("asset_id") or "").strip()
    ]


def sort_calibration_followup_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    indexed_rows = list(enumerate(rows))

    def _sort_key(item: tuple[int, dict[str, Any]]) -> tuple[float, float, int]:
        index, row = item
        absolute_delta = row.get("absolute_delta_iou")
        if absolute_delta is None:
            delta_value = row.get("delta_iou")
            try:
                absolute_delta = abs(float(delta_value))
            except (TypeError, ValueError):
                absolute_delta = -1.0
        else:
            try:
                absolute_delta = float(absolute_delta)
            except (TypeError, ValueError):
                absolute_delta = -1.0
        replay_created_at = str(row.get("replay_created_at") or "").strip()
        replay_recency = 0.0
        if replay_created_at:
            try:
                replay_recency = datetime.fromisoformat(replay_created_at).timestamp()
            except ValueError:
                replay_recency = 0.0
        return (-absolute_delta, -replay_recency, index)

    sorted_rows = sorted(indexed_rows, key=_sort_key, reverse=False)
    return [row for _, row in sorted_rows]


def top_calibration_followup_payload(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    top_row = rows[0]
    return {
        "review_record_path": top_row.get("review_record_path"),
        "run_id": top_row.get("run_id"),
        "runtime_sidecar_path": top_row.get("runtime_sidecar_path"),
        "difference_summary": top_row.get("difference_summary"),
        "delta_iou": top_row.get("delta_iou"),
        "absolute_delta_iou": top_row.get("absolute_delta_iou"),
        "primary_iou": top_row.get("primary_iou"),
        "secondary_iou": top_row.get("secondary_iou"),
        "primary_source": top_row.get("primary_source"),
        "secondary_source": top_row.get("secondary_source"),
        "primary_reference_crop": top_row.get("primary_reference_crop"),
        "secondary_reference_crop": top_row.get("secondary_reference_crop"),
        "primary_reference_source": top_row.get("primary_reference_source"),
        "secondary_reference_source": top_row.get("secondary_reference_source"),
    }


def render_calibration_followup_summary(
    row: dict[str, Any],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
) -> str:
    if not manifest_loaded:
        return "No calibration follow-up manifest loaded."
    matched_rows = match_calibration_followup_rows(
        row,
        manifest_loaded=manifest_loaded,
        followup_rows=followup_rows,
    )
    matched_rows = sort_calibration_followup_rows(matched_rows)
    if not matched_rows:
        return "No calibration follow-up rows for this record."

    def _format_followup_row(matched_row: dict[str, Any]) -> str:
        return "\n".join(
            [
                f"Replay run id: {matched_row.get('run_id') or 'n/a'}",
                f"Candidate id: {matched_row.get('candidate_id') or 'n/a'}",
                f"Asset id: {matched_row.get('asset_id') or 'n/a'}",
                f"Difference summary: {matched_row.get('difference_summary') or 'n/a'}",
                f"Primary source: {matched_row.get('primary_source') or 'n/a'}",
                f"Secondary source: {matched_row.get('secondary_source') or 'n/a'}",
                f"Primary IoU: {matched_row.get('primary_iou') if matched_row.get('primary_iou') is not None else 'n/a'}",
                f"Secondary IoU: {matched_row.get('secondary_iou') if matched_row.get('secondary_iou') is not None else 'n/a'}",
                f"Delta IoU: {matched_row.get('delta_iou') if matched_row.get('delta_iou') is not None else 'n/a'}",
                f"Review record path: {matched_row.get('review_record_path') or 'n/a'}",
            ]
        )

    sections: list[str] = [
        "Top follow-up",
        f"Top follow-up review record path: {matched_rows[0].get('review_record_path') or 'n/a'}",
        f"Top follow-up replay run id: {matched_rows[0].get('run_id') or 'n/a'}",
        f"Top follow-up runtime sidecar path: {matched_rows[0].get('runtime_sidecar_path') or 'n/a'}",
        _format_followup_row(matched_rows[0]),
    ]
    if len(matched_rows) > 1:
        additional_rows = "\n\n".join(_format_followup_row(matched_row) for matched_row in matched_rows[1:])
        sections.extend(["Additional follow-ups", additional_rows])
    return "\n\n".join(sections).strip()


def render_calibration_handoff_summary(viewer_payload: dict[str, Any] | None) -> str:
    if not isinstance(viewer_payload, dict):
        return ""
    selected_item_id = str(viewer_payload.get("selected_item_id") or "").strip()
    if not selected_item_id:
        return "No detector calibration handoff available for this record."
    handoffs = viewer_payload.get("detector_calibration_handoffs", {})
    if not isinstance(handoffs, dict):
        return "No detector calibration handoff available for this record."
    row = handoffs.get("by_item_id", {}).get(selected_item_id, {}) if isinstance(handoffs.get("by_item_id"), dict) else {}
    if not isinstance(row, dict) or not row.get("available"):
        return "No detector calibration handoff available for this record."
    comparison = row.get("template_comparison")
    comparison_lines = ["Crop comparison: unavailable"]
    if isinstance(comparison, dict):
        if str(comparison.get("status") or "").strip() == "ok":
            published = comparison.get("published_dimensions", {}) if isinstance(comparison.get("published_dimensions"), dict) else {}
            revised = comparison.get("revised_dimensions", {}) if isinstance(comparison.get("revised_dimensions"), dict) else {}
            delta = comparison.get("delta", {}) if isinstance(comparison.get("delta"), dict) else {}
            comparison_lines = [
                f"Crop comparison source: {row.get('template_comparison_source') or 'crop_candidate'}",
                f"Crop comparison: published {published.get('width', 'n/a')}x{published.get('height', 'n/a')} -> revised {revised.get('width', 'n/a')}x{revised.get('height', 'n/a')}",
                f"Crop comparison delta: {delta.get('width', 'n/a')}x / {delta.get('height', 'n/a')}y",
                f"Crop comparison dimensions match: {bool(comparison.get('dimensions_match', False))}",
            ]
            overlap = comparison.get("spatial_overlap", {}) if isinstance(comparison.get("spatial_overlap"), dict) else {}
            if overlap:
                intersection = overlap.get("intersection", {}) if isinstance(overlap.get("intersection"), dict) else {}
                comparison_lines.extend(
                    [
                        f"Spatial overlap reference: {overlap.get('reference_source', 'unknown')} {overlap.get('reference_crop', 'n/a')}",
                        f"Spatial overlap intersection: {intersection.get('w', 0)}x{intersection.get('h', 0)} @ {intersection.get('x', 0)},{intersection.get('y', 0)}",
                        f"Spatial overlap IoU: {overlap.get('iou', 'n/a')}",
                        f"Spatial overlap revised coverage: {overlap.get('revised_coverage_ratio', 'n/a')}",
                        f"Spatial overlap reference coverage: {overlap.get('reference_coverage_ratio', 'n/a')}",
                    ]
                )
        else:
            comparison_lines = [f"Crop comparison: {comparison.get('status') or 'unavailable'}"]
    difference_summary = str(row.get("template_comparison_difference_summary") or "").strip()
    if difference_summary:
        comparison_lines.append(f"Difference summary: {difference_summary}")
    secondary_comparison = row.get("template_comparison_secondary")
    if isinstance(secondary_comparison, dict) and str(secondary_comparison.get("status") or "").strip() == "ok":
        secondary_delta = secondary_comparison.get("delta", {}) if isinstance(secondary_comparison.get("delta"), dict) else {}
        comparison_lines.append(f"Candidate-time baseline source: {row.get('template_comparison_secondary_source') or 'crop_candidate'}")
        comparison_lines.append(
            f"Candidate-time baseline delta: {secondary_delta.get('width', 'n/a')}x / {secondary_delta.get('height', 'n/a')}y"
        )
        secondary_overlap = secondary_comparison.get("spatial_overlap", {}) if isinstance(secondary_comparison.get("spatial_overlap"), dict) else {}
        if secondary_overlap:
            secondary_intersection = secondary_overlap.get("intersection", {}) if isinstance(secondary_overlap.get("intersection"), dict) else {}
            comparison_lines.extend(
                [
                    f"Candidate-time baseline overlap reference: {secondary_overlap.get('reference_source', 'unknown')} {secondary_overlap.get('reference_crop', 'n/a')}",
                    f"Candidate-time baseline IoU: {secondary_overlap.get('iou', 'n/a')}",
                    f"Candidate-time baseline revised coverage: {secondary_overlap.get('revised_coverage_ratio', 'n/a')}",
                    f"Candidate-time baseline reference coverage: {secondary_overlap.get('reference_coverage_ratio', 'n/a')}",
                    f"Candidate-time baseline intersection: {secondary_intersection.get('w', 0)}x{secondary_intersection.get('h', 0)} @ {secondary_intersection.get('x', 0)},{secondary_intersection.get('y', 0)}",
                ]
            )
    return "\n".join(
        [
            f"Asset id: {row.get('asset_id') or 'n/a'}",
            f"ROI: {row.get('roi_ref') or 'n/a'}",
            f"Timestamp: {row.get('timestamp_seconds') if row.get('timestamp_seconds') is not None else 'n/a'}",
            f"Suggested judgment: {row.get('suggested_judgment') or 'n/a'}",
            f"Suggested cause: {row.get('suggested_suspected_cause') or 'n/a'}",
            f"Suggested crop: {row.get('suggested_crop') or row.get('suggested_crop_placeholder') or 'x,y,w,h'}",
            f"Suggested crop source: {row.get('suggested_crop_source') or 'placeholder'}",
            *comparison_lines,
            "Workflow note: Run Init first. The session tool will return concrete Create Crop and Replay commands with the real session root.",
            "",
            "Init:",
            str(row.get("init_command") or row.get("command") or ""),
            "",
            "Create Crop:",
            str(row.get("create_crop_command_template") or ""),
            "",
            "Replay:",
            str(row.get("replay_command_template") or ""),
        ]
    ).strip()
