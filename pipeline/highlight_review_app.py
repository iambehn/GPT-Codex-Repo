from __future__ import annotations

import importlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.clip_registry import load_candidate_lifecycle_details, load_hook_candidate_details
from pipeline.evaluation_fixtures import load_evaluation_fixture_manifest
from pipeline.fused_review_bridge import FUSED_REVIEW_SESSION_SCHEMA_VERSION
from pipeline.proxy_review_bridge import PROXY_REVIEW_SESSION_SCHEMA_VERSION
from pipeline.review_calibration import ui_support as review_calibration_ui_support
from pipeline.unified_replay_viewer import render_unified_replay_viewer


def load_highlight_review_records(
    *,
    sidecar_root: str | Path | None = None,
    fixture_manifest_path: str | Path | None = None,
    fixture_comparison_report: str | Path | None = None,
    fixture_trial_batch_manifest: str | Path | None = None,
    proxy_review_session_manifest: str | Path | None = None,
    fused_review_session_manifest: str | Path | None = None,
    registry_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    comparison_rows_by_fixture: dict[str, list[dict[str, Any]]] = {}
    batch_summary_by_fixture: dict[str, list[dict[str, Any]]] = {}
    if fixture_comparison_report is not None:
        comparison_payload = _load_json(_resolve_path(fixture_comparison_report))
        for row in list(comparison_payload.get("comparison", {}).get("fixture_rows", [])):
            if not isinstance(row, dict):
                continue
            fixture_id = str(row.get("fixture_id", "")).strip()
            if not fixture_id:
                continue
            comparison_rows_by_fixture.setdefault(fixture_id, []).append(row)
    if fixture_trial_batch_manifest is not None:
        batch_payload = _load_json(_resolve_path(fixture_trial_batch_manifest))
        for comparison in list(batch_payload.get("trial_comparisons", [])):
            if not isinstance(comparison, dict):
                continue
            report_path = str(comparison.get("comparison_report_path", "")).strip()
            if not report_path:
                continue
            report_payload = _load_json(_resolve_path(report_path))
            for row in list(report_payload.get("comparison", {}).get("fixture_rows", [])):
                if not isinstance(row, dict):
                    continue
                fixture_id = str(row.get("fixture_id", "")).strip()
                if not fixture_id:
                    continue
                batch_summary_by_fixture.setdefault(fixture_id, []).append(
                    {
                        "trial_name": str(comparison.get("trial_name", "")).strip(),
                        "artifact_layer": str(comparison.get("artifact_layer", "")).strip(),
                        "comparison_status": str(comparison.get("comparison_status", "")).strip(),
                        "recommendation": dict(comparison.get("recommendation", {})),
                        "comparison_report_path": report_path,
                    }
                )
    if fixture_manifest_path is not None:
        manifest = load_evaluation_fixture_manifest(fixture_manifest_path)
        for row in list(manifest.get("fixtures", [])):
            records.append(
                {
                    "record_id": str(row["fixture_id"]),
                    "label": str(row["label"]),
                    "kind": "fixture",
                    "game": None,
                    "source": None,
                    "expected_review_outcome": str(row["expected_review_outcome"]),
                    "latency_budget_class": str(row["latency_budget_class"]),
                    "artifact_refs": dict(row.get("artifact_refs", {})),
                    "expected_artifacts": dict(row.get("expected_artifacts", {})),
                    "notes": str(row.get("notes", "")),
                    "fixture_comparison_rows": comparison_rows_by_fixture.get(str(row["fixture_id"]), []),
                    "fixture_trial_batch_rows": batch_summary_by_fixture.get(str(row["fixture_id"]), []),
                }
            )

    if proxy_review_session_manifest is not None:
        records.extend(_load_proxy_review_session_records(proxy_review_session_manifest))
    if fused_review_session_manifest is not None:
        records.extend(_load_fused_review_session_records(fused_review_session_manifest))

    if sidecar_root is not None:
        root = _resolve_path(sidecar_root)
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for path in sorted(root.rglob("*.json")):
            if path.name.endswith(".proxy_scan.json"):
                payload = _load_json(path)
                key = (str(payload.get("game", "")), str(payload.get("source", "")))
                record = grouped.setdefault(key, _base_sidecar_record(payload))
                record["proxy_sidecar_path"] = str(path.resolve())
                review = payload.get("proxy_review", {})
                if isinstance(review, dict):
                    record["proxy_review_status"] = str(review.get("review_status", "")).strip() or None
            elif path.name.endswith(".runtime_analysis.json"):
                payload = _load_json(path)
                key = (str(payload.get("game", "")), str(payload.get("source", "")))
                record = grouped.setdefault(key, _base_sidecar_record(payload))
                record["runtime_sidecar_path"] = str(path.resolve())
                review = payload.get("runtime_review", {})
                if isinstance(review, dict):
                    record["runtime_review_status"] = str(review.get("review_status", "")).strip() or None
            elif path.name.endswith(".fused_analysis.json"):
                payload = _load_json(path)
                key = (str(payload.get("game", "")), str(payload.get("source", "")))
                record = grouped.setdefault(key, _base_sidecar_record(payload))
                record["fused_sidecar_path"] = str(path.resolve())
                review = payload.get("fused_review", {})
                if isinstance(review, dict):
                    events = review.get("events", {})
                    if isinstance(events, dict):
                        statuses = {
                            str(event.get("review_status", "")).strip()
                            for event in events.values()
                            if isinstance(event, dict) and str(event.get("review_status", "")).strip()
                        }
                        if len(statuses) == 1:
                            record["fused_review_status"] = next(iter(statuses))
                        elif statuses:
                            record["fused_review_status"] = "mixed"
                lifecycle_rows = load_candidate_lifecycle_details(
                    game=str(payload.get("game", "")).strip() or None,
                    source=str(payload.get("source", "")).strip() or None,
                    fused_sidecar_path=path,
                    registry_path=registry_path,
                )
                if lifecycle_rows:
                    record["candidate_lifecycle_states"] = sorted(
                        {
                            str(row.get("lifecycle_state") or "").strip()
                            for row in lifecycle_rows
                            if str(row.get("lifecycle_state") or "").strip()
                        }
                    )
                    record["candidate_lifecycle_count"] = len(lifecycle_rows)
                    record["selected_highlight_event_types"] = sorted(
                        {
                            event_type
                            for row in lifecycle_rows
                            for event_type in [_selected_highlight_details_from_lifecycle_row(row).get("event_type")]
                            if isinstance(event_type, str) and event_type.strip()
                        }
                    )
                    record["selected_highlight_producer_families"] = sorted(
                        {
                            family
                            for row in lifecycle_rows
                            for family in _selected_highlight_producer_families_from_lifecycle_row(row)
                        }
                    )
                    record["selected_highlight_fusion_ids"] = sorted(
                        {
                            fusion_id
                            for row in lifecycle_rows
                            for fusion_id in [_selected_highlight_details_from_lifecycle_row(row).get("fusion_id")]
                            if isinstance(fusion_id, str) and fusion_id.strip()
                        }
                    )
                hook_rows = load_hook_candidate_details(
                    game=str(payload.get("game", "")).strip() or None,
                    source=str(payload.get("source", "")).strip() or None,
                    fused_sidecar_path=path,
                    registry_path=registry_path,
                )
                if hook_rows:
                    record["hook_candidate_count"] = len(hook_rows)
                    record["hook_modes"] = sorted(
                        {
                            str(row.get("hook_mode") or "").strip()
                            for row in hook_rows
                            if str(row.get("hook_mode") or "").strip()
                        }
                    )
                    strongest = max(
                        hook_rows,
                        key=lambda row: float(row.get("hook_strength", 0.0) or 0.0),
                    )
                    record["strongest_hook_archetype"] = str(strongest.get("hook_archetype") or "").strip() or None

        records.extend(
            sorted(grouped.values(), key=lambda row: (str(row.get("game") or ""), str(row.get("source") or "")))
        )
    return records


def launch_highlight_review_app(
    *,
    sidecar_root: str | Path | None = None,
    fixture_manifest_path: str | Path | None = None,
    fixture_comparison_report: str | Path | None = None,
    fixture_trial_batch_manifest: str | Path | None = None,
    proxy_review_session_manifest: str | Path | None = None,
    fused_review_session_manifest: str | Path | None = None,
    proxy_calibration_report: str | Path | None = None,
    proxy_replay_report: str | Path | None = None,
    runtime_calibration_report: str | Path | None = None,
    runtime_replay_report: str | Path | None = None,
    detector_calibration_followup_manifest: str | Path | None = None,
    registry_path: str | Path | None = None,
    output_path: str | Path | None = None,
    launch: bool = True,
) -> dict[str, Any]:
    records = load_highlight_review_records(
        sidecar_root=sidecar_root,
        fixture_manifest_path=fixture_manifest_path,
        fixture_comparison_report=fixture_comparison_report,
        fixture_trial_batch_manifest=fixture_trial_batch_manifest,
        proxy_review_session_manifest=proxy_review_session_manifest,
        fused_review_session_manifest=fused_review_session_manifest,
        registry_path=registry_path,
    )
    if not records:
        return {
            "ok": False,
            "status": "no_review_records",
            "error": "no fixture records or sidecar records were found",
        }
    followup_manifest_result = _load_detector_calibration_followup_manifest(
        detector_calibration_followup_manifest
    )
    if not followup_manifest_result["ok"]:
        return followup_manifest_result

    try:
        gradio = importlib.import_module("gradio")
    except Exception as exc:
        return {
            "ok": False,
            "status": "missing_review_runtime",
            "error": f"gradio is required for the highlight review app: {exc}",
        }

    records_by_id = {str(row["record_id"]): row for row in records}
    record_order = [str(row["record_id"]) for row in records]
    followup_rows = list(followup_manifest_result.get("rows", []))

    def _render_empty_record() -> tuple[str, str | None, str, str, str, str, str]:
        return (
            "## No records available\n- Current selector filter produced no review records.",
            None,
            _display_path("Primary viewer path", None),
            _display_path("Secondary viewer path", None),
            "No detector calibration handoff available for this record.",
            _render_calibration_followup_summary(
                {},
                manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
                followup_rows=followup_rows,
            ),
            json.dumps({"status": "empty_active_record_set"}, indent=2),
        )

    def _render_record(record_id: str | None) -> tuple[str, str | None, str, str, str, str, str]:
        if not record_id:
            return _render_empty_record()
        row = records_by_id[str(record_id)]
        summary = _record_summary(row)
        if row.get("kind") == "proxy_review_session_item":
            return (
                summary,
                str(row.get("processed_clip_path") or "") or None,
                _display_path("Source clip path", row.get("source_clip_path")),
                _display_path("Proxy sidecar path", row.get("proxy_sidecar_path")),
                "",
                _render_calibration_followup_summary(
                    row,
                    manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
                    followup_rows=followup_rows,
                ),
                json.dumps(_proxy_review_session_payload(row), indent=2),
            )
        if row.get("kind") == "fused_review_session_item":
            return (
                summary,
                str(row.get("processed_clip_path") or "") or None,
                _display_path("Source clip path", row.get("source_clip_path")),
                _display_path("Fused sidecar path", row.get("fused_sidecar_path")),
                "",
                _render_calibration_followup_summary(
                    row,
                    manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
                    followup_rows=followup_rows,
                ),
                json.dumps(_fused_review_session_payload(row), indent=2),
            )
        if row.get("kind") == "fixture":
            comparison_rows = list(row.get("fixture_comparison_rows", []))
            batch_rows = list(row.get("fixture_trial_batch_rows", []))
            baseline_path = ""
            trial_path = ""
            render_payload: dict[str, Any] = {"fixture": row}
            if comparison_rows:
                preferred = _preferred_fixture_row(comparison_rows)
                baseline_path = _render_fixture_viewer(
                    preferred,
                    side="baseline",
                    fixture_comparison_report=fixture_comparison_report,
                    fixture_trial_batch_manifest=fixture_trial_batch_manifest,
                    proxy_calibration_report=proxy_calibration_report,
                    proxy_replay_report=proxy_replay_report,
                    runtime_calibration_report=runtime_calibration_report,
                    runtime_replay_report=runtime_replay_report,
                    registry_path=registry_path,
                    output_path=output_path,
                )
                trial_path = _render_fixture_viewer(
                    preferred,
                    side="trial",
                    fixture_comparison_report=fixture_comparison_report,
                    fixture_trial_batch_manifest=fixture_trial_batch_manifest,
                    proxy_calibration_report=proxy_calibration_report,
                    proxy_replay_report=proxy_replay_report,
                    runtime_calibration_report=runtime_calibration_report,
                    runtime_replay_report=runtime_replay_report,
                    registry_path=registry_path,
                    output_path=output_path,
                )
                render_payload["preferred_comparison"] = preferred
                if batch_rows:
                    render_payload["fixture_trial_batch_rows"] = batch_rows
                return (
                    summary,
                    None,
                    _display_path("Primary viewer path", baseline_path),
                    _display_path("Secondary viewer path", trial_path),
                    "",
                    _render_calibration_followup_summary(
                        row,
                        manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
                        followup_rows=followup_rows,
                    ),
                    json.dumps(render_payload, indent=2),
                )
        result = render_unified_replay_viewer(
            proxy_sidecar=row.get("proxy_sidecar_path"),
            runtime_sidecar=row.get("runtime_sidecar_path"),
            fused_sidecar=row.get("fused_sidecar_path"),
            fixture_comparison_report=fixture_comparison_report,
            fixture_trial_batch_manifest=fixture_trial_batch_manifest,
            proxy_calibration_report=proxy_calibration_report,
            proxy_replay_report=proxy_replay_report,
            runtime_calibration_report=runtime_calibration_report,
            runtime_replay_report=runtime_replay_report,
            registry_path=registry_path,
            output_path=output_path,
        )
        calibration_handoff_summary = _render_calibration_handoff_summary(result.get("viewer_payload"))
        matched_followup_rows = _match_calibration_followup_rows(
            row,
            manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
            followup_rows=followup_rows,
        )
        matched_followup_rows = _sort_calibration_followup_rows(matched_followup_rows)
        followup_summary = _render_calibration_followup_summary(
            row,
            manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
            followup_rows=followup_rows,
        )
        debug_result = dict(result)
        debug_result["matched_calibration_followup_rows"] = matched_followup_rows
        top_calibration_followup = _top_calibration_followup_payload(matched_followup_rows)
        if top_calibration_followup is not None:
            debug_result["top_calibration_followup"] = top_calibration_followup
        return (
            summary,
            None,
            _display_path("Primary viewer path", result.get("viewer_path")),
            _display_path("Secondary viewer path", None),
            calibration_handoff_summary,
            followup_summary,
            json.dumps(debug_result, indent=2),
        )

    def _next_record_id(record_id: str, *, active_record_ids: list[str]) -> str:
        try:
            index = active_record_ids.index(str(record_id))
        except ValueError:
            return str(record_id)
        for candidate_id in active_record_ids[index + 1 :]:
            candidate_row = records_by_id.get(candidate_id, {})
            if not _is_review_session_item(candidate_row):
                return candidate_id
            if str(candidate_row.get("review_status") or "unreviewed") == "unreviewed":
                return candidate_id
        return str(record_id)

    def _render_active_record(
        record_id: str | None,
        show_only_followup_records: bool,
    ) -> tuple[dict[str, Any], dict[str, Any], str, str, str | None, str, str, str, str, str]:
        active_rows = _active_review_records(
            records,
            manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
            followup_rows=followup_rows,
            show_only_followup_records=show_only_followup_records,
        )
        active_record_ids = [str(item["record_id"]) for item in active_rows]
        resolved_record_id = _resolve_active_record_id(record_id, active_record_ids)
        selector_payload = _selector_update_payload(
            choices=[(str(item["label"]), str(item["record_id"])) for item in active_rows],
            value=resolved_record_id,
        )
        toggle_payload = _toggle_update_payload(
            label=_followup_toggle_label(
                records,
                manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
                followup_rows=followup_rows,
            ),
            value=show_only_followup_records,
        )
        followup_status = _render_calibration_followup_status(
            records,
            manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
            followup_rows=followup_rows,
            show_only_followup_records=show_only_followup_records,
        )
        if resolved_record_id is None:
            summary, media_path, primary_path, secondary_path, calibration_handoff, calibration_followup, payload = _render_empty_record()
        else:
            summary, media_path, primary_path, secondary_path, calibration_handoff, calibration_followup, payload = _render_record(resolved_record_id)
        return (
            selector_payload,
            toggle_payload,
            followup_status,
            summary,
            media_path,
            primary_path,
            secondary_path,
            calibration_handoff,
            calibration_followup,
            payload,
        )

    def _apply_decision(
        record_id: str | None,
        show_only_followup_records: bool,
        decision: str,
    ) -> tuple[dict[str, Any], dict[str, Any], str, str, str | None, str, str, str, str, str, str]:
        if not record_id:
            selector_payload, toggle_payload, followup_status, summary, media_path, primary_path, secondary_path, calibration_handoff, calibration_followup, payload = _render_active_record(
                None,
                show_only_followup_records,
            )
            return (
                selector_payload,
                toggle_payload,
                followup_status,
                summary,
                media_path,
                primary_path,
                secondary_path,
                calibration_handoff,
                calibration_followup,
                payload,
                "No active record is available for the current selector filter.",
            )
        row = records_by_id[str(record_id)]
        if not _is_review_session_item(row):
            selector_payload, toggle_payload, followup_status, summary, media_path, primary_path, secondary_path, calibration_handoff, calibration_followup, payload = _render_active_record(
                record_id,
                show_only_followup_records,
            )
            return (
                selector_payload,
                toggle_payload,
                followup_status,
                summary,
                media_path,
                primary_path,
                secondary_path,
                calibration_handoff,
                calibration_followup,
                payload,
                "Selected record is not a review session item.",
            )
        active_rows = _active_review_records(
            records,
            manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
            followup_rows=followup_rows,
            show_only_followup_records=show_only_followup_records,
        )
        active_record_ids = [str(item["record_id"]) for item in active_rows]
        if row.get("kind") == "proxy_review_session_item":
            write_result = _finalize_proxy_review_session_decision(row, decision)
        else:
            write_result = _finalize_fused_review_session_decision(row, decision)
        if write_result.get("ok"):
            row["review_status"] = write_result.get("review_status")
            row["reviewed_at"] = write_result.get("reviewed_at")
            if write_result.get("gpt_meta_path"):
                row["gpt_meta_path"] = str(write_result["gpt_meta_path"])
            if write_result.get("gpt_final_path"):
                row["gpt_final_path"] = str(write_result["gpt_final_path"])
            if write_result.get("gpt_meta_payload"):
                row["gpt_meta_payload"] = dict(write_result["gpt_meta_payload"])
            next_record_id = _next_record_id(record_id, active_record_ids=active_record_ids)
            if next_record_id == str(record_id):
                status_message = f"Updated review status to {row['review_status']}. Reached last item."
            else:
                status_message = f"Updated review status to {row['review_status']}. Moved to next item."
        else:
            next_record_id = str(record_id)
            status_message = str(write_result.get("error") or "Failed to update review status.")
        selector_payload, toggle_payload, followup_status, summary, media_path, primary_path, secondary_path, calibration_handoff, calibration_followup, payload = _render_active_record(
            next_record_id,
            show_only_followup_records,
        )
        return selector_payload, toggle_payload, followup_status, summary, media_path, primary_path, secondary_path, calibration_handoff, calibration_followup, payload, status_message

    with gradio.Blocks(title="Highlight Review App") as app:
        gradio.Markdown("# Highlight Review App")
        initial_rows = _active_review_records(
            records,
            manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
            followup_rows=followup_rows,
            show_only_followup_records=False,
        )
        initial_record_id = str(initial_rows[0]["record_id"]) if initial_rows else None
        selector = gradio.Dropdown(
            choices=[(str(row["label"]), str(row["record_id"])) for row in initial_rows],
            value=initial_record_id,
            label="Fixture or reviewed clip",
        )
        followup_only_toggle = gradio.Checkbox(
            label=_followup_toggle_label(
                records,
                manifest_loaded=bool(followup_manifest_result.get("manifest_loaded")),
                followup_rows=followup_rows,
            ),
            value=False,
        )
        followup_status_box = gradio.Textbox(label="Calibration follow-up status", lines=2)
        with gradio.Row():
            with gradio.Column(scale=3):
                media_player = gradio.Video(label="Review media", height=396)
            with gradio.Column(scale=2):
                summary_box = gradio.Markdown()
                decision_status_box = gradio.Textbox(label="Review decision status")
                approve_button = gradio.Button("Approve")
                reject_button = gradio.Button("Reject")
                unreviewed_button = gradio.Button("Leave unreviewed")
                baseline_viewer_path_box = gradio.Textbox(label="Primary path", lines=3)
                trial_viewer_path_box = gradio.Textbox(label="Secondary path", lines=3)
                calibration_handoff_box = gradio.Textbox(label="Detector calibration handoff", lines=8)
                calibration_followup_box = gradio.Textbox(label="Calibration follow-up", lines=8)
                with gradio.Accordion("Debug details", open=False):
                    payload_box = gradio.Code(label="Viewer render payload", language="json")
        selector.change(
            _render_active_record,
            inputs=[selector, followup_only_toggle],
            outputs=[selector, followup_only_toggle, followup_status_box, summary_box, media_player, baseline_viewer_path_box, trial_viewer_path_box, calibration_handoff_box, calibration_followup_box, payload_box],
        )
        followup_only_toggle.change(
            _render_active_record,
            inputs=[selector, followup_only_toggle],
            outputs=[selector, followup_only_toggle, followup_status_box, summary_box, media_player, baseline_viewer_path_box, trial_viewer_path_box, calibration_handoff_box, calibration_followup_box, payload_box],
        )
        approve_button.click(
            lambda record_id, show_only_followup_records: _apply_decision(record_id, show_only_followup_records, "approved"),
            inputs=[selector, followup_only_toggle],
            outputs=[selector, followup_only_toggle, followup_status_box, summary_box, media_player, baseline_viewer_path_box, trial_viewer_path_box, calibration_handoff_box, calibration_followup_box, payload_box, decision_status_box],
        )
        reject_button.click(
            lambda record_id, show_only_followup_records: _apply_decision(record_id, show_only_followup_records, "rejected"),
            inputs=[selector, followup_only_toggle],
            outputs=[selector, followup_only_toggle, followup_status_box, summary_box, media_player, baseline_viewer_path_box, trial_viewer_path_box, calibration_handoff_box, calibration_followup_box, payload_box, decision_status_box],
        )
        unreviewed_button.click(
            lambda record_id, show_only_followup_records: _apply_decision(record_id, show_only_followup_records, "unreviewed"),
            inputs=[selector, followup_only_toggle],
            outputs=[selector, followup_only_toggle, followup_status_box, summary_box, media_player, baseline_viewer_path_box, trial_viewer_path_box, calibration_handoff_box, calibration_followup_box, payload_box, decision_status_box],
        )
        app.load(
            lambda: (*_render_active_record(initial_record_id, False), ""),
            inputs=None,
            outputs=[selector, followup_only_toggle, followup_status_box, summary_box, media_player, baseline_viewer_path_box, trial_viewer_path_box, calibration_handoff_box, calibration_followup_box, payload_box, decision_status_box],
        )

    if launch:
        app.launch(allowed_paths=_allowed_launch_paths(records))

    return {
        "ok": True,
        "status": "ok",
        "record_count": len(records),
        "app": app,
    }


def _base_sidecar_record(payload: dict[str, Any]) -> dict[str, Any]:
    game = str(payload.get("game", "")).strip() or None
    source = str(payload.get("source", "")).strip() or None
    return {
        "record_id": f"{game or 'unknown'}::{source or 'unknown'}",
        "label": Path(source or "unknown").name,
        "kind": "sidecar",
        "game": game,
        "source": source,
        "proxy_sidecar_path": None,
        "runtime_sidecar_path": None,
        "fused_sidecar_path": None,
        "proxy_review_status": None,
        "runtime_review_status": None,
        "fused_review_status": None,
        "candidate_lifecycle_states": [],
        "candidate_lifecycle_count": 0,
        "hook_candidate_count": 0,
        "hook_modes": [],
        "strongest_hook_archetype": None,
        "selected_highlight_event_types": [],
        "selected_highlight_producer_families": [],
        "selected_highlight_fusion_ids": [],
    }


def _load_detector_calibration_followup_manifest(path: str | Path | None) -> dict[str, Any]:
    return review_calibration_ui_support.load_detector_calibration_followup_manifest(path)


def _active_review_records(
    records: list[dict[str, Any]],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
    show_only_followup_records: bool,
) -> list[dict[str, Any]]:
    return review_calibration_ui_support.active_review_records(
        records,
        manifest_loaded=manifest_loaded,
        followup_rows=followup_rows,
        show_only_followup_records=show_only_followup_records,
    )


def _resolve_active_record_id(current_record_id: str | None, active_record_ids: list[str]) -> str | None:
    return review_calibration_ui_support.resolve_active_record_id(current_record_id, active_record_ids)


def _selector_update_payload(*, choices: list[tuple[str, str]], value: str | None) -> dict[str, Any]:
    return {
        "choices": choices,
        "value": value,
    }


def _calibration_followup_counts(
    records: list[dict[str, Any]],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
) -> tuple[int, int]:
    return review_calibration_ui_support.calibration_followup_counts(
        records,
        manifest_loaded=manifest_loaded,
        followup_rows=followup_rows,
    )


def _followup_toggle_label(
    records: list[dict[str, Any]],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
) -> str:
    return review_calibration_ui_support.followup_toggle_label(
        records,
        manifest_loaded=manifest_loaded,
        followup_rows=followup_rows,
    )


def _toggle_update_payload(*, label: str, value: bool) -> dict[str, Any]:
    return {
        "label": label,
        "value": value,
    }


def _render_calibration_followup_status(
    records: list[dict[str, Any]],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
    show_only_followup_records: bool,
) -> str:
    return review_calibration_ui_support.render_calibration_followup_status(
        records,
        manifest_loaded=manifest_loaded,
        followup_rows=followup_rows,
        show_only_followup_records=show_only_followup_records,
    )


def _match_calibration_followup_rows(
    row: dict[str, Any],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return review_calibration_ui_support.match_calibration_followup_rows(
        row,
        manifest_loaded=manifest_loaded,
        followup_rows=followup_rows,
    )


def _sort_calibration_followup_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return review_calibration_ui_support.sort_calibration_followup_rows(rows)


def _top_calibration_followup_payload(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return review_calibration_ui_support.top_calibration_followup_payload(rows)


def _render_calibration_followup_summary(
    row: dict[str, Any],
    *,
    manifest_loaded: bool,
    followup_rows: list[dict[str, Any]],
) -> str:
    return review_calibration_ui_support.render_calibration_followup_summary(
        row,
        manifest_loaded=manifest_loaded,
        followup_rows=followup_rows,
    )


def _render_calibration_handoff_summary(viewer_payload: dict[str, Any] | None) -> str:
    return review_calibration_ui_support.render_calibration_handoff_summary(viewer_payload)


def _record_summary(row: dict[str, Any]) -> str:
    if row.get("kind") == "proxy_review_session_item":
        transcript_status = "available" if row.get("transcript_available") else "missing"
        return "\n".join(
            [
                f"## {row['label']}",
                f"- Game: `{row.get('game') or 'unknown'}`",
                f"- Review status: `{row.get('review_status') or 'unreviewed'}`",
                f"- Session: `{row.get('session_id') or 'n/a'}`",
                f"- Bridge score: `{row.get('bridge_score') if row.get('bridge_score') is not None else 'n/a'}`",
                f"- Bridge sources: `{','.join(row.get('bridge_sources', [])) or 'n/a'}`",
                f"- Bridge source families: `{','.join(row.get('bridge_source_families', [])) or 'n/a'}`",
                f"- Transcript: `{transcript_status}`",
                f"- Meta path: `{row.get('gpt_meta_path') or 'n/a'}`",
            ]
        )
    if row.get("kind") == "fused_review_session_item":
        return "\n".join(
            [
                f"## {row['label']}",
                f"- Game: `{row.get('game') or 'unknown'}`",
                f"- Review status: `{row.get('review_status') or 'unreviewed'}`",
                f"- Session: `{row.get('session_id') or 'n/a'}`",
                f"- Event id: `{row.get('event_id') or 'n/a'}`",
                f"- Event type: `{row.get('event_type') or 'n/a'}`",
                f"- Entity: `{row.get('entity_id') or 'n/a'}`",
                f"- Review window: `{_format_review_window(row.get('suggested_start_timestamp'), row.get('suggested_end_timestamp'))}`",
                f"- Final score: `{row.get('final_score') if row.get('final_score') is not None else 'n/a'}`",
                f"- Recommended action: `{row.get('recommended_action') or 'n/a'}`",
                f"- Gate status: `{row.get('gate_status') or 'n/a'}`",
                f"- Synergy applied: `{bool(row.get('synergy_applied', False))}`",
                f"- Sidecar path: `{row.get('fused_sidecar_path') or 'n/a'}`",
                f"- Meta path: `{row.get('gpt_meta_path') or 'n/a'}`",
            ]
        )
    if row.get("kind") == "fixture":
        comparison_rows = list(row.get("fixture_comparison_rows", []))
        batch_rows = list(row.get("fixture_trial_batch_rows", []))
        comparison_note = "n/a"
        if comparison_rows:
            preferred = _preferred_fixture_row(comparison_rows)
            comparison_note = (
                f"{preferred.get('artifact_layer')} | {preferred.get('coverage_status')} | "
                f"{preferred.get('recommendation_signal')}"
            )
        disagreement_rows = [
            row
            for row in comparison_rows
            if str(row.get("review_status", "")).strip()
            and str(row.get("recommendation_signal", "")).strip()
            and str(row.get("recommendation_signal", "")).strip() not in {"n/a", ""}
        ]
        batch_trials = ",".join(sorted({str(item.get("trial_name", "")).strip() for item in batch_rows if str(item.get("trial_name", "")).strip()}))
        return "\n".join(
            [
                f"## {row['label']}",
                f"- Expected review: `{row['expected_review_outcome']}`",
                f"- Latency class: `{row['latency_budget_class']}`",
                f"- Expected artifacts: `{','.join(sorted(key for key, value in dict(row.get('expected_artifacts', {})).items() if value)) or 'n/a'}`",
                f"- Fixture comparison: `{comparison_note}`",
                f"- Comparison rows with review/recommendation context: `{len(disagreement_rows)}`",
                f"- Batch trials: `{batch_trials or 'n/a'}`",
                f"- Notes: {row['notes'] or 'n/a'}",
            ]
        )
    review_statuses = {
        str(row.get("proxy_review_status") or "").strip(),
        str(row.get("runtime_review_status") or "").strip(),
        str(row.get("fused_review_status") or "").strip(),
    }
    review_statuses.discard("")
    return "\n".join(
        [
            f"## {row['label']}",
            f"- Game: `{row.get('game') or 'unknown'}`",
            f"- Proxy review: `{row.get('proxy_review_status') or 'unreviewed'}`",
            f"- Runtime review: `{row.get('runtime_review_status') or 'unreviewed'}`",
            f"- Fused review: `{row.get('fused_review_status') or 'unreviewed'}`",
            f"- Candidate lifecycle states: `{','.join(row.get('candidate_lifecycle_states', [])) or 'n/a'}`",
            f"- Candidate lifecycle count: `{row.get('candidate_lifecycle_count', 0)}`",
            f"- Export-state context: `{_export_state_context(row.get('candidate_lifecycle_states', []))}`",
            f"- Selected event types: `{','.join(row.get('selected_highlight_event_types', [])) or 'n/a'}`",
            f"- Selected producer families: `{','.join(row.get('selected_highlight_producer_families', [])) or 'n/a'}`",
            f"- Selected fusion ids: `{','.join(row.get('selected_highlight_fusion_ids', [])) or 'n/a'}`",
            f"- Hook candidate count: `{row.get('hook_candidate_count', 0)}`",
            f"- Hook modes: `{','.join(row.get('hook_modes', [])) or 'n/a'}`",
            f"- Strongest hook archetype: `{row.get('strongest_hook_archetype') or 'n/a'}`",
            f"- Cross-layer review disagreement: `{'yes' if len(review_statuses) > 1 else 'no'}`",
        ]
    )


def _load_json(path: Path) -> dict[str, Any]:
    return review_calibration_ui_support.load_json_payload_or_empty(path)


def _load_proxy_review_session_records(session_manifest_path: str | Path) -> list[dict[str, Any]]:
    manifest_path = _resolve_path(session_manifest_path)
    payload = _load_json(manifest_path)
    if payload.get("schema_version") != PROXY_REVIEW_SESSION_SCHEMA_VERSION:
        return []
    game = str(payload.get("game", "")).strip() or None
    session_id = str(payload.get("session_id", "")).strip() or None
    gpt_repo_path = str(_resolve_path(str(payload.get("gpt_repo", "")).strip())) if str(payload.get("gpt_repo", "")).strip() else None
    records: list[dict[str, Any]] = []
    for index, item in enumerate(list(payload.get("items", []))):
        if not isinstance(item, dict):
            continue
        gpt_meta_path_value = str(item.get("gpt_meta_path", "")).strip()
        processed_clip_value = str(item.get("gpt_processed_path", "")).strip()
        source_value = str(item.get("source", "")).strip()
        if not gpt_meta_path_value or not processed_clip_value:
            continue
        gpt_meta_path = _resolve_path(gpt_meta_path_value)
        meta_payload = _load_json(gpt_meta_path)
        review_status = _normalized_review_status(meta_payload.get("review_status") or item.get("review_status"))
        if review_status != "unreviewed":
            continue
        transcript_paths = _discover_proxy_review_transcript_paths(
            source_clip_path=source_value or None,
            gpt_meta_path=gpt_meta_path,
        )
        label = Path(source_value).name if source_value else Path(processed_clip_value).name
        records.append(
            {
                "record_id": f"proxy-session::{session_id or 'unknown'}::{index:03d}",
                "label": label,
                "kind": "proxy_review_session_item",
                "game": game,
                "source": source_value or None,
                "processed_clip_path": processed_clip_value,
                "source_clip_path": source_value or None,
                "proxy_sidecar_path": str(item.get("sidecar_path", "")).strip() or None,
                "session_manifest_path": str(manifest_path),
                "gpt_repo_path": gpt_repo_path,
                "gpt_meta_path": str(gpt_meta_path),
                "gpt_processed_path": processed_clip_value,
                "review_status": review_status,
                "reviewed_at": meta_payload.get("reviewed_at"),
                "bridge_score": item.get("top_proxy_score"),
                "bridge_sources": list(item.get("sources", [])) if isinstance(item.get("sources"), list) else [],
                "bridge_source_families": list(item.get("source_families", [])) if isinstance(item.get("source_families"), list) else [],
                "transcript_srt_path": transcript_paths.get("srt"),
                "transcript_whisper_json_path": transcript_paths.get("whisper_json"),
                "transcript_available": bool(transcript_paths.get("srt") or transcript_paths.get("whisper_json")),
                "session_id": session_id,
                "gpt_meta_payload": meta_payload,
            }
        )
    return records


def _load_fused_review_session_records(session_manifest_path: str | Path) -> list[dict[str, Any]]:
    manifest_path = _resolve_path(session_manifest_path)
    payload = _load_json(manifest_path)
    if payload.get("schema_version") != FUSED_REVIEW_SESSION_SCHEMA_VERSION:
        return []
    game = str(payload.get("game", "")).strip() or None
    session_id = str(payload.get("session_id", "")).strip() or None
    gpt_repo_path = str(_resolve_path(str(payload.get("gpt_repo", "")).strip())) if str(payload.get("gpt_repo", "")).strip() else None
    records: list[dict[str, Any]] = []
    for index, item in enumerate(list(payload.get("items", []))):
        if not isinstance(item, dict):
            continue
        gpt_meta_path_value = str(item.get("gpt_meta_path", "")).strip()
        processed_clip_value = str(item.get("gpt_processed_path", "")).strip()
        source_value = str(item.get("source", "")).strip()
        if not gpt_meta_path_value or not processed_clip_value:
            continue
        gpt_meta_path = _resolve_path(gpt_meta_path_value)
        meta_payload = _load_json(gpt_meta_path)
        review_status = _normalized_review_status(meta_payload.get("review_status") or item.get("review_status"))
        if review_status != "unreviewed":
            continue
        event_type = str(item.get("event_type", "")).strip() or "unknown_event"
        source_label = Path(source_value).stem if source_value else Path(processed_clip_value).name
        entity_label = str(item.get("entity_id", "")).strip() or "unknown_entity"
        window_label = _format_review_window(item.get("suggested_start_timestamp"), item.get("suggested_end_timestamp"))
        label = f"{source_label} | {entity_label} | {window_label}"
        records.append(
            {
                "record_id": f"fused-session::{session_id or 'unknown'}::{index:03d}",
                "label": label,
                "kind": "fused_review_session_item",
                "game": game,
                "source": source_value or None,
                "processed_clip_path": processed_clip_value,
                "source_clip_path": source_value or None,
                "fused_sidecar_path": str(item.get("sidecar_path", "")).strip() or None,
                "session_manifest_path": str(manifest_path),
                "gpt_repo_path": gpt_repo_path,
                "gpt_meta_path": str(gpt_meta_path),
                "gpt_processed_path": processed_clip_value,
                "review_status": review_status,
                "reviewed_at": meta_payload.get("reviewed_at"),
                "session_id": session_id,
                "event_id": str(item.get("event_id", "")).strip() or None,
                "event_type": event_type,
                "final_score": item.get("final_score"),
                "recommended_action": str(item.get("recommended_action", "")).strip() or None,
                "gate_status": str(item.get("gate_status", "")).strip() or None,
                "synergy_applied": bool(item.get("synergy_applied", False)),
                "suggested_start_timestamp": item.get("suggested_start_timestamp"),
                "suggested_end_timestamp": item.get("suggested_end_timestamp"),
                "entity_id": item.get("entity_id"),
                "ability_id": item.get("ability_id"),
                "equipment_id": item.get("equipment_id"),
                "event_row_id": item.get("event_row_id"),
                "gpt_meta_payload": meta_payload,
            }
        )
    return records


def _allowed_launch_paths(records: list[dict[str, Any]]) -> list[str]:
    allowed_dirs: set[str] = set()
    for row in records:
        if not _is_review_session_item(row):
            continue
        for key in (
            "processed_clip_path",
            "source_clip_path",
            "proxy_sidecar_path",
            "fused_sidecar_path",
            "gpt_meta_path",
            "transcript_srt_path",
            "transcript_whisper_json_path",
        ):
            value = row.get(key)
            if not isinstance(value, str) or not value.strip():
                continue
            path = Path(value).expanduser()
            try:
                resolved = path.resolve()
            except OSError:
                continue
            parent = resolved.parent
            if parent.exists():
                allowed_dirs.add(str(parent))
    return sorted(allowed_dirs)


def _display_path(label: str, value: Any) -> str:
    text = str(value or "").strip()
    if text:
        return f"{label}:\n{text}"
    return f"{label}:\nnot applicable"


def _format_review_window(start_value: Any, end_value: Any) -> str:
    try:
        start = float(start_value or 0.0)
        end = float(end_value or 0.0)
    except (TypeError, ValueError):
        return "n/a"
    return f"{start:.1f}s-{end:.1f}s"


def _discover_proxy_review_transcript_paths(
    *,
    source_clip_path: str | None,
    gpt_meta_path: Path,
) -> dict[str, str | None]:
    results: dict[str, str | None] = {"srt": None, "whisper_json": None}
    if not source_clip_path:
        return results
    source_stem = Path(source_clip_path).stem
    inbox_dir = gpt_meta_path.parent
    srt_path = inbox_dir / f"{source_stem}.srt"
    whisper_path = inbox_dir / f"{source_stem}.whisper.json"
    if srt_path.exists():
        results["srt"] = str(srt_path.resolve())
    if whisper_path.exists():
        results["whisper_json"] = str(whisper_path.resolve())
    return results


def _normalized_review_status(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"approved", "rejected", "unreviewed"}:
        return text
    return "unreviewed"


def _is_review_session_item(row: dict[str, Any]) -> bool:
    return str(row.get("kind") or "") in {"proxy_review_session_item", "fused_review_session_item"}


def _write_proxy_review_session_decision(gpt_meta_path: str | Path, decision: str) -> dict[str, Any]:
    meta_path = _resolve_path(gpt_meta_path)
    payload = _load_json(meta_path)
    if not payload:
        return {
            "ok": False,
            "error": "gpt meta payload is unreadable or invalid",
        }
    normalized = _normalized_review_status(decision)
    payload["review_status"] = normalized
    if normalized == "unreviewed":
        payload.pop("reviewed_at", None)
    else:
        payload["reviewed_at"] = datetime.now(UTC).isoformat()
    try:
        meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as exc:
        return {
            "ok": False,
            "error": str(exc),
        }
    return {
        "ok": True,
        "review_status": normalized,
        "reviewed_at": payload.get("reviewed_at"),
        "gpt_meta_payload": payload,
        "gpt_meta_path": str(meta_path),
    }


def _finalize_proxy_review_session_decision(row: dict[str, Any], decision: str) -> dict[str, Any]:
    write_result = _write_proxy_review_session_decision(row["gpt_meta_path"], decision)
    if not write_result.get("ok"):
        return write_result
    normalized = str(write_result.get("review_status") or "unreviewed")
    if normalized == "unreviewed":
        _update_proxy_review_session_manifest_item(
            row,
            gpt_meta_path=str(row.get("gpt_meta_path") or ""),
            review_status=normalized,
            reviewed_at=write_result.get("reviewed_at"),
            gpt_final_path=None,
        )
        return write_result

    gpt_repo_path = str(row.get("gpt_repo_path") or "").strip()
    game = str(row.get("game") or "").strip()
    if not gpt_repo_path or not game:
        return {
            "ok": False,
            "error": "proxy review session row is missing gpt repo or game context",
        }

    bucket = "accepted" if normalized == "approved" else "rejected"
    meta_payload = dict(write_result.get("gpt_meta_payload", {}))
    clip_id = str(meta_payload.get("clip_id") or Path(str(row.get("gpt_processed_path") or "")).stem).strip()
    source_meta_path = _resolve_path(str(row.get("gpt_meta_path") or ""))
    processed_path = _resolve_path(str(row.get("gpt_processed_path") or ""))
    destination_dir = _resolve_path(gpt_repo_path) / bucket / game
    destination_dir.mkdir(parents=True, exist_ok=True)
    clip_suffix = processed_path.suffix or ".mp4"
    final_clip_path = destination_dir / f"{clip_id}{clip_suffix}"
    final_meta_path = destination_dir / f"{clip_id}.meta.json"

    if final_clip_path.exists():
        final_clip_path.unlink()
    if processed_path.exists():
        shutil.move(str(processed_path), str(final_clip_path))

    meta_payload["final_path"] = str(final_clip_path)
    meta_payload["meta_path"] = str(final_meta_path)
    meta_payload["status"] = bucket
    final_meta_path.write_text(json.dumps(meta_payload, indent=2), encoding="utf-8")
    if source_meta_path.exists() and source_meta_path != final_meta_path:
        source_meta_path.unlink()

    _update_proxy_review_session_manifest_item(
        row,
        gpt_meta_path=str(final_meta_path),
        review_status=normalized,
        reviewed_at=meta_payload.get("reviewed_at"),
        gpt_final_path=str(final_clip_path),
    )
    return {
        "ok": True,
        "review_status": normalized,
        "reviewed_at": meta_payload.get("reviewed_at"),
        "gpt_meta_path": str(final_meta_path),
        "gpt_final_path": str(final_clip_path),
        "gpt_meta_payload": meta_payload,
    }


def _finalize_fused_review_session_decision(row: dict[str, Any], decision: str) -> dict[str, Any]:
    write_result = _write_proxy_review_session_decision(row["gpt_meta_path"], decision)
    if not write_result.get("ok"):
        return write_result
    normalized = str(write_result.get("review_status") or "unreviewed")
    if normalized == "unreviewed":
        _update_fused_review_session_manifest_item(
            row,
            gpt_meta_path=str(row.get("gpt_meta_path") or ""),
            review_status=normalized,
            reviewed_at=write_result.get("reviewed_at"),
            gpt_final_path=None,
        )
        return write_result

    gpt_repo_path = str(row.get("gpt_repo_path") or "").strip()
    game = str(row.get("game") or "").strip()
    if not gpt_repo_path or not game:
        return {
            "ok": False,
            "error": "fused review session row is missing gpt repo or game context",
        }

    bucket = "accepted" if normalized == "approved" else "rejected"
    meta_payload = dict(write_result.get("gpt_meta_payload", {}))
    clip_id = str(meta_payload.get("clip_id") or Path(str(row.get("gpt_processed_path") or "")).stem).strip()
    source_meta_path = _resolve_path(str(row.get("gpt_meta_path") or ""))
    processed_path = _resolve_path(str(row.get("gpt_processed_path") or ""))
    destination_dir = _resolve_path(gpt_repo_path) / bucket / game
    destination_dir.mkdir(parents=True, exist_ok=True)
    clip_suffix = processed_path.suffix or ".mp4"
    final_clip_path = destination_dir / f"{clip_id}{clip_suffix}"
    final_meta_path = destination_dir / f"{clip_id}.meta.json"

    if final_clip_path.exists():
        final_clip_path.unlink()
    if processed_path.exists():
        shutil.move(str(processed_path), str(final_clip_path))

    meta_payload["final_path"] = str(final_clip_path)
    meta_payload["meta_path"] = str(final_meta_path)
    meta_payload["status"] = bucket
    final_meta_path.write_text(json.dumps(meta_payload, indent=2), encoding="utf-8")
    if source_meta_path.exists() and source_meta_path != final_meta_path:
        source_meta_path.unlink()

    _update_fused_review_session_manifest_item(
        row,
        gpt_meta_path=str(final_meta_path),
        review_status=normalized,
        reviewed_at=meta_payload.get("reviewed_at"),
        gpt_final_path=str(final_clip_path),
    )
    return {
        "ok": True,
        "review_status": normalized,
        "reviewed_at": meta_payload.get("reviewed_at"),
        "gpt_meta_path": str(final_meta_path),
        "gpt_final_path": str(final_clip_path),
        "gpt_meta_payload": meta_payload,
    }


def _update_proxy_review_session_manifest_item(
    row: dict[str, Any],
    *,
    gpt_meta_path: str,
    review_status: str,
    reviewed_at: Any,
    gpt_final_path: str | None,
) -> None:
    session_manifest_value = str(row.get("session_manifest_path") or "").strip()
    if not session_manifest_value:
        return
    session_manifest_path = _resolve_path(session_manifest_value)
    payload = _load_json(session_manifest_path)
    items = payload.get("items")
    if not isinstance(items, list):
        return
    source_clip_id = None
    source_meta_payload = row.get("gpt_meta_payload")
    if isinstance(source_meta_payload, dict):
        source_clip_id = str(source_meta_payload.get("clip_id") or "").strip() or None
    for item in items:
        if not isinstance(item, dict):
            continue
        item_clip_id = str(item.get("clip_id") or "").strip() or None
        item_meta_path = str(item.get("gpt_meta_path") or "").strip()
        if (source_clip_id and item_clip_id == source_clip_id) or item_meta_path == str(row.get("gpt_meta_path") or ""):
            item["gpt_meta_path"] = gpt_meta_path
            item["review_status"] = review_status
            item["reviewed_at"] = reviewed_at
            item["apply_status"] = "reviewed"
            if gpt_final_path:
                item["gpt_final_path"] = gpt_final_path
            break
    session_manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _update_fused_review_session_manifest_item(
    row: dict[str, Any],
    *,
    gpt_meta_path: str,
    review_status: str,
    reviewed_at: Any,
    gpt_final_path: str | None,
) -> None:
    session_manifest_value = str(row.get("session_manifest_path") or "").strip()
    if not session_manifest_value:
        return
    session_manifest_path = _resolve_path(session_manifest_value)
    payload = _load_json(session_manifest_path)
    items = payload.get("items")
    if not isinstance(items, list):
        return
    source_clip_id = None
    source_event_id = str(row.get("event_id") or "").strip() or None
    source_meta_payload = row.get("gpt_meta_payload")
    if isinstance(source_meta_payload, dict):
        source_clip_id = str(source_meta_payload.get("clip_id") or "").strip() or None
    for item in items:
        if not isinstance(item, dict):
            continue
        item_clip_id = str(item.get("clip_id") or "").strip() or None
        item_event_id = str(item.get("event_id") or "").strip() or None
        item_meta_path = str(item.get("gpt_meta_path") or "").strip()
        if (
            (source_clip_id and item_clip_id == source_clip_id)
            or (source_event_id and item_event_id == source_event_id and item_meta_path == str(row.get("gpt_meta_path") or ""))
            or item_meta_path == str(row.get("gpt_meta_path") or "")
        ):
            item["gpt_meta_path"] = gpt_meta_path
            item["review_status"] = review_status
            item["reviewed_at"] = reviewed_at
            item["apply_status"] = "reviewed"
            if gpt_final_path:
                item["gpt_final_path"] = gpt_final_path
            break
    session_manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _proxy_review_session_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": row.get("kind"),
        "session_id": row.get("session_id"),
        "processed_clip_path": row.get("processed_clip_path"),
        "source_clip_path": row.get("source_clip_path"),
        "proxy_sidecar_path": row.get("proxy_sidecar_path"),
        "gpt_meta_path": row.get("gpt_meta_path"),
        "review_status": row.get("review_status"),
        "reviewed_at": row.get("reviewed_at"),
        "bridge_score": row.get("bridge_score"),
        "bridge_sources": row.get("bridge_sources", []),
        "bridge_source_families": row.get("bridge_source_families", []),
        "transcript_srt_path": row.get("transcript_srt_path"),
        "transcript_whisper_json_path": row.get("transcript_whisper_json_path"),
        "gpt_meta_payload": row.get("gpt_meta_payload", {}),
    }


def _fused_review_session_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": row.get("kind"),
        "session_id": row.get("session_id"),
        "processed_clip_path": row.get("processed_clip_path"),
        "source_clip_path": row.get("source_clip_path"),
        "fused_sidecar_path": row.get("fused_sidecar_path"),
        "gpt_meta_path": row.get("gpt_meta_path"),
        "review_status": row.get("review_status"),
        "reviewed_at": row.get("reviewed_at"),
        "event_id": row.get("event_id"),
        "event_type": row.get("event_type"),
        "final_score": row.get("final_score"),
        "recommended_action": row.get("recommended_action"),
        "gate_status": row.get("gate_status"),
        "synergy_applied": row.get("synergy_applied"),
        "suggested_start_timestamp": row.get("suggested_start_timestamp"),
        "suggested_end_timestamp": row.get("suggested_end_timestamp"),
        "entity_id": row.get("entity_id"),
        "ability_id": row.get("ability_id"),
        "equipment_id": row.get("equipment_id"),
        "event_row_id": row.get("event_row_id"),
        "gpt_meta_payload": row.get("gpt_meta_payload", {}),
    }


def _selected_highlight_details_from_lifecycle_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("selected_highlight_details_json")
    if isinstance(payload, dict):
        return payload
    if payload in (None, "", "null"):
        return {}
    try:
        parsed = json.loads(str(payload))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _selected_highlight_producer_families_from_lifecycle_row(row: dict[str, Any]) -> list[str]:
    details = _selected_highlight_details_from_lifecycle_row(row)
    values = details.get("contributing_producer_families")
    if not isinstance(values, list):
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def _resolve_path(path_like: str | Path) -> Path:
    return review_calibration_ui_support.resolve_review_path(path_like)


def _preferred_fixture_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = sorted(
        rows,
        key=lambda row: (
            {"proxy": 0, "runtime": 1, "fused": 2}.get(str(row.get("artifact_layer", "")), 3),
            0 if str(row.get("coverage_status", "")) == "both" else 1,
        ),
    )
    return ranked[0] if ranked else {}


def _export_state_context(states: list[str]) -> str:
    normalized = {str(state).strip() for state in states if str(state).strip()}
    relevant = [state for state in ("approved", "selected_for_export", "exported", "posted") if state in normalized]
    return ",".join(relevant) or "n/a"


def _render_fixture_viewer(
    row: dict[str, Any],
    *,
    side: str,
    fixture_comparison_report: str | Path | None,
    fixture_trial_batch_manifest: str | Path | None,
    proxy_calibration_report: str | Path | None,
    proxy_replay_report: str | Path | None,
    runtime_calibration_report: str | Path | None,
    runtime_replay_report: str | Path | None,
    registry_path: str | Path | None,
    output_path: str | Path | None,
) -> str:
    proxy_sidecar = row.get(f"{side}_sidecar_path") if str(row.get("artifact_layer")) == "proxy" else None
    runtime_sidecar = row.get(f"{side}_sidecar_path") if str(row.get("artifact_layer")) == "runtime" else None
    fused_sidecar = row.get(f"{side}_sidecar_path") if str(row.get("artifact_layer")) == "fused" else None
    if not any([proxy_sidecar, runtime_sidecar, fused_sidecar]):
        return ""
    resolved_output_path = None
    if output_path is not None:
        target = _resolve_path(output_path)
        resolved_output_path = str(target.with_name(f"{target.stem}-{side}{target.suffix or '.html'}"))
    result = render_unified_replay_viewer(
        proxy_sidecar=proxy_sidecar,
        runtime_sidecar=runtime_sidecar,
        fused_sidecar=fused_sidecar,
        fixture_comparison_report=fixture_comparison_report,
        fixture_trial_batch_manifest=fixture_trial_batch_manifest,
        proxy_calibration_report=proxy_calibration_report,
        proxy_replay_report=proxy_replay_report,
        runtime_calibration_report=runtime_calibration_report,
        runtime_replay_report=runtime_replay_report,
        registry_path=registry_path,
        output_path=resolved_output_path,
    )
    return str(result.get("viewer_path", ""))
