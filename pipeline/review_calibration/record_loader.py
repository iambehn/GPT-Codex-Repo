from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.clip_registry import load_candidate_lifecycle_details, load_hook_candidate_details
from pipeline.evaluation_fixtures import load_evaluation_fixture_manifest
from pipeline.fused_review_bridge import FUSED_REVIEW_SESSION_SCHEMA_VERSION
from pipeline.proxy_review_bridge import PROXY_REVIEW_SESSION_SCHEMA_VERSION
from pipeline.review_calibration import ui_support as review_calibration_ui_support


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
        comparison_payload = review_calibration_ui_support.load_json_payload_or_empty(
            review_calibration_ui_support.resolve_review_path(fixture_comparison_report)
        )
        for row in list(comparison_payload.get("comparison", {}).get("fixture_rows", [])):
            if not isinstance(row, dict):
                continue
            fixture_id = str(row.get("fixture_id", "")).strip()
            if not fixture_id:
                continue
            comparison_rows_by_fixture.setdefault(fixture_id, []).append(row)
    if fixture_trial_batch_manifest is not None:
        batch_payload = review_calibration_ui_support.load_json_payload_or_empty(
            review_calibration_ui_support.resolve_review_path(fixture_trial_batch_manifest)
        )
        for comparison in list(batch_payload.get("trial_comparisons", [])):
            if not isinstance(comparison, dict):
                continue
            report_path = str(comparison.get("comparison_report_path", "")).strip()
            if not report_path:
                continue
            report_payload = review_calibration_ui_support.load_json_payload_or_empty(
                review_calibration_ui_support.resolve_review_path(report_path)
            )
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
        root = review_calibration_ui_support.resolve_review_path(sidecar_root)
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for path in sorted(root.rglob("*.json")):
            if path.name.endswith(".proxy_scan.json"):
                payload = review_calibration_ui_support.load_json_payload_or_empty(path)
                key = (str(payload.get("game", "")), str(payload.get("source", "")))
                record = grouped.setdefault(key, _base_sidecar_record(payload))
                record["proxy_sidecar_path"] = str(path.resolve())
                review = payload.get("proxy_review", {})
                if isinstance(review, dict):
                    record["proxy_review_status"] = str(review.get("review_status", "")).strip() or None
            elif path.name.endswith(".runtime_analysis.json"):
                payload = review_calibration_ui_support.load_json_payload_or_empty(path)
                key = (str(payload.get("game", "")), str(payload.get("source", "")))
                record = grouped.setdefault(key, _base_sidecar_record(payload))
                record["runtime_sidecar_path"] = str(path.resolve())
                review = payload.get("runtime_review", {})
                if isinstance(review, dict):
                    record["runtime_review_status"] = str(review.get("review_status", "")).strip() or None
            elif path.name.endswith(".fused_analysis.json"):
                payload = review_calibration_ui_support.load_json_payload_or_empty(path)
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


def _load_proxy_review_session_records(session_manifest_path: str | Path) -> list[dict[str, Any]]:
    manifest_path = review_calibration_ui_support.resolve_review_path(session_manifest_path)
    payload = review_calibration_ui_support.load_json_payload_or_empty(manifest_path)
    if payload.get("schema_version") != PROXY_REVIEW_SESSION_SCHEMA_VERSION:
        return []
    game = str(payload.get("game", "")).strip() or None
    session_id = str(payload.get("session_id", "")).strip() or None
    gpt_repo_path = (
        str(review_calibration_ui_support.resolve_review_path(str(payload.get("gpt_repo", "")).strip()))
        if str(payload.get("gpt_repo", "")).strip()
        else None
    )
    records: list[dict[str, Any]] = []
    for index, item in enumerate(list(payload.get("items", []))):
        if not isinstance(item, dict):
            continue
        gpt_meta_path_value = str(item.get("gpt_meta_path", "")).strip()
        processed_clip_value = str(item.get("gpt_processed_path", "")).strip()
        source_value = str(item.get("source", "")).strip()
        if not gpt_meta_path_value or not processed_clip_value:
            continue
        gpt_meta_path = review_calibration_ui_support.resolve_review_path(gpt_meta_path_value)
        meta_payload = review_calibration_ui_support.load_json_payload_or_empty(gpt_meta_path)
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
    manifest_path = review_calibration_ui_support.resolve_review_path(session_manifest_path)
    payload = review_calibration_ui_support.load_json_payload_or_empty(manifest_path)
    if payload.get("schema_version") != FUSED_REVIEW_SESSION_SCHEMA_VERSION:
        return []
    game = str(payload.get("game", "")).strip() or None
    session_id = str(payload.get("session_id", "")).strip() or None
    gpt_repo_path = (
        str(review_calibration_ui_support.resolve_review_path(str(payload.get("gpt_repo", "")).strip()))
        if str(payload.get("gpt_repo", "")).strip()
        else None
    )
    records: list[dict[str, Any]] = []
    for index, item in enumerate(list(payload.get("items", []))):
        if not isinstance(item, dict):
            continue
        gpt_meta_path_value = str(item.get("gpt_meta_path", "")).strip()
        processed_clip_value = str(item.get("gpt_processed_path", "")).strip()
        source_value = str(item.get("source", "")).strip()
        if not gpt_meta_path_value or not processed_clip_value:
            continue
        gpt_meta_path = review_calibration_ui_support.resolve_review_path(gpt_meta_path_value)
        meta_payload = review_calibration_ui_support.load_json_payload_or_empty(gpt_meta_path)
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


def _format_review_window(start_value: Any, end_value: Any) -> str:
    try:
        start = float(start_value or 0.0)
        end = float(end_value or 0.0)
    except (TypeError, ValueError):
        return "n/a"
    return f"{start:.1f}s-{end:.1f}s"
