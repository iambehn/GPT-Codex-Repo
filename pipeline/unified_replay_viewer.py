from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

from pipeline.clip_registry import load_candidate_lifecycle_details, load_hook_candidate_details
from pipeline import proxy_replay_viewer, replay_viewer


REPO_ROOT = Path(__file__).resolve().parent.parent
UNIFIED_REPLAY_VIEWER_SCHEMA_VERSION = "unified_replay_viewer_v1"
SUPPORTED_PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"
SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "unified_replay_viewers"


def render_unified_replay_viewer(
    *,
    proxy_sidecar: str | Path | None = None,
    runtime_sidecar: str | Path | None = None,
    fused_sidecar: str | Path | None = None,
    fixture_comparison_report: str | Path | None = None,
    fixture_trial_batch_manifest: str | Path | None = None,
    proxy_calibration_report: str | Path | None = None,
    proxy_replay_report: str | Path | None = None,
    runtime_calibration_report: str | Path | None = None,
    runtime_replay_report: str | Path | None = None,
    registry_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    if proxy_sidecar is None and runtime_sidecar is None and fused_sidecar is None:
        return {
            "ok": False,
            "status": "missing_sidecars",
            "error": "at least one of proxy_sidecar, runtime_sidecar, or fused_sidecar is required",
        }

    proxy_path, proxy_payload = _maybe_load_proxy(proxy_sidecar)
    if proxy_payload is False:
        return {
            "ok": False,
            "status": "invalid_proxy_sidecar",
            "proxy_sidecar_path": str(proxy_path),
            "error": "proxy sidecar does not use proxy_scan_v1",
        }

    runtime_path, runtime_payload = _maybe_load_runtime(runtime_sidecar)
    if runtime_payload is False:
        return {
            "ok": False,
            "status": "invalid_runtime_sidecar",
            "runtime_sidecar_path": str(runtime_path),
            "error": "runtime sidecar does not use runtime_analysis_v1",
        }

    fused_path, fused_payload = _maybe_load_fused(fused_sidecar)
    if fused_payload is False:
        return {
            "ok": False,
            "status": "invalid_fused_sidecar",
            "fused_sidecar_path": str(fused_path),
            "error": "fused sidecar does not use fused_analysis_v1",
        }

    mismatch = _sidecar_mismatch(proxy_payload, runtime_payload, fused_payload)
    if mismatch is not None:
        return {
            "ok": False,
            "status": "mismatched_sidecars",
            "proxy_sidecar_path": str(proxy_path) if proxy_path is not None else None,
            "runtime_sidecar_path": str(runtime_path) if runtime_path is not None else None,
            "fused_sidecar_path": str(fused_path) if fused_path is not None else None,
            "error": mismatch,
        }

    game = _first_nonempty(
        _payload_text(proxy_payload, "game"),
        _payload_text(runtime_payload, "game"),
        _payload_text(fused_payload, "game"),
    ) or "unknown_game"
    source = _first_nonempty(
        _payload_text(proxy_payload, "source"),
        _payload_text(runtime_payload, "source"),
        _payload_text(fused_payload, "source"),
    )
    media_path = _resolve_media_path(source)
    media_exists = bool(media_path and media_path.exists() and media_path.is_file())
    warnings: list[dict[str, Any]] = []
    if media_path is not None and not media_exists:
        warnings.append(
            {
                "status": "missing_media_source",
                "path": str(media_path),
                "message": "sidecar source media path does not exist locally; viewer will render without inline media playback",
            }
        )
    reports = _load_reports(
        fixture_comparison_report=fixture_comparison_report,
        fixture_trial_batch_manifest=fixture_trial_batch_manifest,
        proxy_calibration_report=proxy_calibration_report,
        proxy_replay_report=proxy_replay_report,
        runtime_calibration_report=runtime_calibration_report,
        runtime_replay_report=runtime_replay_report,
    )

    viewer_path = _viewer_output_path(game, proxy_payload, runtime_payload, fused_payload, proxy_path, runtime_path, fused_path, output_path)
    derived = _build_unified_payload(
        proxy_payload=proxy_payload,
        runtime_payload=runtime_payload,
        fused_payload=fused_payload,
        proxy_path=proxy_path,
        runtime_path=runtime_path,
        fused_path=fused_path,
        media_path=media_path,
        media_exists=media_exists,
        game=game,
        source=source,
        reports=reports,
        registry_path=registry_path,
    )
    html_text = _render_html(derived)

    viewer_path.parent.mkdir(parents=True, exist_ok=True)
    viewer_path.write_text(html_text, encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": UNIFIED_REPLAY_VIEWER_SCHEMA_VERSION,
        "viewer_path": str(viewer_path),
        "proxy_sidecar_path": str(proxy_path) if proxy_path is not None else None,
        "runtime_sidecar_path": str(runtime_path) if runtime_path is not None else None,
        "fused_sidecar_path": str(fused_path) if fused_path is not None else None,
        "media_path": str(media_path) if media_path is not None else None,
        "media_embed_available": media_exists,
        "proxy_available": bool(proxy_payload),
        "runtime_available": bool(runtime_payload),
        "fused_available": bool(fused_payload),
        "timeline_entry_count": len(derived["timeline"]),
        "cross_link_count": derived["artifact_summary"]["cross_link_count"],
        "report_overlay_count": derived["artifact_summary"]["report_overlay_count"],
        "warnings": warnings,
    }


def _maybe_load_proxy(path: str | Path | None) -> tuple[Path | None, dict[str, Any] | bool | None]:
    if path is None:
        return None, None
    resolved = _resolve_path(path)
    payload = _load_json(resolved)
    if payload.get("schema_version") != SUPPORTED_PROXY_SCAN_SCHEMA_VERSION:
        return resolved, False
    return resolved, payload


def _maybe_load_runtime(path: str | Path | None) -> tuple[Path | None, dict[str, Any] | bool | None]:
    if path is None:
        return None, None
    resolved = _resolve_path(path)
    payload = _load_json(resolved)
    if payload.get("schema_version") != SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION:
        return resolved, False
    return resolved, payload


def _maybe_load_fused(path: str | Path | None) -> tuple[Path | None, dict[str, Any] | bool | None]:
    if path is None:
        return None, None
    resolved = _resolve_path(path)
    payload = _load_json(resolved)
    if payload.get("schema_version") != SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION:
        return resolved, False
    return resolved, payload


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_report(path: str | Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return _load_json(_resolve_path(path))


def _load_reports(
    *,
    fixture_comparison_report: str | Path | None,
    fixture_trial_batch_manifest: str | Path | None,
    proxy_calibration_report: str | Path | None,
    proxy_replay_report: str | Path | None,
    runtime_calibration_report: str | Path | None,
    runtime_replay_report: str | Path | None,
) -> dict[str, Any]:
    return {
        "fixture_comparison": _load_report(fixture_comparison_report),
        "fixture_trial_batch": _load_report(fixture_trial_batch_manifest),
        "proxy_calibration": _load_report(proxy_calibration_report),
        "proxy_replay": _load_report(proxy_replay_report),
        "runtime_calibration": _load_report(runtime_calibration_report),
        "runtime_replay": _load_report(runtime_replay_report),
    }


def _payload_text(payload: dict[str, Any] | None, key: str) -> str:
    return str(payload.get(key, "")).strip() if isinstance(payload, dict) else ""


def _first_nonempty(*values: str) -> str:
    for value in values:
        if str(value).strip():
            return str(value).strip()
    return ""


def _sidecar_mismatch(
    proxy_payload: dict[str, Any] | None,
    runtime_payload: dict[str, Any] | None,
    fused_payload: dict[str, Any] | None,
) -> str | None:
    games = {_payload_text(payload, "game") for payload in (proxy_payload, runtime_payload, fused_payload) if _payload_text(payload, "game")}
    if len(games) > 1:
        return "provided sidecars refer to different games"
    sources = {_payload_text(payload, "source") for payload in (proxy_payload, runtime_payload, fused_payload) if _payload_text(payload, "source")}
    if len(sources) > 1:
        return "provided sidecars refer to different sources"
    return None


def _resolve_media_path(value: Any) -> Path | None:
    source_text = str(value or "").strip()
    if not source_text:
        return None
    path = Path(source_text).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _viewer_output_path(
    game: str,
    proxy_payload: dict[str, Any] | None,
    runtime_payload: dict[str, Any] | None,
    fused_payload: dict[str, Any] | None,
    proxy_path: Path | None,
    runtime_path: Path | None,
    fused_path: Path | None,
    output_path: str | Path | None,
) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    identifier = _first_nonempty(
        str(proxy_payload.get("scan_id", "")).strip() if isinstance(proxy_payload, dict) else "",
        str(runtime_payload.get("analysis_id", "")).strip() if isinstance(runtime_payload, dict) else "",
        str(fused_payload.get("fusion_id", "")).strip() if isinstance(fused_payload, dict) else "",
        proxy_path.stem if proxy_path is not None else "",
        runtime_path.stem if runtime_path is not None else "",
        fused_path.stem if fused_path is not None else "",
    )
    return DEFAULT_OUTPUT_ROOT / game / f"{_slug(identifier)}.unified_replay_view.html"


def _slug(value: str) -> str:
    lowered = value.lower()
    return "".join(char if char.isalnum() else "-" for char in lowered).strip("-") or "unified-replay-view"


def _build_unified_payload(
    *,
    proxy_payload: dict[str, Any] | None,
    runtime_payload: dict[str, Any] | None,
    fused_payload: dict[str, Any] | None,
    proxy_path: Path | None,
    runtime_path: Path | None,
    fused_path: Path | None,
    media_path: Path | None,
    media_exists: bool,
    game: str,
    source: str,
    reports: dict[str, Any],
    registry_path: str | Path | None,
) -> dict[str, Any]:
    proxy_section = _build_proxy_section(proxy_payload, proxy_path, media_path, media_exists)
    runtime_section = _build_runtime_section(runtime_payload)
    fused_section = _build_fused_section(fused_payload)
    cross_links = _build_cross_links(proxy_section, runtime_section, fused_section)
    review = _build_review_summary(proxy_section, runtime_section, fused_section)
    evaluation = _build_evaluation_overlay(
        proxy_payload,
        runtime_payload,
        fused_payload,
        proxy_path=proxy_path,
        runtime_path=runtime_path,
        fused_path=fused_path,
        reports=reports,
    )
    lifecycle = _build_lifecycle_overlay(
        game=game,
        source=source,
        fused_sidecar_path=fused_path,
        fused_section=fused_section,
        registry_path=registry_path,
    )
    hooks = _build_hook_overlay(
        game=game,
        source=source,
        fused_sidecar_path=fused_path,
        fused_section=fused_section,
        registry_path=registry_path,
    )
    provenance = _build_provenance(
        proxy_section,
        runtime_section,
        fused_section,
        cross_links=cross_links,
        proxy_path=proxy_path,
        runtime_path=runtime_path,
        fused_path=fused_path,
    )
    disagreements = _build_disagreements(
        proxy_section,
        runtime_section,
        fused_section,
        cross_links=cross_links,
        evaluation=evaluation,
    )
    timeline = _build_timeline(proxy_section, runtime_section, fused_section)
    raw_sections = _build_raw_sections(proxy_payload, runtime_payload, fused_payload)
    selected = _select_default_item(proxy_section, runtime_section, fused_section)
    artifact_summary = {
        "proxy_available": proxy_section["available"],
        "runtime_available": runtime_section["available"],
        "fused_available": fused_section["available"],
        "proxy_window_count": len(proxy_section["windows"]),
        "runtime_event_count": len(runtime_section["events"]),
        "fused_event_count": len(fused_section["events"]),
        "cross_link_count": sum(len(value) for value in cross_links.values()),
        "report_overlay_count": (
            int(bool(evaluation.get("proxy")))
            + int(bool(evaluation.get("runtime")))
            + int(bool(evaluation.get("fixture_comparison")))
            + int(bool(evaluation.get("fixture_trial_batch")))
        ),
        "disagreement_count": sum(1 for value in disagreements.values() if value.get("has_any")),
        "lifecycle_count": len(lifecycle.get("by_item_id", {})),
        "hook_candidate_count": len(hooks.get("by_item_id", {})),
    }
    return {
        "clip": {
            "game": game,
            "source": source,
            "media_path": str(media_path) if media_path is not None else None,
            "media_uri": media_path.as_uri() if media_exists and media_path is not None else None,
            "media_exists": media_exists,
            "proxy_sidecar_path": str(proxy_path) if proxy_path is not None else None,
            "runtime_sidecar_path": str(runtime_path) if runtime_path is not None else None,
            "fused_sidecar_path": str(fused_path) if fused_path is not None else None,
        },
        "artifact_summary": artifact_summary,
        "proxy": proxy_section,
        "runtime": runtime_section,
        "fused": fused_section,
        "cross_links": cross_links,
        "provenance": provenance,
        "disagreements": disagreements,
        "lifecycle": lifecycle,
        "hooks": hooks,
        "review": review,
        "evaluation": evaluation,
        "timeline": timeline,
        "selected_item_id": selected,
        "raw_sections": raw_sections,
        "raw_artifacts": {
            "proxy_sidecar_json": proxy_payload or {},
            "runtime_sidecar_json": runtime_payload or {},
            "fused_sidecar_json": fused_payload or {},
            "reports_json": reports,
        },
    }


def _build_lifecycle_overlay(
    *,
    game: str,
    source: str,
    fused_sidecar_path: Path | None,
    fused_section: dict[str, Any],
    registry_path: str | Path | None,
) -> dict[str, Any]:
    if fused_sidecar_path is None or not fused_section.get("available"):
        return {"available": False, "by_item_id": {}, "summary": {}}
    rows = load_candidate_lifecycle_details(
        game=game,
        source=source,
        fused_sidecar_path=fused_sidecar_path,
        registry_path=registry_path,
    )
    by_event_id = {str(row.get("event_id") or ""): row for row in rows if str(row.get("event_id") or "").strip()}
    by_item_id: dict[str, dict[str, Any]] = {}
    states: dict[str, int] = {}
    for event in fused_section.get("events", []):
        event_id = str(event.get("event_id") or "").strip()
        row = by_event_id.get(event_id)
        if not row:
            continue
        by_item_id[str(event["row_id"])] = row
        state = str(row.get("lifecycle_state") or "").strip()
        if state:
            states[state] = states.get(state, 0) + 1
    return {
        "available": bool(by_item_id),
        "by_item_id": by_item_id,
        "summary": {"states": states, "candidate_count": len(by_item_id)},
    }


def _build_hook_overlay(
    *,
    game: str,
    source: str,
    fused_sidecar_path: Path | None,
    fused_section: dict[str, Any],
    registry_path: str | Path | None,
) -> dict[str, Any]:
    if fused_sidecar_path is None or not fused_section.get("available"):
        return {"available": False, "by_item_id": {}, "summary": {}}
    rows = load_hook_candidate_details(
        game=game,
        source=source,
        fused_sidecar_path=fused_sidecar_path,
        registry_path=registry_path,
    )
    by_event_id = {str(row.get("event_id") or ""): row for row in rows if str(row.get("event_id") or "").strip()}
    by_item_id: dict[str, dict[str, Any]] = {}
    mode_counts: dict[str, int] = {}
    for event in fused_section.get("events", []):
        event_id = str(event.get("event_id") or "").strip()
        row = by_event_id.get(event_id)
        if not row:
            continue
        by_item_id[str(event["row_id"])] = row
        mode = str(row.get("hook_mode") or "").strip()
        if mode:
            mode_counts[mode] = mode_counts.get(mode, 0) + 1
    return {
        "available": bool(by_item_id),
        "by_item_id": by_item_id,
        "summary": {"modes": mode_counts, "hook_candidate_count": len(by_item_id)},
    }


def _build_proxy_section(
    payload: dict[str, Any] | None,
    sidecar_path: Path | None,
    media_path: Path | None,
    media_exists: bool,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "available": False,
            "windows": [],
            "top_window": None,
            "review": {"review_status": None, "is_reviewed": False},
            "hf_pipeline": {"available": False, "stages": [], "window_details": {}, "structured_outputs": {}, "status": "missing"},
            "signals_by_window": {},
            "window_lookup": {},
        }
    derived = proxy_replay_viewer._derived_payload(payload, sidecar_path or Path(str(payload.get("sidecar_path") or ".")), media_path, media_exists)
    signals_by_window = {row["window_id"]: list(row.get("signals", [])) for row in derived["windows"]}
    window_lookup = {row["window_id"]: row for row in derived["windows"]}
    return {
        "available": True,
        "summary": derived["proxy_summary"],
        "windows": derived["windows"],
        "top_window": derived["top_window"],
        "review": derived["review"],
        "hf_pipeline": derived["hf_pipeline"],
        "signals_by_window": signals_by_window,
        "window_lookup": window_lookup,
    }


def _build_runtime_section(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "available": False,
            "detections": [],
            "signals": [],
            "events": [],
            "review": {"review_status": None},
            "event_lookup": {},
            "signal_lookup": {},
        }
    matcher = payload.get("matcher", {}) if isinstance(payload.get("matcher"), dict) else {}
    events = payload.get("events", {}) if isinstance(payload.get("events"), dict) else {}
    detections = [replay_viewer._decorate_detection(index, row) for index, row in enumerate(matcher.get("confirmed_detections", [])) if isinstance(row, dict)]
    signals = [replay_viewer._decorate_runtime_signal(index, row) for index, row in enumerate(matcher.get("signals", [])) if isinstance(row, dict)]
    runtime_events = [replay_viewer._decorate_runtime_event(index, row) for index, row in enumerate(events.get("rows", [])) if isinstance(row, dict)]
    return {
        "available": True,
        "summary": {
            "matcher_status": matcher.get("status"),
            "events_status": events.get("status"),
            "signal_count": len(signals),
            "event_count": len(runtime_events),
            "detection_count": len(detections),
        },
        "detections": detections,
        "signals": signals,
        "events": runtime_events,
        "review": payload.get("runtime_review", {}) if isinstance(payload.get("runtime_review"), dict) else {"review_status": None},
        "event_lookup": {row["row_id"]: row for row in runtime_events},
        "signal_lookup": {str(row.get("signal_id")): row for row in signals if str(row.get("signal_id", "")).strip()},
        "detection_lookup": {row["row_id"]: row for row in detections},
    }


def _build_fused_section(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "available": False,
            "summary": {},
            "events": [],
            "normalized_signals": [],
            "review": {"events": {}},
            "event_lookup": {},
            "signal_lookup": {},
            "embedded_proxy": {},
            "embedded_runtime": {},
        }
    fused_review = payload.get("fused_review", {}) if isinstance(payload.get("fused_review"), dict) else {}
    normalized_signals = [
        replay_viewer._decorate_normalized_signal(index, row)
        for index, row in enumerate(payload.get("normalized_signals", []))
        if isinstance(row, dict)
    ]
    events = [
        replay_viewer._decorate_fused_event(index, row, fused_review)
        for index, row in enumerate(payload.get("fused_events", []))
        if isinstance(row, dict)
    ]
    return {
        "available": True,
        "summary": payload.get("fusion_summary", {}) if isinstance(payload.get("fusion_summary"), dict) else {},
        "events": events,
        "normalized_signals": normalized_signals,
        "review": fused_review,
        "event_lookup": {row["row_id"]: row for row in events},
        "signal_lookup": {str(row.get("signal_id")): row for row in normalized_signals if str(row.get("signal_id", "")).strip()},
        "embedded_proxy": payload.get("proxy", {}) if isinstance(payload.get("proxy"), dict) else {},
        "embedded_runtime": payload.get("runtime", {}) if isinstance(payload.get("runtime"), dict) else {},
    }


def _build_cross_links(proxy: dict[str, Any], runtime: dict[str, Any], fused: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    links: dict[str, list[dict[str, Any]]] = {}
    proxy_window_links: dict[str, list[dict[str, Any]]] = {row["window_id"]: [] for row in proxy["windows"]}
    runtime_event_links: dict[str, list[dict[str, Any]]] = {row["row_id"]: [] for row in runtime["events"]}
    fused_event_links: dict[str, list[dict[str, Any]]] = {row["row_id"]: [] for row in fused["events"]}

    normalized_signal_lookup = fused["signal_lookup"]

    for fused_event in fused["events"]:
        contributing_ids = [str(value) for value in fused_event.get("contributing_signals", []) if str(value).strip()]
        contributing_signals = [normalized_signal_lookup[item] for item in contributing_ids if item in normalized_signal_lookup]
        linked_runtime_events = [
            row
            for row in runtime["events"]
            if replay_viewer._rows_overlap(
                fused_event["start_timestamp"],
                fused_event["end_timestamp"],
                row["start_timestamp"],
                row["end_timestamp"],
            )
            and replay_viewer._semantic_overlap(
                fused_event.get("metadata", {}) if isinstance(fused_event.get("metadata"), dict) else {},
                row,
            )
        ]
        linked_detections = [
            row
            for row in runtime["detections"]
            if replay_viewer._rows_overlap(
                fused_event["start_timestamp"],
                fused_event["end_timestamp"],
                row["start_timestamp"],
                row["end_timestamp"],
            )
            and replay_viewer._detection_overlap(
                fused_event.get("metadata", {}) if isinstance(fused_event.get("metadata"), dict) else {},
                row,
                [runtime["signal_lookup"][item] for item in contributing_ids if item in runtime["signal_lookup"]],
            )
        ]
        linked_windows: list[dict[str, Any]] = []
        for signal in contributing_signals:
            evidence = signal.get("evidence", {}) if isinstance(signal.get("evidence"), dict) else {}
            for match in evidence.get("matching_windows", []):
                if not isinstance(match, dict):
                    continue
                window_index = match.get("window_index")
                if isinstance(window_index, int):
                    window_id = f"window-{window_index}"
                    window = proxy["window_lookup"].get(window_id)
                    if window is not None and window not in linked_windows:
                        linked_windows.append(window)
        if not linked_windows:
            linked_windows = [
                row
                for row in proxy["windows"]
                if replay_viewer._rows_overlap(
                    fused_event["start_timestamp"],
                    fused_event["end_timestamp"],
                    row["start_seconds"],
                    row["end_seconds"],
                )
            ]
        for window in linked_windows:
            proxy_window_links.setdefault(window["window_id"], []).append({"kind": "fused_event", "id": fused_event["row_id"], "label": fused_event["label"]})
            fused_event_links.setdefault(fused_event["row_id"], []).append({"kind": "proxy_window", "id": window["window_id"], "label": window["recommended_action"]})
        for event in linked_runtime_events:
            runtime_event_links.setdefault(event["row_id"], []).append({"kind": "fused_event", "id": fused_event["row_id"], "label": fused_event["label"]})
            fused_event_links.setdefault(fused_event["row_id"], []).append({"kind": "runtime_event", "id": event["row_id"], "label": event["label"]})
        for detection in linked_detections:
            fused_event_links.setdefault(fused_event["row_id"], []).append({"kind": "detection", "id": detection["row_id"], "label": detection["label"]})

    for runtime_event in runtime["events"]:
        linked_windows = [
            row
            for row in proxy["windows"]
            if replay_viewer._rows_overlap(
                runtime_event["start_timestamp"],
                runtime_event["end_timestamp"],
                row["start_seconds"],
                row["end_seconds"],
            )
        ]
        for window in linked_windows:
            proxy_window_links.setdefault(window["window_id"], []).append({"kind": "runtime_event", "id": runtime_event["row_id"], "label": runtime_event["label"]})
            runtime_event_links.setdefault(runtime_event["row_id"], []).append({"kind": "proxy_window", "id": window["window_id"], "label": window["recommended_action"]})

    for window_id, items in proxy_window_links.items():
        links[window_id] = _dedupe_links(items)
    for event_id, items in runtime_event_links.items():
        links[event_id] = _dedupe_links(items)
    for fused_id, items in fused_event_links.items():
        links[fused_id] = _dedupe_links(items)
    return links


def _dedupe_links(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in items:
        key = (str(item.get("kind")), str(item.get("id")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _build_provenance(
    proxy: dict[str, Any],
    runtime: dict[str, Any],
    fused: dict[str, Any],
    *,
    cross_links: dict[str, list[dict[str, Any]]],
    proxy_path: Path | None,
    runtime_path: Path | None,
    fused_path: Path | None,
) -> dict[str, dict[str, Any]]:
    provenance: dict[str, dict[str, Any]] = {}
    runtime_signals = runtime.get("signals", [])
    runtime_detections = runtime.get("detections", [])
    fused_signal_lookup = fused.get("signal_lookup", {})

    for window in proxy["windows"]:
        window_id = str(window["window_id"])
        hf_details = proxy["hf_pipeline"].get("window_details", {}).get(window_id, {})
        provenance[window_id] = {
            "kind": "proxy_window",
            "artifact_layer": "proxy",
            "artifact_path": str(proxy_path) if proxy_path is not None else None,
            "source_families": list(window.get("source_families", [])),
            "signal_sources": [str(signal.get("source", "")) for signal in window.get("signals", []) if str(signal.get("source", "")).strip()],
            "signal_count": len(window.get("signals", [])),
            "hf_stage_contributions": list(hf_details.get("signal_contributions", [])) if isinstance(hf_details, dict) else [],
            "score_breakdown": dict(hf_details.get("score_breakdown", {})) if isinstance(hf_details, dict) else {},
            "downstream_consumers": list(cross_links.get(window_id, [])),
            "missing_evidence": not bool(window.get("signals")) and not bool(hf_details),
        }

    for event in runtime["events"]:
        row_id = str(event["row_id"])
        overlapping_signals = [
            signal
            for signal in runtime_signals
            if replay_viewer._rows_overlap(
                float(event["start_timestamp"]),
                float(event["end_timestamp"]),
                float(signal["start_timestamp"]),
                float(signal["end_timestamp"]),
            )
        ]
        overlapping_detections = [
            detection
            for detection in runtime_detections
            if replay_viewer._rows_overlap(
                float(event["start_timestamp"]),
                float(event["end_timestamp"]),
                float(detection["start_timestamp"]),
                float(detection["end_timestamp"]),
            )
        ]
        provenance[row_id] = {
            "kind": "runtime_event",
            "artifact_layer": "runtime",
            "artifact_path": str(runtime_path) if runtime_path is not None else None,
            "supporting_signals": [
                {
                    "signal_id": signal.get("signal_id"),
                    "label": signal.get("label"),
                    "score": signal.get("score"),
                }
                for signal in overlapping_signals
            ],
            "supporting_detections": [
                {
                    "row_id": detection.get("row_id"),
                    "label": detection.get("label"),
                    "score": detection.get("score"),
                }
                for detection in overlapping_detections
            ],
            "downstream_consumers": [
                link for link in cross_links.get(row_id, []) if str(link.get("kind")) == "fused_event"
            ],
            "upstream_sources": [
                link for link in cross_links.get(row_id, []) if str(link.get("kind")) == "proxy_window"
            ],
            "missing_evidence": not overlapping_signals and not overlapping_detections,
        }

    for event in fused["events"]:
        row_id = str(event["row_id"])
        contributing_ids = [str(value) for value in event.get("contributing_signals", []) if str(value).strip()]
        normalized_signals = [fused_signal_lookup[item] for item in contributing_ids if item in fused_signal_lookup]
        matching_windows: list[dict[str, Any]] = []
        for signal in normalized_signals:
            evidence = signal.get("evidence", {}) if isinstance(signal.get("evidence"), dict) else {}
            for match in evidence.get("matching_windows", []):
                if isinstance(match, dict):
                    matching_windows.append(match)
        provenance[row_id] = {
            "kind": "fused_event",
            "artifact_layer": "fused",
            "artifact_path": str(fused_path) if fused_path is not None else None,
            "contributing_signal_ids": contributing_ids,
            "contributing_signals": [
                {
                    "signal_id": signal.get("signal_id"),
                    "label": signal.get("label"),
                    "producer_family": signal.get("producer_family"),
                    "score": signal.get("score"),
                }
                for signal in normalized_signals
            ],
            "matching_windows": matching_windows,
            "upstream_sources": [
                link for link in cross_links.get(row_id, []) if str(link.get("kind")) in {"proxy_window", "runtime_event", "detection"}
            ],
            "missing_evidence": not normalized_signals,
        }
    return provenance


def _build_disagreements(
    proxy: dict[str, Any],
    runtime: dict[str, Any],
    fused: dict[str, Any],
    *,
    cross_links: dict[str, list[dict[str, Any]]],
    evaluation: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    disagreements: dict[str, dict[str, Any]] = {}
    review_status_by_id: dict[str, str] = {}
    proxy_status = str(proxy["review"].get("review_status") or "").strip().lower()
    runtime_status = str(runtime["review"].get("review_status") or "").strip().lower()
    for window in proxy["windows"]:
        if proxy_status:
            review_status_by_id[str(window["window_id"])] = proxy_status
    for event in runtime["events"]:
        if runtime_status:
            review_status_by_id[str(event["row_id"])] = runtime_status
    for event in fused["events"]:
        fused_status = str(event.get("review", {}).get("review_status") or "").strip().lower()
        if fused_status:
            review_status_by_id[str(event["row_id"])] = fused_status

    comparison_payload = evaluation.get("fixture_comparison", {}) if isinstance(evaluation, dict) else {}
    comparison_row = comparison_payload.get("row", {}) if isinstance(comparison_payload, dict) else {}
    comparison_recommendation = _normalize_recommendation_payload(
        comparison_payload.get("recommendation", {}) if isinstance(comparison_payload, dict) else {}
    )
    proxy_replay = evaluation.get("proxy", {}).get("replay", {}) if isinstance(evaluation.get("proxy"), dict) else {}
    proxy_recommendation = _normalize_recommendation_payload(
        evaluation.get("proxy", {}).get("replay_recommendation", {}) if isinstance(evaluation.get("proxy"), dict) else {}
    )
    runtime_replay = evaluation.get("runtime", {}).get("replay", {}) if isinstance(evaluation.get("runtime"), dict) else {}
    runtime_recommendation = _normalize_recommendation_payload(
        evaluation.get("runtime", {}).get("replay_recommendation", {}) if isinstance(evaluation.get("runtime"), dict) else {}
    )

    for row_id in cross_links:
        linked_statuses = {
            review_status_by_id.get(str(link.get("id")), "")
            for link in cross_links.get(row_id, [])
            if review_status_by_id.get(str(link.get("id")), "")
        }
        own_status = review_status_by_id.get(row_id, "")
        statuses = {status for status in linked_statuses | ({own_status} if own_status else set()) if status}
        review_disagreement = own_status == "rejected" and any(status == "approved" for status in linked_statuses)
        cross_layer_disagreement = len(statuses) > 1
        weak_evidence_downstream = False
        weak_reason = ""
        if row_id.startswith("fused-event-"):
            linked_items = cross_links.get(row_id, [])
            linked_proxy_ids = [str(link.get("id")) for link in linked_items if str(link.get("kind")) == "proxy_window"]
            linked_runtime_ids = [str(link.get("id")) for link in linked_items if str(link.get("kind")) == "runtime_event"]
            rejected_proxy = any(review_status_by_id.get(item) == "rejected" for item in linked_proxy_ids)
            if rejected_proxy or not linked_runtime_ids:
                weak_evidence_downstream = True
                weak_reason = (
                    "Downstream fused approval depends on rejected upstream proxy evidence."
                    if rejected_proxy
                    else "Downstream fused event has no linked runtime event support."
                )

        trial_disagreement = False
        trial_reason = ""
        if row_id.startswith("window-") and proxy_recommendation.get("decision"):
            score_delta = float(proxy_replay.get("score_delta") or 0.0)
            decision = str(proxy_recommendation.get("decision", "")).strip()
            trial_disagreement = decision in {"prefer_trial", "keep_current"} or abs(score_delta) > 1e-9
            if trial_disagreement:
                trial_reason = str(proxy_recommendation.get("reason") or f"Proxy trial delta {score_delta:+.3f}").strip()
        elif row_id.startswith("runtime-event-") and runtime_recommendation.get("decision"):
            score_delta = float(runtime_replay.get("score_delta") or 0.0)
            decision = str(runtime_recommendation.get("decision", "")).strip()
            trial_disagreement = decision in {"prefer_trial", "keep_current"} or abs(score_delta) > 1e-9
            if trial_disagreement:
                trial_reason = str(runtime_recommendation.get("reason") or f"Runtime trial delta {score_delta:+.3f}").strip()
        elif comparison_row:
            decision = str(comparison_recommendation.get("decision") or comparison_row.get("recommendation_signal") or "").strip()
            trial_disagreement = bool(decision)
            if trial_disagreement:
                trial_reason = str(comparison_recommendation.get("reason") or comparison_row.get("recommendation_signal") or "").strip()

        groups = {
            "review_disagreement": {
                "active": review_disagreement,
                "reason": "Review status conflicts with linked evidence layers." if review_disagreement else "",
            },
            "cross_layer_disagreement": {
                "active": cross_layer_disagreement,
                "reason": "Linked artifact layers disagree on review outcome." if cross_layer_disagreement else "",
            },
            "trial_disagreement": {
                "active": trial_disagreement,
                "reason": trial_reason,
            },
            "weak_evidence_downstream": {
                "active": weak_evidence_downstream,
                "reason": weak_reason,
            },
        }
        disagreements[row_id] = {
            "has_any": any(group["active"] for group in groups.values()),
            "groups": groups,
            "status_snapshot": sorted(statuses),
        }
    return disagreements


def _build_review_summary(proxy: dict[str, Any], runtime: dict[str, Any], fused: dict[str, Any]) -> dict[str, Any]:
    proxy_status = str(proxy["review"].get("review_status") or "").strip().lower() or None
    runtime_status = str(runtime["review"].get("review_status") or "").strip().lower() or None
    fused_status = _top_fused_review_status(fused["review"])
    statuses = [value for value in (proxy_status, runtime_status, fused_status) if value]
    disagreement = len(set(statuses)) > 1
    note = None
    if proxy_status == "rejected" and proxy["top_window"] is not None and float(proxy["top_window"].get("proxy_score") or 0.0) >= 0.75:
        note = "High-scoring proxy window was rejected in proxy review."
    elif disagreement:
        note = "Review outcomes differ across artifact layers."
    return {
        "proxy_review_status": proxy_status,
        "runtime_review_status": runtime_status,
        "fused_review_status": fused_status,
        "has_disagreement": disagreement,
        "note": note,
        "status_snapshot": [value for value in (proxy_status, runtime_status, fused_status) if value],
    }


def _build_evaluation_overlay(
    proxy_payload: dict[str, Any] | None,
    runtime_payload: dict[str, Any] | None,
    fused_payload: dict[str, Any] | None,
    *,
    proxy_path: Path | None,
    runtime_path: Path | None,
    fused_path: Path | None,
    reports: dict[str, Any],
) -> dict[str, Any]:
    proxy_section = {}
    runtime_section = {}
    fixture_section = {}
    current_paths = {
        "proxy": str(proxy_path.resolve()) if proxy_path is not None else "",
        "runtime": str(runtime_path.resolve()) if runtime_path is not None else "",
        "fused": str(fused_path.resolve()) if fused_path is not None else "",
    }
    if isinstance(proxy_payload, dict):
        scan_id = str(proxy_payload.get("scan_id", "")).strip()
        proxy_section = {
            "scan_id": scan_id,
            "calibration": _proxy_calibration_clip(reports.get("proxy_calibration"), scan_id),
            "replay": _proxy_replay_clip(reports.get("proxy_replay"), scan_id),
            "calibration_recommendations": list(
                (reports.get("proxy_calibration") or {}).get("recommendations", {}).get("threshold_observations", [])
            )
            + list((reports.get("proxy_calibration") or {}).get("recommendations", {}).get("weight_observations", [])),
            "replay_recommendation": _normalize_recommendation_payload((reports.get("proxy_replay") or {}).get("recommendation", {})),
            "current_scoring": (reports.get("proxy_calibration") or {}).get("current_scoring")
            or (reports.get("proxy_replay") or {}).get("current_proxy_scoring", {}),
            "trial_scoring": (reports.get("proxy_replay") or {}).get("trial_proxy_scoring", {}),
        }
    if isinstance(runtime_payload, dict):
        analysis_id = str(runtime_payload.get("analysis_id", "")).strip()
        runtime_section = {
            "analysis_id": analysis_id,
            "calibration": _runtime_calibration_clip(reports.get("runtime_calibration"), analysis_id),
            "replay": _runtime_replay_clip(reports.get("runtime_replay"), analysis_id),
            "calibration_recommendations": list(
                (reports.get("runtime_calibration") or {}).get("recommendations", {}).get("threshold_observations", [])
            )
            + list((reports.get("runtime_calibration") or {}).get("recommendations", {}).get("weight_observations", [])),
            "replay_recommendation": _normalize_recommendation_payload((reports.get("runtime_replay") or {}).get("recommendation", {})),
            "current_scoring": (reports.get("runtime_calibration") or {}).get("current_scoring")
            or (reports.get("runtime_replay") or {}).get("current_scoring", {}),
            "trial_scoring": (reports.get("runtime_replay") or {}).get("trial_scoring", {}),
        }
    fixture_section = _fixture_comparison_clip(reports.get("fixture_comparison"), current_paths)
    fixture_batch_section = _fixture_trial_batch_clip(reports.get("fixture_trial_batch"), current_paths)
    return {
        "fixture_comparison": fixture_section,
        "fixture_trial_batch": fixture_batch_section,
        "proxy": proxy_section,
        "runtime": runtime_section,
    }


def _proxy_calibration_clip(report: dict[str, Any] | None, scan_id: str) -> dict[str, Any]:
    reviewed = list((report or {}).get("diagnostics", {}).get("reviewed_clips", []))
    for row in reviewed:
        if isinstance(row, dict) and str(row.get("scan_id", "")).strip() == scan_id:
            return row
    return {}


def _normalize_recommendation_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, Any] = {}
    decision = str(payload.get("decision", "")).strip()
    if decision:
        normalized["decision"] = decision
    reason = str(payload.get("reason", "")).strip()
    if reason:
        normalized["reason"] = reason
    if isinstance(payload.get("supporting_metrics"), dict):
        normalized["supporting_metrics"] = dict(payload["supporting_metrics"])
    if isinstance(payload.get("data_quality_notes"), list):
        normalized["data_quality_notes"] = [str(item) for item in payload["data_quality_notes"] if str(item).strip()]
    follow_up = str(payload.get("follow_up", "")).strip()
    if follow_up:
        normalized["follow_up"] = follow_up
    return normalized


def _proxy_replay_clip(report: dict[str, Any] | None, scan_id: str) -> dict[str, Any]:
    reviewed = list((report or {}).get("comparison", {}).get("reviewed_comparisons", []))
    for row in reviewed:
        if isinstance(row, dict) and str(row.get("scan_id", "")).strip() == scan_id:
            return row
    return {}


def _runtime_calibration_clip(report: dict[str, Any] | None, analysis_id: str) -> dict[str, Any]:
    reviewed = list((report or {}).get("diagnostics", {}).get("reviewed_clips", []))
    for row in reviewed:
        if isinstance(row, dict) and str(row.get("analysis_id", "")).strip() == analysis_id:
            return row
    return {}


def _runtime_replay_clip(report: dict[str, Any] | None, analysis_id: str) -> dict[str, Any]:
    reviewed = list((report or {}).get("comparison", {}).get("reviewed_comparisons", []))
    for row in reviewed:
        if isinstance(row, dict) and str(row.get("analysis_id", "")).strip() == analysis_id:
            return row
    return {}


def _fixture_comparison_clip(
    report: dict[str, Any] | None,
    current_paths: dict[str, str],
) -> dict[str, Any]:
    rows = list((report or {}).get("comparison", {}).get("fixture_rows", []))
    normalized_current_paths = {
        key: str(Path(value).resolve()) if str(value).strip() else ""
        for key, value in current_paths.items()
        if str(value).strip()
    }
    for row in rows:
        if not isinstance(row, dict):
            continue
        layer = str(row.get("artifact_layer", "")).strip()
        current_path = normalized_current_paths.get(layer, "")
        baseline_path = str(row.get("baseline_sidecar_path", "")).strip()
        trial_path = str(row.get("trial_sidecar_path", "")).strip()
        baseline_match = bool(current_path and baseline_path and Path(baseline_path).resolve() == Path(current_path).resolve())
        trial_match = bool(current_path and trial_path and Path(trial_path).resolve() == Path(current_path).resolve())
        if baseline_match or trial_match:
            return {
                "role": "baseline" if baseline_match else "trial",
                "row": row,
                "recommendation": (report or {}).get("recommendation", {}),
            }
    return {}


def _fixture_trial_batch_clip(
    report: dict[str, Any] | None,
    current_paths: dict[str, str],
) -> dict[str, Any]:
    if not isinstance(report, dict):
        return {}
    matches: list[dict[str, Any]] = []
    for comparison in list(report.get("trial_comparisons", [])):
        if not isinstance(comparison, dict):
            continue
        report_path = str(comparison.get("comparison_report_path", "")).strip()
        if not report_path:
            continue
        try:
            comparison_payload = _load_json(_resolve_path(report_path))
        except Exception:
            continue
        fixture_match = _fixture_comparison_clip(comparison_payload, current_paths)
        if not fixture_match:
            continue
        matches.append(
            {
                "trial_name": str(comparison.get("trial_name", "")).strip(),
                "comparison_report_path": report_path,
                "artifact_layer": str(comparison.get("artifact_layer", "")).strip(),
                "comparison_status": str(comparison.get("comparison_status", "")).strip(),
                "recommendation": dict(comparison.get("recommendation", {})),
                "fixture_match": fixture_match,
            }
        )
    if not matches:
        return {}
    return {
        "batch_name": str(report.get("batch_name", "")).strip(),
        "baseline_trial_name": str(report.get("baseline_trial_name", "")).strip(),
        "overall_recommendation": dict(report.get("overall_recommendation", {})),
        "covered_trials": [row["trial_name"] for row in matches if row["trial_name"]],
        "matches": matches,
    }


def _top_fused_review_status(review: dict[str, Any]) -> str | None:
    events = review.get("events", {}) if isinstance(review, dict) and isinstance(review.get("events"), dict) else {}
    statuses = {
        str(value.get("review_status", "")).strip().lower()
        for value in events.values()
        if isinstance(value, dict) and str(value.get("review_status", "")).strip()
    }
    if not statuses:
        return None
    if len(statuses) == 1:
        return next(iter(statuses))
    return "mixed"


def _build_timeline(proxy: dict[str, Any], runtime: dict[str, Any], fused: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in proxy["windows"]:
        rows.append(
            {
                "entry_id": row["window_id"],
                "target_id": row["window_id"],
                "kind": "proxy_window",
                "label": str(row.get("recommended_action") or "proxy_window"),
                "start_timestamp": row["start_seconds"],
                "end_timestamp": row["end_seconds"],
                "jump_timestamp": row["start_seconds"],
                "score": row.get("proxy_score"),
                "detail": ",".join(row.get("source_families", [])),
                "search_text": row.get("search_text", ""),
            }
        )
    for row in runtime["events"]:
        rows.append(
            {
                "entry_id": row["row_id"],
                "target_id": row["row_id"],
                "kind": "runtime_event",
                "label": str(row.get("label") or "runtime_event"),
                "start_timestamp": row["start_timestamp"],
                "end_timestamp": row["end_timestamp"],
                "jump_timestamp": row["jump_timestamp"],
                "score": row.get("score"),
                "detail": str(row.get("event_id") or ""),
                "search_text": row.get("search_text", ""),
            }
        )
    for row in fused["events"]:
        rows.append(
            {
                "entry_id": row["row_id"],
                "target_id": row["row_id"],
                "kind": "fused_event",
                "label": str(row.get("label") or "fused_event"),
                "start_timestamp": round(float(row.get("suggested_start_timestamp", row["start_timestamp"]) or row["start_timestamp"]), 5),
                "end_timestamp": round(float(row.get("suggested_end_timestamp", row["end_timestamp"]) or row["end_timestamp"]), 5),
                "jump_timestamp": row["jump_timestamp"],
                "score": row.get("score"),
                "detail": str(row.get("gate_status") or ""),
                "search_text": row.get("search_text", ""),
            }
        )
    rows.sort(key=lambda item: (float(item["start_timestamp"]), str(item["kind"]), str(item["label"])))
    return rows


def _build_raw_sections(
    proxy_payload: dict[str, Any] | None,
    runtime_payload: dict[str, Any] | None,
    fused_payload: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    sections = []
    if isinstance(proxy_payload, dict):
        sections.append({"section_id": "raw-proxy-sidecar", "title": "Proxy Sidecar", "payload": proxy_payload})
    if isinstance(runtime_payload, dict):
        sections.append({"section_id": "raw-runtime-sidecar", "title": "Runtime Sidecar", "payload": runtime_payload})
    if isinstance(fused_payload, dict):
        sections.append({"section_id": "raw-fused-sidecar", "title": "Fused Sidecar", "payload": fused_payload})
    return sections


def _select_default_item(proxy: dict[str, Any], runtime: dict[str, Any], fused: dict[str, Any]) -> str | None:
    if proxy["top_window"] is not None:
        return str(proxy["top_window"]["window_id"])
    if fused["events"]:
        return str(fused["events"][0]["row_id"])
    if runtime["events"]:
        return str(runtime["events"][0]["row_id"])
    return None


def _render_html(derived: dict[str, Any]) -> str:
    clip = derived["clip"]
    game = html.escape(str(clip["game"] or "unknown_game"))
    media_html = (
        f'<video id="viewer-media" controls preload="metadata" src="{html.escape(str(clip["media_uri"]))}" class="video-player"></video>'
        if clip["media_exists"]
        else '<div class="media-missing">Local media source not available. The viewer still renders sidecar diagnostics.</div>'
    )
    data_json = json.dumps(derived, sort_keys=True)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Unified Replay Viewer - {game}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4f6fb;
      --panel: #ffffff;
      --muted: #5f6b7a;
      --text: #122033;
      --border: #d7dfeb;
      --accent: #1166cc;
      --proxy: #0f8a5f;
      --runtime: #9d6400;
      --fused: #7c3aed;
      --bad: #b43737;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: var(--bg); color: var(--text); }}
    .page {{ max-width: 1540px; margin: 0 auto; padding: 20px; }}
    .hero, .panel {{ background: var(--panel); border: 1px solid var(--border); border-radius: 14px; box-shadow: 0 10px 24px rgba(17, 24, 39, 0.05); }}
    .hero {{ padding: 20px; margin-bottom: 18px; }}
    .hero h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .meta {{ color: var(--muted); font-size: 14px; display: grid; gap: 4px; }}
    .top-grid {{ display: grid; grid-template-columns: 1.35fr 1fr; gap: 18px; margin-bottom: 18px; }}
    .panel {{ padding: 16px; }}
    .video-player {{ width: 100%; border-radius: 10px; background: #000; }}
    .media-missing {{ padding: 18px; border: 1px dashed var(--border); border-radius: 10px; color: var(--muted); }}
    .summary-grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-top: 14px; }}
    .metric {{ border: 1px solid var(--border); border-radius: 10px; padding: 12px; background: #fbfcff; }}
    .metric-label {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }}
    .metric-value {{ margin-top: 4px; font-size: 20px; font-weight: 700; }}
    .tag {{ display: inline-flex; align-items: center; border-radius: 999px; padding: 3px 10px; font-size: 12px; font-weight: 600; border: 1px solid var(--border); background: #f9fbff; margin-right: 6px; margin-bottom: 6px; }}
    .tag.proxy {{ color: var(--proxy); border-color: rgba(15,138,95,.25); background: rgba(15,138,95,.08); }}
    .tag.runtime {{ color: var(--runtime); border-color: rgba(157,100,0,.25); background: rgba(157,100,0,.08); }}
    .tag.fused {{ color: var(--fused); border-color: rgba(124,58,237,.25); background: rgba(124,58,237,.08); }}
    .tag.bad {{ color: var(--bad); border-color: rgba(180,55,55,.25); background: rgba(180,55,55,.08); }}
    .controls {{ display: grid; grid-template-columns: 1fr 180px auto; gap: 12px; margin-bottom: 14px; }}
    .toggle-row {{ display: flex; gap: 12px; align-items: center; color: var(--muted); font-size: 13px; }}
    .control-input {{ width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid var(--border); background: #fff; }}
    .layout {{ display: grid; grid-template-columns: 360px 1fr; gap: 18px; }}
    .item-list {{ display: grid; gap: 10px; max-height: 860px; overflow: auto; padding-right: 4px; }}
    .item-card {{ border: 1px solid var(--border); border-radius: 12px; padding: 12px; background: #fbfcff; cursor: pointer; }}
    .item-card.active {{ border-color: var(--accent); box-shadow: inset 0 0 0 1px var(--accent); background: #f4f9ff; }}
    .item-card h3 {{ margin: 0 0 8px; font-size: 15px; }}
    .item-meta {{ font-size: 13px; color: var(--muted); display: grid; gap: 4px; }}
    .button-row {{ display: flex; gap: 8px; margin-top: 10px; flex-wrap: wrap; }}
    button {{ border: 1px solid var(--border); background: #fff; color: var(--text); border-radius: 10px; padding: 8px 12px; cursor: pointer; }}
    button:hover {{ border-color: var(--accent); color: var(--accent); }}
    .details-grid {{ display: grid; gap: 16px; }}
    .subgrid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }}
    .section-title {{ margin: 0 0 10px; font-size: 18px; }}
    .kv {{ display: grid; grid-template-columns: 180px 1fr; gap: 8px; font-size: 14px; margin-bottom: 6px; }}
    .kv .key {{ color: var(--muted); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 600; }}
    pre {{ margin: 0; white-space: pre-wrap; word-break: break-word; font-size: 12px; background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 10px; max-height: 360px; overflow: auto; }}
    .timeline-track {{ position: relative; height: 62px; border-radius: 10px; background: linear-gradient(90deg, #edf3ff 0%, #f9fbff 100%); border: 1px solid var(--border); overflow: hidden; }}
    .timeline-bar {{ position: absolute; top: 8px; bottom: 8px; border-radius: 8px; border: 1px solid; opacity: .82; }}
    .timeline-bar.proxy_window {{ background: rgba(15,138,95,.20); border-color: rgba(15,138,95,.45); }}
    .timeline-bar.runtime_event {{ background: rgba(157,100,0,.18); border-color: rgba(157,100,0,.45); }}
    .timeline-bar.fused_event {{ background: rgba(124,58,237,.18); border-color: rgba(124,58,237,.45); }}
    .timeline-bar.active {{ opacity: 1; box-shadow: inset 0 0 0 1px rgba(17,102,204,.9); }}
    details {{ border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; background: #fff; }}
    summary {{ cursor: pointer; font-weight: 600; }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>Unified Replay Viewer - {game}</h1>
      <div class="meta">
        <div><strong>Proxy sidecar:</strong> {html.escape(str(clip["proxy_sidecar_path"] or "not provided"))}</div>
        <div><strong>Runtime sidecar:</strong> {html.escape(str(clip["runtime_sidecar_path"] or "not provided"))}</div>
        <div><strong>Fused sidecar:</strong> {html.escape(str(clip["fused_sidecar_path"] or "not provided"))}</div>
        <div><strong>Source:</strong> {html.escape(str(clip["source"] or ""))}</div>
      </div>
    </section>

    <section class="top-grid">
      <div class="panel">
        <h2 class="section-title">Clip Playback</h2>
        {media_html}
        <div class="summary-grid">
          <div class="metric"><div class="metric-label">Proxy Windows</div><div class="metric-value">{derived["artifact_summary"]["proxy_window_count"]}</div></div>
          <div class="metric"><div class="metric-label">Runtime Events</div><div class="metric-value">{derived["artifact_summary"]["runtime_event_count"]}</div></div>
          <div class="metric"><div class="metric-label">Fused Events</div><div class="metric-value">{derived["artifact_summary"]["fused_event_count"]}</div></div>
          <div class="metric"><div class="metric-label">Cross Links / Disagreements</div><div class="metric-value">{derived["artifact_summary"]["cross_link_count"]} / {derived["artifact_summary"]["disagreement_count"]}</div></div>
        </div>
      </div>
      <div class="panel">
        <h2 class="section-title">Review Snapshot</h2>
        <div id="review-summary">{_render_review_summary(derived["review"])}</div>
      </div>
    </section>

    <section class="panel" style="margin-bottom: 18px;">
      <h2 class="section-title">Unified Timeline</h2>
      <div class="controls">
        <input id="viewer-search" class="control-input" type="text" placeholder="filter by action, event type, signal, or id">
        <input id="viewer-min-score" class="control-input" type="number" min="0" max="1" step="0.01" placeholder="min score">
        <div class="toggle-row">
          <label><input type="checkbox" data-kind-toggle="proxy_window" checked> Proxy</label>
          <label><input type="checkbox" data-kind-toggle="runtime_event" checked> Runtime</label>
          <label><input type="checkbox" data-kind-toggle="fused_event" checked> Fused</label>
        </div>
      </div>
      <div class="timeline-track" id="timeline-track">
        {_render_timeline(derived["timeline"], derived["selected_item_id"])}
      </div>
    </section>

    <section class="layout">
      <div class="panel">
        <h2 class="section-title">Inspectable Items</h2>
        <div class="item-list" id="item-list">
          {_render_item_list(derived, derived["selected_item_id"])}
        </div>
      </div>
      <div class="details-grid">
        <div class="panel">
          <h2 class="section-title">Selected Item Detail</h2>
          <div id="selected-detail"></div>
        </div>
        <div class="subgrid">
          <div class="panel">
            <h2 class="section-title">Provenance And Linked Evidence</h2>
            <div id="linked-evidence"></div>
          </div>
          <div class="panel">
            <h2 class="section-title">Layer Status</h2>
            <div id="layer-status">{_render_layer_status(derived)}</div>
          </div>
        </div>
        <div class="panel">
          <h2 class="section-title">Evaluation Overlay</h2>
          <div id="evaluation-overlay"></div>
        </div>
        <div class="panel">
          <h2 class="section-title">Raw JSON Inspector</h2>
          {_render_raw_sections(derived["raw_sections"])}
        </div>
      </div>
    </section>
  </div>
  <script>
    const VIEWER_DATA = {data_json};
    const viewerState = {{ selectedItemId: {json.dumps(derived["selected_item_id"])} }};

    function formatNumber(value) {{
      if (value === null || value === undefined || value === "") return "n/a";
      const number = Number(value);
      if (Number.isNaN(number)) return String(value);
      return number.toFixed(3).replace(/0+$/, "").replace(/\\.$/, "");
    }}

    function escapeHtml(value) {{
      return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }}

    function itemLookup() {{
      const lookup = {{}};
      for (const row of VIEWER_DATA.proxy.windows) lookup[row.window_id] = {{ kind: "proxy_window", row }};
      for (const row of VIEWER_DATA.runtime.events) lookup[row.row_id] = {{ kind: "runtime_event", row }};
      for (const row of VIEWER_DATA.fused.events) lookup[row.row_id] = {{ kind: "fused_event", row }};
      return lookup;
    }}

    const ITEM_LOOKUP = itemLookup();

    function itemKey(item) {{
      return item.row.window_id || item.row.row_id || "";
    }}

    function currentProvenance(item) {{
      return VIEWER_DATA.provenance[itemKey(item)] || {{}};
    }}

    function currentDisagreements(item) {{
      return VIEWER_DATA.disagreements[itemKey(item)] || {{}};
    }}

    function currentLifecycle(item) {{
      return (VIEWER_DATA.lifecycle && VIEWER_DATA.lifecycle.by_item_id && VIEWER_DATA.lifecycle.by_item_id[itemKey(item)]) || {{}};
    }}

    function currentHook(item) {{
      return (VIEWER_DATA.hooks && VIEWER_DATA.hooks.by_item_id && VIEWER_DATA.hooks.by_item_id[itemKey(item)]) || {{}};
    }}

    function renderTagList(values, className = "") {{
      if (!Array.isArray(values) || !values.length) return "n/a";
      return values.map((value) => `<span class="tag ${{className}}">${{escapeHtml(value)}}</span>`).join("");
    }}

    function renderDisagreementBlock(item) {{
      const payload = currentDisagreements(item);
      const groups = payload.groups || {{}};
      const activeEntries = Object.entries(groups).filter(([, value]) => value && value.active);
      if (!activeEntries.length) {{
        return `<div class="kv"><div class="key">Disagreements</div><div>None detected for this item.</div></div>`;
      }}
      const rows = activeEntries.map(([name, value]) => `
        <tr>
          <td>${{escapeHtml(name)}}</td>
          <td>${{escapeHtml(value.reason || "active")}}</td>
        </tr>
      `).join("");
      return `
        <div class="kv"><div class="key">Disagreements</div><div>${{activeEntries.length}} active</div></div>
        <table>
          <thead><tr><th>Group</th><th>Reason</th></tr></thead>
          <tbody>${{rows}}</tbody>
        </table>
      `;
    }}

    function renderProvenanceBlock(item) {{
      const provenance = currentProvenance(item);
      if (!Object.keys(provenance).length) {{
        return `<div class="kv"><div class="key">Provenance</div><div>No structured provenance available.</div></div>`;
      }}
      const rows = [
        `<div class="kv"><div class="key">Artifact layer</div><div>${{escapeHtml(provenance.artifact_layer || "n/a")}}</div></div>`,
        `<div class="kv"><div class="key">Artifact path</div><div>${{escapeHtml(provenance.artifact_path || "n/a")}}</div></div>`,
      ];
      if (provenance.source_families) {{
        rows.push(`<div class="kv"><div class="key">Source families</div><div>${{renderTagList(provenance.source_families, "proxy")}}</div></div>`);
      }}
      if (provenance.signal_sources) {{
        rows.push(`<div class="kv"><div class="key">Signal sources</div><div>${{renderTagList(provenance.signal_sources)}}</div></div>`);
      }}
      if (provenance.contributing_signal_ids) {{
        rows.push(`<div class="kv"><div class="key">Contributing signals</div><div>${{renderTagList(provenance.contributing_signal_ids, "fused")}}</div></div>`);
      }}
      if (provenance.supporting_signals) {{
        rows.push(`<div class="kv"><div class="key">Supporting signal count</div><div>${{provenance.supporting_signals.length}}</div></div>`);
      }}
      if (provenance.supporting_detections) {{
        rows.push(`<div class="kv"><div class="key">Supporting detection count</div><div>${{provenance.supporting_detections.length}}</div></div>`);
      }}
      if (provenance.score_breakdown && Object.keys(provenance.score_breakdown).length) {{
        rows.push(`<div class="kv"><div class="key">Score breakdown</div><div><pre>${{escapeHtml(JSON.stringify(provenance.score_breakdown, null, 2))}}</pre></div></div>`);
      }}
      if (provenance.missing_evidence) {{
        rows.push(`<div class="kv"><div class="key">Degraded state</div><div>Structured evidence is partial or missing for this item.</div></div>`);
      }}
      return rows.join("");
    }}

    function renderLifecycleBlock(item) {{
      const payload = currentLifecycle(item);
      if (!Object.keys(payload).length) {{
        return `<div class="kv"><div class="key">Lifecycle</div><div>No lifecycle row indexed for this item.</div></div>`;
      }}
      let transitions = [];
      try {{
        transitions = JSON.parse(payload.transitions_json || "[]") || [];
      }} catch (_error) {{
        transitions = [];
      }}
      const transitionRows = transitions.map((row) => `
        <tr>
          <td>${{escapeHtml(row.from_state || "none")}}</td>
          <td>${{escapeHtml(row.to_state || "n/a")}}</td>
          <td>${{escapeHtml(row.transition_source || "n/a")}}</td>
          <td>${{escapeHtml(row.created_at || "n/a")}}</td>
        </tr>
      `).join("");
      return `
        <div class="kv"><div class="key">Lifecycle state</div><div>${{escapeHtml(payload.lifecycle_state || "n/a")}}</div></div>
        <div class="kv"><div class="key">Candidate id</div><div>${{escapeHtml(payload.candidate_id || "n/a")}}</div></div>
        <div class="kv"><div class="key">Latest review</div><div>${{escapeHtml(payload.latest_review_status || "unreviewed")}}</div></div>
        <div class="kv"><div class="key">Selection basis</div><div>${{escapeHtml(payload.selection_basis || "n/a")}}</div></div>
        <div class="kv"><div class="key">Eligible for export/post</div><div>${{payload.lifecycle_state === "approved" || payload.lifecycle_state === "selected_for_export" || payload.lifecycle_state === "exported" ? "yes" : "no"}}</div></div>
        <div class="kv"><div class="key">Selection manifest</div><div>${{escapeHtml(payload.highlight_selection_manifest_path || "n/a")}}</div></div>
        <div class="kv"><div class="key">Export artifact</div><div>${{escapeHtml(payload.export_artifact_path || "n/a")}}</div></div>
        <div class="kv"><div class="key">Post ledger</div><div>${{escapeHtml(payload.post_ledger_path || "n/a")}}</div></div>
        <table>
          <thead><tr><th>From</th><th>To</th><th>Source</th><th>Created</th></tr></thead>
          <tbody>${{transitionRows || '<tr><td colspan="4">No transition history.</td></tr>'}}</tbody>
        </table>
      `;
    }}

    function renderHookBlock(item) {{
      const hook = currentHook(item);
      const lifecycle = currentLifecycle(item);
      if (!Object.keys(hook).length) {{
        if (!Object.keys(lifecycle).length) {{
          return `<div class="kv"><div class="key">Hook context</div><div>No hook candidate available because lifecycle context is not loaded for this fused event.</div></div>`;
        }}
        if (!["approved", "selected_for_export"].includes(lifecycle.lifecycle_state || "")) {{
          return `<div class="kv"><div class="key">Hook context</div><div>No hook candidate available because lifecycle state ${{escapeHtml(lifecycle.lifecycle_state || "unknown")}} is not eligible.</div></div>`;
        }}
        return `<div class="kv"><div class="key">Hook context</div><div>No hook candidate derived yet for this fused event.</div></div>`;
      }}
      return `
        <div class="kv"><div class="key">Hook archetype</div><div>${{escapeHtml(hook.hook_archetype || "n/a")}}</div></div>
        <div class="kv"><div class="key">Hook mode</div><div>${{escapeHtml(hook.hook_mode || "n/a")}}</div></div>
        <div class="kv"><div class="key">Hook strength</div><div>${{formatNumber(hook.hook_strength)}}</div></div>
        <div class="kv"><div class="key">Packaging strategy</div><div>${{escapeHtml(hook.packaging_strategy || "n/a")}}</div></div>
        <div class="kv"><div class="key">Rejection reason</div><div>${{escapeHtml(hook.rejection_reason || "n/a")}}</div></div>
        <table>
          <thead><tr><th>Dimension</th><th>Score</th></tr></thead>
          <tbody>
            <tr><td>Intensity</td><td>${{formatNumber(hook.intensity_score)}}</td></tr>
            <tr><td>Clarity</td><td>${{formatNumber(hook.clarity_score)}}</td></tr>
            <tr><td>Novelty</td><td>${{formatNumber(hook.novelty_score)}}</td></tr>
            <tr><td>Context sufficiency</td><td>${{formatNumber(hook.context_sufficiency_score)}}</td></tr>
            <tr><td>Payoff readability</td><td>${{formatNumber(hook.payoff_readability_score)}}</td></tr>
            <tr><td>Title/thumbnail potential</td><td>${{formatNumber(hook.title_thumbnail_potential_score)}}</td></tr>
            <tr><td>Authenticity risk</td><td>${{formatNumber(hook.authenticity_risk_score)}}</td></tr>
            <tr><td>Sound-off legibility</td><td>${{formatNumber(hook.sound_off_legibility_score)}}</td></tr>
          </tbody>
        </table>
      `;
    }}

    function renderRecommendationSummary(item) {{
      const fixtureEval = VIEWER_DATA.evaluation.fixture_comparison || {{}};
      const batchEval = VIEWER_DATA.evaluation.fixture_trial_batch || {{}};
      const proxyEval = VIEWER_DATA.evaluation.proxy || {{}};
      const runtimeEval = VIEWER_DATA.evaluation.runtime || {{}};
      let summary = {{}};
      if (item.kind === "proxy_window" && Object.keys(proxyEval).length) {{
        summary = {{
          role: fixtureEval.role || "current_artifact",
          currentScore: proxyEval.replay?.current_proxy_score ?? proxyEval.calibration?.proxy_score,
          trialScore: proxyEval.replay?.trial_proxy_score,
          currentAction: proxyEval.replay?.current_action,
          trialAction: proxyEval.replay?.trial_action,
          decision: proxyEval.replay_recommendation?.decision,
          reason: proxyEval.replay_recommendation?.reason,
          dataQualityNotes: proxyEval.replay_recommendation?.data_quality_notes,
          followUp: proxyEval.replay_recommendation?.follow_up,
        }};
      }} else if (item.kind === "runtime_event" && Object.keys(runtimeEval).length) {{
        summary = {{
          role: fixtureEval.role || "current_artifact",
          currentScore: runtimeEval.replay?.current_highlight_score ?? runtimeEval.calibration?.highlight_score,
          trialScore: runtimeEval.replay?.trial_highlight_score,
          currentAction: runtimeEval.replay?.current_action,
          trialAction: runtimeEval.replay?.trial_action,
          decision: runtimeEval.replay_recommendation?.decision,
          reason: runtimeEval.replay_recommendation?.reason,
          dataQualityNotes: runtimeEval.replay_recommendation?.data_quality_notes,
          followUp: runtimeEval.replay_recommendation?.follow_up,
        }};
      }} else if (Object.keys(fixtureEval).length) {{
        summary = {{
          role: fixtureEval.role || "current_artifact",
          currentScore: item.kind === "proxy_window" ? fixtureEval.row?.baseline_score : null,
          trialScore: item.kind === "proxy_window" ? fixtureEval.row?.trial_score : null,
          currentAction: fixtureEval.row?.baseline_action,
          trialAction: fixtureEval.row?.trial_action,
          decision: fixtureEval.recommendation?.decision || fixtureEval.row?.recommendation_signal,
          reason: fixtureEval.recommendation?.reason || fixtureEval.row?.recommendation_signal,
          dataQualityNotes: fixtureEval.recommendation?.data_quality_notes,
          followUp: fixtureEval.recommendation?.follow_up,
        }};
      }}
      const batchContext = Object.keys(batchEval).length ? `<div class="kv"><div class="key">Batch coverage</div><div>${{escapeHtml((batchEval.covered_trials || []).join(", ") || "n/a")}}</div></div>` : "";
      const noteBlock = Array.isArray(summary.dataQualityNotes) && summary.dataQualityNotes.length
        ? `<div class="kv"><div class="key">Data quality notes</div><div>${{escapeHtml(summary.dataQualityNotes.join(" | "))}}</div></div>`
        : "";
      const followUpBlock = summary.followUp
        ? `<div class="kv"><div class="key">Follow-up</div><div>${{escapeHtml(summary.followUp)}}</div></div>`
        : "";
      if (!Object.keys(summary).length) {{
        return `<div class="kv"><div class="key">Recommendation summary</div><div>No comparison or replay overlay loaded.</div></div>${{batchContext}}`;
      }}
      return `
        <div class="kv"><div class="key">Artifact role</div><div>${{escapeHtml(summary.role || "n/a")}}</div></div>
        <div class="kv"><div class="key">Current -> trial score</div><div>${{formatNumber(summary.currentScore)}} -> ${{formatNumber(summary.trialScore)}}</div></div>
        <div class="kv"><div class="key">Current -> trial action</div><div>${{escapeHtml(summary.currentAction || "n/a")}} -> ${{escapeHtml(summary.trialAction || "n/a")}}</div></div>
        <div class="kv"><div class="key">Recommendation decision</div><div>${{escapeHtml(summary.decision || "n/a")}}</div></div>
        <div class="kv"><div class="key">Recommendation reason</div><div>${{escapeHtml(summary.reason || "n/a")}}</div></div>
        ${{noteBlock}}
        ${{followUpBlock}}
        ${{batchContext}}
      `;
    }}

    function currentItem() {{
      return ITEM_LOOKUP[viewerState.selectedItemId] || null;
    }}

    function jumpToTimestamp(seconds) {{
      const media = document.getElementById("viewer-media");
      if (!media || seconds === null || seconds === undefined) return;
      media.currentTime = Number(seconds || 0);
      media.play().catch(() => undefined);
    }}

    function selectItem(itemId, options = {{ jump: false }}) {{
      viewerState.selectedItemId = itemId;
      renderSelection();
      if (options.jump) {{
        const item = currentItem();
        if (item) jumpToTimestamp(item.row.jump_timestamp ?? item.row.start_seconds ?? item.row.start_timestamp ?? 0);
      }}
    }}

    function renderSelection() {{
      const item = currentItem();
      if (!item) return;
      for (const card of document.querySelectorAll(".item-card")) {{
        card.classList.toggle("active", card.dataset.itemId === viewerState.selectedItemId);
      }}
      for (const bar of document.querySelectorAll(".timeline-bar")) {{
        bar.classList.toggle("active", bar.dataset.itemId === viewerState.selectedItemId);
      }}
      document.getElementById("selected-detail").innerHTML = renderSelectedDetail(item);
      document.getElementById("linked-evidence").innerHTML = renderLinkedEvidence(item);
      document.getElementById("evaluation-overlay").innerHTML = renderEvaluationOverlay(item);
    }}

    function renderSelectedDetail(item) {{
      const row = item.row;
      const provenanceBlock = renderProvenanceBlock(item);
      const disagreementBlock = renderDisagreementBlock(item);
      const recommendationBlock = renderRecommendationSummary(item);
      if (item.kind === "proxy_window") {{
        const hfDetails = VIEWER_DATA.proxy.hf_pipeline.window_details[row.window_id] || {{}};
        const reasonCodes = Array.isArray(hfDetails.rerank?.reason_codes) ? hfDetails.rerank.reason_codes.map((code) => `<span class="tag fused">${{escapeHtml(code)}}</span>`).join("") : "";
        const signals = (row.signals || []).map((signal) => `
          <tr>
            <td>${{escapeHtml(signal.stage || "other")}}</td>
            <td>${{escapeHtml(signal.source || "")}}</td>
            <td>${{formatNumber(signal.strength)}}</td>
            <td>${{formatNumber(signal.confidence)}}</td>
            <td>${{escapeHtml(signal.reason || "")}}</td>
          </tr>
        `).join("");
        return `
          <div class="kv"><div class="key">Kind</div><div>Proxy window</div></div>
          <div class="kv"><div class="key">Action</div><div>${{escapeHtml(row.recommended_action || "n/a")}}</div></div>
          <div class="kv"><div class="key">Window</div><div>${{formatNumber(row.start_seconds)}}s - ${{formatNumber(row.end_seconds)}}s</div></div>
          <div class="kv"><div class="key">Proxy score</div><div>${{formatNumber(row.proxy_score)}}</div></div>
          <div class="kv"><div class="key">Base / rerank</div><div>${{formatNumber(hfDetails.score_breakdown?.base_score)}} / ${{formatNumber(hfDetails.score_breakdown?.rerank_score)}}</div></div>
          <div>${{reasonCodes}}</div>
          <div class="button-row"><button type="button" data-jump-item="${{escapeHtml(row.window_id)}}">Jump To Window</button></div>
          <h3>Provenance</h3>
          ${{provenanceBlock}}
          <h3>Disagreements</h3>
          ${{disagreementBlock}}
          <h3>Recommendation Summary</h3>
          ${{recommendationBlock}}
          <table>
            <thead><tr><th>Stage</th><th>Source</th><th>Strength</th><th>Confidence</th><th>Reason</th></tr></thead>
            <tbody>${{signals || '<tr><td colspan="5">No proxy signal rows.</td></tr>'}}</tbody>
          </table>
        `;
      }}
      if (item.kind === "runtime_event") {{
        const linkedSignalRows = (currentProvenance(item).supporting_signals || []).map((signal) => `
          <tr><td>${{escapeHtml(signal.signal_id || "n/a")}}</td><td>${{escapeHtml(signal.label || "")}}</td><td>${{formatNumber(signal.score)}}</td></tr>
        `).join("");
        return `
          <div class="kv"><div class="key">Kind</div><div>Runtime event</div></div>
          <div class="kv"><div class="key">Event type</div><div>${{escapeHtml(row.label || "")}}</div></div>
          <div class="kv"><div class="key">Event id</div><div>${{escapeHtml(row.event_id || "n/a")}}</div></div>
          <div class="kv"><div class="key">Window</div><div>${{formatNumber(row.start_timestamp)}}s - ${{formatNumber(row.end_timestamp)}}s</div></div>
          <div class="kv"><div class="key">Confidence</div><div>${{formatNumber(row.score)}}</div></div>
          <div class="button-row"><button type="button" data-jump-item="${{escapeHtml(row.row_id)}}">Jump To Event</button></div>
          <h3>Provenance</h3>
          ${{provenanceBlock}}
          <h3>Disagreements</h3>
          ${{disagreementBlock}}
          <h3>Recommendation Summary</h3>
          ${{recommendationBlock}}
          <table>
            <thead><tr><th>Supporting signal</th><th>Label</th><th>Score</th></tr></thead>
            <tbody>${{linkedSignalRows || '<tr><td colspan="3">No supporting runtime signals.</td></tr>'}}</tbody>
          </table>
        `;
      }}
      const contributingRows = (currentProvenance(item).contributing_signals || []).map((signal) => `
        <tr><td>${{escapeHtml(signal.signal_id || "n/a")}}</td><td>${{escapeHtml(signal.producer_family || "")}}</td><td>${{escapeHtml(signal.label || "")}}</td><td>${{formatNumber(signal.score)}}</td></tr>
      `).join("");
      const lifecycleBlock = renderLifecycleBlock(item);
      const hookBlock = renderHookBlock(item);
      return `
        <div class="kv"><div class="key">Kind</div><div>Fused event</div></div>
        <div class="kv"><div class="key">Event type</div><div>${{escapeHtml(row.label || "")}}</div></div>
        <div class="kv"><div class="key">Score</div><div>${{formatNumber(row.score)}}</div></div>
        <div class="kv"><div class="key">Gate</div><div>${{escapeHtml(row.gate_status || "n/a")}}</div></div>
        <div class="kv"><div class="key">Review</div><div>${{escapeHtml(row.review?.review_status || "unreviewed")}}</div></div>
        <div class="button-row"><button type="button" data-jump-item="${{escapeHtml(row.row_id)}}">Jump To Event</button></div>
        <h3>Lifecycle</h3>
        ${{lifecycleBlock}}
        <h3>Hook Context</h3>
        ${{hookBlock}}
        <h3>Provenance</h3>
        ${{provenanceBlock}}
        <h3>Disagreements</h3>
        ${{disagreementBlock}}
        <h3>Recommendation Summary</h3>
        ${{recommendationBlock}}
        <table>
          <thead><tr><th>Signal id</th><th>Producer</th><th>Label</th><th>Score</th></tr></thead>
          <tbody>${{contributingRows || '<tr><td colspan="4">No contributing normalized signals.</td></tr>'}}</tbody>
        </table>
      `;
    }}

    function renderLinkedEvidence(item) {{
      const provenance = currentProvenance(item);
      const links = VIEWER_DATA.cross_links[item.row.window_id || item.row.row_id] || [];
      const provenanceBits = [];
      if (provenance.downstream_consumers?.length) {{
        provenanceBits.push(`<div class="kv"><div class="key">Downstream consumers</div><div>${{provenance.downstream_consumers.length}}</div></div>`);
      }}
      if (provenance.upstream_sources?.length) {{
        provenanceBits.push(`<div class="kv"><div class="key">Upstream sources</div><div>${{provenance.upstream_sources.length}}</div></div>`);
      }}
      if (!links.length) return provenanceBits.join("") || "<p>No linked evidence for this item.</p>";
      const rows = links.map((link) => `
        <tr>
          <td>${{escapeHtml(link.kind)}}</td>
          <td>${{escapeHtml(link.label || "")}}</td>
          <td><button type="button" data-select-item="${{escapeHtml(link.id)}}">Open</button></td>
        </tr>
      `).join("");
      return `${{provenanceBits.join("")}}<table><thead><tr><th>Kind</th><th>Label</th><th>Action</th></tr></thead><tbody>${{rows}}</tbody></table>`;
    }}

    function renderEvaluationOverlay(item) {{
      const fixtureEval = VIEWER_DATA.evaluation.fixture_comparison || {{}};
      const batchEval = VIEWER_DATA.evaluation.fixture_trial_batch || {{}};
      const proxyEval = VIEWER_DATA.evaluation.proxy || {{}};
      const runtimeEval = VIEWER_DATA.evaluation.runtime || {{}};
      if (item.kind === "proxy_window") {{
        return [renderFixtureComparison(fixtureEval), renderFixtureTrialBatch(batchEval), renderProxyEvaluation(proxyEval)].filter(Boolean).join("");
      }}
      if (item.kind === "runtime_event") {{
        return [renderFixtureComparison(fixtureEval), renderFixtureTrialBatch(batchEval), renderRuntimeEvaluation(runtimeEval)].filter(Boolean).join("");
      }}
      if (item.kind === "fused_event") {{
        return [renderFixtureComparison(fixtureEval), renderFixtureTrialBatch(batchEval)].filter(Boolean).join("") || "<p>No evaluation overlays loaded.</p>";
      }}
      const parts = [];
      if (Object.keys(fixtureEval).length) parts.push(renderFixtureComparison(fixtureEval));
      if (Object.keys(batchEval).length) parts.push(renderFixtureTrialBatch(batchEval));
      if (Object.keys(proxyEval).length) parts.push(renderProxyEvaluation(proxyEval));
      if (Object.keys(runtimeEval).length) parts.push(renderRuntimeEvaluation(runtimeEval));
      return parts.join("") || "<p>No evaluation overlays loaded.</p>";
    }}

    function renderFixtureComparison(payload) {{
      if (!payload || !Object.keys(payload).length) return "";
      const row = payload.row || {{}};
      const recommendation = payload.recommendation || {{}};
      const latencyDeltas = row.stage_latency_deltas || {{}};
      const dataQualityNotes = Array.isArray(recommendation.data_quality_notes) ? recommendation.data_quality_notes : [];
      return `
        <div class="kv"><div class="key">Fixture</div><div>${{escapeHtml(row.fixture_id || "n/a")}}</div></div>
        <div class="kv"><div class="key">Role</div><div>${{escapeHtml(payload.role || "n/a")}}</div></div>
        <div class="kv"><div class="key">Coverage</div><div>${{escapeHtml(row.coverage_status || "n/a")}}</div></div>
        <div class="kv"><div class="key">Review alignment</div><div>${{escapeHtml(row.review_status || "unreviewed")}}</div></div>
        <div class="kv"><div class="key">Current -> trial action</div><div>${{escapeHtml(row.baseline_action || "n/a")}} -> ${{escapeHtml(row.trial_action || "n/a")}}</div></div>
        <div class="kv"><div class="key">Score delta</div><div>${{formatNumber(row.score_delta)}}</div></div>
        <div class="kv"><div class="key">Shortlist / rerank</div><div>${{row.shortlist_changed ? "shortlist changed" : "shortlist stable"}}; ${{row.rerank_changed ? "rerank changed" : "rerank stable"}}</div></div>
        <div class="kv"><div class="key">Recommendation</div><div>${{escapeHtml(recommendation.decision || row.recommendation_signal || "n/a")}}${{recommendation.reason ? `: ${{escapeHtml(recommendation.reason)}}` : ""}}</div></div>
        <div class="kv"><div class="key">Follow-up</div><div>${{escapeHtml(recommendation.follow_up || "n/a")}}</div></div>
        <div class="kv"><div class="key">Data quality notes</div><div>${{escapeHtml(dataQualityNotes.join(" | ") || "n/a")}}</div></div>
        <div class="kv"><div class="key">Stage latency deltas</div><div><pre>${{escapeHtml(JSON.stringify(latencyDeltas, null, 2))}}</pre></div></div>
      `;
    }}

    function renderFixtureTrialBatch(payload) {{
      if (!payload || !Object.keys(payload).length) return "";
      const matches = payload.matches || [];
      const rows = matches.map((match) => `
        <tr>
          <td>${{escapeHtml(match.trial_name || "n/a")}}</td>
          <td>${{escapeHtml(match.artifact_layer || "n/a")}}</td>
          <td>${{escapeHtml(match.fixture_match?.row?.coverage_status || "n/a")}}</td>
          <td>${{escapeHtml(match.recommendation?.decision || match.fixture_match?.row?.recommendation_signal || "n/a")}}</td>
        </tr>
      `).join("");
      return `
        <div class="kv"><div class="key">Trial batch</div><div>${{escapeHtml(payload.batch_name || "n/a")}}</div></div>
        <div class="kv"><div class="key">Baseline trial</div><div>${{escapeHtml(payload.baseline_trial_name || "baseline")}}</div></div>
        <div class="kv"><div class="key">Overall recommendation</div><div>${{escapeHtml(payload.overall_recommendation?.decision || "n/a")}}</div></div>
        <div class="kv"><div class="key">Covered trials</div><div>${{escapeHtml((payload.covered_trials || []).join(", ") || "n/a")}}</div></div>
        <table><thead><tr><th>Trial</th><th>Layer</th><th>Coverage</th><th>Decision</th></tr></thead><tbody>${{rows}}</tbody></table>
      `;
    }}

    function renderProxyEvaluation(payload) {{
      if (!payload || !Object.keys(payload).length) return "<p>No proxy evaluation overlays loaded.</p>";
      const calibration = payload.calibration || {{}};
      const replay = payload.replay || {{}};
      const recommendation = payload.replay_recommendation || {{}};
      const dataQualityNotes = Array.isArray(recommendation.data_quality_notes) ? recommendation.data_quality_notes : [];
      const currentScoring = payload.current_scoring || {{}};
      const trialScoring = payload.trial_scoring || {{}};
      return `
        <div class="kv"><div class="key">Proxy calibration score</div><div>${{formatNumber(calibration.proxy_score)}}</div></div>
        <div class="kv"><div class="key">Proxy replay delta</div><div>${{formatNumber(replay.score_delta)}}</div></div>
        <div class="kv"><div class="key">Current -> trial action</div><div>${{escapeHtml(replay.current_action || "n/a")}} -> ${{escapeHtml(replay.trial_action || "n/a")}}</div></div>
        <div class="kv"><div class="key">Recommendation</div><div>${{escapeHtml(recommendation.decision || "n/a")}}${{recommendation.reason ? `: ${{escapeHtml(recommendation.reason)}}` : ""}}</div></div>
        <div class="kv"><div class="key">Follow-up</div><div>${{escapeHtml(recommendation.follow_up || "n/a")}}</div></div>
        <div class="kv"><div class="key">Data quality notes</div><div>${{escapeHtml(dataQualityNotes.join(" | ") || "n/a")}}</div></div>
        <div class="kv"><div class="key">Stage weights</div><div><pre>${{escapeHtml(JSON.stringify(currentScoring.hf_multimodal?.stage_weights || currentScoring.stage_weights || {{}}, null, 2))}}</pre></div></div>
        <div class="kv"><div class="key">Signal thresholds</div><div><pre>${{escapeHtml(JSON.stringify(currentScoring.hf_multimodal?.signal_thresholds || currentScoring.signal_thresholds || {{}}, null, 2))}}</pre></div></div>
        <div class="kv"><div class="key">Contribution deltas</div><div><pre>${{escapeHtml(JSON.stringify(replay, null, 2))}}</pre></div></div>
        <div class="kv"><div class="key">Trial scoring</div><div><pre>${{escapeHtml(JSON.stringify(trialScoring.hf_multimodal || trialScoring || {{}}, null, 2))}}</pre></div></div>
      `;
    }}

    function renderRuntimeEvaluation(payload) {{
      if (!payload || !Object.keys(payload).length) return "<p>No runtime evaluation overlays loaded.</p>";
      const calibration = payload.calibration || {{}};
      const replay = payload.replay || {{}};
      const recommendation = payload.replay_recommendation || {{}};
      const dataQualityNotes = Array.isArray(recommendation.data_quality_notes) ? recommendation.data_quality_notes : [];
      return `
        <div class="kv"><div class="key">Runtime calibration score</div><div>${{formatNumber(calibration.highlight_score)}}</div></div>
        <div class="kv"><div class="key">Runtime replay delta</div><div>${{formatNumber(replay.score_delta)}}</div></div>
        <div class="kv"><div class="key">Current -> trial action</div><div>${{escapeHtml(replay.current_action || "n/a")}} -> ${{escapeHtml(replay.trial_action || "n/a")}}</div></div>
        <div class="kv"><div class="key">Recommendation</div><div>${{escapeHtml(recommendation.decision || "n/a")}}${{recommendation.reason ? `: ${{escapeHtml(recommendation.reason)}}` : ""}}</div></div>
        <div class="kv"><div class="key">Follow-up</div><div>${{escapeHtml(recommendation.follow_up || "n/a")}}</div></div>
        <div class="kv"><div class="key">Data quality notes</div><div>${{escapeHtml(dataQualityNotes.join(" | ") || "n/a")}}</div></div>
        <div class="kv"><div class="key">Current scoring</div><div><pre>${{escapeHtml(JSON.stringify(payload.current_scoring || {{}}, null, 2))}}</pre></div></div>
        <div class="kv"><div class="key">Trial scoring</div><div><pre>${{escapeHtml(JSON.stringify(payload.trial_scoring || {{}}, null, 2))}}</pre></div></div>
        <div class="kv"><div class="key">Replay row</div><div><pre>${{escapeHtml(JSON.stringify(replay, null, 2))}}</pre></div></div>
      `;
    }}

    function applyFilters() {{
      const searchText = String(document.getElementById("viewer-search").value || "").toLowerCase().trim();
      const minScoreText = String(document.getElementById("viewer-min-score").value || "").trim();
      const minScore = minScoreText ? Number(minScoreText) : null;
      const enabledKinds = new Set(Array.from(document.querySelectorAll("[data-kind-toggle]")).filter((node) => node.checked).map((node) => node.getAttribute("data-kind-toggle")));
      for (const card of document.querySelectorAll(".item-card")) {{
        const item = ITEM_LOOKUP[card.dataset.itemId];
        if (!item) continue;
        const searchBlob = JSON.stringify(item.row).toLowerCase();
        const scoreValue = Number(item.row.proxy_score ?? item.row.score ?? 0);
        const matchesSearch = !searchText || searchBlob.includes(searchText);
        const matchesScore = minScore === null || scoreValue >= minScore;
        const matchesKind = enabledKinds.has(item.kind);
        card.style.display = matchesSearch && matchesScore && matchesKind ? "" : "none";
      }}
      for (const bar of document.querySelectorAll(".timeline-bar")) {{
        const item = ITEM_LOOKUP[bar.dataset.itemId];
        if (!item) continue;
        const searchBlob = JSON.stringify(item.row).toLowerCase();
        const scoreValue = Number(item.row.proxy_score ?? item.row.score ?? 0);
        const matchesSearch = !searchText || searchBlob.includes(searchText);
        const matchesScore = minScore === null || scoreValue >= minScore;
        const matchesKind = enabledKinds.has(item.kind);
        bar.style.display = matchesSearch && matchesScore && matchesKind ? "" : "none";
      }}
    }}

    document.addEventListener("click", (event) => {{
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const card = target.closest(".item-card");
      if (card) {{
        selectItem(card.dataset.itemId, {{ jump: false }});
        return;
      }}
      const jumpButton = target.closest("[data-jump-item]");
      if (jumpButton) {{
        selectItem(jumpButton.getAttribute("data-jump-item"), {{ jump: true }});
        return;
      }}
      const openButton = target.closest("[data-select-item]");
      if (openButton) {{
        selectItem(openButton.getAttribute("data-select-item"), {{ jump: false }});
      }}
    }});

    document.getElementById("viewer-search").addEventListener("input", applyFilters);
    document.getElementById("viewer-min-score").addEventListener("input", applyFilters);
    for (const toggle of document.querySelectorAll("[data-kind-toggle]")) {{
      toggle.addEventListener("change", applyFilters);
    }}
    renderSelection();
    applyFilters();
  </script>
</body>
</html>"""


def _render_review_summary(review: dict[str, Any]) -> str:
    tags = []
    if review.get("proxy_review_status"):
        tags.append(f'<span class="tag proxy">Proxy: {html.escape(str(review["proxy_review_status"]))}</span>')
    if review.get("runtime_review_status"):
        tags.append(f'<span class="tag runtime">Runtime: {html.escape(str(review["runtime_review_status"]))}</span>')
    if review.get("fused_review_status"):
        tags.append(f'<span class="tag fused">Fused: {html.escape(str(review["fused_review_status"]))}</span>')
    if not tags:
        tags.append('<span class="tag bad">No review metadata present</span>')
    note = f"<p>{html.escape(str(review['note']))}</p>" if review.get("note") else ""
    disagreement = '<p><strong>Layer disagreement detected.</strong></p>' if review.get("has_disagreement") else ""
    snapshot = ""
    if review.get("status_snapshot"):
        snapshot = f"<p><strong>Review status snapshot:</strong> {html.escape(', '.join(str(value) for value in review['status_snapshot']))}</p>"
    return "".join(tags) + disagreement + snapshot + note


def _render_timeline(rows: list[dict[str, Any]], selected_item_id: str | None) -> str:
    if not rows:
        return '<div class="timeline-bar proxy_window" style="left:0%;width:100%;">No timeline entries</div>'
    max_end = max(float(row.get("end_timestamp", row.get("start_timestamp", 0.0)) or 0.0) for row in rows) or 1.0
    bars = []
    for row in rows:
        start = float(row.get("start_timestamp", 0.0) or 0.0)
        end = float(row.get("end_timestamp", start) or start)
        left = max(0.0, min(100.0, (start / max_end) * 100.0))
        width = max(1.1, min(100.0 - left, ((end - start) / max_end) * 100.0))
        active = " active" if row["target_id"] == selected_item_id else ""
        bars.append(
            f'<div class="timeline-bar {html.escape(str(row["kind"]))}{active}" data-item-id="{html.escape(str(row["target_id"]))}" '
            f'style="left:{left:.3f}%;width:{width:.3f}%;" title="{html.escape(str(row["label"]))}"></div>'
        )
    return "".join(bars)


def _render_item_list(derived: dict[str, Any], selected_item_id: str | None) -> str:
    cards = []
    for row in derived["proxy"]["windows"]:
        active = " active" if row["window_id"] == selected_item_id else ""
        cards.append(
            f'<div class="item-card{active}" data-item-id="{html.escape(row["window_id"])}">'
            f'<h3><span class="tag proxy">Proxy</span> {html.escape(str(row["recommended_action"] or "window"))}</h3>'
            f'<div class="item-meta"><div>{_fmt_number(row["proxy_score"])} | {_fmt_number(row["start_seconds"])}s - {_fmt_number(row["end_seconds"])}s</div>'
            f'<div>{" ,".join(html.escape(item) for item in row.get("source_families", []))}</div></div></div>'
        )
    for row in derived["runtime"]["events"]:
        active = " active" if row["row_id"] == selected_item_id else ""
        cards.append(
            f'<div class="item-card{active}" data-item-id="{html.escape(row["row_id"])}">'
            f'<h3><span class="tag runtime">Runtime</span> {html.escape(str(row["label"] or "event"))}</h3>'
            f'<div class="item-meta"><div>{_fmt_number(row["score"])} | {_fmt_number(row["start_timestamp"])}s - {_fmt_number(row["end_timestamp"])}s</div>'
            f'<div>{html.escape(str(row.get("event_id") or ""))}</div></div></div>'
        )
    for row in derived["fused"]["events"]:
        active = " active" if row["row_id"] == selected_item_id else ""
        cards.append(
            f'<div class="item-card{active}" data-item-id="{html.escape(row["row_id"])}">'
            f'<h3><span class="tag fused">Fused</span> {html.escape(str(row["label"] or "event"))}</h3>'
            f'<div class="item-meta"><div>{_fmt_number(row["score"])} | {_fmt_number(row["start_timestamp"])}s - {_fmt_number(row["end_timestamp"])}s</div>'
            f'<div>{html.escape(str(row.get("gate_status") or ""))}</div></div></div>'
        )
    return "".join(cards) or '<div class="item-card"><h3>No inspectable items</h3><div class="item-meta">No proxy windows, runtime events, or fused events were available.</div></div>'


def _render_layer_status(derived: dict[str, Any]) -> str:
    proxy = derived["proxy"]
    runtime = derived["runtime"]
    fused = derived["fused"]
    lines = [
        f'<div class="kv"><div class="key">Proxy</div><div>{"available" if proxy["available"] else "missing"}</div></div>',
        f'<div class="kv"><div class="key">Runtime</div><div>{"available" if runtime["available"] else "missing"}</div></div>',
        f'<div class="kv"><div class="key">Fused</div><div>{"available" if fused["available"] else "missing"}</div></div>',
    ]
    if proxy["available"] and proxy["hf_pipeline"].get("available"):
        stage_rows = []
        for row in proxy["hf_pipeline"].get("stages", []):
            stage_rows.append(
                f"<tr><td>{html.escape(str(row.get('stage_name')))}</td><td>{html.escape(str(row.get('status')))}</td><td>{_fmt_number(row.get('duration_ms'))}</td></tr>"
            )
        lines.append(
            "<table><thead><tr><th>HF stage</th><th>Status</th><th>Duration ms</th></tr></thead><tbody>"
            + "".join(stage_rows)
            + "</tbody></table>"
        )
    return "".join(lines)


def _render_raw_sections(sections: list[dict[str, Any]]) -> str:
    blocks = []
    for section in sections:
        blocks.append(
            f'<details id="{html.escape(str(section["section_id"]))}"><summary>Show JSON - {html.escape(str(section["title"]))}</summary>'
            f'<pre>{html.escape(json.dumps(section["payload"], indent=2, sort_keys=True))}</pre></details>'
        )
    return "".join(blocks)


def _fmt_number(value: Any) -> str:
    if value is None or value == "":
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return html.escape(str(value))
    text = f"{number:.3f}"
    text = text.rstrip("0").rstrip(".") if "." in text else text
    return text
