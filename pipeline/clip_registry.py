from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.fused_export import DEFAULT_ACTION_THRESHOLDS


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REGISTRY_PATH = REPO_ROOT / "outputs" / "registry" / "clip_event_registry.sqlite"
RUNTIME_REVIEW_SESSION_SCHEMA_VERSION = "runtime_review_session_v1"
FUSED_REVIEW_SESSION_SCHEMA_VERSION = "fused_review_session_v1"
PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"
FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"
FIXTURE_SIDECAR_COMPARISON_SCHEMA_VERSION = "fixture_sidecar_comparison_v1"
FIXTURE_TRIAL_RUN_SCHEMA_VERSION = "fixture_trial_run_v1"
FIXTURE_TRIAL_BATCH_SCHEMA_VERSION = "fixture_trial_batch_v1"


def refresh_clip_registry(
    refresh_root: str | Path,
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    registry_path: str | Path | None = None,
) -> dict[str, Any]:
    root = _resolve_path(refresh_root)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "refresh_root": str(root),
            "error": "refresh root does not exist or is not a directory",
        }

    registry = _resolve_registry_path(registry_path)
    registry.parent.mkdir(parents=True, exist_ok=True)

    collected = _collect_registry_rows(root, game=game)
    result = _write_registry(registry, root, collected, game=game)

    warnings_path = None
    if debug_output_dir is not None:
        debug_root = _resolve_path(debug_output_dir)
        debug_root.mkdir(parents=True, exist_ok=True)
        warnings_path = debug_root / "ingest_warnings.json"
        warnings_path.write_text(json.dumps(collected["warnings"], indent=2), encoding="utf-8")
        result["warnings_path"] = str(warnings_path)

    if output_path is not None:
        output = _resolve_path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2), encoding="utf-8")

    return result


def query_clip_registry(
    *,
    mode: str = "fused-events",
    game: str | None = None,
    event_type: str | None = None,
    action: str | None = None,
    review_status: str | None = None,
    gate_status: str | None = None,
    fixture_id: str | None = None,
    trial_name: str | None = None,
    artifact_layer: str | None = None,
    recommendation_decision: str | None = None,
    coverage_status: str | None = None,
    has_disagreement: bool | None = None,
    limit: int | None = None,
    registry_path: str | Path | None = None,
) -> dict[str, Any]:
    registry = _resolve_registry_path(registry_path)
    if not registry.exists() or not registry.is_file():
        return {
            "ok": False,
            "registry_path": str(registry),
            "error": "registry database does not exist",
        }

    normalized_mode = str(mode or "fused-events").strip().lower()
    normalized_review = str(review_status).strip().lower() if review_status is not None else None
    if normalized_mode not in {
        "clips",
        "proxy-windows",
        "runtime-events",
        "fused-events",
        "review-items",
        "fixture-comparisons",
        "fixture-trials",
        "batch-comparisons",
    }:
        return {
            "ok": False,
            "registry_path": str(registry),
            "error": f"unsupported query mode: {mode}",
        }

    connection = sqlite3.connect(str(registry))
    connection.row_factory = sqlite3.Row
    try:
        rows = _query_rows(
            connection,
            mode=normalized_mode,
            game=game,
            event_type=event_type,
            action=action,
            review_status=normalized_review,
            gate_status=gate_status,
            fixture_id=fixture_id,
            trial_name=trial_name,
            artifact_layer=artifact_layer,
            recommendation_decision=recommendation_decision,
            coverage_status=coverage_status,
            has_disagreement=has_disagreement,
            limit=limit,
        )
    finally:
        connection.close()

    return {
        "ok": True,
        "registry_path": str(registry),
        "mode": normalized_mode,
        "row_count": len(rows),
        "rows": rows,
    }


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _resolve_registry_path(path: str | Path | None) -> Path:
    if path is None:
        return DEFAULT_REGISTRY_PATH
    return _resolve_path(path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _collect_registry_rows(root: Path, *, game: str | None) -> dict[str, Any]:
    rows: dict[str, Any] = {
        "clips": {},
        "proxy_windows": [],
        "runtime_analyses": [],
        "runtime_events": [],
        "runtime_detections": [],
        "fused_analyses": [],
        "fused_events": [],
        "fused_signal_refs": [],
        "runtime_review_sessions": [],
        "runtime_review_items": [],
        "fused_review_sessions": [],
        "fused_review_items": [],
        "fixture_comparisons": [],
        "fixture_trial_runs": [],
        "fixture_trial_run_fixtures": [],
        "fixture_trial_batches": [],
        "fixture_trial_batch_comparisons": [],
        "warnings": [],
        "counts": {
            "proxy_sidecar_count": 0,
            "runtime_sidecar_count": 0,
            "fused_sidecar_count": 0,
            "runtime_review_session_count": 0,
            "fused_review_session_count": 0,
            "fixture_comparison_report_count": 0,
            "fixture_trial_run_manifest_count": 0,
            "fixture_trial_batch_manifest_count": 0,
        },
    }

    for path in sorted(root.rglob("*.proxy_scan.json")):
        _ingest_proxy_sidecar(path, rows, game=game)
    for path in sorted(root.rglob("*.runtime_analysis.json")):
        _ingest_runtime_sidecar(path, rows, game=game)
    for path in sorted(root.rglob("*.fused_analysis.json")):
        _ingest_fused_sidecar(path, rows, game=game)
    for path in sorted(root.rglob("*.runtime_review_session.json")):
        _ingest_runtime_review_session(path, rows, game=game)
    for path in sorted(root.rglob("*.fused_review_session.json")):
        _ingest_fused_review_session(path, rows, game=game)
    for path in sorted(root.rglob("fixture_trial_run_manifest.json")):
        _ingest_fixture_trial_run_manifest(path, rows, game=game)
    for path in sorted(root.rglob("fixture_trial_batch_manifest.json")):
        _ingest_fixture_trial_batch_manifest(path, rows, game=game)
    for path in sorted(root.rglob("*.json")):
        if path.name in {
            "fixture_trial_run_manifest.json",
            "fixture_trial_batch_manifest.json",
        }:
            continue
        _ingest_fixture_comparison_report(path, rows, game=game)

    clip_rows = []
    for clip in rows["clips"].values():
        clip_rows.append(clip)
    rows["clips"] = clip_rows
    return rows


def _warning(rows: dict[str, Any], *, path: Path, reason: str, detail: str | None = None) -> None:
    payload = {"path": str(path.resolve()), "reason": reason}
    if detail:
        payload["detail"] = detail
    rows["warnings"].append(payload)


def _load_json(path: Path, rows: dict[str, Any]) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _warning(rows, path=path, reason="malformed_json", detail=str(exc))
        return None


def _clip_row(rows: dict[str, Any], *, game: str, source: str) -> dict[str, Any]:
    key = (game, source)
    clip = rows["clips"].get(key)
    if clip is None:
        clip = {
            "game": game,
            "source": source,
            "proxy_sidecar_path": None,
            "runtime_sidecar_path": None,
            "fused_sidecar_path": None,
            "has_proxy_sidecar": 0,
            "has_runtime_sidecar": 0,
            "has_fused_sidecar": 0,
            "proxy_review_status": None,
            "runtime_review_status": None,
            "fused_review_status": None,
            "fixture_ids_json": "[]",
            "top_proxy_action": None,
            "top_proxy_score": None,
            "top_fused_action": None,
            "top_fused_score": None,
            "has_review_disagreement": 0,
            "has_cross_layer_disagreement": 0,
            "has_trial_preference": 0,
            "last_seen_at": _utc_now(),
        }
        rows["clips"][key] = clip
    else:
        clip["last_seen_at"] = _utc_now()
    return clip


def _clip_add_fixture_id(clip: dict[str, Any], fixture_id: str) -> None:
    text = str(fixture_id or "").strip()
    if not text:
        return
    existing = set(_load_json_list(clip.get("fixture_ids_json")))
    if text in existing:
        return
    existing.add(text)
    clip["fixture_ids_json"] = json.dumps(sorted(existing))


def _update_clip_disagreement_flags(clip: dict[str, Any]) -> None:
    statuses = {
        str(clip.get("proxy_review_status") or "").strip().lower(),
        str(clip.get("runtime_review_status") or "").strip().lower(),
        str(clip.get("fused_review_status") or "").strip().lower(),
    }
    statuses.discard("")
    clip["has_review_disagreement"] = int(len(statuses) > 1)
    clip["has_cross_layer_disagreement"] = int(len(statuses) > 1)


def _load_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload if str(item).strip()]


def _load_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _ingest_proxy_sidecar(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != PROXY_SCAN_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if not payload.get("ok", False):
        _warning(rows, path=path, reason="failed_analysis")
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    source = str(payload.get("source") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    if source and payload_game:
        clip = _clip_row(rows, game=payload_game, source=source)
        clip["proxy_sidecar_path"] = resolved_path
        clip["has_proxy_sidecar"] = 1
        review = payload.get("proxy_review", {}) if isinstance(payload.get("proxy_review"), dict) else {}
        clip["proxy_review_status"] = _normalized_review_status(review.get("review_status"))
        top_window = payload.get("windows", [{}])[0] if isinstance(payload.get("windows"), list) and payload.get("windows") else {}
        clip["top_proxy_action"] = top_window.get("recommended_action")
        clip["top_proxy_score"] = top_window.get("proxy_score")
        _update_clip_disagreement_flags(clip)

    review = payload.get("proxy_review", {}) if isinstance(payload.get("proxy_review"), dict) else {}
    for index, window in enumerate(payload.get("windows", [])):
        rows["proxy_windows"].append(
            {
                "scan_id": payload.get("scan_id"),
                "window_index": index,
                "game": payload_game,
                "source": source,
                "sidecar_path": resolved_path,
                "start_seconds": window.get("start_seconds"),
                "end_seconds": window.get("end_seconds"),
                "proxy_score": window.get("proxy_score"),
                "signal_count": window.get("signal_count"),
                "recommended_action": window.get("recommended_action"),
                "sources_json": json.dumps(window.get("sources", []), sort_keys=True),
                "source_families_json": json.dumps(window.get("source_families", []), sort_keys=True),
                "review_status": _normalized_review_status(review.get("review_status")),
            }
        )
    rows["counts"]["proxy_sidecar_count"] += 1


def _ingest_runtime_sidecar(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != RUNTIME_ANALYSIS_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if not payload.get("ok", False):
        _warning(rows, path=path, reason="failed_analysis")
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    source = str(payload.get("source") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    analysis_id = str(payload.get("analysis_id") or "").strip()
    matcher = payload.get("matcher", {}) if isinstance(payload.get("matcher"), dict) else {}
    events = payload.get("events", {}) if isinstance(payload.get("events"), dict) else {}
    review = payload.get("runtime_review", {}) if isinstance(payload.get("runtime_review"), dict) else {}
    review_status = _normalized_review_status(review.get("review_status"))

    if source and payload_game:
        clip = _clip_row(rows, game=payload_game, source=source)
        clip["runtime_sidecar_path"] = resolved_path
        clip["has_runtime_sidecar"] = 1
        clip["runtime_review_status"] = review_status
        _update_clip_disagreement_flags(clip)

    rows["runtime_analyses"].append(
        {
            "analysis_id": analysis_id,
            "game": payload_game,
            "source": source,
            "sidecar_path": resolved_path,
            "status": payload.get("status"),
            "frame_count": matcher.get("frame_count"),
            "confirmed_detection_count": len(list(matcher.get("confirmed_detections", []))),
            "event_count": events.get("event_count", len(list(events.get("rows", [])))),
            "runtime_review_status": review_status,
            "runtime_review_session_id": review.get("session_id"),
            "runtime_recommended_action": review.get("recommended_action"),
            "runtime_highlight_score": review.get("highlight_score"),
            "last_ingested_mtime": path.stat().st_mtime,
        }
    )

    for index, event_row in enumerate(events.get("rows", [])):
        rows["runtime_events"].append(
            {
                "analysis_id": analysis_id,
                "event_index": index,
                "event_id": event_row.get("event_id"),
                "game": payload_game,
                "source": source,
                "sidecar_path": resolved_path,
                "event_type": event_row.get("event_type"),
                "confidence": event_row.get("confidence"),
                "start_timestamp": event_row.get("start_timestamp"),
                "end_timestamp": event_row.get("end_timestamp"),
                "entity_id": event_row.get("entity_id"),
                "ability_id": event_row.get("ability_id"),
                "equipment_id": event_row.get("equipment_id"),
                "event_row_id": event_row.get("event_row_id"),
                "review_status": review_status,
                "recommended_action": review.get("recommended_action"),
            }
        )

    for index, detection_row in enumerate(matcher.get("confirmed_detections", [])):
        rows["runtime_detections"].append(
            {
                "analysis_id": analysis_id,
                "detection_index": index,
                "game": payload_game,
                "source": source,
                "sidecar_path": resolved_path,
                "asset_id": detection_row.get("asset_id"),
                "roi_ref": detection_row.get("roi_ref"),
                "entity_id": detection_row.get("entity_id"),
                "ability_id": detection_row.get("ability_id"),
                "equipment_id": detection_row.get("equipment_id"),
                "first_timestamp": detection_row.get("first_timestamp"),
                "last_timestamp": detection_row.get("last_timestamp"),
                "peak_score": detection_row.get("peak_score"),
            }
        )
    rows["counts"]["runtime_sidecar_count"] += 1


def _ingest_fused_sidecar(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != FUSED_ANALYSIS_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if not payload.get("ok", False):
        _warning(rows, path=path, reason="failed_analysis")
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    source = str(payload.get("source") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    fusion_id = str(payload.get("fusion_id") or "").strip()
    fused_review = payload.get("fused_review", {}) if isinstance(payload.get("fused_review"), dict) else {}
    review_events = fused_review.get("events", {}) if isinstance(fused_review.get("events"), dict) else {}

    if source and payload_game:
        clip = _clip_row(rows, game=payload_game, source=source)
        clip["fused_sidecar_path"] = resolved_path
        clip["has_fused_sidecar"] = 1
        clip["fused_review_status"] = _top_fused_review_status(review_events)
        top_event = payload.get("fused_events", [{}])[0] if isinstance(payload.get("fused_events"), list) and payload.get("fused_events") else {}
        clip["top_fused_action"] = _recommended_fused_action(float(top_event.get("final_score", top_event.get("confidence", 0.0)) or 0.0)) if top_event else None
        clip["top_fused_score"] = top_event.get("final_score", top_event.get("confidence")) if top_event else None
        _update_clip_disagreement_flags(clip)

    rows["fused_analyses"].append(
        {
            "fusion_id": fusion_id,
            "game": payload_game,
            "source": source,
            "sidecar_path": resolved_path,
            "status": payload.get("status"),
            "normalized_signal_count": len(list(payload.get("normalized_signals", []))),
            "fused_event_count": len(list(payload.get("fused_events", []))),
            "fused_reviewed_event_count": fused_review.get("reviewed_event_count"),
            "fused_review_session_id": fused_review.get("session_id"),
            "fused_review_status": _top_fused_review_status(review_events),
            "last_ingested_mtime": path.stat().st_mtime,
        }
    )

    signal_by_id = {
        str(signal_row.get("signal_id")): signal_row
        for signal_row in payload.get("normalized_signals", [])
        if str(signal_row.get("signal_id") or "").strip()
    }

    for index, event_row in enumerate(payload.get("fused_events", [])):
        event_id = str(event_row.get("event_id") or "").strip()
        metadata = event_row.get("metadata", {}) if isinstance(event_row.get("metadata"), dict) else {}
        event_review = review_events.get(event_id, {}) if isinstance(review_events.get(event_id), dict) else {}
        final_score = float(event_row.get("final_score", event_row.get("confidence", 0.0)) or 0.0)
        recommended_action = _recommended_fused_action(final_score)
        rows["fused_events"].append(
            {
                "fusion_id": fusion_id,
                "event_index": index,
                "event_id": event_id,
                "game": payload_game,
                "source": source,
                "sidecar_path": resolved_path,
                "event_type": event_row.get("event_type"),
                "confidence": event_row.get("confidence"),
                "final_score": final_score,
                "gate_status": event_row.get("gate_status"),
                "synergy_applied": bool(event_row.get("synergy_applied", False)),
                "synergy_multiplier": event_row.get("synergy_multiplier"),
                "minimum_required_signals_met": event_row.get("minimum_required_signals_met"),
                "suggested_start_timestamp": event_row.get("suggested_start_timestamp"),
                "suggested_end_timestamp": event_row.get("suggested_end_timestamp"),
                "entity_id": metadata.get("entity_id"),
                "ability_id": metadata.get("ability_id"),
                "equipment_id": metadata.get("equipment_id"),
                "event_row_id": metadata.get("event_row_id"),
                "review_status": _normalized_review_status(event_review.get("review_status")),
                "recommended_action": recommended_action,
            }
        )
        for signal_index, signal_id in enumerate(event_row.get("contributing_signals", [])):
            signal_key = str(signal_id)
            signal_row = signal_by_id.get(signal_key, {})
            rows["fused_signal_refs"].append(
                {
                    "fusion_id": fusion_id,
                    "event_id": event_id,
                    "signal_index": signal_index,
                    "signal_id": signal_key,
                    "signal_type": signal_row.get("signal_type"),
                    "producer_family": signal_row.get("producer_family"),
                    "source_family": signal_row.get("source_family"),
                    "asset_id": signal_row.get("asset_id"),
                    "roi_ref": signal_row.get("roi_ref"),
                }
            )
    rows["counts"]["fused_sidecar_count"] += 1


def _top_fused_review_status(review_events: dict[str, Any]) -> str | None:
    statuses = {_normalized_review_status(row.get("review_status")) for row in review_events.values() if isinstance(row, dict)}
    statuses.discard(None)
    if "approved" in statuses:
        return "approved"
    if "rejected" in statuses:
        return "rejected"
    if "unreviewed" in statuses:
        return "unreviewed"
    return None


def _recommended_fused_action(final_score: float) -> str:
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["highlight_candidate"]):
        return "highlight_candidate"
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["inspect"]):
        return "inspect"
    return "skip"


def _ingest_runtime_review_session(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != RUNTIME_REVIEW_SESSION_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    session_id = str(payload.get("session_id") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    rows["runtime_review_sessions"].append(
        {
            "session_id": session_id,
            "game": payload_game,
            "manifest_path": resolved_path,
            "selection_source": payload.get("selection_source"),
            "selection_action_filter": payload.get("selection_action_filter"),
            "created_at": payload.get("created_at"),
            "applied_at": payload.get("applied_at"),
            "cleanup_at": payload.get("cleanup_at"),
            "item_count": payload.get("item_count"),
            "approved_count": payload.get("approved_count"),
            "rejected_count": payload.get("rejected_count"),
            "unreviewed_count": payload.get("unreviewed_count"),
        }
    )

    for index, item in enumerate(payload.get("items", [])):
        sidecar_path = _safe_resolve(item.get("sidecar_path"))
        if sidecar_path is not None and not sidecar_path.exists():
            _warning(rows, path=path, reason="missing_runtime_review_sidecar", detail=str(sidecar_path))
        rows["runtime_review_items"].append(
            {
                "session_id": session_id,
                "item_index": index,
                "game": payload_game,
                "sidecar_path": str(sidecar_path) if sidecar_path is not None else str(item.get("sidecar_path") or ""),
                "source": item.get("source"),
                "analysis_id": item.get("analysis_id"),
                "review_status": _normalized_review_status(item.get("review_status")),
                "apply_status": item.get("apply_status"),
                "highlight_score": item.get("highlight_score"),
                "recommended_action": item.get("recommended_action"),
                "gpt_meta_path": item.get("gpt_meta_path"),
                "gpt_processed_path": item.get("gpt_processed_path"),
                "gpt_final_path": item.get("gpt_final_path"),
            }
        )
    rows["counts"]["runtime_review_session_count"] += 1


def _ingest_fused_review_session(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != FUSED_REVIEW_SESSION_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    session_id = str(payload.get("session_id") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    rows["fused_review_sessions"].append(
        {
            "session_id": session_id,
            "game": payload_game,
            "manifest_path": resolved_path,
            "selection_source": payload.get("selection_source"),
            "selection_action_filter": payload.get("selection_action_filter"),
            "selection_event_type_filter": payload.get("selection_event_type_filter"),
            "created_at": payload.get("created_at"),
            "applied_at": payload.get("applied_at"),
            "cleanup_at": payload.get("cleanup_at"),
            "item_count": payload.get("item_count"),
            "approved_count": payload.get("approved_count"),
            "rejected_count": payload.get("rejected_count"),
            "unreviewed_count": payload.get("unreviewed_count"),
        }
    )

    for index, item in enumerate(payload.get("items", [])):
        sidecar_path = _safe_resolve(item.get("sidecar_path"))
        if sidecar_path is not None and not sidecar_path.exists():
            _warning(rows, path=path, reason="missing_fused_review_sidecar", detail=str(sidecar_path))
        rows["fused_review_items"].append(
            {
                "session_id": session_id,
                "item_index": index,
                "game": payload_game,
                "sidecar_path": str(sidecar_path) if sidecar_path is not None else str(item.get("sidecar_path") or ""),
                "source": item.get("source"),
                "fusion_id": item.get("fusion_id"),
                "event_id": item.get("event_id"),
                "event_type": item.get("event_type"),
                "review_status": _normalized_review_status(item.get("review_status")),
                "apply_status": item.get("apply_status"),
                "final_score": item.get("final_score"),
                "recommended_action": item.get("recommended_action"),
                "gate_status": item.get("gate_status"),
                "gpt_meta_path": item.get("gpt_meta_path"),
                "gpt_processed_path": item.get("gpt_processed_path"),
                "gpt_final_path": item.get("gpt_final_path"),
            }
        )
    rows["counts"]["fused_review_session_count"] += 1


def _safe_resolve(value: Any) -> Path | None:
    if not value:
        return None
    return _resolve_path(str(value))


def _normalized_review_status(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"accepted", "approved"}:
        return "approved"
    if text in {"rejected", "reject"}:
        return "rejected"
    if text in {"unreviewed", "pending"}:
        return "unreviewed"
    return text


def _load_json_silent(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _ingest_fixture_comparison_report(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json_silent(path)
    if not isinstance(payload, dict):
        return
    fixture_rows = list(payload.get("comparison", {}).get("fixture_rows", [])) if isinstance(payload.get("comparison"), dict) else []
    if not fixture_rows:
        return
    if payload.get("schema_version") not in {None, "", FIXTURE_SIDECAR_COMPARISON_SCHEMA_VERSION}:
        return
    comparison_path = str(path.resolve())
    recommendation = payload.get("recommendation", {}) if isinstance(payload.get("recommendation"), dict) else {}
    for index, row in enumerate(fixture_rows):
        if not isinstance(row, dict):
            continue
        row_game = str(row.get("game", "")).strip()
        if game is not None and row_game and row_game != game:
            continue
        artifact_layer = str(row.get("artifact_layer", "")).strip()
        fixture_id = str(row.get("fixture_id", "")).strip()
        baseline_sidecar_path = str(row.get("baseline_sidecar_path") or "").strip() or None
        trial_sidecar_path = str(row.get("trial_sidecar_path") or "").strip() or None
        rows["fixture_comparisons"].append(
            {
                "comparison_path": comparison_path,
                "row_index": index,
                "fixture_id": fixture_id,
                "label": row.get("label"),
                "artifact_layer": artifact_layer,
                "game": row_game or None,
                "source": row.get("source"),
                "coverage_status": row.get("coverage_status"),
                "review_status": row.get("review_status"),
                "baseline_sidecar_path": baseline_sidecar_path,
                "trial_sidecar_path": trial_sidecar_path,
                "baseline_action": row.get("baseline_action"),
                "trial_action": row.get("trial_action"),
                "baseline_score": row.get("baseline_score"),
                "trial_score": row.get("trial_score"),
                "score_delta": row.get("score_delta"),
                "shortlist_changed": int(bool(row.get("shortlist_changed", False))),
                "rerank_changed": int(bool(row.get("rerank_changed", False))),
                "stage_latency_deltas_json": json.dumps(row.get("stage_latency_deltas", {}), sort_keys=True),
                "recommendation_signal": row.get("recommendation_signal"),
                "recommendation_decision": recommendation.get("decision") or row.get("recommendation_signal"),
                "recommendation_reason": recommendation.get("reason"),
            }
        )
        _attach_comparison_to_clips(rows, fixture_id=fixture_id, baseline_sidecar_path=baseline_sidecar_path, trial_sidecar_path=trial_sidecar_path, recommendation_decision=str(recommendation.get("decision") or row.get("recommendation_signal") or "").strip())
    rows["counts"]["fixture_comparison_report_count"] += 1


def _attach_comparison_to_clips(
    rows: dict[str, Any],
    *,
    fixture_id: str,
    baseline_sidecar_path: str | None,
    trial_sidecar_path: str | None,
    recommendation_decision: str,
) -> None:
    normalized_baseline = str(Path(baseline_sidecar_path).resolve()) if baseline_sidecar_path else None
    normalized_trial = str(Path(trial_sidecar_path).resolve()) if trial_sidecar_path else None
    for clip in rows["clips"].values():
        sidecar_paths = {
            str(clip.get("proxy_sidecar_path") or ""),
            str(clip.get("runtime_sidecar_path") or ""),
            str(clip.get("fused_sidecar_path") or ""),
        }
        if normalized_baseline and normalized_baseline in sidecar_paths:
            _clip_add_fixture_id(clip, fixture_id)
            if recommendation_decision in {"prefer_trial", "trial_better"}:
                clip["has_trial_preference"] = 1
        if normalized_trial and normalized_trial in sidecar_paths:
            _clip_add_fixture_id(clip, fixture_id)
            if recommendation_decision in {"prefer_trial", "trial_better"}:
                clip["has_trial_preference"] = 1
        _update_clip_disagreement_flags(clip)


def _ingest_fixture_trial_run_manifest(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != FIXTURE_TRIAL_RUN_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    trial_name = str(payload.get("trial_name") or "").strip()
    rows["fixture_trial_runs"].append(
        {
            "trial_name": trial_name,
            "trial_root": payload.get("trial_root"),
            "manifest_path": str(path.resolve()),
            "proxy_sidecar_root": payload.get("proxy_sidecar_root"),
            "runtime_sidecar_root": payload.get("runtime_sidecar_root"),
            "fused_sidecar_root": payload.get("fused_sidecar_root"),
            "fixture_manifest_path": payload.get("fixture_manifest_path"),
            "fixture_source_manifest_path": payload.get("fixture_source_manifest_path"),
            "status": payload.get("status"),
            "completed_fixture_count": payload.get("completed_fixture_count"),
            "failed_fixture_count": payload.get("failed_fixture_count"),
            "effective_overrides_json": json.dumps(payload.get("effective_overrides", {}), sort_keys=True),
        }
    )
    for index, fixture in enumerate(payload.get("fixtures", [])):
        if not isinstance(fixture, dict):
            continue
        fixture_game = str(fixture.get("game", "")).strip()
        if game is not None and fixture_game and fixture_game != game:
            continue
        layers = fixture.get("layers", {}) if isinstance(fixture.get("layers"), dict) else {}
        rows["fixture_trial_run_fixtures"].append(
            {
                "trial_name": trial_name,
                "fixture_index": index,
                "fixture_id": fixture.get("fixture_id"),
                "game": fixture_game or None,
                "source_path": fixture.get("source_path"),
                "status": fixture.get("status"),
                "failure_reason": fixture.get("failure_reason") or fixture.get("error"),
                "proxy_sidecar_path": (layers.get("proxy") or {}).get("sidecar_path"),
                "runtime_sidecar_path": (layers.get("runtime") or {}).get("sidecar_path"),
                "fused_sidecar_path": (layers.get("fused") or {}).get("sidecar_path"),
            }
        )
        for clip in rows["clips"].values():
            if str(clip.get("source") or "") == str(fixture.get("source_path") or "") and (not fixture_game or str(clip.get("game") or "") == fixture_game):
                _clip_add_fixture_id(clip, str(fixture.get("fixture_id") or ""))
    rows["counts"]["fixture_trial_run_manifest_count"] += 1


def _ingest_fixture_trial_batch_manifest(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != FIXTURE_TRIAL_BATCH_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    batch_name = str(payload.get("batch_name") or "").strip()
    rows["fixture_trial_batches"].append(
        {
            "batch_name": batch_name,
            "manifest_path": str(path.resolve()),
            "baseline_trial_name": payload.get("baseline_trial_name"),
            "overall_recommendation_decision": (payload.get("overall_recommendation") or {}).get("decision"),
            "overall_recommendation_trial_name": (payload.get("overall_recommendation") or {}).get("trial_name"),
            "selected_trials_json": json.dumps(payload.get("selected_trials", []), sort_keys=True),
        }
    )
    for index, comparison in enumerate(payload.get("trial_comparisons", [])):
        if not isinstance(comparison, dict):
            continue
        rows["fixture_trial_batch_comparisons"].append(
            {
                "batch_name": batch_name,
                "comparison_index": index,
                "trial_name": comparison.get("trial_name"),
                "comparison_report_path": comparison.get("comparison_report_path"),
                "artifact_layer": comparison.get("artifact_layer"),
                "comparison_status": comparison.get("comparison_status"),
                "recommendation_decision": (comparison.get("recommendation") or {}).get("decision"),
            }
        )
    rows["counts"]["fixture_trial_batch_manifest_count"] += 1


def _write_registry(registry_path: Path, root: Path, collected: dict[str, Any], *, game: str | None) -> dict[str, Any]:
    connection = sqlite3.connect(str(registry_path))
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        _create_schema(connection)
        with connection:
            _clear_mirror_tables(connection)
            run_id = _insert_ingest_run(connection, registry_path, root, collected, game=game)
            _bulk_insert(connection, "clips", collected["clips"], _CLIP_COLUMNS)
            _bulk_insert(connection, "proxy_windows", collected["proxy_windows"], _PROXY_WINDOW_COLUMNS)
            _bulk_insert(connection, "runtime_analyses", collected["runtime_analyses"], _RUNTIME_ANALYSIS_COLUMNS)
            _bulk_insert(connection, "runtime_events", collected["runtime_events"], _RUNTIME_EVENT_COLUMNS)
            _bulk_insert(connection, "runtime_detections", collected["runtime_detections"], _RUNTIME_DETECTION_COLUMNS)
            _bulk_insert(connection, "fused_analyses", collected["fused_analyses"], _FUSED_ANALYSIS_COLUMNS)
            _bulk_insert(connection, "fused_events", collected["fused_events"], _FUSED_EVENT_COLUMNS)
            _bulk_insert(connection, "fused_signal_refs", collected["fused_signal_refs"], _FUSED_SIGNAL_REF_COLUMNS)
            _bulk_insert(connection, "runtime_review_sessions", collected["runtime_review_sessions"], _RUNTIME_REVIEW_SESSION_COLUMNS)
            _bulk_insert(connection, "runtime_review_items", collected["runtime_review_items"], _RUNTIME_REVIEW_ITEM_COLUMNS)
            _bulk_insert(connection, "fused_review_sessions", collected["fused_review_sessions"], _FUSED_REVIEW_SESSION_COLUMNS)
            _bulk_insert(connection, "fused_review_items", collected["fused_review_items"], _FUSED_REVIEW_ITEM_COLUMNS)
            _bulk_insert(connection, "fixture_comparisons", collected["fixture_comparisons"], _FIXTURE_COMPARISON_COLUMNS)
            _bulk_insert(connection, "fixture_trial_runs", collected["fixture_trial_runs"], _FIXTURE_TRIAL_RUN_COLUMNS)
            _bulk_insert(connection, "fixture_trial_run_fixtures", collected["fixture_trial_run_fixtures"], _FIXTURE_TRIAL_RUN_FIXTURE_COLUMNS)
            _bulk_insert(connection, "fixture_trial_batches", collected["fixture_trial_batches"], _FIXTURE_TRIAL_BATCH_COLUMNS)
            _bulk_insert(connection, "fixture_trial_batch_comparisons", collected["fixture_trial_batch_comparisons"], _FIXTURE_TRIAL_BATCH_COMPARISON_COLUMNS)
            _insert_ingest_warnings(connection, run_id, collected["warnings"])
    finally:
        connection.close()

    counts = collected["counts"]
    return {
        "ok": True,
        "registry_path": str(registry_path),
        "refresh_root": str(root),
        "game_filter": game,
        "proxy_sidecar_count": counts["proxy_sidecar_count"],
        "runtime_sidecar_count": counts["runtime_sidecar_count"],
        "fused_sidecar_count": counts["fused_sidecar_count"],
        "runtime_review_session_count": counts["runtime_review_session_count"],
        "fused_review_session_count": counts["fused_review_session_count"],
        "fixture_comparison_report_count": counts["fixture_comparison_report_count"],
        "fixture_trial_run_manifest_count": counts["fixture_trial_run_manifest_count"],
        "fixture_trial_batch_manifest_count": counts["fixture_trial_batch_manifest_count"],
        "clip_row_count": len(collected["clips"]),
        "proxy_window_row_count": len(collected["proxy_windows"]),
        "runtime_analysis_row_count": len(collected["runtime_analyses"]),
        "runtime_event_row_count": len(collected["runtime_events"]),
        "runtime_detection_row_count": len(collected["runtime_detections"]),
        "fused_analysis_row_count": len(collected["fused_analyses"]),
        "fused_event_row_count": len(collected["fused_events"]),
        "fused_signal_ref_row_count": len(collected["fused_signal_refs"]),
        "runtime_review_item_row_count": len(collected["runtime_review_items"]),
        "fused_review_item_row_count": len(collected["fused_review_items"]),
        "fixture_comparison_row_count": len(collected["fixture_comparisons"]),
        "fixture_trial_run_row_count": len(collected["fixture_trial_runs"]),
        "fixture_trial_run_fixture_row_count": len(collected["fixture_trial_run_fixtures"]),
        "fixture_trial_batch_row_count": len(collected["fixture_trial_batches"]),
        "fixture_trial_batch_comparison_row_count": len(collected["fixture_trial_batch_comparisons"]),
        "warning_count": len(collected["warnings"]),
    }


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS clips (
            game TEXT NOT NULL,
            source TEXT NOT NULL,
            proxy_sidecar_path TEXT,
            runtime_sidecar_path TEXT,
            fused_sidecar_path TEXT,
            has_proxy_sidecar INTEGER DEFAULT 0,
            has_runtime_sidecar INTEGER DEFAULT 0,
            has_fused_sidecar INTEGER DEFAULT 0,
            proxy_review_status TEXT,
            runtime_review_status TEXT,
            fused_review_status TEXT,
            fixture_ids_json TEXT,
            top_proxy_action TEXT,
            top_proxy_score REAL,
            top_fused_action TEXT,
            top_fused_score REAL,
            has_review_disagreement INTEGER DEFAULT 0,
            has_cross_layer_disagreement INTEGER DEFAULT 0,
            has_trial_preference INTEGER DEFAULT 0,
            last_seen_at TEXT,
            PRIMARY KEY (game, source)
        );
        CREATE TABLE IF NOT EXISTS proxy_windows (
            scan_id TEXT,
            window_index INTEGER NOT NULL,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            start_seconds REAL,
            end_seconds REAL,
            proxy_score REAL,
            signal_count INTEGER,
            recommended_action TEXT,
            sources_json TEXT,
            source_families_json TEXT,
            review_status TEXT,
            PRIMARY KEY (scan_id, window_index)
        );
        CREATE TABLE IF NOT EXISTS runtime_analyses (
            analysis_id TEXT PRIMARY KEY,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            status TEXT,
            frame_count INTEGER,
            confirmed_detection_count INTEGER,
            event_count INTEGER,
            runtime_review_status TEXT,
            runtime_review_session_id TEXT,
            runtime_recommended_action TEXT,
            runtime_highlight_score REAL,
            last_ingested_mtime REAL
        );
        CREATE TABLE IF NOT EXISTS runtime_events (
            analysis_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            event_id TEXT,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            event_type TEXT,
            confidence REAL,
            start_timestamp REAL,
            end_timestamp REAL,
            entity_id TEXT,
            ability_id TEXT,
            equipment_id TEXT,
            event_row_id TEXT,
            review_status TEXT,
            recommended_action TEXT,
            PRIMARY KEY (analysis_id, event_index)
        );
        CREATE TABLE IF NOT EXISTS runtime_detections (
            analysis_id TEXT NOT NULL,
            detection_index INTEGER NOT NULL,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            asset_id TEXT,
            roi_ref TEXT,
            entity_id TEXT,
            ability_id TEXT,
            equipment_id TEXT,
            first_timestamp REAL,
            last_timestamp REAL,
            peak_score REAL,
            PRIMARY KEY (analysis_id, detection_index)
        );
        CREATE TABLE IF NOT EXISTS fused_analyses (
            fusion_id TEXT PRIMARY KEY,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            status TEXT,
            normalized_signal_count INTEGER,
            fused_event_count INTEGER,
            fused_reviewed_event_count INTEGER,
            fused_review_session_id TEXT,
            fused_review_status TEXT,
            last_ingested_mtime REAL
        );
        CREATE TABLE IF NOT EXISTS fused_events (
            fusion_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            event_id TEXT,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            event_type TEXT,
            confidence REAL,
            final_score REAL,
            gate_status TEXT,
            synergy_applied INTEGER,
            synergy_multiplier REAL,
            minimum_required_signals_met INTEGER,
            suggested_start_timestamp REAL,
            suggested_end_timestamp REAL,
            entity_id TEXT,
            ability_id TEXT,
            equipment_id TEXT,
            event_row_id TEXT,
            review_status TEXT,
            recommended_action TEXT,
            PRIMARY KEY (fusion_id, event_index)
        );
        CREATE TABLE IF NOT EXISTS fused_signal_refs (
            fusion_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            signal_index INTEGER NOT NULL,
            signal_id TEXT,
            signal_type TEXT,
            producer_family TEXT,
            source_family TEXT,
            asset_id TEXT,
            roi_ref TEXT,
            PRIMARY KEY (fusion_id, event_id, signal_index)
        );
        CREATE TABLE IF NOT EXISTS runtime_review_sessions (
            session_id TEXT PRIMARY KEY,
            game TEXT,
            manifest_path TEXT,
            selection_source TEXT,
            selection_action_filter TEXT,
            created_at TEXT,
            applied_at TEXT,
            cleanup_at TEXT,
            item_count INTEGER,
            approved_count INTEGER,
            rejected_count INTEGER,
            unreviewed_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS runtime_review_items (
            session_id TEXT NOT NULL,
            item_index INTEGER NOT NULL,
            game TEXT,
            sidecar_path TEXT,
            source TEXT,
            analysis_id TEXT,
            review_status TEXT,
            apply_status TEXT,
            highlight_score REAL,
            recommended_action TEXT,
            gpt_meta_path TEXT,
            gpt_processed_path TEXT,
            gpt_final_path TEXT,
            PRIMARY KEY (session_id, item_index)
        );
        CREATE TABLE IF NOT EXISTS fused_review_sessions (
            session_id TEXT PRIMARY KEY,
            game TEXT,
            manifest_path TEXT,
            selection_source TEXT,
            selection_action_filter TEXT,
            selection_event_type_filter TEXT,
            created_at TEXT,
            applied_at TEXT,
            cleanup_at TEXT,
            item_count INTEGER,
            approved_count INTEGER,
            rejected_count INTEGER,
            unreviewed_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS fused_review_items (
            session_id TEXT NOT NULL,
            item_index INTEGER NOT NULL,
            game TEXT,
            sidecar_path TEXT,
            source TEXT,
            fusion_id TEXT,
            event_id TEXT,
            event_type TEXT,
            review_status TEXT,
            apply_status TEXT,
            final_score REAL,
            recommended_action TEXT,
            gate_status TEXT,
            gpt_meta_path TEXT,
            gpt_processed_path TEXT,
            gpt_final_path TEXT,
            PRIMARY KEY (session_id, item_index)
        );
        CREATE TABLE IF NOT EXISTS fixture_comparisons (
            comparison_path TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            fixture_id TEXT,
            label TEXT,
            artifact_layer TEXT,
            game TEXT,
            source TEXT,
            coverage_status TEXT,
            review_status TEXT,
            baseline_sidecar_path TEXT,
            trial_sidecar_path TEXT,
            baseline_action TEXT,
            trial_action TEXT,
            baseline_score REAL,
            trial_score REAL,
            score_delta REAL,
            shortlist_changed INTEGER,
            rerank_changed INTEGER,
            stage_latency_deltas_json TEXT,
            recommendation_signal TEXT,
            recommendation_decision TEXT,
            recommendation_reason TEXT,
            PRIMARY KEY (comparison_path, row_index)
        );
        CREATE TABLE IF NOT EXISTS fixture_trial_runs (
            trial_name TEXT PRIMARY KEY,
            trial_root TEXT,
            manifest_path TEXT,
            proxy_sidecar_root TEXT,
            runtime_sidecar_root TEXT,
            fused_sidecar_root TEXT,
            fixture_manifest_path TEXT,
            fixture_source_manifest_path TEXT,
            status TEXT,
            completed_fixture_count INTEGER,
            failed_fixture_count INTEGER,
            effective_overrides_json TEXT
        );
        CREATE TABLE IF NOT EXISTS fixture_trial_run_fixtures (
            trial_name TEXT NOT NULL,
            fixture_index INTEGER NOT NULL,
            fixture_id TEXT,
            game TEXT,
            source_path TEXT,
            status TEXT,
            failure_reason TEXT,
            proxy_sidecar_path TEXT,
            runtime_sidecar_path TEXT,
            fused_sidecar_path TEXT,
            PRIMARY KEY (trial_name, fixture_index)
        );
        CREATE TABLE IF NOT EXISTS fixture_trial_batches (
            batch_name TEXT PRIMARY KEY,
            manifest_path TEXT,
            baseline_trial_name TEXT,
            overall_recommendation_decision TEXT,
            overall_recommendation_trial_name TEXT,
            selected_trials_json TEXT
        );
        CREATE TABLE IF NOT EXISTS fixture_trial_batch_comparisons (
            batch_name TEXT NOT NULL,
            comparison_index INTEGER NOT NULL,
            trial_name TEXT,
            comparison_report_path TEXT,
            artifact_layer TEXT,
            comparison_status TEXT,
            recommendation_decision TEXT,
            PRIMARY KEY (batch_name, comparison_index)
        );
        CREATE TABLE IF NOT EXISTS ingest_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            registry_path TEXT,
            refresh_root TEXT,
            game_filter TEXT,
            warning_count INTEGER,
            proxy_sidecar_count INTEGER,
            runtime_sidecar_count INTEGER,
            fused_sidecar_count INTEGER,
            runtime_review_session_count INTEGER,
            fused_review_session_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS ingest_warnings (
            run_id INTEGER NOT NULL,
            warning_index INTEGER NOT NULL,
            path TEXT,
            reason TEXT,
            detail TEXT,
            PRIMARY KEY (run_id, warning_index)
        );
        """
    )
    _ensure_columns(
        connection,
        "clips",
        (
            ("has_proxy_sidecar", "INTEGER DEFAULT 0"),
            ("has_runtime_sidecar", "INTEGER DEFAULT 0"),
            ("has_fused_sidecar", "INTEGER DEFAULT 0"),
            ("fixture_ids_json", "TEXT"),
            ("top_proxy_action", "TEXT"),
            ("top_proxy_score", "REAL"),
            ("top_fused_action", "TEXT"),
            ("top_fused_score", "REAL"),
            ("has_review_disagreement", "INTEGER DEFAULT 0"),
            ("has_cross_layer_disagreement", "INTEGER DEFAULT 0"),
            ("has_trial_preference", "INTEGER DEFAULT 0"),
        ),
    )


def _ensure_columns(connection: sqlite3.Connection, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    existing = {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column_name, column_sql in columns:
        if column_name in existing:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def _clear_mirror_tables(connection: sqlite3.Connection) -> None:
    for table_name in (
        "clips",
        "proxy_windows",
        "runtime_analyses",
        "runtime_events",
        "runtime_detections",
        "fused_analyses",
        "fused_events",
        "fused_signal_refs",
        "runtime_review_sessions",
        "runtime_review_items",
        "fused_review_sessions",
        "fused_review_items",
        "fixture_comparisons",
        "fixture_trial_runs",
        "fixture_trial_run_fixtures",
        "fixture_trial_batches",
        "fixture_trial_batch_comparisons",
    ):
        connection.execute(f"DELETE FROM {table_name}")


def _insert_ingest_run(
    connection: sqlite3.Connection,
    registry_path: Path,
    refresh_root: Path,
    collected: dict[str, Any],
    *,
    game: str | None,
) -> int:
    counts = collected["counts"]
    cursor = connection.execute(
        """
        INSERT INTO ingest_runs (
            created_at,
            registry_path,
            refresh_root,
            game_filter,
            warning_count,
            proxy_sidecar_count,
            runtime_sidecar_count,
            fused_sidecar_count,
            runtime_review_session_count,
            fused_review_session_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _utc_now(),
            str(registry_path),
            str(refresh_root),
            game,
            len(collected["warnings"]),
            counts["proxy_sidecar_count"],
            counts["runtime_sidecar_count"],
            counts["fused_sidecar_count"],
            counts["runtime_review_session_count"],
            counts["fused_review_session_count"],
        ),
    )
    return int(cursor.lastrowid)


def _insert_ingest_warnings(connection: sqlite3.Connection, run_id: int, warnings: list[dict[str, Any]]) -> None:
    for index, warning in enumerate(warnings):
        connection.execute(
            "INSERT INTO ingest_warnings (run_id, warning_index, path, reason, detail) VALUES (?, ?, ?, ?, ?)",
            (run_id, index, warning.get("path"), warning.get("reason"), warning.get("detail")),
        )


def _bulk_insert(connection: sqlite3.Connection, table_name: str, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in columns)
    column_sql = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"
    payloads = []
    for row in rows:
        payloads.append(tuple(row.get(column) for column in columns))
    connection.executemany(sql, payloads)


def _query_rows(
    connection: sqlite3.Connection,
    *,
    mode: str,
    game: str | None,
    event_type: str | None,
    action: str | None,
    review_status: str | None,
    gate_status: str | None,
    fixture_id: str | None,
    trial_name: str | None,
    artifact_layer: str | None,
    recommendation_decision: str | None,
    coverage_status: str | None,
    has_disagreement: bool | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    if mode == "clips":
        sql = """
            SELECT game, source, proxy_sidecar_path, runtime_sidecar_path, fused_sidecar_path,
                   has_proxy_sidecar, has_runtime_sidecar, has_fused_sidecar,
                   proxy_review_status, runtime_review_status, fused_review_status,
                   fixture_ids_json, top_proxy_action, top_proxy_score, top_fused_action, top_fused_score,
                   has_review_disagreement, has_cross_layer_disagreement, has_trial_preference, last_seen_at
            FROM clips
        """
        where = []
        params: list[Any] = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("fixture_ids_json LIKE ?")
            params.append(f'%"{fixture_id}"%')
        if has_disagreement is not None:
            where.append("has_review_disagreement = ?")
            params.append(int(bool(has_disagreement)))
        rows = _execute_query(connection, sql, where, params, order_by="game ASC, source ASC", limit=limit)
        return rows

    if mode == "proxy-windows":
        sql = """
            SELECT scan_id, window_index, game, source, sidecar_path, start_seconds, end_seconds,
                   proxy_score, signal_count, recommended_action, sources_json, source_families_json, review_status
            FROM proxy_windows
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if action is not None:
            where.append("recommended_action = ?")
            params.append(action)
        if review_status is not None:
            where.append("review_status = ?")
            params.append(review_status)
        return _execute_query(connection, sql, where, params, order_by="COALESCE(proxy_score, 0.0) DESC, source ASC", limit=limit)

    if mode == "runtime-events":
        sql = """
            SELECT e.game, e.source, e.analysis_id, e.event_id, e.event_type, e.confidence,
                   e.review_status, e.recommended_action, e.sidecar_path
            FROM runtime_events e
        """
        where = []
        params = []
        if game is not None:
            where.append("e.game = ?")
            params.append(game)
        if event_type is not None:
            where.append("e.event_type = ?")
            params.append(event_type)
        if action is not None:
            where.append("e.recommended_action = ?")
            params.append(action)
        if review_status is not None:
            where.append("e.review_status = ?")
            params.append(review_status)
        return _execute_query(connection, sql, where, params, order_by="COALESCE(e.confidence, 0.0) DESC, e.source ASC", limit=limit)

    if mode == "review-items":
        sql = """
            SELECT review_type, game, session_id, item_index, sidecar_path, source, analysis_id, fusion_id,
                   event_id, event_type, review_status, apply_status, recommended_action
            FROM (
                SELECT 'runtime' AS review_type, game, session_id, item_index, sidecar_path, source,
                       analysis_id, NULL AS fusion_id, NULL AS event_id, NULL AS event_type,
                       review_status, apply_status, recommended_action
                FROM runtime_review_items
                UNION ALL
                SELECT 'fused' AS review_type, game, session_id, item_index, sidecar_path, source,
                       NULL AS analysis_id, fusion_id, event_id, event_type,
                       review_status, apply_status, recommended_action
                FROM fused_review_items
            )
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if event_type is not None:
            where.append("(event_type = ? OR event_type IS NULL)")
            params.append(event_type)
        if action is not None:
            where.append("recommended_action = ?")
            params.append(action)
        if review_status is not None:
            where.append("review_status = ?")
            params.append(review_status)
        return _execute_query(connection, sql, where, params, order_by="game ASC, session_id ASC, item_index ASC", limit=limit)

    if mode == "fixture-comparisons":
        sql = """
            SELECT comparison_path, row_index, fixture_id, label, artifact_layer, game, source,
                   coverage_status, review_status, baseline_sidecar_path, trial_sidecar_path,
                   baseline_action, trial_action, baseline_score, trial_score, score_delta,
                   shortlist_changed, rerank_changed, stage_latency_deltas_json,
                   recommendation_signal, recommendation_decision, recommendation_reason
            FROM fixture_comparisons
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("fixture_id = ?")
            params.append(fixture_id)
        if artifact_layer is not None:
            where.append("artifact_layer = ?")
            params.append(artifact_layer)
        if review_status is not None:
            where.append("review_status = ?")
            params.append(review_status)
        if recommendation_decision is not None:
            where.append("recommendation_decision = ?")
            params.append(recommendation_decision)
        if coverage_status is not None:
            where.append("coverage_status = ?")
            params.append(coverage_status)
        return _execute_query(connection, sql, where, params, order_by="fixture_id ASC, artifact_layer ASC, row_index ASC", limit=limit)

    if mode == "fixture-trials":
        sql = """
            SELECT f.trial_name, r.status AS run_status, r.manifest_path, r.trial_root, r.proxy_sidecar_root,
                   r.runtime_sidecar_root, r.fused_sidecar_root, f.fixture_id, f.game, f.source_path,
                   f.status, f.failure_reason, f.proxy_sidecar_path, f.runtime_sidecar_path, f.fused_sidecar_path
            FROM fixture_trial_run_fixtures f
            JOIN fixture_trial_runs r ON r.trial_name = f.trial_name
        """
        where = []
        params = []
        if game is not None:
            where.append("f.game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("f.fixture_id = ?")
            params.append(fixture_id)
        if trial_name is not None:
            where.append("f.trial_name = ?")
            params.append(trial_name)
        return _execute_query(connection, sql, where, params, order_by="f.trial_name ASC, f.fixture_id ASC", limit=limit)

    if mode == "batch-comparisons":
        sql = """
            SELECT c.batch_name, b.manifest_path, b.baseline_trial_name, b.overall_recommendation_decision,
                   b.overall_recommendation_trial_name, c.trial_name, c.comparison_report_path,
                   c.artifact_layer, c.comparison_status, c.recommendation_decision
            FROM fixture_trial_batch_comparisons c
            JOIN fixture_trial_batches b ON b.batch_name = c.batch_name
        """
        where = []
        params = []
        if trial_name is not None:
            where.append("c.trial_name = ?")
            params.append(trial_name)
        if artifact_layer is not None:
            where.append("c.artifact_layer = ?")
            params.append(artifact_layer)
        if recommendation_decision is not None:
            where.append("c.recommendation_decision = ?")
            params.append(recommendation_decision)
        return _execute_query(connection, sql, where, params, order_by="c.batch_name ASC, c.trial_name ASC", limit=limit)

    sql = """
        SELECT e.game, e.source, e.fusion_id, e.event_id, e.event_type, e.final_score, e.gate_status,
               e.synergy_applied, e.suggested_start_timestamp, e.suggested_end_timestamp,
               e.review_status, e.sidecar_path
        FROM fused_events e
    """
    where = []
    params = []
    if game is not None:
        where.append("e.game = ?")
        params.append(game)
    if event_type is not None:
        where.append("e.event_type = ?")
        params.append(event_type)
    if action is not None:
        where.append("e.recommended_action = ?")
        params.append(action)
    if review_status is not None:
        where.append("e.review_status = ?")
        params.append(review_status)
    if gate_status is not None:
        where.append("e.gate_status = ?")
        params.append(gate_status)
    if fixture_id is not None:
        where.append(
            "EXISTS (SELECT 1 FROM clips c WHERE c.game = e.game AND c.source = e.source AND c.fixture_ids_json LIKE ?)"
        )
        params.append(f'%"{fixture_id}"%')
    if has_disagreement is not None:
        where.append(
            "EXISTS (SELECT 1 FROM clips c WHERE c.game = e.game AND c.source = e.source AND c.has_review_disagreement = ?)"
        )
        params.append(int(bool(has_disagreement)))
    return _execute_query(connection, sql, where, params, order_by="COALESCE(e.final_score, 0.0) DESC, e.source ASC", limit=limit)


def _execute_query(
    connection: sqlite3.Connection,
    sql: str,
    where: list[str],
    params: list[Any],
    *,
    order_by: str,
    limit: int | None,
) -> list[dict[str, Any]]:
    text = sql
    if where:
        text += " WHERE " + " AND ".join(where)
    text += f" ORDER BY {order_by}"
    if limit is not None:
        text += " LIMIT ?"
        params = [*params, int(limit)]
    cursor = connection.execute(text, params)
    return [dict(row) for row in cursor.fetchall()]


_CLIP_COLUMNS = (
    "game",
    "source",
    "proxy_sidecar_path",
    "runtime_sidecar_path",
    "fused_sidecar_path",
    "has_proxy_sidecar",
    "has_runtime_sidecar",
    "has_fused_sidecar",
    "proxy_review_status",
    "runtime_review_status",
    "fused_review_status",
    "fixture_ids_json",
    "top_proxy_action",
    "top_proxy_score",
    "top_fused_action",
    "top_fused_score",
    "has_review_disagreement",
    "has_cross_layer_disagreement",
    "has_trial_preference",
    "last_seen_at",
)
_PROXY_WINDOW_COLUMNS = (
    "scan_id",
    "window_index",
    "game",
    "source",
    "sidecar_path",
    "start_seconds",
    "end_seconds",
    "proxy_score",
    "signal_count",
    "recommended_action",
    "sources_json",
    "source_families_json",
    "review_status",
)
_RUNTIME_ANALYSIS_COLUMNS = (
    "analysis_id",
    "game",
    "source",
    "sidecar_path",
    "status",
    "frame_count",
    "confirmed_detection_count",
    "event_count",
    "runtime_review_status",
    "runtime_review_session_id",
    "runtime_recommended_action",
    "runtime_highlight_score",
    "last_ingested_mtime",
)
_RUNTIME_EVENT_COLUMNS = (
    "analysis_id",
    "event_index",
    "event_id",
    "game",
    "source",
    "sidecar_path",
    "event_type",
    "confidence",
    "start_timestamp",
    "end_timestamp",
    "entity_id",
    "ability_id",
    "equipment_id",
    "event_row_id",
    "review_status",
    "recommended_action",
)
_RUNTIME_DETECTION_COLUMNS = (
    "analysis_id",
    "detection_index",
    "game",
    "source",
    "sidecar_path",
    "asset_id",
    "roi_ref",
    "entity_id",
    "ability_id",
    "equipment_id",
    "first_timestamp",
    "last_timestamp",
    "peak_score",
)
_FUSED_ANALYSIS_COLUMNS = (
    "fusion_id",
    "game",
    "source",
    "sidecar_path",
    "status",
    "normalized_signal_count",
    "fused_event_count",
    "fused_reviewed_event_count",
    "fused_review_session_id",
    "fused_review_status",
    "last_ingested_mtime",
)
_FUSED_EVENT_COLUMNS = (
    "fusion_id",
    "event_index",
    "event_id",
    "game",
    "source",
    "sidecar_path",
    "event_type",
    "confidence",
    "final_score",
    "gate_status",
    "synergy_applied",
    "synergy_multiplier",
    "minimum_required_signals_met",
    "suggested_start_timestamp",
    "suggested_end_timestamp",
    "entity_id",
    "ability_id",
    "equipment_id",
    "event_row_id",
    "review_status",
    "recommended_action",
)
_FUSED_SIGNAL_REF_COLUMNS = (
    "fusion_id",
    "event_id",
    "signal_index",
    "signal_id",
    "signal_type",
    "producer_family",
    "source_family",
    "asset_id",
    "roi_ref",
)
_RUNTIME_REVIEW_SESSION_COLUMNS = (
    "session_id",
    "game",
    "manifest_path",
    "selection_source",
    "selection_action_filter",
    "created_at",
    "applied_at",
    "cleanup_at",
    "item_count",
    "approved_count",
    "rejected_count",
    "unreviewed_count",
)
_RUNTIME_REVIEW_ITEM_COLUMNS = (
    "session_id",
    "item_index",
    "game",
    "sidecar_path",
    "source",
    "analysis_id",
    "review_status",
    "apply_status",
    "highlight_score",
    "recommended_action",
    "gpt_meta_path",
    "gpt_processed_path",
    "gpt_final_path",
)
_FUSED_REVIEW_SESSION_COLUMNS = (
    "session_id",
    "game",
    "manifest_path",
    "selection_source",
    "selection_action_filter",
    "selection_event_type_filter",
    "created_at",
    "applied_at",
    "cleanup_at",
    "item_count",
    "approved_count",
    "rejected_count",
    "unreviewed_count",
)
_FUSED_REVIEW_ITEM_COLUMNS = (
    "session_id",
    "item_index",
    "game",
    "sidecar_path",
    "source",
    "fusion_id",
    "event_id",
    "event_type",
    "review_status",
    "apply_status",
    "final_score",
    "recommended_action",
    "gate_status",
    "gpt_meta_path",
    "gpt_processed_path",
    "gpt_final_path",
)
_FIXTURE_COMPARISON_COLUMNS = (
    "comparison_path",
    "row_index",
    "fixture_id",
    "label",
    "artifact_layer",
    "game",
    "source",
    "coverage_status",
    "review_status",
    "baseline_sidecar_path",
    "trial_sidecar_path",
    "baseline_action",
    "trial_action",
    "baseline_score",
    "trial_score",
    "score_delta",
    "shortlist_changed",
    "rerank_changed",
    "stage_latency_deltas_json",
    "recommendation_signal",
    "recommendation_decision",
    "recommendation_reason",
)
_FIXTURE_TRIAL_RUN_COLUMNS = (
    "trial_name",
    "trial_root",
    "manifest_path",
    "proxy_sidecar_root",
    "runtime_sidecar_root",
    "fused_sidecar_root",
    "fixture_manifest_path",
    "fixture_source_manifest_path",
    "status",
    "completed_fixture_count",
    "failed_fixture_count",
    "effective_overrides_json",
)
_FIXTURE_TRIAL_RUN_FIXTURE_COLUMNS = (
    "trial_name",
    "fixture_index",
    "fixture_id",
    "game",
    "source_path",
    "status",
    "failure_reason",
    "proxy_sidecar_path",
    "runtime_sidecar_path",
    "fused_sidecar_path",
)
_FIXTURE_TRIAL_BATCH_COLUMNS = (
    "batch_name",
    "manifest_path",
    "baseline_trial_name",
    "overall_recommendation_decision",
    "overall_recommendation_trial_name",
    "selected_trials_json",
)
_FIXTURE_TRIAL_BATCH_COMPARISON_COLUMNS = (
    "batch_name",
    "comparison_index",
    "trial_name",
    "comparison_report_path",
    "artifact_layer",
    "comparison_status",
    "recommendation_decision",
)
