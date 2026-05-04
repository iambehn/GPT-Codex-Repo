from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import median
from typing import Any, Callable

from pipeline.fusion_analysis import load_fusion_rules
from pipeline.event_mapper import EventMapperError, load_runtime_rule_trial_overrides
from pipeline.roi_matcher import RoiMatcherError, load_template_trial_overrides
from pipeline.runtime_ontology import load_runtime_signal_event_ontology
from pipeline.simple_yaml import load_yaml_file


DEFAULT_TIMESTAMP_TOLERANCE_SECONDS = 0.5
DEFAULT_BOUNDARY_TOLERANCE_SECONDS = 1.0
DEFAULT_OUTPUT_NAME = "fusion_goldset_validation_report.json"
SUPPORTED_GOLDSET_SCHEMA_VERSION = "fusion_goldset_clip_v1"


ClipRunner = Callable[..., dict[str, Any]]


def validate_fusion_goldset(
    goldset_root: str | Path,
    *,
    clip_runner: ClipRunner,
    game: str | None = None,
    media_root: str | Path | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    proxy_sidecar_root: str | Path | None = None,
    runtime_sidecar_root: str | Path | None = None,
    fused_sidecar_root: str | Path | None = None,
) -> dict[str, Any]:
    root = _resolve_path(goldset_root)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "status": "invalid_goldset_root",
            "goldset_root": str(root),
            "error": "goldset root does not exist or is not a directory",
        }
    resolved_media_root = _validate_media_root(media_root)
    if isinstance(resolved_media_root, dict):
        return resolved_media_root

    report = _build_validation_report(
        root,
        clip_runner=clip_runner,
        game=game,
        media_root=resolved_media_root,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=proxy_sidecar_root,
        runtime_sidecar_root=runtime_sidecar_root,
        fused_sidecar_root=fused_sidecar_root,
    )
    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if debug_output_dir is not None:
        _write_validation_debug_bundle(_resolve_path(debug_output_dir), report)
    return report


def replay_fusion_rules(
    goldset_root: str | Path,
    trial_rules_path: str | Path,
    *,
    clip_runner: ClipRunner,
    game: str | None = None,
    media_root: str | Path | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    proxy_sidecar_root: str | Path | None = None,
    runtime_sidecar_root: str | Path | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    root = _resolve_path(goldset_root)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "status": "invalid_goldset_root",
            "goldset_root": str(root),
            "error": "goldset root does not exist or is not a directory",
        }
    resolved_media_root = _validate_media_root(media_root)
    if isinstance(resolved_media_root, dict):
        return resolved_media_root

    trial_path = _resolve_path(trial_rules_path)
    if not trial_path.exists() or not trial_path.is_file():
        return {
            "ok": False,
            "status": "invalid_trial_rules",
            "goldset_root": str(root),
            "trial_rules_path": str(trial_path),
            "error": "trial rules path does not exist or is not a file",
        }

    trial_payload = _load_trial_rules_payload(trial_path)
    if isinstance(trial_payload, dict) and "error" in trial_payload:
        return {
            "ok": False,
            "status": "invalid_trial_rules",
            "goldset_root": str(root),
            "trial_rules_path": str(trial_path),
            "error": str(trial_payload["error"]),
        }

    effective_trial_name = str(trial_name or trial_payload.get("trial_name") or trial_payload.get("name") or trial_path.stem)

    current_report = _build_validation_report(
        root,
        clip_runner=clip_runner,
        game=game,
        media_root=resolved_media_root,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=proxy_sidecar_root,
        runtime_sidecar_root=runtime_sidecar_root,
        fused_sidecar_root=None,
    )
    trial_report = _build_validation_report(
        root,
        clip_runner=clip_runner,
        game=game,
        media_root=resolved_media_root,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=proxy_sidecar_root,
        runtime_sidecar_root=runtime_sidecar_root,
        fused_sidecar_root=None,
        trial_rules_path=trial_path,
    )

    result = _build_replay_report(root, current_report, trial_report, effective_trial_name)
    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if debug_output_dir is not None:
        _write_replay_debug_bundle(_resolve_path(debug_output_dir), result)
    return result


def replay_template_thresholds(
    goldset_root: str | Path,
    trial_templates_path: str | Path,
    *,
    clip_runner: ClipRunner,
    game: str | None = None,
    media_root: str | Path | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    root = _resolve_path(goldset_root)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "status": "invalid_goldset_root",
            "goldset_root": str(root),
            "error": "goldset root does not exist or is not a directory",
        }
    resolved_media_root = _validate_media_root(media_root)
    if isinstance(resolved_media_root, dict):
        return resolved_media_root

    trial_path = _resolve_path(trial_templates_path)
    if not trial_path.exists() or not trial_path.is_file():
        return {
            "ok": False,
            "status": "invalid_trial_templates",
            "goldset_root": str(root),
            "trial_templates_path": str(trial_path),
            "error": "trial template path does not exist or is not a file",
        }
    try:
        load_template_trial_overrides(trial_path)
    except RoiMatcherError as exc:
        return {
            "ok": False,
            "status": exc.status,
            "goldset_root": str(root),
            "trial_templates_path": str(trial_path),
            "error": exc.message,
        }

    effective_trial_name = str(trial_name or trial_path.stem)
    current_report = _build_validation_report(
        root,
        clip_runner=clip_runner,
        game=game,
        media_root=resolved_media_root,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=None,
        runtime_sidecar_root=None,
        fused_sidecar_root=None,
    )
    trial_report = _build_validation_report(
        root,
        clip_runner=clip_runner,
        game=game,
        media_root=resolved_media_root,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=None,
        runtime_sidecar_root=None,
        fused_sidecar_root=None,
        trial_template_overrides_path=trial_path,
    )
    result = _build_template_replay_report(root, current_report, trial_report, effective_trial_name, trial_path)
    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if debug_output_dir is not None:
        _write_template_replay_debug_bundle(_resolve_path(debug_output_dir), result)
    return result


def replay_runtime_event_rules(
    goldset_root: str | Path,
    trial_runtime_rules_path: str | Path,
    *,
    clip_runner: ClipRunner,
    game: str | None = None,
    media_root: str | Path | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    root = _resolve_path(goldset_root)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "status": "invalid_goldset_root",
            "goldset_root": str(root),
            "error": "goldset root does not exist or is not a directory",
        }
    resolved_media_root = _validate_media_root(media_root)
    if isinstance(resolved_media_root, dict):
        return resolved_media_root

    trial_path = _resolve_path(trial_runtime_rules_path)
    if not trial_path.exists() or not trial_path.is_file():
        return {
            "ok": False,
            "status": "invalid_trial_runtime_rules",
            "goldset_root": str(root),
            "trial_runtime_rules_path": str(trial_path),
            "error": "trial runtime rule path does not exist or is not a file",
        }
    try:
        load_runtime_rule_trial_overrides(trial_path)
    except EventMapperError as exc:
        return {
            "ok": False,
            "status": exc.status,
            "goldset_root": str(root),
            "trial_runtime_rules_path": str(trial_path),
            "error": exc.message,
        }

    effective_trial_name = str(trial_name or trial_path.stem)
    current_report = _build_validation_report(
        root,
        clip_runner=clip_runner,
        game=game,
        media_root=resolved_media_root,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=None,
        runtime_sidecar_root=None,
        fused_sidecar_root=None,
    )
    trial_report = _build_validation_report(
        root,
        clip_runner=clip_runner,
        game=game,
        media_root=resolved_media_root,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=None,
        runtime_sidecar_root=None,
        fused_sidecar_root=None,
        trial_runtime_rule_overrides_path=trial_path,
    )
    result = _build_runtime_rule_replay_report(root, current_report, trial_report, effective_trial_name, trial_path)
    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if debug_output_dir is not None:
        _write_runtime_rule_replay_debug_bundle(_resolve_path(debug_output_dir), result)
    return result


def _build_validation_report(
    root: Path,
    *,
    clip_runner: ClipRunner,
    game: str | None,
    media_root: Path | None,
    sample_fps: float | None,
    limit_frames: int | None,
    proxy_sidecar_root: str | Path | None,
    runtime_sidecar_root: str | Path | None,
    fused_sidecar_root: str | Path | None,
    trial_rules_path: str | Path | None = None,
    trial_template_overrides_path: str | Path | None = None,
    trial_runtime_rule_overrides_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest_paths = sorted(root.rglob("*.fusion_goldset.json"))
    warnings: list[dict[str, str]] = []
    per_clip_results: list[dict[str, Any]] = []
    aggregated = _empty_aggregate()

    for manifest_path in manifest_paths:
        clip_result = _validate_gold_manifest(
            manifest_path,
            clip_runner=clip_runner,
            game_filter=game,
            media_root=media_root,
            sample_fps=sample_fps,
            limit_frames=limit_frames,
            proxy_sidecar_root=proxy_sidecar_root,
            runtime_sidecar_root=runtime_sidecar_root,
            fused_sidecar_root=fused_sidecar_root,
            trial_rules_path=trial_rules_path,
            trial_template_overrides_path=trial_template_overrides_path,
            trial_runtime_rule_overrides_path=trial_runtime_rule_overrides_path,
        )
        if clip_result["status"] != "ok":
            warnings.append({"path": str(manifest_path), "reason": str(clip_result["status"])})
            per_clip_results.append(clip_result)
            continue
        per_clip_results.append(clip_result)
        _accumulate_layer_metrics(aggregated["detection"], clip_result["detection_metrics"])
        _accumulate_layer_metrics(aggregated["runtime"], clip_result["runtime_event_metrics"])
        _accumulate_layer_metrics(aggregated["fusion"], clip_result["fusion_metrics"])
        _accumulate_boundary_metrics(aggregated["boundary"], clip_result["boundary_metrics"])

    validated_rows = [row for row in per_clip_results if row["status"] == "ok"]
    report = {
        "ok": True,
        "status": "ok",
        "goldset_root": str(root),
        "scanned_clip_count": len(manifest_paths),
        "validated_clip_count": len(validated_rows),
        "skipped_clip_count": len(manifest_paths) - len(validated_rows),
        "detection_metrics": _finalize_layer_metrics(aggregated["detection"]),
        "runtime_event_metrics": _finalize_layer_metrics(aggregated["runtime"]),
        "fusion_metrics": _finalize_layer_metrics(aggregated["fusion"]),
        "boundary_metrics": _finalize_boundary_metrics(aggregated["boundary"]),
        "detection_diagnostics": _summarize_detection_diagnostics(validated_rows),
        "runtime_diagnostics": _summarize_runtime_diagnostics(validated_rows),
        "fusion_diagnostics": _summarize_fusion_diagnostics(validated_rows),
        "boundary_diagnostics": _summarize_boundary_diagnostics(validated_rows),
        "clip_summaries": _build_clip_summaries(validated_rows),
        "coverage_summary": _build_coverage_summary(validated_rows),
        "per_clip_results": per_clip_results,
        "failure_buckets": _failure_buckets(per_clip_results),
        "warnings": warnings,
    }
    if game is not None:
        report["game_filter"] = game
    if media_root is not None:
        report["media_root"] = str(media_root)
    return report


def _validate_gold_manifest(
    manifest_path: Path,
    *,
    clip_runner: ClipRunner,
    game_filter: str | None,
    media_root: Path | None,
    sample_fps: float | None,
    limit_frames: int | None,
    proxy_sidecar_root: str | Path | None,
    runtime_sidecar_root: str | Path | None,
    fused_sidecar_root: str | Path | None,
    trial_rules_path: str | Path | None,
    trial_template_overrides_path: str | Path | None,
    trial_runtime_rule_overrides_path: str | Path | None,
) -> dict[str, Any]:
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "status": "malformed_gold_manifest",
            "manifest_path": str(manifest_path.resolve()),
            "error": str(exc),
        }
    validation_error = _validate_gold_manifest_shape(manifest)
    if validation_error is not None:
        return {
            "status": validation_error,
            "manifest_path": str(manifest_path.resolve()),
        }
    if game_filter is not None and str(manifest.get("game")) != game_filter:
        return {
            "status": "game_filter_mismatch",
            "manifest_path": str(manifest_path.resolve()),
            "game": manifest.get("game"),
            "source": manifest.get("source"),
        }

    source = str(manifest["source"])
    resolved_source = _resolve_manifest_source(manifest_path, source, media_root)
    if isinstance(resolved_source, dict):
        return resolved_source
    game = str(manifest["game"])
    runner_result = clip_runner(
        resolved_source,
        game,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=proxy_sidecar_root,
        runtime_sidecar_root=runtime_sidecar_root,
        fused_sidecar_root=fused_sidecar_root,
        trial_rules_path=trial_rules_path,
        trial_template_overrides_path=trial_template_overrides_path,
        trial_runtime_rule_overrides_path=trial_runtime_rule_overrides_path,
    )
    if not runner_result.get("ok", False):
        return {
            "status": "analysis_failed",
            "manifest_path": str(manifest_path.resolve()),
            "game": game,
            "source": source,
            "analysis_result": runner_result,
        }

    runtime_sidecar = runner_result["runtime"]
    fused_sidecar = runner_result["fused"]
    detection_rows = list(runtime_sidecar.get("matcher", {}).get("confirmed_detections", []))
    runtime_event_rows = list(runtime_sidecar.get("events", {}).get("rows", []))
    fused_event_rows = list(fused_sidecar.get("fused_events", []))

    tolerances = manifest.get("tolerances", {}) if isinstance(manifest.get("tolerances"), dict) else {}
    detection_metrics = _match_layer(
        list(manifest.get("expected_detections", [])),
        detection_rows,
        matcher=_detection_match,
        default_tolerance=float(tolerances.get("timestamp_tolerance_seconds", DEFAULT_TIMESTAMP_TOLERANCE_SECONDS)),
        semantic_fields=("entity_id", "ability_id", "equipment_id", "event_row_id"),
    )
    runtime_metrics = _match_layer(
        list(manifest.get("expected_runtime_events", [])),
        runtime_event_rows,
        matcher=_runtime_event_match,
        default_tolerance=float(tolerances.get("timestamp_tolerance_seconds", DEFAULT_TIMESTAMP_TOLERANCE_SECONDS)),
        semantic_fields=("entity_id", "ability_id", "equipment_id", "event_row_id"),
    )
    fusion_metrics = _match_layer(
        list(manifest.get("expected_fused_events", [])),
        fused_event_rows,
        matcher=_fused_event_match,
        default_tolerance=float(tolerances.get("timestamp_tolerance_seconds", DEFAULT_TIMESTAMP_TOLERANCE_SECONDS)),
        semantic_fields=("entity_id", "ability_id", "equipment_id", "event_row_id"),
        gate_status_matcher=_fused_event_match_ignoring_gate_status,
    )
    fusion_metrics.update(
        _score_fused_synergy_diagnostics(
            list(manifest.get("expected_fused_events", [])),
            fused_event_rows,
            default_tolerance=float(tolerances.get("timestamp_tolerance_seconds", DEFAULT_TIMESTAMP_TOLERANCE_SECONDS)),
            semantic_fields=("entity_id", "ability_id", "equipment_id", "event_row_id"),
        )
    )
    boundary_metrics = _score_boundaries(
        list(manifest.get("expected_boundaries", [])),
        fused_event_rows,
        default_tolerance=float(tolerances.get("boundary_tolerance_seconds", DEFAULT_BOUNDARY_TOLERANCE_SECONDS)),
    )
    return {
        "status": "ok",
        "manifest_path": str(manifest_path.resolve()),
        "clip_id": manifest.get("clip_id"),
        "game": game,
        "source": source,
        "resolved_source": resolved_source,
        "coverage_tags": [str(value) for value in manifest.get("coverage_tags", []) if str(value).strip()],
        "expected_detections": list(manifest.get("expected_detections", [])),
        "expected_fused_events": list(manifest.get("expected_fused_events", [])),
        "boundary_expectations": list(manifest.get("expected_boundaries", [])),
        "boundary_tolerance_seconds": float(tolerances.get("boundary_tolerance_seconds", DEFAULT_BOUNDARY_TOLERANCE_SECONDS)),
        "detection_rows": detection_rows,
        "runtime_event_rows": runtime_event_rows,
        "runtime_event_summary": runtime_sidecar.get("events", {}).get("event_summary", {}),
        "fused_event_rows": fused_event_rows,
        "detection_metrics": detection_metrics,
        "runtime_event_metrics": runtime_metrics,
        "fusion_metrics": fusion_metrics,
        "boundary_metrics": boundary_metrics,
    }


def _validate_gold_manifest_shape(manifest: Any) -> str | None:
    if not isinstance(manifest, dict):
        return "invalid_gold_manifest"
    if str(manifest.get("schema_version", "")) != SUPPORTED_GOLDSET_SCHEMA_VERSION:
        return "unsupported_gold_manifest_schema"
    if not str(manifest.get("game", "")).strip():
        return "missing_gold_manifest_game"
    if not str(manifest.get("source", "")).strip():
        return "missing_gold_manifest_source"
    for key in ("expected_detections", "expected_runtime_events", "expected_fused_events", "expected_boundaries"):
        if key in manifest and not isinstance(manifest[key], list):
            return f"invalid_{key}"
    ontology = load_runtime_signal_event_ontology()
    for row in manifest.get("expected_runtime_events", []):
        if isinstance(row, dict):
            event_type = str(row.get("event_type", "")).strip()
            if event_type and event_type not in ontology.event_types:
                return "invalid_gold_manifest_runtime_event_type"
    for row in manifest.get("expected_fused_events", []):
        if not isinstance(row, dict):
            continue
        event_type = str(row.get("event_type", "")).strip()
        if event_type and event_type not in ontology.event_types:
            return "invalid_gold_manifest_fused_event_type"
        required_signal_types = [str(value).strip() for value in row.get("required_signal_types", []) if str(value).strip()]
        if any(signal_type not in ontology.signal_types for signal_type in required_signal_types):
            return "invalid_gold_manifest_required_signal_type"
    return None


def _match_layer(
    expected_rows: list[dict[str, Any]],
    actual_rows: list[dict[str, Any]],
    *,
    matcher: Callable[[dict[str, Any], dict[str, Any], float, tuple[str, ...]], bool],
    default_tolerance: float,
    semantic_fields: tuple[str, ...],
    gate_status_matcher: Callable[[dict[str, Any], dict[str, Any], float, tuple[str, ...]], bool] | None = None,
) -> dict[str, Any]:
    matched_actual_indexes: set[int] = set()
    matched_rows = 0
    gate_status_match_count = 0
    gate_status_comparable_count = 0
    misses: list[dict[str, Any]] = []
    for expected in expected_rows:
        tolerance = float(expected.get("timestamp_tolerance_seconds", default_tolerance))
        found_index = None
        for index, actual in enumerate(actual_rows):
            if index in matched_actual_indexes:
                continue
            if matcher(expected, actual, tolerance, semantic_fields):
                found_index = index
                break
        if found_index is None:
            if gate_status_matcher is not None:
                for index, actual in enumerate(actual_rows):
                    if index in matched_actual_indexes:
                        continue
                    if gate_status_matcher(expected, actual, tolerance, semantic_fields):
                        gate_status_comparable_count += 1
                        break
            misses.append(expected)
            continue
        matched_actual_indexes.add(found_index)
        matched_rows += 1
        if gate_status_matcher is not None:
            gate_status_match_count += 1
            gate_status_comparable_count += 1

    false_positives = [actual_rows[index] for index in range(len(actual_rows)) if index not in matched_actual_indexes]
    expected_count = len(expected_rows)
    actual_count = len(actual_rows)
    precision = round(matched_rows / actual_count, 4) if actual_count else 1.0
    recall = round(matched_rows / expected_count, 4) if expected_count else 1.0
    return {
        "expected_count": expected_count,
        "actual_count": actual_count,
        "matched_count": matched_rows,
        "miss_count": len(misses),
        "false_positive_count": len(false_positives),
        "precision": precision,
        "recall": recall,
        "gate_status_match_count": gate_status_match_count,
        "gate_status_comparable_count": gate_status_comparable_count,
        "misses": misses,
        "false_positives": false_positives,
    }


def _detection_match(
    expected: dict[str, Any],
    actual: dict[str, Any],
    tolerance: float,
    semantic_fields: tuple[str, ...],
) -> bool:
    if expected.get("asset_family") and str(expected.get("asset_family")) != str(actual.get("asset_family")):
        return False
    if expected.get("roi_ref") and str(expected.get("roi_ref")) != str(actual.get("roi_ref")):
        return False
    if not _semantic_match(expected, actual, semantic_fields):
        return False
    return _timestamp_window_match(expected, actual, tolerance, start_key="first_timestamp", end_key="last_timestamp")


def _runtime_event_match(
    expected: dict[str, Any],
    actual: dict[str, Any],
    tolerance: float,
    semantic_fields: tuple[str, ...],
) -> bool:
    if expected.get("event_type") and str(expected.get("event_type")) != str(actual.get("event_type")):
        return False
    if not _semantic_match(expected, actual, semantic_fields):
        return False
    return _timestamp_window_match(expected, actual, tolerance, start_key="start_timestamp", end_key="end_timestamp")


def _fused_event_match(
    expected: dict[str, Any],
    actual: dict[str, Any],
    tolerance: float,
    semantic_fields: tuple[str, ...],
) -> bool:
    if expected.get("event_type") and str(expected.get("event_type")) != str(actual.get("event_type")):
        return False
    if expected.get("gate_status") and str(expected.get("gate_status")) != str(actual.get("gate_status")):
        return False
    if "synergy_expected" in expected and bool(expected.get("synergy_expected")) != bool(actual.get("synergy_applied", False)):
        return False
    if "minimum_required_signals_met" in expected and bool(expected.get("minimum_required_signals_met")) != bool(
        actual.get("minimum_required_signals_met", False)
    ):
        return False
    if not _semantic_match(expected, actual.get("metadata", {}), semantic_fields):
        return False
    required_signal_types = sorted(str(value) for value in expected.get("required_signal_types", []) if str(value))
    actual_signal_types = sorted(str(value) for value in actual.get("metadata", {}).get("matched_signal_types", []) if str(value))
    if required_signal_types and any(signal_type not in actual_signal_types for signal_type in required_signal_types):
        return False
    return _timestamp_window_match(expected, actual, tolerance, start_key="start_timestamp", end_key="end_timestamp")


def _fused_event_match_ignoring_gate_status(
    expected: dict[str, Any],
    actual: dict[str, Any],
    tolerance: float,
    semantic_fields: tuple[str, ...],
) -> bool:
    relaxed = dict(expected)
    relaxed.pop("gate_status", None)
    relaxed.pop("synergy_expected", None)
    relaxed.pop("minimum_required_signals_met", None)
    return _fused_event_match(relaxed, actual, tolerance, semantic_fields)


def _fused_event_match_ignoring_synergy(
    expected: dict[str, Any],
    actual: dict[str, Any],
    tolerance: float,
    semantic_fields: tuple[str, ...],
) -> bool:
    relaxed = dict(expected)
    relaxed.pop("gate_status", None)
    relaxed.pop("synergy_expected", None)
    relaxed.pop("minimum_required_signals_met", None)
    return _fused_event_match(relaxed, actual, tolerance, semantic_fields)


def _score_fused_synergy_diagnostics(
    expected_rows: list[dict[str, Any]],
    actual_rows: list[dict[str, Any]],
    *,
    default_tolerance: float,
    semantic_fields: tuple[str, ...],
) -> dict[str, Any]:
    synergy_expected_match_count = 0
    synergy_expected_comparable_count = 0
    minimum_required_signals_match_count = 0
    minimum_required_signals_comparable_count = 0
    for expected in expected_rows:
        tolerance = float(expected.get("timestamp_tolerance_seconds", default_tolerance))
        comparable_actual = next(
            (
                actual
                for actual in actual_rows
                if _fused_event_match_ignoring_synergy(expected, actual, tolerance, semantic_fields)
            ),
            None,
        )
        if comparable_actual is None:
            continue
        if "synergy_expected" in expected:
            synergy_expected_comparable_count += 1
            if bool(expected.get("synergy_expected")) == bool(comparable_actual.get("synergy_applied", False)):
                synergy_expected_match_count += 1
        if "minimum_required_signals_met" in expected:
            minimum_required_signals_comparable_count += 1
            if bool(expected.get("minimum_required_signals_met")) == bool(
                comparable_actual.get("minimum_required_signals_met", False)
            ):
                minimum_required_signals_match_count += 1
    return {
        "synergy_expected_match_count": synergy_expected_match_count,
        "synergy_expected_comparable_count": synergy_expected_comparable_count,
        "minimum_required_signals_match_count": minimum_required_signals_match_count,
        "minimum_required_signals_comparable_count": minimum_required_signals_comparable_count,
    }


def _semantic_match(expected: dict[str, Any], actual: dict[str, Any], semantic_fields: tuple[str, ...]) -> bool:
    for field in semantic_fields:
        if field in expected and str(expected.get(field)) != str(actual.get(field)):
            return False
    return True


def _timestamp_window_match(
    expected: dict[str, Any],
    actual: dict[str, Any],
    tolerance: float,
    *,
    start_key: str,
    end_key: str,
) -> bool:
    expected_timestamp = expected.get("timestamp")
    if expected_timestamp is not None:
        return abs(float(expected_timestamp) - _actual_timestamp(actual, start_key, end_key)) <= tolerance
    expected_start = expected.get("start_timestamp")
    expected_end = expected.get("end_timestamp")
    actual_start = float(actual.get(start_key, actual.get("timestamp", 0.0)) or 0.0)
    actual_end = float(actual.get(end_key, actual.get("timestamp", actual_start)) or actual_start)
    if expected_start is not None and actual_end < float(expected_start) - tolerance:
        return False
    if expected_end is not None and actual_start > float(expected_end) + tolerance:
        return False
    return True


def _actual_timestamp(actual: dict[str, Any], start_key: str, end_key: str) -> float:
    start = float(actual.get(start_key, actual.get("timestamp", 0.0)) or 0.0)
    end = float(actual.get(end_key, actual.get("timestamp", start)) or start)
    return (start + end) / 2.0


def _score_boundaries(
    expected_rows: list[dict[str, Any]],
    fused_event_rows: list[dict[str, Any]],
    *,
    default_tolerance: float,
) -> dict[str, Any]:
    matched = 0
    start_errors: list[float] = []
    end_errors: list[float] = []
    misses: list[dict[str, Any]] = []
    for expected in expected_rows:
        tolerance = float(expected.get("boundary_tolerance_seconds", default_tolerance))
        actual = _find_boundary_event(expected, fused_event_rows)
        if actual is None:
            misses.append(expected)
            continue
        start_error = abs(float(expected["expected_start_timestamp"]) - float(actual.get("suggested_start_timestamp", 0.0)))
        end_error = abs(float(expected["expected_end_timestamp"]) - float(actual.get("suggested_end_timestamp", 0.0)))
        if start_error <= tolerance and end_error <= tolerance:
            matched += 1
        start_errors.append(round(start_error, 5))
        end_errors.append(round(end_error, 5))
    expected_count = len(expected_rows)
    return {
        "expected_count": expected_count,
        "within_tolerance_count": matched,
        "within_tolerance_rate": round(matched / expected_count, 4) if expected_count else 1.0,
        "miss_count": len(misses),
        "misses": misses,
        "average_start_error": _safe_average(start_errors),
        "average_end_error": _safe_average(end_errors),
        "median_start_error": _safe_median(start_errors),
        "median_end_error": _safe_median(end_errors),
    }


def _find_boundary_event(expected: dict[str, Any], fused_event_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in fused_event_rows:
        if expected.get("event_type") and str(expected.get("event_type")) != str(row.get("event_type")):
            continue
        if expected.get("gate_status") and str(expected.get("gate_status")) != str(row.get("gate_status")):
            continue
        if expected.get("event_row_id") and str(expected.get("event_row_id")) != str(row.get("metadata", {}).get("event_row_id")):
            continue
        return row
    return None


def _empty_aggregate() -> dict[str, Any]:
    return {
        "detection": {"expected": 0, "actual": 0, "matched": 0, "false_positive": 0, "miss": 0},
        "runtime": {"expected": 0, "actual": 0, "matched": 0, "false_positive": 0, "miss": 0},
        "fusion": {
            "expected": 0,
            "actual": 0,
            "matched": 0,
            "false_positive": 0,
            "miss": 0,
            "gate_status_match": 0,
            "synergy_expected_match": 0,
            "synergy_expected_comparable": 0,
            "minimum_required_signals_match": 0,
            "minimum_required_signals_comparable": 0,
        },
        "boundary": {"expected": 0, "within_tolerance": 0, "start_errors": [], "end_errors": [], "miss": 0},
    }


def _accumulate_layer_metrics(aggregate: dict[str, Any], metrics: dict[str, Any]) -> None:
    aggregate["expected"] += int(metrics["expected_count"])
    aggregate["actual"] += int(metrics["actual_count"])
    aggregate["matched"] += int(metrics["matched_count"])
    aggregate["false_positive"] += int(metrics["false_positive_count"])
    aggregate["miss"] += int(metrics["miss_count"])
    aggregate["gate_status_match"] = aggregate.get("gate_status_match", 0) + int(metrics.get("gate_status_match_count", 0))
    aggregate["gate_status_comparable"] = aggregate.get("gate_status_comparable", 0) + int(
        metrics.get("gate_status_comparable_count", 0)
    )
    aggregate["synergy_expected_match"] = aggregate.get("synergy_expected_match", 0) + int(
        metrics.get("synergy_expected_match_count", 0)
    )
    aggregate["synergy_expected_comparable"] = aggregate.get("synergy_expected_comparable", 0) + int(
        metrics.get("synergy_expected_comparable_count", 0)
    )
    aggregate["minimum_required_signals_match"] = aggregate.get("minimum_required_signals_match", 0) + int(
        metrics.get("minimum_required_signals_match_count", 0)
    )
    aggregate["minimum_required_signals_comparable"] = aggregate.get("minimum_required_signals_comparable", 0) + int(
        metrics.get("minimum_required_signals_comparable_count", 0)
    )


def _accumulate_boundary_metrics(aggregate: dict[str, Any], metrics: dict[str, Any]) -> None:
    aggregate["expected"] += int(metrics["expected_count"])
    aggregate["within_tolerance"] += int(metrics["within_tolerance_count"])
    aggregate["miss"] += int(metrics["miss_count"])
    if metrics["average_start_error"] is not None:
        aggregate["start_errors"].append(float(metrics["average_start_error"]))
    if metrics["average_end_error"] is not None:
        aggregate["end_errors"].append(float(metrics["average_end_error"]))


def _finalize_layer_metrics(aggregate: dict[str, Any]) -> dict[str, Any]:
    expected = int(aggregate["expected"])
    actual = int(aggregate["actual"])
    matched = int(aggregate["matched"])
    return {
        "expected_count": expected,
        "actual_count": actual,
        "matched_count": matched,
        "miss_count": int(aggregate["miss"]),
        "false_positive_count": int(aggregate["false_positive"]),
        "precision": round(matched / actual, 4) if actual else 1.0,
        "recall": round(matched / expected, 4) if expected else 1.0,
        "gate_status_accuracy": round(
            aggregate.get("gate_status_match", matched) / aggregate.get("gate_status_comparable", matched),
            4,
        )
        if aggregate.get("gate_status_comparable", matched)
        else 1.0,
        "synergy_applied_accuracy": round(
            aggregate.get("synergy_expected_match", 0) / aggregate.get("synergy_expected_comparable", 0),
            4,
        )
        if aggregate.get("synergy_expected_comparable", 0)
        else 1.0,
        "minimum_required_signals_accuracy": round(
            aggregate.get("minimum_required_signals_match", 0) / aggregate.get("minimum_required_signals_comparable", 0),
            4,
        )
        if aggregate.get("minimum_required_signals_comparable", 0)
        else 1.0,
    }


def _finalize_boundary_metrics(aggregate: dict[str, Any]) -> dict[str, Any]:
    expected = int(aggregate["expected"])
    within = int(aggregate["within_tolerance"])
    return {
        "expected_count": expected,
        "within_tolerance_count": within,
        "within_tolerance_rate": round(within / expected, 4) if expected else 1.0,
        "miss_count": int(aggregate["miss"]),
        "average_start_error": _safe_average(aggregate["start_errors"]),
        "average_end_error": _safe_average(aggregate["end_errors"]),
        "median_start_error": _safe_median(aggregate["start_errors"]),
        "median_end_error": _safe_median(aggregate["end_errors"]),
    }


def _failure_buckets(per_clip_results: list[dict[str, Any]]) -> dict[str, int]:
    buckets: dict[str, int] = {}
    for row in per_clip_results:
        status = str(row.get("status", "unknown"))
        buckets[status] = buckets.get(status, 0) + 1
    return buckets


def _build_replay_report(
    root: Path,
    current_report: dict[str, Any],
    trial_report: dict[str, Any],
    trial_name: str,
) -> dict[str, Any]:
    current_fusion = current_report["fusion_metrics"]
    trial_fusion = trial_report["fusion_metrics"]
    current_boundary = current_report["boundary_metrics"]
    trial_boundary = trial_report["boundary_metrics"]

    comparison = {
        "current": {
            "fusion_metrics": current_fusion,
            "boundary_metrics": current_boundary,
            "fusion_diagnostics": current_report.get("fusion_diagnostics", {}),
        },
        "trial": {
            "fusion_metrics": trial_fusion,
            "boundary_metrics": trial_boundary,
            "fusion_diagnostics": trial_report.get("fusion_diagnostics", {}),
        },
        "delta": {
            "fusion_recall_delta": round(float(trial_fusion["recall"]) - float(current_fusion["recall"]), 4),
            "fusion_precision_delta": round(float(trial_fusion["precision"]) - float(current_fusion["precision"]), 4),
            "gate_status_accuracy_delta": round(
                float(trial_fusion["gate_status_accuracy"]) - float(current_fusion["gate_status_accuracy"]), 4
            ),
            "synergy_applied_accuracy_delta": round(
                float(trial_fusion.get("synergy_applied_accuracy", 1.0))
                - float(current_fusion.get("synergy_applied_accuracy", 1.0)),
                4,
            ),
            "minimum_required_signals_accuracy_delta": round(
                float(trial_fusion.get("minimum_required_signals_accuracy", 1.0))
                - float(current_fusion.get("minimum_required_signals_accuracy", 1.0)),
                4,
            ),
            "boundary_within_tolerance_rate_delta": round(
                float(trial_boundary["within_tolerance_rate"]) - float(current_boundary["within_tolerance_rate"]), 4
            ),
        },
        "per_clip_comparisons": _per_clip_comparisons(current_report, trial_report),
    }
    recommendation = _fusion_recommendation(current_report, trial_report, comparison)
    return {
        "ok": True,
        "status": "ok",
        "goldset_root": str(root),
        "trial_name": trial_name,
        "current_rules": {"rule_count": len(current_report.get("warnings", [])), "source": "published_pack"},
        "trial_rules": {"source": trial_name},
        "comparison": comparison,
        "recommendation": recommendation,
        "warnings": list(current_report.get("warnings", [])) + list(trial_report.get("warnings", [])),
    }


def _per_clip_comparisons(current_report: dict[str, Any], trial_report: dict[str, Any]) -> list[dict[str, Any]]:
    current_by_key = {
        (row.get("game"), row.get("source")): row
        for row in current_report.get("per_clip_results", [])
        if row.get("status") == "ok"
    }
    trial_by_key = {
        (row.get("game"), row.get("source")): row
        for row in trial_report.get("per_clip_results", [])
        if row.get("status") == "ok"
    }
    rows: list[dict[str, Any]] = []
    for key in sorted(set(current_by_key) | set(trial_by_key)):
        current = current_by_key.get(key)
        trial = trial_by_key.get(key)
        if current is None or trial is None:
            continue
        rows.append(
            {
                "game": key[0],
                "source": key[1],
                "current_fusion_recall": current["fusion_metrics"]["recall"],
                "trial_fusion_recall": trial["fusion_metrics"]["recall"],
                "current_synergy_applied_accuracy": current["fusion_metrics"].get("synergy_applied_accuracy"),
                "trial_synergy_applied_accuracy": trial["fusion_metrics"].get("synergy_applied_accuracy"),
                "current_boundary_within_tolerance_rate": current["boundary_metrics"]["within_tolerance_rate"],
                "trial_boundary_within_tolerance_rate": trial["boundary_metrics"]["within_tolerance_rate"],
            }
        )
    return rows


def _fusion_recommendation(
    current_report: dict[str, Any],
    trial_report: dict[str, Any],
    comparison: dict[str, Any],
) -> dict[str, Any]:
    current_fusion = current_report["fusion_metrics"]
    trial_fusion = trial_report["fusion_metrics"]
    current_boundary = current_report["boundary_metrics"]
    trial_boundary = trial_report["boundary_metrics"]

    better_recall = float(trial_fusion["recall"]) > float(current_fusion["recall"])
    better_precision = float(trial_fusion["precision"]) >= float(current_fusion["precision"])
    better_gate = float(trial_fusion["gate_status_accuracy"]) >= float(current_fusion["gate_status_accuracy"])
    better_synergy = float(trial_fusion.get("synergy_applied_accuracy", 1.0)) >= float(
        current_fusion.get("synergy_applied_accuracy", 1.0)
    ) and float(trial_fusion.get("minimum_required_signals_accuracy", 1.0)) >= float(
        current_fusion.get("minimum_required_signals_accuracy", 1.0)
    )
    better_boundary = float(trial_boundary["within_tolerance_rate"]) >= float(current_boundary["within_tolerance_rate"])
    worse_precision = float(trial_fusion["precision"]) < float(current_fusion["precision"])
    worse_boundary = float(trial_boundary["within_tolerance_rate"]) < float(current_boundary["within_tolerance_rate"])
    worse_synergy = float(trial_fusion.get("synergy_applied_accuracy", 1.0)) < float(
        current_fusion.get("synergy_applied_accuracy", 1.0)
    )

    if better_recall and better_precision and better_gate and better_synergy and better_boundary:
        decision = "prefer_trial"
        reason = "Trial fusion rules improve or preserve event precision while increasing recall, gate accuracy, synergy correctness, and boundary quality."
    elif worse_precision or worse_boundary or worse_synergy:
        decision = "keep_current"
        reason = "Trial fusion rules regress event precision, synergy correctness, or clip-boundary quality."
    else:
        decision = "inconclusive"
        reason = "Current and trial fusion-rule outcomes are mixed across recall, gate accuracy, synergy correctness, and boundary quality."

    return {
        "decision": decision,
        "reason": reason,
        "supporting_metrics": comparison["delta"],
        "data_quality_notes": [
            f"validated_clip_count={current_report.get('validated_clip_count', 0)}",
        ],
        "follow_up": (
            "Adjust lag window or gate multipliers and replay again."
            if decision != "keep_current"
            else "Tighten the trial gate configuration before replaying."
        ),
    }


def _build_template_replay_report(
    root: Path,
    current_report: dict[str, Any],
    trial_report: dict[str, Any],
    trial_name: str,
    trial_path: Path,
) -> dict[str, Any]:
    current_detection = current_report["detection_metrics"]
    trial_detection = trial_report["detection_metrics"]
    current_runtime = current_report["runtime_event_metrics"]
    trial_runtime = trial_report["runtime_event_metrics"]
    current_fusion = current_report["fusion_metrics"]
    trial_fusion = trial_report["fusion_metrics"]
    current_boundary = current_report["boundary_metrics"]
    trial_boundary = trial_report["boundary_metrics"]
    comparison = {
        "current": {
            "detection_metrics": current_detection,
            "detection_diagnostics": current_report.get("detection_diagnostics", {}),
            "runtime_event_metrics": current_runtime,
            "fusion_metrics": current_fusion,
            "boundary_metrics": current_boundary,
        },
        "trial": {
            "detection_metrics": trial_detection,
            "detection_diagnostics": trial_report.get("detection_diagnostics", {}),
            "runtime_event_metrics": trial_runtime,
            "fusion_metrics": trial_fusion,
            "boundary_metrics": trial_boundary,
        },
        "delta": {
            "detection_recall_delta": round(float(trial_detection["recall"]) - float(current_detection["recall"]), 4),
            "detection_precision_delta": round(float(trial_detection["precision"]) - float(current_detection["precision"]), 4),
            "runtime_recall_delta": round(float(trial_runtime["recall"]) - float(current_runtime["recall"]), 4),
            "runtime_precision_delta": round(float(trial_runtime["precision"]) - float(current_runtime["precision"]), 4),
            "fusion_recall_delta": round(float(trial_fusion["recall"]) - float(current_fusion["recall"]), 4),
            "fusion_precision_delta": round(float(trial_fusion["precision"]) - float(current_fusion["precision"]), 4),
            "boundary_within_tolerance_rate_delta": round(
                float(trial_boundary["within_tolerance_rate"]) - float(current_boundary["within_tolerance_rate"]),
                4,
            ),
        },
        "per_clip_comparisons": _template_per_clip_comparisons(current_report, trial_report),
    }
    recommendation = _template_recommendation(current_report, trial_report, comparison)
    return {
        "ok": True,
        "status": "ok",
        "goldset_root": str(root),
        "trial_name": trial_name,
        "current_templates": {"source": "published_pack"},
        "trial_templates": {"source": str(trial_path)},
        "comparison": comparison,
        "recommendation": recommendation,
        "warnings": list(current_report.get("warnings", [])) + list(trial_report.get("warnings", [])),
    }


def _build_runtime_rule_replay_report(
    root: Path,
    current_report: dict[str, Any],
    trial_report: dict[str, Any],
    trial_name: str,
    trial_path: Path,
) -> dict[str, Any]:
    current_runtime = current_report["runtime_event_metrics"]
    trial_runtime = trial_report["runtime_event_metrics"]
    current_fusion = current_report["fusion_metrics"]
    trial_fusion = trial_report["fusion_metrics"]
    current_boundary = current_report["boundary_metrics"]
    trial_boundary = trial_report["boundary_metrics"]
    comparison = {
        "current": {
            "runtime_event_metrics": current_runtime,
            "runtime_diagnostics": current_report.get("runtime_diagnostics", {}),
            "fusion_metrics": current_fusion,
            "boundary_metrics": current_boundary,
        },
        "trial": {
            "runtime_event_metrics": trial_runtime,
            "runtime_diagnostics": trial_report.get("runtime_diagnostics", {}),
            "fusion_metrics": trial_fusion,
            "boundary_metrics": trial_boundary,
        },
        "delta": {
            "runtime_recall_delta": round(float(trial_runtime["recall"]) - float(current_runtime["recall"]), 4),
            "runtime_precision_delta": round(float(trial_runtime["precision"]) - float(current_runtime["precision"]), 4),
            "fusion_recall_delta": round(float(trial_fusion["recall"]) - float(current_fusion["recall"]), 4),
            "fusion_precision_delta": round(float(trial_fusion["precision"]) - float(current_fusion["precision"]), 4),
            "boundary_within_tolerance_rate_delta": round(
                float(trial_boundary["within_tolerance_rate"]) - float(current_boundary["within_tolerance_rate"]),
                4,
            ),
            "identity_competition_drop_delta": int(trial_report.get("runtime_diagnostics", {}).get("identity_competition_drops", 0))
            - int(current_report.get("runtime_diagnostics", {}).get("identity_competition_drops", 0)),
        },
        "per_clip_comparisons": _runtime_rule_per_clip_comparisons(current_report, trial_report),
    }
    recommendation = _runtime_rule_recommendation(current_report, trial_report, comparison)
    return {
        "ok": True,
        "status": "ok",
        "goldset_root": str(root),
        "trial_name": trial_name,
        "current_runtime_rules": {"source": "published_pack"},
        "trial_runtime_rules": {"source": str(trial_path)},
        "comparison": comparison,
        "recommendation": recommendation,
        "warnings": list(current_report.get("warnings", [])) + list(trial_report.get("warnings", [])),
    }


def _template_per_clip_comparisons(current_report: dict[str, Any], trial_report: dict[str, Any]) -> list[dict[str, Any]]:
    current_by_key = {
        (row.get("game"), row.get("source")): row
        for row in current_report.get("per_clip_results", [])
        if row.get("status") == "ok"
    }
    trial_by_key = {
        (row.get("game"), row.get("source")): row
        for row in trial_report.get("per_clip_results", [])
        if row.get("status") == "ok"
    }
    rows: list[dict[str, Any]] = []
    for key in sorted(set(current_by_key) | set(trial_by_key)):
        current = current_by_key.get(key)
        trial = trial_by_key.get(key)
        if current is None or trial is None:
            continue
        rows.append(
            {
                "game": key[0],
                "source": key[1],
                "current_detection_recall": current["detection_metrics"]["recall"],
                "trial_detection_recall": trial["detection_metrics"]["recall"],
                "current_detection_precision": current["detection_metrics"]["precision"],
                "trial_detection_precision": trial["detection_metrics"]["precision"],
                "current_runtime_recall": current["runtime_event_metrics"]["recall"],
                "trial_runtime_recall": trial["runtime_event_metrics"]["recall"],
                "current_fusion_recall": current["fusion_metrics"]["recall"],
                "trial_fusion_recall": trial["fusion_metrics"]["recall"],
                "current_boundary_within_tolerance_rate": current["boundary_metrics"]["within_tolerance_rate"],
                "trial_boundary_within_tolerance_rate": trial["boundary_metrics"]["within_tolerance_rate"],
            }
        )
    return rows


def _runtime_rule_per_clip_comparisons(current_report: dict[str, Any], trial_report: dict[str, Any]) -> list[dict[str, Any]]:
    current_by_key = {
        (row.get("game"), row.get("source")): row
        for row in current_report.get("per_clip_results", [])
        if row.get("status") == "ok"
    }
    trial_by_key = {
        (row.get("game"), row.get("source")): row
        for row in trial_report.get("per_clip_results", [])
        if row.get("status") == "ok"
    }
    rows: list[dict[str, Any]] = []
    for key in sorted(set(current_by_key) | set(trial_by_key)):
        current = current_by_key.get(key)
        trial = trial_by_key.get(key)
        if current is None or trial is None:
            continue
        rows.append(
            {
                "game": key[0],
                "source": key[1],
                "current_runtime_recall": current["runtime_event_metrics"]["recall"],
                "trial_runtime_recall": trial["runtime_event_metrics"]["recall"],
                "current_runtime_precision": current["runtime_event_metrics"]["precision"],
                "trial_runtime_precision": trial["runtime_event_metrics"]["precision"],
                "current_fusion_recall": current["fusion_metrics"]["recall"],
                "trial_fusion_recall": trial["fusion_metrics"]["recall"],
                "current_boundary_within_tolerance_rate": current["boundary_metrics"]["within_tolerance_rate"],
                "trial_boundary_within_tolerance_rate": trial["boundary_metrics"]["within_tolerance_rate"],
            }
        )
    return rows


def _template_recommendation(
    current_report: dict[str, Any],
    trial_report: dict[str, Any],
    comparison: dict[str, Any],
) -> dict[str, Any]:
    current_detection = current_report["detection_metrics"]
    trial_detection = trial_report["detection_metrics"]
    current_runtime = current_report["runtime_event_metrics"]
    trial_runtime = trial_report["runtime_event_metrics"]
    current_fusion = current_report["fusion_metrics"]
    trial_fusion = trial_report["fusion_metrics"]
    current_boundary = current_report["boundary_metrics"]
    trial_boundary = trial_report["boundary_metrics"]

    better_matcher = (
        float(trial_detection["recall"]) > float(current_detection["recall"])
        or float(trial_detection["precision"]) > float(current_detection["precision"])
    )
    no_matcher_regression = float(trial_detection["precision"]) >= float(current_detection["precision"])
    no_downstream_regression = (
        float(trial_runtime["recall"]) >= float(current_runtime["recall"])
        and float(trial_fusion["recall"]) >= float(current_fusion["recall"])
        and float(trial_boundary["within_tolerance_rate"]) >= float(current_boundary["within_tolerance_rate"])
    )
    downstream_regression = (
        float(trial_runtime["recall"]) < float(current_runtime["recall"])
        or float(trial_fusion["recall"]) < float(current_fusion["recall"])
        or float(trial_boundary["within_tolerance_rate"]) < float(current_boundary["within_tolerance_rate"])
    )

    if better_matcher and no_matcher_regression and no_downstream_regression:
        decision = "prefer_trial"
        reason = "Trial template overrides improve matcher quality without regressing runtime-event, fusion, or boundary outcomes."
    elif float(trial_detection["precision"]) < float(current_detection["precision"]) or downstream_regression:
        decision = "keep_current"
        reason = "Trial template overrides increase matcher false positives or materially regress downstream runtime/fusion behavior."
    else:
        decision = "inconclusive"
        reason = "Template trial outcomes are mixed across matcher quality and downstream runtime/fusion impact."

    return {
        "decision": decision,
        "reason": reason,
        "supporting_metrics": comparison["delta"],
        "data_quality_notes": [f"validated_clip_count={current_report.get('validated_clip_count', 0)}"],
        "follow_up": (
            "Inspect the per-template false-positive and miss buckets before narrowing the next template override trial."
            if decision != "keep_current"
            else "Tighten thresholds, reduce aggressive scale sets, or shorten temporal windows before replaying."
        ),
    }


def _runtime_rule_recommendation(
    current_report: dict[str, Any],
    trial_report: dict[str, Any],
    comparison: dict[str, Any],
) -> dict[str, Any]:
    current_runtime = current_report["runtime_event_metrics"]
    trial_runtime = trial_report["runtime_event_metrics"]
    current_fusion = current_report["fusion_metrics"]
    trial_fusion = trial_report["fusion_metrics"]
    current_boundary = current_report["boundary_metrics"]
    trial_boundary = trial_report["boundary_metrics"]
    current_runtime_fp = int(current_runtime["false_positive_count"])
    trial_runtime_fp = int(trial_runtime["false_positive_count"])

    better_runtime = (
        float(trial_runtime["recall"]) > float(current_runtime["recall"])
        or float(trial_runtime["precision"]) > float(current_runtime["precision"])
    )
    no_runtime_regression = (
        float(trial_runtime["precision"]) >= float(current_runtime["precision"])
        and trial_runtime_fp <= current_runtime_fp
    )
    no_downstream_regression = (
        float(trial_fusion["recall"]) >= float(current_fusion["recall"])
        and float(trial_boundary["within_tolerance_rate"]) >= float(current_boundary["within_tolerance_rate"])
    )
    downstream_regression = (
        float(trial_fusion["recall"]) < float(current_fusion["recall"])
        or float(trial_boundary["within_tolerance_rate"]) < float(current_boundary["within_tolerance_rate"])
    )

    if better_runtime and no_runtime_regression and no_downstream_regression:
        decision = "prefer_trial"
        reason = "Trial runtime rule overrides improve runtime-event quality without regressing downstream fusion or boundary outcomes."
    elif not no_runtime_regression or downstream_regression:
        decision = "keep_current"
        reason = "Trial runtime rule overrides increase runtime false positives or materially regress downstream fusion/boundary behavior."
    else:
        decision = "inconclusive"
        reason = "Runtime rule trial outcomes are mixed across runtime-event quality and downstream fusion impact."

    return {
        "decision": decision,
        "reason": reason,
        "supporting_metrics": comparison["delta"],
        "data_quality_notes": [f"validated_clip_count={current_report.get('validated_clip_count', 0)}"],
        "follow_up": (
            "Inspect the runtime miss/false-positive buckets by family and ROI before refining the next runtime rule trial."
            if decision != "keep_current"
            else "Tighten collapse or identity-competition overrides before replaying."
        ),
    }


def _load_trial_rules_payload(path: Path) -> dict[str, Any]:
    try:
        if path.suffix.lower() == ".json":
            return json.loads(path.read_text(encoding="utf-8"))
        return load_yaml_file(path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"error": f"failed to load trial rules: {exc}"}


def _validate_media_root(media_root: str | Path | None) -> Path | dict[str, Any] | None:
    if media_root is None:
        return None
    resolved = _resolve_path(media_root)
    if not resolved.exists() or not resolved.is_dir():
        return {
            "ok": False,
            "status": "invalid_media_root",
            "media_root": str(resolved),
            "error": "media root does not exist or is not a directory",
        }
    return resolved


def _resolve_manifest_source(manifest_path: Path, source: str, media_root: Path | None) -> str | dict[str, Any]:
    if "://" in source:
        return source
    source_path = Path(source).expanduser()
    if source_path.is_absolute():
        return str(source_path.resolve())
    if media_root is None:
        return source
    resolved = (media_root / source_path).resolve()
    if not resolved.exists():
        return {
            "status": "unresolved_gold_manifest_source",
            "manifest_path": str(manifest_path.resolve()),
            "source": source,
            "resolved_source": str(resolved),
        }
    return str(resolved)


def _summarize_detection_diagnostics(validated_rows: list[dict[str, Any]]) -> dict[str, Any]:
    misses_by_asset_id: dict[str, int] = {}
    misses_by_asset_family: dict[str, int] = {}
    misses_by_roi_ref: dict[str, int] = {}
    misses_by_semantic_target_field: dict[str, int] = {}
    false_positives_by_asset_id: dict[str, int] = {}
    false_positives_by_asset_family: dict[str, int] = {}
    false_positives_by_roi_ref: dict[str, int] = {}
    for row in validated_rows:
        for miss in row.get("detection_metrics", {}).get("misses", []):
            _bump(misses_by_asset_id, str(miss.get("asset_id", "unknown")))
            _bump(misses_by_asset_family, str(miss.get("asset_family", "unknown")))
            _bump(misses_by_roi_ref, str(miss.get("roi_ref", "unknown")))
            _bump(misses_by_semantic_target_field, _semantic_target_field_name(miss))
        for false_positive in row.get("detection_metrics", {}).get("false_positives", []):
            _bump(false_positives_by_asset_id, str(false_positive.get("asset_id", "unknown")))
            _bump(false_positives_by_asset_family, str(false_positive.get("asset_family", "unknown")))
            _bump(false_positives_by_roi_ref, str(false_positive.get("roi_ref", "unknown")))
    return {
        "misses_by_asset_id": misses_by_asset_id,
        "misses_by_asset_family": misses_by_asset_family,
        "misses_by_roi_ref": misses_by_roi_ref,
        "misses_by_semantic_target_field": misses_by_semantic_target_field,
        "false_positives_by_asset_id": false_positives_by_asset_id,
        "false_positives_by_asset_family": false_positives_by_asset_family,
        "false_positives_by_roi_ref": false_positives_by_roi_ref,
        "overfiring_assets": {asset_id: count for asset_id, count in false_positives_by_asset_id.items() if count > 0},
    }


def _summarize_runtime_diagnostics(validated_rows: list[dict[str, Any]]) -> dict[str, Any]:
    misses_by_event_type: dict[str, int] = {}
    false_positives_by_event_type: dict[str, int] = {}
    misses_by_asset_family: dict[str, int] = {}
    false_positives_by_asset_family: dict[str, int] = {}
    misses_by_roi_ref: dict[str, int] = {}
    false_positives_by_roi_ref: dict[str, int] = {}
    misses_by_semantic_target_field: dict[str, int] = {}
    identity_competition_drops = 0
    for row in validated_rows:
        metrics = row.get("runtime_event_metrics", {})
        for miss in metrics.get("misses", []):
            _bump(misses_by_event_type, str(miss.get("event_type", "unknown")))
            _bump(misses_by_asset_family, str(miss.get("asset_family", "unknown")))
            _bump(misses_by_roi_ref, str(miss.get("roi_ref", "unknown")))
            _bump(misses_by_semantic_target_field, _semantic_target_field_name(miss))
        for false_positive in metrics.get("false_positives", []):
            _bump(false_positives_by_event_type, str(false_positive.get("event_type", "unknown")))
            _bump(false_positives_by_asset_family, str(false_positive.get("asset_family", "unknown")))
            _bump(false_positives_by_roi_ref, str(false_positive.get("roi_ref", "unknown")))
        identity_competition_drops += int(row.get("runtime_event_summary", {}).get("identity_competition_drop_count", 0) or 0)
    return {
        "misses_by_event_type": misses_by_event_type,
        "false_positives_by_event_type": false_positives_by_event_type,
        "misses_by_asset_family": misses_by_asset_family,
        "false_positives_by_asset_family": false_positives_by_asset_family,
        "misses_by_roi_ref": misses_by_roi_ref,
        "false_positives_by_roi_ref": false_positives_by_roi_ref,
        "misses_by_semantic_target_field": misses_by_semantic_target_field,
        "identity_competition_drops": identity_competition_drops,
    }


def _summarize_fusion_diagnostics(validated_rows: list[dict[str, Any]]) -> dict[str, Any]:
    misses_by_event_type: dict[str, int] = {}
    false_positives_by_event_type: dict[str, int] = {}
    required_signal_coverage_failures: dict[str, int] = {}
    for row in validated_rows:
        metrics = row.get("fusion_metrics", {})
        for miss in metrics.get("misses", []):
            event_type = str(miss.get("event_type", "unknown"))
            _bump(misses_by_event_type, event_type)
            required = sorted(str(value) for value in miss.get("required_signal_types", []) if str(value).strip())
            if required:
                _bump(required_signal_coverage_failures, f"{event_type}:{','.join(required)}")
        for false_positive in metrics.get("false_positives", []):
            _bump(false_positives_by_event_type, str(false_positive.get("event_type", "unknown")))
    return {
        "misses_by_event_type": misses_by_event_type,
        "false_positives_by_event_type": false_positives_by_event_type,
        "required_signal_coverage_failures": required_signal_coverage_failures,
    }


def _summarize_boundary_diagnostics(validated_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _boundary_diagnostics_from_validated_rows(validated_rows)


def _boundary_diagnostics_from_validated_rows(validated_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_event_type: dict[str, dict[str, Any]] = {}
    for row in validated_rows:
        for expected in row.get("boundary_expectations", []):
            event_type = str(expected.get("event_type", "unknown"))
            bucket = by_event_type.setdefault(
                event_type,
                {"expected_count": 0, "within_tolerance_count": 0, "start_errors": [], "end_errors": []},
            )
            bucket["expected_count"] += 1
            actual = _find_boundary_event(expected, row.get("fused_event_rows", []))
            if actual is None:
                continue
            tolerance = float(
                expected.get("boundary_tolerance_seconds", row.get("boundary_tolerance_seconds", DEFAULT_BOUNDARY_TOLERANCE_SECONDS))
            )
            start_error = abs(float(expected["expected_start_timestamp"]) - float(actual.get("suggested_start_timestamp", 0.0)))
            end_error = abs(float(expected["expected_end_timestamp"]) - float(actual.get("suggested_end_timestamp", 0.0)))
            bucket["start_errors"].append(round(start_error, 5))
            bucket["end_errors"].append(round(end_error, 5))
            if start_error <= tolerance and end_error <= tolerance:
                bucket["within_tolerance_count"] += 1
    return {
        "by_event_type": {
            event_type: {
                "expected_count": int(bucket["expected_count"]),
                "within_tolerance_count": int(bucket["within_tolerance_count"]),
                "within_tolerance_rate": round(bucket["within_tolerance_count"] / bucket["expected_count"], 4)
                if bucket["expected_count"]
                else 1.0,
                "average_start_error": _safe_average(bucket["start_errors"]),
                "average_end_error": _safe_average(bucket["end_errors"]),
            }
            for event_type, bucket in sorted(by_event_type.items())
        }
    }


def _build_clip_summaries(validated_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in validated_rows:
        layer_scores = {
            "detection": float(row.get("detection_metrics", {}).get("recall", 1.0)),
            "runtime": float(row.get("runtime_event_metrics", {}).get("recall", 1.0)),
            "fusion": float(row.get("fusion_metrics", {}).get("recall", 1.0)),
            "boundary": float(row.get("boundary_metrics", {}).get("within_tolerance_rate", 1.0)),
        }
        failed_first = next((layer for layer in ("detection", "runtime", "fusion", "boundary") if layer_scores[layer] < 1.0), "none")
        worst_layer = min(layer_scores, key=layer_scores.get)
        rows.append(
            {
                "clip_id": row.get("clip_id"),
                "game": row.get("game"),
                "source": row.get("source"),
                "resolved_source": row.get("resolved_source"),
                "coverage_tags": row.get("coverage_tags", []),
                "failed_first": failed_first,
                "worst_layer": worst_layer,
                "worst_score": round(layer_scores[worst_layer], 4),
                "detection_recall": layer_scores["detection"],
                "runtime_recall": layer_scores["runtime"],
                "fusion_recall": layer_scores["fusion"],
                "boundary_within_tolerance_rate": layer_scores["boundary"],
            }
        )
    return rows


FIRST_PASS_COVERAGE_TAGS = (
    "identity_heavy",
    "medal_heavy",
    "ability_or_equipment_visibility",
    "gated_confirmation",
    "synergy_positive",
    "low_signal_negative",
)


def _build_coverage_summary(validated_rows: list[dict[str, Any]]) -> dict[str, Any]:
    clips_by_behavior: dict[str, list[str]] = {}
    fused_event_types_covered: dict[str, int] = {}
    required_signal_types_covered: dict[str, int] = {}
    for row in validated_rows:
        clip_ref = str(row.get("clip_id") or row.get("source") or "unknown")
        for tag in row.get("coverage_tags", []):
            clips_by_behavior.setdefault(str(tag), []).append(clip_ref)
        for expected in row.get("expected_fused_events", []):
            event_type = str(expected.get("event_type", "")).strip()
            if event_type:
                _bump(fused_event_types_covered, event_type)
            for signal_type in expected.get("required_signal_types", []):
                normalized = str(signal_type).strip()
                if normalized:
                    _bump(required_signal_types_covered, normalized)
    return {
        "clips_by_behavior": {key: sorted(value) for key, value in sorted(clips_by_behavior.items())},
        "missing_behavior_tags": [tag for tag in FIRST_PASS_COVERAGE_TAGS if tag not in clips_by_behavior],
        "fused_event_types_covered": fused_event_types_covered,
        "required_signal_types_covered": required_signal_types_covered,
    }


def _semantic_target_field_name(row: dict[str, Any]) -> str:
    for field in ("entity_id", "ability_id", "equipment_id", "event_row_id"):
        if field in row:
            return field
    return "unknown"


def _bump(bucket: dict[str, int], key: str) -> None:
    bucket[key] = bucket.get(key, 0) + 1


def load_sidecar_index(root: str | Path, glob_pattern: str) -> dict[tuple[str, str], dict[str, Any]]:
    resolved = _resolve_path(root)
    index: dict[tuple[str, str], dict[str, Any]] = {}
    if not resolved.exists() or not resolved.is_dir():
        return index
    for path in sorted(resolved.rglob(glob_pattern)):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        key = (str(payload.get("game", "")), str(payload.get("source", "")))
        if key[0] and key[1]:
            index[key] = payload
    return index


def _write_validation_debug_bundle(debug_root: Path, report: dict[str, Any]) -> None:
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / DEFAULT_OUTPUT_NAME).write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(debug_root / "per_clip_results.csv", _flatten_clip_results(report))
    (debug_root / "warnings.json").write_text(json.dumps(report.get("warnings", []), indent=2), encoding="utf-8")


def _write_replay_debug_bundle(debug_root: Path, report: dict[str, Any]) -> None:
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "fusion_rule_replay_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(debug_root / "per_clip_comparisons.csv", list(report["comparison"].get("per_clip_comparisons", [])))
    (debug_root / "warnings.json").write_text(json.dumps(report.get("warnings", []), indent=2), encoding="utf-8")


def _write_template_replay_debug_bundle(debug_root: Path, report: dict[str, Any]) -> None:
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "template_threshold_replay_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(debug_root / "per_clip_comparisons.csv", list(report["comparison"].get("per_clip_comparisons", [])))
    (debug_root / "warnings.json").write_text(json.dumps(report.get("warnings", []), indent=2), encoding="utf-8")


def _write_runtime_rule_replay_debug_bundle(debug_root: Path, report: dict[str, Any]) -> None:
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "runtime_rule_replay_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(debug_root / "per_clip_comparisons.csv", list(report["comparison"].get("per_clip_comparisons", [])))
    (debug_root / "warnings.json").write_text(json.dumps(report.get("warnings", []), indent=2), encoding="utf-8")


def _flatten_clip_results(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in report.get("per_clip_results", []):
        rows.append(
            {
                "status": row.get("status"),
                "game": row.get("game"),
                "source": row.get("source"),
                "detection_recall": row.get("detection_metrics", {}).get("recall"),
                "detection_precision": row.get("detection_metrics", {}).get("precision"),
                "runtime_event_recall": row.get("runtime_event_metrics", {}).get("recall"),
                "fusion_recall": row.get("fusion_metrics", {}).get("recall"),
                "fusion_synergy_applied_accuracy": row.get("fusion_metrics", {}).get("synergy_applied_accuracy"),
                "boundary_within_tolerance_rate": row.get("boundary_metrics", {}).get("within_tolerance_rate"),
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
                    for key, value in row.items()
                }
            )


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        return (Path.cwd() / path).resolve()
    return path.resolve()


def _safe_average(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 4) if values else None


def _safe_median(values: list[float]) -> float | None:
    return round(float(median(values)), 4) if values else None
