from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pipeline.evaluation_fixtures import load_evaluation_fixture_manifest
from pipeline.runtime_export import merged_scoring_config, score_runtime_clip


FIXTURE_SIDECAR_COMPARISON_SCHEMA_VERSION = "fixture_sidecar_comparison_v1"
SUPPORTED_PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"
SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"
DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "outputs" / "fixture_sidecar_comparisons"
ACTION_RANK = {
    "skip": 0,
    "inspect": 1,
    "highlight_candidate": 2,
    "download_candidate": 2,
}
LAYER_CHOICES = {"proxy", "runtime", "fused", "all"}


def compare_fixture_sidecars(
    fixture_manifest: str | Path,
    *,
    baseline_sidecar_root: str | Path,
    trial_sidecar_root: str | Path,
    artifact_layer: str = "all",
    game: str | None = None,
    output_path: str | Path | None = None,
    runtime_scoring_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = load_evaluation_fixture_manifest(fixture_manifest)
    baseline_root = _resolve_path(baseline_sidecar_root)
    trial_root = _resolve_path(trial_sidecar_root)
    layer = str(artifact_layer or "all").strip().lower()
    if layer not in LAYER_CHOICES:
        return {
            "ok": False,
            "status": "invalid_artifact_layer",
            "error": "artifact layer must be proxy, runtime, fused, or all",
        }
    if not baseline_root.exists() or not baseline_root.is_dir():
        return {
            "ok": False,
            "status": "invalid_baseline_sidecar_root",
            "baseline_sidecar_root": str(baseline_root),
            "error": "baseline sidecar root does not exist or is not a directory",
        }
    if not trial_root.exists() or not trial_root.is_dir():
        return {
            "ok": False,
            "status": "invalid_trial_sidecar_root",
            "trial_sidecar_root": str(trial_root),
            "error": "trial sidecar root does not exist or is not a directory",
        }

    runtime_scoring = merged_scoring_config(runtime_scoring_config)
    warnings: list[dict[str, Any]] = []
    fixture_rows: list[dict[str, Any]] = []
    layers = ["proxy", "runtime", "fused"] if layer == "all" else [layer]

    for fixture in list(manifest.get("fixtures", [])):
        fixture_id = str(fixture["fixture_id"])
        for layer_name in layers:
            row = _compare_fixture_layer(
                fixture,
                layer_name=layer_name,
                baseline_root=baseline_root,
                trial_root=trial_root,
                game=game,
                runtime_scoring=runtime_scoring,
                warnings=warnings,
            )
            fixture_rows.append(row)

    layer_summaries = _layer_summaries(fixture_rows)
    recommendation = _recommendation(fixture_rows)
    ok = recommendation["decision"] != "inconclusive" or any(
        row["review_status"] in {"approved", "rejected"} for row in fixture_rows
    )
    status = "ok" if ok else "insufficient_review_coverage"
    result = {
        "ok": ok,
        "status": status,
        "schema_version": FIXTURE_SIDECAR_COMPARISON_SCHEMA_VERSION,
        "fixture_manifest_path": str(_resolve_path(fixture_manifest)),
        "baseline_sidecar_root": str(baseline_root),
        "trial_sidecar_root": str(trial_root),
        "artifact_layer": layer,
        "fixture_count": int(manifest.get("fixture_count", len(manifest.get("fixtures", [])))),
        "comparison_row_count": len(fixture_rows),
        "comparison": {
            "fixture_rows": fixture_rows,
            "layer_summaries": layer_summaries,
        },
        "recommendation": recommendation,
        "warnings": warnings,
    }
    if game is not None:
        result["game_filter"] = game

    if output_path is not None:
        report_path = _resolve_path(output_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        csv_path = report_path.with_suffix(".csv")
        warnings_path = report_path.with_suffix(".warnings.json")
        _write_csv(csv_path, fixture_rows)
        warnings_path.write_text(json.dumps(warnings, indent=2), encoding="utf-8")
        result["report_path"] = str(report_path)
        result["csv_path"] = str(csv_path)
        result["warnings_path"] = str(warnings_path)
    return result


def _compare_fixture_layer(
    fixture: dict[str, Any],
    *,
    layer_name: str,
    baseline_root: Path,
    trial_root: Path,
    game: str | None,
    runtime_scoring: dict[str, Any],
    warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    fixture_id = str(fixture["fixture_id"])
    baseline_sidecar = _resolve_fixture_sidecar(
        baseline_root,
        fixture,
        layer_name=layer_name,
        trial_name="baseline",
        warnings=warnings,
    )
    trial_sidecar = _resolve_fixture_sidecar(
        trial_root,
        fixture,
        layer_name=layer_name,
        trial_name="trial",
        warnings=warnings,
    )
    expected = bool(dict(fixture.get("expected_artifacts", {})).get(layer_name, False))

    baseline = _load_fixture_layer_payload(
        baseline_sidecar,
        layer_name=layer_name,
        game=game,
        runtime_scoring=runtime_scoring,
        warnings=warnings,
        fixture_id=fixture_id,
        trial_name="baseline",
    )
    trial = _load_fixture_layer_payload(
        trial_sidecar,
        layer_name=layer_name,
        game=game,
        runtime_scoring=runtime_scoring,
        warnings=warnings,
        fixture_id=fixture_id,
        trial_name="trial",
    )

    coverage_status = _coverage_status(bool(baseline), bool(trial))
    review_status = _effective_review_status(baseline, trial)
    recommendation_signal = _recommendation_signal(review_status, baseline, trial)
    row = {
        "fixture_id": fixture_id,
        "label": str(fixture.get("label", fixture_id)),
        "artifact_layer": layer_name,
        "expected_review_outcome": str(fixture.get("expected_review_outcome", "")),
        "baseline_present": bool(baseline),
        "trial_present": bool(trial),
        "expected_artifact": expected,
        "coverage_status": coverage_status,
        "review_status": review_status,
        "baseline_sidecar_path": str(baseline_sidecar) if baseline_sidecar is not None else None,
        "trial_sidecar_path": str(trial_sidecar) if trial_sidecar is not None else None,
        "baseline_score": baseline.get("score") if baseline else None,
        "trial_score": trial.get("score") if trial else None,
        "score_delta": _score_delta(baseline, trial),
        "baseline_action": baseline.get("action") if baseline else None,
        "trial_action": trial.get("action") if trial else None,
        "baseline_review_status": baseline.get("review_status") if baseline else None,
        "trial_review_status": trial.get("review_status") if trial else None,
        "baseline_shortlist": baseline.get("shortlist", []) if baseline else [],
        "trial_shortlist": trial.get("shortlist", []) if trial else [],
        "shortlist_changed": bool(baseline and trial and baseline.get("shortlist", []) != trial.get("shortlist", [])),
        "baseline_rerank_order": baseline.get("rerank_order", []) if baseline else [],
        "trial_rerank_order": trial.get("rerank_order", []) if trial else [],
        "rerank_changed": bool(baseline and trial and baseline.get("rerank_order", []) != trial.get("rerank_order", [])),
        "stage_latency_deltas": _latency_deltas(baseline, trial),
        "recommendation_signal": recommendation_signal,
    }
    return row


def _resolve_fixture_sidecar(
    root: Path,
    fixture: dict[str, Any],
    *,
    layer_name: str,
    trial_name: str,
    warnings: list[dict[str, Any]],
) -> Path | None:
    fixture_id = str(fixture["fixture_id"])
    artifact_refs = dict(fixture.get("artifact_refs", {}))
    ref_key = {
        "proxy": "proxy_sidecar",
        "runtime": "runtime_sidecar",
        "fused": "fused_sidecar",
    }[layer_name]
    candidates: list[Path] = []
    ref_value = str(artifact_refs.get(ref_key, "")).strip()
    if ref_value:
        ref_path = Path(ref_value)
        candidates.append(root / ref_path)
        candidates.append(root / ref_path.name)
    candidates.append(root / _default_sidecar_name(fixture_id, layer_name))
    basename = _default_sidecar_name(fixture_id, layer_name)
    for path in candidates:
        resolved = path.expanduser()
        if resolved.exists() and resolved.is_file():
            return resolved.resolve()
    matches = sorted(root.rglob(basename))
    if matches:
        return matches[0].resolve()
    if ref_value:
        ref_name_matches = sorted(root.rglob(Path(ref_value).name))
        if ref_name_matches:
            return ref_name_matches[0].resolve()
    warnings.append(
        {
            "fixture_id": fixture_id,
            "artifact_layer": layer_name,
            "trial_name": trial_name,
            "reason": "missing_artifact",
        }
    )
    return None


def _load_fixture_layer_payload(
    sidecar_path: Path | None,
    *,
    layer_name: str,
    game: str | None,
    runtime_scoring: dict[str, Any],
    warnings: list[dict[str, Any]],
    fixture_id: str,
    trial_name: str,
) -> dict[str, Any] | None:
    if sidecar_path is None:
        return None
    try:
        payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        warnings.append(
            {
                "fixture_id": fixture_id,
                "artifact_layer": layer_name,
                "trial_name": trial_name,
                "path": str(sidecar_path),
                "reason": "malformed_json",
            }
        )
        return None
    schema_key = str(payload.get("schema_version", "")).strip()
    expected_schema = {
        "proxy": SUPPORTED_PROXY_SCAN_SCHEMA_VERSION,
        "runtime": SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION,
        "fused": SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION,
    }[layer_name]
    if schema_key != expected_schema:
        warnings.append(
            {
                "fixture_id": fixture_id,
                "artifact_layer": layer_name,
                "trial_name": trial_name,
                "path": str(sidecar_path),
                "reason": "unsupported_schema_version",
            }
        )
        return None
    if game is not None and str(payload.get("game", "")).strip() != game:
        warnings.append(
            {
                "fixture_id": fixture_id,
                "artifact_layer": layer_name,
                "trial_name": trial_name,
                "path": str(sidecar_path),
                "reason": "game_filter_mismatch",
            }
        )
        return None
    if layer_name == "proxy":
        return _proxy_payload_summary(payload, sidecar_path)
    if layer_name == "runtime":
        return _runtime_payload_summary(payload, sidecar_path, runtime_scoring)
    return _fused_payload_summary(payload, sidecar_path)


def _proxy_payload_summary(payload: dict[str, Any], sidecar_path: Path) -> dict[str, Any]:
    windows = list(payload.get("windows", []))
    top_window = windows[0] if windows else {}
    hf_metadata = (
        dict(payload.get("source_results", {}).get("hf_multimodal", {}).get("metadata", {}))
        if isinstance(payload.get("source_results", {}), dict)
        else {}
    )
    structured = dict(hf_metadata.get("structured_outputs", {}))
    stages = dict(hf_metadata.get("stages", {}))
    return {
        "sidecar_path": str(sidecar_path),
        "score": _as_float(top_window.get("proxy_score")),
        "action": str(top_window.get("recommended_action", "skip")),
        "review_status": str(payload.get("proxy_review", {}).get("review_status", "")).strip().lower() or None,
        "shortlist": [
            _span_key(row.get("start_seconds"), row.get("end_seconds"))
            for row in list(structured.get("shortlisted_candidates", []))
            if isinstance(row, dict)
        ],
        "rerank_order": [
            _span_key(row.get("start_seconds"), row.get("end_seconds"))
            for row in list(structured.get("reranked_candidates", []))
            if isinstance(row, dict)
        ],
        "stage_latencies": {
            str(name): _as_float(dict(stage).get("duration_ms"), default=None)
            for name, stage in stages.items()
            if isinstance(stage, dict)
        },
    }


def _runtime_payload_summary(
    payload: dict[str, Any],
    sidecar_path: Path,
    runtime_scoring: dict[str, Any],
) -> dict[str, Any]:
    event_rows = list(payload.get("events", {}).get("rows", []))
    detection_rows = list(payload.get("matcher", {}).get("confirmed_detections", []))
    score = score_runtime_clip(event_rows, detection_rows, runtime_scoring)
    return {
        "sidecar_path": str(sidecar_path),
        "score": _as_float(score.get("highlight_score")),
        "action": str(score.get("recommended_action", "skip")),
        "review_status": str(payload.get("runtime_review", {}).get("review_status", "")).strip().lower() or None,
        "shortlist": [],
        "rerank_order": [],
        "stage_latencies": {},
    }


def _fused_payload_summary(payload: dict[str, Any], sidecar_path: Path) -> dict[str, Any]:
    events = [row for row in list(payload.get("fused_events", [])) if isinstance(row, dict)]
    top_event = sorted(
        events,
        key=lambda row: (-_as_float(row.get("final_score")), _as_float(row.get("start_timestamp"))),
    )[0] if events else {}
    review_status = _fused_review_status(payload)
    return {
        "sidecar_path": str(sidecar_path),
        "score": _as_float(top_event.get("final_score")),
        "action": "highlight_candidate" if events else "skip",
        "review_status": review_status,
        "shortlist": [],
        "rerank_order": [],
        "stage_latencies": {},
    }


def _fused_review_status(payload: dict[str, Any]) -> str | None:
    events = dict(payload.get("fused_review", {}).get("events", {}))
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


def _coverage_status(baseline_present: bool, trial_present: bool) -> str:
    if baseline_present and trial_present:
        return "both"
    if baseline_present:
        return "baseline_only"
    if trial_present:
        return "trial_only"
    return "missing"


def _effective_review_status(
    baseline: dict[str, Any] | None,
    trial: dict[str, Any] | None,
) -> str | None:
    statuses = [
        str(candidate.get("review_status", "")).strip().lower()
        for candidate in (baseline, trial)
        if isinstance(candidate, dict) and str(candidate.get("review_status", "")).strip()
    ]
    normalized = {status for status in statuses if status in {"approved", "rejected"}}
    if not statuses:
        return None
    if len(normalized) == 1 and len(set(statuses)) == 1:
        return next(iter(normalized))
    if len(normalized) == 1 and len(statuses) == 1:
        return next(iter(normalized))
    return "mixed"


def _recommendation_signal(
    review_status: str | None,
    baseline: dict[str, Any] | None,
    trial: dict[str, Any] | None,
) -> str:
    if baseline is None and trial is None:
        return "coverage_gap"
    if baseline is None or trial is None:
        return "coverage_gap"
    if review_status not in {"approved", "rejected"}:
        return "review_mismatch" if review_status == "mixed" else "unreviewed"
    baseline_rank = ACTION_RANK.get(str(baseline.get("action", "")).strip(), 0)
    trial_rank = ACTION_RANK.get(str(trial.get("action", "")).strip(), 0)
    baseline_score = _as_float(baseline.get("score"))
    trial_score = _as_float(trial.get("score"))
    if review_status == "approved":
        if trial_rank > baseline_rank:
            return "trial_better"
        if trial_rank < baseline_rank:
            return "current_better"
        if trial_score > baseline_score:
            return "trial_better"
        if trial_score < baseline_score:
            return "current_better"
    else:
        if trial_rank < baseline_rank:
            return "trial_better"
        if trial_rank > baseline_rank:
            return "current_better"
        if trial_score < baseline_score:
            return "trial_better"
        if trial_score > baseline_score:
            return "current_better"
    return "neutral"


def _score_delta(baseline: dict[str, Any] | None, trial: dict[str, Any] | None) -> float | None:
    if baseline is None or trial is None:
        return None
    return round(_as_float(trial.get("score")) - _as_float(baseline.get("score")), 4)


def _latency_deltas(
    baseline: dict[str, Any] | None,
    trial: dict[str, Any] | None,
) -> dict[str, float]:
    if baseline is None or trial is None:
        return {}
    baseline_latencies = dict(baseline.get("stage_latencies", {}))
    trial_latencies = dict(trial.get("stage_latencies", {}))
    deltas: dict[str, float] = {}
    for key in sorted(set(baseline_latencies) | set(trial_latencies)):
        baseline_value = baseline_latencies.get(key)
        trial_value = trial_latencies.get(key)
        if baseline_value is None or trial_value is None:
            continue
        deltas[key] = round(_as_float(trial_value) - _as_float(baseline_value), 4)
    return deltas


def _layer_summaries(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for layer_name in ["proxy", "runtime", "fused"]:
        layer_rows = [row for row in rows if row["artifact_layer"] == layer_name]
        summaries[layer_name] = {
            "row_count": len(layer_rows),
            "both_present_count": sum(1 for row in layer_rows if row["coverage_status"] == "both"),
            "coverage_gap_count": sum(1 for row in layer_rows if row["recommendation_signal"] == "coverage_gap"),
            "trial_better_count": sum(1 for row in layer_rows if row["recommendation_signal"] == "trial_better"),
            "current_better_count": sum(1 for row in layer_rows if row["recommendation_signal"] == "current_better"),
            "mixed_review_count": sum(1 for row in layer_rows if row["review_status"] == "mixed"),
            "reviewed_count": sum(1 for row in layer_rows if row["review_status"] in {"approved", "rejected"}),
        }
    return summaries


def _recommendation(rows: list[dict[str, Any]]) -> dict[str, str]:
    reviewed_rows = [row for row in rows if row["review_status"] in {"approved", "rejected"} and row["coverage_status"] == "both"]
    if len(reviewed_rows) < 2:
        return {
            "decision": "inconclusive",
            "reason": "Reviewed fixture coverage is too sparse to recommend a change.",
        }
    trial_better = sum(1 for row in reviewed_rows if row["recommendation_signal"] == "trial_better")
    current_better = sum(1 for row in reviewed_rows if row["recommendation_signal"] == "current_better")
    if trial_better > current_better and trial_better >= 2:
        return {
            "decision": "prefer_trial",
            "reason": "Trial sidecars improve more reviewed fixtures than baseline.",
        }
    if current_better > trial_better and current_better >= 2:
        return {
            "decision": "keep_current",
            "reason": "Baseline sidecars retain better reviewed fixture behavior than trial.",
        }
    return {
        "decision": "inconclusive",
        "reason": "Reviewed fixture deltas are mixed or too small to support a change.",
    }


def _default_sidecar_name(fixture_id: str, layer_name: str) -> str:
    return {
        "proxy": f"{fixture_id}.proxy_scan.json",
        "runtime": f"{fixture_id}.runtime_analysis.json",
        "fused": f"{fixture_id}.fused_analysis.json",
    }[layer_name]


def _span_key(start_seconds: Any, end_seconds: Any) -> str:
    return f"{_as_float(start_seconds):.4f}-{_as_float(end_seconds):.4f}"


def _as_float(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "fixture_id",
                "artifact_layer",
                "baseline_present",
                "trial_present",
                "review_status",
                "baseline_score",
                "trial_score",
                "score_delta",
                "baseline_action",
                "trial_action",
                "recommendation_signal",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "fixture_id": row["fixture_id"],
                    "artifact_layer": row["artifact_layer"],
                    "baseline_present": row["baseline_present"],
                    "trial_present": row["trial_present"],
                    "review_status": row["review_status"],
                    "baseline_score": row["baseline_score"],
                    "trial_score": row["trial_score"],
                    "score_delta": row["score_delta"],
                    "baseline_action": row["baseline_action"],
                    "trial_action": row["trial_action"],
                    "recommendation_signal": row["recommendation_signal"],
                }
            )
