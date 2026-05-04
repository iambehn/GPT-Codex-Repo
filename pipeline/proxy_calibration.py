from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import median
from typing import Any


SUPPORTED_PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
DEFAULT_MIN_REVIEWED = 3
HF_SIGNAL_SOURCES = (
    "hf_shot_boundary",
    "hf_transcript_salience",
    "hf_semantic_match",
    "hf_keyframe_novelty",
    "hf_rerank_highlight",
)
HF_STAGE_TO_SIGNAL = {
    "proposal": "hf_shot_boundary",
    "transcript": "hf_transcript_salience",
    "semantic": "hf_semantic_match",
    "novelty": "hf_keyframe_novelty",
    "rerank": "hf_rerank_highlight",
}


def calibrate_proxy_review(
    sidecar_root: str | Path,
    *,
    game: str | None = None,
    scoring_config: dict[str, Any] | None = None,
    output_path: str | Path | None = None,
    min_reviewed: int = DEFAULT_MIN_REVIEWED,
    include_unreviewed: bool = False,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    root = _resolve_path(sidecar_root)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "sidecar_root": str(root),
            "error": "sidecar root does not exist or is not a directory",
        }

    config = _merged_scoring_config(scoring_config)
    report = _build_report(
        root,
        game=game,
        scoring_config=config,
        min_reviewed=min_reviewed,
        include_unreviewed=include_unreviewed,
    )

    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if debug_output_dir is not None:
        _write_debug_bundle(_resolve_path(debug_output_dir), report)

    return report


def _build_report(
    root: Path,
    *,
    game: str | None,
    scoring_config: dict[str, Any],
    min_reviewed: int,
    include_unreviewed: bool,
) -> dict[str, Any]:
    sidecar_paths = sorted(root.rglob("*.proxy_scan.json"))
    warnings: list[dict[str, str]] = []
    reviewed_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    skipped_counts = {
        "malformed_json": 0,
        "unsupported_schema_version": 0,
        "game_filter_mismatch": 0,
        "failed_scan": 0,
        "empty_scan": 0,
        "unreviewed": 0,
        "non_hf_source": 0,
    }

    for sidecar_path in sidecar_paths:
        status, row = _review_row_from_sidecar(sidecar_path, game=game)
        if status is None and row is not None:
            reviewed_rows.append(row)
            coverage_rows.append(row)
            continue
        if status == "unreviewed" and row is not None and include_unreviewed:
            coverage_rows.append(row)
        if status is not None:
            warnings.append({"path": str(sidecar_path), "reason": status})
            skipped_counts[status] = skipped_counts.get(status, 0) + 1

    approved_rows = [row for row in reviewed_rows if row["review_status"] == "approved"]
    rejected_rows = [row for row in reviewed_rows if row["review_status"] == "rejected"]
    diagnostics = _diagnostics(reviewed_rows, approved_rows, rejected_rows, coverage_rows, scoring_config)
    recommendations = _recommendations(reviewed_rows, approved_rows, rejected_rows, diagnostics, scoring_config, min_reviewed)

    ok = True
    status = "ok"
    if len(reviewed_rows) < min_reviewed:
        ok = False
        status = "insufficient_review_data"
        warnings.append(
            {
                "reason": "insufficient_review_data",
                "detail": f"reviewed sidecar count {len(reviewed_rows)} is below min_reviewed {min_reviewed}",
            }
        )

    result = {
        "ok": ok,
        "status": status,
        "sidecar_root": str(root),
        "scanned_sidecar_count": len(sidecar_paths),
        "reviewed_sidecar_count": len(reviewed_rows),
        "approved_count": len(approved_rows),
        "rejected_count": len(rejected_rows),
        "skipped_sidecar_count": len(sidecar_paths) - len(reviewed_rows),
        "current_scoring": scoring_config,
        "diagnostics": diagnostics,
        "recommendations": recommendations,
        "warnings": warnings,
        "skipped_counts": skipped_counts,
    }
    if game is not None:
        result["game_filter"] = game
    return result


def _review_row_from_sidecar(
    sidecar_path: Path,
    *,
    game: str | None,
) -> tuple[str | None, dict[str, Any] | None]:
    try:
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "malformed_json", None

    if sidecar.get("schema_version") != SUPPORTED_PROXY_SCAN_SCHEMA_VERSION:
        return "unsupported_schema_version", None
    if game is not None and sidecar.get("game") != game:
        return "game_filter_mismatch", None
    if not sidecar.get("ok", False):
        return "failed_scan", None
    windows = list(sidecar.get("windows", []))
    if not windows:
        return "empty_scan", None
    if not _is_hf_sidecar(sidecar):
        return "non_hf_source", None

    review = sidecar.get("proxy_review", {}) if isinstance(sidecar.get("proxy_review"), dict) else {}
    review_status = str(review.get("review_status", "")).strip().lower()
    row = _build_review_row(sidecar_path, sidecar, review_status=review_status if review_status in {"approved", "rejected"} else "unreviewed")
    if review_status not in {"approved", "rejected"}:
        return "unreviewed", row
    return None, row


def _build_review_row(sidecar_path: Path, sidecar: dict[str, Any], *, review_status: str) -> dict[str, Any]:
    windows = list(sidecar.get("windows", []))
    top_window = windows[0] if windows else {}
    metadata = _hf_metadata(sidecar)
    stage_statuses = dict(metadata.get("stage_statuses", {}))
    stages = dict(metadata.get("stages", {}))
    telemetry = _telemetry_from_metadata(metadata)
    signals = list(top_window.get("signals", []))
    signal_counts: dict[str, int] = {}
    signal_strengths: dict[str, float] = {}
    for signal in signals:
        source = str(signal.get("source", "")).strip()
        if not source:
            continue
        signal_counts[source] = signal_counts.get(source, 0) + 1
        signal_strengths[source] = max(signal_strengths.get(source, 0.0), float(signal.get("strength", 0.0)))

    return {
        "scan_id": sidecar.get("scan_id"),
        "game": sidecar.get("game"),
        "source": sidecar.get("source"),
        "sidecar_path": str(sidecar_path.resolve()),
        "review_status": review_status,
        "proxy_score": float(top_window.get("proxy_score", 0.0)),
        "recommended_action": str(top_window.get("recommended_action", "skip")),
        "signals": signals,
        "signal_counts": signal_counts,
        "signal_strengths": signal_strengths,
        "source_families": list(top_window.get("source_families", [])),
        "source_results_status": str(sidecar.get("source_results", {}).get("hf_multimodal", {}).get("status", "unknown")),
        "stage_statuses": stage_statuses,
        "stage_telemetry": telemetry,
        "stages": stages,
        "pipeline_config": dict(metadata.get("pipeline", {})),
    }


def _diagnostics(
    reviewed_rows: list[dict[str, Any]],
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    coverage_rows: list[dict[str, Any]],
    scoring_config: dict[str, Any],
) -> dict[str, Any]:
    return {
        "score_distribution": {
            "approved": _score_stats(approved_rows),
            "rejected": _score_stats(rejected_rows),
        },
        "reviewed_clips": [
            {
                "scan_id": row["scan_id"],
                "game": row["game"],
                "source": row["source"],
                "review_status": row["review_status"],
                "proxy_score": row["proxy_score"],
                "recommended_action": row["recommended_action"],
                "stage_statuses": row["stage_statuses"],
            }
            for row in reviewed_rows
        ],
        "action_outcomes": _action_outcomes(reviewed_rows),
        "threshold_diagnostics": _threshold_diagnostics(approved_rows, rejected_rows, scoring_config),
        "signal_incidence": {
            "approved": _signal_incidence(approved_rows),
            "rejected": _signal_incidence(rejected_rows),
            "coverage": _signal_incidence(coverage_rows),
        },
        "stage_coverage": {
            "reviewed_clip_count": len(reviewed_rows),
            "coverage_clip_count": len(coverage_rows),
            "stage_status_counts": _stage_status_counts(coverage_rows),
            "stage_latency_ms": _stage_latency_summary(coverage_rows),
        },
    }


def _recommendations(
    reviewed_rows: list[dict[str, Any]],
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    diagnostics: dict[str, Any],
    scoring_config: dict[str, Any],
    min_reviewed: int,
) -> dict[str, Any]:
    threshold_notes: list[str] = []
    for stage_name, thresholds in dict(diagnostics.get("threshold_diagnostics", {})).items():
        threshold_value = float(thresholds.get("threshold", 0.0))
        if int(thresholds.get("approved_below_threshold", 0)) > 0:
            threshold_notes.append(
                f"{stage_name} threshold {threshold_value:.2f} leaves {thresholds['approved_below_threshold']} approved clips below threshold."
            )
        if int(thresholds.get("rejected_at_or_above_threshold", 0)) > 0:
            threshold_notes.append(
                f"{stage_name} threshold {threshold_value:.2f} still allows {thresholds['rejected_at_or_above_threshold']} rejected clips at or above threshold."
            )

    weight_notes: list[str] = []
    stage_weights = dict(scoring_config.get("stage_weights", {}))
    if approved_rows and rejected_rows:
        approved_signals = _signal_incidence(approved_rows)
        rejected_signals = _signal_incidence(rejected_rows)
        for stage_name, signal_source in HF_STAGE_TO_SIGNAL.items():
            approved_count = int(approved_signals.get(signal_source, 0))
            rejected_count = int(rejected_signals.get(signal_source, 0))
            if approved_count > rejected_count:
                weight_notes.append(
                    f"{stage_name} weight is a stronger positive candidate; signal {signal_source} appears more often on approved clips."
                )
            elif rejected_count > approved_count:
                weight_notes.append(
                    f"{stage_name} weight deserves review; signal {signal_source} appears more often on rejected clips."
                )
    data_quality_notes: list[str] = []
    if len(reviewed_rows) < min_reviewed:
        data_quality_notes.append("Not enough reviewed HF proxy sidecars to calibrate confidently.")
    if not approved_rows or not rejected_rows:
        data_quality_notes.append("Reviewed HF proxy sidecars should include both approved and rejected clips.")
    if abs(len(approved_rows) - len(rejected_rows)) > max(1, len(reviewed_rows) // 2):
        data_quality_notes.append("Reviewed HF proxy sidecars are materially imbalanced between approved and rejected.")
    return {
        "threshold_observations": threshold_notes,
        "stage_weight_observations": weight_notes,
        "data_quality_notes": data_quality_notes,
    }


def _threshold_diagnostics(
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    scoring_config: dict[str, Any],
) -> dict[str, Any]:
    thresholds = dict(scoring_config.get("signal_thresholds", {}))
    diagnostics: dict[str, Any] = {}
    for stage_name, signal_source in HF_STAGE_TO_SIGNAL.items():
        threshold = float(thresholds.get(stage_name, 0.0))
        diagnostics[stage_name] = {
            "threshold": threshold,
            "approved_below_threshold": sum(
                1 for row in approved_rows if float(row["signal_strengths"].get(signal_source, 0.0)) < threshold
            ),
            "rejected_at_or_above_threshold": sum(
                1 for row in rejected_rows if float(row["signal_strengths"].get(signal_source, 0.0)) >= threshold
            ),
        }
    return diagnostics


def _action_outcomes(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    summary = {action: {"approved": 0, "rejected": 0} for action in ("download_candidate", "inspect", "skip")}
    for row in rows:
        action = str(row.get("recommended_action", "skip"))
        if action not in summary:
            summary[action] = {"approved": 0, "rejected": 0}
        status = str(row.get("review_status", "rejected"))
        if status in {"approved", "rejected"}:
            summary[action][status] += 1
    return summary


def _signal_incidence(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {source: 0 for source in HF_SIGNAL_SOURCES}
    for row in rows:
        seen = set(str(item.get("source", "")) for item in row.get("signals", []) if item.get("source"))
        for source in counts:
            if source in seen:
                counts[source] += 1
    return counts


def _stage_status_counts(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        for stage_name, status in dict(row.get("stage_statuses", {})).items():
            bucket = counts.setdefault(stage_name, {})
            status_key = str(status or "unknown")
            bucket[status_key] = bucket.get(status_key, 0) + 1
    return counts


def _stage_latency_summary(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    for stage_name in ("shot_detector", "asr", "semantic", "keyframes", "reranker"):
        values = [
            float(dict(row.get("stage_telemetry", {})).get(stage_name, {}).get("duration_ms", 0.0))
            for row in rows
            if dict(row.get("stage_telemetry", {})).get(stage_name, {}).get("duration_ms") is not None
        ]
        if not values:
            summary[stage_name] = {"count": 0, "average_ms": 0.0, "median_ms": 0.0, "max_ms": 0.0}
            continue
        summary[stage_name] = {
            "count": len(values),
            "average_ms": round(sum(values) / len(values), 3),
            "median_ms": round(float(median(values)), 3),
            "max_ms": round(max(values), 3),
        }
    return summary


def _score_stats(rows: list[dict[str, Any]]) -> dict[str, float]:
    scores = [float(row["proxy_score"]) for row in rows]
    if not scores:
        return {"count": 0, "average": 0.0, "median": 0.0, "min": 0.0, "max": 0.0}
    return {
        "count": len(scores),
        "average": round(sum(scores) / len(scores), 4),
        "median": round(float(median(scores)), 4),
        "min": round(min(scores), 4),
        "max": round(max(scores), 4),
    }


def _write_debug_bundle(root: Path, report: dict[str, Any]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "proxy_calibration_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(
        root / "reviewed_clips.csv",
        report.get("diagnostics", {}).get("reviewed_clips", []),
        ("scan_id", "game", "source", "review_status", "proxy_score", "recommended_action"),
    )
    stage_rows: list[dict[str, Any]] = []
    for stage_name, stats in dict(report.get("diagnostics", {}).get("stage_coverage", {}).get("stage_latency_ms", {})).items():
        stage_rows.append({"stage_name": stage_name, **stats})
    _write_csv(root / "stage_latency.csv", stage_rows, ("stage_name", "count", "average_ms", "median_ms", "max_ms"))
    signal_rows: list[dict[str, Any]] = []
    signal_incidence = dict(report.get("diagnostics", {}).get("signal_incidence", {}))
    for label in ("approved", "rejected", "coverage"):
        for signal_source, count in dict(signal_incidence.get(label, {})).items():
            signal_rows.append({"population": label, "signal_source": signal_source, "count": count})
    _write_csv(root / "signal_incidence.csv", signal_rows, ("population", "signal_source", "count"))
    (root / "warnings.json").write_text(json.dumps(report.get("warnings", []), indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: tuple[str, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def _hf_metadata(sidecar: dict[str, Any]) -> dict[str, Any]:
    source_results = sidecar.get("source_results", {})
    if not isinstance(source_results, dict):
        return {}
    hf_result = source_results.get("hf_multimodal", {})
    if not isinstance(hf_result, dict):
        return {}
    metadata = hf_result.get("metadata", {})
    return metadata if isinstance(metadata, dict) else {}


def _telemetry_from_metadata(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    telemetry: dict[str, dict[str, Any]] = {}
    stages = metadata.get("stages", {})
    if not isinstance(stages, dict):
        return telemetry
    for stage_name, payload in stages.items():
        if not isinstance(payload, dict):
            continue
        telemetry[str(stage_name)] = {
            "duration_ms": round(float(payload.get("duration_ms", 0.0)), 3),
            "output_counts": dict(payload.get("output_counts", {})),
        }
    return telemetry


def _is_hf_sidecar(sidecar: dict[str, Any]) -> bool:
    source_results = sidecar.get("source_results", {})
    if isinstance(source_results, dict) and "hf_multimodal" in source_results:
        return True
    windows = list(sidecar.get("windows", []))
    return any("hf_multimodal" in list(window.get("source_families", [])) for window in windows if isinstance(window, dict))


def _merged_scoring_config(config: dict[str, Any] | None) -> dict[str, Any]:
    payload = config if isinstance(config, dict) else {}
    return {
        "shortlist_count": max(1, int(payload.get("shortlist_count", 5))),
        "stage_weights": {
            "proposal": float(payload.get("stage_weights", {}).get("proposal", 0.35)),
            "transcript": float(payload.get("stage_weights", {}).get("transcript", 0.20)),
            "semantic": float(payload.get("stage_weights", {}).get("semantic", 0.25)),
            "novelty": float(payload.get("stage_weights", {}).get("novelty", 0.20)),
        },
        "signal_thresholds": {
            "proposal": float(payload.get("signal_thresholds", {}).get("proposal", 0.55)),
            "transcript": float(payload.get("signal_thresholds", {}).get("transcript", 0.60)),
            "semantic": float(payload.get("signal_thresholds", {}).get("semantic", 0.60)),
            "novelty": float(payload.get("signal_thresholds", {}).get("novelty", 0.60)),
            "rerank": float(payload.get("signal_thresholds", {}).get("rerank", 0.65)),
        },
    }


def _resolve_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path
