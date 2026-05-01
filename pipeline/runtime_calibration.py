from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import median
from typing import Any

from pipeline.runtime_export import merged_scoring_config, score_runtime_clip


REPO_ROOT = Path(__file__).resolve().parent.parent
SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"
DEFAULT_MIN_REVIEWED = 3


def calibrate_runtime_review(
    sidecar_root: str | Path,
    *,
    game: str | None = None,
    scoring_config: dict[str, Any] | None = None,
    output_path: str | Path | None = None,
    min_reviewed: int = DEFAULT_MIN_REVIEWED,
    include_unreviewed: bool = False,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    root = _resolve_root(sidecar_root)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "sidecar_root": str(root),
            "error": "sidecar root does not exist or is not a directory",
        }

    config = merged_scoring_config(scoring_config)
    report = _build_calibration_report(
        root,
        game=game,
        scoring_config=config,
        min_reviewed=min_reviewed,
        include_unreviewed=include_unreviewed,
    )

    if output_path is not None:
        target = _resolve_output_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if debug_output_dir is not None:
        _write_debug_bundle(_resolve_output_path(debug_output_dir), report)

    return report


def _build_calibration_report(
    root: Path,
    *,
    game: str | None,
    scoring_config: dict[str, Any],
    min_reviewed: int,
    include_unreviewed: bool,
) -> dict[str, Any]:
    sidecar_paths = sorted(root.rglob("*.runtime_analysis.json"))
    warnings: list[dict[str, str]] = []
    reviewed_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    skipped_counts = {
        "malformed_json": 0,
        "unsupported_schema_version": 0,
        "failed_analysis": 0,
        "game_filter_mismatch": 0,
        "unreviewed": 0,
    }

    for sidecar_path in sidecar_paths:
        status, row = _review_row_from_sidecar(sidecar_path, game=game, scoring_config=scoring_config)
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
    recommendations = _recommendations(reviewed_rows, approved_rows, rejected_rows, scoring_config, min_reviewed)

    status = "ok"
    ok = True
    if len(reviewed_rows) < min_reviewed:
        status = "insufficient_review_data"
        ok = False
        warnings.append(
            {
                "reason": "insufficient_review_data",
                "detail": f"reviewed sidecar count {len(reviewed_rows)} is below min_reviewed {min_reviewed}",
            }
        )

    report = {
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
    }
    if game is not None:
        report["game_filter"] = game
    return report


def _review_row_from_sidecar(
    sidecar_path: Path,
    *,
    game: str | None,
    scoring_config: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    try:
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "malformed_json", None

    if sidecar.get("schema_version") != SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION:
        return "unsupported_schema_version", None
    if game is not None and sidecar.get("game") != game:
        return "game_filter_mismatch", None
    if not sidecar.get("ok", False):
        return "failed_analysis", None

    review = sidecar.get("runtime_review", {})
    review_status = str(review.get("review_status", "")).strip().lower()
    if review_status not in {"approved", "rejected"}:
        return "unreviewed", _coverage_row(sidecar_path, sidecar, scoring_config)

    return None, _build_review_row(sidecar_path, sidecar, scoring_config, review_status)


def _coverage_row(sidecar_path: Path, sidecar: dict[str, Any], scoring_config: dict[str, Any]) -> dict[str, Any]:
    return _build_review_row(sidecar_path, sidecar, scoring_config, review_status="unreviewed")


def _build_review_row(
    sidecar_path: Path,
    sidecar: dict[str, Any],
    scoring_config: dict[str, Any],
    review_status: str,
) -> dict[str, Any]:
    event_rows = list(sidecar.get("events", {}).get("rows", []))
    detection_rows = list(sidecar.get("matcher", {}).get("confirmed_detections", []))
    score = score_runtime_clip(event_rows, detection_rows, scoring_config)
    matcher_summary = sidecar.get("matcher", {}).get("summary", {})
    event_counts = dict(score["score_breakdown"].get("event_counts", {}))
    event_contributions = dict(score["score_breakdown"].get("event_contributions", {}))

    detection_asset_families = dict(matcher_summary.get("detections_by_asset_family", {}))
    detection_rois = dict(matcher_summary.get("detections_by_roi", {}))
    if not detection_rois:
        detection_rois = _counts_by_key(detection_rows, "roi_ref")
    if not detection_asset_families:
        detection_asset_families = _counts_by_key(detection_rows, "asset_family")

    return {
        "analysis_id": sidecar.get("analysis_id"),
        "game": sidecar.get("game"),
        "source": sidecar.get("source"),
        "sidecar_path": str(sidecar_path.resolve()),
        "review_status": review_status,
        "highlight_score": float(score["highlight_score"]),
        "recommended_action": score["recommended_action"],
        "event_rows": event_rows,
        "detection_rows": detection_rows,
        "event_counts": event_counts,
        "event_contributions": event_contributions,
        "detection_support": float(score["score_breakdown"].get("detection_support", 0.0)),
        "score_reasoning": score["score_reasoning"],
        "event_types": sorted(event_counts.keys()),
        "event_count": len(event_rows),
        "confirmed_detection_count": len(detection_rows),
        "detection_rois": detection_rois,
        "detection_asset_families": detection_asset_families,
    }


def _diagnostics(
    reviewed_rows: list[dict[str, Any]],
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    coverage_rows: list[dict[str, Any]],
    scoring_config: dict[str, Any],
) -> dict[str, Any]:
    threshold_highlight = float(scoring_config["action_thresholds"]["highlight_candidate"])
    threshold_inspect = float(scoring_config["action_thresholds"]["inspect"])
    return {
        "score_distribution": {
            "approved": _score_stats(approved_rows),
            "rejected": _score_stats(rejected_rows),
        },
        "reviewed_clips": [
            {
                "analysis_id": row["analysis_id"],
                "game": row["game"],
                "source": row["source"],
                "review_status": row["review_status"],
                "highlight_score": row["highlight_score"],
                "recommended_action": row["recommended_action"],
                "event_count": row["event_count"],
                "confirmed_detection_count": row["confirmed_detection_count"],
                "event_types": row["event_types"],
            }
            for row in reviewed_rows
        ],
        "action_outcomes": _action_outcomes(reviewed_rows),
        "event_type_incidence": {
            "approved": _event_type_incidence(approved_rows),
            "rejected": _event_type_incidence(rejected_rows),
        },
        "score_contributions": {
            "approved": _mean_contributions(approved_rows),
            "rejected": _mean_contributions(rejected_rows),
        },
        "coverage": {
            "reviewed_clip_count": len(reviewed_rows),
            "coverage_clip_count": len(coverage_rows),
            "event_type_counts": _event_type_incidence(coverage_rows),
            "detection_roi_counts": _merged_counts(coverage_rows, "detection_rois"),
            "detection_asset_family_counts": _merged_counts(coverage_rows, "detection_asset_families"),
        },
        "threshold_diagnostics": {
            "highlight_candidate_threshold": threshold_highlight,
            "inspect_threshold": threshold_inspect,
            "approved_below_highlight_threshold": _count_if(
                approved_rows, lambda row: row["highlight_score"] < threshold_highlight
            ),
            "approved_below_inspect_threshold": _count_if(
                approved_rows, lambda row: row["highlight_score"] < threshold_inspect
            ),
            "rejected_above_highlight_threshold": _count_if(
                rejected_rows, lambda row: row["highlight_score"] >= threshold_highlight
            ),
            "rejected_above_inspect_threshold": _count_if(
                rejected_rows, lambda row: row["highlight_score"] >= threshold_inspect
            ),
        },
        "event_saturation": {
            event_type: _saturation_stats(reviewed_rows, event_type, int(cap))
            for event_type, cap in sorted(scoring_config["event_caps"].items())
        },
    }


def _recommendations(
    reviewed_rows: list[dict[str, Any]],
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    scoring_config: dict[str, Any],
    min_reviewed: int,
) -> dict[str, Any]:
    if len(reviewed_rows) < min_reviewed:
        return {
            "threshold_observations": ["Need more reviewed runtime sidecars before tuning thresholds confidently."],
            "weight_observations": ["Need more reviewed runtime sidecars before tuning event weights confidently."],
            "candidate_threshold_ranges": {},
            "candidate_weight_adjustments": {},
            "data_quality_notes": [
                f"Only {len(reviewed_rows)} reviewed sidecars available; minimum configured sample is {min_reviewed}."
            ],
        }

    threshold_highlight = float(scoring_config["action_thresholds"]["highlight_candidate"])
    threshold_inspect = float(scoring_config["action_thresholds"]["inspect"])
    approved_scores = [row["highlight_score"] for row in approved_rows]
    rejected_scores = [row["highlight_score"] for row in rejected_rows]
    median_approved = _safe_median(approved_scores)
    median_rejected = _safe_median(rejected_scores)

    threshold_observations: list[str] = []
    if _count_if(approved_rows, lambda row: row["highlight_score"] < threshold_highlight) > 0:
        threshold_observations.append("Some approved clips are falling below the current highlight threshold.")
    else:
        threshold_observations.append("Current highlight threshold is not suppressing reviewed approved clips.")
    if _count_if(rejected_rows, lambda row: row["highlight_score"] >= threshold_highlight) > 0:
        threshold_observations.append("Some rejected clips are clearing the current highlight threshold.")
    else:
        threshold_observations.append("Current highlight threshold is filtering reviewed rejected clips cleanly.")
    if _count_if(approved_rows, lambda row: row["highlight_score"] < threshold_inspect) > 0:
        threshold_observations.append("Current inspect threshold appears too high for some approved clips.")
    else:
        threshold_observations.append("Current inspect threshold is not excluding reviewed approved clips.")

    candidate_threshold_ranges = {
        "inspect": _threshold_range(median_rejected, median_approved, threshold_inspect),
        "highlight_candidate": _threshold_range(median_approved, 1.0, threshold_highlight),
    }

    weight_observations: list[str] = []
    candidate_weight_adjustments: dict[str, str] = {}
    approved_incidence = _event_type_incidence(approved_rows)
    rejected_incidence = _event_type_incidence(rejected_rows)
    for event_type in sorted(scoring_config["event_weights"].keys()):
        approved_hits = int(approved_incidence.get(event_type, 0))
        rejected_hits = int(rejected_incidence.get(event_type, 0))
        if approved_hits > rejected_hits:
            candidate_weight_adjustments[event_type] = "increase" if rejected_hits == 0 else "hold"
            if rejected_hits == 0:
                weight_observations.append(f"{event_type} appears concentrated in approved clips and may be underweighted.")
            else:
                weight_observations.append(f"{event_type} is more common in approved clips than rejected clips.")
        elif rejected_hits > approved_hits:
            candidate_weight_adjustments[event_type] = "decrease"
            weight_observations.append(f"{event_type} appears more often in rejected clips and may be overweighted.")
        else:
            candidate_weight_adjustments[event_type] = "hold"
            weight_observations.append(f"{event_type} is not separating reviewed outcomes clearly yet.")

    data_quality_notes: list[str] = []
    if not approved_rows or not rejected_rows:
        data_quality_notes.append("Reviewed data is class-imbalanced; one review outcome is missing.")
    if abs(len(approved_rows) - len(rejected_rows)) > max(1, len(reviewed_rows) // 2):
        data_quality_notes.append("Reviewed runtime sidecars are meaningfully imbalanced between approved and rejected.")
    if not any(row["event_count"] > 0 for row in reviewed_rows):
        data_quality_notes.append("Reviewed sidecars contain no mapped events; score tuning will not be meaningful.")

    return {
        "threshold_observations": threshold_observations,
        "weight_observations": weight_observations,
        "candidate_threshold_ranges": candidate_threshold_ranges,
        "candidate_weight_adjustments": candidate_weight_adjustments,
        "data_quality_notes": data_quality_notes,
    }


def _score_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [float(row["highlight_score"]) for row in rows]
    return {
        "count": len(scores),
        "average": round(sum(scores) / len(scores), 4) if scores else 0.0,
        "median": _safe_median(scores),
        "min": round(min(scores), 4) if scores else 0.0,
        "max": round(max(scores), 4) if scores else 0.0,
    }


def _action_outcomes(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for row in rows:
        action = str(row["recommended_action"])
        status = str(row["review_status"])
        bucket = summary.setdefault(action, {"approved": 0, "rejected": 0})
        if status in bucket:
            bucket[status] += 1
    for action in ("highlight_candidate", "inspect", "skip"):
        summary.setdefault(action, {"approved": 0, "rejected": 0})
    return summary


def _event_type_incidence(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for event_type, count in row.get("event_counts", {}).items():
            counts[str(event_type)] = counts.get(str(event_type), 0) + int(count)
    return counts


def _mean_contributions(rows: list[dict[str, Any]]) -> dict[str, float]:
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    detection_support_total = 0.0
    for row in rows:
        for event_type, value in row.get("event_contributions", {}).items():
            totals[str(event_type)] = totals.get(str(event_type), 0.0) + float(value)
            counts[str(event_type)] = counts.get(str(event_type), 0) + 1
        detection_support_total += float(row.get("detection_support", 0.0))
    means = {
        event_type: round(totals[event_type] / max(counts[event_type], 1), 4)
        for event_type in sorted(totals.keys())
    }
    means["detection_support"] = round(detection_support_total / max(len(rows), 1), 4) if rows else 0.0
    return means


def _merged_counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    merged: dict[str, int] = {}
    for row in rows:
        for item_key, value in row.get(key, {}).items():
            merged[str(item_key)] = merged.get(str(item_key), 0) + int(value)
    return merged


def _saturation_stats(rows: list[dict[str, Any]], event_type: str, cap: int) -> dict[str, Any]:
    raw_counts = [int(row.get("event_counts", {}).get(event_type, 0)) for row in rows]
    capped_hits = sum(1 for count in raw_counts if count >= cap and cap > 0)
    return {
        "cap": cap,
        "max_raw_count": max(raw_counts) if raw_counts else 0,
        "rows_at_or_above_cap": capped_hits,
    }


def _threshold_range(low_value: float, high_value: float, current: float) -> dict[str, float]:
    floor = round(max(0.0, min(low_value, current)), 4)
    ceiling = round(min(1.0, max(high_value, current)), 4)
    return {"current": round(current, 4), "trial_min": floor, "trial_max": ceiling}


def _count_if(rows: list[dict[str, Any]], predicate: Any) -> int:
    return sum(1 for row in rows if predicate(row))


def _safe_median(values: list[float]) -> float:
    return round(float(median(values)), 4) if values else 0.0


def _counts_by_key(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        counts[str(value)] = counts.get(str(value), 0) + 1
    return counts


def _resolve_root(sidecar_root: str | Path) -> Path:
    path = Path(sidecar_root).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _resolve_output_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _write_debug_bundle(debug_root: Path, report: dict[str, Any]) -> None:
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "runtime_calibration_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(debug_root / "reviewed_clips.csv", _reviewed_clip_rows(report))
    _write_csv(debug_root / "event_diagnostics.csv", _event_diagnostic_rows(report))
    _write_csv(debug_root / "score_buckets.csv", _score_bucket_rows(report))
    (debug_root / "warnings.json").write_text(json.dumps(report.get("warnings", []), indent=2), encoding="utf-8")


def _reviewed_clip_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = report.get("diagnostics", {}).get("reviewed_clips", [])
    return list(rows) if isinstance(rows, list) else []


def _event_diagnostic_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics = report.get("diagnostics", {})
    approved = diagnostics.get("event_type_incidence", {}).get("approved", {})
    rejected = diagnostics.get("event_type_incidence", {}).get("rejected", {})
    rows: list[dict[str, Any]] = []
    for event_type in sorted(set(approved) | set(rejected)):
        rows.append(
            {
                "event_type": event_type,
                "approved_count": approved.get(event_type, 0),
                "rejected_count": rejected.get(event_type, 0),
            }
        )
    return rows


def _score_bucket_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    outcomes = report.get("diagnostics", {}).get("action_outcomes", {})
    rows: list[dict[str, Any]] = []
    for action in ("highlight_candidate", "inspect", "skip"):
        bucket = outcomes.get(action, {})
        rows.append(
            {
                "recommended_action": action,
                "approved_count": bucket.get("approved", 0),
                "rejected_count": bucket.get("rejected", 0),
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(_flatten_row_for_csv(row))


def _flatten_row_for_csv(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
        for key, value in row.items()
    }
