from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import median
from typing import Any

from pipeline.runtime_export import merged_scoring_config, score_runtime_clip
from pipeline.simple_yaml import load_yaml_file


SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"
DEFAULT_MIN_REVIEWED = 3


def replay_runtime_scoring(
    sidecar_root: str | Path,
    trial_config_path: str | Path,
    *,
    game: str | None = None,
    current_scoring_config: dict[str, Any] | None = None,
    output_path: str | Path | None = None,
    min_reviewed: int = DEFAULT_MIN_REVIEWED,
    include_unreviewed: bool = False,
    debug_output_dir: str | Path | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    root = _resolve_path(sidecar_root)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "sidecar_root": str(root),
            "error": "sidecar root does not exist or is not a directory",
        }

    trial_path = _resolve_path(trial_config_path)
    if not trial_path.exists() or not trial_path.is_file():
        return {
            "ok": False,
            "sidecar_root": str(root),
            "trial_config_path": str(trial_path),
            "error": "trial config path does not exist or is not a file",
        }

    current_scoring = merged_scoring_config(current_scoring_config)
    trial_payload = _load_trial_config(trial_path)
    if isinstance(trial_payload, dict) and "error" in trial_payload:
        return {
            "ok": False,
            "sidecar_root": str(root),
            "trial_config_path": str(trial_path),
            "error": str(trial_payload["error"]),
        }
    trial_override = _extract_trial_scoring_payload(trial_payload)
    if not isinstance(trial_override, dict):
        return {
            "ok": False,
            "sidecar_root": str(root),
            "trial_config_path": str(trial_path),
            "error": "trial config must be a mapping shaped like runtime_analysis.scoring",
        }
    trial_scoring = merged_scoring_config(trial_override)
    effective_trial_name = str(trial_name or trial_payload.get("trial_name") or trial_payload.get("name") or trial_path.stem)

    report = _build_report(
        root,
        game=game,
        current_scoring=current_scoring,
        trial_scoring=trial_scoring,
        trial_name=effective_trial_name,
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
    current_scoring: dict[str, Any],
    trial_scoring: dict[str, Any],
    trial_name: str,
    min_reviewed: int,
    include_unreviewed: bool,
) -> dict[str, Any]:
    sidecar_paths = sorted(root.rglob("*.runtime_analysis.json"))
    warnings: list[dict[str, str]] = []
    reviewed_rows: list[dict[str, Any]] = []
    coverage_count = 0

    for sidecar_path in sidecar_paths:
        status, row = _comparison_row_from_sidecar(
            sidecar_path,
            game=game,
            current_scoring=current_scoring,
            trial_scoring=trial_scoring,
        )
        if status is None and row is not None:
            reviewed_rows.append(row)
            coverage_count += 1
            continue
        if status == "unreviewed" and include_unreviewed:
            coverage_count += 1
        if status is not None:
            warnings.append({"path": str(sidecar_path), "reason": status})

    approved_rows = [row for row in reviewed_rows if row["review_status"] == "approved"]
    rejected_rows = [row for row in reviewed_rows if row["review_status"] == "rejected"]
    comparison = _comparison(reviewed_rows, approved_rows, rejected_rows, current_scoring, trial_scoring, coverage_count)
    recommendation = _recommendation(reviewed_rows, comparison, min_reviewed)

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
        "trial_name": trial_name,
        "scanned_sidecar_count": len(sidecar_paths),
        "reviewed_sidecar_count": len(reviewed_rows),
        "approved_count": len(approved_rows),
        "rejected_count": len(rejected_rows),
        "skipped_sidecar_count": len(sidecar_paths) - len(reviewed_rows),
        "current_scoring": current_scoring,
        "trial_scoring": trial_scoring,
        "comparison": comparison,
        "recommendation": recommendation,
        "warnings": warnings,
    }
    if game is not None:
        result["game_filter"] = game
    return result


def _comparison_row_from_sidecar(
    sidecar_path: Path,
    *,
    game: str | None,
    current_scoring: dict[str, Any],
    trial_scoring: dict[str, Any],
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

    review_status = str(sidecar.get("runtime_review", {}).get("review_status", "")).strip().lower()
    if review_status not in {"approved", "rejected"}:
        return "unreviewed", None

    event_rows = list(sidecar.get("events", {}).get("rows", []))
    detection_rows = list(sidecar.get("matcher", {}).get("confirmed_detections", []))
    current_score = score_runtime_clip(event_rows, detection_rows, current_scoring)
    trial_score = score_runtime_clip(event_rows, detection_rows, trial_scoring)

    return None, {
        "analysis_id": sidecar.get("analysis_id"),
        "game": sidecar.get("game"),
        "source": sidecar.get("source"),
        "sidecar_path": str(sidecar_path.resolve()),
        "review_status": review_status,
        "current_highlight_score": float(current_score["highlight_score"]),
        "trial_highlight_score": float(trial_score["highlight_score"]),
        "score_delta": round(float(trial_score["highlight_score"]) - float(current_score["highlight_score"]), 4),
        "current_action": str(current_score["recommended_action"]),
        "trial_action": str(trial_score["recommended_action"]),
        "action_changed": str(current_score["recommended_action"]) != str(trial_score["recommended_action"]),
        "current_score_breakdown": current_score["score_breakdown"],
        "trial_score_breakdown": trial_score["score_breakdown"],
        "current_score_reasoning": current_score["score_reasoning"],
        "trial_score_reasoning": trial_score["score_reasoning"],
        "event_types": sorted({str(row.get("event_type", "")) for row in event_rows if row.get("event_type")}),
    }


def _comparison(
    reviewed_rows: list[dict[str, Any]],
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    current_scoring: dict[str, Any],
    trial_scoring: dict[str, Any],
    coverage_count: int,
) -> dict[str, Any]:
    current_actions = _action_outcomes(reviewed_rows, "current_action")
    trial_actions = _action_outcomes(reviewed_rows, "trial_action")
    movements = _movement_summary(reviewed_rows)
    current_sep = _score_separation(approved_rows, rejected_rows, "current_highlight_score")
    trial_sep = _score_separation(approved_rows, rejected_rows, "trial_highlight_score")
    current_targeted = _targeted_errors(approved_rows, rejected_rows, current_scoring, "current_highlight_score")
    trial_targeted = _targeted_errors(approved_rows, rejected_rows, trial_scoring, "trial_highlight_score")
    return {
        "reviewed_comparisons": [
            {
                "analysis_id": row["analysis_id"],
                "game": row["game"],
                "source": row["source"],
                "review_status": row["review_status"],
                "current_highlight_score": row["current_highlight_score"],
                "trial_highlight_score": row["trial_highlight_score"],
                "score_delta": row["score_delta"],
                "current_action": row["current_action"],
                "trial_action": row["trial_action"],
                "action_changed": row["action_changed"],
            }
            for row in reviewed_rows
        ],
        "action_quality": {
            "current": current_actions,
            "trial": trial_actions,
            "delta": _action_delta(current_actions, trial_actions),
        },
        "score_separation": {
            "current": current_sep,
            "trial": trial_sep,
            "delta": {
                "approved_average_delta": round(trial_sep["approved"]["average"] - current_sep["approved"]["average"], 4),
                "rejected_average_delta": round(trial_sep["rejected"]["average"] - current_sep["rejected"]["average"], 4),
                "gap_delta": round(trial_sep["score_gap"] - current_sep["score_gap"], 4),
            },
        },
        "targeted_errors": {
            "current": current_targeted,
            "trial": trial_targeted,
            "delta": {
                key: int(trial_targeted[key]) - int(current_targeted[key])
                for key in sorted(set(current_targeted) | set(trial_targeted))
            },
        },
        "clip_movements": movements,
        "event_weight_effects": _event_weight_effects(reviewed_rows),
        "coverage": {
            "reviewed_clip_count": len(reviewed_rows),
            "coverage_clip_count": coverage_count,
        },
    }


def _recommendation(
    reviewed_rows: list[dict[str, Any]],
    comparison: dict[str, Any],
    min_reviewed: int,
) -> dict[str, Any]:
    if len(reviewed_rows) < min_reviewed:
        return {
            "decision": "inconclusive",
            "reason": "Not enough reviewed runtime sidecars to evaluate a trial config confidently.",
            "supporting_metrics": {"reviewed_sidecar_count": len(reviewed_rows), "min_reviewed": min_reviewed},
            "data_quality_notes": [f"Only {len(reviewed_rows)} reviewed sidecars available."],
            "follow_up": "Gather more reviewed clips before changing runtime scoring.",
        }

    action_current = comparison["action_quality"]["current"]
    action_trial = comparison["action_quality"]["trial"]
    targeted_current = comparison["targeted_errors"]["current"]
    targeted_trial = comparison["targeted_errors"]["trial"]

    current_approved_non_skip = int(action_current["inspect"]["approved"]) + int(action_current["highlight_candidate"]["approved"])
    trial_approved_non_skip = int(action_trial["inspect"]["approved"]) + int(action_trial["highlight_candidate"]["approved"])
    current_approved_highlight = int(action_current["highlight_candidate"]["approved"])
    trial_approved_highlight = int(action_trial["highlight_candidate"]["approved"])
    current_rejected_non_skip = int(action_current["inspect"]["rejected"]) + int(action_current["highlight_candidate"]["rejected"])
    trial_rejected_non_skip = int(action_trial["inspect"]["rejected"]) + int(action_trial["highlight_candidate"]["rejected"])
    current_rejected_highlight = int(action_current["highlight_candidate"]["rejected"])
    trial_rejected_highlight = int(action_trial["highlight_candidate"]["rejected"])

    improved_approved = (trial_approved_non_skip > current_approved_non_skip) or (
        trial_approved_non_skip == current_approved_non_skip and trial_approved_highlight > current_approved_highlight
    )
    worsened_approved = (trial_approved_non_skip < current_approved_non_skip) or (
        trial_approved_non_skip == current_approved_non_skip and trial_approved_highlight < current_approved_highlight
    )
    worsened_rejected = (trial_rejected_non_skip > current_rejected_non_skip) or (
        trial_rejected_highlight > current_rejected_highlight
    )
    improved_rejected = (trial_rejected_non_skip < current_rejected_non_skip) or (
        trial_rejected_highlight < current_rejected_highlight
    )

    if improved_approved and not worsened_rejected:
        decision = "prefer_trial"
        reason = "Trial scoring improves approved clip routing without increasing rejected clips that clear inspect or highlight."
    elif worsened_approved or worsened_rejected:
        decision = "keep_current"
        reason = "Trial scoring worsens approved routing or promotes more rejected clips into inspect/highlight."
    elif improved_rejected and not worsened_approved:
        decision = "prefer_trial"
        reason = "Trial scoring reduces rejected false positives without harming approved clip routing."
    else:
        decision = "inconclusive"
        reason = "Current and trial routing are mixed, so the replay does not clearly support a scoring change."

    data_quality_notes: list[str] = []
    approved_count = sum(1 for row in reviewed_rows if row["review_status"] == "approved")
    rejected_count = sum(1 for row in reviewed_rows if row["review_status"] == "rejected")
    if not approved_count or not rejected_count:
        data_quality_notes.append("Reviewed sidecars are missing one review class.")
    if abs(approved_count - rejected_count) > max(1, len(reviewed_rows) // 2):
        data_quality_notes.append("Reviewed runtime sidecars are materially imbalanced between approved and rejected.")
    if targeted_trial["approved_below_highlight_threshold"] > 0:
        data_quality_notes.append("Some approved clips still remain below the trial highlight threshold.")

    follow_up = "Inspect moved clips and adjust medal weight or action thresholds in the next trial config."
    if decision == "keep_current":
        follow_up = "Tighten the trial config around inspect/highlight thresholds or reduce aggressive event weights."
    elif decision == "inconclusive":
        follow_up = "Try a narrower threshold change or gather more reviewed clips before selecting a config."

    return {
        "decision": decision,
        "reason": reason,
        "supporting_metrics": {
            "current_approved_non_skip": current_approved_non_skip,
            "trial_approved_non_skip": trial_approved_non_skip,
            "current_approved_highlight": current_approved_highlight,
            "trial_approved_highlight": trial_approved_highlight,
            "current_rejected_non_skip": current_rejected_non_skip,
            "trial_rejected_non_skip": trial_rejected_non_skip,
            "current_rejected_highlight": current_rejected_highlight,
            "trial_rejected_highlight": trial_rejected_highlight,
            "current_rejected_above_inspect_threshold": targeted_current["rejected_at_or_above_inspect_threshold"],
            "trial_rejected_above_inspect_threshold": targeted_trial["rejected_at_or_above_inspect_threshold"],
        },
        "data_quality_notes": data_quality_notes,
        "follow_up": follow_up,
    }


def _action_outcomes(rows: list[dict[str, Any]], action_key: str) -> dict[str, dict[str, int]]:
    summary = {action: {"approved": 0, "rejected": 0} for action in ("highlight_candidate", "inspect", "skip")}
    for row in rows:
        action = str(row[action_key])
        status = str(row["review_status"])
        if action in summary and status in summary[action]:
            summary[action][status] += 1
    return summary


def _action_delta(
    current_actions: dict[str, dict[str, int]],
    trial_actions: dict[str, dict[str, int]],
) -> dict[str, dict[str, int]]:
    return {
        action: {
            status: int(trial_actions[action][status]) - int(current_actions[action][status])
            for status in ("approved", "rejected")
        }
        for action in ("highlight_candidate", "inspect", "skip")
    }


def _movement_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    moved_rows: list[dict[str, Any]] = []
    for row in rows:
        movement = f"{row['current_action']} -> {row['trial_action']}"
        counts[movement] = counts.get(movement, 0) + 1
        if row["action_changed"]:
            moved_rows.append(
                {
                    "analysis_id": row["analysis_id"],
                    "source": row["source"],
                    "review_status": row["review_status"],
                    "movement": movement,
                    "score_delta": row["score_delta"],
                }
            )
    return {"counts": counts, "moved_rows": moved_rows}


def _score_separation(
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    score_key: str,
) -> dict[str, Any]:
    approved_scores = [float(row[score_key]) for row in approved_rows]
    rejected_scores = [float(row[score_key]) for row in rejected_rows]
    approved_average = round(sum(approved_scores) / len(approved_scores), 4) if approved_scores else 0.0
    rejected_average = round(sum(rejected_scores) / len(rejected_scores), 4) if rejected_scores else 0.0
    return {
        "approved": {
            "count": len(approved_scores),
            "average": approved_average,
            "median": _safe_median(approved_scores),
        },
        "rejected": {
            "count": len(rejected_scores),
            "average": rejected_average,
            "median": _safe_median(rejected_scores),
        },
        "score_gap": round(approved_average - rejected_average, 4),
    }


def _targeted_errors(
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    scoring_config: dict[str, Any],
    score_key: str,
) -> dict[str, int]:
    inspect_threshold = float(scoring_config["action_thresholds"]["inspect"])
    highlight_threshold = float(scoring_config["action_thresholds"]["highlight_candidate"])
    return {
        "approved_below_inspect_threshold": sum(1 for row in approved_rows if float(row[score_key]) < inspect_threshold),
        "approved_below_highlight_threshold": sum(
            1 for row in approved_rows if float(row[score_key]) < highlight_threshold
        ),
        "rejected_at_or_above_inspect_threshold": sum(
            1 for row in rejected_rows if float(row[score_key]) >= inspect_threshold
        ),
        "rejected_at_or_above_highlight_threshold": sum(
            1 for row in rejected_rows if float(row[score_key]) >= highlight_threshold
        ),
    }


def _event_weight_effects(rows: list[dict[str, Any]]) -> dict[str, Any]:
    totals: dict[str, float] = {}
    moved_count_by_event: dict[str, int] = {}
    for row in rows:
        if not row["action_changed"]:
            continue
        current_contrib = row["current_score_breakdown"].get("event_contributions", {})
        trial_contrib = row["trial_score_breakdown"].get("event_contributions", {})
        for event_type in sorted(set(current_contrib) | set(trial_contrib)):
            delta = round(float(trial_contrib.get(event_type, 0.0)) - float(current_contrib.get(event_type, 0.0)), 4)
            totals[event_type] = round(totals.get(event_type, 0.0) + delta, 4)
            moved_count_by_event[event_type] = moved_count_by_event.get(event_type, 0) + 1
    return {
        "contribution_delta_by_event_type": totals,
        "moved_clip_count_by_event_type": moved_count_by_event,
    }


def _load_trial_config(path: Path) -> dict[str, Any]:
    try:
        if path.suffix.lower() == ".json":
            return json.loads(path.read_text(encoding="utf-8"))
        return load_yaml_file(path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"error": f"failed to load trial config: {exc}"}


def _extract_trial_scoring_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if "runtime_analysis" in payload:
        runtime_analysis = payload.get("runtime_analysis")
        if isinstance(runtime_analysis, dict) and isinstance(runtime_analysis.get("scoring"), dict):
            return dict(runtime_analysis["scoring"])
    if isinstance(payload.get("scoring"), dict):
        return dict(payload["scoring"])
    return dict(payload)


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _safe_median(values: list[float]) -> float:
    return round(float(median(values)), 4) if values else 0.0


def _write_debug_bundle(debug_root: Path, report: dict[str, Any]) -> None:
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "runtime_tuning_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(debug_root / "reviewed_comparisons.csv", list(report["comparison"].get("reviewed_comparisons", [])))
    _write_csv(debug_root / "action_movements.csv", list(report["comparison"].get("clip_movements", {}).get("moved_rows", [])))
    _write_csv(debug_root / "bucket_outcomes.csv", _bucket_rows(report))
    (debug_root / "warnings.json").write_text(json.dumps(report.get("warnings", []), indent=2), encoding="utf-8")


def _bucket_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant in ("current", "trial"):
        outcomes = report["comparison"]["action_quality"][variant]
        for action in ("highlight_candidate", "inspect", "skip"):
            rows.append(
                {
                    "variant": variant,
                    "action": action,
                    "approved_count": outcomes[action]["approved"],
                    "rejected_count": outcomes[action]["rejected"],
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
