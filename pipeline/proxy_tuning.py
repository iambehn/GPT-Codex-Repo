from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import median
from typing import Any

from pipeline.hf_highlight import reconstruct_hf_multimodal_outputs
from pipeline.proxy_scanner import build_proxy_windows
from pipeline.simple_yaml import load_yaml_file


SUPPORTED_PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
DEFAULT_MIN_REVIEWED = 3
ALLOWED_TRIAL_KEYS = {"shortlist_count", "stage_weights", "signal_thresholds"}
REQUIRED_REPLAY_REPORT_FIELDS = (
    "ok",
    "status",
    "sidecar_root",
    "trial_name",
    "scanned_sidecar_count",
    "reviewed_sidecar_count",
    "approved_count",
    "rejected_count",
    "skipped_sidecar_count",
    "current_proxy_scoring",
    "trial_proxy_scoring",
    "comparison",
    "recommendation",
    "warnings",
)
REQUIRED_RECOMMENDATION_FIELDS = ("decision", "reason", "supporting_metrics", "data_quality_notes", "follow_up")
VALID_REPLAY_DECISIONS = {"prefer_trial", "keep_current", "inconclusive"}


def replay_proxy_scoring(
    sidecar_root: str | Path,
    trial_config_path: str | Path,
    *,
    game: str | None = None,
    current_proxy_config: dict[str, Any] | None = None,
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

    current_config = _merged_proxy_config(current_proxy_config)
    trial_payload = _load_trial_config(trial_path)
    if isinstance(trial_payload, dict) and "error" in trial_payload:
        return {
            "ok": False,
            "sidecar_root": str(root),
            "trial_config_path": str(trial_path),
            "error": str(trial_payload["error"]),
        }
    trial_override = _extract_trial_payload(trial_payload)
    if isinstance(trial_override, dict) and "error" in trial_override:
        return {
            "ok": False,
            "sidecar_root": str(root),
            "trial_config_path": str(trial_path),
            "error": str(trial_override["error"]),
        }
    trial_config = _apply_trial_override(current_config, trial_override)
    effective_trial_name = str(trial_name or trial_payload.get("trial_name") or trial_payload.get("name") or trial_path.stem)

    report = _build_report(
        root,
        game=game,
        current_proxy_config=current_config,
        trial_proxy_config=trial_config,
        trial_name=effective_trial_name,
        min_reviewed=min_reviewed,
        include_unreviewed=include_unreviewed,
    )
    _validate_replay_report_contract(report)

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
    current_proxy_config: dict[str, Any],
    trial_proxy_config: dict[str, Any],
    trial_name: str,
    min_reviewed: int,
    include_unreviewed: bool,
) -> dict[str, Any]:
    sidecar_paths = sorted(root.rglob("*.proxy_scan.json"))
    warnings: list[dict[str, str]] = []
    reviewed_rows: list[dict[str, Any]] = []
    coverage_count = 0

    for sidecar_path in sidecar_paths:
        status, row = _comparison_row_from_sidecar(
            sidecar_path,
            game=game,
            current_proxy_config=current_proxy_config,
            trial_proxy_config=trial_proxy_config,
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
    comparison = _comparison(reviewed_rows, approved_rows, rejected_rows, current_proxy_config, trial_proxy_config, coverage_count)
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
        "current_proxy_scoring": current_proxy_config,
        "trial_proxy_scoring": trial_proxy_config,
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
    current_proxy_config: dict[str, Any],
    trial_proxy_config: dict[str, Any],
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
    if not _is_hf_sidecar(sidecar):
        return "non_hf_source", None

    review_status = str(sidecar.get("proxy_review", {}).get("review_status", "")).strip().lower()
    if review_status not in {"approved", "rejected"}:
        return "unreviewed", None

    current = _reconstruct_sidecar(sidecar, current_proxy_config)
    trial = _reconstruct_sidecar(sidecar, trial_proxy_config)
    return None, {
        "scan_id": sidecar.get("scan_id"),
        "game": sidecar.get("game"),
        "source": sidecar.get("source"),
        "sidecar_path": str(sidecar_path.resolve()),
        "review_status": review_status,
        "current_proxy_score": float(current["top_proxy_score"]),
        "trial_proxy_score": float(trial["top_proxy_score"]),
        "score_delta": round(float(trial["top_proxy_score"]) - float(current["top_proxy_score"]), 4),
        "current_action": current["top_recommended_action"],
        "trial_action": trial["top_recommended_action"],
        "action_changed": current["top_recommended_action"] != trial["top_recommended_action"],
        "current_shortlist": current["shortlist"],
        "trial_shortlist": trial["shortlist"],
        "shortlist_changed": current["shortlist"] != trial["shortlist"],
        "current_rerank_order": current["rerank_order"],
        "trial_rerank_order": trial["rerank_order"],
        "current_contributions": current["top_contributions"],
        "trial_contributions": trial["top_contributions"],
    }


def _reconstruct_sidecar(sidecar: dict[str, Any], proxy_config: dict[str, Any]) -> dict[str, Any]:
    metadata = _hf_metadata(sidecar)
    analysis = reconstruct_hf_multimodal_outputs(
        dict(metadata.get("structured_outputs", {})),
        {
            "duration_seconds": dict(metadata.get("pipeline", {})).get("duration_seconds"),
            "shortlist_count": dict(proxy_config.get("hf_multimodal", {})).get("shortlist_count"),
            "stage_weights": dict(proxy_config.get("hf_multimodal", {})).get("stage_weights", {}),
            "signal_thresholds": dict(proxy_config.get("hf_multimodal", {})).get("signal_thresholds", {}),
        },
    )
    windows = build_proxy_windows(
        analysis["signals"],
        {
            "weights": dict(proxy_config.get("weights", {})),
            "candidate_selection": dict(proxy_config.get("candidate_selection", {})),
            "cost_gates": dict(proxy_config.get("cost_gates", {})),
        },
        media_duration_seconds=_as_float(dict(metadata.get("pipeline", {})).get("duration_seconds"), default=None),
    )
    top_window = windows[0].to_dict() if windows else {"proxy_score": 0.0, "recommended_action": "skip", "signals": []}
    top_candidate = analysis["reranked_candidates"][0] if analysis["reranked_candidates"] else {}
    contributions = {
        "proposal": round(float(top_candidate.get("proposal_score", 0.0)) * float(proxy_config["hf_multimodal"]["stage_weights"]["proposal"]), 4),
        "transcript": round(float(top_candidate.get("transcript_score", 0.0)) * float(proxy_config["hf_multimodal"]["stage_weights"]["transcript"]), 4),
        "semantic": round(float(top_candidate.get("semantic_score", 0.0)) * float(proxy_config["hf_multimodal"]["stage_weights"]["semantic"]), 4),
        "novelty": round(float(top_candidate.get("novelty_score", 0.0)) * float(proxy_config["hf_multimodal"]["stage_weights"]["novelty"]), 4),
    }
    return {
        "top_proxy_score": round(float(top_window.get("proxy_score", 0.0)), 4),
        "top_recommended_action": str(top_window.get("recommended_action", "skip")),
        "shortlist": [
            f"{row['start_seconds']:.4f}-{row['end_seconds']:.4f}"
            for row in list(analysis.get("shortlisted_candidates", []))
        ],
        "rerank_order": [
            f"{row['start_seconds']:.4f}-{row['end_seconds']:.4f}"
            for row in sorted(
                list(analysis.get("reranked_candidates", [])),
                key=lambda row: (-float(row.get("rerank_score", row.get("base_score", 0.0))), float(row.get("start_seconds", 0.0))),
            )
        ],
        "top_contributions": contributions,
    }


def _comparison(
    reviewed_rows: list[dict[str, Any]],
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    current_proxy_config: dict[str, Any],
    trial_proxy_config: dict[str, Any],
    coverage_count: int,
) -> dict[str, Any]:
    current_actions = _action_outcomes(reviewed_rows, "current_action")
    trial_actions = _action_outcomes(reviewed_rows, "trial_action")
    current_sep = _score_separation(approved_rows, rejected_rows, "current_proxy_score")
    trial_sep = _score_separation(approved_rows, rejected_rows, "trial_proxy_score")
    current_targeted = _targeted_errors(approved_rows, rejected_rows, current_proxy_config, "current_proxy_score")
    trial_targeted = _targeted_errors(approved_rows, rejected_rows, trial_proxy_config, "trial_proxy_score")
    return {
        "reviewed_comparisons": [
            {
                "scan_id": row["scan_id"],
                "game": row["game"],
                "source": row["source"],
                "review_status": row["review_status"],
                "current_proxy_score": row["current_proxy_score"],
                "trial_proxy_score": row["trial_proxy_score"],
                "score_delta": row["score_delta"],
                "current_action": row["current_action"],
                "trial_action": row["trial_action"],
                "action_changed": row["action_changed"],
                "shortlist_changed": row["shortlist_changed"],
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
            "delta": {key: int(trial_targeted[key]) - int(current_targeted[key]) for key in sorted(set(current_targeted) | set(trial_targeted))},
        },
        "clip_movements": _movement_summary(reviewed_rows),
        "stage_contribution_deltas": _stage_contribution_deltas(reviewed_rows),
        "coverage": {
            "reviewed_clip_count": len(reviewed_rows),
            "coverage_clip_count": coverage_count,
        },
    }


def _recommendation(reviewed_rows: list[dict[str, Any]], comparison: dict[str, Any], min_reviewed: int) -> dict[str, Any]:
    if len(reviewed_rows) < min_reviewed:
        return {
            "decision": "inconclusive",
            "reason": "Not enough reviewed proxy sidecars to evaluate a trial config confidently.",
            "supporting_metrics": {"reviewed_sidecar_count": len(reviewed_rows), "min_reviewed": min_reviewed},
            "data_quality_notes": [f"Only {len(reviewed_rows)} reviewed proxy sidecars available."],
            "follow_up": "Gather more reviewed HF proxy sidecars before changing proxy scoring.",
        }
    action_current = comparison["action_quality"]["current"]
    action_trial = comparison["action_quality"]["trial"]
    current_approved_non_skip = int(action_current["inspect"]["approved"]) + int(action_current["download_candidate"]["approved"])
    trial_approved_non_skip = int(action_trial["inspect"]["approved"]) + int(action_trial["download_candidate"]["approved"])
    current_approved_download = int(action_current["download_candidate"]["approved"])
    trial_approved_download = int(action_trial["download_candidate"]["approved"])
    current_rejected_non_skip = int(action_current["inspect"]["rejected"]) + int(action_current["download_candidate"]["rejected"])
    trial_rejected_non_skip = int(action_trial["inspect"]["rejected"]) + int(action_trial["download_candidate"]["rejected"])

    improved_approved = (trial_approved_non_skip, trial_approved_download) > (current_approved_non_skip, current_approved_download)
    worsened_approved = (trial_approved_non_skip, trial_approved_download) < (current_approved_non_skip, current_approved_download)
    improved_rejected = trial_rejected_non_skip < current_rejected_non_skip
    worsened_rejected = trial_rejected_non_skip > current_rejected_non_skip

    if improved_approved and not worsened_rejected:
        decision = "prefer_trial"
        reason = "Trial routing improves approved-clip handling without worsening rejected routing."
    elif improved_rejected and not worsened_approved:
        decision = "prefer_trial"
        reason = "Trial routing reduces rejected false positives without harming approved clips."
    elif worsened_approved or worsened_rejected:
        decision = "keep_current"
        reason = "Trial routing regresses reviewed clip outcomes."
    else:
        decision = "inconclusive"
        reason = "Reviewed proxy outcomes do not clearly favor the current or trial config."

    approved_count = sum(1 for row in reviewed_rows if row["review_status"] == "approved")
    rejected_count = sum(1 for row in reviewed_rows if row["review_status"] == "rejected")
    data_quality_notes: list[str] = []
    if not approved_count or not rejected_count:
        data_quality_notes.append("Reviewed proxy sidecars are missing one review class.")
    if abs(approved_count - rejected_count) > max(1, len(reviewed_rows) // 2):
        data_quality_notes.append("Reviewed proxy sidecars are materially imbalanced between approved and rejected.")
    follow_up = "Inspect moved clips and adjust one proxy threshold or stage weight in the next trial."
    if decision == "keep_current":
        follow_up = "Tighten the trial threshold or weight change before replaying."
    elif decision == "inconclusive":
        follow_up = "Try a narrower proxy scoring change or gather more reviewed clips."
    return {
        "decision": decision,
        "reason": reason,
        "supporting_metrics": {
            "current_approved_non_skip": current_approved_non_skip,
            "trial_approved_non_skip": trial_approved_non_skip,
            "current_approved_download": current_approved_download,
            "trial_approved_download": trial_approved_download,
            "current_rejected_non_skip": current_rejected_non_skip,
            "trial_rejected_non_skip": trial_rejected_non_skip,
        },
        "data_quality_notes": data_quality_notes,
        "follow_up": follow_up,
    }


def _validate_replay_report_contract(report: dict[str, Any]) -> None:
    missing_fields = [field for field in REQUIRED_REPLAY_REPORT_FIELDS if field not in report]
    if missing_fields:
        raise ValueError(f"invalid_proxy_replay_report_contract: missing fields: {', '.join(missing_fields)}")
    if not isinstance(report.get("comparison"), dict):
        raise ValueError("invalid_proxy_replay_report_contract: comparison must be a dict")
    if not isinstance(report.get("warnings"), list):
        raise ValueError("invalid_proxy_replay_report_contract: warnings must be a list")
    recommendation = report.get("recommendation")
    if not isinstance(recommendation, dict):
        raise ValueError("invalid_proxy_replay_report_contract: recommendation must be a dict")
    missing_recommendation_fields = [field for field in REQUIRED_RECOMMENDATION_FIELDS if field not in recommendation]
    if missing_recommendation_fields:
        raise ValueError(
            f"invalid_proxy_replay_report_contract: recommendation missing fields: {', '.join(missing_recommendation_fields)}"
        )
    if str(recommendation.get("decision")) not in VALID_REPLAY_DECISIONS:
        raise ValueError("invalid_proxy_replay_report_contract: recommendation.decision must be prefer_trial, keep_current, or inconclusive")
    if not isinstance(recommendation.get("supporting_metrics"), dict):
        raise ValueError("invalid_proxy_replay_report_contract: recommendation.supporting_metrics must be a dict")
    if not isinstance(recommendation.get("data_quality_notes"), list):
        raise ValueError("invalid_proxy_replay_report_contract: recommendation.data_quality_notes must be a list")


def _action_outcomes(rows: list[dict[str, Any]], action_key: str) -> dict[str, dict[str, int]]:
    summary = {action: {"approved": 0, "rejected": 0} for action in ("download_candidate", "inspect", "skip")}
    for row in rows:
        action = str(row.get(action_key, "skip"))
        if action not in summary:
            summary[action] = {"approved": 0, "rejected": 0}
        summary[action][str(row["review_status"])] += 1
    return summary


def _action_delta(current: dict[str, Any], trial: dict[str, Any]) -> dict[str, dict[str, int]]:
    return {
        action: {
            status: int(trial.get(action, {}).get(status, 0)) - int(current.get(action, {}).get(status, 0))
            for status in ("approved", "rejected")
        }
        for action in sorted(set(current) | set(trial))
    }


def _score_separation(approved_rows: list[dict[str, Any]], rejected_rows: list[dict[str, Any]], score_key: str) -> dict[str, Any]:
    approved_scores = [float(row[score_key]) for row in approved_rows]
    rejected_scores = [float(row[score_key]) for row in rejected_rows]
    approved_average = round(sum(approved_scores) / len(approved_scores), 4) if approved_scores else 0.0
    rejected_average = round(sum(rejected_scores) / len(rejected_scores), 4) if rejected_scores else 0.0
    return {
        "approved": {"count": len(approved_scores), "average": approved_average, "median": _safe_median(approved_scores)},
        "rejected": {"count": len(rejected_scores), "average": rejected_average, "median": _safe_median(rejected_scores)},
        "score_gap": round(approved_average - rejected_average, 4),
    }


def _targeted_errors(
    approved_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    proxy_config: dict[str, Any],
    score_key: str,
) -> dict[str, int]:
    inspect_threshold = float(proxy_config["cost_gates"]["inspect_min_score"])
    download_threshold = float(proxy_config["cost_gates"]["download_candidate_min_score"])
    return {
        "approved_below_inspect_threshold": sum(1 for row in approved_rows if float(row[score_key]) < inspect_threshold),
        "approved_below_download_threshold": sum(1 for row in approved_rows if float(row[score_key]) < download_threshold),
        "rejected_at_or_above_inspect_threshold": sum(1 for row in rejected_rows if float(row[score_key]) >= inspect_threshold),
        "rejected_at_or_above_download_threshold": sum(1 for row in rejected_rows if float(row[score_key]) >= download_threshold),
    }


def _movement_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    moved_rows: list[dict[str, Any]] = []
    for row in rows:
        if not row["action_changed"] and not row["shortlist_changed"]:
            continue
        movement = f"{row['current_action']} -> {row['trial_action']}"
        moved_rows.append(
            {
                "scan_id": row["scan_id"],
                "game": row["game"],
                "source": row["source"],
                "review_status": row["review_status"],
                "movement": movement,
                "score_delta": row["score_delta"],
                "current_shortlist": row["current_shortlist"],
                "trial_shortlist": row["trial_shortlist"],
            }
        )
    return {"moved_count": len(moved_rows), "moved_rows": moved_rows}


def _stage_contribution_deltas(rows: list[dict[str, Any]]) -> dict[str, float]:
    summary: dict[str, float] = {}
    for stage_name in ("proposal", "transcript", "semantic", "novelty"):
        deltas = [
            float(row["trial_contributions"].get(stage_name, 0.0)) - float(row["current_contributions"].get(stage_name, 0.0))
            for row in rows
        ]
        summary[stage_name] = round(sum(deltas) / len(deltas), 4) if deltas else 0.0
    return summary


def _write_debug_bundle(root: Path, report: dict[str, Any]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "proxy_tuning_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(
        root / "reviewed_comparisons.csv",
        report.get("comparison", {}).get("reviewed_comparisons", []),
        ("scan_id", "game", "source", "review_status", "current_proxy_score", "trial_proxy_score", "current_action", "trial_action", "action_changed", "shortlist_changed"),
    )
    _write_csv(
        root / "action_movements.csv",
        report.get("comparison", {}).get("clip_movements", {}).get("moved_rows", []),
        ("scan_id", "game", "source", "review_status", "movement", "score_delta"),
    )
    bucket_rows = []
    for config_name in ("current", "trial"):
        for action, counts in dict(report.get("comparison", {}).get("action_quality", {}).get(config_name, {})).items():
            bucket_rows.append(
                {
                    "config_name": config_name,
                    "action": action,
                    "approved_count": counts.get("approved", 0),
                    "rejected_count": counts.get("rejected", 0),
                }
            )
    _write_csv(root / "bucket_outcomes.csv", bucket_rows, ("config_name", "action", "approved_count", "rejected_count"))
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


def _is_hf_sidecar(sidecar: dict[str, Any]) -> bool:
    source_results = sidecar.get("source_results", {})
    if isinstance(source_results, dict) and "hf_multimodal" in source_results:
        return True
    windows = list(sidecar.get("windows", []))
    return any("hf_multimodal" in list(window.get("source_families", [])) for window in windows if isinstance(window, dict))


def _extract_trial_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"error": "trial config must be a mapping"}
    unknown_keys = sorted(set(payload) - ALLOWED_TRIAL_KEYS - {"trial_name", "name"})
    if unknown_keys:
        return {"error": f"trial proxy config includes unsupported keys: {', '.join(unknown_keys)}"}
    return {key: payload[key] for key in sorted(ALLOWED_TRIAL_KEYS & set(payload))}


def _apply_trial_override(current_proxy_config: dict[str, Any], trial_override: dict[str, Any]) -> dict[str, Any]:
    merged = json.loads(json.dumps(current_proxy_config))
    hf_config = merged["hf_multimodal"]
    if "shortlist_count" in trial_override:
        hf_config["shortlist_count"] = max(1, int(trial_override["shortlist_count"]))
    if isinstance(trial_override.get("stage_weights"), dict):
        for key, value in dict(trial_override["stage_weights"]).items():
            if key in hf_config["stage_weights"]:
                hf_config["stage_weights"][key] = float(value)
    if isinstance(trial_override.get("signal_thresholds"), dict):
        for key, value in dict(trial_override["signal_thresholds"]).items():
            if key in hf_config["signal_thresholds"]:
                hf_config["signal_thresholds"][key] = float(value)
    return merged


def _merged_proxy_config(config: dict[str, Any] | None) -> dict[str, Any]:
    payload = config if isinstance(config, dict) else {}
    hf_payload = payload.get("hf_multimodal", {}) if isinstance(payload.get("hf_multimodal"), dict) else {}
    return {
        "hf_multimodal": {
            "shortlist_count": max(1, int(hf_payload.get("shortlist_count", 5))),
            "stage_weights": {
                "proposal": float(hf_payload.get("stage_weights", {}).get("proposal", 0.35)),
                "transcript": float(hf_payload.get("stage_weights", {}).get("transcript", 0.20)),
                "semantic": float(hf_payload.get("stage_weights", {}).get("semantic", 0.25)),
                "novelty": float(hf_payload.get("stage_weights", {}).get("novelty", 0.20)),
            },
            "signal_thresholds": {
                "proposal": float(hf_payload.get("signal_thresholds", {}).get("proposal", 0.55)),
                "transcript": float(hf_payload.get("signal_thresholds", {}).get("transcript", 0.60)),
                "semantic": float(hf_payload.get("signal_thresholds", {}).get("semantic", 0.60)),
                "novelty": float(hf_payload.get("signal_thresholds", {}).get("novelty", 0.60)),
                "rerank": float(hf_payload.get("signal_thresholds", {}).get("rerank", 0.65)),
            },
        },
        "weights": {str(key): float(value) for key, value in dict(payload.get("weights", {})).items()},
        "candidate_selection": {str(key): value for key, value in dict(payload.get("candidate_selection", {})).items()},
        "cost_gates": {str(key): value for key, value in dict(payload.get("cost_gates", {})).items()},
    }


def _load_trial_config(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    loaded = load_yaml_file(path)
    if isinstance(loaded, dict):
        return loaded
    return {"error": "trial config must deserialize to a mapping"}


def _safe_median(values: list[float]) -> float:
    return round(float(median(values)), 4) if values else 0.0


def _as_float(value: Any, *, default: float | None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path
