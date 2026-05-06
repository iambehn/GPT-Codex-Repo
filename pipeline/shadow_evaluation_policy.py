from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.shadow_ranking_replay import _load_json, _resolve_path


REPO_ROOT = Path(__file__).resolve().parent.parent
SHADOW_EVALUATION_POLICY_SCHEMA_VERSION = "shadow_evaluation_policy_v1"
SHADOW_EXPERIMENT_LEDGER_SCHEMA_VERSION = "shadow_experiment_ledger_v1"
DEFAULT_POLICY_OUTPUT_ROOT = REPO_ROOT / "outputs" / "shadow_evaluation_policies"
DEFAULT_LEDGER_OUTPUT_ROOT = REPO_ROOT / "outputs" / "shadow_experiment_ledgers"
DEFAULT_POLICY_FILENAME = "default.shadow_evaluation_policy.json"
DEFAULT_EVALUATION_TARGET = "candidate_approval_probability"
SUPPORTED_TARGETS = (
    "candidate_approval_probability",
    "export_selection_probability",
    "post_performance_score",
)


def write_shadow_evaluation_policy(output_path: str | Path | None = None) -> dict[str, Any]:
    target = _default_policy_path() if output_path is None else _resolve_path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    artifact = _default_policy_artifact()
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    artifact["manifest_path"] = str(target)
    return artifact


def evaluate_shadow_experiment_policy(
    experiment_manifest: str | Path,
    *,
    policy_path: str | Path | None = None,
    target: str | None = None,
    output_path: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    experiment_path = _resolve_path(experiment_manifest)
    experiment = _load_json(experiment_path)
    if experiment is None:
        return {
            "ok": False,
            "status": "invalid_shadow_experiment",
            "experiment_manifest_path": str(experiment_path),
            "error": "shadow experiment manifest is missing or malformed",
        }
    if experiment.get("schema_version") != "shadow_ranking_experiment_v1":
        return {
            "ok": False,
            "status": "unsupported_shadow_experiment",
            "experiment_manifest_path": str(experiment_path),
            "error": f"unsupported experiment schema version: {experiment.get('schema_version')}",
        }

    policy = _load_policy(policy_path)
    if not policy.get("ok"):
        return policy

    evaluation_target = str(target or _default_target_for_experiment(experiment)).strip()
    if evaluation_target not in SUPPORTED_TARGETS:
        return {
            "ok": False,
            "status": "unsupported_evaluation_target",
            "target": evaluation_target,
            "error": f"unsupported evaluation target: {evaluation_target}",
        }

    replay_path = _resolve_path(str(experiment.get("replay_manifest_path") or ""))
    replay = _load_json(replay_path)
    if replay is None:
        return {
            "ok": False,
            "status": "invalid_shadow_replay",
            "replay_manifest_path": str(replay_path),
            "error": "shadow replay manifest is missing or malformed",
        }
    rows = [row for row in list(replay.get("rows", [])) if isinstance(row, dict)]
    if game is not None:
        rows = [row for row in rows if str(row.get("game") or "").strip() == game]
    if platform is not None:
        rows = [row for row in rows if str(row.get("platform") or "").strip() == platform]

    prepared_rows = _prepare_rows(rows, evaluation_target=evaluation_target)
    policy_target = dict(policy["targets"].get(evaluation_target, {}))
    global_slice = _slice_metrics(prepared_rows, slice_type="global", slice_value="all", target=evaluation_target)
    slice_rows = _build_slice_rows(prepared_rows, target=evaluation_target)
    governed_recommendation = _govern_recommendation(
        global_slice,
        slice_rows,
        policy_target=policy_target,
        evaluation_target=evaluation_target,
    )

    ledger_id = _ledger_id(
        experiment_manifest_path=str(experiment_path),
        policy_manifest_path=policy["manifest_path"],
        evaluation_target=evaluation_target,
        filters={key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
    )
    artifact = {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_EXPERIMENT_LEDGER_SCHEMA_VERSION,
        "ledger_id": ledger_id,
        "created_at": datetime.now(UTC).isoformat(),
        "policy_manifest_path": policy["manifest_path"],
        "policy_id": policy["policy_id"],
        "policy_schema_version": policy["schema_version"],
        "experiment_manifest_path": str(experiment_path),
        "experiment_id": experiment.get("experiment_id"),
        "model_path": experiment.get("model_path"),
        "model_id": experiment.get("model_id"),
        "model_family": experiment.get("model_family"),
        "model_version": experiment.get("model_version"),
        "dataset_manifest_path": experiment.get("dataset_manifest_path"),
        "dataset_export_id": experiment.get("dataset_export_id"),
        "training_target": experiment.get("training_target"),
        "evaluation_target": evaluation_target,
        "replay_manifest_path": str(replay_path),
        "comparison_report_path": experiment.get("comparison_report_path"),
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        "coverage_status": global_slice["coverage_status"],
        "global_metrics": global_slice,
        "slice_count": len(slice_rows),
        "slice_rows": slice_rows,
        "recommendation": governed_recommendation,
        "training_metrics": experiment.get("training_metrics"),
        "evaluation_metrics": experiment.get("evaluation_metrics"),
        "comparison_summary": experiment.get("comparison_summary"),
    }
    target_path = _default_ledger_path(experiment_path, ledger_id) if output_path is None else _resolve_path(output_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    csv_path = target_path.with_suffix(".csv")
    _write_csv(csv_path, slice_rows)
    artifact["manifest_path"] = str(target_path)
    artifact["csv_path"] = str(csv_path)
    return artifact


def summarize_shadow_experiment_ledger(
    registry_payload: dict[str, Any],
    *,
    target: str | None = None,
) -> dict[str, Any]:
    if not registry_payload.get("ok"):
        return registry_payload
    rows = [row for row in list(registry_payload.get("rows", [])) if isinstance(row, dict)]
    if target is not None:
        rows = [row for row in rows if str(row.get("evaluation_target") or "") == target]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("evaluation_target") or "unknown"), []).append(row)
    summaries: list[dict[str, Any]] = []
    for evaluation_target, bucket in sorted(grouped.items()):
        ordered = sorted(
            bucket,
            key=lambda row: (
                0 if str(row.get("recommendation_decision") or "") == "prefer_shadow" else 1,
                -float(row.get("global_primary_metric_delta") or 0.0),
                str(row.get("created_at") or ""),
            ),
        )
        summaries.append(
            {
                "evaluation_target": evaluation_target,
                "experiment_count": len(bucket),
                "prefer_shadow_count": sum(1 for row in bucket if str(row.get("recommendation_decision") or "") == "prefer_shadow"),
                "blocked_count": sum(1 for row in bucket if str(row.get("recommendation_decision") or "") == "blocked_by_policy"),
                "inconclusive_count": sum(1 for row in bucket if str(row.get("recommendation_decision") or "") == "inconclusive"),
                "best_experiment": ordered[0] if ordered else None,
            }
        )
    return {
        "ok": True,
        "status": "ok",
        "row_count": len(rows),
        "target_count": len(summaries),
        "targets": summaries,
    }


def _default_policy_artifact() -> dict[str, Any]:
    return {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_EVALUATION_POLICY_SCHEMA_VERSION,
        "policy_id": "shadow-eval-policy-v1",
        "created_at": datetime.now(UTC).isoformat(),
        "targets": {
            "candidate_approval_probability": {
                "primary_metric": "top_k_recall",
                "minimum_row_count": 2,
                "minimum_positive_count": 1,
                "minimum_global_delta": 0.05,
                "protected_slice_min_rows": 1,
                "protected_regression_tolerance": -0.15,
                "protected_slice_types": [
                    "game",
                    "fixture_id",
                    "lifecycle_state",
                    "coverage_tier",
                    "hook_mode",
                    "hook_archetype",
                    "split_bucket",
                ],
            },
            "export_selection_probability": {
                "primary_metric": "top_k_recall",
                "minimum_row_count": 2,
                "minimum_positive_count": 1,
                "minimum_global_delta": 0.05,
                "protected_slice_min_rows": 1,
                "protected_regression_tolerance": -0.15,
                "protected_slice_types": [
                    "game",
                    "fixture_id",
                    "lifecycle_state",
                    "coverage_tier",
                    "hook_mode",
                    "hook_archetype",
                    "split_bucket",
                ],
            },
            "post_performance_score": {
                "primary_metric": "pearson_correlation",
                "minimum_row_count": 1,
                "minimum_positive_count": 1,
                "minimum_global_delta": 0.01,
                "protected_slice_min_rows": 1,
                "protected_regression_tolerance": -0.10,
                "protected_slice_types": [
                    "game",
                    "fixture_id",
                    "coverage_tier",
                    "hook_mode",
                    "hook_archetype",
                    "platform",
                    "split_bucket",
                ],
            },
        },
    }


def _load_policy(policy_path: str | Path | None) -> dict[str, Any]:
    if policy_path is None:
        policy = write_shadow_evaluation_policy()
    else:
        manifest_path = _resolve_path(policy_path)
        policy = _load_json(manifest_path)
        if policy is None:
            return {
                "ok": False,
                "status": "invalid_shadow_evaluation_policy",
                "policy_path": str(manifest_path),
                "error": "shadow evaluation policy is missing or malformed",
            }
        if policy.get("schema_version") != SHADOW_EVALUATION_POLICY_SCHEMA_VERSION:
            return {
                "ok": False,
                "status": "unsupported_shadow_evaluation_policy",
                "policy_path": str(manifest_path),
                "error": f"unsupported policy schema version: {policy.get('schema_version')}",
            }
        policy = dict(policy)
        policy["manifest_path"] = str(manifest_path)
    return policy


def _default_target_for_experiment(experiment: dict[str, Any]) -> str:
    training_target = str(experiment.get("training_target") or "").strip()
    if training_target == "export_selection_probability":
        return "export_selection_probability"
    if training_target == "post_performance_score":
        return "post_performance_score"
    return DEFAULT_EVALUATION_TARGET


def _prepare_rows(rows: list[dict[str, Any]], *, evaluation_target: str) -> list[dict[str, Any]]:
    performance_scores = _performance_scores(rows)
    performance_threshold = statistics.median(performance_scores.values()) if performance_scores else None
    prepared: list[dict[str, Any]] = []
    for row in rows:
        prepared_row = dict(row)
        prepared_row["split_bucket"] = _split_bucket(str(row.get("candidate_id") or ""))
        prepared_row["coverage_tier"] = _coverage_tier(row)
        label_positive, label_score, covered = _target_label(row, evaluation_target=evaluation_target, performance_scores=performance_scores, performance_threshold=performance_threshold)
        prepared_row["target_label_positive"] = label_positive
        prepared_row["target_label_score"] = label_score
        prepared_row["target_label_covered"] = covered
        prepared.append(prepared_row)
    return prepared


def _performance_scores(rows: list[dict[str, Any]]) -> dict[str, float]:
    covered = [
        row for row in rows
        if bool(row.get("latest_post_performance_label_eligible")) and row.get("latest_post_performance_target_score") is not None
    ]
    scores: dict[str, float] = {}
    for row in covered:
        candidate_key = str(row.get("candidate_id") or "")
        scores[candidate_key] = round(float(row.get("latest_post_performance_target_score") or 0.0), 6)
    return scores


def _target_label(
    row: dict[str, Any],
    *,
    evaluation_target: str,
    performance_scores: dict[str, float],
    performance_threshold: float | None,
) -> tuple[bool, float, bool]:
    if evaluation_target == "candidate_approval_probability":
        return bool(row.get("label_positive")), float(row.get("label_score") or 0.0), True
    if evaluation_target == "export_selection_probability":
        positive = bool(row.get("export_present"))
        return positive, 1.0 if positive else 0.0, True
    candidate_key = str(row.get("candidate_id") or "")
    if candidate_key not in performance_scores or performance_threshold is None:
        return False, 0.0, False
    score = float(performance_scores[candidate_key])
    return bool(score >= float(performance_threshold)), score, True


def _coverage_tier(row: dict[str, Any]) -> str:
    post_performance_tier = str(row.get("latest_post_performance_coverage_tier") or "").strip()
    if bool(row.get("post_present")) and post_performance_tier:
        return post_performance_tier
    if bool(row.get("metrics_present")):
        return "posted_with_metrics"
    if bool(row.get("post_present")):
        return "posted"
    if bool(row.get("export_present")):
        return "exported"
    lifecycle = str(row.get("heuristic_lifecycle_state") or "").strip()
    if lifecycle == "selected_for_export":
        return "selected_for_export"
    if str(row.get("review_outcome") or "").strip().lower() in {"approved", "rejected"}:
        return "reviewed"
    return "unreviewed"


def _slice_metrics(
    rows: list[dict[str, Any]],
    *,
    slice_type: str,
    slice_value: str,
    target: str,
) -> dict[str, Any]:
    covered_rows = [row for row in rows if bool(row.get("target_label_covered"))]
    positive_count = sum(1 for row in covered_rows if bool(row.get("target_label_positive")))
    shadow_sorted = sorted(covered_rows, key=lambda row: (-float(_predicted_score(row, target=target)), str(row.get("candidate_id") or "")))
    heuristic_sorted = sorted(covered_rows, key=lambda row: (-float(row.get("heuristic_final_score") or 0.0), str(row.get("candidate_id") or "")))
    top_k = positive_count if positive_count > 0 else len(covered_rows)
    shadow_hits = _topk_hits(shadow_sorted, top_k)
    heuristic_hits = _topk_hits(heuristic_sorted, top_k)
    shadow_gain = _ranking_gain(shadow_sorted)
    heuristic_gain = _ranking_gain(heuristic_sorted)
    shadow_corr = _pearson(shadow_sorted, value_key="target_label_score", score_func=lambda row: _predicted_score(row, target=target))
    heuristic_corr = _pearson(heuristic_sorted, value_key="target_label_score", score_func=lambda row: float(row.get("heuristic_final_score") or 0.0))
    primary_name = "pearson_correlation" if target == "post_performance_score" else "top_k_recall"
    shadow_primary = shadow_corr if primary_name == "pearson_correlation" else (shadow_hits / positive_count if positive_count > 0 else None)
    heuristic_primary = heuristic_corr if primary_name == "pearson_correlation" else (heuristic_hits / positive_count if positive_count > 0 else None)
    shadow_fp = _false_positive_cost(shadow_sorted, top_k)
    heuristic_fp = _false_positive_cost(heuristic_sorted, top_k)
    coverage_status = "sufficient"
    if not covered_rows:
        coverage_status = "no_coverage"
    elif positive_count == 0:
        coverage_status = "no_positive_labels"
    elif len(covered_rows) < 2 and target != "post_performance_score":
        coverage_status = "sparse"
    return {
        "slice_type": slice_type,
        "slice_value": slice_value,
        "target": target,
        "row_count": len(rows),
        "covered_row_count": len(covered_rows),
        "positive_count": positive_count,
        "top_k": top_k,
        "shadow_topk_hits": shadow_hits,
        "heuristic_topk_hits": heuristic_hits,
        "shadow_precision_at_k": round((shadow_hits / top_k), 6) if top_k else None,
        "heuristic_precision_at_k": round((heuristic_hits / top_k), 6) if top_k else None,
        "shadow_ranking_gain": round(shadow_gain, 6),
        "heuristic_ranking_gain": round(heuristic_gain, 6),
        "shadow_false_positive_cost": round(shadow_fp, 6) if shadow_fp is not None else None,
        "heuristic_false_positive_cost": round(heuristic_fp, 6) if heuristic_fp is not None else None,
        "shadow_pearson_correlation": round(shadow_corr, 6) if shadow_corr is not None else None,
        "heuristic_pearson_correlation": round(heuristic_corr, 6) if heuristic_corr is not None else None,
        "primary_metric_name": primary_name,
        "shadow_primary_metric": round(shadow_primary, 6) if shadow_primary is not None else None,
        "heuristic_primary_metric": round(heuristic_primary, 6) if heuristic_primary is not None else None,
        "primary_metric_delta": round((shadow_primary or 0.0) - (heuristic_primary or 0.0), 6) if shadow_primary is not None and heuristic_primary is not None else None,
        "coverage_status": coverage_status,
    }


def _build_slice_rows(rows: list[dict[str, Any]], *, target: str) -> list[dict[str, Any]]:
    slice_specs = (
        ("game", lambda row: str(row.get("game") or "unknown")),
        ("fixture_id", lambda row: str(row.get("fixture_id") or "unassigned")),
        ("lifecycle_state", lambda row: str(row.get("heuristic_lifecycle_state") or "unknown")),
        ("coverage_tier", lambda row: str(row.get("coverage_tier") or "unknown")),
        ("hook_mode", lambda row: str(row.get("hook_mode") or "none")),
        ("hook_archetype", lambda row: str(row.get("hook_archetype") or "none")),
        ("platform", lambda row: str(row.get("platform") or "none")),
        ("split_bucket", lambda row: str(row.get("split_bucket") or "0")),
    )
    slices: list[dict[str, Any]] = []
    for slice_type, selector in slice_specs:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(selector(row), []).append(row)
        for slice_value, bucket in sorted(grouped.items()):
            slices.append(_slice_metrics(bucket, slice_type=slice_type, slice_value=slice_value, target=target))
    return slices


def _govern_recommendation(
    global_slice: dict[str, Any],
    slice_rows: list[dict[str, Any]],
    *,
    policy_target: dict[str, Any],
    evaluation_target: str,
) -> dict[str, Any]:
    min_rows = int(policy_target.get("minimum_row_count") or 1)
    min_positive = int(policy_target.get("minimum_positive_count") or 1)
    min_global_delta = float(policy_target.get("minimum_global_delta") or 0.0)
    protected_slice_min_rows = int(policy_target.get("protected_slice_min_rows") or 1)
    protected_regression_tolerance = float(policy_target.get("protected_regression_tolerance") or 0.0)
    protected_slice_types = {str(value) for value in list(policy_target.get("protected_slice_types", []))}

    blocking_reasons: list[str] = []
    if int(global_slice.get("covered_row_count") or 0) < min_rows:
        return {
            "decision": "inconclusive",
            "reason": "evaluation coverage is below the minimum policy threshold",
            "blocking_reasons": ["insufficient_coverage"],
        }
    if int(global_slice.get("positive_count") or 0) < min_positive:
        return {
            "decision": "inconclusive",
            "reason": "positive-label coverage is below the minimum policy threshold",
            "blocking_reasons": ["insufficient_positive_labels"],
        }

    global_delta = global_slice.get("primary_metric_delta")
    if global_delta is None:
        return {
            "decision": "inconclusive",
            "reason": "global primary metric could not be computed for the requested target",
            "blocking_reasons": ["missing_global_metric"],
        }

    protected_regressions = [
        slice_row for slice_row in slice_rows
        if slice_row["slice_type"] in protected_slice_types
        and int(slice_row.get("covered_row_count") or 0) >= protected_slice_min_rows
        and slice_row.get("primary_metric_delta") is not None
        and float(slice_row["primary_metric_delta"]) < protected_regression_tolerance
    ]
    if protected_regressions:
        blocking_reasons.extend(
            f"{slice_row['slice_type']}={slice_row['slice_value']}" for slice_row in protected_regressions[:8]
        )
        return {
            "decision": "blocked_by_policy",
            "reason": "global improvement is blocked by protected-slice regression",
            "blocking_reasons": blocking_reasons,
            "protected_regression_count": len(protected_regressions),
        }

    if float(global_delta) >= min_global_delta:
        return {
            "decision": "prefer_shadow",
            "reason": f"shadow model clears policy thresholds for {evaluation_target}",
            "blocking_reasons": [],
            "protected_regression_count": 0,
        }
    return {
        "decision": "keep_current",
        "reason": "shadow model does not improve the primary metric enough to clear policy thresholds",
        "blocking_reasons": [],
        "protected_regression_count": 0,
    }


def _predicted_score(row: dict[str, Any], *, target: str) -> float:
    if target == "export_selection_probability":
        return float(row.get("predicted_export_score") or 0.0)
    if target == "post_performance_score":
        return float(row.get("predicted_post_performance_score") or 0.0)
    return float(row.get("predicted_candidate_score") or 0.0)


def _topk_hits(rows: list[dict[str, Any]], k: int) -> int:
    if k <= 0:
        return 0
    return sum(1 for row in rows[:k] if bool(row.get("target_label_positive")))


def _ranking_gain(rows: list[dict[str, Any]]) -> float:
    gain = 0.0
    for index, row in enumerate(rows, start=1):
        gain += float(row.get("target_label_score") or 0.0) / index
    return gain


def _false_positive_cost(rows: list[dict[str, Any]], k: int) -> float | None:
    if k <= 0:
        return None
    top_rows = rows[:k]
    if not top_rows:
        return None
    false_positives = sum(1 for row in top_rows if not bool(row.get("target_label_positive")))
    return false_positives / len(top_rows)


def _pearson(rows: list[dict[str, Any]], *, value_key: str, score_func: Any) -> float | None:
    if len(rows) < 2:
        return None
    xs = [float(score_func(row)) for row in rows]
    ys = [float(row.get(value_key) or 0.0) for row in rows]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=False))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0.0 or den_y == 0.0:
        return None
    return num / (den_x * den_y)


def _split_bucket(candidate_id: str) -> int:
    digest = hashlib.sha1(candidate_id.encode("utf-8")).hexdigest()[:8]
    return int(digest, 16) % 4


def _ledger_id(
    *,
    experiment_manifest_path: str,
    policy_manifest_path: str,
    evaluation_target: str,
    filters: dict[str, Any],
) -> str:
    payload = json.dumps(
        {
            "experiment_manifest_path": str(Path(experiment_manifest_path).resolve()),
            "policy_manifest_path": str(Path(policy_manifest_path).resolve()),
            "evaluation_target": evaluation_target,
            "filters": filters,
        },
        sort_keys=True,
    )
    return f"shadow-ledger-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _default_policy_path() -> Path:
    return DEFAULT_POLICY_OUTPUT_ROOT / DEFAULT_POLICY_FILENAME


def _default_ledger_path(experiment_path: Path, ledger_id: str) -> Path:
    return DEFAULT_LEDGER_OUTPUT_ROOT / experiment_path.stem / f"{ledger_id}.shadow_experiment_ledger.json"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
