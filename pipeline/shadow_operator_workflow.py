from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.shadow_benchmark_matrix import run_shadow_benchmark_matrix
from pipeline.shadow_benchmark_review import review_shadow_benchmark_results
from pipeline.shadow_model_training import evaluate_shadow_ranking_model, train_shadow_ranking_model


REPO_ROOT = Path(__file__).resolve().parent.parent
SHADOW_OPERATOR_RUN_SCHEMA_VERSION = "shadow_operator_run_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "shadow_operator_runs"
SUPPORTED_MODES = ("train", "benchmark", "govern", "full")


def run_shadow_operator_workflow(
    *,
    mode: str,
    dataset_manifest: str | Path | None = None,
    model_path: str | Path | None = None,
    model_family: str | None = None,
    model_version: str | None = None,
    training_target: str | None = None,
    target: str | None = None,
    policy_path: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
    split_key: str | None = None,
    train_fraction: float | None = None,
    step_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    created_at = datetime.now(UTC).isoformat()
    inputs = _normalized_inputs(
        dataset_manifest=dataset_manifest,
        model_path=model_path,
        model_family=model_family,
        model_version=model_version,
        training_target=training_target,
        target=target,
        policy_path=policy_path,
        split_key=split_key,
        train_fraction=train_fraction,
    )
    filters = {
        key: value
        for key, value in {
            "game": _normalized_optional(game),
            "platform": _normalized_optional(platform),
        }.items()
        if value is not None
    }

    validation_errors: list[str] = []
    normalized_mode = _normalize_mode(mode)
    if normalized_mode is None:
        validation_errors.append(f"unsupported mode: {mode}")
        mode_for_artifact = _normalized_optional(mode) or "invalid"
    else:
        mode_for_artifact = normalized_mode
        for field_name in _required_inputs_for_mode(normalized_mode):
            if inputs.get(field_name) is None:
                validation_errors.append(f"missing required input for mode {normalized_mode}: {field_name}")

    operator_run_id = _operator_run_id(
        mode=mode_for_artifact,
        inputs=inputs,
        filters=filters,
        created_at=created_at,
    )
    artifact_target = _operator_artifact_target(
        output_root=output_root,
        output_path=output_path,
        operator_run_id=operator_run_id,
    )
    step_output_root = artifact_target.parent / operator_run_id
    normalized_steps = [_step_result(**step) for step in (step_results or [])]
    if not validation_errors and not normalized_steps:
        normalized_steps = _execute_mode(
            mode=normalized_mode,
            inputs=inputs,
            filters=filters,
            step_output_root=step_output_root,
        )
    produced_artifacts = _produced_artifacts(normalized_steps)
    final_status = _final_status(
        normalized_steps,
        validation_errors=validation_errors,
    )
    final_recommendation = _final_recommendation(
        normalized_steps,
        status=final_status,
        validation_errors=validation_errors,
    )
    artifact = {
        "ok": final_status == "ok",
        "status": final_status,
        "schema_version": SHADOW_OPERATOR_RUN_SCHEMA_VERSION,
        "operator_run_id": operator_run_id,
        "created_at": created_at,
        "mode": mode_for_artifact,
        "inputs": inputs,
        "filters": filters,
        "step_results": normalized_steps,
        "produced_artifacts": produced_artifacts,
        "final_summary": {
            "executed_step_count": len(normalized_steps),
            "successful_step_count": sum(1 for step in normalized_steps if step["status"] == "ok"),
            "failed_step_count": sum(1 for step in normalized_steps if step["status"] == "failed"),
            "warning_count": sum(int(step.get("warning_count") or 0) for step in normalized_steps),
            "primary_model_family": inputs.get("model_family"),
            "primary_training_target": inputs.get("training_target"),
            "primary_evaluation_target": inputs.get("target"),
            "validation_error_count": len(validation_errors),
        },
        "final_recommendation": final_recommendation,
    }
    if validation_errors:
        artifact["errors"] = validation_errors

    target_path = _write_operator_artifact(
        artifact,
        target_path=artifact_target,
    )
    artifact["manifest_path"] = str(target_path)
    return artifact


def _normalize_mode(mode: str) -> str | None:
    normalized = str(mode or "").strip().lower()
    if normalized not in SUPPORTED_MODES:
        return None
    return normalized


def _required_inputs_for_mode(mode: str) -> tuple[str, ...]:
    required = {
        "train": ("dataset_manifest",),
        "benchmark": ("dataset_manifest",),
        "govern": (),
        "full": ("dataset_manifest",),
    }
    return required.get(mode, ())


def _normalized_inputs(
    *,
    dataset_manifest: str | Path | None,
    model_path: str | Path | None,
    model_family: str | None,
    model_version: str | None,
    training_target: str | None,
    target: str | None,
    policy_path: str | Path | None,
    split_key: str | None,
    train_fraction: float | None,
) -> dict[str, Any]:
    return {
        "dataset_manifest": _normalized_path(dataset_manifest),
        "model_path": _normalized_path(model_path),
        "model_family": _normalized_optional(model_family),
        "model_version": _normalized_optional(model_version),
        "training_target": _normalized_optional(training_target),
        "target": _normalized_optional(target),
        "policy_path": _normalized_path(policy_path),
        "split_key": _normalized_optional(split_key),
        "train_fraction": None if train_fraction is None else float(train_fraction),
    }


def _normalized_path(value: str | Path | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalized_optional(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _operator_run_id(
    *,
    mode: str,
    inputs: dict[str, Any],
    filters: dict[str, Any],
    created_at: str,
) -> str:
    payload = {
        "mode": mode,
        "inputs": inputs,
        "filters": filters,
        "created_at": created_at,
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    return f"shadow-operator-{digest}"


def _step_result(
    *,
    step_name: str,
    status: str,
    error: str | None = None,
    artifact_path: str | Path | None = None,
    summary: dict[str, Any] | None = None,
    warning_count: int | None = None,
    recommendation: dict[str, Any] | None = None,
    produced_artifacts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"ok", "failed", "skipped"}:
        normalized_status = "failed"
    return {
        "step_name": str(step_name).strip(),
        "status": normalized_status,
        "error": _normalized_optional(error),
        "artifact_path": _normalized_path(artifact_path),
        "summary": dict(summary or {}),
        "warning_count": int(warning_count or 0),
        "recommendation": dict(recommendation or {}),
        "produced_artifacts": dict(produced_artifacts or {}),
    }


def _execute_mode(
    *,
    mode: str | None,
    inputs: dict[str, Any],
    filters: dict[str, Any],
    step_output_root: Path,
) -> list[dict[str, Any]]:
    if mode == "train":
        return _run_train_mode(inputs=inputs, filters=filters, step_output_root=step_output_root)
    if mode == "benchmark":
        return _run_benchmark_mode(inputs=inputs, filters=filters, step_output_root=step_output_root)
    return []


def _run_train_mode(
    *,
    inputs: dict[str, Any],
    filters: dict[str, Any],
    step_output_root: Path,
) -> list[dict[str, Any]]:
    model_result = train_shadow_ranking_model(
        inputs["dataset_manifest"],
        model_output_path=step_output_root / "train" / "model.shadow_ranking_model.json",
        model_family=inputs.get("model_family") or "linear_shadow_ranker",
        training_target=inputs.get("training_target") or "approved_or_selected_probability",
        split_key=inputs.get("split_key") or "fixture_id",
        train_fraction=inputs.get("train_fraction") or 0.8,
        game=filters.get("game"),
        platform=filters.get("platform"),
    )
    steps = [
        _step_result(
            step_name="train_model",
            status="ok" if model_result.get("ok") else "failed",
            error=model_result.get("error"),
            artifact_path=model_result.get("manifest_path"),
            summary={
                "row_count": model_result.get("row_count"),
                "train_row_count": model_result.get("train_row_count"),
                "eval_row_count": model_result.get("eval_row_count"),
                "model_family": model_result.get("model_family"),
                "training_target": model_result.get("training_target"),
            },
            warning_count=len(list(model_result.get("warnings", []))) if model_result.get("ok") else 0,
            produced_artifacts={
                "model_manifest_path": model_result.get("manifest_path"),
            },
        )
    ]
    if not model_result.get("ok"):
        return steps

    experiment_result = evaluate_shadow_ranking_model(
        model_path=model_result["manifest_path"],
        dataset_manifest=inputs["dataset_manifest"],
        output_path=step_output_root / "train" / "experiment.shadow_ranking_experiment.json",
        game=filters.get("game"),
        platform=filters.get("platform"),
    )
    steps.append(
        _step_result(
            step_name="evaluate_model",
            status="ok" if experiment_result.get("ok") else "failed",
            error=experiment_result.get("error"),
            artifact_path=experiment_result.get("manifest_path"),
            summary={
                "model_family": experiment_result.get("model_family"),
                "training_target": experiment_result.get("training_target"),
                "replay_row_count": experiment_result.get("replay_row_count"),
                "comparison_row_count": experiment_result.get("comparison_row_count"),
                "comparison_decision": (experiment_result.get("comparison_recommendation") or {}).get("decision"),
            },
            recommendation=_train_recommendation(experiment_result),
            produced_artifacts={
                "experiment_manifest_path": experiment_result.get("manifest_path"),
                "replay_manifest_path": experiment_result.get("replay_manifest_path"),
                "comparison_report_path": experiment_result.get("comparison_report_path"),
            },
        )
    )
    return steps


def _run_benchmark_mode(
    *,
    inputs: dict[str, Any],
    filters: dict[str, Any],
    step_output_root: Path,
) -> list[dict[str, Any]]:
    benchmark_result = run_shadow_benchmark_matrix(
        inputs["dataset_manifest"],
        policy_path=inputs.get("policy_path"),
        model_families=[inputs["model_family"]] if inputs.get("model_family") else None,
        training_targets=[inputs["training_target"]] if inputs.get("training_target") else None,
        split_key=inputs.get("split_key") or "fixture_id",
        train_fraction=inputs.get("train_fraction") or 0.8,
        game=filters.get("game"),
        platform=filters.get("platform"),
        output_path=step_output_root / "benchmark" / "matrix.shadow_benchmark_matrix.json",
    )
    steps = [
        _step_result(
            step_name="run_benchmark_matrix",
            status="ok" if benchmark_result.get("ok") else "failed",
            error=benchmark_result.get("error"),
            artifact_path=benchmark_result.get("manifest_path"),
            summary={
                "run_count": benchmark_result.get("run_count"),
                "blocked_run_count": (benchmark_result.get("summary") or {}).get("blocked_run_count"),
                "inconclusive_run_count": (benchmark_result.get("summary") or {}).get("inconclusive_run_count"),
                "failed_run_count": (benchmark_result.get("summary") or {}).get("failed_run_count"),
            },
            warning_count=len(list(benchmark_result.get("warnings", []))) if benchmark_result.get("ok") else 0,
            produced_artifacts={
                "benchmark_manifest_path": benchmark_result.get("manifest_path"),
                "benchmark_csv_path": benchmark_result.get("csv_path"),
            },
        )
    ]
    if not benchmark_result.get("ok"):
        return steps

    review_result = review_shadow_benchmark_results(
        [benchmark_result["manifest_path"]],
        output_path=step_output_root / "benchmark" / "review.shadow_benchmark_review.json",
        training_target=inputs.get("training_target"),
        model_family=inputs.get("model_family"),
        game=filters.get("game"),
        platform=filters.get("platform"),
    )
    steps.append(
        _step_result(
            step_name="review_benchmark_results",
            status="ok" if review_result.get("ok") else "failed",
            error=review_result.get("error"),
            artifact_path=review_result.get("manifest_path"),
            summary={
                "target_count": len(list(review_result.get("target_reviews", []))) if review_result.get("ok") else None,
                "ready_target_count": (review_result.get("aggregate_conclusions") or {}).get("ready_target_count"),
                "label_calibration_target_count": (review_result.get("aggregate_conclusions") or {}).get("label_calibration_target_count"),
                "feature_cleanup_target_count": (review_result.get("aggregate_conclusions") or {}).get("feature_cleanup_target_count"),
                "coverage_blocked_target_count": (review_result.get("aggregate_conclusions") or {}).get("coverage_blocked_target_count"),
            },
            warning_count=int(review_result.get("warning_count") or 0) if review_result.get("ok") else 0,
            recommendation=_benchmark_recommendation(review_result),
            produced_artifacts={
                "benchmark_review_manifest_path": review_result.get("manifest_path"),
                "benchmark_review_csv_path": review_result.get("csv_path"),
            },
        )
    )
    return steps


def _train_recommendation(experiment_result: dict[str, Any]) -> dict[str, Any]:
    recommendation = dict(experiment_result.get("comparison_recommendation") or {})
    if not recommendation:
        return {}
    return {
        "decision": recommendation.get("decision"),
        "reason": recommendation.get("reason"),
        "supporting_artifacts": [
            path
            for path in [
                experiment_result.get("comparison_report_path"),
                experiment_result.get("manifest_path"),
                experiment_result.get("replay_manifest_path"),
            ]
            if path
        ],
        "follow_up": [],
    }


def _benchmark_recommendation(review_result: dict[str, Any]) -> dict[str, Any]:
    if not review_result.get("ok"):
        return {}
    conclusions = dict(review_result.get("aggregate_conclusions") or {})
    target_count = int(conclusions.get("target_count") or 0)
    ready_count = int(conclusions.get("ready_target_count") or 0)
    coverage_blocked = int(conclusions.get("coverage_blocked_target_count") or 0)
    label_calibration = int(conclusions.get("label_calibration_target_count") or 0)
    feature_cleanup = int(conclusions.get("feature_cleanup_target_count") or 0)
    if target_count > 0 and ready_count == target_count:
        decision = "prefer_shadow"
        reason = "all reviewed benchmark targets are ready for the next iteration"
    elif coverage_blocked > 0:
        decision = "inconclusive"
        reason = "benchmark review is blocked by target coverage gaps"
    elif label_calibration > 0 or feature_cleanup > 0:
        decision = "keep_current"
        reason = "benchmark review recommends more calibration or feature cleanup before promotion"
    else:
        decision = "inconclusive"
        reason = "benchmark review did not produce a decisive readiness signal"
    return {
        "decision": decision,
        "reason": reason,
        "supporting_artifacts": [review_result.get("manifest_path")] if review_result.get("manifest_path") else [],
        "follow_up": [row.get("recommended_next_action") for row in list(review_result.get("recommended_follow_up_actions", [])) if row.get("recommended_next_action")],
    }


def _final_status(
    step_results: list[dict[str, Any]],
    *,
    validation_errors: list[str],
) -> str:
    if validation_errors:
        return "failed"
    if not step_results:
        return "failed"
    successful = [step for step in step_results if step["status"] == "ok"]
    failed = [step for step in step_results if step["status"] == "failed"]
    if successful and failed:
        return "partial"
    if successful:
        return "ok"
    return "failed"


def _final_recommendation(
    step_results: list[dict[str, Any]],
    *,
    status: str,
    validation_errors: list[str],
) -> dict[str, Any]:
    successful_steps = [step for step in step_results if step["status"] == "ok"]
    latest_success = successful_steps[-1] if successful_steps else None
    latest_artifact = latest_success.get("artifact_path") if latest_success else None
    if validation_errors:
        return {
            "decision": "inconclusive",
            "reason": validation_errors[0],
            "supporting_artifacts": [],
            "follow_up": ["provide the required inputs and rerun the operator workflow"],
        }
    if latest_success and latest_success.get("recommendation"):
        recommendation = dict(latest_success["recommendation"])
        recommendation.setdefault("supporting_artifacts", [latest_artifact] if latest_artifact else [])
        recommendation.setdefault("follow_up", [])
        return recommendation
    if status == "ok":
        return {
            "decision": "inconclusive",
            "reason": "operator run completed without a downstream recommendation payload",
            "supporting_artifacts": [latest_artifact] if latest_artifact else [],
            "follow_up": [],
        }
    if status == "partial":
        return {
            "decision": "inconclusive",
            "reason": "operator run completed partially",
            "supporting_artifacts": [latest_artifact] if latest_artifact else [],
            "follow_up": ["inspect the failed step before promoting this run"],
        }
    return {
        "decision": "inconclusive",
        "reason": "operator run did not produce any successful required steps",
        "supporting_artifacts": [],
        "follow_up": ["resolve the failure and rerun the operator workflow"],
    }


def _produced_artifacts(step_results: list[dict[str, Any]]) -> dict[str, Any]:
    artifacts: dict[str, Any] = {}
    for step in step_results:
        artifact_path = step.get("artifact_path")
        if artifact_path:
            artifacts[str(step["step_name"])] = artifact_path
        for key, value in dict(step.get("produced_artifacts") or {}).items():
            if value is not None:
                artifacts[str(key)] = value
    return artifacts


def _write_operator_artifact(
    artifact: dict[str, Any], *, target_path: Path
) -> Path:
    target = target_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    return target


def _operator_artifact_target(
    *,
    output_root: str | Path | None,
    output_path: str | Path | None,
    operator_run_id: str,
) -> Path:
    if output_path is not None:
        return Path(output_path).expanduser()
    root = Path(output_root).expanduser() if output_root is not None else DEFAULT_OUTPUT_ROOT
    return root / f"{operator_run_id}.shadow_operator_run.json"
