from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


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

    normalized_steps = [_step_result(**step) for step in (step_results or [])]
    operator_run_id = _operator_run_id(
        mode=mode_for_artifact,
        inputs=inputs,
        filters=filters,
        created_at=created_at,
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
        output_root=output_root,
        output_path=output_path,
        operator_run_id=operator_run_id,
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
    artifact: dict[str, Any],
    *,
    output_root: str | Path | None,
    output_path: str | Path | None,
    operator_run_id: str,
) -> Path:
    if output_path is not None:
        target = Path(output_path).expanduser()
    else:
        root = Path(output_root).expanduser() if output_root is not None else DEFAULT_OUTPUT_ROOT
        target = root / f"{operator_run_id}.shadow_operator_run.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    return target
