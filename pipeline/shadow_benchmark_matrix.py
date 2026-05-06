from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.clip_registry import query_clip_registry
from pipeline.shadow_evaluation_policy import (
    DEFAULT_EVALUATION_TARGET,
    SUPPORTED_TARGETS as SUPPORTED_EVALUATION_TARGETS,
    evaluate_shadow_experiment_policy,
    summarize_shadow_experiment_ledger,
)
from pipeline.shadow_model_training import (
    BOOSTED_MODEL_FAMILY,
    DEFAULT_SPLIT_KEY,
    DEFAULT_TRAINING_TARGET,
    LINEAR_MODEL_FAMILY,
    SUPPORTED_MODEL_FAMILIES,
    evaluate_shadow_ranking_model,
    train_shadow_ranking_model,
)
from pipeline.shadow_ranking_replay import _load_json, _resolve_path


REPO_ROOT = Path(__file__).resolve().parent.parent
SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION = "shadow_benchmark_matrix_v1"
DEFAULT_BENCHMARK_OUTPUT_ROOT = REPO_ROOT / "outputs" / "shadow_benchmark_matrices"
DEFAULT_BENCHMARK_FAMILIES = (LINEAR_MODEL_FAMILY, BOOSTED_MODEL_FAMILY)
DEFAULT_BENCHMARK_TARGETS = (
    "approved_or_selected_probability",
    "export_selection_probability",
    "post_performance_score",
)


def run_shadow_benchmark_matrix(
    dataset_manifest: str | Path,
    *,
    policy_path: str | Path | None = None,
    model_families: list[str] | tuple[str, ...] | None = None,
    training_targets: list[str] | tuple[str, ...] | None = None,
    split_key: str = DEFAULT_SPLIT_KEY,
    train_fraction: float = 0.8,
    game: str | None = None,
    platform: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(dataset_manifest)
    dataset_payload = _load_json(manifest_path)
    if dataset_payload is None:
        return {
            "ok": False,
            "status": "invalid_dataset_manifest",
            "dataset_manifest_path": str(manifest_path),
            "error": "dataset manifest is missing or malformed",
        }

    benchmark_families = _normalize_model_families(model_families)
    benchmark_targets = _normalize_training_targets(training_targets)
    benchmark_id = _benchmark_id(
        dataset_manifest_path=str(manifest_path),
        families=benchmark_families,
        targets=benchmark_targets,
        split_key=split_key,
        train_fraction=train_fraction,
        filters={key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
    )
    benchmark_root = DEFAULT_BENCHMARK_OUTPUT_ROOT / benchmark_id
    warnings: list[dict[str, Any]] = []
    runs: list[dict[str, Any]] = []

    for training_target in benchmark_targets:
        for model_family in benchmark_families:
            run_id = _benchmark_run_id(benchmark_id=benchmark_id, training_target=training_target, model_family=model_family)
            if model_family not in SUPPORTED_MODEL_FAMILIES:
                runs.append(
                    _failed_run(
                        run_id=run_id,
                        model_family=model_family,
                        training_target=training_target,
                        reason="unsupported_model_family",
                        detail=f"unsupported model family: {model_family}",
                    )
                )
                continue
            if training_target not in DEFAULT_BENCHMARK_TARGETS:
                runs.append(
                    _failed_run(
                        run_id=run_id,
                        model_family=model_family,
                        training_target=training_target,
                        reason="unsupported_training_target",
                        detail=f"unsupported training target: {training_target}",
                    )
                )
                continue

            cell_root = benchmark_root / training_target / model_family
            model_result = train_shadow_ranking_model(
                manifest_path,
                model_output_path=cell_root / "model.shadow_ranking_model.json",
                model_family=model_family,
                training_target=training_target,
                split_key=split_key,
                train_fraction=train_fraction,
                game=game,
                platform=platform,
            )
            if not model_result.get("ok"):
                runs.append(
                    _failed_run(
                        run_id=run_id,
                        model_family=model_family,
                        training_target=training_target,
                        reason=str(model_result.get("status") or "training_failed"),
                        detail=str(model_result.get("error") or "training failed"),
                    )
                )
                continue

            experiment_result = evaluate_shadow_ranking_model(
                model_path=model_result["manifest_path"],
                dataset_manifest=manifest_path,
                output_path=cell_root / "experiment.shadow_ranking_experiment.json",
                game=game,
                platform=platform,
            )
            if not experiment_result.get("ok"):
                runs.append(
                    _failed_run(
                        run_id=run_id,
                        model_family=model_family,
                        training_target=training_target,
                        reason=str(experiment_result.get("status") or "evaluation_failed"),
                        detail=str(experiment_result.get("error") or "evaluation failed"),
                        model_path=model_result.get("manifest_path"),
                    )
                )
                continue

            evaluation_target = _evaluation_target_for_training_target(training_target)
            governed_result = evaluate_shadow_experiment_policy(
                experiment_result["manifest_path"],
                policy_path=policy_path,
                target=evaluation_target,
                output_path=cell_root / "ledger.shadow_experiment_ledger.json",
                game=game,
                platform=platform,
            )
            if not governed_result.get("ok"):
                runs.append(
                    _failed_run(
                        run_id=run_id,
                        model_family=model_family,
                        training_target=training_target,
                        reason=str(governed_result.get("status") or "policy_evaluation_failed"),
                        detail=str(governed_result.get("error") or "policy evaluation failed"),
                        model_path=model_result.get("manifest_path"),
                        experiment_path=experiment_result.get("manifest_path"),
                    )
                )
                continue

            warnings.extend(list(model_result.get("warnings", [])))
            runs.append(
                {
                    "run_id": run_id,
                    "status": "ok",
                    "model_family": model_family,
                    "training_target": training_target,
                    "evaluation_target": evaluation_target,
                    "split_key": split_key,
                    "train_fraction": float(train_fraction),
                    "model_manifest_path": model_result.get("manifest_path"),
                    "experiment_manifest_path": experiment_result.get("manifest_path"),
                    "replay_manifest_path": experiment_result.get("replay_manifest_path"),
                    "comparison_report_path": experiment_result.get("comparison_report_path"),
                    "governed_ledger_manifest_path": governed_result.get("manifest_path"),
                    "recommendation_decision": governed_result.get("recommendation", {}).get("decision"),
                    "recommendation_reason": governed_result.get("recommendation", {}).get("reason"),
                    "coverage_status": governed_result.get("coverage_status"),
                    "evidence_mode": model_result.get("evidence_mode"),
                    "synthetic_row_count": model_result.get("synthetic_row_count"),
                    "real_row_count": model_result.get("real_row_count"),
                    "primary_metric_name": governed_result.get("global_metrics", {}).get("primary_metric_name"),
                    "primary_metric_delta": governed_result.get("global_metrics", {}).get("primary_metric_delta"),
                    "protected_regression_count": governed_result.get("recommendation", {}).get("protected_regression_count"),
                    "blocking_reasons": list(governed_result.get("recommendation", {}).get("blocking_reasons", [])),
                }
            )

    summary = _benchmark_summary(runs)
    artifact = {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION,
        "benchmark_id": benchmark_id,
        "created_at": datetime.now(UTC).isoformat(),
        "dataset_manifest_path": str(manifest_path),
        "dataset_export_id": dataset_payload.get("dataset_export_id"),
        "policy_path": str(_resolve_path(policy_path)) if policy_path is not None else None,
        "benchmark_config": {
            "model_families": list(benchmark_families),
            "training_targets": list(benchmark_targets),
            "split_key": split_key,
            "train_fraction": float(train_fraction),
            "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        },
        "run_count": len(runs),
        "runs": runs,
        "summary": summary,
        "warnings": warnings,
    }
    target = _default_benchmark_output_path(benchmark_id) if output_path is None else _resolve_path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    csv_path = target.with_suffix(".csv")
    _write_csv(csv_path, runs)
    artifact["manifest_path"] = str(target)
    artifact["csv_path"] = str(csv_path)
    return artifact


def summarize_shadow_benchmark_matrix(
    benchmark_manifest: str | Path | None = None,
    *,
    registry_path: str | Path | None = None,
    training_target: str | None = None,
    game: str | None = None,
    platform: str | None = None,
    recommendation_decision: str | None = None,
    model_family: str | None = None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]]
    if benchmark_manifest is not None:
        manifest_path = _resolve_path(benchmark_manifest)
        payload = _load_json(manifest_path)
        if payload is None:
            return {
                "ok": False,
                "status": "invalid_shadow_benchmark_matrix",
                "benchmark_manifest_path": str(manifest_path),
                "error": "shadow benchmark manifest is missing or malformed",
            }
        if payload.get("schema_version") != SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION:
            return {
                "ok": False,
                "status": "unsupported_shadow_benchmark_matrix",
                "benchmark_manifest_path": str(manifest_path),
                "error": f"unsupported benchmark schema version: {payload.get('schema_version')}",
            }
        rows = [row for row in list(payload.get("runs", [])) if isinstance(row, dict)]
    else:
        registry_payload = query_clip_registry(
            mode="shadow-benchmark-runs",
            registry_path=registry_path,
            training_target=training_target,
            game=game,
            platform=platform,
            recommendation_decision=recommendation_decision,
            model_family=model_family,
        )
        if not registry_payload.get("ok"):
            return registry_payload
        rows = [row for row in list(registry_payload.get("rows", [])) if isinstance(row, dict)]

    filtered = []
    for row in rows:
        if training_target is not None and str(row.get("training_target") or "") != training_target:
            continue
        if game is not None and str(row.get("game") or "").strip() not in {"", game}:
            continue
        if platform is not None and str(row.get("platform") or "").strip() not in {"", platform}:
            continue
        if recommendation_decision is not None and str(row.get("recommendation_decision") or "") != recommendation_decision:
            continue
        if model_family is not None and str(row.get("model_family") or "") != model_family:
            continue
        filtered.append(row)

    summary = _benchmark_summary(filtered)
    return {
        "ok": True,
        "status": "ok",
        "row_count": len(filtered),
        "summary": summary,
    }


def _normalize_model_families(model_families: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    raw = model_families or DEFAULT_BENCHMARK_FAMILIES
    seen: list[str] = []
    for item in raw:
        normalized = str(item or "").strip()
        if normalized and normalized not in seen:
            seen.append(normalized)
    return tuple(seen)


def _normalize_training_targets(training_targets: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    raw = training_targets or DEFAULT_BENCHMARK_TARGETS
    seen: list[str] = []
    for item in raw:
        normalized = str(item or "").strip()
        if normalized and normalized not in seen:
            seen.append(normalized)
    return tuple(seen)


def _evaluation_target_for_training_target(training_target: str) -> str:
    if training_target == "export_selection_probability":
        return "export_selection_probability"
    if training_target == "post_performance_score":
        return "post_performance_score"
    return DEFAULT_EVALUATION_TARGET


def _failed_run(
    *,
    run_id: str,
    model_family: str,
    training_target: str,
    reason: str,
    detail: str,
    model_path: str | None = None,
    experiment_path: str | None = None,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "status": "failed",
        "model_family": model_family,
        "training_target": training_target,
        "evaluation_target": _evaluation_target_for_training_target(training_target)
        if training_target in DEFAULT_BENCHMARK_TARGETS
        else None,
        "model_manifest_path": model_path,
        "experiment_manifest_path": experiment_path,
        "recommendation_decision": "failed",
        "recommendation_reason": detail,
        "failure_reason": reason,
    }


def _benchmark_summary(runs: list[dict[str, Any]]) -> dict[str, Any]:
    success_rows = [row for row in runs if str(row.get("status") or "") == "ok"]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in success_rows:
        grouped.setdefault(str(row.get("training_target") or "unknown"), []).append(row)

    best_family_per_target: list[dict[str, Any]] = []
    family_counts: dict[str, dict[str, int]] = {}
    blocked_runs = 0
    inconclusive_runs = 0
    failed_runs = sum(1 for row in runs if str(row.get("status") or "") != "ok")
    unstable_slices: list[dict[str, Any]] = []

    for row in success_rows:
        family = str(row.get("model_family") or "unknown")
        decision = str(row.get("recommendation_decision") or "")
        family_counts.setdefault(family, {"wins": 0, "blocked": 0, "inconclusive": 0, "keep_current": 0, "prefer_shadow": 0})
        if decision == "blocked_by_policy":
            family_counts[family]["blocked"] += 1
            blocked_runs += 1
        elif decision == "inconclusive":
            family_counts[family]["inconclusive"] += 1
            inconclusive_runs += 1
        elif decision == "keep_current":
            family_counts[family]["keep_current"] += 1
        elif decision == "prefer_shadow":
            family_counts[family]["prefer_shadow"] += 1
        if int(row.get("protected_regression_count") or 0) > 0:
            unstable_slices.append(
                {
                    "training_target": row.get("training_target"),
                    "model_family": row.get("model_family"),
                    "protected_regression_count": row.get("protected_regression_count"),
                    "blocking_reasons": list(row.get("blocking_reasons", [])),
                }
            )

    for training_target, bucket in sorted(grouped.items()):
        ordered = sorted(
            bucket,
            key=lambda row: (
                -_recommendation_priority(str(row.get("recommendation_decision") or "")),
                -float(row.get("primary_metric_delta") or 0.0),
                str(row.get("model_family") or ""),
            ),
        )
        best = ordered[0] if ordered else {}
        best_family = str(best.get("model_family") or "")
        if best_family:
            family_counts.setdefault(best_family, {"wins": 0, "blocked": 0, "inconclusive": 0, "keep_current": 0, "prefer_shadow": 0})
            family_counts[best_family]["wins"] += 1
        best_family_per_target.append(
            {
                "training_target": training_target,
                "best_model_family": best.get("model_family"),
                "recommendation_decision": best.get("recommendation_decision"),
                "evidence_mode": best.get("evidence_mode"),
                "synthetic_row_count": best.get("synthetic_row_count"),
                "real_row_count": best.get("real_row_count"),
                "primary_metric_name": best.get("primary_metric_name"),
                "primary_metric_delta": best.get("primary_metric_delta"),
                "governed_ledger_manifest_path": best.get("governed_ledger_manifest_path"),
            }
        )

    benchmark_recommendation = "inconclusive"
    if best_family_per_target and all(str(row.get("recommendation_decision") or "") == "prefer_shadow" for row in best_family_per_target):
        benchmark_recommendation = "prefer_shadow"
    elif any(str(row.get("recommendation_decision") or "") == "blocked_by_policy" for row in success_rows):
        benchmark_recommendation = "keep_current"

    return {
        "best_family_per_target": best_family_per_target,
        "family_counts": [{"model_family": family, **counts} for family, counts in sorted(family_counts.items())],
        "blocked_run_count": blocked_runs,
        "inconclusive_run_count": inconclusive_runs,
        "failed_run_count": failed_runs,
        "unstable_slices": unstable_slices,
        "benchmark_recommendation": benchmark_recommendation,
    }


def _recommendation_priority(decision: str) -> int:
    if decision == "prefer_shadow":
        return 4
    if decision == "inconclusive":
        return 3
    if decision == "keep_current":
        return 2
    if decision == "blocked_by_policy":
        return 1
    return 0


def _benchmark_id(
    *,
    dataset_manifest_path: str,
    families: tuple[str, ...],
    targets: tuple[str, ...],
    split_key: str,
    train_fraction: float,
    filters: dict[str, Any],
) -> str:
    payload = json.dumps(
        {
            "dataset_manifest_path": str(Path(dataset_manifest_path).resolve()),
            "families": list(families),
            "targets": list(targets),
            "split_key": split_key,
            "train_fraction": train_fraction,
            "filters": filters,
        },
        sort_keys=True,
    )
    return f"shadow-benchmark-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _benchmark_run_id(*, benchmark_id: str, training_target: str, model_family: str) -> str:
    payload = json.dumps(
        {
            "benchmark_id": benchmark_id,
            "training_target": training_target,
            "model_family": model_family,
        },
        sort_keys=True,
    )
    return f"shadow-bench-run-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _default_benchmark_output_path(benchmark_id: str) -> Path:
    return DEFAULT_BENCHMARK_OUTPUT_ROOT / f"{benchmark_id}.shadow_benchmark_matrix.json"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})
