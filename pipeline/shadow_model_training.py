from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.shadow_ranking_replay import (
    SHADOW_RANKING_COMPARISON_SCHEMA_VERSION,
    V2_DATASET_SCHEMA_VERSION,
    _feature_vector,
    _filter_rows,
    _index_by_candidate,
    _latest_performance_by_candidate,
    _load_dataset_rows,
    _load_json,
    _preferred_hook_row,
    _preferred_outcome_row,
    _resolve_path,
    compare_shadow_ranking_replay,
    run_shadow_ranking_replay,
)


SHADOW_RANKING_MODEL_SCHEMA_VERSION = "shadow_ranking_model_v1"
SHADOW_RANKING_EXPERIMENT_SCHEMA_VERSION = "shadow_ranking_experiment_v1"
SHADOW_MODEL_FAMILY_COMPARISON_SCHEMA_VERSION = "shadow_model_family_comparison_v1"
DEFAULT_MODEL_OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "outputs" / "shadow_ranking_models"
DEFAULT_EXPERIMENT_OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "outputs" / "shadow_ranking_experiments"
DEFAULT_FAMILY_COMPARISON_OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "outputs" / "shadow_model_family_comparisons"
DEFAULT_TRAINING_TARGET = "approved_or_selected_probability"
DEFAULT_SPLIT_KEY = "fixture_id"
LINEAR_MODEL_FAMILY = "linear_shadow_ranker"
BOOSTED_MODEL_FAMILY = "gradient_boosted_shadow_ranker"
SUPPORTED_MODEL_FAMILIES = {LINEAR_MODEL_FAMILY, BOOSTED_MODEL_FAMILY}
WEIGHTED_SUM_BACKEND = "weighted_sum"
BOOSTED_STUMPS_BACKEND = "boosted_stumps"
BOOSTED_NUM_ROUNDS = 8
BOOSTED_LEARNING_RATE = 0.35
TARGET_PRIMARY = "primary_target_value"
TARGET_EXPORT = "export_target_value"
TARGET_POST = "post_target_value"
FEATURE_FIELDS = (
    "final_score",
    "fused_confidence",
    "fused_synergy_applied",
    "fused_minimum_required_signals_met",
    "fused_duration_seconds",
    "fused_contributing_signal_count",
    "fused_runtime_signal_count",
    "fused_proxy_signal_count",
    "fused_source_family_count",
    "fused_entity_present",
    "fused_ability_present",
    "fused_equipment_present",
    "fused_event_type_has_combo",
    "fused_event_type_has_medal",
    "fused_event_type_has_ability",
    "fused_event_type_has_identity",
    "hook_strength",
    "preferred_hook_intensity_score",
    "preferred_hook_clarity_score",
    "preferred_hook_novelty_score",
    "preferred_hook_context_sufficiency_score",
    "preferred_hook_payoff_readability_score",
    "preferred_hook_title_thumbnail_potential_score",
    "preferred_hook_authenticity_risk_score",
    "preferred_hook_sound_off_legibility_score",
    "preferred_hook_packaging_strategy_present",
    "preferred_hook_rejection_reason_present",
    "is_approved",
    "export_present",
    "post_present",
    "metrics_present",
    "selection_present",
    "latest_view_count_norm",
    "latest_completion_rate",
    "latest_engagement_rate",
    "hook_mode_natural",
    "hook_mode_synthetic",
    "hook_mode_reject",
    "hook_archetype_clutch",
    "hook_archetype_reversal",
    "hook_archetype_domination",
    "hook_archetype_comedy",
    "hook_archetype_chaos",
    "hook_archetype_fail",
    "hook_archetype_flex",
    "hook_archetype_other",
    "account_context_present",
    "outcome_platform_present",
    "performance_platform_present",
    "metrics_complete_present",
)
POST_PERFORMANCE_FEATURE_FIELDS = (
    "final_score",
    "fused_confidence",
    "fused_synergy_applied",
    "fused_minimum_required_signals_met",
    "fused_duration_seconds",
    "fused_contributing_signal_count",
    "fused_runtime_signal_count",
    "fused_proxy_signal_count",
    "fused_source_family_count",
    "fused_entity_present",
    "fused_ability_present",
    "fused_equipment_present",
    "fused_event_type_has_combo",
    "fused_event_type_has_medal",
    "fused_event_type_has_ability",
    "fused_event_type_has_identity",
    "hook_strength",
    "preferred_hook_intensity_score",
    "preferred_hook_clarity_score",
    "preferred_hook_novelty_score",
    "preferred_hook_context_sufficiency_score",
    "preferred_hook_payoff_readability_score",
    "preferred_hook_title_thumbnail_potential_score",
    "preferred_hook_authenticity_risk_score",
    "preferred_hook_sound_off_legibility_score",
)


def _feature_fields_for_target(training_target: str) -> tuple[str, ...]:
    if training_target == "post_performance_score":
        return POST_PERFORMANCE_FEATURE_FIELDS
    return FEATURE_FIELDS


def train_shadow_ranking_model(
    dataset_manifest: str | Path,
    *,
    model_output_path: str | Path | None = None,
    model_family: str = LINEAR_MODEL_FAMILY,
    training_target: str = DEFAULT_TRAINING_TARGET,
    split_key: str = DEFAULT_SPLIT_KEY,
    train_fraction: float = 0.8,
    game: str | None = None,
    fixture_id: str | None = None,
    candidate_id: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(dataset_manifest)
    payload = _load_json(manifest_path)
    if payload is None:
        return {
            "ok": False,
            "status": "invalid_dataset_manifest",
            "dataset_manifest_path": str(manifest_path),
            "error": "dataset manifest is missing or malformed",
        }
    if payload.get("schema_version") != V2_DATASET_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "unsupported_dataset_manifest",
            "dataset_manifest_path": str(manifest_path),
            "error": f"unsupported dataset schema version: {payload.get('schema_version')}",
        }
    if not 0.0 < float(train_fraction) < 1.0:
        return {
            "ok": False,
            "status": "invalid_train_fraction",
            "error": "train_fraction must be between 0 and 1",
        }
    normalized_family = str(model_family or LINEAR_MODEL_FAMILY).strip() or LINEAR_MODEL_FAMILY
    if normalized_family not in SUPPORTED_MODEL_FAMILIES:
        return {
            "ok": False,
            "status": "unsupported_model_family",
            "error": f"unsupported model family: {normalized_family}",
        }

    training_rows = _training_rows(
        payload,
        game=game,
        fixture_id=fixture_id,
        candidate_id=candidate_id,
        platform=platform,
        training_target=training_target,
        split_key=split_key,
    )
    warnings = list(training_rows["warnings"])
    feature_rows = training_rows["rows"]
    evidence_summary = training_rows["evidence_summary"]
    if not feature_rows:
        return {
            "ok": False,
            "status": "no_training_rows",
            "dataset_manifest_path": str(manifest_path),
            "error": "no candidate rows matched the requested dataset filters",
        }
    overall_positive_count, overall_negative_count = _training_label_counts(
        feature_rows,
        training_target=training_target,
    )
    if _requires_strict_target_label_balance(training_target) and (
        overall_positive_count == 0 or overall_negative_count == 0
    ):
        return {
            "ok": False,
            "status": "insufficient_target_label_balance",
            "dataset_manifest_path": str(manifest_path),
            "training_target": training_target,
            "row_count": len(feature_rows),
            "positive_count": overall_positive_count,
            "negative_count": overall_negative_count,
            "error": (
                f"{training_target} requires both positive and negative labels after target construction "
                f"(positive_count={overall_positive_count}, negative_count={overall_negative_count})"
            ),
        }

    train_rows, eval_rows = _split_rows(
        feature_rows,
        split_key=split_key,
        train_fraction=float(train_fraction),
        training_target=training_target,
    )
    positive_count, negative_count = _training_label_counts(train_rows, training_target=training_target)
    if positive_count == 0 or negative_count == 0:
        warnings.append(
            {
                "code": "single_class_training_data",
                "detail": f"training split contains {positive_count} positive and {negative_count} negative rows",
            }
        )

    if normalized_family == LINEAR_MODEL_FAMILY:
        model_payload = _fit_linear_family(train_rows)
        model_version = "v1"
    else:
        model_payload = _fit_boosted_family(train_rows)
        model_version = "v1"

    eval_metrics = _evaluate_model(eval_rows if eval_rows else train_rows, model=model_payload)
    training_metrics = _evaluate_model(train_rows, model=model_payload)
    model_id = _model_id(
        dataset_manifest_path=str(manifest_path),
        training_target=training_target,
        split_key=split_key,
        train_fraction=float(train_fraction),
        model_family=normalized_family,
        filters={
            key: value
            for key, value in {
                "game": game,
                "fixture_id": fixture_id,
                "candidate_id": candidate_id,
                "platform": platform,
            }.items()
            if value is not None
        },
    )

    artifact = {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_RANKING_MODEL_SCHEMA_VERSION,
        "model_id": model_id,
        "created_at": datetime.now(UTC).isoformat(),
        "model_family": normalized_family,
        "model_version": model_version,
        "scoring_backend": model_payload["scoring_backend"],
        "training_dataset_manifest_path": str(manifest_path),
        "training_target": training_target,
        "feature_fields": list(_feature_fields_for_target(training_target)),
        "feature_fields_by_head": {
            "candidate": list(_feature_fields_for_target("approved_or_selected_probability")),
            "export": list(_feature_fields_for_target("export_selection_probability")),
            "post_performance": list(_feature_fields_for_target("post_performance_score")),
        },
        "split_key": split_key,
        "train_fraction": float(train_fraction),
        "row_count": len(feature_rows),
        "train_row_count": len(train_rows),
        "eval_row_count": len(eval_rows),
        "label_positive_count": positive_count,
        "label_negative_count": negative_count,
        "evidence_mode": evidence_summary["evidence_mode"],
        "synthetic_row_count": evidence_summary["synthetic_row_count"],
        "real_row_count": evidence_summary["real_row_count"],
        "training_metrics": training_metrics,
        "evaluation_metrics": eval_metrics,
        "filters": {
            key: value
            for key, value in {
                "game": game,
                "fixture_id": fixture_id,
                "candidate_id": candidate_id,
                "platform": platform,
            }.items()
            if value is not None
        },
        "warnings": warnings,
        **model_payload["artifact_fields"],
    }
    target = _default_model_output_path(manifest_path, model_id) if model_output_path is None else _resolve_path(model_output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    artifact["manifest_path"] = str(target)
    return artifact


def evaluate_shadow_ranking_model(
    *,
    model_path: str | Path,
    dataset_manifest: str | Path | None = None,
    output_path: str | Path | None = None,
    game: str | None = None,
    fixture_id: str | None = None,
    candidate_id: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    model_manifest_path = _resolve_path(model_path)
    model_payload = _load_json(model_manifest_path)
    if model_payload is None:
        return {
            "ok": False,
            "status": "invalid_shadow_model",
            "model_path": str(model_manifest_path),
            "error": "shadow model artifact is missing or malformed",
        }
    if model_payload.get("schema_version") != SHADOW_RANKING_MODEL_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "unsupported_shadow_model",
            "model_path": str(model_manifest_path),
            "error": f"unsupported shadow model schema version: {model_payload.get('schema_version')}",
        }

    dataset_manifest_path = _resolve_path(dataset_manifest or str(model_payload.get("training_dataset_manifest_path") or ""))
    replay = run_shadow_ranking_replay(
        dataset_manifest_path,
        model_path=model_manifest_path,
        model_family=str(model_payload.get("model_family") or ""),
        model_version=str(model_payload.get("model_version") or ""),
        output_path=None if output_path is None else _resolve_path(output_path).with_suffix(".shadow_ranking_replay.json"),
        game=game,
        fixture_id=fixture_id,
        candidate_id=candidate_id,
        platform=platform,
    )
    if not replay.get("ok"):
        return replay
    comparison = compare_shadow_ranking_replay(
        replay["manifest_path"],
        output_path=None if output_path is None else _resolve_path(output_path).with_suffix(".shadow_ranking_comparison.json"),
    )
    if not comparison.get("ok"):
        return comparison

    dataset_payload = _load_json(dataset_manifest_path)
    experiment_id = _experiment_id(
        model_manifest_path=str(model_manifest_path),
        dataset_manifest_path=str(dataset_manifest_path),
        filters={
            key: value
            for key, value in {
                "game": game,
                "fixture_id": fixture_id,
                "candidate_id": candidate_id,
                "platform": platform,
            }.items()
            if value is not None
        },
    )
    experiment = {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_RANKING_EXPERIMENT_SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "created_at": datetime.now(UTC).isoformat(),
        "model_path": str(model_manifest_path),
        "model_id": model_payload.get("model_id"),
        "model_family": model_payload.get("model_family"),
        "model_version": model_payload.get("model_version"),
        "dataset_manifest_path": str(dataset_manifest_path),
        "dataset_export_id": dataset_payload.get("dataset_export_id") if dataset_payload else None,
        "training_target": model_payload.get("training_target"),
        "split_key": model_payload.get("split_key"),
        "train_fraction": model_payload.get("train_fraction"),
        "filters": {
            key: value
            for key, value in {
                "game": game,
                "fixture_id": fixture_id,
                "candidate_id": candidate_id,
                "platform": platform,
            }.items()
            if value is not None
        },
        "replay_manifest_path": replay.get("manifest_path"),
        "comparison_report_path": comparison.get("report_path"),
        "replay_row_count": replay.get("row_count"),
        "comparison_row_count": comparison.get("row_count"),
        "comparison_recommendation": comparison.get("recommendation"),
        "comparison_summary": comparison.get("comparison", {}).get("summary"),
        "training_metrics": model_payload.get("training_metrics"),
        "evaluation_metrics": model_payload.get("evaluation_metrics"),
    }
    target = _default_experiment_output_path(model_manifest_path, experiment_id) if output_path is None else _resolve_path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(experiment, indent=2), encoding="utf-8")
    experiment["manifest_path"] = str(target)
    return experiment


def compare_shadow_model_families(
    manifest_paths: list[str | Path],
    *,
    output_path: str | Path | None = None,
    training_target: str | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for item in manifest_paths:
        path = _resolve_path(item)
        payload = _load_json(path)
        if payload is None:
            warnings.append({"code": "invalid_manifest", "manifest_path": str(path)})
            continue
        row = _family_comparison_row(payload, manifest_path=str(path), training_target=training_target, game=game, platform=platform)
        if row is None:
            continue
        rows.append(row)

    rows.sort(
        key=lambda row: (
            str(row.get("training_target") or ""),
            -_recommendation_priority(str(row.get("recommendation_decision") or "")),
            -float(row.get("primary_metric_delta") or 0.0),
            str(row.get("model_family") or ""),
            str(row.get("model_id") or ""),
        )
    )
    summaries = _family_comparison_summaries(rows)
    comparison_id = _family_comparison_id(rows, filters={"training_target": training_target, "game": game, "platform": platform})
    result = {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_MODEL_FAMILY_COMPARISON_SCHEMA_VERSION,
        "comparison_id": comparison_id,
        "created_at": datetime.now(UTC).isoformat(),
        "filters": {key: value for key, value in {"training_target": training_target, "game": game, "platform": platform}.items() if value is not None},
        "row_count": len(rows),
        "rows": rows,
        "summaries": summaries,
        "warnings": warnings,
    }
    target = _default_family_comparison_output_path(comparison_id) if output_path is None else _resolve_path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    csv_path = target.with_suffix(".csv")
    _write_csv(csv_path, rows)
    result["manifest_path"] = str(target)
    result["csv_path"] = str(csv_path)
    return result


def _training_rows(
    dataset_manifest: dict[str, Any],
    *,
    game: str | None,
    fixture_id: str | None,
    candidate_id: str | None,
    platform: str | None,
    training_target: str,
    split_key: str,
) -> dict[str, Any]:
    candidate_rows = _load_dataset_rows(dataset_manifest, "candidates")
    hook_rows = _load_dataset_rows(dataset_manifest, "hooks")
    outcome_rows = _load_dataset_rows(dataset_manifest, "outcomes")
    performance_rows = _load_dataset_rows(dataset_manifest, "performance")
    filtered_candidates = _filter_rows(
        candidate_rows,
        game=game,
        fixture_id=fixture_id,
        candidate_id=candidate_id,
        platform=platform,
        outcome_rows=outcome_rows,
        performance_rows=performance_rows,
    )
    hook_by_candidate = _index_by_candidate(hook_rows)
    outcome_by_candidate = _index_by_candidate(outcome_rows)
    latest_performance = _latest_performance_by_candidate(performance_rows)
    warnings: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for candidate in filtered_candidates:
        current_candidate_id = str(candidate.get("candidate_id") or "")
        hook_row = _preferred_hook_row(hook_by_candidate.get(current_candidate_id, []))
        outcome_row = _preferred_outcome_row(outcome_by_candidate.get(current_candidate_id, []), platform=platform)
        performance_row = latest_performance.get(current_candidate_id)
        features = _feature_vector(candidate, hook_row=hook_row, outcome_row=outcome_row, performance_row=performance_row)
        primary_target = _target_value(candidate, training_target=training_target)
        export_target = _target_value(candidate, training_target="export_selection_probability")
        post_target = _target_value(candidate, training_target="post_performance_score")
        if primary_target is None:
            warnings.append({"code": "skipped_row_without_target", "candidate_id": current_candidate_id, "training_target": training_target})
            continue
        rows.append(
            {
                "candidate_id": current_candidate_id,
                "fixture_id": candidate.get("fixture_id"),
                "split_value": str(candidate.get(split_key) or candidate.get("fixture_id") or current_candidate_id),
                "features": {field: float(features.get(field) or 0.0) for field in FEATURE_FIELDS},
                "synthetic_benchmark": _row_has_synthetic_benchmark(
                    candidate,
                    outcome_row=outcome_row,
                    performance_row=performance_row,
                ),
                TARGET_PRIMARY: float(primary_target),
                TARGET_EXPORT: float(export_target if export_target is not None else 0.0),
                TARGET_POST: float(post_target if post_target is not None else 0.0),
            }
        )
        if training_target == "post_performance_score" and post_target is None:
            warnings.append({"code": "sparse_post_performance_target", "candidate_id": current_candidate_id})
    return {
        "rows": rows,
        "warnings": warnings,
        "evidence_summary": _evidence_summary(rows),
    }


def _target_value(candidate: dict[str, Any], *, training_target: str) -> float | None:
    lifecycle_state = str(candidate.get("lifecycle_state") or "").strip()
    review_outcome = str(candidate.get("review_outcome") or "").strip().lower()
    if training_target == "approved_or_selected_probability":
        if review_outcome == "rejected":
            return 0.0
        if review_outcome == "approved":
            return 1.0
        if lifecycle_state in {"approved", "selected_for_export", "exported", "posted"}:
            return 1.0
        if lifecycle_state in {"pending_review", "rejected", "invalidated", "superseded"}:
            return 0.0
        return None
    if training_target == "export_selection_probability":
        if lifecycle_state in {"selected_for_export", "exported", "posted"} or candidate.get("export_present"):
            return 1.0
        if lifecycle_state in {"pending_review", "rejected", "approved", "invalidated", "superseded"}:
            return 0.0
        return None
    if training_target == "post_performance_score":
        if not bool(candidate.get("post_present")):
            return None
        if not bool(candidate.get("latest_post_performance_label_eligible")):
            return None
        score = _float(candidate.get("latest_post_performance_target_score"))
        if score is None:
            return None
        return round(score, 6)
    raise ValueError(f"unsupported training target: {training_target}")


def _row_has_synthetic_benchmark(
    candidate: dict[str, Any],
    *,
    outcome_row: dict[str, Any] | None,
    performance_row: dict[str, Any] | None,
) -> bool:
    for value in (
        candidate.get("evidence_mode"),
        (outcome_row or {}).get("evidence_mode"),
        (performance_row or {}).get("evidence_mode"),
        candidate.get("latest_post_performance_evidence_mode"),
        (outcome_row or {}).get("latest_post_performance_evidence_mode"),
    ):
        if str(value or "").strip() == "synthetic_augmented":
            return True
    performance_metadata = performance_row.get("metadata") if isinstance(performance_row, dict) else None
    if isinstance(performance_metadata, dict) and bool(performance_metadata.get("synthetic_benchmark")):
        return True
    for value in (
        candidate.get("highlight_selection_manifest_path"),
        candidate.get("export_artifact_path"),
        candidate.get("post_ledger_path"),
        (outcome_row or {}).get("export_batch_manifest_path"),
        (outcome_row or {}).get("post_ledger_manifest_path"),
        (outcome_row or {}).get("highlight_selection_manifest_path"),
        (outcome_row or {}).get("export_artifact_path"),
        (performance_row or {}).get("post_ledger_manifest_path"),
        (performance_row or {}).get("export_batch_manifest_path"),
        (performance_row or {}).get("highlight_selection_manifest_path"),
    ):
        if _is_synthetic_path(value):
            return True
    return False


def _is_synthetic_path(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return bool(normalized) and "synthetic" in normalized


def _evidence_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    synthetic_row_count = sum(1 for row in rows if bool(row.get("synthetic_benchmark")))
    real_row_count = len(rows) - synthetic_row_count
    return {
        "evidence_mode": "synthetic_augmented" if synthetic_row_count > 0 else "real_only",
        "synthetic_row_count": synthetic_row_count,
        "real_row_count": real_row_count,
    }


def _split_rows(
    rows: list[dict[str, Any]],
    *,
    split_key: str,
    train_fraction: float,
    training_target: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    del training_target
    del split_key
    keyed = sorted(
        rows,
        key=lambda row: (
            str(row.get("split_value") or row.get("fixture_id") or row.get("candidate_id") or ""),
            str(row.get("candidate_id") or ""),
        ),
    )
    train_rows: list[dict[str, Any]] = []
    eval_rows: list[dict[str, Any]] = []
    for row in keyed:
        token = str(row.get("split_value") or row.get("fixture_id") or row.get("candidate_id") or "")
        bucket = _fraction_bucket(token)
        if bucket < train_fraction:
            train_rows.append(row)
        else:
            eval_rows.append(row)
    if not train_rows and keyed:
        train_rows.append(keyed[0])
        eval_rows = keyed[1:]
    if not eval_rows and len(keyed) > 1:
        eval_rows.append(train_rows.pop())
    return train_rows, eval_rows


def _training_label_counts(rows: list[dict[str, Any]], *, training_target: str) -> tuple[int, int]:
    if not rows:
        return 0, 0
    if training_target == "post_performance_score":
        values = sorted(float(row[TARGET_PRIMARY]) for row in rows)
        threshold = values[len(values) // 2]
        positive_count = sum(1 for value in values if value >= threshold)
        negative_count = len(values) - positive_count
        return positive_count, negative_count
    positive_count = sum(1 for row in rows if float(row[TARGET_PRIMARY]) >= 0.5)
    negative_count = len(rows) - positive_count
    return positive_count, negative_count


def _requires_strict_target_label_balance(training_target: str) -> bool:
    return training_target == "approved_or_selected_probability"


def _fit_linear_family(rows: list[dict[str, Any]]) -> dict[str, Any]:
    means: dict[str, float] = {}
    stds: dict[str, float] = {}
    for field in FEATURE_FIELDS:
        values = [float(row["features"].get(field, 0.0)) for row in rows] or [0.0]
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / max(1, len(values))
        means[field] = mean
        stds[field] = math.sqrt(variance) or 1.0
    candidate_weights, candidate_bias = _fit_linear_head(
        rows,
        target_field=TARGET_PRIMARY,
        means=means,
        stds=stds,
        feature_fields=_feature_fields_for_target("approved_or_selected_probability"),
    )
    export_weights, export_bias = _fit_linear_head(
        rows,
        target_field=TARGET_EXPORT,
        means=means,
        stds=stds,
        feature_fields=_feature_fields_for_target("export_selection_probability"),
    )
    post_weights, post_bias = _fit_linear_head(
        rows,
        target_field=TARGET_POST,
        means=means,
        stds=stds,
        feature_fields=_feature_fields_for_target("post_performance_score"),
    )
    return {
        "scoring_backend": WEIGHTED_SUM_BACKEND,
        "artifact_fields": {
            "candidate_score_weights": {"bias": round(candidate_bias, 6), **{field: round(weight, 6) for field, weight in candidate_weights.items()}},
            "export_score_weights": {"bias": round(export_bias, 6), **{field: round(weight, 6) for field, weight in export_weights.items()}},
            "post_performance_score_weights": {"bias": round(post_bias, 6), **{field: round(weight, 6) for field, weight in post_weights.items()}},
            "normalization": {"means": means, "stds": stds},
        },
        "candidate_score_weights": {"bias": candidate_bias, **candidate_weights},
        "export_score_weights": {"bias": export_bias, **export_weights},
        "post_performance_score_weights": {"bias": post_bias, **post_weights},
    }


def _fit_boosted_family(rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_model = _fit_boosted_head(
        rows,
        target_field=TARGET_PRIMARY,
        feature_fields=_feature_fields_for_target("approved_or_selected_probability"),
    )
    export_model = _fit_boosted_head(
        rows,
        target_field=TARGET_EXPORT,
        feature_fields=_feature_fields_for_target("export_selection_probability"),
    )
    post_model = _fit_boosted_head(
        rows,
        target_field=TARGET_POST,
        feature_fields=_feature_fields_for_target("post_performance_score"),
    )
    return {
        "scoring_backend": BOOSTED_STUMPS_BACKEND,
        "artifact_fields": {
            "candidate_score_model": candidate_model,
            "export_score_model": export_model,
            "post_performance_score_model": post_model,
            "boosting_rounds": BOOSTED_NUM_ROUNDS,
            "learning_rate": BOOSTED_LEARNING_RATE,
        },
        "candidate_score_model": candidate_model,
        "export_score_model": export_model,
        "post_performance_score_model": post_model,
    }


def _fit_linear_head(
    rows: list[dict[str, Any]],
    *,
    target_field: str,
    means: dict[str, float],
    stds: dict[str, float],
    feature_fields: tuple[str, ...],
) -> tuple[dict[str, float], float]:
    targets = [float(row[target_field]) for row in rows] or [0.0]
    target_mean = sum(targets) / len(targets)
    centered_targets = [target - target_mean for target in targets]
    target_variance = sum(value * value for value in centered_targets) / max(1, len(centered_targets))
    if target_variance <= 1e-9:
        return ({field: 0.0 for field in feature_fields}, target_mean)

    raw_weights: dict[str, float] = {}
    for field in feature_fields:
        covariance = 0.0
        for index, row in enumerate(rows):
            std = float(stds.get(field) or 1.0) or 1.0
            normalized = (float(row["features"].get(field, 0.0)) - float(means.get(field, 0.0))) / std
            covariance += normalized * centered_targets[index]
        coefficient = covariance / max(1, len(rows))
        raw_weights[field] = coefficient / (float(stds.get(field) or 1.0) or 1.0)

    bias = target_mean - sum(raw_weights[field] * float(means.get(field, 0.0)) for field in feature_fields)
    return raw_weights, bias


def _fit_boosted_head(rows: list[dict[str, Any]], *, target_field: str, feature_fields: tuple[str, ...]) -> dict[str, Any]:
    targets = [float(row[target_field]) for row in rows] or [0.0]
    bias = sum(targets) / len(targets)
    predictions = [bias for _ in rows]
    trees: list[dict[str, Any]] = []
    if len(rows) <= 1:
        return {"bias": round(bias, 6), "trees": trees}

    for _ in range(BOOSTED_NUM_ROUNDS):
        residuals = [targets[index] - predictions[index] for index in range(len(rows))]
        stump = _best_residual_stump(rows, residuals, feature_fields=feature_fields)
        if stump is None:
            break
        trees.append(stump)
        for index, row in enumerate(rows):
            predictions[index] += _stump_value(row["features"], stump)
    return {"bias": round(bias, 6), "trees": trees}


def _best_residual_stump(
    rows: list[dict[str, Any]],
    residuals: list[float],
    *,
    feature_fields: tuple[str, ...],
) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_error: float | None = None
    for field in feature_fields:
        values = [float(row["features"].get(field, 0.0)) for row in rows]
        thresholds = _candidate_thresholds(values)
        if not thresholds:
            continue
        for threshold in thresholds:
            left_indices = [index for index, value in enumerate(values) if value <= threshold]
            right_indices = [index for index, value in enumerate(values) if value > threshold]
            if not left_indices or not right_indices:
                continue
            left_mean = sum(residuals[index] for index in left_indices) / len(left_indices)
            right_mean = sum(residuals[index] for index in right_indices) / len(right_indices)
            left_value = BOOSTED_LEARNING_RATE * left_mean
            right_value = BOOSTED_LEARNING_RATE * right_mean
            error = 0.0
            for index in left_indices:
                delta = residuals[index] - left_value
                error += delta * delta
            for index in right_indices:
                delta = residuals[index] - right_value
                error += delta * delta
            if best_error is None or error < best_error:
                best_error = error
                best = {
                    "field": field,
                    "threshold": round(float(threshold), 6),
                    "left_value": round(float(left_value), 6),
                    "right_value": round(float(right_value), 6),
                }
    return best


def _candidate_thresholds(values: list[float]) -> list[float]:
    unique = sorted(set(round(float(value), 6) for value in values))
    if len(unique) <= 1:
        return []
    mids = [(unique[index] + unique[index + 1]) / 2.0 for index in range(len(unique) - 1)]
    if len(mids) <= 8:
        return mids
    step = max(1, len(mids) // 8)
    sampled = [mids[index] for index in range(0, len(mids), step)]
    if sampled[-1] != mids[-1]:
        sampled.append(mids[-1])
    return sampled[:8]


def _evaluate_model(rows: list[dict[str, Any]], *, model: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_head": _evaluate_head(rows, model=model, target_field=TARGET_PRIMARY, head="candidate"),
        "export_head": _evaluate_head(rows, model=model, target_field=TARGET_EXPORT, head="export"),
        "post_performance_head": _evaluate_head(rows, model=model, target_field=TARGET_POST, head="post"),
    }


def _evaluate_head(rows: list[dict[str, Any]], *, model: dict[str, Any], target_field: str, head: str) -> dict[str, Any]:
    if not rows:
        return {
            "row_count": 0,
            "positive_count": 0,
            "negative_count": 0,
            "average_positive_score": 0.0,
            "average_negative_score": 0.0,
            "score_margin": 0.0,
            "rmse": 0.0,
        }
    positives = 0
    negatives = 0
    positive_scores: list[float] = []
    negative_scores: list[float] = []
    squared_error = 0.0
    for row in rows:
        target_value = float(row[target_field])
        score = _score_row_features(row["features"], model=model, head=head)
        squared_error += (score - target_value) ** 2
        if target_value >= 0.5:
            positives += 1
            positive_scores.append(score)
        else:
            negatives += 1
            negative_scores.append(score)
    return {
        "row_count": len(rows),
        "positive_count": positives,
        "negative_count": negatives,
        "average_positive_score": round(sum(positive_scores) / len(positive_scores), 6) if positive_scores else 0.0,
        "average_negative_score": round(sum(negative_scores) / len(negative_scores), 6) if negative_scores else 0.0,
        "score_margin": round(
            ((sum(positive_scores) / len(positive_scores)) if positive_scores else 0.0)
            - ((sum(negative_scores) / len(negative_scores)) if negative_scores else 0.0),
            6,
        ),
        "rmse": round(math.sqrt(squared_error / len(rows)), 6),
    }


def _score_row_features(features: dict[str, float], *, model: dict[str, Any], head: str) -> float:
    normalized_head = "post_performance" if head == "post" else head
    if model.get("scoring_backend") == BOOSTED_STUMPS_BACKEND:
        model_payload = model[f"{normalized_head}_score_model"]
        total = float(model_payload.get("bias", 0.0))
        for stump in list(model_payload.get("trees", [])):
            if isinstance(stump, dict):
                total += _stump_value(features, stump)
        return total
    weight_key = f"{normalized_head}_score_weights"
    weights = model[weight_key]
    total = float(weights.get("bias", 0.0))
    for field in FEATURE_FIELDS:
        total += float(weights.get(field, 0.0)) * float(features.get(field, 0.0))
    return total


def _stump_value(features: dict[str, float], stump: dict[str, Any]) -> float:
    field = str(stump.get("field") or "")
    threshold = float(stump.get("threshold", 0.0))
    if float(features.get(field, 0.0)) <= threshold:
        return float(stump.get("left_value", 0.0))
    return float(stump.get("right_value", 0.0))


def _family_comparison_row(
    payload: dict[str, Any],
    *,
    manifest_path: str,
    training_target: str | None,
    game: str | None,
    platform: str | None,
) -> dict[str, Any] | None:
    schema_version = str(payload.get("schema_version") or "")
    if schema_version == SHADOW_RANKING_EXPERIMENT_SCHEMA_VERSION:
        current_target = str(payload.get("training_target") or "").strip() or None
        filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
        if training_target is not None and current_target != training_target:
            return None
        if game is not None and str(filters.get("game") or "").strip() not in {"", game}:
            return None
        if platform is not None and str(filters.get("platform") or "").strip() not in {"", platform}:
            return None
        summary = payload.get("comparison_summary", {}) if isinstance(payload.get("comparison_summary"), dict) else {}
        shadow_gain = float(summary.get("shadow_ranking_gain") or 0.0)
        heuristic_gain = float(summary.get("heuristic_ranking_gain") or 0.0)
        recommendation = payload.get("comparison_recommendation", {}) if isinstance(payload.get("comparison_recommendation"), dict) else {}
        return {
            "manifest_path": manifest_path,
            "source_schema_version": schema_version,
            "model_id": str(payload.get("model_id") or "").strip() or None,
            "model_family": str(payload.get("model_family") or "").strip() or None,
            "model_version": str(payload.get("model_version") or "").strip() or None,
            "training_target": current_target,
            "recommendation_decision": str(recommendation.get("decision") or "").strip() or None,
            "recommendation_reason": str(recommendation.get("reason") or "").strip() or None,
            "primary_metric_name": "ranking_gain_delta",
            "primary_metric_delta": round(shadow_gain - heuristic_gain, 6),
            "global_primary_metric_delta": round(shadow_gain - heuristic_gain, 6),
            "game": str(filters.get("game") or "").strip() or None,
            "platform": str(filters.get("platform") or "").strip() or None,
            "experiment_id": str(payload.get("experiment_id") or "").strip() or None,
            "ledger_id": None,
        }
    if schema_version == "shadow_experiment_ledger_v1":
        current_target = str(payload.get("training_target") or "").strip() or None
        filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
        if training_target is not None and current_target != training_target:
            return None
        if game is not None and str(filters.get("game") or "").strip() not in {"", game}:
            return None
        if platform is not None and str(filters.get("platform") or "").strip() not in {"", platform}:
            return None
        recommendation = payload.get("recommendation", {}) if isinstance(payload.get("recommendation"), dict) else {}
        global_metrics = payload.get("global_metrics", {}) if isinstance(payload.get("global_metrics"), dict) else {}
        return {
            "manifest_path": manifest_path,
            "source_schema_version": schema_version,
            "model_id": str(payload.get("model_id") or "").strip() or None,
            "model_family": str(payload.get("model_family") or "").strip() or None,
            "model_version": str(payload.get("model_version") or "").strip() or None,
            "training_target": current_target,
            "recommendation_decision": str(recommendation.get("decision") or "").strip() or None,
            "recommendation_reason": str(recommendation.get("reason") or "").strip() or None,
            "primary_metric_name": str(global_metrics.get("primary_metric_name") or "").strip() or None,
            "primary_metric_delta": float(global_metrics.get("primary_metric_delta") or 0.0),
            "global_primary_metric_delta": float(global_metrics.get("primary_metric_delta") or 0.0),
            "game": str(filters.get("game") or "").strip() or None,
            "platform": str(filters.get("platform") or "").strip() or None,
            "experiment_id": str(payload.get("experiment_id") or "").strip() or None,
            "ledger_id": str(payload.get("ledger_id") or "").strip() or None,
        }
    return None


def _family_comparison_summaries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        target = str(row.get("training_target") or "unassigned")
        grouped.setdefault(target, []).append(row)
    summaries: list[dict[str, Any]] = []
    for target, items in sorted(grouped.items()):
        ranked = sorted(
            items,
            key=lambda row: (
                -_recommendation_priority(str(row.get("recommendation_decision") or "")),
                -float(row.get("primary_metric_delta") or 0.0),
                str(row.get("model_family") or ""),
                str(row.get("model_id") or ""),
            ),
        )
        best = ranked[0] if ranked else {}
        summaries.append(
            {
                "training_target": target,
                "family_count": len({str(row.get("model_family") or "") for row in items}),
                "row_count": len(items),
                "best_model_family": best.get("model_family"),
                "best_model_id": best.get("model_id"),
                "best_recommendation_decision": best.get("recommendation_decision"),
                "best_primary_metric_name": best.get("primary_metric_name"),
                "best_primary_metric_delta": best.get("primary_metric_delta"),
            }
        )
    return summaries


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


def _fraction_bucket(token: str) -> float:
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:8]
    return int(digest, 16) / 0xFFFFFFFF


def _model_id(
    *,
    dataset_manifest_path: str,
    training_target: str,
    split_key: str,
    train_fraction: float,
    model_family: str,
    filters: dict[str, Any],
) -> str:
    payload = json.dumps(
        {
            "dataset_manifest_path": str(Path(dataset_manifest_path).resolve()),
            "training_target": training_target,
            "split_key": split_key,
            "train_fraction": train_fraction,
            "model_family": model_family,
            "filters": filters,
        },
        sort_keys=True,
    )
    return f"shadow-model-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _experiment_id(*, model_manifest_path: str, dataset_manifest_path: str, filters: dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "model_manifest_path": str(Path(model_manifest_path).resolve()),
            "dataset_manifest_path": str(Path(dataset_manifest_path).resolve()),
            "filters": filters,
        },
        sort_keys=True,
    )
    return f"shadow-exp-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _family_comparison_id(rows: list[dict[str, Any]], *, filters: dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "rows": [
                {
                    "manifest_path": row.get("manifest_path"),
                    "model_id": row.get("model_id"),
                    "model_family": row.get("model_family"),
                    "training_target": row.get("training_target"),
                    "primary_metric_delta": row.get("primary_metric_delta"),
                }
                for row in rows
            ],
            "filters": filters,
        },
        sort_keys=True,
    )
    return f"shadow-family-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _default_model_output_path(dataset_manifest_path: Path, model_id: str) -> Path:
    return DEFAULT_MODEL_OUTPUT_ROOT / dataset_manifest_path.stem / f"{model_id}.shadow_ranking_model.json"


def _default_experiment_output_path(model_manifest_path: Path, experiment_id: str) -> Path:
    return DEFAULT_EXPERIMENT_OUTPUT_ROOT / model_manifest_path.stem / f"{experiment_id}.shadow_ranking_experiment.json"


def _default_family_comparison_output_path(comparison_id: str) -> Path:
    return DEFAULT_FAMILY_COMPARISON_OUTPUT_ROOT / f"{comparison_id}.shadow_model_family_comparison.json"


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


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))
