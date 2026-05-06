from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
V2_DATASET_SCHEMA_VERSION = "v2_training_dataset_export_v1"
SHADOW_RANKING_REPLAY_SCHEMA_VERSION = "shadow_ranking_replay_v1"
SHADOW_RANKING_COMPARISON_SCHEMA_VERSION = "shadow_ranking_comparison_v1"
DEFAULT_REPLAY_OUTPUT_ROOT = REPO_ROOT / "outputs" / "shadow_ranking_replays"
DEFAULT_COMPARISON_OUTPUT_ROOT = REPO_ROOT / "outputs" / "shadow_ranking_comparisons"


def run_shadow_ranking_replay(
    dataset_manifest: str | Path,
    *,
    model_path: str | Path | None = None,
    model_family: str | None = None,
    model_version: str | None = None,
    output_path: str | Path | None = None,
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

    candidate_rows = _load_dataset_rows(payload, "candidates")
    hook_rows = _load_dataset_rows(payload, "hooks")
    outcome_rows = _load_dataset_rows(payload, "outcomes")
    performance_rows = _load_dataset_rows(payload, "performance")

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
    latest_performance_by_candidate = _latest_performance_by_candidate(performance_rows)

    adapter = _load_adapter(model_path=model_path, model_family=model_family, model_version=model_version)
    replay_id = _replay_id(
        dataset_manifest_path=str(manifest_path),
        model_family=adapter["model_family"],
        model_version=adapter["model_version"],
        filters={
            "game": game,
            "fixture_id": fixture_id,
            "candidate_id": candidate_id,
            "platform": platform,
        },
    )

    rows: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for candidate in filtered_candidates:
        current_candidate_id = str(candidate.get("candidate_id") or "")
        hook_row = _preferred_hook_row(hook_by_candidate.get(current_candidate_id, []))
        outcome_row = _preferred_outcome_row(outcome_by_candidate.get(current_candidate_id, []), platform=platform)
        performance_row = latest_performance_by_candidate.get(current_candidate_id)
        features = _feature_vector(candidate, hook_row=hook_row, outcome_row=outcome_row, performance_row=performance_row)
        prediction = _predict_scores(features, adapter)
        row = {
            "candidate_id": current_candidate_id,
            "event_id": candidate.get("event_id"),
            "hook_id": (hook_row or {}).get("hook_id"),
            "export_id": (outcome_row or {}).get("export_id"),
            "post_record_id": (outcome_row or {}).get("post_record_id"),
            "game": candidate.get("game"),
            "fixture_id": candidate.get("fixture_id"),
            "source": candidate.get("source"),
            "fused_sidecar_path": candidate.get("fused_sidecar_path"),
            "hook_manifest_path": (hook_row or {}).get("hook_manifest_path"),
            "highlight_selection_manifest_path": candidate.get("highlight_selection_manifest_path"),
            "export_batch_manifest_path": (outcome_row or {}).get("export_batch_manifest_path"),
            "post_ledger_manifest_path": (outcome_row or {}).get("post_ledger_manifest_path"),
            "platform": (outcome_row or {}).get("platform") or (performance_row or {}).get("platform"),
            "account_id": (outcome_row or {}).get("account_id") or (performance_row or {}).get("account_id"),
            "heuristic_final_score": candidate.get("final_score"),
            "heuristic_recommended_action": candidate.get("recommended_action"),
            "heuristic_lifecycle_state": candidate.get("lifecycle_state"),
            "review_outcome": candidate.get("review_outcome"),
            "export_present": bool(candidate.get("export_present")),
            "post_present": bool(candidate.get("post_present")),
            "metrics_present": bool(candidate.get("metrics_present")),
            "latest_post_performance_coverage_tier": candidate.get("latest_post_performance_coverage_tier"),
            "latest_post_performance_label_eligible": bool(candidate.get("latest_post_performance_label_eligible")),
            "latest_post_performance_target_score": candidate.get("latest_post_performance_target_score"),
            "latest_post_performance_target_bucket": candidate.get("latest_post_performance_target_bucket"),
            "latest_post_performance_label_reason": candidate.get("latest_post_performance_label_reason"),
            "latest_view_count": candidate.get("latest_view_count"),
            "latest_engagement_rate": candidate.get("latest_engagement_rate"),
            "hook_mode": (hook_row or {}).get("hook_mode"),
            "hook_archetype": (hook_row or {}).get("hook_archetype"),
            "packaging_strategy": (hook_row or {}).get("packaging_strategy") or (outcome_row or {}).get("packaging_strategy"),
            "label_positive": features["label_positive"],
            "label_score": features["label_score"],
            "predicted_candidate_score": prediction["predicted_candidate_score"],
            "predicted_export_score": prediction["predicted_export_score"],
            "predicted_post_performance_score": prediction["predicted_post_performance_score"],
            "feature_values": features,
        }
        if not current_candidate_id:
            warnings.append({"code": "missing_candidate_id", "row": row})
        rows.append(row)

    _assign_ranks(rows)
    replay_payload = {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_RANKING_REPLAY_SCHEMA_VERSION,
        "replay_id": replay_id,
        "created_at": datetime.now(UTC).isoformat(),
        "model_family": adapter["model_family"],
        "model_version": adapter["model_version"],
        "dataset_manifest_path": str(manifest_path),
        "dataset_export_id": payload.get("dataset_export_id"),
        "dataset_schema_version": payload.get("schema_version"),
        "feature_views": ["candidates", "hooks", "outcomes", "performance"],
        "filters": {key: value for key, value in {"game": game, "fixture_id": fixture_id, "candidate_id": candidate_id, "platform": platform}.items() if value is not None},
        "row_count": len(rows),
        "rows": rows,
        "warnings": warnings,
    }
    target = _default_replay_output_path(manifest_path, replay_id) if output_path is None else _resolve_path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(replay_payload, indent=2), encoding="utf-8")
    csv_path = target.with_suffix(".csv")
    _write_csv(csv_path, rows)
    replay_payload["manifest_path"] = str(target)
    replay_payload["csv_path"] = str(csv_path)
    return replay_payload


def compare_shadow_ranking_replay(
    replay_manifest: str | Path,
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(replay_manifest)
    payload = _load_json(manifest_path)
    if payload is None:
        return {
            "ok": False,
            "status": "invalid_shadow_ranking_replay",
            "replay_manifest_path": str(manifest_path),
            "error": "shadow replay manifest is missing or malformed",
        }
    if payload.get("schema_version") != SHADOW_RANKING_REPLAY_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "unsupported_shadow_ranking_replay",
            "replay_manifest_path": str(manifest_path),
            "error": f"unsupported replay schema version: {payload.get('schema_version')}",
        }

    rows = [row for row in list(payload.get("rows", [])) if isinstance(row, dict)]
    comparison_rows: list[dict[str, Any]] = []
    shadow_sorted = sorted(rows, key=lambda row: (-_float(row.get("predicted_candidate_score")), str(row.get("candidate_id") or "")))
    heuristic_sorted = sorted(rows, key=lambda row: (-_float(row.get("heuristic_final_score")), str(row.get("candidate_id") or "")))
    shadow_rank = {str(row.get("candidate_id") or ""): index + 1 for index, row in enumerate(shadow_sorted)}
    heuristic_rank = {str(row.get("candidate_id") or ""): index + 1 for index, row in enumerate(heuristic_sorted)}

    for row in rows:
        current_candidate_id = str(row.get("candidate_id") or "")
        comparison_rows.append(
            {
                "candidate_id": current_candidate_id,
                "event_id": row.get("event_id"),
                "game": row.get("game"),
                "fixture_id": row.get("fixture_id"),
                "platform": row.get("platform"),
                "heuristic_final_score": row.get("heuristic_final_score"),
                "predicted_candidate_score": row.get("predicted_candidate_score"),
                "heuristic_rank": heuristic_rank.get(current_candidate_id),
                "predicted_rank": shadow_rank.get(current_candidate_id),
                "rank_delta": (heuristic_rank.get(current_candidate_id) or 0) - (shadow_rank.get(current_candidate_id) or 0),
                "label_positive": bool(row.get("label_positive")),
                "label_score": _float(row.get("label_score")),
                "review_outcome": row.get("review_outcome"),
                "export_present": bool(row.get("export_present")),
                "post_present": bool(row.get("post_present")),
                "latest_view_count": row.get("latest_view_count"),
                "latest_engagement_rate": row.get("latest_engagement_rate"),
            }
        )

    summary = _comparison_summary(comparison_rows)
    recommendation = _comparison_recommendation(summary)
    comparison_id = _comparison_id(str(manifest_path), payload.get("model_family"), payload.get("model_version"))
    result = {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_RANKING_COMPARISON_SCHEMA_VERSION,
        "comparison_id": comparison_id,
        "created_at": datetime.now(UTC).isoformat(),
        "replay_manifest_path": str(manifest_path),
        "replay_id": payload.get("replay_id"),
        "dataset_manifest_path": payload.get("dataset_manifest_path"),
        "model_family": payload.get("model_family"),
        "model_version": payload.get("model_version"),
        "row_count": len(comparison_rows),
        "comparison": {
            "rows": comparison_rows,
            "summary": summary,
        },
        "recommendation": recommendation,
    }
    target = _default_comparison_output_path(manifest_path, comparison_id) if output_path is None else _resolve_path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    csv_path = target.with_suffix(".csv")
    _write_csv(csv_path, comparison_rows)
    result["report_path"] = str(target)
    result["csv_path"] = str(csv_path)
    return result


def _comparison_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    shadow_gain = _ranking_gain(sorted(rows, key=lambda row: (int(row.get("predicted_rank") or 10**9), str(row.get("candidate_id") or ""))))
    heuristic_gain = _ranking_gain(sorted(rows, key=lambda row: (int(row.get("heuristic_rank") or 10**9), str(row.get("candidate_id") or ""))))
    positive_count = sum(1 for row in rows if row.get("label_positive"))
    top_k = max(1, positive_count) if rows else 0
    shadow_topk_hits = _topk_hits(rows, rank_field="predicted_rank", k=top_k)
    heuristic_topk_hits = _topk_hits(rows, rank_field="heuristic_rank", k=top_k)
    by_fixture: dict[str, dict[str, Any]] = {}
    for row in rows:
        fixture_key = str(row.get("fixture_id") or "unassigned")
        bucket = by_fixture.setdefault(
            fixture_key,
            {
                "row_count": 0,
                "positive_count": 0,
                "shadow_topk_hits": 0,
                "heuristic_topk_hits": 0,
            },
        )
        bucket["row_count"] += 1
        if row.get("label_positive"):
            bucket["positive_count"] += 1
        predicted_rank = int(row.get("predicted_rank") or 10**9)
        heuristic_rank = int(row.get("heuristic_rank") or 10**9)
        if predicted_rank <= top_k:
            bucket["shadow_topk_hits"] += int(bool(row.get("label_positive")))
        if heuristic_rank <= top_k:
            bucket["heuristic_topk_hits"] += int(bool(row.get("label_positive")))
    return {
        "row_count": len(rows),
        "positive_count": positive_count,
        "top_k": top_k,
        "shadow_topk_hits": shadow_topk_hits,
        "heuristic_topk_hits": heuristic_topk_hits,
        "shadow_ranking_gain": round(shadow_gain, 6),
        "heuristic_ranking_gain": round(heuristic_gain, 6),
        "by_fixture": by_fixture,
    }


def _comparison_recommendation(summary: dict[str, Any]) -> dict[str, Any]:
    shadow_gain = float(summary.get("shadow_ranking_gain") or 0.0)
    heuristic_gain = float(summary.get("heuristic_ranking_gain") or 0.0)
    shadow_hits = int(summary.get("shadow_topk_hits") or 0)
    heuristic_hits = int(summary.get("heuristic_topk_hits") or 0)
    positive_count = int(summary.get("positive_count") or 0)
    if positive_count == 0:
        return {
            "decision": "inconclusive",
            "reason": "no positive operational labels were present in the replay slice",
        }
    if shadow_hits > heuristic_hits or shadow_gain >= heuristic_gain + 0.15:
        return {
            "decision": "prefer_shadow",
            "reason": "shadow ranking surfaces stronger operational-label alignment than the current heuristic ordering",
        }
    if heuristic_hits > shadow_hits or heuristic_gain >= shadow_gain + 0.15:
        return {
            "decision": "keep_current",
            "reason": "current heuristic ordering still aligns better with the available operational labels",
        }
    return {
        "decision": "inconclusive",
        "reason": "shadow and heuristic ordering are too close to justify a recommendation",
    }


def _ranking_gain(rows: list[dict[str, Any]]) -> float:
    gain = 0.0
    for index, row in enumerate(rows, start=1):
        gain += _float(row.get("label_score")) / index
    return gain


def _topk_hits(rows: list[dict[str, Any]], *, rank_field: str, k: int) -> int:
    if k <= 0:
        return 0
    count = 0
    for row in rows:
        if int(row.get(rank_field) or 10**9) <= k and row.get("label_positive"):
            count += 1
    return count


def _assign_ranks(rows: list[dict[str, Any]]) -> None:
    shadow_sorted = sorted(rows, key=lambda row: (-_float(row.get("predicted_candidate_score")), str(row.get("candidate_id") or "")))
    heuristic_sorted = sorted(rows, key=lambda row: (-_float(row.get("heuristic_final_score")), str(row.get("candidate_id") or "")))
    for index, row in enumerate(shadow_sorted, start=1):
        row["predicted_rank"] = index
    for index, row in enumerate(heuristic_sorted, start=1):
        row["heuristic_rank"] = index


def _preferred_hook_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return sorted(
        rows,
        key=lambda row: (-_float(row.get("hook_strength")), str(row.get("hook_id") or "")),
    )[0]


def _preferred_outcome_row(rows: list[dict[str, Any]], *, platform: str | None) -> dict[str, Any] | None:
    if not rows:
        return None
    filtered = rows
    if platform is not None:
        platform_filtered = [row for row in rows if str(row.get("platform") or "").strip() == platform]
        if platform_filtered:
            filtered = platform_filtered
    return sorted(
        filtered,
        key=lambda row: (
            int(bool(row.get("post_record_id"))),
            int(bool(row.get("export_id"))),
            str(row.get("post_record_id") or ""),
            str(row.get("export_id") or ""),
        ),
        reverse=True,
    )[0]


def _latest_performance_by_candidate(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped = _index_by_candidate(rows)
    result: dict[str, dict[str, Any]] = {}
    for candidate_id, items in grouped.items():
        result[candidate_id] = sorted(
            items,
            key=lambda row: (
                int(bool(row.get("is_latest_snapshot"))),
                str(row.get("captured_at") or ""),
                str(row.get("snapshot_row_id") or ""),
            ),
            reverse=True,
        )[0]
    return result


def _feature_vector(
    candidate: dict[str, Any],
    *,
    hook_row: dict[str, Any] | None,
    outcome_row: dict[str, Any] | None,
    performance_row: dict[str, Any] | None,
) -> dict[str, Any]:
    latest_view_count = _float(candidate.get("latest_view_count"))
    latest_engagement_rate = _float(candidate.get("latest_engagement_rate"))
    hook_strength = _float(candidate.get("preferred_hook_strength", (hook_row or {}).get("hook_strength")))
    label_positive = bool(
        str(candidate.get("review_outcome") or "").strip().lower() == "approved"
        or candidate.get("export_present")
        or candidate.get("post_present")
    )
    label_score = (
        (_float(candidate.get("final_score")) * 0.25)
        + (0.30 if str(candidate.get("review_outcome") or "").strip().lower() == "approved" else 0.0)
        + (0.20 if candidate.get("export_present") else 0.0)
        + (0.20 if candidate.get("post_present") else 0.0)
        + min(latest_view_count / 1000.0, 0.10)
        + min(max(latest_engagement_rate, 0.0), 0.10)
    )
    return {
        "final_score": _float(candidate.get("final_score")),
        "fused_confidence": _feature_value(candidate, "fused_confidence"),
        "fused_synergy_applied": _feature_value(candidate, "fused_synergy_applied"),
        "fused_minimum_required_signals_met": _feature_value(candidate, "fused_minimum_required_signals_met"),
        "fused_duration_seconds": _feature_value(candidate, "fused_duration_seconds"),
        "fused_contributing_signal_count": _feature_value(candidate, "fused_contributing_signal_count"),
        "fused_runtime_signal_count": _feature_value(candidate, "fused_runtime_signal_count"),
        "fused_proxy_signal_count": _feature_value(candidate, "fused_proxy_signal_count"),
        "fused_source_family_count": _feature_value(candidate, "fused_source_family_count"),
        "fused_entity_present": _feature_value(candidate, "fused_entity_present"),
        "fused_ability_present": _feature_value(candidate, "fused_ability_present"),
        "fused_equipment_present": _feature_value(candidate, "fused_equipment_present"),
        "fused_event_type_has_combo": _feature_value(candidate, "fused_event_type_has_combo"),
        "fused_event_type_has_medal": _feature_value(candidate, "fused_event_type_has_medal"),
        "fused_event_type_has_ability": _feature_value(candidate, "fused_event_type_has_ability"),
        "fused_event_type_has_identity": _feature_value(candidate, "fused_event_type_has_identity"),
        "hook_strength": hook_strength,
        "preferred_hook_intensity_score": _feature_value(candidate, "preferred_hook_intensity_score", (hook_row or {}).get("intensity_score")),
        "preferred_hook_clarity_score": _feature_value(candidate, "preferred_hook_clarity_score", (hook_row or {}).get("clarity_score")),
        "preferred_hook_novelty_score": _feature_value(candidate, "preferred_hook_novelty_score", (hook_row or {}).get("novelty_score")),
        "preferred_hook_context_sufficiency_score": _feature_value(candidate, "preferred_hook_context_sufficiency_score", (hook_row or {}).get("context_sufficiency_score")),
        "preferred_hook_payoff_readability_score": _feature_value(candidate, "preferred_hook_payoff_readability_score", (hook_row or {}).get("payoff_readability_score")),
        "preferred_hook_title_thumbnail_potential_score": _feature_value(candidate, "preferred_hook_title_thumbnail_potential_score", (hook_row or {}).get("title_thumbnail_potential_score")),
        "preferred_hook_authenticity_risk_score": _feature_value(candidate, "preferred_hook_authenticity_risk_score", (hook_row or {}).get("authenticity_risk_score")),
        "preferred_hook_sound_off_legibility_score": _feature_value(candidate, "preferred_hook_sound_off_legibility_score", (hook_row or {}).get("sound_off_legibility_score")),
        "preferred_hook_packaging_strategy_present": _feature_value(candidate, "preferred_hook_packaging_strategy_present"),
        "preferred_hook_rejection_reason_present": _feature_value(candidate, "preferred_hook_rejection_reason_present"),
        "is_approved": 1.0 if str(candidate.get("review_outcome") or "").strip().lower() == "approved" else 0.0,
        "export_present": 1.0 if candidate.get("export_present") else 0.0,
        "post_present": 1.0 if candidate.get("post_present") else 0.0,
        "metrics_present": 1.0 if candidate.get("metrics_present") else 0.0,
        "latest_view_count_norm": _feature_value(candidate, "latest_view_count_norm", min(latest_view_count / 1000.0, 1.0)),
        "latest_completion_rate": _feature_value(candidate, "latest_completion_rate_capped", candidate.get("latest_completion_rate")),
        "latest_engagement_rate": _feature_value(candidate, "latest_engagement_rate_capped", latest_engagement_rate),
        "hook_mode_natural": _feature_value(candidate, "preferred_hook_mode_natural", 1.0 if str((hook_row or {}).get("hook_mode") or "") == "natural" else 0.0),
        "hook_mode_synthetic": _feature_value(candidate, "preferred_hook_mode_synthetic", 1.0 if str((hook_row or {}).get("hook_mode") or "") == "synthetic" else 0.0),
        "hook_mode_reject": _feature_value(candidate, "preferred_hook_mode_reject", 1.0 if str((hook_row or {}).get("hook_mode") or "") == "reject" else 0.0),
        "hook_archetype_clutch": _feature_value(candidate, "hook_archetype_clutch"),
        "hook_archetype_reversal": _feature_value(candidate, "hook_archetype_reversal"),
        "hook_archetype_domination": _feature_value(candidate, "hook_archetype_domination"),
        "hook_archetype_comedy": _feature_value(candidate, "hook_archetype_comedy"),
        "hook_archetype_chaos": _feature_value(candidate, "hook_archetype_chaos"),
        "hook_archetype_fail": _feature_value(candidate, "hook_archetype_fail"),
        "hook_archetype_flex": _feature_value(candidate, "hook_archetype_flex"),
        "hook_archetype_other": _feature_value(candidate, "hook_archetype_other"),
        "selection_present": _feature_value(candidate, "selection_present_feature", 1.0 if candidate.get("selection_present") else 0.0),
        "account_context_present": _feature_value(candidate, "account_context_present"),
        "outcome_platform_present": _feature_value(candidate, "outcome_platform_present", 1.0 if str((outcome_row or {}).get("platform") or "") else 0.0),
        "performance_platform_present": _feature_value(candidate, "performance_platform_present", 1.0 if str((performance_row or {}).get("platform") or "") else 0.0),
        "metrics_complete_present": _feature_value(candidate, "metrics_complete_present"),
        "label_positive": label_positive,
        "label_score": round(label_score, 6),
    }


def _feature_value(candidate: dict[str, Any], key: str, fallback: Any = 0.0) -> float:
    value = candidate.get(key, fallback)
    return _float(value)


def _predict_scores(features: dict[str, Any], adapter: dict[str, Any]) -> dict[str, float]:
    if str(adapter.get("scoring_backend") or "weighted_sum") == "boosted_stumps":
        candidate_score = _boosted_score(features, adapter.get("candidate_score_model", {}))
        export_score = _boosted_score(features, adapter.get("export_score_model", {}))
        post_score = _boosted_score(features, adapter.get("post_performance_score_model", {}))
    else:
        candidate_score = _weighted_score(features, adapter["candidate_score_weights"])
        export_score = _weighted_score(features, adapter["export_score_weights"])
        post_score = _weighted_score(features, adapter["post_performance_score_weights"])
    return {
        "predicted_candidate_score": round(candidate_score, 6),
        "predicted_export_score": round(export_score, 6),
        "predicted_post_performance_score": round(post_score, 6),
    }


def _weighted_score(features: dict[str, Any], weights: dict[str, float]) -> float:
    bias = float(weights.get("bias", 0.0))
    total = bias
    for field, weight in weights.items():
        if field == "bias":
            continue
        total += _float(features.get(field)) * float(weight)
    return total


def _boosted_score(features: dict[str, Any], model: dict[str, Any]) -> float:
    total = float(model.get("bias", 0.0))
    for stump in list(model.get("trees", [])):
        if not isinstance(stump, dict):
            continue
        field = str(stump.get("field") or "")
        threshold = float(stump.get("threshold", 0.0))
        value = float(stump.get("left_value", 0.0)) if _float(features.get(field)) <= threshold else float(stump.get("right_value", 0.0))
        total += value
    return total


def _load_adapter(*, model_path: str | Path | None, model_family: str | None, model_version: str | None) -> dict[str, Any]:
    if model_path is None:
        return {
            "scoring_backend": "weighted_sum",
            "model_family": model_family or "deterministic_shadow_baseline",
            "model_version": model_version or "v1",
            "candidate_score_weights": {
                "bias": 0.05,
                "final_score": 0.55,
                "fused_confidence": 0.08,
                "fused_synergy_applied": 0.04,
                "fused_contributing_signal_count": 0.03,
                "is_approved": 0.18,
                "export_present": 0.10,
                "post_present": 0.10,
                "latest_view_count_norm": 0.04,
                "latest_engagement_rate": 0.08,
                "hook_strength": 0.06,
                "preferred_hook_clarity_score": 0.04,
                "preferred_hook_payoff_readability_score": 0.04,
                "preferred_hook_authenticity_risk_score": -0.05,
                "hook_mode_natural": 0.04,
                "hook_mode_reject": -0.06,
                "hook_archetype_clutch": 0.03,
            },
            "export_score_weights": {
                "bias": 0.03,
                "final_score": 0.40,
                "fused_confidence": 0.06,
                "is_approved": 0.20,
                "export_present": 0.18,
                "hook_strength": 0.08,
                "preferred_hook_title_thumbnail_potential_score": 0.05,
                "preferred_hook_packaging_strategy_present": 0.04,
                "hook_mode_natural": 0.06,
                "hook_mode_reject": -0.08,
            },
            "post_performance_score_weights": {
                "bias": 0.02,
                "final_score": 0.20,
                "hook_strength": 0.05,
                "preferred_hook_clarity_score": 0.04,
                "preferred_hook_payoff_readability_score": 0.04,
                "preferred_hook_title_thumbnail_potential_score": 0.05,
                "preferred_hook_authenticity_risk_score": -0.04,
            },
        }
    payload = _load_json(_resolve_path(model_path))
    if payload is None:
        raise FileNotFoundError(f"model adapter file could not be read: {model_path}")
    return {
        "scoring_backend": str(payload.get("scoring_backend") or "weighted_sum"),
        "model_family": str(payload.get("model_family") or model_family or "custom_shadow_model"),
        "model_version": str(payload.get("model_version") or model_version or "custom"),
        "candidate_score_weights": dict(payload.get("candidate_score_weights") or {}),
        "export_score_weights": dict(payload.get("export_score_weights") or {}),
        "post_performance_score_weights": dict(payload.get("post_performance_score_weights") or {}),
        "candidate_score_model": dict(payload.get("candidate_score_model") or {}),
        "export_score_model": dict(payload.get("export_score_model") or {}),
        "post_performance_score_model": dict(payload.get("post_performance_score_model") or {}),
    }


def _load_dataset_rows(manifest: dict[str, Any], view: str) -> list[dict[str, Any]]:
    dataset_view = manifest.get("dataset_views", {}).get(view, {})
    jsonl_path = str(dataset_view.get("jsonl_path") or "").strip()
    if not jsonl_path:
        return []
    path = _resolve_path(jsonl_path)
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _filter_rows(
    rows: list[dict[str, Any]],
    *,
    game: str | None,
    fixture_id: str | None,
    candidate_id: str | None,
    platform: str | None,
    outcome_rows: list[dict[str, Any]],
    performance_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    platform_candidates: set[str] | None = None
    if platform is not None:
        platform_candidates = {
            str(row.get("candidate_id") or "")
            for row in [*outcome_rows, *performance_rows]
            if str(row.get("platform") or "").strip() == platform and str(row.get("candidate_id") or "").strip()
        }
    filtered = []
    for row in rows:
        if game is not None and str(row.get("game") or "").strip() != game:
            continue
        if fixture_id is not None and str(row.get("fixture_id") or "").strip() != fixture_id:
            continue
        if candidate_id is not None and str(row.get("candidate_id") or "").strip() != candidate_id:
            continue
        if platform_candidates is not None and str(row.get("candidate_id") or "").strip() not in platform_candidates:
            continue
        filtered.append(row)
    return filtered


def _index_by_candidate(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        candidate = str(row.get("candidate_id") or "").strip()
        if not candidate:
            continue
        grouped.setdefault(candidate, []).append(row)
    return grouped


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _replay_id(
    *,
    dataset_manifest_path: str,
    model_family: str,
    model_version: str,
    filters: dict[str, Any],
) -> str:
    payload = json.dumps(
        {
            "dataset_manifest_path": str(Path(dataset_manifest_path).resolve()),
            "model_family": model_family,
            "model_version": model_version,
            "filters": {key: value for key, value in filters.items() if value is not None},
        },
        sort_keys=True,
    )
    return f"shadow-replay-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _comparison_id(replay_manifest_path: str, model_family: Any, model_version: Any) -> str:
    payload = json.dumps(
        {
            "replay_manifest_path": str(Path(replay_manifest_path).resolve()),
            "model_family": model_family,
            "model_version": model_version,
        },
        sort_keys=True,
    )
    return f"shadow-compare-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _default_replay_output_path(dataset_manifest_path: Path, replay_id: str) -> Path:
    return DEFAULT_REPLAY_OUTPUT_ROOT / dataset_manifest_path.stem / f"{replay_id}.shadow_ranking_replay.json"


def _default_comparison_output_path(replay_manifest_path: Path, comparison_id: str) -> Path:
    return DEFAULT_COMPARISON_OUTPUT_ROOT / replay_manifest_path.stem / f"{comparison_id}.shadow_ranking_comparison.json"


def _resolve_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        if not rows:
            writer.writerow({"empty": ""})
            return
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
                    for key, value in row.items()
                }
            )
