from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.clip_registry import query_clip_registry


REPO_ROOT = Path(__file__).resolve().parent.parent
V2_TRAINING_EXPORT_SCHEMA_VERSION = "v2_training_dataset_export_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "v2_training_datasets"
DATASET_VIEWS = ("candidates", "hooks", "outcomes", "performance")


def export_v2_training_datasets(
    *,
    registry_path: str | Path | None = None,
    output_root: str | Path | None = None,
    game: str | None = None,
    fixture_id: str | None = None,
    candidate_id: str | None = None,
    lifecycle_state: str | None = None,
    hook_archetype: str | None = None,
    hook_mode: str | None = None,
    platform: str | None = None,
    account_id: str | None = None,
    evidence_mode: str | None = None,
) -> dict[str, Any]:
    normalized_evidence_mode = _normalize_evidence_mode(evidence_mode)
    if normalized_evidence_mode is None:
        return {
            "ok": False,
            "status": "unsupported_evidence_mode",
            "error": f"unsupported evidence mode: {evidence_mode}",
        }
    filters = {
        "game": game,
        "fixture_id": fixture_id,
        "candidate_id": candidate_id,
        "lifecycle_state": lifecycle_state,
        "hook_archetype": hook_archetype,
        "hook_mode": hook_mode,
        "platform": platform,
        "account_id": account_id,
        "evidence_mode": normalized_evidence_mode,
    }

    query_results = _load_registry_views(registry_path=registry_path, filters=filters)
    if not query_results["ok"]:
        return query_results

    dataset = _assemble_dataset(query_results, filters=filters)
    export_root = _resolve_output_root(output_root)
    paths = _dataset_paths(export_root, dataset["dataset_export_id"], game)
    _write_dataset_artifacts(dataset, paths)

    manifest = dataset["manifest"]
    return {
        "ok": True,
        "registry_path": query_results["registry_path"],
        "dataset_export_id": dataset["dataset_export_id"],
        "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
        "manifest_path": str(paths["manifest_path"]),
        "output_root": str(export_root),
        "dataset_views": {
            view: {
                "jsonl_path": str(paths[f"{view}_jsonl_path"]),
                "csv_path": str(paths[f"{view}_csv_path"]),
                "row_count": manifest["dataset_views"][view]["row_count"],
            }
            for view in DATASET_VIEWS
        },
        "row_count": manifest["row_count"],
        "warning_count": manifest["warning_count"],
        "filters": manifest["filters"],
    }


def _load_registry_views(*, registry_path: str | Path | None, filters: dict[str, Any]) -> dict[str, Any]:
    common = {
        "registry_path": registry_path,
        "game": filters["game"],
        "fixture_id": filters["fixture_id"],
        "candidate_id": filters["candidate_id"],
    }
    candidate_result = query_clip_registry(
        mode="candidate-lifecycles",
        lifecycle_state=filters["lifecycle_state"],
        **common,
    )
    if not candidate_result.get("ok"):
        return candidate_result

    hook_result = query_clip_registry(
        mode="hook-candidates",
        lifecycle_state=filters["lifecycle_state"],
        hook_archetype=filters["hook_archetype"],
        hook_mode=filters["hook_mode"],
        **common,
    )
    if not hook_result.get("ok"):
        return hook_result

    fused_event_result = query_clip_registry(
        mode="fused-events",
        **common,
    )
    if not fused_event_result.get("ok"):
        return fused_event_result

    hook_comparison_result = query_clip_registry(
        mode="hook-comparisons",
        hook_archetype=filters["hook_archetype"],
        hook_mode=filters["hook_mode"],
        **common,
    )
    if not hook_comparison_result.get("ok"):
        return hook_comparison_result

    export_result = query_clip_registry(
        mode="highlight-exports",
        hook_archetype=filters["hook_archetype"],
        hook_mode=filters["hook_mode"],
        **common,
    )
    if not export_result.get("ok"):
        return export_result

    post_result = query_clip_registry(
        mode="post-ledger-records",
        platform=filters["platform"],
        account_id=filters["account_id"],
        **common,
    )
    if not post_result.get("ok"):
        return post_result

    metrics_result = query_clip_registry(
        mode="posted-metrics",
        hook_archetype=filters["hook_archetype"],
        hook_mode=filters["hook_mode"],
        platform=filters["platform"],
        account_id=filters["account_id"],
        **common,
    )
    if not metrics_result.get("ok"):
        return metrics_result

    workflow_result = query_clip_registry(
        mode="workflow-runs",
        **common,
    )
    if not workflow_result.get("ok"):
        return workflow_result

    return {
        "ok": True,
        "registry_path": candidate_result["registry_path"],
        "candidate_rows": candidate_result["rows"],
        "hook_rows": hook_result["rows"],
        "fused_event_rows": fused_event_result["rows"],
        "hook_comparison_rows": hook_comparison_result["rows"],
        "export_rows": export_result["rows"],
        "post_rows": post_result["rows"],
        "metrics_rows": metrics_result["rows"],
        "workflow_rows": workflow_result["rows"],
    }


def _assemble_dataset(query_results: dict[str, Any], *, filters: dict[str, Any]) -> dict[str, Any]:
    candidate_rows = list(query_results["candidate_rows"])
    hook_rows = list(query_results["hook_rows"])
    fused_event_rows = list(query_results["fused_event_rows"])
    hook_comparison_rows = list(query_results["hook_comparison_rows"])
    export_rows = list(query_results["export_rows"])
    post_rows = list(query_results["post_rows"])
    metrics_rows = list(query_results["metrics_rows"])
    workflow_rows = list(query_results["workflow_rows"])
    normalized_evidence_mode = str(filters.get("evidence_mode") or "mixed")

    export_rows = [row for row in export_rows if _matches_evidence_mode(_export_row_evidence_mode(row), normalized_evidence_mode)]
    post_rows = [row for row in post_rows if _matches_evidence_mode(_post_row_evidence_mode(row), normalized_evidence_mode)]
    metrics_rows = [row for row in metrics_rows if _matches_evidence_mode(_metrics_row_evidence_mode(row), normalized_evidence_mode)]

    selected_candidate_ids = _selected_candidate_ids(
        candidate_rows,
        hook_rows=hook_rows,
        post_rows=post_rows,
        metrics_rows=metrics_rows,
        hook_filter_active=bool(filters["hook_archetype"] or filters["hook_mode"]),
        platform_filter_active=bool(filters["platform"] or filters["account_id"]),
    )

    selected_candidates = [row for row in candidate_rows if str(row.get("candidate_id") or "") in selected_candidate_ids]
    selected_hooks = [row for row in hook_rows if str(row.get("candidate_id") or "") in selected_candidate_ids]
    selected_hook_comparisons = [
        row for row in hook_comparison_rows if str(row.get("candidate_id") or "") in selected_candidate_ids
    ]
    selected_exports = [row for row in export_rows if str(row.get("candidate_id") or "") in selected_candidate_ids]
    selected_posts = [row for row in post_rows if str(row.get("candidate_id") or "") in selected_candidate_ids]
    selected_metrics = [row for row in metrics_rows if str(row.get("candidate_id") or "") in selected_candidate_ids]
    selected_workflows = [row for row in workflow_rows if str(row.get("candidate_id") or "") in selected_candidate_ids]

    warnings: list[dict[str, Any]] = []

    candidate_by_id = {str(row.get("candidate_id")): row for row in selected_candidates}
    hooks_by_candidate = _group_by(selected_hooks, "candidate_id")
    fused_event_by_key = _index_fused_events(fused_event_rows)
    hook_by_id = {str(row.get("hook_id")): row for row in selected_hooks if row.get("hook_id")}
    exports_by_candidate = _group_by(selected_exports, "candidate_id")
    exports_by_id = {str(row.get("export_id")): row for row in selected_exports if row.get("export_id")}
    posts_by_candidate = _group_by(selected_posts, "candidate_id")
    posts_by_export = _group_by(selected_posts, "export_id")
    posts_by_id = {str(row.get("post_record_id")): row for row in selected_posts if row.get("post_record_id")}
    metrics_by_candidate = _group_by(selected_metrics, "candidate_id")
    metrics_by_post = _group_by(selected_metrics, "post_record_id")
    workflows_by_candidate = _group_by(selected_workflows, "candidate_id")
    comparisons_by_candidate = _group_by(selected_hook_comparisons, "candidate_id")

    candidate_dataset_rows: list[dict[str, Any]] = []
    for row in selected_candidates:
        current_candidate_id = str(row.get("candidate_id") or "")
        selected_highlight = _selected_highlight_details(row)
        hook_entries = hooks_by_candidate.get(current_candidate_id, [])
        preferred_hook = _preferred_hook_entry(hook_entries)
        fused_event = fused_event_by_key.get((str(row.get("event_id") or ""), str(row.get("fused_sidecar_path") or "")))
        export_entries = exports_by_candidate.get(current_candidate_id, [])
        post_entries = posts_by_candidate.get(current_candidate_id, [])
        metrics_entries = metrics_by_candidate.get(current_candidate_id, [])
        workflow_entries = _workflow_lineage_rows(workflows_by_candidate.get(current_candidate_id, []))
        latest_metric = _latest_metric(metrics_entries)
        comparison_summary = _hook_comparison_summary(comparisons_by_candidate.get(current_candidate_id, []))
        candidate_dataset_rows.append(
            {
                "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
                "dataset_view": "candidates",
                "candidate_id": current_candidate_id,
                "event_id": row.get("event_id"),
                "game": row.get("game"),
                "source": row.get("source"),
                "fixture_id": row.get("fixture_id"),
                "fused_sidecar_path": row.get("fused_sidecar_path"),
                "review_outcome": row.get("latest_review_status"),
                "lifecycle_state": row.get("lifecycle_state"),
                "recommended_action": row.get("recommended_action"),
                "final_score": row.get("final_score"),
                "fused_event_type": fused_event.get("event_type") if fused_event else None,
                "fused_confidence": fused_event.get("confidence") if fused_event else None,
                "fused_gate_status": fused_event.get("gate_status") if fused_event else None,
                "fused_synergy_applied": bool((fused_event or {}).get("synergy_applied")),
                "fused_synergy_multiplier": fused_event.get("synergy_multiplier") if fused_event else None,
                "fused_minimum_required_signals_met": bool((fused_event or {}).get("minimum_required_signals_met")),
                "fused_duration_seconds": _duration_seconds(fused_event),
                "fused_contributing_signal_count": fused_event.get("contributing_signal_count") if fused_event else 0,
                "fused_runtime_signal_count": fused_event.get("runtime_signal_count") if fused_event else 0,
                "fused_proxy_signal_count": fused_event.get("proxy_signal_count") if fused_event else 0,
                "fused_source_family_count": fused_event.get("source_family_count") if fused_event else 0,
                "fused_event_type_has_combo": _contains_token((fused_event or {}).get("event_type"), "combo"),
                "fused_event_type_has_medal": _contains_token((fused_event or {}).get("event_type"), "medal"),
                "fused_event_type_has_ability": _contains_token((fused_event or {}).get("event_type"), "ability"),
                "fused_event_type_has_identity": _contains_token((fused_event or {}).get("event_type"), "identity"),
                "fused_entity_id": fused_event.get("entity_id") if fused_event else None,
                "fused_ability_id": fused_event.get("ability_id") if fused_event else None,
                "fused_equipment_id": fused_event.get("equipment_id") if fused_event else None,
                "fused_entity_present": bool((fused_event or {}).get("entity_id")),
                "fused_ability_present": bool((fused_event or {}).get("ability_id")),
                "fused_equipment_present": bool((fused_event or {}).get("equipment_id")),
                "selection_basis": row.get("selection_basis"),
                "highlight_selection_manifest_path": row.get("highlight_selection_manifest_path"),
                "selected_highlight_fusion_id": selected_highlight.get("fusion_id"),
                "selected_highlight_event_type": selected_highlight.get("event_type"),
                "selected_highlight_gate_status": selected_highlight.get("gate_status"),
                "selected_highlight_contributing_producer_families": list(selected_highlight.get("contributing_producer_families", [])),
                "export_artifact_path": row.get("export_artifact_path"),
                "post_ledger_path": row.get("post_ledger_path"),
                "has_review_disagreement": bool(row.get("has_review_disagreement")),
                "has_cross_layer_disagreement": bool(row.get("has_cross_layer_disagreement")),
                "has_trial_preference": bool(row.get("has_trial_preference")),
                "selection_present": bool(row.get("highlight_selection_manifest_path")),
                "export_present": bool(export_entries or row.get("export_artifact_path")),
                "post_present": bool(post_entries or row.get("post_ledger_path")),
                "metrics_present": bool(metrics_entries),
                "hook_candidate_present": bool(hook_entries),
                "hook_count": len(hook_entries),
                "export_count": len(export_entries),
                "post_count": len(post_entries),
                "metrics_snapshot_count": len(metrics_entries),
                "coverage_tier": _coverage_tier_from_flags(
                    selection_present=bool(row.get("highlight_selection_manifest_path")),
                    export_present=bool(export_entries or row.get("export_artifact_path")),
                    post_present=bool(post_entries or row.get("post_ledger_path")),
                    metrics_present=bool(metrics_entries),
                    review_outcome=row.get("latest_review_status"),
                ),
                "hook_modes": _sorted_unique(item.get("hook_mode") for item in hook_entries),
                "hook_archetypes": _sorted_unique(item.get("hook_archetype") for item in hook_entries),
                "preferred_hook_id": preferred_hook.get("hook_id") if preferred_hook else None,
                "preferred_hook_mode": preferred_hook.get("hook_mode") if preferred_hook else None,
                "preferred_hook_archetype": preferred_hook.get("hook_archetype") if preferred_hook else None,
                "preferred_hook_strength": preferred_hook.get("hook_strength") if preferred_hook else 0.0,
                "preferred_hook_intensity_score": preferred_hook.get("intensity_score") if preferred_hook else 0.0,
                "preferred_hook_clarity_score": preferred_hook.get("clarity_score") if preferred_hook else 0.0,
                "preferred_hook_novelty_score": preferred_hook.get("novelty_score") if preferred_hook else 0.0,
                "preferred_hook_context_sufficiency_score": preferred_hook.get("context_sufficiency_score") if preferred_hook else 0.0,
                "preferred_hook_payoff_readability_score": preferred_hook.get("payoff_readability_score") if preferred_hook else 0.0,
                "preferred_hook_title_thumbnail_potential_score": preferred_hook.get("title_thumbnail_potential_score") if preferred_hook else 0.0,
                "preferred_hook_authenticity_risk_score": preferred_hook.get("authenticity_risk_score") if preferred_hook else 0.0,
                "preferred_hook_sound_off_legibility_score": preferred_hook.get("sound_off_legibility_score") if preferred_hook else 0.0,
                "preferred_hook_packaging_strategy": preferred_hook.get("packaging_strategy") if preferred_hook else None,
                "preferred_hook_packaging_strategy_present": bool((preferred_hook or {}).get("packaging_strategy")),
                "preferred_hook_rejection_reason_present": bool((preferred_hook or {}).get("rejection_reason")),
                "preferred_hook_mode_natural": 1.0 if str((preferred_hook or {}).get("hook_mode") or "") == "natural" else 0.0,
                "preferred_hook_mode_synthetic": 1.0 if str((preferred_hook or {}).get("hook_mode") or "") == "synthetic" else 0.0,
                "preferred_hook_mode_reject": 1.0 if str((preferred_hook or {}).get("hook_mode") or "") == "reject" else 0.0,
                **_hook_archetype_feature_columns(str((preferred_hook or {}).get("hook_archetype") or "")),
                "workflow_run_ids": _sorted_unique(item.get("workflow_run_id") for item in workflow_entries),
                "workflow_lineage": workflow_entries,
                "hook_comparison_report_count": comparison_summary["report_count"],
                "hook_comparison_recommendation_decisions": comparison_summary["recommendation_decisions"],
                "hook_comparison_statuses": comparison_summary["comparison_statuses"],
                "lifecycle_transitions": _parse_json(row.get("transitions_json")),
                "evidence_mode": _candidate_evidence_mode(
                    export_entries=export_entries,
                    post_entries=post_entries,
                    metrics_entries=metrics_entries,
                ),
                "latest_metrics_captured_at": latest_metric.get("captured_at") if latest_metric else None,
                "latest_metrics_coverage_status": latest_metric.get("metrics_coverage_status") if latest_metric else None,
                "latest_post_performance_coverage_tier": latest_metric.get("post_performance_coverage_tier") if latest_metric else ("posted_no_metrics" if post_entries else "no_post_record"),
                "latest_post_performance_label_eligible": bool(latest_metric.get("post_performance_label_eligible")) if latest_metric else False,
                "latest_post_performance_target_score": latest_metric.get("post_performance_target_score") if latest_metric else None,
                "latest_post_performance_target_bucket": latest_metric.get("post_performance_target_bucket") if latest_metric else None,
                "latest_post_performance_label_reason": latest_metric.get("post_performance_label_reason") if latest_metric else ("missing_metrics_snapshot_fields" if post_entries else "no_post_record"),
                "latest_post_performance_recoverable": bool(latest_metric.get("post_performance_recoverable")) if latest_metric else False,
                "latest_post_performance_missing_fields": list(latest_metric.get("post_performance_missing_fields", [])) if latest_metric else [],
                "latest_post_performance_minimum_signal_set": list(latest_metric.get("post_performance_minimum_signal_set", [])) if latest_metric else [],
                "latest_post_performance_recoverability_reason": latest_metric.get("post_performance_recoverability_reason") if latest_metric else ("no_post_record" if not post_entries else "missing_metrics_snapshot_fields"),
                "latest_post_performance_evidence_mode": _metrics_row_evidence_mode(latest_metric) if latest_metric else ("real_only" if not post_entries else None),
                "latest_view_count": latest_metric.get("view_count") if latest_metric else None,
                "latest_like_count": latest_metric.get("like_count") if latest_metric else None,
                "latest_comment_count": latest_metric.get("comment_count") if latest_metric else None,
                "latest_share_count": latest_metric.get("share_count") if latest_metric else None,
                "latest_save_count": latest_metric.get("save_count") if latest_metric else None,
                "latest_watch_time_seconds": latest_metric.get("watch_time_seconds") if latest_metric else None,
                "latest_average_watch_time_seconds": latest_metric.get("average_watch_time_seconds") if latest_metric else None,
                "latest_completion_rate": latest_metric.get("completion_rate") if latest_metric else None,
                "latest_engagement_rate": latest_metric.get("engagement_rate") if latest_metric else None,
                "latest_view_count_norm": _normalized_view_count(latest_metric),
                "latest_completion_rate_capped": _capped_rate(latest_metric.get("completion_rate") if latest_metric else None),
                "latest_engagement_rate_capped": _capped_rate(latest_metric.get("engagement_rate") if latest_metric else None),
                "selection_present_feature": 1.0 if bool(row.get("highlight_selection_manifest_path")) else 0.0,
                "account_context_present": 1.0 if any(str(item.get("account_id") or "").strip() for item in [*post_entries, *metrics_entries]) else 0.0,
                "outcome_platform_present": 1.0 if any(str(item.get("platform") or "").strip() for item in [*export_entries, *post_entries]) else 0.0,
                "performance_platform_present": 1.0 if any(str(item.get("platform") or "").strip() for item in metrics_entries) else 0.0,
                "metrics_complete_present": 1.0 if (latest_metric and latest_metric.get("metrics_coverage_status") == "complete") else 0.0,
                "post_performance_label_eligible_present": 1.0 if (latest_metric and latest_metric.get("post_performance_label_eligible")) else 0.0,
                "split_candidate_key": current_candidate_id,
                "split_fixture_key": row.get("fixture_id") or "",
                "split_lineage_key": _split_lineage_key(row),
            }
        )

    hook_dataset_rows: list[dict[str, Any]] = []
    for row in selected_hooks:
        current_candidate_id = str(row.get("candidate_id") or "")
        latest_metric = _latest_metric(metrics_by_candidate.get(current_candidate_id, []))
        comparison_summary = _hook_comparison_summary(comparisons_by_candidate.get(current_candidate_id, []))
        hook_dataset_rows.append(
            {
                "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
                "dataset_view": "hooks",
                "hook_id": row.get("hook_id"),
                "candidate_id": current_candidate_id,
                "event_id": row.get("event_id"),
                "game": row.get("game"),
                "source": row.get("source"),
                "fixture_id": row.get("fixture_id"),
                "fused_sidecar_path": row.get("fused_sidecar_path"),
                "lifecycle_state": row.get("lifecycle_state"),
                "hook_archetype": row.get("hook_archetype"),
                "hook_mode": row.get("hook_mode"),
                "hook_strength": row.get("hook_strength"),
                "hook_mode_natural": 1.0 if str(row.get("hook_mode") or "") == "natural" else 0.0,
                "hook_mode_synthetic": 1.0 if str(row.get("hook_mode") or "") == "synthetic" else 0.0,
                "hook_mode_reject": 1.0 if str(row.get("hook_mode") or "") == "reject" else 0.0,
                **_hook_archetype_feature_columns(str(row.get("hook_archetype") or "")),
                "intensity_score": row.get("intensity_score"),
                "clarity_score": row.get("clarity_score"),
                "novelty_score": row.get("novelty_score"),
                "context_sufficiency_score": row.get("context_sufficiency_score"),
                "payoff_readability_score": row.get("payoff_readability_score"),
                "title_thumbnail_potential_score": row.get("title_thumbnail_potential_score"),
                "authenticity_risk_score": row.get("authenticity_risk_score"),
                "sound_off_legibility_score": row.get("sound_off_legibility_score"),
                "packaging_strategy": row.get("packaging_strategy"),
                "packaging_strategy_present": bool(row.get("packaging_strategy")),
                "rejection_reason": row.get("rejection_reason"),
                "rejection_reason_present": bool(row.get("rejection_reason")),
                "highlight_selection_manifest_path": row.get("highlight_selection_manifest_path"),
                "hook_manifest_path": row.get("manifest_path"),
                "metadata_summary": _parse_json(row.get("metadata_summary_json")),
                "metadata_summary_present": _parse_json(row.get("metadata_summary_json")) is not None,
                "metadata_entity_present": bool(_metadata_summary_value(row.get("metadata_summary_json"), "entity_id")),
                "metadata_ability_present": bool(_metadata_summary_value(row.get("metadata_summary_json"), "ability_id")),
                "metadata_equipment_present": bool(_metadata_summary_value(row.get("metadata_summary_json"), "equipment_id")),
                "hook_comparison_report_count": comparison_summary["report_count"],
                "hook_comparison_recommendation_decisions": comparison_summary["recommendation_decisions"],
                "hook_comparison_statuses": comparison_summary["comparison_statuses"],
                "evidence_mode": _candidate_evidence_mode(
                    export_entries=exports_by_candidate.get(current_candidate_id, []),
                    post_entries=posts_by_candidate.get(current_candidate_id, []),
                    metrics_entries=metrics_by_candidate.get(current_candidate_id, []),
                ),
                "latest_metrics_captured_at": latest_metric.get("captured_at") if latest_metric else None,
                "latest_post_performance_coverage_tier": latest_metric.get("post_performance_coverage_tier") if latest_metric else None,
                "latest_post_performance_label_eligible": bool(latest_metric.get("post_performance_label_eligible")) if latest_metric else False,
                "latest_post_performance_target_score": latest_metric.get("post_performance_target_score") if latest_metric else None,
                "latest_post_performance_target_bucket": latest_metric.get("post_performance_target_bucket") if latest_metric else None,
                "latest_post_performance_label_reason": latest_metric.get("post_performance_label_reason") if latest_metric else None,
                "latest_post_performance_recoverable": bool(latest_metric.get("post_performance_recoverable")) if latest_metric else False,
                "latest_post_performance_missing_fields": list(latest_metric.get("post_performance_missing_fields", [])) if latest_metric else [],
                "latest_post_performance_minimum_signal_set": list(latest_metric.get("post_performance_minimum_signal_set", [])) if latest_metric else [],
                "latest_post_performance_recoverability_reason": latest_metric.get("post_performance_recoverability_reason") if latest_metric else None,
                "latest_post_performance_evidence_mode": _metrics_row_evidence_mode(latest_metric) if latest_metric else None,
                "latest_view_count": latest_metric.get("view_count") if latest_metric else None,
                "latest_engagement_rate": latest_metric.get("engagement_rate") if latest_metric else None,
                "split_candidate_key": current_candidate_id,
                "split_fixture_key": row.get("fixture_id") or "",
                "split_lineage_key": _split_lineage_key(row),
            }
        )

    outcome_dataset_rows: list[dict[str, Any]] = []
    handled_post_ids: set[str] = set()
    for export_row in selected_exports:
        current_candidate_id = str(export_row.get("candidate_id") or "")
        post_entries = posts_by_export.get(str(export_row.get("export_id") or ""), [])
        if not post_entries:
            outcome_dataset_rows.append(
                _build_outcome_row(
                    export_row=export_row,
                    post_row=None,
                    latest_metric=None,
                )
            )
            continue
        for post_row in post_entries:
            handled_post_ids.add(str(post_row.get("post_record_id") or ""))
            latest_metric = _latest_metric(metrics_by_post.get(str(post_row.get("post_record_id") or ""), []))
            outcome_dataset_rows.append(
                _build_outcome_row(
                    export_row=export_row,
                    post_row=post_row,
                    latest_metric=latest_metric,
                )
            )
        if current_candidate_id not in candidate_by_id:
            warnings.append(
                {
                    "code": "export_missing_candidate_lifecycle",
                    "candidate_id": current_candidate_id,
                    "export_id": export_row.get("export_id"),
                }
            )

    for post_row in selected_posts:
        post_record_id = str(post_row.get("post_record_id") or "")
        if post_record_id in handled_post_ids:
            continue
        export_row = exports_by_id.get(str(post_row.get("export_id") or ""))
        if export_row is None:
            warnings.append(
                {
                    "code": "post_record_missing_export",
                    "candidate_id": post_row.get("candidate_id"),
                    "post_record_id": post_record_id,
                    "export_id": post_row.get("export_id"),
                }
            )
        latest_metric = _latest_metric(metrics_by_post.get(post_record_id, []))
        outcome_dataset_rows.append(
            _build_outcome_row(
                export_row=export_row,
                post_row=post_row,
                latest_metric=latest_metric,
            )
        )

    performance_dataset_rows: list[dict[str, Any]] = []
    for metrics_row in selected_metrics:
        post_record_id = str(metrics_row.get("post_record_id") or "")
        export_id = str(metrics_row.get("export_id") or "")
        selected_highlight = _selected_highlight_details(metrics_row)
        post_row = posts_by_id.get(post_record_id)
        export_row = exports_by_id.get(export_id)
        hook_row = hook_by_id.get(str(metrics_row.get("hook_id") or "")) or (
            hook_by_id.get(str(export_row.get("hook_id") or "")) if export_row else None
        )
        if post_row is None:
            warnings.append(
                {
                    "code": "metrics_snapshot_missing_post_record",
                    "candidate_id": metrics_row.get("candidate_id"),
                    "snapshot_row_id": metrics_row.get("snapshot_row_id"),
                    "post_record_id": post_record_id,
                }
            )
        if export_row is None and export_id:
            warnings.append(
                {
                    "code": "metrics_snapshot_missing_export",
                    "candidate_id": metrics_row.get("candidate_id"),
                    "snapshot_row_id": metrics_row.get("snapshot_row_id"),
                    "export_id": export_id,
                }
            )
        performance_dataset_rows.append(
            {
                "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
                "dataset_view": "performance",
                "snapshot_row_id": metrics_row.get("snapshot_row_id"),
                "post_record_id": post_record_id or None,
                "export_id": export_id or None,
                "candidate_id": metrics_row.get("candidate_id"),
                "event_id": export_row.get("event_id") if export_row else post_row.get("event_id") if post_row else None,
                "hook_id": metrics_row.get("hook_id") or export_row.get("hook_id") if export_row else None,
                "game": metrics_row.get("game") or export_row.get("game") if export_row else post_row.get("game") if post_row else None,
                "fixture_id": export_row.get("fixture_id") if export_row else None,
                "platform": metrics_row.get("platform"),
                "account_id": metrics_row.get("account_id"),
                "captured_at": metrics_row.get("captured_at"),
                "is_latest_snapshot": bool(metrics_row.get("is_latest_snapshot")),
                "metrics_coverage_status": metrics_row.get("metrics_coverage_status"),
                "post_performance_coverage_tier": metrics_row.get("post_performance_coverage_tier"),
                "post_performance_label_eligible": bool(metrics_row.get("post_performance_label_eligible")),
                "post_performance_target_score": metrics_row.get("post_performance_target_score"),
                "post_performance_target_bucket": metrics_row.get("post_performance_target_bucket"),
                "post_performance_label_reason": metrics_row.get("post_performance_label_reason"),
                "post_performance_recoverable": bool(metrics_row.get("post_performance_recoverable")),
                "post_performance_missing_fields": list(metrics_row.get("post_performance_missing_fields", [])),
                "post_performance_minimum_signal_set": list(metrics_row.get("post_performance_minimum_signal_set", [])),
                "post_performance_recoverability_reason": metrics_row.get("post_performance_recoverability_reason"),
                "evidence_mode": _metrics_row_evidence_mode(metrics_row),
                "post_status": post_row.get("post_status") if post_row else metrics_row.get("post_status"),
                "posted_at": post_row.get("posted_at") if post_row else metrics_row.get("posted_at"),
                "external_post_id": metrics_row.get("external_post_id"),
                "external_url": metrics_row.get("external_url"),
                "hook_archetype": export_row.get("hook_archetype") if export_row else hook_row.get("hook_archetype") if hook_row else metrics_row.get("hook_archetype"),
                "hook_mode": export_row.get("hook_mode") if export_row else hook_row.get("hook_mode") if hook_row else metrics_row.get("hook_mode"),
                "packaging_strategy": export_row.get("packaging_strategy") if export_row else None,
                "view_count": metrics_row.get("view_count"),
                "like_count": metrics_row.get("like_count"),
                "comment_count": metrics_row.get("comment_count"),
                "share_count": metrics_row.get("share_count"),
                "save_count": metrics_row.get("save_count"),
                "watch_time_seconds": metrics_row.get("watch_time_seconds"),
                "average_watch_time_seconds": metrics_row.get("average_watch_time_seconds"),
                "completion_rate": metrics_row.get("completion_rate"),
                "engagement_rate": metrics_row.get("engagement_rate"),
                "post_ledger_manifest_path": metrics_row.get("post_ledger_manifest_path"),
                "export_batch_manifest_path": post_row.get("export_batch_manifest_path") if post_row else None,
                "highlight_selection_manifest_path": export_row.get("highlight_selection_manifest_path") if export_row else None,
                "hook_manifest_path": export_row.get("hook_manifest_path") if export_row else None,
                "fused_sidecar_path": export_row.get("fused_sidecar_path") if export_row else None,
                "selected_highlight_fusion_id": selected_highlight.get("fusion_id"),
                "selected_highlight_event_type": selected_highlight.get("event_type"),
                "selected_highlight_gate_status": selected_highlight.get("gate_status"),
                "selected_highlight_contributing_producer_families": list(selected_highlight.get("contributing_producer_families", [])),
                "metadata": _parse_json(metrics_row.get("metadata_json")),
                "split_candidate_key": metrics_row.get("candidate_id"),
                "split_fixture_key": export_row.get("fixture_id") if export_row else "",
                "split_lineage_key": _split_lineage_key(export_row or post_row or metrics_row),
            }
        )

    dataset_views = {
        "candidates": candidate_dataset_rows,
        "hooks": hook_dataset_rows,
        "outcomes": outcome_dataset_rows,
        "performance": performance_dataset_rows,
    }
    dataset_export_id = _dataset_export_id(query_results["registry_path"], filters)
    manifest = _build_manifest(
        dataset_export_id=dataset_export_id,
        registry_path=query_results["registry_path"],
        filters=filters,
        dataset_views=dataset_views,
        warnings=warnings,
    )
    return {
        "dataset_export_id": dataset_export_id,
        "dataset_views": dataset_views,
        "manifest": manifest,
    }


def _build_outcome_row(
    *,
    export_row: dict[str, Any] | None,
    post_row: dict[str, Any] | None,
    latest_metric: dict[str, Any] | None,
) -> dict[str, Any]:
    reference = export_row or post_row or {}
    selected_highlight = _selected_highlight_details(export_row or post_row or latest_metric or {})
    return {
        "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
        "dataset_view": "outcomes",
        "export_id": export_row.get("export_id") if export_row else post_row.get("export_id") if post_row else None,
        "post_record_id": post_row.get("post_record_id") if post_row else None,
        "candidate_id": reference.get("candidate_id"),
        "event_id": export_row.get("event_id") if export_row else post_row.get("event_id") if post_row else None,
        "hook_id": export_row.get("hook_id") if export_row else post_row.get("hook_id") if post_row else None,
        "fixture_id": export_row.get("fixture_id") if export_row else None,
        "game": export_row.get("game") if export_row else post_row.get("game") if post_row else None,
        "source": export_row.get("source") if export_row else None,
        "fused_sidecar_path": export_row.get("fused_sidecar_path") if export_row else None,
        "hook_manifest_path": export_row.get("hook_manifest_path") if export_row else None,
        "highlight_selection_manifest_path": export_row.get("highlight_selection_manifest_path") if export_row else None,
        "selected_highlight_fusion_id": selected_highlight.get("fusion_id"),
        "selected_highlight_event_type": selected_highlight.get("event_type"),
        "selected_highlight_gate_status": selected_highlight.get("gate_status"),
        "selected_highlight_contributing_producer_families": list(selected_highlight.get("contributing_producer_families", [])),
        "export_batch_manifest_path": export_row.get("manifest_path") if export_row else post_row.get("export_batch_manifest_path") if post_row else None,
        "export_status": export_row.get("export_status") if export_row else None,
        "export_artifact_path": export_row.get("export_artifact_path") if export_row else None,
        "otio_path": export_row.get("otio_path") if export_row else None,
        "post_ledger_manifest_path": post_row.get("manifest_path") if post_row else None,
        "post_status": post_row.get("post_status") if post_row else None,
        "posted_at": post_row.get("posted_at") if post_row else None,
        "platform": post_row.get("platform") if post_row else None,
        "account_id": post_row.get("account_id") if post_row else None,
        "external_post_id": post_row.get("external_post_id") if post_row else None,
        "external_url": post_row.get("external_url") if post_row else None,
        "caption_ref": post_row.get("caption_ref") if post_row else None,
        "caption_text": post_row.get("caption_text") if post_row else None,
        "duration_seconds": post_row.get("duration_seconds") if post_row else None,
        "media_asset_path": post_row.get("media_asset_path") if post_row else None,
        "initial_view_count": post_row.get("initial_view_count") if post_row else None,
        "initial_like_count": post_row.get("initial_like_count") if post_row else None,
        "initial_comment_count": post_row.get("initial_comment_count") if post_row else None,
        "hook_archetype": export_row.get("hook_archetype") if export_row else None,
        "hook_mode": export_row.get("hook_mode") if export_row else None,
        "packaging_strategy": export_row.get("packaging_strategy") if export_row else None,
        "final_score": export_row.get("final_score") if export_row else None,
        "start_seconds": export_row.get("start_seconds") if export_row else None,
        "end_seconds": export_row.get("end_seconds") if export_row else None,
        "evidence_mode": _candidate_evidence_mode(
            export_entries=[export_row] if export_row else [],
            post_entries=[post_row] if post_row else [],
            metrics_entries=[latest_metric] if latest_metric else [],
        ),
        "latest_metrics_captured_at": latest_metric.get("captured_at") if latest_metric else None,
        "latest_metrics_coverage_status": latest_metric.get("metrics_coverage_status") if latest_metric else None,
        "latest_post_performance_coverage_tier": latest_metric.get("post_performance_coverage_tier") if latest_metric else ("posted_no_metrics" if post_row else "no_post_record"),
        "latest_post_performance_label_eligible": bool(latest_metric.get("post_performance_label_eligible")) if latest_metric else False,
        "latest_post_performance_target_score": latest_metric.get("post_performance_target_score") if latest_metric else None,
        "latest_post_performance_target_bucket": latest_metric.get("post_performance_target_bucket") if latest_metric else None,
        "latest_post_performance_label_reason": latest_metric.get("post_performance_label_reason") if latest_metric else ("missing_metrics_snapshot_fields" if post_row else "no_post_record"),
        "latest_post_performance_recoverable": bool(latest_metric.get("post_performance_recoverable")) if latest_metric else False,
        "latest_post_performance_missing_fields": list(latest_metric.get("post_performance_missing_fields", [])) if latest_metric else [],
        "latest_post_performance_minimum_signal_set": list(latest_metric.get("post_performance_minimum_signal_set", [])) if latest_metric else [],
        "latest_post_performance_recoverability_reason": latest_metric.get("post_performance_recoverability_reason") if latest_metric else ("no_post_record" if not post_row else "missing_metrics_snapshot_fields"),
        "latest_post_performance_evidence_mode": _metrics_row_evidence_mode(latest_metric) if latest_metric else ("real_only" if not post_row else None),
        "latest_view_count": latest_metric.get("view_count") if latest_metric else None,
        "latest_like_count": latest_metric.get("like_count") if latest_metric else None,
        "latest_comment_count": latest_metric.get("comment_count") if latest_metric else None,
        "latest_share_count": latest_metric.get("share_count") if latest_metric else None,
        "latest_save_count": latest_metric.get("save_count") if latest_metric else None,
        "latest_watch_time_seconds": latest_metric.get("watch_time_seconds") if latest_metric else None,
        "latest_average_watch_time_seconds": latest_metric.get("average_watch_time_seconds") if latest_metric else None,
        "latest_completion_rate": latest_metric.get("completion_rate") if latest_metric else None,
        "latest_engagement_rate": latest_metric.get("engagement_rate") if latest_metric else None,
        "split_candidate_key": reference.get("candidate_id"),
        "split_fixture_key": export_row.get("fixture_id") if export_row else "",
        "split_lineage_key": _split_lineage_key(reference),
    }


def _build_manifest(
    *,
    dataset_export_id: str,
    registry_path: str,
    filters: dict[str, Any],
    dataset_views: dict[str, list[dict[str, Any]]],
    warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    coverage = dataset_views["candidates"]
    return {
        "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
        "dataset_export_id": dataset_export_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "registry_path": registry_path,
        "filters": {key: value for key, value in filters.items() if value is not None},
        "dataset_views": {
            view: {
                "row_count": len(rows),
            }
            for view, rows in dataset_views.items()
        },
        "row_count": sum(len(rows) for rows in dataset_views.values()),
        "coverage_counts": {
            "candidate_count": len(dataset_views["candidates"]),
            "hook_count": len(dataset_views["hooks"]),
            "outcome_count": len(dataset_views["outcomes"]),
            "performance_count": len(dataset_views["performance"]),
            "candidates_with_hooks": sum(1 for row in coverage if row.get("hook_candidate_present")),
            "candidates_with_exports": sum(1 for row in coverage if row.get("export_present")),
            "candidates_with_posts": sum(1 for row in coverage if row.get("post_present")),
            "candidates_with_metrics": sum(1 for row in coverage if row.get("metrics_present")),
            "candidates_with_eligible_post_performance_labels": sum(1 for row in coverage if row.get("latest_post_performance_label_eligible")),
        },
        "split_dimensions": ["candidate_id", "fixture_id", "game", "source"],
        "legacy_training_export_note": "training_export_v1 remains supported for proxy-scan sidecars and is not the canonical V2 training export surface.",
        "warning_count": len(warnings),
        "warnings": warnings,
    }


def _selected_candidate_ids(
    candidate_rows: list[dict[str, Any]],
    *,
    hook_rows: list[dict[str, Any]],
    post_rows: list[dict[str, Any]],
    metrics_rows: list[dict[str, Any]],
    hook_filter_active: bool,
    platform_filter_active: bool,
) -> set[str]:
    candidate_ids = {str(row.get("candidate_id") or "") for row in candidate_rows if row.get("candidate_id")}
    if hook_filter_active:
        candidate_ids &= {str(row.get("candidate_id") or "") for row in hook_rows if row.get("candidate_id")}
    if platform_filter_active:
        platform_ids = {
            str(row.get("candidate_id") or "")
            for row in [*post_rows, *metrics_rows]
            if row.get("candidate_id")
        }
        candidate_ids &= platform_ids
    return candidate_ids


def _normalize_evidence_mode(value: str | None) -> str | None:
    normalized = str(value or "mixed").strip().lower()
    return normalized if normalized in {"mixed", "real_only", "synthetic_augmented"} else None


def _matches_evidence_mode(row_mode: str, requested_mode: str) -> bool:
    if requested_mode == "mixed":
        return True
    return row_mode == requested_mode


def _candidate_evidence_mode(
    *,
    export_entries: list[dict[str, Any]],
    post_entries: list[dict[str, Any]],
    metrics_entries: list[dict[str, Any]],
) -> str:
    row_modes = [
        *[_export_row_evidence_mode(row) for row in export_entries],
        *[_post_row_evidence_mode(row) for row in post_entries],
        *[_metrics_row_evidence_mode(row) for row in metrics_entries],
    ]
    return "synthetic_augmented" if "synthetic_augmented" in row_modes else "real_only"


def _export_row_evidence_mode(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return "real_only"
    for value in (
        row.get("manifest_path"),
        row.get("hook_manifest_path"),
        row.get("highlight_selection_manifest_path"),
        row.get("export_artifact_path"),
        row.get("otio_path"),
    ):
        if _is_synthetic_path(value):
            return "synthetic_augmented"
    return "real_only"


def _post_row_evidence_mode(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return "real_only"
    for value in (
        row.get("manifest_path"),
        row.get("export_batch_manifest_path"),
        row.get("media_asset_path"),
    ):
        if _is_synthetic_path(value):
            return "synthetic_augmented"
    return "real_only"


def _metrics_row_evidence_mode(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return "real_only"
    metadata = row.get("metadata")
    if not isinstance(metadata, dict):
        metadata = _parse_json(row.get("metadata_json"))
    if isinstance(metadata, dict) and bool(metadata.get("synthetic_benchmark")):
        return "synthetic_augmented"
    for value in (
        row.get("manifest_path"),
        row.get("post_ledger_manifest_path"),
        row.get("export_batch_manifest_path"),
        row.get("highlight_selection_manifest_path"),
    ):
        if _is_synthetic_path(value):
            return "synthetic_augmented"
    return "real_only"


def _is_synthetic_path(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return bool(normalized) and "synthetic" in normalized


def _index_fused_events(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        event_id = str(row.get("event_id") or "").strip()
        sidecar_path = str(row.get("sidecar_path") or "").strip()
        if not event_id or not sidecar_path:
            continue
        result[(event_id, sidecar_path)] = row
    return result


def _preferred_hook_entry(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return sorted(
        rows,
        key=lambda row: (
            float(row.get("hook_strength") or 0.0),
            str(row.get("hook_id") or ""),
        ),
        reverse=True,
    )[0]


def _duration_seconds(row: dict[str, Any] | None) -> float:
    if not row:
        return 0.0
    start = float(row.get("suggested_start_timestamp") or 0.0)
    end = float(row.get("suggested_end_timestamp") or start)
    return round(max(0.0, end - start), 6)


def _contains_token(value: Any, token: str) -> float:
    return 1.0 if token in str(value or "").strip().lower() else 0.0


def _hook_archetype_feature_columns(archetype: str) -> dict[str, float]:
    normalized = str(archetype or "").strip().lower()
    return {
        "hook_archetype_clutch": 1.0 if normalized == "clutch" else 0.0,
        "hook_archetype_reversal": 1.0 if normalized == "reversal" else 0.0,
        "hook_archetype_domination": 1.0 if normalized == "domination" else 0.0,
        "hook_archetype_comedy": 1.0 if normalized == "comedy" else 0.0,
        "hook_archetype_chaos": 1.0 if normalized == "chaos" else 0.0,
        "hook_archetype_fail": 1.0 if normalized == "fail" else 0.0,
        "hook_archetype_flex": 1.0 if normalized == "flex" else 0.0,
        "hook_archetype_other": 1.0 if normalized in {"", "other"} else 0.0,
    }


def _normalized_view_count(metric_row: dict[str, Any] | None) -> float:
    if not metric_row:
        return 0.0
    return min(float(metric_row.get("view_count") or 0.0) / 1000.0, 1.0)


def _capped_rate(value: Any) -> float:
    return min(max(float(value or 0.0), 0.0), 1.0)


def _coverage_tier_from_flags(
    *,
    selection_present: bool,
    export_present: bool,
    post_present: bool,
    metrics_present: bool,
    review_outcome: Any,
) -> str:
    if metrics_present:
        return "posted_with_metrics"
    if post_present:
        return "posted"
    if export_present:
        return "exported"
    if selection_present:
        return "selected_for_export"
    if str(review_outcome or "").strip().lower() in {"approved", "rejected"}:
        return "reviewed"
    return "unreviewed"


def _metadata_summary_value(value: Any, key: str) -> Any:
    payload = _parse_json(value)
    if isinstance(payload, dict):
        return payload.get(key)
    return None


def _workflow_lineage_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lineage: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        key = (
            row.get("workflow_run_id"),
            row.get("workflow_type"),
            row.get("stage"),
            row.get("run_status"),
            row.get("item_status"),
        )
        if key in seen:
            continue
        seen.add(key)
        lineage.append(
            {
                "workflow_run_id": row.get("workflow_run_id"),
                "workflow_type": row.get("workflow_type"),
                "stage": row.get("stage"),
                "run_status": row.get("run_status"),
                "item_status": row.get("item_status"),
                "manifest_path": row.get("manifest_path"),
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
            }
        )
    lineage.sort(
        key=lambda item: (
            str(item.get("workflow_run_id") or ""),
            str(item.get("workflow_type") or ""),
            str(item.get("stage") or ""),
        )
    )
    return lineage


def _hook_comparison_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    decisions = _sorted_unique(row.get("recommendation_decision") for row in rows)
    statuses = _sorted_unique(row.get("comparison_status") for row in rows)
    return {
        "report_count": len(rows),
        "recommendation_decisions": decisions,
        "comparison_statuses": statuses,
    }


def _latest_metric(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    latest = sorted(
        rows,
        key=lambda row: (
            str(row.get("captured_at") or ""),
            str(row.get("snapshot_row_id") or ""),
        ),
        reverse=True,
    )[0]
    return latest


def _group_by(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        value = str(row.get(key) or "").strip()
        if not value:
            continue
        grouped.setdefault(value, []).append(row)
    return grouped


def _parse_json(value: Any) -> Any:
    if value in (None, "", "null"):
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return value


def _selected_highlight_details(row: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    payload = _parse_json(row.get("selected_highlight_details_json"))
    return payload if isinstance(payload, dict) else {}


def _split_lineage_key(row: dict[str, Any] | None) -> str:
    if not row:
        return ""
    return "::".join(
        [
            str(row.get("game") or ""),
            str(row.get("source") or ""),
            str(row.get("fixture_id") or ""),
            str(row.get("candidate_id") or ""),
        ]
    )


def _sorted_unique(values: Any) -> list[Any]:
    cleaned = [value for value in values if value not in (None, "")]
    return sorted(set(cleaned))


def _write_dataset_artifacts(dataset: dict[str, Any], paths: dict[str, Path]) -> None:
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    manifest = dict(dataset["manifest"])
    for view in DATASET_VIEWS:
        rows = dataset["dataset_views"][view]
        jsonl_path = paths[f"{view}_jsonl_path"]
        csv_path = paths[f"{view}_csv_path"]
        _write_jsonl(jsonl_path, rows)
        _write_csv(csv_path, rows)
        manifest["dataset_views"][view]["jsonl_path"] = str(jsonl_path)
        manifest["dataset_views"][view]["csv_path"] = str(csv_path)

    paths["manifest_path"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


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


def _dataset_paths(output_root: Path, dataset_export_id: str, game: str | None) -> dict[str, Path]:
    scope = game or "all"
    base_dir = output_root / scope
    paths: dict[str, Path] = {
        "manifest_path": base_dir / f"{dataset_export_id}.manifest.json",
    }
    for view in DATASET_VIEWS:
        paths[f"{view}_jsonl_path"] = base_dir / f"{dataset_export_id}.{view}.jsonl"
        paths[f"{view}_csv_path"] = base_dir / f"{dataset_export_id}.{view}.csv"
    return paths


def _dataset_export_id(registry_path: str, filters: dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "registry_path": str(Path(registry_path).resolve()),
            "filters": {key: value for key, value in filters.items() if value is not None},
            "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
        },
        sort_keys=True,
    )
    return f"v2-training-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _resolve_output_root(output_root: str | Path | None) -> Path:
    root = Path(output_root).expanduser() if output_root is not None else DEFAULT_OUTPUT_ROOT
    if not root.is_absolute():
        root = (Path.cwd() / root).resolve()
    else:
        root = root.resolve()
    return root
