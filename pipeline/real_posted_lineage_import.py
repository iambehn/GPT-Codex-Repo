from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.clip_registry import _post_performance_label_fields, query_clip_registry, refresh_clip_registry
from pipeline.shadow_benchmark_evidence_comparison import (
    SHADOW_BENCHMARK_EVIDENCE_COMPARISON_SCHEMA_VERSION,
    compare_shadow_benchmark_evidence_modes,
)
from pipeline.shadow_benchmark_matrix import run_shadow_benchmark_matrix
from pipeline.shadow_benchmark_review import review_shadow_benchmark_results
from pipeline.v2_training_export import export_v2_training_datasets


REPO_ROOT = Path(__file__).resolve().parent.parent
REAL_POSTED_LINEAGE_IMPORT_SCHEMA_VERSION = "real_posted_lineage_import_v1"
REAL_ARTIFACT_INTAKE_VALIDATION_SCHEMA_VERSION = "real_artifact_intake_validation_v1"
REAL_ARTIFACT_INTAKE_REFRESH_SCHEMA_VERSION = "real_artifact_intake_refresh_v1"
REAL_ARTIFACT_INTAKE_SUMMARY_SCHEMA_VERSION = "real_artifact_intake_summary_v1"
REAL_ARTIFACT_INTAKE_BUNDLE_BOOTSTRAP_SCHEMA_VERSION = "real_artifact_intake_bundle_bootstrap_v1"
REAL_ARTIFACT_INTAKE_BUNDLE_MANIFEST_SCHEMA_VERSION = "real_artifact_intake_bundle_manifest_v1"
REAL_ARTIFACT_INTAKE_COVERAGE_REPORT_SCHEMA_VERSION = "real_artifact_intake_coverage_report_v1"
REAL_ARTIFACT_INTAKE_DEDUP_ADVISORY_SCHEMA_VERSION = "real_artifact_intake_dedup_advisory_v1"
REAL_ARTIFACT_INTAKE_DEDUP_RESOLUTION_SCHEMA_VERSION = "real_artifact_intake_dedup_resolution_v1"
REAL_ARTIFACT_INTAKE_DEDUP_RESOLUTION_SUMMARY_SCHEMA_VERSION = "real_artifact_intake_dedup_resolution_summary_v1"
REAL_ARTIFACT_INTAKE_DEDUP_RESOLUTION_UPDATE_SCHEMA_VERSION = "real_artifact_intake_dedup_resolution_update_v1"
REAL_ARTIFACT_INTAKE_REFRESH_PREFLIGHT_SCHEMA_VERSION = "real_artifact_intake_refresh_preflight_v1"
REAL_ARTIFACT_INTAKE_REFRESH_PREFLIGHT_HISTORY_SCHEMA_VERSION = "real_artifact_intake_refresh_preflight_history_v1"
REAL_ARTIFACT_INTAKE_REFRESH_PREFLIGHT_HISTORY_SUMMARY_SCHEMA_VERSION = "real_artifact_intake_refresh_preflight_history_summary_v1"
REAL_ARTIFACT_INTAKE_REFRESH_PREFLIGHT_TREND_REPORT_SCHEMA_VERSION = "real_artifact_intake_refresh_preflight_trend_report_v1"
REAL_ARTIFACT_INTAKE_REFRESH_OUTCOME_HISTORY_SCHEMA_VERSION = "real_artifact_intake_refresh_outcome_history_v1"
REAL_ARTIFACT_INTAKE_REFRESH_OUTCOME_HISTORY_SUMMARY_SCHEMA_VERSION = "real_artifact_intake_refresh_outcome_history_summary_v1"
REAL_ARTIFACT_INTAKE_REFRESH_OUTCOME_TREND_REPORT_SCHEMA_VERSION = "real_artifact_intake_refresh_outcome_trend_report_v1"
REAL_ARTIFACT_INTAKE_HISTORY_COMPARISON_REPORT_SCHEMA_VERSION = "real_artifact_intake_history_comparison_report_v1"
REAL_ARTIFACT_INTAKE_DASHBOARD_SCHEMA_VERSION = "real_artifact_intake_dashboard_v1"
REAL_ARTIFACT_INTAKE_DASHBOARD_REGISTRY_SUMMARY_SCHEMA_VERSION = "real_artifact_intake_dashboard_registry_summary_v1"
REAL_ARTIFACT_INTAKE_COMPARISON_TARGET_SUMMARY_SCHEMA_VERSION = "real_artifact_intake_comparison_target_summary_v1"
REAL_ARTIFACT_INTAKE_DASHBOARD_SUMMARY_HISTORY_SCHEMA_VERSION = "real_artifact_intake_dashboard_summary_history_v1"
REAL_ARTIFACT_INTAKE_DASHBOARD_SUMMARY_HISTORY_SUMMARY_SCHEMA_VERSION = "real_artifact_intake_dashboard_summary_history_summary_v1"
REAL_ARTIFACT_INTAKE_DASHBOARD_SUMMARY_TREND_REPORT_SCHEMA_VERSION = "real_artifact_intake_dashboard_summary_trend_report_v1"
DEFAULT_IMPORT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "real_posted_lineage_imports"
DEFAULT_REAL_ARTIFACT_INTAKE_ROOT = REPO_ROOT / "outputs" / "real_artifact_intake"
DEFAULT_REAL_ARTIFACT_INTAKE_BUNDLES_ROOT = DEFAULT_REAL_ARTIFACT_INTAKE_ROOT / "bundles"

_WORKSPACE_ARTIFACT_SUFFIXES = (
    ".proxy_scan.json",
    ".runtime_analysis.json",
    ".fused_analysis.json",
    ".runtime_review_session.json",
    ".fused_review_session.json",
    ".highlight_selection.json",
    ".hook_candidates.json",
    "hook_comparison.json",
    ".highlight_export_batch.json",
    ".posted_highlight_ledger.json",
    ".posted_highlight_metrics_snapshot.json",
    ".shadow_evaluation_policy.json",
    ".shadow_ranking_model.json",
    ".shadow_ranking_experiment.json",
    ".shadow_experiment_ledger.json",
    ".shadow_ranking_replay.json",
    ".shadow_ranking_comparison.json",
    ".shadow_model_family_comparison.json",
    ".shadow_benchmark_matrix.json",
    ".shadow_benchmark_review.json",
    ".workflow_run.json",
    "fixture_trial_run_manifest.json",
    "fixture_trial_batch_manifest.json",
)
_SOURCE_ARTIFACT_SUFFIXES = (
    ".fused_analysis.json",
    ".hook_candidates.json",
    "hook_comparison.json",
    ".highlight_selection.json",
    ".highlight_export_batch.json",
    ".posted_highlight_ledger.json",
    ".posted_highlight_metrics_snapshot.json",
    ".workflow_run.json",
)

DEFAULT_REAL_ONLY_REFRESH_OUTPUT_ROOT = REPO_ROOT / "outputs" / "real_only_benchmark_refresh"


def validate_real_artifact_intake(
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_intake_root = _resolve_path(intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    bundles_root = resolved_intake_root / "bundles"
    bundle_roots = _intake_bundle_roots(resolved_intake_root)
    resolved_intake_root.mkdir(parents=True, exist_ok=True)

    bundle_summaries = [_intake_bundle_summary(bundle_root, game=game, platform=platform) for bundle_root in bundle_roots]
    real_records = [record for summary in bundle_summaries for record in summary.pop("_real_records")]
    synthetic_records = [record for summary in bundle_summaries for record in summary.pop("_synthetic_records")]
    intake_status = _intake_status_from_bundle_summaries(bundle_summaries)

    validation_root = resolved_intake_root / "_intake_validation"
    validation_root.mkdir(parents=True, exist_ok=True)
    import_manifest_path = validation_root / "validation.real_posted_lineage_import.json"
    import_registry_path = validation_root / "registry.sqlite"

    import_result: dict[str, Any] | None = None
    coverage_inventory: dict[str, Any] = {
        "candidate_lifecycle_row_count": 0,
        "imported_candidate_count": 0,
        "imported_hook_count": 0,
        "imported_export_count": 0,
        "imported_post_count": 0,
        "imported_posted_metrics_row_count": 0,
        "eligible_real_post_performance_label_count": 0,
        "selected_event_type_counts": {},
        "selected_producer_family_counts": {},
        "workspace_root": str(validation_root.resolve()),
        "game": game,
        "platform": platform,
    }
    imported_counts = _empty_imported_counts()
    unresolved_lineage_counts = {"candidate": 0, "hook": 0, "export": 0, "post": 0, "metrics": 0, "other": 0}
    warnings: list[dict[str, Any]] = []

    if bundle_roots:
        import_result = import_real_posted_lineage(
            source_roots=bundle_roots,
            registry_path=import_registry_path,
            game=game,
            platform=platform,
            output_path=import_manifest_path,
        )
        if import_result.get("ok"):
            coverage_inventory = dict(import_result.get("coverage_inventory", {}))
            imported_counts = dict(import_result.get("imported_counts", {}))
            unresolved_lineage_counts = dict(import_result.get("unresolved_lineage_counts", unresolved_lineage_counts))
            warnings = list(import_result.get("warnings", []))
            bundle_summaries = _enrich_bundle_summaries(bundle_summaries, warnings=warnings)
            intake_status = _intake_status_from_coverage(bundle_summaries=bundle_summaries, coverage_inventory=coverage_inventory)

    if not warnings:
        bundle_summaries = _enrich_bundle_summaries(bundle_summaries, warnings=warnings)
    readiness_rollups = _bundle_readiness_rollups(bundle_summaries)
    dedup_resolution_summary = _dedup_resolution_summary_for_bundle_summaries(
        bundle_summaries,
        intake_root=resolved_intake_root,
    )

    result = {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_VALIDATION_SCHEMA_VERSION,
        "validated_at": datetime.now(UTC).isoformat(),
        "intake_root": str(resolved_intake_root.resolve()),
        "bundles_root": str(bundles_root.resolve()),
        "bundle_count": len(bundle_roots),
        "bundle_roots": [str(bundle.resolve()) for bundle in bundle_roots],
        "intake_status": intake_status,
        "bundle_summaries": bundle_summaries,
        "discovered_real_artifact_count": len(real_records),
        "discovered_synthetic_artifact_count": len(synthetic_records),
        "discovered_real_counts": _artifact_counts(real_records),
        "discovered_synthetic_counts": _artifact_counts(synthetic_records),
        "imported_counts": imported_counts,
        "coverage_inventory": coverage_inventory,
        "bundle_readiness_rollups": readiness_rollups,
        "dedup_resolution_summary": dedup_resolution_summary,
        "unresolved_lineage_counts": unresolved_lineage_counts,
        "warning_count": len(warnings),
        "warnings": warnings,
        "validation_import_manifest_path": str(import_manifest_path.resolve()) if import_manifest_path.exists() else None,
        "validation_registry_path": str(import_registry_path.resolve()) if import_registry_path.exists() else None,
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
    }
    target = _resolve_path(output_path) if output_path is not None else resolved_intake_root / "reports" / "real_artifact_intake.validation.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    result["manifest_path"] = str(target.resolve())
    return result


def bootstrap_real_artifact_intake_bundle(
    bundle_name: str,
    *,
    intake_root: str | Path | None = None,
) -> dict[str, Any]:
    normalized_bundle_name = _normalized_bundle_name(bundle_name)
    if not normalized_bundle_name:
        return {
            "ok": False,
            "status": "invalid_bundle_name",
            "error": "bundle name must contain at least one alphanumeric character",
        }
    resolved_intake_root = _resolve_path(intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    bundle_root = resolved_intake_root / "bundles" / normalized_bundle_name
    subdirectories = {
        "fused": bundle_root / "fused",
        "hooks": bundle_root / "hooks",
        "selection": bundle_root / "selection",
        "exports": bundle_root / "exports",
        "posted": bundle_root / "posted",
        "metrics": bundle_root / "metrics",
    }
    for path in subdirectories.values():
        path.mkdir(parents=True, exist_ok=True)
        gitkeep = path / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.write_text("", encoding="utf-8")
    bundle_manifest_path = bundle_root / "bundle.manifest.json"
    if not bundle_manifest_path.exists():
        bundle_manifest_path.write_text(
            json.dumps(_bundle_manifest_template(normalized_bundle_name), indent=2),
            encoding="utf-8",
        )
    checklist_path = bundle_root / "CHECKLIST.md"
    if not checklist_path.exists():
        checklist_path.write_text(_bundle_checklist_contents(normalized_bundle_name), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_BUNDLE_BOOTSTRAP_SCHEMA_VERSION,
        "intake_root": str(resolved_intake_root.resolve()),
        "bundle_name": normalized_bundle_name,
        "bundle_root": str(bundle_root.resolve()),
        "subdirectories": {name: str(path.resolve()) for name, path in subdirectories.items()},
        "bundle_manifest_path": str(bundle_manifest_path.resolve()),
        "checklist_path": str(checklist_path.resolve()),
    }


def summarize_real_artifact_intake(
    validation_manifest: str | Path | None = None,
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    payload, source_manifest_path = _load_or_build_intake_validation(
        validation_manifest=validation_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if payload is None:
        return {
            "ok": False,
            "status": "invalid_real_artifact_intake_validation",
            "error": "validation artifact is missing or malformed",
        }
    if str(payload.get("schema_version") or "").strip() != REAL_ARTIFACT_INTAKE_VALIDATION_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "unsupported_real_artifact_intake_validation",
            "error": f"unsupported validation schema version: {payload.get('schema_version')}",
            "manifest_path": source_manifest_path,
        }
    bundle_summaries = [summary for summary in list(payload.get("bundle_summaries", [])) if isinstance(summary, dict)]
    rollups = payload.get("bundle_readiness_rollups")
    if not isinstance(rollups, dict):
        rollups = _bundle_readiness_rollups(bundle_summaries)
    summary_rows = [
        {
            "bundle_name": row.get("bundle_name"),
            "bundle_root": row.get("bundle_root"),
            "readiness_status": row.get("readiness_status"),
            "dominant_gap_reason": row.get("dominant_gap_reason"),
            "eligible_post_performance_label_count": row.get("eligible_post_performance_label_count", 0),
            "unresolved_lineage_warning_count": row.get("unresolved_lineage_warning_count", 0),
            "missing_required_artifact_types": row.get("missing_required_artifact_types", []),
            "bundle_manifest_present": row.get("bundle_manifest_present", False),
            "bundle_manifest_valid": row.get("bundle_manifest_valid", False),
            "bundle_manifest_errors": row.get("bundle_manifest_errors", []),
            "manifest_source_label": row.get("manifest_source_label"),
            "manifest_game": row.get("manifest_game"),
            "manifest_platform": row.get("manifest_platform"),
            "manifest_date_range": row.get("manifest_date_range"),
        }
        for row in bundle_summaries
    ]
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_SUMMARY_SCHEMA_VERSION,
        "manifest_path": source_manifest_path,
        "intake_root": payload.get("intake_root"),
        "intake_status": payload.get("intake_status"),
        "bundle_count": payload.get("bundle_count", 0),
        "coverage_inventory": payload.get("coverage_inventory", {}),
        "bundle_readiness_rollups": rollups,
        "dedup_resolution_summary": payload.get("dedup_resolution_summary", {}),
        "bundle_summaries": summary_rows,
        "warning_count": payload.get("warning_count", 0),
    }


def report_real_artifact_intake_coverage(
    validation_manifest: str | Path | None = None,
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    payload, source_manifest_path = _load_or_build_intake_validation(
        validation_manifest=validation_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if payload is None:
        return {
            "ok": False,
            "status": "invalid_real_artifact_intake_validation",
            "error": "validation artifact is missing or malformed",
        }
    bundle_summaries = [summary for summary in list(payload.get("bundle_summaries", [])) if isinstance(summary, dict)]
    game_rollup = _bundle_count_rollup(bundle_summaries, key="manifest_game")
    platform_rollup = _bundle_count_rollup(bundle_summaries, key="manifest_platform")
    date_range_rollup = _bundle_count_rollup(bundle_summaries, key="manifest_date_range", formatter=_date_range_label)
    duplicate_summary = _cross_bundle_duplicate_summary(bundle_summaries)
    bundle_rows = [
        {
            "bundle_name": row.get("bundle_name"),
            "bundle_root": row.get("bundle_root"),
            "readiness_status": row.get("readiness_status"),
            "dominant_gap_reason": row.get("dominant_gap_reason"),
            "manifest_game": row.get("manifest_game"),
            "manifest_platform": row.get("manifest_platform"),
            "manifest_date_range": row.get("manifest_date_range"),
            "manifest_source_label": row.get("manifest_source_label"),
            "eligible_post_performance_label_count": row.get("eligible_post_performance_label_count", 0),
            "candidate_artifact_count": row.get("candidate_artifact_count", 0),
            "export_artifact_count": row.get("export_artifact_count", 0),
            "post_artifact_count": row.get("post_artifact_count", 0),
            "metrics_artifact_count": row.get("metrics_artifact_count", 0),
            "duplicate_downstream_record_count": duplicate_summary["bundle_duplicate_counts"].get(str(row.get("bundle_name") or ""), 0),
        }
        for row in bundle_summaries
    ]
    benchmark_ready_count = int(payload.get("bundle_readiness_rollups", {}).get("benchmark_ready_bundle_count", 0))
    eligible_label_count = int(payload.get("coverage_inventory", {}).get("eligible_real_post_performance_label_count") or 0)
    sufficiency_status = "ready_for_real_only_refresh" if benchmark_ready_count > 0 and eligible_label_count > 0 else "needs_more_real_bundles"
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_COVERAGE_REPORT_SCHEMA_VERSION,
        "manifest_path": source_manifest_path,
        "intake_root": payload.get("intake_root"),
        "intake_status": payload.get("intake_status"),
        "bundle_count": payload.get("bundle_count", 0),
        "bundle_count_by_game": game_rollup,
        "bundle_count_by_platform": platform_rollup,
        "bundle_count_by_date_range": date_range_rollup,
        "duplicate_downstream_summary": duplicate_summary,
        "bundle_contributions": bundle_rows,
        "coverage_inventory": payload.get("coverage_inventory", {}),
        "bundle_readiness_rollups": payload.get("bundle_readiness_rollups", {}),
        "sufficiency_assessment": {
            "status": sufficiency_status,
            "benchmark_ready_bundle_count": benchmark_ready_count,
            "eligible_real_post_performance_label_count": eligible_label_count,
            "duplicate_downstream_record_total": duplicate_summary["duplicate_downstream_record_total"],
        },
    }


def advise_real_artifact_intake_dedup(
    validation_manifest: str | Path | None = None,
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    coverage_report = report_real_artifact_intake_coverage(
        validation_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not coverage_report.get("ok"):
        return coverage_report
    bundle_rows = [row for row in list(coverage_report.get("bundle_contributions", [])) if isinstance(row, dict)]
    bundle_index = {str(row.get("bundle_name") or ""): row for row in bundle_rows}
    duplicate_rows = [
        row
        for row in list(coverage_report.get("duplicate_downstream_summary", {}).get("duplicate_rows", []))
        if isinstance(row, dict)
    ]
    advisory_rows = [
        _dedup_advisory_row(row, bundle_index=bundle_index)
        for row in duplicate_rows
    ]
    recommended_cleanup_count = sum(1 for row in advisory_rows if row.get("non_canonical_bundle_names"))
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_DEDUP_ADVISORY_SCHEMA_VERSION,
        "manifest_path": coverage_report.get("manifest_path"),
        "intake_root": coverage_report.get("intake_root"),
        "coverage_report_schema_version": coverage_report.get("schema_version"),
        "duplicate_group_count": len(advisory_rows),
        "recommended_cleanup_group_count": recommended_cleanup_count,
        "advisories": advisory_rows,
        "sufficiency_assessment": coverage_report.get("sufficiency_assessment", {}),
    }


def materialize_real_artifact_intake_dedup_resolutions(
    advisory_manifest: str | Path | None = None,
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    advisory, source_manifest_path = _load_or_build_intake_dedup_advisory(
        advisory_manifest=advisory_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if advisory is None:
        return {
            "ok": False,
            "status": "invalid_real_artifact_intake_dedup_advisory",
            "error": "dedup advisory artifact is missing or malformed",
        }
    resolved_intake_root = _resolve_path(advisory.get("intake_root") or intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    resolutions_root = resolved_intake_root / "resolutions"
    resolutions_root.mkdir(parents=True, exist_ok=True)
    advisories = [row for row in list(advisory.get("advisories", [])) if isinstance(row, dict)]
    rows: list[dict[str, Any]] = []
    created_count = 0
    existing_count = 0
    for advisory_row in advisories:
        resolution_payload = _dedup_resolution_template(advisory_row)
        resolution_path = resolutions_root / f"{resolution_payload['group_id']}.resolution.json"
        if resolution_path.exists():
            existing_payload = _load_json(resolution_path)
            if isinstance(existing_payload, dict):
                resolution_payload = existing_payload
            existing_count += 1
        else:
            resolution_path.write_text(json.dumps(resolution_payload, indent=2), encoding="utf-8")
            created_count += 1
        rows.append(
            {
                "group_id": resolution_payload["group_id"],
                "status": resolution_payload.get("status"),
                "resolution_path": str(resolution_path.resolve()),
                "canonical_bundle_name": resolution_payload.get("canonical_bundle_name"),
                "bundle_names": resolution_payload.get("bundle_names", []),
            }
        )
    return {
        "ok": True,
        "status": "ok",
        "schema_version": "real_artifact_intake_dedup_resolution_materialization_v1",
        "manifest_path": source_manifest_path,
        "intake_root": str(resolved_intake_root.resolve()),
        "resolutions_root": str(resolutions_root.resolve()),
        "created_count": created_count,
        "existing_count": existing_count,
        "resolution_count": len(rows),
        "resolutions": rows,
    }


def summarize_real_artifact_intake_dedup_resolutions(
    advisory_manifest: str | Path | None = None,
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    advisory, source_manifest_path = _load_or_build_intake_dedup_advisory(
        advisory_manifest=advisory_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if advisory is None:
        return {
            "ok": False,
            "status": "invalid_real_artifact_intake_dedup_advisory",
            "error": "dedup advisory artifact is missing or malformed",
        }
    resolved_intake_root = _resolve_path(advisory.get("intake_root") or intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    resolutions_root = resolved_intake_root / "resolutions"
    advisories = [row for row in list(advisory.get("advisories", [])) if isinstance(row, dict)]
    status_counts: dict[str, int] = {}
    unresolved_rows: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for advisory_row in advisories:
        group_id = _dedup_group_id(advisory_row)
        resolution_path = resolutions_root / f"{group_id}.resolution.json"
        payload = _load_json(resolution_path) if resolution_path.exists() else _dedup_resolution_template(advisory_row)
        status = str(payload.get("status") or "pending")
        status_counts[status] = status_counts.get(status, 0) + 1
        row = {
            "group_id": group_id,
            "status": status,
            "resolution_path": str(resolution_path.resolve()),
            "canonical_bundle_name": payload.get("canonical_bundle_name"),
            "bundle_names": payload.get("bundle_names", []),
            "recommended_actions": payload.get("recommended_actions", []),
        }
        rows.append(row)
        if status == "pending":
            unresolved_rows.append(row)
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_DEDUP_RESOLUTION_SUMMARY_SCHEMA_VERSION,
        "manifest_path": source_manifest_path,
        "intake_root": str(resolved_intake_root.resolve()),
        "resolutions_root": str(resolutions_root.resolve()),
        "resolution_count": len(rows),
        "status_counts": status_counts,
        "unresolved_count": status_counts.get("pending", 0),
        "unresolved_groups": unresolved_rows,
        "resolution_rows": rows,
    }


def update_real_artifact_intake_dedup_resolution(
    advisory_manifest: str | Path | None = None,
    *,
    group_id: str,
    status: str,
    reviewed_by: str | None = None,
    notes: str | None = None,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    advisory, source_manifest_path = _load_or_build_intake_dedup_advisory(
        advisory_manifest=advisory_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if advisory is None:
        return {
            "ok": False,
            "status": "invalid_real_artifact_intake_dedup_advisory",
            "error": "dedup advisory artifact is missing or malformed",
        }
    normalized_group_id = str(group_id or "").strip()
    if not normalized_group_id:
        return {
            "ok": False,
            "status": "missing_group_id",
            "error": "group_id is required",
        }
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"accepted", "ignored"}:
        return {
            "ok": False,
            "status": "invalid_resolution_status",
            "error": "status must be one of: accepted, ignored",
        }
    advisories = [row for row in list(advisory.get("advisories", [])) if isinstance(row, dict)]
    advisory_row = next((row for row in advisories if _dedup_group_id(row) == normalized_group_id), None)
    if advisory_row is None:
        return {
            "ok": False,
            "status": "unknown_group_id",
            "error": f"no advisory group found for {normalized_group_id}",
            "group_id": normalized_group_id,
        }
    resolved_intake_root = _resolve_path(advisory.get("intake_root") or intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    resolutions_root = resolved_intake_root / "resolutions"
    resolutions_root.mkdir(parents=True, exist_ok=True)
    resolution_path = resolutions_root / f"{normalized_group_id}.resolution.json"
    payload = _load_json(resolution_path) if resolution_path.exists() else _dedup_resolution_template(advisory_row)
    if not isinstance(payload, dict):
        payload = _dedup_resolution_template(advisory_row)
    now = datetime.now(UTC).isoformat()
    payload["schema_version"] = REAL_ARTIFACT_INTAKE_DEDUP_RESOLUTION_SCHEMA_VERSION
    payload["group_id"] = normalized_group_id
    payload["status"] = normalized_status
    payload["reviewed_at"] = now
    payload["reviewed_by"] = str(reviewed_by or "").strip() or None
    payload["notes"] = str(notes or "").strip()
    resolution_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_DEDUP_RESOLUTION_UPDATE_SCHEMA_VERSION,
        "manifest_path": source_manifest_path,
        "intake_root": str(resolved_intake_root.resolve()),
        "resolution_path": str(resolution_path.resolve()),
        "group_id": normalized_group_id,
        "resolution_status": normalized_status,
        "reviewed_at": now,
        "reviewed_by": payload.get("reviewed_by"),
        "notes": payload.get("notes"),
    }


def preflight_real_artifact_intake_refresh(
    validation_manifest: str | Path | None = None,
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    require_resolved_dedup: bool = False,
) -> dict[str, Any]:
    payload, source_manifest_path = _load_or_build_intake_validation(
        validation_manifest=validation_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if payload is None:
        return {
            "ok": False,
            "status": "invalid_real_artifact_intake_validation",
            "error": "validation artifact is missing or malformed",
        }
    bundle_readiness_rollups = payload.get("bundle_readiness_rollups", {})
    coverage_inventory = payload.get("coverage_inventory", {})
    dedup_resolution_summary = payload.get("dedup_resolution_summary", {})
    benchmark_ready_bundle_count = int(bundle_readiness_rollups.get("benchmark_ready_bundle_count", 0) or 0)
    eligible_real_label_count = int(coverage_inventory.get("eligible_real_post_performance_label_count", 0) or 0)
    unresolved_dedup_group_count = int(dedup_resolution_summary.get("unresolved_count", 0) or 0)

    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    if benchmark_ready_bundle_count <= 0:
        blockers.append(
            {
                "status": "missing_benchmark_ready_bundles",
                "detail": "no benchmark-ready real artifact bundles are currently available for refresh",
                "benchmark_ready_bundle_count": benchmark_ready_bundle_count,
            }
        )
    if eligible_real_label_count <= 0:
        blockers.append(
            {
                "status": "zero_eligible_real_labels",
                "detail": "no eligible real post-performance labels are currently available for refresh",
                "eligible_real_post_performance_label_count": eligible_real_label_count,
            }
        )
    if unresolved_dedup_group_count > 0:
        dedup_item = {
            "status": "unresolved_dedup_groups",
            "detail": f"{unresolved_dedup_group_count} duplicate-lineage group(s) remain unresolved",
            "unresolved_dedup_group_count": unresolved_dedup_group_count,
            "unresolved_group_ids": dedup_resolution_summary.get("unresolved_group_ids", []),
        }
        if require_resolved_dedup:
            blockers.append(dedup_item)
        else:
            warnings.append(dedup_item)

    if blockers:
        preflight_status = "blocked"
    elif warnings:
        preflight_status = "warning"
    else:
        preflight_status = "ready"

    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_REFRESH_PREFLIGHT_SCHEMA_VERSION,
        "manifest_path": source_manifest_path,
        "intake_root": payload.get("intake_root"),
        "filters": payload.get("filters", {}),
        "preflight_status": preflight_status,
        "require_resolved_dedup": require_resolved_dedup,
        "blocking_issue_count": len(blockers),
        "warning_issue_count": len(warnings),
        "blockers": blockers,
        "warnings": warnings,
        "summary": {
            "benchmark_ready_bundle_count": benchmark_ready_bundle_count,
            "eligible_real_post_performance_label_count": eligible_real_label_count,
            "unresolved_dedup_group_count": unresolved_dedup_group_count,
            "intake_status": payload.get("intake_status"),
            "bundle_count": payload.get("bundle_count", 0),
        },
    }


def record_real_artifact_intake_preflight_history(
    validation_manifest: str | Path | None = None,
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    require_resolved_dedup: bool = False,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    preflight = preflight_real_artifact_intake_refresh(
        validation_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
        require_resolved_dedup=require_resolved_dedup,
    )
    if not preflight.get("ok"):
        return preflight
    resolved_intake_root = _resolve_path(preflight.get("intake_root") or intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    timestamp = datetime.now(UTC)
    game_slug = _safe_history_slug(game or preflight.get("filters", {}).get("game") or "all-games")
    platform_slug = _safe_history_slug(platform or preflight.get("filters", {}).get("platform") or "all-platforms")
    default_target = (
        resolved_intake_root
        / "reports"
        / "preflight_history"
        / f"{game_slug}.{platform_slug}.{timestamp.strftime('%Y%m%dT%H%M%SZ')}.real_artifact_intake_refresh_preflight.json"
    )
    target = _resolve_path(output_path) if output_path is not None else default_target
    target.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_REFRESH_PREFLIGHT_HISTORY_SCHEMA_VERSION,
        "recorded_at": timestamp.isoformat(),
        "intake_root": str(resolved_intake_root.resolve()),
        "history_filters": {
            "game": game or preflight.get("filters", {}).get("game"),
            "platform": platform or preflight.get("filters", {}).get("platform"),
        },
        "preflight": preflight,
    }
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    artifact["manifest_path"] = str(target.resolve())
    return artifact


def summarize_real_artifact_intake_preflight_history(
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    resolved_intake_root = _resolve_path(intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    history_root = resolved_intake_root / "reports" / "preflight_history"
    rows: list[dict[str, Any]] = []
    if history_root.exists():
        for path in sorted(history_root.glob("*.real_artifact_intake_refresh_preflight.json")):
            payload = _load_json(path)
            if not isinstance(payload, dict):
                continue
            if str(payload.get("schema_version") or "") != REAL_ARTIFACT_INTAKE_REFRESH_PREFLIGHT_HISTORY_SCHEMA_VERSION:
                continue
            preflight = payload.get("preflight", {})
            filters = payload.get("history_filters", {})
            row_game = str(filters.get("game") or preflight.get("filters", {}).get("game") or "").strip() or None
            row_platform = str(filters.get("platform") or preflight.get("filters", {}).get("platform") or "").strip() or None
            if game and row_game != game:
                continue
            if platform and row_platform != platform:
                continue
            row = {
                "manifest_path": str(path.resolve()),
                "recorded_at": payload.get("recorded_at"),
                "game": row_game,
                "platform": row_platform,
                "preflight_status": preflight.get("preflight_status"),
                "blocking_issue_count": preflight.get("blocking_issue_count", 0),
                "warning_issue_count": preflight.get("warning_issue_count", 0),
                "benchmark_ready_bundle_count": preflight.get("summary", {}).get("benchmark_ready_bundle_count", 0),
                "eligible_real_post_performance_label_count": preflight.get("summary", {}).get(
                    "eligible_real_post_performance_label_count", 0
                ),
                "unresolved_dedup_group_count": preflight.get("summary", {}).get("unresolved_dedup_group_count", 0),
            }
            rows.append(row)
    status_counts: dict[str, int] = {}
    for row in rows:
        status_name = str(row.get("preflight_status") or "unknown")
        status_counts[status_name] = status_counts.get(status_name, 0) + 1
    latest_row = max(rows, key=lambda row: str(row.get("recorded_at") or "")) if rows else None
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_REFRESH_PREFLIGHT_HISTORY_SUMMARY_SCHEMA_VERSION,
        "intake_root": str(resolved_intake_root.resolve()),
        "history_root": str(history_root.resolve()),
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        "entry_count": len(rows),
        "status_counts": status_counts,
        "latest_entry": latest_row,
        "entries": rows,
    }


def report_real_artifact_intake_preflight_trends(
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    summary = summarize_real_artifact_intake_preflight_history(
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not summary.get("ok"):
        return summary
    entries = sorted(
        [row for row in list(summary.get("entries", [])) if isinstance(row, dict)],
        key=lambda row: str(row.get("recorded_at") or ""),
    )
    earliest_entry = entries[0] if entries else None
    latest_entry = entries[-1] if entries else None
    status_transition_counts: dict[str, int] = {}
    blocker_trend_counts = {
        "blocking_issue_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "warning_issue_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "benchmark_ready_bundle_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "eligible_real_post_performance_label_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "unresolved_dedup_group_count": {"increased": 0, "decreased": 0, "unchanged": 0},
    }
    for previous, current in zip(entries, entries[1:]):
        transition_key = f"{previous.get('preflight_status') or 'unknown'}->{current.get('preflight_status') or 'unknown'}"
        status_transition_counts[transition_key] = status_transition_counts.get(transition_key, 0) + 1
        for field_name, counts in blocker_trend_counts.items():
            previous_value = int(previous.get(field_name, 0) or 0)
            current_value = int(current.get(field_name, 0) or 0)
            if current_value > previous_value:
                counts["increased"] += 1
            elif current_value < previous_value:
                counts["decreased"] += 1
            else:
                counts["unchanged"] += 1

    def _delta(field_name: str) -> int:
        if not earliest_entry or not latest_entry:
            return 0
        return int(latest_entry.get(field_name, 0) or 0) - int(earliest_entry.get(field_name, 0) or 0)

    status_rank = {"blocked": 0, "warning": 1, "ready": 2}
    trend_status = "no_history"
    if earliest_entry and latest_entry:
        earliest_rank = status_rank.get(str(earliest_entry.get("preflight_status") or "unknown"), -1)
        latest_rank = status_rank.get(str(latest_entry.get("preflight_status") or "unknown"), -1)
        if latest_rank > earliest_rank:
            trend_status = "improving"
        elif latest_rank < earliest_rank:
            trend_status = "regressing"
        elif _delta("benchmark_ready_bundle_count") > 0 or _delta("eligible_real_post_performance_label_count") > 0:
            trend_status = "improving"
        elif _delta("blocking_issue_count") < 0 or _delta("unresolved_dedup_group_count") < 0:
            trend_status = "improving"
        else:
            trend_status = "stable"

    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_REFRESH_PREFLIGHT_TREND_REPORT_SCHEMA_VERSION,
        "intake_root": summary.get("intake_root"),
        "history_root": summary.get("history_root"),
        "filters": summary.get("filters", {}),
        "entry_count": summary.get("entry_count", 0),
        "status_counts": summary.get("status_counts", {}),
        "status_transition_counts": status_transition_counts,
        "trend_status": trend_status,
        "earliest_entry": earliest_entry,
        "latest_entry": latest_entry,
        "delta_summary": {
            "blocking_issue_count_delta": _delta("blocking_issue_count"),
            "warning_issue_count_delta": _delta("warning_issue_count"),
            "benchmark_ready_bundle_count_delta": _delta("benchmark_ready_bundle_count"),
            "eligible_real_post_performance_label_count_delta": _delta("eligible_real_post_performance_label_count"),
            "unresolved_dedup_group_count_delta": _delta("unresolved_dedup_group_count"),
        },
        "field_direction_counts": blocker_trend_counts,
        "entries": entries,
    }


def record_real_artifact_intake_refresh_outcome_history(
    *,
    intake_root: str | Path | None = None,
    registry_path: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    require_resolved_dedup: bool = False,
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    refresh_result = refresh_real_artifact_intake(
        intake_root=intake_root,
        registry_path=registry_path,
        game=game,
        platform=platform,
        require_resolved_dedup=require_resolved_dedup,
        output_root=output_root,
    )
    if not refresh_result.get("ok"):
        return refresh_result
    return _write_real_artifact_intake_refresh_outcome_history(
        refresh_result=refresh_result,
        intake_root=intake_root,
        game=game,
        platform=platform,
        output_path=output_path,
    )


def _write_real_artifact_intake_refresh_outcome_history(
    *,
    refresh_result: dict[str, Any],
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_intake_root = _resolve_path(refresh_result.get("intake_root") or intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    timestamp = datetime.now(UTC)
    game_slug = _safe_history_slug(game or "all-games")
    platform_slug = _safe_history_slug(platform or "all-platforms")
    default_target = (
        resolved_intake_root
        / "reports"
        / "refresh_outcome_history"
        / f"{game_slug}.{platform_slug}.{timestamp.strftime('%Y%m%dT%H%M%SZ')}.real_artifact_intake_refresh_outcome.json"
    )
    target = _resolve_path(output_path) if output_path is not None else default_target
    target.parent.mkdir(parents=True, exist_ok=True)
    target_reviews = _load_refresh_target_reviews(refresh_result.get("review_manifest_path"))
    artifact = {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_REFRESH_OUTCOME_HISTORY_SCHEMA_VERSION,
        "recorded_at": timestamp.isoformat(),
        "intake_root": str(resolved_intake_root.resolve()),
        "history_filters": {"game": game, "platform": platform},
        "refresh": refresh_result,
        "target_reviews": target_reviews,
    }
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    artifact["manifest_path"] = str(target.resolve())
    return artifact


def summarize_real_artifact_intake_refresh_outcome_history(
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    resolved_intake_root = _resolve_path(intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    history_root = resolved_intake_root / "reports" / "refresh_outcome_history"
    rows: list[dict[str, Any]] = []
    if history_root.exists():
        for path in sorted(history_root.glob("*.real_artifact_intake_refresh_outcome.json")):
            payload = _load_json(path)
            if not isinstance(payload, dict):
                continue
            if str(payload.get("schema_version") or "") != REAL_ARTIFACT_INTAKE_REFRESH_OUTCOME_HISTORY_SCHEMA_VERSION:
                continue
            refresh = payload.get("refresh", {})
            filters = payload.get("history_filters", {})
            row_game = str(filters.get("game") or "").strip() or None
            row_platform = str(filters.get("platform") or "").strip() or None
            if game and row_game != game:
                continue
            if platform and row_platform != platform:
                continue
            target_reviews = [row for row in list(payload.get("target_reviews", [])) if isinstance(row, dict)]
            readiness_counts: dict[str, int] = {}
            for target_row in target_reviews:
                readiness = str(target_row.get("readiness_classification") or "unknown")
                readiness_counts[readiness] = readiness_counts.get(readiness, 0) + 1
            row = {
                "manifest_path": str(path.resolve()),
                "recorded_at": payload.get("recorded_at"),
                "game": row_game,
                "platform": row_platform,
                "benchmark_recommendation": refresh.get("benchmark_recommendation"),
                "target_review_count": len(target_reviews),
                "readiness_counts": readiness_counts,
                "candidate_count": int(refresh.get("dataset_coverage_counts", {}).get("candidate_count", 0) or 0),
                "outcome_count": int(refresh.get("dataset_coverage_counts", {}).get("outcome_count", 0) or 0),
                "performance_count": int(refresh.get("dataset_coverage_counts", {}).get("performance_count", 0) or 0),
                "eligible_real_post_performance_label_count": int(
                    refresh.get("validation_summary", {}).get("eligible_real_post_performance_label_count", 0) or 0
                ),
                "unresolved_dedup_group_count": int(
                    refresh.get("validation_summary", {}).get("unresolved_dedup_group_count", 0) or 0
                ),
                "target_reviews": target_reviews,
            }
            rows.append(row)
    recommendation_counts: dict[str, int] = {}
    for row in rows:
        recommendation = str(row.get("benchmark_recommendation") or "unknown")
        recommendation_counts[recommendation] = recommendation_counts.get(recommendation, 0) + 1
    latest_row = max(rows, key=lambda row: str(row.get("recorded_at") or "")) if rows else None
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_REFRESH_OUTCOME_HISTORY_SUMMARY_SCHEMA_VERSION,
        "intake_root": str(resolved_intake_root.resolve()),
        "history_root": str(history_root.resolve()),
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        "entry_count": len(rows),
        "benchmark_recommendation_counts": recommendation_counts,
        "latest_entry": latest_row,
        "entries": rows,
    }


def report_real_artifact_intake_refresh_outcome_trends(
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    summary = summarize_real_artifact_intake_refresh_outcome_history(
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not summary.get("ok"):
        return summary
    entries = sorted(
        [row for row in list(summary.get("entries", [])) if isinstance(row, dict)],
        key=lambda row: str(row.get("recorded_at") or ""),
    )
    earliest_entry = entries[0] if entries else None
    latest_entry = entries[-1] if entries else None
    recommendation_transition_counts: dict[str, int] = {}
    direction_counts = {
        "candidate_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "outcome_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "performance_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "eligible_real_post_performance_label_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "unresolved_dedup_group_count": {"increased": 0, "decreased": 0, "unchanged": 0},
    }
    for previous, current in zip(entries, entries[1:]):
        transition_key = f"{previous.get('benchmark_recommendation') or 'unknown'}->{current.get('benchmark_recommendation') or 'unknown'}"
        recommendation_transition_counts[transition_key] = recommendation_transition_counts.get(transition_key, 0) + 1
        for field_name, counts in direction_counts.items():
            previous_value = int(previous.get(field_name, 0) or 0)
            current_value = int(current.get(field_name, 0) or 0)
            if current_value > previous_value:
                counts["increased"] += 1
            elif current_value < previous_value:
                counts["decreased"] += 1
            else:
                counts["unchanged"] += 1

    def _delta(field_name: str) -> int:
        if not earliest_entry or not latest_entry:
            return 0
        return int(latest_entry.get(field_name, 0) or 0) - int(earliest_entry.get(field_name, 0) or 0)

    trend_status = "no_history"
    if earliest_entry and latest_entry:
        if _delta("performance_count") > 0 or _delta("eligible_real_post_performance_label_count") > 0:
            trend_status = "improving"
        elif _delta("unresolved_dedup_group_count") < 0:
            trend_status = "improving"
        elif _delta("candidate_count") < 0 or _delta("performance_count") < 0:
            trend_status = "regressing"
        else:
            trend_status = "stable"
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_REFRESH_OUTCOME_TREND_REPORT_SCHEMA_VERSION,
        "intake_root": summary.get("intake_root"),
        "history_root": summary.get("history_root"),
        "filters": summary.get("filters", {}),
        "entry_count": summary.get("entry_count", 0),
        "benchmark_recommendation_counts": summary.get("benchmark_recommendation_counts", {}),
        "recommendation_transition_counts": recommendation_transition_counts,
        "trend_status": trend_status,
        "earliest_entry": earliest_entry,
        "latest_entry": latest_entry,
        "delta_summary": {
            "candidate_count_delta": _delta("candidate_count"),
            "outcome_count_delta": _delta("outcome_count"),
            "performance_count_delta": _delta("performance_count"),
            "eligible_real_post_performance_label_count_delta": _delta("eligible_real_post_performance_label_count"),
            "unresolved_dedup_group_count_delta": _delta("unresolved_dedup_group_count"),
        },
        "field_direction_counts": direction_counts,
        "entries": entries,
    }


def report_real_artifact_intake_history_comparison(
    comparison_manifest: str | Path | None = None,
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    preflight_trends = report_real_artifact_intake_preflight_trends(
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not preflight_trends.get("ok"):
        return preflight_trends
    refresh_outcome_trends = report_real_artifact_intake_refresh_outcome_trends(
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not refresh_outcome_trends.get("ok"):
        return refresh_outcome_trends

    comparison_payload = _load_history_comparison_evidence(
        comparison_manifest=comparison_manifest,
        game=game,
        platform=platform,
    )
    if comparison_payload is not None and not comparison_payload.get("ok"):
        return comparison_payload
    comparison_rows = comparison_payload.get("rows", []) if isinstance(comparison_payload, dict) else []
    comparison_summary = comparison_payload.get("summary", {}) if isinstance(comparison_payload, dict) else {}

    latest_real_target_reviews = {
        str(row.get("training_target") or ""): row
        for row in list(refresh_outcome_trends.get("latest_entry", {}).get("target_reviews", []))
        if isinstance(row, dict) and str(row.get("training_target") or "")
    }
    comparison_index = {
        str(row.get("training_target") or ""): row
        for row in comparison_rows
        if isinstance(row, dict) and str(row.get("training_target") or "")
    }
    targets = sorted({*latest_real_target_reviews.keys(), *comparison_index.keys()})
    target_rows: list[dict[str, Any]] = []
    for target in targets:
        real_row = latest_real_target_reviews.get(target, {})
        comparison_row = comparison_index.get(target, {})
        target_rows.append(
            {
                "training_target": target,
                "latest_real_readiness_classification": real_row.get("readiness_classification"),
                "latest_real_best_recommendation_decision": real_row.get("best_recommendation_decision"),
                "latest_real_current_best_family": real_row.get("current_best_family"),
                "latest_real_primary_metric_delta": real_row.get("primary_metric_delta"),
                "synthetic_readiness_classification": comparison_row.get("synthetic_readiness_classification"),
                "synthetic_best_recommendation_decision": comparison_row.get("synthetic_best_recommendation_decision"),
                "synthetic_current_best_family": comparison_row.get("synthetic_current_best_family"),
                "synthetic_primary_metric_delta": comparison_row.get("synthetic_primary_metric_delta"),
                "disagreement_indicators": comparison_row.get("disagreement_indicators", []),
                "readiness_changed": comparison_row.get("readiness_changed"),
                "recommendation_changed": comparison_row.get("recommendation_changed"),
                "family_winner_changed": comparison_row.get("family_winner_changed"),
            }
        )

    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_HISTORY_COMPARISON_REPORT_SCHEMA_VERSION,
        "intake_root": preflight_trends.get("intake_root") or refresh_outcome_trends.get("intake_root"),
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        "comparison_manifest_path": comparison_payload.get("manifest_path") if isinstance(comparison_payload, dict) else None,
        "preflight_trends": {
            "entry_count": preflight_trends.get("entry_count", 0),
            "trend_status": preflight_trends.get("trend_status"),
            "latest_entry": preflight_trends.get("latest_entry"),
            "delta_summary": preflight_trends.get("delta_summary", {}),
        },
        "refresh_outcome_trends": {
            "entry_count": refresh_outcome_trends.get("entry_count", 0),
            "trend_status": refresh_outcome_trends.get("trend_status"),
            "latest_entry": refresh_outcome_trends.get("latest_entry"),
            "delta_summary": refresh_outcome_trends.get("delta_summary", {}),
            "benchmark_recommendation_counts": refresh_outcome_trends.get("benchmark_recommendation_counts", {}),
        },
        "evidence_comparison_summary": comparison_summary,
        "history_alignment": {
            "preflight_to_refresh_status": _history_alignment_status(preflight_trends, refresh_outcome_trends),
            "real_vs_synthetic_status": _history_comparison_gap_status(comparison_summary),
            "next_focus": _history_comparison_next_focus(preflight_trends, refresh_outcome_trends, comparison_summary),
        },
        "target_rows": target_rows,
    }


def render_real_artifact_intake_dashboard(
    comparison_manifest: str | Path | None = None,
    *,
    validation_manifest: str | Path | None = None,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    intake_summary = summarize_real_artifact_intake(
        validation_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not intake_summary.get("ok"):
        return intake_summary
    coverage_report = report_real_artifact_intake_coverage(
        validation_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not coverage_report.get("ok"):
        return coverage_report
    preflight_trends = report_real_artifact_intake_preflight_trends(
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not preflight_trends.get("ok"):
        return preflight_trends
    refresh_outcome_trends = report_real_artifact_intake_refresh_outcome_trends(
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not refresh_outcome_trends.get("ok"):
        return refresh_outcome_trends
    history_comparison = report_real_artifact_intake_history_comparison(
        comparison_manifest,
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not history_comparison.get("ok"):
        return history_comparison

    resolved_intake_root = _resolve_path(intake_summary.get("intake_root") or intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    headline_status = _real_artifact_intake_dashboard_status(
        intake_summary=intake_summary,
        coverage_report=coverage_report,
        preflight_trends=preflight_trends,
        refresh_outcome_trends=refresh_outcome_trends,
        history_comparison=history_comparison,
    )
    dashboard = {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_DASHBOARD_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "intake_root": str(resolved_intake_root.resolve()),
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        "headline_status": headline_status,
        "current_intake": {
            "intake_status": intake_summary.get("intake_status"),
            "bundle_count": intake_summary.get("bundle_count", 0),
            "warning_count": intake_summary.get("warning_count", 0),
            "bundle_readiness_rollups": intake_summary.get("bundle_readiness_rollups", {}),
            "coverage_inventory": intake_summary.get("coverage_inventory", {}),
            "dedup_resolution_summary": intake_summary.get("dedup_resolution_summary", {}),
        },
        "coverage": {
            "sufficiency_assessment": coverage_report.get("sufficiency_assessment", {}),
            "duplicate_downstream_summary": coverage_report.get("duplicate_downstream_summary", {}),
        },
        "preflight_trends": {
            "trend_status": preflight_trends.get("trend_status"),
            "entry_count": preflight_trends.get("entry_count", 0),
            "latest_entry": preflight_trends.get("latest_entry"),
            "delta_summary": preflight_trends.get("delta_summary", {}),
        },
        "refresh_outcome_trends": {
            "trend_status": refresh_outcome_trends.get("trend_status"),
            "entry_count": refresh_outcome_trends.get("entry_count", 0),
            "latest_entry": refresh_outcome_trends.get("latest_entry"),
            "delta_summary": refresh_outcome_trends.get("delta_summary", {}),
            "benchmark_recommendation_counts": refresh_outcome_trends.get("benchmark_recommendation_counts", {}),
        },
        "history_comparison": {
            "comparison_manifest_path": history_comparison.get("comparison_manifest_path"),
            "history_alignment": history_comparison.get("history_alignment", {}),
            "evidence_comparison_summary": history_comparison.get("evidence_comparison_summary", {}),
            "target_rows": history_comparison.get("target_rows", []),
        },
    }
    target = _resolve_path(output_path) if output_path is not None else resolved_intake_root / "reports" / "real_artifact_intake.dashboard.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(dashboard, indent=2), encoding="utf-8")
    dashboard["manifest_path"] = str(target.resolve())
    return dashboard


def summarize_real_artifact_intake_dashboard_registry(
    *,
    registry_path: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    result = query_clip_registry(
        mode="real-artifact-intake-dashboards",
        registry_path=registry_path,
        game=game,
        platform=platform,
    )
    if not result.get("ok"):
        return result
    rows = list(result.get("rows", []))
    latest_dashboard = rows[0] if rows else None
    headline_status_counts: dict[str, int] = {}
    for row in rows:
        headline_status = str(row.get("headline_status") or "").strip() or "unknown"
        headline_status_counts[headline_status] = headline_status_counts.get(headline_status, 0) + 1

    latest_by_scope: list[dict[str, Any]] = []
    seen_scopes: set[tuple[str | None, str | None]] = set()
    for row in rows:
        scope = (
            str(row.get("game") or "").strip() or None,
            str(row.get("platform") or "").strip() or None,
        )
        if scope in seen_scopes:
            continue
        seen_scopes.add(scope)
        latest_by_scope.append(
            {
                "game": scope[0],
                "platform": scope[1],
                "manifest_path": row.get("manifest_path"),
                "generated_at": row.get("generated_at"),
                "headline_status": row.get("headline_status"),
                "benchmark_ready_bundle_count": row.get("benchmark_ready_bundle_count"),
                "eligible_real_post_performance_label_count": row.get("eligible_real_post_performance_label_count"),
                "next_focus": row.get("next_focus"),
            }
        )

    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_DASHBOARD_REGISTRY_SUMMARY_SCHEMA_VERSION,
        "registry_path": result.get("registry_path"),
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        "row_count": len(rows),
        "scope_count": len(latest_by_scope),
        "headline_status_counts": headline_status_counts,
        "latest_dashboard": latest_dashboard,
        "latest_by_scope": latest_by_scope,
    }


def summarize_real_artifact_intake_comparison_targets(
    *,
    registry_path: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    manifest_sort_cache: dict[str, tuple[str, float, str]] = {}

    def _comparison_sort_key(row: dict[str, Any]) -> tuple[str, float, str]:
        manifest_path = str(row.get("manifest_path") or "")
        cached = manifest_sort_cache.get(manifest_path)
        if cached is not None:
            return cached
        created_at = ""
        mtime = 0.0
        if manifest_path:
            try:
                payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
                created_at = str(payload.get("created_at") or "").strip()
            except (OSError, json.JSONDecodeError):
                created_at = ""
            try:
                mtime = Path(manifest_path).stat().st_mtime
            except OSError:
                mtime = 0.0
        cached = (created_at, mtime, manifest_path)
        manifest_sort_cache[manifest_path] = cached
        return cached

    result = query_clip_registry(
        mode="shadow-benchmark-evidence-comparisons",
        registry_path=registry_path,
        game=game,
        platform=platform,
    )
    if not result.get("ok"):
        return result
    rows = list(result.get("rows", []))
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        training_target = str(row.get("training_target") or "").strip()
        if not training_target:
            continue
        grouped.setdefault(training_target, []).append(row)

    target_summaries: list[dict[str, Any]] = []
    aggregate_counts = {
        "readiness_changed_count": 0,
        "recommendation_changed_count": 0,
        "family_winner_changed_count": 0,
        "ready_only_under_synthetic_count": 0,
        "ready_only_under_real_count": 0,
    }
    for training_target in sorted(grouped):
        target_rows = grouped[training_target]
        latest_row = max(target_rows, key=_comparison_sort_key)
        readiness_changed_count = 0
        recommendation_changed_count = 0
        family_winner_changed_count = 0
        ready_only_under_synthetic_count = 0
        ready_only_under_real_count = 0
        disagreement_indicator_counts: dict[str, int] = {}
        for row in target_rows:
            if bool(row.get("readiness_changed")):
                readiness_changed_count += 1
            if bool(row.get("recommendation_changed")):
                recommendation_changed_count += 1
            if bool(row.get("family_winner_changed")):
                family_winner_changed_count += 1
            disagreement_raw = row.get("disagreement_indicators_json")
            disagreement_indicators: list[str] = []
            if isinstance(disagreement_raw, str) and disagreement_raw.strip():
                try:
                    decoded = json.loads(disagreement_raw)
                    if isinstance(decoded, list):
                        disagreement_indicators = [str(item) for item in decoded if str(item).strip()]
                except json.JSONDecodeError:
                    disagreement_indicators = []
            for indicator in disagreement_indicators:
                disagreement_indicator_counts[indicator] = disagreement_indicator_counts.get(indicator, 0) + 1
            if "ready_only_under_synthetic" in disagreement_indicators:
                ready_only_under_synthetic_count += 1
            if "ready_only_under_real" in disagreement_indicators:
                ready_only_under_real_count += 1

        aggregate_counts["readiness_changed_count"] += readiness_changed_count
        aggregate_counts["recommendation_changed_count"] += recommendation_changed_count
        aggregate_counts["family_winner_changed_count"] += family_winner_changed_count
        aggregate_counts["ready_only_under_synthetic_count"] += ready_only_under_synthetic_count
        aggregate_counts["ready_only_under_real_count"] += ready_only_under_real_count
        target_summaries.append(
            {
                "training_target": training_target,
                "comparison_row_count": len(target_rows),
                "latest_manifest_path": latest_row.get("manifest_path"),
                "latest_real_manifest_path": latest_row.get("real_manifest_path"),
                "latest_synthetic_manifest_path": latest_row.get("synthetic_manifest_path"),
                "latest_real_current_best_family": latest_row.get("real_current_best_family"),
                "latest_synthetic_current_best_family": latest_row.get("synthetic_current_best_family"),
                "latest_real_best_recommendation_decision": latest_row.get("real_best_recommendation_decision"),
                "latest_synthetic_best_recommendation_decision": latest_row.get("synthetic_best_recommendation_decision"),
                "latest_real_readiness_classification": latest_row.get("real_readiness_classification"),
                "latest_synthetic_readiness_classification": latest_row.get("synthetic_readiness_classification"),
                "latest_real_primary_metric_name": latest_row.get("real_primary_metric_name"),
                "latest_synthetic_primary_metric_name": latest_row.get("synthetic_primary_metric_name"),
                "latest_real_primary_metric_delta": latest_row.get("real_primary_metric_delta"),
                "latest_synthetic_primary_metric_delta": latest_row.get("synthetic_primary_metric_delta"),
                "latest_primary_metric_delta_gap": latest_row.get("primary_metric_delta_gap"),
                "latest_real_confidence_level": latest_row.get("real_confidence_level"),
                "latest_synthetic_confidence_level": latest_row.get("synthetic_confidence_level"),
                "readiness_changed_count": readiness_changed_count,
                "recommendation_changed_count": recommendation_changed_count,
                "family_winner_changed_count": family_winner_changed_count,
                "ready_only_under_synthetic_count": ready_only_under_synthetic_count,
                "ready_only_under_real_count": ready_only_under_real_count,
                "disagreement_indicator_counts": disagreement_indicator_counts,
                "game": latest_row.get("game"),
                "platform": latest_row.get("platform"),
            }
        )

    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_COMPARISON_TARGET_SUMMARY_SCHEMA_VERSION,
        "registry_path": result.get("registry_path"),
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        "row_count": len(rows),
        "target_count": len(target_summaries),
        "aggregate_counts": aggregate_counts,
        "targets": target_summaries,
    }


def record_real_artifact_intake_dashboard_summary_history(
    *,
    registry_path: str | Path | None = None,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    summary = summarize_real_artifact_intake_dashboard_registry(
        registry_path=registry_path,
        game=game,
        platform=platform,
    )
    if not summary.get("ok"):
        return summary
    resolved_intake_root = _resolve_path(intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    timestamp = datetime.now(UTC)
    game_slug = _safe_history_slug(game or "all-games")
    platform_slug = _safe_history_slug(platform or "all-platforms")
    default_target = (
        resolved_intake_root
        / "reports"
        / "dashboard_summary_history"
        / f"{game_slug}.{platform_slug}.{timestamp.strftime('%Y%m%dT%H%M%SZ')}.real_artifact_intake_dashboard_registry_summary.json"
    )
    target = _resolve_path(output_path) if output_path is not None else default_target
    target.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_DASHBOARD_SUMMARY_HISTORY_SCHEMA_VERSION,
        "recorded_at": timestamp.isoformat(),
        "intake_root": str(resolved_intake_root.resolve()),
        "history_filters": {"game": game, "platform": platform},
        "dashboard_summary": summary,
    }
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    artifact["manifest_path"] = str(target.resolve())
    return artifact


def summarize_real_artifact_intake_dashboard_summary_history(
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    resolved_intake_root = _resolve_path(intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    history_root = resolved_intake_root / "reports" / "dashboard_summary_history"
    rows: list[dict[str, Any]] = []
    if history_root.exists():
        for path in sorted(history_root.glob("*.real_artifact_intake_dashboard_registry_summary.json")):
            payload = _load_json(path)
            if not isinstance(payload, dict):
                continue
            if str(payload.get("schema_version") or "") != REAL_ARTIFACT_INTAKE_DASHBOARD_SUMMARY_HISTORY_SCHEMA_VERSION:
                continue
            summary = payload.get("dashboard_summary", {})
            filters = payload.get("history_filters", {})
            row_game = str(filters.get("game") or summary.get("filters", {}).get("game") or "").strip() or None
            row_platform = str(filters.get("platform") or summary.get("filters", {}).get("platform") or "").strip() or None
            if game and row_game != game:
                continue
            if platform and row_platform != platform:
                continue
            latest_dashboard = summary.get("latest_dashboard") if isinstance(summary.get("latest_dashboard"), dict) else {}
            rows.append(
                {
                    "manifest_path": str(path.resolve()),
                    "recorded_at": payload.get("recorded_at"),
                    "game": row_game,
                    "platform": row_platform,
                    "row_count": int(summary.get("row_count", 0) or 0),
                    "scope_count": int(summary.get("scope_count", 0) or 0),
                    "headline_status_counts": dict(summary.get("headline_status_counts", {})),
                    "latest_headline_status": latest_dashboard.get("headline_status"),
                    "latest_benchmark_ready_bundle_count": latest_dashboard.get("benchmark_ready_bundle_count"),
                    "latest_eligible_real_post_performance_label_count": latest_dashboard.get("eligible_real_post_performance_label_count"),
                    "latest_next_focus": latest_dashboard.get("next_focus"),
                    "latest_dashboard": latest_dashboard,
                }
            )
    latest_row = max(rows, key=lambda row: str(row.get("recorded_at") or "")) if rows else None
    headline_status_counts: dict[str, int] = {}
    for row in rows:
        headline = str(row.get("latest_headline_status") or "unknown")
        headline_status_counts[headline] = headline_status_counts.get(headline, 0) + 1
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_DASHBOARD_SUMMARY_HISTORY_SUMMARY_SCHEMA_VERSION,
        "intake_root": str(resolved_intake_root.resolve()),
        "history_root": str(history_root.resolve()),
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        "entry_count": len(rows),
        "latest_headline_status_counts": headline_status_counts,
        "latest_entry": latest_row,
        "entries": rows,
    }


def report_real_artifact_intake_dashboard_summary_trends(
    *,
    intake_root: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    summary = summarize_real_artifact_intake_dashboard_summary_history(
        intake_root=intake_root,
        game=game,
        platform=platform,
    )
    if not summary.get("ok"):
        return summary
    entries = sorted(
        [row for row in list(summary.get("entries", [])) if isinstance(row, dict)],
        key=lambda row: str(row.get("recorded_at") or ""),
    )
    earliest_entry = entries[0] if entries else None
    latest_entry = entries[-1] if entries else None
    headline_transition_counts: dict[str, int] = {}
    direction_counts = {
        "row_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "scope_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "latest_benchmark_ready_bundle_count": {"increased": 0, "decreased": 0, "unchanged": 0},
        "latest_eligible_real_post_performance_label_count": {"increased": 0, "decreased": 0, "unchanged": 0},
    }
    for previous, current in zip(entries, entries[1:]):
        transition_key = f"{previous.get('latest_headline_status') or 'unknown'}->{current.get('latest_headline_status') or 'unknown'}"
        headline_transition_counts[transition_key] = headline_transition_counts.get(transition_key, 0) + 1
        for field_name, counts in direction_counts.items():
            previous_value = int(previous.get(field_name, 0) or 0)
            current_value = int(current.get(field_name, 0) or 0)
            if current_value > previous_value:
                counts["increased"] += 1
            elif current_value < previous_value:
                counts["decreased"] += 1
            else:
                counts["unchanged"] += 1

    def _delta(field_name: str) -> int:
        if not earliest_entry or not latest_entry:
            return 0
        return int(latest_entry.get(field_name, 0) or 0) - int(earliest_entry.get(field_name, 0) or 0)

    def _headline_rank(value: Any) -> int:
        text = str(value or "").strip().lower()
        if text in {"ready", "improving_end_to_end"}:
            return 3
        if text.startswith("improving"):
            return 2
        if "warning" in text:
            return 1
        if "lag" in text or "blocked" in text:
            return 0
        return 0

    trend_status = "no_history"
    if earliest_entry and latest_entry:
        earliest_rank = _headline_rank(earliest_entry.get("latest_headline_status"))
        latest_rank = _headline_rank(latest_entry.get("latest_headline_status"))
        if latest_rank > earliest_rank:
            trend_status = "improving"
        elif latest_rank < earliest_rank:
            trend_status = "regressing"
        elif _delta("latest_benchmark_ready_bundle_count") > 0 or _delta("latest_eligible_real_post_performance_label_count") > 0:
            trend_status = "improving"
        else:
            trend_status = "stable"

    return {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_DASHBOARD_SUMMARY_TREND_REPORT_SCHEMA_VERSION,
        "intake_root": summary.get("intake_root"),
        "history_root": summary.get("history_root"),
        "filters": summary.get("filters", {}),
        "entry_count": summary.get("entry_count", 0),
        "latest_headline_status_counts": summary.get("latest_headline_status_counts", {}),
        "headline_transition_counts": headline_transition_counts,
        "trend_status": trend_status,
        "earliest_entry": earliest_entry,
        "latest_entry": latest_entry,
        "delta_summary": {
            "row_count_delta": _delta("row_count"),
            "scope_count_delta": _delta("scope_count"),
            "latest_benchmark_ready_bundle_count_delta": _delta("latest_benchmark_ready_bundle_count"),
            "latest_eligible_real_post_performance_label_count_delta": _delta("latest_eligible_real_post_performance_label_count"),
        },
        "field_direction_counts": direction_counts,
        "entries": entries,
    }


def refresh_real_artifact_intake(
    *,
    intake_root: str | Path | None = None,
    registry_path: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    require_resolved_dedup: bool = False,
    record_dashboard_summary_history: bool = False,
    record_refresh_outcome_history: bool = False,
    render_dashboard: bool = False,
    refresh_artifact_registry: bool = False,
    comparison_manifest: str | Path | None = None,
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_intake_root = _resolve_path(intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    resolved_output_root = _resolve_path(output_root) if output_root is not None else resolved_intake_root / "refresh"
    resolved_output_root.mkdir(parents=True, exist_ok=True)
    resolved_registry_path = _resolve_path(registry_path) if registry_path is not None else resolved_output_root / "registry.sqlite"

    validation_result = validate_real_artifact_intake(
        intake_root=resolved_intake_root,
        game=game,
        platform=platform,
        output_path=resolved_output_root / "reports" / "real_artifact_intake.validation.json",
    )
    if not validation_result.get("ok"):
        return validation_result

    bundle_roots = [Path(path) for path in list(validation_result.get("bundle_roots", []))]
    if not bundle_roots:
        return {
            "ok": False,
            "status": "empty_intake_root",
            "intake_root": str(resolved_intake_root.resolve()),
            "validation_manifest_path": validation_result.get("manifest_path"),
            "error": "no real artifact bundles were found under the intake root",
        }

    dedup_resolution_summary = validation_result.get("dedup_resolution_summary", {})
    unresolved_dedup_group_count = int(dedup_resolution_summary.get("unresolved_count") or 0)
    dedup_gating_warning = None
    if unresolved_dedup_group_count > 0:
        dedup_gating_warning = {
            "status": "unresolved_dedup_groups",
            "detail": (
                f"{unresolved_dedup_group_count} duplicate-lineage group(s) remain unresolved; "
                "real-only refresh may include unreviewed duplicate downstream evidence"
            ),
            "unresolved_count": unresolved_dedup_group_count,
            "unresolved_group_ids": dedup_resolution_summary.get("unresolved_group_ids", []),
        }
        if require_resolved_dedup:
            return {
                "ok": False,
                "status": "blocked_by_unresolved_dedup_groups",
                "intake_root": str(resolved_intake_root.resolve()),
                "validation_manifest_path": validation_result.get("manifest_path"),
                "validation_summary": {
                    "bundle_count": validation_result.get("bundle_count", 0),
                    "intake_status": validation_result.get("intake_status"),
                    "eligible_real_post_performance_label_count": validation_result.get("coverage_inventory", {}).get(
                        "eligible_real_post_performance_label_count"
                    ),
                    "unresolved_dedup_group_count": unresolved_dedup_group_count,
                    "warning_count": validation_result.get("warning_count", 0),
                },
                "dedup_resolution_summary": dedup_resolution_summary,
                "warning": dedup_gating_warning,
                "error": "refresh blocked because unresolved dedup advisory groups remain",
            }

    refresh_result = refresh_real_only_benchmark(
        source_roots=bundle_roots,
        registry_path=resolved_registry_path,
        game=game,
        platform=platform,
        import_output_path=resolved_output_root / "imports" / "real_only.real_posted_lineage_import.json",
        output_root=resolved_output_root,
    )
    if not refresh_result.get("ok"):
        return {
            "ok": False,
            "status": "real_artifact_intake_refresh_failed",
            "intake_root": str(resolved_intake_root.resolve()),
            "validation_manifest_path": validation_result.get("manifest_path"),
            "refresh_result": refresh_result,
        }

    result = {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_ARTIFACT_INTAKE_REFRESH_SCHEMA_VERSION,
        "refreshed_at": datetime.now(UTC).isoformat(),
        "intake_root": str(resolved_intake_root.resolve()),
        "registry_path": str(resolved_registry_path.resolve()),
        "validation_manifest_path": validation_result.get("manifest_path"),
        "validation_summary": {
            "bundle_count": validation_result.get("bundle_count", 0),
            "intake_status": validation_result.get("intake_status"),
            "eligible_real_post_performance_label_count": validation_result.get("coverage_inventory", {}).get(
                "eligible_real_post_performance_label_count"
            ),
            "unresolved_dedup_group_count": validation_result.get("dedup_resolution_summary", {}).get("unresolved_count", 0),
            "warning_count": validation_result.get("warning_count", 0),
        },
        "dedup_resolution_summary": dedup_resolution_summary,
        "warnings": [dedup_gating_warning] if dedup_gating_warning is not None else [],
        "import_manifest_path": refresh_result.get("import_manifest_path"),
        "dataset_manifest_path": refresh_result.get("dataset_manifest_path"),
        "benchmark_manifest_path": refresh_result.get("benchmark_manifest_path"),
        "review_manifest_path": refresh_result.get("review_manifest_path"),
        "dataset_coverage_counts": refresh_result.get("dataset_coverage_counts", {}),
        "benchmark_recommendation": refresh_result.get("benchmark_recommendation"),
        "review_aggregate_conclusions": refresh_result.get("review_aggregate_conclusions", {}),
    }
    dashboard_comparison_manifest = comparison_manifest
    if comparison_manifest is not None:
        comparison_result = compare_shadow_benchmark_evidence_modes(
            result["review_manifest_path"],
            comparison_manifest,
            game=game,
            platform=platform,
            output_path=resolved_intake_root / "reports" / "real_vs_synthetic.shadow_benchmark_evidence_comparison.json",
        )
        if comparison_result.get("ok"):
            result["evidence_comparison_created"] = True
            result["evidence_comparison_manifest_path"] = comparison_result.get("manifest_path")
            dashboard_comparison_manifest = comparison_result.get("manifest_path")
        else:
            result["evidence_comparison_created"] = False
            result["evidence_comparison_error"] = comparison_result.get("error") or comparison_result.get("status")
            result.setdefault("warnings", []).append(
                {
                    "status": "evidence_comparison_failed",
                    "detail": result["evidence_comparison_error"],
                }
            )
    if record_dashboard_summary_history:
        history_result = record_real_artifact_intake_dashboard_summary_history(
            registry_path=resolved_registry_path,
            intake_root=resolved_intake_root,
            game=game,
            platform=platform,
        )
        if history_result.get("ok"):
            result["dashboard_summary_history_recorded"] = True
            result["dashboard_summary_history_manifest_path"] = history_result.get("manifest_path")
        else:
            result["dashboard_summary_history_recorded"] = False
            result["dashboard_summary_history_error"] = history_result.get("error") or history_result.get("status")
            result.setdefault("warnings", []).append(
                {
                    "status": "dashboard_summary_history_record_failed",
                    "detail": result["dashboard_summary_history_error"],
                }
            )
    if record_refresh_outcome_history:
        history_result = _write_real_artifact_intake_refresh_outcome_history(
            refresh_result=result,
            intake_root=resolved_intake_root,
            game=game,
            platform=platform,
        )
        if history_result.get("ok"):
            result["refresh_outcome_history_recorded"] = True
            result["refresh_outcome_history_manifest_path"] = history_result.get("manifest_path")
        else:
            result["refresh_outcome_history_recorded"] = False
            result["refresh_outcome_history_error"] = history_result.get("error") or history_result.get("status")
            result.setdefault("warnings", []).append(
                {
                    "status": "refresh_outcome_history_record_failed",
                    "detail": result["refresh_outcome_history_error"],
                }
            )
    if render_dashboard:
        dashboard_result = render_real_artifact_intake_dashboard(
            dashboard_comparison_manifest,
            intake_root=resolved_intake_root,
            game=game,
            platform=platform,
        )
        if dashboard_result.get("ok"):
            result["dashboard_rendered"] = True
            result["dashboard_manifest_path"] = dashboard_result.get("manifest_path")
        else:
            result["dashboard_rendered"] = False
            result["dashboard_render_error"] = dashboard_result.get("error") or dashboard_result.get("status")
            result.setdefault("warnings", []).append(
                {
                    "status": "dashboard_render_failed",
                    "detail": result["dashboard_render_error"],
                }
            )
    if refresh_artifact_registry:
        artifact_registry_path = resolved_intake_root / "registry.sqlite"
        artifact_registry_root = resolved_intake_root / "reports"
        artifact_registry_root.mkdir(parents=True, exist_ok=True)
        artifact_registry_result = refresh_clip_registry(
            artifact_registry_root,
            game=game,
            registry_path=artifact_registry_path,
        )
        if artifact_registry_result.get("ok"):
            result["artifact_registry_refreshed"] = True
            result["artifact_registry_path"] = str(artifact_registry_path.resolve())
            result["artifact_registry_row_counts"] = artifact_registry_result.get("row_counts", {})
        else:
            result["artifact_registry_refreshed"] = False
            result["artifact_registry_error"] = artifact_registry_result.get("error") or artifact_registry_result.get("status")
            result.setdefault("warnings", []).append(
                {
                    "status": "artifact_registry_refresh_failed",
                    "detail": result["artifact_registry_error"],
                }
            )
    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")
        result["manifest_path"] = str(target.resolve())
    return result


def import_real_posted_lineage(
    *,
    source_roots: list[str | Path] | tuple[str | Path, ...],
    registry_path: str | Path,
    game: str | None = None,
    platform: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_sources = _unique_paths(source_roots)
    if not resolved_sources:
        return {
            "ok": False,
            "status": "missing_source_roots",
            "error": "at least one source root is required",
        }
    registry = _resolve_path(registry_path)
    workspace_root = registry.parent
    workspace_root.mkdir(parents=True, exist_ok=True)

    import_id = _import_id(
        workspace_root=workspace_root,
        source_roots=resolved_sources,
        game=game,
        platform=platform,
    )
    staging_root = workspace_root / "_real_posted_lineage_import" / import_id
    _reset_directory(staging_root)

    workspace_records = _discover_artifacts(workspace_root, suffixes=_WORKSPACE_ARTIFACT_SUFFIXES, platform=None, exclude_synthetic=False)
    source_records: list[dict[str, Any]] = []
    for source_root in resolved_sources:
        source_records.extend(
            _discover_artifacts(
                source_root,
                suffixes=_SOURCE_ARTIFACT_SUFFIXES,
                platform=platform,
                exclude_synthetic=True,
            )
        )

    _symlink_records(staging_root, workspace_records, bucket="workspace")
    _materialize_source_records(staging_root, source_records, bucket="source")
    refresh_result = refresh_clip_registry(staging_root, game=game, registry_path=registry)

    import_summary = _build_import_summary(
        workspace_root=workspace_root,
        source_roots=resolved_sources,
        source_records=source_records,
        refresh_result=refresh_result,
        registry_path=registry,
        game=game,
        platform=platform,
    )

    artifact = {
        "ok": True,
        "status": "ok",
        "schema_version": REAL_POSTED_LINEAGE_IMPORT_SCHEMA_VERSION,
        "import_id": import_id,
        "created_at": datetime.now(UTC).isoformat(),
        "workspace_root": str(workspace_root.resolve()),
        "registry_path": str(registry.resolve()),
        "refresh_root": str(staging_root.resolve()),
        "source_roots": [str(path.resolve()) for path in resolved_sources],
        "scanned_roots": [str(workspace_root.resolve()), *[str(path.resolve()) for path in resolved_sources]],
        "filters": {key: value for key, value in {"game": game, "platform": platform}.items() if value is not None},
        "workspace_artifact_count": len(workspace_records),
        "source_artifact_count": len(source_records),
        "discovered_counts": _artifact_counts(source_records),
        "imported_counts": import_summary["imported_counts"],
        "refresh_summary": {
            "warning_count": refresh_result.get("warning_count", 0),
            "posted_highlight_row_count": refresh_result.get("posted_highlight_row_count", 0),
            "posted_metrics_snapshot_row_count": refresh_result.get("posted_metrics_snapshot_row_count", 0),
            "highlight_export_row_count": refresh_result.get("highlight_export_row_count", 0),
        },
        "coverage_inventory": import_summary["coverage_inventory"],
        "source_root_summaries": import_summary["source_root_summaries"],
        "unresolved_lineage_counts": import_summary["unresolved_lineage_counts"],
        "warnings": import_summary["warnings"],
        "warning_count": refresh_result.get("warning_count", 0),
    }
    target = _resolve_path(output_path) if output_path is not None else DEFAULT_IMPORT_OUTPUT_ROOT / f"{import_id}.real_posted_lineage_import.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    artifact["manifest_path"] = str(target.resolve())

    _symlink_single(staging_root, target, bucket="workspace", label="real-posted-lineage-import")
    refresh_result = refresh_clip_registry(staging_root, game=game, registry_path=registry)
    artifact["refresh_summary"]["warning_count"] = refresh_result.get("warning_count", 0)
    artifact["warning_count"] = refresh_result.get("warning_count", 0)
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    return artifact


def refresh_real_only_benchmark(
    *,
    source_roots: list[str | Path] | tuple[str | Path, ...],
    registry_path: str | Path,
    game: str | None = None,
    platform: str | None = None,
    import_output_path: str | Path | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    registry = _resolve_path(registry_path)
    base_output_root = _resolve_path(output_root) if output_root is not None else DEFAULT_REAL_ONLY_REFRESH_OUTPUT_ROOT
    base_output_root.mkdir(parents=True, exist_ok=True)

    import_result = import_real_posted_lineage(
        source_roots=source_roots,
        registry_path=registry,
        game=game,
        platform=platform,
        output_path=import_output_path or (base_output_root / "imports" / "real_only.real_posted_lineage_import.json"),
    )
    if not import_result.get("ok"):
        return import_result

    export_result = export_v2_training_datasets(
        registry_path=registry,
        output_root=base_output_root / "dataset_exports",
        game=game,
        platform=platform,
        evidence_mode="real_only",
    )
    if not export_result.get("ok"):
        return {
            "ok": False,
            "status": "real_only_dataset_export_failed",
            "import_result": import_result,
            "export_result": export_result,
        }

    benchmark_result = run_shadow_benchmark_matrix(
        export_result["manifest_path"],
        game=game,
        platform=platform,
        output_path=base_output_root / "benchmarks" / "real_only.shadow_benchmark_matrix.json",
    )
    if not benchmark_result.get("ok"):
        return {
            "ok": False,
            "status": "real_only_benchmark_failed",
            "import_result": import_result,
            "export_result": export_result,
            "benchmark_result": benchmark_result,
        }

    review_result = review_shadow_benchmark_results(
        [benchmark_result["manifest_path"]],
        game=game,
        platform=platform,
        output_path=base_output_root / "reviews" / "real_only.shadow_benchmark_review.json",
    )
    if not review_result.get("ok"):
        return {
            "ok": False,
            "status": "real_only_benchmark_review_failed",
            "import_result": import_result,
            "export_result": export_result,
            "benchmark_result": benchmark_result,
            "review_result": review_result,
        }

    export_manifest = json.loads(Path(str(export_result["manifest_path"])).read_text(encoding="utf-8"))

    return {
        "ok": True,
        "status": "ok",
        "schema_version": "real_only_benchmark_refresh_v1",
        "registry_path": str(registry),
        "game": game,
        "platform": platform,
        "source_roots": [str(_resolve_path(item)) for item in source_roots],
        "import_manifest_path": import_result.get("manifest_path"),
        "dataset_manifest_path": export_result.get("manifest_path"),
        "benchmark_manifest_path": benchmark_result.get("manifest_path"),
        "review_manifest_path": review_result.get("manifest_path"),
        "import_summary": {
            "eligible_real_post_performance_label_count": import_result.get("coverage_inventory", {}).get(
                "eligible_real_post_performance_label_count"
            ),
            "candidate_lifecycle_row_count": import_result.get("coverage_inventory", {}).get("candidate_lifecycle_row_count"),
            "warning_count": import_result.get("warning_count", 0),
        },
        "dataset_coverage_counts": export_manifest.get("coverage_counts", {}),
        "benchmark_recommendation": benchmark_result.get("summary", {}).get("benchmark_recommendation"),
        "review_aggregate_conclusions": review_result.get("aggregate_conclusions", {}),
    }


def _resolve_path(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _load_or_build_intake_validation(
    *,
    validation_manifest: str | Path | None,
    intake_root: str | Path | None,
    game: str | None,
    platform: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    payload: dict[str, Any] | None = None
    source_manifest_path: str | None = None
    if validation_manifest is not None:
        manifest_path = _resolve_path(validation_manifest)
        payload = _load_json(manifest_path)
        source_manifest_path = str(manifest_path.resolve())
        return payload, source_manifest_path
    resolved_intake_root = _resolve_path(intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    default_manifest = resolved_intake_root / "reports" / "real_artifact_intake.validation.json"
    if default_manifest.exists():
        payload = _load_json(default_manifest)
        source_manifest_path = str(default_manifest.resolve())
    else:
        payload = validate_real_artifact_intake(
            intake_root=resolved_intake_root,
            game=game,
            platform=platform,
        )
        source_manifest_path = str(payload.get("manifest_path")) if isinstance(payload, dict) else None
    return payload, source_manifest_path


def _load_or_build_intake_dedup_advisory(
    *,
    advisory_manifest: str | Path | None,
    intake_root: str | Path | None,
    game: str | None,
    platform: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    payload: dict[str, Any] | None = None
    source_manifest_path: str | None = None
    if advisory_manifest is not None:
        manifest_path = _resolve_path(advisory_manifest)
        payload = _load_json(manifest_path)
        source_manifest_path = str(manifest_path.resolve())
        payload = _coerce_to_dedup_advisory(
            payload,
            source_manifest_path=source_manifest_path,
            intake_root=intake_root,
            game=game,
            platform=platform,
        )
        return payload, source_manifest_path
    resolved_intake_root = _resolve_path(intake_root or DEFAULT_REAL_ARTIFACT_INTAKE_ROOT)
    default_manifest = resolved_intake_root / "reports" / "real_artifact_intake.dedup_advisory.json"
    if default_manifest.exists():
        payload = _load_json(default_manifest)
        source_manifest_path = str(default_manifest.resolve())
    else:
        payload = advise_real_artifact_intake_dedup(
            intake_root=resolved_intake_root,
            game=game,
            platform=platform,
        )
        source_manifest_path = None
    return payload, source_manifest_path


def _load_refresh_target_reviews(review_manifest_path: str | Path | None) -> list[dict[str, Any]]:
    if review_manifest_path is None:
        return []
    payload = _load_json(_resolve_path(review_manifest_path))
    if not isinstance(payload, dict):
        return []
    return [row for row in list(payload.get("target_reviews", [])) if isinstance(row, dict)]


def _load_history_comparison_evidence(
    *,
    comparison_manifest: str | Path | None,
    game: str | None,
    platform: str | None,
) -> dict[str, Any] | None:
    if comparison_manifest is None:
        return None
    manifest_path = _resolve_path(comparison_manifest)
    payload = _load_json(manifest_path)
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "status": "invalid_shadow_benchmark_evidence_comparison",
            "manifest_path": str(manifest_path.resolve()),
            "error": "comparison manifest is missing or malformed",
        }
    if str(payload.get("schema_version") or "") != SHADOW_BENCHMARK_EVIDENCE_COMPARISON_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "unsupported_shadow_benchmark_evidence_comparison",
            "manifest_path": str(manifest_path.resolve()),
            "error": f"unsupported comparison schema version: {payload.get('schema_version')}",
        }
    rows = []
    for row in list(payload.get("rows", [])):
        if not isinstance(row, dict):
            continue
        if game is not None and str(row.get("game") or "").strip() not in {"", game}:
            continue
        if platform is not None and str(row.get("platform") or "").strip() not in {"", platform}:
            continue
        rows.append(row)
    return {
        "ok": True,
        "manifest_path": str(manifest_path.resolve()),
        "summary": payload.get("summary", {}),
        "rows": rows,
    }


def _history_alignment_status(
    preflight_trends: dict[str, Any],
    refresh_outcome_trends: dict[str, Any],
) -> str:
    preflight_status = str(preflight_trends.get("trend_status") or "no_history")
    refresh_status = str(refresh_outcome_trends.get("trend_status") or "no_history")
    if preflight_status == "no_history":
        return "missing_preflight_history"
    if refresh_status == "no_history":
        return "missing_refresh_history"
    if preflight_status == "improving" and refresh_status == "improving":
        return "translating_to_benchmark_improvement"
    if preflight_status == "improving" and refresh_status in {"stable", "regressing"}:
        return "intake_improving_benchmark_not_following"
    if preflight_status in {"stable", "regressing"} and refresh_status == "improving":
        return "benchmark_improving_without_preflight_gain"
    return "aligned_but_flat"


def _history_comparison_gap_status(comparison_summary: dict[str, Any]) -> str:
    if not comparison_summary:
        return "no_evidence_comparison"
    synthetic_only_ready_count = int(comparison_summary.get("synthetic_only_ready_count", 0) or 0)
    real_only_ready_count = int(comparison_summary.get("real_only_ready_count", 0) or 0)
    readiness_changed_count = int(comparison_summary.get("readiness_changed_count", 0) or 0)
    recommendation_changed_count = int(comparison_summary.get("recommendation_changed_count", 0) or 0)
    if synthetic_only_ready_count > 0:
        return "real_lags_synthetic"
    if real_only_ready_count > 0:
        return "real_exceeds_synthetic"
    if readiness_changed_count == 0 and recommendation_changed_count == 0:
        return "real_matches_synthetic"
    return "mixed_real_vs_synthetic"


def _history_comparison_next_focus(
    preflight_trends: dict[str, Any],
    refresh_outcome_trends: dict[str, Any],
    comparison_summary: dict[str, Any],
) -> str:
    preflight_status = str(preflight_trends.get("trend_status") or "no_history")
    refresh_status = str(refresh_outcome_trends.get("trend_status") or "no_history")
    gap_status = _history_comparison_gap_status(comparison_summary)
    if preflight_status == "no_history":
        return "record_preflight_history"
    if refresh_status == "no_history":
        return "record_refresh_outcome_history"
    if gap_status == "real_lags_synthetic":
        return "add_real_bundles_and_refresh"
    if preflight_status == "improving" and refresh_status in {"stable", "regressing"}:
        return "investigate_real_only_benchmark_quality"
    if preflight_status in {"stable", "regressing"}:
        return "improve_intake_health"
    return "continue_tracking"


def _real_artifact_intake_dashboard_status(
    *,
    intake_summary: dict[str, Any],
    coverage_report: dict[str, Any],
    preflight_trends: dict[str, Any],
    refresh_outcome_trends: dict[str, Any],
    history_comparison: dict[str, Any],
) -> str:
    sufficiency_status = str(coverage_report.get("sufficiency_assessment", {}).get("status") or "")
    preflight_status = str(preflight_trends.get("trend_status") or "no_history")
    refresh_status = str(refresh_outcome_trends.get("trend_status") or "no_history")
    next_focus = str(history_comparison.get("history_alignment", {}).get("next_focus") or "")
    unresolved_dedup_group_count = int(
        intake_summary.get("dedup_resolution_summary", {}).get("unresolved_count", 0) or 0
    )
    benchmark_ready_bundle_count = int(
        intake_summary.get("bundle_readiness_rollups", {}).get("benchmark_ready_bundle_count", 0) or 0
    )
    if benchmark_ready_bundle_count <= 0:
        return "blocked_no_benchmark_ready_bundles"
    if sufficiency_status != "ready_for_real_only_refresh":
        return "blocked_needs_more_real_bundles"
    if unresolved_dedup_group_count > 0:
        return "warning_unresolved_dedup"
    if next_focus == "add_real_bundles_and_refresh":
        return "real_lags_synthetic"
    if preflight_status == "improving" and refresh_status == "improving":
        return "improving_end_to_end"
    if refresh_status == "improving":
        return "improving_real_only_outcomes"
    if preflight_status == "improving":
        return "improving_intake_health"
    return "stable_monitoring"


def _coerce_to_dedup_advisory(
    payload: dict[str, Any] | None,
    *,
    source_manifest_path: str | None,
    intake_root: str | Path | None,
    game: str | None,
    platform: str | None,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    schema_version = str(payload.get("schema_version") or "").strip()
    if schema_version == REAL_ARTIFACT_INTAKE_DEDUP_ADVISORY_SCHEMA_VERSION:
        return payload
    if schema_version == REAL_ARTIFACT_INTAKE_VALIDATION_SCHEMA_VERSION:
        return advise_real_artifact_intake_dedup(
            source_manifest_path,
            intake_root=intake_root,
            game=game,
            platform=platform,
        )
    return payload


def _unique_paths(paths: list[str | Path] | tuple[str | Path, ...]) -> list[Path]:
    unique: list[Path] = []
    seen: set[str] = set()
    for item in paths:
        resolved = _resolve_path(item)
        if not resolved.exists() or not resolved.is_dir():
            continue
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    return unique


def _import_id(*, workspace_root: Path, source_roots: list[Path], game: str | None, platform: str | None) -> str:
    payload = "\n".join(
        [
            str(workspace_root.resolve()),
            *(str(path.resolve()) for path in source_roots),
            str(game or ""),
            str(platform or ""),
        ]
    )
    return f"real-import-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _reset_directory(root: Path) -> None:
    if root.exists():
        for path in sorted(root.rglob("*"), reverse=True):
            if path.is_symlink() or path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
    root.mkdir(parents=True, exist_ok=True)


def _intake_bundle_roots(intake_root: Path) -> list[Path]:
    bundles_root = intake_root / "bundles"
    candidates: list[Path] = []
    if bundles_root.exists():
        candidates = [path for path in sorted(bundles_root.iterdir()) if path.is_dir()]
    elif intake_root.exists():
        candidates = [
            path
            for path in sorted(intake_root.iterdir())
            if path.is_dir() and not path.name.startswith("_") and path.name not in {"reports", "refresh"}
        ]
        if not candidates and _discover_artifacts(
            intake_root,
            suffixes=_SOURCE_ARTIFACT_SUFFIXES,
            platform=None,
            exclude_synthetic=False,
        ):
            candidates = [intake_root]
    return candidates


def _intake_bundle_summary(bundle_root: Path, *, game: str | None, platform: str | None) -> dict[str, Any]:
    bundle_manifest_path = bundle_root / "bundle.manifest.json"
    all_records = _discover_artifacts(
        bundle_root,
        suffixes=_SOURCE_ARTIFACT_SUFFIXES,
        platform=platform,
        exclude_synthetic=False,
    )
    real_records = [record for record in all_records if not _record_is_synthetic(record)]
    synthetic_records = [record for record in all_records if _record_is_synthetic(record)]
    artifact_types = {str(record.get("artifact_type") or "") for record in real_records}
    bundle_manifest_payload = _load_json(bundle_manifest_path) if bundle_manifest_path.exists() else None
    bundle_manifest_valid, bundle_manifest_errors = _validate_bundle_manifest(
        bundle_manifest_payload,
        bundle_name=bundle_root.name,
        artifact_types=artifact_types,
        expected_game=game,
        expected_platform=platform,
    )
    real_artifact_counts = _artifact_counts(real_records)
    candidate_count = int(real_artifact_counts.get("fused_analysis", 0))
    hook_count = int(real_artifact_counts.get("hook_candidates", 0))
    export_count = int(real_artifact_counts.get("highlight_export_batch", 0))
    post_count = int(real_artifact_counts.get("posted_highlight_ledger", 0))
    metrics_count = int(real_artifact_counts.get("posted_highlight_metrics_snapshot", 0))
    eligible_label_count = _eligible_label_count(real_records)
    missing_required_artifact_types = _missing_required_artifact_types(
        artifact_types,
        bundle_manifest_present=bundle_manifest_path.exists(),
        bundle_manifest_valid=bundle_manifest_valid,
    )
    missing_optional_artifact_types = _missing_optional_artifact_types(artifact_types)
    readiness_status = _bundle_readiness_status(
        artifact_types=artifact_types,
        eligible_label_count=eligible_label_count,
        bundle_manifest_valid=bundle_manifest_valid,
        bundle_manifest_present=bundle_manifest_path.exists(),
    )
    dominant_gap_reason = _dominant_gap_reason(
        artifact_types=artifact_types,
        eligible_label_count=eligible_label_count,
        bundle_manifest_valid=bundle_manifest_valid,
        bundle_manifest_present=bundle_manifest_path.exists(),
    )
    manifest_source = bundle_manifest_payload.get("source") if isinstance(bundle_manifest_payload, dict) else None
    manifest_date_range = bundle_manifest_payload.get("date_range") if isinstance(bundle_manifest_payload, dict) else None
    manifest_completeness_expectations = (
        bundle_manifest_payload.get("completeness_expectations") if isinstance(bundle_manifest_payload, dict) else None
    )
    candidate_identifiers = sorted(_candidate_identifiers(real_records))
    export_signatures = sorted(_artifact_row_signatures(real_records, artifact_type="highlight_export_batch"))
    posted_signatures = sorted(_artifact_row_signatures(real_records, artifact_type="posted_highlight_ledger"))
    metrics_signatures = sorted(_artifact_row_signatures(real_records, artifact_type="posted_highlight_metrics_snapshot"))
    return {
        "bundle_name": bundle_root.name,
        "bundle_root": str(bundle_root.resolve()),
        "status": _bundle_status(artifact_types),
        "real_artifact_count": len(real_records),
        "synthetic_artifact_count": len(synthetic_records),
        "real_artifact_counts": real_artifact_counts,
        "synthetic_artifact_counts": _artifact_counts(synthetic_records),
        "bundle_manifest_path": str(bundle_manifest_path.resolve()),
        "bundle_manifest_present": bundle_manifest_path.exists(),
        "bundle_manifest_valid": bundle_manifest_valid,
        "bundle_manifest_errors": bundle_manifest_errors,
        "manifest_source_label": manifest_source.get("label") if isinstance(manifest_source, dict) else None,
        "manifest_source_kind": manifest_source.get("kind") if isinstance(manifest_source, dict) else None,
        "manifest_game": bundle_manifest_payload.get("game") if isinstance(bundle_manifest_payload, dict) else None,
        "manifest_platform": bundle_manifest_payload.get("platform") if isinstance(bundle_manifest_payload, dict) else None,
        "manifest_operator_notes": bundle_manifest_payload.get("operator_notes") if isinstance(bundle_manifest_payload, dict) else None,
        "manifest_date_range": manifest_date_range if isinstance(manifest_date_range, dict) else None,
        "manifest_completeness_expectations": (
            manifest_completeness_expectations if isinstance(manifest_completeness_expectations, dict) else None
        ),
        "has_candidate_lineage": "fused_analysis" in artifact_types,
        "has_downstream_lineage": bool(
            artifact_types & {"highlight_export_batch", "posted_highlight_ledger", "posted_highlight_metrics_snapshot"}
        ),
        "eligible_for_import": bool(real_records),
        "readiness_status": readiness_status,
        "missing_required_artifact_types": missing_required_artifact_types,
        "missing_optional_artifact_types": missing_optional_artifact_types,
        "has_eligible_post_performance_labels": eligible_label_count > 0,
        "eligible_post_performance_label_count": eligible_label_count,
        "unresolved_lineage_warning_count": 0,
        "dominant_gap_reason": dominant_gap_reason,
        "candidate_artifact_count": candidate_count,
        "hook_artifact_count": hook_count,
        "export_artifact_count": export_count,
        "post_artifact_count": post_count,
        "metrics_artifact_count": metrics_count,
        "contributes_imported_candidate_lineage": candidate_count > 0,
        "contributes_imported_downstream_lineage": any(count > 0 for count in (export_count, post_count, metrics_count)),
        "candidate_identifiers": candidate_identifiers,
        "export_record_signatures": export_signatures,
        "posted_record_signatures": posted_signatures,
        "metrics_record_signatures": metrics_signatures,
        "_real_records": real_records,
        "_synthetic_records": synthetic_records,
    }


def _bundle_status(artifact_types: set[str]) -> str:
    if not artifact_types:
        return "empty_bundle"
    has_candidate_lineage = "fused_analysis" in artifact_types
    has_downstream_lineage = bool(
        artifact_types & {"highlight_export_batch", "posted_highlight_ledger", "posted_highlight_metrics_snapshot"}
    )
    has_benchmark_minimum = {
        "fused_analysis",
        "highlight_export_batch",
        "posted_highlight_ledger",
        "posted_highlight_metrics_snapshot",
    }.issubset(artifact_types)
    if has_benchmark_minimum:
        return "lineage_complete"
    if has_downstream_lineage and not has_candidate_lineage:
        return "downstream_only"
    if has_candidate_lineage:
        return "partial_lineage"
    return "artifact_only"


def _intake_status_from_bundle_summaries(bundle_summaries: list[dict[str, Any]]) -> str:
    if not bundle_summaries:
        return "empty_intake_root"
    statuses = {str(summary.get("status") or "") for summary in bundle_summaries}
    if "lineage_complete" in statuses:
        return "lineage_complete"
    if "downstream_only" in statuses:
        return "downstream_only"
    if "partial_lineage" in statuses:
        return "partial_lineage"
    return "artifact_only"


def _intake_status_from_coverage(*, bundle_summaries: list[dict[str, Any]], coverage_inventory: dict[str, Any]) -> str:
    if not bundle_summaries:
        return "empty_intake_root"
    readiness_statuses = {str(summary.get("readiness_status") or "") for summary in bundle_summaries}
    if "benchmark_ready" in readiness_statuses and int(coverage_inventory.get("eligible_real_post_performance_label_count") or 0) > 0:
        return "benchmark_ready"
    if "missing_bundle_manifest" in {str(summary.get("dominant_gap_reason") or "") for summary in bundle_summaries}:
        return "lineage_complete_without_required_metadata"
    if int(coverage_inventory.get("imported_candidate_count") or 0) == 0 and (
        int(coverage_inventory.get("imported_post_count") or 0) > 0
        or int(coverage_inventory.get("imported_export_count") or 0) > 0
        or int(coverage_inventory.get("imported_posted_metrics_row_count") or 0) > 0
    ):
        return "downstream_only"
    if int(coverage_inventory.get("imported_candidate_count") or 0) > 0 and int(
        coverage_inventory.get("imported_posted_metrics_row_count") or 0
    ) == 0:
        return "lineage_complete_without_metrics"
    if int(coverage_inventory.get("imported_candidate_count") or 0) > 0:
        return "lineage_complete_without_eligible_metrics"
    return _intake_status_from_bundle_summaries(bundle_summaries)


def _record_is_synthetic(record: dict[str, Any]) -> bool:
    path = Path(str(record.get("path") or ""))
    artifact_type = str(record.get("artifact_type") or "")
    payload = _load_json(path) if path.exists() and artifact_type in {"posted_highlight_ledger", "posted_highlight_metrics_snapshot"} else None
    return _is_synthetic_artifact(path, payload, artifact_type=artifact_type) if artifact_type else False


def _empty_imported_counts() -> dict[str, int]:
    return {
        "fused_analysis_manifest_count": 0,
        "hook_candidate_manifest_count": 0,
        "hook_comparison_report_count": 0,
        "highlight_selection_manifest_count": 0,
        "highlight_export_batch_manifest_count": 0,
        "post_ledger_manifest_count": 0,
        "posted_metrics_snapshot_manifest_count": 0,
        "workflow_run_manifest_count": 0,
    }


def _discover_artifacts(
    root: Path,
    *,
    suffixes: tuple[str, ...],
    platform: str | None,
    exclude_synthetic: bool,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.json")):
        if "_real_posted_lineage_import" in path.parts:
            continue
        if not _matches_any_suffix(path.name, suffixes):
            continue
        artifact_type = _artifact_type(path.name)
        if artifact_type is None:
            continue
        payload = _load_json(path) if artifact_type in {"posted_highlight_ledger", "posted_highlight_metrics_snapshot"} else None
        if exclude_synthetic and _is_synthetic_artifact(path, payload, artifact_type=artifact_type):
            continue
        if platform is not None and not _platform_matches(payload, artifact_type=artifact_type, platform=platform):
            continue
        records.append(
            {
                "path": path.resolve(),
                "artifact_type": artifact_type,
            }
        )
    return records


def _matches_any_suffix(name: str, suffixes: tuple[str, ...]) -> bool:
    return any(name.endswith(suffix) for suffix in suffixes)


def _artifact_type(name: str) -> str | None:
    if name.endswith(".highlight_selection.json"):
        return "highlight_selection"
    if name.endswith(".highlight_export_batch.json"):
        return "highlight_export_batch"
    if name.endswith(".posted_highlight_ledger.json"):
        return "posted_highlight_ledger"
    if name.endswith(".posted_highlight_metrics_snapshot.json"):
        return "posted_highlight_metrics_snapshot"
    if name.endswith(".fused_analysis.json"):
        return "fused_analysis"
    if name.endswith(".hook_candidates.json"):
        return "hook_candidates"
    if name.endswith("hook_comparison.json"):
        return "hook_comparison"
    if name.endswith(".shadow_benchmark_matrix.json"):
        return "shadow_benchmark_matrix"
    if name.endswith(".shadow_benchmark_review.json"):
        return "shadow_benchmark_review"
    if name.endswith(".shadow_ranking_model.json"):
        return "shadow_ranking_model"
    if name.endswith(".shadow_ranking_experiment.json"):
        return "shadow_ranking_experiment"
    if name.endswith(".shadow_experiment_ledger.json"):
        return "shadow_experiment_ledger"
    if name.endswith(".shadow_ranking_replay.json"):
        return "shadow_ranking_replay"
    if name.endswith(".shadow_ranking_comparison.json"):
        return "shadow_ranking_comparison"
    if name.endswith(".shadow_model_family_comparison.json"):
        return "shadow_model_family_comparison"
    if name.endswith(".shadow_evaluation_policy.json"):
        return "shadow_evaluation_policy"
    if name.endswith(".workflow_run.json"):
        return "workflow_run"
    if name.endswith(".proxy_scan.json"):
        return "proxy_scan"
    if name.endswith(".runtime_analysis.json"):
        return "runtime_analysis"
    if name.endswith(".runtime_review_session.json"):
        return "runtime_review_session"
    if name.endswith(".fused_review_session.json"):
        return "fused_review_session"
    if name == "fixture_trial_run_manifest.json":
        return "fixture_trial_run_manifest"
    if name == "fixture_trial_batch_manifest.json":
        return "fixture_trial_batch_manifest"
    return None


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _is_synthetic_artifact(path: Path, payload: dict[str, Any] | None, *, artifact_type: str) -> bool:
    if "synthetic" in str(path).lower():
        return True
    if artifact_type == "posted_highlight_metrics_snapshot" and isinstance(payload, dict):
        for row in list(payload.get("snapshots", [])):
            if not isinstance(row, dict):
                continue
            metadata = row.get("metadata")
            if isinstance(metadata, dict) and bool(metadata.get("synthetic_benchmark")):
                return True
    return False


def _platform_matches(payload: dict[str, Any] | None, *, artifact_type: str, platform: str) -> bool:
    if payload is None:
        return True
    normalized = str(platform or "").strip()
    if not normalized:
        return True
    top_level = str(payload.get("platform") or "").strip()
    if top_level == normalized:
        return True
    if artifact_type == "posted_highlight_ledger":
        return any(str(item.get("platform") or "").strip() == normalized for item in payload.get("posted_records", []) if isinstance(item, dict))
    if artifact_type == "posted_highlight_metrics_snapshot":
        return any(str(item.get("platform") or "").strip() == normalized for item in payload.get("snapshots", []) if isinstance(item, dict))
    return True


def _symlink_records(staging_root: Path, records: list[dict[str, Any]], *, bucket: str) -> None:
    for index, record in enumerate(records):
        _symlink_single(staging_root, record["path"], bucket=bucket, label=f"{index:04d}-{record['artifact_type']}")


def _materialize_source_records(staging_root: Path, records: list[dict[str, Any]], *, bucket: str) -> None:
    target_map: dict[str, Path] = {}
    basename_counts: dict[str, int] = {}
    payload_map: dict[str, dict[str, Any] | None] = {}
    for index, record in enumerate(records):
        source_path = Path(record["path"])
        target = _staging_target(staging_root, source_path, bucket=bucket, label=f"{index:04d}-{record['artifact_type']}")
        target_map[str(source_path)] = target
        basename_counts[source_path.name] = basename_counts.get(source_path.name, 0) + 1
        payload_map[str(source_path)] = _load_json(source_path)

    basename_map = {
        name: str(target_map[source])
        for source, target in target_map.items()
        for name in [Path(source).name]
        if basename_counts.get(name, 0) == 1
    }
    path_map = {source: str(target) for source, target in target_map.items()}
    candidate_id_map = _candidate_id_remap(records=records, payload_map=payload_map, path_map=path_map)

    for record in records:
        source_path = Path(record["path"])
        target = target_map[str(source_path)]
        payload = payload_map[str(source_path)]
        target.parent.mkdir(parents=True, exist_ok=True)
        if payload is None:
            if target.exists() or target.is_symlink():
                target.unlink()
            os.symlink(source_path, target)
            continue
        rewritten = _rewrite_artifact_paths(
            payload,
            path_map=path_map,
            basename_map=basename_map,
            candidate_id_map=candidate_id_map,
        )
        target.write_text(json.dumps(rewritten, indent=2), encoding="utf-8")


def _symlink_single(staging_root: Path, source_path: Path, *, bucket: str, label: str) -> None:
    target = _staging_target(staging_root, source_path, bucket=bucket, label=label)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        target.unlink()
    os.symlink(source_path, target)


def _staging_target(staging_root: Path, source_path: Path, *, bucket: str, label: str) -> Path:
    return staging_root / bucket / f"{label}-{source_path.name}"


def _artifact_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        key = str(record.get("artifact_type") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _eligible_label_count(records: list[dict[str, Any]]) -> int:
    count = 0
    for record in records:
        if str(record.get("artifact_type") or "") != "posted_highlight_metrics_snapshot":
            continue
        payload = _load_json(Path(str(record.get("path") or "")))
        if not isinstance(payload, dict):
            continue
        for row in list(payload.get("snapshots", [])):
            if not isinstance(row, dict):
                continue
            if bool(_post_performance_label_fields(row).get("post_performance_label_eligible")):
                count += 1
    return count


def _candidate_identifiers(records: list[dict[str, Any]]) -> set[str]:
    identifiers: set[str] = set()
    for record in records:
        if str(record.get("artifact_type") or "") != "fused_analysis":
            continue
        payload = _load_json(Path(str(record.get("path") or "")))
        if not isinstance(payload, dict):
            continue
        for row in list(payload.get("candidates", [])):
            if not isinstance(row, dict):
                continue
            candidate_id = str(row.get("candidate_id") or "").strip()
            if candidate_id:
                identifiers.add(candidate_id)
    return identifiers


def _artifact_row_signatures(records: list[dict[str, Any]], *, artifact_type: str) -> set[str]:
    signatures: set[str] = set()
    payload_key = {
        "highlight_export_batch": "exports",
        "posted_highlight_ledger": "posted_records",
        "posted_highlight_metrics_snapshot": "snapshots",
    }.get(artifact_type)
    if payload_key is None:
        return signatures
    for record in records:
        if str(record.get("artifact_type") or "") != artifact_type:
            continue
        payload = _load_json(Path(str(record.get("path") or "")))
        if not isinstance(payload, dict):
            continue
        for row in list(payload.get(payload_key, [])):
            if not isinstance(row, dict):
                continue
            signatures.add(_row_signature(artifact_type, row))
    return signatures


def _row_signature(artifact_type: str, row: dict[str, Any]) -> str:
    preferred_fields = {
        "highlight_export_batch": ("candidate_id", "platform", "account_id", "exported_at", "clip_path", "render_path"),
        "posted_highlight_ledger": ("candidate_id", "platform", "account_id", "posted_at", "post_url", "external_post_id"),
        "posted_highlight_metrics_snapshot": (
            "candidate_id",
            "platform",
            "account_id",
            "snapshot_at",
            "external_post_id",
            "post_url",
        ),
    }.get(artifact_type, ())
    parts = [artifact_type]
    for field in preferred_fields:
        value = row.get(field)
        if value not in (None, "", []):
            parts.append(f"{field}={value}")
    if len(parts) == 1:
        parts.append(json.dumps(row, sort_keys=True))
    return "|".join(parts)


def _missing_required_artifact_types(
    artifact_types: set[str],
    *,
    bundle_manifest_present: bool,
    bundle_manifest_valid: bool,
) -> list[str]:
    required = (
        "fused_analysis",
        "highlight_export_batch",
        "posted_highlight_ledger",
        "posted_highlight_metrics_snapshot",
    )
    missing = [artifact_type for artifact_type in required if artifact_type not in artifact_types]
    if not bundle_manifest_present:
        missing.append("bundle_manifest")
    elif not bundle_manifest_valid:
        missing.append("bundle_manifest_metadata")
    return missing


def _missing_optional_artifact_types(artifact_types: set[str]) -> list[str]:
    optional = ("hook_candidates", "highlight_selection", "hook_comparison", "workflow_run")
    return [artifact_type for artifact_type in optional if artifact_type not in artifact_types]


def _bundle_readiness_status(
    *,
    artifact_types: set[str],
    eligible_label_count: int,
    bundle_manifest_present: bool,
    bundle_manifest_valid: bool,
) -> str:
    if not artifact_types:
        return "empty"
    if "fused_analysis" not in artifact_types and artifact_types & {"highlight_export_batch", "posted_highlight_ledger", "posted_highlight_metrics_snapshot"}:
        return "downstream_only"
    if _missing_required_artifact_types(
        artifact_types,
        bundle_manifest_present=bundle_manifest_present,
        bundle_manifest_valid=bundle_manifest_valid,
    ):
        return "partial_lineage"
    if eligible_label_count > 0:
        return "benchmark_ready"
    return "lineage_complete_without_eligible_metrics"


def _dominant_gap_reason(
    *,
    artifact_types: set[str],
    eligible_label_count: int,
    bundle_manifest_present: bool,
    bundle_manifest_valid: bool,
) -> str:
    if not artifact_types:
        return "empty_bundle"
    if not bundle_manifest_present:
        return "missing_bundle_manifest"
    if "fused_analysis" not in artifact_types:
        return "missing_fused_lineage"
    if "highlight_export_batch" not in artifact_types:
        return "missing_export_manifest"
    if "posted_highlight_ledger" not in artifact_types:
        return "missing_post_ledger"
    if "posted_highlight_metrics_snapshot" not in artifact_types:
        return "missing_metrics_snapshot"
    if not bundle_manifest_valid:
        return "invalid_bundle_manifest"
    if eligible_label_count <= 0:
        return "no_eligible_post_performance_labels"
    return "benchmark_ready"


def _enrich_bundle_summaries(bundle_summaries: list[dict[str, Any]], *, warnings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for summary in bundle_summaries:
        bundle_root = str(summary.get("bundle_root") or "")
        bundle_warnings = [warning for warning in warnings if bundle_root and bundle_root in str(warning.get("path") or "")]
        enriched.append(
            {
                **summary,
                "unresolved_lineage_warning_count": len(bundle_warnings),
            }
        )
    return enriched


def _bundle_readiness_rollups(bundle_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    readiness_status_counts: dict[str, int] = {}
    dominant_gap_reason_counts: dict[str, int] = {}
    bundles_with_eligible_labels_count = 0
    missing_fused_lineage_bundle_count = 0
    missing_post_or_metrics_bundle_count = 0
    missing_bundle_manifest_bundle_count = 0
    for summary in bundle_summaries:
        readiness_status = str(summary.get("readiness_status") or "unknown")
        dominant_gap_reason = str(summary.get("dominant_gap_reason") or "unknown")
        readiness_status_counts[readiness_status] = readiness_status_counts.get(readiness_status, 0) + 1
        dominant_gap_reason_counts[dominant_gap_reason] = dominant_gap_reason_counts.get(dominant_gap_reason, 0) + 1
        if bool(summary.get("has_eligible_post_performance_labels")):
            bundles_with_eligible_labels_count += 1
        if dominant_gap_reason == "missing_fused_lineage":
            missing_fused_lineage_bundle_count += 1
        if dominant_gap_reason in {"missing_bundle_manifest", "invalid_bundle_manifest"}:
            missing_bundle_manifest_bundle_count += 1
        if dominant_gap_reason in {"missing_post_ledger", "missing_metrics_snapshot", "no_eligible_post_performance_labels"}:
            missing_post_or_metrics_bundle_count += 1
    return {
        "bundle_count_by_readiness_status": readiness_status_counts,
        "bundle_count_by_dominant_gap_reason": dominant_gap_reason_counts,
        "benchmark_ready_bundle_count": readiness_status_counts.get("benchmark_ready", 0),
        "bundles_with_eligible_labels_count": bundles_with_eligible_labels_count,
        "missing_fused_lineage_bundle_count": missing_fused_lineage_bundle_count,
        "missing_bundle_manifest_bundle_count": missing_bundle_manifest_bundle_count,
        "missing_post_or_metrics_bundle_count": missing_post_or_metrics_bundle_count,
    }


def _dedup_resolution_summary_for_bundle_summaries(
    bundle_summaries: list[dict[str, Any]],
    *,
    intake_root: str | Path,
) -> dict[str, Any]:
    duplicate_summary = _cross_bundle_duplicate_summary(bundle_summaries)
    duplicate_rows = [row for row in list(duplicate_summary.get("duplicate_rows", [])) if isinstance(row, dict)]
    if not duplicate_rows:
        return {
            "resolution_count": 0,
            "status_counts": {},
            "unresolved_count": 0,
            "has_unresolved_duplicate_groups": False,
        }
    bundle_index = {
        str(summary.get("bundle_name") or ""): summary
        for summary in bundle_summaries
        if isinstance(summary, dict)
    }
    resolved_intake_root = _resolve_path(intake_root)
    resolutions_root = resolved_intake_root / "resolutions"
    status_counts: dict[str, int] = {}
    unresolved_groups: list[str] = []
    for duplicate_row in duplicate_rows:
        advisory_row = _dedup_advisory_row(duplicate_row, bundle_index=bundle_index)
        group_id = _dedup_group_id(advisory_row)
        resolution_path = resolutions_root / f"{group_id}.resolution.json"
        payload = _load_json(resolution_path) if resolution_path.exists() else _dedup_resolution_template(advisory_row)
        status = str(payload.get("status") or "pending").strip().lower() or "pending"
        status_counts[status] = status_counts.get(status, 0) + 1
        if status == "pending":
            unresolved_groups.append(group_id)
    return {
        "resolution_count": len(duplicate_rows),
        "status_counts": status_counts,
        "unresolved_count": status_counts.get("pending", 0),
        "has_unresolved_duplicate_groups": bool(unresolved_groups),
        "unresolved_group_ids": unresolved_groups,
    }


def _bundle_count_rollup(
    bundle_summaries: list[dict[str, Any]],
    *,
    key: str,
    formatter: Any | None = None,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for summary in bundle_summaries:
        raw_value = summary.get(key)
        value = formatter(raw_value) if formatter is not None else raw_value
        normalized = str(value or "unknown").strip() or "unknown"
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _date_range_label(value: Any) -> str:
    if not isinstance(value, dict):
        return "unknown"
    start = str(value.get("start") or "").strip()
    end = str(value.get("end") or "").strip()
    if not start and not end:
        return "unknown"
    return f"{start or '?'}..{end or '?'}"


def _cross_bundle_duplicate_summary(bundle_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    duplicate_bundle_counts: dict[str, int] = {}
    artifact_types = {
        "export_record_signatures": "highlight_export_batch",
        "posted_record_signatures": "posted_highlight_ledger",
        "metrics_record_signatures": "posted_highlight_metrics_snapshot",
    }
    duplicate_rows: list[dict[str, Any]] = []
    duplicate_total = 0
    for summary_key, artifact_type in artifact_types.items():
        signature_to_bundles: dict[str, set[str]] = {}
        for summary in bundle_summaries:
            bundle_name = str(summary.get("bundle_name") or "")
            for signature in list(summary.get(summary_key, [])):
                normalized = str(signature or "").strip()
                if not normalized:
                    continue
                signature_to_bundles.setdefault(normalized, set()).add(bundle_name)
        duplicate_count = 0
        for signature, bundles in signature_to_bundles.items():
            if len(bundles) <= 1:
                continue
            duplicate_count += 1
            duplicate_total += 1
            sorted_bundles = sorted(bundle for bundle in bundles if bundle)
            duplicate_rows.append(
                {
                    "artifact_type": artifact_type,
                    "signature": signature,
                    "bundle_names": sorted_bundles,
                }
            )
            for bundle_name in sorted_bundles:
                duplicate_bundle_counts[bundle_name] = duplicate_bundle_counts.get(bundle_name, 0) + 1
    return {
        "duplicate_downstream_record_total": duplicate_total,
        "duplicate_downstream_record_count_by_artifact_type": _bundle_count_rollup(
            duplicate_rows,
            key="artifact_type",
        ),
        "duplicate_rows": duplicate_rows,
        "bundle_duplicate_counts": duplicate_bundle_counts,
    }


def _dedup_advisory_row(duplicate_row: dict[str, Any], *, bundle_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    bundle_names = [str(name or "").strip() for name in list(duplicate_row.get("bundle_names", [])) if str(name or "").strip()]
    ranked_bundle_names = sorted(bundle_names, key=lambda name: _canonical_bundle_sort_key(bundle_index.get(name, {})))
    canonical_bundle_name = ranked_bundle_names[0] if ranked_bundle_names else None
    non_canonical_bundle_names = [name for name in ranked_bundle_names if name != canonical_bundle_name]
    canonical_bundle = bundle_index.get(str(canonical_bundle_name or ""), {})
    recommendation_reason = _dedup_recommendation_reason(canonical_bundle)
    row = {
        "artifact_type": duplicate_row.get("artifact_type"),
        "signature": duplicate_row.get("signature"),
        "bundle_names": ranked_bundle_names,
        "canonical_bundle_name": canonical_bundle_name,
        "non_canonical_bundle_names": non_canonical_bundle_names,
        "recommendation_reason": recommendation_reason,
        "recommended_actions": (
            ["keep_canonical_bundle"]
            + (["remove_duplicate_downstream_lineage"] if non_canonical_bundle_names else [])
            + ["review_before_cleanup"]
        ),
    }
    row["group_id"] = _dedup_group_id(row)
    return row


def _canonical_bundle_sort_key(bundle_row: dict[str, Any]) -> tuple[int, int, int, str]:
    readiness_status = str(bundle_row.get("readiness_status") or "")
    manifest_valid = bool(bundle_row.get("bundle_manifest_valid"))
    eligible_labels = int(bundle_row.get("eligible_post_performance_label_count") or 0)
    bundle_name = str(bundle_row.get("bundle_name") or "")
    readiness_rank = 0 if readiness_status == "benchmark_ready" else 1
    manifest_rank = 0 if manifest_valid else 1
    label_rank = -eligible_labels
    return (readiness_rank, manifest_rank, label_rank, bundle_name)


def _dedup_recommendation_reason(bundle_row: dict[str, Any]) -> str:
    readiness_status = str(bundle_row.get("readiness_status") or "")
    if readiness_status == "benchmark_ready":
        return "selected benchmark-ready bundle over weaker duplicate candidates"
    if bool(bundle_row.get("bundle_manifest_valid")):
        return "selected manifest-valid bundle over weaker duplicate candidates"
    return "selected deterministic lexical tie-break after readiness and metadata comparison"


def _dedup_group_id(advisory_row: dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "artifact_type": advisory_row.get("artifact_type"),
            "signature": advisory_row.get("signature"),
            "bundle_names": sorted(str(item or "") for item in list(advisory_row.get("bundle_names", []))),
        },
        sort_keys=True,
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
    return f"dedup-{digest}"


def _dedup_resolution_template(advisory_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": REAL_ARTIFACT_INTAKE_DEDUP_RESOLUTION_SCHEMA_VERSION,
        "group_id": _dedup_group_id(advisory_row),
        "artifact_type": advisory_row.get("artifact_type"),
        "signature": advisory_row.get("signature"),
        "bundle_names": advisory_row.get("bundle_names", []),
        "canonical_bundle_name": advisory_row.get("canonical_bundle_name"),
        "status": "pending",
        "reviewed_at": None,
        "reviewed_by": None,
        "notes": "",
        "recommendation_reason": advisory_row.get("recommendation_reason"),
        "recommended_actions": advisory_row.get("recommended_actions", []),
    }


def _normalized_bundle_name(value: str) -> str:
    cleaned = "".join(character if character.isalnum() or character in {"-", "_"} else "-" for character in str(value or "").strip())
    parts = [part for part in cleaned.split("-") if part]
    if not parts:
        return ""
    return "-".join(parts)


def _safe_history_slug(value: str) -> str:
    normalized = _normalized_bundle_name(value)
    return normalized or "unknown"


def _bundle_manifest_template(bundle_name: str) -> dict[str, Any]:
    return {
        "schema_version": REAL_ARTIFACT_INTAKE_BUNDLE_MANIFEST_SCHEMA_VERSION,
        "bundle_name": bundle_name,
        "source": {
            "label": "",
            "kind": "local_drop",
            "description": "",
        },
        "game": "",
        "platform": "",
        "date_range": {
            "start": "",
            "end": "",
        },
        "operator_notes": "",
        "completeness_expectations": {
            "expected_artifact_types": [
                "fused_analysis",
                "highlight_export_batch",
                "posted_highlight_ledger",
                "posted_highlight_metrics_snapshot",
            ],
            "notes": "",
        },
    }


def _validate_bundle_manifest(
    payload: dict[str, Any] | None,
    *,
    bundle_name: str,
    artifact_types: set[str],
    expected_game: str | None,
    expected_platform: str | None,
) -> tuple[bool, list[str]]:
    if not isinstance(payload, dict):
        return False, ["manifest is missing or malformed"]
    errors: list[str] = []
    if str(payload.get("schema_version") or "").strip() != REAL_ARTIFACT_INTAKE_BUNDLE_MANIFEST_SCHEMA_VERSION:
        errors.append("schema_version must be real_artifact_intake_bundle_manifest_v1")
    if str(payload.get("bundle_name") or "").strip() != str(bundle_name).strip():
        errors.append("bundle_name must match the bundle directory name")
    source = payload.get("source")
    if not isinstance(source, dict):
        errors.append("source must be an object")
    else:
        if not str(source.get("label") or "").strip():
            errors.append("source.label is required")
        if not str(source.get("kind") or "").strip():
            errors.append("source.kind is required")
    if not str(payload.get("game") or "").strip():
        errors.append("game is required")
    elif expected_game is not None and str(payload.get("game") or "").strip() != str(expected_game).strip():
        errors.append("game must match the validation game filter")
    if not str(payload.get("platform") or "").strip():
        errors.append("platform is required")
    elif expected_platform is not None and str(payload.get("platform") or "").strip() != str(expected_platform).strip():
        errors.append("platform must match the validation platform filter")
    date_range = payload.get("date_range")
    if not isinstance(date_range, dict):
        errors.append("date_range must be an object")
    else:
        start = str(date_range.get("start") or "").strip()
        end = str(date_range.get("end") or "").strip()
        if not start:
            errors.append("date_range.start is required")
        elif not _is_iso_date(start):
            errors.append("date_range.start must use YYYY-MM-DD")
        if not end:
            errors.append("date_range.end is required")
        elif not _is_iso_date(end):
            errors.append("date_range.end must use YYYY-MM-DD")
        if _is_iso_date(start) and _is_iso_date(end) and start > end:
            errors.append("date_range.start must be on or before date_range.end")
    completeness_expectations = payload.get("completeness_expectations")
    if not isinstance(completeness_expectations, dict):
        errors.append("completeness_expectations must be an object")
    else:
        expected_artifact_types = completeness_expectations.get("expected_artifact_types")
        if not isinstance(expected_artifact_types, list) or not expected_artifact_types:
            errors.append("completeness_expectations.expected_artifact_types is required")
        else:
            declared = [str(item).strip() for item in expected_artifact_types if str(item).strip()]
            unknown = sorted(item for item in declared if item not in _declared_bundle_artifact_types())
            if unknown:
                errors.append(f"completeness_expectations.expected_artifact_types contains unknown types: {', '.join(unknown)}")
            missing_declared = sorted(item for item in declared if item not in artifact_types)
            if missing_declared:
                errors.append(
                    "declared expected_artifact_types missing from bundle artifacts: "
                    + ", ".join(missing_declared)
                )
    return not errors, errors


def _is_iso_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _declared_bundle_artifact_types() -> set[str]:
    return {
        "fused_analysis",
        "hook_candidates",
        "highlight_selection",
        "highlight_export_batch",
        "posted_highlight_ledger",
        "posted_highlight_metrics_snapshot",
        "hook_comparison",
        "workflow_run",
    }


def _bundle_checklist_contents(bundle_name: str) -> str:
    return "\n".join(
        [
            f"# Real Artifact Intake Bundle: {bundle_name}",
            "",
            "Fill in `bundle.manifest.json` before expecting this bundle to become `benchmark_ready`.",
            "",
            "Place real local artifacts into these folders:",
            "",
            "- `fused/`: required `.fused_analysis.json` files",
            "- `hooks/`: optional `.hook_candidates.json` files",
            "- `selection/`: optional `.highlight_selection.json` files",
            "- `exports/`: required `.highlight_export_batch.json` files",
            "- `posted/`: required `.posted_highlight_ledger.json` files",
            "- `metrics/`: required `.posted_highlight_metrics_snapshot.json` files",
            "",
            "Checklist:",
            "",
            "- [ ] Completed `bundle.manifest.json` metadata",
            "- [ ] Added fused candidate lineage",
            "- [ ] Added export manifests",
            "- [ ] Added posted ledger manifests",
            "- [ ] Added metrics snapshots",
            "- [ ] Confirmed no synthetic benchmark artifacts are present",
            "- [ ] Ran `python3 run.py --validate-real-artifact-intake --game <game> --platform <platform>`",
            "- [ ] Ran `python3 run.py --summarize-real-artifact-intake --game <game> --platform <platform>`",
            "",
        ]
    )


def _candidate_id_remap(
    *,
    records: list[dict[str, Any]],
    payload_map: dict[str, dict[str, Any] | None],
    path_map: dict[str, str],
) -> dict[str, str]:
    candidate_id_map: dict[str, str] = {}
    for record in records:
        if str(record.get("artifact_type") or "") != "fused_analysis":
            continue
        source_path = str(record.get("path") or "")
        payload = payload_map.get(source_path)
        if not isinstance(payload, dict):
            continue
        game = str(payload.get("game") or "").strip()
        source = str(payload.get("source") or "").strip()
        original_sidecar_path = str(payload.get("sidecar_path") or source_path).strip()
        staged_sidecar_path = str(path_map.get(source_path) or "").strip()
        if not game or not source or not original_sidecar_path or not staged_sidecar_path:
            continue
        for event in list(payload.get("fused_events", [])):
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("event_id") or "").strip()
            if not event_id:
                continue
            original_candidate_id = _candidate_id_for_lineage(
                game=game,
                source=source,
                fused_sidecar_path=original_sidecar_path,
                event_id=event_id,
            )
            staged_candidate_id = _candidate_id_for_lineage(
                game=game,
                source=source,
                fused_sidecar_path=staged_sidecar_path,
                event_id=event_id,
            )
            candidate_id_map[original_candidate_id] = staged_candidate_id
    return candidate_id_map


def _candidate_id_for_lineage(*, game: str, source: str, fused_sidecar_path: str, event_id: str) -> str:
    digest = hashlib.sha1(
        "::".join([game.strip(), source.strip(), fused_sidecar_path.strip(), event_id.strip()]).encode("utf-8")
    ).hexdigest()[:16]
    return f"candidate-{digest}"


def _rewrite_artifact_paths(
    value: Any,
    *,
    path_map: dict[str, str],
    basename_map: dict[str, str],
    candidate_id_map: dict[str, str],
) -> Any:
    if isinstance(value, dict):
        rewritten: dict[str, Any] = {}
        for key, item in value.items():
            if key == "candidate_id" and isinstance(item, str):
                rewritten[key] = candidate_id_map.get(item, item)
                continue
            rewritten[key] = _rewrite_artifact_paths(
                item,
                path_map=path_map,
                basename_map=basename_map,
                candidate_id_map=candidate_id_map,
            )
        return rewritten
    if isinstance(value, list):
        return [
            _rewrite_artifact_paths(
                item,
                path_map=path_map,
                basename_map=basename_map,
                candidate_id_map=candidate_id_map,
            )
            for item in value
        ]
    if not isinstance(value, str):
        return value
    if value in path_map:
        return path_map[value]
    basename = Path(value).name
    if basename in basename_map:
        return basename_map[basename]
    return value


def _build_import_summary(
    *,
    workspace_root: Path,
    source_roots: list[Path],
    source_records: list[dict[str, Any]],
    refresh_result: dict[str, Any],
    registry_path: Path,
    game: str | None,
    platform: str | None,
) -> dict[str, Any]:
    imported_counts = {
        "fused_analysis_manifest_count": sum(1 for record in source_records if record["artifact_type"] == "fused_analysis"),
        "hook_candidate_manifest_count": sum(1 for record in source_records if record["artifact_type"] == "hook_candidates"),
        "hook_comparison_report_count": sum(1 for record in source_records if record["artifact_type"] == "hook_comparison"),
        "highlight_selection_manifest_count": sum(1 for record in source_records if record["artifact_type"] == "highlight_selection"),
        "highlight_export_batch_manifest_count": sum(1 for record in source_records if record["artifact_type"] == "highlight_export_batch"),
        "post_ledger_manifest_count": sum(1 for record in source_records if record["artifact_type"] == "posted_highlight_ledger"),
        "posted_metrics_snapshot_manifest_count": sum(
            1 for record in source_records if record["artifact_type"] == "posted_highlight_metrics_snapshot"
        ),
        "workflow_run_manifest_count": sum(1 for record in source_records if record["artifact_type"] == "workflow_run"),
    }
    candidate_rows = query_clip_registry(mode="candidate-lifecycles", registry_path=registry_path, game=game).get("rows", [])
    hook_rows = query_clip_registry(mode="hook-candidates", registry_path=registry_path, game=game).get("rows", [])
    export_rows = query_clip_registry(mode="highlight-exports", registry_path=registry_path, game=game).get("rows", [])
    post_rows = query_clip_registry(mode="post-ledger-records", registry_path=registry_path, game=game, platform=platform).get("rows", [])
    metric_rows = query_clip_registry(mode="posted-metrics", registry_path=registry_path, game=game, platform=platform, evidence_mode="real_only").get("rows", [])
    eligible_metric_rows = [row for row in metric_rows if bool(row.get("post_performance_label_eligible"))]

    warnings = list(refresh_result.get("warnings", [])) if isinstance(refresh_result.get("warnings"), list) else []
    unresolved_lineage_counts = _unresolved_lineage_counts(warnings)
    source_root_summaries = [_source_root_summary(source_root, platform=platform) for source_root in source_roots]
    coverage_inventory = {
        "candidate_lifecycle_row_count": len(candidate_rows),
        "imported_candidate_count": len(candidate_rows),
        "imported_hook_count": len(hook_rows),
        "imported_export_count": len(export_rows),
        "imported_post_count": len(post_rows),
        "imported_posted_metrics_row_count": len(metric_rows),
        "eligible_real_post_performance_label_count": len(eligible_metric_rows),
        "selected_event_type_counts": _selected_highlight_field_counts(metric_rows, "event_type"),
        "selected_producer_family_counts": _selected_highlight_list_counts(metric_rows, "contributing_producer_families"),
        "workspace_root": str(workspace_root.resolve()),
        "game": game,
        "platform": platform,
    }
    return {
        "imported_counts": imported_counts,
        "coverage_inventory": coverage_inventory,
        "source_root_summaries": source_root_summaries,
        "unresolved_lineage_counts": unresolved_lineage_counts,
        "warnings": warnings,
    }


def _selected_highlight_details(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("selected_highlight_details_json")
    if not isinstance(payload, str) or not payload.strip():
        return {}
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _selected_highlight_field_counts(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in _dedupe_metric_rows_by_post_record_id(rows):
        details = _selected_highlight_details(row)
        value = str(details.get(field) or "").strip()
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _selected_highlight_list_counts(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in _dedupe_metric_rows_by_post_record_id(rows):
        details = _selected_highlight_details(row)
        values = details.get(field)
        if not isinstance(values, list):
            continue
        seen: set[str] = set()
        for value in values:
            normalized = str(value or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _dedupe_metric_rows_by_post_record_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique_rows: list[dict[str, Any]] = []
    seen_post_record_ids: set[str] = set()
    for row in rows:
        post_record_id = str(row.get("post_record_id") or "").strip()
        if post_record_id:
            if post_record_id in seen_post_record_ids:
                continue
            seen_post_record_ids.add(post_record_id)
        unique_rows.append(row)
    return unique_rows


def _source_root_summary(source_root: Path, *, platform: str | None) -> dict[str, Any]:
    source_records = _discover_artifacts(
        source_root,
        suffixes=_SOURCE_ARTIFACT_SUFFIXES,
        platform=platform,
        exclude_synthetic=True,
    )
    return {
        "source_root": str(source_root.resolve()),
        "artifact_count": len(source_records),
        "artifact_counts": _artifact_counts(source_records),
    }


def _unresolved_lineage_counts(warnings: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "candidate": 0,
        "hook": 0,
        "export": 0,
        "post": 0,
        "metrics": 0,
        "other": 0,
    }
    for warning in warnings:
        detail = str(warning.get("detail") or "").lower()
        if "candidate" in detail:
            counts["candidate"] += 1
        elif "hook" in detail:
            counts["hook"] += 1
        elif "export" in detail:
            counts["export"] += 1
        elif "post" in detail or "ledger" in detail:
            counts["post"] += 1
        elif "metric" in detail or "snapshot" in detail:
            counts["metrics"] += 1
        else:
            counts["other"] += 1
    return counts
