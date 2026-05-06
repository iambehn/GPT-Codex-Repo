from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.fused_export import DEFAULT_ACTION_THRESHOLDS
from pipeline.highlight_selection_export import HIGHLIGHT_SELECTION_SCHEMA_VERSION


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REGISTRY_PATH = REPO_ROOT / "outputs" / "registry" / "clip_event_registry.sqlite"
RUNTIME_REVIEW_SESSION_SCHEMA_VERSION = "runtime_review_session_v1"
FUSED_REVIEW_SESSION_SCHEMA_VERSION = "fused_review_session_v1"
PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"
FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"
FIXTURE_SIDECAR_COMPARISON_SCHEMA_VERSION = "fixture_sidecar_comparison_v1"
FIXTURE_TRIAL_RUN_SCHEMA_VERSION = "fixture_trial_run_v1"
FIXTURE_TRIAL_BATCH_SCHEMA_VERSION = "fixture_trial_batch_v1"
HOOK_CANDIDATE_SCHEMA_VERSION = "hook_candidate_v1"
WORKFLOW_RUN_SCHEMA_VERSION = "workflow_run_v1"
HOOK_CANDIDATE_COMPARISON_SCHEMA_VERSION = "hook_candidate_comparison_v1"
HOOK_EVALUATION_REPORT_SCHEMA_VERSION = "hook_evaluation_report_v1"
HIGHLIGHT_EXPORT_BATCH_SCHEMA_VERSION = "highlight_export_batch_v1"
POSTED_HIGHLIGHT_LEDGER_SCHEMA_VERSION = "posted_highlight_ledger_v1"
POSTED_HIGHLIGHT_METRICS_SNAPSHOT_SCHEMA_VERSION = "posted_highlight_metrics_snapshot_v1"
SHADOW_RANKING_REPLAY_SCHEMA_VERSION = "shadow_ranking_replay_v1"
SHADOW_RANKING_COMPARISON_SCHEMA_VERSION = "shadow_ranking_comparison_v1"
SHADOW_RANKING_MODEL_SCHEMA_VERSION = "shadow_ranking_model_v1"
SHADOW_RANKING_EXPERIMENT_SCHEMA_VERSION = "shadow_ranking_experiment_v1"
SHADOW_EVALUATION_POLICY_SCHEMA_VERSION = "shadow_evaluation_policy_v1"
SHADOW_EXPERIMENT_LEDGER_SCHEMA_VERSION = "shadow_experiment_ledger_v1"
SHADOW_MODEL_FAMILY_COMPARISON_SCHEMA_VERSION = "shadow_model_family_comparison_v1"
SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION = "shadow_benchmark_matrix_v1"
SHADOW_BENCHMARK_REVIEW_SCHEMA_VERSION = "shadow_benchmark_review_v1"
REAL_POSTED_LINEAGE_IMPORT_SCHEMA_VERSION = "real_posted_lineage_import_v1"
SHADOW_BENCHMARK_EVIDENCE_COMPARISON_SCHEMA_VERSION = "shadow_benchmark_evidence_comparison_v1"
REAL_ARTIFACT_INTAKE_DASHBOARD_SCHEMA_VERSION = "real_artifact_intake_dashboard_v1"
CANDIDATE_LIFECYCLE_STATES = (
    "pending_review",
    "approved",
    "rejected",
    "selected_for_export",
    "exported",
    "posted",
    "superseded",
    "invalidated",
)
_TERMINAL_LIFECYCLE_STATES = {"rejected", "superseded", "invalidated"}
_MANUAL_PRESERVE_LIFECYCLE_STATES = {"selected_for_export", "exported", "posted", "superseded", "invalidated"}
_VALID_LIFECYCLE_TRANSITIONS = {
    "pending_review": {"approved", "rejected", "invalidated"},
    "approved": {"selected_for_export", "superseded", "invalidated"},
    "selected_for_export": {"exported", "superseded", "invalidated"},
    "exported": {"posted", "superseded", "invalidated"},
    "posted": {"superseded", "invalidated"},
    "rejected": {"invalidated"},
    "superseded": set(),
    "invalidated": set(),
}
_LIFECYCLE_PROGRESS_ORDER = {
    "pending_review": 0,
    "approved": 1,
    "selected_for_export": 2,
    "exported": 3,
    "posted": 4,
}


def refresh_clip_registry(
    refresh_root: str | Path,
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    registry_path: str | Path | None = None,
) -> dict[str, Any]:
    root = _resolve_path(refresh_root)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "refresh_root": str(root),
            "error": "refresh root does not exist or is not a directory",
        }

    registry = _resolve_registry_path(registry_path)
    registry.parent.mkdir(parents=True, exist_ok=True)

    collected = _collect_registry_rows(root, game=game)
    result = _write_registry(registry, root, collected, game=game)

    warnings_path = None
    if debug_output_dir is not None:
        debug_root = _resolve_path(debug_output_dir)
        debug_root.mkdir(parents=True, exist_ok=True)
        warnings_path = debug_root / "ingest_warnings.json"
        warnings_path.write_text(json.dumps(collected["warnings"], indent=2), encoding="utf-8")
        result["warnings_path"] = str(warnings_path)

    if output_path is not None:
        output = _resolve_path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2), encoding="utf-8")

    return result


def query_clip_registry(
    *,
    mode: str = "fused-events",
    game: str | None = None,
    event_type: str | None = None,
    action: str | None = None,
    review_status: str | None = None,
    gate_status: str | None = None,
    fixture_id: str | None = None,
    trial_name: str | None = None,
    artifact_layer: str | None = None,
    recommendation_decision: str | None = None,
    coverage_status: str | None = None,
    has_disagreement: bool | None = None,
    candidate_id: str | None = None,
    lifecycle_state: str | None = None,
    hook_archetype: str | None = None,
    hook_mode: str | None = None,
    comparison_status: str | None = None,
    export_status: str | None = None,
    post_status: str | None = None,
    platform: str | None = None,
    account_id: str | None = None,
    evidence_mode: str | None = None,
    model_family: str | None = None,
    training_target: str | None = None,
    workflow_type: str | None = None,
    workflow_run_id: str | None = None,
    stage: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    registry_path: str | Path | None = None,
) -> dict[str, Any]:
    registry = _resolve_registry_path(registry_path)
    if not registry.exists() or not registry.is_file():
        return {
            "ok": False,
            "registry_path": str(registry),
            "error": "registry database does not exist",
        }

    normalized_mode = str(mode or "fused-events").strip().lower()
    normalized_review = str(review_status).strip().lower() if review_status is not None else None
    if normalized_mode not in {
        "clips",
        "proxy-windows",
        "runtime-events",
        "fused-events",
        "review-items",
        "fixture-comparisons",
        "fixture-trials",
        "batch-comparisons",
        "candidate-lifecycles",
        "hook-candidates",
        "hook-comparisons",
        "hook-evaluation-reports",
        "hook-quality-rollups",
        "highlight-exports",
        "post-ledger-records",
        "posted-metrics",
        "posted-performance-rollups",
        "shadow-ranking-models",
        "shadow-ranking-experiments",
        "shadow-evaluation-policies",
        "shadow-ranking-experiment-ledgers",
        "shadow-ranking-experiment-slices",
        "shadow-ranking-replays",
        "shadow-ranking-comparisons",
        "shadow-model-family-comparisons",
        "shadow-benchmark-matrices",
        "shadow-benchmark-runs",
        "shadow-benchmark-reviews",
        "shadow-target-readiness",
        "real-posted-lineage-imports",
        "shadow-benchmark-evidence-comparisons",
        "real-artifact-intake-dashboards",
        "workflow-runs",
    }:
        return {
            "ok": False,
            "registry_path": str(registry),
            "error": f"unsupported query mode: {mode}",
        }

    connection = sqlite3.connect(str(registry))
    connection.row_factory = sqlite3.Row
    try:
        rows = _query_rows(
            connection,
            mode=normalized_mode,
            game=game,
            event_type=event_type,
            action=action,
            review_status=normalized_review,
            gate_status=gate_status,
            fixture_id=fixture_id,
            trial_name=trial_name,
            artifact_layer=artifact_layer,
            recommendation_decision=recommendation_decision,
            coverage_status=coverage_status,
            has_disagreement=has_disagreement,
            candidate_id=candidate_id,
            lifecycle_state=lifecycle_state,
            hook_archetype=hook_archetype,
            hook_mode=hook_mode,
            comparison_status=comparison_status,
            export_status=export_status,
            post_status=post_status,
            platform=platform,
            account_id=account_id,
            evidence_mode=evidence_mode,
            model_family=model_family,
            training_target=training_target,
            workflow_type=workflow_type,
            workflow_run_id=workflow_run_id,
            stage=stage,
            status=status,
            limit=limit,
        )
    finally:
        connection.close()

    if normalized_mode == "posted-metrics":
        latest_by_post_record = _latest_metrics_timestamp_by_post_record(rows)
        for row in rows:
            post_record_id = str(row.get("post_record_id") or "").strip()
            captured_at = str(row.get("captured_at") or "").strip()
            row["is_latest_snapshot"] = bool(post_record_id and captured_at and latest_by_post_record.get(post_record_id) == captured_at)
            row["metrics_coverage_status"] = _metrics_coverage_status([row])
            row.update(_post_performance_label_fields(row))
            row["evidence_mode"] = _posted_metrics_evidence_mode(row)
        if evidence_mode is not None:
            rows = [row for row in rows if str(row.get("evidence_mode") or "") == evidence_mode]
    elif normalized_mode == "shadow-target-readiness" and evidence_mode is not None:
        rows = [row for row in rows if evidence_mode in _load_json_list(row.get("evidence_modes_json"))]

    return {
        "ok": True,
        "registry_path": str(registry),
        "mode": normalized_mode,
        "query_shape": "aggregate" if normalized_mode in {"posted-performance-rollups", "hook-quality-rollups"} else "rows",
        "row_count": len(rows),
        "rows": rows,
    }


def transition_candidate_lifecycle(
    candidate_id: str,
    to_state: str,
    *,
    reason: str | None = None,
    source_artifact: str | Path | None = None,
    actor: str | None = None,
    registry_path: str | Path | None = None,
) -> dict[str, Any]:
    registry = _resolve_registry_path(registry_path)
    if not registry.exists() or not registry.is_file():
        return {
            "ok": False,
            "registry_path": str(registry),
            "error": "registry database does not exist",
        }

    normalized_candidate_id = str(candidate_id or "").strip()
    normalized_to_state = str(to_state or "").strip().lower()
    if not normalized_candidate_id:
        return {
            "ok": False,
            "registry_path": str(registry),
            "error": "candidate_id is required",
        }
    if normalized_to_state not in CANDIDATE_LIFECYCLE_STATES:
        return {
            "ok": False,
            "registry_path": str(registry),
            "error": f"unsupported lifecycle state: {to_state}",
        }

    connection = sqlite3.connect(str(registry))
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        _create_schema(connection)
        with connection:
            current_row = connection.execute(
                "SELECT * FROM candidate_lifecycles WHERE candidate_id = ?",
                (normalized_candidate_id,),
            ).fetchone()
            if current_row is None:
                return {
                    "ok": False,
                    "registry_path": str(registry),
                    "candidate_id": normalized_candidate_id,
                    "error": "candidate lifecycle does not exist",
                }
            current_state = str(current_row["lifecycle_state"] or "").strip().lower()
            if current_state == normalized_to_state:
                return {
                    "ok": True,
                    "registry_path": str(registry),
                    "candidate_id": normalized_candidate_id,
                    "from_state": current_state,
                    "to_state": normalized_to_state,
                    "transition_applied": False,
                    "message": "candidate already in requested lifecycle state",
                }
            allowed = _VALID_LIFECYCLE_TRANSITIONS.get(current_state, set())
            if normalized_to_state not in allowed:
                return {
                    "ok": False,
                    "registry_path": str(registry),
                    "candidate_id": normalized_candidate_id,
                    "from_state": current_state,
                    "to_state": normalized_to_state,
                    "error": "invalid lifecycle transition",
                }
            _append_candidate_lifecycle_transition(
                connection,
                candidate_id=normalized_candidate_id,
                from_state=current_state,
                to_state=normalized_to_state,
                reason=reason,
                transition_source="manual_registry_transition",
                actor=actor or "operator",
                source_artifact_path=str(_resolve_path(source_artifact)) if source_artifact is not None else None,
                metadata={"manual": True},
            )
            updated_at = _utc_now()
            connection.execute(
                """
                UPDATE candidate_lifecycles
                SET lifecycle_state = ?, updated_at = ?
                WHERE candidate_id = ?
                """,
                (normalized_to_state, updated_at, normalized_candidate_id),
            )
    finally:
        connection.close()

    return {
        "ok": True,
        "registry_path": str(registry),
        "candidate_id": normalized_candidate_id,
        "from_state": current_state,
        "to_state": normalized_to_state,
        "transition_applied": True,
    }


def load_candidate_lifecycle_details(
    *,
    game: str | None = None,
    source: str | None = None,
    fused_sidecar_path: str | Path | None = None,
    candidate_id: str | None = None,
    registry_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    registry = _resolve_registry_path(registry_path)
    if not registry.exists() or not registry.is_file():
        return []
    connection = sqlite3.connect(str(registry))
    connection.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT l.candidate_id, l.game, l.source, l.fixture_id, l.event_id, l.fused_sidecar_path,
                   l.lifecycle_state, l.latest_review_status, l.recommended_action, l.final_score,
                   l.has_review_disagreement, l.has_cross_layer_disagreement, l.has_trial_preference,
                   l.selection_basis,
                   l.highlight_selection_manifest_path, l.selected_highlight_details_json,
                   l.export_artifact_path, l.post_ledger_path,
                   l.created_at, l.updated_at, l.last_seen_at,
                   (
                       SELECT json_group_array(
                           json_object(
                               'transition_id', t.transition_id,
                               'from_state', t.from_state,
                               'to_state', t.to_state,
                               'transition_reason', t.transition_reason,
                               'transition_source', t.transition_source,
                               'actor', t.actor,
                               'source_artifact_path', t.source_artifact_path,
                               'created_at', t.created_at,
                               'metadata_json', t.metadata_json
                           )
                       )
                       FROM candidate_lifecycle_transitions t
                       WHERE t.candidate_id = l.candidate_id
                       ORDER BY t.transition_id ASC
                   ) AS transitions_json
            FROM candidate_lifecycles l
        """
        where = []
        params: list[Any] = []
        if game is not None:
            where.append("l.game = ?")
            params.append(game)
        if source is not None:
            where.append("l.source = ?")
            params.append(source)
        if fused_sidecar_path is not None:
            where.append("l.fused_sidecar_path = ?")
            params.append(str(_resolve_path(fused_sidecar_path)))
        if candidate_id is not None:
            where.append("l.candidate_id = ?")
            params.append(candidate_id)
        rows = _execute_query(connection, sql, where, params, order_by="l.updated_at DESC, l.candidate_id ASC", limit=None)
    finally:
        connection.close()
    return rows


def load_hook_candidate_details(
    *,
    game: str | None = None,
    source: str | None = None,
    fused_sidecar_path: str | Path | None = None,
    candidate_id: str | None = None,
    registry_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    registry = _resolve_registry_path(registry_path)
    if not registry.exists() or not registry.is_file():
        return []
    connection = sqlite3.connect(str(registry))
    connection.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT h.hook_id, h.candidate_id, h.event_id, h.game, h.source, h.fixture_id, h.fused_sidecar_path,
                   h.lifecycle_state, h.hook_archetype, h.hook_mode, h.hook_strength,
                   h.intensity_score, h.clarity_score, h.novelty_score, h.context_sufficiency_score,
                   h.payoff_readability_score, h.title_thumbnail_potential_score, h.authenticity_risk_score,
                   h.sound_off_legibility_score, h.packaging_strategy, h.rejection_reason,
                   h.highlight_selection_manifest_path, h.metadata_summary_json, h.created_at
            FROM hook_candidates h
        """
        where = []
        params: list[Any] = []
        if game is not None:
            where.append("h.game = ?")
            params.append(game)
        if source is not None:
            where.append("h.source = ?")
            params.append(source)
        if fused_sidecar_path is not None:
            where.append("h.fused_sidecar_path = ?")
            params.append(str(_resolve_path(fused_sidecar_path)))
        if candidate_id is not None:
            where.append("h.candidate_id = ?")
            params.append(candidate_id)
        return _execute_query(connection, sql, where, params, order_by="COALESCE(h.hook_strength, 0.0) DESC, h.hook_id ASC", limit=None)
    finally:
        connection.close()
    return rows


def load_workflow_run_details(
    *,
    workflow_run_id: str | None = None,
    workflow_type: str | None = None,
    candidate_id: str | None = None,
    registry_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    registry = _resolve_registry_path(registry_path)
    if not registry.exists() or not registry.is_file():
        return []
    connection = sqlite3.connect(str(registry))
    connection.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT i.workflow_run_id, r.workflow_type, r.stage, r.status AS run_status, r.manifest_path,
                   i.item_index, i.item_status, i.candidate_id, i.game, i.source, i.fixture_id,
                   i.event_id, i.lifecycle_state, i.fused_sidecar_path,
                   i.highlight_selection_manifest_path, i.export_artifact_path, i.post_ledger_path,
                   i.hook_manifest_path, i.created_at
            FROM workflow_run_items i
            JOIN workflow_runs r ON r.workflow_run_id = i.workflow_run_id
        """
        where = []
        params: list[Any] = []
        if workflow_run_id is not None:
            where.append("i.workflow_run_id = ?")
            params.append(str(workflow_run_id).strip())
        if workflow_type is not None:
            where.append("r.workflow_type = ?")
            params.append(str(workflow_type).strip())
        if candidate_id is not None:
            where.append("i.candidate_id = ?")
            params.append(str(candidate_id).strip())
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY i.workflow_run_id ASC, i.item_index ASC"
        return [dict(row) for row in connection.execute(sql, params).fetchall()]
    finally:
        connection.close()


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _resolve_registry_path(path: str | Path | None) -> Path:
    if path is None:
        return DEFAULT_REGISTRY_PATH
    return _resolve_path(path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _collect_registry_rows(root: Path, *, game: str | None) -> dict[str, Any]:
    rows: dict[str, Any] = {
        "clips": {},
        "proxy_windows": [],
        "runtime_analyses": [],
        "runtime_events": [],
        "runtime_detections": [],
        "fused_analyses": [],
        "fused_events": [],
        "fused_signal_refs": [],
        "runtime_review_sessions": [],
        "runtime_review_items": [],
        "fused_review_sessions": [],
        "fused_review_items": [],
        "fixture_comparisons": [],
        "fixture_trial_runs": [],
        "fixture_trial_run_fixtures": [],
        "fixture_trial_batches": [],
        "fixture_trial_batch_comparisons": [],
        "highlight_selection_manifests": [],
        "hook_candidate_manifests": [],
        "hook_candidates": [],
        "hook_comparison_reports": [],
        "hook_comparisons": [],
        "hook_evaluation_reports": [],
        "highlight_export_batches": [],
        "highlight_exports": [],
        "post_ledgers": [],
        "posted_highlights": [],
        "posted_metrics_snapshots": [],
        "posted_metrics_snapshot_rows": [],
        "shadow_evaluation_policies": [],
        "shadow_ranking_models": [],
        "shadow_ranking_experiments": [],
        "shadow_ranking_experiment_ledgers": [],
        "shadow_ranking_experiment_slices": [],
        "shadow_ranking_replays": [],
        "shadow_ranking_replay_rows": [],
        "shadow_ranking_comparisons": [],
        "shadow_model_family_comparisons": [],
        "shadow_benchmark_matrices": [],
        "shadow_benchmark_runs": [],
        "shadow_benchmark_reviews": [],
        "shadow_target_readiness": [],
        "real_posted_lineage_imports": [],
        "shadow_benchmark_evidence_comparisons": [],
        "real_artifact_intake_dashboards": [],
        "workflow_runs": [],
        "workflow_run_items": [],
        "warnings": [],
        "counts": {
            "proxy_sidecar_count": 0,
            "runtime_sidecar_count": 0,
            "fused_sidecar_count": 0,
            "runtime_review_session_count": 0,
            "fused_review_session_count": 0,
            "fixture_comparison_report_count": 0,
            "fixture_trial_run_manifest_count": 0,
            "fixture_trial_batch_manifest_count": 0,
            "highlight_selection_manifest_count": 0,
            "hook_candidate_manifest_count": 0,
            "hook_comparison_report_count": 0,
            "highlight_export_batch_manifest_count": 0,
            "post_ledger_manifest_count": 0,
            "posted_metrics_snapshot_manifest_count": 0,
            "shadow_evaluation_policy_manifest_count": 0,
            "shadow_ranking_model_manifest_count": 0,
            "shadow_ranking_experiment_manifest_count": 0,
            "shadow_ranking_experiment_ledger_manifest_count": 0,
            "shadow_ranking_replay_manifest_count": 0,
            "shadow_ranking_comparison_report_count": 0,
            "shadow_model_family_comparison_manifest_count": 0,
            "shadow_benchmark_matrix_manifest_count": 0,
            "shadow_benchmark_review_manifest_count": 0,
            "real_posted_lineage_import_manifest_count": 0,
            "shadow_benchmark_evidence_comparison_manifest_count": 0,
            "real_artifact_intake_dashboard_manifest_count": 0,
            "workflow_run_manifest_count": 0,
        },
    }

    for path in sorted(root.rglob("*.proxy_scan.json")):
        _ingest_proxy_sidecar(path, rows, game=game)
    for path in sorted(root.rglob("*.runtime_analysis.json")):
        _ingest_runtime_sidecar(path, rows, game=game)
    for path in sorted(root.rglob("*.fused_analysis.json")):
        _ingest_fused_sidecar(path, rows, game=game)
    for path in sorted(root.rglob("*.runtime_review_session.json")):
        _ingest_runtime_review_session(path, rows, game=game)
    for path in sorted(root.rglob("*.fused_review_session.json")):
        _ingest_fused_review_session(path, rows, game=game)
    for path in sorted(root.rglob("fixture_trial_run_manifest.json")):
        _ingest_fixture_trial_run_manifest(path, rows, game=game)
    for path in sorted(root.rglob("fixture_trial_batch_manifest.json")):
        _ingest_fixture_trial_batch_manifest(path, rows, game=game)
    for path in sorted(root.rglob("*.highlight_selection.json")):
        _ingest_highlight_selection_manifest(path, rows, game=game)
    for path in sorted(root.rglob("*.hook_candidates.json")):
        _ingest_hook_candidate_manifest(path, rows, game=game)
    for path in sorted(root.rglob("*hook_comparison.json")):
        _ingest_hook_comparison_report(path, rows, game=game)
    for path in sorted(root.rglob("*.hook_evaluation_report.json")):
        _ingest_hook_evaluation_report(path, rows, game=game)
    for path in sorted(root.rglob("*.highlight_export_batch.json")):
        _ingest_highlight_export_batch_manifest(path, rows, game=game)
    for path in sorted(root.rglob("*.posted_highlight_ledger.json")):
        _ingest_posted_highlight_ledger(path, rows, game=game)
    for path in sorted(root.rglob("*.posted_highlight_metrics_snapshot.json")):
        _ingest_posted_metrics_snapshot(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_evaluation_policy.json")):
        _ingest_shadow_evaluation_policy(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_ranking_model.json")):
        _ingest_shadow_ranking_model(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_ranking_experiment.json")):
        _ingest_shadow_ranking_experiment(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_experiment_ledger.json")):
        _ingest_shadow_ranking_experiment_ledger(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_ranking_replay.json")):
        _ingest_shadow_ranking_replay(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_ranking_comparison.json")):
        _ingest_shadow_ranking_comparison(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_model_family_comparison.json")):
        _ingest_shadow_model_family_comparison(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_benchmark_matrix.json")):
        _ingest_shadow_benchmark_matrix(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_benchmark_review.json")):
        _ingest_shadow_benchmark_review(path, rows, game=game)
    for path in sorted(root.rglob("*.real_posted_lineage_import.json")):
        _ingest_real_posted_lineage_import(path, rows, game=game)
    for path in sorted(root.rglob("*.shadow_benchmark_evidence_comparison.json")):
        _ingest_shadow_benchmark_evidence_comparison(path, rows, game=game)
    for path in sorted(root.rglob("*real_artifact_intake*.dashboard.json")):
        _ingest_real_artifact_intake_dashboard(path, rows, game=game)
    for path in sorted(root.rglob("*.workflow_run.json")):
        _ingest_workflow_run_manifest(path, rows, game=game)
    for path in sorted(root.rglob("*.json")):
        if path.name in {
            "fixture_trial_run_manifest.json",
            "fixture_trial_batch_manifest.json",
        } or path.name.endswith(".highlight_selection.json") or path.name.endswith(".hook_candidates.json") or path.name.endswith("hook_comparison.json") or path.name.endswith(".hook_evaluation_report.json") or path.name.endswith(".highlight_export_batch.json") or path.name.endswith(".posted_highlight_ledger.json") or path.name.endswith(".posted_highlight_metrics_snapshot.json") or path.name.endswith(".shadow_evaluation_policy.json") or path.name.endswith(".shadow_ranking_model.json") or path.name.endswith(".shadow_ranking_experiment.json") or path.name.endswith(".shadow_experiment_ledger.json") or path.name.endswith(".shadow_ranking_replay.json") or path.name.endswith(".shadow_ranking_comparison.json") or path.name.endswith(".shadow_model_family_comparison.json") or path.name.endswith(".shadow_benchmark_matrix.json") or path.name.endswith(".shadow_benchmark_review.json") or path.name.endswith(".real_posted_lineage_import.json") or path.name.endswith(".shadow_benchmark_evidence_comparison.json") or path.name.endswith(".workflow_run.json") or path.name.endswith("real_artifact_intake.dashboard.json"):
            continue
        _ingest_fixture_comparison_report(path, rows, game=game)

    clip_rows = []
    for clip in rows["clips"].values():
        clip_rows.append(clip)
    rows["clips"] = clip_rows
    return rows


def _warning(rows: dict[str, Any], *, path: Path, reason: str, detail: str | None = None) -> None:
    payload = {"path": str(path.resolve()), "reason": reason}
    if detail:
        payload["detail"] = detail
    rows["warnings"].append(payload)


def _warn_missing_fields(rows: dict[str, Any], *, path: Path, artifact_type: str, missing_fields: list[str], row_index: int | None = None) -> None:
    detail = f"{artifact_type} missing required fields: {', '.join(sorted(missing_fields))}"
    if row_index is not None:
        detail = f"{detail} (row_index={row_index})"
    _warning(rows, path=path, reason="missing_required_fields", detail=detail)


def _load_json(path: Path, rows: dict[str, Any]) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _warning(rows, path=path, reason="malformed_json", detail=str(exc))
        return None


def _clip_row(rows: dict[str, Any], *, game: str, source: str) -> dict[str, Any]:
    key = (game, source)
    clip = rows["clips"].get(key)
    if clip is None:
        clip = {
            "game": game,
            "source": source,
            "proxy_sidecar_path": None,
            "runtime_sidecar_path": None,
            "fused_sidecar_path": None,
            "has_proxy_sidecar": 0,
            "has_runtime_sidecar": 0,
            "has_fused_sidecar": 0,
            "proxy_review_status": None,
            "runtime_review_status": None,
            "fused_review_status": None,
            "fixture_ids_json": "[]",
            "top_proxy_action": None,
            "top_proxy_score": None,
            "top_fused_action": None,
            "top_fused_score": None,
            "has_review_disagreement": 0,
            "has_cross_layer_disagreement": 0,
            "has_trial_preference": 0,
            "last_seen_at": _utc_now(),
        }
        rows["clips"][key] = clip
    else:
        clip["last_seen_at"] = _utc_now()
    return clip


def _clip_add_fixture_id(clip: dict[str, Any], fixture_id: str) -> None:
    text = str(fixture_id or "").strip()
    if not text:
        return
    existing = set(_load_json_list(clip.get("fixture_ids_json")))
    if text in existing:
        return
    existing.add(text)
    clip["fixture_ids_json"] = json.dumps(sorted(existing))


def _update_clip_disagreement_flags(clip: dict[str, Any]) -> None:
    statuses = {
        str(clip.get("proxy_review_status") or "").strip().lower(),
        str(clip.get("runtime_review_status") or "").strip().lower(),
        str(clip.get("fused_review_status") or "").strip().lower(),
    }
    statuses.discard("")
    clip["has_review_disagreement"] = int(len(statuses) > 1)
    clip["has_cross_layer_disagreement"] = int(len(statuses) > 1)


def _load_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload if str(item).strip()]


def _load_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _ingest_proxy_sidecar(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != PROXY_SCAN_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if not payload.get("ok", False):
        _warning(rows, path=path, reason="failed_analysis")
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    source = str(payload.get("source") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    if source and payload_game:
        clip = _clip_row(rows, game=payload_game, source=source)
        clip["proxy_sidecar_path"] = resolved_path
        clip["has_proxy_sidecar"] = 1
        review = payload.get("proxy_review", {}) if isinstance(payload.get("proxy_review"), dict) else {}
        clip["proxy_review_status"] = _normalized_review_status(review.get("review_status"))
        top_window = payload.get("windows", [{}])[0] if isinstance(payload.get("windows"), list) and payload.get("windows") else {}
        clip["top_proxy_action"] = top_window.get("recommended_action")
        clip["top_proxy_score"] = top_window.get("proxy_score")
        _update_clip_disagreement_flags(clip)

    review = payload.get("proxy_review", {}) if isinstance(payload.get("proxy_review"), dict) else {}
    for index, window in enumerate(payload.get("windows", [])):
        rows["proxy_windows"].append(
            {
                "scan_id": payload.get("scan_id"),
                "window_index": index,
                "game": payload_game,
                "source": source,
                "sidecar_path": resolved_path,
                "start_seconds": window.get("start_seconds"),
                "end_seconds": window.get("end_seconds"),
                "proxy_score": window.get("proxy_score"),
                "signal_count": window.get("signal_count"),
                "recommended_action": window.get("recommended_action"),
                "sources_json": json.dumps(window.get("sources", []), sort_keys=True),
                "source_families_json": json.dumps(window.get("source_families", []), sort_keys=True),
                "review_status": _normalized_review_status(review.get("review_status")),
            }
        )
    rows["counts"]["proxy_sidecar_count"] += 1


def _ingest_runtime_sidecar(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != RUNTIME_ANALYSIS_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if not payload.get("ok", False):
        _warning(rows, path=path, reason="failed_analysis")
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    source = str(payload.get("source") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    analysis_id = str(payload.get("analysis_id") or "").strip()
    matcher = payload.get("matcher", {}) if isinstance(payload.get("matcher"), dict) else {}
    events = payload.get("events", {}) if isinstance(payload.get("events"), dict) else {}
    review = payload.get("runtime_review", {}) if isinstance(payload.get("runtime_review"), dict) else {}
    review_status = _normalized_review_status(review.get("review_status"))

    if source and payload_game:
        clip = _clip_row(rows, game=payload_game, source=source)
        clip["runtime_sidecar_path"] = resolved_path
        clip["has_runtime_sidecar"] = 1
        clip["runtime_review_status"] = review_status
        _update_clip_disagreement_flags(clip)

    rows["runtime_analyses"].append(
        {
            "analysis_id": analysis_id,
            "game": payload_game,
            "source": source,
            "sidecar_path": resolved_path,
            "status": payload.get("status"),
            "frame_count": matcher.get("frame_count"),
            "confirmed_detection_count": len(list(matcher.get("confirmed_detections", []))),
            "event_count": events.get("event_count", len(list(events.get("rows", [])))),
            "runtime_review_status": review_status,
            "runtime_review_session_id": review.get("session_id"),
            "runtime_recommended_action": review.get("recommended_action"),
            "runtime_highlight_score": review.get("highlight_score"),
            "last_ingested_mtime": path.stat().st_mtime,
        }
    )

    for index, event_row in enumerate(events.get("rows", [])):
        rows["runtime_events"].append(
            {
                "analysis_id": analysis_id,
                "event_index": index,
                "event_id": event_row.get("event_id"),
                "game": payload_game,
                "source": source,
                "sidecar_path": resolved_path,
                "event_type": event_row.get("event_type"),
                "confidence": event_row.get("confidence"),
                "start_timestamp": event_row.get("start_timestamp"),
                "end_timestamp": event_row.get("end_timestamp"),
                "entity_id": event_row.get("entity_id"),
                "ability_id": event_row.get("ability_id"),
                "equipment_id": event_row.get("equipment_id"),
                "event_row_id": event_row.get("event_row_id"),
                "review_status": review_status,
                "recommended_action": review.get("recommended_action"),
            }
        )

    for index, detection_row in enumerate(matcher.get("confirmed_detections", [])):
        rows["runtime_detections"].append(
            {
                "analysis_id": analysis_id,
                "detection_index": index,
                "game": payload_game,
                "source": source,
                "sidecar_path": resolved_path,
                "asset_id": detection_row.get("asset_id"),
                "roi_ref": detection_row.get("roi_ref"),
                "entity_id": detection_row.get("entity_id"),
                "ability_id": detection_row.get("ability_id"),
                "equipment_id": detection_row.get("equipment_id"),
                "first_timestamp": detection_row.get("first_timestamp"),
                "last_timestamp": detection_row.get("last_timestamp"),
                "peak_score": detection_row.get("peak_score"),
            }
        )
    rows["counts"]["runtime_sidecar_count"] += 1


def _ingest_fused_sidecar(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != FUSED_ANALYSIS_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if not payload.get("ok", False):
        _warning(rows, path=path, reason="failed_analysis")
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    source = str(payload.get("source") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    fusion_id = str(payload.get("fusion_id") or "").strip()
    fused_review = payload.get("fused_review", {}) if isinstance(payload.get("fused_review"), dict) else {}
    review_events = fused_review.get("events", {}) if isinstance(fused_review.get("events"), dict) else {}

    if source and payload_game:
        clip = _clip_row(rows, game=payload_game, source=source)
        clip["fused_sidecar_path"] = resolved_path
        clip["has_fused_sidecar"] = 1
        clip["fused_review_status"] = _top_fused_review_status(review_events)
        top_event = payload.get("fused_events", [{}])[0] if isinstance(payload.get("fused_events"), list) and payload.get("fused_events") else {}
        clip["top_fused_action"] = _recommended_fused_action(float(top_event.get("final_score", top_event.get("confidence", 0.0)) or 0.0)) if top_event else None
        clip["top_fused_score"] = top_event.get("final_score", top_event.get("confidence")) if top_event else None
        _update_clip_disagreement_flags(clip)

    rows["fused_analyses"].append(
        {
            "fusion_id": fusion_id,
            "game": payload_game,
            "source": source,
            "sidecar_path": resolved_path,
            "status": payload.get("status"),
            "normalized_signal_count": len(list(payload.get("normalized_signals", []))),
            "fused_event_count": len(list(payload.get("fused_events", []))),
            "fused_reviewed_event_count": fused_review.get("reviewed_event_count"),
            "fused_review_session_id": fused_review.get("session_id"),
            "fused_review_status": _top_fused_review_status(review_events),
            "last_ingested_mtime": path.stat().st_mtime,
        }
    )

    signal_by_id = {
        str(signal_row.get("signal_id")): signal_row
        for signal_row in payload.get("normalized_signals", [])
        if str(signal_row.get("signal_id") or "").strip()
    }

    for index, event_row in enumerate(payload.get("fused_events", [])):
        event_id = str(event_row.get("event_id") or "").strip()
        metadata = event_row.get("metadata", {}) if isinstance(event_row.get("metadata"), dict) else {}
        event_review = review_events.get(event_id, {}) if isinstance(review_events.get(event_id), dict) else {}
        final_score = float(event_row.get("final_score", event_row.get("confidence", 0.0)) or 0.0)
        recommended_action = _recommended_fused_action(final_score)
        rows["fused_events"].append(
            {
                "fusion_id": fusion_id,
                "event_index": index,
                "event_id": event_id,
                "game": payload_game,
                "source": source,
                "sidecar_path": resolved_path,
                "event_type": event_row.get("event_type"),
                "confidence": event_row.get("confidence"),
                "final_score": final_score,
                "gate_status": event_row.get("gate_status"),
                "synergy_applied": bool(event_row.get("synergy_applied", False)),
                "synergy_multiplier": event_row.get("synergy_multiplier"),
                "minimum_required_signals_met": event_row.get("minimum_required_signals_met"),
                "suggested_start_timestamp": event_row.get("suggested_start_timestamp"),
                "suggested_end_timestamp": event_row.get("suggested_end_timestamp"),
                "entity_id": metadata.get("entity_id"),
                "ability_id": metadata.get("ability_id"),
                "equipment_id": metadata.get("equipment_id"),
                "event_row_id": metadata.get("event_row_id"),
                "review_status": _normalized_review_status(event_review.get("review_status")),
                "recommended_action": recommended_action,
            }
        )
        for signal_index, signal_id in enumerate(event_row.get("contributing_signals", [])):
            signal_key = str(signal_id)
            signal_row = signal_by_id.get(signal_key, {})
            rows["fused_signal_refs"].append(
                {
                    "fusion_id": fusion_id,
                    "event_id": event_id,
                    "signal_index": signal_index,
                    "signal_id": signal_key,
                    "signal_type": signal_row.get("signal_type"),
                    "producer_family": signal_row.get("producer_family"),
                    "source_family": signal_row.get("source_family"),
                    "asset_id": signal_row.get("asset_id"),
                    "roi_ref": signal_row.get("roi_ref"),
                }
            )
    rows["counts"]["fused_sidecar_count"] += 1


def _top_fused_review_status(review_events: dict[str, Any]) -> str | None:
    statuses = {_normalized_review_status(row.get("review_status")) for row in review_events.values() if isinstance(row, dict)}
    statuses.discard(None)
    if "approved" in statuses:
        return "approved"
    if "rejected" in statuses:
        return "rejected"
    if "unreviewed" in statuses:
        return "unreviewed"
    return None


def _recommended_fused_action(final_score: float) -> str:
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["highlight_candidate"]):
        return "highlight_candidate"
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["inspect"]):
        return "inspect"
    return "skip"


def _ingest_runtime_review_session(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != RUNTIME_REVIEW_SESSION_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    session_id = str(payload.get("session_id") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    rows["runtime_review_sessions"].append(
        {
            "session_id": session_id,
            "game": payload_game,
            "manifest_path": resolved_path,
            "selection_source": payload.get("selection_source"),
            "selection_action_filter": payload.get("selection_action_filter"),
            "created_at": payload.get("created_at"),
            "applied_at": payload.get("applied_at"),
            "cleanup_at": payload.get("cleanup_at"),
            "item_count": payload.get("item_count"),
            "approved_count": payload.get("approved_count"),
            "rejected_count": payload.get("rejected_count"),
            "unreviewed_count": payload.get("unreviewed_count"),
        }
    )

    for index, item in enumerate(payload.get("items", [])):
        sidecar_path = _safe_resolve(item.get("sidecar_path"))
        if sidecar_path is not None and not sidecar_path.exists():
            _warning(rows, path=path, reason="missing_runtime_review_sidecar", detail=str(sidecar_path))
        rows["runtime_review_items"].append(
            {
                "session_id": session_id,
                "item_index": index,
                "game": payload_game,
                "sidecar_path": str(sidecar_path) if sidecar_path is not None else str(item.get("sidecar_path") or ""),
                "source": item.get("source"),
                "analysis_id": item.get("analysis_id"),
                "review_status": _normalized_review_status(item.get("review_status")),
                "apply_status": item.get("apply_status"),
                "highlight_score": item.get("highlight_score"),
                "recommended_action": item.get("recommended_action"),
                "gpt_meta_path": item.get("gpt_meta_path"),
                "gpt_processed_path": item.get("gpt_processed_path"),
                "gpt_final_path": item.get("gpt_final_path"),
            }
        )
    rows["counts"]["runtime_review_session_count"] += 1


def _ingest_fused_review_session(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != FUSED_REVIEW_SESSION_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    if game is not None and payload.get("game") != game:
        return

    resolved_path = str(path.resolve())
    session_id = str(payload.get("session_id") or "").strip()
    payload_game = str(payload.get("game") or "").strip()
    rows["fused_review_sessions"].append(
        {
            "session_id": session_id,
            "game": payload_game,
            "manifest_path": resolved_path,
            "selection_source": payload.get("selection_source"),
            "selection_action_filter": payload.get("selection_action_filter"),
            "selection_event_type_filter": payload.get("selection_event_type_filter"),
            "created_at": payload.get("created_at"),
            "applied_at": payload.get("applied_at"),
            "cleanup_at": payload.get("cleanup_at"),
            "item_count": payload.get("item_count"),
            "approved_count": payload.get("approved_count"),
            "rejected_count": payload.get("rejected_count"),
            "unreviewed_count": payload.get("unreviewed_count"),
        }
    )

    for index, item in enumerate(payload.get("items", [])):
        sidecar_path = _safe_resolve(item.get("sidecar_path"))
        if sidecar_path is not None and not sidecar_path.exists():
            _warning(rows, path=path, reason="missing_fused_review_sidecar", detail=str(sidecar_path))
        rows["fused_review_items"].append(
            {
                "session_id": session_id,
                "item_index": index,
                "game": payload_game,
                "sidecar_path": str(sidecar_path) if sidecar_path is not None else str(item.get("sidecar_path") or ""),
                "source": item.get("source"),
                "fusion_id": item.get("fusion_id"),
                "event_id": item.get("event_id"),
                "event_type": item.get("event_type"),
                "review_status": _normalized_review_status(item.get("review_status")),
                "apply_status": item.get("apply_status"),
                "final_score": item.get("final_score"),
                "recommended_action": item.get("recommended_action"),
                "gate_status": item.get("gate_status"),
                "gpt_meta_path": item.get("gpt_meta_path"),
                "gpt_processed_path": item.get("gpt_processed_path"),
                "gpt_final_path": item.get("gpt_final_path"),
            }
        )
    rows["counts"]["fused_review_session_count"] += 1


def _safe_resolve(value: Any) -> Path | None:
    if not value:
        return None
    return _resolve_path(str(value))


def _normalized_review_status(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"accepted", "approved"}:
        return "approved"
    if text in {"rejected", "reject"}:
        return "rejected"
    if text in {"unreviewed", "pending"}:
        return "unreviewed"
    return text


def _load_json_silent(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _ingest_fixture_comparison_report(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json_silent(path)
    if not isinstance(payload, dict):
        return
    fixture_rows = list(payload.get("comparison", {}).get("fixture_rows", [])) if isinstance(payload.get("comparison"), dict) else []
    if not fixture_rows:
        return
    if payload.get("schema_version") not in {None, "", FIXTURE_SIDECAR_COMPARISON_SCHEMA_VERSION}:
        return
    comparison_path = str(path.resolve())
    recommendation = payload.get("recommendation", {}) if isinstance(payload.get("recommendation"), dict) else {}
    for index, row in enumerate(fixture_rows):
        if not isinstance(row, dict):
            continue
        row_game = str(row.get("game", "")).strip()
        if game is not None and row_game and row_game != game:
            continue
        artifact_layer = str(row.get("artifact_layer", "")).strip()
        fixture_id = str(row.get("fixture_id", "")).strip()
        baseline_sidecar_path = str(row.get("baseline_sidecar_path") or "").strip() or None
        trial_sidecar_path = str(row.get("trial_sidecar_path") or "").strip() or None
        rows["fixture_comparisons"].append(
            {
                "comparison_path": comparison_path,
                "row_index": index,
                "fixture_id": fixture_id,
                "label": row.get("label"),
                "artifact_layer": artifact_layer,
                "game": row_game or None,
                "source": row.get("source"),
                "coverage_status": row.get("coverage_status"),
                "review_status": row.get("review_status"),
                "baseline_sidecar_path": baseline_sidecar_path,
                "trial_sidecar_path": trial_sidecar_path,
                "baseline_action": row.get("baseline_action"),
                "trial_action": row.get("trial_action"),
                "baseline_score": row.get("baseline_score"),
                "trial_score": row.get("trial_score"),
                "score_delta": row.get("score_delta"),
                "shortlist_changed": int(bool(row.get("shortlist_changed", False))),
                "rerank_changed": int(bool(row.get("rerank_changed", False))),
                "stage_latency_deltas_json": json.dumps(row.get("stage_latency_deltas", {}), sort_keys=True),
                "recommendation_signal": row.get("recommendation_signal"),
                "recommendation_decision": recommendation.get("decision") or row.get("recommendation_signal"),
                "recommendation_reason": recommendation.get("reason"),
            }
        )
        _attach_comparison_to_clips(rows, fixture_id=fixture_id, baseline_sidecar_path=baseline_sidecar_path, trial_sidecar_path=trial_sidecar_path, recommendation_decision=str(recommendation.get("decision") or row.get("recommendation_signal") or "").strip())
    rows["counts"]["fixture_comparison_report_count"] += 1


def _attach_comparison_to_clips(
    rows: dict[str, Any],
    *,
    fixture_id: str,
    baseline_sidecar_path: str | None,
    trial_sidecar_path: str | None,
    recommendation_decision: str,
) -> None:
    normalized_baseline = str(Path(baseline_sidecar_path).resolve()) if baseline_sidecar_path else None
    normalized_trial = str(Path(trial_sidecar_path).resolve()) if trial_sidecar_path else None
    for clip in rows["clips"].values():
        sidecar_paths = {
            str(clip.get("proxy_sidecar_path") or ""),
            str(clip.get("runtime_sidecar_path") or ""),
            str(clip.get("fused_sidecar_path") or ""),
        }
        if normalized_baseline and normalized_baseline in sidecar_paths:
            _clip_add_fixture_id(clip, fixture_id)
            if recommendation_decision in {"prefer_trial", "trial_better"}:
                clip["has_trial_preference"] = 1
        if normalized_trial and normalized_trial in sidecar_paths:
            _clip_add_fixture_id(clip, fixture_id)
            if recommendation_decision in {"prefer_trial", "trial_better"}:
                clip["has_trial_preference"] = 1
        _update_clip_disagreement_flags(clip)


def _ingest_fixture_trial_run_manifest(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != FIXTURE_TRIAL_RUN_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    trial_name = str(payload.get("trial_name") or "").strip()
    rows["fixture_trial_runs"].append(
        {
            "trial_name": trial_name,
            "trial_root": payload.get("trial_root"),
            "manifest_path": str(path.resolve()),
            "proxy_sidecar_root": payload.get("proxy_sidecar_root"),
            "runtime_sidecar_root": payload.get("runtime_sidecar_root"),
            "fused_sidecar_root": payload.get("fused_sidecar_root"),
            "fixture_manifest_path": payload.get("fixture_manifest_path"),
            "fixture_source_manifest_path": payload.get("fixture_source_manifest_path"),
            "status": payload.get("status"),
            "completed_fixture_count": payload.get("completed_fixture_count"),
            "failed_fixture_count": payload.get("failed_fixture_count"),
            "effective_overrides_json": json.dumps(payload.get("effective_overrides", {}), sort_keys=True),
        }
    )
    for index, fixture in enumerate(payload.get("fixtures", [])):
        if not isinstance(fixture, dict):
            continue
        fixture_game = str(fixture.get("game", "")).strip()
        if game is not None and fixture_game and fixture_game != game:
            continue
        layers = fixture.get("layers", {}) if isinstance(fixture.get("layers"), dict) else {}
        rows["fixture_trial_run_fixtures"].append(
            {
                "trial_name": trial_name,
                "fixture_index": index,
                "fixture_id": fixture.get("fixture_id"),
                "game": fixture_game or None,
                "source_path": fixture.get("source_path"),
                "status": fixture.get("status"),
                "failure_reason": fixture.get("failure_reason") or fixture.get("error"),
                "proxy_sidecar_path": (layers.get("proxy") or {}).get("sidecar_path"),
                "runtime_sidecar_path": (layers.get("runtime") or {}).get("sidecar_path"),
                "fused_sidecar_path": (layers.get("fused") or {}).get("sidecar_path"),
            }
        )
        for clip in rows["clips"].values():
            if str(clip.get("source") or "") == str(fixture.get("source_path") or "") and (not fixture_game or str(clip.get("game") or "") == fixture_game):
                _clip_add_fixture_id(clip, str(fixture.get("fixture_id") or ""))
    rows["counts"]["fixture_trial_run_manifest_count"] += 1


def _ingest_fixture_trial_batch_manifest(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != FIXTURE_TRIAL_BATCH_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    batch_name = str(payload.get("batch_name") or "").strip()
    rows["fixture_trial_batches"].append(
        {
            "batch_name": batch_name,
            "manifest_path": str(path.resolve()),
            "baseline_trial_name": payload.get("baseline_trial_name"),
            "overall_recommendation_decision": (payload.get("overall_recommendation") or {}).get("decision"),
            "overall_recommendation_trial_name": (payload.get("overall_recommendation") or {}).get("trial_name"),
            "selected_trials_json": json.dumps(payload.get("selected_trials", []), sort_keys=True),
        }
    )
    for index, comparison in enumerate(payload.get("trial_comparisons", [])):
        if not isinstance(comparison, dict):
            continue
        rows["fixture_trial_batch_comparisons"].append(
            {
                "batch_name": batch_name,
                "comparison_index": index,
                "trial_name": comparison.get("trial_name"),
                "comparison_report_path": comparison.get("comparison_report_path"),
                "artifact_layer": comparison.get("artifact_layer"),
                "comparison_status": comparison.get("comparison_status"),
                "recommendation_decision": (comparison.get("recommendation") or {}).get("decision"),
            }
        )
    rows["counts"]["fixture_trial_batch_manifest_count"] += 1


def _ingest_highlight_selection_manifest(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != HIGHLIGHT_SELECTION_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    payload_game = str(payload.get("game") or "").strip()
    if game is not None and payload_game and payload_game != game:
        return
    rows["highlight_selection_manifests"].append(
        {
            "manifest_path": str(path.resolve()),
            "game": payload_game,
            "source": str(payload.get("source") or "").strip(),
            "selection_basis": str(payload.get("selection_basis") or "proxy").strip() or "proxy",
            "proxy_sidecar_path": str(payload.get("proxy_sidecar_path") or "").strip() or None,
            "fused_sidecar_path": str(payload.get("fused_sidecar_path") or "").strip() or None,
            "selected_highlights": list(payload.get("selected_highlights", [])),
        }
    )
    rows["counts"]["highlight_selection_manifest_count"] += 1


def _ingest_hook_candidate_manifest(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != HOOK_CANDIDATE_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    payload_game = str(payload.get("game") or "").strip()
    if game is not None and payload_game and payload_game != game:
        return
    manifest_path = str(path.resolve())
    fused_sidecar_path = str(payload.get("fused_sidecar_path") or "").strip() or None
    source = str(payload.get("source") or "").strip()
    rows["hook_candidate_manifests"].append(
        {
            "manifest_path": manifest_path,
            "game": payload_game,
            "source": source,
            "fused_sidecar_path": fused_sidecar_path,
            "hook_candidate_count": int(payload.get("hook_candidate_count", 0) or 0),
        }
    )
    for index, hook_row in enumerate(list(payload.get("hook_candidates", []))):
        if not isinstance(hook_row, dict):
            continue
        rows["hook_candidates"].append(
            {
                "manifest_path": manifest_path,
                "hook_index": index,
                "hook_id": str(hook_row.get("hook_id") or "").strip() or f"hook-{index}",
                "candidate_id": str(hook_row.get("candidate_id") or "").strip() or None,
                "event_id": str(hook_row.get("event_id") or "").strip() or None,
                "game": payload_game or None,
                "source": source or None,
                "fixture_id": _fixture_id_for_event(rows, sidecar_path=fused_sidecar_path or "", game=payload_game, source=source),
                "fused_sidecar_path": fused_sidecar_path,
                "lifecycle_state": hook_row.get("lifecycle_state"),
                "hook_archetype": hook_row.get("hook_archetype"),
                "hook_mode": hook_row.get("hook_mode"),
                "hook_strength": hook_row.get("hook_strength"),
                "intensity_score": hook_row.get("intensity_score"),
                "clarity_score": hook_row.get("clarity_score"),
                "novelty_score": hook_row.get("novelty_score"),
                "context_sufficiency_score": hook_row.get("context_sufficiency_score"),
                "payoff_readability_score": hook_row.get("payoff_readability_score"),
                "title_thumbnail_potential_score": hook_row.get("title_thumbnail_potential_score"),
                "authenticity_risk_score": hook_row.get("authenticity_risk_score"),
                "sound_off_legibility_score": hook_row.get("sound_off_legibility_score"),
                "packaging_strategy": hook_row.get("packaging_strategy"),
                "rejection_reason": hook_row.get("rejection_reason"),
                "highlight_selection_manifest_path": _highlight_selection_manifest_path_for_candidate(
                    rows,
                    candidate_id=str(hook_row.get("candidate_id") or "").strip(),
                ),
                "metadata_summary_json": json.dumps(hook_row.get("metadata_summary", {}), sort_keys=True),
                "created_at": _utc_now(),
            }
        )
    rows["counts"]["hook_candidate_manifest_count"] += 1


def _ingest_workflow_run_manifest(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != WORKFLOW_RUN_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
    filter_game = str(filters.get("game") or "").strip()
    if game is not None and filter_game and filter_game != game:
        return
    manifest_path = str(path.resolve())
    workflow_run_id = str(payload.get("workflow_run_id") or "").strip() or path.stem
    workflow_type = str(payload.get("workflow_type") or "").strip() or None
    if workflow_type is None:
        _warn_missing_fields(rows, path=path, artifact_type="workflow_run_manifest", missing_fields=["workflow_type"])
    linked_artifacts = payload.get("linked_artifacts", {}) if isinstance(payload.get("linked_artifacts"), dict) else {}
    item_counts = payload.get("item_counts", {}) if isinstance(payload.get("item_counts"), dict) else {}
    rows["workflow_runs"].append(
        {
            "workflow_run_id": workflow_run_id,
            "manifest_path": manifest_path,
            "workflow_type": workflow_type,
            "stage": str(payload.get("stage") or "").strip() or None,
            "status": str(payload.get("status") or "").strip() or None,
            "registry_path": str(payload.get("registry_path") or "").strip() or None,
            "game_filter": filter_game or None,
            "fixture_id_filter": str(filters.get("fixture_id") or "").strip() or None,
            "item_counts_json": json.dumps(item_counts, sort_keys=True),
            "linked_artifacts_json": json.dumps(linked_artifacts, sort_keys=True),
            "error": str(payload.get("error") or "").strip() or None,
            "created_at": str(payload.get("created_at") or "").strip() or None,
            "updated_at": str(payload.get("updated_at") or payload.get("created_at") or "").strip() or None,
        }
    )
    for index, item in enumerate(list(payload.get("items", []))):
        if not isinstance(item, dict):
            continue
        candidate_id = str(item.get("candidate_id") or "").strip() or None
        lifecycle_state = str(item.get("lifecycle_state") or "").strip() or None
        if candidate_id is None:
            _warn_missing_fields(rows, path=path, artifact_type="workflow_run_item", missing_fields=["candidate_id"], row_index=index)
        rows["workflow_run_items"].append(
            {
                "workflow_run_id": workflow_run_id,
                "item_index": index,
                "candidate_id": candidate_id,
                "item_status": str(item.get("item_status") or "ready").strip() or "ready",
                "game": str(item.get("game") or "").strip() or None,
                "source": str(item.get("source") or "").strip() or None,
                "fixture_id": str(item.get("fixture_id") or "").strip() or None,
                "event_id": str(item.get("event_id") or "").strip() or None,
                "lifecycle_state": lifecycle_state,
                "fused_sidecar_path": str(item.get("fused_sidecar_path") or "").strip() or None,
                "highlight_selection_manifest_path": str(item.get("highlight_selection_manifest_path") or "").strip() or None,
                "export_artifact_path": str(item.get("export_artifact_path") or "").strip() or None,
                "post_ledger_path": str(item.get("post_ledger_path") or "").strip() or None,
                "hook_manifest_path": str(item.get("hook_manifest_path") or "").strip() or None,
                "created_at": str(payload.get("created_at") or "").strip() or _utc_now(),
            }
        )
    rows["counts"]["workflow_run_manifest_count"] += 1


def _ingest_hook_comparison_report(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != HOOK_CANDIDATE_COMPARISON_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    report_path = str(path.resolve())
    baseline_root = str(payload.get("baseline_sidecar_root") or "").strip() or None
    trial_root = str(payload.get("trial_sidecar_root") or "").strip() or None
    recommendation = payload.get("recommendation", {}) if isinstance(payload.get("recommendation"), dict) else {}
    rows["hook_comparison_reports"].append(
        {
            "report_path": report_path,
            "fixture_manifest_path": str(payload.get("fixture_manifest_path") or "").strip() or None,
            "baseline_sidecar_root": baseline_root,
            "trial_sidecar_root": trial_root,
            "comparison_row_count": int(payload.get("comparison_row_count", 0) or 0),
            "recommendation_decision": str(recommendation.get("decision") or "").strip() or None,
            "recommendation_reason": str(recommendation.get("reason") or "").strip() or None,
        }
    )
    fixture_rows = list(payload.get("comparison", {}).get("fixture_rows", [])) if isinstance(payload.get("comparison"), dict) else []
    for index, row in enumerate(fixture_rows):
        if not isinstance(row, dict):
            continue
        row_game = str(row.get("game") or "").strip()
        if game is not None and row_game and row_game != game:
            continue
        rows["hook_comparisons"].append(
            {
                "report_path": report_path,
                "row_index": index,
                "fixture_id": str(row.get("fixture_id") or "").strip() or None,
                "label": str(row.get("label") or "").strip() or None,
                "game": row_game or None,
                "source": str(row.get("source") or "").strip() or None,
                "candidate_id": str(row.get("candidate_id") or "").strip() or None,
                "event_id": str(row.get("event_id") or "").strip() or None,
                "comparison_status": str(row.get("comparison_status") or "").strip() or None,
                "review_status": str(row.get("review_status") or "").strip() or None,
                "baseline_manifest_path": str(row.get("baseline_manifest_path") or "").strip() or None,
                "trial_manifest_path": str(row.get("trial_manifest_path") or "").strip() or None,
                "baseline_fused_sidecar_path": str(row.get("baseline_fused_sidecar_path") or "").strip() or None,
                "trial_fused_sidecar_path": str(row.get("trial_fused_sidecar_path") or "").strip() or None,
                "baseline_hook_mode": str(row.get("baseline_hook_mode") or "").strip() or None,
                "trial_hook_mode": str(row.get("trial_hook_mode") or "").strip() or None,
                "baseline_hook_archetype": str(row.get("baseline_hook_archetype") or "").strip() or None,
                "trial_hook_archetype": str(row.get("trial_hook_archetype") or "").strip() or None,
                "baseline_hook_strength": row.get("baseline_hook_strength"),
                "trial_hook_strength": row.get("trial_hook_strength"),
                "hook_strength_delta": row.get("hook_strength_delta"),
                "baseline_lifecycle_state": str(row.get("baseline_lifecycle_state") or "").strip() or None,
                "trial_lifecycle_state": str(row.get("trial_lifecycle_state") or "").strip() or None,
                "baseline_selection_manifest_path": str(row.get("baseline_selection_manifest_path") or "").strip() or None,
                "trial_selection_manifest_path": str(row.get("trial_selection_manifest_path") or "").strip() or None,
                "strong_fused_weak_hook": int(bool(row.get("strong_fused_weak_hook"))),
                "approved_reject_hook": int(bool(row.get("approved_reject_hook"))),
                "reject_to_synthetic": int(bool(row.get("reject_to_synthetic"))),
                "natural_to_synthetic": int(bool(row.get("natural_to_synthetic"))),
                "recommendation_signal": str(row.get("recommendation_signal") or "").strip() or None,
                "recommendation_decision": str(recommendation.get("decision") or "").strip() or None,
                "recommendation_reason": str(recommendation.get("reason") or "").strip() or None,
            }
        )
    rows["counts"]["hook_comparison_report_count"] += 1


def _ingest_hook_evaluation_report(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != HOOK_EVALUATION_REPORT_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    payload_game = str(payload.get("game_filter") or "").strip()
    if game is not None and payload_game and payload_game != game:
        return
    trial_comparison = payload.get("trial_comparison", {}) if isinstance(payload.get("trial_comparison"), dict) else {}
    recommendation = trial_comparison.get("recommendation", {}) if isinstance(trial_comparison.get("recommendation"), dict) else {}
    selected = payload.get("candidate_rollups", {}).get("selected_or_approved", {}) if isinstance(payload.get("candidate_rollups"), dict) else {}
    exported = payload.get("candidate_rollups", {}).get("exported", {}) if isinstance(payload.get("candidate_rollups"), dict) else {}
    disagreement = payload.get("fused_hook_disagreement", {}) if isinstance(payload.get("fused_hook_disagreement"), dict) else {}
    policy = payload.get("policy", {}) if isinstance(payload.get("policy"), dict) else {}
    rows["hook_evaluation_reports"].append(
        {
            "report_path": str(path.resolve()),
            "fixture_manifest_path": str(payload.get("fixture_manifest_path") or "").strip() or None,
            "baseline_sidecar_root": str(payload.get("baseline_sidecar_root") or "").strip() or None,
            "trial_sidecar_root": str(payload.get("trial_sidecar_root") or "").strip() or None,
            "registry_path": str(payload.get("registry_path") or "").strip() or None,
            "game": payload_game or None,
            "comparison_row_count": int(trial_comparison.get("comparison_row_count", 0) or 0),
            "recommendation_decision": str(recommendation.get("decision") or "").strip() or None,
            "recommendation_reason": str(recommendation.get("reason") or "").strip() or None,
            "selected_candidate_count": int(selected.get("candidate_count", 0) or 0),
            "exported_candidate_count": int(exported.get("candidate_count", 0) or 0),
            "strong_fused_weak_hook_count": int(disagreement.get("strong_fused_weak_hook_count", 0) or 0),
            "approved_reject_hook_count": int(disagreement.get("approved_reject_hook_count", 0) or 0),
            "reject_to_synthetic_count": int(disagreement.get("reject_to_synthetic_count", 0) or 0),
            "natural_to_synthetic_count": int(disagreement.get("natural_to_synthetic_count", 0) or 0),
            "hook_artifacts_policy": str(policy.get("hook_artifacts_policy") or "").strip() or None,
            "future_gate_readiness": str(policy.get("future_gate_readiness") or "").strip() or None,
            "created_at": str(payload.get("created_at") or "").strip() or _utc_now(),
        }
    )


def _ingest_highlight_export_batch_manifest(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != HIGHLIGHT_EXPORT_BATCH_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    payload_game = str(payload.get("game") or "").strip()
    if game is not None and payload_game and payload_game != game:
        return
    manifest_path = str(path.resolve())
    export_batch_id = str(payload.get("export_batch_id") or "").strip() or path.stem
    linked_inputs = payload.get("linked_inputs", {}) if isinstance(payload.get("linked_inputs"), dict) else {}
    rows["highlight_export_batches"].append(
        {
            "manifest_path": manifest_path,
            "export_batch_id": export_batch_id,
            "game": payload_game or None,
            "workflow_run_id": str(payload.get("workflow_run_id") or "").strip() or None,
            "selection_manifest_path": str(payload.get("selection_manifest_path") or "").strip() or None,
            "fused_sidecar_paths_json": json.dumps(list(linked_inputs.get("fused_sidecar_paths", [])), sort_keys=True),
            "hook_manifest_paths_json": json.dumps(list(linked_inputs.get("hook_manifest_paths", [])), sort_keys=True),
            "selection_manifest_paths_json": json.dumps(list(linked_inputs.get("selection_manifest_paths", [])), sort_keys=True),
            "export_count": int(payload.get("export_count", 0) or 0),
            "created_at": str(payload.get("created_at") or "").strip() or None,
        }
    )
    for index, export_row in enumerate(list(payload.get("exports", []))):
        if not isinstance(export_row, dict):
            continue
        export_id = str(export_row.get("export_id") or "").strip() or None
        candidate_id = str(export_row.get("candidate_id") or "").strip() or None
        event_id = str(export_row.get("event_id") or "").strip() or None
        missing_fields = [name for name, value in (("export_id", export_id), ("candidate_id", candidate_id), ("event_id", event_id)) if value is None]
        if missing_fields:
            _warn_missing_fields(rows, path=path, artifact_type="highlight_export_row", missing_fields=missing_fields, row_index=index)
        rows["highlight_exports"].append(
            {
                "manifest_path": manifest_path,
                "export_batch_id": export_batch_id,
                "export_index": index,
                "export_id": export_id,
                "candidate_id": candidate_id,
                "event_id": event_id,
                "hook_id": str(export_row.get("hook_id") or "").strip() or None,
                "fixture_id": str(export_row.get("fixture_id") or "").strip() or None,
                "game": payload_game or None,
                "source": str(export_row.get("source") or "").strip() or None,
                "fused_sidecar_path": str(export_row.get("fused_sidecar_path") or "").strip() or None,
                "hook_manifest_path": str(export_row.get("hook_manifest_path") or "").strip() or None,
                "highlight_selection_manifest_path": str(export_row.get("highlight_selection_manifest_path") or "").strip() or None,
                "start_seconds": export_row.get("start_seconds"),
                "end_seconds": export_row.get("end_seconds"),
                "final_score": export_row.get("final_score"),
                "hook_archetype": str(export_row.get("hook_archetype") or "").strip() or None,
                "hook_mode": str(export_row.get("hook_mode") or "").strip() or None,
                "packaging_strategy": str(export_row.get("packaging_strategy") or "").strip() or None,
                "export_status": str(export_row.get("export_status") or "").strip() or None,
                "export_artifact_path": str(export_row.get("export_artifact_path") or "").strip() or None,
                "otio_path": str(export_row.get("otio_path") or "").strip() or None,
                "selected_highlight_details_json": json.dumps(
                    (
                        _selection_manifest_details_for_export(
                            rows,
                            manifest_path=str(export_row.get("highlight_selection_manifest_path") or "").strip() or None,
                            candidate_id=candidate_id,
                            event_id=event_id,
                            game=payload_game or None,
                            source=str(export_row.get("source") or "").strip() or None,
                            fused_sidecar_path=str(export_row.get("fused_sidecar_path") or "").strip() or None,
                        )
                        or {}
                    ),
                    sort_keys=True,
                ),
                "metadata_json": json.dumps(export_row.get("metadata_json", {}), sort_keys=True),
            }
        )
    rows["counts"]["highlight_export_batch_manifest_count"] += 1


def _ingest_posted_highlight_ledger(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != POSTED_HIGHLIGHT_LEDGER_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    manifest_path = str(path.resolve())
    ledger_platform = str(payload.get("platform") or "").strip() or None
    rows["post_ledgers"].append(
        {
            "manifest_path": manifest_path,
            "ledger_id": str(payload.get("ledger_id") or "").strip() or path.stem,
            "platform": ledger_platform,
            "account_id": str(payload.get("account_id") or "").strip() or None,
            "workflow_run_id": str(payload.get("workflow_run_id") or "").strip() or None,
            "posted_count": int(payload.get("posted_count", 0) or 0),
            "created_at": str(payload.get("created_at") or "").strip() or None,
        }
    )
    for index, record in enumerate(list(payload.get("posted_records", []))):
        if not isinstance(record, dict):
            continue
        export_batch_manifest_path = str(record.get("export_batch_manifest_path") or "").strip() or None
        post_record_id = str(record.get("post_record_id") or "").strip() or None
        export_id = str(record.get("export_id") or "").strip() or None
        candidate_id = str(record.get("candidate_id") or "").strip() or None
        missing_fields = [
            name
            for name, value in (
                ("post_record_id", post_record_id),
                ("export_id", export_id),
                ("candidate_id", candidate_id),
                ("export_batch_manifest_path", export_batch_manifest_path),
            )
            if value is None
        ]
        if missing_fields:
            _warn_missing_fields(rows, path=path, artifact_type="posted_highlight_record", missing_fields=missing_fields, row_index=index)
        row_game = _game_for_export_manifest(rows, str(export_batch_manifest_path or ""))
        if game is not None and row_game and row_game != game:
            continue
        if export_batch_manifest_path and row_game is None:
            _warning(rows, path=path, reason="unresolved_export_manifest_lineage", detail=f"posted_highlight_record row_index={index} export_batch_manifest_path={export_batch_manifest_path}")
        if export_id and _export_details(rows, export_id=export_id, candidate_id=candidate_id) is None:
            _warning(rows, path=path, reason="missing_export_lineage", detail=f"posted_highlight_record row_index={index} export_id={export_id}")
        export_details = _export_details(rows, export_id=export_id, candidate_id=candidate_id) or {}
        rows["posted_highlights"].append(
            {
                "manifest_path": manifest_path,
                "ledger_id": str(payload.get("ledger_id") or "").strip() or path.stem,
                "record_index": index,
                "post_record_id": post_record_id,
                "export_id": export_id,
                "candidate_id": candidate_id,
                "event_id": str(record.get("event_id") or "").strip() or None,
                "hook_id": str(record.get("hook_id") or "").strip() or None,
                "export_batch_manifest_path": export_batch_manifest_path,
                "posted_at": str(record.get("posted_at") or "").strip() or None,
                "post_status": str(record.get("post_status") or "").strip() or None,
                "external_post_id": str(record.get("external_post_id") or "").strip() or None,
                "external_url": str(record.get("external_url") or "").strip() or None,
                "platform": str(record.get("platform") or payload.get("platform") or "").strip() or None,
                "account_id": str(record.get("account_id") or payload.get("account_id") or "").strip() or None,
                "caption_ref": str(record.get("caption_ref") or "").strip() or None,
                "caption_text": str(record.get("caption_text") or "").strip() or None,
                "duration_seconds": record.get("duration_seconds"),
                "media_asset_path": str(record.get("media_asset_path") or "").strip() or None,
                "initial_view_count": record.get("initial_view_count"),
                "initial_like_count": record.get("initial_like_count"),
                "initial_comment_count": record.get("initial_comment_count"),
                "selected_highlight_details_json": str(export_details.get("selected_highlight_details_json") or "").strip() or "{}",
                "game": row_game,
            }
        )
    rows["counts"]["post_ledger_manifest_count"] += 1


def _ingest_posted_metrics_snapshot(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != POSTED_HIGHLIGHT_METRICS_SNAPSHOT_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    manifest_path = str(path.resolve())
    snapshot_platform = str(payload.get("platform") or "").strip() or None
    snapshot_account_id = str(payload.get("account_id") or "").strip() or None
    rows["posted_metrics_snapshots"].append(
        {
            "manifest_path": manifest_path,
            "snapshot_id": str(payload.get("snapshot_id") or "").strip() or path.stem,
            "platform": snapshot_platform,
            "account_id": snapshot_account_id,
            "workflow_run_id": str(payload.get("workflow_run_id") or "").strip() or None,
            "captured_at": str(payload.get("captured_at") or "").strip() or None,
            "snapshot_count": int(payload.get("snapshot_count", 0) or 0),
        }
    )
    for index, record in enumerate(list(payload.get("snapshots", []))):
        if not isinstance(record, dict):
            continue
        post_record_id = str(record.get("post_record_id") or "").strip() or None
        export_id = str(record.get("export_id") or "").strip() or None
        post_ledger_manifest_path = str(record.get("post_ledger_manifest_path") or "").strip() or None
        missing_fields = [
            name
            for name, value in (
                ("snapshot_row_id", str(record.get("snapshot_row_id") or "").strip() or None),
                ("post_record_id", post_record_id),
                ("export_id", export_id),
                ("candidate_id", str(record.get("candidate_id") or "").strip() or None),
                ("post_ledger_manifest_path", post_ledger_manifest_path),
            )
            if value is None
        ]
        if missing_fields:
            _warn_missing_fields(rows, path=path, artifact_type="posted_metrics_snapshot_row", missing_fields=missing_fields, row_index=index)
        candidate = _metrics_candidate_details(
            rows,
            post_record_id=post_record_id,
            export_id=export_id,
            candidate_id=str(record.get("candidate_id") or "").strip() or None,
        )
        row_game = str(candidate.get("game") or "").strip() or None
        if game is not None and row_game and row_game != game:
            continue
        if post_ledger_manifest_path:
            ledger_path = Path(post_ledger_manifest_path).expanduser()
            if not ledger_path.is_absolute():
                ledger_path = (Path.cwd() / ledger_path).resolve()
            else:
                ledger_path = ledger_path.resolve()
            if not ledger_path.exists():
                _warning(rows, path=path, reason="missing_post_ledger_manifest", detail=f"posted_metrics_snapshot_row row_index={index} post_ledger_manifest_path={post_ledger_manifest_path}")
        if post_record_id and _post_record_details(rows, post_record_id=post_record_id) is None:
            _warning(rows, path=path, reason="missing_post_record_lineage", detail=f"posted_metrics_snapshot_row row_index={index} post_record_id={post_record_id}")
        if export_id and _export_details(rows, export_id=export_id, candidate_id=str(candidate.get('candidate_id') or '').strip() or None) is None:
            _warning(rows, path=path, reason="missing_export_lineage", detail=f"posted_metrics_snapshot_row row_index={index} export_id={export_id}")
        rows["posted_metrics_snapshot_rows"].append(
            {
                "manifest_path": manifest_path,
                "snapshot_index": index,
                "snapshot_row_id": str(record.get("snapshot_row_id") or "").strip() or None,
                "post_record_id": post_record_id,
                "export_id": export_id,
                "candidate_id": str(candidate.get("candidate_id") or record.get("candidate_id") or "").strip() or None,
                "hook_id": str(candidate.get("hook_id") or record.get("hook_id") or "").strip() or None,
                "post_ledger_manifest_path": post_ledger_manifest_path,
                "captured_at": str(record.get("captured_at") or payload.get("captured_at") or "").strip() or None,
                "platform": str(record.get("platform") or snapshot_platform or "").strip() or None,
                "account_id": snapshot_account_id,
                "external_post_id": str(record.get("external_post_id") or "").strip() or None,
                "external_url": str(record.get("external_url") or "").strip() or None,
                "view_count": record.get("view_count"),
                "like_count": record.get("like_count"),
                "comment_count": record.get("comment_count"),
                "share_count": record.get("share_count"),
                "save_count": record.get("save_count"),
                "watch_time_seconds": record.get("watch_time_seconds"),
                "average_watch_time_seconds": record.get("average_watch_time_seconds"),
                "completion_rate": record.get("completion_rate"),
                "engagement_rate": record.get("engagement_rate"),
                "metadata_json": json.dumps(record.get("metadata_json", {}), sort_keys=True),
                "game": row_game,
            }
        )
    rows["counts"]["posted_metrics_snapshot_manifest_count"] += 1


def _ingest_shadow_ranking_model(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_RANKING_MODEL_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
    filter_game = str(filters.get("game") or "").strip() or None
    if game is not None and filter_game and filter_game != game:
        return
    rows["shadow_ranking_models"].append(
        {
            "manifest_path": str(path.resolve()),
            "model_id": str(payload.get("model_id") or "").strip() or path.stem,
            "created_at": str(payload.get("created_at") or "").strip() or None,
            "model_family": str(payload.get("model_family") or "").strip() or None,
            "model_version": str(payload.get("model_version") or "").strip() or None,
            "training_dataset_manifest_path": str(payload.get("training_dataset_manifest_path") or "").strip() or None,
            "training_target": str(payload.get("training_target") or "").strip() or None,
            "split_key": str(payload.get("split_key") or "").strip() or None,
            "train_fraction": payload.get("train_fraction"),
            "row_count": payload.get("row_count"),
            "train_row_count": payload.get("train_row_count"),
            "eval_row_count": payload.get("eval_row_count"),
            "label_positive_count": payload.get("label_positive_count"),
            "label_negative_count": payload.get("label_negative_count"),
            "feature_fields_json": json.dumps(payload.get("feature_fields", []), sort_keys=True),
            "training_metrics_json": json.dumps(payload.get("training_metrics", {}), sort_keys=True),
            "evaluation_metrics_json": json.dumps(payload.get("evaluation_metrics", {}), sort_keys=True),
            "filters_json": json.dumps(filters, sort_keys=True),
            "warning_count": len(list(payload.get("warnings", []))),
        }
    )
    rows["counts"]["shadow_ranking_model_manifest_count"] += 1


def _ingest_shadow_evaluation_policy(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    del game
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_EVALUATION_POLICY_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    rows["shadow_evaluation_policies"].append(
        {
            "manifest_path": str(path.resolve()),
            "policy_id": str(payload.get("policy_id") or "").strip() or path.stem,
            "created_at": str(payload.get("created_at") or "").strip() or None,
            "targets_json": json.dumps(payload.get("targets", {}), sort_keys=True),
        }
    )
    rows["counts"]["shadow_evaluation_policy_manifest_count"] += 1


def _ingest_shadow_ranking_experiment(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_RANKING_EXPERIMENT_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
    filter_game = str(filters.get("game") or "").strip() or None
    if game is not None and filter_game and filter_game != game:
        return
    recommendation = payload.get("comparison_recommendation", {}) if isinstance(payload.get("comparison_recommendation"), dict) else {}
    rows["shadow_ranking_experiments"].append(
        {
            "manifest_path": str(path.resolve()),
            "experiment_id": str(payload.get("experiment_id") or "").strip() or path.stem,
            "created_at": str(payload.get("created_at") or "").strip() or None,
            "model_path": str(payload.get("model_path") or "").strip() or None,
            "model_id": str(payload.get("model_id") or "").strip() or None,
            "model_family": str(payload.get("model_family") or "").strip() or None,
            "model_version": str(payload.get("model_version") or "").strip() or None,
            "dataset_manifest_path": str(payload.get("dataset_manifest_path") or "").strip() or None,
            "dataset_export_id": str(payload.get("dataset_export_id") or "").strip() or None,
            "training_target": str(payload.get("training_target") or "").strip() or None,
            "split_key": str(payload.get("split_key") or "").strip() or None,
            "train_fraction": payload.get("train_fraction"),
            "replay_manifest_path": str(payload.get("replay_manifest_path") or "").strip() or None,
            "comparison_report_path": str(payload.get("comparison_report_path") or "").strip() or None,
            "replay_row_count": payload.get("replay_row_count"),
            "comparison_row_count": payload.get("comparison_row_count"),
            "recommendation_decision": str(recommendation.get("decision") or "").strip() or None,
            "recommendation_reason": str(recommendation.get("reason") or "").strip() or None,
            "training_metrics_json": json.dumps(payload.get("training_metrics", {}), sort_keys=True),
            "evaluation_metrics_json": json.dumps(payload.get("evaluation_metrics", {}), sort_keys=True),
            "comparison_summary_json": json.dumps(payload.get("comparison_summary", {}), sort_keys=True),
            "filters_json": json.dumps(filters, sort_keys=True),
        }
    )
    rows["counts"]["shadow_ranking_experiment_manifest_count"] += 1


def _ingest_shadow_ranking_experiment_ledger(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_EXPERIMENT_LEDGER_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
    target_game = str(filters.get("game") or "").strip() or None
    if game is not None and target_game and target_game != game:
        return
    recommendation = payload.get("recommendation", {}) if isinstance(payload.get("recommendation"), dict) else {}
    global_metrics = payload.get("global_metrics", {}) if isinstance(payload.get("global_metrics"), dict) else {}
    rows["shadow_ranking_experiment_ledgers"].append(
        {
            "manifest_path": str(path.resolve()),
            "ledger_id": str(payload.get("ledger_id") or "").strip() or path.stem,
            "created_at": str(payload.get("created_at") or "").strip() or None,
            "policy_manifest_path": str(payload.get("policy_manifest_path") or "").strip() or None,
            "policy_id": str(payload.get("policy_id") or "").strip() or None,
            "experiment_manifest_path": str(payload.get("experiment_manifest_path") or "").strip() or None,
            "experiment_id": str(payload.get("experiment_id") or "").strip() or None,
            "model_id": str(payload.get("model_id") or "").strip() or None,
            "model_family": str(payload.get("model_family") or "").strip() or None,
            "model_version": str(payload.get("model_version") or "").strip() or None,
            "dataset_manifest_path": str(payload.get("dataset_manifest_path") or "").strip() or None,
            "dataset_export_id": str(payload.get("dataset_export_id") or "").strip() or None,
            "training_target": str(payload.get("training_target") or "").strip() or None,
            "evaluation_target": str(payload.get("evaluation_target") or "").strip() or None,
            "replay_manifest_path": str(payload.get("replay_manifest_path") or "").strip() or None,
            "comparison_report_path": str(payload.get("comparison_report_path") or "").strip() or None,
            "coverage_status": str(payload.get("coverage_status") or "").strip() or None,
            "slice_count": payload.get("slice_count"),
            "recommendation_decision": str(recommendation.get("decision") or "").strip() or None,
            "recommendation_reason": str(recommendation.get("reason") or "").strip() or None,
            "blocking_reasons_json": json.dumps(recommendation.get("blocking_reasons", []), sort_keys=True),
            "protected_regression_count": recommendation.get("protected_regression_count"),
            "global_metrics_json": json.dumps(global_metrics, sort_keys=True),
            "global_primary_metric_name": str(global_metrics.get("primary_metric_name") or "").strip() or None,
            "global_primary_metric_delta": global_metrics.get("primary_metric_delta"),
            "filters_json": json.dumps(filters, sort_keys=True),
        }
    )
    for index, slice_row in enumerate(list(payload.get("slice_rows", []))):
        if not isinstance(slice_row, dict):
            continue
        rows["shadow_ranking_experiment_slices"].append(
            {
                "manifest_path": str(path.resolve()),
                "ledger_id": str(payload.get("ledger_id") or "").strip() or path.stem,
                "slice_index": index,
                "policy_id": str(payload.get("policy_id") or "").strip() or None,
                "experiment_id": str(payload.get("experiment_id") or "").strip() or None,
                "model_id": str(payload.get("model_id") or "").strip() or None,
                "model_family": str(payload.get("model_family") or "").strip() or None,
                "model_version": str(payload.get("model_version") or "").strip() or None,
                "training_target": str(payload.get("training_target") or "").strip() or None,
                "evaluation_target": str(payload.get("evaluation_target") or "").strip() or None,
                "slice_type": str(slice_row.get("slice_type") or "").strip() or None,
                "slice_value": str(slice_row.get("slice_value") or "").strip() or None,
                "coverage_status": str(slice_row.get("coverage_status") or "").strip() or None,
                "row_count": slice_row.get("row_count"),
                "covered_row_count": slice_row.get("covered_row_count"),
                "positive_count": slice_row.get("positive_count"),
                "top_k": slice_row.get("top_k"),
                "shadow_topk_hits": slice_row.get("shadow_topk_hits"),
                "heuristic_topk_hits": slice_row.get("heuristic_topk_hits"),
                "shadow_precision_at_k": slice_row.get("shadow_precision_at_k"),
                "heuristic_precision_at_k": slice_row.get("heuristic_precision_at_k"),
                "shadow_ranking_gain": slice_row.get("shadow_ranking_gain"),
                "heuristic_ranking_gain": slice_row.get("heuristic_ranking_gain"),
                "shadow_false_positive_cost": slice_row.get("shadow_false_positive_cost"),
                "heuristic_false_positive_cost": slice_row.get("heuristic_false_positive_cost"),
                "shadow_pearson_correlation": slice_row.get("shadow_pearson_correlation"),
                "heuristic_pearson_correlation": slice_row.get("heuristic_pearson_correlation"),
                "primary_metric_name": str(slice_row.get("primary_metric_name") or "").strip() or None,
                "shadow_primary_metric": slice_row.get("shadow_primary_metric"),
                "heuristic_primary_metric": slice_row.get("heuristic_primary_metric"),
                "primary_metric_delta": slice_row.get("primary_metric_delta"),
                "game": target_game,
                "platform": str(filters.get("platform") or "").strip() or None,
            }
        )
    rows["counts"]["shadow_ranking_experiment_ledger_manifest_count"] += 1


def _ingest_shadow_ranking_replay(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_RANKING_REPLAY_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    manifest_path = str(path.resolve())
    replay_id = str(payload.get("replay_id") or "").strip() or path.stem
    rows["shadow_ranking_replays"].append(
        {
            "manifest_path": manifest_path,
            "replay_id": replay_id,
            "dataset_manifest_path": str(payload.get("dataset_manifest_path") or "").strip() or None,
            "dataset_export_id": str(payload.get("dataset_export_id") or "").strip() or None,
            "model_family": str(payload.get("model_family") or "").strip() or None,
            "model_version": str(payload.get("model_version") or "").strip() or None,
            "row_count": int(payload.get("row_count", 0) or 0),
            "created_at": str(payload.get("created_at") or "").strip() or None,
        }
    )
    for index, record in enumerate(list(payload.get("rows", []))):
        if not isinstance(record, dict):
            continue
        record_game = str(record.get("game") or "").strip() or None
        if game is not None and record_game and record_game != game:
            continue
        candidate_id = str(record.get("candidate_id") or "").strip() or None
        event_id = str(record.get("event_id") or "").strip() or None
        missing_fields = [
            name
            for name, value in (
                ("candidate_id", candidate_id),
                ("event_id", event_id),
            )
            if value is None
        ]
        if missing_fields:
            _warn_missing_fields(rows, path=path, artifact_type="shadow_ranking_replay_row", missing_fields=missing_fields, row_index=index)
        rows["shadow_ranking_replay_rows"].append(
            {
                "manifest_path": manifest_path,
                "replay_id": replay_id,
                "row_index": index,
                "candidate_id": candidate_id,
                "event_id": event_id,
                "hook_id": str(record.get("hook_id") or "").strip() or None,
                "export_id": str(record.get("export_id") or "").strip() or None,
                "post_record_id": str(record.get("post_record_id") or "").strip() or None,
                "game": record_game,
                "fixture_id": str(record.get("fixture_id") or "").strip() or None,
                "source": str(record.get("source") or "").strip() or None,
                "platform": str(record.get("platform") or "").strip() or None,
                "account_id": str(record.get("account_id") or "").strip() or None,
                "heuristic_final_score": record.get("heuristic_final_score"),
                "heuristic_recommended_action": str(record.get("heuristic_recommended_action") or "").strip() or None,
                "heuristic_lifecycle_state": str(record.get("heuristic_lifecycle_state") or "").strip() or None,
                "review_outcome": str(record.get("review_outcome") or "").strip() or None,
                "export_present": int(bool(record.get("export_present"))),
                "post_present": int(bool(record.get("post_present"))),
                "metrics_present": int(bool(record.get("metrics_present"))),
                "latest_view_count": record.get("latest_view_count"),
                "latest_engagement_rate": record.get("latest_engagement_rate"),
                "hook_mode": str(record.get("hook_mode") or "").strip() or None,
                "hook_archetype": str(record.get("hook_archetype") or "").strip() or None,
                "packaging_strategy": str(record.get("packaging_strategy") or "").strip() or None,
                "label_positive": int(bool(record.get("label_positive"))),
                "label_score": record.get("label_score"),
                "predicted_candidate_score": record.get("predicted_candidate_score"),
                "predicted_export_score": record.get("predicted_export_score"),
                "predicted_post_performance_score": record.get("predicted_post_performance_score"),
                "predicted_rank": record.get("predicted_rank"),
                "heuristic_rank": record.get("heuristic_rank"),
                "feature_values_json": json.dumps(record.get("feature_values", {}), sort_keys=True),
            }
        )
    rows["counts"]["shadow_ranking_replay_manifest_count"] += 1


def _ingest_shadow_ranking_comparison(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_RANKING_COMPARISON_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    report_path = str(path.resolve())
    comparison_id = str(payload.get("comparison_id") or "").strip() or path.stem
    recommendation = payload.get("recommendation", {}) if isinstance(payload.get("recommendation"), dict) else {}
    rows_list = list(payload.get("comparison", {}).get("rows", [])) if isinstance(payload.get("comparison"), dict) else []
    for index, row in enumerate(rows_list):
        if not isinstance(row, dict):
            continue
        row_game = str(row.get("game") or "").strip() or None
        if game is not None and row_game and row_game != game:
            continue
        rows["shadow_ranking_comparisons"].append(
            {
                "report_path": report_path,
                "comparison_id": comparison_id,
                "row_index": index,
                "replay_manifest_path": str(payload.get("replay_manifest_path") or "").strip() or None,
                "replay_id": str(payload.get("replay_id") or "").strip() or None,
                "dataset_manifest_path": str(payload.get("dataset_manifest_path") or "").strip() or None,
                "model_family": str(payload.get("model_family") or "").strip() or None,
                "model_version": str(payload.get("model_version") or "").strip() or None,
                "candidate_id": str(row.get("candidate_id") or "").strip() or None,
                "event_id": str(row.get("event_id") or "").strip() or None,
                "game": row_game,
                "fixture_id": str(row.get("fixture_id") or "").strip() or None,
                "platform": str(row.get("platform") or "").strip() or None,
                "heuristic_final_score": row.get("heuristic_final_score"),
                "predicted_candidate_score": row.get("predicted_candidate_score"),
                "heuristic_rank": row.get("heuristic_rank"),
                "predicted_rank": row.get("predicted_rank"),
                "rank_delta": row.get("rank_delta"),
                "label_positive": int(bool(row.get("label_positive"))),
                "label_score": row.get("label_score"),
                "review_outcome": str(row.get("review_outcome") or "").strip() or None,
                "export_present": int(bool(row.get("export_present"))),
                "post_present": int(bool(row.get("post_present"))),
                "latest_view_count": row.get("latest_view_count"),
                "latest_engagement_rate": row.get("latest_engagement_rate"),
                "recommendation_decision": str(recommendation.get("decision") or "").strip() or None,
                "recommendation_reason": str(recommendation.get("reason") or "").strip() or None,
            }
        )
    rows["counts"]["shadow_ranking_comparison_report_count"] += 1


def _ingest_shadow_model_family_comparison(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_MODEL_FAMILY_COMPARISON_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
    target_game = str(filters.get("game") or "").strip() or None
    if game is not None and target_game and target_game != game:
        return
    manifest_path = str(path.resolve())
    comparison_id = str(payload.get("comparison_id") or "").strip() or path.stem
    for index, record in enumerate(list(payload.get("rows", []))):
        if not isinstance(record, dict):
            continue
        rows["shadow_model_family_comparisons"].append(
            {
                "manifest_path": manifest_path,
                "comparison_id": comparison_id,
                "row_index": index,
                "source_schema_version": str(record.get("source_schema_version") or "").strip() or None,
                "model_id": str(record.get("model_id") or "").strip() or None,
                "model_family": str(record.get("model_family") or "").strip() or None,
                "model_version": str(record.get("model_version") or "").strip() or None,
                "training_target": str(record.get("training_target") or "").strip() or None,
                "recommendation_decision": str(record.get("recommendation_decision") or "").strip() or None,
                "recommendation_reason": str(record.get("recommendation_reason") or "").strip() or None,
                "primary_metric_name": str(record.get("primary_metric_name") or "").strip() or None,
                "primary_metric_delta": record.get("primary_metric_delta"),
                "experiment_id": str(record.get("experiment_id") or "").strip() or None,
                "ledger_id": str(record.get("ledger_id") or "").strip() or None,
                "game": str(record.get("game") or "").strip() or target_game,
                "platform": str(record.get("platform") or "").strip() or str(filters.get("platform") or "").strip() or None,
            }
        )
    rows["counts"]["shadow_model_family_comparison_manifest_count"] += 1


def _ingest_shadow_benchmark_matrix(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    config = payload.get("benchmark_config", {}) if isinstance(payload.get("benchmark_config"), dict) else {}
    filters = config.get("filters", {}) if isinstance(config.get("filters"), dict) else {}
    filter_game = str(filters.get("game") or "").strip() or None
    if game is not None and filter_game and filter_game != game:
        return
    manifest_path = str(path.resolve())
    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
    rows["shadow_benchmark_matrices"].append(
        {
            "manifest_path": manifest_path,
            "benchmark_id": str(payload.get("benchmark_id") or "").strip() or path.stem,
            "created_at": str(payload.get("created_at") or "").strip() or None,
            "dataset_manifest_path": str(payload.get("dataset_manifest_path") or "").strip() or None,
            "dataset_export_id": str(payload.get("dataset_export_id") or "").strip() or None,
            "policy_path": str(payload.get("policy_path") or "").strip() or None,
            "model_families_json": json.dumps(config.get("model_families", []), sort_keys=True),
            "training_targets_json": json.dumps(config.get("training_targets", []), sort_keys=True),
            "split_key": str(config.get("split_key") or "").strip() or None,
            "train_fraction": config.get("train_fraction"),
            "filters_json": json.dumps(filters, sort_keys=True),
            "run_count": payload.get("run_count"),
            "benchmark_recommendation": str(summary.get("benchmark_recommendation") or "").strip() or None,
            "blocked_run_count": summary.get("blocked_run_count"),
            "inconclusive_run_count": summary.get("inconclusive_run_count"),
            "failed_run_count": summary.get("failed_run_count"),
            "warning_count": len(list(payload.get("warnings", []))),
        }
    )
    for index, record in enumerate(list(payload.get("runs", []))):
        if not isinstance(record, dict):
            continue
        rows["shadow_benchmark_runs"].append(
            {
                "manifest_path": manifest_path,
                "benchmark_id": str(payload.get("benchmark_id") or "").strip() or path.stem,
                "run_index": index,
                "run_id": str(record.get("run_id") or "").strip() or None,
                "status": str(record.get("status") or "").strip() or None,
                "model_family": str(record.get("model_family") or "").strip() or None,
                "training_target": str(record.get("training_target") or "").strip() or None,
                "evaluation_target": str(record.get("evaluation_target") or "").strip() or None,
                "split_key": str(record.get("split_key") or config.get("split_key") or "").strip() or None,
                "train_fraction": record.get("train_fraction", config.get("train_fraction")),
                "model_manifest_path": str(record.get("model_manifest_path") or "").strip() or None,
                "experiment_manifest_path": str(record.get("experiment_manifest_path") or "").strip() or None,
                "replay_manifest_path": str(record.get("replay_manifest_path") or "").strip() or None,
                "comparison_report_path": str(record.get("comparison_report_path") or "").strip() or None,
                "governed_ledger_manifest_path": str(record.get("governed_ledger_manifest_path") or "").strip() or None,
                "recommendation_decision": str(record.get("recommendation_decision") or "").strip() or None,
                "recommendation_reason": str(record.get("recommendation_reason") or "").strip() or None,
                "coverage_status": str(record.get("coverage_status") or "").strip() or None,
                "evidence_mode": str(record.get("evidence_mode") or "").strip() or None,
                "synthetic_row_count": record.get("synthetic_row_count"),
                "real_row_count": record.get("real_row_count"),
                "primary_metric_name": str(record.get("primary_metric_name") or "").strip() or None,
                "primary_metric_delta": record.get("primary_metric_delta"),
                "protected_regression_count": record.get("protected_regression_count"),
                "blocking_reasons_json": json.dumps(record.get("blocking_reasons", []), sort_keys=True),
                "failure_reason": str(record.get("failure_reason") or "").strip() or None,
                "game": filter_game,
                "platform": str(filters.get("platform") or "").strip() or None,
            }
        )
    rows["counts"]["shadow_benchmark_matrix_manifest_count"] += 1


def _ingest_shadow_benchmark_review(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_BENCHMARK_REVIEW_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    manifest_path = str(path.resolve())
    review_targets = [row for row in list(payload.get("target_reviews", [])) if isinstance(row, dict)]
    reviewed_targets = [str(item) for item in list(payload.get("reviewed_targets", [])) if str(item).strip()]
    reviewed_families = [str(item) for item in list(payload.get("reviewed_families", [])) if str(item).strip()]
    filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
    target_game = str(filters.get("game") or "").strip() or None
    if game is not None and target_game and target_game != game:
        return
    aggregate = payload.get("aggregate_conclusions", {}) if isinstance(payload.get("aggregate_conclusions"), dict) else {}
    rows["shadow_benchmark_reviews"].append(
        {
            "manifest_path": manifest_path,
            "review_id": str(payload.get("review_id") or "").strip() or path.stem,
            "created_at": str(payload.get("created_at") or "").strip() or None,
            "source_benchmark_manifest_paths_json": json.dumps(payload.get("source_benchmark_manifest_paths", []), sort_keys=True),
            "reviewed_targets_json": json.dumps(reviewed_targets, sort_keys=True),
            "reviewed_families_json": json.dumps(reviewed_families, sort_keys=True),
            "filters_json": json.dumps(filters, sort_keys=True),
            "target_count": len(review_targets),
            "ready_target_count": aggregate.get("ready_target_count"),
            "label_calibration_target_count": aggregate.get("label_calibration_target_count"),
            "feature_cleanup_target_count": aggregate.get("feature_cleanup_target_count"),
            "coverage_blocked_target_count": aggregate.get("coverage_blocked_target_count"),
            "warning_count": payload.get("warning_count", len(list(payload.get("warnings", [])))),
        }
    )
    for index, record in enumerate(review_targets):
        rows["shadow_target_readiness"].append(
            {
                "manifest_path": manifest_path,
                "review_id": str(payload.get("review_id") or "").strip() or path.stem,
                "target_index": index,
                "training_target": str(record.get("training_target") or "").strip() or None,
                "current_best_family": str(record.get("current_best_family") or "").strip() or None,
                "best_recommendation_decision": str(record.get("best_recommendation_decision") or "").strip() or None,
                "current_best_evidence_mode": str(record.get("current_best_evidence_mode") or "").strip() or None,
                "evidence_modes_json": json.dumps(record.get("evidence_modes", []), sort_keys=True),
                "synthetic_augmented_run_count": record.get("synthetic_augmented_run_count"),
                "real_only_run_count": record.get("real_only_run_count"),
                "primary_metric_name": str(record.get("primary_metric_name") or "").strip() or None,
                "primary_metric_delta": record.get("primary_metric_delta"),
                "run_count": record.get("run_count"),
                "successful_run_count": record.get("successful_run_count"),
                "win_count": record.get("win_count"),
                "keep_current_count": record.get("keep_current_count"),
                "blocked_count": record.get("blocked_count"),
                "inconclusive_count": record.get("inconclusive_count"),
                "failed_count": record.get("failed_count"),
                "dominant_failure_modes_json": json.dumps(record.get("dominant_failure_modes", []), sort_keys=True),
                "confidence_level": str(record.get("confidence_level") or "").strip() or None,
                "readiness_classification": str(record.get("readiness_classification") or "").strip() or None,
                "recommended_next_action": str(record.get("recommended_next_action") or "").strip() or None,
                "game": str(record.get("game") or "").strip() or target_game,
                "platform": str(record.get("platform") or "").strip() or str(filters.get("platform") or "").strip() or None,
            }
        )
    rows["counts"]["shadow_benchmark_review_manifest_count"] += 1


def _ingest_real_posted_lineage_import(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != REAL_POSTED_LINEAGE_IMPORT_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
    target_game = str(filters.get("game") or "").strip() or None
    if game is not None and target_game and target_game != game:
        return
    rows["real_posted_lineage_imports"].append(
        {
            "manifest_path": str(path.resolve()),
            "import_id": str(payload.get("import_id") or "").strip() or path.stem,
            "created_at": str(payload.get("created_at") or "").strip() or None,
            "workspace_root": str(payload.get("workspace_root") or "").strip() or None,
            "registry_path": str(payload.get("registry_path") or "").strip() or None,
            "refresh_root": str(payload.get("refresh_root") or "").strip() or None,
            "source_roots_json": json.dumps(payload.get("source_roots", []), sort_keys=True),
            "scanned_roots_json": json.dumps(payload.get("scanned_roots", []), sort_keys=True),
            "filters_json": json.dumps(filters, sort_keys=True),
            "workspace_artifact_count": payload.get("workspace_artifact_count"),
            "source_artifact_count": payload.get("source_artifact_count"),
            "discovered_counts_json": json.dumps(payload.get("discovered_counts", {}), sort_keys=True),
            "imported_counts_json": json.dumps(payload.get("imported_counts", {}), sort_keys=True),
            "coverage_inventory_json": json.dumps(payload.get("coverage_inventory", {}), sort_keys=True),
            "source_root_summaries_json": json.dumps(payload.get("source_root_summaries", []), sort_keys=True),
            "unresolved_lineage_counts_json": json.dumps(payload.get("unresolved_lineage_counts", {}), sort_keys=True),
            "eligible_real_post_performance_label_count": (payload.get("coverage_inventory") or {}).get(
                "eligible_real_post_performance_label_count"
            ),
            "imported_candidate_count": (payload.get("coverage_inventory") or {}).get("imported_candidate_count"),
            "imported_hook_count": (payload.get("coverage_inventory") or {}).get("imported_hook_count"),
            "warning_count": payload.get("warning_count"),
            "game": target_game,
            "platform": str(filters.get("platform") or "").strip() or None,
        }
    )
    rows["counts"]["real_posted_lineage_import_manifest_count"] += 1


def _ingest_shadow_benchmark_evidence_comparison(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != SHADOW_BENCHMARK_EVIDENCE_COMPARISON_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
    target_game = str(filters.get("game") or "").strip() or None
    if game is not None and target_game and target_game != game:
        return
    manifest_path = str(path.resolve())
    for index, record in enumerate(list(payload.get("rows", []))):
        if not isinstance(record, dict):
            continue
        rows["shadow_benchmark_evidence_comparisons"].append(
            {
                "manifest_path": manifest_path,
                "comparison_id": str(payload.get("comparison_id") or "").strip() or path.stem,
                "row_index": index,
                "training_target": str(record.get("training_target") or "").strip() or None,
                "real_manifest_path": str(record.get("real_manifest_path") or "").strip() or None,
                "synthetic_manifest_path": str(record.get("synthetic_manifest_path") or "").strip() or None,
                "real_current_best_family": str(record.get("real_current_best_family") or "").strip() or None,
                "synthetic_current_best_family": str(record.get("synthetic_current_best_family") or "").strip() or None,
                "real_best_recommendation_decision": str(record.get("real_best_recommendation_decision") or "").strip() or None,
                "synthetic_best_recommendation_decision": str(record.get("synthetic_best_recommendation_decision") or "").strip() or None,
                "real_current_best_evidence_mode": str(record.get("real_current_best_evidence_mode") or "").strip() or None,
                "synthetic_current_best_evidence_mode": str(record.get("synthetic_current_best_evidence_mode") or "").strip() or None,
                "real_readiness_classification": str(record.get("real_readiness_classification") or "").strip() or None,
                "synthetic_readiness_classification": str(record.get("synthetic_readiness_classification") or "").strip() or None,
                "real_primary_metric_name": str(record.get("real_primary_metric_name") or "").strip() or None,
                "synthetic_primary_metric_name": str(record.get("synthetic_primary_metric_name") or "").strip() or None,
                "real_primary_metric_delta": record.get("real_primary_metric_delta"),
                "synthetic_primary_metric_delta": record.get("synthetic_primary_metric_delta"),
                "primary_metric_delta_gap": record.get("primary_metric_delta_gap"),
                "real_confidence_level": str(record.get("real_confidence_level") or "").strip() or None,
                "synthetic_confidence_level": str(record.get("synthetic_confidence_level") or "").strip() or None,
                "real_successful_run_count": record.get("real_successful_run_count"),
                "synthetic_successful_run_count": record.get("synthetic_successful_run_count"),
                "real_run_count": record.get("real_run_count"),
                "synthetic_run_count": record.get("synthetic_run_count"),
                "family_winner_changed": int(bool(record.get("family_winner_changed"))),
                "readiness_changed": int(bool(record.get("readiness_changed"))),
                "recommendation_changed": int(bool(record.get("recommendation_changed"))),
                "disagreement_indicators_json": json.dumps(record.get("disagreement_indicators", []), sort_keys=True),
                "game": str(record.get("game") or "").strip() or target_game,
                "platform": str(record.get("platform") or "").strip() or str(filters.get("platform") or "").strip() or None,
            }
        )
    rows["counts"]["shadow_benchmark_evidence_comparison_manifest_count"] += 1


def _ingest_real_artifact_intake_dashboard(path: Path, rows: dict[str, Any], *, game: str | None) -> None:
    payload = _load_json(path, rows)
    if payload is None:
        return
    if payload.get("schema_version") != REAL_ARTIFACT_INTAKE_DASHBOARD_SCHEMA_VERSION:
        _warning(rows, path=path, reason="unsupported_schema_version", detail=str(payload.get("schema_version")))
        return
    filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
    target_game = str(filters.get("game") or "").strip() or None
    if game is not None and target_game and target_game != game:
        return
    current_intake = payload.get("current_intake", {}) if isinstance(payload.get("current_intake"), dict) else {}
    coverage_inventory = current_intake.get("coverage_inventory", {}) if isinstance(current_intake.get("coverage_inventory"), dict) else {}
    preflight_trends = payload.get("preflight_trends", {}) if isinstance(payload.get("preflight_trends"), dict) else {}
    refresh_outcome_trends = payload.get("refresh_outcome_trends", {}) if isinstance(payload.get("refresh_outcome_trends"), dict) else {}
    history_comparison = payload.get("history_comparison", {}) if isinstance(payload.get("history_comparison"), dict) else {}
    history_alignment = history_comparison.get("history_alignment", {}) if isinstance(history_comparison.get("history_alignment"), dict) else {}
    rows["real_artifact_intake_dashboards"].append(
        {
            "manifest_path": str(path.resolve()),
            "generated_at": str(payload.get("generated_at") or "").strip() or None,
            "intake_root": str(payload.get("intake_root") or "").strip() or None,
            "filters_json": json.dumps(filters, sort_keys=True),
            "headline_status": str(payload.get("headline_status") or "").strip() or None,
            "intake_status": str(current_intake.get("intake_status") or "").strip() or None,
            "bundle_count": current_intake.get("bundle_count"),
            "warning_count": current_intake.get("warning_count"),
            "benchmark_ready_bundle_count": ((current_intake.get("bundle_readiness_rollups") or {}).get("readiness_status_counts") or {}).get("benchmark_ready"),
            "eligible_real_post_performance_label_count": coverage_inventory.get("eligible_real_post_performance_label_count"),
            "preflight_trend_status": str(preflight_trends.get("trend_status") or "").strip() or None,
            "preflight_entry_count": preflight_trends.get("entry_count"),
            "refresh_outcome_trend_status": str(refresh_outcome_trends.get("trend_status") or "").strip() or None,
            "refresh_outcome_entry_count": refresh_outcome_trends.get("entry_count"),
            "history_alignment_status": str(history_alignment.get("preflight_to_refresh_status") or "").strip() or None,
            "real_vs_synthetic_gap_status": str(history_alignment.get("real_vs_synthetic_status") or "").strip() or None,
            "next_focus": str(history_alignment.get("next_focus") or "").strip() or None,
            "game": target_game,
            "platform": str(filters.get("platform") or "").strip() or None,
        }
    )
    rows["counts"]["real_artifact_intake_dashboard_manifest_count"] += 1


def _highlight_selection_manifest_path_for_candidate(rows: dict[str, Any], *, candidate_id: str) -> str | None:
    normalized = str(candidate_id or "").strip()
    if not normalized:
        return None
    for lifecycle_row in _derive_candidate_lifecycle_rows(rows):
        if str(lifecycle_row.get("candidate_id") or "").strip() == normalized:
            return str(lifecycle_row.get("highlight_selection_manifest_path") or "").strip() or None
    return None


def _game_for_export_manifest(rows: dict[str, Any], manifest_path: str) -> str | None:
    normalized = str(manifest_path or "").strip()
    if not normalized:
        return None
    for batch in rows.get("highlight_export_batches", []):
        if str(batch.get("manifest_path") or "").strip() == normalized:
            return str(batch.get("game") or "").strip() or None
    return None


def _metrics_candidate_details(
    rows: dict[str, Any],
    *,
    post_record_id: str | None,
    export_id: str | None,
    candidate_id: str | None,
) -> dict[str, Any]:
    normalized_post_record_id = str(post_record_id or "").strip()
    if normalized_post_record_id:
        post_row = _post_record_details(rows, post_record_id=normalized_post_record_id)
        if post_row is not None:
            export_match = _export_details(rows, export_id=str(post_row.get("export_id") or "").strip(), candidate_id=str(post_row.get("candidate_id") or "").strip())
            if export_match:
                return {
                    "candidate_id": export_match.get("candidate_id"),
                    "hook_id": export_match.get("hook_id") or post_row.get("hook_id"),
                    "game": export_match.get("game") or post_row.get("game"),
                }
            return {
                "candidate_id": post_row.get("candidate_id"),
                "hook_id": post_row.get("hook_id"),
                "game": post_row.get("game"),
            }
    export_match = _export_details(rows, export_id=export_id, candidate_id=candidate_id)
    if export_match:
        return {
            "candidate_id": export_match.get("candidate_id"),
            "hook_id": export_match.get("hook_id"),
            "game": export_match.get("game"),
        }
    return {
        "candidate_id": str(candidate_id or "").strip() or None,
        "hook_id": None,
        "game": None,
    }


def _export_details(rows: dict[str, Any], *, export_id: str | None, candidate_id: str | None) -> dict[str, Any] | None:
    normalized_export_id = str(export_id or "").strip()
    if normalized_export_id:
        for export_row in rows.get("highlight_exports", []):
            if str(export_row.get("export_id") or "").strip() == normalized_export_id:
                return export_row
    normalized_candidate_id = str(candidate_id or "").strip()
    if normalized_candidate_id:
        for export_row in rows.get("highlight_exports", []):
            if str(export_row.get("candidate_id") or "").strip() == normalized_candidate_id:
                return export_row
    return None


def _post_record_details(rows: dict[str, Any], *, post_record_id: str | None) -> dict[str, Any] | None:
    normalized_post_record_id = str(post_record_id or "").strip()
    if not normalized_post_record_id:
        return None
    for post_row in rows.get("posted_highlights", []):
        if str(post_row.get("post_record_id") or "").strip() == normalized_post_record_id:
            return post_row
    return None


def _candidate_id(*, game: str, source: str, fused_sidecar_path: str, event_id: str) -> str:
    digest = hashlib.sha1(
        "::".join([game.strip(), source.strip(), fused_sidecar_path.strip(), event_id.strip()]).encode("utf-8")
    ).hexdigest()[:16]
    return f"candidate-{digest}"


def _derive_candidate_lifecycle_rows(collected: dict[str, Any]) -> list[dict[str, Any]]:
    selected_manifests_by_source: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for manifest in collected.get("highlight_selection_manifests", []):
        key = (str(manifest.get("game") or "").strip(), str(manifest.get("source") or "").strip())
        if not key[0] or not key[1]:
            continue
        selected_manifests_by_source.setdefault(key, []).append(manifest)
    export_by_candidate: dict[str, dict[str, Any]] = {}
    for export_row in collected.get("highlight_exports", []):
        candidate_id = str(export_row.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        export_by_candidate[candidate_id] = export_row
    post_by_candidate: dict[str, dict[str, Any]] = {}
    for post_row in collected.get("posted_highlights", []):
        candidate_id = str(post_row.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        post_by_candidate[candidate_id] = post_row

    rows: list[dict[str, Any]] = []
    for event in collected.get("fused_events", []):
        game = str(event.get("game") or "").strip()
        source = str(event.get("source") or "").strip()
        sidecar_path = str(event.get("sidecar_path") or "").strip()
        event_id = str(event.get("event_id") or "").strip()
        if not game or not source or not sidecar_path or not event_id:
            continue
        candidate_id = _candidate_id(game=game, source=source, fused_sidecar_path=sidecar_path, event_id=event_id)
        review_status = _normalized_review_status(event.get("review_status"))
        recommended_action = str(event.get("recommended_action") or "").strip().lower() or None
        selection_manifest = _selection_manifest_details_for_event(
            selected_manifests_by_source.get((game, source), []),
            event,
            candidate_id=candidate_id,
            game=game,
            source=source,
            fused_sidecar_path=sidecar_path,
            event_id=event_id,
        )
        export_row = export_by_candidate.get(candidate_id)
        post_row = post_by_candidate.get(candidate_id)
        lifecycle_state = _derived_lifecycle_state(
            review_status=review_status,
            recommended_action=recommended_action,
            selection_manifest=selection_manifest,
            export_row=export_row,
            post_row=post_row,
        )
        if lifecycle_state is None:
            continue
        rows.append(
            {
                "candidate_id": candidate_id,
                "game": game,
                "source": source,
                "fixture_id": _fixture_id_for_event(collected, sidecar_path=sidecar_path, game=game, source=source),
                "event_id": event_id,
                "fused_sidecar_path": sidecar_path,
                "lifecycle_state": lifecycle_state,
                "latest_review_status": review_status,
                "recommended_action": recommended_action,
                "final_score": event.get("final_score"),
                "has_review_disagreement": _clip_disagreement_value(
                    collected,
                    game=game,
                    source=source,
                    key="has_review_disagreement",
                ),
                "has_cross_layer_disagreement": _clip_disagreement_value(
                    collected,
                    game=game,
                    source=source,
                    key="has_cross_layer_disagreement",
                ),
                "has_trial_preference": _clip_disagreement_value(
                    collected,
                    game=game,
                    source=source,
                    key="has_trial_preference",
                ),
                "selection_basis": (selection_manifest or {}).get("selection_basis"),
                "highlight_selection_manifest_path": (selection_manifest or {}).get("manifest_path"),
                "selected_highlight_details_json": json.dumps(
                    (selection_manifest or {}).get("selected_highlight_details") or {},
                    sort_keys=True,
                ),
                "export_artifact_path": str((export_row or {}).get("export_artifact_path") or "").strip() or None,
                "post_ledger_path": str((post_row or {}).get("manifest_path") or "").strip() or None,
                "last_seen_at": _utc_now(),
            }
        )
    return rows


def _derived_lifecycle_state(
    *,
    review_status: str | None,
    recommended_action: str | None,
    selection_manifest: dict[str, Any] | None,
    export_row: dict[str, Any] | None,
    post_row: dict[str, Any] | None,
) -> str | None:
    if post_row is not None:
        return "posted"
    if export_row is not None and selection_manifest is not None and review_status == "approved":
        return "exported"
    if selection_manifest is not None and review_status == "approved":
        return "selected_for_export"
    if review_status == "approved":
        return "approved"
    if review_status == "rejected":
        return "rejected"
    if recommended_action and recommended_action != "skip":
        return "pending_review"
    return None


def _selection_manifest_details_for_event(
    selection_manifests: list[dict[str, Any]],
    event: dict[str, Any],
    *,
    candidate_id: str,
    game: str,
    source: str,
    fused_sidecar_path: str,
    event_id: str,
) -> dict[str, Any] | None:
    event_start = float(event.get("suggested_start_timestamp", 0.0) or 0.0)
    event_end = float(event.get("suggested_end_timestamp", event_start) or event_start)
    for manifest in selection_manifests:
        selection_basis = str(manifest.get("selection_basis") or "proxy").strip() or "proxy"
        manifest_sidecar_path = str(manifest.get("fused_sidecar_path") or "").strip()
        for highlight in list(manifest.get("selected_highlights", [])):
            if not isinstance(highlight, dict):
                continue
            if selection_basis == "fused":
                highlight_candidate_id = str(highlight.get("candidate_id") or "").strip()
                if highlight_candidate_id and highlight_candidate_id == candidate_id:
                    return {
                        "manifest_path": str(manifest.get("manifest_path") or "").strip() or None,
                        "selection_basis": selection_basis,
                        "selected_highlight_details": _selected_highlight_details(highlight),
                    }
                if (
                    str(highlight.get("event_id") or "").strip() == event_id
                    and manifest_sidecar_path
                    and manifest_sidecar_path == fused_sidecar_path
                    and str(manifest.get("game") or "").strip() == game
                    and str(manifest.get("source") or "").strip() == source
                ):
                    return {
                        "manifest_path": str(manifest.get("manifest_path") or "").strip() or None,
                        "selection_basis": selection_basis,
                        "selected_highlight_details": _selected_highlight_details(highlight),
                    }
            start_seconds = float(highlight.get("start_seconds", 0.0) or 0.0)
            end_seconds = float(highlight.get("end_seconds", start_seconds) or start_seconds)
            if _ranges_overlap(event_start, event_end, start_seconds, end_seconds):
                return {
                    "manifest_path": str(manifest.get("manifest_path") or "").strip() or None,
                    "selection_basis": selection_basis,
                    "selected_highlight_details": _selected_highlight_details(highlight),
                }
    return None


def _selection_manifest_details_for_export(
    rows: dict[str, Any],
    *,
    manifest_path: str | None,
    candidate_id: str | None,
    event_id: str | None,
    game: str | None,
    source: str | None,
    fused_sidecar_path: str | None,
) -> dict[str, Any] | None:
    normalized_manifest_path = str(manifest_path or "").strip()
    normalized_candidate_id = str(candidate_id or "").strip()
    normalized_event_id = str(event_id or "").strip()
    normalized_game = str(game or "").strip()
    normalized_source = str(source or "").strip()
    normalized_sidecar_path = str(fused_sidecar_path or "").strip()
    for manifest in rows.get("highlight_selection_manifests", []):
        manifest_manifest_path = str(manifest.get("manifest_path") or "").strip()
        if normalized_manifest_path and manifest_manifest_path != normalized_manifest_path:
            continue
        manifest_game = str(manifest.get("game") or "").strip()
        manifest_source = str(manifest.get("source") or "").strip()
        manifest_sidecar_path = str(manifest.get("fused_sidecar_path") or "").strip()
        for highlight in list(manifest.get("selected_highlights", [])):
            if not isinstance(highlight, dict):
                continue
            if normalized_candidate_id and str(highlight.get("candidate_id") or "").strip() == normalized_candidate_id:
                return _selected_highlight_details(highlight)
            if (
                normalized_event_id
                and str(highlight.get("event_id") or "").strip() == normalized_event_id
                and (not normalized_game or manifest_game == normalized_game)
                and (not normalized_source or manifest_source == normalized_source)
                and (not normalized_sidecar_path or manifest_sidecar_path == normalized_sidecar_path)
            ):
                return _selected_highlight_details(highlight)
    return None


def _selected_highlight_details(highlight: dict[str, Any]) -> dict[str, Any]:
    details: dict[str, Any] = {}
    for key in (
        "highlight_id",
        "candidate_id",
        "fusion_id",
        "event_id",
        "start_seconds",
        "end_seconds",
        "final_score",
        "recommended_action",
        "gate_status",
        "event_type",
        "entity_id",
    ):
        value = highlight.get(key)
        if value not in (None, "", [], {}):
            details[key] = value
    if isinstance(highlight.get("contributing_signal_ids"), list):
        details["contributing_signal_ids"] = [str(item) for item in highlight["contributing_signal_ids"] if str(item).strip()]
    if isinstance(highlight.get("contributing_producer_families"), list):
        details["contributing_producer_families"] = [
            str(item) for item in highlight["contributing_producer_families"] if str(item).strip()
        ]
    if isinstance(highlight.get("metadata_summary"), dict) and highlight["metadata_summary"]:
        details["metadata_summary"] = dict(highlight["metadata_summary"])
    return details


def _ranges_overlap(start_a: float, end_a: float, start_b: float, end_b: float) -> bool:
    return max(start_a, start_b) <= min(end_a, end_b)


def _fixture_id_for_event(collected: dict[str, Any], *, sidecar_path: str, game: str, source: str) -> str | None:
    normalized_sidecar = str(Path(sidecar_path).resolve())
    for row in collected.get("fixture_comparisons", []):
        if normalized_sidecar in {
            str(row.get("baseline_sidecar_path") or "").strip(),
            str(row.get("trial_sidecar_path") or "").strip(),
        }:
            fixture_id = str(row.get("fixture_id") or "").strip()
            if fixture_id:
                return fixture_id
    for row in collected.get("fixture_trial_run_fixtures", []):
        if normalized_sidecar in {
            str(row.get("proxy_sidecar_path") or "").strip(),
            str(row.get("runtime_sidecar_path") or "").strip(),
            str(row.get("fused_sidecar_path") or "").strip(),
        }:
            fixture_id = str(row.get("fixture_id") or "").strip()
            if fixture_id:
                return fixture_id
    clips = collected.get("clips", [])
    if isinstance(clips, dict):
        iterable = clips.values()
    else:
        iterable = clips
    for clip in iterable:
        if str(clip.get("game") or "") == game and str(clip.get("source") or "") == source:
            fixture_ids = _load_json_list(clip.get("fixture_ids_json"))
            if fixture_ids:
                return fixture_ids[0]
    return None


def _clip_disagreement_value(collected: dict[str, Any], *, game: str, source: str, key: str) -> int:
    clips = collected.get("clips", [])
    if isinstance(clips, dict):
        iterable = clips.values()
    else:
        iterable = clips
    for clip in iterable:
        if str(clip.get("game") or "") == game and str(clip.get("source") or "") == source:
            return int(bool(clip.get(key)))
    return 0


def _write_registry(registry_path: Path, root: Path, collected: dict[str, Any], *, game: str | None) -> dict[str, Any]:
    connection = sqlite3.connect(str(registry_path))
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        _create_schema(connection)
        with connection:
            _clear_mirror_tables(connection)
            run_id = _insert_ingest_run(connection, registry_path, root, collected, game=game)
            _bulk_insert(connection, "clips", collected["clips"], _CLIP_COLUMNS)
            _bulk_insert(connection, "proxy_windows", collected["proxy_windows"], _PROXY_WINDOW_COLUMNS)
            _bulk_insert(connection, "runtime_analyses", collected["runtime_analyses"], _RUNTIME_ANALYSIS_COLUMNS)
            _bulk_insert(connection, "runtime_events", collected["runtime_events"], _RUNTIME_EVENT_COLUMNS)
            _bulk_insert(connection, "runtime_detections", collected["runtime_detections"], _RUNTIME_DETECTION_COLUMNS)
            _bulk_insert(connection, "fused_analyses", collected["fused_analyses"], _FUSED_ANALYSIS_COLUMNS)
            _bulk_insert(connection, "fused_events", collected["fused_events"], _FUSED_EVENT_COLUMNS)
            _bulk_insert(connection, "fused_signal_refs", collected["fused_signal_refs"], _FUSED_SIGNAL_REF_COLUMNS)
            _bulk_insert(connection, "runtime_review_sessions", collected["runtime_review_sessions"], _RUNTIME_REVIEW_SESSION_COLUMNS)
            _bulk_insert(connection, "runtime_review_items", collected["runtime_review_items"], _RUNTIME_REVIEW_ITEM_COLUMNS)
            _bulk_insert(connection, "fused_review_sessions", collected["fused_review_sessions"], _FUSED_REVIEW_SESSION_COLUMNS)
            _bulk_insert(connection, "fused_review_items", collected["fused_review_items"], _FUSED_REVIEW_ITEM_COLUMNS)
            _bulk_insert(connection, "fixture_comparisons", collected["fixture_comparisons"], _FIXTURE_COMPARISON_COLUMNS)
            _bulk_insert(connection, "fixture_trial_runs", collected["fixture_trial_runs"], _FIXTURE_TRIAL_RUN_COLUMNS)
            _bulk_insert(connection, "fixture_trial_run_fixtures", collected["fixture_trial_run_fixtures"], _FIXTURE_TRIAL_RUN_FIXTURE_COLUMNS)
            _bulk_insert(connection, "fixture_trial_batches", collected["fixture_trial_batches"], _FIXTURE_TRIAL_BATCH_COLUMNS)
            _bulk_insert(connection, "fixture_trial_batch_comparisons", collected["fixture_trial_batch_comparisons"], _FIXTURE_TRIAL_BATCH_COMPARISON_COLUMNS)
            _bulk_insert(connection, "hook_candidate_manifests", collected["hook_candidate_manifests"], _HOOK_CANDIDATE_MANIFEST_COLUMNS)
            _bulk_insert(connection, "hook_candidates", collected["hook_candidates"], _HOOK_CANDIDATE_COLUMNS)
            _bulk_insert(connection, "hook_comparison_reports", collected["hook_comparison_reports"], _HOOK_COMPARISON_REPORT_COLUMNS)
            _bulk_insert(connection, "hook_comparisons", collected["hook_comparisons"], _HOOK_COMPARISON_COLUMNS)
            _bulk_insert(connection, "hook_evaluation_reports", collected["hook_evaluation_reports"], _HOOK_EVALUATION_REPORT_COLUMNS)
            _bulk_insert(connection, "highlight_export_batches", collected["highlight_export_batches"], _HIGHLIGHT_EXPORT_BATCH_COLUMNS)
            _bulk_insert(connection, "highlight_exports", collected["highlight_exports"], _HIGHLIGHT_EXPORT_COLUMNS)
            _bulk_insert(connection, "post_ledgers", collected["post_ledgers"], _POST_LEDGER_COLUMNS)
            _bulk_insert(connection, "posted_highlights", collected["posted_highlights"], _POSTED_HIGHLIGHT_COLUMNS)
            _bulk_insert(connection, "posted_metrics_snapshots", collected["posted_metrics_snapshots"], _POSTED_METRICS_SNAPSHOT_COLUMNS)
            _bulk_insert(connection, "posted_metrics_snapshot_rows", collected["posted_metrics_snapshot_rows"], _POSTED_METRICS_SNAPSHOT_ROW_COLUMNS)
            _bulk_insert(connection, "shadow_evaluation_policies", collected["shadow_evaluation_policies"], _SHADOW_EVALUATION_POLICY_COLUMNS)
            _bulk_insert(connection, "shadow_ranking_models", collected["shadow_ranking_models"], _SHADOW_RANKING_MODEL_COLUMNS)
            _bulk_insert(connection, "shadow_ranking_experiments", collected["shadow_ranking_experiments"], _SHADOW_RANKING_EXPERIMENT_COLUMNS)
            _bulk_insert(connection, "shadow_ranking_experiment_ledgers", collected["shadow_ranking_experiment_ledgers"], _SHADOW_RANKING_EXPERIMENT_LEDGER_COLUMNS)
            _bulk_insert(connection, "shadow_ranking_experiment_slices", collected["shadow_ranking_experiment_slices"], _SHADOW_RANKING_EXPERIMENT_SLICE_COLUMNS)
            _bulk_insert(connection, "shadow_ranking_replays", collected["shadow_ranking_replays"], _SHADOW_RANKING_REPLAY_COLUMNS)
            _bulk_insert(connection, "shadow_ranking_replay_rows", collected["shadow_ranking_replay_rows"], _SHADOW_RANKING_REPLAY_ROW_COLUMNS)
            _bulk_insert(connection, "shadow_ranking_comparisons", collected["shadow_ranking_comparisons"], _SHADOW_RANKING_COMPARISON_COLUMNS)
            _bulk_insert(connection, "shadow_model_family_comparisons", collected["shadow_model_family_comparisons"], _SHADOW_MODEL_FAMILY_COMPARISON_COLUMNS)
            _bulk_insert(connection, "shadow_benchmark_matrices", collected["shadow_benchmark_matrices"], _SHADOW_BENCHMARK_MATRIX_COLUMNS)
            _bulk_insert(connection, "shadow_benchmark_runs", collected["shadow_benchmark_runs"], _SHADOW_BENCHMARK_RUN_COLUMNS)
            _bulk_insert(connection, "shadow_benchmark_reviews", collected["shadow_benchmark_reviews"], _SHADOW_BENCHMARK_REVIEW_COLUMNS)
            _bulk_insert(connection, "shadow_target_readiness", collected["shadow_target_readiness"], _SHADOW_TARGET_READINESS_COLUMNS)
            _bulk_insert(connection, "real_posted_lineage_imports", collected["real_posted_lineage_imports"], _REAL_POSTED_LINEAGE_IMPORT_COLUMNS)
            _bulk_insert(connection, "shadow_benchmark_evidence_comparisons", collected["shadow_benchmark_evidence_comparisons"], _SHADOW_BENCHMARK_EVIDENCE_COMPARISON_COLUMNS)
            _bulk_insert(connection, "real_artifact_intake_dashboards", collected["real_artifact_intake_dashboards"], _REAL_ARTIFACT_INTAKE_DASHBOARD_COLUMNS)
            _bulk_insert(connection, "workflow_runs", collected["workflow_runs"], _WORKFLOW_RUN_COLUMNS)
            _bulk_insert(connection, "workflow_run_items", collected["workflow_run_items"], _WORKFLOW_RUN_ITEM_COLUMNS)
            _sync_candidate_lifecycles(connection, collected)
            _insert_ingest_warnings(connection, run_id, collected["warnings"])
    finally:
        connection.close()

    counts = collected["counts"]
    return {
        "ok": True,
        "registry_path": str(registry_path),
        "refresh_root": str(root),
        "game_filter": game,
        "proxy_sidecar_count": counts["proxy_sidecar_count"],
        "runtime_sidecar_count": counts["runtime_sidecar_count"],
        "fused_sidecar_count": counts["fused_sidecar_count"],
        "runtime_review_session_count": counts["runtime_review_session_count"],
        "fused_review_session_count": counts["fused_review_session_count"],
        "fixture_comparison_report_count": counts["fixture_comparison_report_count"],
        "fixture_trial_run_manifest_count": counts["fixture_trial_run_manifest_count"],
        "fixture_trial_batch_manifest_count": counts["fixture_trial_batch_manifest_count"],
        "clip_row_count": len(collected["clips"]),
        "proxy_window_row_count": len(collected["proxy_windows"]),
        "runtime_analysis_row_count": len(collected["runtime_analyses"]),
        "runtime_event_row_count": len(collected["runtime_events"]),
        "runtime_detection_row_count": len(collected["runtime_detections"]),
        "fused_analysis_row_count": len(collected["fused_analyses"]),
        "fused_event_row_count": len(collected["fused_events"]),
        "fused_signal_ref_row_count": len(collected["fused_signal_refs"]),
        "runtime_review_item_row_count": len(collected["runtime_review_items"]),
        "fused_review_item_row_count": len(collected["fused_review_items"]),
        "fixture_comparison_row_count": len(collected["fixture_comparisons"]),
        "fixture_trial_run_row_count": len(collected["fixture_trial_runs"]),
        "fixture_trial_run_fixture_row_count": len(collected["fixture_trial_run_fixtures"]),
        "fixture_trial_batch_row_count": len(collected["fixture_trial_batches"]),
        "fixture_trial_batch_comparison_row_count": len(collected["fixture_trial_batch_comparisons"]),
        "highlight_selection_manifest_count": counts["highlight_selection_manifest_count"],
        "hook_candidate_manifest_count": counts["hook_candidate_manifest_count"],
        "hook_candidate_row_count": len(collected["hook_candidates"]),
        "hook_comparison_report_count": counts["hook_comparison_report_count"],
        "hook_comparison_row_count": len(collected["hook_comparisons"]),
        "hook_evaluation_report_count": len(collected["hook_evaluation_reports"]),
        "highlight_export_batch_manifest_count": counts["highlight_export_batch_manifest_count"],
        "highlight_export_row_count": len(collected["highlight_exports"]),
        "post_ledger_manifest_count": counts["post_ledger_manifest_count"],
        "posted_highlight_row_count": len(collected["posted_highlights"]),
        "posted_metrics_snapshot_manifest_count": counts["posted_metrics_snapshot_manifest_count"],
        "posted_metrics_snapshot_row_count": len(collected["posted_metrics_snapshot_rows"]),
        "shadow_evaluation_policy_manifest_count": counts["shadow_evaluation_policy_manifest_count"],
        "shadow_ranking_model_manifest_count": counts["shadow_ranking_model_manifest_count"],
        "shadow_ranking_experiment_manifest_count": counts["shadow_ranking_experiment_manifest_count"],
        "shadow_ranking_experiment_ledger_manifest_count": counts["shadow_ranking_experiment_ledger_manifest_count"],
        "shadow_ranking_experiment_slice_row_count": len(collected["shadow_ranking_experiment_slices"]),
        "shadow_ranking_replay_manifest_count": counts["shadow_ranking_replay_manifest_count"],
        "shadow_ranking_replay_row_count": len(collected["shadow_ranking_replay_rows"]),
        "shadow_ranking_comparison_report_count": counts["shadow_ranking_comparison_report_count"],
        "shadow_ranking_comparison_row_count": len(collected["shadow_ranking_comparisons"]),
        "shadow_model_family_comparison_manifest_count": counts["shadow_model_family_comparison_manifest_count"],
        "shadow_model_family_comparison_row_count": len(collected["shadow_model_family_comparisons"]),
        "shadow_benchmark_matrix_manifest_count": counts["shadow_benchmark_matrix_manifest_count"],
        "shadow_benchmark_run_row_count": len(collected["shadow_benchmark_runs"]),
        "shadow_benchmark_review_manifest_count": counts["shadow_benchmark_review_manifest_count"],
        "shadow_target_readiness_row_count": len(collected["shadow_target_readiness"]),
        "real_posted_lineage_import_manifest_count": counts["real_posted_lineage_import_manifest_count"],
        "shadow_benchmark_evidence_comparison_row_count": len(collected["shadow_benchmark_evidence_comparisons"]),
        "real_artifact_intake_dashboard_manifest_count": counts["real_artifact_intake_dashboard_manifest_count"],
        "workflow_run_manifest_count": counts["workflow_run_manifest_count"],
        "workflow_run_item_row_count": len(collected["workflow_run_items"]),
        "candidate_lifecycle_row_count": len(_derive_candidate_lifecycle_rows(collected)),
        "warning_count": len(collected["warnings"]),
        "warnings": list(collected["warnings"]),
    }


def _sync_candidate_lifecycles(connection: sqlite3.Connection, collected: dict[str, Any]) -> None:
    derived_rows = _derive_candidate_lifecycle_rows(collected)
    for row in derived_rows:
        current = connection.execute(
            "SELECT * FROM candidate_lifecycles WHERE candidate_id = ?",
            (row["candidate_id"],),
        ).fetchone()
        if current is None:
            created_at = _utc_now()
            connection.execute(
                """
                INSERT INTO candidate_lifecycles (
                    candidate_id, game, source, fixture_id, event_id, fused_sidecar_path,
                    lifecycle_state, latest_review_status, recommended_action, final_score,
                    has_review_disagreement, has_cross_layer_disagreement, has_trial_preference,
                    selection_basis, highlight_selection_manifest_path, selected_highlight_details_json,
                    export_artifact_path, post_ledger_path, created_at, updated_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["candidate_id"],
                    row["game"],
                    row["source"],
                    row["fixture_id"],
                    row["event_id"],
                    row["fused_sidecar_path"],
                    row["lifecycle_state"],
                    row["latest_review_status"],
                    row["recommended_action"],
                    row["final_score"],
                    row["has_review_disagreement"],
                    row["has_cross_layer_disagreement"],
                    row["has_trial_preference"],
                    row["selection_basis"],
                    row["highlight_selection_manifest_path"],
                    row["selected_highlight_details_json"],
                    row["export_artifact_path"],
                    row["post_ledger_path"],
                    created_at,
                    created_at,
                    row["last_seen_at"],
                ),
            )
            _append_candidate_lifecycle_transition(
                connection,
                candidate_id=row["candidate_id"],
                from_state=None,
                to_state=row["lifecycle_state"],
                reason="Derived from fused sidecar refresh.",
                transition_source=_derived_transition_source(row),
                actor="system",
                source_artifact_path=_derived_source_artifact_path(row),
                metadata={"derived": True},
            )
            continue

        current_state = str(current["lifecycle_state"] or "").strip().lower()
        desired_state = str(row["lifecycle_state"] or "").strip().lower()
        next_state = current_state
        if current_state not in _MANUAL_PRESERVE_LIFECYCLE_STATES:
            if desired_state != current_state:
                next_state = desired_state
        elif _should_advance_preserved_lifecycle(current_state=current_state, desired_state=desired_state):
            next_state = desired_state
        connection.execute(
            """
            UPDATE candidate_lifecycles
            SET game = ?, source = ?, fixture_id = ?, event_id = ?, fused_sidecar_path = ?,
                lifecycle_state = ?, latest_review_status = ?, recommended_action = ?, final_score = ?,
                has_review_disagreement = ?, has_cross_layer_disagreement = ?, has_trial_preference = ?,
                selection_basis = COALESCE(?, selection_basis),
                highlight_selection_manifest_path = COALESCE(?, highlight_selection_manifest_path),
                selected_highlight_details_json = CASE
                    WHEN ? IS NOT NULL AND ? != '{}' THEN ?
                    ELSE selected_highlight_details_json
                END,
                export_artifact_path = COALESCE(export_artifact_path, ?),
                post_ledger_path = COALESCE(post_ledger_path, ?),
                updated_at = ?, last_seen_at = ?
            WHERE candidate_id = ?
            """,
            (
                row["game"],
                row["source"],
                row["fixture_id"],
                row["event_id"],
                row["fused_sidecar_path"],
                next_state,
                row["latest_review_status"],
                row["recommended_action"],
                row["final_score"],
                row["has_review_disagreement"],
                row["has_cross_layer_disagreement"],
                row["has_trial_preference"],
                row["selection_basis"],
                row["highlight_selection_manifest_path"],
                row["selected_highlight_details_json"],
                row["selected_highlight_details_json"],
                row["selected_highlight_details_json"],
                row["export_artifact_path"],
                row["post_ledger_path"],
                _utc_now(),
                row["last_seen_at"],
                row["candidate_id"],
            ),
        )
        if next_state != current_state:
            _append_candidate_lifecycle_transition(
                connection,
                candidate_id=row["candidate_id"],
                from_state=current_state,
                to_state=next_state,
                reason="Derived state changed during refresh.",
                transition_source=_derived_transition_source(row),
                actor="system",
                source_artifact_path=_derived_source_artifact_path(row),
                metadata={"derived": True},
            )


def _derived_transition_source(row: dict[str, Any]) -> str:
    if str(row.get("post_ledger_path") or "").strip():
        return "post_ledger_ingest"
    if str(row.get("export_artifact_path") or "").strip():
        return "export_manifest_ingest"
    if str(row.get("highlight_selection_manifest_path") or "").strip():
        return "export_manifest_ingest"
    if str(row.get("latest_review_status") or "").strip() in {"approved", "rejected"}:
        return "fused_review_apply"
    return "fused_review_apply"


def _derived_source_artifact_path(row: dict[str, Any]) -> str | None:
    return str(
        row.get("post_ledger_path")
        or row.get("export_artifact_path")
        or row.get("highlight_selection_manifest_path")
        or row.get("fused_sidecar_path")
        or ""
    ).strip() or None


def _should_advance_preserved_lifecycle(*, current_state: str, desired_state: str) -> bool:
    current_rank = _LIFECYCLE_PROGRESS_ORDER.get(current_state)
    desired_rank = _LIFECYCLE_PROGRESS_ORDER.get(desired_state)
    if current_rank is None or desired_rank is None:
        return False
    return desired_rank > current_rank


def _append_candidate_lifecycle_transition(
    connection: sqlite3.Connection,
    *,
    candidate_id: str,
    from_state: str | None,
    to_state: str,
    reason: str | None,
    transition_source: str,
    actor: str | None,
    source_artifact_path: str | None,
    metadata: dict[str, Any] | None,
) -> None:
    existing = connection.execute(
        """
        SELECT transition_id
        FROM candidate_lifecycle_transitions
        WHERE candidate_id = ? AND COALESCE(from_state, '') = COALESCE(?, '') AND to_state = ?
          AND transition_source = ? AND COALESCE(source_artifact_path, '') = COALESCE(?, '')
        ORDER BY transition_id DESC
        LIMIT 1
        """,
        (candidate_id, from_state, to_state, transition_source, source_artifact_path),
    ).fetchone()
    if existing is not None and metadata and metadata.get("derived"):
        return
    connection.execute(
        """
        INSERT INTO candidate_lifecycle_transitions (
            candidate_id, from_state, to_state, transition_reason, transition_source,
            actor, source_artifact_path, created_at, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            from_state,
            to_state,
            reason,
            transition_source,
            actor,
            source_artifact_path,
            _utc_now(),
            json.dumps(metadata or {}, sort_keys=True),
        ),
    )


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS clips (
            game TEXT NOT NULL,
            source TEXT NOT NULL,
            proxy_sidecar_path TEXT,
            runtime_sidecar_path TEXT,
            fused_sidecar_path TEXT,
            has_proxy_sidecar INTEGER DEFAULT 0,
            has_runtime_sidecar INTEGER DEFAULT 0,
            has_fused_sidecar INTEGER DEFAULT 0,
            proxy_review_status TEXT,
            runtime_review_status TEXT,
            fused_review_status TEXT,
            fixture_ids_json TEXT,
            top_proxy_action TEXT,
            top_proxy_score REAL,
            top_fused_action TEXT,
            top_fused_score REAL,
            has_review_disagreement INTEGER DEFAULT 0,
            has_cross_layer_disagreement INTEGER DEFAULT 0,
            has_trial_preference INTEGER DEFAULT 0,
            last_seen_at TEXT,
            PRIMARY KEY (game, source)
        );
        CREATE TABLE IF NOT EXISTS proxy_windows (
            scan_id TEXT,
            window_index INTEGER NOT NULL,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            start_seconds REAL,
            end_seconds REAL,
            proxy_score REAL,
            signal_count INTEGER,
            recommended_action TEXT,
            sources_json TEXT,
            source_families_json TEXT,
            review_status TEXT,
            PRIMARY KEY (scan_id, window_index)
        );
        CREATE TABLE IF NOT EXISTS runtime_analyses (
            analysis_id TEXT PRIMARY KEY,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            status TEXT,
            frame_count INTEGER,
            confirmed_detection_count INTEGER,
            event_count INTEGER,
            runtime_review_status TEXT,
            runtime_review_session_id TEXT,
            runtime_recommended_action TEXT,
            runtime_highlight_score REAL,
            last_ingested_mtime REAL
        );
        CREATE TABLE IF NOT EXISTS runtime_events (
            analysis_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            event_id TEXT,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            event_type TEXT,
            confidence REAL,
            start_timestamp REAL,
            end_timestamp REAL,
            entity_id TEXT,
            ability_id TEXT,
            equipment_id TEXT,
            event_row_id TEXT,
            review_status TEXT,
            recommended_action TEXT,
            PRIMARY KEY (analysis_id, event_index)
        );
        CREATE TABLE IF NOT EXISTS runtime_detections (
            analysis_id TEXT NOT NULL,
            detection_index INTEGER NOT NULL,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            asset_id TEXT,
            roi_ref TEXT,
            entity_id TEXT,
            ability_id TEXT,
            equipment_id TEXT,
            first_timestamp REAL,
            last_timestamp REAL,
            peak_score REAL,
            PRIMARY KEY (analysis_id, detection_index)
        );
        CREATE TABLE IF NOT EXISTS fused_analyses (
            fusion_id TEXT PRIMARY KEY,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            status TEXT,
            normalized_signal_count INTEGER,
            fused_event_count INTEGER,
            fused_reviewed_event_count INTEGER,
            fused_review_session_id TEXT,
            fused_review_status TEXT,
            last_ingested_mtime REAL
        );
        CREATE TABLE IF NOT EXISTS fused_events (
            fusion_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            event_id TEXT,
            game TEXT,
            source TEXT,
            sidecar_path TEXT,
            event_type TEXT,
            confidence REAL,
            final_score REAL,
            gate_status TEXT,
            synergy_applied INTEGER,
            synergy_multiplier REAL,
            minimum_required_signals_met INTEGER,
            suggested_start_timestamp REAL,
            suggested_end_timestamp REAL,
            entity_id TEXT,
            ability_id TEXT,
            equipment_id TEXT,
            event_row_id TEXT,
            review_status TEXT,
            recommended_action TEXT,
            PRIMARY KEY (fusion_id, event_index)
        );
        CREATE TABLE IF NOT EXISTS fused_signal_refs (
            fusion_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            signal_index INTEGER NOT NULL,
            signal_id TEXT,
            signal_type TEXT,
            producer_family TEXT,
            source_family TEXT,
            asset_id TEXT,
            roi_ref TEXT,
            PRIMARY KEY (fusion_id, event_id, signal_index)
        );
        CREATE TABLE IF NOT EXISTS runtime_review_sessions (
            session_id TEXT PRIMARY KEY,
            game TEXT,
            manifest_path TEXT,
            selection_source TEXT,
            selection_action_filter TEXT,
            created_at TEXT,
            applied_at TEXT,
            cleanup_at TEXT,
            item_count INTEGER,
            approved_count INTEGER,
            rejected_count INTEGER,
            unreviewed_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS runtime_review_items (
            session_id TEXT NOT NULL,
            item_index INTEGER NOT NULL,
            game TEXT,
            sidecar_path TEXT,
            source TEXT,
            analysis_id TEXT,
            review_status TEXT,
            apply_status TEXT,
            highlight_score REAL,
            recommended_action TEXT,
            gpt_meta_path TEXT,
            gpt_processed_path TEXT,
            gpt_final_path TEXT,
            PRIMARY KEY (session_id, item_index)
        );
        CREATE TABLE IF NOT EXISTS fused_review_sessions (
            session_id TEXT PRIMARY KEY,
            game TEXT,
            manifest_path TEXT,
            selection_source TEXT,
            selection_action_filter TEXT,
            selection_event_type_filter TEXT,
            created_at TEXT,
            applied_at TEXT,
            cleanup_at TEXT,
            item_count INTEGER,
            approved_count INTEGER,
            rejected_count INTEGER,
            unreviewed_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS fused_review_items (
            session_id TEXT NOT NULL,
            item_index INTEGER NOT NULL,
            game TEXT,
            sidecar_path TEXT,
            source TEXT,
            fusion_id TEXT,
            event_id TEXT,
            event_type TEXT,
            review_status TEXT,
            apply_status TEXT,
            final_score REAL,
            recommended_action TEXT,
            gate_status TEXT,
            gpt_meta_path TEXT,
            gpt_processed_path TEXT,
            gpt_final_path TEXT,
            PRIMARY KEY (session_id, item_index)
        );
        CREATE TABLE IF NOT EXISTS fixture_comparisons (
            comparison_path TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            fixture_id TEXT,
            label TEXT,
            artifact_layer TEXT,
            game TEXT,
            source TEXT,
            coverage_status TEXT,
            review_status TEXT,
            baseline_sidecar_path TEXT,
            trial_sidecar_path TEXT,
            baseline_action TEXT,
            trial_action TEXT,
            baseline_score REAL,
            trial_score REAL,
            score_delta REAL,
            shortlist_changed INTEGER,
            rerank_changed INTEGER,
            stage_latency_deltas_json TEXT,
            recommendation_signal TEXT,
            recommendation_decision TEXT,
            recommendation_reason TEXT,
            PRIMARY KEY (comparison_path, row_index)
        );
        CREATE TABLE IF NOT EXISTS fixture_trial_runs (
            trial_name TEXT PRIMARY KEY,
            trial_root TEXT,
            manifest_path TEXT,
            proxy_sidecar_root TEXT,
            runtime_sidecar_root TEXT,
            fused_sidecar_root TEXT,
            fixture_manifest_path TEXT,
            fixture_source_manifest_path TEXT,
            status TEXT,
            completed_fixture_count INTEGER,
            failed_fixture_count INTEGER,
            effective_overrides_json TEXT
        );
        CREATE TABLE IF NOT EXISTS fixture_trial_run_fixtures (
            trial_name TEXT NOT NULL,
            fixture_index INTEGER NOT NULL,
            fixture_id TEXT,
            game TEXT,
            source_path TEXT,
            status TEXT,
            failure_reason TEXT,
            proxy_sidecar_path TEXT,
            runtime_sidecar_path TEXT,
            fused_sidecar_path TEXT,
            PRIMARY KEY (trial_name, fixture_index)
        );
        CREATE TABLE IF NOT EXISTS fixture_trial_batches (
            batch_name TEXT PRIMARY KEY,
            manifest_path TEXT,
            baseline_trial_name TEXT,
            overall_recommendation_decision TEXT,
            overall_recommendation_trial_name TEXT,
            selected_trials_json TEXT
        );
        CREATE TABLE IF NOT EXISTS fixture_trial_batch_comparisons (
            batch_name TEXT NOT NULL,
            comparison_index INTEGER NOT NULL,
            trial_name TEXT,
            comparison_report_path TEXT,
            artifact_layer TEXT,
            comparison_status TEXT,
            recommendation_decision TEXT,
            PRIMARY KEY (batch_name, comparison_index)
        );
        CREATE TABLE IF NOT EXISTS hook_candidate_manifests (
            manifest_path TEXT PRIMARY KEY,
            game TEXT,
            source TEXT,
            fused_sidecar_path TEXT,
            hook_candidate_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS hook_candidates (
            hook_id TEXT NOT NULL,
            manifest_path TEXT,
            hook_index INTEGER,
            candidate_id TEXT,
            event_id TEXT,
            game TEXT,
            source TEXT,
            fixture_id TEXT,
            fused_sidecar_path TEXT,
            lifecycle_state TEXT,
            hook_archetype TEXT,
            hook_mode TEXT,
            hook_strength REAL,
            intensity_score REAL,
            clarity_score REAL,
            novelty_score REAL,
            context_sufficiency_score REAL,
            payoff_readability_score REAL,
            title_thumbnail_potential_score REAL,
            authenticity_risk_score REAL,
            sound_off_legibility_score REAL,
            packaging_strategy TEXT,
            rejection_reason TEXT,
            highlight_selection_manifest_path TEXT,
            metadata_summary_json TEXT,
            created_at TEXT,
            PRIMARY KEY (manifest_path, hook_id)
        );
        CREATE TABLE IF NOT EXISTS hook_comparison_reports (
            report_path TEXT PRIMARY KEY,
            fixture_manifest_path TEXT,
            baseline_sidecar_root TEXT,
            trial_sidecar_root TEXT,
            comparison_row_count INTEGER,
            recommendation_decision TEXT,
            recommendation_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS hook_comparisons (
            report_path TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            fixture_id TEXT,
            label TEXT,
            game TEXT,
            source TEXT,
            candidate_id TEXT,
            event_id TEXT,
            comparison_status TEXT,
            review_status TEXT,
            baseline_manifest_path TEXT,
            trial_manifest_path TEXT,
            baseline_fused_sidecar_path TEXT,
            trial_fused_sidecar_path TEXT,
            baseline_hook_mode TEXT,
            trial_hook_mode TEXT,
            baseline_hook_archetype TEXT,
            trial_hook_archetype TEXT,
            baseline_hook_strength REAL,
            trial_hook_strength REAL,
            hook_strength_delta REAL,
            baseline_lifecycle_state TEXT,
            trial_lifecycle_state TEXT,
            baseline_selection_manifest_path TEXT,
            trial_selection_manifest_path TEXT,
            strong_fused_weak_hook INTEGER,
            approved_reject_hook INTEGER,
            reject_to_synthetic INTEGER,
            natural_to_synthetic INTEGER,
            recommendation_signal TEXT,
            recommendation_decision TEXT,
            recommendation_reason TEXT,
            PRIMARY KEY (report_path, row_index)
        );
        CREATE TABLE IF NOT EXISTS hook_evaluation_reports (
            report_path TEXT PRIMARY KEY,
            fixture_manifest_path TEXT,
            baseline_sidecar_root TEXT,
            trial_sidecar_root TEXT,
            registry_path TEXT,
            game TEXT,
            comparison_row_count INTEGER,
            recommendation_decision TEXT,
            recommendation_reason TEXT,
            selected_candidate_count INTEGER,
            exported_candidate_count INTEGER,
            strong_fused_weak_hook_count INTEGER,
            approved_reject_hook_count INTEGER,
            reject_to_synthetic_count INTEGER,
            natural_to_synthetic_count INTEGER,
            hook_artifacts_policy TEXT,
            future_gate_readiness TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS highlight_export_batches (
            manifest_path TEXT PRIMARY KEY,
            export_batch_id TEXT,
            game TEXT,
            workflow_run_id TEXT,
            selection_manifest_path TEXT,
            fused_sidecar_paths_json TEXT,
            hook_manifest_paths_json TEXT,
            selection_manifest_paths_json TEXT,
            export_count INTEGER,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS highlight_exports (
            manifest_path TEXT NOT NULL,
            export_batch_id TEXT,
            export_index INTEGER NOT NULL,
            export_id TEXT,
            candidate_id TEXT,
            event_id TEXT,
            hook_id TEXT,
            fixture_id TEXT,
            game TEXT,
            source TEXT,
            fused_sidecar_path TEXT,
            hook_manifest_path TEXT,
            highlight_selection_manifest_path TEXT,
            start_seconds REAL,
            end_seconds REAL,
            final_score REAL,
            hook_archetype TEXT,
            hook_mode TEXT,
            packaging_strategy TEXT,
            export_status TEXT,
            export_artifact_path TEXT,
            otio_path TEXT,
            selected_highlight_details_json TEXT,
            metadata_json TEXT,
            PRIMARY KEY (manifest_path, export_index)
        );
        CREATE TABLE IF NOT EXISTS post_ledgers (
            manifest_path TEXT PRIMARY KEY,
            ledger_id TEXT,
            platform TEXT,
            account_id TEXT,
            workflow_run_id TEXT,
            posted_count INTEGER,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS posted_highlights (
            manifest_path TEXT NOT NULL,
            ledger_id TEXT,
            record_index INTEGER NOT NULL,
            post_record_id TEXT,
            export_id TEXT,
            candidate_id TEXT,
            event_id TEXT,
            hook_id TEXT,
            export_batch_manifest_path TEXT,
            posted_at TEXT,
            post_status TEXT,
            external_post_id TEXT,
            external_url TEXT,
            platform TEXT,
            account_id TEXT,
            caption_ref TEXT,
            caption_text TEXT,
            duration_seconds REAL,
            media_asset_path TEXT,
            initial_view_count INTEGER,
            initial_like_count INTEGER,
            initial_comment_count INTEGER,
            selected_highlight_details_json TEXT,
            game TEXT,
            PRIMARY KEY (manifest_path, record_index)
        );
        CREATE TABLE IF NOT EXISTS posted_metrics_snapshots (
            manifest_path TEXT PRIMARY KEY,
            snapshot_id TEXT,
            platform TEXT,
            account_id TEXT,
            workflow_run_id TEXT,
            captured_at TEXT,
            snapshot_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS posted_metrics_snapshot_rows (
            manifest_path TEXT NOT NULL,
            snapshot_index INTEGER NOT NULL,
            snapshot_row_id TEXT,
            post_record_id TEXT,
            export_id TEXT,
            candidate_id TEXT,
            hook_id TEXT,
            post_ledger_manifest_path TEXT,
            captured_at TEXT,
            platform TEXT,
            account_id TEXT,
            external_post_id TEXT,
            external_url TEXT,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            share_count INTEGER,
            save_count INTEGER,
            watch_time_seconds REAL,
            average_watch_time_seconds REAL,
            completion_rate REAL,
            engagement_rate REAL,
            metadata_json TEXT,
            game TEXT,
            PRIMARY KEY (manifest_path, snapshot_index)
        );
        CREATE TABLE IF NOT EXISTS shadow_ranking_models (
            manifest_path TEXT PRIMARY KEY,
            model_id TEXT,
            created_at TEXT,
            model_family TEXT,
            model_version TEXT,
            training_dataset_manifest_path TEXT,
            training_target TEXT,
            split_key TEXT,
            train_fraction REAL,
            row_count INTEGER,
            train_row_count INTEGER,
            eval_row_count INTEGER,
            label_positive_count INTEGER,
            label_negative_count INTEGER,
            feature_fields_json TEXT,
            training_metrics_json TEXT,
            evaluation_metrics_json TEXT,
            filters_json TEXT,
            warning_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS shadow_evaluation_policies (
            manifest_path TEXT PRIMARY KEY,
            policy_id TEXT,
            created_at TEXT,
            targets_json TEXT
        );
        CREATE TABLE IF NOT EXISTS shadow_ranking_experiments (
            manifest_path TEXT PRIMARY KEY,
            experiment_id TEXT,
            created_at TEXT,
            model_path TEXT,
            model_id TEXT,
            model_family TEXT,
            model_version TEXT,
            dataset_manifest_path TEXT,
            dataset_export_id TEXT,
            training_target TEXT,
            split_key TEXT,
            train_fraction REAL,
            replay_manifest_path TEXT,
            comparison_report_path TEXT,
            replay_row_count INTEGER,
            comparison_row_count INTEGER,
            recommendation_decision TEXT,
            recommendation_reason TEXT,
            training_metrics_json TEXT,
            evaluation_metrics_json TEXT,
            comparison_summary_json TEXT,
            filters_json TEXT
        );
        CREATE TABLE IF NOT EXISTS shadow_ranking_experiment_ledgers (
            manifest_path TEXT PRIMARY KEY,
            ledger_id TEXT,
            created_at TEXT,
            policy_manifest_path TEXT,
            policy_id TEXT,
            experiment_manifest_path TEXT,
            experiment_id TEXT,
            model_id TEXT,
            model_family TEXT,
            model_version TEXT,
            dataset_manifest_path TEXT,
            dataset_export_id TEXT,
            training_target TEXT,
            evaluation_target TEXT,
            replay_manifest_path TEXT,
            comparison_report_path TEXT,
            coverage_status TEXT,
            slice_count INTEGER,
            recommendation_decision TEXT,
            recommendation_reason TEXT,
            blocking_reasons_json TEXT,
            protected_regression_count INTEGER,
            global_metrics_json TEXT,
            global_primary_metric_name TEXT,
            global_primary_metric_delta REAL,
            filters_json TEXT
        );
        CREATE TABLE IF NOT EXISTS shadow_ranking_experiment_slices (
            manifest_path TEXT NOT NULL,
            ledger_id TEXT,
            slice_index INTEGER NOT NULL,
            policy_id TEXT,
            experiment_id TEXT,
            model_id TEXT,
            model_family TEXT,
            model_version TEXT,
            training_target TEXT,
            evaluation_target TEXT,
            slice_type TEXT,
            slice_value TEXT,
            coverage_status TEXT,
            row_count INTEGER,
            covered_row_count INTEGER,
            positive_count INTEGER,
            top_k INTEGER,
            shadow_topk_hits INTEGER,
            heuristic_topk_hits INTEGER,
            shadow_precision_at_k REAL,
            heuristic_precision_at_k REAL,
            shadow_ranking_gain REAL,
            heuristic_ranking_gain REAL,
            shadow_false_positive_cost REAL,
            heuristic_false_positive_cost REAL,
            shadow_pearson_correlation REAL,
            heuristic_pearson_correlation REAL,
            primary_metric_name TEXT,
            shadow_primary_metric REAL,
            heuristic_primary_metric REAL,
            primary_metric_delta REAL,
            game TEXT,
            platform TEXT,
            PRIMARY KEY (manifest_path, slice_index)
        );
        CREATE TABLE IF NOT EXISTS shadow_ranking_replays (
            manifest_path TEXT PRIMARY KEY,
            replay_id TEXT,
            dataset_manifest_path TEXT,
            dataset_export_id TEXT,
            model_family TEXT,
            model_version TEXT,
            row_count INTEGER,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS shadow_ranking_replay_rows (
            manifest_path TEXT NOT NULL,
            replay_id TEXT,
            row_index INTEGER NOT NULL,
            candidate_id TEXT,
            event_id TEXT,
            hook_id TEXT,
            export_id TEXT,
            post_record_id TEXT,
            game TEXT,
            fixture_id TEXT,
            source TEXT,
            platform TEXT,
            account_id TEXT,
            heuristic_final_score REAL,
            heuristic_recommended_action TEXT,
            heuristic_lifecycle_state TEXT,
            review_outcome TEXT,
            export_present INTEGER,
            post_present INTEGER,
            metrics_present INTEGER,
            latest_view_count INTEGER,
            latest_engagement_rate REAL,
            hook_mode TEXT,
            hook_archetype TEXT,
            packaging_strategy TEXT,
            label_positive INTEGER,
            label_score REAL,
            predicted_candidate_score REAL,
            predicted_export_score REAL,
            predicted_post_performance_score REAL,
            predicted_rank INTEGER,
            heuristic_rank INTEGER,
            feature_values_json TEXT,
            PRIMARY KEY (manifest_path, row_index)
        );
        CREATE TABLE IF NOT EXISTS shadow_ranking_comparisons (
            report_path TEXT NOT NULL,
            comparison_id TEXT,
            row_index INTEGER NOT NULL,
            replay_manifest_path TEXT,
            replay_id TEXT,
            dataset_manifest_path TEXT,
            model_family TEXT,
            model_version TEXT,
            candidate_id TEXT,
            event_id TEXT,
            game TEXT,
            fixture_id TEXT,
            platform TEXT,
            heuristic_final_score REAL,
            predicted_candidate_score REAL,
            heuristic_rank INTEGER,
            predicted_rank INTEGER,
            rank_delta INTEGER,
            label_positive INTEGER,
            label_score REAL,
            review_outcome TEXT,
            export_present INTEGER,
            post_present INTEGER,
            latest_view_count INTEGER,
            latest_engagement_rate REAL,
            recommendation_decision TEXT,
            recommendation_reason TEXT,
            PRIMARY KEY (report_path, row_index)
        );
        CREATE TABLE IF NOT EXISTS shadow_model_family_comparisons (
            manifest_path TEXT NOT NULL,
            comparison_id TEXT,
            row_index INTEGER NOT NULL,
            source_schema_version TEXT,
            model_id TEXT,
            model_family TEXT,
            model_version TEXT,
            training_target TEXT,
            recommendation_decision TEXT,
            recommendation_reason TEXT,
            primary_metric_name TEXT,
            primary_metric_delta REAL,
            experiment_id TEXT,
            ledger_id TEXT,
            game TEXT,
            platform TEXT,
            PRIMARY KEY (manifest_path, row_index)
        );
        CREATE TABLE IF NOT EXISTS shadow_benchmark_matrices (
            manifest_path TEXT PRIMARY KEY,
            benchmark_id TEXT,
            created_at TEXT,
            dataset_manifest_path TEXT,
            dataset_export_id TEXT,
            policy_path TEXT,
            model_families_json TEXT,
            training_targets_json TEXT,
            split_key TEXT,
            train_fraction REAL,
            filters_json TEXT,
            run_count INTEGER,
            benchmark_recommendation TEXT,
            blocked_run_count INTEGER,
            inconclusive_run_count INTEGER,
            failed_run_count INTEGER,
            warning_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS shadow_benchmark_runs (
            manifest_path TEXT NOT NULL,
            benchmark_id TEXT,
            run_index INTEGER NOT NULL,
            run_id TEXT,
            status TEXT,
            model_family TEXT,
            training_target TEXT,
            evaluation_target TEXT,
            split_key TEXT,
            train_fraction REAL,
            model_manifest_path TEXT,
            experiment_manifest_path TEXT,
            replay_manifest_path TEXT,
            comparison_report_path TEXT,
            governed_ledger_manifest_path TEXT,
            recommendation_decision TEXT,
            recommendation_reason TEXT,
            coverage_status TEXT,
            evidence_mode TEXT,
            synthetic_row_count INTEGER,
            real_row_count INTEGER,
            primary_metric_name TEXT,
            primary_metric_delta REAL,
            protected_regression_count INTEGER,
            blocking_reasons_json TEXT,
            failure_reason TEXT,
            game TEXT,
            platform TEXT,
            PRIMARY KEY (manifest_path, run_index)
        );
        CREATE TABLE IF NOT EXISTS shadow_benchmark_reviews (
            manifest_path TEXT PRIMARY KEY,
            review_id TEXT,
            created_at TEXT,
            source_benchmark_manifest_paths_json TEXT,
            reviewed_targets_json TEXT,
            reviewed_families_json TEXT,
            filters_json TEXT,
            target_count INTEGER,
            ready_target_count INTEGER,
            label_calibration_target_count INTEGER,
            feature_cleanup_target_count INTEGER,
            coverage_blocked_target_count INTEGER,
            warning_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS shadow_target_readiness (
            manifest_path TEXT NOT NULL,
            review_id TEXT,
            target_index INTEGER NOT NULL,
            training_target TEXT,
            current_best_family TEXT,
            best_recommendation_decision TEXT,
            current_best_evidence_mode TEXT,
            evidence_modes_json TEXT,
            synthetic_augmented_run_count INTEGER,
            real_only_run_count INTEGER,
            primary_metric_name TEXT,
            primary_metric_delta REAL,
            run_count INTEGER,
            successful_run_count INTEGER,
            win_count INTEGER,
            keep_current_count INTEGER,
            blocked_count INTEGER,
            inconclusive_count INTEGER,
            failed_count INTEGER,
            dominant_failure_modes_json TEXT,
            confidence_level TEXT,
            readiness_classification TEXT,
            recommended_next_action TEXT,
            game TEXT,
            platform TEXT,
            PRIMARY KEY (manifest_path, target_index)
        );
        CREATE TABLE IF NOT EXISTS real_posted_lineage_imports (
            manifest_path TEXT PRIMARY KEY,
            import_id TEXT,
            created_at TEXT,
            workspace_root TEXT,
            registry_path TEXT,
            refresh_root TEXT,
            source_roots_json TEXT,
            scanned_roots_json TEXT,
            filters_json TEXT,
            workspace_artifact_count INTEGER,
            source_artifact_count INTEGER,
            discovered_counts_json TEXT,
            imported_counts_json TEXT,
            coverage_inventory_json TEXT,
            source_root_summaries_json TEXT,
            unresolved_lineage_counts_json TEXT,
            eligible_real_post_performance_label_count INTEGER,
            imported_candidate_count INTEGER,
            imported_hook_count INTEGER,
            warning_count INTEGER,
            game TEXT,
            platform TEXT
        );
        CREATE TABLE IF NOT EXISTS shadow_benchmark_evidence_comparisons (
            manifest_path TEXT NOT NULL,
            comparison_id TEXT,
            row_index INTEGER NOT NULL,
            training_target TEXT,
            real_manifest_path TEXT,
            synthetic_manifest_path TEXT,
            real_current_best_family TEXT,
            synthetic_current_best_family TEXT,
            real_best_recommendation_decision TEXT,
            synthetic_best_recommendation_decision TEXT,
            real_current_best_evidence_mode TEXT,
            synthetic_current_best_evidence_mode TEXT,
            real_readiness_classification TEXT,
            synthetic_readiness_classification TEXT,
            real_primary_metric_name TEXT,
            synthetic_primary_metric_name TEXT,
            real_primary_metric_delta REAL,
            synthetic_primary_metric_delta REAL,
            primary_metric_delta_gap REAL,
            real_confidence_level TEXT,
            synthetic_confidence_level TEXT,
            real_successful_run_count INTEGER,
            synthetic_successful_run_count INTEGER,
            real_run_count INTEGER,
            synthetic_run_count INTEGER,
            family_winner_changed INTEGER,
            readiness_changed INTEGER,
            recommendation_changed INTEGER,
            disagreement_indicators_json TEXT,
            game TEXT,
            platform TEXT,
            PRIMARY KEY (manifest_path, row_index)
        );
        CREATE TABLE IF NOT EXISTS real_artifact_intake_dashboards (
            manifest_path TEXT PRIMARY KEY,
            generated_at TEXT,
            intake_root TEXT,
            filters_json TEXT,
            headline_status TEXT,
            intake_status TEXT,
            bundle_count INTEGER,
            warning_count INTEGER,
            benchmark_ready_bundle_count INTEGER,
            eligible_real_post_performance_label_count INTEGER,
            preflight_trend_status TEXT,
            preflight_entry_count INTEGER,
            refresh_outcome_trend_status TEXT,
            refresh_outcome_entry_count INTEGER,
            history_alignment_status TEXT,
            real_vs_synthetic_gap_status TEXT,
            next_focus TEXT,
            game TEXT,
            platform TEXT
        );
        CREATE TABLE IF NOT EXISTS workflow_runs (
            workflow_run_id TEXT PRIMARY KEY,
            manifest_path TEXT,
            workflow_type TEXT,
            stage TEXT,
            status TEXT,
            registry_path TEXT,
            game_filter TEXT,
            fixture_id_filter TEXT,
            item_counts_json TEXT,
            linked_artifacts_json TEXT,
            error TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS workflow_run_items (
            workflow_run_id TEXT NOT NULL,
            item_index INTEGER NOT NULL,
            candidate_id TEXT,
            item_status TEXT,
            game TEXT,
            source TEXT,
            fixture_id TEXT,
            event_id TEXT,
            lifecycle_state TEXT,
            fused_sidecar_path TEXT,
            highlight_selection_manifest_path TEXT,
            export_artifact_path TEXT,
            post_ledger_path TEXT,
            hook_manifest_path TEXT,
            created_at TEXT,
            PRIMARY KEY (workflow_run_id, item_index)
        );
        CREATE TABLE IF NOT EXISTS candidate_lifecycles (
            candidate_id TEXT PRIMARY KEY,
            game TEXT,
            source TEXT,
            fixture_id TEXT,
            event_id TEXT,
            fused_sidecar_path TEXT,
            lifecycle_state TEXT,
            latest_review_status TEXT,
            recommended_action TEXT,
            final_score REAL,
            has_review_disagreement INTEGER DEFAULT 0,
            has_cross_layer_disagreement INTEGER DEFAULT 0,
            has_trial_preference INTEGER DEFAULT 0,
            selection_basis TEXT,
            highlight_selection_manifest_path TEXT,
            selected_highlight_details_json TEXT,
            export_artifact_path TEXT,
            post_ledger_path TEXT,
            created_at TEXT,
            updated_at TEXT,
            last_seen_at TEXT
        );
        CREATE TABLE IF NOT EXISTS candidate_lifecycle_transitions (
            transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id TEXT NOT NULL,
            from_state TEXT,
            to_state TEXT NOT NULL,
            transition_reason TEXT,
            transition_source TEXT,
            actor TEXT,
            source_artifact_path TEXT,
            created_at TEXT,
            metadata_json TEXT
        );
        CREATE TABLE IF NOT EXISTS ingest_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            registry_path TEXT,
            refresh_root TEXT,
            game_filter TEXT,
            warning_count INTEGER,
            proxy_sidecar_count INTEGER,
            runtime_sidecar_count INTEGER,
            fused_sidecar_count INTEGER,
            runtime_review_session_count INTEGER,
            fused_review_session_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS ingest_warnings (
            run_id INTEGER NOT NULL,
            warning_index INTEGER NOT NULL,
            path TEXT,
            reason TEXT,
            detail TEXT,
            PRIMARY KEY (run_id, warning_index)
        );
        """
    )
    _ensure_hook_candidate_primary_key(connection)
    _ensure_table_column(connection, "candidate_lifecycles", "selection_basis", "TEXT")
    _ensure_table_column(connection, "candidate_lifecycles", "selected_highlight_details_json", "TEXT")
    _ensure_table_column(connection, "highlight_exports", "selected_highlight_details_json", "TEXT")
    _ensure_table_column(connection, "posted_highlights", "selected_highlight_details_json", "TEXT")
    _ensure_table_column(connection, "hook_candidates", "created_at", "TEXT")
    _ensure_columns(
        connection,
        "shadow_benchmark_runs",
        (
            ("evidence_mode", "TEXT"),
            ("synthetic_row_count", "INTEGER"),
            ("real_row_count", "INTEGER"),
        ),
    )
    _ensure_columns(
        connection,
        "shadow_target_readiness",
        (
            ("current_best_evidence_mode", "TEXT"),
            ("evidence_modes_json", "TEXT"),
            ("synthetic_augmented_run_count", "INTEGER"),
            ("real_only_run_count", "INTEGER"),
        ),
    )
    _ensure_columns(
        connection,
        "real_posted_lineage_imports",
        (
            ("coverage_inventory_json", "TEXT"),
            ("source_root_summaries_json", "TEXT"),
            ("unresolved_lineage_counts_json", "TEXT"),
            ("eligible_real_post_performance_label_count", "INTEGER"),
            ("imported_candidate_count", "INTEGER"),
            ("imported_hook_count", "INTEGER"),
        ),
    )
    _ensure_columns(
        connection,
        "hook_comparisons",
        (
            ("strong_fused_weak_hook", "INTEGER"),
            ("approved_reject_hook", "INTEGER"),
            ("reject_to_synthetic", "INTEGER"),
            ("natural_to_synthetic", "INTEGER"),
        ),
    )


def _ensure_hook_candidate_primary_key(connection: sqlite3.Connection) -> None:
    pk_columns = [
        str(row[1])
        for row in connection.execute("PRAGMA table_info(hook_candidates)").fetchall()
        if int(row[5] or 0) > 0
    ]
    if pk_columns == ["manifest_path", "hook_id"]:
        return
    connection.execute("DROP TABLE IF EXISTS hook_candidates")
    connection.execute(
        """
        CREATE TABLE hook_candidates (
            hook_id TEXT NOT NULL,
            manifest_path TEXT,
            hook_index INTEGER,
            candidate_id TEXT,
            event_id TEXT,
            game TEXT,
            source TEXT,
            fixture_id TEXT,
            fused_sidecar_path TEXT,
            lifecycle_state TEXT,
            hook_archetype TEXT,
            hook_mode TEXT,
            hook_strength REAL,
            intensity_score REAL,
            clarity_score REAL,
            novelty_score REAL,
            context_sufficiency_score REAL,
            payoff_readability_score REAL,
            title_thumbnail_potential_score REAL,
            authenticity_risk_score REAL,
            sound_off_legibility_score REAL,
            packaging_strategy TEXT,
            rejection_reason TEXT,
            highlight_selection_manifest_path TEXT,
            metadata_summary_json TEXT,
            created_at TEXT,
            PRIMARY KEY (manifest_path, hook_id)
        )
        """
    )


def _ensure_table_column(connection: sqlite3.Connection, table_name: str, column_name: str, column_type: str) -> None:
    existing = {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name in existing:
        return
    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
    _ensure_columns(
        connection,
        "clips",
        (
            ("has_proxy_sidecar", "INTEGER DEFAULT 0"),
            ("has_runtime_sidecar", "INTEGER DEFAULT 0"),
            ("has_fused_sidecar", "INTEGER DEFAULT 0"),
            ("fixture_ids_json", "TEXT"),
            ("top_proxy_action", "TEXT"),
            ("top_proxy_score", "REAL"),
            ("top_fused_action", "TEXT"),
            ("top_fused_score", "REAL"),
            ("has_review_disagreement", "INTEGER DEFAULT 0"),
            ("has_cross_layer_disagreement", "INTEGER DEFAULT 0"),
            ("has_trial_preference", "INTEGER DEFAULT 0"),
        ),
    )


def _ensure_columns(connection: sqlite3.Connection, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    existing = {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column_name, column_sql in columns:
        if column_name in existing:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def _clear_mirror_tables(connection: sqlite3.Connection) -> None:
    for table_name in (
        "clips",
        "proxy_windows",
        "runtime_analyses",
        "runtime_events",
        "runtime_detections",
        "fused_analyses",
        "fused_events",
        "fused_signal_refs",
        "runtime_review_sessions",
        "runtime_review_items",
        "fused_review_sessions",
        "fused_review_items",
        "fixture_comparisons",
        "fixture_trial_runs",
        "fixture_trial_run_fixtures",
        "fixture_trial_batches",
        "fixture_trial_batch_comparisons",
        "hook_candidate_manifests",
        "hook_candidates",
        "hook_comparison_reports",
        "hook_comparisons",
        "hook_evaluation_reports",
        "highlight_export_batches",
        "highlight_exports",
        "post_ledgers",
        "posted_highlights",
        "posted_metrics_snapshots",
        "posted_metrics_snapshot_rows",
        "shadow_evaluation_policies",
        "shadow_ranking_models",
        "shadow_ranking_experiments",
        "shadow_ranking_experiment_ledgers",
        "shadow_ranking_experiment_slices",
        "shadow_ranking_replays",
        "shadow_ranking_replay_rows",
        "shadow_ranking_comparisons",
        "shadow_model_family_comparisons",
        "shadow_benchmark_matrices",
        "shadow_benchmark_runs",
        "shadow_benchmark_reviews",
        "shadow_target_readiness",
        "real_posted_lineage_imports",
        "shadow_benchmark_evidence_comparisons",
        "real_artifact_intake_dashboards",
        "workflow_runs",
        "workflow_run_items",
    ):
        connection.execute(f"DELETE FROM {table_name}")


def _insert_ingest_run(
    connection: sqlite3.Connection,
    registry_path: Path,
    refresh_root: Path,
    collected: dict[str, Any],
    *,
    game: str | None,
) -> int:
    counts = collected["counts"]
    cursor = connection.execute(
        """
        INSERT INTO ingest_runs (
            created_at,
            registry_path,
            refresh_root,
            game_filter,
            warning_count,
            proxy_sidecar_count,
            runtime_sidecar_count,
            fused_sidecar_count,
            runtime_review_session_count,
            fused_review_session_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _utc_now(),
            str(registry_path),
            str(refresh_root),
            game,
            len(collected["warnings"]),
            counts["proxy_sidecar_count"],
            counts["runtime_sidecar_count"],
            counts["fused_sidecar_count"],
            counts["runtime_review_session_count"],
            counts["fused_review_session_count"],
        ),
    )
    return int(cursor.lastrowid)


def _insert_ingest_warnings(connection: sqlite3.Connection, run_id: int, warnings: list[dict[str, Any]]) -> None:
    for index, warning in enumerate(warnings):
        connection.execute(
            "INSERT INTO ingest_warnings (run_id, warning_index, path, reason, detail) VALUES (?, ?, ?, ?, ?)",
            (run_id, index, warning.get("path"), warning.get("reason"), warning.get("detail")),
        )


def _bulk_insert(connection: sqlite3.Connection, table_name: str, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in columns)
    column_sql = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"
    payloads = []
    for row in rows:
        payloads.append(tuple(row.get(column) for column in columns))
    connection.executemany(sql, payloads)


def _query_rows(
    connection: sqlite3.Connection,
    *,
    mode: str,
    game: str | None,
    event_type: str | None,
    action: str | None,
    review_status: str | None,
    gate_status: str | None,
    fixture_id: str | None,
    trial_name: str | None,
    artifact_layer: str | None,
    recommendation_decision: str | None,
    coverage_status: str | None,
    has_disagreement: bool | None,
    candidate_id: str | None,
    lifecycle_state: str | None,
    hook_archetype: str | None,
    hook_mode: str | None,
    comparison_status: str | None,
    export_status: str | None,
    post_status: str | None,
    platform: str | None,
    account_id: str | None,
    evidence_mode: str | None,
    model_family: str | None,
    training_target: str | None,
    workflow_type: str | None,
    workflow_run_id: str | None,
    stage: str | None,
    status: str | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    if mode == "clips":
        sql = """
            SELECT game, source, proxy_sidecar_path, runtime_sidecar_path, fused_sidecar_path,
                   has_proxy_sidecar, has_runtime_sidecar, has_fused_sidecar,
                   proxy_review_status, runtime_review_status, fused_review_status,
                   fixture_ids_json, top_proxy_action, top_proxy_score, top_fused_action, top_fused_score,
                   has_review_disagreement, has_cross_layer_disagreement, has_trial_preference, last_seen_at
            FROM clips
        """
        where = []
        params: list[Any] = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("fixture_ids_json LIKE ?")
            params.append(f'%"{fixture_id}"%')
        if has_disagreement is not None:
            where.append("has_review_disagreement = ?")
            params.append(int(bool(has_disagreement)))
        rows = _execute_query(connection, sql, where, params, order_by="game ASC, source ASC", limit=limit)
        return rows

    if mode == "proxy-windows":
        sql = """
            SELECT scan_id, window_index, game, source, sidecar_path, start_seconds, end_seconds,
                   proxy_score, signal_count, recommended_action, sources_json, source_families_json, review_status
            FROM proxy_windows
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if action is not None:
            where.append("recommended_action = ?")
            params.append(action)
        if review_status is not None:
            where.append("review_status = ?")
            params.append(review_status)
        return _execute_query(connection, sql, where, params, order_by="COALESCE(proxy_score, 0.0) DESC, source ASC", limit=limit)

    if mode == "runtime-events":
        sql = """
            SELECT e.game, e.source, e.analysis_id, e.event_id, e.event_type, e.confidence,
                   e.review_status, e.recommended_action, e.sidecar_path
            FROM runtime_events e
        """
        where = []
        params = []
        if game is not None:
            where.append("e.game = ?")
            params.append(game)
        if event_type is not None:
            where.append("e.event_type = ?")
            params.append(event_type)
        if action is not None:
            where.append("e.recommended_action = ?")
            params.append(action)
        if review_status is not None:
            where.append("e.review_status = ?")
            params.append(review_status)
        return _execute_query(connection, sql, where, params, order_by="COALESCE(e.confidence, 0.0) DESC, e.source ASC", limit=limit)

    if mode == "review-items":
        sql = """
            SELECT review_type, game, session_id, item_index, sidecar_path, source, analysis_id, fusion_id,
                   event_id, event_type, review_status, apply_status, recommended_action
            FROM (
                SELECT 'runtime' AS review_type, game, session_id, item_index, sidecar_path, source,
                       analysis_id, NULL AS fusion_id, NULL AS event_id, NULL AS event_type,
                       review_status, apply_status, recommended_action
                FROM runtime_review_items
                UNION ALL
                SELECT 'fused' AS review_type, game, session_id, item_index, sidecar_path, source,
                       NULL AS analysis_id, fusion_id, event_id, event_type,
                       review_status, apply_status, recommended_action
                FROM fused_review_items
            )
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if event_type is not None:
            where.append("(event_type = ? OR event_type IS NULL)")
            params.append(event_type)
        if action is not None:
            where.append("recommended_action = ?")
            params.append(action)
        if review_status is not None:
            where.append("review_status = ?")
            params.append(review_status)
        return _execute_query(connection, sql, where, params, order_by="game ASC, session_id ASC, item_index ASC", limit=limit)

    if mode == "fixture-comparisons":
        sql = """
            SELECT comparison_path, row_index, fixture_id, label, artifact_layer, game, source,
                   coverage_status, review_status, baseline_sidecar_path, trial_sidecar_path,
                   baseline_action, trial_action, baseline_score, trial_score, score_delta,
                   shortlist_changed, rerank_changed, stage_latency_deltas_json,
                   recommendation_signal, recommendation_decision, recommendation_reason
            FROM fixture_comparisons
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("fixture_id = ?")
            params.append(fixture_id)
        if artifact_layer is not None:
            where.append("artifact_layer = ?")
            params.append(artifact_layer)
        if review_status is not None:
            where.append("review_status = ?")
            params.append(review_status)
        if recommendation_decision is not None:
            where.append("recommendation_decision = ?")
            params.append(recommendation_decision)
        if coverage_status is not None:
            where.append("coverage_status = ?")
            params.append(coverage_status)
        return _execute_query(connection, sql, where, params, order_by="fixture_id ASC, artifact_layer ASC, row_index ASC", limit=limit)

    if mode == "fixture-trials":
        sql = """
            SELECT f.trial_name, r.status AS run_status, r.manifest_path, r.trial_root, r.proxy_sidecar_root,
                   r.runtime_sidecar_root, r.fused_sidecar_root, f.fixture_id, f.game, f.source_path,
                   f.status, f.failure_reason, f.proxy_sidecar_path, f.runtime_sidecar_path, f.fused_sidecar_path
            FROM fixture_trial_run_fixtures f
            JOIN fixture_trial_runs r ON r.trial_name = f.trial_name
        """
        where = []
        params = []
        if game is not None:
            where.append("f.game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("f.fixture_id = ?")
            params.append(fixture_id)
        if trial_name is not None:
            where.append("f.trial_name = ?")
            params.append(trial_name)
        return _execute_query(connection, sql, where, params, order_by="f.trial_name ASC, f.fixture_id ASC", limit=limit)

    if mode == "batch-comparisons":
        sql = """
            SELECT c.batch_name, b.manifest_path, b.baseline_trial_name, b.overall_recommendation_decision,
                   b.overall_recommendation_trial_name, c.trial_name, c.comparison_report_path,
                   c.artifact_layer, c.comparison_status, c.recommendation_decision
            FROM fixture_trial_batch_comparisons c
            JOIN fixture_trial_batches b ON b.batch_name = c.batch_name
        """
        where = []
        params = []
        if trial_name is not None:
            where.append("c.trial_name = ?")
            params.append(trial_name)
        if artifact_layer is not None:
            where.append("c.artifact_layer = ?")
            params.append(artifact_layer)
        if recommendation_decision is not None:
            where.append("c.recommendation_decision = ?")
            params.append(recommendation_decision)
        return _execute_query(connection, sql, where, params, order_by="c.batch_name ASC, c.trial_name ASC", limit=limit)

    if mode == "candidate-lifecycles":
        sql = """
            SELECT l.candidate_id, l.game, l.source, l.fixture_id, l.event_id, l.fused_sidecar_path,
                   l.lifecycle_state, l.latest_review_status, l.recommended_action, l.final_score,
                   l.has_review_disagreement, l.has_cross_layer_disagreement, l.has_trial_preference,
                   l.selection_basis,
                   l.highlight_selection_manifest_path, l.selected_highlight_details_json,
                   l.export_artifact_path, l.post_ledger_path,
                   l.created_at, l.updated_at, l.last_seen_at,
                   (
                       SELECT json_group_array(
                           json_object(
                               'transition_id', t.transition_id,
                               'from_state', t.from_state,
                               'to_state', t.to_state,
                               'transition_reason', t.transition_reason,
                               'transition_source', t.transition_source,
                               'actor', t.actor,
                               'source_artifact_path', t.source_artifact_path,
                               'created_at', t.created_at,
                               'metadata_json', t.metadata_json
                           )
                       )
                       FROM candidate_lifecycle_transitions t
                       WHERE t.candidate_id = l.candidate_id
                       ORDER BY t.transition_id ASC
                   ) AS transitions_json
            FROM candidate_lifecycles l
        """
        where = []
        params = []
        if game is not None:
            where.append("l.game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("l.fixture_id = ?")
            params.append(fixture_id)
        if candidate_id is not None:
            where.append("l.candidate_id = ?")
            params.append(candidate_id)
        if lifecycle_state is not None:
            where.append("l.lifecycle_state = ?")
            params.append(lifecycle_state)
        if review_status is not None:
            where.append("l.latest_review_status = ?")
            params.append(review_status)
        if action is not None:
            where.append("l.recommended_action = ?")
            params.append(action)
        if has_disagreement is not None:
            where.append("(l.has_review_disagreement = ? OR l.has_cross_layer_disagreement = ?)")
            params.extend([int(bool(has_disagreement)), int(bool(has_disagreement))])
        if trial_name is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM fixture_trial_run_fixtures f
                    WHERE f.trial_name = ?
                      AND (f.fused_sidecar_path = l.fused_sidecar_path OR f.source_path = l.source)
                )
                """
            )
            params.append(trial_name)
        if recommendation_decision is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM fixture_comparisons fc
                    WHERE fc.recommendation_decision = ?
                      AND (fc.baseline_sidecar_path = l.fused_sidecar_path OR fc.trial_sidecar_path = l.fused_sidecar_path)
                )
                """
            )
            params.append(recommendation_decision)
        return _execute_query(connection, sql, where, params, order_by="l.updated_at DESC, l.candidate_id ASC", limit=limit)

    if mode == "hook-candidates":
        sql = """
            SELECT h.hook_id, h.candidate_id, h.event_id, h.game, h.source, h.fixture_id, h.fused_sidecar_path,
                   h.lifecycle_state, h.hook_archetype, h.hook_mode, h.hook_strength,
                   h.intensity_score, h.clarity_score, h.novelty_score, h.context_sufficiency_score,
                   h.payoff_readability_score, h.title_thumbnail_potential_score, h.authenticity_risk_score,
                   h.sound_off_legibility_score, h.packaging_strategy, h.rejection_reason,
                   h.highlight_selection_manifest_path, h.metadata_summary_json, m.manifest_path
            FROM hook_candidates h
            LEFT JOIN hook_candidate_manifests m ON m.manifest_path = h.manifest_path
        """
        where = []
        params = []
        if game is not None:
            where.append("h.game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("h.fixture_id = ?")
            params.append(fixture_id)
        if candidate_id is not None:
            where.append("h.candidate_id = ?")
            params.append(candidate_id)
        if lifecycle_state is not None:
            where.append("h.lifecycle_state = ?")
            params.append(lifecycle_state)
        if hook_archetype is not None:
            where.append("h.hook_archetype = ?")
            params.append(hook_archetype)
        if hook_mode is not None:
            where.append("h.hook_mode = ?")
            params.append(hook_mode)
        if has_disagreement is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM candidate_lifecycles l
                    WHERE l.candidate_id = h.candidate_id
                      AND (l.has_review_disagreement = ? OR l.has_cross_layer_disagreement = ?)
                )
                """
            )
            params.extend([int(bool(has_disagreement)), int(bool(has_disagreement))])
        if recommendation_decision is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM fixture_comparisons fc
                    WHERE fc.recommendation_decision = ?
                      AND (fc.baseline_sidecar_path = h.fused_sidecar_path OR fc.trial_sidecar_path = h.fused_sidecar_path)
                )
                """
            )
            params.append(recommendation_decision)
        return _execute_query(connection, sql, where, params, order_by="COALESCE(h.hook_strength, 0.0) DESC, h.hook_id ASC", limit=limit)

    if mode == "hook-comparisons":
        sql = """
            SELECT report_path, row_index, fixture_id, label, game, source, candidate_id, event_id,
                   comparison_status, review_status,
                   baseline_manifest_path, trial_manifest_path,
                   baseline_fused_sidecar_path, trial_fused_sidecar_path,
                   baseline_hook_mode, trial_hook_mode,
                   baseline_hook_archetype, trial_hook_archetype,
                   baseline_hook_strength, trial_hook_strength, hook_strength_delta,
                   baseline_lifecycle_state, trial_lifecycle_state,
                   baseline_selection_manifest_path, trial_selection_manifest_path,
                   strong_fused_weak_hook, approved_reject_hook, reject_to_synthetic, natural_to_synthetic,
                   recommendation_signal, recommendation_decision, recommendation_reason
            FROM hook_comparisons
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("fixture_id = ?")
            params.append(fixture_id)
        if candidate_id is not None:
            where.append("candidate_id = ?")
            params.append(candidate_id)
        if hook_mode is not None:
            where.append("(baseline_hook_mode = ? OR trial_hook_mode = ?)")
            params.extend([hook_mode, hook_mode])
        if hook_archetype is not None:
            where.append("(baseline_hook_archetype = ? OR trial_hook_archetype = ?)")
            params.extend([hook_archetype, hook_archetype])
        if recommendation_decision is not None:
            where.append("recommendation_decision = ?")
            params.append(recommendation_decision)
        if comparison_status is not None:
            where.append("comparison_status = ?")
            params.append(comparison_status)
        return _execute_query(connection, sql, where, params, order_by="fixture_id ASC, row_index ASC", limit=limit)

    if mode == "hook-evaluation-reports":
        sql = """
            SELECT report_path, fixture_manifest_path, baseline_sidecar_root, trial_sidecar_root,
                   registry_path, game, comparison_row_count, recommendation_decision,
                   recommendation_reason, selected_candidate_count, exported_candidate_count,
                   strong_fused_weak_hook_count, approved_reject_hook_count,
                   reject_to_synthetic_count, natural_to_synthetic_count,
                   hook_artifacts_policy, future_gate_readiness, created_at
            FROM hook_evaluation_reports
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if recommendation_decision is not None:
            where.append("recommendation_decision = ?")
            params.append(recommendation_decision)
        if status is not None:
            where.append("future_gate_readiness = ?")
            params.append(status)
        return _execute_query(connection, sql, where, params, order_by="COALESCE(created_at, '') DESC, report_path ASC", limit=limit)

    if mode == "hook-quality-rollups":
        return _query_hook_quality_rollups(
            connection,
            game=game,
            candidate_id=candidate_id,
            hook_mode=hook_mode,
            hook_archetype=hook_archetype,
        )

    if mode == "highlight-exports":
        sql = """
            SELECT manifest_path, export_batch_id, export_index, export_id, candidate_id, event_id, hook_id,
                   fixture_id, game, source, fused_sidecar_path, hook_manifest_path,
                   highlight_selection_manifest_path, start_seconds, end_seconds, final_score,
                   hook_archetype, hook_mode, packaging_strategy, export_status,
                   export_artifact_path, otio_path, selected_highlight_details_json, metadata_json
            FROM highlight_exports
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("fixture_id = ?")
            params.append(fixture_id)
        if candidate_id is not None:
            where.append("candidate_id = ?")
            params.append(candidate_id)
        if workflow_run_id is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM highlight_export_batches b
                    WHERE b.manifest_path = highlight_exports.manifest_path
                      AND b.workflow_run_id = ?
                )
                """
            )
            params.append(workflow_run_id)
        if hook_mode is not None:
            where.append("hook_mode = ?")
            params.append(hook_mode)
        if hook_archetype is not None:
            where.append("hook_archetype = ?")
            params.append(hook_archetype)
        if export_status is not None:
            where.append("export_status = ?")
            params.append(export_status)
        return _execute_query(connection, sql, where, params, order_by="manifest_path ASC, export_index ASC", limit=limit)

    if mode == "post-ledger-records":
        sql = """
            SELECT manifest_path, ledger_id, record_index, post_record_id, export_id, candidate_id, event_id,
                   hook_id, export_batch_manifest_path, posted_at, post_status, external_post_id,
                   external_url, platform, account_id, caption_ref, caption_text, duration_seconds,
                   media_asset_path, initial_view_count, initial_like_count, initial_comment_count,
                   selected_highlight_details_json, game
            FROM posted_highlights
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if candidate_id is not None:
            where.append("candidate_id = ?")
            params.append(candidate_id)
        if workflow_run_id is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM post_ledgers l
                    WHERE l.manifest_path = posted_highlights.manifest_path
                      AND l.workflow_run_id = ?
                )
                """
            )
            params.append(workflow_run_id)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        if account_id is not None:
            where.append("account_id = ?")
            params.append(account_id)
        if post_status is not None:
            where.append("post_status = ?")
            params.append(post_status)
        return _execute_query(connection, sql, where, params, order_by="posted_at DESC, manifest_path ASC, record_index ASC", limit=limit)

    if mode == "posted-metrics":
        sql = """
            SELECT r.manifest_path, r.snapshot_index, r.snapshot_row_id, r.post_record_id, r.export_id,
                   r.candidate_id, r.hook_id, r.post_ledger_manifest_path, r.captured_at, r.platform,
                   r.account_id, r.external_post_id, r.external_url,
                   r.view_count, r.like_count, r.comment_count, r.share_count, r.save_count,
                   r.watch_time_seconds, r.average_watch_time_seconds, r.completion_rate, r.engagement_rate,
                   r.metadata_json, r.game,
                   e.fixture_id, e.hook_archetype, e.hook_mode, e.packaging_strategy,
                   p.posted_at, p.post_status
            FROM posted_metrics_snapshot_rows r
            LEFT JOIN highlight_exports e ON e.export_id = r.export_id
            LEFT JOIN posted_highlights p ON p.post_record_id = r.post_record_id
        """
        where = []
        params = []
        if game is not None:
            where.append("COALESCE(r.game, e.game) = ?")
            params.append(game)
        if candidate_id is not None:
            where.append("r.candidate_id = ?")
            params.append(candidate_id)
        if fixture_id is not None:
            where.append("e.fixture_id = ?")
            params.append(fixture_id)
        if platform is not None:
            where.append("r.platform = ?")
            params.append(platform)
        if account_id is not None:
            where.append("r.account_id = ?")
            params.append(account_id)
        if workflow_run_id is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM posted_metrics_snapshots s
                    WHERE s.manifest_path = r.manifest_path
                      AND s.workflow_run_id = ?
                )
                """
            )
            params.append(workflow_run_id)
        if hook_archetype is not None:
            where.append("e.hook_archetype = ?")
            params.append(hook_archetype)
        if hook_mode is not None:
            where.append("e.hook_mode = ?")
            params.append(hook_mode)
        return _execute_query(connection, sql, where, params, order_by="r.captured_at DESC, r.manifest_path ASC, r.snapshot_index ASC", limit=limit)

    if mode == "posted-performance-rollups":
        return _posted_performance_rollups(
            connection,
            game=game,
            fixture_id=fixture_id,
            candidate_id=candidate_id,
            platform=platform,
            account_id=account_id,
            workflow_run_id=workflow_run_id,
            hook_archetype=hook_archetype,
            hook_mode=hook_mode,
        )

    if mode == "shadow-evaluation-policies":
        sql = """
            SELECT manifest_path, policy_id, created_at, targets_json
            FROM shadow_evaluation_policies
        """
        return _execute_query(connection, sql, [], [], order_by="created_at DESC, policy_id ASC", limit=limit)

    if mode == "shadow-ranking-models":
        sql = """
            SELECT manifest_path, model_id, created_at, model_family, model_version,
                   training_dataset_manifest_path, training_target, split_key, train_fraction,
                   row_count, train_row_count, eval_row_count,
                   label_positive_count, label_negative_count,
                   feature_fields_json, training_metrics_json, evaluation_metrics_json,
                   filters_json, warning_count
            FROM shadow_ranking_models
        """
        where = []
        params = []
        if status is not None:
            where.append("model_version = ?")
            params.append(status)
        if model_family is not None:
            where.append("model_family = ?")
            params.append(model_family)
        if training_target is not None:
            where.append("training_target = ?")
            params.append(training_target)
        if recommendation_decision is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM shadow_ranking_experiments e
                    WHERE e.model_id = shadow_ranking_models.model_id
                      AND e.recommendation_decision = ?
                )
                """
            )
            params.append(recommendation_decision)
        if game is not None:
            where.append("json_extract(filters_json, '$.game') = ?")
            params.append(game)
        return _execute_query(connection, sql, where, params, order_by="created_at DESC, model_id ASC", limit=limit)

    if mode == "shadow-ranking-experiments":
        sql = """
            SELECT manifest_path, experiment_id, created_at, model_path, model_id, model_family, model_version,
                   dataset_manifest_path, dataset_export_id, training_target, split_key, train_fraction,
                   replay_manifest_path, comparison_report_path, replay_row_count, comparison_row_count,
                   recommendation_decision, recommendation_reason,
                   training_metrics_json, evaluation_metrics_json, comparison_summary_json, filters_json
            FROM shadow_ranking_experiments
        """
        where = []
        params = []
        if recommendation_decision is not None:
            where.append("recommendation_decision = ?")
            params.append(recommendation_decision)
        if status is not None:
            where.append("model_version = ?")
            params.append(status)
        if model_family is not None:
            where.append("model_family = ?")
            params.append(model_family)
        if training_target is not None:
            where.append("training_target = ?")
            params.append(training_target)
        if game is not None:
            where.append("json_extract(filters_json, '$.game') = ?")
            params.append(game)
        return _execute_query(connection, sql, where, params, order_by="created_at DESC, experiment_id ASC", limit=limit)

    if mode == "shadow-ranking-experiment-ledgers":
        sql = """
            SELECT manifest_path, ledger_id, created_at, policy_manifest_path, policy_id,
                   experiment_manifest_path, experiment_id, model_id, model_family, model_version,
                   dataset_manifest_path, dataset_export_id, training_target, evaluation_target,
                   replay_manifest_path, comparison_report_path, coverage_status, slice_count,
                   recommendation_decision, recommendation_reason, blocking_reasons_json,
                   protected_regression_count, global_metrics_json, global_primary_metric_name,
                   global_primary_metric_delta, filters_json
            FROM shadow_ranking_experiment_ledgers
        """
        where = []
        params = []
        if recommendation_decision is not None:
            where.append("recommendation_decision = ?")
            params.append(recommendation_decision)
        if status is not None:
            where.append("model_version = ?")
            params.append(status)
        if model_family is not None:
            where.append("model_family = ?")
            params.append(model_family)
        if training_target is not None:
            where.append("training_target = ?")
            params.append(training_target)
        if platform is not None:
            where.append("json_extract(filters_json, '$.platform') = ?")
            params.append(platform)
        if game is not None:
            where.append("json_extract(filters_json, '$.game') = ?")
            params.append(game)
        if hook_mode is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM shadow_ranking_experiment_slices s
                    WHERE s.manifest_path = shadow_ranking_experiment_ledgers.manifest_path
                      AND s.slice_type = 'hook_mode'
                      AND s.slice_value = ?
                )
                """
            )
            params.append(hook_mode)
        if hook_archetype is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM shadow_ranking_experiment_slices s
                    WHERE s.manifest_path = shadow_ranking_experiment_ledgers.manifest_path
                      AND s.slice_type = 'hook_archetype'
                      AND s.slice_value = ?
                )
                """
            )
            params.append(hook_archetype)
        return _execute_query(connection, sql, where, params, order_by="created_at DESC, ledger_id ASC", limit=limit)

    if mode == "shadow-ranking-experiment-slices":
        sql = """
            SELECT manifest_path, ledger_id, slice_index, policy_id, experiment_id, model_id,
                   model_family, model_version, training_target, evaluation_target,
                   slice_type, slice_value, coverage_status, row_count, covered_row_count,
                   positive_count, top_k, shadow_topk_hits, heuristic_topk_hits,
                   shadow_precision_at_k, heuristic_precision_at_k,
                   shadow_ranking_gain, heuristic_ranking_gain,
                   shadow_false_positive_cost, heuristic_false_positive_cost,
                   shadow_pearson_correlation, heuristic_pearson_correlation,
                   primary_metric_name, shadow_primary_metric, heuristic_primary_metric,
                   primary_metric_delta, game, platform
            FROM shadow_ranking_experiment_slices
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        if model_family is not None:
            where.append("model_family = ?")
            params.append(model_family)
        if training_target is not None:
            where.append("training_target = ?")
            params.append(training_target)
        if recommendation_decision is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM shadow_ranking_experiment_ledgers l
                    WHERE l.manifest_path = shadow_ranking_experiment_slices.manifest_path
                      AND l.recommendation_decision = ?
                )
                """
            )
            params.append(recommendation_decision)
        if fixture_id is not None:
            where.append("slice_type = 'fixture_id' AND slice_value = ?")
            params.append(fixture_id)
        if hook_mode is not None:
            where.append(
                "(slice_type = 'hook_mode' AND slice_value = ?)"
            )
            params.append(hook_mode)
        if hook_archetype is not None:
            where.append(
                "(slice_type = 'hook_archetype' AND slice_value = ?)"
            )
            params.append(hook_archetype)
        return _execute_query(connection, sql, where, params, order_by="manifest_path ASC, slice_type ASC, slice_value ASC", limit=limit)

    if mode == "shadow-ranking-replays":
        sql = """
            SELECT r.manifest_path, r.replay_id, r.row_index, m.dataset_manifest_path, m.dataset_export_id,
                   m.model_family, m.model_version, r.candidate_id, r.event_id, r.hook_id, r.export_id, r.post_record_id,
                   r.game, r.fixture_id, r.source, r.platform, r.account_id,
                   r.heuristic_final_score, r.heuristic_recommended_action, r.heuristic_lifecycle_state,
                   r.review_outcome, r.export_present, r.post_present, r.metrics_present,
                   r.latest_view_count, r.latest_engagement_rate,
                   r.hook_mode, r.hook_archetype, r.packaging_strategy,
                   r.label_positive, r.label_score,
                   r.predicted_candidate_score, r.predicted_export_score, r.predicted_post_performance_score,
                   r.predicted_rank, r.heuristic_rank, r.feature_values_json
            FROM shadow_ranking_replay_rows r
            JOIN shadow_ranking_replays m ON m.manifest_path = r.manifest_path
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("fixture_id = ?")
            params.append(fixture_id)
        if candidate_id is not None:
            where.append("candidate_id = ?")
            params.append(candidate_id)
        if lifecycle_state is not None:
            where.append("heuristic_lifecycle_state = ?")
            params.append(lifecycle_state)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        if status is not None:
            where.append("model_version = ?")
            params.append(status)
        if model_family is not None:
            where.append("model_family = ?")
            params.append(model_family)
        return _execute_query(connection, sql, where, params, order_by="predicted_rank ASC, candidate_id ASC", limit=limit)

    if mode == "shadow-ranking-comparisons":
        sql = """
            SELECT report_path, comparison_id, row_index, replay_manifest_path, replay_id,
                   dataset_manifest_path, model_family, model_version,
                   candidate_id, event_id, game, fixture_id, platform,
                   heuristic_final_score, predicted_candidate_score,
                   heuristic_rank, predicted_rank, rank_delta,
                   label_positive, label_score, review_outcome,
                   export_present, post_present, latest_view_count, latest_engagement_rate,
                   recommendation_decision, recommendation_reason
            FROM shadow_ranking_comparisons
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if fixture_id is not None:
            where.append("fixture_id = ?")
            params.append(fixture_id)
        if candidate_id is not None:
            where.append("candidate_id = ?")
            params.append(candidate_id)
        if lifecycle_state is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM shadow_ranking_replay_rows r
                    WHERE r.manifest_path = shadow_ranking_comparisons.replay_manifest_path
                      AND r.candidate_id = shadow_ranking_comparisons.candidate_id
                      AND r.heuristic_lifecycle_state = ?
                )
                """
            )
            params.append(lifecycle_state)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        if recommendation_decision is not None:
            where.append("recommendation_decision = ?")
            params.append(recommendation_decision)
        if status is not None:
            where.append("model_version = ?")
            params.append(status)
        if model_family is not None:
            where.append("model_family = ?")
            params.append(model_family)
        return _execute_query(connection, sql, where, params, order_by="report_path ASC, predicted_rank ASC, candidate_id ASC", limit=limit)

    if mode == "shadow-model-family-comparisons":
        sql = """
            SELECT manifest_path, comparison_id, row_index, source_schema_version,
                   model_id, model_family, model_version, training_target,
                   recommendation_decision, recommendation_reason,
                   primary_metric_name, primary_metric_delta,
                   experiment_id, ledger_id, game, platform
            FROM shadow_model_family_comparisons
        """
        where = []
        params = []
        if training_target is not None:
            where.append("training_target = ?")
            params.append(training_target)
        if model_family is not None:
            where.append("model_family = ?")
            params.append(model_family)
        if recommendation_decision is not None:
            where.append("recommendation_decision = ?")
            params.append(recommendation_decision)
        if evidence_mode is not None:
            where.append("evidence_mode = ?")
            params.append(evidence_mode)
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        if status is not None:
            where.append("model_version = ?")
            params.append(status)
        return _execute_query(connection, sql, where, params, order_by="training_target ASC, primary_metric_delta DESC, model_family ASC", limit=limit)

    if mode == "shadow-benchmark-matrices":
        sql = """
            SELECT manifest_path, benchmark_id, created_at, dataset_manifest_path, dataset_export_id,
                   policy_path, model_families_json, training_targets_json, split_key, train_fraction,
                   filters_json, run_count, benchmark_recommendation, blocked_run_count,
                   inconclusive_run_count, failed_run_count, warning_count
            FROM shadow_benchmark_matrices
        """
        where = []
        params = []
        if game is not None:
            where.append("json_extract(filters_json, '$.game') = ?")
            params.append(game)
        if platform is not None:
            where.append("json_extract(filters_json, '$.platform') = ?")
            params.append(platform)
        if training_target is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM json_each(shadow_benchmark_matrices.training_targets_json)
                    WHERE json_each.value = ?
                )
                """
            )
            params.append(training_target)
        if model_family is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM json_each(shadow_benchmark_matrices.model_families_json)
                    WHERE json_each.value = ?
                )
                """
            )
            params.append(model_family)
        if recommendation_decision is not None:
            where.append("benchmark_recommendation = ?")
            params.append(recommendation_decision)
        return _execute_query(connection, sql, where, params, order_by="created_at DESC, benchmark_id ASC", limit=limit)

    if mode == "shadow-benchmark-runs":
        sql = """
            SELECT manifest_path, benchmark_id, run_index, run_id, status, model_family,
                   training_target, evaluation_target, split_key, train_fraction,
                   model_manifest_path, experiment_manifest_path, replay_manifest_path,
                   comparison_report_path, governed_ledger_manifest_path,
                   recommendation_decision, recommendation_reason, coverage_status,
                   evidence_mode, synthetic_row_count, real_row_count,
                   primary_metric_name, primary_metric_delta, protected_regression_count,
                   blocking_reasons_json, failure_reason, game, platform
            FROM shadow_benchmark_runs
        """
        where = []
        params = []
        if training_target is not None:
            where.append("training_target = ?")
            params.append(training_target)
        if model_family is not None:
            where.append("model_family = ?")
            params.append(model_family)
        if recommendation_decision is not None:
            where.append("recommendation_decision = ?")
            params.append(recommendation_decision)
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        if status is not None:
            where.append("status = ?")
            params.append(status)
        return _execute_query(connection, sql, where, params, order_by="training_target ASC, model_family ASC, run_index ASC", limit=limit)

    if mode == "shadow-benchmark-reviews":
        sql = """
            SELECT manifest_path, review_id, created_at, source_benchmark_manifest_paths_json,
                   reviewed_targets_json, reviewed_families_json, filters_json, target_count,
                   ready_target_count, label_calibration_target_count,
                   feature_cleanup_target_count, coverage_blocked_target_count, warning_count
            FROM shadow_benchmark_reviews
        """
        where = []
        params = []
        if game is not None:
            where.append("json_extract(filters_json, '$.game') = ?")
            params.append(game)
        if platform is not None:
            where.append("json_extract(filters_json, '$.platform') = ?")
            params.append(platform)
        if training_target is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM json_each(shadow_benchmark_reviews.reviewed_targets_json)
                    WHERE json_each.value = ?
                )
                """
            )
            params.append(training_target)
        if model_family is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM json_each(shadow_benchmark_reviews.reviewed_families_json)
                    WHERE json_each.value = ?
                )
                """
            )
            params.append(model_family)
        return _execute_query(connection, sql, where, params, order_by="created_at DESC, review_id ASC", limit=limit)

    if mode == "shadow-target-readiness":
        sql = """
            SELECT manifest_path, review_id, target_index, training_target, current_best_family,
                   best_recommendation_decision, current_best_evidence_mode,
                   evidence_modes_json, synthetic_augmented_run_count, real_only_run_count,
                   primary_metric_name, primary_metric_delta,
                   run_count, successful_run_count, win_count, keep_current_count,
                   blocked_count, inconclusive_count, failed_count,
                   dominant_failure_modes_json, confidence_level,
                   readiness_classification, recommended_next_action, game, platform
            FROM shadow_target_readiness
        """
        where = []
        params = []
        if training_target is not None:
            where.append("training_target = ?")
            params.append(training_target)
        if model_family is not None:
            where.append("current_best_family = ?")
            params.append(model_family)
        if recommendation_decision is not None:
            where.append("best_recommendation_decision = ?")
            params.append(recommendation_decision)
        if evidence_mode is not None:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM json_each(shadow_target_readiness.evidence_modes_json)
                    WHERE json_each.value = ?
                )
                """
            )
            params.append(evidence_mode)
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        if status is not None:
            where.append("readiness_classification = ?")
            params.append(status)
        return _execute_query(connection, sql, where, params, order_by="training_target ASC, manifest_path ASC, target_index ASC", limit=limit)

    if mode == "real-posted-lineage-imports":
        sql = """
            SELECT manifest_path, import_id, created_at, workspace_root, registry_path, refresh_root,
                   source_roots_json, scanned_roots_json, filters_json, workspace_artifact_count,
                   source_artifact_count, discovered_counts_json, imported_counts_json,
                   coverage_inventory_json, source_root_summaries_json, unresolved_lineage_counts_json,
                   eligible_real_post_performance_label_count, imported_candidate_count, imported_hook_count,
                   warning_count, game, platform
            FROM real_posted_lineage_imports
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        return _execute_query(connection, sql, where, params, order_by="created_at DESC, import_id ASC", limit=limit)

    if mode == "shadow-benchmark-evidence-comparisons":
        sql = """
            SELECT manifest_path, comparison_id, row_index, training_target, real_manifest_path,
                   synthetic_manifest_path, real_current_best_family, synthetic_current_best_family,
                   real_best_recommendation_decision, synthetic_best_recommendation_decision,
                   real_current_best_evidence_mode, synthetic_current_best_evidence_mode,
                   real_readiness_classification, synthetic_readiness_classification,
                   real_primary_metric_name, synthetic_primary_metric_name,
                   real_primary_metric_delta, synthetic_primary_metric_delta, primary_metric_delta_gap,
                   real_confidence_level, synthetic_confidence_level,
                   real_successful_run_count, synthetic_successful_run_count,
                   real_run_count, synthetic_run_count,
                   family_winner_changed, readiness_changed, recommendation_changed,
                   disagreement_indicators_json, game, platform
            FROM shadow_benchmark_evidence_comparisons
        """
        where = []
        params = []
        if training_target is not None:
            where.append("training_target = ?")
            params.append(training_target)
        if recommendation_decision is not None:
            where.append("(real_best_recommendation_decision = ? OR synthetic_best_recommendation_decision = ?)")
            params.extend([recommendation_decision, recommendation_decision])
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        return _execute_query(connection, sql, where, params, order_by="training_target ASC, manifest_path ASC, row_index ASC", limit=limit)

    if mode == "real-artifact-intake-dashboards":
        sql = """
            SELECT manifest_path, generated_at, intake_root, filters_json, headline_status,
                   intake_status, bundle_count, warning_count, benchmark_ready_bundle_count,
                   eligible_real_post_performance_label_count, preflight_trend_status,
                   preflight_entry_count, refresh_outcome_trend_status, refresh_outcome_entry_count,
                   history_alignment_status, real_vs_synthetic_gap_status, next_focus, game, platform
            FROM real_artifact_intake_dashboards
        """
        where = []
        params = []
        if game is not None:
            where.append("game = ?")
            params.append(game)
        if platform is not None:
            where.append("platform = ?")
            params.append(platform)
        if status is not None:
            where.append("headline_status = ?")
            params.append(status)
        return _execute_query(connection, sql, where, params, order_by="generated_at DESC, manifest_path ASC", limit=limit)

    if mode == "workflow-runs":
        sql = """
            SELECT i.workflow_run_id, r.manifest_path, r.workflow_type, r.stage, r.status AS run_status,
                   r.game_filter, r.fixture_id_filter, r.item_counts_json, r.linked_artifacts_json,
                   r.error, r.created_at, r.updated_at,
                   i.item_index, i.item_status, i.candidate_id, i.game, i.source, i.fixture_id,
                   i.event_id, i.lifecycle_state, i.fused_sidecar_path,
                   i.highlight_selection_manifest_path, i.export_artifact_path, i.post_ledger_path,
                   i.hook_manifest_path
            FROM workflow_run_items i
            JOIN workflow_runs r ON r.workflow_run_id = i.workflow_run_id
        """
        where = []
        params = []
        if workflow_run_id is not None:
            where.append("i.workflow_run_id = ?")
            params.append(workflow_run_id)
        if workflow_type is not None:
            where.append("r.workflow_type = ?")
            params.append(workflow_type)
        if stage is not None:
            where.append("r.stage = ?")
            params.append(stage)
        if status is not None:
            where.append("(r.status = ? OR i.item_status = ?)")
            params.extend([status, status])
        if candidate_id is not None:
            where.append("i.candidate_id = ?")
            params.append(candidate_id)
        if fixture_id is not None:
            where.append("i.fixture_id = ?")
            params.append(fixture_id)
        if game is not None:
            where.append("i.game = ?")
            params.append(game)
        return _execute_query(connection, sql, where, params, order_by="r.created_at DESC, i.workflow_run_id ASC, i.item_index ASC", limit=limit)

    sql = """
        SELECT e.game, e.source, e.fusion_id, e.event_id, e.event_type, e.confidence, e.final_score, e.gate_status,
               e.synergy_applied, e.synergy_multiplier, e.minimum_required_signals_met,
               e.suggested_start_timestamp, e.suggested_end_timestamp,
               e.entity_id, e.ability_id, e.equipment_id,
               (
                   SELECT COUNT(*) FROM fused_signal_refs s
                   WHERE s.fusion_id = e.fusion_id AND s.event_id = e.event_id
               ) AS contributing_signal_count,
               (
                   SELECT COUNT(*) FROM fused_signal_refs s
                   WHERE s.fusion_id = e.fusion_id AND s.event_id = e.event_id AND s.producer_family = 'runtime'
               ) AS runtime_signal_count,
               (
                   SELECT COUNT(*) FROM fused_signal_refs s
                   WHERE s.fusion_id = e.fusion_id AND s.event_id = e.event_id AND s.producer_family = 'proxy'
               ) AS proxy_signal_count,
               (
                   SELECT COUNT(DISTINCT COALESCE(s.source_family, ''))
                   FROM fused_signal_refs s
                   WHERE s.fusion_id = e.fusion_id AND s.event_id = e.event_id
               ) AS source_family_count,
               e.review_status, e.sidecar_path
        FROM fused_events e
    """
    where = []
    params = []
    if game is not None:
        where.append("e.game = ?")
        params.append(game)
    if event_type is not None:
        where.append("e.event_type = ?")
        params.append(event_type)
    if action is not None:
        where.append("e.recommended_action = ?")
        params.append(action)
    if review_status is not None:
        where.append("e.review_status = ?")
        params.append(review_status)
    if gate_status is not None:
        where.append("e.gate_status = ?")
        params.append(gate_status)
    if fixture_id is not None:
        where.append(
            "EXISTS (SELECT 1 FROM clips c WHERE c.game = e.game AND c.source = e.source AND c.fixture_ids_json LIKE ?)"
        )
        params.append(f'%"{fixture_id}"%')
    if has_disagreement is not None:
        where.append(
            "EXISTS (SELECT 1 FROM clips c WHERE c.game = e.game AND c.source = e.source AND c.has_review_disagreement = ?)"
        )
        params.append(int(bool(has_disagreement)))
    return _execute_query(connection, sql, where, params, order_by="COALESCE(e.final_score, 0.0) DESC, e.source ASC", limit=limit)


def _execute_query(
    connection: sqlite3.Connection,
    sql: str,
    where: list[str],
    params: list[Any],
    *,
    order_by: str,
    limit: int | None,
) -> list[dict[str, Any]]:
    text = sql
    if where:
        text += " WHERE " + " AND ".join(where)
    text += f" ORDER BY {order_by}"
    if limit is not None:
        text += " LIMIT ?"
        params = [*params, int(limit)]
    cursor = connection.execute(text, params)
    return [dict(row) for row in cursor.fetchall()]


def _posted_performance_rollups(
    connection: sqlite3.Connection,
    *,
    game: str | None,
    fixture_id: str | None,
    candidate_id: str | None,
    platform: str | None,
    account_id: str | None,
    workflow_run_id: str | None,
    hook_archetype: str | None,
    hook_mode: str | None,
) -> list[dict[str, Any]]:
    sql = """
        SELECT r.post_record_id, r.export_id, r.candidate_id, r.hook_id, r.platform, r.account_id,
               r.captured_at, r.view_count, r.like_count, r.comment_count, r.share_count, r.save_count,
               r.watch_time_seconds, r.average_watch_time_seconds, r.completion_rate, r.engagement_rate,
               COALESCE(r.game, e.game) AS game,
               e.fixture_id, e.hook_archetype, e.hook_mode, e.packaging_strategy
        FROM posted_metrics_snapshot_rows r
        LEFT JOIN highlight_exports e ON e.export_id = r.export_id
    """
    where = []
    params: list[Any] = []
    if game is not None:
        where.append("COALESCE(r.game, e.game) = ?")
        params.append(game)
    if fixture_id is not None:
        where.append("e.fixture_id = ?")
        params.append(fixture_id)
    if candidate_id is not None:
        where.append("r.candidate_id = ?")
        params.append(candidate_id)
    if platform is not None:
        where.append("r.platform = ?")
        params.append(platform)
    if account_id is not None:
        where.append("r.account_id = ?")
        params.append(account_id)
    if workflow_run_id is not None:
        where.append(
            """
            EXISTS (
                SELECT 1 FROM posted_metrics_snapshots s
                WHERE s.manifest_path = r.manifest_path
                  AND s.workflow_run_id = ?
            )
            """
        )
        params.append(workflow_run_id)
    if hook_archetype is not None:
        where.append("e.hook_archetype = ?")
        params.append(hook_archetype)
    if hook_mode is not None:
        where.append("e.hook_mode = ?")
        params.append(hook_mode)
    rows = _execute_query(connection, sql, where, params, order_by="r.captured_at DESC, r.post_record_id ASC", limit=None)
    if not rows:
        return []
    for row in rows:
        row["metrics_coverage_status"] = _metrics_coverage_status([row])
        row.update(_post_performance_label_fields(row))
    latest_by_post_record = _latest_metrics_timestamp_by_post_record(rows)
    return [
        {
            "post_count": len({str(row.get("post_record_id") or "").strip() for row in rows if str(row.get("post_record_id") or "").strip()}),
            "snapshot_count": len(rows),
            "latest_snapshot_timestamp": max(str(row.get("captured_at") or "") for row in rows),
            "metrics_coverage_status": _metrics_coverage_status(rows),
            "post_performance_coverage_tiers_json": json.dumps(_count_post_performance_tiers(rows), sort_keys=True),
            "post_performance_eligible_snapshot_count": sum(1 for row in rows if bool(row.get("post_performance_label_eligible"))),
            "post_performance_eligible_post_count": len({str(row.get("post_record_id") or "").strip() for row in rows if bool(row.get("post_performance_label_eligible")) and str(row.get("post_record_id") or "").strip()}),
            "post_performance_recoverable_snapshot_count": sum(1 for row in rows if bool(row.get("post_performance_recoverable"))),
            "post_performance_recoverable_post_count": len({str(row.get("post_record_id") or "").strip() for row in rows if bool(row.get("post_performance_recoverable")) and str(row.get("post_record_id") or "").strip()}),
            "post_performance_missing_field_counts_json": json.dumps(_count_post_performance_missing_fields(rows), sort_keys=True),
            "by_platform_json": json.dumps(_aggregate_posted_metric_rows(rows, "platform"), sort_keys=True),
            "by_game_json": json.dumps(_aggregate_posted_metric_rows(rows, "game"), sort_keys=True),
            "by_hook_archetype_json": json.dumps(_aggregate_posted_metric_rows(rows, "hook_archetype"), sort_keys=True),
            "by_hook_mode_json": json.dumps(_aggregate_posted_metric_rows(rows, "hook_mode"), sort_keys=True),
            "by_packaging_strategy_json": json.dumps(_aggregate_posted_metric_rows(rows, "packaging_strategy"), sort_keys=True),
            "latest_snapshot_by_post_record_json": json.dumps(latest_by_post_record, sort_keys=True),
        }
    ]


def _query_hook_quality_rollups(
    connection: sqlite3.Connection,
    *,
    game: str | None,
    candidate_id: str | None,
    hook_mode: str | None,
    hook_archetype: str | None,
) -> list[dict[str, Any]]:
    hook_sql = """
        SELECT candidate_id, lifecycle_state, hook_mode, hook_archetype,
               hook_strength, intensity_score, clarity_score, novelty_score,
               context_sufficiency_score, payoff_readability_score,
               title_thumbnail_potential_score, authenticity_risk_score,
               sound_off_legibility_score
        FROM hook_candidates
    """
    hook_where = []
    hook_params: list[Any] = []
    if game is not None:
        hook_where.append("game = ?")
        hook_params.append(game)
    if candidate_id is not None:
        hook_where.append("candidate_id = ?")
        hook_params.append(candidate_id)
    if hook_mode is not None:
        hook_where.append("hook_mode = ?")
        hook_params.append(hook_mode)
    if hook_archetype is not None:
        hook_where.append("hook_archetype = ?")
        hook_params.append(hook_archetype)
    hook_rows = _execute_query(connection, hook_sql, hook_where, hook_params, order_by="candidate_id ASC, hook_id ASC", limit=None)

    export_sql = """
        SELECT candidate_id, hook_mode, hook_archetype, export_status
        FROM highlight_exports
    """
    export_where = []
    export_params: list[Any] = []
    if game is not None:
        export_where.append("game = ?")
        export_params.append(game)
    if candidate_id is not None:
        export_where.append("candidate_id = ?")
        export_params.append(candidate_id)
    if hook_mode is not None:
        export_where.append("hook_mode = ?")
        export_params.append(hook_mode)
    if hook_archetype is not None:
        export_where.append("hook_archetype = ?")
        export_params.append(hook_archetype)
    export_where.append("export_status = ?")
    export_params.append("exported")
    export_rows = _execute_query(connection, export_sql, export_where, export_params, order_by="candidate_id ASC", limit=None)

    comparison_sql = """
        SELECT strong_fused_weak_hook, approved_reject_hook, reject_to_synthetic, natural_to_synthetic
        FROM hook_comparisons
    """
    comparison_where = []
    comparison_params: list[Any] = []
    if game is not None:
        comparison_where.append("game = ?")
        comparison_params.append(game)
    if candidate_id is not None:
        comparison_where.append("candidate_id = ?")
        comparison_params.append(candidate_id)
    if hook_mode is not None:
        comparison_where.append("(baseline_hook_mode = ? OR trial_hook_mode = ?)")
        comparison_params.extend([hook_mode, hook_mode])
    if hook_archetype is not None:
        comparison_where.append("(baseline_hook_archetype = ? OR trial_hook_archetype = ?)")
        comparison_params.extend([hook_archetype, hook_archetype])
    comparison_rows = _execute_query(connection, comparison_sql, comparison_where, comparison_params, order_by="report_path ASC, row_index ASC", limit=None)
    report_disagreement = {
        "strong_fused_weak_hook_count": 0,
        "approved_reject_hook_count": 0,
        "reject_to_synthetic_count": 0,
        "natural_to_synthetic_count": 0,
    }
    if not comparison_rows and candidate_id is None and hook_mode is None and hook_archetype is None:
        report_sql = """
            SELECT strong_fused_weak_hook_count, approved_reject_hook_count,
                   reject_to_synthetic_count, natural_to_synthetic_count
            FROM hook_evaluation_reports
        """
        report_where = []
        report_params: list[Any] = []
        if game is not None:
            report_where.append("game = ?")
            report_params.append(game)
        report_rows = _execute_query(connection, report_sql, report_where, report_params, order_by="COALESCE(created_at, '') DESC, report_path ASC", limit=None)
        report_disagreement = {
            "strong_fused_weak_hook_count": sum(int(row.get("strong_fused_weak_hook_count") or 0) for row in report_rows),
            "approved_reject_hook_count": sum(int(row.get("approved_reject_hook_count") or 0) for row in report_rows),
            "reject_to_synthetic_count": sum(int(row.get("reject_to_synthetic_count") or 0) for row in report_rows),
            "natural_to_synthetic_count": sum(int(row.get("natural_to_synthetic_count") or 0) for row in report_rows),
        }

    selected_rows = [
        row for row in hook_rows if str(row.get("lifecycle_state") or "").strip() in {"approved", "selected_for_export", "exported", "posted"}
    ]
    selected_candidate_ids = {
        str(row.get("candidate_id") or "").strip()
        for row in selected_rows
        if str(row.get("candidate_id") or "").strip()
    }
    exported_candidate_ids = {
        str(row.get("candidate_id") or "").strip()
        for row in export_rows
        if str(row.get("candidate_id") or "").strip()
    }
    return [
        {
            "selected_candidate_count": len(selected_candidate_ids),
            "exported_candidate_count": len(exported_candidate_ids),
            "selected_hook_mode_counts_json": json.dumps(_count_rows_by_field(selected_rows, "hook_mode"), sort_keys=True),
            "selected_hook_archetype_counts_json": json.dumps(_count_rows_by_field(selected_rows, "hook_archetype"), sort_keys=True),
            "selected_dimension_averages_json": json.dumps(_average_numeric_fields(selected_rows), sort_keys=True),
            "exported_hook_mode_counts_json": json.dumps(_count_rows_by_field(export_rows, "hook_mode"), sort_keys=True),
            "exported_hook_archetype_counts_json": json.dumps(_count_rows_by_field(export_rows, "hook_archetype"), sort_keys=True),
            "strong_fused_weak_hook_count": sum(int(bool(row.get("strong_fused_weak_hook"))) for row in comparison_rows) or int(report_disagreement["strong_fused_weak_hook_count"]),
            "approved_reject_hook_count": sum(int(bool(row.get("approved_reject_hook"))) for row in comparison_rows) or int(report_disagreement["approved_reject_hook_count"]),
            "reject_to_synthetic_count": sum(int(bool(row.get("reject_to_synthetic"))) for row in comparison_rows) or int(report_disagreement["reject_to_synthetic_count"]),
            "natural_to_synthetic_count": sum(int(bool(row.get("natural_to_synthetic"))) for row in comparison_rows) or int(report_disagreement["natural_to_synthetic_count"]),
        }
    ]


def _count_rows_by_field(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(field) or "").strip() or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return counts


def _average_numeric_fields(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    numeric_fields = (
        "hook_strength",
        "intensity_score",
        "clarity_score",
        "novelty_score",
        "context_sufficiency_score",
        "payoff_readability_score",
        "title_thumbnail_potential_score",
        "authenticity_risk_score",
        "sound_off_legibility_score",
    )
    averages: dict[str, float | None] = {}
    for field in numeric_fields:
        values: list[float] = []
        for row in rows:
            value = row.get(field)
            try:
                if value not in (None, ""):
                    values.append(float(value))
            except (TypeError, ValueError):
                continue
        averages[field] = round(sum(values) / len(values), 4) if values else None
    return averages


def _aggregate_posted_metric_rows(rows: list[dict[str, Any]], group_key: str) -> dict[str, Any]:
    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        bucket_key = str(row.get(group_key) or "unknown").strip() or "unknown"
        bucket = buckets.setdefault(
            bucket_key,
            {
                "post_count": 0,
                "snapshot_count": 0,
                "sum_view_count": 0,
                "sum_like_count": 0,
                "sum_comment_count": 0,
                "sum_share_count": 0,
                "sum_save_count": 0,
                "sum_watch_time_seconds": 0.0,
                "average_watch_time_seconds_values": [],
                "completion_rate_values": [],
                "engagement_rate_values": [],
                "target_score_values": [],
                "latest_snapshot_timestamp": None,
                "missing_metrics_count": 0,
                "eligible_snapshot_count": 0,
                "recoverable_snapshot_count": 0,
                "coverage_tiers": {},
                "missing_field_counts": {},
                "_post_ids": set(),
            },
        )
        post_record_id = str(row.get("post_record_id") or "").strip()
        if post_record_id and post_record_id not in bucket["_post_ids"]:
            bucket["_post_ids"].add(post_record_id)
            bucket["post_count"] += 1
        bucket["snapshot_count"] += 1
        missing_row = True
        for field_name in ("view_count", "like_count", "comment_count", "share_count", "save_count"):
            value = row.get(field_name)
            if value is not None:
                bucket[f"sum_{field_name}"] += int(value)
                missing_row = False
        watch_time_seconds = row.get("watch_time_seconds")
        if watch_time_seconds is not None:
            bucket["sum_watch_time_seconds"] += float(watch_time_seconds)
            missing_row = False
        average_watch_time_seconds = row.get("average_watch_time_seconds")
        if average_watch_time_seconds is not None:
            bucket["average_watch_time_seconds_values"].append(float(average_watch_time_seconds))
            missing_row = False
        completion_rate = row.get("completion_rate")
        if completion_rate is not None:
            bucket["completion_rate_values"].append(float(completion_rate))
            missing_row = False
        engagement_rate = row.get("engagement_rate")
        if engagement_rate is not None:
            bucket["engagement_rate_values"].append(float(engagement_rate))
            missing_row = False
        target_score = row.get("post_performance_target_score")
        if target_score is not None:
            bucket["target_score_values"].append(float(target_score))
        if bool(row.get("post_performance_label_eligible")):
            bucket["eligible_snapshot_count"] += 1
        if bool(row.get("post_performance_recoverable")):
            bucket["recoverable_snapshot_count"] += 1
        tier = str(row.get("post_performance_coverage_tier") or "unknown").strip() or "unknown"
        bucket["coverage_tiers"][tier] = int(bucket["coverage_tiers"].get(tier, 0)) + 1
        for field_name in list(row.get("post_performance_missing_fields", [])) if isinstance(row.get("post_performance_missing_fields"), list) else []:
            normalized_field = str(field_name or "").strip()
            if normalized_field:
                bucket["missing_field_counts"][normalized_field] = int(bucket["missing_field_counts"].get(normalized_field, 0)) + 1
        captured_at = str(row.get("captured_at") or "").strip() or None
        if captured_at is not None and (
            bucket["latest_snapshot_timestamp"] is None or captured_at > str(bucket["latest_snapshot_timestamp"])
        ):
            bucket["latest_snapshot_timestamp"] = captured_at
        if missing_row:
            bucket["missing_metrics_count"] += 1
    result: dict[str, Any] = {}
    for bucket_key, bucket in buckets.items():
        result[bucket_key] = {
            "post_count": bucket["post_count"],
            "snapshot_count": bucket["snapshot_count"],
            "sum_view_count": bucket["sum_view_count"],
            "sum_like_count": bucket["sum_like_count"],
            "sum_comment_count": bucket["sum_comment_count"],
            "sum_share_count": bucket["sum_share_count"],
            "sum_save_count": bucket["sum_save_count"],
            "sum_watch_time_seconds": round(float(bucket["sum_watch_time_seconds"]), 4),
            "avg_average_watch_time_seconds": _average_or_none(bucket["average_watch_time_seconds_values"]),
            "avg_completion_rate": _average_or_none(bucket["completion_rate_values"]),
            "avg_engagement_rate": _average_or_none(bucket["engagement_rate_values"]),
            "avg_post_performance_target_score": _average_or_none(bucket["target_score_values"]),
            "latest_snapshot_timestamp": bucket["latest_snapshot_timestamp"],
            "missing_metrics_count": bucket["missing_metrics_count"],
            "eligible_snapshot_count": bucket["eligible_snapshot_count"],
            "recoverable_snapshot_count": bucket["recoverable_snapshot_count"],
            "post_performance_coverage_tiers": dict(bucket["coverage_tiers"]),
            "post_performance_missing_field_counts": dict(bucket["missing_field_counts"]),
        }
    return result


def _average_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _metrics_coverage_status(rows: list[dict[str, Any]]) -> str:
    total = len(rows)
    if total <= 0:
        return "empty"
    missing = 0
    for row in rows:
        if all(
            row.get(field_name) is None
            for field_name in (
                "view_count",
                "like_count",
                "comment_count",
                "share_count",
                "save_count",
                "watch_time_seconds",
                "average_watch_time_seconds",
                "completion_rate",
                "engagement_rate",
            )
        ):
            missing += 1
    if missing == 0:
        return "complete"
    if missing == total:
        return "missing"
    return "partial"


def _post_performance_label_fields(row: dict[str, Any]) -> dict[str, Any]:
    post_record_id = str(row.get("post_record_id") or "").strip()
    metrics_coverage_status = str(row.get("metrics_coverage_status") or "").strip() or _metrics_coverage_status([row])
    completion = _bounded_rate(row.get("completion_rate"))
    engagement = _bounded_rate(row.get("engagement_rate"))
    avg_watch_time = _float_or_none(row.get("average_watch_time_seconds"))
    watch_time = _float_or_none(row.get("watch_time_seconds"))
    view_count = _float_or_none(row.get("view_count"))
    has_avg_watch = avg_watch_time is not None and avg_watch_time > 0.0
    has_watch_time = watch_time is not None and watch_time > 0.0
    has_view_count = view_count is not None and view_count > 0.0
    present_signals = {
        "engagement_rate": engagement is not None,
        "completion_rate": completion is not None,
        "average_watch_time_seconds": has_avg_watch,
        "watch_time_seconds": has_watch_time,
        "view_count": has_view_count,
    }
    recoverable, minimum_signal_set, missing_fields, recoverability_reason = _post_performance_recoverability(present_signals)

    if not post_record_id:
        return {
            "post_performance_coverage_tier": "no_post_record",
            "post_performance_label_eligible": False,
            "post_performance_target_score": None,
            "post_performance_target_bucket": None,
            "post_performance_label_reason": "no_post_record",
            "post_performance_recoverable": False,
            "post_performance_missing_fields": [],
            "post_performance_minimum_signal_set": [],
            "post_performance_recoverability_reason": "no_post_record",
        }
    if metrics_coverage_status == "missing":
        return {
            "post_performance_coverage_tier": "posted_no_metrics",
            "post_performance_label_eligible": False,
            "post_performance_target_score": None,
            "post_performance_target_bucket": None,
            "post_performance_label_reason": "missing_metrics_snapshot_fields",
            "post_performance_recoverable": False,
            "post_performance_missing_fields": ["engagement_rate", "completion_rate", "average_watch_time_seconds", "watch_time_seconds", "view_count"],
            "post_performance_minimum_signal_set": [],
            "post_performance_recoverability_reason": "missing_metrics_snapshot_fields",
        }
    has_watch_context = any(value is not None and value > 0.0 for value in (avg_watch_time, watch_time))
    has_volume_context = view_count is not None and view_count > 0.0
    if completion is None or engagement is None or not (has_watch_context or has_volume_context):
        return {
            "post_performance_coverage_tier": "posted_sparse_metrics",
            "post_performance_label_eligible": False,
            "post_performance_target_score": None,
            "post_performance_target_bucket": None,
            "post_performance_label_reason": "insufficient_core_engagement_coverage",
            "post_performance_recoverable": recoverable,
            "post_performance_missing_fields": missing_fields,
            "post_performance_minimum_signal_set": minimum_signal_set,
            "post_performance_recoverability_reason": recoverability_reason,
        }
    view_component = min(math.log1p(max(view_count or 0.0, 0.0)) / math.log1p(10000.0), 1.0)
    watch_component = 0.0
    if avg_watch_time is not None and avg_watch_time > 0.0:
        watch_component = min(avg_watch_time / 15.0, 1.0)
    elif watch_time is not None and view_count is not None and view_count > 0.0:
        watch_component = min((watch_time / view_count) / 15.0, 1.0)
    score = round((engagement * 0.45) + (completion * 0.35) + (watch_component * 0.15) + (view_component * 0.05), 6)
    return {
        "post_performance_coverage_tier": "posted_usable_metrics",
        "post_performance_label_eligible": True,
        "post_performance_target_score": score,
        "post_performance_target_bucket": _post_performance_bucket(score),
        "post_performance_label_reason": "eligible_usable_metrics",
        "post_performance_recoverable": True,
        "post_performance_missing_fields": [],
        "post_performance_minimum_signal_set": ["engagement_rate", "completion_rate", "watch_or_view_context"],
        "post_performance_recoverability_reason": "eligible_usable_metrics",
    }


def _bounded_rate(value: Any) -> float | None:
    numeric = _float_or_none(value)
    if numeric is None:
        return None
    return min(max(numeric, 0.0), 1.0)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _post_performance_bucket(score: float) -> str:
    if score < 0.35:
        return "low"
    if score < 0.65:
        return "medium"
    return "high"


def _count_post_performance_tiers(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        tier = str(row.get("post_performance_coverage_tier") or "unknown").strip() or "unknown"
        counts[tier] = counts.get(tier, 0) + 1
    return counts


def _count_post_performance_missing_fields(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for field_name in list(row.get("post_performance_missing_fields", [])) if isinstance(row.get("post_performance_missing_fields"), list) else []:
            normalized = str(field_name or "").strip()
            if normalized:
                counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _post_performance_recoverability(present_signals: dict[str, bool]) -> tuple[bool, list[str], list[str], str]:
    candidate_sets = (
        ["engagement_rate", "completion_rate"],
        ["engagement_rate", "average_watch_time_seconds"],
        ["completion_rate", "average_watch_time_seconds"],
        ["engagement_rate", "watch_time_seconds"],
        ["completion_rate", "watch_time_seconds"],
        ["view_count", "engagement_rate", "average_watch_time_seconds"],
        ["view_count", "engagement_rate", "watch_time_seconds"],
        ["view_count", "completion_rate", "average_watch_time_seconds"],
        ["view_count", "completion_rate", "watch_time_seconds"],
    )
    missing_by_set: list[tuple[int, list[str], list[str]]] = []
    for signal_set in candidate_sets:
        missing_fields = [field_name for field_name in signal_set if not present_signals.get(field_name, False)]
        missing_by_set.append((len(missing_fields), signal_set, missing_fields))
    missing_by_set.sort(key=lambda item: (item[0], len(item[1]), item[1]))
    missing_count, minimum_signal_set, missing_fields = missing_by_set[0]
    if missing_count == 0:
        return True, minimum_signal_set, [], "eligible_signal_set_present"
    if missing_count == 1:
        return True, minimum_signal_set, missing_fields, "one_field_away_from_eligibility"
    return False, minimum_signal_set, missing_fields, "insufficient_signal_coverage"


def _posted_metrics_evidence_mode(row: dict[str, Any]) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else _load_json_dict(row.get("metadata_json"))
    if isinstance(metadata, dict) and bool(metadata.get("synthetic_benchmark")):
        return "synthetic_augmented"
    for value in (
        row.get("manifest_path"),
        row.get("post_ledger_manifest_path"),
        row.get("external_url"),
    ):
        normalized = str(value or "").strip().lower()
        if normalized and "synthetic" in normalized:
            return "synthetic_augmented"
    return "real_only"


def _latest_metrics_timestamp_by_post_record(rows: list[dict[str, Any]]) -> dict[str, str]:
    latest: dict[str, str] = {}
    for row in rows:
        post_record_id = str(row.get("post_record_id") or "").strip()
        captured_at = str(row.get("captured_at") or "").strip()
        if not post_record_id or not captured_at:
            continue
        current = latest.get(post_record_id)
        if current is None or captured_at > current:
            latest[post_record_id] = captured_at
    return latest


_CLIP_COLUMNS = (
    "game",
    "source",
    "proxy_sidecar_path",
    "runtime_sidecar_path",
    "fused_sidecar_path",
    "has_proxy_sidecar",
    "has_runtime_sidecar",
    "has_fused_sidecar",
    "proxy_review_status",
    "runtime_review_status",
    "fused_review_status",
    "fixture_ids_json",
    "top_proxy_action",
    "top_proxy_score",
    "top_fused_action",
    "top_fused_score",
    "has_review_disagreement",
    "has_cross_layer_disagreement",
    "has_trial_preference",
    "last_seen_at",
)
_PROXY_WINDOW_COLUMNS = (
    "scan_id",
    "window_index",
    "game",
    "source",
    "sidecar_path",
    "start_seconds",
    "end_seconds",
    "proxy_score",
    "signal_count",
    "recommended_action",
    "sources_json",
    "source_families_json",
    "review_status",
)
_RUNTIME_ANALYSIS_COLUMNS = (
    "analysis_id",
    "game",
    "source",
    "sidecar_path",
    "status",
    "frame_count",
    "confirmed_detection_count",
    "event_count",
    "runtime_review_status",
    "runtime_review_session_id",
    "runtime_recommended_action",
    "runtime_highlight_score",
    "last_ingested_mtime",
)
_RUNTIME_EVENT_COLUMNS = (
    "analysis_id",
    "event_index",
    "event_id",
    "game",
    "source",
    "sidecar_path",
    "event_type",
    "confidence",
    "start_timestamp",
    "end_timestamp",
    "entity_id",
    "ability_id",
    "equipment_id",
    "event_row_id",
    "review_status",
    "recommended_action",
)
_RUNTIME_DETECTION_COLUMNS = (
    "analysis_id",
    "detection_index",
    "game",
    "source",
    "sidecar_path",
    "asset_id",
    "roi_ref",
    "entity_id",
    "ability_id",
    "equipment_id",
    "first_timestamp",
    "last_timestamp",
    "peak_score",
)
_FUSED_ANALYSIS_COLUMNS = (
    "fusion_id",
    "game",
    "source",
    "sidecar_path",
    "status",
    "normalized_signal_count",
    "fused_event_count",
    "fused_reviewed_event_count",
    "fused_review_session_id",
    "fused_review_status",
    "last_ingested_mtime",
)
_FUSED_EVENT_COLUMNS = (
    "fusion_id",
    "event_index",
    "event_id",
    "game",
    "source",
    "sidecar_path",
    "event_type",
    "confidence",
    "final_score",
    "gate_status",
    "synergy_applied",
    "synergy_multiplier",
    "minimum_required_signals_met",
    "suggested_start_timestamp",
    "suggested_end_timestamp",
    "entity_id",
    "ability_id",
    "equipment_id",
    "event_row_id",
    "review_status",
    "recommended_action",
)
_FUSED_SIGNAL_REF_COLUMNS = (
    "fusion_id",
    "event_id",
    "signal_index",
    "signal_id",
    "signal_type",
    "producer_family",
    "source_family",
    "asset_id",
    "roi_ref",
)
_RUNTIME_REVIEW_SESSION_COLUMNS = (
    "session_id",
    "game",
    "manifest_path",
    "selection_source",
    "selection_action_filter",
    "created_at",
    "applied_at",
    "cleanup_at",
    "item_count",
    "approved_count",
    "rejected_count",
    "unreviewed_count",
)
_RUNTIME_REVIEW_ITEM_COLUMNS = (
    "session_id",
    "item_index",
    "game",
    "sidecar_path",
    "source",
    "analysis_id",
    "review_status",
    "apply_status",
    "highlight_score",
    "recommended_action",
    "gpt_meta_path",
    "gpt_processed_path",
    "gpt_final_path",
)
_FUSED_REVIEW_SESSION_COLUMNS = (
    "session_id",
    "game",
    "manifest_path",
    "selection_source",
    "selection_action_filter",
    "selection_event_type_filter",
    "created_at",
    "applied_at",
    "cleanup_at",
    "item_count",
    "approved_count",
    "rejected_count",
    "unreviewed_count",
)
_FUSED_REVIEW_ITEM_COLUMNS = (
    "session_id",
    "item_index",
    "game",
    "sidecar_path",
    "source",
    "fusion_id",
    "event_id",
    "event_type",
    "review_status",
    "apply_status",
    "final_score",
    "recommended_action",
    "gate_status",
    "gpt_meta_path",
    "gpt_processed_path",
    "gpt_final_path",
)
_FIXTURE_COMPARISON_COLUMNS = (
    "comparison_path",
    "row_index",
    "fixture_id",
    "label",
    "artifact_layer",
    "game",
    "source",
    "coverage_status",
    "review_status",
    "baseline_sidecar_path",
    "trial_sidecar_path",
    "baseline_action",
    "trial_action",
    "baseline_score",
    "trial_score",
    "score_delta",
    "shortlist_changed",
    "rerank_changed",
    "stage_latency_deltas_json",
    "recommendation_signal",
    "recommendation_decision",
    "recommendation_reason",
)
_FIXTURE_TRIAL_RUN_COLUMNS = (
    "trial_name",
    "trial_root",
    "manifest_path",
    "proxy_sidecar_root",
    "runtime_sidecar_root",
    "fused_sidecar_root",
    "fixture_manifest_path",
    "fixture_source_manifest_path",
    "status",
    "completed_fixture_count",
    "failed_fixture_count",
    "effective_overrides_json",
)
_FIXTURE_TRIAL_RUN_FIXTURE_COLUMNS = (
    "trial_name",
    "fixture_index",
    "fixture_id",
    "game",
    "source_path",
    "status",
    "failure_reason",
    "proxy_sidecar_path",
    "runtime_sidecar_path",
    "fused_sidecar_path",
)
_FIXTURE_TRIAL_BATCH_COLUMNS = (
    "batch_name",
    "manifest_path",
    "baseline_trial_name",
    "overall_recommendation_decision",
    "overall_recommendation_trial_name",
    "selected_trials_json",
)
_FIXTURE_TRIAL_BATCH_COMPARISON_COLUMNS = (
    "batch_name",
    "comparison_index",
    "trial_name",
    "comparison_report_path",
    "artifact_layer",
    "comparison_status",
    "recommendation_decision",
)
_HOOK_CANDIDATE_MANIFEST_COLUMNS = (
    "manifest_path",
    "game",
    "source",
    "fused_sidecar_path",
    "hook_candidate_count",
)
_HOOK_CANDIDATE_COLUMNS = (
    "manifest_path",
    "hook_index",
    "hook_id",
    "candidate_id",
    "event_id",
    "game",
    "source",
    "fixture_id",
    "fused_sidecar_path",
    "lifecycle_state",
    "hook_archetype",
    "hook_mode",
    "hook_strength",
    "intensity_score",
    "clarity_score",
    "novelty_score",
    "context_sufficiency_score",
    "payoff_readability_score",
    "title_thumbnail_potential_score",
    "authenticity_risk_score",
    "sound_off_legibility_score",
    "packaging_strategy",
    "rejection_reason",
    "highlight_selection_manifest_path",
    "metadata_summary_json",
    "created_at",
)
_HOOK_COMPARISON_REPORT_COLUMNS = (
    "report_path",
    "fixture_manifest_path",
    "baseline_sidecar_root",
    "trial_sidecar_root",
    "comparison_row_count",
    "recommendation_decision",
    "recommendation_reason",
)
_HOOK_COMPARISON_COLUMNS = (
    "report_path",
    "row_index",
    "fixture_id",
    "label",
    "game",
    "source",
    "candidate_id",
    "event_id",
    "comparison_status",
    "review_status",
    "baseline_manifest_path",
    "trial_manifest_path",
    "baseline_fused_sidecar_path",
    "trial_fused_sidecar_path",
    "baseline_hook_mode",
    "trial_hook_mode",
    "baseline_hook_archetype",
    "trial_hook_archetype",
    "baseline_hook_strength",
    "trial_hook_strength",
    "hook_strength_delta",
    "baseline_lifecycle_state",
    "trial_lifecycle_state",
    "baseline_selection_manifest_path",
    "trial_selection_manifest_path",
    "strong_fused_weak_hook",
    "approved_reject_hook",
    "reject_to_synthetic",
    "natural_to_synthetic",
    "recommendation_signal",
    "recommendation_decision",
    "recommendation_reason",
)
_HOOK_EVALUATION_REPORT_COLUMNS = (
    "report_path",
    "fixture_manifest_path",
    "baseline_sidecar_root",
    "trial_sidecar_root",
    "registry_path",
    "game",
    "comparison_row_count",
    "recommendation_decision",
    "recommendation_reason",
    "selected_candidate_count",
    "exported_candidate_count",
    "strong_fused_weak_hook_count",
    "approved_reject_hook_count",
    "reject_to_synthetic_count",
    "natural_to_synthetic_count",
    "hook_artifacts_policy",
    "future_gate_readiness",
    "created_at",
)
_HIGHLIGHT_EXPORT_BATCH_COLUMNS = (
    "manifest_path",
    "export_batch_id",
    "game",
    "workflow_run_id",
    "selection_manifest_path",
    "fused_sidecar_paths_json",
    "hook_manifest_paths_json",
    "selection_manifest_paths_json",
    "export_count",
    "created_at",
)
_HIGHLIGHT_EXPORT_COLUMNS = (
    "manifest_path",
    "export_batch_id",
    "export_index",
    "export_id",
    "candidate_id",
    "event_id",
    "hook_id",
    "fixture_id",
    "game",
    "source",
    "fused_sidecar_path",
    "hook_manifest_path",
    "highlight_selection_manifest_path",
    "start_seconds",
    "end_seconds",
    "final_score",
    "hook_archetype",
    "hook_mode",
    "packaging_strategy",
    "export_status",
    "export_artifact_path",
    "otio_path",
    "selected_highlight_details_json",
    "metadata_json",
)
_POST_LEDGER_COLUMNS = (
    "manifest_path",
    "ledger_id",
    "platform",
    "account_id",
    "workflow_run_id",
    "posted_count",
    "created_at",
)
_POSTED_HIGHLIGHT_COLUMNS = (
    "manifest_path",
    "ledger_id",
    "record_index",
    "post_record_id",
    "export_id",
    "candidate_id",
    "event_id",
    "hook_id",
    "export_batch_manifest_path",
    "posted_at",
    "post_status",
    "external_post_id",
    "external_url",
    "platform",
    "account_id",
    "caption_ref",
    "caption_text",
    "duration_seconds",
    "media_asset_path",
    "initial_view_count",
    "initial_like_count",
    "initial_comment_count",
    "selected_highlight_details_json",
    "game",
)
_POSTED_METRICS_SNAPSHOT_COLUMNS = (
    "manifest_path",
    "snapshot_id",
    "platform",
    "account_id",
    "workflow_run_id",
    "captured_at",
    "snapshot_count",
)
_POSTED_METRICS_SNAPSHOT_ROW_COLUMNS = (
    "manifest_path",
    "snapshot_index",
    "snapshot_row_id",
    "post_record_id",
    "export_id",
    "candidate_id",
    "hook_id",
    "post_ledger_manifest_path",
    "captured_at",
    "platform",
    "account_id",
    "external_post_id",
    "external_url",
    "view_count",
    "like_count",
    "comment_count",
    "share_count",
    "save_count",
    "watch_time_seconds",
    "average_watch_time_seconds",
    "completion_rate",
    "engagement_rate",
    "metadata_json",
    "game",
)
_SHADOW_EVALUATION_POLICY_COLUMNS = (
    "manifest_path",
    "policy_id",
    "created_at",
    "targets_json",
)
_SHADOW_RANKING_MODEL_COLUMNS = (
    "manifest_path",
    "model_id",
    "created_at",
    "model_family",
    "model_version",
    "training_dataset_manifest_path",
    "training_target",
    "split_key",
    "train_fraction",
    "row_count",
    "train_row_count",
    "eval_row_count",
    "label_positive_count",
    "label_negative_count",
    "feature_fields_json",
    "training_metrics_json",
    "evaluation_metrics_json",
    "filters_json",
    "warning_count",
)
_SHADOW_RANKING_EXPERIMENT_COLUMNS = (
    "manifest_path",
    "experiment_id",
    "created_at",
    "model_path",
    "model_id",
    "model_family",
    "model_version",
    "dataset_manifest_path",
    "dataset_export_id",
    "training_target",
    "split_key",
    "train_fraction",
    "replay_manifest_path",
    "comparison_report_path",
    "replay_row_count",
    "comparison_row_count",
    "recommendation_decision",
    "recommendation_reason",
    "training_metrics_json",
    "evaluation_metrics_json",
    "comparison_summary_json",
    "filters_json",
)
_SHADOW_RANKING_EXPERIMENT_LEDGER_COLUMNS = (
    "manifest_path",
    "ledger_id",
    "created_at",
    "policy_manifest_path",
    "policy_id",
    "experiment_manifest_path",
    "experiment_id",
    "model_id",
    "model_family",
    "model_version",
    "dataset_manifest_path",
    "dataset_export_id",
    "training_target",
    "evaluation_target",
    "replay_manifest_path",
    "comparison_report_path",
    "coverage_status",
    "slice_count",
    "recommendation_decision",
    "recommendation_reason",
    "blocking_reasons_json",
    "protected_regression_count",
    "global_metrics_json",
    "global_primary_metric_name",
    "global_primary_metric_delta",
    "filters_json",
)
_SHADOW_RANKING_EXPERIMENT_SLICE_COLUMNS = (
    "manifest_path",
    "ledger_id",
    "slice_index",
    "policy_id",
    "experiment_id",
    "model_id",
    "model_family",
    "model_version",
    "training_target",
    "evaluation_target",
    "slice_type",
    "slice_value",
    "coverage_status",
    "row_count",
    "covered_row_count",
    "positive_count",
    "top_k",
    "shadow_topk_hits",
    "heuristic_topk_hits",
    "shadow_precision_at_k",
    "heuristic_precision_at_k",
    "shadow_ranking_gain",
    "heuristic_ranking_gain",
    "shadow_false_positive_cost",
    "heuristic_false_positive_cost",
    "shadow_pearson_correlation",
    "heuristic_pearson_correlation",
    "primary_metric_name",
    "shadow_primary_metric",
    "heuristic_primary_metric",
    "primary_metric_delta",
    "game",
    "platform",
)
_SHADOW_RANKING_REPLAY_COLUMNS = (
    "manifest_path",
    "replay_id",
    "dataset_manifest_path",
    "dataset_export_id",
    "model_family",
    "model_version",
    "row_count",
    "created_at",
)
_SHADOW_RANKING_REPLAY_ROW_COLUMNS = (
    "manifest_path",
    "replay_id",
    "row_index",
    "candidate_id",
    "event_id",
    "hook_id",
    "export_id",
    "post_record_id",
    "game",
    "fixture_id",
    "source",
    "platform",
    "account_id",
    "heuristic_final_score",
    "heuristic_recommended_action",
    "heuristic_lifecycle_state",
    "review_outcome",
    "export_present",
    "post_present",
    "metrics_present",
    "latest_view_count",
    "latest_engagement_rate",
    "hook_mode",
    "hook_archetype",
    "packaging_strategy",
    "label_positive",
    "label_score",
    "predicted_candidate_score",
    "predicted_export_score",
    "predicted_post_performance_score",
    "predicted_rank",
    "heuristic_rank",
    "feature_values_json",
)
_SHADOW_RANKING_COMPARISON_COLUMNS = (
    "report_path",
    "comparison_id",
    "row_index",
    "replay_manifest_path",
    "replay_id",
    "dataset_manifest_path",
    "model_family",
    "model_version",
    "candidate_id",
    "event_id",
    "game",
    "fixture_id",
    "platform",
    "heuristic_final_score",
    "predicted_candidate_score",
    "heuristic_rank",
    "predicted_rank",
    "rank_delta",
    "label_positive",
    "label_score",
    "review_outcome",
    "export_present",
    "post_present",
    "latest_view_count",
    "latest_engagement_rate",
    "recommendation_decision",
    "recommendation_reason",
)
_SHADOW_MODEL_FAMILY_COMPARISON_COLUMNS = (
    "manifest_path",
    "comparison_id",
    "row_index",
    "source_schema_version",
    "model_id",
    "model_family",
    "model_version",
    "training_target",
    "recommendation_decision",
    "recommendation_reason",
    "primary_metric_name",
    "primary_metric_delta",
    "experiment_id",
    "ledger_id",
    "game",
    "platform",
)
_SHADOW_BENCHMARK_MATRIX_COLUMNS = (
    "manifest_path",
    "benchmark_id",
    "created_at",
    "dataset_manifest_path",
    "dataset_export_id",
    "policy_path",
    "model_families_json",
    "training_targets_json",
    "split_key",
    "train_fraction",
    "filters_json",
    "run_count",
    "benchmark_recommendation",
    "blocked_run_count",
    "inconclusive_run_count",
    "failed_run_count",
    "warning_count",
)
_SHADOW_BENCHMARK_RUN_COLUMNS = (
    "manifest_path",
    "benchmark_id",
    "run_index",
    "run_id",
    "status",
    "model_family",
    "training_target",
    "evaluation_target",
    "split_key",
    "train_fraction",
    "model_manifest_path",
    "experiment_manifest_path",
    "replay_manifest_path",
    "comparison_report_path",
    "governed_ledger_manifest_path",
    "recommendation_decision",
    "recommendation_reason",
    "coverage_status",
    "evidence_mode",
    "synthetic_row_count",
    "real_row_count",
    "primary_metric_name",
    "primary_metric_delta",
    "protected_regression_count",
    "blocking_reasons_json",
    "failure_reason",
    "game",
    "platform",
)
_SHADOW_BENCHMARK_REVIEW_COLUMNS = (
    "manifest_path",
    "review_id",
    "created_at",
    "source_benchmark_manifest_paths_json",
    "reviewed_targets_json",
    "reviewed_families_json",
    "filters_json",
    "target_count",
    "ready_target_count",
    "label_calibration_target_count",
    "feature_cleanup_target_count",
    "coverage_blocked_target_count",
    "warning_count",
)
_SHADOW_TARGET_READINESS_COLUMNS = (
    "manifest_path",
    "review_id",
    "target_index",
    "training_target",
    "current_best_family",
    "best_recommendation_decision",
    "current_best_evidence_mode",
    "evidence_modes_json",
    "synthetic_augmented_run_count",
    "real_only_run_count",
    "primary_metric_name",
    "primary_metric_delta",
    "run_count",
    "successful_run_count",
    "win_count",
    "keep_current_count",
    "blocked_count",
    "inconclusive_count",
    "failed_count",
    "dominant_failure_modes_json",
    "confidence_level",
    "readiness_classification",
    "recommended_next_action",
    "game",
    "platform",
)
_REAL_POSTED_LINEAGE_IMPORT_COLUMNS = (
    "manifest_path",
    "import_id",
    "created_at",
    "workspace_root",
    "registry_path",
    "refresh_root",
    "source_roots_json",
    "scanned_roots_json",
    "filters_json",
    "workspace_artifact_count",
    "source_artifact_count",
    "discovered_counts_json",
    "imported_counts_json",
    "coverage_inventory_json",
    "source_root_summaries_json",
    "unresolved_lineage_counts_json",
    "eligible_real_post_performance_label_count",
    "imported_candidate_count",
    "imported_hook_count",
    "warning_count",
    "game",
    "platform",
)
_SHADOW_BENCHMARK_EVIDENCE_COMPARISON_COLUMNS = (
    "manifest_path",
    "comparison_id",
    "row_index",
    "training_target",
    "real_manifest_path",
    "synthetic_manifest_path",
    "real_current_best_family",
    "synthetic_current_best_family",
    "real_best_recommendation_decision",
    "synthetic_best_recommendation_decision",
    "real_current_best_evidence_mode",
    "synthetic_current_best_evidence_mode",
    "real_readiness_classification",
    "synthetic_readiness_classification",
    "real_primary_metric_name",
    "synthetic_primary_metric_name",
    "real_primary_metric_delta",
    "synthetic_primary_metric_delta",
    "primary_metric_delta_gap",
    "real_confidence_level",
    "synthetic_confidence_level",
    "real_successful_run_count",
    "synthetic_successful_run_count",
    "real_run_count",
    "synthetic_run_count",
    "family_winner_changed",
    "readiness_changed",
    "recommendation_changed",
    "disagreement_indicators_json",
    "game",
    "platform",
)
_REAL_ARTIFACT_INTAKE_DASHBOARD_COLUMNS = (
    "manifest_path",
    "generated_at",
    "intake_root",
    "filters_json",
    "headline_status",
    "intake_status",
    "bundle_count",
    "warning_count",
    "benchmark_ready_bundle_count",
    "eligible_real_post_performance_label_count",
    "preflight_trend_status",
    "preflight_entry_count",
    "refresh_outcome_trend_status",
    "refresh_outcome_entry_count",
    "history_alignment_status",
    "real_vs_synthetic_gap_status",
    "next_focus",
    "game",
    "platform",
)
_WORKFLOW_RUN_COLUMNS = (
    "workflow_run_id",
    "manifest_path",
    "workflow_type",
    "stage",
    "status",
    "registry_path",
    "game_filter",
    "fixture_id_filter",
    "item_counts_json",
    "linked_artifacts_json",
    "error",
    "created_at",
    "updated_at",
)
_WORKFLOW_RUN_ITEM_COLUMNS = (
    "workflow_run_id",
    "item_index",
    "candidate_id",
    "item_status",
    "game",
    "source",
    "fixture_id",
    "event_id",
    "lifecycle_state",
    "fused_sidecar_path",
    "highlight_selection_manifest_path",
    "export_artifact_path",
    "post_ledger_path",
    "hook_manifest_path",
    "created_at",
)
