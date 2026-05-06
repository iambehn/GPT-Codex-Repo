from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.clip_registry import (
    load_candidate_lifecycle_details,
    load_hook_candidate_details,
    load_workflow_run_details,
    refresh_clip_registry,
)
from pipeline.highlight_selection_export import export_highlight_selection


REPO_ROOT = Path(__file__).resolve().parent.parent
HIGHLIGHT_EXPORT_BATCH_SCHEMA_VERSION = "highlight_export_batch_v1"
POSTED_HIGHLIGHT_LEDGER_SCHEMA_VERSION = "posted_highlight_ledger_v1"
POSTED_HIGHLIGHT_METRICS_SNAPSHOT_SCHEMA_VERSION = "posted_highlight_metrics_snapshot_v1"
DEFAULT_EXPORT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "highlight_exports"
DEFAULT_POST_LEDGER_OUTPUT_ROOT = REPO_ROOT / "outputs" / "posted_highlight_ledgers"
DEFAULT_POSTED_METRICS_OUTPUT_ROOT = REPO_ROOT / "outputs" / "posted_highlight_metrics"
_SYNTHETIC_POST_COVERAGE_PROFILES = {"balanced", "strong", "weak"}


def create_highlight_export_batch(
    *,
    registry_path: str | Path | None = None,
    workflow_run_id: str | None = None,
    selection_manifest: str | Path | None = None,
    game: str | None = None,
    fixture_id: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    lifecycle_rows = _selected_lifecycle_rows(
        registry_path=registry_path,
        workflow_run_id=workflow_run_id,
        selection_manifest=selection_manifest,
        game=game,
        fixture_id=fixture_id,
    )
    if not lifecycle_rows:
        return {
            "ok": False,
            "status": "no_selected_candidates",
            "error": "no selected-for-export candidates matched the requested filters",
        }

    hook_by_candidate = _hook_details_by_candidate(
        registry_path=registry_path,
        candidate_ids=[str(row.get("candidate_id") or "").strip() for row in lifecycle_rows],
    )
    target = _resolve_path(output_path) if output_path is not None else _default_export_output_path(
        game=str(lifecycle_rows[0].get("game") or "").strip() or "unknown_game",
        export_batch_id=_export_batch_id(
            workflow_run_id=workflow_run_id,
            selection_manifest=selection_manifest,
            candidate_ids=[str(row.get("candidate_id") or "").strip() for row in lifecycle_rows],
        ),
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    artifact_root = target.parent / f"{target.stem}_artifacts"
    artifact_root.mkdir(parents=True, exist_ok=True)

    exports: list[dict[str, Any]] = []
    fused_paths: set[str] = set()
    hook_paths: set[str] = set()
    selection_paths: set[str] = set()
    for row in lifecycle_rows:
        candidate_id = str(row.get("candidate_id") or "").strip()
        hook_row = hook_by_candidate.get(candidate_id)
        selection_path = str(row.get("highlight_selection_manifest_path") or "").strip() or None
        highlight = _selection_highlight(
            selection_path=selection_path,
            candidate_id=candidate_id,
            event_id=str(row.get("event_id") or "").strip(),
        )
        export_id = _export_id(
            candidate_id=candidate_id,
            workflow_run_id=workflow_run_id,
            selection_manifest=selection_manifest,
        )
        otio_path = artifact_root / f"{export_id}.otio.json"
        export_artifact_path = str(otio_path.resolve())
        export_row = {
            "export_id": export_id,
            "candidate_id": candidate_id,
            "event_id": str(row.get("event_id") or "").strip() or None,
            "hook_id": (hook_row or {}).get("hook_id"),
            "fixture_id": row.get("fixture_id"),
            "source": row.get("source"),
            "fused_sidecar_path": row.get("fused_sidecar_path"),
            "hook_manifest_path": (hook_row or {}).get("manifest_path"),
            "highlight_selection_manifest_path": selection_path,
            "start_seconds": highlight.get("start_seconds"),
            "end_seconds": highlight.get("end_seconds"),
            "final_score": row.get("final_score"),
            "hook_archetype": (hook_row or {}).get("hook_archetype"),
            "hook_mode": (hook_row or {}).get("hook_mode"),
            "packaging_strategy": (hook_row or {}).get("packaging_strategy"),
            "export_status": "exported",
            "export_artifact_path": export_artifact_path,
            "otio_path": export_artifact_path,
            "metadata_json": {
                "selection_basis": row.get("selection_basis"),
                "recommended_action": row.get("recommended_action"),
                "latest_review_status": row.get("latest_review_status"),
            },
        }
        otio_path.write_text(json.dumps(_otio_clip(export_row), indent=2), encoding="utf-8")
        exports.append(export_row)
        if str(row.get("fused_sidecar_path") or "").strip():
            fused_paths.add(str(row.get("fused_sidecar_path")))
        if str((hook_row or {}).get("manifest_path") or "").strip():
            hook_paths.add(str((hook_row or {}).get("manifest_path")))
        if selection_path:
            selection_paths.add(selection_path)

    export_batch_id = _export_batch_id(
        workflow_run_id=workflow_run_id,
        selection_manifest=selection_manifest,
        candidate_ids=[str(row.get("candidate_id") or "").strip() for row in lifecycle_rows],
    )
    manifest = {
        "schema_version": HIGHLIGHT_EXPORT_BATCH_SCHEMA_VERSION,
        "export_batch_id": export_batch_id,
        "created_at": _utc_now(),
        "game": str(lifecycle_rows[0].get("game") or "").strip() or "unknown_game",
        "workflow_run_id": str(workflow_run_id or "").strip() or None,
        "selection_manifest_path": str(_resolve_path(selection_manifest)) if selection_manifest is not None else None,
        "linked_inputs": {
            "fused_sidecar_paths": sorted(fused_paths),
            "hook_manifest_paths": sorted(hook_paths),
            "selection_manifest_paths": sorted(selection_paths),
        },
        "export_count": len(exports),
        "exports": exports,
    }
    target.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": HIGHLIGHT_EXPORT_BATCH_SCHEMA_VERSION,
        "export_batch_id": export_batch_id,
        "manifest_path": str(target),
        "export_count": len(exports),
    }


def record_post_ledger(
    export_manifest: str | Path,
    *,
    workflow_run_id: str | None = None,
    platform: str | None = None,
    account_id: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(export_manifest)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != HIGHLIGHT_EXPORT_BATCH_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "invalid_export_manifest",
            "error": "export manifest does not use highlight_export_batch_v1",
            "export_manifest_path": str(manifest_path),
        }
    exports = [row for row in list(payload.get("exports", [])) if isinstance(row, dict)]
    if not exports:
        return {
            "ok": False,
            "status": "empty_export_manifest",
            "error": "export manifest does not contain export rows",
            "export_manifest_path": str(manifest_path),
        }

    ledger_id = _post_ledger_id(
        export_manifest_path=str(manifest_path),
        workflow_run_id=workflow_run_id,
        platform=platform,
        account_id=account_id,
    )
    records: list[dict[str, Any]] = []
    for export_row in exports:
        post_record_id = _post_record_id(ledger_id=ledger_id, export_id=str(export_row.get("export_id") or "").strip())
        duration_seconds = _duration_seconds(
            export_row.get("start_seconds"),
            export_row.get("end_seconds"),
        )
        records.append(
            {
                "post_record_id": post_record_id,
                "export_id": export_row.get("export_id"),
                "candidate_id": export_row.get("candidate_id"),
                "event_id": export_row.get("event_id"),
                "hook_id": export_row.get("hook_id"),
                "export_batch_manifest_path": str(manifest_path),
                "posted_at": _utc_now(),
                "post_status": "posted",
                "external_post_id": None,
                "external_url": None,
                "platform": str(platform or "").strip() or None,
                "account_id": str(account_id or "").strip() or None,
                "caption_ref": None,
                "caption_text": None,
                "duration_seconds": duration_seconds,
                "media_asset_path": export_row.get("export_artifact_path"),
                "initial_view_count": None,
                "initial_like_count": None,
                "initial_comment_count": None,
            }
        )
    ledger = {
        "schema_version": POSTED_HIGHLIGHT_LEDGER_SCHEMA_VERSION,
        "ledger_id": ledger_id,
        "created_at": _utc_now(),
        "platform": str(platform or "").strip() or None,
        "account_id": str(account_id or "").strip() or None,
        "workflow_run_id": str(workflow_run_id or "").strip() or None,
        "posted_count": len(records),
        "posted_records": records,
    }
    target = _resolve_path(output_path) if output_path is not None else _default_post_ledger_output_path(
        game=str(payload.get("game") or "").strip() or "unknown_game",
        ledger_id=ledger_id,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": POSTED_HIGHLIGHT_LEDGER_SCHEMA_VERSION,
        "ledger_id": ledger_id,
        "manifest_path": str(target),
        "posted_count": len(records),
    }


def record_posted_metrics_snapshot(
    post_ledger_manifest: str | Path,
    *,
    workflow_run_id: str | None = None,
    platform: str | None = None,
    account_id: str | None = None,
    output_path: str | Path | None = None,
    view_count: int | None = None,
    like_count: int | None = None,
    comment_count: int | None = None,
    share_count: int | None = None,
    save_count: int | None = None,
    watch_time_seconds: float | None = None,
    average_watch_time_seconds: float | None = None,
    completion_rate: float | None = None,
    engagement_rate: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(post_ledger_manifest)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != POSTED_HIGHLIGHT_LEDGER_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "invalid_post_ledger_manifest",
            "error": "post ledger manifest does not use posted_highlight_ledger_v1",
            "post_ledger_manifest_path": str(manifest_path),
        }
    records = [row for row in list(payload.get("posted_records", [])) if isinstance(row, dict)]
    if not records:
        return {
            "ok": False,
            "status": "empty_post_ledger_manifest",
            "error": "post ledger manifest does not contain posted records",
            "post_ledger_manifest_path": str(manifest_path),
        }

    effective_platform = str(platform or payload.get("platform") or "").strip() or None
    effective_account_id = str(account_id or payload.get("account_id") or "").strip() or None
    snapshot_id = _posted_metrics_snapshot_id(
        post_ledger_manifest_path=str(manifest_path),
        workflow_run_id=workflow_run_id,
        platform=effective_platform,
        account_id=effective_account_id,
    )
    captured_at = _utc_now()
    metrics_defaults = {
        "view_count": view_count,
        "like_count": like_count,
        "comment_count": comment_count,
        "share_count": share_count,
        "save_count": save_count,
        "watch_time_seconds": watch_time_seconds,
        "average_watch_time_seconds": average_watch_time_seconds,
        "completion_rate": completion_rate,
        "engagement_rate": engagement_rate,
    }
    snapshots: list[dict[str, Any]] = []
    for record in records:
        post_record_id = str(record.get("post_record_id") or "").strip()
        if not post_record_id:
            continue
        snapshot_row_id = _posted_metrics_snapshot_row_id(snapshot_id=snapshot_id, post_record_id=post_record_id)
        snapshots.append(
            {
                "snapshot_row_id": snapshot_row_id,
                "post_record_id": post_record_id,
                "export_id": str(record.get("export_id") or "").strip() or None,
                "candidate_id": str(record.get("candidate_id") or "").strip() or None,
                "hook_id": str(record.get("hook_id") or "").strip() or None,
                "post_ledger_manifest_path": str(manifest_path),
                "captured_at": captured_at,
                "platform": str(effective_platform or "").strip() or None,
                "external_post_id": str(record.get("external_post_id") or "").strip() or None,
                "external_url": str(record.get("external_url") or "").strip() or None,
                "view_count": metrics_defaults["view_count"],
                "like_count": metrics_defaults["like_count"],
                "comment_count": metrics_defaults["comment_count"],
                "share_count": metrics_defaults["share_count"],
                "save_count": metrics_defaults["save_count"],
                "watch_time_seconds": metrics_defaults["watch_time_seconds"],
                "average_watch_time_seconds": metrics_defaults["average_watch_time_seconds"],
                "completion_rate": metrics_defaults["completion_rate"],
                "engagement_rate": metrics_defaults["engagement_rate"],
                "metadata_json": metadata or {},
            }
        )
    if not snapshots:
        return {
            "ok": False,
            "status": "empty_posted_metrics_snapshot",
            "error": "no posted records produced metrics snapshot rows",
            "post_ledger_manifest_path": str(manifest_path),
        }

    snapshot = {
        "schema_version": POSTED_HIGHLIGHT_METRICS_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "captured_at": captured_at,
        "platform": effective_platform,
        "account_id": effective_account_id,
        "workflow_run_id": str(workflow_run_id or "").strip() or None,
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
    }
    target = _resolve_path(output_path) if output_path is not None else _default_posted_metrics_output_path(
        game=_game_for_post_ledger(payload) or "unknown_game",
        snapshot_id=snapshot_id,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": POSTED_HIGHLIGHT_METRICS_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "manifest_path": str(target),
        "snapshot_count": len(snapshots),
    }


def report_posted_performance(
    *,
    registry_path: str | Path | None = None,
    game: str | None = None,
    platform: str | None = None,
    account_id: str | None = None,
    workflow_run_id: str | None = None,
    candidate_id: str | None = None,
    fixture_id: str | None = None,
    hook_archetype: str | None = None,
    hook_mode: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    from pipeline.clip_registry import query_clip_registry

    result = query_clip_registry(
        mode="posted-performance-rollups",
        game=game,
        platform=platform,
        candidate_id=candidate_id,
        fixture_id=fixture_id,
        hook_archetype=hook_archetype,
        hook_mode=hook_mode,
        workflow_run_id=workflow_run_id,
        registry_path=registry_path,
        account_id=account_id,
    )
    if result.get("ok"):
        result["lineage_summary"] = _posted_performance_lineage_summary(result)
    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def _posted_performance_lineage_summary(result: dict[str, Any]) -> dict[str, Any]:
    rows = list(result.get("rows", [])) if isinstance(result.get("rows"), list) else []
    if not rows:
        return {
            "selected_event_type_counts": {},
            "selected_producer_family_counts": {},
        }
    row = rows[0] if isinstance(rows[0], dict) else {}
    return {
        "selected_event_type_counts": _json_dict(row.get("by_selected_event_type_json")),
        "selected_producer_family_counts": _json_dict(row.get("by_selected_producer_family_json")),
    }


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, "", "null"):
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def materialize_synthetic_post_coverage(
    *,
    registry_path: str | Path,
    game: str | None = None,
    fixture_id: str | None = None,
    platform: str | None = None,
    account_id: str | None = None,
    workflow_run_id: str | None = None,
    output_root: str | Path | None = None,
    synthetic_profile: str = "balanced",
    include_rejected: bool = False,
) -> dict[str, Any]:
    normalized_profile = str(synthetic_profile or "balanced").strip().lower() or "balanced"
    if normalized_profile not in _SYNTHETIC_POST_COVERAGE_PROFILES:
        return {
            "ok": False,
            "status": "unsupported_synthetic_profile",
            "error": f"synthetic profile must be one of {sorted(_SYNTHETIC_POST_COVERAGE_PROFILES)}",
            "synthetic_profile": synthetic_profile,
        }

    registry = _resolve_path(registry_path)
    fixture_root = registry.parent
    target_root = _resolve_path(output_root) if output_root is not None else fixture_root / "synthetic_post_coverage"
    effective_platform = str(platform or "synthetic_benchmark").strip() or "synthetic_benchmark"
    effective_account_id = str(account_id or "synthetic-acct").strip() or "synthetic-acct"

    lifecycle_rows = _synthetic_post_candidates(
        registry_path=registry,
        game=game,
        fixture_id=fixture_id,
        include_rejected=include_rejected,
    )
    if not lifecycle_rows:
        return {
            "ok": False,
            "status": "no_synthetic_post_candidates",
            "error": "no eligible candidates without posted lineage matched the requested filters",
            "registry_path": str(registry),
            "include_rejected": include_rejected,
        }

    warnings: list[dict[str, Any]] = []
    prepared_rows: list[dict[str, Any]] = []
    created_selection_paths: list[str] = []
    reused_selection_paths: list[str] = []
    skipped_candidate_ids: list[str] = []
    for row in lifecycle_rows:
        candidate_id = str(row.get("candidate_id") or "").strip()
        fused_sidecar_path = str(row.get("fused_sidecar_path") or "").strip()
        if not fused_sidecar_path:
            skipped_candidate_ids.append(candidate_id)
            warnings.append(
                {
                    "code": "missing_fused_sidecar_path",
                    "candidate_id": candidate_id,
                }
            )
            continue
        selection_manifest_path = str(row.get("highlight_selection_manifest_path") or "").strip()
        selection_exists = bool(selection_manifest_path) and _resolve_path(selection_manifest_path).exists()
        if selection_exists:
            reused_selection_paths.append(selection_manifest_path)
        else:
            selection_output_path = target_root / "selection" / f"{candidate_id}.highlight_selection.json"
            selection_result = export_highlight_selection(
                fused_sidecar=fused_sidecar_path,
                output_path=selection_output_path,
            )
            if not selection_result.get("ok"):
                skipped_candidate_ids.append(candidate_id)
                warnings.append(
                    {
                        "code": "synthetic_selection_failed",
                        "candidate_id": candidate_id,
                        "detail": selection_result.get("error") or selection_result.get("status"),
                    }
                )
                continue
            selection_manifest_path = str(selection_result["manifest_path"])
            created_selection_paths.append(selection_manifest_path)
        prepared_rows.append(
            {
                **row,
                "highlight_selection_manifest_path": selection_manifest_path,
            }
        )

    if not prepared_rows:
        return {
            "ok": False,
            "status": "no_synthetic_post_candidates_after_selection",
            "error": "synthetic post coverage could not materialize selection lineage for any candidate",
            "registry_path": str(registry),
            "warning_count": len(warnings),
            "warnings": warnings,
        }

    hook_by_candidate = _hook_details_by_candidate(
        registry_path=registry,
        candidate_ids=[str(row.get("candidate_id") or "").strip() for row in prepared_rows],
    )
    export_manifest_path = target_root / "exports" / "synthetic.highlight_export_batch.json"
    export_result = _write_export_batch_from_rows(
        prepared_rows,
        hook_by_candidate=hook_by_candidate,
        workflow_run_id=workflow_run_id,
        selection_manifest=None,
        output_path=export_manifest_path,
        metadata_extra={
            "synthetic_benchmark": True,
            "synthetic_profile": normalized_profile,
            "synthetic_source": "materialize_synthetic_post_coverage",
        },
    )
    ledger_result = record_post_ledger(
        export_result["manifest_path"],
        workflow_run_id=workflow_run_id,
        platform=effective_platform,
        account_id=effective_account_id,
        output_path=target_root / "posted" / "synthetic.posted_highlight_ledger.json",
    )
    snapshot_result = _record_synthetic_posted_metrics_snapshot(
        ledger_result["manifest_path"],
        synthetic_profile=normalized_profile,
        workflow_run_id=workflow_run_id,
        platform=effective_platform,
        account_id=effective_account_id,
        output_path=target_root / "metrics" / "synthetic.posted_highlight_metrics_snapshot.json",
    )
    refresh_result = refresh_clip_registry(fixture_root, registry_path=registry)
    posted_rows = load_candidate_lifecycle_details(
        registry_path=registry,
        game=game,
    )
    posted_count = sum(
        1
        for row in posted_rows
        if str(row.get("candidate_id") or "").strip() in {
            str(item.get("candidate_id") or "").strip() for item in prepared_rows
        }
        and str(row.get("lifecycle_state") or "").strip() == "posted"
    )
    return {
        "ok": True,
        "status": "ok",
        "registry_path": str(registry),
        "synthetic_profile": normalized_profile,
        "include_rejected": include_rejected,
        "candidate_count": len(prepared_rows),
        "skipped_candidate_count": len(skipped_candidate_ids),
        "skipped_candidate_ids": skipped_candidate_ids,
        "created_selection_count": len(created_selection_paths),
        "reused_selection_count": len(reused_selection_paths),
        "created_selection_paths": created_selection_paths,
        "reused_selection_paths": reused_selection_paths,
        "export_manifest_path": export_result["manifest_path"],
        "post_ledger_manifest_path": ledger_result["manifest_path"],
        "metrics_snapshot_manifest_path": snapshot_result["manifest_path"],
        "posted_candidate_count_after_refresh": posted_count,
        "registry_refresh": {
            "ok": refresh_result.get("ok"),
            "warning_count": refresh_result.get("warning_count", 0),
        },
        "warning_count": len(warnings),
        "warnings": warnings,
    }


def _selected_lifecycle_rows(
    *,
    registry_path: str | Path | None,
    workflow_run_id: str | None,
    selection_manifest: str | Path | None,
    game: str | None,
    fixture_id: str | None,
) -> list[dict[str, Any]]:
    rows = load_candidate_lifecycle_details(
        game=game,
        candidate_id=None,
        registry_path=registry_path,
    )
    selected = [
        row for row in rows
        if str(row.get("lifecycle_state") or "").strip() == "selected_for_export"
    ]
    if fixture_id is not None:
        selected = [row for row in selected if str(row.get("fixture_id") or "").strip() == str(fixture_id).strip()]
    if selection_manifest is not None:
        normalized_selection = str(_resolve_path(selection_manifest))
        selected = [
            row for row in selected
            if str(row.get("highlight_selection_manifest_path") or "").strip() == normalized_selection
        ]
    if workflow_run_id is not None:
        workflow_rows = load_workflow_run_details(workflow_run_id=workflow_run_id, registry_path=registry_path)
        allowed = {
            str(row.get("candidate_id") or "").strip()
            for row in workflow_rows
            if str(row.get("candidate_id") or "").strip()
        }
        if allowed:
            selected = [row for row in selected if str(row.get("candidate_id") or "").strip() in allowed]
    selected.sort(key=lambda row: (str(row.get("game") or ""), str(row.get("fixture_id") or ""), str(row.get("candidate_id") or "")))
    return selected


def _synthetic_post_candidates(
    *,
    registry_path: str | Path,
    game: str | None,
    fixture_id: str | None,
    include_rejected: bool,
) -> list[dict[str, Any]]:
    rows = load_candidate_lifecycle_details(
        game=game,
        registry_path=registry_path,
    )
    eligible_states = {"approved", "selected_for_export", "exported"}
    eligible: list[dict[str, Any]] = []
    for row in rows:
        review_status = str(row.get("latest_review_status") or "").strip()
        lifecycle_state = str(row.get("lifecycle_state") or "").strip()
        if str(row.get("post_ledger_path") or "").strip():
            continue
        if review_status == "approved" and lifecycle_state in eligible_states:
            eligible.append(row)
            continue
        if include_rejected and review_status == "rejected" and lifecycle_state == "rejected":
            eligible.append(row)
    if fixture_id is not None:
        eligible = [row for row in eligible if str(row.get("fixture_id") or "").strip() == str(fixture_id).strip()]
    eligible.sort(
        key=lambda row: (
            str(row.get("game") or ""),
            str(row.get("fixture_id") or ""),
            0 if str(row.get("latest_review_status") or "").strip() == "approved" else 1,
            -float(row.get("final_score") or 0.0),
            str(row.get("candidate_id") or ""),
        )
    )
    return eligible


def _hook_details_by_candidate(*, registry_path: str | Path | None, candidate_ids: list[str]) -> dict[str, dict[str, Any]]:
    details: dict[str, dict[str, Any]] = {}
    for candidate_id in candidate_ids:
        if not candidate_id:
            continue
        rows = load_hook_candidate_details(candidate_id=candidate_id, registry_path=registry_path)
        if rows:
            details[candidate_id] = rows[0]
    return details


def _write_export_batch_from_rows(
    lifecycle_rows: list[dict[str, Any]],
    *,
    hook_by_candidate: dict[str, dict[str, Any]],
    workflow_run_id: str | None,
    selection_manifest: str | Path | None,
    output_path: str | Path,
    metadata_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    target = _resolve_path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    artifact_root = target.parent / f"{target.stem}_artifacts"
    artifact_root.mkdir(parents=True, exist_ok=True)

    exports: list[dict[str, Any]] = []
    fused_paths: set[str] = set()
    hook_paths: set[str] = set()
    selection_paths: set[str] = set()
    for row in lifecycle_rows:
        candidate_id = str(row.get("candidate_id") or "").strip()
        hook_row = hook_by_candidate.get(candidate_id)
        selection_path = str(row.get("highlight_selection_manifest_path") or "").strip() or None
        highlight = _selection_highlight(
            selection_path=selection_path,
            candidate_id=candidate_id,
            event_id=str(row.get("event_id") or "").strip(),
        )
        export_id = _export_id(
            candidate_id=candidate_id,
            workflow_run_id=workflow_run_id,
            selection_manifest=selection_manifest,
        )
        otio_path = artifact_root / f"{export_id}.otio.json"
        export_artifact_path = str(otio_path.resolve())
        metadata_json = {
            "selection_basis": row.get("selection_basis"),
            "recommended_action": row.get("recommended_action"),
            "latest_review_status": row.get("latest_review_status"),
        }
        if metadata_extra:
            metadata_json.update(metadata_extra)
        export_row = {
            "export_id": export_id,
            "candidate_id": candidate_id,
            "event_id": str(row.get("event_id") or "").strip() or None,
            "hook_id": (hook_row or {}).get("hook_id"),
            "fixture_id": row.get("fixture_id"),
            "source": row.get("source"),
            "fused_sidecar_path": row.get("fused_sidecar_path"),
            "hook_manifest_path": (hook_row or {}).get("manifest_path"),
            "highlight_selection_manifest_path": selection_path,
            "start_seconds": highlight.get("start_seconds"),
            "end_seconds": highlight.get("end_seconds"),
            "final_score": row.get("final_score"),
            "hook_archetype": (hook_row or {}).get("hook_archetype"),
            "hook_mode": (hook_row or {}).get("hook_mode"),
            "packaging_strategy": (hook_row or {}).get("packaging_strategy"),
            "export_status": "exported",
            "export_artifact_path": export_artifact_path,
            "otio_path": export_artifact_path,
            "metadata_json": metadata_json,
        }
        otio_path.write_text(json.dumps(_otio_clip(export_row), indent=2), encoding="utf-8")
        exports.append(export_row)
        if str(row.get("fused_sidecar_path") or "").strip():
            fused_paths.add(str(row.get("fused_sidecar_path")))
        if str((hook_row or {}).get("manifest_path") or "").strip():
            hook_paths.add(str((hook_row or {}).get("manifest_path")))
        if selection_path:
            selection_paths.add(selection_path)

    export_batch_id = _export_batch_id(
        workflow_run_id=workflow_run_id,
        selection_manifest=selection_manifest,
        candidate_ids=[str(row.get("candidate_id") or "").strip() for row in lifecycle_rows],
    )
    manifest = {
        "schema_version": HIGHLIGHT_EXPORT_BATCH_SCHEMA_VERSION,
        "export_batch_id": export_batch_id,
        "created_at": _utc_now(),
        "game": str(lifecycle_rows[0].get("game") or "").strip() or "unknown_game",
        "workflow_run_id": str(workflow_run_id or "").strip() or None,
        "selection_manifest_path": str(_resolve_path(selection_manifest)) if selection_manifest is not None else None,
        "linked_inputs": {
            "fused_sidecar_paths": sorted(fused_paths),
            "hook_manifest_paths": sorted(hook_paths),
            "selection_manifest_paths": sorted(selection_paths),
        },
        "export_count": len(exports),
        "exports": exports,
    }
    target.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": HIGHLIGHT_EXPORT_BATCH_SCHEMA_VERSION,
        "export_batch_id": export_batch_id,
        "manifest_path": str(target),
        "export_count": len(exports),
    }


def _selection_highlight(*, selection_path: str | None, candidate_id: str, event_id: str) -> dict[str, Any]:
    fallback = {"start_seconds": 0.0, "end_seconds": 0.0}
    if not selection_path:
        return fallback
    path = _resolve_path(selection_path)
    if not path.exists() or not path.is_file():
        return fallback
    payload = json.loads(path.read_text(encoding="utf-8"))
    for row in list(payload.get("selected_highlights", [])):
        if not isinstance(row, dict):
            continue
        if str(row.get("candidate_id") or "").strip() == candidate_id:
            return {
                "start_seconds": round(float(row.get("start_seconds", 0.0) or 0.0), 4),
                "end_seconds": round(float(row.get("end_seconds", row.get("start_seconds", 0.0)) or 0.0), 4),
            }
        if str(row.get("event_id") or "").strip() == event_id:
            return {
                "start_seconds": round(float(row.get("start_seconds", 0.0) or 0.0), 4),
                "end_seconds": round(float(row.get("end_seconds", row.get("start_seconds", 0.0)) or 0.0), 4),
            }
    return fallback


def _otio_clip(export_row: dict[str, Any]) -> dict[str, Any]:
    start_seconds = round(float(export_row.get("start_seconds", 0.0) or 0.0), 4)
    end_seconds = round(max(start_seconds, float(export_row.get("end_seconds", start_seconds) or start_seconds)), 4)
    return {
        "OTIO_SCHEMA": "Timeline.1",
        "name": str(export_row.get("export_id") or "highlight-export"),
        "metadata": {
            "schema_version": HIGHLIGHT_EXPORT_BATCH_SCHEMA_VERSION,
            "candidate_id": export_row.get("candidate_id"),
            "event_id": export_row.get("event_id"),
            "hook_id": export_row.get("hook_id"),
            "export_status": export_row.get("export_status"),
        },
        "tracks": {
            "OTIO_SCHEMA": "Stack.1",
            "children": [
                {
                    "OTIO_SCHEMA": "Track.1",
                    "name": "export",
                    "kind": "Video",
                    "children": [
                        {
                            "OTIO_SCHEMA": "Clip.2",
                            "name": str(export_row.get("export_id") or "clip"),
                            "source_range": {
                                "OTIO_SCHEMA": "TimeRange.1",
                                "start_time": {"OTIO_SCHEMA": "RationalTime.1", "value": start_seconds, "rate": 1},
                                "duration": {"OTIO_SCHEMA": "RationalTime.1", "value": round(end_seconds - start_seconds, 4), "rate": 1},
                            },
                            "metadata": {
                                "candidate_id": export_row.get("candidate_id"),
                                "event_id": export_row.get("event_id"),
                                "hook_mode": export_row.get("hook_mode"),
                                "hook_archetype": export_row.get("hook_archetype"),
                            },
                        }
                    ],
                }
            ],
        },
    }


def _record_synthetic_posted_metrics_snapshot(
    post_ledger_manifest: str | Path,
    *,
    synthetic_profile: str,
    workflow_run_id: str | None,
    platform: str,
    account_id: str,
    output_path: str | Path,
) -> dict[str, Any]:
    manifest_path = _resolve_path(post_ledger_manifest)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != POSTED_HIGHLIGHT_LEDGER_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "invalid_post_ledger_manifest",
            "error": "post ledger manifest does not use posted_highlight_ledger_v1",
            "post_ledger_manifest_path": str(manifest_path),
        }
    records = [row for row in list(payload.get("posted_records", [])) if isinstance(row, dict)]
    if not records:
        return {
            "ok": False,
            "status": "empty_posted_metrics_snapshot",
            "error": "no posted records produced metrics snapshot rows",
            "post_ledger_manifest_path": str(manifest_path),
        }

    snapshot_id = _posted_metrics_snapshot_id(
        post_ledger_manifest_path=str(manifest_path),
        workflow_run_id=workflow_run_id,
        platform=platform,
        account_id=account_id,
    )
    captured_at = _utc_now()
    snapshots: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        post_record_id = str(record.get("post_record_id") or "").strip()
        if not post_record_id:
            continue
        metrics = _synthetic_metrics_for_post_record(
            record,
            synthetic_profile=synthetic_profile,
            index=index,
        )
        snapshot_row_id = _posted_metrics_snapshot_row_id(snapshot_id=snapshot_id, post_record_id=post_record_id)
        snapshots.append(
            {
                "snapshot_row_id": snapshot_row_id,
                "post_record_id": post_record_id,
                "export_id": str(record.get("export_id") or "").strip() or None,
                "candidate_id": str(record.get("candidate_id") or "").strip() or None,
                "hook_id": str(record.get("hook_id") or "").strip() or None,
                "post_ledger_manifest_path": str(manifest_path),
                "captured_at": captured_at,
                "platform": platform,
                "external_post_id": str(record.get("external_post_id") or "").strip() or None,
                "external_url": str(record.get("external_url") or "").strip() or None,
                **metrics,
                "metadata_json": {
                    "synthetic_benchmark": True,
                    "synthetic_profile": synthetic_profile,
                    "synthetic_source": "materialize_synthetic_post_coverage",
                },
            }
        )
    if not snapshots:
        return {
            "ok": False,
            "status": "empty_posted_metrics_snapshot",
            "error": "no posted records produced metrics snapshot rows",
            "post_ledger_manifest_path": str(manifest_path),
        }
    snapshot = {
        "schema_version": POSTED_HIGHLIGHT_METRICS_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "captured_at": captured_at,
        "platform": platform,
        "account_id": account_id,
        "workflow_run_id": str(workflow_run_id or "").strip() or None,
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
    }
    target = _resolve_path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": POSTED_HIGHLIGHT_METRICS_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "manifest_path": str(target),
        "snapshot_count": len(snapshots),
    }


def _duration_seconds(start: Any, end: Any) -> float:
    start_value = float(start or 0.0)
    end_value = float(end or start_value)
    return round(max(0.0, end_value - start_value), 4)


def _synthetic_metrics_for_post_record(
    record: dict[str, Any],
    *,
    synthetic_profile: str,
    index: int,
) -> dict[str, Any]:
    export_id = str(record.get("export_id") or "").strip()
    candidate_id = str(record.get("candidate_id") or "").strip()
    seed = int(hashlib.sha1(f"{export_id}::{candidate_id}::{synthetic_profile}".encode("utf-8")).hexdigest()[:8], 16)
    jitter = (seed % 1000) / 1000.0
    profile_bias = {
        "weak": 0.55,
        "balanced": 0.75,
        "strong": 0.92,
    }[synthetic_profile]
    duration_seconds = float(record.get("duration_seconds") or 8.0)
    base_strength = min(1.0, max(0.35, profile_bias + ((index % 3) - 1) * 0.04 + (jitter - 0.5) * 0.06))
    view_count = max(120, int(round(180 + base_strength * 780 + jitter * 90)))
    engagement_rate = round(min(0.32, max(0.045, 0.035 + base_strength * 0.14 + (jitter - 0.5) * 0.01)), 4)
    completion_rate = round(min(0.92, max(0.28, 0.22 + base_strength * 0.5 + (jitter - 0.5) * 0.03)), 4)
    average_watch_time_seconds = round(max(3.5, min(18.0, duration_seconds * (0.55 + base_strength * 0.45))), 4)
    watch_time_seconds = round(view_count * average_watch_time_seconds, 4)
    like_count = max(1, int(round(view_count * engagement_rate * 0.62)))
    comment_count = max(0, int(round(view_count * engagement_rate * 0.11)))
    share_count = max(0, int(round(view_count * engagement_rate * 0.08)))
    save_count = max(0, int(round(view_count * engagement_rate * 0.06)))
    return {
        "view_count": view_count,
        "like_count": like_count,
        "comment_count": comment_count,
        "share_count": share_count,
        "save_count": save_count,
        "watch_time_seconds": watch_time_seconds,
        "average_watch_time_seconds": average_watch_time_seconds,
        "completion_rate": completion_rate,
        "engagement_rate": engagement_rate,
    }


def _export_batch_id(*, workflow_run_id: str | None, selection_manifest: str | Path | None, candidate_ids: list[str]) -> str:
    payload = {
        "workflow_run_id": str(workflow_run_id or "").strip(),
        "selection_manifest": str(_resolve_path(selection_manifest)) if selection_manifest is not None else "",
        "candidate_ids": sorted(candidate_ids),
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"export-batch-{digest}"


def _export_id(*, candidate_id: str, workflow_run_id: str | None, selection_manifest: str | Path | None) -> str:
    payload = {
        "candidate_id": candidate_id,
        "workflow_run_id": str(workflow_run_id or "").strip(),
        "selection_manifest": str(_resolve_path(selection_manifest)) if selection_manifest is not None else "",
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"export-{digest}"


def _post_ledger_id(*, export_manifest_path: str, workflow_run_id: str | None, platform: str | None, account_id: str | None) -> str:
    payload = {
        "export_manifest_path": export_manifest_path,
        "workflow_run_id": str(workflow_run_id or "").strip(),
        "platform": str(platform or "").strip(),
        "account_id": str(account_id or "").strip(),
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"post-ledger-{digest}"


def _post_record_id(*, ledger_id: str, export_id: str) -> str:
    digest = hashlib.sha1(f"{ledger_id}::{export_id}".encode("utf-8")).hexdigest()[:16]
    return f"post-{digest}"


def _posted_metrics_snapshot_id(
    *,
    post_ledger_manifest_path: str,
    workflow_run_id: str | None,
    platform: str | None,
    account_id: str | None,
) -> str:
    payload = {
        "post_ledger_manifest_path": post_ledger_manifest_path,
        "workflow_run_id": str(workflow_run_id or "").strip(),
        "platform": str(platform or "").strip(),
        "account_id": str(account_id or "").strip(),
        "captured_at": _utc_now(),
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"posted-metrics-{digest}"


def _posted_metrics_snapshot_row_id(*, snapshot_id: str, post_record_id: str) -> str:
    digest = hashlib.sha1(f"{snapshot_id}::{post_record_id}".encode("utf-8")).hexdigest()[:16]
    return f"posted-metrics-row-{digest}"


def _default_export_output_path(game: str, export_batch_id: str) -> Path:
    return DEFAULT_EXPORT_OUTPUT_ROOT / game / f"{export_batch_id}.highlight_export_batch.json"


def _default_post_ledger_output_path(game: str, ledger_id: str) -> Path:
    return DEFAULT_POST_LEDGER_OUTPUT_ROOT / game / f"{ledger_id}.posted_highlight_ledger.json"


def _default_posted_metrics_output_path(game: str, snapshot_id: str) -> Path:
    return DEFAULT_POSTED_METRICS_OUTPUT_ROOT / game / f"{snapshot_id}.posted_highlight_metrics_snapshot.json"


def _game_for_post_ledger(payload: dict[str, Any]) -> str | None:
    for record in list(payload.get("posted_records", [])):
        if not isinstance(record, dict):
            continue
        export_manifest_path = str(record.get("export_batch_manifest_path") or "").strip()
        if not export_manifest_path:
            continue
        export_path = _resolve_path(export_manifest_path)
        if not export_path.exists() or not export_path.is_file():
            continue
        export_payload = json.loads(export_path.read_text(encoding="utf-8"))
        game = str(export_payload.get("game") or "").strip()
        if game:
            return game
    return None


def _resolve_path(path_like: str | Path | None) -> Path:
    if path_like is None:
        return Path()
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
