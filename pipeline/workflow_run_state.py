from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.clip_registry import query_clip_registry


REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_RUN_SCHEMA_VERSION = "workflow_run_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "workflow_runs"
WORKFLOW_TYPE_TO_LIFECYCLE_STATE = {
    "review_queue": "pending_review",
    "selection_queue": "approved",
    "export_queue": "selected_for_export",
    "post_queue": "exported",
}


def create_workflow_run(
    workflow_type: str,
    *,
    registry_path: str | Path | None = None,
    output_path: str | Path | None = None,
    game: str | None = None,
    fixture_id: str | None = None,
) -> dict[str, Any]:
    normalized_workflow_type = str(workflow_type or "").strip().lower()
    lifecycle_state = WORKFLOW_TYPE_TO_LIFECYCLE_STATE.get(normalized_workflow_type)
    if lifecycle_state is None:
        return {
            "ok": False,
            "status": "invalid_workflow_type",
            "error": f"unsupported workflow_type: {workflow_type}",
        }

    lifecycle_query = query_clip_registry(
        mode="candidate-lifecycles",
        registry_path=registry_path,
        game=game,
        fixture_id=fixture_id,
        lifecycle_state=lifecycle_state,
    )
    if not lifecycle_query.get("ok"):
        return lifecycle_query

    hook_query = query_clip_registry(
        mode="hook-candidates",
        registry_path=registry_path,
        game=game,
        fixture_id=fixture_id,
        lifecycle_state=lifecycle_state,
    )
    hook_rows = hook_query.get("rows", []) if hook_query.get("ok") else []
    hook_by_candidate_id = {
        str(row.get("candidate_id") or "").strip(): row
        for row in hook_rows
        if str(row.get("candidate_id") or "").strip()
    }

    lifecycle_rows = list(lifecycle_query.get("rows", []))
    items = [_workflow_item(row, hook_by_candidate_id.get(str(row.get("candidate_id") or "").strip())) for row in lifecycle_rows]
    run_id = _workflow_run_id(
        workflow_type=normalized_workflow_type,
        game=game,
        fixture_id=fixture_id,
        items=items,
    )
    created_at = _utc_now()
    manifest = {
        "schema_version": WORKFLOW_RUN_SCHEMA_VERSION,
        "workflow_run_id": run_id,
        "workflow_type": normalized_workflow_type,
        "stage": lifecycle_state,
        "status": "ready",
        "registry_path": str(_resolve_path(registry_path)) if registry_path is not None else None,
        "filters": {
            "game": str(game).strip() if game is not None else None,
            "fixture_id": str(fixture_id).strip() if fixture_id is not None else None,
        },
        "created_at": created_at,
        "updated_at": created_at,
        "linked_artifacts": {
            "highlight_selection_manifest_paths": sorted(
                {str(item.get("highlight_selection_manifest_path") or "").strip() for item in items if str(item.get("highlight_selection_manifest_path") or "").strip()}
            ),
            "export_artifact_paths": sorted(
                {str(item.get("export_artifact_path") or "").strip() for item in items if str(item.get("export_artifact_path") or "").strip()}
            ),
            "post_ledger_paths": sorted(
                {str(item.get("post_ledger_path") or "").strip() for item in items if str(item.get("post_ledger_path") or "").strip()}
            ),
            "hook_manifest_paths": sorted(
                {str(item.get("hook_manifest_path") or "").strip() for item in items if str(item.get("hook_manifest_path") or "").strip()}
            ),
            "fused_sidecar_paths": sorted(
                {str(item.get("fused_sidecar_path") or "").strip() for item in items if str(item.get("fused_sidecar_path") or "").strip()}
            ),
        },
        "item_counts": {
            "total": len(items),
            "by_lifecycle_state": {lifecycle_state: len(items)},
            "with_selection_manifest": sum(1 for item in items if item.get("highlight_selection_manifest_path")),
            "with_export_artifact": sum(1 for item in items if item.get("export_artifact_path")),
            "with_post_ledger": sum(1 for item in items if item.get("post_ledger_path")),
            "with_hook_candidate": sum(1 for item in items if item.get("hook_id")),
        },
        "items": items,
    }
    target = _resolve_path(output_path) if output_path is not None else _default_output_path(normalized_workflow_type, run_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": WORKFLOW_RUN_SCHEMA_VERSION,
        "workflow_run_id": run_id,
        "workflow_type": normalized_workflow_type,
        "stage": lifecycle_state,
        "manifest_path": str(target),
        "item_count": len(items),
    }


def query_workflow_queue(
    workflow_type: str,
    *,
    registry_path: str | Path | None = None,
    game: str | None = None,
    fixture_id: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    normalized_workflow_type = str(workflow_type or "").strip().lower()
    lifecycle_state = WORKFLOW_TYPE_TO_LIFECYCLE_STATE.get(normalized_workflow_type)
    if lifecycle_state is None:
        return {
            "ok": False,
            "status": "invalid_workflow_type",
            "error": f"unsupported workflow_type: {workflow_type}",
        }
    lifecycle_query = query_clip_registry(
        mode="candidate-lifecycles",
        registry_path=registry_path,
        game=game,
        fixture_id=fixture_id,
        lifecycle_state=lifecycle_state,
        limit=limit,
    )
    if not lifecycle_query.get("ok"):
        return lifecycle_query
    workflow_history = query_clip_registry(
        mode="workflow-runs",
        registry_path=registry_path,
        game=game,
        fixture_id=fixture_id,
    )
    latest_workflow_by_candidate: dict[str, dict[str, Any]] = {}
    if workflow_history.get("ok"):
        for row in list(workflow_history.get("rows", [])):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if not candidate_id:
                continue
            existing = latest_workflow_by_candidate.get(candidate_id)
            if existing is None or str(row.get("updated_at") or "") > str(existing.get("updated_at") or ""):
                latest_workflow_by_candidate[candidate_id] = row
    rows = []
    lifecycle_rows = sorted(
        list(lifecycle_query.get("rows", [])),
        key=lambda row: (
            str(row.get("game") or ""),
            str(row.get("fixture_id") or ""),
            str(row.get("candidate_id") or ""),
        ),
    )
    for row in lifecycle_rows:
        candidate_id = str(row.get("candidate_id") or "").strip()
        latest_workflow = latest_workflow_by_candidate.get(candidate_id, {})
        rows.append(
            {
                "workflow_type": normalized_workflow_type,
                "stage": lifecycle_state,
                "status": "ready",
                "candidate_id": row.get("candidate_id"),
                "game": row.get("game"),
                "source": row.get("source"),
                "fixture_id": row.get("fixture_id"),
                "event_id": row.get("event_id"),
                "lifecycle_state": row.get("lifecycle_state"),
                "latest_review_status": row.get("latest_review_status"),
                "recommended_action": row.get("recommended_action"),
                "final_score": row.get("final_score"),
                "fused_sidecar_path": row.get("fused_sidecar_path"),
                "highlight_selection_manifest_path": row.get("highlight_selection_manifest_path"),
                "export_artifact_path": row.get("export_artifact_path"),
                "post_ledger_path": row.get("post_ledger_path"),
                "latest_workflow_run_id": latest_workflow.get("workflow_run_id"),
                "latest_workflow_updated_at": latest_workflow.get("updated_at"),
            }
        )
    return {
        "ok": True,
        "status": "ok",
        "workflow_type": normalized_workflow_type,
        "stage": lifecycle_state,
        "row_count": len(rows),
        "rows": rows,
    }


def _workflow_item(lifecycle_row: dict[str, Any], hook_row: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "candidate_id": lifecycle_row.get("candidate_id"),
        "item_status": "ready",
        "game": lifecycle_row.get("game"),
        "source": lifecycle_row.get("source"),
        "fixture_id": lifecycle_row.get("fixture_id"),
        "event_id": lifecycle_row.get("event_id"),
        "lifecycle_state": lifecycle_row.get("lifecycle_state"),
        "latest_review_status": lifecycle_row.get("latest_review_status"),
        "recommended_action": lifecycle_row.get("recommended_action"),
        "final_score": lifecycle_row.get("final_score"),
        "fused_sidecar_path": lifecycle_row.get("fused_sidecar_path"),
        "selection_basis": lifecycle_row.get("selection_basis"),
        "highlight_selection_manifest_path": lifecycle_row.get("highlight_selection_manifest_path"),
        "export_artifact_path": lifecycle_row.get("export_artifact_path"),
        "post_ledger_path": lifecycle_row.get("post_ledger_path"),
        "hook_id": (hook_row or {}).get("hook_id"),
        "hook_archetype": (hook_row or {}).get("hook_archetype"),
        "hook_mode": (hook_row or {}).get("hook_mode"),
        "hook_manifest_path": (hook_row or {}).get("manifest_path"),
    }


def _workflow_run_id(*, workflow_type: str, game: str | None, fixture_id: str | None, items: list[dict[str, Any]]) -> str:
    normalized_items = [
        {
            "candidate_id": str(item.get("candidate_id") or "").strip(),
            "lifecycle_state": str(item.get("lifecycle_state") or "").strip(),
            "highlight_selection_manifest_path": str(item.get("highlight_selection_manifest_path") or "").strip(),
            "export_artifact_path": str(item.get("export_artifact_path") or "").strip(),
            "post_ledger_path": str(item.get("post_ledger_path") or "").strip(),
        }
        for item in items
    ]
    payload = {
        "workflow_type": workflow_type,
        "game": str(game or "").strip(),
        "fixture_id": str(fixture_id or "").strip(),
        "items": sorted(
            normalized_items,
            key=lambda row: (
                row["candidate_id"],
                row["lifecycle_state"],
                row["highlight_selection_manifest_path"],
                row["export_artifact_path"],
                row["post_ledger_path"],
            ),
        ),
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"workflow-{digest}"


def _default_output_path(workflow_type: str, workflow_run_id: str) -> Path:
    return DEFAULT_OUTPUT_ROOT / workflow_type / f"{workflow_run_id}.workflow_run.json"


def _resolve_path(path: str | Path | None) -> Path:
    if path is None:
        return Path()
    return Path(path).expanduser().resolve()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
