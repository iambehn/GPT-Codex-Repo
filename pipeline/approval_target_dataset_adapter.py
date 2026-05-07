from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
APPROVAL_TARGET_DATASET_SCHEMA_VERSION = "approval_target_dataset_v1"
V2_TRAINING_EXPORT_SCHEMA_VERSION = "v2_training_dataset_export_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "approval_target_dataset_adapted"
DATASET_VIEWS = ("candidates", "hooks", "outcomes", "performance")


def adapt_approval_target_dataset(
    approval_target_manifest: str | Path,
    *,
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_manifest_path(approval_target_manifest)
    payload = _load_approval_target_manifest(manifest_path)
    if payload is None:
        return {
            "ok": False,
            "status": "invalid_approval_target_manifest",
            "approval_target_manifest_path": str(manifest_path),
            "error": "approval target manifest is missing or malformed",
        }
    validation_error = _validate_approval_target_manifest(payload)
    if validation_error is not None:
        return {
            "ok": False,
            "status": validation_error["status"],
            "approval_target_manifest_path": str(manifest_path),
            "error": validation_error["error"],
        }

    candidate_rows = [
        _adapt_candidate_row(row)
        for row in payload.get("rows", [])
        if isinstance(row, dict)
    ]
    dataset_export_id = _adapted_dataset_id(str(manifest_path), payload)
    export_root = _resolve_output_root(output_root)
    paths = _dataset_paths(
        export_root=export_root,
        dataset_export_id=dataset_export_id,
        game=str(payload.get("filters", {}).get("game") or ""),
        output_path=output_path,
    )

    dataset_views = {
        "candidates": candidate_rows,
        "hooks": [],
        "outcomes": [],
        "performance": [],
    }
    manifest = _build_manifest(
        dataset_export_id=dataset_export_id,
        source_manifest_path=str(manifest_path),
        source_payload=payload,
        dataset_views=dataset_views,
    )
    _write_dataset_artifacts(dataset_views=dataset_views, manifest=manifest, paths=paths)

    return {
        "ok": True,
        "status": "ok",
        "dataset_export_id": dataset_export_id,
        "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
        "manifest_path": str(paths["manifest_path"]),
        "output_root": str(export_root),
        "row_count": manifest["row_count"],
        "source_approval_target_manifest_path": str(manifest_path),
        "source_approval_target_dataset_id": payload.get("dataset_id"),
        "source_approval_target_schema_version": payload.get("schema_version"),
        "dataset_views": {
            view: {
                "jsonl_path": str(paths[f"{view}_jsonl_path"]),
                "csv_path": str(paths[f"{view}_csv_path"]),
                "row_count": manifest["dataset_views"][view]["row_count"],
            }
            for view in DATASET_VIEWS
        },
    }


def _resolve_manifest_path(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _load_approval_target_manifest(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _validate_approval_target_manifest(payload: dict[str, Any]) -> dict[str, str] | None:
    if payload.get("schema_version") != APPROVAL_TARGET_DATASET_SCHEMA_VERSION:
        return {
            "status": "unsupported_approval_target_manifest",
            "error": f"unsupported approval target schema version: {payload.get('schema_version')}",
        }
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return {
            "status": "invalid_approval_target_manifest",
            "error": "approval target manifest rows are missing or malformed",
        }
    return None


def _adapt_candidate_row(row: dict[str, Any]) -> dict[str, Any]:
    review_outcome = str(row.get("review_outcome") or "").strip().lower() or None
    hook_mode = str(row.get("hook_mode") or "").strip().lower()
    hook_archetype = str(row.get("hook_archetype") or "").strip().lower()
    selected_event_type = str(row.get("selected_highlight_event_type") or "").strip().lower()
    has_selection = bool(row.get("highlight_selection_manifest_path") or row.get("selected_highlight_fusion_id") or row.get("selected_highlight_event_type"))
    return {
        "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
        "dataset_view": "candidates",
        "candidate_id": row.get("candidate_id"),
        "game": row.get("game"),
        "source": row.get("source"),
        "fixture_id": row.get("fixture_id"),
        "event_id": row.get("event_id"),
        "fused_sidecar_path": row.get("fused_sidecar_path"),
        "highlight_selection_manifest_path": row.get("highlight_selection_manifest_path"),
        "selected_highlight_fusion_id": row.get("selected_highlight_fusion_id"),
        "selected_highlight_event_type": row.get("selected_highlight_event_type"),
        "review_outcome": review_outcome,
        "lifecycle_state": row.get("lifecycle_state"),
        "final_score": row.get("final_score"),
        "fused_confidence": row.get("fused_confidence"),
        "evidence_mode": row.get("evidence_mode"),
        "approval_label": row.get("approval_label"),
        "label_source": row.get("label_source"),
        "hook_candidate_present": False,
        "export_present": False,
        "post_present": False,
        "metrics_present": False,
        "coverage_tier": "reviewed",
        "latest_post_performance_coverage_tier": "no_post_record",
        "latest_post_performance_label_eligible": False,
        "latest_post_performance_recoverable": False,
        "latest_post_performance_target_score": None,
        "latest_post_performance_target_bucket": None,
        "latest_post_performance_label_reason": "no_post_record",
        "latest_post_performance_evidence_mode": row.get("evidence_mode"),
        "preferred_hook_strength": row.get("hook_strength"),
        "preferred_hook_mode_natural": 1.0 if hook_mode == "natural" else 0.0,
        "preferred_hook_mode_synthetic": 1.0 if hook_mode == "synthetic" else 0.0,
        "preferred_hook_mode_reject": 1.0 if hook_mode == "reject" else 0.0,
        "hook_archetype_clutch": 1.0 if hook_archetype == "clutch" else 0.0,
        "hook_archetype_reversal": 1.0 if hook_archetype == "reversal" else 0.0,
        "hook_archetype_domination": 1.0 if hook_archetype == "domination" else 0.0,
        "hook_archetype_comedy": 1.0 if hook_archetype == "comedy" else 0.0,
        "hook_archetype_chaos": 1.0 if hook_archetype == "chaos" else 0.0,
        "hook_archetype_fail": 1.0 if hook_archetype == "fail" else 0.0,
        "hook_archetype_flex": 1.0 if hook_archetype == "flex" else 0.0,
        "hook_archetype_other": 1.0 if hook_archetype not in {"", "clutch", "reversal", "domination", "comedy", "chaos", "fail", "flex"} else 0.0,
        "selection_present": has_selection,
        "selection_present_feature": 1.0 if has_selection else 0.0,
        "is_approved": 1.0 if review_outcome == "approved" else 0.0,
        "fused_synergy_applied": 1.0 if "combo" in selected_event_type else 0.0,
        "fused_minimum_required_signals_met": 1.0 if has_selection else 0.0,
        "fused_duration_seconds": 0.0,
        "fused_contributing_signal_count": 0.0,
        "fused_runtime_signal_count": 0.0,
        "fused_proxy_signal_count": 0.0,
        "fused_source_family_count": 0.0,
        "fused_entity_present": 1.0 if "identity" in selected_event_type else 0.0,
        "fused_ability_present": 1.0 if "ability" in selected_event_type else 0.0,
        "fused_equipment_present": 0.0,
        "fused_event_type_has_combo": 1.0 if "combo" in selected_event_type else 0.0,
        "fused_event_type_has_medal": 1.0 if "medal" in selected_event_type else 0.0,
        "fused_event_type_has_ability": 1.0 if "ability" in selected_event_type else 0.0,
        "fused_event_type_has_identity": 1.0 if "identity" in selected_event_type else 0.0,
        "preferred_hook_intensity_score": 0.0,
        "preferred_hook_clarity_score": 0.0,
        "preferred_hook_novelty_score": 0.0,
        "preferred_hook_context_sufficiency_score": 0.0,
        "preferred_hook_payoff_readability_score": 0.0,
        "preferred_hook_title_thumbnail_potential_score": 0.0,
        "preferred_hook_authenticity_risk_score": 0.0,
        "preferred_hook_sound_off_legibility_score": 0.0,
        "preferred_hook_packaging_strategy_present": 0.0,
        "preferred_hook_rejection_reason_present": 0.0,
        "latest_view_count_norm": 0.0,
        "latest_completion_rate_capped": 0.0,
        "latest_engagement_rate_capped": 0.0,
        "account_context_present": 0.0,
        "outcome_platform_present": 0.0,
        "performance_platform_present": 0.0,
        "metrics_complete_present": 0.0,
        "split_candidate_key": row.get("candidate_id"),
        "split_fixture_key": row.get("fixture_id") or "",
        "split_lineage_key": row.get("candidate_id"),
    }


def _adapted_dataset_id(source_manifest_path: str, payload: dict[str, Any]) -> str:
    raw = json.dumps(
        {
            "source_manifest_path": source_manifest_path,
            "source_dataset_id": payload.get("dataset_id"),
            "filters": payload.get("filters"),
        },
        sort_keys=True,
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _build_manifest(
    *,
    dataset_export_id: str,
    source_manifest_path: str,
    source_payload: dict[str, Any],
    dataset_views: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    return {
        "schema_version": V2_TRAINING_EXPORT_SCHEMA_VERSION,
        "dataset_export_id": dataset_export_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "filters": dict(source_payload.get("filters") or {}),
        "dataset_views": {view: {"row_count": len(rows)} for view, rows in dataset_views.items()},
        "row_count": sum(len(rows) for rows in dataset_views.values()),
        "coverage_counts": {
            "candidate_count": len(dataset_views["candidates"]),
            "hook_count": 0,
            "outcome_count": 0,
            "performance_count": 0,
        },
        "warning_count": 0,
        "warnings": [],
        "source_approval_target_manifest_path": source_manifest_path,
        "source_approval_target_dataset_id": source_payload.get("dataset_id"),
        "source_approval_target_schema_version": source_payload.get("schema_version"),
    }


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return DEFAULT_OUTPUT_ROOT if output_root is None else Path(output_root).expanduser().resolve()


def _dataset_paths(
    *,
    export_root: Path,
    dataset_export_id: str,
    game: str,
    output_path: str | Path | None,
) -> dict[str, Path]:
    manifest_path = _resolve_manifest_path(output_path) if output_path is not None else export_root / game / f"v2-training-{dataset_export_id}.manifest.json"
    dataset_root = manifest_path.parent
    base_name = manifest_path.stem.removesuffix(".manifest")
    paths = {"manifest_path": manifest_path}
    for view in DATASET_VIEWS:
        paths[f"{view}_jsonl_path"] = dataset_root / f"{base_name}.{view}.jsonl"
        paths[f"{view}_csv_path"] = dataset_root / f"{base_name}.{view}.csv"
    return paths


def _write_dataset_artifacts(
    *,
    dataset_views: dict[str, list[dict[str, Any]]],
    manifest: dict[str, Any],
    paths: dict[str, Path],
) -> None:
    paths["manifest_path"].parent.mkdir(parents=True, exist_ok=True)
    for view in DATASET_VIEWS:
        jsonl_path = paths[f"{view}_jsonl_path"]
        csv_path = paths[f"{view}_csv_path"]
        _write_jsonl_view(jsonl_path, dataset_views[view])
        _write_csv_view(csv_path, dataset_views[view])
        manifest["dataset_views"][view]["jsonl_path"] = str(jsonl_path)
        manifest["dataset_views"][view]["csv_path"] = str(csv_path)
    paths["manifest_path"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _write_jsonl_view(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _write_csv_view(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not fieldnames:
            handle.write("")
            return
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
