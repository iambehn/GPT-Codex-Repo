from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.clip_registry import query_clip_registry
from pipeline.hook_candidate_comparison import compare_hook_candidates


HOOK_EVALUATION_REPORT_SCHEMA_VERSION = "hook_evaluation_report_v1"
DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "outputs" / "hook_evaluation_reports"
_HOOK_DIMENSION_FIELDS = (
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
_ELIGIBLE_LIFECYCLE_STATES = {"approved", "selected_for_export", "exported", "posted"}


def report_hook_evaluation(
    fixture_manifest: str | Path,
    *,
    baseline_sidecar_root: str | Path,
    trial_sidecar_root: str | Path,
    registry_path: str | Path,
    game: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    comparison = compare_hook_candidates(
        fixture_manifest,
        baseline_sidecar_root=baseline_sidecar_root,
        trial_sidecar_root=trial_sidecar_root,
        game=game,
    )
    if not comparison.get("ok"):
        return comparison

    rollups = _candidate_rollups(registry_path=registry_path, game=game)
    if not rollups.get("ok"):
        return rollups

    disagreement_summary = _comparison_disagreement_summary(comparison)
    future_gate_readiness = _future_gate_readiness(comparison, rollups, disagreement_summary)
    report = {
        "ok": True,
        "status": "ok",
        "schema_version": HOOK_EVALUATION_REPORT_SCHEMA_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "fixture_manifest_path": str(Path(fixture_manifest).expanduser().resolve()),
        "baseline_sidecar_root": str(Path(baseline_sidecar_root).expanduser().resolve()),
        "trial_sidecar_root": str(Path(trial_sidecar_root).expanduser().resolve()),
        "registry_path": str(Path(registry_path).expanduser().resolve()),
        "game_filter": game,
        "trial_comparison": {
            "comparison_row_count": comparison.get("comparison_row_count", 0),
            "summary": comparison.get("comparison", {}).get("summary", {}),
            "recommendation": comparison.get("recommendation", {}),
            "fixture_rows": comparison.get("comparison", {}).get("fixture_rows", []),
            "warning_count": len(comparison.get("warnings", [])),
        },
        "candidate_rollups": rollups["rollups"],
        "fused_hook_disagreement": disagreement_summary,
        "policy": {
            "hook_artifacts_policy": "advisory",
            "future_gate_readiness": future_gate_readiness,
            "gating_note": "Hook artifacts remain advisory in V1; this report exists to measure whether future gating is justified.",
        },
        "warnings": comparison.get("warnings", []),
    }
    target = Path(output_path).expanduser().resolve() if output_path is not None else _default_output_path(game=game)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report["report_path"] = str(target)
    return report


def _candidate_rollups(*, registry_path: str | Path, game: str | None) -> dict[str, Any]:
    hook_rows_result = query_clip_registry(
        mode="hook-candidates",
        registry_path=registry_path,
        game=game,
        limit=100000,
    )
    if not hook_rows_result.get("ok"):
        return hook_rows_result
    export_rows_result = query_clip_registry(
        mode="highlight-exports",
        registry_path=registry_path,
        game=game,
        limit=100000,
    )
    if not export_rows_result.get("ok"):
        return export_rows_result

    hook_rows = [
        row
        for row in list(hook_rows_result.get("rows", []))
        if str(row.get("lifecycle_state") or "").strip() in _ELIGIBLE_LIFECYCLE_STATES
    ]
    exported_rows = [
        row
        for row in list(export_rows_result.get("rows", []))
        if str(row.get("export_status") or "").strip() == "exported"
    ]
    selected_or_approved_candidate_ids = {
        str(row.get("candidate_id") or "").strip()
        for row in hook_rows
        if str(row.get("candidate_id") or "").strip()
    }
    exported_candidate_ids = {
        str(row.get("candidate_id") or "").strip()
        for row in exported_rows
        if str(row.get("candidate_id") or "").strip()
    }
    rollups = {
        "selected_or_approved": {
            "candidate_count": len(selected_or_approved_candidate_ids),
            "hook_mode_counts": _count_by_field(hook_rows, "hook_mode"),
            "hook_archetype_counts": _count_by_field(hook_rows, "hook_archetype"),
            "dimension_averages": _dimension_averages(hook_rows),
        },
        "exported": {
            "candidate_count": len(exported_candidate_ids),
            "row_count": len(exported_rows),
            "hook_mode_counts": _count_by_field(exported_rows, "hook_mode"),
            "hook_archetype_counts": _count_by_field(exported_rows, "hook_archetype"),
        },
    }
    return {"ok": True, "rollups": rollups}


def _comparison_disagreement_summary(comparison: dict[str, Any]) -> dict[str, Any]:
    rows = list(comparison.get("comparison", {}).get("fixture_rows", []))
    return {
        "row_count": len(rows),
        "strong_fused_weak_hook_count": sum(1 for row in rows if bool(row.get("strong_fused_weak_hook"))),
        "approved_reject_hook_count": sum(1 for row in rows if bool(row.get("approved_reject_hook"))),
        "reject_to_synthetic_count": sum(1 for row in rows if bool(row.get("reject_to_synthetic"))),
        "natural_to_synthetic_count": sum(1 for row in rows if bool(row.get("natural_to_synthetic"))),
    }


def _future_gate_readiness(
    comparison: dict[str, Any],
    rollups: dict[str, Any],
    disagreement_summary: dict[str, Any],
) -> str:
    selected_count = int(rollups.get("rollups", {}).get("selected_or_approved", {}).get("candidate_count", 0) or 0)
    matched_count = sum(
        1
        for row in list(comparison.get("comparison", {}).get("fixture_rows", []))
        if str(row.get("comparison_status") or "") == "matched"
    )
    if selected_count < 3 or matched_count < 3:
        return "insufficient_evidence"
    if int(disagreement_summary.get("approved_reject_hook_count", 0) or 0) > 0:
        return "candidate_for_gate_review"
    return "not_evaluated"


def _count_by_field(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(field) or "").strip() or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return counts


def _dimension_averages(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    averages: dict[str, float | None] = {}
    for field in _HOOK_DIMENSION_FIELDS:
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


def _default_output_path(*, game: str | None) -> Path:
    slug = _safe_slug(game or "all-games")
    return DEFAULT_OUTPUT_ROOT / slug / f"{slug}.hook_evaluation_report.json"


def _safe_slug(value: str) -> str:
    normalized = "".join(character.lower() if character.isalnum() else "-" for character in value.strip())
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized.strip("-") or "default"
