from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pipeline.evaluation_fixtures import load_evaluation_fixture_manifest


HOOK_CANDIDATE_COMPARISON_SCHEMA_VERSION = "hook_candidate_comparison_v1"
HOOK_CANDIDATE_SCHEMA_VERSION = "hook_candidate_v1"
DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "outputs" / "hook_candidate_comparisons"
DIMENSION_FIELDS = (
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


def compare_hook_candidates(
    fixture_manifest: str | Path,
    *,
    baseline_sidecar_root: str | Path,
    trial_sidecar_root: str | Path,
    game: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest = load_evaluation_fixture_manifest(fixture_manifest)
    baseline_root = _resolve_path(baseline_sidecar_root)
    trial_root = _resolve_path(trial_sidecar_root)
    if not baseline_root.exists() or not baseline_root.is_dir():
        return {
            "ok": False,
            "status": "invalid_baseline_sidecar_root",
            "baseline_sidecar_root": str(baseline_root),
            "error": "baseline sidecar root does not exist or is not a directory",
        }
    if not trial_root.exists() or not trial_root.is_dir():
        return {
            "ok": False,
            "status": "invalid_trial_sidecar_root",
            "trial_sidecar_root": str(trial_root),
            "error": "trial sidecar root does not exist or is not a directory",
        }

    warnings: list[dict[str, Any]] = []
    fixture_rows: list[dict[str, Any]] = []
    for fixture in list(manifest.get("fixtures", [])):
        fixture_rows.extend(
            _compare_fixture(
                fixture,
                baseline_root=baseline_root,
                trial_root=trial_root,
                game=game,
                warnings=warnings,
            )
        )

    summary = _summary(fixture_rows)
    recommendation = _recommendation(fixture_rows)
    report = {
        "ok": True,
        "status": "ok",
        "schema_version": HOOK_CANDIDATE_COMPARISON_SCHEMA_VERSION,
        "fixture_manifest_path": str(_resolve_path(fixture_manifest)),
        "baseline_sidecar_root": str(baseline_root),
        "trial_sidecar_root": str(trial_root),
        "fixture_count": int(manifest.get("fixture_count", len(manifest.get("fixtures", [])))),
        "comparison_row_count": len(fixture_rows),
        "comparison": {
            "fixture_rows": fixture_rows,
            "summary": summary,
        },
        "recommendation": recommendation,
        "warnings": warnings,
    }
    if game is not None:
        report["game_filter"] = game

    if output_path is not None:
        report_path = _resolve_path(output_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        csv_path = report_path.with_suffix(".csv")
        warnings_path = report_path.with_suffix(".warnings.json")
        _write_csv(csv_path, fixture_rows)
        warnings_path.write_text(json.dumps(warnings, indent=2), encoding="utf-8")
        report["report_path"] = str(report_path)
        report["csv_path"] = str(csv_path)
        report["warnings_path"] = str(warnings_path)
    return report


def _compare_fixture(
    fixture: dict[str, Any],
    *,
    baseline_root: Path,
    trial_root: Path,
    game: str | None,
    warnings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    fixture_id = str(fixture["fixture_id"])
    baseline_path = _resolve_hook_sidecar(baseline_root, fixture, trial_name="baseline", warnings=warnings)
    trial_path = _resolve_hook_sidecar(trial_root, fixture, trial_name="trial", warnings=warnings)
    baseline_manifest = _load_hook_manifest(baseline_path, fixture_id=fixture_id, trial_name="baseline", game=game, warnings=warnings)
    trial_manifest = _load_hook_manifest(trial_path, fixture_id=fixture_id, trial_name="trial", game=game, warnings=warnings)

    baseline_by_key = _row_index((baseline_manifest or {}).get("hook_candidates", []))
    trial_by_key = _row_index((trial_manifest or {}).get("hook_candidates", []))
    keys = sorted(set(baseline_by_key) | set(trial_by_key))
    if not keys:
        return [
            _comparison_row(
                fixture=fixture,
                baseline_manifest=baseline_manifest,
                trial_manifest=trial_manifest,
                baseline_row=None,
                trial_row=None,
            )
        ]
    rows = []
    for key in keys:
        rows.append(
            _comparison_row(
                fixture=fixture,
                baseline_manifest=baseline_manifest,
                trial_manifest=trial_manifest,
                baseline_row=baseline_by_key.get(key),
                trial_row=trial_by_key.get(key),
            )
        )
    return rows


def _comparison_row(
    *,
    fixture: dict[str, Any],
    baseline_manifest: dict[str, Any] | None,
    trial_manifest: dict[str, Any] | None,
    baseline_row: dict[str, Any] | None,
    trial_row: dict[str, Any] | None,
) -> dict[str, Any]:
    fixture_id = str(fixture["fixture_id"])
    comparison_status = _comparison_status(baseline_row, trial_row)
    candidate_id = _coalesce(baseline_row, trial_row, "candidate_id")
    event_id = _coalesce(baseline_row, trial_row, "event_id")
    baseline_quality = _hook_quality(baseline_row)
    trial_quality = _hook_quality(trial_row)
    recommendation_signal = _recommendation_signal(comparison_status, baseline_quality, trial_quality)
    baseline_final_score = _float((baseline_row or {}).get("final_score"))
    trial_final_score = _float((trial_row or {}).get("final_score"))
    fused_score = baseline_final_score if baseline_final_score is not None else trial_final_score
    row = {
        "fixture_id": fixture_id,
        "label": str(fixture.get("label", fixture_id)),
        "game": _coalesce(baseline_manifest, trial_manifest, "game"),
        "source": _coalesce(baseline_manifest, trial_manifest, "source"),
        "candidate_id": candidate_id,
        "event_id": event_id,
        "comparison_status": comparison_status,
        "review_status": _effective_review_status(baseline_row, trial_row),
        "baseline_manifest_path": (baseline_manifest or {}).get("manifest_path"),
        "trial_manifest_path": (trial_manifest or {}).get("manifest_path"),
        "baseline_fused_sidecar_path": (baseline_manifest or {}).get("fused_sidecar_path"),
        "trial_fused_sidecar_path": (trial_manifest or {}).get("fused_sidecar_path"),
        "baseline_hook_id": (baseline_row or {}).get("hook_id"),
        "trial_hook_id": (trial_row or {}).get("hook_id"),
        "baseline_hook_mode": (baseline_row or {}).get("hook_mode"),
        "trial_hook_mode": (trial_row or {}).get("hook_mode"),
        "baseline_hook_archetype": (baseline_row or {}).get("hook_archetype"),
        "trial_hook_archetype": (trial_row or {}).get("hook_archetype"),
        "baseline_hook_strength": _float((baseline_row or {}).get("hook_strength")),
        "trial_hook_strength": _float((trial_row or {}).get("hook_strength")),
        "hook_strength_delta": _delta((baseline_row or {}).get("hook_strength"), (trial_row or {}).get("hook_strength")),
        "baseline_lifecycle_state": (baseline_row or {}).get("lifecycle_state"),
        "trial_lifecycle_state": (trial_row or {}).get("lifecycle_state"),
        "baseline_selection_manifest_path": (baseline_row or {}).get("highlight_selection_manifest_path"),
        "trial_selection_manifest_path": (trial_row or {}).get("highlight_selection_manifest_path"),
        "baseline_final_score": baseline_final_score,
        "trial_final_score": trial_final_score,
        "strong_fused_weak_hook": bool(fused_score is not None and fused_score >= 0.85 and max(_float((baseline_row or {}).get("hook_strength")) or 0.0, _float((trial_row or {}).get("hook_strength")) or 0.0) < 0.55),
        "approved_reject_hook": bool(_effective_review_status(baseline_row, trial_row) == "approved" and ((baseline_row or {}).get("hook_mode") == "reject" or (trial_row or {}).get("hook_mode") == "reject")),
        "reject_to_synthetic": (baseline_row or {}).get("hook_mode") == "reject" and (trial_row or {}).get("hook_mode") == "synthetic",
        "natural_to_synthetic": (baseline_row or {}).get("hook_mode") == "natural" and (trial_row or {}).get("hook_mode") == "synthetic",
        "recommendation_signal": recommendation_signal,
    }
    for field in DIMENSION_FIELDS:
        row[f"baseline_{field}"] = _float((baseline_row or {}).get(field))
        row[f"trial_{field}"] = _float((trial_row or {}).get(field))
        row[f"{field}_delta"] = _delta((baseline_row or {}).get(field), (trial_row or {}).get(field))
    return row


def _hook_quality(row: dict[str, Any] | None) -> float | None:
    if row is None:
        return None
    strength = _float(row.get("hook_strength")) or 0.0
    mode = str(row.get("hook_mode") or "").strip()
    authenticity = _float(row.get("authenticity_risk_score")) or 0.0
    final_score = _float(row.get("final_score")) or 0.0
    quality = strength
    if mode == "natural":
        quality += 0.08
    elif mode == "synthetic":
        quality -= 0.05
    elif mode == "reject":
        quality -= 0.3
        if final_score >= 0.8:
            quality -= 0.12
    quality -= authenticity * 0.18
    return round(quality, 4)


def _comparison_status(baseline_row: dict[str, Any] | None, trial_row: dict[str, Any] | None) -> str:
    if baseline_row and trial_row:
        return "matched"
    if baseline_row and not trial_row:
        return "baseline_only"
    if trial_row and not baseline_row:
        return "trial_only"
    return "missing"


def _recommendation_signal(comparison_status: str, baseline_quality: float | None, trial_quality: float | None) -> str:
    if comparison_status != "matched" or baseline_quality is None or trial_quality is None:
        return "inconclusive"
    if trial_quality >= baseline_quality + 0.05:
        return "trial_better"
    if baseline_quality >= trial_quality + 0.05:
        return "current_better"
    return "inconclusive"


def _effective_review_status(baseline_row: dict[str, Any] | None, trial_row: dict[str, Any] | None) -> str | None:
    for row in (baseline_row, trial_row):
        state = str((row or {}).get("lifecycle_state") or "").strip()
        if state == "approved":
            return "approved"
        if state == "selected_for_export":
            return "approved"
        if state == "rejected":
            return "rejected"
    return None


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    mode_summary = {"baseline": {}, "trial": {}}
    archetype_summary = {"baseline": {}, "trial": {}}
    recommendation_counts: dict[str, int] = {}
    for row in rows:
        for side in ("baseline", "trial"):
            mode = str(row.get(f"{side}_hook_mode") or "").strip()
            archetype = str(row.get(f"{side}_hook_archetype") or "").strip()
            if mode:
                mode_summary[side][mode] = int(mode_summary[side].get(mode, 0)) + 1
            if archetype:
                archetype_summary[side][archetype] = int(archetype_summary[side].get(archetype, 0)) + 1
        signal = str(row.get("recommendation_signal") or "inconclusive")
        recommendation_counts[signal] = int(recommendation_counts.get(signal, 0)) + 1
    return {
        "hook_mode_counts": mode_summary,
        "hook_archetype_counts": archetype_summary,
        "recommendation_signal_counts": recommendation_counts,
    }


def _recommendation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    matched = [row for row in rows if str(row.get("comparison_status")) == "matched"]
    if not matched:
        return {
            "decision": "inconclusive",
            "reason": "No matched baseline and trial hook candidates were available.",
        }
    prefer_trial = 0
    keep_current = 0
    for row in matched:
        signal = str(row.get("recommendation_signal") or "")
        if signal == "trial_better":
            prefer_trial += 1
        elif signal == "current_better":
            keep_current += 1
    if prefer_trial > keep_current and prefer_trial > 0:
        return {
            "decision": "prefer_trial",
            "reason": "Trial hook heuristics improved matched hook candidates more often than they regressed them.",
        }
    if keep_current > prefer_trial and keep_current > 0:
        return {
            "decision": "keep_current",
            "reason": "Current hook heuristics outperformed the trial on matched hook candidates.",
        }
    return {
        "decision": "inconclusive",
        "reason": "Hook heuristic results are mixed or too sparse to support a change.",
    }


def _row_index(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        candidate_id = str(row.get("candidate_id") or "").strip()
        event_id = str(row.get("event_id") or "").strip()
        key = (candidate_id, event_id) if candidate_id else ("", event_id)
        if key == ("", ""):
            continue
        index[key] = row
    return index


def _load_hook_manifest(
    path: Path | None,
    *,
    fixture_id: str,
    trial_name: str,
    game: str | None,
    warnings: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        warnings.append({"fixture_id": fixture_id, "trial_name": trial_name, "reason": "malformed_json", "detail": str(exc)})
        return None
    if payload.get("schema_version") != HOOK_CANDIDATE_SCHEMA_VERSION:
        warnings.append({"fixture_id": fixture_id, "trial_name": trial_name, "reason": "unsupported_schema_version", "detail": str(payload.get("schema_version"))})
        return None
    payload_game = str(payload.get("game") or "").strip()
    if game is not None and payload_game and payload_game != game:
        return None
    enriched = dict(payload)
    enriched["manifest_path"] = str(path.resolve())
    return enriched


def _resolve_hook_sidecar(root: Path, fixture: dict[str, Any], *, trial_name: str, warnings: list[dict[str, Any]]) -> Path | None:
    fixture_id = str(fixture["fixture_id"])
    artifact_refs = dict(fixture.get("artifact_refs", {}))
    fused_ref = str(artifact_refs.get("fused_sidecar", "")).strip()
    candidates: list[Path] = []
    if fused_ref:
        fused_name = Path(fused_ref).name
        if fused_name.endswith(".fused_analysis.json"):
            candidates.append(root / fused_name.replace(".fused_analysis.json", ".hook_candidates.json"))
    candidates.append(root / f"{fixture_id}.hook_candidates.json")
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()
    matches = sorted(root.rglob("*.hook_candidates.json"))
    if len(matches) == 1:
        return matches[0].resolve()
    if matches:
        fused_stem = Path(fused_ref).stem.replace(".fused_analysis", "") if fused_ref else fixture_id
        for match in matches:
            if fused_stem and fused_stem in match.stem:
                return match.resolve()
    warnings.append({"fixture_id": fixture_id, "trial_name": trial_name, "reason": "missing_hook_artifact"})
    return None


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "fixture_id",
        "candidate_id",
        "event_id",
        "comparison_status",
        "review_status",
        "baseline_hook_mode",
        "trial_hook_mode",
        "baseline_hook_archetype",
        "trial_hook_archetype",
        "baseline_hook_strength",
        "trial_hook_strength",
        "hook_strength_delta",
        "recommendation_signal",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def _coalesce(baseline: dict[str, Any] | None, trial: dict[str, Any] | None, key: str) -> Any:
    return (baseline or {}).get(key) if (baseline or {}).get(key) not in (None, "") else (trial or {}).get(key)


def _delta(baseline: Any, trial: Any) -> float | None:
    baseline_value = _float(baseline)
    trial_value = _float(trial)
    if baseline_value is None or trial_value is None:
        return None
    return round(trial_value - baseline_value, 4)


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _resolve_path(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()
