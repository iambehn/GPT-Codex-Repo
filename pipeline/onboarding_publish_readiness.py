from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pipeline.simple_yaml import load_yaml_file


_INFO_QA_TYPES = {"binding_candidate"}
_POPULATION_QA_TYPES = {
    "source_fetch_failed",
    "ambiguous_seed_match",
    "ambiguous_identity_match",
    "ambiguous_structured_extraction",
    "conflicting_identity_match",
    "identity_match_rejected",
    "source_seed_disagreement",
}
_INFO_QA_TYPES = _INFO_QA_TYPES | {"canonical_identity_preference_applied"}
_BINDING_QA_TYPES = {
    "missing_semantic_values",
    "missing_binding",
    "unbound_candidate",
    "low_quality_candidate",
    "image_kind_mismatch",
    "duplicate_candidate_cluster",
    "weak_name_match",
    "lower_trust_source_kind",
    "binding_image_kind_mismatch",
    "conflicting_binding_candidates",
}


def validate_onboarding_publish(
    draft_root: str | Path,
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    resolved_root = Path(draft_root).expanduser().resolve()
    state = _load_json_required(resolved_root / "manifests" / "onboarding_state.json", label="onboarding state")
    assets_manifest = _load_json_required(resolved_root / "manifests" / "assets_manifest.json", label="assets manifest")
    detection_manifest = _load_yaml_mapping(resolved_root / "manifests" / "detection_manifest.yaml", label="detection manifest")

    game = str(state.get("game_id", assets_manifest.get("game_id", ""))).strip()
    if not game:
        raise ValueError("draft onboarding state must include game_id")

    candidates = assets_manifest.get("candidates", [])
    if not isinstance(candidates, list):
        raise ValueError("draft assets manifest candidates must be a list")
    candidates_by_id = {str(row.get("candidate_id", "")): row for row in candidates if str(row.get("candidate_id", "")).strip()}

    detection_rows = detection_manifest.get("rows", [])
    if not isinstance(detection_rows, list):
        raise ValueError("draft detection manifest rows must be a list")
    bindings = _read_csv_rows(resolved_root / "catalog" / "bindings.csv")
    qa_rows = _read_csv_rows(resolved_root / "catalog" / "qa_queue.csv")
    source_fetch_log = assets_manifest.get("source_fetch_log", [])
    if not isinstance(source_fetch_log, list):
        raise ValueError("draft assets manifest source_fetch_log must be a list")

    required_rows = [row for row in detection_rows if bool(row.get("requires_asset", True))]
    accepted_bindings = [row for row in bindings if str(row.get("status", "")).strip() == "accepted"]
    accepted_by_detection: dict[str, list[dict[str, Any]]] = {}
    structural_findings: list[dict[str, Any]] = []
    for row in accepted_bindings:
        detection_id = str(row.get("detection_id", "")).strip()
        if not detection_id:
            structural_findings.append(
                {
                    "type": "invalid_accepted_binding",
                    "severity": "structural",
                    "message": "accepted binding is missing detection_id",
                }
            )
            continue
        accepted_by_detection.setdefault(detection_id, []).append(row)

    for detection_id, grouped in accepted_by_detection.items():
        if len(grouped) > 1:
            structural_findings.append(
                {
                    "type": "conflicting_accepted_bindings",
                    "severity": "structural",
                    "detection_id": detection_id,
                    "message": f"multiple accepted bindings exist for detection row '{detection_id}'",
                }
            )

    binding_findings: list[dict[str, Any]] = []
    for row in required_rows:
        detection_id = str(row.get("detection_id", "")).strip()
        target_id = str(row.get("target_id", "")).strip()
        accepted = accepted_by_detection.get(detection_id, [])
        if not accepted:
            binding_findings.append(
                {
                    "type": "missing_accepted_binding",
                    "severity": "binding",
                    "detection_id": detection_id,
                    "target_id": target_id,
                    "message": f"required detection row '{detection_id}' does not have an accepted binding",
                }
            )
            continue
        candidate_id = str(accepted[0].get("candidate_id", "")).strip()
        candidate = candidates_by_id.get(candidate_id)
        if candidate is None:
            structural_findings.append(
                {
                    "type": "accepted_binding_missing_candidate",
                    "severity": "structural",
                    "detection_id": detection_id,
                    "candidate_id": candidate_id,
                    "message": f"accepted binding for '{detection_id}' references missing candidate '{candidate_id}'",
                }
            )
            continue
        master_path = str(candidate.get("master_path", "")).strip()
        if not master_path:
            structural_findings.append(
                {
                    "type": "accepted_binding_missing_master_path",
                    "severity": "structural",
                    "detection_id": detection_id,
                    "candidate_id": candidate_id,
                    "message": f"accepted binding for '{detection_id}' does not have a candidate master_path",
                }
            )
            continue
        if not Path(master_path).exists():
            structural_findings.append(
                {
                    "type": "accepted_binding_missing_asset_file",
                    "severity": "structural",
                    "detection_id": detection_id,
                    "candidate_id": candidate_id,
                    "message": f"accepted binding for '{detection_id}' points to a missing asset file",
                }
            )

    source_status_counts = _count_by_field(source_fetch_log, "status")
    source_fetched_count = int(source_status_counts.get("fetched", 0))
    source_count = int(state.get("source_count", assets_manifest.get("source_count", 0)) or 0)
    population_findings: list[dict[str, Any]] = []
    if source_count > 0 and source_fetched_count == 0:
        population_findings.append(
            {
                "type": "missing_required_source_coverage",
                "severity": "population",
                "message": "draft has source inputs but no successfully fetched populated sources",
            }
        )

    for row in qa_rows:
        item_type = str(row.get("item_type", "")).strip()
        if not item_type or item_type in _INFO_QA_TYPES:
            continue
        finding = {
            "type": item_type,
            "severity": "population" if item_type in _POPULATION_QA_TYPES else "binding",
            "status": str(row.get("status", "")).strip(),
            "reason": str(row.get("reason", "")).strip(),
            "detection_id": str(row.get("detection_id", "")).strip(),
            "target_id": str(row.get("target_id", "")).strip(),
        }
        if item_type in _POPULATION_QA_TYPES:
            population_findings.append(finding)
        elif item_type in _BINDING_QA_TYPES:
            binding_findings.append(finding)

    if structural_findings:
        readiness = "structurally_invalid"
    elif population_findings:
        readiness = "needs_population_review"
    elif binding_findings:
        readiness = "needs_binding_review"
    else:
        readiness = "ready_to_publish"

    findings = structural_findings + population_findings + binding_findings
    return {
        "ok": True,
        "draft_root": str(resolved_root),
        "game": game,
        "phase_status": str(state.get("phase_status", "")).strip(),
        "can_publish": readiness == "ready_to_publish",
        "readiness": readiness,
        "source_summary": {
            "source_count": source_count,
            "fetched_count": source_fetched_count,
            "status_counts": source_status_counts,
        },
        "counts": {
            "required_detection_rows": len(required_rows),
            "accepted_bindings": len(accepted_bindings),
            "structural_findings": len(structural_findings),
            "population_findings": len(population_findings),
            "binding_findings": len(binding_findings),
            "qa_rows": len(qa_rows),
        },
        "findings": findings,
    }


def _load_json_required(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"draft is missing {label}: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"draft {label} must be a mapping: {path}")
    return payload


def _load_yaml_mapping(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"draft is missing {label}: {path}")
    payload = load_yaml_file(path)
    if not isinstance(payload, dict):
        raise ValueError(f"draft {label} must be a mapping: {path}")
    return payload


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
    if len(rows) == 1 and set(rows[0].keys()) == {"empty"}:
        return []
    return rows


def _count_by_field(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(field, "")).strip() or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return counts
