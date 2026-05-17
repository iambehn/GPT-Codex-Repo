from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .dossier import DEFAULT_DOSSIER_TEMPLATE, dossier_section_spec

SCHEMA_VERSION = "research_runtime_evaluation_v1"
SPECULATION_MARKERS = ("maybe", "probably", "perhaps", "i think", "guess", "might")
VAGUE_CITATION_MARKERS = ("source", "citation", "unknown", "n/a", "tbd")


def evaluate_research_runtime_artifacts(
    *,
    artifact_path: str | Path,
    trace_path: str | Path,
) -> dict[str, Any]:
    artifact = _load_json(artifact_path)
    trace = _load_json(trace_path)
    return evaluate_research_runtime_payloads(
        artifact=artifact,
        trace=trace,
        artifact_path=str(_resolve_path(artifact_path)),
        trace_path=str(_resolve_path(trace_path)),
    )


def evaluate_research_runtime_payloads(
    *,
    artifact: dict[str, Any],
    trace: dict[str, Any],
    artifact_path: str | None = None,
    trace_path: str | None = None,
) -> dict[str, Any]:
    sections = artifact.get("sections") if isinstance(artifact.get("sections"), dict) else {}
    turn_plan = artifact.get("turn_plan") if isinstance(artifact.get("turn_plan"), list) else []
    trace_rows = trace.get("rows") if isinstance(trace.get("rows"), list) else []
    dossier_template = str(artifact.get("dossier_template") or DEFAULT_DOSSIER_TEMPLATE)
    checks = [
        _check_turn_plan_alignment(sections=sections, turn_plan=turn_plan),
        _check_required_lists_present(sections=sections, field_name="assumptions"),
        _check_required_lists_present(sections=sections, field_name="uncertainties"),
        _check_required_lists_present(sections=sections, field_name="failure_modes"),
        _check_source_refs_present(sections=sections),
        _check_source_ref_quality(sections=sections),
        _check_acceptance_criteria_coverage(sections=sections),
        _check_trace_rows_have_outcomes(trace_rows=trace_rows),
        _check_prompt_bundle_alignment(trace_rows=trace_rows, dossier_template=dossier_template),
        _check_speculation_markers(sections=sections),
    ]
    summary = {
        "pass_count": sum(1 for check in checks if check["status"] == "pass"),
        "warn_count": sum(1 for check in checks if check["status"] == "warn"),
        "fail_count": sum(1 for check in checks if check["status"] == "fail"),
    }
    overall_status = "pass"
    if summary["fail_count"] > 0:
        overall_status = "fail"
    elif summary["warn_count"] > 0:
        overall_status = "warn"
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_path": artifact_path,
        "trace_path": trace_path,
        "overall_status": overall_status,
        "summary": summary,
        "checks": checks,
    }


def _check_turn_plan_alignment(*, sections: dict[str, Any], turn_plan: list[Any]) -> dict[str, Any]:
    completed = set(sections.keys())
    planned = {str(item) for item in turn_plan if isinstance(item, str)}
    unexpected = sorted(completed - planned)
    missing = [item for item in turn_plan if item not in completed]
    if unexpected:
        return {
            "name": "turn_plan_alignment",
            "status": "fail",
            "detail": f"Committed sections are outside the turn plan: {', '.join(unexpected)}",
        }
    return {
        "name": "turn_plan_alignment",
        "status": "pass",
        "detail": f"Committed sections align to the turn plan; missing planned sections: {len(missing)}",
    }


def _check_required_lists_present(*, sections: dict[str, Any], field_name: str) -> dict[str, Any]:
    missing = []
    for section_name, section_record in sections.items():
        payload = section_record.get("payload") if isinstance(section_record, dict) else None
        values = payload.get(field_name) if isinstance(payload, dict) else None
        if not isinstance(values, list) or not values:
            missing.append(section_name)
    if missing:
        return {
            "name": f"{field_name}_present",
            "status": "fail",
            "detail": f"Committed sections missing non-empty {field_name}: {', '.join(sorted(missing))}",
        }
    return {
        "name": f"{field_name}_present",
        "status": "pass",
        "detail": f"All committed sections include non-empty {field_name}.",
    }


def _check_source_refs_present(*, sections: dict[str, Any]) -> dict[str, Any]:
    missing = []
    for section_name, section_record in sections.items():
        source_refs = section_record.get("source_refs") if isinstance(section_record, dict) else None
        if not isinstance(source_refs, list) or not source_refs:
            missing.append(section_name)
    if missing:
        return {
            "name": "source_refs_present",
            "status": "fail",
            "detail": f"Committed sections missing source_refs: {', '.join(sorted(missing))}",
        }
    return {
        "name": "source_refs_present",
        "status": "pass",
        "detail": "All committed sections include source_refs.",
    }


def _check_source_ref_quality(*, sections: dict[str, Any]) -> dict[str, Any]:
    vague_hits: list[str] = []
    for section_name, section_record in sections.items():
        source_refs = section_record.get("source_refs") if isinstance(section_record, dict) else None
        if not isinstance(source_refs, list):
            continue
        for index, source_ref in enumerate(source_refs):
            if not isinstance(source_ref, dict):
                vague_hits.append(f"{section_name}[{index}]:non-object")
                continue
            citation = str(source_ref.get("citation") or "").strip().lower()
            locator = str(source_ref.get("locator") or "").strip().lower()
            if len(citation) < 8 or citation in VAGUE_CITATION_MARKERS:
                vague_hits.append(f"{section_name}[{index}]:weak_citation")
            if not locator or locator in VAGUE_CITATION_MARKERS:
                vague_hits.append(f"{section_name}[{index}]:weak_locator")
    if vague_hits:
        return {
            "name": "source_ref_quality",
            "status": "warn",
            "detail": f"Source refs contain vague citation or locator fields: {', '.join(vague_hits)}",
        }
    return {
        "name": "source_ref_quality",
        "status": "pass",
        "detail": "Source refs have non-trivial citation and locator fields.",
    }


def _check_acceptance_criteria_coverage(*, sections: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    for section_name, section_record in sections.items():
        payload = section_record.get("payload") if isinstance(section_record, dict) else None
        if not isinstance(payload, dict):
            failures.append(f"{section_name}:missing_payload")
            continue
        if section_name == "domain_framing":
            if not str(payload.get("problem_statement") or "").strip():
                failures.append(f"{section_name}:problem_statement")
            if not str(payload.get("research_goal") or "").strip():
                failures.append(f"{section_name}:research_goal")
        elif section_name == "question_decomposition":
            subquestions = payload.get("subquestions")
            if not str(payload.get("primary_question") or "").strip():
                failures.append(f"{section_name}:primary_question")
            if not isinstance(subquestions, list) or len(subquestions) < 2:
                failures.append(f"{section_name}:subquestions")
        elif section_name == "architecture_dataflow":
            stages = payload.get("stages")
            dependencies = payload.get("crossmodal_dependencies")
            if not str(payload.get("current_pipeline_summary") or "").strip():
                failures.append(f"{section_name}:current_pipeline_summary")
            if not isinstance(stages, list) or not stages:
                failures.append(f"{section_name}:stages")
            if not isinstance(dependencies, list) or not dependencies:
                failures.append(f"{section_name}:crossmodal_dependencies")
    if failures:
        return {
            "name": "acceptance_criteria_coverage",
            "status": "fail",
            "detail": f"Committed sections do not satisfy dossier coverage heuristics: {', '.join(failures)}",
        }
    return {
        "name": "acceptance_criteria_coverage",
        "status": "pass",
        "detail": "Committed sections satisfy dossier coverage heuristics.",
    }


def _check_trace_rows_have_outcomes(*, trace_rows: list[Any]) -> dict[str, Any]:
    unresolved = []
    for index, row in enumerate(trace_rows):
        if not isinstance(row, dict):
            unresolved.append(str(index))
            continue
        committed = bool(row.get("committed"))
        quarantined = bool(row.get("quarantined"))
        validation_error = row.get("validation_error")
        if not committed and not quarantined and not validation_error:
            unresolved.append(str(row.get("turn_index") or index))
    if unresolved:
        return {
            "name": "trace_rows_have_outcomes",
            "status": "fail",
            "detail": f"Trace rows missing a terminal outcome: {', '.join(unresolved)}",
        }
    return {
        "name": "trace_rows_have_outcomes",
        "status": "pass",
        "detail": "All trace rows record a terminal validation outcome.",
    }


def _check_prompt_bundle_alignment(*, trace_rows: list[Any], dossier_template: str) -> dict[str, Any]:
    mismatches: list[str] = []
    for index, row in enumerate(trace_rows):
        if not isinstance(row, dict):
            mismatches.append(f"row_{index}:non_object")
            continue
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else None
        if not isinstance(request_payload, dict):
            mismatches.append(f"row_{index}:missing_request_payload")
            continue
        turn_input = request_payload.get("turn_input") if isinstance(request_payload.get("turn_input"), dict) else None
        if not isinstance(turn_input, dict):
            mismatches.append(f"row_{index}:missing_turn_input")
            continue
        section_name = str(turn_input.get("target_section") or row.get("target_section") or "").strip()
        if not section_name:
            mismatches.append(f"row_{index}:missing_target_section")
            continue
        try:
            section_spec = dossier_section_spec(section_name, template_name=dossier_template)
        except Exception:
            mismatches.append(f"{section_name}:unsupported_dossier_section")
            continue
        prompt_bundle = turn_input.get("prompt_bundle") if isinstance(turn_input.get("prompt_bundle"), dict) else None
        if not isinstance(prompt_bundle, dict):
            mismatches.append(f"{section_name}:missing_prompt_bundle")
            continue
        if str(prompt_bundle.get("turn_lens") or "").strip() != str(section_spec["turn_lens"]):
            mismatches.append(f"{section_name}:turn_lens")
        prompt_criteria = prompt_bundle.get("acceptance_criteria")
        if list(prompt_criteria or []) != list(section_spec["acceptance_criteria"]):
            mismatches.append(f"{section_name}:acceptance_criteria")
    if mismatches:
        return {
            "name": "prompt_bundle_alignment",
            "status": "fail",
            "detail": f"Trace prompt bundles drift from dossier section specs: {', '.join(mismatches)}",
        }
    return {
        "name": "prompt_bundle_alignment",
        "status": "pass",
        "detail": "Trace prompt bundles align with dossier section specs.",
    }


def _check_speculation_markers(*, sections: dict[str, Any]) -> dict[str, Any]:
    hits: list[str] = []
    for section_name, section_record in sections.items():
        payload = section_record.get("payload") if isinstance(section_record, dict) else None
        for marker in SPECULATION_MARKERS:
            if _payload_contains_marker(payload, marker):
                hits.append(f"{section_name}:{marker}")
    if hits:
        return {
            "name": "speculation_markers",
            "status": "warn",
            "detail": f"Speculative language markers found in committed sections: {', '.join(hits)}",
        }
    return {
        "name": "speculation_markers",
        "status": "pass",
        "detail": "No heuristic speculative language markers found in committed payloads.",
    }


def _payload_contains_marker(payload: Any, marker: str) -> bool:
    if isinstance(payload, str):
        return marker in payload.lower()
    if isinstance(payload, list):
        return any(_payload_contains_marker(item, marker) for item in payload)
    if isinstance(payload, dict):
        return any(_payload_contains_marker(value, marker) for value in payload.values())
    return False


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        return (Path.cwd() / candidate).resolve()
    return candidate.resolve()
