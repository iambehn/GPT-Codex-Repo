from __future__ import annotations

from typing import Any

from .schemas import SECTION_MODELS

DOSSIER_SCHEMA_VERSION = "research_runtime_pipeline_design_dossier_v1"
DEFAULT_DOSSIER_TEMPLATE = "pipeline_design_research_v1"

_DEFAULT_DOSSIER = {
    "schema_version": DOSSIER_SCHEMA_VERSION,
    "dossier_template": DEFAULT_DOSSIER_TEMPLATE,
    "title": "Pipeline Design Research Dossier",
    "turn_plan": [
        "domain_framing",
        "question_decomposition",
        "architecture_dataflow",
    ],
    "sections": {
        "domain_framing": {
            "turn_lens": "frame the bounded problem and research objective for the pipeline work",
            "acceptance_criteria": [
                "State the pipeline problem in repo-specific terms.",
                "Define the research goal without implementation sprawl.",
                "List assumptions, uncertainties, and failure modes explicitly.",
            ],
        },
        "question_decomposition": {
            "turn_lens": "decompose the next research questions needed to move the pipeline design forward",
            "acceptance_criteria": [
                "Identify one primary question.",
                "Break it into non-overlapping subquestions.",
                "Keep the decomposition bounded to this repo and pipeline.",
            ],
        },
        "architecture_dataflow": {
            "turn_lens": "map the current pipeline dataflow and the crossmodal dependencies relevant to the research question",
            "acceptance_criteria": [
                "Summarize the current pipeline path in implementation-relevant terms.",
                "List the main stages in order.",
                "Call out crossmodal dependencies, assumptions, uncertainties, and failure modes.",
            ],
        },
    },
}


def dossier_template_names() -> list[str]:
    return [DEFAULT_DOSSIER_TEMPLATE]


def load_dossier_template(template_name: str = DEFAULT_DOSSIER_TEMPLATE) -> dict[str, Any]:
    if template_name != DEFAULT_DOSSIER_TEMPLATE:
        raise ValueError(f"unsupported dossier template: {template_name}")
    return {
        "schema_version": _DEFAULT_DOSSIER["schema_version"],
        "dossier_template": _DEFAULT_DOSSIER["dossier_template"],
        "title": _DEFAULT_DOSSIER["title"],
        "turn_plan": list(_DEFAULT_DOSSIER["turn_plan"]),
        "sections": {
            key: {
                "turn_lens": value["turn_lens"],
                "acceptance_criteria": list(value["acceptance_criteria"]),
            }
            for key, value in _DEFAULT_DOSSIER["sections"].items()
        },
    }


def default_turn_plan(template_name: str = DEFAULT_DOSSIER_TEMPLATE) -> list[str]:
    return list(load_dossier_template(template_name)["turn_plan"])


def dossier_section_spec(section_name: str, template_name: str = DEFAULT_DOSSIER_TEMPLATE) -> dict[str, Any]:
    template = load_dossier_template(template_name)
    section_spec = template["sections"].get(section_name)
    if section_spec is None:
        raise ValueError(f"section {section_name} is not configured for dossier {template_name}")
    if section_name not in SECTION_MODELS:
        raise ValueError(f"section {section_name} does not have a runtime schema")
    return {
        "section_name": section_name,
        "turn_lens": section_spec["turn_lens"],
        "acceptance_criteria": list(section_spec["acceptance_criteria"]),
    }


def dossier_metadata(template_name: str = DEFAULT_DOSSIER_TEMPLATE) -> dict[str, Any]:
    template = load_dossier_template(template_name)
    return {
        "schema_version": template["schema_version"],
        "dossier_template": template["dossier_template"],
        "title": template["title"],
        "turn_plan": list(template["turn_plan"]),
    }
