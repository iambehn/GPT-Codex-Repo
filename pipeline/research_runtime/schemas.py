from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class ResearchRuntimeValidationError(ValueError):
    pass


def _require_dict(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ResearchRuntimeValidationError(f"{label} must be an object")
    return value


def _require_list(value: Any, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ResearchRuntimeValidationError(f"{label} must be a list")
    return value


def _require_str(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchRuntimeValidationError(f"{label} must be a non-empty string")
    return value.strip()


def _require_int(value: Any, *, label: str) -> int:
    if not isinstance(value, int):
        raise ResearchRuntimeValidationError(f"{label} must be an integer")
    return value


def _require_bool(value: Any, *, label: str) -> bool:
    if not isinstance(value, bool):
        raise ResearchRuntimeValidationError(f"{label} must be a boolean")
    return value


def _require_str_list(value: Any, *, label: str) -> list[str]:
    items = _require_list(value, label=label)
    return [_require_str(item, label=f"{label}[]") for item in items]


def _reject_extra_keys(payload: dict[str, Any], *, label: str, allowed_keys: set[str]) -> None:
    extra_keys = sorted(set(payload.keys()) - allowed_keys)
    if extra_keys:
        raise ResearchRuntimeValidationError(f"{label} contains unsupported keys: {', '.join(extra_keys)}")


@dataclass(slots=True)
class SourceRef:
    source_type: str
    locator: str
    citation: str
    excerpt: str | None = None

    @classmethod
    def model_validate(cls, payload: Any) -> "SourceRef":
        data = _require_dict(payload, label="source_ref")
        _reject_extra_keys(data, label="source_ref", allowed_keys={"source_type", "locator", "citation", "excerpt"})
        excerpt = data.get("excerpt")
        if excerpt is not None and not isinstance(excerpt, str):
            raise ResearchRuntimeValidationError("source_ref.excerpt must be a string when provided")
        return cls(
            source_type=_require_str(data.get("source_type"), label="source_ref.source_type"),
            locator=_require_str(data.get("locator"), label="source_ref.locator"),
            citation=_require_str(data.get("citation"), label="source_ref.citation"),
            excerpt=excerpt,
        )

    def model_dump(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "locator": self.locator,
            "citation": self.citation,
            "excerpt": self.excerpt,
        }


@dataclass(slots=True)
class RuntimeMeta:
    adapter_name: str
    attempt_index: int
    corrective_retry: bool = False
    notes: list[str] = field(default_factory=list)

    @classmethod
    def model_validate(cls, payload: Any) -> "RuntimeMeta":
        data = _require_dict(payload, label="runtime_meta")
        _reject_extra_keys(
            data,
            label="runtime_meta",
            allowed_keys={"adapter_name", "attempt_index", "corrective_retry", "notes"},
        )
        notes = data.get("notes", [])
        return cls(
            adapter_name=_require_str(data.get("adapter_name"), label="runtime_meta.adapter_name"),
            attempt_index=_require_int(data.get("attempt_index"), label="runtime_meta.attempt_index"),
            corrective_retry=_require_bool(data.get("corrective_retry", False), label="runtime_meta.corrective_retry"),
            notes=_require_str_list(notes, label="runtime_meta.notes"),
        )

    def model_dump(self) -> dict[str, Any]:
        return {
            "adapter_name": self.adapter_name,
            "attempt_index": self.attempt_index,
            "corrective_retry": self.corrective_retry,
            "notes": list(self.notes),
        }


@dataclass(slots=True)
class TurnOutputEnvelope:
    target_section: str
    artifact_payload: dict[str, Any]
    source_refs: list[SourceRef]
    runtime_meta: RuntimeMeta

    @classmethod
    def model_validate(cls, payload: Any) -> "TurnOutputEnvelope":
        data = _require_dict(payload, label="turn_output_envelope")
        _reject_extra_keys(
            data,
            label="turn_output_envelope",
            allowed_keys={"target_section", "artifact_payload", "source_refs", "runtime_meta"},
        )
        source_refs = [SourceRef.model_validate(item) for item in _require_list(data.get("source_refs"), label="source_refs")]
        artifact_payload = _require_dict(data.get("artifact_payload"), label="artifact_payload")
        return cls(
            target_section=_require_str(data.get("target_section"), label="turn_output_envelope.target_section"),
            artifact_payload=artifact_payload,
            source_refs=source_refs,
            runtime_meta=RuntimeMeta.model_validate(data.get("runtime_meta")),
        )

    def model_dump(self) -> dict[str, Any]:
        return {
            "target_section": self.target_section,
            "artifact_payload": dict(self.artifact_payload),
            "source_refs": [ref.model_dump() for ref in self.source_refs],
            "runtime_meta": self.runtime_meta.model_dump(),
        }


@dataclass(slots=True)
class DomainFramingSection:
    problem_statement: str
    research_goal: str
    assumptions: list[str]
    uncertainties: list[str]
    failure_modes: list[str]

    @classmethod
    def model_validate(cls, payload: Any) -> "DomainFramingSection":
        data = _require_dict(payload, label="domain_framing")
        _reject_extra_keys(
            data,
            label="domain_framing",
            allowed_keys={"problem_statement", "research_goal", "assumptions", "uncertainties", "failure_modes"},
        )
        return cls(
            problem_statement=_require_str(data.get("problem_statement"), label="domain_framing.problem_statement"),
            research_goal=_require_str(data.get("research_goal"), label="domain_framing.research_goal"),
            assumptions=_require_str_list(data.get("assumptions"), label="domain_framing.assumptions"),
            uncertainties=_require_str_list(data.get("uncertainties"), label="domain_framing.uncertainties"),
            failure_modes=_require_str_list(data.get("failure_modes"), label="domain_framing.failure_modes"),
        )

    def model_dump(self) -> dict[str, Any]:
        return {
            "problem_statement": self.problem_statement,
            "research_goal": self.research_goal,
            "assumptions": list(self.assumptions),
            "uncertainties": list(self.uncertainties),
            "failure_modes": list(self.failure_modes),
        }


@dataclass(slots=True)
class QuestionDecompositionSection:
    primary_question: str
    subquestions: list[str]
    assumptions: list[str]
    uncertainties: list[str]
    failure_modes: list[str]

    @classmethod
    def model_validate(cls, payload: Any) -> "QuestionDecompositionSection":
        data = _require_dict(payload, label="question_decomposition")
        _reject_extra_keys(
            data,
            label="question_decomposition",
            allowed_keys={"primary_question", "subquestions", "assumptions", "uncertainties", "failure_modes"},
        )
        return cls(
            primary_question=_require_str(data.get("primary_question"), label="question_decomposition.primary_question"),
            subquestions=_require_str_list(data.get("subquestions"), label="question_decomposition.subquestions"),
            assumptions=_require_str_list(data.get("assumptions"), label="question_decomposition.assumptions"),
            uncertainties=_require_str_list(data.get("uncertainties"), label="question_decomposition.uncertainties"),
            failure_modes=_require_str_list(data.get("failure_modes"), label="question_decomposition.failure_modes"),
        )

    def model_dump(self) -> dict[str, Any]:
        return {
            "primary_question": self.primary_question,
            "subquestions": list(self.subquestions),
            "assumptions": list(self.assumptions),
            "uncertainties": list(self.uncertainties),
            "failure_modes": list(self.failure_modes),
        }


@dataclass(slots=True)
class ArchitectureDataflowSection:
    current_pipeline_summary: str
    stages: list[str]
    crossmodal_dependencies: list[str]
    assumptions: list[str]
    uncertainties: list[str]
    failure_modes: list[str]

    @classmethod
    def model_validate(cls, payload: Any) -> "ArchitectureDataflowSection":
        data = _require_dict(payload, label="architecture_dataflow")
        _reject_extra_keys(
            data,
            label="architecture_dataflow",
            allowed_keys={
                "current_pipeline_summary",
                "stages",
                "crossmodal_dependencies",
                "assumptions",
                "uncertainties",
                "failure_modes",
            },
        )
        return cls(
            current_pipeline_summary=_require_str(
                data.get("current_pipeline_summary"),
                label="architecture_dataflow.current_pipeline_summary",
            ),
            stages=_require_str_list(data.get("stages"), label="architecture_dataflow.stages"),
            crossmodal_dependencies=_require_str_list(
                data.get("crossmodal_dependencies"),
                label="architecture_dataflow.crossmodal_dependencies",
            ),
            assumptions=_require_str_list(data.get("assumptions"), label="architecture_dataflow.assumptions"),
            uncertainties=_require_str_list(data.get("uncertainties"), label="architecture_dataflow.uncertainties"),
            failure_modes=_require_str_list(data.get("failure_modes"), label="architecture_dataflow.failure_modes"),
        )

    def model_dump(self) -> dict[str, Any]:
        return {
            "current_pipeline_summary": self.current_pipeline_summary,
            "stages": list(self.stages),
            "crossmodal_dependencies": list(self.crossmodal_dependencies),
            "assumptions": list(self.assumptions),
            "uncertainties": list(self.uncertainties),
            "failure_modes": list(self.failure_modes),
        }


SECTION_MODELS = {
    "domain_framing": DomainFramingSection,
    "question_decomposition": QuestionDecompositionSection,
    "architecture_dataflow": ArchitectureDataflowSection,
}

SECTION_JSON_SCHEMAS: dict[str, dict[str, Any]] = {
    "domain_framing": {
        "type": "object",
        "additionalProperties": False,
        "required": ["problem_statement", "research_goal", "assumptions", "uncertainties", "failure_modes"],
        "properties": {
            "problem_statement": {"type": "string"},
            "research_goal": {"type": "string"},
            "assumptions": {"type": "array", "items": {"type": "string"}},
            "uncertainties": {"type": "array", "items": {"type": "string"}},
            "failure_modes": {"type": "array", "items": {"type": "string"}},
        },
    },
    "question_decomposition": {
        "type": "object",
        "additionalProperties": False,
        "required": ["primary_question", "subquestions", "assumptions", "uncertainties", "failure_modes"],
        "properties": {
            "primary_question": {"type": "string"},
            "subquestions": {"type": "array", "items": {"type": "string"}},
            "assumptions": {"type": "array", "items": {"type": "string"}},
            "uncertainties": {"type": "array", "items": {"type": "string"}},
            "failure_modes": {"type": "array", "items": {"type": "string"}},
        },
    },
    "architecture_dataflow": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "current_pipeline_summary",
            "stages",
            "crossmodal_dependencies",
            "assumptions",
            "uncertainties",
            "failure_modes",
        ],
        "properties": {
            "current_pipeline_summary": {"type": "string"},
            "stages": {"type": "array", "items": {"type": "string"}},
            "crossmodal_dependencies": {"type": "array", "items": {"type": "string"}},
            "assumptions": {"type": "array", "items": {"type": "string"}},
            "uncertainties": {"type": "array", "items": {"type": "string"}},
            "failure_modes": {"type": "array", "items": {"type": "string"}},
        },
    },
}


def validate_section_payload(section_name: str, payload: Any) -> dict[str, Any]:
    model_cls = SECTION_MODELS.get(section_name)
    if model_cls is None:
        raise ResearchRuntimeValidationError(f"unsupported section: {section_name}")
    return model_cls.model_validate(payload).model_dump()


def turn_output_envelope_json_schema(section_name: str) -> dict[str, Any]:
    section_schema = SECTION_JSON_SCHEMAS.get(section_name)
    if section_schema is None:
        raise ResearchRuntimeValidationError(f"unsupported section: {section_name}")
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["target_section", "artifact_payload", "source_refs", "runtime_meta"],
        "properties": {
            "target_section": {"type": "string", "const": section_name},
            "artifact_payload": section_schema,
            "source_refs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["source_type", "locator", "citation", "excerpt"],
                    "properties": {
                        "source_type": {"type": "string"},
                        "locator": {"type": "string"},
                        "citation": {"type": "string"},
                        "excerpt": {"type": ["string", "null"]},
                    },
                },
            },
            "runtime_meta": {
                "type": "object",
                "additionalProperties": False,
                "required": ["adapter_name", "attempt_index", "corrective_retry", "notes"],
                "properties": {
                    "adapter_name": {"type": "string"},
                    "attempt_index": {"type": "integer"},
                    "corrective_retry": {"type": "boolean"},
                    "notes": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
    }


@dataclass(slots=True)
class CheckpointRecord:
    turn_index: int
    artifact_section_keys_completed: list[str]
    sections_snapshot: dict[str, Any]
    runtime_summary: dict[str, Any]

    def model_dump(self) -> dict[str, Any]:
        return {
            "turn_index": self.turn_index,
            "artifact_section_keys_completed": list(self.artifact_section_keys_completed),
            "sections_snapshot": self.sections_snapshot,
            "runtime_summary": self.runtime_summary,
        }


@dataclass(slots=True)
class QuarantineRecord:
    turn_index: int
    target_section: str
    failed_payload: Any
    validation_error: str
    evidence_refs: list[dict[str, Any]]
    attempt_count: int

    def model_dump(self) -> dict[str, Any]:
        return {
            "turn_index": self.turn_index,
            "target_section": self.target_section,
            "failed_payload": self.failed_payload,
            "validation_error": self.validation_error,
            "evidence_refs": list(self.evidence_refs),
            "attempt_count": self.attempt_count,
        }


@dataclass(slots=True)
class TurnTraceRecord:
    turn_index: int
    target_section: str
    attempt_index: int
    adapter_name: str
    corrective_retry: bool
    request_payload: dict[str, Any]
    raw_response: Any
    parsed_output: Any
    validation_error: str | None
    committed: bool = False
    quarantined: bool = False

    def model_dump(self) -> dict[str, Any]:
        return {
            "turn_index": self.turn_index,
            "target_section": self.target_section,
            "attempt_index": self.attempt_index,
            "adapter_name": self.adapter_name,
            "corrective_retry": self.corrective_retry,
            "request_payload": self.request_payload,
            "raw_response": self.raw_response,
            "parsed_output": self.parsed_output,
            "validation_error": self.validation_error,
            "committed": self.committed,
            "quarantined": self.quarantined,
        }
