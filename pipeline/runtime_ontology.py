from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.simple_yaml import load_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ONTOLOGY_PATH = REPO_ROOT / "starter_assets" / "runtime_signal_event_ontology.yaml"


class RuntimeOntologyError(ValueError):
    pass


@dataclass(frozen=True)
class RuntimeOntology:
    schema_version: str
    signal_types: frozenset[str]
    event_types: frozenset[str]
    semantic_target_fields: frozenset[str]
    producer_families: frozenset[str]
    group_by_fields: frozenset[str]
    signal_event_targets: dict[str, dict[str, tuple[str, ...]]]


def load_runtime_signal_event_ontology(*, repo_root: Path | None = None) -> RuntimeOntology:
    preferred_path = (repo_root / "starter_assets" / "runtime_signal_event_ontology.yaml") if repo_root is not None else DEFAULT_ONTOLOGY_PATH
    ontology_path = preferred_path if preferred_path.exists() else DEFAULT_ONTOLOGY_PATH
    payload = load_yaml_file(ontology_path)
    if not isinstance(payload, dict):
        raise RuntimeOntologyError("runtime signal-event ontology must be a mapping")
    schema_version = str(payload.get("schema_version", "")).strip()
    signal_types = _string_set(payload.get("signal_types"), field_name="signal_types")
    event_types = _string_set(payload.get("event_types"), field_name="event_types")
    semantic_target_fields = _string_set(payload.get("semantic_target_fields"), field_name="semantic_target_fields")
    producer_families = _string_set(payload.get("producer_families"), field_name="producer_families")
    group_by_fields = _string_set(payload.get("group_by_fields"), field_name="group_by_fields")
    raw_targets = payload.get("signal_event_targets", {})
    if not isinstance(raw_targets, dict):
        raise RuntimeOntologyError("runtime signal-event ontology signal_event_targets must be a mapping")
    signal_event_targets: dict[str, dict[str, tuple[str, ...]]] = {}
    for signal_type, row in raw_targets.items():
        normalized_signal_type = str(signal_type).strip()
        if normalized_signal_type not in signal_types:
            raise RuntimeOntologyError(
                f"runtime signal-event ontology references unknown signal_type '{normalized_signal_type}' in signal_event_targets"
            )
        if not isinstance(row, dict):
            raise RuntimeOntologyError(f"runtime signal-event ontology target row for '{normalized_signal_type}' must be a mapping")
        allowed_event_types = tuple(
            event_type
            for event_type in _string_list(row.get("event_types"), field_name=f"signal_event_targets.{normalized_signal_type}.event_types")
        )
        unknown_event_types = [event_type for event_type in allowed_event_types if event_type not in event_types]
        if unknown_event_types:
            raise RuntimeOntologyError(
                f"runtime signal-event ontology target row for '{normalized_signal_type}' uses unknown event_types {unknown_event_types}"
            )
        allowed_target_fields = tuple(
            target_field
            for target_field in _string_list(row.get("target_fields"), field_name=f"signal_event_targets.{normalized_signal_type}.target_fields")
        )
        unknown_target_fields = [target_field for target_field in allowed_target_fields if target_field not in semantic_target_fields]
        if unknown_target_fields:
            raise RuntimeOntologyError(
                f"runtime signal-event ontology target row for '{normalized_signal_type}' uses unknown target_fields {unknown_target_fields}"
            )
        signal_event_targets[normalized_signal_type] = {
            "event_types": allowed_event_types,
            "target_fields": allowed_target_fields,
        }
    return RuntimeOntology(
        schema_version=schema_version,
        signal_types=frozenset(signal_types),
        event_types=frozenset(event_types),
        semantic_target_fields=frozenset(semantic_target_fields),
        producer_families=frozenset(producer_families),
        group_by_fields=frozenset(group_by_fields),
        signal_event_targets=signal_event_targets,
    )


def validate_runtime_rule_terms(
    ontology: RuntimeOntology,
    *,
    signal_type: str,
    event_type: str,
    target_field: str | None,
    target_value_field: str | None,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    if signal_type not in ontology.signal_types:
        findings.append({"status": "unknown_signal_type", "signal_type": signal_type})
        return findings
    if event_type not in ontology.event_types:
        findings.append({"status": "unknown_event_type", "event_type": event_type})
    if target_field is not None and target_field not in ontology.semantic_target_fields:
        findings.append({"status": "unknown_target_field", "target_field": target_field})
    if target_value_field is not None and target_value_field not in ontology.semantic_target_fields:
        findings.append({"status": "unknown_target_value_field", "target_value_field": target_value_field})
    target_spec = ontology.signal_event_targets.get(signal_type, {})
    allowed_event_types = set(target_spec.get("event_types", ()))
    if allowed_event_types and event_type not in allowed_event_types:
        findings.append(
            {
                "status": "invalid_signal_event_mapping",
                "signal_type": signal_type,
                "event_type": event_type,
            }
        )
    allowed_target_fields = set(target_spec.get("target_fields", ()))
    if target_field is not None and allowed_target_fields and target_field not in allowed_target_fields:
        findings.append(
            {
                "status": "invalid_signal_target_field_mapping",
                "signal_type": signal_type,
                "target_field": target_field,
            }
        )
    return findings


def validate_group_by_fields(ontology: RuntimeOntology, fields: list[str] | tuple[str, ...]) -> list[str]:
    return [field for field in fields if field not in ontology.group_by_fields]


def _string_set(value: Any, *, field_name: str) -> set[str]:
    rows = _string_list(value, field_name=field_name)
    if not rows:
        raise RuntimeOntologyError(f"runtime signal-event ontology {field_name} must be a non-empty list")
    return set(rows)


def _string_list(value: Any, *, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise RuntimeOntologyError(f"runtime signal-event ontology {field_name} must be a list")
    rows = [str(item).strip() for item in value if str(item).strip()]
    return rows
