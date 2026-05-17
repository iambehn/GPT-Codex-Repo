from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .schemas import CheckpointRecord, QuarantineRecord, TurnOutputEnvelope, validate_section_payload


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class SectionCommitRecord:
    section_name: str
    payload: dict[str, Any]
    source_refs: list[dict[str, Any]]
    runtime_meta: dict[str, Any]
    committed_at: str
    turn_index: int

    def model_dump(self) -> dict[str, Any]:
        return {
            "section_name": self.section_name,
            "payload": self.payload,
            "source_refs": list(self.source_refs),
            "runtime_meta": self.runtime_meta,
            "committed_at": self.committed_at,
            "turn_index": self.turn_index,
        }


@dataclass(slots=True)
class ResearchArtifact:
    run_id: str
    topic: str
    sections: dict[str, SectionCommitRecord] = field(default_factory=dict)

    def apply_section_update(
        self,
        *,
        assigned_section: str,
        envelope: TurnOutputEnvelope,
        turn_index: int,
    ) -> None:
        if assigned_section != envelope.target_section:
            raise ValueError("assigned section does not match envelope target_section")
        validated_payload = validate_section_payload(assigned_section, envelope.artifact_payload)
        self.sections[assigned_section] = SectionCommitRecord(
            section_name=assigned_section,
            payload=validated_payload,
            source_refs=[ref.model_dump() for ref in envelope.source_refs],
            runtime_meta=envelope.runtime_meta.model_dump(),
            committed_at=_utc_now(),
            turn_index=turn_index,
        )

    def section_keys(self) -> list[str]:
        return list(self.sections.keys())

    def sections_snapshot(self) -> dict[str, Any]:
        return {key: record.model_dump() for key, record in self.sections.items()}

    def model_dump(
        self,
        *,
        dossier: dict[str, Any],
        max_turns: int,
        turn_plan: list[str],
        checkpoints: list[CheckpointRecord],
        quarantine_records: list[QuarantineRecord],
    ) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "topic": self.topic,
            "schema_version": dossier.get("schema_version"),
            "dossier_template": dossier.get("dossier_template"),
            "dossier_title": dossier.get("title"),
            "max_turns": max_turns,
            "turn_plan": list(turn_plan),
            "sections": self.sections_snapshot(),
            "checkpoints": [record.model_dump() for record in checkpoints],
            "quarantine_records": [record.model_dump() for record in quarantine_records],
        }
