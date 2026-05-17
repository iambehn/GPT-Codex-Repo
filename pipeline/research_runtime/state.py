from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, TypedDict
from uuid import uuid4

from .artifact import ResearchArtifact
from .dossier import DEFAULT_DOSSIER_TEMPLATE, dossier_metadata
from .protocol import checkpoint_turns, load_protocol_lock


class ResearchRuntimeState(TypedDict, total=False):
    run_id: str
    topic: str
    turn_index: int
    max_turns: int
    allowed_sections: list[str]
    artifact: ResearchArtifact
    pending_section: str | None
    checkpoint_history: list[Any]
    quarantine_records: list[Any]
    turn_trace_records: list[Any]
    evidence_bundle: dict[str, Any]
    runtime_meta: dict[str, Any]
    turn_plan: list[str]
    dossier: dict[str, Any]
    checkpoint_turns: tuple[int, ...]
    protocol_lock: str
    current_attempt: int
    prepared_turn_input: dict[str, Any]
    current_raw_output: Any
    current_validated_output: Any
    current_validation_error: str | None
    current_status: str
    current_trace_record: Any
    result: dict[str, Any]
    adapter: Any


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def create_initial_state(
    *,
    topic: str,
    turn_plan: list[str],
    max_turns: int,
    checkpoint_every: int | None,
    evidence_bundle: dict[str, Any] | None,
    dossier_template: str,
    adapter: Any,
) -> ResearchRuntimeState:
    run_id = f"research_runtime_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    dossier = dossier_metadata(dossier_template)
    return {
        "run_id": run_id,
        "topic": topic,
        "turn_index": 0,
        "max_turns": max_turns,
        "allowed_sections": list(dict.fromkeys(turn_plan)),
        "artifact": ResearchArtifact(run_id=run_id, topic=topic),
        "pending_section": None,
        "checkpoint_history": [],
        "quarantine_records": [],
        "turn_trace_records": [],
        "evidence_bundle": dict(evidence_bundle or {}),
        "runtime_meta": {
            "started_at": _utc_now(),
            "retry_limit": 1,
            "dossier_template": dossier_template,
        },
        "turn_plan": list(turn_plan),
        "dossier": dossier,
        "checkpoint_turns": checkpoint_turns(max_turns=max_turns, checkpoint_every=checkpoint_every),
        "protocol_lock": load_protocol_lock(),
        "current_attempt": 0,
        "prepared_turn_input": {},
        "current_raw_output": None,
        "current_validated_output": None,
        "current_validation_error": None,
        "current_status": "pending",
        "current_trace_record": None,
        "adapter": adapter,
    }
