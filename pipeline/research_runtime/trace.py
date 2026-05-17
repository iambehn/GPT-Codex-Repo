from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .schemas import TurnTraceRecord

TRACE_SCHEMA_VERSION = "research_runtime_turn_trace_v1"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def build_trace_payload(
    *,
    run_id: str,
    topic: str,
    dossier_template: str,
    adapter_name: str,
    turn_plan: list[str],
    max_turns: int,
    trace_records: list[TurnTraceRecord],
    artifact_output_path: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": TRACE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "run_id": run_id,
        "topic": topic,
        "dossier_template": dossier_template,
        "adapter_name": adapter_name,
        "turn_plan": list(turn_plan),
        "max_turns": max_turns,
        "artifact_output_path": artifact_output_path,
        "row_count": len(trace_records),
        "rows": [record.model_dump() for record in trace_records],
    }


def default_trace_output_path(artifact_output_path: str | Path) -> Path:
    resolved = Path(artifact_output_path)
    if resolved.suffix:
        return resolved.with_name(f"{resolved.stem}.trace{resolved.suffix}")
    return resolved.with_name(f"{resolved.name}.trace.json")
