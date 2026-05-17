from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from .protocol import DEFAULT_RETRY_LIMIT, NO_CROSS_SECTION_WRITES, describe_section_schema, supported_sections
from .prompt_assets import build_prompt_bundle
from .schemas import (
    CheckpointRecord,
    QuarantineRecord,
    ResearchRuntimeValidationError,
    TurnTraceRecord,
    TurnOutputEnvelope,
    validate_section_payload,
)
from .state import ResearchRuntimeState

try:
    from langgraph.graph import END, StateGraph  # type: ignore
except Exception:  # pragma: no cover - exercised indirectly by fallback tests
    END = "__end__"
    StateGraph = None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def initialize_run(state: ResearchRuntimeState) -> ResearchRuntimeState:
    topic = str(state.get("topic") or "").strip()
    if not topic:
        raise ValueError("topic must be non-empty")
    turn_plan = list(state.get("turn_plan") or [])
    if not turn_plan:
        raise ValueError("turn_plan must not be empty")
    max_turns = int(state.get("max_turns") or 0)
    if max_turns <= 0:
        raise ValueError("max_turns must be positive")
    unsupported = [section for section in turn_plan if section not in supported_sections()]
    if unsupported:
        raise ValueError(f"turn_plan contains unsupported sections: {', '.join(unsupported)}")
    state["allowed_sections"] = list(dict.fromkeys(turn_plan))
    state["current_status"] = "initialized"
    return state


def assign_turn(state: ResearchRuntimeState) -> ResearchRuntimeState:
    turn_index = int(state.get("turn_index") or 0)
    turn_plan = list(state.get("turn_plan") or [])
    max_turns = int(state.get("max_turns") or 0)
    if turn_index >= min(len(turn_plan), max_turns):
        state["pending_section"] = None
        state["current_status"] = "complete"
        return state
    state["pending_section"] = turn_plan[turn_index]
    state["current_attempt"] = 0
    state["current_raw_output"] = None
    state["current_validated_output"] = None
    state["current_validation_error"] = None
    state["prepared_turn_input"] = {}
    state["current_status"] = "turn_assigned"
    return state


def prepare_inputs(state: ResearchRuntimeState) -> ResearchRuntimeState:
    pending_section = str(state.get("pending_section") or "")
    checkpoint_history = list(state.get("checkpoint_history") or [])
    checkpoint_summary = checkpoint_history[-1].model_dump() if checkpoint_history else None
    current_attempt = int(state.get("current_attempt") or 0)
    dossier = dict(state.get("dossier") or {})
    evidence_bundle = deepcopy(state.get("evidence_bundle") or {})
    prompt_bundle = build_prompt_bundle(
        section_name=pending_section,
        dossier_template=str(dossier.get("dossier_template") or ""),
        corrective_retry=current_attempt > 0,
    )
    evaluator_feedback = evidence_bundle.get("evaluator_feedback") if isinstance(evidence_bundle, dict) else None
    if isinstance(evaluator_feedback, dict):
        prompt_appendix = str(evaluator_feedback.get("prompt_appendix") or "").strip()
        escalation_instructions = evaluator_feedback.get("escalation_instructions")
        if prompt_appendix:
            prompt_bundle["evaluator_escalation_prompt"] = prompt_appendix
        if isinstance(escalation_instructions, list):
            prompt_bundle["evaluator_escalation_instructions"] = [str(item).strip() for item in escalation_instructions if str(item).strip()]
    state["prepared_turn_input"] = {
        "protocol_lock": state.get("protocol_lock"),
        "canonical_brief": state.get("topic"),
        "allowed_sections": list(state.get("allowed_sections") or []),
        "checkpoint_summary": checkpoint_summary,
        "target_schema": describe_section_schema(pending_section),
        "evidence_bundle": evidence_bundle,
        "turn_lens": pending_section,
        "target_section": pending_section,
        "attempt_index": current_attempt + 1,
        "corrective_retry": current_attempt > 0,
        "dossier": dossier,
        "prompt_bundle": prompt_bundle,
    }
    state["current_status"] = "inputs_prepared"
    return state


def execute_turn(state: ResearchRuntimeState) -> ResearchRuntimeState:
    adapter = state.get("adapter")
    prepared_turn_input = dict(state.get("prepared_turn_input") or {})
    adapter_result = adapter.generate_turn(prepared_turn_input)
    state["current_raw_output"] = adapter_result.output_payload
    trace_record = TurnTraceRecord(
        turn_index=int(state.get("turn_index") or 0) + 1,
        target_section=str(prepared_turn_input.get("target_section") or ""),
        attempt_index=int(prepared_turn_input.get("attempt_index") or 1),
        adapter_name=str(adapter_result.adapter_name),
        corrective_retry=bool(prepared_turn_input.get("corrective_retry")),
        request_payload=deepcopy(adapter_result.request_payload),
        raw_response=deepcopy(adapter_result.raw_response),
        parsed_output=deepcopy(adapter_result.output_payload),
        validation_error=None,
    )
    state["current_trace_record"] = trace_record
    state["current_status"] = "turn_executed"
    return state


def validate_turn(state: ResearchRuntimeState) -> ResearchRuntimeState:
    pending_section = str(state.get("pending_section") or "")
    raw_output = state.get("current_raw_output")
    try:
        envelope = TurnOutputEnvelope.model_validate(raw_output)
        if NO_CROSS_SECTION_WRITES and envelope.target_section != pending_section:
            raise ResearchRuntimeValidationError("target_section does not match assigned section")
        validate_section_payload(pending_section, envelope.artifact_payload)
        state["current_validated_output"] = envelope
        state["current_validation_error"] = None
        trace_record = state.get("current_trace_record")
        if trace_record is not None:
            trace_record.validation_error = None
        state["current_status"] = "turn_validated"
        return state
    except Exception as exc:
        state["current_validated_output"] = None
        state["current_validation_error"] = str(exc)
        trace_record = state.get("current_trace_record")
        if trace_record is not None:
            trace_record.validation_error = str(exc)
        state["current_status"] = "turn_invalid"
        return state


def retry_or_quarantine(state: ResearchRuntimeState) -> ResearchRuntimeState:
    validation_error = state.get("current_validation_error")
    if not validation_error:
        return state
    current_attempt = int(state.get("current_attempt") or 0)
    trace_record = state.get("current_trace_record")
    turn_trace_records = list(state.get("turn_trace_records") or [])
    if trace_record is not None:
        if current_attempt < DEFAULT_RETRY_LIMIT:
            trace_record.quarantined = False
        else:
            trace_record.quarantined = True
        turn_trace_records.append(trace_record)
        state["turn_trace_records"] = turn_trace_records
        state["current_trace_record"] = None
    if current_attempt < DEFAULT_RETRY_LIMIT:
        state["current_attempt"] = current_attempt + 1
        state["current_status"] = "retry_requested"
        return state
    pending_section = str(state.get("pending_section") or "")
    quarantine = QuarantineRecord(
        turn_index=int(state.get("turn_index") or 0) + 1,
        target_section=pending_section,
        failed_payload=deepcopy(state.get("current_raw_output")),
        validation_error=str(validation_error),
        evidence_refs=deepcopy((state.get("evidence_bundle") or {}).get("source_refs", [])),
        attempt_count=current_attempt + 1,
    )
    quarantine_records = list(state.get("quarantine_records") or [])
    quarantine_records.append(quarantine)
    state["quarantine_records"] = quarantine_records
    state["turn_index"] = int(state.get("turn_index") or 0) + 1
    state["pending_section"] = None
    state["current_status"] = "quarantined"
    return state


def commit_section(state: ResearchRuntimeState) -> ResearchRuntimeState:
    envelope = state.get("current_validated_output")
    if envelope is None:
        raise ValueError("cannot commit section without a validated envelope")
    pending_section = str(state.get("pending_section") or "")
    turn_index = int(state.get("turn_index") or 0) + 1
    artifact = state["artifact"]
    artifact.apply_section_update(assigned_section=pending_section, envelope=envelope, turn_index=turn_index)
    trace_record = state.get("current_trace_record")
    if trace_record is not None:
        trace_record.committed = True
        turn_trace_records = list(state.get("turn_trace_records") or [])
        turn_trace_records.append(trace_record)
        state["turn_trace_records"] = turn_trace_records
        state["current_trace_record"] = None
    state["turn_index"] = turn_index
    state["pending_section"] = None
    state["current_status"] = "section_committed"
    return state


def checkpoint_if_needed(state: ResearchRuntimeState) -> ResearchRuntimeState:
    turn_index = int(state.get("turn_index") or 0)
    checkpoint_turn_set = set(state.get("checkpoint_turns") or ())
    checkpoint_history = list(state.get("checkpoint_history") or [])
    if turn_index in checkpoint_turn_set and all(record.turn_index != turn_index for record in checkpoint_history):
        artifact = state["artifact"]
        checkpoint = CheckpointRecord(
            turn_index=turn_index,
            artifact_section_keys_completed=artifact.section_keys(),
            sections_snapshot=artifact.sections_snapshot(),
            runtime_summary={
                "generated_at": _utc_now(),
                "quarantine_count": len(state.get("quarantine_records") or []),
                "completed_sections": artifact.section_keys(),
            },
        )
        checkpoint_history.append(checkpoint)
        state["checkpoint_history"] = checkpoint_history
    state["current_status"] = "checkpointed"
    return state


def finalize_run(state: ResearchRuntimeState) -> ResearchRuntimeState:
    artifact = state["artifact"]
    checkpoints = list(state.get("checkpoint_history") or [])
    quarantines = list(state.get("quarantine_records") or [])
    turn_traces = list(state.get("turn_trace_records") or [])
    result = {
        "ok": True,
        "status": "ok",
        "run_id": state.get("run_id"),
        "topic": state.get("topic"),
        "completed_turn_count": int(state.get("turn_index") or 0),
        "artifact": artifact.model_dump(
            dossier=dict(state.get("dossier") or {}),
            max_turns=int(state.get("max_turns") or 0),
            turn_plan=list(state.get("turn_plan") or []),
            checkpoints=checkpoints,
            quarantine_records=quarantines,
        ),
        "checkpoint_count": len(checkpoints),
        "quarantine_count": len(quarantines),
        "turn_trace_count": len(turn_traces),
        "turn_traces": [record.model_dump() for record in turn_traces],
    }
    state["result"] = result
    state["current_status"] = "finalized"
    return state


class _FallbackResearchGraph:
    def invoke(self, initial_state: ResearchRuntimeState) -> ResearchRuntimeState:
        state = initial_state
        state = initialize_run(state)
        while True:
            state = assign_turn(state)
            if state.get("current_status") == "complete":
                break
            while True:
                state = prepare_inputs(state)
                state = execute_turn(state)
                state = validate_turn(state)
                if state.get("current_validation_error"):
                    state = retry_or_quarantine(state)
                    if state.get("current_status") == "retry_requested":
                        continue
                else:
                    state = commit_section(state)
                break
            state = checkpoint_if_needed(state)
        state = finalize_run(state)
        return state


def _build_langgraph() -> Any:
    graph = StateGraph(ResearchRuntimeState)
    graph.add_node("initialize_run", initialize_run)
    graph.add_node("assign_turn", assign_turn)
    graph.add_node("prepare_inputs", prepare_inputs)
    graph.add_node("execute_turn", execute_turn)
    graph.add_node("validate_turn", validate_turn)
    graph.add_node("retry_or_quarantine", retry_or_quarantine)
    graph.add_node("commit_section", commit_section)
    graph.add_node("checkpoint_if_needed", checkpoint_if_needed)
    graph.add_node("finalize_run", finalize_run)
    graph.set_entry_point("initialize_run")
    graph.add_edge("initialize_run", "assign_turn")
    graph.add_conditional_edges(
        "assign_turn",
        lambda state: "finalize_run" if state.get("current_status") == "complete" else "prepare_inputs",
        {"finalize_run": "finalize_run", "prepare_inputs": "prepare_inputs"},
    )
    graph.add_edge("prepare_inputs", "execute_turn")
    graph.add_edge("execute_turn", "validate_turn")
    graph.add_conditional_edges(
        "validate_turn",
        lambda state: "retry_or_quarantine" if state.get("current_validation_error") else "commit_section",
        {"retry_or_quarantine": "retry_or_quarantine", "commit_section": "commit_section"},
    )
    graph.add_conditional_edges(
        "retry_or_quarantine",
        lambda state: "prepare_inputs" if state.get("current_status") == "retry_requested" else "checkpoint_if_needed",
        {"prepare_inputs": "prepare_inputs", "checkpoint_if_needed": "checkpoint_if_needed"},
    )
    graph.add_edge("commit_section", "checkpoint_if_needed")
    graph.add_edge("checkpoint_if_needed", "assign_turn")
    graph.add_edge("finalize_run", END)
    return graph.compile()


def build_research_graph() -> Any:
    if StateGraph is None:
        return _FallbackResearchGraph()
    return _build_langgraph()
