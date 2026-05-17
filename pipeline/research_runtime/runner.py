from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .adapter import OpenAIResponsesResearchModelAdapter, ResearchModelAdapter, StubResearchModelAdapter
from .dossier import DEFAULT_DOSSIER_TEMPLATE, default_turn_plan
from .graph import build_research_graph
from .state import create_initial_state
from .trace import build_trace_payload, default_trace_output_path


def run_research_runtime(
    *,
    topic: str,
    turn_plan: list[str] | None = None,
    dossier_template: str = DEFAULT_DOSSIER_TEMPLATE,
    max_turns: int | None = None,
    checkpoint_every: int | None = None,
    output_path: str | Path | None = None,
    trace_output_path: str | Path | None = None,
    evidence_bundle: dict[str, Any] | None = None,
    adapter: ResearchModelAdapter | None = None,
) -> dict[str, Any]:
    selected_adapter = adapter or StubResearchModelAdapter()
    selected_turn_plan = list(turn_plan) if turn_plan is not None else default_turn_plan(dossier_template)
    selected_max_turns = max_turns if max_turns is not None else len(selected_turn_plan)
    graph = build_research_graph()
    initial_state = create_initial_state(
        topic=topic,
        turn_plan=selected_turn_plan,
        max_turns=selected_max_turns,
        checkpoint_every=checkpoint_every,
        evidence_bundle=evidence_bundle,
        dossier_template=dossier_template,
        adapter=selected_adapter,
    )
    final_state = graph.invoke(initial_state)
    result = dict(final_state["result"])
    result["adapter_name"] = selected_adapter.adapter_name
    result["dossier_template"] = dossier_template
    trace_payload = build_trace_payload(
        run_id=str(result.get("run_id") or ""),
        topic=topic,
        dossier_template=dossier_template,
        adapter_name=selected_adapter.adapter_name,
        turn_plan=selected_turn_plan,
        max_turns=selected_max_turns,
        trace_records=list(final_state.get("turn_trace_records") or []),
        artifact_output_path=str(output_path) if output_path is not None else None,
    )
    result["trace"] = trace_payload
    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result["artifact"], indent=2), encoding="utf-8")
        result["output_path"] = str(target)
        trace_target = _resolve_path(trace_output_path) if trace_output_path is not None else default_trace_output_path(target)
        trace_target.parent.mkdir(parents=True, exist_ok=True)
        trace_target.write_text(json.dumps(trace_payload, indent=2), encoding="utf-8")
        result["trace_output_path"] = str(trace_target)
    else:
        result["output_path"] = None
        result["trace_output_path"] = None
    return result


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        return (Path.cwd() / resolved).resolve()
    return resolved.resolve()


def build_research_adapter(
    *,
    adapter_name: str,
    model: str | None = None,
    api_key: str | None = None,
) -> ResearchModelAdapter:
    if adapter_name == "stub":
        return StubResearchModelAdapter()
    if adapter_name == "openai":
        return OpenAIResponsesResearchModelAdapter(
            api_key=api_key,
            model=model or "gpt-5",
        )
    raise ValueError(f"unsupported adapter: {adapter_name}")
