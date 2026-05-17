from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .adapter import ResearchModelAdapter
from .dossier import DEFAULT_DOSSIER_TEMPLATE, default_turn_plan
from .evaluator import evaluate_research_runtime_artifacts
from .runner import run_research_runtime

SCHEMA_VERSION = "research_runtime_feedback_loop_report_v1"
ESCALATION_RULES = {
    "source_ref_quality": "Strengthen citation specificity and locator quality; avoid vague placeholders.",
    "acceptance_criteria_coverage": "Satisfy every dossier acceptance criterion explicitly before closing the section.",
    "prompt_bundle_alignment": "Stay aligned to the assigned dossier turn lens and acceptance criteria exactly.",
    "speculation_markers": "Remove speculative language and restate claims as bounded findings or explicit uncertainties.",
    "trace_rows_have_outcomes": "Return a terminal outcome for every turn; do not leave unresolved attempts.",
    "assumptions_present": "Populate assumptions explicitly and keep them concrete.",
    "uncertainties_present": "Populate uncertainties explicitly and keep them concrete.",
    "failure_modes_present": "Populate failure modes explicitly and keep them concrete.",
    "source_refs_present": "Include source references for every evidence-backed section.",
}


def run_research_runtime_feedback_loop(
    *,
    topic: str,
    output_dir: str | Path,
    adapter: ResearchModelAdapter,
    turn_plan: list[str] | None = None,
    dossier_template: str = DEFAULT_DOSSIER_TEMPLATE,
    max_turns: int | None = None,
    checkpoint_every: int | None = None,
    max_feedback_loops: int = 1,
) -> dict[str, Any]:
    target_dir = _resolve_path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    selected_turn_plan = list(turn_plan) if turn_plan is not None else default_turn_plan(dossier_template)
    selected_max_turns = max_turns if max_turns is not None else len(selected_turn_plan)
    iterations: list[dict[str, Any]] = []
    evidence_bundle: dict[str, Any] | None = None
    loop_limit = max(0, int(max_feedback_loops))
    total_iterations = loop_limit + 1
    final_status = "fail"
    stopped_reason = "max_feedback_loops_reached"
    latest_artifact_path = None
    latest_trace_path = None
    latest_evaluation_path = None

    for iteration_index in range(1, total_iterations + 1):
        artifact_path = target_dir / f"iteration_{iteration_index}.artifact.json"
        trace_path = target_dir / f"iteration_{iteration_index}.trace.json"
        evaluation_path = target_dir / f"iteration_{iteration_index}.evaluation.json"
        runtime_result = run_research_runtime(
            topic=topic,
            turn_plan=selected_turn_plan,
            dossier_template=dossier_template,
            max_turns=selected_max_turns,
            checkpoint_every=checkpoint_every,
            output_path=artifact_path,
            trace_output_path=trace_path,
            evidence_bundle=evidence_bundle,
            adapter=adapter,
        )
        evaluation = evaluate_research_runtime_artifacts(
            artifact_path=artifact_path,
            trace_path=runtime_result["trace_output_path"],
        )
        evaluation_path.write_text(json.dumps(evaluation, indent=2), encoding="utf-8")
        iteration_row = {
            "iteration_index": iteration_index,
            "artifact_path": str(artifact_path),
            "trace_path": str(runtime_result["trace_output_path"]),
            "evaluation_path": str(evaluation_path),
            "overall_status": evaluation["overall_status"],
            "pass_count": evaluation["summary"]["pass_count"],
            "warn_count": evaluation["summary"]["warn_count"],
            "fail_count": evaluation["summary"]["fail_count"],
        }
        iterations.append(iteration_row)
        latest_artifact_path = str(artifact_path)
        latest_trace_path = str(runtime_result["trace_output_path"])
        latest_evaluation_path = str(evaluation_path)
        final_status = str(evaluation["overall_status"])
        if final_status == "pass":
            stopped_reason = "pass"
            break
        if iteration_index > loop_limit:
            stopped_reason = "max_feedback_loops_reached"
            break
        failing_checks = [
            {
                "name": check.get("name"),
                "status": check.get("status"),
                "detail": check.get("detail"),
                "escalation_instruction": ESCALATION_RULES.get(str(check.get("name") or "")),
            }
            for check in evaluation.get("checks", [])
            if isinstance(check, dict) and check.get("status") in {"warn", "fail"}
        ]
        evidence_bundle = {
            "evaluator_feedback": {
                "previous_iteration": iteration_index,
                "overall_status": evaluation["overall_status"],
                "checks": failing_checks,
                "escalation_instructions": [
                    check["escalation_instruction"]
                    for check in failing_checks
                    if isinstance(check.get("escalation_instruction"), str) and str(check.get("escalation_instruction")).strip()
                ],
                "prompt_appendix": _build_prompt_appendix(failing_checks),
                "source_refs": [
                    {
                        "source_type": "research_runtime_evaluation",
                        "locator": str(evaluation_path),
                        "citation": f"Research runtime evaluation iteration {iteration_index}",
                        "excerpt": None,
                    }
                ],
            }
        }

    report = {
        "schema_version": SCHEMA_VERSION,
        "topic": topic,
        "dossier_template": dossier_template,
        "adapter_name": adapter.adapter_name,
        "max_feedback_loops": loop_limit,
        "iteration_count": len(iterations),
        "final_overall_status": final_status,
        "stopped_reason": stopped_reason,
        "final_artifact_path": latest_artifact_path,
        "final_trace_path": latest_trace_path,
        "final_evaluation_path": latest_evaluation_path,
        "iterations": iterations,
    }
    report_path = target_dir / "feedback_loop_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "report_path": str(report_path),
        "iteration_count": len(iterations),
        "final_overall_status": final_status,
        "stopped_reason": stopped_reason,
        "final_artifact_path": latest_artifact_path,
        "final_trace_path": latest_trace_path,
        "final_evaluation_path": latest_evaluation_path,
        "iterations": iterations,
    }


def _build_prompt_appendix(failing_checks: list[dict[str, Any]]) -> str:
    instructions = [
        str(check.get("escalation_instruction") or "").strip()
        for check in failing_checks
        if str(check.get("escalation_instruction") or "").strip()
    ]
    if not instructions:
        return ""
    unique_instructions = list(dict.fromkeys(instructions))
    lines = ["Evaluator escalation instructions:"]
    lines.extend(f"- {instruction}" for instruction in unique_instructions)
    return "\n".join(lines)


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        return (Path.cwd() / candidate).resolve()
    return candidate.resolve()
