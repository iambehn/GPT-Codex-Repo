from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.research_runtime import DEFAULT_DOSSIER_TEMPLATE, build_research_adapter
from pipeline.research_runtime.feedback_loop import run_research_runtime_feedback_loop


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the research runtime with an evaluator feedback loop")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--turn-plan")
    parser.add_argument("--dossier-template", default=DEFAULT_DOSSIER_TEMPLATE)
    parser.add_argument("--max-turns", type=int)
    parser.add_argument("--checkpoint-every", type=int)
    parser.add_argument("--max-feedback-loops", type=int, default=1)
    parser.add_argument("--adapter", choices=["stub", "openai"], default="stub")
    parser.add_argument("--openai-model")
    parser.add_argument("--use-stub-adapter", action="store_true")
    return parser


def _load_turn_plan(raw_value: str | None) -> list[str] | None:
    if not raw_value:
        return None
    candidate = Path(raw_value).expanduser()
    if candidate.exists():
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    else:
        payload = json.loads(raw_value)
    if not isinstance(payload, list) or not all(isinstance(item, str) for item in payload):
        raise ValueError("turn plan must be a JSON list of section strings")
    return [item.strip() for item in payload]


def _compact_summary(result: dict[str, object]) -> dict[str, object]:
    return {
        "ok": result.get("ok"),
        "status": result.get("status"),
        "report_path": result.get("report_path"),
        "iteration_count": result.get("iteration_count"),
        "final_overall_status": result.get("final_overall_status"),
        "stopped_reason": result.get("stopped_reason"),
        "final_artifact_path": result.get("final_artifact_path"),
        "final_trace_path": result.get("final_trace_path"),
        "final_evaluation_path": result.get("final_evaluation_path"),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    adapter_name = "stub" if args.use_stub_adapter else args.adapter
    adapter = build_research_adapter(adapter_name=adapter_name, model=args.openai_model)
    result = run_research_runtime_feedback_loop(
        topic=args.topic,
        output_dir=args.output_dir,
        adapter=adapter,
        turn_plan=_load_turn_plan(args.turn_plan),
        dossier_template=args.dossier_template,
        max_turns=args.max_turns,
        checkpoint_every=args.checkpoint_every,
        max_feedback_loops=args.max_feedback_loops,
    )
    print(json.dumps(_compact_summary(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
