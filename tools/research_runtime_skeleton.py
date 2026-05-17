from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.research_runtime import DEFAULT_DOSSIER_TEMPLATE, build_research_adapter, run_research_runtime


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the minimal research runtime skeleton")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--turn-plan", help="JSON array string or path to a JSON array file")
    parser.add_argument("--dossier-template", default=DEFAULT_DOSSIER_TEMPLATE)
    parser.add_argument("--max-turns", type=int)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--trace-output-path")
    parser.add_argument("--checkpoint-every", type=int)
    parser.add_argument("--adapter", choices=["stub", "openai"], default="stub")
    parser.add_argument("--openai-model")
    parser.add_argument("--use-stub-adapter", action="store_true")
    return parser


def _load_turn_plan(raw_value: str) -> list[str]:
    candidate = Path(raw_value).expanduser()
    if candidate.exists():
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    else:
        payload = json.loads(raw_value)
    if not isinstance(payload, list) or not all(isinstance(item, str) for item in payload):
        raise ValueError("turn plan must be a JSON list of section strings")
    return [item.strip() for item in payload]


def _compact_summary(result: dict[str, Any]) -> dict[str, Any]:
    artifact = result.get("artifact") if isinstance(result.get("artifact"), dict) else {}
    return {
        "ok": result.get("ok"),
        "status": result.get("status"),
        "run_id": result.get("run_id"),
        "dossier_template": result.get("dossier_template"),
        "output_path": result.get("output_path"),
        "trace_output_path": result.get("trace_output_path"),
        "completed_turn_count": result.get("completed_turn_count"),
        "completed_sections": sorted((artifact.get("sections") or {}).keys()) if isinstance(artifact.get("sections"), dict) else [],
        "checkpoint_count": result.get("checkpoint_count"),
        "quarantine_count": result.get("quarantine_count"),
        "turn_trace_count": result.get("turn_trace_count"),
        "adapter_name": result.get("adapter_name"),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    turn_plan = _load_turn_plan(args.turn_plan) if args.turn_plan else None
    adapter_name = "stub" if args.use_stub_adapter else args.adapter
    adapter = build_research_adapter(adapter_name=adapter_name, model=args.openai_model)
    result = run_research_runtime(
        topic=args.topic,
        turn_plan=turn_plan,
        dossier_template=args.dossier_template,
        max_turns=args.max_turns,
        checkpoint_every=args.checkpoint_every,
        output_path=args.output_path,
        trace_output_path=args.trace_output_path,
        adapter=adapter,
    )
    print(json.dumps(_compact_summary(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
