from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.research_runtime.evaluator import evaluate_research_runtime_artifacts


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate a research runtime dossier artifact and its trace")
    parser.add_argument("--artifact", required=True)
    parser.add_argument("--trace", required=True)
    return parser


def _compact_summary(result: dict[str, object]) -> dict[str, object]:
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    return {
        "schema_version": result.get("schema_version"),
        "overall_status": result.get("overall_status"),
        "pass_count": summary.get("pass_count"),
        "warn_count": summary.get("warn_count"),
        "fail_count": summary.get("fail_count"),
        "checks": result.get("checks"),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = evaluate_research_runtime_artifacts(
        artifact_path=args.artifact,
        trace_path=args.trace,
    )
    print(json.dumps(_compact_summary(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
