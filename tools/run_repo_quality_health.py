from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_quality_maintenance_findings import inspect_quality_maintenance_findings
from tools.run_decision_regression_goldsets import run_decision_regression_goldsets


def run_repo_quality_health(*, emit_json: bool = False) -> dict[str, Any]:
    maintenance_result = inspect_quality_maintenance_findings(repo_root=REPO_ROOT, emit_json=False)
    regression_result = run_decision_regression_goldsets(emit_json=False)

    maintenance_payload = maintenance_result["inspection_payload"]
    regression_payload = regression_result["result_payload"]
    severity_counts = maintenance_payload.get("severity_counts", {}) if isinstance(maintenance_payload.get("severity_counts"), dict) else {}
    blocking_count = int(severity_counts.get("blocking", 0) or 0)
    warning_count = int(severity_counts.get("warning", 0) or 0)
    maintenance_ok = blocking_count == 0 and warning_count == 0
    overall_ok = bool(regression_result.get("ok")) and maintenance_ok

    payload = {
        "repo_root": str(REPO_ROOT),
        "ok": overall_ok,
        "maintenance_ok": maintenance_ok,
        "decision_regression_ok": bool(regression_result.get("ok")),
        "maintenance_summary": {
            "total_findings": maintenance_payload.get("total_findings"),
            "severity_counts": severity_counts,
        },
        "decision_regression_summary": {
            "suite_count": regression_payload.get("suite_count"),
            "total_tests": regression_payload.get("total_tests"),
            "failure_count": regression_payload.get("failure_count"),
            "error_count": regression_payload.get("error_count"),
            "skipped_count": regression_payload.get("skipped_count"),
        },
        "maintenance_findings": maintenance_payload.get("quality_maintenance_findings", []),
        "decision_regression_suites": regression_payload.get("suite_rows", []),
    }
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(payload)
    return {
        "ok": overall_ok,
        "status": "ok" if overall_ok else "repo_quality_failures",
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "result_payload": payload,
    }


def _render_compact_text(payload: dict[str, Any]) -> str:
    maintenance_summary = payload.get("maintenance_summary", {}) if isinstance(payload.get("maintenance_summary"), dict) else {}
    severity_counts = maintenance_summary.get("severity_counts", {}) if isinstance(maintenance_summary.get("severity_counts"), dict) else {}
    regression_summary = payload.get("decision_regression_summary", {}) if isinstance(payload.get("decision_regression_summary"), dict) else {}
    lines = [
        f"Repo root: {payload.get('repo_root')}",
        f"Ok: {payload.get('ok')}",
        f"Maintenance ok: {payload.get('maintenance_ok')}",
        f"Decision regression ok: {payload.get('decision_regression_ok')}",
        "",
        "Maintenance",
        f"Total findings: {maintenance_summary.get('total_findings')}",
        f"Blocking count: {severity_counts.get('blocking', 0)}",
        f"Warning count: {severity_counts.get('warning', 0)}",
        f"Informational count: {severity_counts.get('informational', 0)}",
        "",
        "Decision Regression",
        f"Suite count: {regression_summary.get('suite_count')}",
        f"Total tests: {regression_summary.get('total_tests')}",
        f"Failure count: {regression_summary.get('failure_count')}",
        f"Error count: {regression_summary.get('error_count')}",
        f"Skipped count: {regression_summary.get('skipped_count')}",
    ]
    return "\n".join(lines)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run combined repo quality health checks")
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_repo_quality_health(emit_json=bool(args.emit_json))
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
