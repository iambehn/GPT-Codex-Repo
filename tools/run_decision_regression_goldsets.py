from __future__ import annotations

import argparse
import io
import json
import sys
import unittest
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DECISION_REGRESSION_SUITES = (
    ("onboarding_review_goldset", "tests.test_onboarding_review_goldset"),
    ("fusion_boundary_goldset", "tests.test_fusion_boundary_goldset"),
    ("runtime_review_bridge_goldset", "tests.test_runtime_review_bridge_goldset"),
    ("proxy_review_bridge_goldset", "tests.test_proxy_review_bridge_goldset"),
    ("fused_review_bridge_goldset", "tests.test_fused_review_bridge_goldset"),
    ("onboarding_identity_review_goldset", "tests.test_onboarding_identity_review_goldset"),
    ("publish_readiness_goldset", "tests.test_publish_readiness_goldset"),
)


def run_decision_regression_goldsets(*, emit_json: bool = False) -> dict[str, Any]:
    suite_rows = _run_suite_modules()
    summary = _summarize_suite_rows(suite_rows)
    payload = {
        "repo_root": str(REPO_ROOT),
        "suite_count": len(suite_rows),
        "total_tests": summary["total_tests"],
        "failure_count": summary["failure_count"],
        "error_count": summary["error_count"],
        "skipped_count": summary["skipped_count"],
        "ok": summary["ok"],
        "suite_rows": suite_rows,
    }
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(payload)
    return {
        "ok": bool(summary["ok"]),
        "status": "ok" if summary["ok"] else "test_failures",
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "result_payload": payload,
    }


def _run_suite_modules() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for suite_name, module_name in DECISION_REGRESSION_SUITES:
        suite = unittest.defaultTestLoader.loadTestsFromName(module_name)
        stream = io.StringIO()
        runner = unittest.TextTestRunner(stream=stream, verbosity=0)
        result = runner.run(suite)
        rows.append(
            {
                "suite_name": suite_name,
                "module_name": module_name,
                "ok": result.wasSuccessful(),
                "tests_run": result.testsRun,
                "failure_count": len(result.failures),
                "error_count": len(result.errors),
                "skipped_count": len(getattr(result, "skipped", [])),
                "output": stream.getvalue().strip(),
            }
        )
    return rows


def _summarize_suite_rows(suite_rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_tests = sum(int(row.get("tests_run", 0) or 0) for row in suite_rows)
    failure_count = sum(int(row.get("failure_count", 0) or 0) for row in suite_rows)
    error_count = sum(int(row.get("error_count", 0) or 0) for row in suite_rows)
    skipped_count = sum(int(row.get("skipped_count", 0) or 0) for row in suite_rows)
    return {
        "total_tests": total_tests,
        "failure_count": failure_count,
        "error_count": error_count,
        "skipped_count": skipped_count,
        "ok": failure_count == 0 and error_count == 0,
    }


def _render_compact_text(payload: dict[str, Any]) -> str:
    suite_rows = payload.get("suite_rows", []) if isinstance(payload.get("suite_rows"), list) else []
    lines = [
        f"Repo root: {payload.get('repo_root')}",
        f"Suite count: {payload.get('suite_count')}",
        f"Total tests: {payload.get('total_tests')}",
        f"Failure count: {payload.get('failure_count')}",
        f"Error count: {payload.get('error_count')}",
        f"Skipped count: {payload.get('skipped_count')}",
        f"Ok: {payload.get('ok')}",
        "",
        "Suites",
    ]
    for row in suite_rows:
        lines.append(
            " | ".join(
                [
                    str(row.get("suite_name")),
                    f"ok={row.get('ok')}",
                    f"tests_run={row.get('tests_run')}",
                    f"failures={row.get('failure_count')}",
                    f"errors={row.get('error_count')}",
                    f"skipped={row.get('skipped_count')}",
                ]
            )
        )
    return "\n".join(lines)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run focused decision-regression goldset suites")
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_decision_regression_goldsets(emit_json=bool(args.emit_json))
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
