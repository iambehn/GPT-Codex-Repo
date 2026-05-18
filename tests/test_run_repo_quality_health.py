from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from tools.run_repo_quality_health import (
    run_repo_quality_health,
    main as run_repo_quality_health_main,
)


def _maintenance_result(*, warning_count: int = 0, blocking_count: int = 0) -> dict:
    return {
        "ok": True,
        "status": "ok",
        "inspection_payload": {
            "repo_root": "/tmp/repo",
            "total_findings": warning_count + blocking_count,
            "severity_counts": {
                "blocking": blocking_count,
                "warning": warning_count,
                "informational": 0,
            },
            "quality_maintenance_findings": [],
        },
    }


def _regression_result(*, ok: bool = True) -> dict:
    return {
        "ok": ok,
        "status": "ok" if ok else "test_failures",
        "result_payload": {
            "suite_count": 9,
            "total_tests": 18,
            "failure_count": 0 if ok else 1,
            "error_count": 0,
            "skipped_count": 0,
            "suite_rows": [],
        },
    }


class RunRepoQualityHealthTests(unittest.TestCase):
    def test_compact_output_reports_passing_health(self) -> None:
        with patch(
            "tools.run_repo_quality_health.inspect_quality_maintenance_findings",
            return_value=_maintenance_result(),
        ), patch(
            "tools.run_repo_quality_health.run_decision_regression_goldsets",
            return_value=_regression_result(),
        ):
            result = run_repo_quality_health()
        self.assertTrue(result["ok"])
        self.assertIn("Maintenance ok: True", result["rendered_output"])
        self.assertIn("Decision regression ok: True", result["rendered_output"])

    def test_warning_findings_fail_overall_health(self) -> None:
        with patch(
            "tools.run_repo_quality_health.inspect_quality_maintenance_findings",
            return_value=_maintenance_result(warning_count=1),
        ), patch(
            "tools.run_repo_quality_health.run_decision_regression_goldsets",
            return_value=_regression_result(),
        ):
            result = run_repo_quality_health()
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "repo_quality_failures")

    def test_regression_failure_fails_overall_health(self) -> None:
        with patch(
            "tools.run_repo_quality_health.inspect_quality_maintenance_findings",
            return_value=_maintenance_result(),
        ), patch(
            "tools.run_repo_quality_health.run_decision_regression_goldsets",
            return_value=_regression_result(ok=False),
        ):
            result = run_repo_quality_health()
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "repo_quality_failures")

    def test_json_mode_returns_payload(self) -> None:
        with patch(
            "tools.run_repo_quality_health.inspect_quality_maintenance_findings",
            return_value=_maintenance_result(),
        ), patch(
            "tools.run_repo_quality_health.run_decision_regression_goldsets",
            return_value=_regression_result(),
        ):
            result = run_repo_quality_health(emit_json=True)
        payload = json.loads(result["rendered_output"])
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["decision_regression_summary"]["suite_count"], 9)

    def test_main_returns_error_code_on_failure(self) -> None:
        with patch(
            "tools.run_repo_quality_health.inspect_quality_maintenance_findings",
            return_value=_maintenance_result(warning_count=1),
        ), patch(
            "tools.run_repo_quality_health.run_decision_regression_goldsets",
            return_value=_regression_result(),
        ):
            exit_code = run_repo_quality_health_main([])
        self.assertEqual(exit_code, 1)
