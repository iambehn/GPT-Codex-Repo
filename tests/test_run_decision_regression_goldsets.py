from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from tools.run_decision_regression_goldsets import (
    run_decision_regression_goldsets,
    main as run_goldset_main,
)


def _suite_rows(*, failing: bool = False) -> list[dict]:
    rows = [
        {
            "suite_name": "onboarding_review_goldset",
            "module_name": "tests.test_onboarding_review_goldset",
            "ok": True,
            "tests_run": 2,
            "failure_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "output": "",
        },
        {
            "suite_name": "fusion_boundary_goldset",
            "module_name": "tests.test_fusion_boundary_goldset",
            "ok": not failing,
            "tests_run": 2,
            "failure_count": 1 if failing else 0,
            "error_count": 0,
            "skipped_count": 0,
            "output": "failure output" if failing else "",
        },
        {
            "suite_name": "runtime_review_bridge_goldset",
            "module_name": "tests.test_runtime_review_bridge_goldset",
            "ok": True,
            "tests_run": 2,
            "failure_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "output": "",
        },
        {
            "suite_name": "proxy_review_bridge_goldset",
            "module_name": "tests.test_proxy_review_bridge_goldset",
            "ok": True,
            "tests_run": 2,
            "failure_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "output": "",
        },
        {
            "suite_name": "fused_review_bridge_goldset",
            "module_name": "tests.test_fused_review_bridge_goldset",
            "ok": True,
            "tests_run": 2,
            "failure_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "output": "",
        },
        {
            "suite_name": "onboarding_identity_review_goldset",
            "module_name": "tests.test_onboarding_identity_review_goldset",
            "ok": True,
            "tests_run": 2,
            "failure_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "output": "",
        },
        {
            "suite_name": "publish_readiness_goldset",
            "module_name": "tests.test_publish_readiness_goldset",
            "ok": True,
            "tests_run": 2,
            "failure_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "output": "",
        },
    ]
    return rows


class RunDecisionRegressionGoldsetsTests(unittest.TestCase):
    def test_compact_output_renders_summary_and_suites(self) -> None:
        with patch(
            "tools.run_decision_regression_goldsets._run_suite_modules",
            return_value=_suite_rows(),
        ):
            result = run_decision_regression_goldsets()
        self.assertTrue(result["ok"])
        self.assertIn("Suite count: 7", result["rendered_output"])
        self.assertIn("Total tests: 14", result["rendered_output"])
        self.assertIn("fused_review_bridge_goldset | ok=True", result["rendered_output"])
        self.assertIn("proxy_review_bridge_goldset | ok=True", result["rendered_output"])
        self.assertIn("runtime_review_bridge_goldset | ok=True", result["rendered_output"])
        self.assertIn("fusion_boundary_goldset | ok=True", result["rendered_output"])

    def test_failure_status_propagates(self) -> None:
        with patch(
            "tools.run_decision_regression_goldsets._run_suite_modules",
            return_value=_suite_rows(failing=True),
        ):
            result = run_decision_regression_goldsets()
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "test_failures")
        self.assertIn("Failure count: 1", result["rendered_output"])

    def test_json_mode_returns_payload(self) -> None:
        with patch(
            "tools.run_decision_regression_goldsets._run_suite_modules",
            return_value=_suite_rows(),
        ):
            result = run_decision_regression_goldsets(emit_json=True)
        payload = json.loads(result["rendered_output"])
        self.assertEqual(payload["suite_count"], 7)
        self.assertEqual(payload["total_tests"], 14)

    def test_main_returns_error_code_when_suite_fails(self) -> None:
        with patch(
            "tools.run_decision_regression_goldsets._run_suite_modules",
            return_value=_suite_rows(failing=True),
        ):
            exit_code = run_goldset_main([])
        self.assertEqual(exit_code, 1)
