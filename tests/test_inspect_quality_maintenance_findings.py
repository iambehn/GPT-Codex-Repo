from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.inspect_quality_maintenance_findings import (
    inspect_quality_maintenance_findings,
    main as inspect_main,
)


def _audit_payload(*, findings: list[dict] | None = None) -> dict:
    return {
        "ok": True,
        "status": "ok",
        "quality_maintenance_findings": findings
        if findings is not None
        else [
            {
                "surface": "fixture_freshness",
                "game": "marvel_rivals",
                "status": "fixture_refresh_recommended",
                "severity": "warning",
                "fixture_count": 6,
                "stale_by_days": 8,
            },
            {
                "surface": "review_backlog",
                "game": "call_of_duty",
                "draft_root": "/tmp/20260505T213332Z",
                "status": "review_backlog_present",
                "severity": "warning",
                "total_review_files": 122,
                "pending_review_count": 2,
                "pending_review_ratio": 0.0164,
            },
            {
                "surface": "fixture_freshness",
                "game": "call_of_duty",
                "status": "fresh",
                "severity": "informational",
                "fixture_count": 4,
            },
        ],
    }


class InspectQualityMaintenanceFindingsTests(unittest.TestCase):
    def test_compact_output_renders_counts_and_finding_lines(self) -> None:
        expected_repo_root = str(Path("/tmp/repo").resolve())
        with patch(
            "tools.inspect_quality_maintenance_findings.audit_pipeline_contracts",
            return_value=_audit_payload(),
        ):
            result = inspect_quality_maintenance_findings(repo_root="/tmp/repo")
        self.assertTrue(result["ok"])
        self.assertIn(f"Repo root: {expected_repo_root}", result["rendered_output"])
        self.assertIn("Total findings: 3", result["rendered_output"])
        self.assertIn("Warning count: 2", result["rendered_output"])
        self.assertIn("[warning] fixture_freshness | status=fixture_refresh_recommended | game=marvel_rivals", result["rendered_output"])
        self.assertIn("stale_by_days=8", result["rendered_output"])
        self.assertIn("[warning] review_backlog | status=review_backlog_present | game=call_of_duty | draft=20260505T213332Z", result["rendered_output"])

    def test_game_filter_limits_rows_and_counts(self) -> None:
        with patch(
            "tools.inspect_quality_maintenance_findings.audit_pipeline_contracts",
            return_value=_audit_payload(),
        ):
            result = inspect_quality_maintenance_findings(repo_root="/tmp/repo", game="call_of_duty")
        payload = result["inspection_payload"]
        self.assertEqual(payload["total_findings"], 2)
        self.assertEqual(payload["severity_counts"]["warning"], 1)
        self.assertEqual(payload["severity_counts"]["informational"], 1)
        self.assertEqual(
            [row["game"] for row in payload["quality_maintenance_findings"]],
            ["call_of_duty", "call_of_duty"],
        )

    def test_empty_findings_render_none(self) -> None:
        with patch(
            "tools.inspect_quality_maintenance_findings.audit_pipeline_contracts",
            return_value=_audit_payload(findings=[]),
        ):
            result = inspect_quality_maintenance_findings(repo_root="/tmp/repo")
        self.assertIn("Total findings: 0", result["rendered_output"])
        self.assertTrue(result["rendered_output"].endswith("Findings\nNone"))

    def test_json_mode_returns_summary_payload(self) -> None:
        expected_repo_root = str(Path("/tmp/repo").resolve())
        with patch(
            "tools.inspect_quality_maintenance_findings.audit_pipeline_contracts",
            return_value=_audit_payload(),
        ):
            result = inspect_quality_maintenance_findings(repo_root="/tmp/repo", emit_json=True)
        payload = json.loads(result["rendered_output"])
        self.assertEqual(payload["repo_root"], expected_repo_root)
        self.assertEqual(payload["total_findings"], 3)
        self.assertEqual(len(payload["quality_maintenance_findings"]), 3)

    def test_main_returns_error_code_on_invalid_shape(self) -> None:
        with patch(
            "tools.inspect_quality_maintenance_findings.audit_pipeline_contracts",
            return_value={"ok": True, "status": "ok", "quality_maintenance_findings": "bad"},
        ):
            exit_code = inspect_main(["--repo-root", "/tmp/repo"])
        self.assertEqual(exit_code, 1)
