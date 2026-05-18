from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_next_action_apply_history_summary import (
    inspect_detector_calibration_next_action_apply_history_summary,
    main as inspect_main,
)


def _summary_payload(*, summary: dict | None = None) -> dict:
    return {
        "schema_version": "detector_calibration_next_action_apply_history_summary_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-15T22:18:08Z",
        "source_ledger_path": "/tmp/ledger.json",
        "source_row_count": 1,
        "summary": summary
        or {
            "total_runs": 1,
            "ok_run_count": 1,
            "partial_failure_run_count": 0,
            "ledger_record_failed_run_count": 0,
            "no_actionable_rows_count": 0,
            "invalid_next_actions_manifest_count": 0,
            "total_selected_rows": 1,
            "total_applied_rows": 1,
            "total_created_count": 0,
            "total_reused_count": 1,
            "total_failed_count": 0,
            "latest_run_id": "20260515T214456Z",
            "latest_status": "ok",
            "latest_top_asset_id": "marvel_rivals.human_torch.hero_portrait",
        },
    }


class InspectDetectorCalibrationNextActionApplyHistorySummaryTests(unittest.TestCase):
    def test_non_empty_summary_renders_expected_compact_output(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            summary_path.write_text(json.dumps(_summary_payload(), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_apply_history_summary(summary=summary_path)
            self.assertTrue(result["ok"])
            self.assertIn("Game: marvel_rivals", result["rendered_output"])
            self.assertIn("Source row count: 1", result["rendered_output"])
            self.assertIn("Total runs: 1", result["rendered_output"])
            self.assertIn("Latest run id: 20260515T214456Z", result["rendered_output"])
            self.assertIn(
                "Latest top asset id: marvel_rivals.human_torch.hero_portrait",
                result["rendered_output"],
            )

    def test_empty_summary_renders_explicit_zero_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            summary_path.write_text(
                json.dumps(
                    _summary_payload(
                        summary={
                            "total_runs": 0,
                            "ok_run_count": 0,
                            "partial_failure_run_count": 0,
                            "ledger_record_failed_run_count": 0,
                            "no_actionable_rows_count": 0,
                            "invalid_next_actions_manifest_count": 0,
                            "total_selected_rows": 0,
                            "total_applied_rows": 0,
                            "total_created_count": 0,
                            "total_reused_count": 0,
                            "total_failed_count": 0,
                            "latest_run_id": None,
                            "latest_status": None,
                            "latest_top_asset_id": None,
                        }
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_next_action_apply_history_summary(summary=summary_path)
            self.assertIn("Total runs: 0", result["rendered_output"])
            self.assertIn("Latest run id: None", result["rendered_output"])
            self.assertIn("Latest top asset id: None", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            payload = _summary_payload()
            summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_apply_history_summary(
                summary=summary_path,
                emit_json=True,
            )
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_summary_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            summary_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "summary missing required fields"):
                inspect_detector_calibration_next_action_apply_history_summary(summary=summary_path)

    def test_main_returns_error_code_for_invalid_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            summary_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--summary", str(summary_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
