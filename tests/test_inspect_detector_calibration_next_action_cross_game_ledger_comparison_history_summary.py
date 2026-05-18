from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary import (
    inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary,
    main as inspect_main,
)


def _summary_payload(*, summary: dict | None = None) -> dict:
    return {
        "schema_version": "detector_calibration_next_action_cross_game_ledger_comparison_history_summary_v1",
        "generated_at": "2026-05-16T16:05:20Z",
        "source_ledger_path": "/tmp/history-ledger.json",
        "source_row_count": 1,
        "summary": summary
        or {
            "total_runs": 1,
            "ok_run_count": 1,
            "partial_input_failure_run_count": 0,
            "ledger_record_failed_run_count": 0,
            "top_game_counts": {"overwatch": 1},
            "latest_run_id": "20260515T233526Z",
            "latest_status": "ok",
            "latest_top_game": "overwatch",
            "latest_valid_game_count": 2,
        },
    }


class InspectDetectorCalibrationNextActionCrossGameLedgerComparisonHistorySummaryTests(unittest.TestCase):
    def test_non_empty_summary_renders_expected_compact_output(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            summary_path.write_text(json.dumps(_summary_payload(), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
                summary=summary_path
            )
            self.assertTrue(result["ok"])
            self.assertIn("Source row count: 1", result["rendered_output"])
            self.assertIn("Total runs: 1", result["rendered_output"])
            self.assertIn("Latest run id: 20260515T233526Z", result["rendered_output"])
            self.assertIn("Latest top game: overwatch", result["rendered_output"])
            self.assertIn("- overwatch: 1", result["rendered_output"])

    def test_empty_top_game_counts_renders_none(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            summary_path.write_text(
                json.dumps(
                    _summary_payload(
                        summary={
                            "total_runs": 0,
                            "ok_run_count": 0,
                            "partial_input_failure_run_count": 0,
                            "ledger_record_failed_run_count": 0,
                            "top_game_counts": {},
                            "latest_run_id": None,
                            "latest_status": None,
                            "latest_top_game": None,
                            "latest_valid_game_count": None,
                        }
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
                summary=summary_path
            )
            self.assertIn("Total runs: 0", result["rendered_output"])
            self.assertIn("Latest top game: None", result["rendered_output"])
            self.assertIn("Top game counts", result["rendered_output"])
            self.assertIn("None", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            payload = _summary_payload()
            summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
                summary=summary_path,
                emit_json=True,
            )
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_summary_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            summary_path.write_text(json.dumps({"source_row_count": 1}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "summary missing required fields"):
                inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
                    summary=summary_path
                )

    def test_main_returns_error_code_for_invalid_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            summary_path = root / "summary.json"
            summary_path.write_text(json.dumps({"source_row_count": 1}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--summary", str(summary_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
