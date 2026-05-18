from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_next_action_apply_history_trend import (
    inspect_detector_calibration_next_action_apply_history_trend,
    main as inspect_main,
)


def _trend_payload(*, trend: dict | None = None) -> dict:
    return {
        "schema_version": "detector_calibration_next_action_apply_history_trend_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-15T23:08:06Z",
        "source_ledger_path": "/tmp/ledger.json",
        "source_row_count": 1,
        "trend": trend
        or {
            "total_runs": 1,
            "latest_run_id": "20260515T214456Z",
            "latest_status": "ok",
            "latest_recorded_at": "2026-05-15T21:44:56.138925Z",
            "ok_rate": 1.0,
            "partial_failure_rate": 0.0,
            "ledger_record_failed_rate": 0.0,
            "average_selected_rows_per_run": 1.0,
            "average_applied_rows_per_run": 1.0,
            "average_created_count_per_run": 0.0,
            "average_reused_count_per_run": 1.0,
            "average_failed_count_per_run": 0.0,
            "recent_window_size": 5,
            "recent_window_size_actual": 1,
            "recent_ok_run_count": 1,
            "recent_partial_failure_run_count": 0,
            "recent_ledger_record_failed_run_count": 0,
            "recent_average_selected_rows_per_run": 1.0,
            "recent_average_applied_rows_per_run": 1.0,
        },
    }


class InspectDetectorCalibrationNextActionApplyHistoryTrendTests(unittest.TestCase):
    def test_non_empty_trend_renders_expected_compact_output(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            trend_path = root / "trend.json"
            trend_path.write_text(json.dumps(_trend_payload(), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_apply_history_trend(trend=trend_path)
            self.assertTrue(result["ok"])
            self.assertIn("Game: marvel_rivals", result["rendered_output"])
            self.assertIn("Source row count: 1", result["rendered_output"])
            self.assertIn("Total runs: 1", result["rendered_output"])
            self.assertIn("Ok rate: 1.0", result["rendered_output"])
            self.assertIn("Latest run id: 20260515T214456Z", result["rendered_output"])

    def test_empty_trend_renders_zero_and_none_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            trend_path = root / "trend.json"
            trend_path.write_text(
                json.dumps(
                    _trend_payload(
                        trend={
                            "total_runs": 0,
                            "latest_run_id": None,
                            "latest_status": None,
                            "latest_recorded_at": None,
                            "ok_rate": 0.0,
                            "partial_failure_rate": 0.0,
                            "ledger_record_failed_rate": 0.0,
                            "average_selected_rows_per_run": 0.0,
                            "average_applied_rows_per_run": 0.0,
                            "average_created_count_per_run": 0.0,
                            "average_reused_count_per_run": 0.0,
                            "average_failed_count_per_run": 0.0,
                            "recent_window_size": 5,
                            "recent_window_size_actual": 0,
                            "recent_ok_run_count": 0,
                            "recent_partial_failure_run_count": 0,
                            "recent_ledger_record_failed_run_count": 0,
                            "recent_average_selected_rows_per_run": 0.0,
                            "recent_average_applied_rows_per_run": 0.0,
                        }
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_next_action_apply_history_trend(trend=trend_path)
            self.assertIn("Total runs: 0", result["rendered_output"])
            self.assertIn("Latest run id: None", result["rendered_output"])
            self.assertIn("Recent window size actual: 0", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            trend_path = root / "trend.json"
            payload = _trend_payload()
            trend_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_apply_history_trend(
                trend=trend_path,
                emit_json=True,
            )
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_trend_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            trend_path = root / "trend.json"
            trend_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "trend missing required fields"):
                inspect_detector_calibration_next_action_apply_history_trend(trend=trend_path)

    def test_main_returns_error_code_for_invalid_trend(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            trend_path = root / "trend.json"
            trend_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--trend", str(trend_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
