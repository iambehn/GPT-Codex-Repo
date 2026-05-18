from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_next_action_cross_game_ledger_comparison import (
    inspect_detector_calibration_next_action_cross_game_ledger_comparison,
    main as inspect_main,
)


def _comparison_payload(*, rows: list[dict] | None = None, input_errors: list[dict] | None = None) -> dict:
    rows = rows or []
    input_errors = input_errors or []
    return {
        "schema_version": "detector_calibration_next_action_cross_game_ledger_comparison_v1",
        "generated_at": "2026-05-15T23:19:54Z",
        "source_ledger_paths": ["/tmp/a.json", "/tmp/b.json"],
        "row_count": len(rows),
        "rows": rows,
        "input_errors": input_errors,
    }


def _comparison_row(
    *,
    game: str,
    latest_status: str,
    ok_rate: float,
    partial_failure_rate: float,
    ledger_record_failed_rate: float,
    average_failed_count_per_run: float,
    latest_top_asset_id: str | None,
) -> dict:
    return {
        "game": game,
        "source_ledger_path": f"/tmp/{game}.json",
        "source_row_count": 1,
        "total_runs": 1,
        "latest_run_id": f"{game}-run-1",
        "latest_status": latest_status,
        "ok_rate": ok_rate,
        "partial_failure_rate": partial_failure_rate,
        "ledger_record_failed_rate": ledger_record_failed_rate,
        "average_selected_rows_per_run": 1.0,
        "average_applied_rows_per_run": 1.0,
        "average_created_count_per_run": 0.0,
        "average_reused_count_per_run": 1.0,
        "average_failed_count_per_run": average_failed_count_per_run,
        "recent_window_size": 5,
        "recent_window_size_actual": 1,
        "recent_ok_run_count": 1 if latest_status == "ok" else 0,
        "recent_partial_failure_run_count": 1 if latest_status == "partial_failure" else 0,
        "recent_ledger_record_failed_run_count": 1 if latest_status == "ledger_record_failed" else 0,
        "latest_top_asset_id": latest_top_asset_id,
    }


class InspectDetectorCalibrationNextActionCrossGameLedgerComparisonTests(unittest.TestCase):
    def test_non_empty_comparison_renders_expected_compact_output(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            comparison_path = root / "comparison.json"
            comparison_path.write_text(
                json.dumps(
                    _comparison_payload(
                        rows=[
                            _comparison_row(
                                game="overwatch",
                                latest_status="partial_failure",
                                ok_rate=0.0,
                                partial_failure_rate=1.0,
                                ledger_record_failed_rate=0.0,
                                average_failed_count_per_run=1.0,
                                latest_top_asset_id="overwatch.tracer.pulse_bomb",
                            ),
                            _comparison_row(
                                game="marvel_rivals",
                                latest_status="ok",
                                ok_rate=1.0,
                                partial_failure_rate=0.0,
                                ledger_record_failed_rate=0.0,
                                average_failed_count_per_run=0.0,
                                latest_top_asset_id="marvel_rivals.human_torch.hero_portrait",
                            ),
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_next_action_cross_game_ledger_comparison(
                comparison=comparison_path
            )
            self.assertTrue(result["ok"])
            self.assertIn("Row count: 2", result["rendered_output"])
            self.assertIn("Game: overwatch", result["rendered_output"])
            self.assertIn("Ok rate: 0.0", result["rendered_output"])
            self.assertIn(
                "- marvel_rivals | latest_status=ok | ok_rate=1.0 | average_failed_count_per_run=0.0",
                result["rendered_output"],
            )

    def test_comparison_with_input_errors_renders_expected_section(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            comparison_path = root / "comparison.json"
            comparison_path.write_text(
                json.dumps(
                    _comparison_payload(
                        rows=[],
                        input_errors=[
                            {
                                "source_ledger_path": "/tmp/bad.json",
                                "error": "ledger missing required fields",
                            }
                        ],
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_next_action_cross_game_ledger_comparison(
                comparison=comparison_path
            )
            self.assertIn("Input error count: 1", result["rendered_output"])
            self.assertIn("/tmp/bad.json", result["rendered_output"])
            self.assertIn("ledger missing required fields", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            comparison_path = root / "comparison.json"
            payload = _comparison_payload()
            comparison_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_cross_game_ledger_comparison(
                comparison=comparison_path,
                emit_json=True,
            )
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_comparison_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            comparison_path = root / "comparison.json"
            comparison_path.write_text(json.dumps({"row_count": 1}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "comparison missing required fields"):
                inspect_detector_calibration_next_action_cross_game_ledger_comparison(
                    comparison=comparison_path
                )

    def test_main_returns_error_code_for_invalid_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            comparison_path = root / "comparison.json"
            comparison_path.write_text(json.dumps({"row_count": 1}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--comparison", str(comparison_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
