from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger import (
    inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger,
    main as inspect_main,
)


def _ledger_payload(*, rows: list[dict] | None = None) -> dict:
    rows = rows or []
    return {
        "schema_version": "detector_calibration_next_action_cross_game_ledger_comparison_ledger_v1",
        "updated_at": "2026-05-15T23:35:26.138925Z",
        "row_count": len(rows),
        "rows": rows,
    }


def _ledger_row(
    *,
    run_id: str = "20260515T233526Z",
    status: str = "ok",
    ok: bool = True,
    valid_game_count: int = 2,
    input_error_count: int = 0,
    top_game: str | None = "overwatch",
    top_ok_rate: float = 0.0,
    top_latest_status: str | None = "partial_failure",
) -> dict:
    return {
        "run_id": run_id,
        "recorded_at": "2026-05-15T23:35:26.138925Z",
        "source_ledger_paths": ["/tmp/marvel.json", "/tmp/overwatch.json"],
        "status": status,
        "ok": ok,
        "valid_game_count": valid_game_count,
        "input_error_count": input_error_count,
        "top_game": top_game,
        "top_ok_rate": top_ok_rate,
        "top_latest_status": top_latest_status,
        "games": [
            {
                "game": "overwatch",
                "ok_rate": 0.0,
                "latest_status": "partial_failure",
                "average_failed_count_per_run": 1.0,
            }
        ],
    }


class InspectDetectorCalibrationNextActionCrossGameLedgerComparisonHistoryLedgerTests(unittest.TestCase):
    def test_empty_ledger_renders_explicit_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps(_ledger_payload(), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger(
                ledger=ledger_path
            )
            self.assertTrue(result["ok"])
            self.assertIn("Latest run", result["rendered_output"])
            self.assertIn("None", result["rendered_output"])
            self.assertIn("Runs", result["rendered_output"])

    def test_non_empty_ledger_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            payload = _ledger_payload(
                rows=[
                    _ledger_row(run_id="20260515T231954Z", top_game="marvel_rivals", top_ok_rate=1.0, top_latest_status="ok"),
                    _ledger_row(),
                ]
            )
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger(
                ledger=ledger_path
            )
            self.assertIn("Row count: 2", result["rendered_output"])
            self.assertIn("Run id: 20260515T233526Z", result["rendered_output"])
            self.assertIn("Top game: overwatch", result["rendered_output"])
            self.assertIn("Top latest status: partial_failure", result["rendered_output"])
            self.assertIn(
                "- 20260515T231954Z | ok | valid_game_count=2 | top_game=marvel_rivals",
                result["rendered_output"],
            )
            self.assertIn(
                "- 20260515T233526Z | ok | valid_game_count=2 | top_game=overwatch",
                result["rendered_output"],
            )

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            payload = _ledger_payload(rows=[_ledger_row()])
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger(
                ledger=ledger_path,
                emit_json=True,
            )
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_ledger_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps({"row_count": 1}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "ledger missing required fields"):
                inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger(ledger=ledger_path)

    def test_main_returns_error_code_for_invalid_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps({"row_count": 1}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--ledger", str(ledger_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
