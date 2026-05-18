from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.detector_calibration_next_action_cross_game_ledger_comparison_history_summary import (
    generate_detector_calibration_next_action_cross_game_ledger_comparison_history_summary,
)


def _ledger_row(
    *,
    run_id: str,
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
                "game": top_game,
                "ok_rate": top_ok_rate,
                "latest_status": top_latest_status,
                "average_failed_count_per_run": 1.0,
            }
        ],
    }


def _ledger_payload(*, rows: list[dict]) -> dict:
    return {
        "schema_version": "detector_calibration_next_action_cross_game_ledger_comparison_ledger_v1",
        "updated_at": "2026-05-15T23:35:26.138925Z",
        "row_count": len(rows),
        "rows": rows,
    }


class DetectorCalibrationNextActionCrossGameLedgerComparisonHistorySummaryTests(unittest.TestCase):
    def test_non_empty_ledger_aggregates_expected_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(
                json.dumps(
                    _ledger_payload(
                        rows=[
                            _ledger_row(run_id="20260515T231954Z", top_game="overwatch"),
                            _ledger_row(
                                run_id="20260515T233526Z",
                                status="partial_input_failure",
                                ok=True,
                                top_game="marvel_rivals",
                                top_ok_rate=1.0,
                                top_latest_status="ok",
                            ),
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            output_path = root / "summary.json"
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
                ledger=ledger_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["emitted_summary"]["total_runs"], 2)
            self.assertEqual(result["emitted_summary"]["ok_run_count"], 1)
            self.assertEqual(result["emitted_summary"]["partial_input_failure_run_count"], 1)
            self.assertEqual(result["emitted_summary"]["latest_run_id"], "20260515T233526Z")
            self.assertEqual(result["emitted_summary"]["latest_top_game"], "marvel_rivals")
            self.assertEqual(result["summary"]["top_game_counts"], {"overwatch": 1, "marvel_rivals": 1})
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(written["summary"]["total_runs"], result["emitted_summary"]["total_runs"])
            self.assertEqual(written["summary"]["latest_run_id"], result["emitted_summary"]["latest_run_id"])

    def test_empty_ledger_writes_deterministic_empty_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps(_ledger_payload(rows=[]), indent=2), encoding="utf-8")
            output_path = root / "summary.json"
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
                ledger=ledger_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "empty_ledger")
            self.assertEqual(result["emitted_summary"]["total_runs"], 0)
            self.assertIsNone(result["emitted_summary"]["latest_run_id"])
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(written["summary"]["total_runs"], 0)
            self.assertEqual(written["summary"]["top_game_counts"], {})
            self.assertIsNone(written["summary"]["latest_top_game"])

    def test_invalid_ledger_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps({"row_count": 1}, indent=2), encoding="utf-8")
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
                ledger=ledger_path
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_ledger")

    def test_emitted_summary_matches_written_summary_high_signal_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(
                json.dumps(_ledger_payload(rows=[_ledger_row(run_id="20260515T233526Z")]), indent=2),
                encoding="utf-8",
            )
            output_path = root / "summary.json"
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
                ledger=ledger_path,
                output_path=output_path,
            )
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(result["emitted_summary"]["source_ledger_path"], written["source_ledger_path"])
            self.assertEqual(result["emitted_summary"]["source_row_count"], written["source_row_count"])
            self.assertEqual(result["emitted_summary"]["total_runs"], written["summary"]["total_runs"])
            self.assertEqual(result["emitted_summary"]["latest_status"], written["summary"]["latest_status"])

    def test_top_game_frequency_counts_repeat_occurrences(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(
                json.dumps(
                    _ledger_payload(
                        rows=[
                            _ledger_row(run_id="20260515T231954Z", top_game="overwatch"),
                            _ledger_row(run_id="20260515T233526Z", top_game="overwatch"),
                            _ledger_row(run_id="20260515T235000Z", top_game="marvel_rivals", top_ok_rate=1.0, top_latest_status="ok"),
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
                ledger=ledger_path
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["summary"]["top_game_counts"], {"overwatch": 2, "marvel_rivals": 1})


if __name__ == "__main__":
    unittest.main()
