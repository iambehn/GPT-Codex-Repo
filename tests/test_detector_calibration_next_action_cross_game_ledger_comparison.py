from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.detector_calibration_next_action_cross_game_ledger_comparison import (
    generate_detector_calibration_next_action_cross_game_ledger_comparison,
)


def _ledger_row(
    *,
    run_id: str,
    status: str = "ok",
    selected_row_count: int = 1,
    applied_row_count: int = 1,
    created_count: int = 0,
    reused_count: int = 1,
    failed_count: int = 0,
    top_asset_id: str | None = "asset.default",
) -> dict:
    return {
        "run_id": run_id,
        "recorded_at": "2026-05-15T21:44:56.138925Z",
        "source_next_actions_manifest_path": "/tmp/next-actions.json",
        "status": status,
        "ok": status == "ok",
        "selected_row_count": selected_row_count,
        "applied_row_count": applied_row_count,
        "created_count": created_count,
        "reused_count": reused_count,
        "failed_count": failed_count,
        "top_asset_id": top_asset_id,
        "failed_asset_ids": [],
        "top_expansion_manifest_path": "/tmp/expansion.json",
    }


def _ledger_payload(*, game: str, rows: list[dict]) -> dict:
    return {
        "schema_version": "detector_calibration_next_action_apply_ledger_v1",
        "game": game,
        "updated_at": "2026-05-15T21:44:56.138925Z",
        "row_count": len(rows),
        "rows": rows,
    }


class DetectorCalibrationNextActionCrossGameLedgerComparisonTests(unittest.TestCase):
    def test_two_valid_ledgers_are_sorted_by_operational_weakness(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            strong = root / "strong.json"
            weak = root / "weak.json"
            strong.write_text(
                json.dumps(
                    _ledger_payload(
                        game="marvel_rivals",
                        rows=[_ledger_row(run_id="run-1", status="ok", top_asset_id="asset.marvel")],
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            weak.write_text(
                json.dumps(
                    _ledger_payload(
                        game="overwatch",
                        rows=[
                            _ledger_row(
                                run_id="run-2",
                                status="partial_failure",
                                selected_row_count=2,
                                applied_row_count=1,
                                failed_count=1,
                                top_asset_id="asset.overwatch",
                            )
                        ],
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            output_path = root / "comparison.json"
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[strong, weak],
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["row_count"], 2)
            self.assertEqual(result["rows"][0]["game"], "overwatch")
            self.assertEqual(result["rows"][1]["game"], "marvel_rivals")
            self.assertEqual(result["emitted_comparison"]["top_game"], "overwatch")
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(written["rows"][0]["game"], "overwatch")

    def test_one_valid_and_one_invalid_input_returns_partial_input_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            valid = root / "valid.json"
            invalid = root / "invalid.json"
            valid.write_text(
                json.dumps(
                    _ledger_payload(game="marvel_rivals", rows=[_ledger_row(run_id="run-1")]),
                    indent=2,
                ),
                encoding="utf-8",
            )
            invalid.write_text(json.dumps({"game": "broken"}, indent=2), encoding="utf-8")
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[valid, invalid],
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "partial_input_failure")
            self.assertEqual(result["row_count"], 1)
            self.assertEqual(len(result["input_errors"]), 1)
            self.assertEqual(result["input_errors"][0]["source_ledger_path"], str(invalid.resolve()))

    def test_all_invalid_inputs_fail_without_writing_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            invalid_a = root / "invalid-a.json"
            invalid_b = root / "invalid-b.json"
            invalid_a.write_text(json.dumps({"game": "broken-a"}, indent=2), encoding="utf-8")
            invalid_b.write_text(json.dumps({"game": "broken-b"}, indent=2), encoding="utf-8")
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[invalid_a, invalid_b],
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_input")
            self.assertEqual(result["row_count"], 0)
            self.assertEqual(len(result["input_errors"]), 2)

    def test_emitted_comparison_matches_written_artifact_high_signal_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(
                json.dumps(
                    _ledger_payload(game="marvel_rivals", rows=[_ledger_row(run_id="run-1")]),
                    indent=2,
                ),
                encoding="utf-8",
            )
            output_path = root / "comparison.json"
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[ledger_path],
                output_path=output_path,
            )
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(result["emitted_comparison"]["row_count"], written["row_count"])
            self.assertEqual(result["emitted_comparison"]["top_game"], written["rows"][0]["game"])
            self.assertEqual(result["emitted_comparison"]["top_ok_rate"], written["rows"][0]["ok_rate"])
            self.assertEqual(result["emitted_comparison"]["top_latest_status"], written["rows"][0]["latest_status"])

    def test_record_ledger_creates_new_history_ledger_for_ok_run(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            input_ledger = root / "ledger.json"
            input_ledger.write_text(
                json.dumps(
                    _ledger_payload(game="marvel_rivals", rows=[_ledger_row(run_id="run-1")]),
                    indent=2,
                ),
                encoding="utf-8",
            )
            output_path = root / "comparison.json"
            history_path = root / "history.json"
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[input_ledger],
                output_path=output_path,
                record_ledger=True,
                ledger_path=history_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertTrue(result["ledger_recorded"])
            self.assertEqual(result["ledger_path"], str(history_path.resolve()))
            self.assertRegex(result["ledger_run_id"], r"^\d{8}T\d{6}Z$")
            history_payload = json.loads(history_path.read_text(encoding="utf-8"))
            self.assertEqual(history_payload["row_count"], 1)
            self.assertEqual(history_payload["rows"][0]["top_game"], "marvel_rivals")
            self.assertEqual(history_payload["rows"][0]["valid_game_count"], 1)

    def test_record_ledger_appends_second_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            first = root / "first.json"
            second = root / "second.json"
            first.write_text(
                json.dumps(
                    _ledger_payload(game="marvel_rivals", rows=[_ledger_row(run_id="run-1")]),
                    indent=2,
                ),
                encoding="utf-8",
            )
            second.write_text(
                json.dumps(
                    _ledger_payload(
                        game="overwatch",
                        rows=[_ledger_row(run_id="run-2", status="partial_failure", failed_count=1)],
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            history_path = root / "history.json"
            generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[first],
                record_ledger=True,
                ledger_path=history_path,
            )
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[second],
                record_ledger=True,
                ledger_path=history_path,
            )
            self.assertTrue(result["ok"])
            history_payload = json.loads(history_path.read_text(encoding="utf-8"))
            self.assertEqual(history_payload["row_count"], 2)
            self.assertEqual(history_payload["rows"][0]["top_game"], "marvel_rivals")
            self.assertEqual(history_payload["rows"][1]["top_game"], "overwatch")

    def test_record_ledger_on_partial_input_failure_appends_history_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            valid = root / "valid.json"
            invalid = root / "invalid.json"
            valid.write_text(
                json.dumps(
                    _ledger_payload(game="marvel_rivals", rows=[_ledger_row(run_id="run-1")]),
                    indent=2,
                ),
                encoding="utf-8",
            )
            invalid.write_text(json.dumps({"game": "broken"}, indent=2), encoding="utf-8")
            history_path = root / "history.json"
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[valid, invalid],
                record_ledger=True,
                ledger_path=history_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "partial_input_failure")
            history_payload = json.loads(history_path.read_text(encoding="utf-8"))
            self.assertEqual(history_payload["row_count"], 1)
            self.assertEqual(history_payload["rows"][0]["status"], "partial_input_failure")
            self.assertEqual(history_payload["rows"][0]["input_error_count"], 1)

    def test_invalid_input_does_not_write_history_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            invalid = root / "invalid.json"
            invalid.write_text(json.dumps({"game": "broken"}, indent=2), encoding="utf-8")
            history_path = root / "history.json"
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[invalid],
                record_ledger=True,
                ledger_path=history_path,
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_input")
            self.assertFalse(history_path.exists())

    def test_requested_ledger_write_failure_returns_ledger_record_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            input_ledger = root / "ledger.json"
            blocking_path = root / "blocking"
            input_ledger.write_text(
                json.dumps(
                    _ledger_payload(game="marvel_rivals", rows=[_ledger_row(run_id="run-1")]),
                    indent=2,
                ),
                encoding="utf-8",
            )
            blocking_path.write_text("not a directory", encoding="utf-8")
            result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
                ledgers=[input_ledger],
                record_ledger=True,
                ledger_path=blocking_path / "history.json",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "ledger_record_failed")
            self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
