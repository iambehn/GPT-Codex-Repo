from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.detector_calibration_next_action_apply_history_trend import (
    generate_detector_calibration_next_action_apply_history_trend,
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
    top_asset_id: str | None = "marvel_rivals.human_torch.hero_portrait",
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


def _ledger_payload(*, rows: list[dict]) -> dict:
    return {
        "schema_version": "detector_calibration_next_action_apply_ledger_v1",
        "game": "marvel_rivals",
        "updated_at": "2026-05-15T21:44:56.138925Z",
        "row_count": len(rows),
        "rows": rows,
    }


class DetectorCalibrationNextActionApplyHistoryTrendTests(unittest.TestCase):
    def test_non_empty_ledger_computes_expected_rate_math(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(
                json.dumps(
                    _ledger_payload(
                        rows=[
                            _ledger_row(run_id="run-1", status="ok", selected_row_count=1, applied_row_count=1),
                            _ledger_row(
                                run_id="run-2",
                                status="partial_failure",
                                selected_row_count=2,
                                applied_row_count=1,
                                failed_count=1,
                            ),
                            _ledger_row(
                                run_id="run-3",
                                status="ledger_record_failed",
                                selected_row_count=0,
                                applied_row_count=0,
                                reused_count=0,
                            ),
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            output_path = root / "trend.json"
            result = generate_detector_calibration_next_action_apply_history_trend(
                ledger=ledger_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["emitted_trend"]["total_runs"], 3)
            self.assertAlmostEqual(result["emitted_trend"]["ok_rate"], 1 / 3)
            self.assertAlmostEqual(result["emitted_trend"]["partial_failure_rate"], 1 / 3)
            self.assertAlmostEqual(result["emitted_trend"]["ledger_record_failed_rate"], 1 / 3)
            self.assertAlmostEqual(result["emitted_trend"]["average_selected_rows_per_run"], 1.0)
            self.assertAlmostEqual(result["emitted_trend"]["average_applied_rows_per_run"], 2 / 3)
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(written["trend"]["latest_run_id"], "run-3")
            self.assertEqual(written["trend"]["latest_status"], "ledger_record_failed")

    def test_recent_window_uses_last_five_runs(self) -> None:
        rows = [
            _ledger_row(run_id=f"run-{index}", status="ok", selected_row_count=index, applied_row_count=index)
            for index in range(1, 7)
        ]
        rows[-1]["status"] = "partial_failure"
        rows[-2]["status"] = "ledger_record_failed"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps(_ledger_payload(rows=rows), indent=2), encoding="utf-8")
            result = generate_detector_calibration_next_action_apply_history_trend(ledger=ledger_path)
            trend = result["trend"]
            self.assertEqual(trend["recent_window_size"], 5)
            self.assertEqual(trend["recent_window_size_actual"], 5)
            self.assertEqual(trend["recent_ok_run_count"], 3)
            self.assertEqual(trend["recent_partial_failure_run_count"], 1)
            self.assertEqual(trend["recent_ledger_record_failed_run_count"], 1)
            self.assertAlmostEqual(trend["recent_average_selected_rows_per_run"], (2 + 3 + 4 + 5 + 6) / 5)
            self.assertAlmostEqual(trend["recent_average_applied_rows_per_run"], (2 + 3 + 4 + 5 + 6) / 5)

    def test_empty_ledger_writes_deterministic_empty_output(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps(_ledger_payload(rows=[]), indent=2), encoding="utf-8")
            output_path = root / "trend.json"
            result = generate_detector_calibration_next_action_apply_history_trend(
                ledger=ledger_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "empty_ledger")
            self.assertEqual(result["trend"]["total_runs"], 0)
            self.assertEqual(result["trend"]["ok_rate"], 0.0)
            self.assertEqual(result["trend"]["recent_window_size"], 5)
            self.assertEqual(result["trend"]["recent_window_size_actual"], 0)
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(written["trend"]["total_runs"], 0)
            self.assertIsNone(written["trend"]["latest_run_id"])

    def test_invalid_ledger_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            result = generate_detector_calibration_next_action_apply_history_trend(ledger=ledger_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_ledger")

    def test_emitted_trend_matches_written_trend_high_signal_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(
                json.dumps(_ledger_payload(rows=[_ledger_row(run_id="run-1")]), indent=2),
                encoding="utf-8",
            )
            output_path = root / "trend.json"
            result = generate_detector_calibration_next_action_apply_history_trend(
                ledger=ledger_path,
                output_path=output_path,
            )
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(result["emitted_trend"]["source_ledger_path"], written["source_ledger_path"])
            self.assertEqual(result["emitted_trend"]["source_row_count"], written["source_row_count"])
            self.assertEqual(result["emitted_trend"]["total_runs"], written["trend"]["total_runs"])
            self.assertEqual(result["emitted_trend"]["latest_run_id"], written["trend"]["latest_run_id"])
            self.assertEqual(result["emitted_trend"]["ok_rate"], written["trend"]["ok_rate"])


if __name__ == "__main__":
    unittest.main()
