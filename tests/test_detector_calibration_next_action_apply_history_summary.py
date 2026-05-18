from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.detector_calibration_next_action_apply_history_summary import (
    generate_detector_calibration_next_action_apply_history_summary,
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


class DetectorCalibrationNextActionApplyHistorySummaryTests(unittest.TestCase):
    def test_non_empty_ledger_aggregates_expected_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(
                json.dumps(
                    _ledger_payload(
                        rows=[
                            _ledger_row(run_id="20260515T214455Z"),
                            _ledger_row(
                                run_id="20260515T214456Z",
                                status="partial_failure",
                                selected_row_count=2,
                                applied_row_count=1,
                                failed_count=1,
                                top_asset_id="marvel_rivals.ace.team_wipe_announcement",
                            ),
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            output_path = root / "summary.json"
            result = generate_detector_calibration_next_action_apply_history_summary(
                ledger=ledger_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["emitted_summary"]["total_runs"], 2)
            self.assertEqual(result["emitted_summary"]["ok_run_count"], 1)
            self.assertEqual(result["emitted_summary"]["partial_failure_run_count"], 1)
            self.assertEqual(result["emitted_summary"]["total_selected_rows"], 3)
            self.assertEqual(result["emitted_summary"]["total_applied_rows"], 2)
            self.assertEqual(result["emitted_summary"]["total_reused_count"], 2)
            self.assertEqual(result["emitted_summary"]["total_failed_count"], 1)
            self.assertEqual(result["emitted_summary"]["latest_run_id"], "20260515T214456Z")
            self.assertEqual(
                result["emitted_summary"]["latest_top_asset_id"],
                "marvel_rivals.ace.team_wipe_announcement",
            )
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(written["summary"]["total_runs"], result["emitted_summary"]["total_runs"])
            self.assertEqual(written["summary"]["latest_run_id"], result["emitted_summary"]["latest_run_id"])

    def test_empty_ledger_writes_deterministic_empty_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps(_ledger_payload(rows=[]), indent=2), encoding="utf-8")
            output_path = root / "summary.json"
            result = generate_detector_calibration_next_action_apply_history_summary(
                ledger=ledger_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "empty_ledger")
            self.assertEqual(result["emitted_summary"]["total_runs"], 0)
            self.assertIsNone(result["emitted_summary"]["latest_run_id"])
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(written["summary"]["total_runs"], 0)
            self.assertIsNone(written["summary"]["latest_run_id"])

    def test_invalid_ledger_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            result = generate_detector_calibration_next_action_apply_history_summary(ledger=ledger_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_ledger")

    def test_emitted_summary_matches_written_summary_high_signal_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(
                json.dumps(_ledger_payload(rows=[_ledger_row(run_id="20260515T214456Z")]), indent=2),
                encoding="utf-8",
            )
            output_path = root / "summary.json"
            result = generate_detector_calibration_next_action_apply_history_summary(
                ledger=ledger_path,
                output_path=output_path,
            )
            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(
                result["emitted_summary"]["source_ledger_path"],
                written["source_ledger_path"],
            )
            self.assertEqual(
                result["emitted_summary"]["source_row_count"],
                written["source_row_count"],
            )
            self.assertEqual(
                result["emitted_summary"]["total_runs"],
                written["summary"]["total_runs"],
            )
            self.assertEqual(
                result["emitted_summary"]["latest_status"],
                written["summary"]["latest_status"],
            )


if __name__ == "__main__":
    unittest.main()
