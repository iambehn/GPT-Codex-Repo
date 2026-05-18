from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_followup_report import (
    inspect_detector_calibration_followup_report,
    main as inspect_main,
)


def _report_payload(*, top_followup: dict | None, top_followups_by_asset: list[dict]) -> dict:
    return {
        "schema_version": "detector_calibration_followup_report_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-14T01:00:00+00:00",
        "source_followup_manifest_path": "/tmp/followup.json",
        "source_row_count": 0 if top_followup is None else 1,
        "asset_count": len(top_followups_by_asset),
        "top_followup": top_followup,
        "top_followups_by_asset": top_followups_by_asset,
    }


def _row(*, asset_id: str, run_id: str, delta_iou: float, difference_summary: str) -> dict:
    return {
        "review_record_path": "/tmp/review.json",
        "run_id": run_id,
        "runtime_sidecar_path": "/tmp/runtime.json",
        "asset_id": asset_id,
        "event_type": "team_wipe_seen",
        "event_row_id": "ace",
        "difference_summary": difference_summary,
        "primary_source": "replay_run",
        "secondary_source": "crop_candidate",
        "primary_reference_crop": "10,20,30,40",
        "secondary_reference_crop": "1,2,3,4",
        "primary_iou": 1.0,
        "secondary_iou": round(1.0 - abs(delta_iou), 6),
        "delta_iou": delta_iou,
        "absolute_delta_iou": abs(delta_iou),
    }


class InspectDetectorCalibrationFollowupReportTests(unittest.TestCase):
    def test_empty_report_renders_explicit_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            report_path = root / "report.json"
            report_path.write_text(
                json.dumps(_report_payload(top_followup=None, top_followups_by_asset=[]), indent=2),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_followup_report(report=report_path)
            self.assertTrue(result["ok"])
            self.assertIn("Top follow-up", result["rendered_output"])
            self.assertIn("No detector calibration follow-up rows.", result["rendered_output"])

    def test_non_empty_report_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            report_path = root / "report.json"
            row = _row(
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                run_id="replay-003",
                delta_iou=0.188046,
                difference_summary="Replay changed overlap source from roi_fallback to localized_match.",
            )
            report_path.write_text(
                json.dumps(_report_payload(top_followup=row, top_followups_by_asset=[row]), indent=2),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_followup_report(report=report_path)
            self.assertIn("Asset id: marvel_rivals.ace.team_wipe_announcement", result["rendered_output"])
            self.assertIn("Run id: replay-003", result["rendered_output"])
            self.assertIn("Delta IoU: 0.188046", result["rendered_output"])
            self.assertIn("Per-asset leaders", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            report_path = root / "report.json"
            row = _row(
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                run_id="replay-003",
                delta_iou=0.188046,
                difference_summary="Replay changed overlap source from roi_fallback to localized_match.",
            )
            payload = _report_payload(top_followup=row, top_followups_by_asset=[row])
            report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_followup_report(report=report_path, emit_json=True)
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_report_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            report_path = root / "report.json"
            report_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "report missing required fields"):
                inspect_detector_calibration_followup_report(report=report_path)

    def test_main_returns_error_code_for_invalid_report(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            report_path = root / "report.json"
            report_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--report", str(report_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
