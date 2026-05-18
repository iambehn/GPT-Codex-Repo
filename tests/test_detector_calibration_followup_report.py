from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.detector_calibration_followup_report import generate_detector_calibration_followup_report


def _row(
    *,
    review_record_path: str,
    run_id: str,
    runtime_sidecar_path: str,
    asset_id: str,
    difference_summary: str,
    delta_iou: float,
    absolute_delta_iou: float | None = None,
) -> dict:
    return {
        "review_record_path": review_record_path,
        "run_id": run_id,
        "runtime_sidecar_path": runtime_sidecar_path,
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
        "absolute_delta_iou": absolute_delta_iou if absolute_delta_iou is not None else abs(delta_iou),
    }


class DetectorCalibrationFollowupReportTests(unittest.TestCase):
    def test_empty_manifest_produces_valid_empty_report(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "followup.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": "detector_calibration_followup_manifest_v1",
                        "game": "marvel_rivals",
                        "generated_at": "2026-05-14T01:00:00+00:00",
                        "source_session_count": 0,
                        "row_count": 0,
                        "rows": [],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            output_path = root / "report.json"
            result = generate_detector_calibration_followup_report(
                followup_manifest=manifest_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["source_row_count"], 0)
            self.assertEqual(result["asset_count"], 0)
            self.assertIsNone(result["top_followup"])
            self.assertEqual(result["top_followups_by_asset"], [])
            self.assertEqual(
                result["emitted_report"],
                {
                    "path": str(output_path.resolve()),
                    "source_row_count": 0,
                    "asset_count": 0,
                    "top_followup": None,
                },
            )
            report = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertIsNone(report["top_followup"])
            self.assertEqual(report["top_followups_by_asset"], [])

    def test_single_row_manifest_populates_global_and_per_asset_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "followup.json"
            row = _row(
                review_record_path="/tmp/review.json",
                run_id="replay-001",
                runtime_sidecar_path="/tmp/runtime.json",
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                difference_summary="strongest",
                delta_iou=0.188046,
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": "detector_calibration_followup_manifest_v1",
                        "game": "marvel_rivals",
                        "generated_at": "2026-05-14T01:00:00+00:00",
                        "source_session_count": 1,
                        "row_count": 1,
                        "rows": [row],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = generate_detector_calibration_followup_report(
                followup_manifest=manifest_path,
                output_path=root / "report.json",
            )
            self.assertEqual(result["top_followup"], row)
            self.assertEqual(result["top_followups_by_asset"], [row])
            self.assertEqual(
                result["emitted_report"],
                {
                    "path": str((root / "report.json").resolve()),
                    "source_row_count": 1,
                    "asset_count": 1,
                    "top_followup": row,
                },
            )

    def test_repeated_asset_rows_choose_first_row_only(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "followup.json"
            first = _row(
                review_record_path="/tmp/review-a.json",
                run_id="replay-001",
                runtime_sidecar_path="/tmp/runtime-a.json",
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                difference_summary="first",
                delta_iou=0.188046,
            )
            second = _row(
                review_record_path="/tmp/review-b.json",
                run_id="replay-002",
                runtime_sidecar_path="/tmp/runtime-b.json",
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                difference_summary="second",
                delta_iou=0.010000,
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": "detector_calibration_followup_manifest_v1",
                        "game": "marvel_rivals",
                        "generated_at": "2026-05-14T01:00:00+00:00",
                        "source_session_count": 2,
                        "row_count": 2,
                        "rows": [first, second],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = generate_detector_calibration_followup_report(
                followup_manifest=manifest_path,
                output_path=root / "report.json",
            )
            self.assertEqual(result["top_followups_by_asset"], [first])

    def test_multi_asset_manifest_preserves_source_order(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "followup.json"
            first = _row(
                review_record_path="/tmp/review-a.json",
                run_id="replay-001",
                runtime_sidecar_path="/tmp/runtime-a.json",
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                difference_summary="ace strongest",
                delta_iou=0.188046,
            )
            second = _row(
                review_record_path="/tmp/review-b.json",
                run_id="replay-002",
                runtime_sidecar_path="/tmp/runtime-b.json",
                asset_id="marvel_rivals.defeat.end_match_result_banner",
                difference_summary="defeat next",
                delta_iou=0.050000,
            )
            third = _row(
                review_record_path="/tmp/review-c.json",
                run_id="replay-003",
                runtime_sidecar_path="/tmp/runtime-c.json",
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                difference_summary="ace later duplicate",
                delta_iou=0.010000,
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": "detector_calibration_followup_manifest_v1",
                        "game": "marvel_rivals",
                        "generated_at": "2026-05-14T01:00:00+00:00",
                        "source_session_count": 3,
                        "row_count": 3,
                        "rows": [first, second, third],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = generate_detector_calibration_followup_report(
                followup_manifest=manifest_path,
                output_path=root / "report.json",
            )
            self.assertEqual(result["top_followups_by_asset"], [first, second])

    def test_output_path_override_writes_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "followup.json"
            row = _row(
                review_record_path="/tmp/review-a.json",
                run_id="replay-001",
                runtime_sidecar_path="/tmp/runtime-a.json",
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                difference_summary="ace strongest",
                delta_iou=0.188046,
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": "detector_calibration_followup_manifest_v1",
                        "game": "marvel_rivals",
                        "generated_at": "2026-05-14T01:00:00+00:00",
                        "source_session_count": 1,
                        "row_count": 1,
                        "rows": [row],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            output_path = root / "nested" / "report.json"
            with patch("tools.detector_calibration_followup_report._utc_now", return_value="2026-05-14T02:00:00+00:00"):
                first = generate_detector_calibration_followup_report(
                    followup_manifest=manifest_path,
                    output_path=output_path,
                )
                second = generate_detector_calibration_followup_report(
                    followup_manifest=manifest_path,
                    output_path=output_path,
                )
            self.assertEqual(first, second)
            report = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(report["generated_at"], "2026-05-14T02:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
