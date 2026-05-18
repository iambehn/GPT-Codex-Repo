from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.detector_calibration_followup_manifest import generate_detector_calibration_followup_manifest


def _comparison(*, source: str, crop: str, iou: float, revised: float, reference: float) -> dict:
    return {
        "status": "ok",
        "published_dimensions": {"width": 100, "height": 40},
        "revised_dimensions": {"width": 96, "height": 38},
        "delta": {"width": -4, "height": -2},
        "dimensions_match": False,
        "spatial_overlap": {
            "reference_crop": crop,
            "reference_source": source,
            "intersection": {"x": 5, "y": 5, "w": 90, "h": 30},
            "intersection_area": 2700,
            "revised_area": 2700,
            "reference_area": 4000,
            "iou": iou,
            "revised_coverage_ratio": revised,
            "reference_coverage_ratio": reference,
        },
    }


class DetectorCalibrationFollowupManifestTests(unittest.TestCase):
    def test_generate_manifest_includes_material_difference_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            game_root = root / "marvel_rivals"
            session_root = game_root / "session-a"
            session_root.mkdir(parents=True)
            runtime_path = session_root / "replay_results" / "replay-001" / "trial" / "runtime_analysis.json"
            runtime_path.parent.mkdir(parents=True, exist_ok=True)
            runtime_path.write_text("{}", encoding="utf-8")
            review_record = {
                "schema_version": "detector_calibration_review_v1",
                "game": "marvel_rivals",
                "target": {
                    "asset_id": "marvel_rivals.ace.team_wipe_announcement",
                    "event_type": "team_wipe_seen",
                    "event_row_id": "ace",
                },
                "crop_candidates": [
                    {"candidate_id": "rev-001", "template_comparison": _comparison(source="roi_fallback", crop="0,0,100,40", iou=0.675, revised=1.0, reference=0.675)}
                ],
                "replay_runs": [
                    {
                        "run_id": "replay-001",
                        "candidate_id": "rev-001",
                        "created_at": "2026-05-13T01:00:00+00:00",
                        "trial_runtime_sidecar_path": str(runtime_path.resolve()),
                        "derived_template_comparison": _comparison(
                            source="localized_match",
                            crop="4,6,22,18",
                            iou=0.13913,
                            revised=0.14,
                            reference=0.954545,
                        ),
                    }
                ],
            }
            (session_root / "review_record.json").write_text(json.dumps(review_record, indent=2), encoding="utf-8")
            output_path = root / "followup.json"
            result = generate_detector_calibration_followup_manifest(
                game="marvel_rivals",
                calibration_root=game_root,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["row_count"], 1)
            row = result["rows"][0]
            self.assertEqual(row["session_root"], str(session_root.resolve()))
            self.assertEqual(row["run_id"], "replay-001")
            self.assertEqual(row["candidate_id"], "rev-001")
            self.assertEqual(row["primary_source"], "replay_run")
            self.assertEqual(row["secondary_source"], "crop_candidate")
            self.assertEqual(row["primary_reference_crop"], "4,6,22,18")
            self.assertEqual(row["secondary_reference_crop"], "0,0,100,40")
            self.assertEqual(row["primary_iou"], 0.13913)
            self.assertEqual(row["secondary_iou"], 0.675)
            self.assertEqual(row["delta_iou"], -0.53587)
            self.assertIn("Replay changed overlap source from roi_fallback to localized_match", row["difference_summary"])
            self.assertEqual(
                result["emitted_manifest"],
                {
                    "path": str(output_path.resolve()),
                    "row_count": 1,
                    "source_session_count": 1,
                    "top_row": row,
                },
            )

    def test_generate_manifest_excludes_identical_comparison_session(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            game_root = root / "marvel_rivals"
            session_root = game_root / "session-a"
            session_root.mkdir(parents=True)
            runtime_path = session_root / "replay_results" / "replay-001" / "trial" / "runtime_analysis.json"
            runtime_path.parent.mkdir(parents=True, exist_ok=True)
            runtime_path.write_text("{}", encoding="utf-8")
            comparison = _comparison(source="localized_match", crop="4,6,22,18", iou=0.13913, revised=0.14, reference=0.954545)
            review_record = {
                "schema_version": "detector_calibration_review_v1",
                "game": "marvel_rivals",
                "target": {"asset_id": "marvel_rivals.ace.team_wipe_announcement"},
                "crop_candidates": [{"candidate_id": "rev-001", "template_comparison": comparison}],
                "replay_runs": [
                    {
                        "run_id": "replay-001",
                        "candidate_id": "rev-001",
                        "created_at": "2026-05-13T01:00:00+00:00",
                        "trial_runtime_sidecar_path": str(runtime_path.resolve()),
                        "derived_template_comparison": comparison,
                    }
                ],
            }
            (session_root / "review_record.json").write_text(json.dumps(review_record, indent=2), encoding="utf-8")
            result = generate_detector_calibration_followup_manifest(
                game="marvel_rivals",
                calibration_root=game_root,
                output_path=root / "followup.json",
            )
            self.assertEqual(result["row_count"], 0)
            self.assertEqual(
                result["emitted_manifest"],
                {
                    "path": str((root / "followup.json").resolve()),
                    "row_count": 0,
                    "source_session_count": 1,
                    "top_row": None,
                },
            )

    def test_generate_manifest_sorts_by_absolute_iou_delta_then_recency(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            game_root = root / "marvel_rivals"
            for name, primary_iou, secondary_iou, created_at in (
                ("session-a", 0.1, 0.7, "2026-05-13T01:00:00+00:00"),
                ("session-b", 0.2, 0.6, "2026-05-13T02:00:00+00:00"),
            ):
                session_root = game_root / name
                session_root.mkdir(parents=True)
                runtime_path = session_root / "replay_results" / "replay-001" / "trial" / "runtime_analysis.json"
                runtime_path.parent.mkdir(parents=True, exist_ok=True)
                runtime_path.write_text("{}", encoding="utf-8")
                review_record = {
                    "schema_version": "detector_calibration_review_v1",
                    "game": "marvel_rivals",
                    "target": {"asset_id": "marvel_rivals.ace.team_wipe_announcement"},
                    "crop_candidates": [
                        {"candidate_id": "rev-001", "template_comparison": _comparison(source="roi_fallback", crop="0,0,100,40", iou=secondary_iou, revised=1.0, reference=secondary_iou)}
                    ],
                    "replay_runs": [
                        {
                            "run_id": "replay-001",
                            "candidate_id": "rev-001",
                            "created_at": created_at,
                            "trial_runtime_sidecar_path": str(runtime_path.resolve()),
                            "derived_template_comparison": _comparison(source="localized_match", crop="4,6,22,18", iou=primary_iou, revised=0.2, reference=0.9),
                        }
                    ],
                }
                (session_root / "review_record.json").write_text(json.dumps(review_record, indent=2), encoding="utf-8")
            result = generate_detector_calibration_followup_manifest(
                game="marvel_rivals",
                calibration_root=game_root,
                output_path=root / "followup.json",
            )
            self.assertEqual(result["row_count"], 2)
            self.assertTrue(result["rows"][0]["session_root"].endswith("session-a"))
            self.assertTrue(result["rows"][1]["session_root"].endswith("session-b"))

    def test_generate_manifest_is_deterministic_with_fixed_time(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            game_root = root / "marvel_rivals"
            session_root = game_root / "session-a"
            session_root.mkdir(parents=True)
            runtime_path = session_root / "replay_results" / "replay-001" / "trial" / "runtime_analysis.json"
            runtime_path.parent.mkdir(parents=True, exist_ok=True)
            runtime_path.write_text("{}", encoding="utf-8")
            review_record = {
                "schema_version": "detector_calibration_review_v1",
                "game": "marvel_rivals",
                "target": {"asset_id": "marvel_rivals.ace.team_wipe_announcement"},
                "crop_candidates": [
                    {"candidate_id": "rev-001", "template_comparison": _comparison(source="roi_fallback", crop="0,0,100,40", iou=0.675, revised=1.0, reference=0.675)}
                ],
                "replay_runs": [
                    {
                        "run_id": "replay-001",
                        "candidate_id": "rev-001",
                        "created_at": "2026-05-13T01:00:00+00:00",
                        "trial_runtime_sidecar_path": str(runtime_path.resolve()),
                        "derived_template_comparison": _comparison(source="localized_match", crop="4,6,22,18", iou=0.13913, revised=0.14, reference=0.954545),
                    }
                ],
            }
            (session_root / "review_record.json").write_text(json.dumps(review_record, indent=2), encoding="utf-8")
            output_path = root / "followup.json"
            with patch("tools.detector_calibration_followup_manifest._utc_now", return_value="2026-05-13T02:00:00+00:00"):
                first = generate_detector_calibration_followup_manifest(
                    game="marvel_rivals",
                    calibration_root=game_root,
                    output_path=output_path,
                )
                second = generate_detector_calibration_followup_manifest(
                    game="marvel_rivals",
                    calibration_root=game_root,
                    output_path=output_path,
                )
            self.assertEqual(first, second)
            manifest = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["generated_at"], "2026-05-13T02:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
