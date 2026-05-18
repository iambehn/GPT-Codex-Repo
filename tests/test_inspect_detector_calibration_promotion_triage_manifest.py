from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_promotion_triage_manifest import (
    inspect_detector_calibration_promotion_triage_manifest,
    main as inspect_main,
)


def _manifest_payload(*, rows: list[dict], status_counts: dict[str, int] | None = None) -> dict:
    computed_status_counts = status_counts or {
        "publish_ready": sum(1 for row in rows if row.get("triage_status") == "publish_ready"),
        "needs_more_replay": sum(1 for row in rows if row.get("triage_status") == "needs_more_replay"),
        "hold": sum(1 for row in rows if row.get("triage_status") == "hold"),
    }
    return {
        "schema_version": "detector_calibration_promotion_triage_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-14T01:00:00+00:00",
        "publish_ready_threshold_delta_iou": 0.15,
        "source_promotion_count": len(rows),
        "row_count": len(rows),
        "status_counts": computed_status_counts,
        "rows": rows,
    }


def _row(*, status: str, asset_id: str, delta_iou: float, promotion_id: str, run_id: str, difference_summary: str) -> dict:
    return {
        "triage_status": status,
        "triage_reason": "reason",
        "promotion_record_path": f"/tmp/{promotion_id}.json",
        "promotion_root": f"/tmp/{promotion_id}",
        "promotion_id": promotion_id,
        "created_at": "2026-05-14T01:00:00+00:00",
        "asset_id": asset_id,
        "candidate_id": "rev-001",
        "run_id": run_id,
        "delta_iou": delta_iou,
        "absolute_delta_iou": abs(delta_iou),
        "primary_iou": 1.0,
        "secondary_iou": round(1.0 - abs(delta_iou), 6),
        "primary_source": "replay_run",
        "secondary_source": "crop_candidate",
        "primary_reference_source": "localized_match",
        "secondary_reference_source": "roi_fallback",
        "difference_summary": difference_summary,
        "draft_validation_status": "ok",
    }


class InspectDetectorCalibrationPromotionTriageManifestTests(unittest.TestCase):
    def test_empty_manifest_renders_explicit_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(
                json.dumps(_manifest_payload(rows=[]), indent=2),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_promotion_triage_manifest(manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertIn("Top publish-ready", result["rendered_output"])
            self.assertIn("No detector calibration promotion triage rows.", result["rendered_output"])

    def test_non_empty_manifest_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            ready = _row(
                status="publish_ready",
                asset_id="marvel_rivals.human_torch.hero_portrait",
                delta_iou=0.954702,
                promotion_id="hero-portrait",
                run_id="replay-001",
                difference_summary="Replay changed overlap source from roi_fallback to localized_match.",
            )
            hold = _row(
                status="hold",
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                delta_iou=-0.1,
                promotion_id="ace-hold",
                run_id="replay-002",
                difference_summary="No improvement.",
            )
            manifest_path.write_text(
                json.dumps(_manifest_payload(rows=[ready, hold]), indent=2),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_promotion_triage_manifest(manifest=manifest_path)
            self.assertIn("Asset id: marvel_rivals.human_torch.hero_portrait", result["rendered_output"])
            self.assertIn("Run id: replay-001", result["rendered_output"])
            self.assertIn("Delta IoU: 0.954702", result["rendered_output"])
            self.assertIn("publish_ready", result["rendered_output"])
            self.assertIn("hold", result["rendered_output"])
            self.assertIn("- marvel_rivals.ace.team_wipe_announcement | delta_iou=-0.1 | promotion_id=ace-hold", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            ready = _row(
                status="publish_ready",
                asset_id="marvel_rivals.human_torch.hero_portrait",
                delta_iou=0.954702,
                promotion_id="hero-portrait",
                run_id="replay-001",
                difference_summary="Replay changed overlap source from roi_fallback to localized_match.",
            )
            payload = _manifest_payload(rows=[ready])
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_promotion_triage_manifest(manifest=manifest_path, emit_json=True)
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_manifest_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "manifest missing required fields"):
                inspect_detector_calibration_promotion_triage_manifest(manifest=manifest_path)

    def test_main_returns_error_code_for_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--manifest", str(manifest_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
