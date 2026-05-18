from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_publish_decision_manifest import (
    inspect_detector_calibration_publish_decision_manifest,
    main as inspect_main,
)


def _manifest_payload(*, rows: list[dict], status_counts: dict[str, int] | None = None) -> dict:
    computed_status_counts = status_counts or {
        "ready_to_publish": sum(1 for row in rows if row.get("decision_status") == "ready_to_publish"),
        "needs_broader_evidence": sum(1 for row in rows if row.get("decision_status") == "needs_broader_evidence"),
        "defer": sum(1 for row in rows if row.get("decision_status") == "defer"),
    }
    return {
        "schema_version": "detector_calibration_publish_decision_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-14T01:00:00+00:00",
        "source_triage_manifest_path": "/tmp/triage.json",
        "source_row_count": len(rows),
        "decision_thresholds": {
            "minimum_publish_ready_replays_per_asset": 2,
            "minimum_distinct_sources_per_asset": 2,
        },
        "row_count": len(rows),
        "status_counts": computed_status_counts,
        "rows": rows,
    }


def _row(*, status: str, asset_id: str, delta_iou: float, promotion_id: str, run_id: str, decision_reason: str) -> dict:
    return {
        "decision_status": status,
        "decision_reason": decision_reason,
        "triage_status": "publish_ready" if status != "defer" else "hold",
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
        "difference_summary": "summary",
        "draft_validation_status": "ok",
        "replay_count_for_asset": 1,
        "distinct_source_count_for_asset": 1,
        "source_keys_for_asset": ["/tmp/clip-a.mp4"],
    }


class InspectDetectorCalibrationPublishDecisionManifestTests(unittest.TestCase):
    def test_empty_manifest_renders_explicit_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(_manifest_payload(rows=[]), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_publish_decision_manifest(manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertIn("Top ready-to-publish", result["rendered_output"])
            self.assertIn("No detector calibration publish decision rows.", result["rendered_output"])

    def test_non_empty_manifest_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            ready = _row(
                status="ready_to_publish",
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                delta_iou=0.4,
                promotion_id="ace-ready",
                run_id="replay-003",
                decision_reason="publish_ready_with_sufficient_breadth",
            )
            broader = _row(
                status="needs_broader_evidence",
                asset_id="marvel_rivals.human_torch.hero_portrait",
                delta_iou=0.95,
                promotion_id="hero-broader",
                run_id="replay-001",
                decision_reason="publish_ready_but_single_evidence_base",
            )
            manifest_path.write_text(json.dumps(_manifest_payload(rows=[ready, broader]), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_publish_decision_manifest(manifest=manifest_path)
            self.assertIn("Asset id: marvel_rivals.ace.team_wipe_announcement", result["rendered_output"])
            self.assertIn("Run id: replay-003", result["rendered_output"])
            self.assertIn("Delta IoU: 0.4", result["rendered_output"])
            self.assertIn("Decision reason: publish_ready_with_sufficient_breadth", result["rendered_output"])
            self.assertIn("ready_to_publish", result["rendered_output"])
            self.assertIn("needs_broader_evidence", result["rendered_output"])
            self.assertIn("- marvel_rivals.human_torch.hero_portrait | delta_iou=0.95 | promotion_id=hero-broader", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            row = _row(
                status="needs_broader_evidence",
                asset_id="marvel_rivals.human_torch.hero_portrait",
                delta_iou=0.95,
                promotion_id="hero-broader",
                run_id="replay-001",
                decision_reason="publish_ready_but_single_evidence_base",
            )
            payload = _manifest_payload(rows=[row])
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_publish_decision_manifest(manifest=manifest_path, emit_json=True)
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_manifest_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "manifest missing required fields"):
                inspect_detector_calibration_publish_decision_manifest(manifest=manifest_path)

    def test_main_returns_error_code_for_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--manifest", str(manifest_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
