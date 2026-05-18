from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_evidence_expansion import (
    inspect_detector_calibration_evidence_expansion,
    main as inspect_main,
)


def _manifest_payload(
    *,
    requested_evidence_items: list[dict] | None = None,
    linked_session_roots: list[str] | None = None,
    linked_promotion_record_paths: list[str] | None = None,
    operator_notes: list[str] | None = None,
    status: str = "in_progress",
) -> dict:
    return {
        "schema_version": "detector_calibration_evidence_expansion_v1",
        "game": "marvel_rivals",
        "asset_id": "marvel_rivals.human_torch.hero_portrait",
        "created_at": "2026-05-14T01:00:00+00:00",
        "source_publish_decision_manifest_path": "/tmp/publish-decision.json",
        "source_publish_decision_row": {"asset_id": "marvel_rivals.human_torch.hero_portrait"},
        "target_thresholds": {
            "minimum_publish_ready_replays_per_asset": 2,
            "minimum_distinct_sources_per_asset": 2,
        },
        "current_evidence_snapshot": {
            "decision_status": "needs_broader_evidence",
            "decision_reason": "publish_ready_but_single_evidence_base",
            "replay_count_for_asset": 1,
            "distinct_source_count_for_asset": 1,
            "source_keys_for_asset": ["/tmp/source-a.mp4"],
        },
        "status": status,
        "requested_evidence_items": requested_evidence_items or [],
        "linked_session_roots": linked_session_roots or [],
        "linked_promotion_record_paths": linked_promotion_record_paths or [],
        "operator_notes": operator_notes or [],
    }


class InspectDetectorCalibrationEvidenceExpansionTests(unittest.TestCase):
    def test_empty_sections_render_explicit_none(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(_manifest_payload(), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_evidence_expansion(manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertIn("Requested evidence", result["rendered_output"])
            self.assertIn("Linked sessions", result["rendered_output"])
            self.assertIn("Linked promotions", result["rendered_output"])
            self.assertIn("Operator notes", result["rendered_output"])
            self.assertIn("None", result["rendered_output"])

    def test_non_empty_manifest_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    _manifest_payload(
                        requested_evidence_items=[
                            {
                                "reason": "need_second_distinct_source",
                                "status": "planned",
                                "source_hint": "/tmp/source-b.mp4",
                            }
                        ],
                        linked_session_roots=["/tmp/session-a"],
                        linked_promotion_record_paths=["/tmp/promotion-a.json"],
                        operator_notes=["operator note"],
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = inspect_detector_calibration_evidence_expansion(manifest=manifest_path)
            self.assertIn("Asset id: marvel_rivals.human_torch.hero_portrait", result["rendered_output"])
            self.assertIn("Status: in_progress", result["rendered_output"])
            self.assertIn("reason=need_second_distinct_source | status=planned | source_hint=/tmp/source-b.mp4", result["rendered_output"])
            self.assertIn("- /tmp/session-a", result["rendered_output"])
            self.assertIn("- /tmp/promotion-a.json", result["rendered_output"])
            self.assertIn("- operator note", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            payload = _manifest_payload(
                linked_session_roots=["/tmp/session-a"],
                operator_notes=["operator note"],
            )
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_evidence_expansion(manifest=manifest_path, emit_json=True)
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_manifest_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "manifest missing required fields"):
                inspect_detector_calibration_evidence_expansion(manifest=manifest_path)

    def test_main_returns_error_code_for_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--manifest", str(manifest_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
