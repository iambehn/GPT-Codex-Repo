from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_evidence_expansion_queue_manifest import (
    inspect_detector_calibration_evidence_expansion_queue_manifest,
    main as inspect_main,
)


def _queue_manifest_payload(*, rows: list[dict] | None = None) -> dict:
    rows = rows or []
    return {
        "schema_version": "detector_calibration_evidence_expansion_queue_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-14T01:00:00+00:00",
        "source_manifest_count": len(rows),
        "row_count": len(rows),
        "status_counts": {
            "in_progress": sum(1 for row in rows if row.get("status") == "in_progress"),
            "planned": sum(1 for row in rows if row.get("status") == "planned"),
            "satisfied": sum(1 for row in rows if row.get("status") == "satisfied"),
            "abandoned": sum(1 for row in rows if row.get("status") == "abandoned"),
        },
        "rows": rows,
    }


def _queue_row(
    *,
    asset_id: str = "marvel_rivals.human_torch.hero_portrait",
    status: str = "in_progress",
    requested_evidence_item_count: int = 2,
    linked_session_count: int = 1,
    linked_promotion_count: int = 0,
    decision_status: str = "needs_broader_evidence",
    decision_reason: str = "publish_ready_but_single_evidence_base",
) -> dict:
    return {
        "status": status,
        "asset_id": asset_id,
        "expansion_manifest_path": "/tmp/expansion.json",
        "created_at": "2026-05-14T01:00:00+00:00",
        "source_publish_decision_manifest_path": "/tmp/publish-decision.json",
        "decision_status": decision_status,
        "decision_reason": decision_reason,
        "replay_count_for_asset": 1,
        "distinct_source_count_for_asset": 1,
        "requested_evidence_item_count": requested_evidence_item_count,
        "linked_session_count": linked_session_count,
        "linked_promotion_count": linked_promotion_count,
    }


class InspectDetectorCalibrationEvidenceExpansionQueueManifestTests(unittest.TestCase):
    def test_empty_manifest_renders_explicit_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(_queue_manifest_payload(), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_evidence_expansion_queue_manifest(manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertIn("Top in-progress", result["rendered_output"])
            self.assertIn("No detector calibration evidence expansion queue rows.", result["rendered_output"])

    def test_non_empty_manifest_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            payload = _queue_manifest_payload(
                rows=[
                    _queue_row(),
                    _queue_row(
                        asset_id="marvel_rivals.ace.team_wipe_announcement",
                        status="planned",
                        requested_evidence_item_count=1,
                        linked_session_count=0,
                        linked_promotion_count=1,
                    ),
                ]
            )
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_evidence_expansion_queue_manifest(manifest=manifest_path)
            self.assertIn("Asset id: marvel_rivals.human_torch.hero_portrait", result["rendered_output"])
            self.assertIn("Requested evidence item count: 2", result["rendered_output"])
            self.assertIn("Linked session count: 1", result["rendered_output"])
            self.assertIn("Decision status: needs_broader_evidence", result["rendered_output"])
            self.assertIn(
                "- marvel_rivals.human_torch.hero_portrait | requested_evidence_item_count=2 | linked_session_count=1 | linked_promotion_count=0",
                result["rendered_output"],
            )
            self.assertIn(
                "- marvel_rivals.ace.team_wipe_announcement | requested_evidence_item_count=1 | linked_session_count=0 | linked_promotion_count=1",
                result["rendered_output"],
            )

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            payload = _queue_manifest_payload(rows=[_queue_row()])
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_evidence_expansion_queue_manifest(
                manifest=manifest_path,
                emit_json=True,
            )
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_manifest_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "manifest missing required fields"):
                inspect_detector_calibration_evidence_expansion_queue_manifest(manifest=manifest_path)

    def test_main_returns_error_code_for_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--manifest", str(manifest_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
