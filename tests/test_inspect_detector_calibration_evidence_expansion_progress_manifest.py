from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_evidence_expansion_progress_manifest import (
    inspect_detector_calibration_evidence_expansion_progress_manifest,
    main as inspect_main,
)


def _progress_manifest_payload(*, rows: list[dict] | None = None) -> dict:
    rows = rows or []
    return {
        "schema_version": "detector_calibration_evidence_expansion_progress_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-14T01:00:00+00:00",
        "source_evidence_expansion_queue_manifest_path": "/tmp/queue.json",
        "source_publish_decision_manifest_path": "/tmp/publish-decision.json",
        "source_promotion_triage_manifest_path": "/tmp/triage.json",
        "row_count": len(rows),
        "status_counts": {
            "actively_collecting": sum(1 for row in rows if row.get("progress_status") == "actively_collecting"),
            "waiting_for_work": sum(1 for row in rows if row.get("progress_status") == "waiting_for_work"),
            "ready_to_publish": sum(1 for row in rows if row.get("progress_status") == "ready_to_publish"),
            "not_triaged": sum(1 for row in rows if row.get("progress_status") == "not_triaged"),
        },
        "rows": rows,
    }


def _progress_row(
    *,
    asset_id: str = "marvel_rivals.human_torch.hero_portrait",
    progress_status: str = "actively_collecting",
    progress_reason: str = "needs_broader_evidence_with_active_expansion_work",
    queue_status: str = "in_progress",
    decision_status: str = "needs_broader_evidence",
    triage_status: str = "publish_ready",
    remaining_replay_gap: int = 1,
    remaining_distinct_source_gap: int = 1,
) -> dict:
    return {
        "asset_id": asset_id,
        "progress_status": progress_status,
        "progress_reason": progress_reason,
        "queue_status": queue_status,
        "decision_status": decision_status,
        "triage_status": triage_status,
        "expansion_manifest_count": 1,
        "in_progress_manifest_count": 1,
        "linked_session_count": 1,
        "linked_promotion_count": 0,
        "replay_count_for_asset": 1,
        "distinct_source_count_for_asset": 1,
        "target_replay_count": 2,
        "target_distinct_source_count": 2,
        "remaining_replay_gap": remaining_replay_gap,
        "remaining_distinct_source_gap": remaining_distinct_source_gap,
    }


class InspectDetectorCalibrationEvidenceExpansionProgressManifestTests(unittest.TestCase):
    def test_empty_manifest_renders_explicit_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(_progress_manifest_payload(), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_evidence_expansion_progress_manifest(manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertIn("Top actively-collecting", result["rendered_output"])
            self.assertIn("No detector calibration evidence expansion progress rows.", result["rendered_output"])

    def test_non_empty_manifest_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            payload = _progress_manifest_payload(
                rows=[
                    _progress_row(),
                    _progress_row(
                        asset_id="marvel_rivals.ace.team_wipe_announcement",
                        progress_status="waiting_for_work",
                        progress_reason="needs_broader_evidence_without_active_expansion_work",
                        queue_status="planned",
                        remaining_replay_gap=1,
                        remaining_distinct_source_gap=1,
                    ),
                ]
            )
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_evidence_expansion_progress_manifest(manifest=manifest_path)
            self.assertIn("Asset id: marvel_rivals.human_torch.hero_portrait", result["rendered_output"])
            self.assertIn("Progress status: actively_collecting", result["rendered_output"])
            self.assertIn("Progress reason: needs_broader_evidence_with_active_expansion_work", result["rendered_output"])
            self.assertIn("Queue status: in_progress", result["rendered_output"])
            self.assertIn("Remaining replay gap: 1", result["rendered_output"])
            self.assertIn(
                "- marvel_rivals.human_torch.hero_portrait | queue_status=in_progress | remaining_replay_gap=1 | remaining_distinct_source_gap=1",
                result["rendered_output"],
            )
            self.assertIn(
                "- marvel_rivals.ace.team_wipe_announcement | queue_status=planned | remaining_replay_gap=1 | remaining_distinct_source_gap=1",
                result["rendered_output"],
            )

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            payload = _progress_manifest_payload(rows=[_progress_row()])
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_evidence_expansion_progress_manifest(
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
                inspect_detector_calibration_evidence_expansion_progress_manifest(manifest=manifest_path)

    def test_main_returns_error_code_for_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--manifest", str(manifest_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
