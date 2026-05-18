from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_next_actions_manifest import (
    inspect_detector_calibration_next_actions_manifest,
    main as inspect_main,
)


def _next_actions_manifest_payload(*, rows: list[dict] | None = None) -> dict:
    rows = rows or []
    return {
        "schema_version": "detector_calibration_next_actions_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-15T01:00:00+00:00",
        "source_progress_manifest_path": "/tmp/progress.json",
        "row_count": len(rows),
        "status_counts": {
            "review_for_publish": sum(1 for row in rows if row.get("action_status") == "review_for_publish"),
            "collect_more_evidence": sum(1 for row in rows if row.get("action_status") == "collect_more_evidence"),
            "investigate_state_gap": sum(1 for row in rows if row.get("action_status") == "investigate_state_gap"),
        },
        "rows": rows,
    }


def _next_action_row(
    *,
    asset_id: str = "marvel_rivals.human_torch.hero_portrait",
    action_status: str = "collect_more_evidence",
    action_reason: str = "progress_row_requires_broader_evidence",
    progress_status: str = "actively_collecting",
    queue_status: str = "in_progress",
    decision_status: str = "needs_broader_evidence",
    triage_status: str = "publish_ready",
    recommended_action_note: str = "Need 1 more replay-backed source and 1 more distinct source for publish readiness.",
    remaining_replay_gap: int | None = 1,
    remaining_distinct_source_gap: int | None = 1,
) -> dict:
    return {
        "asset_id": asset_id,
        "action_status": action_status,
        "action_reason": action_reason,
        "progress_status": progress_status,
        "queue_status": queue_status,
        "decision_status": decision_status,
        "triage_status": triage_status,
        "remaining_replay_gap": remaining_replay_gap,
        "remaining_distinct_source_gap": remaining_distinct_source_gap,
        "linked_session_count": 1,
        "linked_promotion_count": 0,
        "recommended_action_note": recommended_action_note,
    }


class InspectDetectorCalibrationNextActionsManifestTests(unittest.TestCase):
    def test_empty_manifest_renders_explicit_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(_next_actions_manifest_payload(), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_actions_manifest(manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertIn("Top review-for-publish", result["rendered_output"])
            self.assertIn("No detector calibration next-action rows.", result["rendered_output"])

    def test_non_empty_manifest_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            payload = _next_actions_manifest_payload(
                rows=[
                    _next_action_row(),
                    _next_action_row(
                        asset_id="marvel_rivals.ace.team_wipe_announcement",
                        action_status="investigate_state_gap",
                        action_reason="progress_row_missing_publish_decision_state",
                        progress_status="not_triaged",
                        queue_status="planned",
                        decision_status=None,
                        triage_status=None,
                        recommended_action_note="Asset is missing a current publish-decision row.",
                        remaining_replay_gap=None,
                        remaining_distinct_source_gap=None,
                    ),
                ]
            )
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_actions_manifest(manifest=manifest_path)
            self.assertIn("Source progress manifest path: /tmp/progress.json", result["rendered_output"])
            self.assertIn("Top review-for-publish", result["rendered_output"])
            self.assertIn("None", result["rendered_output"])
            self.assertIn(
                "- marvel_rivals.human_torch.hero_portrait | progress_status=actively_collecting | remaining_replay_gap=1 | remaining_distinct_source_gap=1",
                result["rendered_output"],
            )
            self.assertIn(
                "- marvel_rivals.ace.team_wipe_announcement | progress_status=not_triaged | remaining_replay_gap=None | remaining_distinct_source_gap=None",
                result["rendered_output"],
            )

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            payload = _next_actions_manifest_payload(rows=[_next_action_row()])
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_actions_manifest(
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
                inspect_detector_calibration_next_actions_manifest(manifest=manifest_path)

    def test_main_returns_error_code_for_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--manifest", str(manifest_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
