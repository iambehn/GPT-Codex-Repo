from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.detector_calibration_next_actions_manifest import (
    generate_detector_calibration_next_actions_manifest,
)


def _progress_manifest(*, rows: list[dict]) -> dict:
    return {
        "schema_version": "detector_calibration_evidence_expansion_progress_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-15T01:00:00+00:00",
        "source_evidence_expansion_queue_manifest_path": "/tmp/queue.json",
        "source_publish_decision_manifest_path": "/tmp/decision.json",
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
    asset_id: str,
    progress_status: str,
    queue_status: str,
    decision_status: str | None,
    triage_status: str | None,
    remaining_replay_gap: int | None,
    remaining_distinct_source_gap: int | None,
    linked_session_count: int = 0,
    linked_promotion_count: int = 0,
) -> dict:
    return {
        "asset_id": asset_id,
        "progress_status": progress_status,
        "progress_reason": f"{progress_status}-reason",
        "queue_status": queue_status,
        "decision_status": decision_status,
        "triage_status": triage_status,
        "expansion_manifest_count": 1,
        "in_progress_manifest_count": 1 if queue_status == "in_progress" else 0,
        "linked_session_count": linked_session_count,
        "linked_promotion_count": linked_promotion_count,
        "replay_count_for_asset": 1,
        "distinct_source_count_for_asset": 1,
        "target_replay_count": 2,
        "target_distinct_source_count": 2,
        "remaining_replay_gap": remaining_replay_gap,
        "remaining_distinct_source_gap": remaining_distinct_source_gap,
    }


class DetectorCalibrationNextActionsManifestTests(unittest.TestCase):
    def test_generate_manifest_derives_all_action_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            progress_path = _write_json(
                root / "progress.json",
                _progress_manifest(
                    rows=[
                        _progress_row(
                            asset_id="asset.review",
                            progress_status="ready_to_publish",
                            queue_status="satisfied",
                            decision_status="ready_to_publish",
                            triage_status="publish_ready",
                            remaining_replay_gap=0,
                            remaining_distinct_source_gap=0,
                            linked_promotion_count=1,
                        ),
                        _progress_row(
                            asset_id="asset.collecting",
                            progress_status="actively_collecting",
                            queue_status="in_progress",
                            decision_status="needs_broader_evidence",
                            triage_status="publish_ready",
                            remaining_replay_gap=1,
                            remaining_distinct_source_gap=1,
                            linked_session_count=2,
                        ),
                        _progress_row(
                            asset_id="asset.gap",
                            progress_status="not_triaged",
                            queue_status="planned",
                            decision_status=None,
                            triage_status=None,
                            remaining_replay_gap=None,
                            remaining_distinct_source_gap=None,
                        ),
                    ]
                ),
            )
            output_path = root / "next-actions.json"
            result = generate_detector_calibration_next_actions_manifest(
                progress_manifest=progress_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            ordered_assets = [row["asset_id"] for row in result["rows"]]
            self.assertEqual(ordered_assets, ["asset.review", "asset.collecting", "asset.gap"])
            by_asset = {row["asset_id"]: row for row in result["rows"]}
            self.assertEqual(by_asset["asset.review"]["action_status"], "review_for_publish")
            self.assertEqual(by_asset["asset.collecting"]["action_status"], "collect_more_evidence")
            self.assertEqual(by_asset["asset.gap"]["action_status"], "investigate_state_gap")
            self.assertEqual(
                result["status_counts"],
                {
                    "review_for_publish": 1,
                    "collect_more_evidence": 1,
                    "investigate_state_gap": 1,
                },
            )
            self.assertEqual(result["emitted_manifest"]["top_row"]["asset_id"], "asset.review")

    def test_generate_manifest_orders_collect_more_evidence_by_gap_then_linked_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            progress_path = _write_json(
                root / "progress.json",
                _progress_manifest(
                    rows=[
                        _progress_row(
                            asset_id="asset.small_gap",
                            progress_status="waiting_for_work",
                            queue_status="planned",
                            decision_status="needs_broader_evidence",
                            triage_status="publish_ready",
                            remaining_replay_gap=1,
                            remaining_distinct_source_gap=0,
                            linked_session_count=5,
                        ),
                        _progress_row(
                            asset_id="asset.big_gap",
                            progress_status="actively_collecting",
                            queue_status="in_progress",
                            decision_status="needs_broader_evidence",
                            triage_status="publish_ready",
                            remaining_replay_gap=1,
                            remaining_distinct_source_gap=1,
                            linked_session_count=1,
                        ),
                        _progress_row(
                            asset_id="asset.same_gap_more_sessions",
                            progress_status="actively_collecting",
                            queue_status="in_progress",
                            decision_status="needs_broader_evidence",
                            triage_status="publish_ready",
                            remaining_replay_gap=1,
                            remaining_distinct_source_gap=0,
                            linked_session_count=6,
                        ),
                    ]
                ),
            )
            result = generate_detector_calibration_next_actions_manifest(
                progress_manifest=progress_path,
                output_path=root / "next-actions.json",
            )
            self.assertEqual(
                [row["asset_id"] for row in result["rows"]],
                ["asset.big_gap", "asset.same_gap_more_sessions", "asset.small_gap"],
            )

    def test_generate_manifest_renders_deterministic_action_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            progress_path = _write_json(
                root / "progress.json",
                _progress_manifest(
                    rows=[
                        _progress_row(
                            asset_id="asset.collecting",
                            progress_status="actively_collecting",
                            queue_status="in_progress",
                            decision_status="needs_broader_evidence",
                            triage_status="publish_ready",
                            remaining_replay_gap=1,
                            remaining_distinct_source_gap=2,
                        ),
                        _progress_row(
                            asset_id="asset.review",
                            progress_status="ready_to_publish",
                            queue_status="satisfied",
                            decision_status="ready_to_publish",
                            triage_status="publish_ready",
                            remaining_replay_gap=0,
                            remaining_distinct_source_gap=0,
                        ),
                        _progress_row(
                            asset_id="asset.gap",
                            progress_status="not_triaged",
                            queue_status="planned",
                            decision_status=None,
                            triage_status=None,
                            remaining_replay_gap=None,
                            remaining_distinct_source_gap=None,
                        ),
                    ]
                ),
            )
            result = generate_detector_calibration_next_actions_manifest(
                progress_manifest=progress_path,
                output_path=root / "next-actions.json",
            )
            by_asset = {row["asset_id"]: row for row in result["rows"]}
            self.assertEqual(
                by_asset["asset.collecting"]["recommended_action_note"],
                "Need 1 more replay-backed source and 2 more distinct sources for publish readiness.",
            )
            self.assertEqual(
                by_asset["asset.review"]["recommended_action_note"],
                "Asset is ready_to_publish; review for live pack mutation.",
            )
            self.assertEqual(
                by_asset["asset.gap"]["recommended_action_note"],
                "Asset is missing a current publish-decision row.",
            )


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
