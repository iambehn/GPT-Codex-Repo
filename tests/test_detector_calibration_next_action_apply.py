from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.detector_calibration_next_action_apply import (
    apply_detector_calibration_next_action,
)


def _next_actions_manifest(*, rows: list[dict], progress_manifest_path: str) -> dict:
    return {
        "schema_version": "detector_calibration_next_actions_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-15T01:00:00+00:00",
        "source_progress_manifest_path": progress_manifest_path,
        "row_count": len(rows),
        "status_counts": {
            "review_for_publish": sum(1 for row in rows if row.get("action_status") == "review_for_publish"),
            "collect_more_evidence": sum(1 for row in rows if row.get("action_status") == "collect_more_evidence"),
            "investigate_state_gap": sum(1 for row in rows if row.get("action_status") == "investigate_state_gap"),
        },
        "rows": rows,
    }


def _next_actions_row(
    *,
    asset_id: str = "marvel_rivals.human_torch.hero_portrait",
    action_status: str = "collect_more_evidence",
) -> dict:
    return {
        "asset_id": asset_id,
        "action_status": action_status,
        "action_reason": "progress_row_requires_broader_evidence",
        "progress_status": "actively_collecting",
        "queue_status": "in_progress",
        "decision_status": "needs_broader_evidence",
        "triage_status": "publish_ready",
        "remaining_replay_gap": 1,
        "remaining_distinct_source_gap": 1,
        "linked_session_count": 1,
        "linked_promotion_count": 0,
        "recommended_action_note": "Need 1 more replay-backed source and 1 more distinct source for publish readiness.",
    }


def _progress_manifest(*, publish_decision_manifest_path: str) -> dict:
    return {
        "schema_version": "detector_calibration_evidence_expansion_progress_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-15T01:00:00+00:00",
        "source_evidence_expansion_queue_manifest_path": "/tmp/queue.json",
        "source_publish_decision_manifest_path": publish_decision_manifest_path,
        "source_promotion_triage_manifest_path": "/tmp/triage.json",
        "row_count": 1,
        "status_counts": {
            "actively_collecting": 1,
            "waiting_for_work": 0,
            "ready_to_publish": 0,
            "not_triaged": 0,
        },
        "rows": [],
    }


def _publish_decision_manifest(*, rows: list[dict]) -> dict:
    return {
        "schema_version": "detector_calibration_publish_decision_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-15T01:00:00+00:00",
        "source_triage_manifest_path": "/tmp/triage.json",
        "source_row_count": len(rows),
        "decision_thresholds": {
            "minimum_publish_ready_replays_per_asset": 2,
            "minimum_distinct_sources_per_asset": 2,
        },
        "row_count": len(rows),
        "status_counts": {
            "ready_to_publish": sum(1 for row in rows if row.get("decision_status") == "ready_to_publish"),
            "needs_broader_evidence": sum(1 for row in rows if row.get("decision_status") == "needs_broader_evidence"),
            "defer": 0,
        },
        "rows": rows,
    }


def _publish_decision_row(
    *,
    asset_id: str = "marvel_rivals.human_torch.hero_portrait",
    decision_status: str = "needs_broader_evidence",
) -> dict:
    return {
        "decision_status": decision_status,
        "decision_reason": "publish_ready_but_single_evidence_base",
        "triage_status": "publish_ready",
        "triage_reason": "validated_replay_improvement_meets_publish_threshold",
        "promotion_record_path": "/tmp/promotion.json",
        "promotion_root": "/tmp/promotion",
        "promotion_id": "promotion-001",
        "created_at": "2026-05-15T01:00:00Z",
        "asset_id": asset_id,
        "candidate_id": "rev-001",
        "run_id": "replay-001",
        "delta_iou": 0.9,
        "absolute_delta_iou": 0.9,
        "difference_summary": "summary",
        "draft_validation_status": "ok",
        "replay_count_for_asset": 1,
        "distinct_source_count_for_asset": 1,
        "source_keys_for_asset": ["/tmp/source-a.mp4"],
    }


def _expansion_manifest(
    *,
    asset_id: str = "marvel_rivals.human_torch.hero_portrait",
    status: str,
    created_at: str = "2026-05-15T01:00:00+00:00",
) -> dict:
    return {
        "schema_version": "detector_calibration_evidence_expansion_v1",
        "game": "marvel_rivals",
        "asset_id": asset_id,
        "created_at": created_at,
        "source_publish_decision_manifest_path": "/tmp/decision.json",
        "source_publish_decision_row": _publish_decision_row(asset_id=asset_id),
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
        "requested_evidence_items": [],
        "linked_session_roots": [],
        "linked_promotion_record_paths": [],
        "operator_notes": [],
    }


class DetectorCalibrationNextActionApplyTests(unittest.TestCase):
    def test_reuses_existing_in_progress_expansion_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            next_actions_path, _, decision_path = _write_source_chain(root)
            expansion_root = root / "expansion-root"
            existing_path = _write_json(
                expansion_root / "hero_portrait" / "existing.detector_calibration_evidence_expansion.json",
                _expansion_manifest(status="in_progress"),
            )
            result = apply_detector_calibration_next_action(
                next_actions_manifest=next_actions_path,
                asset_id="marvel_rivals.human_torch.hero_portrait",
                evidence_expansion_root=expansion_root,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["reuse_mode"], "reused_existing")
            self.assertEqual(result["applied_action"], "reuse_existing_expansion")
            self.assertEqual(result["expansion_manifest_path"], str(existing_path.resolve()))
            self.assertEqual(result["publish_decision_manifest_path"], str(decision_path.resolve()))

    def test_reuses_existing_planned_when_no_in_progress_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            next_actions_path, _, _ = _write_source_chain(root)
            expansion_root = root / "expansion-root"
            planned_path = _write_json(
                expansion_root / "hero_portrait" / "planned.detector_calibration_evidence_expansion.json",
                _expansion_manifest(status="planned"),
            )
            result = apply_detector_calibration_next_action(
                next_actions_manifest=next_actions_path,
                asset_id="marvel_rivals.human_torch.hero_portrait",
                evidence_expansion_root=expansion_root,
            )
            self.assertEqual(result["reuse_mode"], "reused_existing")
            self.assertEqual(result["expansion_manifest_path"], str(planned_path.resolve()))

    def test_creates_new_when_only_abandoned_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            next_actions_path, _, _ = _write_source_chain(root)
            expansion_root = root / "expansion-root"
            _write_json(
                expansion_root / "hero_portrait" / "abandoned.detector_calibration_evidence_expansion.json",
                _expansion_manifest(status="abandoned"),
            )
            result = apply_detector_calibration_next_action(
                next_actions_manifest=next_actions_path,
                asset_id="marvel_rivals.human_torch.hero_portrait",
                evidence_expansion_root=expansion_root,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["reuse_mode"], "created_new")
            self.assertEqual(result["applied_action"], "create_new_expansion")
            self.assertTrue(Path(result["expansion_manifest_path"]).is_file())

    def test_creates_new_when_only_satisfied_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            next_actions_path, _, _ = _write_source_chain(root)
            expansion_root = root / "expansion-root"
            _write_json(
                expansion_root / "hero_portrait" / "satisfied.detector_calibration_evidence_expansion.json",
                _expansion_manifest(status="satisfied"),
            )
            result = apply_detector_calibration_next_action(
                next_actions_manifest=next_actions_path,
                asset_id="marvel_rivals.human_torch.hero_portrait",
                evidence_expansion_root=expansion_root,
            )
            self.assertEqual(result["reuse_mode"], "created_new")
            self.assertTrue(Path(result["expansion_manifest_path"]).is_file())

    def test_fails_when_publish_decision_row_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            next_actions_path, _, _ = _write_source_chain(
                root,
                decision_status="ready_to_publish",
            )
            result = apply_detector_calibration_next_action(
                next_actions_manifest=next_actions_path,
                asset_id="marvel_rivals.human_torch.hero_portrait",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "stale_publish_decision_status")

    def test_fails_for_unsupported_action_status(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            next_actions_path, _, _ = _write_source_chain(
                root,
                action_status="review_for_publish",
            )
            result = apply_detector_calibration_next_action(
                next_actions_manifest=next_actions_path,
                asset_id="marvel_rivals.human_torch.hero_portrait",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "unsupported_action_status")

    def test_source_chain_resolution_uses_progress_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            next_actions_path, progress_path, decision_path = _write_source_chain(root)
            result = apply_detector_calibration_next_action(
                next_actions_manifest=next_actions_path,
                asset_id="marvel_rivals.human_torch.hero_portrait",
                evidence_expansion_root=root / "expansion-root",
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["source_next_actions_manifest_path"], str(next_actions_path.resolve()))
            self.assertEqual(result["publish_decision_manifest_path"], str(decision_path.resolve()))
            self.assertTrue(progress_path.is_file())


def _write_source_chain(
    root: Path,
    *,
    decision_status: str = "needs_broader_evidence",
    action_status: str = "collect_more_evidence",
) -> tuple[Path, Path, Path]:
    decision_path = _write_json(
        root / "decision.json",
        _publish_decision_manifest(rows=[_publish_decision_row(decision_status=decision_status)]),
    )
    progress_path = _write_json(
        root / "progress.json",
        _progress_manifest(publish_decision_manifest_path=str(decision_path.resolve())),
    )
    next_actions_path = _write_json(
        root / "next-actions.json",
        _next_actions_manifest(
            rows=[_next_actions_row(action_status=action_status)],
            progress_manifest_path=str(progress_path.resolve()),
        ),
    )
    return next_actions_path, progress_path, decision_path


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
