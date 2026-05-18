from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.detector_calibration_evidence_expansion import (
    abandon_evidence_expansion,
    check_evidence_expansion,
    create_evidence_expansion,
    link_evidence_expansion_promotion,
    link_evidence_expansion_session,
)


def _decision_manifest(*, rows: list[dict]) -> dict:
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
        "status_counts": {
            "ready_to_publish": sum(1 for row in rows if row.get("decision_status") == "ready_to_publish"),
            "needs_broader_evidence": sum(1 for row in rows if row.get("decision_status") == "needs_broader_evidence"),
            "defer": sum(1 for row in rows if row.get("decision_status") == "defer"),
        },
        "rows": rows,
    }


def _decision_row(*, asset_id: str, decision_status: str = "needs_broader_evidence") -> dict:
    return {
        "decision_status": decision_status,
        "decision_reason": "publish_ready_but_single_evidence_base",
        "triage_status": "publish_ready",
        "triage_reason": "validated_replay_improvement_meets_publish_threshold",
        "promotion_record_path": "/tmp/promotion.json",
        "promotion_root": "/tmp/promotion",
        "promotion_id": "promotion-001",
        "created_at": "2026-05-14T01:00:00Z",
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


def _review_record(*, asset_id: str) -> dict:
    return {
        "schema_version": "detector_calibration_review_v1",
        "game": "marvel_rivals",
        "target": {"asset_id": asset_id},
    }


def _promotion_record(*, asset_id: str) -> dict:
    return {
        "schema_version": "detector_calibration_revised_crop_promotion_v1",
        "published_asset": {"asset_id": asset_id},
    }


class DetectorCalibrationEvidenceExpansionTests(unittest.TestCase):
    def test_create_from_needs_broader_evidence_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = _write_json(
                root / "decision.json",
                _decision_manifest(rows=[_decision_row(asset_id="marvel_rivals.human_torch.hero_portrait")]),
            )
            output_path = root / "expansion.json"
            result = create_evidence_expansion(
                publish_decision_manifest=manifest_path,
                asset_id="marvel_rivals.human_torch.hero_portrait",
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "planned")
            self.assertEqual(payload["asset_id"], "marvel_rivals.human_torch.hero_portrait")
            self.assertEqual(len(payload["requested_evidence_items"]), 2)

    def test_link_session_rejects_asset_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_path = _create_expansion_fixture(root)
            session_root = root / "session-a"
            session_root.mkdir(parents=True, exist_ok=True)
            _write_json(session_root / "review_record.json", _review_record(asset_id="marvel_rivals.ace.team_wipe_announcement"))
            result = link_evidence_expansion_session(
                expansion_manifest_path=expansion_path,
                session_root=session_root,
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "asset_id_mismatch")

    def test_link_session_moves_planned_to_in_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_path = _create_expansion_fixture(root)
            session_root = root / "session-a"
            session_root.mkdir(parents=True, exist_ok=True)
            _write_json(session_root / "review_record.json", _review_record(asset_id="marvel_rivals.human_torch.hero_portrait"))
            result = link_evidence_expansion_session(
                expansion_manifest_path=expansion_path,
                session_root=session_root,
            )
            self.assertTrue(result["ok"])
            payload = json.loads(Path(expansion_path).read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "in_progress")
            self.assertEqual(payload["linked_session_roots"], [str(session_root.resolve())])
            duplicate = link_evidence_expansion_session(
                expansion_manifest_path=expansion_path,
                session_root=session_root,
            )
            self.assertEqual(len(duplicate["linked_session_roots"]), 1)

    def test_link_promotion_rejects_asset_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_path = _create_expansion_fixture(root)
            promotion_path = _write_json(
                root / "promotion.json",
                _promotion_record(asset_id="marvel_rivals.ace.team_wipe_announcement"),
            )
            result = link_evidence_expansion_promotion(
                expansion_manifest_path=expansion_path,
                promotion_record_path=promotion_path,
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "asset_id_mismatch")

    def test_check_derives_satisfied_only_from_ready_to_publish(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_path = _create_expansion_fixture(root, status="in_progress")
            same_manifest = _write_json(
                root / "decision-still-broader.json",
                _decision_manifest(rows=[_decision_row(asset_id="marvel_rivals.human_torch.hero_portrait", decision_status="needs_broader_evidence")]),
            )
            result = check_evidence_expansion(
                expansion_manifest_path=expansion_path,
                publish_decision_manifest=same_manifest,
                apply=False,
            )
            self.assertEqual(result["suggested_status"], "in_progress")
            ready_manifest = _write_json(
                root / "decision-ready.json",
                _decision_manifest(rows=[_decision_row(asset_id="marvel_rivals.human_torch.hero_portrait", decision_status="ready_to_publish")]),
            )
            ready_result = check_evidence_expansion(
                expansion_manifest_path=expansion_path,
                publish_decision_manifest=ready_manifest,
                apply=False,
            )
            self.assertEqual(ready_result["suggested_status"], "satisfied")

    def test_check_apply_persists_satisfied(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_path = _create_expansion_fixture(root, status="in_progress")
            ready_manifest = _write_json(
                root / "decision-ready.json",
                _decision_manifest(rows=[_decision_row(asset_id="marvel_rivals.human_torch.hero_portrait", decision_status="ready_to_publish")]),
            )
            result = check_evidence_expansion(
                expansion_manifest_path=expansion_path,
                publish_decision_manifest=ready_manifest,
                apply=True,
            )
            self.assertTrue(result["applied"])
            payload = json.loads(Path(expansion_path).read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "satisfied")

    def test_abandon_persists_status_and_note(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_path = _create_expansion_fixture(root, status="in_progress")
            result = abandon_evidence_expansion(
                expansion_manifest_path=expansion_path,
                note="not pursuing this asset now",
            )
            self.assertTrue(result["ok"])
            payload = json.loads(Path(expansion_path).read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "abandoned")
            self.assertEqual(payload["operator_notes"], ["not pursuing this asset now"])


def _create_expansion_fixture(root: Path, *, status: str = "planned") -> Path:
    decision_row = _decision_row(asset_id="marvel_rivals.human_torch.hero_portrait")
    payload = {
        "schema_version": "detector_calibration_evidence_expansion_v1",
        "game": "marvel_rivals",
        "asset_id": "marvel_rivals.human_torch.hero_portrait",
        "created_at": "2026-05-14T01:00:00+00:00",
        "source_publish_decision_manifest_path": str((root / "decision.json").resolve()),
        "source_publish_decision_row": decision_row,
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
    return _write_json(root / "expansion.json", payload)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
