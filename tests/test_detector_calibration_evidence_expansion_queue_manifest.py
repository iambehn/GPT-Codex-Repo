from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.detector_calibration_evidence_expansion_queue_manifest import (
    generate_detector_calibration_evidence_expansion_queue_manifest,
)


def _expansion_manifest(
    *,
    asset_id: str,
    status: str,
    created_at: str,
    decision_status: str = "needs_broader_evidence",
    decision_reason: str = "publish_ready_but_single_evidence_base",
    replay_count_for_asset: int = 1,
    distinct_source_count_for_asset: int = 1,
    requested_evidence_items: list[dict] | None = None,
    linked_session_roots: list[str] | None = None,
    linked_promotion_record_paths: list[str] | None = None,
) -> dict:
    return {
        "schema_version": "detector_calibration_evidence_expansion_v1",
        "game": "marvel_rivals",
        "asset_id": asset_id,
        "created_at": created_at,
        "source_publish_decision_manifest_path": "/tmp/publish_decision.json",
        "source_publish_decision_row": {
            "decision_status": decision_status,
            "decision_reason": decision_reason,
            "replay_count_for_asset": replay_count_for_asset,
            "distinct_source_count_for_asset": distinct_source_count_for_asset,
        },
        "target_thresholds": {
            "minimum_publish_ready_replays_per_asset": 2,
            "minimum_distinct_sources_per_asset": 2,
        },
        "current_evidence_snapshot": {
            "decision_status": decision_status,
            "decision_reason": decision_reason,
            "replay_count_for_asset": replay_count_for_asset,
            "distinct_source_count_for_asset": distinct_source_count_for_asset,
        },
        "status": status,
        "requested_evidence_items": requested_evidence_items or [],
        "linked_session_roots": linked_session_roots or [],
        "linked_promotion_record_paths": linked_promotion_record_paths or [],
        "operator_notes": [],
    }


class DetectorCalibrationEvidenceExpansionQueueManifestTests(unittest.TestCase):
    def test_generate_manifest_reflects_single_in_progress_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_root = root / "outputs" / "detector_calibration" / "marvel_rivals" / "evidence_expansion"
            manifest_path = _write_expansion_manifest(
                expansion_root / "hero_portrait" / "20260513T234641Z.detector_calibration_evidence_expansion.json",
                _expansion_manifest(
                    asset_id="marvel_rivals.human_torch.hero_portrait",
                    status="in_progress",
                    created_at="2026-05-13T23:46:41.182722+00:00",
                    requested_evidence_items=[{"reason": "a"}, {"reason": "b"}],
                    linked_session_roots=["/tmp/session-a"],
                ),
            )
            output_path = root / "queue.json"
            result = generate_detector_calibration_evidence_expansion_queue_manifest(
                game="marvel_rivals",
                evidence_expansion_root=expansion_root,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["row_count"], 1)
            row = result["rows"][0]
            self.assertEqual(row["status"], "in_progress")
            self.assertEqual(row["asset_id"], "marvel_rivals.human_torch.hero_portrait")
            self.assertEqual(row["requested_evidence_item_count"], 2)
            self.assertEqual(row["linked_session_count"], 1)
            self.assertEqual(row["linked_promotion_count"], 0)
            self.assertEqual(row["expansion_manifest_path"], str(manifest_path.resolve()))
            self.assertEqual(
                result["emitted_manifest"],
                {
                    "path": str(output_path.resolve()),
                    "row_count": 1,
                    "source_manifest_count": 1,
                    "status_counts": {
                        "in_progress": 1,
                        "planned": 0,
                        "satisfied": 0,
                        "abandoned": 0,
                    },
                    "top_row": row,
                },
            )

    def test_generate_manifest_sorts_by_status_then_requested_count_then_recency(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_root = root / "outputs" / "detector_calibration" / "marvel_rivals" / "evidence_expansion"
            fixtures = [
                (
                    "planned-newer",
                    _expansion_manifest(
                        asset_id="marvel_rivals.asset.planned_newer",
                        status="planned",
                        created_at="2026-05-14T03:00:00Z",
                        requested_evidence_items=[{"reason": "a"}],
                    ),
                ),
                (
                    "abandoned",
                    _expansion_manifest(
                        asset_id="marvel_rivals.asset.abandoned",
                        status="abandoned",
                        created_at="2026-05-14T04:00:00Z",
                        requested_evidence_items=[{"reason": "a"}, {"reason": "b"}, {"reason": "c"}],
                    ),
                ),
                (
                    "in-progress-high",
                    _expansion_manifest(
                        asset_id="marvel_rivals.asset.in_progress_high",
                        status="in_progress",
                        created_at="2026-05-14T02:00:00Z",
                        requested_evidence_items=[{"reason": "a"}, {"reason": "b"}],
                    ),
                ),
                (
                    "planned-older-more",
                    _expansion_manifest(
                        asset_id="marvel_rivals.asset.planned_older_more",
                        status="planned",
                        created_at="2026-05-14T01:00:00Z",
                        requested_evidence_items=[{"reason": "a"}, {"reason": "b"}],
                    ),
                ),
                (
                    "satisfied",
                    _expansion_manifest(
                        asset_id="marvel_rivals.asset.satisfied",
                        status="satisfied",
                        created_at="2026-05-14T05:00:00Z",
                        requested_evidence_items=[],
                    ),
                ),
                (
                    "in-progress-low-newer",
                    _expansion_manifest(
                        asset_id="marvel_rivals.asset.in_progress_low_newer",
                        status="in_progress",
                        created_at="2026-05-14T06:00:00Z",
                        requested_evidence_items=[{"reason": "a"}],
                    ),
                ),
            ]
            for name, payload in fixtures:
                _write_expansion_manifest(
                    expansion_root / name / f"{name}.detector_calibration_evidence_expansion.json",
                    payload,
                )
            with patch(
                "tools.detector_calibration_evidence_expansion_queue_manifest._utc_now",
                return_value="2026-05-14T07:00:00+00:00",
            ):
                result = generate_detector_calibration_evidence_expansion_queue_manifest(
                    game="marvel_rivals",
                    evidence_expansion_root=expansion_root,
                    output_path=root / "queue.json",
                )
            ordered_asset_ids = [row["asset_id"] for row in result["rows"]]
            self.assertEqual(
                ordered_asset_ids,
                [
                    "marvel_rivals.asset.in_progress_high",
                    "marvel_rivals.asset.in_progress_low_newer",
                    "marvel_rivals.asset.planned_older_more",
                    "marvel_rivals.asset.planned_newer",
                    "marvel_rivals.asset.satisfied",
                    "marvel_rivals.asset.abandoned",
                ],
            )
            manifest = json.loads((root / "queue.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["generated_at"], "2026-05-14T07:00:00+00:00")
            self.assertEqual(
                manifest["status_counts"],
                {
                    "in_progress": 2,
                    "planned": 2,
                    "satisfied": 1,
                    "abandoned": 1,
                },
            )

    def test_generate_manifest_handles_empty_root(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_root = root / "missing-expansions"
            result = generate_detector_calibration_evidence_expansion_queue_manifest(
                game="marvel_rivals",
                evidence_expansion_root=expansion_root,
                output_path=root / "queue.json",
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["row_count"], 0)
            self.assertEqual(result["rows"], [])
            self.assertEqual(
                result["status_counts"],
                {
                    "in_progress": 0,
                    "planned": 0,
                    "satisfied": 0,
                    "abandoned": 0,
                },
            )
            self.assertIsNone(result["emitted_manifest"]["top_row"])

    def test_generate_manifest_uses_stored_status_without_recomputation(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            expansion_root = root / "outputs" / "detector_calibration" / "marvel_rivals" / "evidence_expansion"
            _write_expansion_manifest(
                expansion_root / "hero_portrait" / "expansion.detector_calibration_evidence_expansion.json",
                _expansion_manifest(
                    asset_id="marvel_rivals.human_torch.hero_portrait",
                    status="planned",
                    created_at="2026-05-14T01:00:00Z",
                    decision_status="ready_to_publish",
                    replay_count_for_asset=2,
                    distinct_source_count_for_asset=2,
                ),
            )
            result = generate_detector_calibration_evidence_expansion_queue_manifest(
                game="marvel_rivals",
                evidence_expansion_root=expansion_root,
                output_path=root / "queue.json",
            )
            self.assertEqual(result["rows"][0]["status"], "planned")
            self.assertEqual(result["status_counts"]["planned"], 1)
            self.assertEqual(result["status_counts"]["satisfied"], 0)


def _write_expansion_manifest(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
