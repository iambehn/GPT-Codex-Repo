from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.detector_calibration_evidence_expansion_progress_manifest import (
    generate_detector_calibration_evidence_expansion_progress_manifest,
)


def _queue_manifest(*, rows: list[dict]) -> dict:
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
    asset_id: str,
    status: str,
    linked_session_count: int = 0,
    linked_promotion_count: int = 0,
) -> dict:
    return {
        "status": status,
        "asset_id": asset_id,
        "expansion_manifest_path": f"/tmp/{asset_id}.expansion.json",
        "created_at": "2026-05-14T01:00:00+00:00",
        "source_publish_decision_manifest_path": "/tmp/publish-decision.json",
        "decision_status": "needs_broader_evidence",
        "decision_reason": "publish_ready_but_single_evidence_base",
        "replay_count_for_asset": 1,
        "distinct_source_count_for_asset": 1,
        "requested_evidence_item_count": 2,
        "linked_session_count": linked_session_count,
        "linked_promotion_count": linked_promotion_count,
    }


def _publish_decision_manifest(*, rows: list[dict]) -> dict:
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


def _decision_row(
    *,
    asset_id: str,
    decision_status: str,
    replay_count_for_asset: int,
    distinct_source_count_for_asset: int,
) -> dict:
    return {
        "decision_status": decision_status,
        "decision_reason": "publish_ready_but_single_evidence_base",
        "triage_status": "publish_ready",
        "triage_reason": "validated_replay_improvement_meets_publish_threshold",
        "promotion_record_path": f"/tmp/{asset_id}.promotion.json",
        "promotion_root": f"/tmp/{asset_id}.promotion",
        "promotion_id": f"{asset_id}-promotion",
        "created_at": "2026-05-14T01:00:00Z",
        "asset_id": asset_id,
        "candidate_id": "rev-001",
        "run_id": "replay-001",
        "delta_iou": 0.2,
        "absolute_delta_iou": 0.2,
        "difference_summary": "summary",
        "draft_validation_status": "ok",
        "replay_count_for_asset": replay_count_for_asset,
        "distinct_source_count_for_asset": distinct_source_count_for_asset,
        "source_keys_for_asset": ["/tmp/source-a.mp4"],
    }


def _promotion_triage_manifest(*, rows: list[dict]) -> dict:
    return {
        "schema_version": "detector_calibration_promotion_triage_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-14T01:00:00+00:00",
        "publish_ready_threshold_delta_iou": 0.15,
        "source_promotion_count": len(rows),
        "row_count": len(rows),
        "status_counts": {
            "publish_ready": sum(1 for row in rows if row.get("triage_status") == "publish_ready"),
            "needs_more_replay": sum(1 for row in rows if row.get("triage_status") == "needs_more_replay"),
            "hold": sum(1 for row in rows if row.get("triage_status") == "hold"),
        },
        "rows": rows,
    }


def _triage_row(*, asset_id: str, triage_status: str = "publish_ready") -> dict:
    return {
        "triage_status": triage_status,
        "triage_reason": "validated_replay_improvement_meets_publish_threshold",
        "promotion_record_path": f"/tmp/{asset_id}.promotion.json",
        "promotion_root": f"/tmp/{asset_id}.promotion",
        "promotion_id": f"{asset_id}-promotion",
        "created_at": "2026-05-14T01:00:00Z",
        "asset_id": asset_id,
        "candidate_id": "rev-001",
        "run_id": "replay-001",
        "delta_iou": 0.2,
        "absolute_delta_iou": 0.2,
        "primary_iou": 1.0,
        "secondary_iou": 0.8,
        "primary_source": "replay_run",
        "secondary_source": "crop_candidate",
        "primary_reference_source": "localized_match",
        "secondary_reference_source": "roi_fallback",
        "difference_summary": "summary",
        "draft_validation_status": "ok",
    }


class DetectorCalibrationEvidenceExpansionProgressManifestTests(unittest.TestCase):
    def test_generate_manifest_derives_all_progress_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            queue_path = _write_json(
                root / "queue.json",
                _queue_manifest(
                    rows=[
                        _queue_row(asset_id="asset.collecting", status="in_progress", linked_session_count=1),
                        _queue_row(asset_id="asset.waiting", status="planned"),
                        _queue_row(asset_id="asset.ready", status="satisfied", linked_promotion_count=1),
                        _queue_row(asset_id="asset.untriaged", status="planned"),
                    ]
                ),
            )
            decision_path = _write_json(
                root / "decision.json",
                _publish_decision_manifest(
                    rows=[
                        _decision_row(asset_id="asset.collecting", decision_status="needs_broader_evidence", replay_count_for_asset=1, distinct_source_count_for_asset=1),
                        _decision_row(asset_id="asset.waiting", decision_status="needs_broader_evidence", replay_count_for_asset=1, distinct_source_count_for_asset=0),
                        _decision_row(asset_id="asset.ready", decision_status="ready_to_publish", replay_count_for_asset=2, distinct_source_count_for_asset=2),
                    ]
                ),
            )
            triage_path = _write_json(
                root / "triage.json",
                _promotion_triage_manifest(
                    rows=[
                        _triage_row(asset_id="asset.collecting"),
                        _triage_row(asset_id="asset.waiting"),
                        _triage_row(asset_id="asset.ready"),
                    ]
                ),
            )
            output_path = root / "progress.json"
            result = generate_detector_calibration_evidence_expansion_progress_manifest(
                queue_manifest=queue_path,
                publish_decision_manifest=decision_path,
                promotion_triage_manifest=triage_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            ordered_assets = [row["asset_id"] for row in result["rows"]]
            self.assertEqual(
                ordered_assets,
                ["asset.collecting", "asset.waiting", "asset.ready", "asset.untriaged"],
            )
            by_asset = {row["asset_id"]: row for row in result["rows"]}
            self.assertEqual(by_asset["asset.collecting"]["progress_status"], "actively_collecting")
            self.assertEqual(by_asset["asset.waiting"]["progress_status"], "waiting_for_work")
            self.assertEqual(by_asset["asset.ready"]["progress_status"], "ready_to_publish")
            self.assertEqual(by_asset["asset.untriaged"]["progress_status"], "not_triaged")
            self.assertEqual(
                result["status_counts"],
                {
                    "actively_collecting": 1,
                    "waiting_for_work": 1,
                    "ready_to_publish": 1,
                    "not_triaged": 1,
                },
            )
            self.assertEqual(result["emitted_manifest"]["top_row"]["asset_id"], "asset.collecting")

    def test_generate_manifest_aggregates_multiple_queue_rows_for_same_asset(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            queue_path = _write_json(
                root / "queue.json",
                _queue_manifest(
                    rows=[
                        _queue_row(asset_id="asset.multi", status="planned", linked_session_count=1),
                        _queue_row(asset_id="asset.multi", status="in_progress", linked_session_count=2, linked_promotion_count=1),
                    ]
                ),
            )
            decision_path = _write_json(
                root / "decision.json",
                _publish_decision_manifest(
                    rows=[
                        _decision_row(asset_id="asset.multi", decision_status="needs_broader_evidence", replay_count_for_asset=1, distinct_source_count_for_asset=1),
                    ]
                ),
            )
            triage_path = _write_json(
                root / "triage.json",
                _promotion_triage_manifest(rows=[_triage_row(asset_id="asset.multi")]),
            )
            result = generate_detector_calibration_evidence_expansion_progress_manifest(
                queue_manifest=queue_path,
                publish_decision_manifest=decision_path,
                promotion_triage_manifest=triage_path,
                output_path=root / "progress.json",
            )
            row = result["rows"][0]
            self.assertEqual(row["expansion_manifest_count"], 2)
            self.assertEqual(row["in_progress_manifest_count"], 1)
            self.assertEqual(row["linked_session_count"], 3)
            self.assertEqual(row["linked_promotion_count"], 1)
            self.assertEqual(row["queue_status"], "in_progress")

    def test_generate_manifest_orders_remaining_gap_before_in_progress_count(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            queue_path = _write_json(
                root / "queue.json",
                _queue_manifest(
                    rows=[
                        _queue_row(asset_id="asset.big_gap", status="in_progress"),
                        _queue_row(asset_id="asset.small_gap", status="in_progress"),
                    ]
                ),
            )
            decision_path = _write_json(
                root / "decision.json",
                _publish_decision_manifest(
                    rows=[
                        _decision_row(asset_id="asset.big_gap", decision_status="needs_broader_evidence", replay_count_for_asset=0, distinct_source_count_for_asset=0),
                        _decision_row(asset_id="asset.small_gap", decision_status="needs_broader_evidence", replay_count_for_asset=1, distinct_source_count_for_asset=1),
                    ]
                ),
            )
            triage_path = _write_json(
                root / "triage.json",
                _promotion_triage_manifest(rows=[_triage_row(asset_id="asset.big_gap"), _triage_row(asset_id="asset.small_gap")]),
            )
            result = generate_detector_calibration_evidence_expansion_progress_manifest(
                queue_manifest=queue_path,
                publish_decision_manifest=decision_path,
                promotion_triage_manifest=triage_path,
                output_path=root / "progress.json",
            )
            self.assertEqual([row["asset_id"] for row in result["rows"]], ["asset.big_gap", "asset.small_gap"])


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
